// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package s3select provides S3 Select functionality for querying objects using SQL.
//
// S3 Select allows clients to use SQL statements to filter and project data from
// CSV, JSON, and Parquet objects. This reduces data transfer by only returning
// the requested subset of data.
//
// Architecture Overview:
//
//	┌─────────────────┐     ┌──────────────┐     ┌─────────────────┐
//	│  HTTP Handler   │────▶│  SQL Parser  │────▶│  Query Executor │
//	└─────────────────┘     └──────────────┘     └─────────────────┘
//	                                                      │
//	                              ┌───────────────────────┼───────────────────────┐
//	                              ▼                       ▼                       ▼
//	                      ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
//	                      │  CSV Reader  │       │  JSON Reader │       │Parquet Reader│
//	                      └──────────────┘       └──────────────┘       └──────────────┘
//	                              │                       │                       │
//	                              └───────────────────────┼───────────────────────┘
//	                                                      ▼
//	                                              ┌──────────────┐
//	                                              │ Event Stream │
//	                                              │   Writer     │
//	                                              └──────────────┘
//
// Implementation Notes:
// - SQL parsing can use a library like github.com/xwb1989/sqlparser or custom parser
// - Event stream format follows AWS S3 Select specification (binary protocol)
// - Supports streaming for large objects without loading entire file into memory
package s3select

import (
	"context"
	"io"
)

// ============================================================================
// Request Types
// ============================================================================

// SelectRequest represents an S3 Select request.
type SelectRequest struct {
	Bucket     string
	Key        string
	VersionID  string
	Expression string
	Input      InputSerialization
	Output     OutputSerialization

	// Optional: scan range for large objects
	ScanRange *ScanRange

	// Optional: request progress updates
	RequestProgress bool
}

// ScanRange specifies the byte range to scan (for large objects).
type ScanRange struct {
	Start int64
	End   int64
}

// InputSerialization defines how input data should be parsed.
type InputSerialization struct {
	// CompressionType: NONE, GZIP, BZIP2
	CompressionType string

	// Only one of these should be set
	CSV     *CSVInput
	JSON    *JSONInput
	Parquet *ParquetInput
}

// CSVInput defines CSV parsing options.
type CSVInput struct {
	// FileHeaderInfo: USE (first row is header), IGNORE, NONE
	FileHeaderInfo string

	// Comments: character indicating comment lines (e.g., "#")
	Comments string

	// QuoteEscapeCharacter: character used to escape quotes
	QuoteEscapeCharacter string

	// RecordDelimiter: character separating records (default: "\n")
	RecordDelimiter string

	// FieldDelimiter: character separating fields (default: ",")
	FieldDelimiter string

	// QuoteCharacter: character used for quoting (default: "\"")
	QuoteCharacter string

	// AllowQuotedRecordDelimiter: allow record delimiter within quotes
	AllowQuotedRecordDelimiter bool
}

// JSONInput defines JSON parsing options.
type JSONInput struct {
	// Type: DOCUMENT (single JSON object) or LINES (JSON lines format)
	Type string
}

// ParquetInput defines Parquet parsing options.
// Parquet is self-describing, so minimal configuration needed.
type ParquetInput struct {
	// No fields - Parquet is self-describing
}

// OutputSerialization defines how output data should be formatted.
type OutputSerialization struct {
	// Only one of these should be set
	CSV  *CSVOutput
	JSON *JSONOutput
}

// CSVOutput defines CSV output formatting options.
type CSVOutput struct {
	// QuoteFields: ALWAYS, ASNEEDED
	QuoteFields string

	// QuoteEscapeCharacter: character used to escape quotes
	QuoteEscapeCharacter string

	// RecordDelimiter: character separating records (default: "\n")
	RecordDelimiter string

	// FieldDelimiter: character separating fields (default: ",")
	FieldDelimiter string

	// QuoteCharacter: character used for quoting (default: "\"")
	QuoteCharacter string
}

// JSONOutput defines JSON output formatting options.
type JSONOutput struct {
	// RecordDelimiter: character separating records (default: "\n")
	RecordDelimiter string
}

// ============================================================================
// Response Types
// ============================================================================

// SelectResult represents the result of an S3 Select query.
// Results are streamed via the EventStream.
type SelectResult struct {
	// EventStream provides access to result events
	EventStream EventStream
}

// EventStream is an iterator over S3 Select events.
type EventStream interface {
	// Next returns the next event, or nil when done.
	// Returns an error if the stream encounters a problem.
	Next() (Event, error)

	// Close closes the event stream and releases resources.
	Close() error
}

// Event represents an S3 Select event in the response stream.
type Event interface {
	eventType() string
}

// RecordsEvent contains query result records.
type RecordsEvent struct {
	// Payload contains the query result data (CSV, JSON, etc.)
	Payload []byte
}

func (e *RecordsEvent) eventType() string { return "Records" }

// StatsEvent contains query execution statistics.
type StatsEvent struct {
	// BytesScanned is the total bytes scanned from the object
	BytesScanned int64

	// BytesProcessed is the total bytes processed after decompression
	BytesProcessed int64

	// BytesReturned is the total bytes returned in the response
	BytesReturned int64
}

func (e *StatsEvent) eventType() string { return "Stats" }

// ProgressEvent contains query progress information.
type ProgressEvent struct {
	// BytesScanned is the bytes scanned so far
	BytesScanned int64

	// BytesProcessed is the bytes processed so far
	BytesProcessed int64

	// BytesReturned is the bytes returned so far
	BytesReturned int64
}

func (e *ProgressEvent) eventType() string { return "Progress" }

// ContinuationEvent is a keep-alive message during long queries.
type ContinuationEvent struct{}

func (e *ContinuationEvent) eventType() string { return "Cont" }

// EndEvent signals the end of the event stream.
type EndEvent struct{}

func (e *EndEvent) eventType() string { return "End" }

// ============================================================================
// SQL Query Types
// ============================================================================

// Query represents a parsed SQL query.
type Query struct {
	// Select columns/expressions
	Projections []Projection

	// FROM clause (always "s3object" or alias)
	FromAlias string

	// WHERE clause (optional)
	Where Expression

	// LIMIT clause (optional, 0 means no limit)
	Limit int64
}

// Projection represents a SELECT column or expression.
type Projection struct {
	// Expression to evaluate
	Expr Expression

	// Alias for the result (optional)
	Alias string
}

// Expression represents a SQL expression.
// This is a marker interface - implementations define specific expression types.
type Expression interface {
	exprType() string
}

// ColumnRef references a column by name or index.
type ColumnRef struct {
	Name  string // Column name (for CSV with headers, JSON)
	Index int    // Column index (for CSV without headers), -1 if using name
}

func (e *ColumnRef) exprType() string { return "column" }

// Literal represents a literal value (string, number, bool, null).
type Literal struct {
	Value any
}

func (e *Literal) exprType() string { return "literal" }

// BinaryOp represents a binary operation (e.g., a > b, a AND b).
type BinaryOp struct {
	Left  Expression
	Op    string // =, <>, <, >, <=, >=, AND, OR, LIKE, etc.
	Right Expression
}

func (e *BinaryOp) exprType() string { return "binary" }

// UnaryOp represents a unary operation (e.g., NOT x, -x).
type UnaryOp struct {
	Op   string // NOT, -, etc.
	Expr Expression
}

func (e *UnaryOp) exprType() string { return "unary" }

// FunctionCall represents a function call (e.g., CAST(x AS INT), LOWER(s)).
type FunctionCall struct {
	Name string
	Args []Expression
}

func (e *FunctionCall) exprType() string { return "function" }

// StarExpr represents SELECT *.
type StarExpr struct{}

func (e *StarExpr) exprType() string { return "star" }

// ============================================================================
// Data Types
// ============================================================================

// Record represents a single row/record from the input data.
type Record interface {
	// Get returns the value for a column by name.
	// Returns nil if column doesn't exist.
	Get(name string) any

	// GetByIndex returns the value for a column by index.
	// Returns nil if index is out of range.
	GetByIndex(index int) any

	// ColumnNames returns all column names in order.
	ColumnNames() []string

	// Values returns all values in order.
	Values() []any
}

// ============================================================================
// Interfaces
// ============================================================================

// Service defines the S3 Select service interface.
type Service interface {
	// Select executes an S3 Select query and returns a streaming result.
	Select(ctx context.Context, req *SelectRequest, objectReader io.ReadCloser) (*SelectResult, error)
}

// Parser parses SQL expressions for S3 Select.
type Parser interface {
	// Parse parses a SQL expression and returns a Query.
	// S3 Select supports a subset of SQL:
	// - SELECT with column names, *, and expressions
	// - FROM s3object (or alias)
	// - WHERE with comparison and logical operators
	// - LIMIT
	// - Functions: CAST, SUBSTRING, TRIM, LOWER, UPPER, etc.
	Parse(expression string) (*Query, error)
}

// RecordReader reads records from input data.
type RecordReader interface {
	// Read returns the next record, or io.EOF when done.
	Read() (Record, error)

	// Close closes the reader and releases resources.
	Close() error
}

// RecordWriter writes records to output format.
type RecordWriter interface {
	// WriteRecord writes a record to the output.
	WriteRecord(record Record) error

	// Flush flushes any buffered data.
	Flush() error

	// Close closes the writer.
	Close() error
}

// Evaluator evaluates SQL expressions against records.
type Evaluator interface {
	// Evaluate evaluates an expression against a record.
	// Returns the result value (string, int64, float64, bool, or nil).
	Evaluate(expr Expression, record Record) (any, error)

	// EvaluateBool evaluates an expression and returns a boolean.
	// Used for WHERE clause evaluation.
	EvaluateBool(expr Expression, record Record) (bool, error)
}
