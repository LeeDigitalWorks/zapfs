// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select/eventstream"
)

const defaultChunkSize = 256 * 1024 // 256KB

// DefaultProgressInterval is the default interval for sending progress events.
const DefaultProgressInterval = 10 * time.Second

// Executor executes S3 Select queries.
type Executor struct {
	query      *Query
	reader     RecordReader
	output     *CSVOutput
	jsonOutput *JSONOutput
	chunkSize  int

	// Progress tracking
	bytesScanned     int64
	bytesProcessed   int64
	bytesReturned    int64
	recordsProcessed int64

	// Progress reporting
	progressEnabled  bool
	progressInterval time.Duration
	lastProgressTime time.Time
}

// ExecutorOption is a functional option for configuring an Executor.
type ExecutorOption func(*Executor)

// WithProgress enables progress event reporting at the specified interval.
func WithProgress(enabled bool, interval time.Duration) ExecutorOption {
	return func(e *Executor) {
		e.progressEnabled = enabled
		if interval > 0 {
			e.progressInterval = interval
		} else {
			e.progressInterval = DefaultProgressInterval
		}
	}
}

// WithJSONOutput configures the executor for JSON output.
func WithJSONOutput(output *JSONOutput) ExecutorOption {
	return func(e *Executor) {
		if output == nil {
			output = &JSONOutput{}
		}
		e.jsonOutput = output
	}
}

// WithChunkSize sets the chunk size for record batching.
func WithChunkSize(size int) ExecutorOption {
	return func(e *Executor) {
		if size > 0 {
			e.chunkSize = size
		}
	}
}

// NewExecutor creates a query executor.
func NewExecutor(query *Query, reader RecordReader, output *CSVOutput, opts ...ExecutorOption) *Executor {
	if output == nil {
		output = &CSVOutput{}
	}
	e := &Executor{
		query:            query,
		reader:           reader,
		output:           output,
		chunkSize:        defaultChunkSize,
		progressInterval: DefaultProgressInterval,
		lastProgressTime: time.Now(),
	}

	// Apply options
	for _, opt := range opts {
		opt(e)
	}

	return e
}

// Execute runs the query and writes results to the writer.
func (e *Executor) Execute(ctx context.Context, w io.Writer) error {
	// Check if this is an aggregate query
	if e.hasAggregates() {
		return e.executeAggregate(ctx, w)
	}
	return e.executeRegular(ctx, w)
}

// maybeWriteProgress writes a progress event if enabled and enough time has passed.
func (e *Executor) maybeWriteProgress(encoder *eventstream.Encoder) {
	if !e.progressEnabled {
		return
	}
	if time.Since(e.lastProgressTime) >= e.progressInterval {
		encoder.WriteProgress(eventstream.ProgressStats{
			BytesScanned:   e.bytesScanned,
			BytesProcessed: e.bytesProcessed,
			BytesReturned:  e.bytesReturned,
		})
		e.lastProgressTime = time.Now()
	}
}

// hasAggregates checks if the query contains any aggregate functions.
func (e *Executor) hasAggregates() bool {
	for _, p := range e.query.Projections {
		if e.exprHasAggregate(p.Expr) {
			return true
		}
	}
	return false
}

// exprHasAggregate checks if an expression contains aggregate functions.
func (e *Executor) exprHasAggregate(expr Expression) bool {
	if expr == nil {
		return false
	}

	switch ex := expr.(type) {
	case *FunctionCall:
		return IsAggregateFunction(ex.Name)
	case *BinaryOp:
		return e.exprHasAggregate(ex.Left) || e.exprHasAggregate(ex.Right)
	case *UnaryOp:
		return e.exprHasAggregate(ex.Expr)
	default:
		return false
	}
}

// aggregateInfo holds information about an aggregate in a projection.
type aggregateInfo struct {
	accumulator Accumulator
	argExpr     Expression // nil for COUNT(*)
	isCountStar bool
}

// executeAggregate executes a query with aggregate functions.
func (e *Executor) executeAggregate(ctx context.Context, w io.Writer) error {
	encoder := eventstream.NewEncoder(w)

	// Initialize accumulators for each aggregate in projections
	aggregates := make([]*aggregateInfo, len(e.query.Projections))
	for i, p := range e.query.Projections {
		fc, ok := p.Expr.(*FunctionCall)
		if !ok {
			// Non-aggregate in aggregate query - not supported yet
			// For now, we'll evaluate it against an empty record
			aggregates[i] = nil
			continue
		}

		acc, err := NewAccumulator(fc.Name)
		if err != nil {
			// Not an aggregate function - skip
			aggregates[i] = nil
			continue
		}

		// Check if this is COUNT(*)
		isCountStar := false
		var argExpr Expression
		if strings.ToUpper(fc.Name) == "COUNT" {
			if len(fc.Args) == 0 {
				isCountStar = true
			} else if _, ok := fc.Args[0].(*StarExpr); ok {
				isCountStar = true
			} else {
				argExpr = fc.Args[0]
			}
		} else if len(fc.Args) > 0 {
			argExpr = fc.Args[0]
		}

		aggregates[i] = &aggregateInfo{
			accumulator: acc,
			argExpr:     argExpr,
			isCountStar: isCountStar,
		}
	}

	// Process all records
	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next record
		record, err := e.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			encoder.WriteError("InternalError", err.Error())
			return err
		}

		// Track bytes scanned (estimate based on record size)
		recordSize := int64(e.estimateRecordSize(record))
		e.bytesScanned += recordSize
		e.bytesProcessed += recordSize

		// Apply WHERE filter
		if e.query.Where != nil {
			match, err := e.evaluateBool(e.query.Where, record)
			if err != nil {
				encoder.WriteError("InvalidQuery", err.Error())
				return err
			}
			if !match {
				// Check for progress reporting
				e.maybeWriteProgress(encoder)
				continue
			}
		}

		// Feed values to accumulators
		for _, agg := range aggregates {
			if agg == nil {
				continue
			}

			var value any
			if agg.isCountStar {
				value = nil // Doesn't matter for COUNT(*)
			} else if agg.argExpr != nil {
				val, err := e.evaluate(agg.argExpr, record)
				if err != nil {
					encoder.WriteError("InvalidQuery", err.Error())
					return err
				}
				value = val
			}

			agg.accumulator.Accumulate(value, agg.isCountStar)
		}

		// Track records processed
		e.recordsProcessed++

		// Check for progress reporting
		e.maybeWriteProgress(encoder)
	}

	// Build result row from aggregate results
	var buf bytes.Buffer

	row := make([]string, len(e.query.Projections))
	for i, agg := range aggregates {
		if agg != nil {
			row[i] = toString(agg.accumulator.Result())
		} else {
			// Non-aggregate expression - evaluate against nil record
			row[i] = ""
		}
	}

	if e.jsonOutput != nil {
		// JSON output for aggregates
		obj := make(map[string]any, len(e.query.Projections))
		for i, p := range e.query.Projections {
			key := p.Alias
			if key == "" {
				if fc, ok := p.Expr.(*FunctionCall); ok {
					key = fc.Name
				} else {
					key = fmt.Sprintf("_%d", i+1)
				}
			}
			obj[key] = row[i]
		}
		jsonBytes, _ := json.Marshal(obj)
		buf.Write(jsonBytes)
		delimiter := "\n"
		if e.jsonOutput.RecordDelimiter != "" {
			delimiter = e.jsonOutput.RecordDelimiter
		}
		buf.WriteString(delimiter)
	} else {
		csvWriter := csv.NewWriter(&buf)

		// Configure output delimiter
		if e.output.FieldDelimiter != "" && len(e.output.FieldDelimiter) > 0 {
			csvWriter.Comma = rune(e.output.FieldDelimiter[0])
		}

		csvWriter.Write(row)
		csvWriter.Flush()
	}

	if buf.Len() > 0 {
		e.bytesReturned = int64(buf.Len())
		encoder.WriteRecords(buf.Bytes())
	}

	// Write stats and end
	encoder.WriteStats(e.bytesScanned, e.bytesProcessed, e.bytesReturned)
	encoder.WriteEnd()

	return nil
}

// executeRegular executes a regular (non-aggregate) query.
func (e *Executor) executeRegular(ctx context.Context, w io.Writer) error {
	encoder := eventstream.NewEncoder(w)

	var buf bytes.Buffer

	// Set up output writer based on format
	useJSON := e.jsonOutput != nil
	var csvWriter *csv.Writer
	if !useJSON {
		csvWriter = csv.NewWriter(&buf)

		// Configure output delimiter
		if e.output.FieldDelimiter != "" && len(e.output.FieldDelimiter) > 0 {
			csvWriter.Comma = rune(e.output.FieldDelimiter[0])
		}
	}

	// JSON record delimiter
	jsonDelimiter := "\n"
	if useJSON && e.jsonOutput.RecordDelimiter != "" {
		jsonDelimiter = e.jsonOutput.RecordDelimiter
	}

	recordCount := int64(0)

	for {
		// Check for cancellation
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read next record
		record, err := e.reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			encoder.WriteError("InternalError", err.Error())
			return err
		}

		// Track bytes scanned (estimate based on record size)
		recordSize := int64(e.estimateRecordSize(record))
		e.bytesScanned += recordSize
		e.bytesProcessed += recordSize

		// Apply WHERE filter
		if e.query.Where != nil {
			match, err := e.evaluateBool(e.query.Where, record)
			if err != nil {
				encoder.WriteError("InvalidQuery", err.Error())
				return err
			}
			if !match {
				// Check for progress reporting
				e.maybeWriteProgress(encoder)
				continue
			}
		}

		// Project columns
		row, err := e.projectRecord(record)
		if err != nil {
			encoder.WriteError("InvalidQuery", err.Error())
			return err
		}

		if useJSON {
			// Write JSON record
			if recordCount > 0 {
				buf.WriteString(jsonDelimiter)
			}
			obj := e.buildJSONObject(record, row)
			jsonBytes, err := json.Marshal(obj)
			if err != nil {
				encoder.WriteError("InternalError", err.Error())
				return err
			}
			buf.Write(jsonBytes)
		} else {
			csvWriter.Write(row)
		}
		recordCount++
		e.recordsProcessed++

		// Check LIMIT
		if e.query.Limit > 0 && recordCount >= e.query.Limit {
			break
		}

		// Flush chunk if needed
		if !useJSON {
			csvWriter.Flush()
		}
		if buf.Len() >= e.chunkSize {
			e.bytesReturned += int64(buf.Len())
			encoder.WriteRecords(buf.Bytes())
			buf.Reset()
		}

		// Check for progress reporting
		e.maybeWriteProgress(encoder)
	}

	// Final flush
	if !useJSON {
		csvWriter.Flush()
	}
	if useJSON && recordCount > 0 {
		buf.WriteString(jsonDelimiter)
	}
	if buf.Len() > 0 {
		e.bytesReturned += int64(buf.Len())
		encoder.WriteRecords(buf.Bytes())
	}

	// Write stats and end
	encoder.WriteStats(e.bytesScanned, e.bytesProcessed, e.bytesReturned)
	encoder.WriteEnd()

	return nil
}

// buildJSONObject creates a JSON object from a record and its projected row values.
func (e *Executor) buildJSONObject(record Record, row []string) map[string]any {
	obj := make(map[string]any, len(row))

	// Handle SELECT *
	if len(e.query.Projections) == 1 {
		if _, ok := e.query.Projections[0].Expr.(*StarExpr); ok {
			names := record.ColumnNames()
			values := record.Values()
			for i, name := range names {
				if i < len(values) {
					key := name
					if key == "" {
						key = fmt.Sprintf("_%d", i+1)
					}
					obj[key] = values[i]
				}
			}
			return obj
		}
	}

	if len(e.query.Projections) == 0 {
		names := record.ColumnNames()
		values := record.Values()
		for i, name := range names {
			if i < len(values) {
				key := name
				if key == "" {
					key = fmt.Sprintf("_%d", i+1)
				}
				obj[key] = values[i]
			}
		}
		return obj
	}

	for i, p := range e.query.Projections {
		if _, ok := p.Expr.(*StarExpr); ok {
			names := record.ColumnNames()
			values := record.Values()
			for j, name := range names {
				if j < len(values) {
					key := name
					if key == "" {
						key = fmt.Sprintf("_%d", j+1)
					}
					obj[key] = values[j]
				}
			}
			return obj
		}

		key := p.Alias
		if key == "" {
			if col, ok := p.Expr.(*ColumnRef); ok && col.Name != "" {
				key = col.Name
			} else {
				key = fmt.Sprintf("_%d", i+1)
			}
		}
		if i < len(row) {
			obj[key] = row[i]
		}
	}
	return obj
}

// estimateRecordSize estimates the size of a record in bytes.
func (e *Executor) estimateRecordSize(record Record) int {
	size := 0
	for _, v := range record.Values() {
		size += len(toString(v))
	}
	return size
}

func (e *Executor) projectRecord(record Record) ([]string, error) {
	// Handle SELECT *
	if len(e.query.Projections) == 1 {
		if _, ok := e.query.Projections[0].Expr.(*StarExpr); ok {
			return toStringSlice(record.Values()), nil
		}
	}

	if len(e.query.Projections) == 0 {
		return toStringSlice(record.Values()), nil
	}

	// Project specific columns
	row := make([]string, 0, len(e.query.Projections))
	for _, p := range e.query.Projections {
		// Check for star in projections
		if _, ok := p.Expr.(*StarExpr); ok {
			return toStringSlice(record.Values()), nil
		}

		val, err := e.evaluate(p.Expr, record)
		if err != nil {
			return nil, err
		}
		row = append(row, toString(val))
	}

	return row, nil
}

// evaluate evaluates an expression against a record.
func (e *Executor) evaluate(expr Expression, record Record) (any, error) {
	if expr == nil {
		return nil, &SelectError{
			Code:    "InvalidExpression",
			Message: "expression is nil",
		}
	}

	switch ex := expr.(type) {
	case *ColumnRef:
		if ex.Name != "" {
			return record.Get(ex.Name), nil
		}
		return record.GetByIndex(ex.Index), nil

	case *Literal:
		return ex.Value, nil

	case *StarExpr:
		return record.Values(), nil

	case *BinaryOp:
		return e.evalBinaryOp(ex, record)

	case *UnaryOp:
		return e.evalUnaryOp(ex, record)

	default:
		return nil, &SelectError{
			Code:    "InvalidExpression",
			Message: fmt.Sprintf("unsupported expression type: %T", expr),
		}
	}
}

// evaluateBool evaluates an expression as a boolean.
func (e *Executor) evaluateBool(expr Expression, record Record) (bool, error) {
	result, err := e.evaluate(expr, record)
	if err != nil {
		return false, err
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case nil:
		return false, nil
	default:
		return toBool(result), nil
	}
}

func (e *Executor) evalBinaryOp(op *BinaryOp, record Record) (any, error) {
	left, err := e.evaluate(op.Left, record)
	if err != nil {
		return nil, err
	}
	right, err := e.evaluate(op.Right, record)
	if err != nil {
		return nil, err
	}

	return applyBinaryOp(op.Op, left, right)
}

func (e *Executor) evalUnaryOp(op *UnaryOp, record Record) (any, error) {
	val, err := e.evaluate(op.Expr, record)
	if err != nil {
		return nil, err
	}

	switch op.Op {
	case "NOT", "not":
		return !toBool(val), nil
	case "-":
		return -toFloat64(val), nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", op.Op)
	}
}

func applyBinaryOp(op string, left, right any) (any, error) {
	switch op {
	case "=", "==":
		return equals(left, right), nil
	case "!=", "<>":
		return !equals(left, right), nil
	case "<":
		return compare(left, right) < 0, nil
	case ">":
		return compare(left, right) > 0, nil
	case "<=":
		return compare(left, right) <= 0, nil
	case ">=":
		return compare(left, right) >= 0, nil
	case "AND", "and":
		return toBool(left) && toBool(right), nil
	case "OR", "or":
		return toBool(left) || toBool(right), nil
	default:
		return nil, fmt.Errorf("unsupported operator: %s", op)
	}
}

func equals(left, right any) bool {
	// String comparison
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return ls == rs
	}

	// Numeric comparison
	return toFloat64(left) == toFloat64(right)
}

func compare(left, right any) int {
	l := toFloat64(left)
	r := toFloat64(right)

	if l < r {
		return -1
	} else if l > r {
		return 1
	}
	return 0
}

func toFloat64(v any) float64 {
	switch val := v.(type) {
	case int:
		return float64(val)
	case int64:
		return float64(val)
	case float64:
		return val
	case float32:
		return float64(val)
	case string:
		var f float64
		fmt.Sscanf(val, "%f", &f)
		return f
	default:
		return 0
	}
}

func toBool(v any) bool {
	switch val := v.(type) {
	case bool:
		return val
	case nil:
		return false
	case int:
		return val != 0
	case int64:
		return val != 0
	case float64:
		return val != 0
	case string:
		return val != ""
	default:
		return true
	}
}

func toStringSlice(vals []any) []string {
	result := make([]string, len(vals))
	for i, v := range vals {
		result[i] = toString(v)
	}
	return result
}

func toString(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	default:
		return fmt.Sprintf("%v", val)
	}
}

// Stats returns the execution statistics.
func (e *Executor) Stats() (bytesScanned, bytesProcessed, bytesReturned, recordsProcessed int64) {
	return e.bytesScanned, e.bytesProcessed, e.bytesReturned, e.recordsProcessed
}
