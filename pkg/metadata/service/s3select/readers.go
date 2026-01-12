// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
)

// ============================================================================
// CSV Reader
// ============================================================================

// csvRecordReader reads CSV records.
type csvRecordReader struct {
	reader        *csv.Reader
	headers       []string
	useHeaders    bool
	maxRecordSize int
	closed        bool
}

// NewCSVReader creates a new CSV record reader.
// This is the public constructor for external use.
func NewCSVReader(r io.Reader, opts *CSVInput) RecordReader {
	reader, err := newCSVReader(r, opts, 1024*1024) // 1MB default max record size
	if err != nil {
		// Return a reader that will return the error on first Read
		return &errorReader{err: err}
	}
	return reader
}

// errorReader returns an error on first read.
type errorReader struct {
	err error
}

func (r *errorReader) Read() (Record, error) { return nil, r.err }
func (r *errorReader) Close() error          { return nil }

// newCSVReader creates a new CSV record reader.
func newCSVReader(r io.Reader, opts *CSVInput, maxRecordSize int) (RecordReader, error) {
	cr := csv.NewReader(r)

	// Configure CSV parser based on options
	if opts.FieldDelimiter != "" && len(opts.FieldDelimiter) == 1 {
		cr.Comma = rune(opts.FieldDelimiter[0])
	}
	if opts.Comments != "" && len(opts.Comments) == 1 {
		cr.Comment = rune(opts.Comments[0])
	}
	cr.LazyQuotes = opts.AllowQuotedRecordDelimiter

	reader := &csvRecordReader{
		reader:        cr,
		maxRecordSize: maxRecordSize,
	}

	// Handle header row
	switch opts.FileHeaderInfo {
	case "USE":
		// Read first row as headers
		headers, err := cr.Read()
		if err != nil {
			if err == io.EOF {
				return reader, nil
			}
			return nil, &SelectError{
				Code:    "CSVParsingError",
				Message: fmt.Sprintf("failed to read CSV header: %v", err),
			}
		}
		reader.headers = headers
		reader.useHeaders = true
	case "IGNORE":
		// Skip first row, don't use as headers
		_, err := cr.Read()
		if err != nil && err != io.EOF {
			return nil, &SelectError{
				Code:    "CSVParsingError",
				Message: fmt.Sprintf("failed to skip CSV header: %v", err),
			}
		}
		reader.useHeaders = false
	case "NONE", "":
		// No header row
		reader.useHeaders = false
	}

	return reader, nil
}

// Read returns the next CSV record.
func (r *csvRecordReader) Read() (Record, error) {
	if r.closed {
		return nil, io.EOF
	}

	fields, err := r.reader.Read()
	if err != nil {
		return nil, err
	}

	return &csvRecord{
		headers: r.headers,
		values:  fields,
	}, nil
}

// Close closes the CSV reader.
func (r *csvRecordReader) Close() error {
	r.closed = true
	return nil
}

// csvRecord represents a CSV record.
type csvRecord struct {
	headers []string
	values  []string
}

func (r *csvRecord) Get(name string) any {
	for i, h := range r.headers {
		if h == name && i < len(r.values) {
			return r.values[i]
		}
	}
	return nil
}

func (r *csvRecord) GetByIndex(index int) any {
	if index >= 0 && index < len(r.values) {
		return r.values[index]
	}
	return nil
}

func (r *csvRecord) ColumnNames() []string {
	if len(r.headers) > 0 {
		return r.headers
	}
	// Generate column names: _1, _2, _3, ...
	names := make([]string, len(r.values))
	for i := range r.values {
		names[i] = fmt.Sprintf("_%d", i+1)
	}
	return names
}

func (r *csvRecord) Values() []any {
	result := make([]any, len(r.values))
	for i, v := range r.values {
		result[i] = v
	}
	return result
}

// ============================================================================
// JSON Reader
// ============================================================================

// jsonRecordReader reads JSON records.
type jsonRecordReader struct {
	decoder       *json.Decoder
	isDocument    bool
	documentRead  bool
	maxRecordSize int
	closed        bool
}

// newJSONReader creates a new JSON record reader.
func newJSONReader(r io.Reader, opts *JSONInput, maxRecordSize int) (RecordReader, error) {
	reader := &jsonRecordReader{
		decoder:       json.NewDecoder(r),
		isDocument:    opts.Type == "DOCUMENT",
		maxRecordSize: maxRecordSize,
	}
	return reader, nil
}

// Read returns the next JSON record.
func (r *jsonRecordReader) Read() (Record, error) {
	if r.closed {
		return nil, io.EOF
	}

	// For DOCUMENT type, entire JSON is one record
	if r.isDocument && r.documentRead {
		return nil, io.EOF
	}

	var data map[string]any
	if err := r.decoder.Decode(&data); err != nil {
		return nil, err
	}

	if r.isDocument {
		r.documentRead = true
	}

	return &jsonRecord{data: data}, nil
}

// Close closes the JSON reader.
func (r *jsonRecordReader) Close() error {
	r.closed = true
	return nil
}

// jsonRecord represents a JSON record.
type jsonRecord struct {
	data map[string]any
	keys []string // cached keys for ordering
}

func (r *jsonRecord) Get(name string) any {
	return r.data[name]
}

func (r *jsonRecord) GetByIndex(index int) any {
	keys := r.ColumnNames()
	if index >= 0 && index < len(keys) {
		return r.data[keys[index]]
	}
	return nil
}

func (r *jsonRecord) ColumnNames() []string {
	if r.keys == nil {
		r.keys = make([]string, 0, len(r.data))
		for k := range r.data {
			r.keys = append(r.keys, k)
		}
	}
	return r.keys
}

func (r *jsonRecord) Values() []any {
	keys := r.ColumnNames()
	result := make([]any, len(keys))
	for i, k := range keys {
		result[i] = r.data[k]
	}
	return result
}

// ============================================================================
// Parquet Reader (Stub)
// ============================================================================

// parquetRecordReader reads Parquet records.
// TODO: Implement using a Parquet library like github.com/xitongsys/parquet-go
type parquetRecordReader struct {
	closed bool
}

// Compile-time check that parquetRecordReader implements RecordReader.
var _ RecordReader = (*parquetRecordReader)(nil)

// newParquetReader creates a new Parquet record reader.
func newParquetReader(r io.Reader, opts *ParquetInput) (RecordReader, error) {
	// TODO: Implement Parquet support
	// Parquet is a columnar format that requires specialized parsing.
	// Consider using:
	// - github.com/xitongsys/parquet-go
	// - github.com/apache/arrow/go/parquet
	//
	// Mark parameters as used for future implementation
	_ = r
	_ = opts
	return nil, &SelectError{
		Code:    "UnsupportedFormat",
		Message: "Parquet format is not yet supported",
	}
}

func (r *parquetRecordReader) Read() (Record, error) {
	return nil, io.EOF
}

func (r *parquetRecordReader) Close() error {
	r.closed = true
	return nil
}
