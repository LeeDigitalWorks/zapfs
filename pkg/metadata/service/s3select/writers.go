// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"encoding/csv"
	"encoding/json"
	"io"
)

// ============================================================================
// CSV Writer
// ============================================================================

// csvRecordWriter writes records as CSV.
type csvRecordWriter struct {
	writer      *csv.Writer
	columns     []string
	quoteFields string
}

// newCSVWriter creates a new CSV record writer.
func newCSVWriter(w io.Writer, opts *CSVOutput, query *Query) (RecordWriter, error) {
	cw := csv.NewWriter(w)

	// Configure CSV writer based on options
	if opts.FieldDelimiter != "" && len(opts.FieldDelimiter) == 1 {
		cw.Comma = rune(opts.FieldDelimiter[0])
	}

	// Determine output columns from query projections
	var columns []string
	for _, p := range query.Projections {
		if p.Alias != "" {
			columns = append(columns, p.Alias)
		} else if col, ok := p.Expr.(*ColumnRef); ok {
			columns = append(columns, col.Name)
		} else {
			columns = append(columns, "")
		}
	}

	return &csvRecordWriter{
		writer:      cw,
		columns:     columns,
		quoteFields: opts.QuoteFields,
	}, nil
}

// WriteRecord writes a record as CSV.
func (w *csvRecordWriter) WriteRecord(record Record) error {
	values := record.Values()
	row := make([]string, len(values))
	for i, v := range values {
		row[i] = formatValue(v)
	}
	return w.writer.Write(row)
}

// Flush flushes buffered data.
func (w *csvRecordWriter) Flush() error {
	w.writer.Flush()
	return w.writer.Error()
}

// Close closes the writer.
func (w *csvRecordWriter) Close() error {
	w.writer.Flush()
	return w.writer.Error()
}

// ============================================================================
// JSON Writer
// ============================================================================

// jsonRecordWriter writes records as JSON.
type jsonRecordWriter struct {
	writer          io.Writer
	encoder         *json.Encoder
	recordDelimiter string
	firstRecord     bool
}

// newJSONWriter creates a new JSON record writer.
func newJSONWriter(w io.Writer, opts *JSONOutput) (RecordWriter, error) {
	delimiter := opts.RecordDelimiter
	if delimiter == "" {
		delimiter = "\n"
	}

	return &jsonRecordWriter{
		writer:          w,
		encoder:         json.NewEncoder(w),
		recordDelimiter: delimiter,
		firstRecord:     true,
	}, nil
}

// WriteRecord writes a record as JSON.
func (w *jsonRecordWriter) WriteRecord(record Record) error {
	// Build JSON object from record
	obj := make(map[string]any)
	names := record.ColumnNames()
	values := record.Values()
	for i, name := range names {
		if i < len(values) {
			obj[name] = values[i]
		}
	}

	// Write JSON with delimiter
	if !w.firstRecord {
		if _, err := w.writer.Write([]byte(w.recordDelimiter)); err != nil {
			return err
		}
	}
	w.firstRecord = false

	return w.encoder.Encode(obj)
}

// Flush flushes buffered data.
func (w *jsonRecordWriter) Flush() error {
	if f, ok := w.writer.(interface{ Flush() error }); ok {
		return f.Flush()
	}
	return nil
}

// Close closes the writer.
func (w *jsonRecordWriter) Close() error {
	return w.Flush()
}

// ============================================================================
// Helpers
// ============================================================================

// formatValue converts a value to string for output.
func formatValue(v any) string {
	if v == nil {
		return ""
	}
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		// Use JSON encoding for other types
		b, _ := json.Marshal(val)
		return string(b)
	}
}
