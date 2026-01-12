// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
)

// ============================================================================
// JSON Reader
// ============================================================================

// JSONReader reads JSON records in either LINES or DOCUMENT mode.
type JSONReader struct {
	decoder    *json.Decoder
	input      *JSONInput
	closed     bool
	arrayItems []map[string]any // For DOCUMENT mode with array
	arrayIndex int              // Current position in array
	singleDone bool             // For DOCUMENT mode with single object
}

// NewJSONReader creates a new JSON record reader.
// LINES mode: reads newline-delimited JSON (one object per line)
// DOCUMENT mode: reads a JSON array or single object
func NewJSONReader(r io.Reader, input *JSONInput) *JSONReader {
	if input == nil {
		input = &JSONInput{Type: "LINES"}
	}
	return &JSONReader{
		decoder: json.NewDecoder(r),
		input:   input,
	}
}

// Read returns the next JSON record.
// For LINES mode, each line is parsed as a separate JSON object.
// For DOCUMENT mode, if the input is an array, each element is returned as a record.
// If the input is a single object, it is returned once.
func (r *JSONReader) Read() (Record, error) {
	if r.closed {
		return nil, io.EOF
	}

	if r.input.Type == "DOCUMENT" {
		return r.readDocument()
	}
	return r.readLines()
}

// readLines reads the next JSON object in LINES mode.
func (r *JSONReader) readLines() (Record, error) {
	var data map[string]any
	if err := r.decoder.Decode(&data); err != nil {
		return nil, err
	}
	return NewJSONRecord(data), nil
}

// readDocument reads records in DOCUMENT mode.
// On first call, parses the entire JSON (array or object).
// For arrays, iterates through elements.
// For single objects, returns once then EOF.
func (r *JSONReader) readDocument() (Record, error) {
	// If we haven't parsed the document yet
	if r.arrayItems == nil && !r.singleDone {
		// Read the raw JSON token to determine if it's an array or object
		var raw json.RawMessage
		if err := r.decoder.Decode(&raw); err != nil {
			return nil, err
		}

		// Try to parse as array first
		var arr []map[string]any
		if err := json.Unmarshal(raw, &arr); err == nil {
			r.arrayItems = arr
			r.arrayIndex = 0
		} else {
			// Try as single object
			var obj map[string]any
			if err := json.Unmarshal(raw, &obj); err != nil {
				return nil, &SelectError{
					Code:    "JSONParsingError",
					Message: fmt.Sprintf("invalid JSON document: %v", err),
				}
			}
			r.singleDone = true
			return NewJSONRecord(obj), nil
		}
	}

	// Handle array iteration
	if r.arrayItems != nil {
		if r.arrayIndex >= len(r.arrayItems) {
			return nil, io.EOF
		}
		record := NewJSONRecord(r.arrayItems[r.arrayIndex])
		r.arrayIndex++
		return record, nil
	}

	// Single object already returned
	return nil, io.EOF
}

// Close closes the JSON reader.
func (r *JSONReader) Close() error {
	r.closed = true
	return nil
}

// ============================================================================
// JSON Record
// ============================================================================

// JSONRecord represents a JSON record with nested path access support.
type JSONRecord struct {
	data map[string]any
	keys []string // cached keys for consistent ordering
}

// NewJSONRecord creates a new JSON record.
func NewJSONRecord(data map[string]any) *JSONRecord {
	return &JSONRecord{data: data}
}

// Get returns the value for a field, supporting nested path access.
// Supports dot notation for nested fields: "address.city"
// Supports array indexing: "items[0].id"
func (r *JSONRecord) Get(name string) any {
	return getNestedValue(r.data, name)
}

// GetByIndex returns the value for a field by index.
// Returns nil if index is out of range.
func (r *JSONRecord) GetByIndex(index int) any {
	keys := r.ColumnNames()
	if index >= 0 && index < len(keys) {
		return r.data[keys[index]]
	}
	return nil
}

// ColumnNames returns all top-level field names in sorted order.
func (r *JSONRecord) ColumnNames() []string {
	if r.keys == nil {
		r.keys = make([]string, 0, len(r.data))
		for k := range r.data {
			r.keys = append(r.keys, k)
		}
		// Sort for consistent ordering
		sort.Strings(r.keys)
	}
	return r.keys
}

// Values returns all top-level values in the same order as ColumnNames.
func (r *JSONRecord) Values() []any {
	keys := r.ColumnNames()
	result := make([]any, len(keys))
	for i, k := range keys {
		result[i] = r.data[k]
	}
	return result
}

// Data returns the underlying map data.
func (r *JSONRecord) Data() map[string]any {
	return r.data
}

// newJSONReader creates a new JSON record reader (internal use with maxRecordSize).
// This is used by the service layer for consistency with other readers.
func newJSONReader(r io.Reader, opts *JSONInput, maxRecordSize int) (RecordReader, error) {
	// maxRecordSize is not currently used but kept for API consistency
	// with other readers. Future implementations may use it to limit
	// the size of individual JSON records.
	_ = maxRecordSize
	return NewJSONReader(r, opts), nil
}

// Compile-time interface checks
var (
	_ RecordReader = (*JSONReader)(nil)
	_ Record       = (*JSONRecord)(nil)
)
