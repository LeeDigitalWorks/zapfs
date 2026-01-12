// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"io"

	"github.com/parquet-go/parquet-go"
)

// ParquetReader reads records from a Parquet file.
// Parquet is a columnar format that requires random access (io.ReaderAt)
// because metadata is stored at the end of the file.
type ParquetReader struct {
	file      *parquet.File
	rowGroups []parquet.RowGroup
	columns   []string

	// Current row group state
	currentRowGroup int
	currentRows     parquet.Rows // current row iterator
	rowBuffer       []parquet.Row
	bufferPos       int

	closed bool
}

// NewParquetReader creates a new Parquet record reader.
// Parquet requires io.ReaderAt because it needs random access to read
// the footer metadata at the end of the file.
func NewParquetReader(r io.ReaderAt, size int64, opts *ParquetInput) (*ParquetReader, error) {
	// Mark opts as used - Parquet is self-describing so options are minimal
	_ = opts

	file, err := parquet.OpenFile(r, size)
	if err != nil {
		return nil, &SelectError{
			Code:    "ParquetParsingError",
			Message: "failed to open parquet file: " + err.Error(),
		}
	}

	// Extract column names from schema
	schema := file.Schema()
	fields := schema.Fields()
	columns := make([]string, 0, len(fields))
	for _, field := range fields {
		columns = append(columns, field.Name())
	}

	return &ParquetReader{
		file:      file,
		rowGroups: file.RowGroups(),
		columns:   columns,
	}, nil
}

// Read returns the next record from the Parquet file.
// Returns io.EOF when no more records are available.
func (r *ParquetReader) Read() (Record, error) {
	if r.closed {
		return nil, io.EOF
	}

	// Try to get next row from buffer
	for {
		// If we have buffered rows, return the next one
		if r.bufferPos < len(r.rowBuffer) {
			row := r.rowBuffer[r.bufferPos]
			r.bufferPos++
			return r.rowToRecord(row), nil
		}

		// Try to read more rows from current row iterator
		if r.currentRows != nil {
			const batchSize = 1000
			r.rowBuffer = make([]parquet.Row, batchSize)
			n, err := r.currentRows.ReadRows(r.rowBuffer)
			if err != nil && err != io.EOF {
				return nil, &SelectError{
					Code:    "ParquetParsingError",
					Message: "failed to read parquet rows: " + err.Error(),
				}
			}

			r.rowBuffer = r.rowBuffer[:n]
			r.bufferPos = 0

			// If we got rows, return the first one
			if n > 0 {
				row := r.rowBuffer[r.bufferPos]
				r.bufferPos++
				return r.rowToRecord(row), nil
			}

			// No more rows in this iterator, close it and move to next row group
			r.currentRows.Close()
			r.currentRows = nil
		}

		// Need to open next row group
		if r.currentRowGroup >= len(r.rowGroups) {
			return nil, io.EOF
		}

		// Open rows iterator for next row group
		rowGroup := r.rowGroups[r.currentRowGroup]
		r.currentRows = rowGroup.Rows()
		r.currentRowGroup++
	}
}

// rowToRecord converts a parquet.Row to a Record.
func (r *ParquetReader) rowToRecord(row parquet.Row) Record {
	values := make([]any, len(r.columns))

	// Group values by column index
	for _, value := range row {
		colIdx := value.Column()
		if colIdx >= 0 && colIdx < len(values) {
			values[colIdx] = parquetValueToGo(value)
		}
	}

	return NewParquetRecord(r.columns, values)
}

// parquetValueToGo converts a parquet.Value to a Go native type.
func parquetValueToGo(v parquet.Value) any {
	if v.IsNull() {
		return nil
	}

	switch v.Kind() {
	case parquet.Boolean:
		return v.Boolean()
	case parquet.Int32:
		return int64(v.Int32())
	case parquet.Int64:
		return v.Int64()
	case parquet.Int96:
		// Int96 is typically used for timestamps, convert to int64
		return v.Int96().Int64()
	case parquet.Float:
		return float64(v.Float())
	case parquet.Double:
		return v.Double()
	case parquet.ByteArray, parquet.FixedLenByteArray:
		// Return as string for easier SQL handling
		return string(v.ByteArray())
	default:
		return v.String()
	}
}

// Close closes the Parquet reader.
func (r *ParquetReader) Close() error {
	r.closed = true
	if r.currentRows != nil {
		r.currentRows.Close()
		r.currentRows = nil
	}
	return nil
}

// Compile-time check that ParquetReader implements RecordReader.
var _ RecordReader = (*ParquetReader)(nil)
