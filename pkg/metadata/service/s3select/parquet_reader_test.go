// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"bytes"
	"io"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRow represents a row for test parquet files.
type TestRow struct {
	ID   int64  `parquet:"id"`
	Name string `parquet:"name"`
	Age  int32  `parquet:"age"`
}

func TestParquetReader_BasicRead(t *testing.T) {
	// Create test parquet data
	data := createTestParquet(t, []TestRow{
		{ID: 1, Name: "Alice", Age: 30},
		{ID: 2, Name: "Bob", Age: 25},
	})

	// Create reader
	reader, err := NewParquetReader(bytes.NewReader(data), int64(len(data)), nil)
	require.NoError(t, err)
	defer reader.Close()

	// Read first record
	record1, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, []string{"id", "name", "age"}, record1.ColumnNames())
	assert.Equal(t, int64(1), record1.Get("id"))
	assert.Equal(t, "Alice", record1.Get("name"))
	assert.Equal(t, int64(30), record1.Get("age"))
	assert.Equal(t, int64(1), record1.GetByIndex(0))
	assert.Equal(t, "Alice", record1.GetByIndex(1))
	assert.Equal(t, int64(30), record1.GetByIndex(2))

	// Read second record
	record2, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, int64(2), record2.Get("id"))
	assert.Equal(t, "Bob", record2.Get("name"))
	assert.Equal(t, int64(25), record2.Get("age"))

	// Read EOF
	_, err = reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestParquetReader_EmptyFile(t *testing.T) {
	// Create empty parquet file
	data := createTestParquet(t, []TestRow{})

	// Create reader
	reader, err := NewParquetReader(bytes.NewReader(data), int64(len(data)), nil)
	require.NoError(t, err)
	defer reader.Close()

	// Should get EOF immediately
	_, err = reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestParquetReader_LargeDataset(t *testing.T) {
	// Create large dataset (10000 rows)
	const numRows = 10000
	rows := make([]TestRow, numRows)
	for i := 0; i < numRows; i++ {
		rows[i] = TestRow{
			ID:   int64(i + 1),
			Name: "User",
			Age:  int32(20 + (i % 50)),
		}
	}

	data := createTestParquet(t, rows)

	// Create reader
	reader, err := NewParquetReader(bytes.NewReader(data), int64(len(data)), nil)
	require.NoError(t, err)
	defer reader.Close()

	// Count all records
	count := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)

		// Verify sequential IDs
		expectedID := int64(count + 1)
		assert.Equal(t, expectedID, record.Get("id"))
		count++
	}

	assert.Equal(t, numRows, count)
}

func TestParquetReader_InvalidFile(t *testing.T) {
	// Test with invalid data
	invalidData := []byte("not a parquet file")
	_, err := NewParquetReader(bytes.NewReader(invalidData), int64(len(invalidData)), nil)
	assert.Error(t, err)

	var selectErr *SelectError
	assert.ErrorAs(t, err, &selectErr)
	assert.Equal(t, "ParquetParsingError", selectErr.Code)
}

func TestParquetReader_ClosedReader(t *testing.T) {
	// Create test parquet data
	data := createTestParquet(t, []TestRow{
		{ID: 1, Name: "Alice", Age: 30},
	})

	reader, err := NewParquetReader(bytes.NewReader(data), int64(len(data)), nil)
	require.NoError(t, err)

	// Close the reader
	err = reader.Close()
	require.NoError(t, err)

	// Reading from closed reader should return EOF
	_, err = reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestParquetReader_Values(t *testing.T) {
	// Create test parquet data
	data := createTestParquet(t, []TestRow{
		{ID: 1, Name: "Alice", Age: 30},
	})

	reader, err := NewParquetReader(bytes.NewReader(data), int64(len(data)), nil)
	require.NoError(t, err)
	defer reader.Close()

	record, err := reader.Read()
	require.NoError(t, err)

	values := record.Values()
	assert.Len(t, values, 3)
	assert.Equal(t, int64(1), values[0])
	assert.Equal(t, "Alice", values[1])
	assert.Equal(t, int64(30), values[2])
}

func TestParquetReader_NilColumn(t *testing.T) {
	// Create test parquet data
	data := createTestParquet(t, []TestRow{
		{ID: 1, Name: "Alice", Age: 30},
	})

	reader, err := NewParquetReader(bytes.NewReader(data), int64(len(data)), nil)
	require.NoError(t, err)
	defer reader.Close()

	record, err := reader.Read()
	require.NoError(t, err)

	// Non-existent column should return nil
	assert.Nil(t, record.Get("nonexistent"))

	// Out of bounds index should return nil
	assert.Nil(t, record.GetByIndex(-1))
	assert.Nil(t, record.GetByIndex(100))
}

// createTestParquet creates a parquet file in memory with the given rows.
func createTestParquet(t *testing.T, rows []TestRow) []byte {
	t.Helper()

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[TestRow](&buf)

	if len(rows) > 0 {
		_, err := writer.Write(rows)
		require.NoError(t, err)
	}

	err := writer.Close()
	require.NoError(t, err)

	return buf.Bytes()
}
