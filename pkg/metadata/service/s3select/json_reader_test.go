// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// JSON Reader Tests - LINES Mode
// ============================================================================

func TestJSONReader_LinesMode_BasicRecords(t *testing.T) {
	input := `{"name": "Alice", "age": 30}
{"name": "Bob", "age": 25}
{"name": "Charlie", "age": 35}`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "LINES"})
	defer reader.Close()

	// First record
	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Alice", rec.Get("name"))
	assert.Equal(t, float64(30), rec.Get("age"))

	// Second record
	rec, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Bob", rec.Get("name"))
	assert.Equal(t, float64(25), rec.Get("age"))

	// Third record
	rec, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Charlie", rec.Get("name"))
	assert.Equal(t, float64(35), rec.Get("age"))

	// EOF
	_, err = reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestJSONReader_LinesMode_NestedAccess(t *testing.T) {
	input := `{"user": {"name": "Alice", "address": {"city": "NYC", "zip": "10001"}}}
{"user": {"name": "Bob", "address": {"city": "LA", "zip": "90001"}}}`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "LINES"})
	defer reader.Close()

	// First record - nested access
	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Alice", rec.Get("user.name"))
	assert.Equal(t, "NYC", rec.Get("user.address.city"))
	assert.Equal(t, "10001", rec.Get("user.address.zip"))

	// Second record
	rec, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Bob", rec.Get("user.name"))
	assert.Equal(t, "LA", rec.Get("user.address.city"))
}

func TestJSONReader_LinesMode_ArrayAccess(t *testing.T) {
	input := `{"id": 1, "items": [{"sku": "A1", "qty": 2}, {"sku": "B2", "qty": 3}]}
{"id": 2, "items": [{"sku": "C3", "qty": 1}]}`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "LINES"})
	defer reader.Close()

	// First record
	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, float64(1), rec.Get("id"))
	assert.Equal(t, "A1", rec.Get("items[0].sku"))
	assert.Equal(t, float64(2), rec.Get("items[0].qty"))
	assert.Equal(t, "B2", rec.Get("items[1].sku"))

	// Second record
	rec, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "C3", rec.Get("items[0].sku"))
}

func TestJSONReader_LinesMode_EmptyInput(t *testing.T) {
	reader := NewJSONReader(strings.NewReader(""), &JSONInput{Type: "LINES"})
	defer reader.Close()

	_, err := reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestJSONReader_LinesMode_DefaultInput(t *testing.T) {
	input := `{"name": "Test"}`

	// nil input should default to LINES mode
	reader := NewJSONReader(strings.NewReader(input), nil)
	defer reader.Close()

	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Test", rec.Get("name"))
}

// ============================================================================
// JSON Reader Tests - DOCUMENT Mode
// ============================================================================

func TestJSONReader_DocumentMode_Array(t *testing.T) {
	input := `[
		{"name": "Alice", "age": 30},
		{"name": "Bob", "age": 25},
		{"name": "Charlie", "age": 35}
	]`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "DOCUMENT"})
	defer reader.Close()

	// First record
	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Alice", rec.Get("name"))
	assert.Equal(t, float64(30), rec.Get("age"))

	// Second record
	rec, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Bob", rec.Get("name"))

	// Third record
	rec, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Charlie", rec.Get("name"))

	// EOF
	_, err = reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestJSONReader_DocumentMode_SingleObject(t *testing.T) {
	input := `{"name": "Alice", "age": 30, "city": "NYC"}`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "DOCUMENT"})
	defer reader.Close()

	// Single record
	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Alice", rec.Get("name"))
	assert.Equal(t, float64(30), rec.Get("age"))
	assert.Equal(t, "NYC", rec.Get("city"))

	// EOF after single object
	_, err = reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestJSONReader_DocumentMode_EmptyArray(t *testing.T) {
	input := `[]`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "DOCUMENT"})
	defer reader.Close()

	// EOF immediately
	_, err := reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestJSONReader_DocumentMode_NestedAccess(t *testing.T) {
	input := `[
		{"user": {"name": "Alice", "scores": [95, 87, 92]}},
		{"user": {"name": "Bob", "scores": [88, 91, 85]}}
	]`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "DOCUMENT"})
	defer reader.Close()

	// First record with nested access
	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Alice", rec.Get("user.name"))
	assert.Equal(t, float64(95), rec.Get("user.scores[0]"))
	assert.Equal(t, float64(92), rec.Get("user.scores[2]"))

	// Second record
	rec, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Bob", rec.Get("user.name"))
	assert.Equal(t, float64(88), rec.Get("user.scores[0]"))
}

// ============================================================================
// JSON Record Tests
// ============================================================================

func TestJSONRecord_ColumnNames(t *testing.T) {
	data := map[string]any{
		"name": "Alice",
		"age":  30,
		"city": "NYC",
	}
	rec := NewJSONRecord(data)

	names := rec.ColumnNames()
	assert.Len(t, names, 3)
	// Should be sorted
	assert.Equal(t, []string{"age", "city", "name"}, names)
}

func TestJSONRecord_Values(t *testing.T) {
	data := map[string]any{
		"name": "Alice",
		"age":  30,
	}
	rec := NewJSONRecord(data)

	values := rec.Values()
	assert.Len(t, values, 2)
	// Values should match sorted column order
	assert.Equal(t, 30, values[0])      // age
	assert.Equal(t, "Alice", values[1]) // name
}

func TestJSONRecord_GetByIndex(t *testing.T) {
	data := map[string]any{
		"name": "Alice",
		"age":  30,
	}
	rec := NewJSONRecord(data)

	// Index based on sorted column names: age=0, name=1
	assert.Equal(t, 30, rec.GetByIndex(0))      // age
	assert.Equal(t, "Alice", rec.GetByIndex(1)) // name
	assert.Nil(t, rec.GetByIndex(2))            // out of bounds
	assert.Nil(t, rec.GetByIndex(-1))           // negative
}

func TestJSONRecord_MissingKey(t *testing.T) {
	data := map[string]any{
		"name": "Alice",
	}
	rec := NewJSONRecord(data)

	assert.Nil(t, rec.Get("nonexistent"))
	assert.Nil(t, rec.Get("name.nested")) // trying to access nested on string
}

func TestJSONRecord_Data(t *testing.T) {
	data := map[string]any{
		"name": "Alice",
		"age":  30,
	}
	rec := NewJSONRecord(data)

	assert.Equal(t, data, rec.Data())
}

// ============================================================================
// Edge Cases
// ============================================================================

func TestJSONReader_ClosedReader(t *testing.T) {
	input := `{"name": "Alice"}`
	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "LINES"})

	// Close immediately
	require.NoError(t, reader.Close())

	// Reads should return EOF
	_, err := reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestJSONReader_MalformedJSON(t *testing.T) {
	input := `{"name": "Alice", invalid json`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "LINES"})
	defer reader.Close()

	_, err := reader.Read()
	assert.Error(t, err)
}

func TestJSONReader_DocumentMode_InvalidDocument(t *testing.T) {
	input := `"just a string"`

	reader := NewJSONReader(strings.NewReader(input), &JSONInput{Type: "DOCUMENT"})
	defer reader.Close()

	_, err := reader.Read()
	assert.Error(t, err)
	selectErr, ok := err.(*SelectError)
	require.True(t, ok)
	assert.Equal(t, "JSONParsingError", selectErr.Code)
}
