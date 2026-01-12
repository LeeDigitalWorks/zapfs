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

func TestCSVReader_WithHeaders(t *testing.T) {
	input := "name,age,city\nAlice,30,NYC\nBob,25,LA\n"
	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})
	defer reader.Close()

	// First record
	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Alice", rec.Get("name"))
	assert.Equal(t, "30", rec.Get("age"))
	assert.Equal(t, "NYC", rec.Get("city"))

	// Second record
	rec, err = reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Bob", rec.Get("name"))
	assert.Equal(t, "25", rec.Get("age"))

	// EOF
	_, err = reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestCSVReader_WithoutHeaders(t *testing.T) {
	input := "Alice,30,NYC\nBob,25,LA\n"
	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "NONE",
	})
	defer reader.Close()

	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Alice", rec.GetByIndex(0))
	assert.Equal(t, "30", rec.GetByIndex(1))
	assert.Equal(t, "NYC", rec.GetByIndex(2))

	// Column names should be _1, _2, _3
	names := rec.ColumnNames()
	assert.Equal(t, "_1", names[0])
	assert.Equal(t, "_2", names[1])
	assert.Equal(t, "_3", names[2])
}

func TestCSVReader_IgnoreHeaders(t *testing.T) {
	input := "name,age,city\nAlice,30,NYC\n"
	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "IGNORE",
	})
	defer reader.Close()

	rec, err := reader.Read()
	require.NoError(t, err)
	// Headers ignored, first data row returned
	assert.Equal(t, "Alice", rec.GetByIndex(0))
	// Can't access by name since headers ignored
	assert.Nil(t, rec.Get("name"))
}

func TestCSVReader_CustomDelimiter(t *testing.T) {
	input := "name|age|city\nAlice|30|NYC\n"
	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
		FieldDelimiter: "|",
	})
	defer reader.Close()

	rec, err := reader.Read()
	require.NoError(t, err)
	assert.Equal(t, "Alice", rec.Get("name"))
	assert.Equal(t, "30", rec.Get("age"))
}

func TestCSVReader_Values(t *testing.T) {
	input := "a,b,c\n1,2,3\n"
	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})
	defer reader.Close()

	rec, err := reader.Read()
	require.NoError(t, err)

	vals := rec.Values()
	assert.Len(t, vals, 3)
	assert.Equal(t, "1", vals[0])
	assert.Equal(t, "2", vals[1])
	assert.Equal(t, "3", vals[2])
}

func TestCSVReader_EmptyInput(t *testing.T) {
	input := ""
	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "NONE",
	})
	defer reader.Close()

	_, err := reader.Read()
	assert.Equal(t, io.EOF, err)
}

func TestCSVReader_HeadersOnly(t *testing.T) {
	input := "name,age,city\n"
	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})
	defer reader.Close()

	_, err := reader.Read()
	assert.Equal(t, io.EOF, err)
}
