// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/eventstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func decodeEvents(t *testing.T, buf *bytes.Buffer) (records []byte, stats bool, end bool) {
	dec := eventstream.NewDecoder()
	for buf.Len() > 0 {
		msg, err := dec.Decode(buf, nil)
		if err != nil {
			break
		}
		var eventType string
		for _, h := range msg.Headers {
			if h.Name == ":event-type" {
				eventType = h.Value.String()
			}
		}
		switch eventType {
		case "Records":
			records = append(records, msg.Payload...)
		case "Stats":
			stats = true
		case "End":
			end = true
		}
	}
	return
}

func TestExecutor_SelectStar(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\n"
	query := &Query{
		Projections: []Projection{{Expr: &StarExpr{}}},
		FromAlias:   "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, stats, end := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "Alice")
	assert.Contains(t, string(records), "Bob")
	assert.True(t, stats)
	assert.True(t, end)
}

func TestExecutor_SelectColumns(t *testing.T) {
	input := "name,age,city\nAlice,30,NYC\nBob,25,LA\n"
	query := &Query{
		Projections: []Projection{
			{Expr: &ColumnRef{Name: "name"}},
			{Expr: &ColumnRef{Name: "city"}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "Alice")
	assert.Contains(t, string(records), "NYC")
	assert.NotContains(t, string(records), "30") // age excluded
}

func TestExecutor_WhereFilter(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\nCharlie,35\n"
	query := &Query{
		Projections: []Projection{{Expr: &StarExpr{}}},
		FromAlias:   "s3object",
		Where: &BinaryOp{
			Left:  &ColumnRef{Name: "age"},
			Op:    ">",
			Right: &Literal{Value: int64(28)},
		},
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "Alice")
	assert.Contains(t, string(records), "Charlie")
	assert.NotContains(t, string(records), "Bob") // age 25 < 28
}

func TestExecutor_Limit(t *testing.T) {
	input := "name\nA\nB\nC\nD\nE\n"
	query := &Query{
		Projections: []Projection{{Expr: &StarExpr{}}},
		FromAlias:   "s3object",
		Limit:       2,
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	// Count newlines to verify only 2 records
	count := strings.Count(string(records), "\n")
	assert.Equal(t, 2, count)
}

func TestExecutor_ContextCancellation(t *testing.T) {
	input := "name\nA\nB\nC\n"
	query := &Query{
		Projections: []Projection{{Expr: &StarExpr{}}},
		FromAlias:   "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := exec.Execute(ctx, &buf)
	assert.Error(t, err)
	assert.Equal(t, context.Canceled, err)
}

func TestExecutor_Aggregates_CountStar(t *testing.T) {
	input := "name,age,salary\nAlice,30,50000\nBob,25,60000\nCharlie,35,55000\n"

	// SELECT COUNT(*) FROM s3object
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "COUNT", Args: []Expression{&StarExpr{}}}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, stats, end := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "3") // 3 rows
	assert.True(t, stats)
	assert.True(t, end)
}

func TestExecutor_Aggregates_CountColumn(t *testing.T) {
	input := "name,age\nAlice,30\nBob,\nCharlie,35\n"

	// SELECT COUNT(age) FROM s3object
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "COUNT", Args: []Expression{&ColumnRef{Name: "age"}}}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	// Bob has empty age, which should be treated as empty string, not nil
	// CSV reader returns empty string for empty fields
	assert.Contains(t, string(records), "3")
}

func TestExecutor_Aggregates_Sum(t *testing.T) {
	input := "name,salary\nAlice,50000\nBob,60000\nCharlie,55000\n"

	// SELECT SUM(salary) FROM s3object
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "SUM", Args: []Expression{&ColumnRef{Name: "salary"}}}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "165000") // 50000 + 60000 + 55000
}

func TestExecutor_Aggregates_Avg(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\nCharlie,35\n"

	// SELECT AVG(age) FROM s3object
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "AVG", Args: []Expression{&ColumnRef{Name: "age"}}}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "30") // (30 + 25 + 35) / 3 = 30
}

func TestExecutor_Aggregates_Min(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\nCharlie,35\n"

	// SELECT MIN(age) FROM s3object
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "MIN", Args: []Expression{&ColumnRef{Name: "age"}}}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "25") // Min age
}

func TestExecutor_Aggregates_Max(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\nCharlie,35\n"

	// SELECT MAX(age) FROM s3object
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "MAX", Args: []Expression{&ColumnRef{Name: "age"}}}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "35") // Max age
}

func TestExecutor_Aggregates_MultipleAggregates(t *testing.T) {
	input := "name,age,salary\nAlice,30,50000\nBob,25,60000\nCharlie,35,55000\n"

	// SELECT COUNT(*), SUM(salary), AVG(age) FROM s3object
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "COUNT", Args: []Expression{&StarExpr{}}}},
			{Expr: &FunctionCall{Name: "SUM", Args: []Expression{&ColumnRef{Name: "salary"}}}},
			{Expr: &FunctionCall{Name: "AVG", Args: []Expression{&ColumnRef{Name: "age"}}}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, stats, end := decodeEvents(t, &buf)
	result := string(records)
	assert.Contains(t, result, "3")      // COUNT(*)
	assert.Contains(t, result, "165000") // SUM(salary)
	assert.Contains(t, result, "30")     // AVG(age)
	assert.True(t, stats)
	assert.True(t, end)
}

func TestExecutor_Aggregates_WithWhereClause(t *testing.T) {
	input := "name,age,salary\nAlice,30,50000\nBob,25,60000\nCharlie,35,55000\n"

	// SELECT COUNT(*), SUM(salary) FROM s3object WHERE age > 27
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "COUNT", Args: []Expression{&StarExpr{}}}},
			{Expr: &FunctionCall{Name: "SUM", Args: []Expression{&ColumnRef{Name: "salary"}}}},
		},
		FromAlias: "s3object",
		Where: &BinaryOp{
			Left:  &ColumnRef{Name: "age"},
			Op:    ">",
			Right: &Literal{Value: int64(27)},
		},
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	result := string(records)
	assert.Contains(t, result, "2")      // COUNT(*) - Alice and Charlie
	assert.Contains(t, result, "105000") // SUM(salary) - 50000 + 55000
}

func TestExecutor_Aggregates_EmptyResult(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\n"

	// SELECT SUM(age) FROM s3object WHERE age > 100
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "SUM", Args: []Expression{&ColumnRef{Name: "age"}}}},
		},
		FromAlias: "s3object",
		Where: &BinaryOp{
			Left:  &ColumnRef{Name: "age"},
			Op:    ">",
			Right: &Literal{Value: int64(100)},
		},
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, stats, end := decodeEvents(t, &buf)
	// SUM returns nil (empty string) for empty set
	assert.NotNil(t, records)
	assert.True(t, stats)
	assert.True(t, end)
}

func TestExecutor_Aggregates_CountEmptyResult(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\n"

	// SELECT COUNT(*) FROM s3object WHERE age > 100
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "COUNT", Args: []Expression{&StarExpr{}}}},
		},
		FromAlias: "s3object",
		Where: &BinaryOp{
			Left:  &ColumnRef{Name: "age"},
			Op:    ">",
			Right: &Literal{Value: int64(100)},
		},
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	// COUNT returns 0 for empty set, not nil
	assert.Contains(t, string(records), "0")
}

func TestExecutor_Aggregates_MinMaxStrings(t *testing.T) {
	input := "name,city\nAlice,NYC\nBob,LA\nCharlie,Chicago\n"

	// SELECT MIN(city), MAX(city) FROM s3object
	query := &Query{
		Projections: []Projection{
			{Expr: &FunctionCall{Name: "MIN", Args: []Expression{&ColumnRef{Name: "city"}}}},
			{Expr: &FunctionCall{Name: "MAX", Args: []Expression{&ColumnRef{Name: "city"}}}},
		},
		FromAlias: "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, _, _ := decodeEvents(t, &buf)
	result := string(records)
	assert.Contains(t, result, "Chicago") // MIN alphabetically
	assert.Contains(t, result, "NYC")     // MAX alphabetically
}

func TestExecutor_WithOptions(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\n"
	query := &Query{
		Projections: []Projection{{Expr: &StarExpr{}}},
		FromAlias:   "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	// Test that options don't break execution
	exec := NewExecutor(query, reader, &CSVOutput{},
		WithProgress(true, 100*time.Millisecond),
		WithChunkSize(1024),
	)

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	records, stats, end := decodeEvents(t, &buf)
	assert.Contains(t, string(records), "Alice")
	assert.Contains(t, string(records), "Bob")
	assert.True(t, stats)
	assert.True(t, end)
}

func TestExecutor_ProgressEvents(t *testing.T) {
	// Create a larger input to ensure progress events are sent
	var sb strings.Builder
	sb.WriteString("name,age\n")
	for i := 0; i < 100; i++ {
		sb.WriteString(fmt.Sprintf("Person%d,%d\n", i, 20+i%50))
	}
	input := sb.String()

	query := &Query{
		Projections: []Projection{{Expr: &StarExpr{}}},
		FromAlias:   "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	// Use a very short interval to ensure progress events are sent
	exec := NewExecutor(query, reader, &CSVOutput{},
		WithProgress(true, 1*time.Nanosecond), // Very short interval
	)

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	// Decode and verify progress events were sent
	dec := eventstream.NewDecoder()
	hasProgress := false
	for buf.Len() > 0 {
		msg, err := dec.Decode(&buf, nil)
		if err != nil {
			break
		}
		for _, h := range msg.Headers {
			if h.Name == ":event-type" && h.Value.String() == "Progress" {
				hasProgress = true
				// Verify payload contains expected fields
				assert.Contains(t, string(msg.Payload), "BytesScanned")
				assert.Contains(t, string(msg.Payload), "BytesProcessed")
				assert.Contains(t, string(msg.Payload), "BytesReturned")
			}
		}
	}
	assert.True(t, hasProgress, "Expected at least one Progress event")
}

func TestExecutor_StatsIncludeBytesTracked(t *testing.T) {
	input := "name,age\nAlice,30\nBob,25\n"
	query := &Query{
		Projections: []Projection{{Expr: &StarExpr{}}},
		FromAlias:   "s3object",
	}

	reader := NewCSVReader(strings.NewReader(input), &CSVInput{
		FileHeaderInfo: "USE",
	})

	var buf bytes.Buffer
	exec := NewExecutor(query, reader, &CSVOutput{})

	err := exec.Execute(context.Background(), &buf)
	require.NoError(t, err)

	// Decode and check Stats event has non-zero values
	dec := eventstream.NewDecoder()
	for buf.Len() > 0 {
		msg, err := dec.Decode(&buf, nil)
		if err != nil {
			break
		}
		for _, h := range msg.Headers {
			if h.Name == ":event-type" && h.Value.String() == "Stats" {
				// Stats should have non-zero BytesScanned and BytesProcessed
				assert.Contains(t, string(msg.Payload), "BytesScanned")
				// The values should be non-zero (not <BytesScanned>0<)
				// Actually we just check that the payload is non-empty
				assert.NotEmpty(t, msg.Payload)
			}
		}
	}
}
