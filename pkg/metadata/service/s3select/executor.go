// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select/eventstream"
)

const defaultChunkSize = 256 * 1024 // 256KB

// Executor executes S3 Select queries.
type Executor struct {
	query     *Query
	reader    RecordReader
	output    *CSVOutput
	chunkSize int
}

// NewExecutor creates a query executor.
func NewExecutor(query *Query, reader RecordReader, output *CSVOutput) *Executor {
	if output == nil {
		output = &CSVOutput{}
	}
	return &Executor{
		query:     query,
		reader:    reader,
		output:    output,
		chunkSize: defaultChunkSize,
	}
}

// Execute runs the query and writes results to the writer.
func (e *Executor) Execute(ctx context.Context, w io.Writer) error {
	encoder := eventstream.NewEncoder(w)

	var buf bytes.Buffer
	csvWriter := csv.NewWriter(&buf)

	// Configure output delimiter
	if e.output.FieldDelimiter != "" && len(e.output.FieldDelimiter) > 0 {
		csvWriter.Comma = rune(e.output.FieldDelimiter[0])
	}

	var bytesScanned, bytesProcessed, bytesReturned int64
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

		// Apply WHERE filter
		if e.query.Where != nil {
			match, err := e.evaluateBool(e.query.Where, record)
			if err != nil {
				encoder.WriteError("InvalidQuery", err.Error())
				return err
			}
			if !match {
				continue
			}
		}

		// Project columns
		row, err := e.projectRecord(record)
		if err != nil {
			encoder.WriteError("InvalidQuery", err.Error())
			return err
		}

		csvWriter.Write(row)
		recordCount++

		// Check LIMIT
		if e.query.Limit > 0 && recordCount >= e.query.Limit {
			break
		}

		// Flush chunk if needed
		csvWriter.Flush()
		if buf.Len() >= e.chunkSize {
			bytesReturned += int64(buf.Len())
			encoder.WriteRecords(buf.Bytes())
			buf.Reset()
		}
	}

	// Final flush
	csvWriter.Flush()
	if buf.Len() > 0 {
		bytesReturned += int64(buf.Len())
		encoder.WriteRecords(buf.Bytes())
	}

	// Write stats and end
	encoder.WriteStats(bytesScanned, bytesProcessed, bytesReturned)
	encoder.WriteEnd()

	return nil
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
