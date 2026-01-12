// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package eval

import (
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select"
)

// Evaluator evaluates S3 Select expressions.
type Evaluator struct{}

// New creates a new evaluator.
func New() *Evaluator {
	return &Evaluator{}
}

// Evaluate evaluates an expression against a record.
func (e *Evaluator) Evaluate(expr s3select.Expression, record s3select.Record) (any, error) {
	if expr == nil {
		return nil, &s3select.SelectError{
			Code:    "InvalidExpression",
			Message: "expression is nil",
		}
	}

	switch ex := expr.(type) {
	case *s3select.ColumnRef:
		if ex.Name != "" {
			return record.Get(ex.Name), nil
		}
		return record.GetByIndex(ex.Index), nil

	case *s3select.Literal:
		return ex.Value, nil

	case *s3select.StarExpr:
		return record.Values(), nil

	case *s3select.BinaryOp:
		return e.evalBinaryOp(ex, record)

	case *s3select.UnaryOp:
		return e.evalUnaryOp(ex, record)

	default:
		return nil, &s3select.SelectError{
			Code:    "InvalidExpression",
			Message: fmt.Sprintf("unsupported expression type: %T", expr),
		}
	}
}

// EvaluateBool evaluates an expression as a boolean.
func (e *Evaluator) EvaluateBool(expr s3select.Expression, record s3select.Record) (bool, error) {
	result, err := e.Evaluate(expr, record)
	if err != nil {
		return false, err
	}

	switch v := result.(type) {
	case bool:
		return v, nil
	case nil:
		return false, nil
	default:
		return e.toBool(result), nil
	}
}

func (e *Evaluator) evalBinaryOp(op *s3select.BinaryOp, record s3select.Record) (any, error) {
	left, err := e.Evaluate(op.Left, record)
	if err != nil {
		return nil, err
	}
	right, err := e.Evaluate(op.Right, record)
	if err != nil {
		return nil, err
	}

	return e.applyBinaryOp(op.Op, left, right)
}

func (e *Evaluator) evalUnaryOp(op *s3select.UnaryOp, record s3select.Record) (any, error) {
	val, err := e.Evaluate(op.Expr, record)
	if err != nil {
		return nil, err
	}

	switch op.Op {
	case "NOT", "not":
		return !e.toBool(val), nil
	case "-":
		return -e.toFloat64(val), nil
	default:
		return nil, fmt.Errorf("unsupported unary operator: %s", op.Op)
	}
}

func (e *Evaluator) applyBinaryOp(op string, left, right any) (any, error) {
	switch op {
	case "=", "==":
		return e.equals(left, right), nil
	case "!=", "<>":
		return !e.equals(left, right), nil
	case "<":
		return e.compare(left, right) < 0, nil
	case ">":
		return e.compare(left, right) > 0, nil
	case "<=":
		return e.compare(left, right) <= 0, nil
	case ">=":
		return e.compare(left, right) >= 0, nil
	case "AND", "and":
		return e.toBool(left) && e.toBool(right), nil
	case "OR", "or":
		return e.toBool(left) || e.toBool(right), nil
	default:
		return nil, fmt.Errorf("unsupported operator: %s", op)
	}
}

func (e *Evaluator) equals(left, right any) bool {
	// String comparison
	ls, lok := left.(string)
	rs, rok := right.(string)
	if lok && rok {
		return ls == rs
	}

	// Numeric comparison
	return e.toFloat64(left) == e.toFloat64(right)
}

func (e *Evaluator) compare(left, right any) int {
	l := e.toFloat64(left)
	r := e.toFloat64(right)

	if l < r {
		return -1
	} else if l > r {
		return 1
	}
	return 0
}

func (e *Evaluator) toFloat64(v any) float64 {
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

func (e *Evaluator) toBool(v any) bool {
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
