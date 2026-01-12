// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package eval

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testRecord for testing
type testRecord struct {
	headers []string
	data    map[string]any
	values  []any
}

func (r *testRecord) Get(name string) any { return r.data[name] }
func (r *testRecord) GetByIndex(idx int) any {
	if idx >= 0 && idx < len(r.values) {
		return r.values[idx]
	}
	return nil
}
func (r *testRecord) ColumnNames() []string { return r.headers }
func (r *testRecord) Values() []any         { return r.values }

func newTestRecord(data map[string]any) *testRecord {
	headers := make([]string, 0, len(data))
	values := make([]any, 0, len(data))
	for k, v := range data {
		headers = append(headers, k)
		values = append(values, v)
	}
	return &testRecord{headers: headers, data: data, values: values}
}

func TestEvaluator_ColumnRef(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"name": "Alice", "age": int64(30)})

	result, err := e.Evaluate(&s3select.ColumnRef{Name: "name"}, record)
	require.NoError(t, err)
	assert.Equal(t, "Alice", result)
}

func TestEvaluator_ColumnRefByIndex(t *testing.T) {
	e := New()
	record := &testRecord{
		headers: []string{"name", "age"},
		data:    map[string]any{"name": "Bob", "age": int64(25)},
		values:  []any{"Bob", int64(25)},
	}

	result, err := e.Evaluate(&s3select.ColumnRef{Index: 1}, record)
	require.NoError(t, err)
	assert.Equal(t, int64(25), result)
}

func TestEvaluator_Literal(t *testing.T) {
	e := New()
	record := newTestRecord(nil)

	tests := []struct {
		name  string
		value any
	}{
		{"int64", int64(42)},
		{"float64", float64(3.14)},
		{"string", "hello"},
		{"bool", true},
		{"nil", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.Evaluate(&s3select.Literal{Value: tt.value}, record)
			require.NoError(t, err)
			assert.Equal(t, tt.value, result)
		})
	}
}

func TestEvaluator_StarExpr(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"a": "1", "b": "2"})

	result, err := e.Evaluate(&s3select.StarExpr{}, record)
	require.NoError(t, err)
	assert.NotNil(t, result)
}

func TestEvaluator_BinaryOp_Equals(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"name": "Alice", "age": int64(30)})

	tests := []struct {
		name     string
		left     s3select.Expression
		op       string
		right    s3select.Expression
		expected bool
	}{
		{
			name:     "string equals - true",
			left:     &s3select.ColumnRef{Name: "name"},
			op:       "=",
			right:    &s3select.Literal{Value: "Alice"},
			expected: true,
		},
		{
			name:     "string equals - false",
			left:     &s3select.ColumnRef{Name: "name"},
			op:       "=",
			right:    &s3select.Literal{Value: "Bob"},
			expected: false,
		},
		{
			name:     "int64 equals",
			left:     &s3select.ColumnRef{Name: "age"},
			op:       "=",
			right:    &s3select.Literal{Value: int64(30)},
			expected: true,
		},
		{
			name:     "not equals",
			left:     &s3select.ColumnRef{Name: "name"},
			op:       "!=",
			right:    &s3select.Literal{Value: "Bob"},
			expected: true,
		},
		{
			name:     "not equals alternate",
			left:     &s3select.ColumnRef{Name: "name"},
			op:       "<>",
			right:    &s3select.Literal{Value: "Bob"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.Evaluate(&s3select.BinaryOp{
				Left:  tt.left,
				Op:    tt.op,
				Right: tt.right,
			}, record)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_BinaryOp_Comparison(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"age": int64(30)})

	tests := []struct {
		name     string
		op       string
		value    any
		expected bool
	}{
		{"less than - true", "<", int64(40), true},
		{"less than - false", "<", int64(20), false},
		{"greater than - true", ">", int64(20), true},
		{"greater than - false", ">", int64(40), false},
		{"less than or equal - true (less)", "<=", int64(40), true},
		{"less than or equal - true (equal)", "<=", int64(30), true},
		{"less than or equal - false", "<=", int64(20), false},
		{"greater than or equal - true (greater)", ">=", int64(20), true},
		{"greater than or equal - true (equal)", ">=", int64(30), true},
		{"greater than or equal - false", ">=", int64(40), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.Evaluate(&s3select.BinaryOp{
				Left:  &s3select.ColumnRef{Name: "age"},
				Op:    tt.op,
				Right: &s3select.Literal{Value: tt.value},
			}, record)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_BinaryOp_Logical(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"a": true, "b": false})

	tests := []struct {
		name     string
		left     s3select.Expression
		op       string
		right    s3select.Expression
		expected bool
	}{
		{
			name:     "AND true true",
			left:     &s3select.Literal{Value: true},
			op:       "AND",
			right:    &s3select.Literal{Value: true},
			expected: true,
		},
		{
			name:     "AND true false",
			left:     &s3select.Literal{Value: true},
			op:       "AND",
			right:    &s3select.Literal{Value: false},
			expected: false,
		},
		{
			name:     "OR true false",
			left:     &s3select.Literal{Value: true},
			op:       "OR",
			right:    &s3select.Literal{Value: false},
			expected: true,
		},
		{
			name:     "OR false false",
			left:     &s3select.Literal{Value: false},
			op:       "OR",
			right:    &s3select.Literal{Value: false},
			expected: false,
		},
		{
			name:     "and lowercase",
			left:     &s3select.Literal{Value: true},
			op:       "and",
			right:    &s3select.Literal{Value: true},
			expected: true,
		},
		{
			name:     "or lowercase",
			left:     &s3select.Literal{Value: true},
			op:       "or",
			right:    &s3select.Literal{Value: false},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.Evaluate(&s3select.BinaryOp{
				Left:  tt.left,
				Op:    tt.op,
				Right: tt.right,
			}, record)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_UnaryOp_NOT(t *testing.T) {
	e := New()
	record := newTestRecord(nil)

	tests := []struct {
		name     string
		value    any
		expected bool
	}{
		{"NOT true", true, false},
		{"NOT false", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.Evaluate(&s3select.UnaryOp{
				Op:   "NOT",
				Expr: &s3select.Literal{Value: tt.value},
			}, record)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_UnaryOp_Negate(t *testing.T) {
	e := New()
	record := newTestRecord(nil)

	result, err := e.Evaluate(&s3select.UnaryOp{
		Op:   "-",
		Expr: &s3select.Literal{Value: float64(42)},
	}, record)
	require.NoError(t, err)
	assert.Equal(t, float64(-42), result)
}

func TestEvaluator_EvaluateBool(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"active": true, "count": int64(5)})

	tests := []struct {
		name     string
		expr     s3select.Expression
		expected bool
	}{
		{
			name:     "bool literal true",
			expr:     &s3select.Literal{Value: true},
			expected: true,
		},
		{
			name:     "bool literal false",
			expr:     &s3select.Literal{Value: false},
			expected: false,
		},
		{
			name:     "nil is false",
			expr:     &s3select.Literal{Value: nil},
			expected: false,
		},
		{
			name:     "non-zero int is true",
			expr:     &s3select.Literal{Value: int64(5)},
			expected: true,
		},
		{
			name:     "zero int is false",
			expr:     &s3select.Literal{Value: int64(0)},
			expected: false,
		},
		{
			name:     "non-empty string is true",
			expr:     &s3select.Literal{Value: "hello"},
			expected: true,
		},
		{
			name:     "empty string is false",
			expr:     &s3select.Literal{Value: ""},
			expected: false,
		},
		{
			name: "comparison expression",
			expr: &s3select.BinaryOp{
				Left:  &s3select.ColumnRef{Name: "count"},
				Op:    ">",
				Right: &s3select.Literal{Value: int64(3)},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.EvaluateBool(tt.expr, record)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_ComplexExpression(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{
		"name":   "Alice",
		"age":    int64(30),
		"active": true,
	})

	// (name = "Alice" AND age > 25)
	expr := &s3select.BinaryOp{
		Left: &s3select.BinaryOp{
			Left:  &s3select.ColumnRef{Name: "name"},
			Op:    "=",
			Right: &s3select.Literal{Value: "Alice"},
		},
		Op: "AND",
		Right: &s3select.BinaryOp{
			Left:  &s3select.ColumnRef{Name: "age"},
			Op:    ">",
			Right: &s3select.Literal{Value: int64(25)},
		},
	}

	result, err := e.EvaluateBool(expr, record)
	require.NoError(t, err)
	assert.True(t, result)
}

func TestEvaluator_UnsupportedExpression(t *testing.T) {
	e := New()
	record := newTestRecord(nil)

	// A type that doesn't implement Expression properly but we pass it in
	// In this case, we pass nil to trigger error handling
	_, err := e.Evaluate(nil, record)
	require.Error(t, err)
}

func TestEvaluator_TypeConversions(t *testing.T) {
	e := New()
	record := newTestRecord(nil)

	// Test float to int comparison
	result, err := e.Evaluate(&s3select.BinaryOp{
		Left:  &s3select.Literal{Value: int64(10)},
		Op:    "=",
		Right: &s3select.Literal{Value: float64(10)},
	}, record)
	require.NoError(t, err)
	assert.True(t, result.(bool))
}
