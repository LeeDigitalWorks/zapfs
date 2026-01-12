// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package eval

import (
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/s3select"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Function Registry Tests
// ============================================================================

func TestFuncRegistry_Get(t *testing.T) {
	r := NewFuncRegistry()

	tests := []struct {
		name     string
		funcName string
		exists   bool
	}{
		{"LOWER exists", "LOWER", true},
		{"lower case lookup", "lower", true},
		{"UPPER exists", "UPPER", true},
		{"Mixed case lookup", "Lower", true},
		{"Unknown function", "UNKNOWN_FUNC", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := r.Get(tt.funcName)
			if tt.exists {
				assert.NotNil(t, fn, "function %s should exist", tt.funcName)
			} else {
				assert.Nil(t, fn, "function %s should not exist", tt.funcName)
			}
		})
	}
}

// ============================================================================
// String Function Tests
// ============================================================================

func TestFuncLower(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		{"simple lowercase", []any{"HELLO"}, "hello", false},
		{"mixed case", []any{"HeLLo WoRLd"}, "hello world", false},
		{"already lowercase", []any{"hello"}, "hello", false},
		{"empty string", []any{""}, "", false},
		{"nil input", []any{nil}, nil, false},
		{"unicode", []any{"HELLO"}, "hello", false},
		{"wrong arg count", []any{}, nil, true},
		{"too many args", []any{"a", "b"}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcLower(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncUpper(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		{"simple uppercase", []any{"hello"}, "HELLO", false},
		{"mixed case", []any{"HeLLo WoRLd"}, "HELLO WORLD", false},
		{"already uppercase", []any{"HELLO"}, "HELLO", false},
		{"empty string", []any{""}, "", false},
		{"nil input", []any{nil}, nil, false},
		{"wrong arg count", []any{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcUpper(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncCharLength(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		{"simple string", []any{"hello"}, int64(5), false},
		{"empty string", []any{""}, int64(0), false},
		{"unicode chars", []any{"hello"}, int64(5), false},
		{"emoji", []any{"hi!"}, int64(3), false},
		{"nil input", []any{nil}, nil, false},
		{"wrong arg count", []any{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcCharLength(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncTrim(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		{"leading spaces", []any{"  hello"}, "hello", false},
		{"trailing spaces", []any{"hello  "}, "hello", false},
		{"both sides", []any{"  hello  "}, "hello", false},
		{"tabs and newlines", []any{"\t\nhello\t\n"}, "hello", false},
		{"no whitespace", []any{"hello"}, "hello", false},
		{"empty string", []any{""}, "", false},
		{"only whitespace", []any{"   "}, "", false},
		{"nil input", []any{nil}, nil, false},
		{"wrong arg count", []any{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcTrim(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncSubstring(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		{"from start", []any{"hello", int64(1)}, "hello", false},
		{"from middle", []any{"hello", int64(2)}, "ello", false},
		{"with length", []any{"hello", int64(2), int64(3)}, "ell", false},
		{"length exceeds", []any{"hello", int64(2), int64(100)}, "ello", false},
		{"start beyond end", []any{"hello", int64(10)}, "", false},
		{"start at 1", []any{"hello", int64(1), int64(1)}, "h", false},
		{"negative start", []any{"hello", int64(-1), int64(3)}, "hel", false},
		{"zero length", []any{"hello", int64(2), int64(0)}, "", false},
		{"nil input", []any{nil, int64(1)}, nil, false},
		{"wrong arg count", []any{"hello"}, nil, true},
		{"too many args", []any{"a", int64(1), int64(1), int64(1)}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcSubstring(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// ============================================================================
// CAST Function Tests
// ============================================================================

func TestFuncCast(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		// INT conversions
		{"string to int", []any{"42", "INT"}, int64(42), false},
		{"float to int", []any{float64(42.9), "INT"}, int64(42), false},
		{"int to int", []any{int64(42), "INTEGER"}, int64(42), false},
		{"bool true to int", []any{true, "INT"}, int64(1), false},
		{"bool false to int", []any{false, "INT"}, int64(0), false},

		// FLOAT conversions
		{"string to float", []any{"3.14", "FLOAT"}, float64(3.14), false},
		{"int to float", []any{int64(42), "DOUBLE"}, float64(42), false},
		{"float to float", []any{float64(3.14), "DECIMAL"}, float64(3.14), false},

		// STRING conversions
		{"int to string", []any{int64(42), "STRING"}, "42", false},
		{"float to string", []any{float64(3.14), "VARCHAR"}, "3.14", false},
		{"bool to string", []any{true, "STRING"}, "true", false},

		// BOOL conversions
		{"string true to bool", []any{"true", "BOOL"}, true, false},
		{"string false to bool", []any{"false", "BOOLEAN"}, false, false},
		{"int 1 to bool", []any{int64(1), "BOOL"}, true, false},
		{"int 0 to bool", []any{int64(0), "BOOL"}, false, false},

		// TIMESTAMP conversions
		{"string to timestamp", []any{"2024-01-15T10:30:00Z", "TIMESTAMP"}, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), false},
		{"unix timestamp", []any{int64(1705315800), "TIMESTAMP"}, time.Unix(1705315800, 0).UTC(), false},

		// Null handling
		{"nil input", []any{nil, "INT"}, nil, false},

		// Errors
		{"wrong arg count", []any{"42"}, nil, true},
		{"unknown type", []any{"42", "UNKNOWN"}, nil, true},
		{"invalid string to int", []any{"not a number", "INT"}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcCast(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// ============================================================================
// Null Handling Function Tests
// ============================================================================

func TestFuncCoalesce(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		{"first non-null", []any{nil, nil, "hello", "world"}, "hello", false},
		{"first is non-null", []any{"hello", nil, "world"}, "hello", false},
		{"all nil", []any{nil, nil, nil}, nil, false},
		{"single non-null", []any{"hello"}, "hello", false},
		{"single nil", []any{nil}, nil, false},
		{"mixed types", []any{nil, int64(42), "hello"}, int64(42), false},
		{"no args", []any{}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcCoalesce(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncNullif(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		{"equal strings returns nil", []any{"hello", "hello"}, nil, false},
		{"different strings returns first", []any{"hello", "world"}, "hello", false},
		{"equal ints returns nil", []any{int64(42), int64(42)}, nil, false},
		{"different ints returns first", []any{int64(42), int64(43)}, int64(42), false},
		{"nil and nil", []any{nil, nil}, nil, false},
		{"value and nil", []any{"hello", nil}, "hello", false},
		{"nil and value", []any{nil, "hello"}, nil, false},
		{"wrong arg count", []any{"hello"}, nil, true},
		{"too many args", []any{"a", "b", "c"}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcNullif(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

// ============================================================================
// Date/Time Function Tests
// ============================================================================

func TestFuncUtcNow(t *testing.T) {
	before := time.Now().UTC()
	result, err := funcUtcNow()
	after := time.Now().UTC()

	require.NoError(t, err)
	ts, ok := result.(time.Time)
	require.True(t, ok, "result should be time.Time")
	assert.True(t, !ts.Before(before) && !ts.After(after), "result should be between before and after")
}

func TestFuncExtract(t *testing.T) {
	testTime := time.Date(2024, 3, 15, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		args     []any
		expected any
		wantErr  bool
	}{
		{"extract year", []any{"YEAR", testTime}, int64(2024), false},
		{"extract month", []any{"MONTH", testTime}, int64(3), false},
		{"extract day", []any{"DAY", testTime}, int64(15), false},
		{"extract hour", []any{"HOUR", testTime}, int64(14), false},
		{"extract minute", []any{"MINUTE", testTime}, int64(30), false},
		{"extract second", []any{"SECOND", testTime}, int64(45), false},
		{"lowercase part", []any{"year", testTime}, int64(2024), false},
		{"nil timestamp", []any{"YEAR", nil}, nil, false},
		{"invalid part", []any{"INVALID", testTime}, nil, true},
		{"wrong arg count", []any{"YEAR"}, nil, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcExtract(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncDateAdd(t *testing.T) {
	testTime := time.Date(2024, 3, 15, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		args     []any
		expected time.Time
		wantErr  bool
	}{
		{"add years", []any{"YEAR", int64(1), testTime}, time.Date(2025, 3, 15, 14, 30, 45, 0, time.UTC), false},
		{"subtract years", []any{"YEAR", int64(-1), testTime}, time.Date(2023, 3, 15, 14, 30, 45, 0, time.UTC), false},
		{"add months", []any{"MONTH", int64(2), testTime}, time.Date(2024, 5, 15, 14, 30, 45, 0, time.UTC), false},
		{"add days", []any{"DAY", int64(10), testTime}, time.Date(2024, 3, 25, 14, 30, 45, 0, time.UTC), false},
		{"add hours", []any{"HOUR", int64(5), testTime}, time.Date(2024, 3, 15, 19, 30, 45, 0, time.UTC), false},
		{"add minutes", []any{"MINUTE", int64(15), testTime}, time.Date(2024, 3, 15, 14, 45, 45, 0, time.UTC), false},
		{"add seconds", []any{"SECOND", int64(30), testTime}, time.Date(2024, 3, 15, 14, 31, 15, 0, time.UTC), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcDateAdd(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncDateAdd_Errors(t *testing.T) {
	testTime := time.Date(2024, 3, 15, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		name string
		args []any
	}{
		{"nil timestamp", []any{"YEAR", int64(1), nil}},
		{"invalid part", []any{"INVALID", int64(1), testTime}},
		{"wrong arg count", []any{"YEAR", int64(1)}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcDateAdd(tt.args...)
			if tt.name == "nil timestamp" {
				// nil timestamp returns nil without error
				require.NoError(t, err)
				assert.Nil(t, result)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestFuncDateDiff(t *testing.T) {
	ts1 := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
	ts2 := time.Date(2024, 3, 20, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		args     []any
		expected int64
		wantErr  bool
	}{
		{"diff years", []any{"YEAR", ts1, ts2}, int64(0), false},
		{"diff months", []any{"MONTH", ts1, ts2}, int64(2), false},
		{"diff days", []any{"DAY", ts1, ts2}, int64(65), false},
		{"diff hours", []any{"HOUR", ts1, ts2}, int64(1564), false},
		{"diff minutes", []any{"MINUTE", ts1, ts2}, int64(93870), false},
		{"diff seconds", []any{"SECOND", ts1, ts2}, int64(5632245), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcDateDiff(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncDateDiff_NilInputs(t *testing.T) {
	ts := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		args []any
	}{
		{"nil first timestamp", []any{"YEAR", nil, ts}},
		{"nil second timestamp", []any{"YEAR", ts, nil}},
		{"both nil", []any{"YEAR", nil, nil}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcDateDiff(tt.args...)
			require.NoError(t, err)
			assert.Nil(t, result)
		})
	}
}

func TestFuncToTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		args     []any
		expected time.Time
		wantErr  bool
	}{
		{"RFC3339 format", []any{"2024-01-15T10:30:00Z"}, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), false},
		{"date only", []any{"2024-01-15"}, time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), false},
		{"datetime no TZ", []any{"2024-01-15 10:30:00"}, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), false},
		{"datetime T no TZ", []any{"2024-01-15T10:30:00"}, time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), false},
		{"unix timestamp", []any{int64(1705315800)}, time.Unix(1705315800, 0).UTC(), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcToTimestamp(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestFuncToTimestamp_Errors(t *testing.T) {
	tests := []struct {
		name string
		args []any
	}{
		{"nil input", []any{nil}},
		{"invalid format", []any{"not a date"}},
		{"wrong arg count", []any{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcToTimestamp(tt.args...)
			if tt.name == "nil input" {
				require.NoError(t, err)
				assert.Nil(t, result)
			} else {
				assert.Error(t, err)
			}
		})
	}
}

func TestFuncToString(t *testing.T) {
	testTime := time.Date(2024, 3, 15, 14, 30, 45, 0, time.UTC)

	tests := []struct {
		name     string
		args     []any
		expected string
		wantErr  bool
	}{
		{"default format", []any{testTime}, "2024-03-15T14:30:45Z", false},
		{"custom format", []any{testTime, "YYYY-MM-DD"}, "2024-03-15", false},
		{"nil timestamp", []any{nil}, "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := funcToString(tt.args...)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				if tt.name == "nil timestamp" {
					assert.Nil(t, result)
				} else {
					assert.Equal(t, tt.expected, result)
				}
			}
		})
	}
}

// ============================================================================
// Evaluator Integration Tests
// ============================================================================

func TestEvaluator_FunctionCall_StringFunctions(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"name": "  HELLO WORLD  "})

	tests := []struct {
		name     string
		funcName string
		args     []s3select.Expression
		expected any
	}{
		{
			name:     "LOWER function",
			funcName: "LOWER",
			args:     []s3select.Expression{&s3select.Literal{Value: "HELLO"}},
			expected: "hello",
		},
		{
			name:     "UPPER function",
			funcName: "UPPER",
			args:     []s3select.Expression{&s3select.Literal{Value: "hello"}},
			expected: "HELLO",
		},
		{
			name:     "TRIM function with column",
			funcName: "TRIM",
			args:     []s3select.Expression{&s3select.ColumnRef{Name: "name"}},
			expected: "HELLO WORLD",
		},
		{
			name:     "CHAR_LENGTH function",
			funcName: "CHAR_LENGTH",
			args:     []s3select.Expression{&s3select.Literal{Value: "hello"}},
			expected: int64(5),
		},
		{
			name:     "SUBSTRING function",
			funcName: "SUBSTRING",
			args: []s3select.Expression{
				&s3select.Literal{Value: "hello"},
				&s3select.Literal{Value: int64(2)},
				&s3select.Literal{Value: int64(3)},
			},
			expected: "ell",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := e.Evaluate(&s3select.FunctionCall{
				Name: tt.funcName,
				Args: tt.args,
			}, record)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEvaluator_FunctionCall_CAST(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"value": "42"})

	result, err := e.Evaluate(&s3select.FunctionCall{
		Name: "CAST",
		Args: []s3select.Expression{
			&s3select.ColumnRef{Name: "value"},
			&s3select.Literal{Value: "INT"},
		},
	}, record)

	require.NoError(t, err)
	assert.Equal(t, int64(42), result)
}

func TestEvaluator_FunctionCall_COALESCE(t *testing.T) {
	e := New()
	record := newTestRecord(map[string]any{"a": nil, "b": "hello"})

	result, err := e.Evaluate(&s3select.FunctionCall{
		Name: "COALESCE",
		Args: []s3select.Expression{
			&s3select.ColumnRef{Name: "a"},
			&s3select.ColumnRef{Name: "b"},
			&s3select.Literal{Value: "default"},
		},
	}, record)

	require.NoError(t, err)
	assert.Equal(t, "hello", result)
}

func TestEvaluator_FunctionCall_Nested(t *testing.T) {
	e := New()
	record := newTestRecord(nil)

	// UPPER(TRIM("  hello  "))
	result, err := e.Evaluate(&s3select.FunctionCall{
		Name: "UPPER",
		Args: []s3select.Expression{
			&s3select.FunctionCall{
				Name: "TRIM",
				Args: []s3select.Expression{
					&s3select.Literal{Value: "  hello  "},
				},
			},
		},
	}, record)

	require.NoError(t, err)
	assert.Equal(t, "HELLO", result)
}

func TestEvaluator_FunctionCall_UnknownFunction(t *testing.T) {
	e := New()
	record := newTestRecord(nil)

	_, err := e.Evaluate(&s3select.FunctionCall{
		Name: "UNKNOWN_FUNC",
		Args: []s3select.Expression{},
	}, record)

	require.Error(t, err)
	selectErr, ok := err.(*s3select.SelectError)
	require.True(t, ok)
	assert.Equal(t, "InvalidFunction", selectErr.Code)
}

func TestEvaluator_FunctionCall_FunctionError(t *testing.T) {
	e := New()
	record := newTestRecord(nil)

	// LOWER with no arguments should fail
	_, err := e.Evaluate(&s3select.FunctionCall{
		Name: "LOWER",
		Args: []s3select.Expression{},
	}, record)

	require.Error(t, err)
	selectErr, ok := err.(*s3select.SelectError)
	require.True(t, ok)
	assert.Equal(t, "FunctionError", selectErr.Code)
}

// ============================================================================
// Helper Type Conversion Tests
// ============================================================================

func TestToString(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected string
	}{
		{"string", "hello", "hello"},
		{"bytes", []byte("hello"), "hello"},
		{"int", 42, "42"},
		{"int64", int64(42), "42"},
		{"float64", float64(3.14), "3.14"},
		{"bool true", true, "true"},
		{"bool false", false, "false"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toString(tt.input)
			require.NoError(t, err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestToInt64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected int64
		wantErr  bool
	}{
		{"int", 42, int64(42), false},
		{"int64", int64(42), int64(42), false},
		{"int32", int32(42), int64(42), false},
		{"float64", float64(42.9), int64(42), false},
		{"string int", "42", int64(42), false},
		{"string float", "42.5", int64(42), false},
		{"bool true", true, int64(1), false},
		{"bool false", false, int64(0), false},
		{"invalid string", "not a number", int64(0), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toInt64(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected float64
		wantErr  bool
	}{
		{"float64", float64(3.14), float64(3.14), false},
		{"float32", float32(3.14), float64(float32(3.14)), false},
		{"int", 42, float64(42), false},
		{"int64", int64(42), float64(42), false},
		{"string", "3.14", float64(3.14), false},
		{"bool true", true, float64(1), false},
		{"bool false", false, float64(0), false},
		{"invalid string", "not a number", float64(0), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toFloat64(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestToBool(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected bool
		wantErr  bool
	}{
		{"bool true", true, true, false},
		{"bool false", false, false, false},
		{"int 1", 1, true, false},
		{"int 0", 0, false, false},
		{"int64 1", int64(1), true, false},
		{"float64 1", float64(1), true, false},
		{"float64 0", float64(0), false, false},
		{"string true", "true", true, false},
		{"string false", "false", false, false},
		{"string yes", "yes", true, false},
		{"string no", "no", false, false},
		{"string 1", "1", true, false},
		{"string 0", "0", false, false},
		{"empty string", "", false, false},
		{"nil", nil, false, false},
		{"invalid string", "maybe", false, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toBool(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestToTimestamp(t *testing.T) {
	tests := []struct {
		name     string
		input    any
		expected time.Time
		wantErr  bool
	}{
		{"time.Time", time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), false},
		{"RFC3339", "2024-01-15T10:30:00Z", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), false},
		{"date only", "2024-01-15", time.Date(2024, 1, 15, 0, 0, 0, 0, time.UTC), false},
		{"datetime space", "2024-01-15 10:30:00", time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC), false},
		{"unix int64", int64(1705315800), time.Unix(1705315800, 0).UTC(), false},
		{"unix float64", float64(1705315800.5), time.Unix(1705315800, 500000000).UTC(), false},
		{"invalid string", "not a date", time.Time{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := toTimestamp(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestEquals(t *testing.T) {
	tests := []struct {
		name     string
		a        any
		b        any
		expected bool
	}{
		{"both nil", nil, nil, true},
		{"nil and value", nil, "hello", false},
		{"value and nil", "hello", nil, false},
		{"equal strings", "hello", "hello", true},
		{"different strings", "hello", "world", false},
		{"equal ints", int64(42), int64(42), true},
		{"different ints", int64(42), int64(43), false},
		{"int and float equal", int64(42), float64(42), true},
		{"int and float different", int64(42), float64(42.5), false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := equals(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}
