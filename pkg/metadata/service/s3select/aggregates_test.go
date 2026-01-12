// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCountAccumulator_CountStar(t *testing.T) {
	acc := NewCountAccumulator()

	// COUNT(*) counts all rows including nulls
	acc.Accumulate("Alice", true)
	acc.Accumulate(nil, true) // null still counted with COUNT(*)
	acc.Accumulate("Bob", true)

	result := acc.Result()
	assert.Equal(t, int64(3), result)
}

func TestCountAccumulator_CountColumn(t *testing.T) {
	acc := NewCountAccumulator()

	// COUNT(col) counts only non-null values
	acc.Accumulate("Alice", false)
	acc.Accumulate(nil, false) // null NOT counted with COUNT(col)
	acc.Accumulate("Bob", false)
	acc.Accumulate(nil, false) // another null
	acc.Accumulate("Charlie", false)

	result := acc.Result()
	assert.Equal(t, int64(3), result) // Only 3 non-null values
}

func TestCountAccumulator_EmptySet(t *testing.T) {
	acc := NewCountAccumulator()

	// COUNT returns 0 for empty set, not nil
	result := acc.Result()
	assert.Equal(t, int64(0), result)
}

func TestCountAccumulator_Reset(t *testing.T) {
	acc := NewCountAccumulator()
	acc.Accumulate("Alice", true)
	acc.Accumulate("Bob", true)

	acc.Reset()

	result := acc.Result()
	assert.Equal(t, int64(0), result)
}

func TestSumAccumulator_Integers(t *testing.T) {
	acc := NewSumAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate(int64(20), false)
	acc.Accumulate(int64(30), false)

	result := acc.Result()
	assert.Equal(t, float64(60), result)
}

func TestSumAccumulator_Floats(t *testing.T) {
	acc := NewSumAccumulator()

	acc.Accumulate(float64(1.5), false)
	acc.Accumulate(float64(2.5), false)
	acc.Accumulate(float64(3.0), false)

	result := acc.Result()
	assert.Equal(t, float64(7.0), result)
}

func TestSumAccumulator_StringNumbers(t *testing.T) {
	acc := NewSumAccumulator()

	acc.Accumulate("10", false)
	acc.Accumulate("20.5", false)
	acc.Accumulate("30", false)

	result := acc.Result()
	assert.Equal(t, float64(60.5), result)
}

func TestSumAccumulator_NilIgnored(t *testing.T) {
	acc := NewSumAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate(nil, false) // nil ignored
	acc.Accumulate(int64(20), false)
	acc.Accumulate(nil, false) // nil ignored

	result := acc.Result()
	assert.Equal(t, float64(30), result)
}

func TestSumAccumulator_EmptySet(t *testing.T) {
	acc := NewSumAccumulator()

	result := acc.Result()
	assert.Nil(t, result)
}

func TestSumAccumulator_AllNulls(t *testing.T) {
	acc := NewSumAccumulator()

	acc.Accumulate(nil, false)
	acc.Accumulate(nil, false)

	result := acc.Result()
	assert.Nil(t, result)
}

func TestSumAccumulator_Reset(t *testing.T) {
	acc := NewSumAccumulator()
	acc.Accumulate(int64(100), false)

	acc.Reset()

	result := acc.Result()
	assert.Nil(t, result)
}

func TestSumAccumulator_MixedTypes(t *testing.T) {
	acc := NewSumAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate(float64(5.5), false)
	acc.Accumulate("4.5", false)
	acc.Accumulate(int(20), false)

	result := acc.Result()
	assert.Equal(t, float64(40.0), result)
}

func TestAvgAccumulator_Integers(t *testing.T) {
	acc := NewAvgAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate(int64(20), false)
	acc.Accumulate(int64(30), false)

	result := acc.Result()
	assert.Equal(t, float64(20), result)
}

func TestAvgAccumulator_Floats(t *testing.T) {
	acc := NewAvgAccumulator()

	acc.Accumulate(float64(1.0), false)
	acc.Accumulate(float64(2.0), false)
	acc.Accumulate(float64(3.0), false)
	acc.Accumulate(float64(4.0), false)

	result := acc.Result()
	assert.Equal(t, float64(2.5), result)
}

func TestAvgAccumulator_EmptySet(t *testing.T) {
	acc := NewAvgAccumulator()

	// AVG returns nil for empty set
	result := acc.Result()
	assert.Nil(t, result)
}

func TestAvgAccumulator_NilIgnored(t *testing.T) {
	acc := NewAvgAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate(nil, false) // ignored
	acc.Accumulate(int64(20), false)

	result := acc.Result()
	assert.Equal(t, float64(15), result) // (10+20)/2 = 15
}

func TestAvgAccumulator_Reset(t *testing.T) {
	acc := NewAvgAccumulator()
	acc.Accumulate(int64(100), false)
	acc.Accumulate(int64(200), false)

	acc.Reset()

	result := acc.Result()
	assert.Nil(t, result)
}

func TestMinAccumulator_Numbers(t *testing.T) {
	acc := NewMinAccumulator()

	acc.Accumulate(int64(30), false)
	acc.Accumulate(int64(10), false)
	acc.Accumulate(int64(20), false)

	result := acc.Result()
	assert.Equal(t, float64(10), result)
}

func TestMinAccumulator_Floats(t *testing.T) {
	acc := NewMinAccumulator()

	acc.Accumulate(float64(3.5), false)
	acc.Accumulate(float64(1.2), false)
	acc.Accumulate(float64(2.8), false)

	result := acc.Result()
	assert.Equal(t, float64(1.2), result)
}

func TestMinAccumulator_Strings(t *testing.T) {
	acc := NewMinAccumulator()

	acc.Accumulate("Charlie", false)
	acc.Accumulate("Alice", false)
	acc.Accumulate("Bob", false)

	result := acc.Result()
	assert.Equal(t, "Alice", result)
}

func TestMinAccumulator_NilIgnored(t *testing.T) {
	acc := NewMinAccumulator()

	acc.Accumulate(int64(20), false)
	acc.Accumulate(nil, false) // ignored
	acc.Accumulate(int64(10), false)
	acc.Accumulate(nil, false) // ignored

	result := acc.Result()
	assert.Equal(t, float64(10), result)
}

func TestMinAccumulator_EmptySet(t *testing.T) {
	acc := NewMinAccumulator()

	result := acc.Result()
	assert.Nil(t, result)
}

func TestMinAccumulator_Reset(t *testing.T) {
	acc := NewMinAccumulator()
	acc.Accumulate(int64(5), false)

	acc.Reset()

	result := acc.Result()
	assert.Nil(t, result)
}

func TestMaxAccumulator_Numbers(t *testing.T) {
	acc := NewMaxAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate(int64(30), false)
	acc.Accumulate(int64(20), false)

	result := acc.Result()
	assert.Equal(t, float64(30), result)
}

func TestMaxAccumulator_Floats(t *testing.T) {
	acc := NewMaxAccumulator()

	acc.Accumulate(float64(1.5), false)
	acc.Accumulate(float64(3.7), false)
	acc.Accumulate(float64(2.2), false)

	result := acc.Result()
	assert.Equal(t, float64(3.7), result)
}

func TestMaxAccumulator_Strings(t *testing.T) {
	acc := NewMaxAccumulator()

	acc.Accumulate("Alice", false)
	acc.Accumulate("Charlie", false)
	acc.Accumulate("Bob", false)

	result := acc.Result()
	assert.Equal(t, "Charlie", result)
}

func TestMaxAccumulator_NilIgnored(t *testing.T) {
	acc := NewMaxAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate(nil, false) // ignored
	acc.Accumulate(int64(30), false)
	acc.Accumulate(nil, false) // ignored

	result := acc.Result()
	assert.Equal(t, float64(30), result)
}

func TestMaxAccumulator_EmptySet(t *testing.T) {
	acc := NewMaxAccumulator()

	result := acc.Result()
	assert.Nil(t, result)
}

func TestMaxAccumulator_Reset(t *testing.T) {
	acc := NewMaxAccumulator()
	acc.Accumulate(int64(100), false)

	acc.Reset()

	result := acc.Result()
	assert.Nil(t, result)
}

func TestNewAccumulator_AllTypes(t *testing.T) {
	tests := []struct {
		name        string
		funcName    string
		expectError bool
	}{
		{"COUNT", "COUNT", false},
		{"count lowercase", "count", false},
		{"SUM", "SUM", false},
		{"sum lowercase", "sum", false},
		{"AVG", "AVG", false},
		{"avg lowercase", "avg", false},
		{"MIN", "MIN", false},
		{"min lowercase", "min", false},
		{"MAX", "MAX", false},
		{"max lowercase", "max", false},
		{"unknown", "UNKNOWN", true},
		{"CONCAT", "CONCAT", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			acc, err := NewAccumulator(tt.funcName)
			if tt.expectError {
				require.Error(t, err)
				assert.Nil(t, acc)
			} else {
				require.NoError(t, err)
				require.NotNil(t, acc)
			}
		})
	}
}

func TestIsAggregateFunction(t *testing.T) {
	tests := []struct {
		name     string
		funcName string
		expected bool
	}{
		{"COUNT", "COUNT", true},
		{"count lowercase", "count", true},
		{"SUM", "SUM", true},
		{"sum lowercase", "sum", true},
		{"AVG", "AVG", true},
		{"avg lowercase", "avg", true},
		{"MIN", "MIN", true},
		{"min lowercase", "min", true},
		{"MAX", "MAX", true},
		{"max lowercase", "max", true},
		{"LOWER not aggregate", "LOWER", false},
		{"UPPER not aggregate", "UPPER", false},
		{"TRIM not aggregate", "TRIM", false},
		{"CAST not aggregate", "CAST", false},
		{"unknown", "FOOBAR", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsAggregateFunction(tt.funcName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestMinAccumulator_StringNumbers(t *testing.T) {
	// Test MIN with numeric strings - first value determines type
	// Since first value is a string, string comparison is used
	acc := NewMinAccumulator()

	acc.Accumulate("30", false)
	acc.Accumulate("10", false)
	acc.Accumulate("20", false)

	result := acc.Result()
	// String comparison: "10" < "20" < "30" alphabetically
	assert.Equal(t, "10", result)
}

func TestMaxAccumulator_StringNumbers(t *testing.T) {
	// Test MAX with numeric strings - first value determines type
	// Since first value is a string, string comparison is used
	acc := NewMaxAccumulator()

	acc.Accumulate("10", false)
	acc.Accumulate("30", false)
	acc.Accumulate("20", false)

	result := acc.Result()
	// String comparison: "30" > "20" > "10" alphabetically
	assert.Equal(t, "30", result)
}

func TestMinAccumulator_MixedNumericTypes(t *testing.T) {
	acc := NewMinAccumulator()

	acc.Accumulate(int64(50), false)
	acc.Accumulate(float64(25.5), false)
	acc.Accumulate(int(10), false)
	acc.Accumulate("5", false) // String number

	result := acc.Result()
	assert.Equal(t, float64(5), result)
}

func TestMaxAccumulator_MixedNumericTypes(t *testing.T) {
	acc := NewMaxAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate(float64(25.5), false)
	acc.Accumulate(int(50), false)
	acc.Accumulate("100", false) // String number

	result := acc.Result()
	assert.Equal(t, float64(100), result)
}

func TestSumAccumulator_InvalidStringsIgnored(t *testing.T) {
	acc := NewSumAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate("not a number", false) // Should be ignored
	acc.Accumulate(int64(20), false)

	result := acc.Result()
	assert.Equal(t, float64(30), result)
}

func TestAvgAccumulator_InvalidStringsIgnored(t *testing.T) {
	acc := NewAvgAccumulator()

	acc.Accumulate(int64(10), false)
	acc.Accumulate("not a number", false) // Should be ignored
	acc.Accumulate(int64(20), false)

	result := acc.Result()
	assert.Equal(t, float64(15), result) // (10+20)/2 = 15
}
