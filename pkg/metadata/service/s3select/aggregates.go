// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"fmt"
	"strconv"
	"strings"
)

// Accumulator is the interface for streaming aggregate functions.
// Each accumulator processes values incrementally without storing all data.
type Accumulator interface {
	// Accumulate adds a value to the accumulator.
	// The isCountStar parameter indicates if this is COUNT(*) (count all rows including nulls).
	Accumulate(value any, isCountStar bool)

	// Result returns the final aggregate result.
	// Returns nil for empty sets (except COUNT which returns 0).
	Result() any

	// Reset clears the accumulator state.
	Reset()
}

// CountAccumulator implements COUNT(*) and COUNT(column).
// COUNT(*) counts all rows including nulls.
// COUNT(column) counts only non-null values.
type CountAccumulator struct {
	count int64
}

// NewCountAccumulator creates a new count accumulator.
func NewCountAccumulator() *CountAccumulator {
	return &CountAccumulator{}
}

// Accumulate increments the count.
// For COUNT(*) (isCountStar=true), counts all rows.
// For COUNT(column) (isCountStar=false), counts only non-null values.
func (a *CountAccumulator) Accumulate(value any, isCountStar bool) {
	if isCountStar || value != nil {
		a.count++
	}
}

// Result returns the count.
// COUNT always returns 0 for empty sets, never nil.
func (a *CountAccumulator) Result() any {
	return a.count
}

// Reset clears the count.
func (a *CountAccumulator) Reset() {
	a.count = 0
}

// SumAccumulator implements SUM(column) with type coercion.
// Null values are ignored.
type SumAccumulator struct {
	sum      float64
	hasValue bool
}

// NewSumAccumulator creates a new sum accumulator.
func NewSumAccumulator() *SumAccumulator {
	return &SumAccumulator{}
}

// Accumulate adds a value to the sum.
// Non-numeric values are coerced to numbers using aggToFloat64.
// Null values are ignored.
func (a *SumAccumulator) Accumulate(value any, _ bool) {
	if value == nil {
		return
	}
	f, err := aggToFloat64(value)
	if err != nil {
		return // Ignore non-numeric values
	}
	a.sum += f
	a.hasValue = true
}

// Result returns the sum or nil for empty sets.
func (a *SumAccumulator) Result() any {
	if !a.hasValue {
		return nil
	}
	return a.sum
}

// Reset clears the sum.
func (a *SumAccumulator) Reset() {
	a.sum = 0
	a.hasValue = false
}

// AvgAccumulator implements AVG(column) by tracking sum and count.
// Null values are ignored.
type AvgAccumulator struct {
	sum   float64
	count int64
}

// NewAvgAccumulator creates a new average accumulator.
func NewAvgAccumulator() *AvgAccumulator {
	return &AvgAccumulator{}
}

// Accumulate adds a value to the average calculation.
// Non-numeric values are coerced to numbers using aggToFloat64.
// Null values are ignored.
func (a *AvgAccumulator) Accumulate(value any, _ bool) {
	if value == nil {
		return
	}
	f, err := aggToFloat64(value)
	if err != nil {
		return // Ignore non-numeric values
	}
	a.sum += f
	a.count++
}

// Result returns the average or nil for empty sets.
func (a *AvgAccumulator) Result() any {
	if a.count == 0 {
		return nil
	}
	return a.sum / float64(a.count)
}

// Reset clears the average state.
func (a *AvgAccumulator) Reset() {
	a.sum = 0
	a.count = 0
}

// MinAccumulator implements MIN(column) for numbers and strings.
// Null values are ignored.
type MinAccumulator struct {
	min      any
	hasValue bool
	isString bool
}

// NewMinAccumulator creates a new min accumulator.
func NewMinAccumulator() *MinAccumulator {
	return &MinAccumulator{}
}

// Accumulate tracks the minimum value.
// Works with both numbers and strings.
// Null values are ignored.
func (a *MinAccumulator) Accumulate(value any, _ bool) {
	if value == nil {
		return
	}

	if !a.hasValue {
		// First value - determine type
		if _, ok := value.(string); ok {
			a.min = value
			a.isString = true
		} else {
			// Try to convert to float64 for numeric comparison
			f, err := aggToFloat64(value)
			if err != nil {
				// Fall back to string comparison
				s := aggToString(value)
				a.min = s
				a.isString = true
			} else {
				a.min = f
				a.isString = false
			}
		}
		a.hasValue = true
		return
	}

	// Compare with current min
	if a.isString {
		s := aggToString(value)
		if s < a.min.(string) {
			a.min = s
		}
	} else {
		f, err := aggToFloat64(value)
		if err != nil {
			return // Skip non-numeric values
		}
		if f < a.min.(float64) {
			a.min = f
		}
	}
}

// Result returns the minimum value or nil for empty sets.
func (a *MinAccumulator) Result() any {
	if !a.hasValue {
		return nil
	}
	return a.min
}

// Reset clears the min state.
func (a *MinAccumulator) Reset() {
	a.min = nil
	a.hasValue = false
	a.isString = false
}

// MaxAccumulator implements MAX(column) for numbers and strings.
// Null values are ignored.
type MaxAccumulator struct {
	max      any
	hasValue bool
	isString bool
}

// NewMaxAccumulator creates a new max accumulator.
func NewMaxAccumulator() *MaxAccumulator {
	return &MaxAccumulator{}
}

// Accumulate tracks the maximum value.
// Works with both numbers and strings.
// Null values are ignored.
func (a *MaxAccumulator) Accumulate(value any, _ bool) {
	if value == nil {
		return
	}

	if !a.hasValue {
		// First value - determine type
		if _, ok := value.(string); ok {
			a.max = value
			a.isString = true
		} else {
			// Try to convert to float64 for numeric comparison
			f, err := aggToFloat64(value)
			if err != nil {
				// Fall back to string comparison
				s := aggToString(value)
				a.max = s
				a.isString = true
			} else {
				a.max = f
				a.isString = false
			}
		}
		a.hasValue = true
		return
	}

	// Compare with current max
	if a.isString {
		s := aggToString(value)
		if s > a.max.(string) {
			a.max = s
		}
	} else {
		f, err := aggToFloat64(value)
		if err != nil {
			return // Skip non-numeric values
		}
		if f > a.max.(float64) {
			a.max = f
		}
	}
}

// Result returns the maximum value or nil for empty sets.
func (a *MaxAccumulator) Result() any {
	if !a.hasValue {
		return nil
	}
	return a.max
}

// Reset clears the max state.
func (a *MaxAccumulator) Reset() {
	a.max = nil
	a.hasValue = false
	a.isString = false
}

// NewAccumulator creates an accumulator for the given aggregate function name.
// Function names are case-insensitive.
// Returns an error if the function name is not a recognized aggregate.
func NewAccumulator(name string) (Accumulator, error) {
	switch strings.ToUpper(name) {
	case "COUNT":
		return NewCountAccumulator(), nil
	case "SUM":
		return NewSumAccumulator(), nil
	case "AVG":
		return NewAvgAccumulator(), nil
	case "MIN":
		return NewMinAccumulator(), nil
	case "MAX":
		return NewMaxAccumulator(), nil
	default:
		return nil, fmt.Errorf("unknown aggregate function: %s", name)
	}
}

// IsAggregateFunction returns true if the function name is an aggregate function.
func IsAggregateFunction(name string) bool {
	switch strings.ToUpper(name) {
	case "COUNT", "SUM", "AVG", "MIN", "MAX":
		return true
	default:
		return false
	}
}

// aggToFloat64 converts a value to float64 for aggregate calculations.
func aggToFloat64(v any) (float64, error) {
	switch val := v.(type) {
	case float64:
		return val, nil
	case float32:
		return float64(val), nil
	case int:
		return float64(val), nil
	case int64:
		return float64(val), nil
	case int32:
		return float64(val), nil
	case string:
		f, err := strconv.ParseFloat(val, 64)
		if err != nil {
			return 0, fmt.Errorf("cannot convert %q to float", val)
		}
		return f, nil
	case bool:
		if val {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float", v)
	}
}

// aggToString converts a value to string for aggregate comparisons.
func aggToString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	case int:
		return strconv.Itoa(val)
	case int64:
		return strconv.FormatInt(val, 10)
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64)
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32)
	case bool:
		return strconv.FormatBool(val)
	default:
		return fmt.Sprintf("%v", v)
	}
}
