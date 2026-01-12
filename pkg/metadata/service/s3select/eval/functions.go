// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package eval

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// FuncImpl is the implementation of a SQL function.
// It takes any number of arguments and returns a value or error.
type FuncImpl func(args ...any) (any, error)

// FuncRegistry holds all registered SQL functions.
type FuncRegistry struct {
	funcs map[string]FuncImpl
}

// NewFuncRegistry creates a new function registry with all built-in functions.
func NewFuncRegistry() *FuncRegistry {
	r := &FuncRegistry{
		funcs: make(map[string]FuncImpl),
	}
	r.registerStringFunctions()
	r.registerTypeFunctions()
	r.registerNullFunctions()
	r.registerDateFunctions()
	return r
}

// Get returns the function implementation for the given name.
// Function names are case-insensitive.
func (r *FuncRegistry) Get(name string) FuncImpl {
	return r.funcs[strings.ToUpper(name)]
}

// register adds a function to the registry.
func (r *FuncRegistry) register(name string, fn FuncImpl) {
	r.funcs[strings.ToUpper(name)] = fn
}

// ============================================================================
// String Functions
// ============================================================================

func (r *FuncRegistry) registerStringFunctions() {
	// LOWER(s) - Convert to lowercase
	r.register("LOWER", funcLower)

	// UPPER(s) - Convert to uppercase
	r.register("UPPER", funcUpper)

	// CHAR_LENGTH(s) - Character count (not bytes)
	r.register("CHAR_LENGTH", funcCharLength)
	r.register("CHARACTER_LENGTH", funcCharLength) // Alias

	// TRIM(s) - Trim whitespace
	r.register("TRIM", funcTrim)

	// SUBSTRING(s, start, [len]) - Extract substring (1-indexed)
	r.register("SUBSTRING", funcSubstring)
}

func funcLower(args ...any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("LOWER requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, err := toString(args[0])
	if err != nil {
		return nil, fmt.Errorf("LOWER: %w", err)
	}
	return strings.ToLower(s), nil
}

func funcUpper(args ...any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("UPPER requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, err := toString(args[0])
	if err != nil {
		return nil, fmt.Errorf("UPPER: %w", err)
	}
	return strings.ToUpper(s), nil
}

func funcCharLength(args ...any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("CHAR_LENGTH requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, err := toString(args[0])
	if err != nil {
		return nil, fmt.Errorf("CHAR_LENGTH: %w", err)
	}
	return int64(utf8.RuneCountInString(s)), nil
}

func funcTrim(args ...any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TRIM requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	s, err := toString(args[0])
	if err != nil {
		return nil, fmt.Errorf("TRIM: %w", err)
	}
	return strings.TrimSpace(s), nil
}

func funcSubstring(args ...any) (any, error) {
	if len(args) < 2 || len(args) > 3 {
		return nil, fmt.Errorf("SUBSTRING requires 2 or 3 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}

	s, err := toString(args[0])
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING: %w", err)
	}

	start, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("SUBSTRING start: %w", err)
	}

	// SQL uses 1-based indexing
	runes := []rune(s)
	runeLen := int64(len(runes))

	// Convert to 0-based index
	startIdx := start - 1
	if startIdx < 0 {
		startIdx = 0
	}
	if startIdx >= runeLen {
		return "", nil
	}

	// Default length is rest of string
	length := runeLen - startIdx
	if len(args) == 3 && args[2] != nil {
		length, err = toInt64(args[2])
		if err != nil {
			return nil, fmt.Errorf("SUBSTRING length: %w", err)
		}
		if length < 0 {
			length = 0
		}
	}

	endIdx := startIdx + length
	if endIdx > runeLen {
		endIdx = runeLen
	}

	return string(runes[startIdx:endIdx]), nil
}

// ============================================================================
// Type Conversion Functions
// ============================================================================

func (r *FuncRegistry) registerTypeFunctions() {
	// CAST(value, type) - Convert to INT, FLOAT, STRING, BOOL, TIMESTAMP
	r.register("CAST", funcCast)
}

func funcCast(args ...any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("CAST requires exactly 2 arguments (value, type), got %d", len(args))
	}

	value := args[0]
	if value == nil {
		return nil, nil
	}

	targetType, err := toString(args[1])
	if err != nil {
		return nil, fmt.Errorf("CAST: invalid type argument: %w", err)
	}

	switch strings.ToUpper(targetType) {
	case "INT", "INTEGER":
		return toInt64(value)
	case "FLOAT", "DOUBLE", "DECIMAL":
		return toFloat64(value)
	case "STRING", "VARCHAR":
		return toString(value)
	case "BOOL", "BOOLEAN":
		return toBool(value)
	case "TIMESTAMP":
		return toTimestamp(value)
	default:
		return nil, fmt.Errorf("CAST: unsupported target type: %s", targetType)
	}
}

// ============================================================================
// Null Handling Functions
// ============================================================================

func (r *FuncRegistry) registerNullFunctions() {
	// COALESCE(a, b, ...) - First non-null value
	r.register("COALESCE", funcCoalesce)

	// NULLIF(a, b) - Return null if a equals b
	r.register("NULLIF", funcNullif)
}

func funcCoalesce(args ...any) (any, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("COALESCE requires at least 1 argument")
	}
	for _, arg := range args {
		if arg != nil {
			return arg, nil
		}
	}
	return nil, nil
}

func funcNullif(args ...any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("NULLIF requires exactly 2 arguments, got %d", len(args))
	}
	if equals(args[0], args[1]) {
		return nil, nil
	}
	return args[0], nil
}

// ============================================================================
// Date/Time Functions
// ============================================================================

func (r *FuncRegistry) registerDateFunctions() {
	// UTCNOW() - Current UTC time
	r.register("UTCNOW", funcUtcNow)

	// EXTRACT(part, timestamp) - Extract YEAR, MONTH, DAY, HOUR, MINUTE, SECOND
	r.register("EXTRACT", funcExtract)

	// DATE_ADD(part, quantity, timestamp) - Add interval
	r.register("DATE_ADD", funcDateAdd)

	// DATE_DIFF(part, ts1, ts2) - Difference in units
	r.register("DATE_DIFF", funcDateDiff)

	// TO_TIMESTAMP(s) - Parse string to timestamp
	r.register("TO_TIMESTAMP", funcToTimestamp)

	// TO_STRING(ts, [format]) - Format timestamp
	r.register("TO_STRING", funcToString)
}

func funcUtcNow(_ ...any) (any, error) {
	return time.Now().UTC(), nil
}

func funcExtract(args ...any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("EXTRACT requires exactly 2 arguments (part, timestamp), got %d", len(args))
	}
	if args[1] == nil {
		return nil, nil
	}

	part, err := toString(args[0])
	if err != nil {
		return nil, fmt.Errorf("EXTRACT: invalid part: %w", err)
	}

	ts, err := toTimestamp(args[1])
	if err != nil {
		return nil, fmt.Errorf("EXTRACT: %w", err)
	}

	switch strings.ToUpper(part) {
	case "YEAR":
		return int64(ts.Year()), nil
	case "MONTH":
		return int64(ts.Month()), nil
	case "DAY":
		return int64(ts.Day()), nil
	case "HOUR":
		return int64(ts.Hour()), nil
	case "MINUTE":
		return int64(ts.Minute()), nil
	case "SECOND":
		return int64(ts.Second()), nil
	default:
		return nil, fmt.Errorf("EXTRACT: unsupported part: %s", part)
	}
}

func funcDateAdd(args ...any) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_ADD requires exactly 3 arguments (part, quantity, timestamp), got %d", len(args))
	}
	if args[2] == nil {
		return nil, nil
	}

	part, err := toString(args[0])
	if err != nil {
		return nil, fmt.Errorf("DATE_ADD: invalid part: %w", err)
	}

	quantity, err := toInt64(args[1])
	if err != nil {
		return nil, fmt.Errorf("DATE_ADD: invalid quantity: %w", err)
	}

	ts, err := toTimestamp(args[2])
	if err != nil {
		return nil, fmt.Errorf("DATE_ADD: %w", err)
	}

	switch strings.ToUpper(part) {
	case "YEAR":
		return ts.AddDate(int(quantity), 0, 0), nil
	case "MONTH":
		return ts.AddDate(0, int(quantity), 0), nil
	case "DAY":
		return ts.AddDate(0, 0, int(quantity)), nil
	case "HOUR":
		return ts.Add(time.Duration(quantity) * time.Hour), nil
	case "MINUTE":
		return ts.Add(time.Duration(quantity) * time.Minute), nil
	case "SECOND":
		return ts.Add(time.Duration(quantity) * time.Second), nil
	default:
		return nil, fmt.Errorf("DATE_ADD: unsupported part: %s", part)
	}
}

func funcDateDiff(args ...any) (any, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("DATE_DIFF requires exactly 3 arguments (part, ts1, ts2), got %d", len(args))
	}
	if args[1] == nil || args[2] == nil {
		return nil, nil
	}

	part, err := toString(args[0])
	if err != nil {
		return nil, fmt.Errorf("DATE_DIFF: invalid part: %w", err)
	}

	ts1, err := toTimestamp(args[1])
	if err != nil {
		return nil, fmt.Errorf("DATE_DIFF: %w", err)
	}

	ts2, err := toTimestamp(args[2])
	if err != nil {
		return nil, fmt.Errorf("DATE_DIFF: %w", err)
	}

	diff := ts2.Sub(ts1)

	switch strings.ToUpper(part) {
	case "YEAR":
		return int64(ts2.Year() - ts1.Year()), nil
	case "MONTH":
		years := ts2.Year() - ts1.Year()
		months := int(ts2.Month()) - int(ts1.Month())
		return int64(years*12 + months), nil
	case "DAY":
		return int64(diff.Hours() / 24), nil
	case "HOUR":
		return int64(diff.Hours()), nil
	case "MINUTE":
		return int64(diff.Minutes()), nil
	case "SECOND":
		return int64(diff.Seconds()), nil
	default:
		return nil, fmt.Errorf("DATE_DIFF: unsupported part: %s", part)
	}
}

func funcToTimestamp(args ...any) (any, error) {
	if len(args) != 1 {
		return nil, fmt.Errorf("TO_TIMESTAMP requires exactly 1 argument, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}
	return toTimestamp(args[0])
}

func funcToString(args ...any) (any, error) {
	if len(args) < 1 || len(args) > 2 {
		return nil, fmt.Errorf("TO_STRING requires 1 or 2 arguments, got %d", len(args))
	}
	if args[0] == nil {
		return nil, nil
	}

	ts, err := toTimestamp(args[0])
	if err != nil {
		return nil, fmt.Errorf("TO_STRING: %w", err)
	}

	// Default format is RFC3339
	format := time.RFC3339
	if len(args) == 2 && args[1] != nil {
		formatStr, err := toString(args[1])
		if err != nil {
			return nil, fmt.Errorf("TO_STRING: invalid format: %w", err)
		}
		format = convertToGoTimeFormat(formatStr)
	}

	return ts.Format(format), nil
}

// ============================================================================
// Helper Functions
// ============================================================================

func toString(v any) (string, error) {
	switch val := v.(type) {
	case string:
		return val, nil
	case []byte:
		return string(val), nil
	case int:
		return strconv.Itoa(val), nil
	case int64:
		return strconv.FormatInt(val, 10), nil
	case float64:
		return strconv.FormatFloat(val, 'f', -1, 64), nil
	case float32:
		return strconv.FormatFloat(float64(val), 'f', -1, 32), nil
	case bool:
		return strconv.FormatBool(val), nil
	case time.Time:
		return val.Format(time.RFC3339), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func toInt64(v any) (int64, error) {
	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int64:
		return val, nil
	case int32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case float32:
		return int64(val), nil
	case string:
		// Try parsing as integer first
		if i, err := strconv.ParseInt(val, 10, 64); err == nil {
			return i, nil
		}
		// Try parsing as float then convert
		if f, err := strconv.ParseFloat(val, 64); err == nil {
			return int64(f), nil
		}
		return 0, fmt.Errorf("cannot convert %q to int", val)
	case bool:
		if val {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}

func toFloat64(v any) (float64, error) {
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

func toBool(v any) (bool, error) {
	switch val := v.(type) {
	case bool:
		return val, nil
	case int:
		return val != 0, nil
	case int64:
		return val != 0, nil
	case float64:
		return val != 0, nil
	case string:
		lower := strings.ToLower(val)
		switch lower {
		case "true", "t", "yes", "y", "1":
			return true, nil
		case "false", "f", "no", "n", "0", "":
			return false, nil
		default:
			return false, fmt.Errorf("cannot convert %q to bool", val)
		}
	case nil:
		return false, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

// Timestamp parsing formats to try
var timestampFormats = []string{
	time.RFC3339,
	time.RFC3339Nano,
	"2006-01-02T15:04:05",
	"2006-01-02 15:04:05",
	"2006-01-02",
	"2006/01/02",
	"01/02/2006",
	"02-Jan-2006",
}

func toTimestamp(v any) (time.Time, error) {
	switch val := v.(type) {
	case time.Time:
		return val, nil
	case string:
		for _, format := range timestampFormats {
			if t, err := time.Parse(format, val); err == nil {
				return t, nil
			}
		}
		return time.Time{}, fmt.Errorf("cannot parse %q as timestamp", val)
	case int64:
		// Assume Unix timestamp in seconds
		return time.Unix(val, 0).UTC(), nil
	case float64:
		// Assume Unix timestamp with fractional seconds
		sec := int64(val)
		nsec := int64((val - float64(sec)) * 1e9)
		return time.Unix(sec, nsec).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to timestamp", v)
	}
}

// equals compares two values for equality
func equals(a, b any) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// String comparison
	as, aok := a.(string)
	bs, bok := b.(string)
	if aok && bok {
		return as == bs
	}

	// Numeric comparison
	af, aerr := toFloat64(a)
	bf, berr := toFloat64(b)
	if aerr == nil && berr == nil {
		return af == bf
	}

	// Fall back to direct comparison
	return a == b
}

// convertToGoTimeFormat converts common SQL date format patterns to Go format
func convertToGoTimeFormat(format string) string {
	// Replacements must be applied in order (longer patterns first)
	// to avoid partial replacement issues (e.g., "YY" replacing part of "YYYY")
	replacements := []struct {
		pattern   string
		goPattern string
	}{
		// Year patterns - longer first
		{"YYYY", "2006"},
		{"yyyy", "2006"},
		{"YY", "06"},
		{"yy", "06"},
		// Month/Day patterns
		{"MM", "01"},
		{"DD", "02"},
		{"dd", "02"},
		// Time patterns
		{"HH", "15"},
		{"hh", "03"},
		{"mm", "04"},
		{"ss", "05"},
		{"SS", "05"},
	}

	result := format
	for _, r := range replacements {
		result = strings.ReplaceAll(result, r.pattern, r.goPattern)
	}
	return result
}
