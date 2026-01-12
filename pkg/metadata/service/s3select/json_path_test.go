// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetNestedValue_SimpleField(t *testing.T) {
	data := map[string]any{
		"name": "Alice",
		"age":  30,
	}

	assert.Equal(t, "Alice", getNestedValue(data, "name"))
	assert.Equal(t, 30, getNestedValue(data, "age"))
}

func TestGetNestedValue_NestedField(t *testing.T) {
	data := map[string]any{
		"user": map[string]any{
			"name": "Bob",
			"address": map[string]any{
				"city":    "NYC",
				"country": "USA",
			},
		},
	}

	assert.Equal(t, "Bob", getNestedValue(data, "user.name"))
	assert.Equal(t, "NYC", getNestedValue(data, "user.address.city"))
	assert.Equal(t, "USA", getNestedValue(data, "user.address.country"))
}

func TestGetNestedValue_ArrayIndex(t *testing.T) {
	data := map[string]any{
		"items": []any{
			map[string]any{"id": 1, "name": "item1"},
			map[string]any{"id": 2, "name": "item2"},
			map[string]any{"id": 3, "name": "item3"},
		},
	}

	// Access array element
	item0 := getNestedValue(data, "items[0]")
	assert.NotNil(t, item0)
	itemMap, ok := item0.(map[string]any)
	assert.True(t, ok)
	assert.Equal(t, 1, itemMap["id"])

	// Access field within array element
	assert.Equal(t, 1, getNestedValue(data, "items[0].id"))
	assert.Equal(t, "item2", getNestedValue(data, "items[1].name"))
	assert.Equal(t, 3, getNestedValue(data, "items[2].id"))
}

func TestGetNestedValue_ComplexPath(t *testing.T) {
	data := map[string]any{
		"users": []any{
			map[string]any{
				"name": "Alice",
				"addresses": []any{
					map[string]any{"city": "NYC", "zip": "10001"},
					map[string]any{"city": "LA", "zip": "90001"},
				},
			},
			map[string]any{
				"name": "Bob",
				"addresses": []any{
					map[string]any{"city": "Chicago", "zip": "60601"},
				},
			},
		},
	}

	assert.Equal(t, "Alice", getNestedValue(data, "users[0].name"))
	assert.Equal(t, "NYC", getNestedValue(data, "users[0].addresses[0].city"))
	assert.Equal(t, "90001", getNestedValue(data, "users[0].addresses[1].zip"))
	assert.Equal(t, "Bob", getNestedValue(data, "users[1].name"))
	assert.Equal(t, "Chicago", getNestedValue(data, "users[1].addresses[0].city"))
}

func TestGetNestedValue_MissingKey(t *testing.T) {
	data := map[string]any{
		"name": "Alice",
		"address": map[string]any{
			"city": "NYC",
		},
	}

	assert.Nil(t, getNestedValue(data, "nonexistent"))
	assert.Nil(t, getNestedValue(data, "address.nonexistent"))
	assert.Nil(t, getNestedValue(data, "address.city.nonexistent"))
}

func TestGetNestedValue_ArrayOutOfBounds(t *testing.T) {
	data := map[string]any{
		"items": []any{
			map[string]any{"id": 1},
			map[string]any{"id": 2},
		},
	}

	assert.Nil(t, getNestedValue(data, "items[5]"))
	assert.Nil(t, getNestedValue(data, "items[-1]"))
	assert.Nil(t, getNestedValue(data, "items[100].id"))
}

func TestGetNestedValue_TypeMismatch(t *testing.T) {
	data := map[string]any{
		"name":  "Alice",
		"items": "not an array",
		"count": 42,
	}

	// Trying to access nested field on non-map
	assert.Nil(t, getNestedValue(data, "name.field"))

	// Trying to access array index on non-array
	assert.Nil(t, getNestedValue(data, "items[0]"))

	// Trying to access nested field on number
	assert.Nil(t, getNestedValue(data, "count.field"))
}

func TestGetNestedValue_EmptyInput(t *testing.T) {
	assert.Nil(t, getNestedValue(nil, "name"))
	assert.Nil(t, getNestedValue(map[string]any{}, "name"))
	assert.Nil(t, getNestedValue(map[string]any{"name": "Alice"}, ""))
}

func TestGetNestedValue_NilValues(t *testing.T) {
	data := map[string]any{
		"user": map[string]any{
			"name":    "Alice",
			"address": nil,
		},
		"items": nil,
	}

	assert.Nil(t, getNestedValue(data, "user.address.city"))
	assert.Nil(t, getNestedValue(data, "items[0]"))
}

func TestParseJSONPath(t *testing.T) {
	tests := []struct {
		name     string
		path     string
		expected []pathSegment
	}{
		{
			name: "simple field",
			path: "name",
			expected: []pathSegment{
				{segType: segmentTypeField, value: "name"},
			},
		},
		{
			name: "nested fields",
			path: "address.city",
			expected: []pathSegment{
				{segType: segmentTypeField, value: "address"},
				{segType: segmentTypeField, value: "city"},
			},
		},
		{
			name: "array index",
			path: "items[0]",
			expected: []pathSegment{
				{segType: segmentTypeField, value: "items"},
				{segType: segmentTypeIndex, index: 0},
			},
		},
		{
			name: "array with field after",
			path: "items[0].id",
			expected: []pathSegment{
				{segType: segmentTypeField, value: "items"},
				{segType: segmentTypeIndex, index: 0},
				{segType: segmentTypeField, value: "id"},
			},
		},
		{
			name: "complex path",
			path: "users[2].address.city",
			expected: []pathSegment{
				{segType: segmentTypeField, value: "users"},
				{segType: segmentTypeIndex, index: 2},
				{segType: segmentTypeField, value: "address"},
				{segType: segmentTypeField, value: "city"},
			},
		},
		{
			name: "nested arrays",
			path: "data[0][1]",
			expected: []pathSegment{
				{segType: segmentTypeField, value: "data"},
				{segType: segmentTypeIndex, index: 0},
				{segType: segmentTypeIndex, index: 1},
			},
		},
		{
			name:     "empty path",
			path:     "",
			expected: nil,
		},
		{
			name:     "malformed - no closing bracket",
			path:     "items[0",
			expected: nil,
		},
		{
			name:     "malformed - non-numeric index",
			path:     "items[abc]",
			expected: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := parseJSONPath(tc.path)
			assert.Equal(t, tc.expected, result)
		})
	}
}
