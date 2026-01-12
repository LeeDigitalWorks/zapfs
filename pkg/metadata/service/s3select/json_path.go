// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3select

import (
	"strconv"
	"strings"
)

// getNestedValue retrieves a value from a nested map structure using dot notation.
// Supports:
// - Simple field access: "name" -> data["name"]
// - Nested field access: "address.city" -> data["address"]["city"]
// - Array index access: "items[0].id" -> data["items"][0]["id"]
// - Mixed access: "users[0].address.city" -> data["users"][0]["address"]["city"]
//
// Returns nil if the path does not exist or any intermediate value is not of the expected type.
func getNestedValue(data map[string]any, path string) any {
	if data == nil || path == "" {
		return nil
	}

	// Parse path into segments
	segments := parseJSONPath(path)
	if len(segments) == 0 {
		return nil
	}

	var current any = data
	for _, seg := range segments {
		if current == nil {
			return nil
		}

		switch seg.segType {
		case segmentTypeField:
			// Field access requires a map
			m, ok := current.(map[string]any)
			if !ok {
				return nil
			}
			current = m[seg.value]

		case segmentTypeIndex:
			// Array index access requires a slice
			arr, ok := current.([]any)
			if !ok {
				return nil
			}
			if seg.index < 0 || seg.index >= len(arr) {
				return nil
			}
			current = arr[seg.index]
		}
	}

	return current
}

// segmentType identifies whether a path segment is a field name or array index.
type segmentType int

const (
	segmentTypeField segmentType = iota
	segmentTypeIndex
)

// pathSegment represents a single part of a JSON path.
type pathSegment struct {
	segType segmentType
	value   string // field name for field access
	index   int    // array index for index access
}

// parseJSONPath parses a JSON path string into segments.
// Examples:
//   - "name" -> [{field, "name", 0}]
//   - "address.city" -> [{field, "address", 0}, {field, "city", 0}]
//   - "items[0]" -> [{field, "items", 0}, {index, "", 0}]
//   - "items[0].id" -> [{field, "items", 0}, {index, "", 0}, {field, "id", 0}]
//   - "users[2].address.city" -> [{field, "users", 0}, {index, "", 2}, {field, "address", 0}, {field, "city", 0}]
func parseJSONPath(path string) []pathSegment {
	if path == "" {
		return nil
	}

	var segments []pathSegment
	var currentField strings.Builder

	i := 0
	for i < len(path) {
		ch := path[i]

		switch ch {
		case '.':
			// Dot separator - flush current field if any
			if currentField.Len() > 0 {
				segments = append(segments, pathSegment{
					segType: segmentTypeField,
					value:   currentField.String(),
				})
				currentField.Reset()
			}
			i++

		case '[':
			// Start of array index - flush current field if any
			if currentField.Len() > 0 {
				segments = append(segments, pathSegment{
					segType: segmentTypeField,
					value:   currentField.String(),
				})
				currentField.Reset()
			}

			// Find closing bracket
			i++ // skip '['
			start := i
			for i < len(path) && path[i] != ']' {
				i++
			}
			if i >= len(path) {
				// Malformed path - no closing bracket
				return nil
			}

			// Parse index
			indexStr := path[start:i]
			index, err := strconv.Atoi(indexStr)
			if err != nil {
				// Invalid index
				return nil
			}

			segments = append(segments, pathSegment{
				segType: segmentTypeIndex,
				index:   index,
			})
			i++ // skip ']'

		default:
			// Regular character - add to current field
			currentField.WriteByte(ch)
			i++
		}
	}

	// Flush remaining field
	if currentField.Len() > 0 {
		segments = append(segments, pathSegment{
			segType: segmentTypeField,
			value:   currentField.String(),
		})
	}

	return segments
}
