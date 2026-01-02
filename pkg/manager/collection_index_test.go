// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// ============================================================================
// insertSorted Tests
// ============================================================================

func TestInsertSorted_Empty(t *testing.T) {
	t.Parallel()

	var slice []string
	insertSorted(&slice, "apple")

	assert.Equal(t, []string{"apple"}, slice)
}

func TestInsertSorted_Beginning(t *testing.T) {
	t.Parallel()

	slice := []string{"banana", "cherry"}
	insertSorted(&slice, "apple")

	assert.Equal(t, []string{"apple", "banana", "cherry"}, slice)
}

func TestInsertSorted_Middle(t *testing.T) {
	t.Parallel()

	slice := []string{"apple", "cherry"}
	insertSorted(&slice, "banana")

	assert.Equal(t, []string{"apple", "banana", "cherry"}, slice)
}

func TestInsertSorted_End(t *testing.T) {
	t.Parallel()

	slice := []string{"apple", "banana"}
	insertSorted(&slice, "cherry")

	assert.Equal(t, []string{"apple", "banana", "cherry"}, slice)
}

func TestInsertSorted_Duplicate(t *testing.T) {
	t.Parallel()

	slice := []string{"apple", "cherry"}
	insertSorted(&slice, "apple") // Duplicate

	// Duplicates get inserted (maintaining sort order)
	assert.Equal(t, []string{"apple", "apple", "cherry"}, slice)
}

func TestInsertSorted_ManyElements(t *testing.T) {
	t.Parallel()

	var slice []string
	insertSorted(&slice, "delta")
	insertSorted(&slice, "alpha")
	insertSorted(&slice, "charlie")
	insertSorted(&slice, "bravo")
	insertSorted(&slice, "echo")

	assert.Equal(t, []string{"alpha", "bravo", "charlie", "delta", "echo"}, slice)
}

// ============================================================================
// removeFromSlice Tests
// ============================================================================

func TestRemoveFromSlice_Exists(t *testing.T) {
	t.Parallel()

	slice := []string{"apple", "banana", "cherry"}
	result := removeFromSlice(slice, "banana")

	assert.Equal(t, []string{"apple", "cherry"}, result)
}

func TestRemoveFromSlice_First(t *testing.T) {
	t.Parallel()

	slice := []string{"apple", "banana", "cherry"}
	result := removeFromSlice(slice, "apple")

	assert.Equal(t, []string{"banana", "cherry"}, result)
}

func TestRemoveFromSlice_Last(t *testing.T) {
	t.Parallel()

	slice := []string{"apple", "banana", "cherry"}
	result := removeFromSlice(slice, "cherry")

	assert.Equal(t, []string{"apple", "banana"}, result)
}

func TestRemoveFromSlice_NotFound(t *testing.T) {
	t.Parallel()

	slice := []string{"apple", "banana", "cherry"}
	result := removeFromSlice(slice, "orange")

	assert.Equal(t, []string{"apple", "banana", "cherry"}, result)
}

func TestRemoveFromSlice_Empty(t *testing.T) {
	t.Parallel()

	var slice []string
	result := removeFromSlice(slice, "apple")

	assert.Empty(t, result)
}

func TestRemoveFromSlice_SingleElement(t *testing.T) {
	t.Parallel()

	slice := []string{"apple"}
	result := removeFromSlice(slice, "apple")

	assert.Empty(t, result)
}

func TestRemoveFromSlice_OnlyRemovesFirst(t *testing.T) {
	t.Parallel()

	// If there are duplicates, only first should be removed
	slice := []string{"apple", "banana", "apple"}
	result := removeFromSlice(slice, "apple")

	assert.Equal(t, []string{"banana", "apple"}, result)
}
