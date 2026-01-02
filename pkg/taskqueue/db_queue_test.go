// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package taskqueue

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIsDeadlockError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "regular error",
			err:      errors.New("some random error"),
			expected: false,
		},
		{
			name:     "mysql deadlock error code 1213",
			err:      errors.New("Error 1213 (40001): Deadlock found when trying to get lock; try restarting transaction"),
			expected: true,
		},
		{
			name:     "deadlock in error message",
			err:      errors.New("Deadlock detected"),
			expected: true,
		},
		{
			name:     "wrapped deadlock error",
			err:      errors.New("database error: Error 1213 (40001): Deadlock found"),
			expected: true,
		},
		{
			name:     "connection error",
			err:      errors.New("connection refused"),
			expected: false,
		},
		{
			name:     "lock timeout (not deadlock)",
			err:      errors.New("Error 1205 (HY000): Lock wait timeout exceeded"),
			expected: false,
		},
		{
			name:     "partial match Error 1213 (contains substring)",
			err:      errors.New("Error 12130"),
			expected: true, // Contains "Error 1213" substring - acceptable false positive
		},
		{
			name:     "case sensitivity - lowercase deadlock",
			err:      errors.New("deadlock found"),
			expected: false, // case sensitive check
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isDeadlockError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDeadlockRetryConstants(t *testing.T) {
	t.Parallel()

	// Verify constants are set to reasonable values
	assert.Equal(t, 3, maxDeadlockRetries, "should retry up to 3 times")
	assert.LessOrEqual(t, baseDeadlockBackoff.Milliseconds(), int64(50),
		"base backoff should be <= 50ms to avoid slow tests")
}
