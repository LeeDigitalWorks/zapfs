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
			expected: false, // case sensitive check for MySQL Deadlock
		},
		// PostgreSQL deadlock error cases
		{
			name:     "postgres deadlock error code 40P01",
			err:      errors.New("ERROR: deadlock detected (SQLSTATE 40P01)"),
			expected: true,
		},
		{
			name:     "postgres deadlock detected message",
			err:      errors.New("pq: deadlock detected"),
			expected: true,
		},
		{
			name:     "postgres wrapped deadlock",
			err:      errors.New("database error: 40P01 deadlock detected"),
			expected: true,
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

func TestRebind(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		driver   Driver
		query    string
		expected string
	}{
		{
			name:     "mysql driver - no change",
			driver:   DriverMySQL,
			query:    "SELECT * FROM tasks WHERE id = ? AND status = ?",
			expected: "SELECT * FROM tasks WHERE id = ? AND status = ?",
		},
		{
			name:     "postgres driver - simple query",
			driver:   DriverPostgres,
			query:    "SELECT * FROM tasks WHERE id = ?",
			expected: "SELECT * FROM tasks WHERE id = $1",
		},
		{
			name:     "postgres driver - multiple placeholders",
			driver:   DriverPostgres,
			query:    "INSERT INTO tasks (id, type, status) VALUES (?, ?, ?)",
			expected: "INSERT INTO tasks (id, type, status) VALUES ($1, $2, $3)",
		},
		{
			name:     "postgres driver - complex query",
			driver:   DriverPostgres,
			query:    "UPDATE tasks SET status = ?, updated_at = ? WHERE id = ? AND worker_id = ?",
			expected: "UPDATE tasks SET status = $1, updated_at = $2 WHERE id = $3 AND worker_id = $4",
		},
		{
			name:     "postgres driver - no placeholders",
			driver:   DriverPostgres,
			query:    "SELECT * FROM tasks",
			expected: "SELECT * FROM tasks",
		},
		{
			name:     "empty driver defaults to mysql (no change)",
			driver:   "",
			query:    "SELECT * FROM tasks WHERE id = ?",
			expected: "SELECT * FROM tasks WHERE id = ?",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := &DBQueue{driver: tt.driver}
			result := q.rebind(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDriverConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, Driver("mysql"), DriverMySQL)
	assert.Equal(t, Driver("postgres"), DriverPostgres)
}
