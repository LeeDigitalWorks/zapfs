// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPostgresDialect_Placeholder(t *testing.T) {
	d := PostgresDialect{}

	assert.Equal(t, "$1", d.Placeholder(1))
	assert.Equal(t, "$2", d.Placeholder(2))
	assert.Equal(t, "$10", d.Placeholder(10))
}

func TestPostgresDialect_Placeholders(t *testing.T) {
	d := PostgresDialect{}

	assert.Equal(t, "", d.Placeholders(0))
	assert.Equal(t, "$1", d.Placeholders(1))
	assert.Equal(t, "$1, $2", d.Placeholders(2))
	assert.Equal(t, "$1, $2, $3", d.Placeholders(3))
}

func TestPostgresDialect_BoolLiteral(t *testing.T) {
	d := PostgresDialect{}

	assert.Equal(t, "TRUE", d.BoolLiteral(true))
	assert.Equal(t, "FALSE", d.BoolLiteral(false))
}

func TestPostgresDialect_BoolColumn(t *testing.T) {
	d := PostgresDialect{}

	assert.Equal(t, "is_latest = TRUE", d.BoolColumn("is_latest", true))
	assert.Equal(t, "is_latest = FALSE", d.BoolColumn("is_latest", false))
}

func TestPostgresDialect_ReplacePlaceholders(t *testing.T) {
	d := PostgresDialect{}

	// PostgreSQL dialect should not change placeholders
	query := "SELECT * FROM objects WHERE bucket = $1 AND key = $2"
	assert.Equal(t, query, d.ReplacePlaceholders(query))
}

func TestMySQLDialect_Placeholder(t *testing.T) {
	d := MySQLDialect{}

	assert.Equal(t, "?", d.Placeholder(1))
	assert.Equal(t, "?", d.Placeholder(2))
	assert.Equal(t, "?", d.Placeholder(10))
}

func TestMySQLDialect_Placeholders(t *testing.T) {
	d := MySQLDialect{}

	assert.Equal(t, "", d.Placeholders(0))
	assert.Equal(t, "?", d.Placeholders(1))
	assert.Equal(t, "?, ?", d.Placeholders(2))
	assert.Equal(t, "?, ?, ?", d.Placeholders(3))
}

func TestMySQLDialect_BoolLiteral(t *testing.T) {
	d := MySQLDialect{}

	assert.Equal(t, "1", d.BoolLiteral(true))
	assert.Equal(t, "0", d.BoolLiteral(false))
}

func TestMySQLDialect_BoolColumn(t *testing.T) {
	d := MySQLDialect{}

	assert.Equal(t, "is_latest = 1", d.BoolColumn("is_latest", true))
	assert.Equal(t, "is_latest = 0", d.BoolColumn("is_latest", false))
}

func TestMySQLDialect_ReplacePlaceholders(t *testing.T) {
	d := MySQLDialect{}

	tests := []struct {
		name     string
		query    string
		expected string
	}{
		{
			name:     "single placeholder",
			query:    "SELECT * FROM objects WHERE bucket = $1",
			expected: "SELECT * FROM objects WHERE bucket = ?",
		},
		{
			name:     "multiple placeholders",
			query:    "SELECT * FROM objects WHERE bucket = $1 AND key = $2",
			expected: "SELECT * FROM objects WHERE bucket = ? AND key = ?",
		},
		{
			name:     "many placeholders",
			query:    "INSERT INTO objects (a, b, c, d, e) VALUES ($1, $2, $3, $4, $5)",
			expected: "INSERT INTO objects (a, b, c, d, e) VALUES (?, ?, ?, ?, ?)",
		},
		{
			name:     "no placeholders",
			query:    "SELECT * FROM objects",
			expected: "SELECT * FROM objects",
		},
		{
			name:     "duplicate placeholder",
			query:    "INSERT INTO bucket_policies (bucket, policy_json, created_at, updated_at) VALUES ($1, $2, $3, $3)",
			expected: "INSERT INTO bucket_policies (bucket, policy_json, created_at, updated_at) VALUES (?, ?, ?, ?)",
		},
		{
			name:     "multiple duplicates",
			query:    "INSERT INTO t (a, b, c, d) VALUES ($1, $1, $2, $2)",
			expected: "INSERT INTO t (a, b, c, d) VALUES (?, ?, ?, ?)",
		},
		{
			name:     "double digit placeholders",
			query:    "INSERT INTO t (a, b, c, d, e, f, g, h, i, j, k, l) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)",
			expected: "INSERT INTO t (a, b, c, d, e, f, g, h, i, j, k, l) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
		},
		{
			name:     "high placeholder numbers with duplicates",
			query:    "INSERT INTO objects VALUES ($1, $2, $12, $12, $3)",
			expected: "INSERT INTO objects VALUES (?, ?, ?, ?, ?)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := d.ReplacePlaceholders(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestDirectBoolScanner(t *testing.T) {
	d := PostgresDialect{}
	scanner := d.ScanBool()

	// Get destination pointer
	dest := scanner.Dest()
	boolPtr, ok := dest.(*bool)
	assert.True(t, ok)

	// Simulate scanning true
	*boolPtr = true
	assert.True(t, scanner.Value())

	// Simulate scanning false
	*boolPtr = false
	assert.False(t, scanner.Value())
}

func TestIntBoolScanner(t *testing.T) {
	d := MySQLDialect{}
	scanner := d.ScanBool()

	// Get destination pointer
	dest := scanner.Dest()
	intPtr, ok := dest.(*int)
	assert.True(t, ok)

	// Simulate scanning 1 (true)
	*intPtr = 1
	assert.True(t, scanner.Value())

	// Simulate scanning 0 (false)
	*intPtr = 0
	assert.False(t, scanner.Value())

	// Simulate scanning other non-zero (true)
	*intPtr = 42
	assert.True(t, scanner.Value())
}
