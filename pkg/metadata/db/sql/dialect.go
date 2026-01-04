// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package sql provides a dialect-aware SQL database implementation.
// It abstracts the differences between PostgreSQL and MySQL/Vitess,
// allowing a single implementation to support both databases.
package sql

import (
	"fmt"
	"strings"
)

// Dialect abstracts database-specific SQL syntax differences.
type Dialect interface {
	// Name returns the dialect name (e.g., "postgres", "mysql").
	Name() string

	// Placeholder returns the placeholder for the nth parameter (1-indexed).
	// PostgreSQL: "$1", "$2", "$3"
	// MySQL/Vitess: "?", "?", "?"
	Placeholder(n int) string

	// Placeholders returns n placeholders joined by comma.
	// PostgreSQL: "$1, $2, $3"
	// MySQL/Vitess: "?, ?, ?"
	Placeholders(n int) string

	// ReplacePlaceholders converts PostgreSQL-style placeholders ($1, $2, ...)
	// to the dialect's format. This allows writing queries with PostgreSQL
	// syntax and converting them at runtime.
	ReplacePlaceholders(query string) string

	// BoolLiteral returns the SQL literal for a boolean.
	// PostgreSQL: "TRUE" or "FALSE"
	// MySQL/Vitess: "1" or "0"
	BoolLiteral(b bool) string

	// BoolColumn returns how to reference a boolean column in WHERE clauses.
	// PostgreSQL: "column = TRUE" or just "column"
	// MySQL/Vitess: "column = 1"
	BoolColumn(column string, value bool) string

	// ScanBool returns a scanner that can read a boolean from a row.
	// PostgreSQL: directly scans to bool
	// MySQL/Vitess: scans to int, then converts
	ScanBool() BoolScanner

	// InsertIgnorePrefix returns the prefix for INSERT statements that should ignore duplicates.
	// PostgreSQL: "" (uses ON CONFLICT suffix instead)
	// MySQL/Vitess: "IGNORE "
	InsertIgnorePrefix() string

	// InsertIgnoreSuffix returns the suffix for INSERT statements that should ignore duplicates.
	// PostgreSQL: "ON CONFLICT (conflict_column) DO NOTHING"
	// MySQL/Vitess: "" (uses INSERT IGNORE prefix instead)
	InsertIgnoreSuffix(conflictColumn string) string

	// UpsertSuffix returns the suffix for INSERT statements that should update on conflict.
	// PostgreSQL: "ON CONFLICT (conflict_columns) DO UPDATE SET col1 = EXCLUDED.col1, ..."
	// MySQL/Vitess: "ON DUPLICATE KEY UPDATE col1 = VALUES(col1), ..."
	// The columns parameter specifies which columns to update on conflict.
	UpsertSuffix(conflictColumns string, updateColumns []string) string
}

// BoolScanner scans a boolean value from SQL.
type BoolScanner interface {
	// Dest returns the destination for Scan().
	Dest() any
	// Value returns the scanned boolean value.
	Value() bool
}

// ============================================================================
// PostgreSQL Dialect
// ============================================================================

// PostgresDialect implements Dialect for PostgreSQL.
type PostgresDialect struct{}

var _ Dialect = PostgresDialect{}

func (d PostgresDialect) Name() string {
	return "postgres"
}

func (d PostgresDialect) Placeholder(n int) string {
	return fmt.Sprintf("$%d", n)
}

func (d PostgresDialect) Placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	parts := make([]string, n)
	for i := 1; i <= n; i++ {
		parts[i-1] = fmt.Sprintf("$%d", i)
	}
	return strings.Join(parts, ", ")
}

func (d PostgresDialect) ReplacePlaceholders(query string) string {
	// PostgreSQL uses $1, $2, etc. - no conversion needed
	return query
}

func (d PostgresDialect) BoolLiteral(b bool) string {
	if b {
		return "TRUE"
	}
	return "FALSE"
}

func (d PostgresDialect) BoolColumn(column string, value bool) string {
	if value {
		return column + " = TRUE"
	}
	return column + " = FALSE"
}

func (d PostgresDialect) ScanBool() BoolScanner {
	return &directBoolScanner{}
}

func (d PostgresDialect) InsertIgnorePrefix() string {
	return ""
}

func (d PostgresDialect) InsertIgnoreSuffix(conflictColumn string) string {
	return fmt.Sprintf(" ON CONFLICT (%s) DO NOTHING", conflictColumn)
}

func (d PostgresDialect) UpsertSuffix(conflictColumns string, updateColumns []string) string {
	if len(updateColumns) == 0 {
		return ""
	}
	updates := make([]string, len(updateColumns))
	for i, col := range updateColumns {
		updates[i] = fmt.Sprintf("%s = EXCLUDED.%s", col, col)
	}
	return fmt.Sprintf(" ON CONFLICT (%s) DO UPDATE SET %s", conflictColumns, strings.Join(updates, ", "))
}

// ============================================================================
// MySQL/Vitess Dialect
// ============================================================================

// MySQLDialect implements Dialect for MySQL/Vitess.
type MySQLDialect struct{}

var _ Dialect = MySQLDialect{}

func (d MySQLDialect) Name() string {
	return "mysql"
}

func (d MySQLDialect) Placeholder(n int) string {
	return "?"
}

func (d MySQLDialect) Placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?, ", n), ", ")
}

func (d MySQLDialect) ReplacePlaceholders(query string) string {
	// Replace $1, $2, etc. with ?
	// Must replace ALL occurrences since the same placeholder can appear multiple times
	// (e.g., VALUES ($1, $2, $3, $3) for created_at and updated_at)
	//
	// IMPORTANT: Replace from highest to lowest to avoid $12 becoming ?2 when we replace $1 first.
	// Example: if we replace $1 first, "$12" becomes "?2" which is wrong.
	// By replacing from 50 down to 1, we ensure $12 is replaced before $1.
	result := query
	for i := 50; i >= 1; i-- {
		old := fmt.Sprintf("$%d", i)
		result = strings.ReplaceAll(result, old, "?")
	}
	return result
}

func (d MySQLDialect) BoolLiteral(b bool) string {
	if b {
		return "1"
	}
	return "0"
}

func (d MySQLDialect) BoolColumn(column string, value bool) string {
	if value {
		return column + " = 1"
	}
	return column + " = 0"
}

func (d MySQLDialect) ScanBool() BoolScanner {
	return &intBoolScanner{}
}

func (d MySQLDialect) InsertIgnorePrefix() string {
	return "IGNORE "
}

func (d MySQLDialect) InsertIgnoreSuffix(conflictColumn string) string {
	return ""
}

func (d MySQLDialect) UpsertSuffix(conflictColumns string, updateColumns []string) string {
	if len(updateColumns) == 0 {
		return ""
	}
	updates := make([]string, len(updateColumns))
	for i, col := range updateColumns {
		updates[i] = fmt.Sprintf("%s = VALUES(%s)", col, col)
	}
	return " ON DUPLICATE KEY UPDATE " + strings.Join(updates, ", ")
}

// ============================================================================
// Boolean Scanners
// ============================================================================

// directBoolScanner scans boolean directly (for PostgreSQL).
type directBoolScanner struct {
	value bool
}

func (s *directBoolScanner) Dest() any {
	return &s.value
}

func (s *directBoolScanner) Value() bool {
	return s.value
}

// intBoolScanner scans boolean as int (for MySQL/Vitess).
type intBoolScanner struct {
	value int
}

func (s *intBoolScanner) Dest() any {
	return &s.value
}

func (s *intBoolScanner) Value() bool {
	return s.value != 0
}
