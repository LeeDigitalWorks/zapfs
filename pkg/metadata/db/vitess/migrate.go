// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
)

// Migrate runs database migrations for Vitess/MySQL
func (v *Vitess) Migrate(ctx context.Context) error {
	// Ensure schema_migrations table exists
	_, err := v.Store.DB().ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	// Use the migrations framework with MySQL driver
	return db.RunMigrations(ctx, &vitessMigrator{db: v.Store.DB()}, db.DriverMySQL)
}

// vitessMigrator implements db.Migrator for Vitess
type vitessMigrator struct {
	db *sql.DB
}

func (m *vitessMigrator) CurrentVersion(ctx context.Context) (int, error) {
	var version int
	err := m.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version), 0) FROM schema_migrations
	`).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("get current version: %w", err)
	}
	return version, nil
}

func (m *vitessMigrator) Apply(ctx context.Context, migration db.Migration) error {
	// Split migration SQL into individual statements (MySQL driver doesn't support multi-statement by default)
	statements := splitSQLStatements(migration.SQL)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := m.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration SQL: %w", err)
		}
	}
	return nil
}

func (m *vitessMigrator) SetVersion(ctx context.Context, version int) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO schema_migrations (version) VALUES (?)
	`, version)
	if err != nil {
		return fmt.Errorf("record migration version: %w", err)
	}
	return nil
}

// splitSQLStatements splits SQL content into individual statements
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip empty lines and comments
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")

		// If line ends with semicolon, it's end of statement
		if strings.HasSuffix(trimmed, ";") {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	// Handle any remaining content
	if current.Len() > 0 {
		statements = append(statements, current.String())
	}

	return statements
}
