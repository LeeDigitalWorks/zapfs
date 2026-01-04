// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
)

// Migrate runs database migrations for PostgreSQL/CockroachDB
func (p *Postgres) Migrate(ctx context.Context) error {
	// Ensure schema_migrations table exists
	_, err := p.Store.DB().ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	// Use the migrations framework with the appropriate driver
	driver := p.config.Driver
	if driver == "" {
		driver = db.DriverPostgres
	}
	return db.RunMigrations(ctx, &postgresMigrator{db: p.Store.DB()}, driver)
}

// postgresMigrator implements db.Migrator for PostgreSQL
type postgresMigrator struct {
	db *sql.DB
}

func (m *postgresMigrator) CurrentVersion(ctx context.Context) (int, error) {
	var version int
	err := m.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version), 0) FROM schema_migrations
	`).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("get current version: %w", err)
	}
	return version, nil
}

func (m *postgresMigrator) Apply(ctx context.Context, migration db.Migration) error {
	// Split migration SQL into individual statements
	statements := splitSQLStatements(migration.SQL)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" {
			continue
		}
		// Strip leading comment lines from statement
		stmt = stripLeadingComments(stmt)
		if stmt == "" {
			continue
		}
		if _, err := m.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute statement: %w", err)
		}
	}
	return nil
}

// stripLeadingComments removes leading SQL comment lines from a statement.
func stripLeadingComments(stmt string) string {
	lines := strings.Split(stmt, "\n")
	for len(lines) > 0 {
		line := strings.TrimSpace(lines[0])
		if line == "" || strings.HasPrefix(line, "--") {
			lines = lines[1:]
			continue
		}
		break
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func (m *postgresMigrator) SetVersion(ctx context.Context, version int) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO schema_migrations (version) VALUES ($1)
	`, version)
	if err != nil {
		return fmt.Errorf("record migration version: %w", err)
	}
	return nil
}

// splitSQLStatements splits a SQL script into individual statements.
// It handles semicolons inside strings and comments.
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder
	inString := false
	stringChar := byte(0)
	inLineComment := false
	inBlockComment := false

	for i := 0; i < len(sql); i++ {
		c := sql[i]

		// Handle line comments
		if !inString && !inBlockComment && i+1 < len(sql) && c == '-' && sql[i+1] == '-' {
			inLineComment = true
			current.WriteByte(c)
			continue
		}

		if inLineComment {
			current.WriteByte(c)
			if c == '\n' {
				inLineComment = false
			}
			continue
		}

		// Handle block comments
		if !inString && !inLineComment && i+1 < len(sql) && c == '/' && sql[i+1] == '*' {
			inBlockComment = true
			current.WriteByte(c)
			continue
		}

		if inBlockComment {
			current.WriteByte(c)
			if c == '*' && i+1 < len(sql) && sql[i+1] == '/' {
				current.WriteByte(sql[i+1])
				i++
				inBlockComment = false
			}
			continue
		}

		// Handle strings
		if !inString && (c == '\'' || c == '"') {
			inString = true
			stringChar = c
			current.WriteByte(c)
			continue
		}

		if inString {
			current.WriteByte(c)
			if c == stringChar {
				// Check for escaped quote
				if i+1 < len(sql) && sql[i+1] == stringChar {
					current.WriteByte(sql[i+1])
					i++
					continue
				}
				inString = false
			}
			continue
		}

		// Handle semicolons
		if c == ';' {
			stmt := strings.TrimSpace(current.String())
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
			continue
		}

		current.WriteByte(c)
	}

	// Add any remaining statement
	if stmt := strings.TrimSpace(current.String()); stmt != "" {
		statements = append(statements, stmt)
	}

	return statements
}
