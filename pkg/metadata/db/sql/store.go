// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
)

// Config holds SQL database connection configuration.
// This is shared between PostgreSQL and MySQL/Vitess.
type Config struct {
	// DSN is the data source name
	DSN string

	// Driver specifies the database driver (postgres, cockroachdb, mysql, vitess)
	Driver db.Driver

	// Connection pool settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig(dsn string, driver db.Driver) Config {
	return Config{
		DSN:             dsn,
		Driver:          driver,
		MaxOpenConns:    db.DefaultMaxOpenConns,
		MaxIdleConns:    db.DefaultMaxIdleConns,
		ConnMaxLifetime: time.Duration(db.DefaultConnMaxLifetime) * time.Second,
		ConnMaxIdleTime: time.Duration(db.DefaultConnMaxIdleTime) * time.Second,
	}
}

// Store is a dialect-aware SQL database implementation.
// It provides the shared implementation for both PostgreSQL and MySQL/Vitess.
type Store struct {
	db      *sql.DB
	dialect Dialect
	config  Config
}

// NewStore creates a new SQL store with the given dialect.
func NewStore(sqlDB *sql.DB, dialect Dialect, config Config) *Store {
	return &Store{
		db:      sqlDB,
		dialect: dialect,
		config:  config,
	}
}

// Open opens a database connection and returns a configured Store.
func Open(driverName string, dialect Dialect, cfg Config) (*Store, error) {
	sqlDB, err := sql.Open(driverName, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Configure connection pool
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	} else {
		sqlDB.SetMaxOpenConns(db.DefaultMaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	} else {
		sqlDB.SetMaxIdleConns(db.DefaultMaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	} else {
		sqlDB.SetConnMaxLifetime(time.Duration(db.DefaultConnMaxLifetime) * time.Second)
	}
	if cfg.ConnMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	} else {
		sqlDB.SetConnMaxIdleTime(time.Duration(db.DefaultConnMaxIdleTime) * time.Second)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return NewStore(sqlDB, dialect, cfg), nil
}

// DB returns the underlying *sql.DB for direct access if needed.
func (s *Store) DB() *sql.DB {
	return s.db
}

// Dialect returns the dialect used by this store.
func (s *Store) Dialect() Dialect {
	return s.dialect
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// ============================================================================
// Query Helpers
// ============================================================================

// Query executes a query with dialect-aware placeholder conversion.
// Write queries using PostgreSQL-style placeholders ($1, $2, ...) and
// they will be automatically converted to the dialect's format.
func (s *Store) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, s.dialect.ReplacePlaceholders(query), args...)
}

// QueryRow executes a query that returns a single row.
func (s *Store) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, s.dialect.ReplacePlaceholders(query), args...)
}

// Exec executes a query that doesn't return rows.
func (s *Store) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.dialect.ReplacePlaceholders(query), args...)
}

// ============================================================================
// Helper Functions
// ============================================================================

// scanner is an interface for sql.Row and sql.Rows.
type scanner interface {
	Scan(dest ...any) error
}

// Querier is the interface for executing SQL queries.
// Both Store and TxStore implement this interface, allowing shared query logic.
type Querier interface {
	Query(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRow(ctx context.Context, query string, args ...any) *sql.Row
	Exec(ctx context.Context, query string, args ...any) (sql.Result, error)
	Dialect() Dialect
}

// ============================================================================
// Transaction Support
// ============================================================================

// TxStore wraps a database transaction with dialect-aware query helpers.
// Use this in transaction implementations to get automatic placeholder conversion.
type TxStore struct {
	tx      *sql.Tx
	dialect Dialect
}

// NewTxStore creates a new transaction store wrapper.
func NewTxStore(tx *sql.Tx, dialect Dialect) *TxStore {
	return &TxStore{
		tx:      tx,
		dialect: dialect,
	}
}

// Tx returns the underlying *sql.Tx for direct access if needed.
func (t *TxStore) Tx() *sql.Tx {
	return t.tx
}

// Dialect returns the dialect used by this transaction.
func (t *TxStore) Dialect() Dialect {
	return t.dialect
}

// Query executes a query with dialect-aware placeholder conversion.
func (t *TxStore) Query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, t.dialect.ReplacePlaceholders(query), args...)
}

// QueryRow executes a query that returns a single row.
func (t *TxStore) QueryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return t.tx.QueryRowContext(ctx, t.dialect.ReplacePlaceholders(query), args...)
}

// Exec executes a query that doesn't return rows.
func (t *TxStore) Exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.tx.ExecContext(ctx, t.dialect.ReplacePlaceholders(query), args...)
}

// BoolValue returns the dialect-specific representation of a boolean value.
// For PostgreSQL this returns true/false, for MySQL this returns 1/0.
func (t *TxStore) BoolValue(b bool) any {
	if _, isMySQL := t.dialect.(MySQLDialect); isMySQL {
		if b {
			return 1
		}
		return 0
	}
	return b
}

// ============================================================================
// Shared Helper Functions
// ============================================================================

// StorageClass returns the storage class, defaulting to STANDARD if empty.
func StorageClass(sc string) string {
	if sc == "" {
		return "STANDARD"
	}
	return sc
}

// ContentType returns the content type, defaulting to application/octet-stream if empty.
func ContentType(ct string) string {
	if ct == "" {
		return "application/octet-stream"
	}
	return ct
}
