// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package postgres provides a PostgreSQL/CockroachDB implementation of the db.DB interface.
package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver (also works with CockroachDB)
)

const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 5 * time.Minute
	defaultConnMaxIdleTime = 1 * time.Minute
)

// Config holds PostgreSQL connection configuration
type Config struct {
	// DSN is the data source name (e.g., "postgres://user:pass@host:port/database?sslmode=disable")
	DSN string

	// Driver is the specific driver type (postgres or cockroachdb)
	Driver db.Driver

	// Connection pool settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(dsn string, driver db.Driver) Config {
	return Config{
		DSN:             dsn,
		Driver:          driver,
		MaxOpenConns:    defaultMaxOpenConns,
		MaxIdleConns:    defaultMaxIdleConns,
		ConnMaxLifetime: defaultConnMaxLifetime,
		ConnMaxIdleTime: defaultConnMaxIdleTime,
	}
}

// storageClass returns the storage class, defaulting to STANDARD if empty
func storageClass(sc string) string {
	if sc == "" {
		return "STANDARD"
	}
	return sc
}

// Postgres implements db.DB using PostgreSQL/CockroachDB as the backing store
type Postgres struct {
	db     *sql.DB
	config Config
}

// NewPostgres creates a new PostgreSQL-backed database
func NewPostgres(cfg Config) (db.DB, error) {
	sqlDB, err := sql.Open("pgx", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Configure connection pool
	if cfg.MaxOpenConns > 0 {
		sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	} else {
		sqlDB.SetMaxOpenConns(defaultMaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	} else {
		sqlDB.SetMaxIdleConns(defaultMaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	} else {
		sqlDB.SetConnMaxLifetime(defaultConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	} else {
		sqlDB.SetConnMaxIdleTime(defaultConnMaxIdleTime)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Postgres{
		db:     sqlDB,
		config: cfg,
	}, nil
}

// Close closes the database connection
func (p *Postgres) Close() error {
	return p.db.Close()
}

// SqlDB returns the underlying *sql.DB for use with taskqueue.DBQueue
func (p *Postgres) SqlDB() *sql.DB {
	return p.db
}

// WithTx executes fn within a database transaction.
func (p *Postgres) WithTx(ctx context.Context, fn func(tx db.TxStore) error) error {
	sqlTx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	txStore := &postgresTx{tx: sqlTx}

	if err := fn(txStore); err != nil {
		if rbErr := sqlTx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed: %v (original error: %w)", rbErr, err)
		}
		return err
	}

	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}
	return nil
}

// Ensure Postgres implements db.DB
var _ db.DB = (*Postgres)(nil)
