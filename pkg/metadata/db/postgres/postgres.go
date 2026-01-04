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
	dbsql "github.com/LeeDigitalWorks/zapfs/pkg/metadata/db/sql"

	_ "github.com/jackc/pgx/v5/stdlib" // PostgreSQL driver (also works with CockroachDB)
)

// Use shared defaults from db package for consistency across drivers

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
		MaxOpenConns:    db.DefaultMaxOpenConns,
		MaxIdleConns:    db.DefaultMaxIdleConns,
		ConnMaxLifetime: time.Duration(db.DefaultConnMaxLifetime) * time.Second,
		ConnMaxIdleTime: time.Duration(db.DefaultConnMaxIdleTime) * time.Second,
	}
}

// Postgres implements db.DB using PostgreSQL/CockroachDB as the backing store
type Postgres struct {
	*dbsql.Store // Embedded for shared object operations
	config       Config
}

// NewPostgres creates a new PostgreSQL-backed database
func NewPostgres(cfg Config) (db.DB, error) {
	// Convert to shared sql.Config
	sqlCfg := dbsql.Config{
		DSN:             cfg.DSN,
		Driver:          cfg.Driver,
		MaxOpenConns:    cfg.MaxOpenConns,
		MaxIdleConns:    cfg.MaxIdleConns,
		ConnMaxLifetime: cfg.ConnMaxLifetime,
		ConnMaxIdleTime: cfg.ConnMaxIdleTime,
	}

	// Use shared Store.Open with PostgreSQL dialect
	store, err := dbsql.Open("pgx", dbsql.PostgresDialect{}, sqlCfg)
	if err != nil {
		return nil, err
	}

	return &Postgres{
		Store:  store,
		config: cfg,
	}, nil
}

// Close closes the database connection
func (p *Postgres) Close() error {
	return p.Store.Close()
}

// SqlDB returns the underlying *sql.DB for use with taskqueue.DBQueue
func (p *Postgres) SqlDB() *sql.DB {
	return p.Store.DB()
}

// WithTx executes fn within a database transaction.
func (p *Postgres) WithTx(ctx context.Context, fn func(tx db.TxStore) error) error {
	sqlTx, err := p.Store.DB().BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	txStore := dbsql.NewTxStore(sqlTx, dbsql.PostgresDialect{})

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
