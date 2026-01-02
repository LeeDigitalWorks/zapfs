// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package vitess provides a Vitess/MySQL implementation of the db.DB interface.
package vitess

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"

	_ "github.com/go-sql-driver/mysql" // MySQL driver for Vitess
)

const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 5 * time.Minute
	defaultConnMaxIdleTime = 1 * time.Minute
)

// Config holds Vitess connection configuration
type Config struct {
	// DSN is the data source name (e.g., "user:pass@tcp(vtgate:3306)/keyspace")
	DSN string

	// Connection pool settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(dsn string) Config {
	return Config{
		DSN:             dsn,
		MaxOpenConns:    defaultMaxOpenConns,
		MaxIdleConns:    defaultMaxIdleConns,
		ConnMaxLifetime: defaultConnMaxLifetime,
		ConnMaxIdleTime: defaultConnMaxIdleTime,
	}
}

// Vitess implements db.DB using Vitess as the backing store
type Vitess struct {
	db     *sql.DB
	config Config
}

// NewVitess creates a new Vitess-backed database
func NewVitess(cfg Config) (db.DB, error) {
	sqlDB, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Configure connection pool
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Vitess{
		db:     sqlDB,
		config: cfg,
	}, nil
}

// Close closes the database connection
func (v *Vitess) Close() error {
	return v.db.Close()
}

// SqlDB returns the underlying *sql.DB for use with taskqueue.DBQueue
func (v *Vitess) SqlDB() *sql.DB {
	return v.db
}

// WithTx executes fn within a database transaction.
// If fn returns an error, the transaction is rolled back.
// If fn returns nil, the transaction is committed.
func (v *Vitess) WithTx(ctx context.Context, fn func(tx db.TxStore) error) error {
	sqlTx, err := v.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	txStore := &vitessTx{tx: sqlTx}

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

// Ensure Vitess implements db.DB
var _ db.DB = (*Vitess)(nil)
