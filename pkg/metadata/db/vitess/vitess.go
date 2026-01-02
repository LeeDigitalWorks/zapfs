// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package vitess provides a Vitess/MySQL implementation of the db.DB interface.
package vitess

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"

	"github.com/go-sql-driver/mysql"
)

const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 5 * time.Minute
	defaultConnMaxIdleTime = 1 * time.Minute
)

// TLSMode specifies how TLS should be configured for MySQL connections
type TLSMode string

const (
	// TLSModeDisabled disables TLS
	TLSModeDisabled TLSMode = "disabled"
	// TLSModePreferred uses TLS if available (default MySQL driver behavior with tls=preferred)
	TLSModePreferred TLSMode = "preferred"
	// TLSModeRequired requires TLS but skips certificate verification
	TLSModeRequired TLSMode = "required"
	// TLSModeVerifyCA requires TLS and verifies the server certificate against a CA
	TLSModeVerifyCA TLSMode = "verify-ca"
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

	// TLS settings
	TLSMode  TLSMode // TLS mode: disabled, preferred, required, verify-ca
	TLSCAFile string  // Path to CA certificate file (for verify-ca mode)
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
	dsn := cfg.DSN

	// Configure TLS if specified
	if cfg.TLSMode != "" && cfg.TLSMode != TLSModeDisabled {
		var err error
		dsn, err = configureTLS(cfg)
		if err != nil {
			return nil, fmt.Errorf("configure TLS: %w", err)
		}
	}

	sqlDB, err := sql.Open("mysql", dsn)
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

// Stats returns database connection pool statistics
func (v *Vitess) Stats() sql.DBStats {
	return v.db.Stats()
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

// configureTLS sets up TLS for MySQL connections and returns the modified DSN
func configureTLS(cfg Config) (string, error) {
	dsn := cfg.DSN

	// Remove any existing tls parameter from DSN
	if idx := strings.Index(dsn, "tls="); idx != -1 {
		end := strings.Index(dsn[idx:], "&")
		if end == -1 {
			// tls is the last parameter
			if dsn[idx-1] == '?' {
				dsn = dsn[:idx-1]
			} else {
				dsn = dsn[:idx-1] // remove preceding &
			}
		} else {
			dsn = dsn[:idx] + dsn[idx+end+1:]
		}
	}

	// Add separator for DSN parameters
	if strings.Contains(dsn, "?") {
		if !strings.HasSuffix(dsn, "&") && !strings.HasSuffix(dsn, "?") {
			dsn += "&"
		}
	} else {
		dsn += "?"
	}

	switch cfg.TLSMode {
	case TLSModePreferred:
		dsn += "tls=preferred"

	case TLSModeRequired:
		// Use skip-verify for required mode (encrypts but doesn't verify cert)
		dsn += "tls=skip-verify"

	case TLSModeVerifyCA:
		// Register custom TLS config with CA verification
		tlsConfig := &tls.Config{
			MinVersion: tls.VersionTLS12,
		}

		if cfg.TLSCAFile != "" {
			caCert, err := os.ReadFile(cfg.TLSCAFile)
			if err != nil {
				return "", fmt.Errorf("read CA file: %w", err)
			}
			caCertPool := x509.NewCertPool()
			if !caCertPool.AppendCertsFromPEM(caCert) {
				return "", fmt.Errorf("failed to append CA certificate")
			}
			tlsConfig.RootCAs = caCertPool
		}

		// Register the TLS config with a unique name
		if err := mysql.RegisterTLSConfig("custom", tlsConfig); err != nil {
			return "", fmt.Errorf("register TLS config: %w", err)
		}
		dsn += "tls=custom"

	default:
		return "", fmt.Errorf("unknown TLS mode: %s", cfg.TLSMode)
	}

	return dsn, nil
}
