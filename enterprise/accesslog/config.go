//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import "time"

// Config holds configuration for the access log system.
type Config struct {
	// Enabled controls whether access logging is active
	Enabled bool

	// ClickHouse connection settings
	DSN          string        // ClickHouse DSN (e.g., clickhouse://localhost:9000/zapfs)
	MaxOpenConns int           // Maximum open connections (default: 10)
	MaxIdleConns int           // Maximum idle connections (default: 5)
	DialTimeout  time.Duration // Connection timeout (default: 5s)
	ReadTimeout  time.Duration // Read timeout (default: 30s)
	WriteTimeout time.Duration // Write timeout (default: 30s)

	// Event batching settings
	BatchSize     int           // Events per batch (default: 10000)
	FlushInterval time.Duration // Max time between flushes (default: 5s)
	BufferSize    int           // Channel buffer size (default: BatchSize * 2)

	// S3 export settings
	ExportEnabled  bool          // Enable S3 bucket delivery
	ExportInterval time.Duration // Time between exports (default: 1h)
	ExportBatch    int           // Max logs per export file (default: 100000)

	// Retention
	RetentionDays int // Days to keep logs in ClickHouse (default: 90)
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		Enabled:        false,
		MaxOpenConns:   10,
		MaxIdleConns:   5,
		DialTimeout:    5 * time.Second,
		ReadTimeout:    30 * time.Second,
		WriteTimeout:   30 * time.Second,
		BatchSize:      10000,
		FlushInterval:  5 * time.Second,
		BufferSize:     20000,
		ExportEnabled:  true,
		ExportInterval: 1 * time.Hour,
		ExportBatch:    100000,
		RetentionDays:  90,
	}
}

// Validate checks the configuration and sets defaults for zero values.
func (c *Config) Validate() {
	if c.MaxOpenConns <= 0 {
		c.MaxOpenConns = 10
	}
	if c.MaxIdleConns <= 0 {
		c.MaxIdleConns = 5
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 5 * time.Second
	}
	if c.ReadTimeout <= 0 {
		c.ReadTimeout = 30 * time.Second
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 30 * time.Second
	}
	if c.BatchSize <= 0 {
		c.BatchSize = 10000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 5 * time.Second
	}
	if c.BufferSize <= 0 {
		c.BufferSize = c.BatchSize * 2
	}
	if c.ExportInterval <= 0 {
		c.ExportInterval = 1 * time.Hour
	}
	if c.ExportBatch <= 0 {
		c.ExportBatch = 100000
	}
	if c.RetentionDays <= 0 {
		c.RetentionDays = 90
	}
}
