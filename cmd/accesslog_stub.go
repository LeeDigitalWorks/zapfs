//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/api"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
)

// AccessLogConfig configures the access log collector.
type AccessLogConfig struct {
	Enabled        bool
	ClickHouseDSN  string
	BatchSize      int
	FlushInterval  time.Duration
	ExportInterval time.Duration
	DB             db.DB
}

// AccessLogManager is a stub for community edition.
type AccessLogManager struct{}

// InitializeAccessLog returns nil in community edition.
func InitializeAccessLog(ctx context.Context, cfg AccessLogConfig) (*AccessLogManager, error) {
	if cfg.Enabled {
		logger.Warn().Msg("access logging requires enterprise edition")
	}
	return nil, nil
}

// Collector returns nil in community edition.
func (m *AccessLogManager) Collector() api.AccessLogCollector {
	return nil
}

// Stop is a no-op in community edition.
func (m *AccessLogManager) Stop() {}
