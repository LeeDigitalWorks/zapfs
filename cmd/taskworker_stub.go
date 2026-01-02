//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"database/sql"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/manager"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
)

// TaskWorkerConfig configures the task worker.
type TaskWorkerConfig struct {
	DB           *sql.DB
	WorkerID     string
	PollInterval time.Duration
	Concurrency  int
	LocalRegion  string

	// Dependencies for replication handler (enterprise only)
	ObjectService object.Service
	RegionConfig  *manager.RegionConfig
	Credentials   ReplicationCredentials
}

// ReplicationCredentials provides credentials for cross-region replication.
type ReplicationCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// TaskWorkerManager is a stub for community edition.
type TaskWorkerManager struct{}

// InitializeTaskWorker returns nil in community edition.
func InitializeTaskWorker(ctx context.Context, cfg TaskWorkerConfig) (*TaskWorkerManager, error) {
	logger.Debug().Msg("task worker not available in community edition")
	return nil, nil
}

// Queue returns nil in community edition.
func (m *TaskWorkerManager) Queue() any {
	return nil
}

// Stop is a no-op in community edition.
func (m *TaskWorkerManager) Stop() {}
