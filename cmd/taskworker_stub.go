//go:build !enterprise

package cmd

import (
	"context"
	"database/sql"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
)

// TaskWorkerConfig configures the task worker.
type TaskWorkerConfig struct {
	DB           *sql.DB
	WorkerID     string
	PollInterval time.Duration
	Concurrency  int
	LocalRegion  string
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
