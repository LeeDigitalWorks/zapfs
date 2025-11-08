//go:build enterprise

package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"zapfs/enterprise/license"
	enttaskqueue "zapfs/enterprise/taskqueue"
	"zapfs/pkg/debug"
	"zapfs/pkg/logger"
	"zapfs/pkg/taskqueue"
)

// TaskWorkerConfig configures the enterprise task worker.
type TaskWorkerConfig struct {
	DB           *sql.DB
	WorkerID     string
	PollInterval time.Duration
	Concurrency  int
	LocalRegion  string
}

// TaskWorkerManager wraps the task queue and worker for enterprise features.
type TaskWorkerManager struct {
	queue  *taskqueue.DBQueue
	worker *taskqueue.Worker
}

// InitializeTaskWorker creates and starts the task worker if licensed.
func InitializeTaskWorker(ctx context.Context, cfg TaskWorkerConfig) (*TaskWorkerManager, error) {
	// Check license
	mgr := license.GetManager()
	if mgr == nil {
		logger.Debug().Msg("task worker not started: no license manager")
		return nil, nil
	}
	if err := mgr.CheckFeature(license.FeatureMultiRegion); err != nil {
		logger.Debug().Msg("task worker not started: multi-region not licensed")
		return nil, nil
	}

	if cfg.DB == nil {
		logger.Warn().Msg("task worker not started: no database connection")
		return nil, nil
	}

	// Create DB queue
	queue, err := taskqueue.NewDBQueue(taskqueue.DBQueueConfig{
		DB:        cfg.DB,
		TableName: "tasks",
	})
	if err != nil {
		return nil, err
	}

	// Create worker
	if cfg.PollInterval == 0 {
		cfg.PollInterval = time.Second
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 5
	}

	worker := taskqueue.NewWorker(taskqueue.WorkerConfig{
		ID:           cfg.WorkerID,
		Queue:        queue,
		PollInterval: cfg.PollInterval,
		Concurrency:  cfg.Concurrency,
	})

	// Register handlers
	worker.RegisterHandler(enttaskqueue.NewReplicationHandler())
	// Future: Add more handlers
	// worker.RegisterHandler(NewAuditLogHandler())
	// worker.RegisterHandler(NewWebhookHandler())

	// Start worker
	worker.Start(ctx)

	// Register debug endpoints
	debug.RegisterHandlerFunc("/debug/tasks/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		stats, err := queue.Stats(r.Context())
		if err != nil {
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(stats)
	})

	debug.RegisterHandlerFunc("/debug/tasks/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		filter := taskqueue.TaskFilter{
			Status: taskqueue.TaskStatus(r.URL.Query().Get("status")),
			Type:   taskqueue.TaskType(r.URL.Query().Get("type")),
			Limit:  100,
		}
		tasks, err := queue.List(r.Context(), filter)
		if err != nil {
			json.NewEncoder(w).Encode(map[string]string{"error": err.Error()})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"count": len(tasks),
			"tasks": tasks,
		})
	})

	logger.Info().
		Str("worker_id", cfg.WorkerID).
		Int("concurrency", cfg.Concurrency).
		Dur("poll_interval", cfg.PollInterval).
		Msg("enterprise task worker started")

	return &TaskWorkerManager{
		queue:  queue,
		worker: worker,
	}, nil
}

// Queue returns the task queue for enqueuing tasks.
func (m *TaskWorkerManager) Queue() taskqueue.Queue {
	return m.queue
}

// Stop shuts down the worker gracefully.
func (m *TaskWorkerManager) Stop() {
	if m.worker != nil {
		m.worker.Stop()
	}
	if m.queue != nil {
		m.queue.Close()
	}
	logger.Info().Msg("enterprise task worker stopped")
}
