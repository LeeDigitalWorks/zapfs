//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package cmd

import (
	"context"
	"database/sql"
	"encoding/json"
	"net/http"
	"time"

	"github.com/LeeDigitalWorks/zapfs/enterprise/events"
	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	enttaskqueue "github.com/LeeDigitalWorks/zapfs/enterprise/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/debug"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/manager"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// TaskWorkerConfig configures the enterprise task worker.
type TaskWorkerConfig struct {
	DB           *sql.DB
	WorkerID     string
	PollInterval time.Duration
	Concurrency  int
	LocalRegion  string

	// Dependencies for replication handler
	ObjectService object.Service         // For reading objects from local storage
	RegionConfig  *manager.RegionConfig  // For getting S3 endpoints per region
	Credentials   ReplicationCredentials // For authenticating to remote regions

	// Dependencies for event notification handler
	NotificationStore events.NotificationStore // For reading bucket notification configs
	EventPublishers   EventPublisherConfig     // Publisher configuration
}

// ReplicationCredentials provides credentials for cross-region replication.
type ReplicationCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// EventPublisherConfig configures event notification publishers.
type EventPublisherConfig struct {
	// Redis publisher configuration
	RedisAddr     string // Redis address (e.g., "localhost:6379")
	RedisPassword string
	RedisDB       int
	RedisChannel  string // Channel prefix (default: "s3:events")

	// Kafka publisher configuration
	KafkaEnabled      bool
	KafkaBrokers      []string // Kafka broker addresses
	KafkaTopic        string   // Topic name (default: "s3-events")
	KafkaRequiredAcks int      // 0=none, 1=leader, -1=all (default: 1)
	KafkaCompression  string   // "none", "gzip", "snappy", "lz4", "zstd" (default: "snappy")
}

// TaskWorkerManager wraps the task queue and worker for enterprise features.
type TaskWorkerManager struct {
	queue      *taskqueue.DBQueue
	worker     *taskqueue.Worker
	publishers []events.Publisher
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

	// Create dependencies for enterprise handlers
	deps := enttaskqueue.Dependencies{}

	// Configure object reader if object service is available
	if cfg.ObjectService != nil {
		deps.ObjectReader = enttaskqueue.NewObjectServiceAdapter(cfg.ObjectService)
	}

	// Configure region endpoints if region config is available
	if cfg.RegionConfig != nil {
		deps.RegionEndpoints = enttaskqueue.NewRegionConfigAdapter(cfg.RegionConfig)
	}

	// Configure replication credentials
	if cfg.Credentials.AccessKeyID != "" {
		deps.ReplicationCredentials = enttaskqueue.ReplicationCredentials{
			AccessKeyID:     cfg.Credentials.AccessKeyID,
			SecretAccessKey: cfg.Credentials.SecretAccessKey,
		}
	}

	// Register replication handlers
	handlers := enttaskqueue.EnterpriseHandlers(deps)
	for _, h := range handlers {
		worker.RegisterHandler(h)
		logger.Debug().Str("type", string(h.Type())).Msg("registered task handler")
	}

	// Initialize event publishers and handlers (requires FeatureEvents license)
	var publishers []events.Publisher
	if license.GetManager().CheckFeature(license.FeatureEvents) == nil {
		// Initialize Redis publisher if configured
		if cfg.EventPublishers.RedisAddr != "" {
			redisCfg := events.RedisConfig{
				Addr:     cfg.EventPublishers.RedisAddr,
				Password: cfg.EventPublishers.RedisPassword,
				DB:       cfg.EventPublishers.RedisDB,
				Channel:  cfg.EventPublishers.RedisChannel,
			}
			redisPub, err := events.NewRedisPublisher(redisCfg)
			if err != nil {
				logger.Warn().Err(err).Msg("failed to initialize Redis event publisher")
			} else {
				publishers = append(publishers, redisPub)
			}
		}

		// Initialize Kafka publisher if configured
		if cfg.EventPublishers.KafkaEnabled && len(cfg.EventPublishers.KafkaBrokers) > 0 {
			kafkaCfg := events.KafkaConfig{
				Brokers:      cfg.EventPublishers.KafkaBrokers,
				Topic:        cfg.EventPublishers.KafkaTopic,
				RequiredAcks: cfg.EventPublishers.KafkaRequiredAcks,
				Compression:  cfg.EventPublishers.KafkaCompression,
			}
			kafkaPub, err := events.NewKafkaPublisher(kafkaCfg)
			if err != nil {
				logger.Warn().Err(err).Msg("failed to initialize Kafka event publisher")
			} else {
				publishers = append(publishers, kafkaPub)
			}
		}

		// Create config store adapter if notification store is available
		var configStore events.ConfigStore
		if cfg.NotificationStore != nil {
			configStore = events.NewDBConfigStore(cfg.NotificationStore)
		}

		// Register event handlers
		eventHandlers := events.EnterpriseEventHandlers(publishers, configStore, cfg.LocalRegion)
		for _, h := range eventHandlers {
			worker.RegisterHandler(h)
			handlers = append(handlers, h)
			logger.Debug().Str("type", string(h.Type())).Msg("registered event handler")
		}

		if len(publishers) > 0 {
			logger.Info().
				Int("publishers", len(publishers)).
				Msg("event notification publishers initialized")
		}
	}

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
		Int("handlers", len(handlers)).
		Msg("enterprise task worker started")

	return &TaskWorkerManager{
		queue:      queue,
		worker:     worker,
		publishers: publishers,
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
	// Close event publishers
	for _, pub := range m.publishers {
		if err := pub.Close(); err != nil {
			logger.Warn().Err(err).Str("publisher", pub.Name()).Msg("failed to close event publisher")
		}
	}
	logger.Info().Msg("enterprise task worker stopped")
}
