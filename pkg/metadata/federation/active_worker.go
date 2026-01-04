// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package federation

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"golang.org/x/time/rate"
)

// Prometheus metrics for active migration worker
var (
	workerActiveTotal = promauto.NewGauge(
		prometheus.GaugeOpts{
			Name: "zapfs_federation_worker_active_total",
			Help: "Number of currently active federation workers",
		},
	)
	workerObjectsIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_worker_objects_ingested_total",
			Help: "Total number of objects ingested by workers",
		},
		[]string{"bucket"},
	)
	workerBytesIngestedTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_worker_bytes_ingested_total",
			Help: "Total bytes ingested by workers",
		},
		[]string{"bucket"},
	)
	workerIngestDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zapfs_federation_worker_ingest_duration_seconds",
			Help:    "Duration of worker ingest operations in seconds",
			Buckets: []float64{0.1, 0.5, 1, 2, 5, 10, 30, 60, 120},
		},
		[]string{"bucket"},
	)
	workerErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_federation_worker_errors_total",
			Help: "Total number of worker errors",
		},
		[]string{"bucket", "error_type"},
	)
)

// WorkerConfig holds configuration for the active migration worker pool.
type WorkerConfig struct {
	// Concurrency is the number of parallel workers
	Concurrency int

	// RateLimit is the max objects/sec to ingest (0 = unlimited)
	RateLimit int

	// BatchSize is the number of objects to process per worker iteration
	BatchSize int

	// PollInterval is how often to check for new work
	PollInterval time.Duration

	// ShutdownTimeout is how long to wait for graceful shutdown
	ShutdownTimeout time.Duration
}

// DefaultWorkerConfig returns sensible defaults for the worker pool.
func DefaultWorkerConfig() WorkerConfig {
	return WorkerConfig{
		Concurrency:     5,
		RateLimit:       100, // 100 objects/sec
		BatchSize:       10,
		PollInterval:    time.Second,
		ShutdownTimeout: 30 * time.Second,
	}
}

// FederationStore provides access to federation configurations.
type FederationStore interface {
	GetFederationConfig(ctx context.Context, bucket string) (*s3types.FederationConfig, error)
	GetFederatedBucketsNeedingSync(ctx context.Context, limit int) ([]*s3types.FederationConfig, error)
	UpdateMigrationProgress(ctx context.Context, bucket string, objectsSynced, bytesSynced int64, lastSyncKey string) error
}

// ObjectIngestor handles the actual ingestion of objects into local storage.
type ObjectIngestor interface {
	// IngestObject fetches an object from external S3 and stores it locally.
	// Returns the size of the ingested object and any error.
	IngestObject(ctx context.Context, bucket, key, versionID string, fedConfig *s3types.FederationConfig) (int64, error)
}

// ActiveWorkerPool manages a pool of workers for active migration.
// Workers fetch objects from external S3 and ingest them into local storage.
type ActiveWorkerPool struct {
	config      WorkerConfig
	clientPool  *ClientPool
	fedStore    FederationStore
	ingestor    ObjectIngestor
	rateLimiter *rate.Limiter

	// State
	running  atomic.Bool
	wg       sync.WaitGroup
	cancelFn context.CancelFunc

	// Progress tracking
	objectsIngested atomic.Int64
	bytesIngested   atomic.Int64
}

// NewActiveWorkerPool creates a new active migration worker pool.
func NewActiveWorkerPool(config WorkerConfig, clientPool *ClientPool, fedStore FederationStore, ingestor ObjectIngestor) *ActiveWorkerPool {
	if config.Concurrency < 1 {
		config.Concurrency = 1
	}
	if config.RateLimit < 0 {
		config.RateLimit = 0
	}
	if config.BatchSize < 1 {
		config.BatchSize = 1
	}
	if config.PollInterval < time.Millisecond {
		config.PollInterval = time.Second
	}

	var limiter *rate.Limiter
	if config.RateLimit > 0 {
		limiter = rate.NewLimiter(rate.Limit(config.RateLimit), config.RateLimit)
	}

	return &ActiveWorkerPool{
		config:      config,
		clientPool:  clientPool,
		fedStore:    fedStore,
		ingestor:    ingestor,
		rateLimiter: limiter,
	}
}

// Start starts the worker pool. This is non-blocking.
func (p *ActiveWorkerPool) Start(ctx context.Context) error {
	if !p.running.CompareAndSwap(false, true) {
		return fmt.Errorf("worker pool already running")
	}

	workerCtx, cancel := context.WithCancel(ctx)
	p.cancelFn = cancel

	for i := 0; i < p.config.Concurrency; i++ {
		p.wg.Add(1)
		go p.runWorker(workerCtx, i)
	}

	logger.Info().
		Int("concurrency", p.config.Concurrency).
		Int("rate_limit", p.config.RateLimit).
		Msg("federation worker pool started")

	return nil
}

// Stop gracefully stops the worker pool.
func (p *ActiveWorkerPool) Stop() error {
	if !p.running.CompareAndSwap(true, false) {
		return nil // Already stopped
	}

	logger.Info().Msg("stopping federation worker pool")

	if p.cancelFn != nil {
		p.cancelFn()
	}

	// Wait for workers to finish with timeout
	done := make(chan struct{})
	go func() {
		p.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		logger.Info().
			Int64("objects_ingested", p.objectsIngested.Load()).
			Int64("bytes_ingested", p.bytesIngested.Load()).
			Msg("federation worker pool stopped gracefully")
	case <-time.After(p.config.ShutdownTimeout):
		logger.Warn().Msg("federation worker pool shutdown timed out")
	}

	return nil
}

// IsRunning returns true if the worker pool is running.
func (p *ActiveWorkerPool) IsRunning() bool {
	return p.running.Load()
}

// Stats returns current worker pool statistics.
func (p *ActiveWorkerPool) Stats() (objectsIngested, bytesIngested int64) {
	return p.objectsIngested.Load(), p.bytesIngested.Load()
}

// runWorker is the main loop for a single worker.
func (p *ActiveWorkerPool) runWorker(ctx context.Context, workerID int) {
	defer p.wg.Done()
	workerActiveTotal.Inc()
	defer workerActiveTotal.Dec()

	logger.Debug().Int("worker_id", workerID).Msg("federation worker started")

	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			logger.Debug().Int("worker_id", workerID).Msg("federation worker stopped")
			return
		case <-ticker.C:
			if err := p.processBatch(ctx, workerID); err != nil {
				if ctx.Err() != nil {
					return // Context cancelled
				}
				logger.Error().
					Err(err).
					Int("worker_id", workerID).
					Msg("error processing migration batch")
			}
		}
	}
}

// processBatch processes a batch of federated buckets needing sync.
func (p *ActiveWorkerPool) processBatch(ctx context.Context, workerID int) error {
	// Get buckets needing sync
	configs, err := p.fedStore.GetFederatedBucketsNeedingSync(ctx, p.config.BatchSize)
	if err != nil {
		return fmt.Errorf("get buckets needing sync: %w", err)
	}

	if len(configs) == 0 {
		return nil // No work
	}

	for _, fedConfig := range configs {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Skip paused migrations
		if fedConfig.MigrationPaused {
			continue
		}

		// Rate limit
		if p.rateLimiter != nil {
			if err := p.rateLimiter.Wait(ctx); err != nil {
				return err
			}
		}

		// Process next object for this bucket
		if err := p.processNextObject(ctx, fedConfig); err != nil {
			workerErrorsTotal.WithLabelValues(fedConfig.Bucket, "ingest").Inc()
			logger.Warn().
				Err(err).
				Str("bucket", fedConfig.Bucket).
				Int("worker_id", workerID).
				Msg("error processing object for migration")
			// Continue to next bucket
		}
	}

	return nil
}

// processNextObject fetches and ingests the next object for a bucket.
func (p *ActiveWorkerPool) processNextObject(ctx context.Context, fedConfig *s3types.FederationConfig) error {
	startTime := time.Now()

	// Use discoverer to get next object
	extCfg := &ExternalS3Config{
		Endpoint:        fedConfig.Endpoint,
		Region:          fedConfig.Region,
		AccessKeyID:     fedConfig.AccessKeyID,
		SecretAccessKey: fedConfig.SecretAccessKey,
		Bucket:          fedConfig.ExternalBucket,
		PathStyle:       fedConfig.PathStyle,
	}

	// List one object starting after the last sync key
	output, err := p.clientPool.ListObjects(ctx, extCfg, "", fedConfig.LastSyncKey, 1)
	if err != nil {
		return fmt.Errorf("list next object: %w", err)
	}

	if len(output.Contents) == 0 {
		// No more objects to sync
		return nil
	}

	obj := output.Contents[0]
	key := *obj.Key

	// Ingest the object
	bytesIngested, err := p.ingestor.IngestObject(ctx, fedConfig.Bucket, key, "", fedConfig)
	if err != nil {
		return fmt.Errorf("ingest object %s: %w", key, err)
	}

	// Update progress
	p.objectsIngested.Add(1)
	p.bytesIngested.Add(bytesIngested)

	// Update metrics
	workerObjectsIngestedTotal.WithLabelValues(fedConfig.Bucket).Inc()
	workerBytesIngestedTotal.WithLabelValues(fedConfig.Bucket).Add(float64(bytesIngested))
	workerIngestDuration.WithLabelValues(fedConfig.Bucket).Observe(time.Since(startTime).Seconds())

	// Update progress in database
	if err := p.fedStore.UpdateMigrationProgress(ctx, fedConfig.Bucket, 1, bytesIngested, key); err != nil {
		logger.Warn().
			Err(err).
			Str("bucket", fedConfig.Bucket).
			Str("key", key).
			Msg("failed to update migration progress")
	}

	return nil
}
