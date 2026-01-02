// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package gc provides centralized garbage collection for the chunk registry.
// It identifies chunks with ref_count=0 past the grace period and removes them
// from the registry. The actual chunk data on file servers is cleaned up by
// the file servers' local GC or can be triggered via DeleteChunk RPC.
package gc

import (
	"context"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	gcRunsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_gc_runs_total",
		Help: "Total number of GC runs",
	})

	gcChunksDeleted = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_gc_chunks_deleted_total",
		Help: "Total number of chunks removed from registry by GC",
	})

	gcBytesReclaimed = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_gc_bytes_reclaimed_total",
		Help: "Total bytes of chunks removed from registry by GC",
	})

	gcLastRunTime = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "zapfs_gc_last_run_timestamp",
		Help: "Timestamp of last GC run",
	})

	gcDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "zapfs_gc_duration_seconds",
		Help:    "Duration of GC runs in seconds",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	})

	gcErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_gc_errors_total",
		Help: "Total number of GC errors",
	})
)

func init() {
	prometheus.MustRegister(
		gcRunsTotal,
		gcChunksDeleted,
		gcBytesReclaimed,
		gcLastRunTime,
		gcDuration,
		gcErrors,
	)
}

// Service performs garbage collection on the chunk registry.
type Service struct {
	db          db.DB
	interval    time.Duration
	gracePeriod time.Duration
	batchSize   int

	mu      sync.Mutex
	running bool
	stopCh  chan struct{}
}

// Config holds configuration for the GC service.
type Config struct {
	// DB is the metadata database
	DB db.DB

	// Interval is how often to run GC (default: 5 minutes)
	Interval time.Duration

	// GracePeriod is how long to wait after ref_count hits 0 before deleting (default: 1 hour)
	GracePeriod time.Duration

	// BatchSize is how many chunks to process per GC run (default: 1000)
	BatchSize int
}

// NewService creates a new GC service.
func NewService(cfg Config) *Service {
	if cfg.Interval == 0 {
		cfg.Interval = 5 * time.Minute
	}
	if cfg.GracePeriod == 0 {
		cfg.GracePeriod = 1 * time.Hour
	}
	if cfg.BatchSize == 0 {
		cfg.BatchSize = 1000
	}

	return &Service{
		db:          cfg.DB,
		interval:    cfg.Interval,
		gracePeriod: cfg.GracePeriod,
		batchSize:   cfg.BatchSize,
	}
}

// Start begins the GC loop.
func (s *Service) Start(ctx context.Context) {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return
	}
	s.running = true
	s.stopCh = make(chan struct{})
	s.mu.Unlock()

	go s.run(ctx)
}

// Stop stops the GC loop.
func (s *Service) Stop() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.running {
		return
	}

	close(s.stopCh)
	s.running = false
}

func (s *Service) run(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	// Run immediately on start
	s.collectGarbage(ctx)

	for {
		select {
		case <-ticker.C:
			s.collectGarbage(ctx)
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

func (s *Service) collectGarbage(ctx context.Context) {
	start := time.Now()
	gcRunsTotal.Inc()
	gcLastRunTime.SetToCurrentTime()

	cutoff := time.Now().Add(-s.gracePeriod)

	// Get chunks with ref_count=0 past grace period
	chunks, err := s.db.GetZeroRefChunks(ctx, cutoff, s.batchSize)
	if err != nil {
		logger.Error().Err(err).Msg("gc: failed to get zero ref chunks")
		gcErrors.Inc()
		return
	}

	if len(chunks) == 0 {
		gcDuration.Observe(time.Since(start).Seconds())
		return
	}

	logger.Info().Int("chunks", len(chunks)).Msg("gc: processing zero-ref chunks")

	var deletedCount int
	var deletedBytes int64

	for _, chunk := range chunks {
		// Use transaction to prevent race: check ref_count still 0, then delete
		err := s.db.WithTx(ctx, func(tx db.TxStore) error {
			// Re-check ref_count in transaction (could have been incremented)
			current, err := tx.GetChunkRefCount(ctx, chunk.ChunkID)
			if err != nil {
				if err == db.ErrChunkNotFound {
					// Already deleted, skip
					return nil
				}
				return err
			}
			if current > 0 {
				// Chunk was re-referenced, skip
				logger.Debug().
					Str("chunk_id", chunk.ChunkID).
					Int("ref_count", current).
					Msg("gc: chunk re-referenced, skipping")
				return nil
			}

			// Remove from registry (CASCADE deletes replicas)
			if err := tx.DeleteChunkRegistry(ctx, chunk.ChunkID); err != nil {
				return err
			}

			deletedCount++
			deletedBytes += chunk.Size
			return nil
		})

		if err != nil {
			logger.Warn().Err(err).Str("chunk_id", chunk.ChunkID).Msg("gc: failed to delete chunk")
			gcErrors.Inc()
			continue
		}
	}

	gcChunksDeleted.Add(float64(deletedCount))
	gcBytesReclaimed.Add(float64(deletedBytes))
	gcDuration.Observe(time.Since(start).Seconds())

	logger.Info().
		Int("deleted", deletedCount).
		Int64("bytes", deletedBytes).
		Dur("duration", time.Since(start)).
		Msg("gc: completed")
}

// RunOnce performs a single GC pass (useful for testing).
func (s *Service) RunOnce(ctx context.Context) {
	s.collectGarbage(ctx)
}
