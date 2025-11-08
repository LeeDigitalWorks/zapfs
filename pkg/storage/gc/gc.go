package gc

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"zapfs/pkg/debug"
	"zapfs/pkg/logger"
	"zapfs/pkg/storage/backend"
	"zapfs/pkg/storage/index"
	"zapfs/pkg/types"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// GC-specific metrics
	gcChunksDeleted = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "gc",
		Name:      "chunks_deleted_total",
		Help:      "Total number of chunks deleted by GC",
	})

	gcBytesReclaimed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "gc",
		Name:      "bytes_reclaimed_total",
		Help:      "Total bytes reclaimed by GC",
	})

	gcRunsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "gc",
		Name:      "runs_total",
		Help:      "Total number of GC runs",
	})
)

func init() {
	debug.Registry().MustRegister(
		gcChunksDeleted,
		gcBytesReclaimed,
		gcRunsTotal,
	)
}

// Default grace period before deleting chunks with RefCount == 0
const DefaultGracePeriod = 5 * time.Minute

// GCWorker garbage collects unreferenced chunks
type GCWorker struct {
	chunkIdx       index.Indexer[types.ChunkID, types.Chunk]
	backendID      string
	interval       time.Duration
	gracePeriod    time.Duration // How long to wait before deleting RefCount=0 chunks
	stopCh         chan struct{}
	concurrency    int
	manager        *backend.Manager
	onChunkDeleted OnChunkDeleted
}

// OnChunkDeleted is called when a chunk is successfully deleted by GC
type OnChunkDeleted func(chunk types.Chunk)

// GCWorkerConfig holds configuration for GCWorker
type GCWorkerConfig struct {
	ChunkIdx       index.Indexer[types.ChunkID, types.Chunk]
	BackendID      string
	Interval       time.Duration
	GracePeriod    time.Duration  // 0 means use DefaultGracePeriod
	Concurrency    int            // 0 means use default (5)
	Manager        *backend.Manager
	OnChunkDeleted OnChunkDeleted // Optional callback when chunk is deleted
}

// NewGCWorker creates a new GC worker for a backend
func NewGCWorker(
	chunkIdx index.Indexer[types.ChunkID, types.Chunk],
	backendID string,
	interval time.Duration,
	manager *backend.Manager,
) *GCWorker {
	return NewGCWorkerWithConfig(GCWorkerConfig{
		ChunkIdx:  chunkIdx,
		BackendID: backendID,
		Interval:  interval,
		Manager:   manager,
	})
}

// NewGCWorkerWithConfig creates a new GC worker with full configuration
func NewGCWorkerWithConfig(cfg GCWorkerConfig) *GCWorker {
	gracePeriod := cfg.GracePeriod
	if gracePeriod == 0 {
		gracePeriod = DefaultGracePeriod
	}

	concurrency := cfg.Concurrency
	if concurrency == 0 {
		concurrency = 5
	}

	return &GCWorker{
		chunkIdx:       cfg.ChunkIdx,
		backendID:      cfg.BackendID,
		interval:       cfg.Interval,
		gracePeriod:    gracePeriod,
		stopCh:         make(chan struct{}),
		concurrency:    concurrency,
		manager:        cfg.Manager,
		onChunkDeleted: cfg.OnChunkDeleted,
	}
}

// Start runs the GC loop in a goroutine
func (gc *GCWorker) Start() {
	if gc.interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(gc.interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				gc.Run()
			case <-gc.stopCh:
				return
			}
		}
	}()
}

// Stop signals the GC worker to exit
func (gc *GCWorker) Stop() {
	close(gc.stopCh)
}

// BackendID returns the backend ID this worker is responsible for
func (gc *GCWorker) BackendID() string {
	return gc.backendID
}

// Run performs a single GC pass
func (gc *GCWorker) Run() {
	gc.RunWithGracePeriod(gc.gracePeriod)
}

// RunWithGracePeriod performs a single GC pass with a specific grace period.
// Use gracePeriod=0 to skip the grace period check (force immediate deletion).
func (gc *GCWorker) RunWithGracePeriod(gracePeriod time.Duration) {
	gcRunsTotal.Inc()
	now := time.Now().Unix()
	gracePeriodSeconds := int64(gracePeriod.Seconds())

	ch := gc.chunkIdx.Stream(func(c types.Chunk) bool {
		return c.BackendID == gc.backendID
	})

	var wg sync.WaitGroup
	sem := make(chan struct{}, gc.concurrency)
	var deleted, skipped atomic.Int64

	for chunk := range ch {
		sem <- struct{}{}
		wg.Add(1)
		go func(c types.Chunk) {
			defer wg.Done()
			defer func() { <-sem }()

			// Only consider chunks with zero reference count
			if c.RefCount != 0 {
				return
			}

			// Check grace period: ZeroRefSince must be set and old enough
			if gracePeriodSeconds > 0 {
				if c.ZeroRefSince == 0 {
					// ZeroRefSince not set - skip (legacy chunk or just decremented)
					skipped.Add(1)
					return
				}
				if now-c.ZeroRefSince < gracePeriodSeconds {
					// Not old enough yet
					skipped.Add(1)
					return
				}
			}

			// Grace period passed (or skipped), delete the chunk
			if err := gc.deleteChunk(c); err != nil {
				logger.Error().Err(err).Msgf("gc: failed to delete chunk %s", c.ID)
			} else {
				deleted.Add(1)
			}
		}(chunk)
	}

	wg.Wait()

	deletedCount := deleted.Load()
	skippedCount := skipped.Load()
	if deletedCount > 0 || skippedCount > 0 {
		logger.Info().
			Str("backend", gc.backendID).
			Int64("deleted", deletedCount).
			Int64("skipped_grace_period", skippedCount).
			Dur("grace_period", gracePeriod).
			Msg("GC pass completed")
	}
}

// deleteChunk removes a chunk from storage
func (gc *GCWorker) deleteChunk(chunk types.Chunk) error {
	store, ok := gc.manager.Get(chunk.BackendID)
	if !ok {
		logger.Warn().Msgf("gc: backend %s not found for chunk %s", chunk.BackendID, chunk.ID)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := store.Delete(ctx, chunk.Path); err != nil {
		return err
	}

	// Remove from index
	if err := gc.chunkIdx.Delete(chunk.ID); err != nil {
		logger.Warn().Err(err).Msgf("gc: failed to delete chunk index for %s", chunk.ID)
	}

	// Update GC metrics
	gcChunksDeleted.Inc()
	gcBytesReclaimed.Add(float64(chunk.Size))

	// Call callback if registered (to update store metrics)
	if gc.onChunkDeleted != nil {
		gc.onChunkDeleted(chunk)
	}

	return nil
}
