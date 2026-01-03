//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package manager

import (
	"context"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Metrics for multi-region cache sync
var (
	regionCacheSyncDuration = promauto.NewHistogram(prometheus.HistogramOpts{
		Name:    "zapfs_manager_region_cache_sync_duration_seconds",
		Help:    "Duration of initial bucket cache sync from primary region",
		Buckets: prometheus.DefBuckets,
	})
	regionCacheSyncErrors = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_manager_region_cache_sync_errors_total",
		Help: "Total number of cache sync errors",
	})
	regionCacheSyncBuckets = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "zapfs_manager_region_cache_buckets",
		Help: "Number of buckets in the local region cache",
	})
	regionCacheEventsReceived = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_manager_region_cache_events_total",
		Help: "Total number of collection events received from primary",
	})
	regionCacheReconnects = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zapfs_manager_region_cache_reconnects_total",
		Help: "Total number of reconnection attempts to primary region",
	})
)

// RegionSyncer handles synchronization of the bucket cache from the primary region
// using a long-lived streaming connection. This runs on secondary regions only.
type RegionSyncer struct {
	ms           *ManagerServer
	regionClient *RegionClient

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Last sync timestamp for resuming after reconnection
	lastSyncTime atomic.Value // stores time.Time
}

// NewRegionSyncer creates a new syncer for the given manager server.
// Returns nil if this is a primary region (no sync needed).
func NewRegionSyncer(ms *ManagerServer) *RegionSyncer {
	if ms.regionClient == nil || ms.IsPrimaryRegion() {
		return nil // No sync needed for primary region
	}

	ctx, cancel := context.WithCancel(context.Background())
	rs := &RegionSyncer{
		ms:           ms,
		regionClient: ms.regionClient,
		ctx:          ctx,
		cancel:       cancel,
	}
	// Initialize with zero time to trigger full sync
	rs.lastSyncTime.Store(time.Time{})
	return rs
}

// Start begins the collection watch loop.
func (rs *RegionSyncer) Start() {
	if rs == nil {
		return
	}

	rs.wg.Add(1)
	go rs.watchCollectionsLoop()

	logger.Info().Msg("Started region cache syncer (streaming mode)")
}

// Stop stops the watch loop and waits for it to finish.
func (rs *RegionSyncer) Stop() {
	if rs == nil {
		return
	}

	rs.cancel()
	rs.wg.Wait()
	logger.Info().Msg("Stopped region cache syncer")
}

// watchCollectionsLoop maintains a long-lived stream to the primary region.
// Automatically reconnects on connection loss with exponential backoff.
func (rs *RegionSyncer) watchCollectionsLoop() {
	defer rs.wg.Done()

	backoff := time.Second

	for {
		select {
		case <-rs.ctx.Done():
			return
		default:
		}

		if err := rs.watchCollections(); err != nil {
			if rs.ctx.Err() != nil {
				return // Context cancelled, exit
			}
			regionCacheSyncErrors.Inc()
			regionCacheReconnects.Inc()
			logger.Warn().Err(err).Dur("backoff", backoff).Msg("Collection watch disconnected, reconnecting...")
			time.Sleep(backoff)
			// Exponential backoff with max 30 seconds
			backoff = min(backoff*2, 30*time.Second)
		} else {
			backoff = time.Second // Reset backoff on clean disconnect
		}
	}
}

// watchCollections opens a stream and processes collection events.
func (rs *RegionSyncer) watchCollections() error {
	startTime := time.Now()

	// Get last sync time for resume
	sinceTime := rs.lastSyncTime.Load().(time.Time)

	req := &manager_pb.WatchCollectionsRequest{
		IncludeTombstoned: true, // We need deletions for cache consistency
	}
	if !sinceTime.IsZero() {
		req.SinceTime = timestamppb.New(sinceTime)
	}

	stream, region, err := rs.regionClient.WatchPrimaryCollections(rs.ctx, req)
	if err != nil {
		return err
	}

	logger.Info().
		Str("region", region).
		Time("since", sinceTime).
		Msg("Connected to primary region for collection sync")

	// Process events
	for {
		event, err := stream.Recv()
		if err != nil {
			return err
		}

		regionCacheEventsReceived.Inc()

		switch event.Type {
		case manager_pb.CollectionEvent_FULL_SYNC:
			rs.handleFullSync(event)
			duration := time.Since(startTime)
			regionCacheSyncDuration.Observe(duration.Seconds())
			logger.Info().
				Int("collections", len(event.Collections)).
				Dur("duration", duration).
				Msg("Completed initial full sync from primary region")

		case manager_pb.CollectionEvent_CREATED:
			rs.handleCreated(event)

		case manager_pb.CollectionEvent_DELETED:
			rs.handleDeleted(event)

		case manager_pb.CollectionEvent_UPDATED:
			rs.handleUpdated(event)
		}

		// Update last sync time for resume on reconnect
		if event.Timestamp != nil {
			rs.lastSyncTime.Store(event.Timestamp.AsTime())
		}
	}
}

// handleFullSync replaces the local cache with the full sync batch.
// Uses atomic index swap to prevent inconsistent state if process crashes mid-rebuild.
func (rs *RegionSyncer) handleFullSync(event *manager_pb.CollectionEvent) {
	// Build new collection map and indexes OUTSIDE the lock.
	// This ensures if we crash during build, the old state remains valid.
	newCollections := make(map[string]*manager_pb.Collection, len(event.Collections))
	newByOwner := make(map[string][]string)
	newOwnerCount := make(map[string]int)
	newByTier := make(map[string][]string)

	for _, col := range event.Collections {
		if !col.IsDeleted {
			// Clone to avoid sharing protobuf memory with stream
			colCopy := proto.Clone(col).(*manager_pb.Collection)
			newCollections[col.Name] = colCopy

			// Build owner index
			newByOwner[col.Owner] = append(newByOwner[col.Owner], col.Name)
			newOwnerCount[col.Owner]++

			// Build tier index
			newByTier[col.Tier] = append(newByTier[col.Tier], col.Name)
		}
	}

	// Sort owner collections for efficient prefix scans
	for owner := range newByOwner {
		sort.Strings(newByOwner[owner])
	}

	// Now atomically swap all state under lock
	rs.ms.state.Lock()
	defer rs.ms.state.Unlock()

	// Atomic swap - either all state is updated or none
	rs.ms.state.Collections = newCollections
	rs.ms.state.CollectionsByOwner = newByOwner
	rs.ms.state.OwnerCollectionCount = newOwnerCount
	rs.ms.state.CollectionsByTier = newByTier
	rs.ms.state.CollectionsVersion = event.Version

	// Rebuild time index (BTree doesn't support bulk replacement, but this is fast)
	// Note: The time index rebuild happens under lock, but collections/indexes
	// are already atomically swapped above, so partial crash here only affects
	// time-ordered queries (less critical than owner/tier lookups).
	rs.ms.state.CollectionsByTime.Clear(false)
	for _, col := range rs.ms.state.Collections {
		rs.ms.state.CollectionsByTime.ReplaceOrInsert(&collectionTimeItem{
			createdAt:  col.CreatedAt.AsTime(),
			name:       col.Name,
			collection: col,
		})
	}

	regionCacheSyncBuckets.Set(float64(len(rs.ms.state.Collections)))
}

// handleCreated adds a new collection to the cache.
func (rs *RegionSyncer) handleCreated(event *manager_pb.CollectionEvent) {
	rs.ms.state.Lock()
	defer rs.ms.state.Unlock()

	for _, col := range event.Collections {
		// Clone to avoid sharing protobuf memory
		colCopy := proto.Clone(col).(*manager_pb.Collection)
		rs.ms.state.Collections[col.Name] = colCopy
		rs.ms.addCollectionToIndexes(colCopy)

		logger.Debug().Str("collection", col.Name).Msg("Added collection from primary")
	}

	rs.ms.state.CollectionsVersion = event.Version
	regionCacheSyncBuckets.Set(float64(len(rs.ms.state.Collections)))
}

// handleDeleted removes a collection from the cache.
func (rs *RegionSyncer) handleDeleted(event *manager_pb.CollectionEvent) {
	rs.ms.state.Lock()
	defer rs.ms.state.Unlock()

	for _, col := range event.Collections {
		if existing, exists := rs.ms.state.Collections[col.Name]; exists {
			rs.ms.removeCollectionFromIndexes(existing)
			delete(rs.ms.state.Collections, col.Name)
			logger.Debug().Str("collection", col.Name).Msg("Removed collection from cache (deleted in primary)")
		}
	}

	rs.ms.state.CollectionsVersion = event.Version
	regionCacheSyncBuckets.Set(float64(len(rs.ms.state.Collections)))
}

// handleUpdated updates an existing collection in the cache.
func (rs *RegionSyncer) handleUpdated(event *manager_pb.CollectionEvent) {
	rs.ms.state.Lock()
	defer rs.ms.state.Unlock()

	for _, col := range event.Collections {
		// Clone to avoid sharing protobuf memory
		colCopy := proto.Clone(col).(*manager_pb.Collection)

		if existing, exists := rs.ms.state.Collections[col.Name]; exists {
			// Remove from old indexes if owner or tier changed
			if existing.Owner != col.Owner || existing.Tier != col.Tier {
				rs.ms.removeCollectionFromIndexes(existing)
				rs.ms.addCollectionToIndexes(colCopy)
			}
		} else {
			// New collection (shouldn't happen for UPDATED, but handle gracefully)
			rs.ms.addCollectionToIndexes(colCopy)
		}

		rs.ms.state.Collections[col.Name] = colCopy
		logger.Debug().Str("collection", col.Name).Msg("Updated collection from primary")
	}

	rs.ms.state.CollectionsVersion = event.Version
}
