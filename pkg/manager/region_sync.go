//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package manager

import (
	"context"
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
func (rs *RegionSyncer) handleFullSync(event *manager_pb.CollectionEvent) {
	rs.ms.mu.Lock()
	defer rs.ms.mu.Unlock()

	// Build new collection map from sync batch
	newCollections := make(map[string]*manager_pb.Collection, len(event.Collections))
	for _, col := range event.Collections {
		if !col.IsDeleted {
			// Clone to avoid sharing protobuf memory with stream
			newCollections[col.Name] = proto.Clone(col).(*manager_pb.Collection)
		}
	}

	// Clear and rebuild indexes
	rs.ms.collectionsByOwner = make(map[string][]string)
	rs.ms.ownerCollectionCount = make(map[string]int)
	rs.ms.collectionsByTier = make(map[string][]string)

	// Replace collections and rebuild indexes
	rs.ms.collections = newCollections
	for _, col := range rs.ms.collections {
		rs.ms.addCollectionToIndexes(col)
	}

	rs.ms.collectionsVersion = event.Version
	regionCacheSyncBuckets.Set(float64(len(rs.ms.collections)))
}

// handleCreated adds a new collection to the cache.
func (rs *RegionSyncer) handleCreated(event *manager_pb.CollectionEvent) {
	rs.ms.mu.Lock()
	defer rs.ms.mu.Unlock()

	for _, col := range event.Collections {
		// Clone to avoid sharing protobuf memory
		colCopy := proto.Clone(col).(*manager_pb.Collection)
		rs.ms.collections[col.Name] = colCopy
		rs.ms.addCollectionToIndexes(colCopy)

		logger.Debug().Str("collection", col.Name).Msg("Added collection from primary")
	}

	rs.ms.collectionsVersion = event.Version
	regionCacheSyncBuckets.Set(float64(len(rs.ms.collections)))
}

// handleDeleted removes a collection from the cache.
func (rs *RegionSyncer) handleDeleted(event *manager_pb.CollectionEvent) {
	rs.ms.mu.Lock()
	defer rs.ms.mu.Unlock()

	for _, col := range event.Collections {
		if existing, exists := rs.ms.collections[col.Name]; exists {
			rs.ms.removeCollectionFromIndexes(existing)
			delete(rs.ms.collections, col.Name)
			logger.Debug().Str("collection", col.Name).Msg("Removed collection from cache (deleted in primary)")
		}
	}

	rs.ms.collectionsVersion = event.Version
	regionCacheSyncBuckets.Set(float64(len(rs.ms.collections)))
}

// handleUpdated updates an existing collection in the cache.
func (rs *RegionSyncer) handleUpdated(event *manager_pb.CollectionEvent) {
	rs.ms.mu.Lock()
	defer rs.ms.mu.Unlock()

	for _, col := range event.Collections {
		// Clone to avoid sharing protobuf memory
		colCopy := proto.Clone(col).(*manager_pb.Collection)

		if existing, exists := rs.ms.collections[col.Name]; exists {
			// Remove from old indexes if owner or tier changed
			if existing.Owner != col.Owner || existing.Tier != col.Tier {
				rs.ms.removeCollectionFromIndexes(existing)
				rs.ms.addCollectionToIndexes(colCopy)
			}
		} else {
			// New collection (shouldn't happen for UPDATED, but handle gracefully)
			rs.ms.addCollectionToIndexes(colCopy)
		}

		rs.ms.collections[col.Name] = colCopy
		logger.Debug().Str("collection", col.Name).Msg("Updated collection from primary")
	}

	rs.ms.collectionsVersion = event.Version
}
