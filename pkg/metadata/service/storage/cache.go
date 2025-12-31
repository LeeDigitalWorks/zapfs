// Package storage provides coordination between file servers and the manager
// for object storage operations.
package storage

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// TargetCache caches file server locations and their backend info to reduce
// calls to the manager for target selection. This is inspired by SeaweedFS's
// client-side volume location caching pattern.
//
// The cache is updated in real-time via WatchTopology streaming RPC (PUSH-based).
// Target selection is done locally using the same logic as the manager's
// selectReplicationTargets.
//
// TargetCache wraps the common cache.Cache[K, V] infrastructure for storage
// and adds domain-specific target selection logic.
type TargetCache struct {
	// Underlying generic cache with TTL and lock striping
	cache *cache.Cache[string, *CachedFileServer]

	// Metadata tracked atomically
	topologyVersion atomic.Uint64
	lastRefresh     atomic.Pointer[time.Time]

	// Configuration
	refreshInterval time.Duration
	enabled         bool

	// For background refresh
	stopCh chan struct{}
	wg     sync.WaitGroup
}

// CachedFileServer represents a cached file server with its storage backends
type CachedFileServer struct {
	Address         string
	Location        *common_pb.Location
	StorageBackends []*manager_pb.StorageBackend
	LastSeen        time.Time
}

// TargetCacheConfig holds configuration for the target cache
type TargetCacheConfig struct {
	// Enabled controls whether caching is active (default: true)
	Enabled bool

	// RefreshInterval is how often to refresh the topology cache (default: 30s)
	RefreshInterval time.Duration

	// StaleThreshold is how long before a cached entry is considered stale (default: 60s)
	StaleThreshold time.Duration
}

// DefaultTargetCacheConfig returns sensible defaults for target caching
func DefaultTargetCacheConfig() TargetCacheConfig {
	return TargetCacheConfig{
		Enabled:         true,
		RefreshInterval: 30 * time.Second,
		StaleThreshold:  60 * time.Second,
	}
}

// NewTargetCache creates a new target cache using the common cache infrastructure
func NewTargetCache(cfg TargetCacheConfig) *TargetCache {
	// Use the common cache with TTL expiry
	c := cache.New[string, *CachedFileServer](
		context.Background(),
		cache.WithExpiry[string, *CachedFileServer](cfg.StaleThreshold),
		cache.WithNumShards[string, *CachedFileServer](16), // Fewer shards since we have fewer file servers
	)

	return &TargetCache{
		cache:           c,
		refreshInterval: cfg.RefreshInterval,
		enabled:         cfg.Enabled,
		stopCh:          make(chan struct{}),
	}
}

// Start begins the background refresh goroutine
func (c *TargetCache) Start(ctx context.Context, refreshFn func(context.Context) error) {
	if !c.enabled {
		return
	}

	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		c.backgroundRefresh(ctx, refreshFn)
	}()
}

// Stop stops the background refresh goroutine and cleanup
func (c *TargetCache) Stop() {
	close(c.stopCh)
	c.wg.Wait()
	c.cache.Stop() // Stop the underlying cache cleanup
}

// backgroundRefresh periodically refreshes the topology cache
func (c *TargetCache) backgroundRefresh(ctx context.Context, refreshFn func(context.Context) error) {
	ticker := time.NewTicker(c.refreshInterval)
	defer ticker.Stop()

	// Initial refresh
	if err := refreshFn(ctx); err != nil {
		logger.Warn().Err(err).Msg("initial topology refresh failed")
	}

	for {
		select {
		case <-c.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := refreshFn(ctx); err != nil {
				logger.Warn().Err(err).Msg("periodic topology refresh failed")
			}
		}
	}
}

// Update updates the cache with topology information from the manager
func (c *TargetCache) Update(services []*manager_pb.ServiceInfo, version uint64) {
	now := time.Now()

	for _, svc := range services {
		// Only cache file services
		if svc.ServiceType != manager_pb.ServiceType_FILE_SERVICE {
			continue
		}

		if svc.Location == nil {
			continue
		}

		addr := svc.Location.Address

		// Try to preserve existing storage backend info
		var backends []*manager_pb.StorageBackend
		if existing, ok := c.cache.Get(addr); ok {
			backends = existing.StorageBackends
		}

		entry := &CachedFileServer{
			Address:         addr,
			Location:        svc.Location,
			StorageBackends: backends,
			LastSeen:        now,
		}

		c.cache.Set(addr, entry)
	}

	c.topologyVersion.Store(version)
	c.lastRefresh.Store(&now)

	logger.Debug().
		Int("file_servers", c.cache.Size()).
		Uint64("topology_version", version).
		Msg("target cache updated")
}

// Remove removes services from the cache (called when services go offline)
func (c *TargetCache) Remove(services []*manager_pb.ServiceInfo, version uint64) {
	for _, svc := range services {
		if svc.Location == nil {
			continue
		}
		c.cache.Delete(svc.Location.Address)
	}

	c.topologyVersion.Store(version)
	now := time.Now()
	c.lastRefresh.Store(&now)

	logger.Debug().
		Int("removed", len(services)).
		Int("remaining", c.cache.Size()).
		Uint64("topology_version", version).
		Msg("target cache: services removed")
}

// UpdateFromRegistration updates a specific file server's storage backends
// This is called when we receive detailed backend info (e.g., from heartbeat)
func (c *TargetCache) UpdateFromRegistration(addr string, location *common_pb.Location, backends []*manager_pb.StorageBackend) {
	entry, ok := c.cache.Get(addr)
	if !ok {
		entry = &CachedFileServer{
			Address:  addr,
			Location: location,
		}
	}

	entry.StorageBackends = backends
	entry.LastSeen = time.Now()
	c.cache.Set(addr, entry)
}

// SelectTargets selects replication targets from the cache.
// Returns nil if cache is empty or insufficient targets available.
// This mirrors the manager's selectReplicationTargets logic.
func (c *TargetCache) SelectTargets(fileSize uint64, numReplicas uint32, tier string) []*manager_pb.ReplicationTarget {
	if !c.enabled {
		return nil
	}

	if c.cache.Size() == 0 {
		return nil
	}

	var targets []*manager_pb.ReplicationTarget
	priority := int32(1)

	// Iterate through cached file servers
	for _, entry := range c.cache.Iter() {
		if len(entry.StorageBackends) == 0 {
			// No backend info cached, skip this server
			continue
		}

		for _, storageBackend := range entry.StorageBackends {
			if len(storageBackend.Backends) == 0 {
				continue
			}

			// Find suitable backend
			for _, backend := range storageBackend.Backends {
				// Check if backend has enough space
				freeBytes := backend.TotalBytes - backend.UsedBytes
				if freeBytes < fileSize {
					continue
				}

				targets = append(targets, &manager_pb.ReplicationTarget{
					Location:    entry.Location,
					BackendId:   backend.Id,
					BackendType: backend.Type,
					Priority:    priority,
				})

				priority++
				if uint32(len(targets)) >= numReplicas {
					return targets
				}

				// One backend per storage backend for now
				break
			}

			if uint32(len(targets)) >= numReplicas {
				return targets
			}
		}

		if uint32(len(targets)) >= numReplicas {
			return targets
		}
	}

	// Return what we have, even if less than requested
	// Caller will fallback to manager if insufficient
	return targets
}

// Size returns the number of cached file servers
func (c *TargetCache) Size() int {
	return c.cache.Size()
}

// TopologyVersion returns the current cached topology version
func (c *TargetCache) TopologyVersion() uint64 {
	return c.topologyVersion.Load()
}

// IsEnabled returns whether caching is enabled
func (c *TargetCache) IsEnabled() bool {
	return c.enabled
}

// Stats returns cache statistics for monitoring
func (c *TargetCache) Stats() CacheStats {
	totalBackends := 0
	for _, entry := range c.cache.Iter() {
		for _, sb := range entry.StorageBackends {
			totalBackends += len(sb.Backends)
		}
	}

	var lastRefresh time.Time
	if lr := c.lastRefresh.Load(); lr != nil {
		lastRefresh = *lr
	}

	return CacheStats{
		FileServers:     c.cache.Size(),
		TotalBackends:   totalBackends,
		TopologyVersion: c.topologyVersion.Load(),
		LastRefresh:     lastRefresh,
	}
}

// CacheStats holds statistics about the target cache
type CacheStats struct {
	FileServers     int
	TotalBackends   int
	TopologyVersion uint64
	LastRefresh     time.Time
}
