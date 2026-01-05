// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"iter"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// Default number of shards for lock striping
const defaultShardCount = 256

// entry wraps a value with access time for LRU eviction and TTL expiry
type entry[V any] struct {
	value      V
	lastAccess atomic.Int64 // Unix nano timestamp
}

// Cache is a high-performance concurrent cache with lock striping.
//
// Features:
//   - Lock striping: Keys distributed across shards for reduced contention
//   - Optional TTL: Entries expire after a configurable duration
//   - Optional max size: LRU eviction when capacity is reached (per-shard)
//   - Bulk loading: Efficiently load many entries with minimal lock contention
//
// Usage:
//
//	// Simple cache with max size
//	c := cache.New[string, User](ctx, cache.WithMaxSize[string, User](10000))
//
//	// Cache with TTL expiry
//	c := cache.New[string, Token](ctx, cache.WithExpiry[string, Token](5*time.Minute))
//
//	// Cache with both
//	c := cache.New[string, Session](ctx,
//	    cache.WithMaxSize[string, Session](1000),
//	    cache.WithExpiry[string, Session](time.Hour),
//	)
type Cache[K comparable, V any] struct {
	ctx context.Context

	// Use ShardedMap for the underlying storage
	store     *utils.ShardedMap[K, *entry[V]]
	numShards int

	// Shard-level locks for LRU eviction (need write access to evict)
	shardLocks []sync.Mutex

	// Optional load function for cache misses
	loadFunc func(ctx context.Context, key K) (V, error)

	hasLoaded atomic.Bool

	// Max size (0 = unlimited)
	maxSize int

	// TTL expiry (0 = no expiry)
	expiry time.Duration

	// Cleanup timer for expired entries
	cleanupTimer *time.Timer
	cleanupStop  chan struct{}
}

// Option configures a Cache
type Option[K comparable, V any] func(*Cache[K, V])

// WithMaxSize sets the maximum total number of entries in the cache.
// When capacity is reached, the least recently accessed entry is evicted.
func WithMaxSize[K comparable, V any](maxSize int) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.maxSize = maxSize
	}
}

// WithExpiry sets the TTL for cache entries. Entries older than this
// duration are considered expired and won't be returned by Get().
// A background cleanup goroutine removes expired entries periodically.
func WithExpiry[K comparable, V any](expiry time.Duration) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.expiry = expiry
	}
}

// WithNumShards sets the number of shards for lock striping.
// More shards = less contention but slightly more memory overhead.
// Default is 64 shards.
func WithNumShards[K comparable, V any](numShards int) Option[K, V] {
	return func(c *Cache[K, V]) {
		if numShards < 1 {
			numShards = 1
		}
		c.numShards = numShards
		c.shardLocks = make([]sync.Mutex, numShards)
		c.store = utils.NewShardedMap[K, *entry[V]](
			utils.WithShardCount[K, *entry[V]](numShards),
		)
	}
}

// WithLoadFunc sets a function to call on cache misses.
// If set, Get() will call this function when an entry is not found
// and cache the result for future requests.
func WithLoadFunc[K comparable, V any](loadFunc func(ctx context.Context, key K) (V, error)) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.loadFunc = loadFunc
	}
}

// New creates a new Cache with the given options.
func New[K comparable, V any](ctx context.Context, opts ...Option[K, V]) *Cache[K, V] {
	c := &Cache[K, V]{
		ctx:         ctx,
		numShards:   defaultShardCount,
		cleanupStop: make(chan struct{}),
	}

	// Initialize default store
	c.shardLocks = make([]sync.Mutex, c.numShards)
	c.store = utils.NewShardedMap[K, *entry[V]](
		utils.WithShardCount[K, *entry[V]](c.numShards),
	)

	// Apply options (may override numShards)
	for _, opt := range opts {
		opt(c)
	}

	// Start cleanup timer if expiry is set
	if c.expiry > 0 {
		c.startCleanup()
	}

	return c
}

// startCleanup starts the background cleanup goroutine
func (c *Cache[K, V]) startCleanup() {
	c.cleanupTimer = time.AfterFunc(c.expiry, func() {
		c.cleanup()
		// Reschedule if not stopped
		select {
		case <-c.cleanupStop:
			return
		default:
			c.cleanupTimer.Reset(c.expiry)
		}
	})
}

// cleanup removes expired entries from all shards
func (c *Cache[K, V]) cleanup() {
	if c.expiry == 0 {
		return
	}

	now := time.Now().UnixNano()
	expiryNanos := c.expiry.Nanoseconds()

	c.store.DeleteIf(func(_ K, e *entry[V]) bool {
		return now-e.lastAccess.Load() > expiryNanos
	})
}

// Stop stops the cleanup goroutine. Call this when the cache is no longer needed.
func (c *Cache[K, V]) Stop() {
	if c.cleanupTimer != nil {
		c.cleanupTimer.Stop()
		close(c.cleanupStop)
	}
}

// Get retrieves a value from the cache.
// Returns the value and true if found (and not expired), or zero value and false otherwise.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	e, exists := c.store.Load(key)
	if !exists {
		var zero V
		return zero, false
	}

	// Check expiry if configured
	if c.expiry > 0 {
		ts := e.lastAccess.Load()
		if time.Now().UnixNano()-ts > c.expiry.Nanoseconds() {
			var zero V
			return zero, false
		}
	}

	// Update access time atomically (no lock needed)
	e.lastAccess.Store(time.Now().UnixNano())

	return e.value, true
}

// GetOrLoad retrieves a value from the cache, loading it if not present.
// Requires WithLoadFunc to be set.
func (c *Cache[K, V]) GetOrLoad(ctx context.Context, key K) (V, error) {
	// Try cache first
	if val, ok := c.Get(key); ok {
		return val, nil
	}

	// Cache miss - try to load
	if c.loadFunc == nil {
		var zero V
		return zero, nil
	}

	val, err := c.loadFunc(ctx, key)
	if err != nil {
		var zero V
		return zero, err
	}

	c.Set(key, val)
	return val, nil
}

// Set adds or updates a value in the cache.
func (c *Cache[K, V]) Set(key K, value V) {
	e := &entry[V]{value: value}
	e.lastAccess.Store(time.Now().UnixNano())

	// Check if we need to evict before storing (avoid deadlock by checking size outside lock)
	if c.maxSize > 0 {
		// Check size before acquiring lock to avoid deadlock
		currentSize := c.store.Len()
		if currentSize >= c.maxSize {
			c.evictOldest()
		}
	}

	c.store.Store(key, e)
}

// evictOldest removes the least recently accessed entry from the cache.
func (c *Cache[K, V]) evictOldest() {
	var oldestKey K
	var oldestTime int64 = 0
	first := true

	c.store.Range(func(k K, e *entry[V]) bool {
		accessTime := e.lastAccess.Load()
		if first || accessTime < oldestTime {
			oldestKey = k
			oldestTime = accessTime
			first = false
		}
		return true
	})

	if !first {
		c.store.Delete(oldestKey)
	}
}

// Delete removes a key from the cache.
func (c *Cache[K, V]) Delete(key K) {
	c.store.Delete(key)
}

// Size returns the current number of entries across all shards.
func (c *Cache[K, V]) Size() int {
	return c.store.Len()
}

// Clear removes all entries from the cache.
func (c *Cache[K, V]) Clear() {
	c.store.Clear()
}

// Entity represents a cache entry for bulk loading
type Entity[K, V any] struct {
	Key       K
	Value     V
	IsDeleted bool
}

// Load bulk loads entries into the cache from an iterator.
// This is optimized for minimal lock contention by grouping entries by shard.
func (c *Cache[K, V]) Load(seq iter.Seq2[Entity[K, V], error]) error {
	const batchSize = 1000

	now := time.Now().UnixNano()
	var batch []Entity[K, V]
	for entity, err := range seq {
		if err != nil {
			return err
		}
		batch = append(batch, entity)
		if len(batch) >= batchSize {
			c.applyBatch(batch, now)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		c.applyBatch(batch, now)
	}

	c.hasLoaded.Store(true)
	return nil
}

func (c *Cache[K, V]) applyBatch(batch []Entity[K, V], now int64) {
	for _, entity := range batch {
		if entity.IsDeleted {
			c.store.Delete(entity.Key)
		} else {
			e := &entry[V]{value: entity.Value}
			e.lastAccess.Store(now)
			c.store.Store(entity.Key, e)
		}
	}
}

// HasLoaded returns true if Load() has been called at least once.
func (c *Cache[K, V]) HasLoaded() bool {
	return c.hasLoaded.Load()
}

// MarkLoaded marks the cache as having completed initial load.
// This is useful when the cache is populated via Set() calls rather than Load().
func (c *Cache[K, V]) MarkLoaded() {
	c.hasLoaded.Store(true)
}

// Iter returns an iterator over all non-expired cache entries.
// Note: This acquires read locks on all shards sequentially, so avoid
// calling during high-traffic periods. Useful for debugging.
func (c *Cache[K, V]) Iter() iter.Seq2[K, V] {
	return func(yield func(K, V) bool) {
		now := time.Now().UnixNano()
		expiryNanos := c.expiry.Nanoseconds()

		c.store.Range(func(key K, e *entry[V]) bool {
			// Skip expired entries
			if c.expiry > 0 && now-e.lastAccess.Load() > expiryNanos {
				return true // continue
			}
			return yield(key, e.value)
		})
	}
}
