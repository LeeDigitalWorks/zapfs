// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"fmt"
	"hash/maphash"
	"iter"
	"sync"
	"sync/atomic"
	"time"
)

// Default number of shards for lock striping
const defaultShardCount = 64

// entry wraps a value with access time for LRU eviction and TTL expiry
type entry[V any] struct {
	value      V
	lastAccess atomic.Int64 // Unix nano timestamp
}

// shard is a single partition of the cache with its own lock
type shard[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]*entry[V]
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

	shards    []*shard[K, V]
	numShards uint64
	seed      maphash.Seed

	// Optional load function for cache misses
	loadFunc func(ctx context.Context, key K) (V, error)

	hasLoaded atomic.Bool

	// Max size per shard (0 = unlimited)
	maxSizePerShard int

	// TTL expiry (0 = no expiry)
	expiry time.Duration

	// Cleanup timer for expired entries
	cleanupTimer *time.Timer
	cleanupStop  chan struct{}
}

// Option configures a Cache
type Option[K comparable, V any] func(*Cache[K, V])

// WithMaxSize sets the maximum total number of entries in the cache.
// The limit is distributed across shards. When a shard exceeds its limit,
// the least recently accessed entry in that shard is evicted.
func WithMaxSize[K comparable, V any](maxSize int) Option[K, V] {
	return func(c *Cache[K, V]) {
		c.maxSizePerShard = maxSize / int(c.numShards)
		if c.maxSizePerShard < 1 && maxSize > 0 {
			c.maxSizePerShard = 1
		}
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
// Default is 32 shards.
func WithNumShards[K comparable, V any](numShards int) Option[K, V] {
	return func(c *Cache[K, V]) {
		if numShards < 1 {
			numShards = 1
		}
		c.numShards = uint64(numShards)
		c.shards = make([]*shard[K, V], numShards)
		for i := range c.shards {
			c.shards[i] = &shard[K, V]{
				data: make(map[K]*entry[V]),
			}
		}
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
		seed:        maphash.MakeSeed(),
		cleanupStop: make(chan struct{}),
	}

	// Initialize default shards
	c.shards = make([]*shard[K, V], c.numShards)
	for i := range c.shards {
		c.shards[i] = &shard[K, V]{
			data: make(map[K]*entry[V]),
		}
	}

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

	for _, s := range c.shards {
		s.mu.Lock()
		for key, e := range s.data {
			if now-e.lastAccess.Load() > expiryNanos {
				delete(s.data, key)
			}
		}
		s.mu.Unlock()
	}
}

// Stop stops the cleanup goroutine. Call this when the cache is no longer needed.
func (c *Cache[K, V]) Stop() {
	if c.cleanupTimer != nil {
		c.cleanupTimer.Stop()
		close(c.cleanupStop)
	}
}

// getShard returns the shard for a given key using consistent hashing
func (c *Cache[K, V]) getShard(key K) *shard[K, V] {
	var h maphash.Hash
	h.SetSeed(c.seed)

	switch k := any(key).(type) {
	case string:
		h.WriteString(k)
	case []byte:
		h.Write(k)
	default:
		h.WriteString(fmt.Sprint(key))
	}

	return c.shards[h.Sum64()%c.numShards]
}

// Get retrieves a value from the cache.
// Returns the value and true if found (and not expired), or zero value and false otherwise.
func (c *Cache[K, V]) Get(key K) (V, bool) {
	s := c.getShard(key)

	s.mu.RLock()
	e, exists := s.data[key]
	s.mu.RUnlock()

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
	s := c.getShard(key)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if we need to evict before adding (per-shard limit)
	if c.maxSizePerShard > 0 && len(s.data) >= c.maxSizePerShard {
		if _, exists := s.data[key]; !exists {
			c.evictOldestInShard(s)
		}
	}

	e := &entry[V]{value: value}
	e.lastAccess.Store(time.Now().UnixNano())
	s.data[key] = e
}

// evictOldestInShard removes the least recently accessed entry from a shard.
// Caller must hold the shard's write lock.
func (c *Cache[K, V]) evictOldestInShard(s *shard[K, V]) {
	var oldestKey K
	var oldestTime int64 = 0
	first := true

	for k, e := range s.data {
		accessTime := e.lastAccess.Load()
		if first || accessTime < oldestTime {
			oldestKey = k
			oldestTime = accessTime
			first = false
		}
	}

	if !first {
		delete(s.data, oldestKey)
	}
}

// Delete removes a key from the cache.
func (c *Cache[K, V]) Delete(key K) {
	s := c.getShard(key)

	s.mu.Lock()
	delete(s.data, key)
	s.mu.Unlock()
}

// Size returns the current number of entries across all shards.
func (c *Cache[K, V]) Size() int {
	total := 0
	for _, s := range c.shards {
		s.mu.RLock()
		total += len(s.data)
		s.mu.RUnlock()
	}
	return total
}

// Clear removes all entries from the cache.
func (c *Cache[K, V]) Clear() {
	for _, s := range c.shards {
		s.mu.Lock()
		s.data = make(map[K]*entry[V])
		s.mu.Unlock()
	}
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

	var batch []Entity[K, V]
	for entity, err := range seq {
		if err != nil {
			return err
		}
		batch = append(batch, entity)
		if len(batch) >= batchSize {
			c.applyBatch(batch)
			batch = batch[:0]
		}
	}

	if len(batch) > 0 {
		c.applyBatch(batch)
	}

	c.hasLoaded.Store(true)
	return nil
}

func (c *Cache[K, V]) applyBatch(batch []Entity[K, V]) {
	// Group entities by shard to minimize lock acquisitions
	shardBatches := make(map[*shard[K, V]][]Entity[K, V])

	for _, entity := range batch {
		s := c.getShard(entity.Key)
		shardBatches[s] = append(shardBatches[s], entity)
	}

	// Apply each shard's batch with a single lock acquisition
	now := time.Now().UnixNano()
	for s, entities := range shardBatches {
		s.mu.Lock()
		for _, entity := range entities {
			if entity.IsDeleted {
				delete(s.data, entity.Key)
			} else {
				if c.maxSizePerShard > 0 && len(s.data) >= c.maxSizePerShard {
					if _, exists := s.data[entity.Key]; !exists {
						c.evictOldestInShard(s)
					}
				}
				e := &entry[V]{value: entity.Value}
				e.lastAccess.Store(now)
				s.data[entity.Key] = e
			}
		}
		s.mu.Unlock()
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

		for _, s := range c.shards {
			s.mu.RLock()
			for key, e := range s.data {
				// Skip expired entries
				if c.expiry > 0 && now-e.lastAccess.Load() > expiryNanos {
					continue
				}
				// Must unlock before calling yield to avoid deadlock
				val := e.value
				s.mu.RUnlock()

				if !yield(key, val) {
					return
				}
				s.mu.RLock()
			}
			s.mu.RUnlock()
		}
	}
}
