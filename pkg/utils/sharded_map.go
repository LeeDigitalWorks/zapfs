// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"hash/maphash"
	"sync"
)

const defaultNumShards = 256

// ShardedMap is a concurrent map with sharding for reduced lock contention.
// Supports generic key types and configurable shard count.
// More efficient than sync.Map for high-throughput scenarios with mixed reads/writes.
type ShardedMap[K comparable, V any] struct {
	shards    []shard[K, V]
	numShards uint64
	hashFunc  func(K) uint64
}

type shard[K comparable, V any] struct {
	sync.RWMutex
	m map[K]V
}

// ShardedMapOption configures a ShardedMap.
type ShardedMapOption[K comparable, V any] func(*ShardedMap[K, V])

// WithShardCount sets the number of shards. Default is 256.
// More shards = less contention but more memory overhead.
func WithShardCount[K comparable, V any](count int) ShardedMapOption[K, V] {
	return func(sm *ShardedMap[K, V]) {
		if count < 1 {
			count = 1
		}
		sm.numShards = uint64(count)
	}
}

// WithHashFunc sets a custom hash function for keys.
// Default uses FNV-1a for strings and maphash for other types.
func WithHashFunc[K comparable, V any](fn func(K) uint64) ShardedMapOption[K, V] {
	return func(sm *ShardedMap[K, V]) {
		sm.hashFunc = fn
	}
}

// NewShardedMap creates a new sharded map with the given options.
func NewShardedMap[K comparable, V any](opts ...ShardedMapOption[K, V]) *ShardedMap[K, V] {
	sm := &ShardedMap[K, V]{
		numShards: defaultNumShards,
	}

	// Apply options
	for _, opt := range opts {
		opt(sm)
	}

	// Initialize shards
	sm.shards = make([]shard[K, V], sm.numShards)
	for i := range sm.shards {
		sm.shards[i].m = make(map[K]V)
	}

	// Set default hash function if not provided
	if sm.hashFunc == nil {
		sm.hashFunc = defaultHashFunc[K]()
	}

	return sm
}

// fnvHash64 computes FNV-1a hash inline without allocations
// Uses fnvOffset64/fnvPrime64 constants from bloom_filter.go
func fnvHash64(data []byte) uint64 {
	h := uint64(fnvOffset64)
	for _, b := range data {
		h ^= uint64(b)
		h *= fnvPrime64
	}
	return h
}

// fnvHashString computes FNV-1a hash for strings without allocations
func fnvHashString(s string) uint64 {
	h := uint64(fnvOffset64)
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= fnvPrime64
	}
	return h
}

// defaultHashFunc returns an appropriate hash function for the key type.
func defaultHashFunc[K comparable]() func(K) uint64 {
	// Check if K is string at runtime
	var zero K
	switch any(zero).(type) {
	case string:
		// Use inline FNV-1a for strings (zero allocations)
		return func(key K) uint64 {
			return fnvHashString(any(key).(string))
		}
	case []byte:
		return func(key K) uint64 {
			return fnvHash64(any(key).([]byte))
		}
	default:
		// Use maphash for other types
		seed := maphash.MakeSeed()
		return func(key K) uint64 {
			var h maphash.Hash
			h.SetSeed(seed)
			h.WriteString(fmt.Sprint(key))
			return h.Sum64()
		}
	}
}

// getShard returns the shard for a given key.
func (sm *ShardedMap[K, V]) getShard(key K) *shard[K, V] {
	return &sm.shards[sm.hashFunc(key)%sm.numShards]
}

// Load returns the value for a key, or the zero value if not found.
func (sm *ShardedMap[K, V]) Load(key K) (V, bool) {
	s := sm.getShard(key)
	s.RLock()
	v, ok := s.m[key]
	s.RUnlock()
	return v, ok
}

// Store sets a value for a key.
func (sm *ShardedMap[K, V]) Store(key K, value V) {
	s := sm.getShard(key)
	s.Lock()
	s.m[key] = value
	s.Unlock()
}

// LoadOrStore returns the existing value if present, otherwise stores and returns the new value.
// Returns true if the value was loaded, false if stored.
func (sm *ShardedMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	s := sm.getShard(key)

	// Fast path: check with read lock first
	s.RLock()
	if v, ok := s.m[key]; ok {
		s.RUnlock()
		return v, true
	}
	s.RUnlock()

	// Slow path: acquire write lock
	s.Lock()
	defer s.Unlock()

	// Double-check after acquiring write lock
	if v, ok := s.m[key]; ok {
		return v, true
	}

	s.m[key] = value
	return value, false
}

// LoadAndDelete loads and deletes a key atomically.
// Returns the value and true if found, zero value and false otherwise.
func (sm *ShardedMap[K, V]) LoadAndDelete(key K) (V, bool) {
	s := sm.getShard(key)
	s.Lock()
	v, ok := s.m[key]
	if ok {
		delete(s.m, key)
	}
	s.Unlock()
	return v, ok
}

// Delete removes a key from the map.
func (sm *ShardedMap[K, V]) Delete(key K) {
	s := sm.getShard(key)
	s.Lock()
	delete(s.m, key)
	s.Unlock()
}

// Range calls f for each key-value pair in the map.
// If f returns false, iteration stops.
func (sm *ShardedMap[K, V]) Range(f func(key K, value V) bool) {
	for i := range sm.shards {
		s := &sm.shards[i]
		s.RLock()
		for k, v := range s.m {
			if !f(k, v) {
				s.RUnlock()
				return
			}
		}
		s.RUnlock()
	}
}

// Len returns the total number of entries across all shards.
func (sm *ShardedMap[K, V]) Len() int {
	count := 0
	for i := range sm.shards {
		s := &sm.shards[i]
		s.RLock()
		count += len(s.m)
		s.RUnlock()
	}
	return count
}

// DeleteIf deletes entries where the predicate returns true.
// Returns the number of entries deleted.
func (sm *ShardedMap[K, V]) DeleteIf(predicate func(key K, value V) bool) int {
	deleted := 0
	for i := range sm.shards {
		s := &sm.shards[i]
		s.Lock()
		for k, v := range s.m {
			if predicate(k, v) {
				delete(s.m, k)
				deleted++
			}
		}
		s.Unlock()
	}
	return deleted
}

// Clear removes all entries from the map.
func (sm *ShardedMap[K, V]) Clear() {
	for i := range sm.shards {
		s := &sm.shards[i]
		s.Lock()
		s.m = make(map[K]V)
		s.Unlock()
	}
}

// Keys returns all keys in the map.
// Note: This is not atomic - keys may be added/removed during iteration.
func (sm *ShardedMap[K, V]) Keys() []K {
	keys := make([]K, 0, sm.Len())
	sm.Range(func(key K, _ V) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// Compute atomically computes a new value for a key.
// If the key exists, f receives the current value and true.
// If the key doesn't exist, f receives zero value and false.
// If f returns false, the key is deleted (if it exists).
// Returns the new value and whether it was stored.
func (sm *ShardedMap[K, V]) Compute(key K, f func(oldValue V, exists bool) (newValue V, store bool)) (V, bool) {
	s := sm.getShard(key)
	s.Lock()
	defer s.Unlock()

	oldValue, exists := s.m[key]
	newValue, store := f(oldValue, exists)

	if store {
		s.m[key] = newValue
		return newValue, true
	} else if exists {
		delete(s.m, key)
	}

	var zero V
	return zero, false
}
