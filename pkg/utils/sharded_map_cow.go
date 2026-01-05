// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"hash/maphash"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
)

// ShardedMapCOW is a copy-on-write sharded map optimized for read-heavy workloads.
// Reads are completely lock-free, while writes copy the entire shard.
// Best for scenarios with high read:write ratios (>10:1).
//
// Trade-offs:
//   - Reads: O(1) lock-free
//   - Writes: O(n) where n is shard size (copies entire shard)
//   - Memory: Higher during writes (temporary copies)
type ShardedMapCOW[K comparable, V any] struct {
	shards    []cowShard[K, V]
	numShards uint64
	hashFunc  func(K) uint64
}

type cowShard[K comparable, V any] struct {
	data atomic.Pointer[map[K]V]
	mu   sync.Mutex // only for writes
}

// ShardedMapCOWOption configures a ShardedMapCOW.
type ShardedMapCOWOption[K comparable, V any] func(*ShardedMapCOW[K, V])

// WithCOWShardCount sets the number of shards.
func WithCOWShardCount[K comparable, V any](count int) ShardedMapCOWOption[K, V] {
	return func(sm *ShardedMapCOW[K, V]) {
		if count < 1 {
			count = 1
		}
		sm.numShards = uint64(count)
	}
}

// WithCOWHashFunc sets a custom hash function.
func WithCOWHashFunc[K comparable, V any](fn func(K) uint64) ShardedMapCOWOption[K, V] {
	return func(sm *ShardedMapCOW[K, V]) {
		sm.hashFunc = fn
	}
}

// NewShardedMapCOW creates a new copy-on-write sharded map.
func NewShardedMapCOW[K comparable, V any](opts ...ShardedMapCOWOption[K, V]) *ShardedMapCOW[K, V] {
	sm := &ShardedMapCOW[K, V]{
		numShards: defaultNumShards,
	}

	for _, opt := range opts {
		opt(sm)
	}

	sm.shards = make([]cowShard[K, V], sm.numShards)
	for i := range sm.shards {
		m := make(map[K]V)
		sm.shards[i].data.Store(&m)
	}

	if sm.hashFunc == nil {
		sm.hashFunc = defaultCOWHashFunc[K]()
	}

	return sm
}

func defaultCOWHashFunc[K comparable]() func(K) uint64 {
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
		seed := maphash.MakeSeed()
		return func(key K) uint64 {
			var h maphash.Hash
			h.SetSeed(seed)
			h.WriteString(fmt.Sprint(key))
			return h.Sum64()
		}
	}
}

func (sm *ShardedMapCOW[K, V]) getShard(key K) *cowShard[K, V] {
	return &sm.shards[sm.hashFunc(key)%sm.numShards]
}

// Load returns the value for a key. Lock-free read.
func (sm *ShardedMapCOW[K, V]) Load(key K) (V, bool) {
	s := sm.getShard(key)
	m := s.data.Load()
	v, ok := (*m)[key]
	return v, ok
}

// Store sets a value for a key. Copies the entire shard.
func (sm *ShardedMapCOW[K, V]) Store(key K, value V) {
	s := sm.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.data.Load()
	newMap := make(map[K]V, len(*old)+1)
	maps.Copy(newMap, *old)
	newMap[key] = value
	s.data.Store(&newMap)
}

// LoadOrStore returns the existing value if present, otherwise stores and returns the new value.
// Returns true if loaded, false if stored.
func (sm *ShardedMapCOW[K, V]) LoadOrStore(key K, value V) (V, bool) {
	s := sm.getShard(key)

	// Lock-free read first
	m := s.data.Load()
	if v, ok := (*m)[key]; ok {
		return v, true
	}

	// Need to write
	s.mu.Lock()
	defer s.mu.Unlock()

	// Double-check after lock
	m = s.data.Load()
	if v, ok := (*m)[key]; ok {
		return v, true
	}

	newMap := make(map[K]V, len(*m)+1)
	maps.Copy(newMap, *m)
	newMap[key] = value
	s.data.Store(&newMap)
	return value, false
}

// LoadAndDelete atomically loads and deletes a key.
func (sm *ShardedMapCOW[K, V]) LoadAndDelete(key K) (V, bool) {
	s := sm.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.data.Load()
	v, ok := (*old)[key]
	if !ok {
		return v, false
	}

	newMap := make(map[K]V, len(*old)-1)
	for k, val := range *old {
		if k != key {
			newMap[k] = val
		}
	}
	s.data.Store(&newMap)
	return v, true
}

// Delete removes a key from the map.
func (sm *ShardedMapCOW[K, V]) Delete(key K) {
	s := sm.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.data.Load()
	if _, ok := (*old)[key]; !ok {
		return // key doesn't exist, no copy needed
	}

	newMap := make(map[K]V, len(*old)-1)
	for k, v := range *old {
		if k != key {
			newMap[k] = v
		}
	}
	s.data.Store(&newMap)
}

// Range calls f for each key-value pair. Lock-free iteration.
// Note: Iterates over a snapshot, so concurrent writes won't affect this iteration.
func (sm *ShardedMapCOW[K, V]) Range(f func(key K, value V) bool) {
	for i := range sm.shards {
		m := sm.shards[i].data.Load()
		for k, v := range *m {
			if !f(k, v) {
				return
			}
		}
	}
}

// Len returns the total number of entries. Lock-free but not atomic across shards.
func (sm *ShardedMapCOW[K, V]) Len() int {
	count := 0
	for i := range sm.shards {
		m := sm.shards[i].data.Load()
		count += len(*m)
	}
	return count
}

// DeleteIf deletes entries where predicate returns true.
func (sm *ShardedMapCOW[K, V]) DeleteIf(predicate func(key K, value V) bool) int {
	deleted := 0
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.Lock()

		old := s.data.Load()
		var toDelete []K
		for k, v := range *old {
			if predicate(k, v) {
				toDelete = append(toDelete, k)
			}
		}

		if len(toDelete) > 0 {
			newMap := make(map[K]V, len(*old)-len(toDelete))
			for k, v := range *old {
				if !slices.Contains(toDelete, k) {
					newMap[k] = v
				}
			}
			s.data.Store(&newMap)
			deleted += len(toDelete)
		}

		s.mu.Unlock()
	}
	return deleted
}

// Clear removes all entries from the map.
func (sm *ShardedMapCOW[K, V]) Clear() {
	for i := range sm.shards {
		s := &sm.shards[i]
		s.mu.Lock()
		m := make(map[K]V)
		s.data.Store(&m)
		s.mu.Unlock()
	}
}

// Keys returns all keys. Lock-free snapshot.
func (sm *ShardedMapCOW[K, V]) Keys() []K {
	keys := make([]K, 0, sm.Len())
	sm.Range(func(key K, _ V) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}

// Compute atomically computes a new value for a key.
func (sm *ShardedMapCOW[K, V]) Compute(key K, f func(oldValue V, exists bool) (newValue V, store bool)) (V, bool) {
	s := sm.getShard(key)
	s.mu.Lock()
	defer s.mu.Unlock()

	old := s.data.Load()
	oldValue, exists := (*old)[key]
	newValue, store := f(oldValue, exists)

	if store {
		newMap := make(map[K]V, len(*old)+1)
		maps.Copy(newMap, *old)
		newMap[key] = newValue
		s.data.Store(&newMap)
		return newValue, true
	} else if exists {
		newMap := make(map[K]V, len(*old)-1)
		maps.Copy(newMap, *old)
		delete(newMap, key)
		s.data.Store(&newMap)
	}

	var zero V
	return zero, false
}

// Snapshot returns a copy of all entries. Lock-free.
func (sm *ShardedMapCOW[K, V]) Snapshot() map[K]V {
	result := make(map[K]V, sm.Len())
	sm.Range(func(k K, v V) bool {
		result[k] = v
		return true
	})
	return result
}
