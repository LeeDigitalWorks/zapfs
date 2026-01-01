package utils

import (
	"hash/fnv"
	"sync"
)

const numShards = 256

// ShardedMap is a concurrent map with sharding for reduced lock contention.
// Uses FNV-1a hash to distribute keys across shards.
// More efficient than sync.Map for high-throughput scenarios with mixed reads/writes.
type ShardedMap[V any] struct {
	shards [numShards]shard[V]
}

type shard[V any] struct {
	sync.RWMutex
	m map[string]V
}

// NewShardedMap creates a new sharded map.
func NewShardedMap[V any]() *ShardedMap[V] {
	sm := &ShardedMap[V]{}
	for i := range sm.shards {
		sm.shards[i].m = make(map[string]V)
	}
	return sm
}

// getShard returns the shard for a given key.
func (sm *ShardedMap[V]) getShard(key string) *shard[V] {
	h := fnv.New32a()
	h.Write([]byte(key))
	return &sm.shards[h.Sum32()%numShards]
}

// Load returns the value for a key, or the zero value if not found.
func (sm *ShardedMap[V]) Load(key string) (V, bool) {
	s := sm.getShard(key)
	s.RLock()
	v, ok := s.m[key]
	s.RUnlock()
	return v, ok
}

// Store sets a value for a key.
func (sm *ShardedMap[V]) Store(key string, value V) {
	s := sm.getShard(key)
	s.Lock()
	s.m[key] = value
	s.Unlock()
}

// LoadOrStore returns the existing value if present, otherwise stores and returns the new value.
// Returns true if the value was loaded, false if stored.
func (sm *ShardedMap[V]) LoadOrStore(key string, value V) (V, bool) {
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

// Delete removes a key from the map.
func (sm *ShardedMap[V]) Delete(key string) {
	s := sm.getShard(key)
	s.Lock()
	delete(s.m, key)
	s.Unlock()
}

// Range calls f for each key-value pair in the map.
// If f returns false, iteration stops.
func (sm *ShardedMap[V]) Range(f func(key string, value V) bool) {
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
func (sm *ShardedMap[V]) Len() int {
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
func (sm *ShardedMap[V]) DeleteIf(predicate func(key string, value V) bool) int {
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
func (sm *ShardedMap[V]) Clear() {
	for i := range sm.shards {
		s := &sm.shards[i]
		s.Lock()
		s.m = make(map[string]V)
		s.Unlock()
	}
}

// Keys returns all keys in the map.
// Note: This is not atomic - keys may be added/removed during iteration.
func (sm *ShardedMap[V]) Keys() []string {
	keys := make([]string, 0, sm.Len())
	sm.Range(func(key string, _ V) bool {
		keys = append(keys, key)
		return true
	})
	return keys
}
