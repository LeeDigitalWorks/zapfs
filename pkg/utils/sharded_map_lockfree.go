// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"github.com/puzpuzpuz/xsync/v3"
)

// LockFreeMap is a high-performance concurrent map using xsync.MapOf.
// Provides lock-free reads and fine-grained locking for writes.
// Best for mixed read/write workloads with high concurrency.
//
// Performance characteristics:
//   - Reads: Lock-free O(1)
//   - Writes: Fine-grained locking O(1)
//   - Memory: Optimized for concurrent access
//
// This is a thin wrapper around xsync.MapOf to provide a consistent API
// with ShardedMap and ShardedMapCOW.
type LockFreeMap[K comparable, V any] struct {
	m *xsync.MapOf[K, V]
}

// LockFreeMapOption configures a LockFreeMap.
type LockFreeMapOption[K comparable, V any] func(*xsync.MapConfig)

// WithLockFreeSize sets the initial size hint for the map.
func WithLockFreeSize[K comparable, V any](size int) LockFreeMapOption[K, V] {
	return func(cfg *xsync.MapConfig) {
		xsync.WithPresize(size)(cfg)
	}
}

// WithLockFreeGrowOnly disables shrinking, useful for maps that only grow.
func WithLockFreeGrowOnly[K comparable, V any]() LockFreeMapOption[K, V] {
	return func(cfg *xsync.MapConfig) {
		xsync.WithGrowOnly()(cfg)
	}
}

// NewLockFreeMap creates a new lock-free map.
func NewLockFreeMap[K comparable, V any](opts ...LockFreeMapOption[K, V]) *LockFreeMap[K, V] {
	// Convert our options to xsync options
	xsyncOpts := make([]func(*xsync.MapConfig), len(opts))
	for i, opt := range opts {
		xsyncOpts[i] = func(cfg *xsync.MapConfig) {
			opt(cfg)
		}
	}

	return &LockFreeMap[K, V]{
		m: xsync.NewMapOf[K, V](xsyncOpts...),
	}
}

// Load returns the value for a key. Lock-free.
func (lf *LockFreeMap[K, V]) Load(key K) (V, bool) {
	return lf.m.Load(key)
}

// Store sets a value for a key.
func (lf *LockFreeMap[K, V]) Store(key K, value V) {
	lf.m.Store(key, value)
}

// LoadOrStore returns the existing value if present, otherwise stores and returns the new value.
// Returns true if loaded, false if stored.
func (lf *LockFreeMap[K, V]) LoadOrStore(key K, value V) (V, bool) {
	return lf.m.LoadOrStore(key, value)
}

// LoadAndStore atomically loads the old value and stores a new value.
// Returns the old value and whether it existed.
func (lf *LockFreeMap[K, V]) LoadAndStore(key K, value V) (V, bool) {
	return lf.m.LoadAndStore(key, value)
}

// LoadOrCompute returns the existing value if present, otherwise computes and stores a new value.
// The compute function is called exactly once if the key doesn't exist.
func (lf *LockFreeMap[K, V]) LoadOrCompute(key K, compute func() V) (V, bool) {
	return lf.m.LoadOrCompute(key, compute)
}

// LoadAndDelete atomically loads and deletes a key.
func (lf *LockFreeMap[K, V]) LoadAndDelete(key K) (V, bool) {
	return lf.m.LoadAndDelete(key)
}

// Delete removes a key from the map.
func (lf *LockFreeMap[K, V]) Delete(key K) {
	lf.m.Delete(key)
}

// Range calls f for each key-value pair.
// If f returns false, iteration stops.
func (lf *LockFreeMap[K, V]) Range(f func(key K, value V) bool) {
	lf.m.Range(f)
}

// Len returns the number of entries. This is an approximation due to concurrent access.
func (lf *LockFreeMap[K, V]) Len() int {
	return lf.m.Size()
}

// Clear removes all entries.
func (lf *LockFreeMap[K, V]) Clear() {
	lf.m.Clear()
}

// Keys returns all keys.
func (lf *LockFreeMap[K, V]) Keys() []K {
	keys := make([]K, 0, lf.Len())
	lf.m.Range(func(k K, _ V) bool {
		keys = append(keys, k)
		return true
	})
	return keys
}

// Compute atomically computes a new value for a key.
// If f returns false, the entry is deleted.
func (lf *LockFreeMap[K, V]) Compute(key K, f func(oldValue V, exists bool) (newValue V, store bool)) (V, bool) {
	var result V
	var stored bool

	lf.m.Compute(key, func(oldValue V, exists bool) (V, bool) {
		result, stored = f(oldValue, exists)
		return result, stored
	})

	return result, stored
}

// DeleteIf deletes entries where predicate returns true.
// Returns the number of deleted entries.
func (lf *LockFreeMap[K, V]) DeleteIf(predicate func(key K, value V) bool) int {
	deleted := 0
	var toDelete []K

	lf.m.Range(func(k K, v V) bool {
		if predicate(k, v) {
			toDelete = append(toDelete, k)
		}
		return true
	})

	for _, k := range toDelete {
		if _, ok := lf.m.LoadAndDelete(k); ok {
			deleted++
		}
	}
	return deleted
}
