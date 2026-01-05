// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"sync"
	"testing"
)

// BenchmarkMapImplementations compares all map implementations
// Run with: go test -bench=BenchmarkCompare -benchmem ./pkg/utils/...

const benchCompareKeys = 10000

// Setup helpers
func setupBenchShardedMap() *ShardedMap[string, int] {
	sm := NewShardedMap[string, int]()
	for i := 0; i < benchCompareKeys; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}
	return sm
}

func setupBenchShardedMapCOW() *ShardedMapCOW[string, int] {
	sm := NewShardedMapCOW[string, int]()
	for i := 0; i < benchCompareKeys; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}
	return sm
}

func setupBenchLockFreeMap() *LockFreeMap[string, int] {
	sm := NewLockFreeMap[string, int]()
	for i := 0; i < benchCompareKeys; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}
	return sm
}

func setupBenchSyncMap() *sync.Map {
	var sm sync.Map
	for i := 0; i < benchCompareKeys; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}
	return &sm
}

// ============== Load (Read) Benchmarks ==============

func BenchmarkCompare_1_ShardedMap_Load(b *testing.B) {
	sm := setupBenchShardedMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Load(fmt.Sprintf("key%d", i%benchCompareKeys))
			i++
		}
	})
}

func BenchmarkCompare_2_ShardedMapCOW_Load(b *testing.B) {
	sm := setupBenchShardedMapCOW()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Load(fmt.Sprintf("key%d", i%benchCompareKeys))
			i++
		}
	})
}

func BenchmarkCompare_3_LockFreeMap_Load(b *testing.B) {
	sm := setupBenchLockFreeMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Load(fmt.Sprintf("key%d", i%benchCompareKeys))
			i++
		}
	})
}

func BenchmarkCompare_4_SyncMap_Load(b *testing.B) {
	sm := setupBenchSyncMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Load(fmt.Sprintf("key%d", i%benchCompareKeys))
			i++
		}
	})
}

// ============== Store (Write) Benchmarks ==============

func BenchmarkCompare_1_ShardedMap_Store(b *testing.B) {
	sm := NewShardedMap[string, int]()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Store(fmt.Sprintf("key%d", i), i)
			i++
		}
	})
}

func BenchmarkCompare_2_ShardedMapCOW_Store(b *testing.B) {
	sm := NewShardedMapCOW[string, int]()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Store(fmt.Sprintf("key%d", i), i)
			i++
		}
	})
}

func BenchmarkCompare_3_LockFreeMap_Store(b *testing.B) {
	sm := NewLockFreeMap[string, int]()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Store(fmt.Sprintf("key%d", i), i)
			i++
		}
	})
}

func BenchmarkCompare_4_SyncMap_Store(b *testing.B) {
	var sm sync.Map
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Store(fmt.Sprintf("key%d", i), i)
			i++
		}
	})
}

// ============== Mixed Read/Write (90% read, 10% write) ==============

func BenchmarkCompare_1_ShardedMap_Mixed90Read(b *testing.B) {
	sm := setupBenchShardedMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				sm.Store(fmt.Sprintf("key%d", i%benchCompareKeys), i)
			} else {
				sm.Load(fmt.Sprintf("key%d", i%benchCompareKeys))
			}
			i++
		}
	})
}

func BenchmarkCompare_2_ShardedMapCOW_Mixed90Read(b *testing.B) {
	sm := setupBenchShardedMapCOW()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				sm.Store(fmt.Sprintf("key%d", i%benchCompareKeys), i)
			} else {
				sm.Load(fmt.Sprintf("key%d", i%benchCompareKeys))
			}
			i++
		}
	})
}

func BenchmarkCompare_3_LockFreeMap_Mixed90Read(b *testing.B) {
	sm := setupBenchLockFreeMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				sm.Store(fmt.Sprintf("key%d", i%benchCompareKeys), i)
			} else {
				sm.Load(fmt.Sprintf("key%d", i%benchCompareKeys))
			}
			i++
		}
	})
}

func BenchmarkCompare_4_SyncMap_Mixed90Read(b *testing.B) {
	sm := setupBenchSyncMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if i%10 == 0 {
				sm.Store(fmt.Sprintf("key%d", i%benchCompareKeys), i)
			} else {
				sm.Load(fmt.Sprintf("key%d", i%benchCompareKeys))
			}
			i++
		}
	})
}

// ============== LoadOrStore Benchmarks ==============

func BenchmarkCompare_1_ShardedMap_LoadOrStore(b *testing.B) {
	sm := setupBenchShardedMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			// 50% existing keys, 50% new keys
			key := fmt.Sprintf("key%d", i%(benchCompareKeys*2))
			sm.LoadOrStore(key, i)
			i++
		}
	})
}

func BenchmarkCompare_2_ShardedMapCOW_LoadOrStore(b *testing.B) {
	sm := setupBenchShardedMapCOW()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%(benchCompareKeys*2))
			sm.LoadOrStore(key, i)
			i++
		}
	})
}

func BenchmarkCompare_3_LockFreeMap_LoadOrStore(b *testing.B) {
	sm := setupBenchLockFreeMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%(benchCompareKeys*2))
			sm.LoadOrStore(key, i)
			i++
		}
	})
}

func BenchmarkCompare_4_SyncMap_LoadOrStore(b *testing.B) {
	sm := setupBenchSyncMap()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key%d", i%(benchCompareKeys*2))
			sm.LoadOrStore(key, i)
			i++
		}
	})
}
