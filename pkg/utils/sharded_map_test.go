// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestShardedMap_BasicOperations(t *testing.T) {
	sm := NewShardedMap[int]()

	// Test Store and Load
	sm.Store("key1", 100)
	sm.Store("key2", 200)

	v1, ok1 := sm.Load("key1")
	assert.True(t, ok1)
	assert.Equal(t, 100, v1)

	v2, ok2 := sm.Load("key2")
	assert.True(t, ok2)
	assert.Equal(t, 200, v2)

	// Test missing key
	_, ok3 := sm.Load("missing")
	assert.False(t, ok3)

	// Test Len
	assert.Equal(t, 2, sm.Len())

	// Test Delete
	sm.Delete("key1")
	_, ok4 := sm.Load("key1")
	assert.False(t, ok4)
	assert.Equal(t, 1, sm.Len())
}

func TestShardedMap_LoadOrStore(t *testing.T) {
	sm := NewShardedMap[string]()

	// First call should store
	v1, loaded1 := sm.LoadOrStore("key", "value1")
	assert.False(t, loaded1)
	assert.Equal(t, "value1", v1)

	// Second call should load existing
	v2, loaded2 := sm.LoadOrStore("key", "value2")
	assert.True(t, loaded2)
	assert.Equal(t, "value1", v2) // Original value, not new one
}

func TestShardedMap_Range(t *testing.T) {
	sm := NewShardedMap[int]()

	for i := 0; i < 100; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}

	count := 0
	sm.Range(func(key string, value int) bool {
		count++
		return true
	})
	assert.Equal(t, 100, count)

	// Test early termination
	count = 0
	sm.Range(func(key string, value int) bool {
		count++
		return count < 10
	})
	assert.Equal(t, 10, count)
}

func TestShardedMap_DeleteIf(t *testing.T) {
	sm := NewShardedMap[int]()

	for i := 0; i < 100; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}

	// Delete even values
	deleted := sm.DeleteIf(func(_ string, v int) bool {
		return v%2 == 0
	})

	assert.Equal(t, 50, deleted)
	assert.Equal(t, 50, sm.Len())

	// Verify all remaining are odd
	sm.Range(func(_ string, v int) bool {
		assert.Equal(t, 1, v%2)
		return true
	})
}

func TestShardedMap_Clear(t *testing.T) {
	sm := NewShardedMap[int]()

	for i := 0; i < 100; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}
	assert.Equal(t, 100, sm.Len())

	sm.Clear()
	assert.Equal(t, 0, sm.Len())
}

func TestShardedMap_Keys(t *testing.T) {
	sm := NewShardedMap[int]()

	sm.Store("a", 1)
	sm.Store("b", 2)
	sm.Store("c", 3)

	keys := sm.Keys()
	assert.Len(t, keys, 3)
	assert.ElementsMatch(t, []string{"a", "b", "c"}, keys)
}

func TestShardedMap_Concurrent(t *testing.T) {
	sm := NewShardedMap[int]()
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sm.Store(fmt.Sprintf("key%d", n), n)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 100, sm.Len())

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			v, ok := sm.Load(fmt.Sprintf("key%d", n))
			assert.True(t, ok)
			assert.Equal(t, n, v)
		}(i)
	}
	wg.Wait()

	// Concurrent LoadOrStore
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sm.LoadOrStore(fmt.Sprintf("new%d", n), n)
		}(i)
	}
	wg.Wait()

	assert.Equal(t, 200, sm.Len())
}

func BenchmarkShardedMap_Store(b *testing.B) {
	sm := NewShardedMap[int]()
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Store(fmt.Sprintf("key%d", i), i)
			i++
		}
	})
}

func BenchmarkShardedMap_Load(b *testing.B) {
	sm := NewShardedMap[int]()
	for i := 0; i < 10000; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Load(fmt.Sprintf("key%d", i%10000))
			i++
		}
	})
}

func BenchmarkSyncMap_Store(b *testing.B) {
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

func BenchmarkSyncMap_Load(b *testing.B) {
	var sm sync.Map
	for i := 0; i < 10000; i++ {
		sm.Store(fmt.Sprintf("key%d", i), i)
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			sm.Load(fmt.Sprintf("key%d", i%10000))
			i++
		}
	})
}
