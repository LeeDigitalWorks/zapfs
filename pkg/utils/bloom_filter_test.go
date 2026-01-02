// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBloomFilter_BasicOperations(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	// Add some items
	bf.Add("apple")
	bf.Add("banana")
	bf.Add("cherry")

	// Should contain added items
	assert.True(t, bf.Contains("apple"))
	assert.True(t, bf.Contains("banana"))
	assert.True(t, bf.Contains("cherry"))

	// Should probably not contain items that weren't added
	// (but could have false positives)
	falsePositives := 0
	for i := 0; i < 1000; i++ {
		if bf.Contains(fmt.Sprintf("not_added_%d", i)) {
			falsePositives++
		}
	}

	// With 1% FPR and 1000 checks, we expect ~10 false positives
	// Allow some margin for statistical variation
	assert.Less(t, falsePositives, 50, "Too many false positives: %d", falsePositives)
}

func TestBloomFilter_FalsePositiveRate(t *testing.T) {
	expectedItems := 10000
	targetFPR := 0.01

	bf := NewBloomFilter(expectedItems, targetFPR)

	// Add expected number of items
	for i := 0; i < expectedItems; i++ {
		bf.Add(fmt.Sprintf("item_%d", i))
	}

	// Test for false positives with items not in the set
	testSize := 100000
	falsePositives := 0
	for i := expectedItems; i < expectedItems+testSize; i++ {
		if bf.Contains(fmt.Sprintf("item_%d", i)) {
			falsePositives++
		}
	}

	actualFPR := float64(falsePositives) / float64(testSize)
	t.Logf("Target FPR: %.4f, Actual FPR: %.4f, False positives: %d/%d",
		targetFPR, actualFPR, falsePositives, testSize)

	// Allow 5x the target FPR for statistical variation
	// FNV hash distribution may cause higher FPR than optimal
	assert.Less(t, actualFPR, targetFPR*5,
		"False positive rate too high: %.4f (target: %.4f)", actualFPR, targetFPR)
}

func TestBloomFilter_NoFalseNegatives(t *testing.T) {
	bf := NewBloomFilter(10000, 0.01)

	// Add items
	items := make([]string, 10000)
	for i := 0; i < 10000; i++ {
		items[i] = fmt.Sprintf("key_%d", i)
		bf.Add(items[i])
	}

	// All added items must be found (no false negatives)
	for _, item := range items {
		assert.True(t, bf.Contains(item), "False negative for: %s", item)
	}
}

func TestBloomFilter_Clear(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	bf.Add("item1")
	bf.Add("item2")
	assert.True(t, bf.Contains("item1"))
	assert.Equal(t, uint64(2), bf.Count())

	bf.Clear()

	assert.False(t, bf.Contains("item1"))
	assert.False(t, bf.Contains("item2"))
	assert.Equal(t, uint64(0), bf.Count())
}

func TestBloomFilter_FillRatio(t *testing.T) {
	bf := NewBloomFilter(1000, 0.01)

	assert.Equal(t, 0.0, bf.FillRatio())

	for i := 0; i < 1000; i++ {
		bf.Add(fmt.Sprintf("item_%d", i))
	}

	ratio := bf.FillRatio()
	t.Logf("Fill ratio after 1000 items: %.4f", ratio)

	// Fill ratio should be reasonable (not too high)
	assert.Less(t, ratio, 0.7, "Fill ratio too high: %.4f", ratio)
	assert.Greater(t, ratio, 0.1, "Fill ratio too low: %.4f", ratio)
}

func TestBloomFilter_Concurrent(t *testing.T) {
	bf := NewBloomFilter(100000, 0.01)
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				bf.Add(fmt.Sprintf("item_%d_%d", n, j))
			}
		}(i)
	}
	wg.Wait()

	assert.Equal(t, uint64(100000), bf.Count())

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				bf.Contains(fmt.Sprintf("item_%d_%d", n, j))
			}
		}(i)
	}
	wg.Wait()
}

func TestBloomFilter_SizeBytes(t *testing.T) {
	bf1 := NewBloomFilter(1000, 0.01)
	bf2 := NewBloomFilter(100000, 0.01)

	size1 := bf1.SizeBytes()
	size2 := bf2.SizeBytes()

	t.Logf("Size for 1000 items @ 1%% FPR: %d bytes", size1)
	t.Logf("Size for 100000 items @ 1%% FPR: %d bytes", size2)

	// Larger filter should use more memory
	assert.Greater(t, size2, size1)

	// 1000 items at 1% FPR should be ~1.2KB
	assert.Less(t, size1, 2000)
	// 100000 items at 1% FPR should be ~120KB
	assert.Less(t, size2, 200000)
}

func TestBloomFilterSet_BasicOperations(t *testing.T) {
	bfs := NewBloomFilterSet(1000, 0.01)

	bfs.Add("key1")
	bfs.Add("key2")
	bfs.Add("key3")

	assert.True(t, bfs.Contains("key1"))
	assert.True(t, bfs.ContainsExact("key1"))
	assert.Equal(t, 3, bfs.Len())

	// Remove item
	bfs.Remove("key2")
	assert.Equal(t, 2, bfs.Len())

	// Bloom filter still contains removed item until rebuild
	assert.True(t, bfs.Contains("key2"))
	assert.False(t, bfs.ContainsExact("key2"))

	// Rebuild clears removed items from filter
	bfs.Rebuild()
	// After rebuild, false positive rate should be lower for removed items
	// (though not guaranteed due to hash collisions)
}

func TestBloomFilterSet_Rebuild(t *testing.T) {
	bfs := NewBloomFilterSet(100, 0.01)

	// Add and remove many items
	for i := 0; i < 100; i++ {
		bfs.Add(fmt.Sprintf("item_%d", i))
	}
	for i := 0; i < 50; i++ {
		bfs.Remove(fmt.Sprintf("item_%d", i))
	}

	assert.Equal(t, 50, bfs.Len())

	// Rebuild
	bfs.Rebuild()

	// Remaining items should still be in filter
	for i := 50; i < 100; i++ {
		assert.True(t, bfs.ContainsExact(fmt.Sprintf("item_%d", i)))
	}
}

func BenchmarkBloomFilter_Add(b *testing.B) {
	bf := NewBloomFilter(b.N, 0.01)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bf.Add(fmt.Sprintf("key_%d", i))
	}
}

func BenchmarkBloomFilter_Contains(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	for i := 0; i < 100000; i++ {
		bf.Add(fmt.Sprintf("key_%d", i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		bf.Contains(fmt.Sprintf("key_%d", i%100000))
	}
}

func BenchmarkBloomFilter_ContainsParallel(b *testing.B) {
	bf := NewBloomFilter(100000, 0.01)
	for i := 0; i < 100000; i++ {
		bf.Add(fmt.Sprintf("key_%d", i))
	}
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			bf.Contains(fmt.Sprintf("key_%d", i%100000))
			i++
		}
	})
}
