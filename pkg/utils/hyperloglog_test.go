// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"fmt"
	"math"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHyperLogLog_BasicOperations(t *testing.T) {
	hll := NewHyperLogLog(14)

	// Add some items
	hll.Add("user1")
	hll.Add("user2")
	hll.Add("user3")

	count := hll.Count()
	t.Logf("Added 3 items, estimated count: %d", count)

	// With precision 14, error should be ~0.8%
	assert.InDelta(t, 3, count, 1, "Count should be approximately 3")
}

func TestHyperLogLog_Duplicates(t *testing.T) {
	hll := NewHyperLogLog(14)

	// Add same item multiple times
	for i := 0; i < 1000; i++ {
		hll.Add("same_item")
	}

	count := hll.Count()
	t.Logf("Added same item 1000 times, estimated count: %d", count)

	// Should still be 1
	assert.Equal(t, uint64(1), count)
}

func TestHyperLogLog_LargeCardinality(t *testing.T) {
	hll := NewHyperLogLog(14)

	// Add many unique items
	n := 100000
	for i := 0; i < n; i++ {
		hll.Add(fmt.Sprintf("item_%d", i))
	}

	count := hll.Count()
	errorRate := math.Abs(float64(count)-float64(n)) / float64(n)
	theoreticalError := hll.ErrorRate()

	t.Logf("Added %d items, estimated count: %d, error: %.2f%%, theoretical: %.2f%%",
		n, count, errorRate*100, theoreticalError*100)

	// Allow 3x theoretical error for statistical variation
	assert.Less(t, errorRate, theoreticalError*3,
		"Error rate too high: %.2f%% (theoretical: %.2f%%)", errorRate*100, theoreticalError*100)
}

func TestHyperLogLog_Merge(t *testing.T) {
	hll1 := NewHyperLogLog(14)
	hll2 := NewHyperLogLog(14)

	// Add items to first HLL
	for i := 0; i < 1000; i++ {
		hll1.Add(fmt.Sprintf("set1_item_%d", i))
	}

	// Add items to second HLL (some overlap)
	for i := 500; i < 1500; i++ {
		hll2.Add(fmt.Sprintf("set1_item_%d", i))
	}

	// Merge
	hll1.Merge(hll2)

	count := hll1.Count()
	expected := 1500 // 0-1499 unique items

	t.Logf("Merged HLLs, expected ~%d, got %d", expected, count)

	// Should be close to 1500 (allow 10% error for merged estimates)
	errorRate := math.Abs(float64(count)-float64(expected)) / float64(expected)
	assert.Less(t, errorRate, 0.10, "Merged count error too high: %.2f%%", errorRate*100)
}

func TestHyperLogLog_Clear(t *testing.T) {
	hll := NewHyperLogLog(14)

	for i := 0; i < 1000; i++ {
		hll.Add(fmt.Sprintf("item_%d", i))
	}

	assert.Greater(t, hll.Count(), uint64(900))

	hll.Clear()

	assert.Equal(t, uint64(0), hll.Count())
}

func TestHyperLogLog_SizeBytes(t *testing.T) {
	tests := []struct {
		precision uint8
		expected  int
	}{
		{10, 1024},   // 2^10
		{12, 4096},   // 2^12
		{14, 16384},  // 2^14
		{16, 65536},  // 2^16
	}

	for _, tt := range tests {
		hll := NewHyperLogLog(tt.precision)
		assert.Equal(t, tt.expected, hll.SizeBytes(),
			"Precision %d should use %d bytes", tt.precision, tt.expected)
	}
}

func TestHyperLogLog_Concurrent(t *testing.T) {
	hll := NewHyperLogLog(14)
	var wg sync.WaitGroup

	// Concurrent adds
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				hll.Add(fmt.Sprintf("item_%d_%d", n, j))
			}
		}(i)
	}
	wg.Wait()

	count := hll.Count()
	expected := 100000

	t.Logf("Concurrent adds: expected ~%d, got %d", expected, count)

	errorRate := math.Abs(float64(count)-float64(expected)) / float64(expected)
	assert.Less(t, errorRate, 0.05, "Concurrent count error too high")
}

func TestHyperLogLogSet_BasicOperations(t *testing.T) {
	hlls := NewHyperLogLogSet(14)

	// Add items to different keys
	for i := 0; i < 100; i++ {
		hlls.Add("bucket1", fmt.Sprintf("visitor_%d", i))
	}
	for i := 0; i < 200; i++ {
		hlls.Add("bucket2", fmt.Sprintf("visitor_%d", i))
	}

	count1 := hlls.Count("bucket1")
	count2 := hlls.Count("bucket2")

	t.Logf("bucket1: expected ~100, got %d", count1)
	t.Logf("bucket2: expected ~200, got %d", count2)

	assert.InDelta(t, 100, count1, 10)
	assert.InDelta(t, 200, count2, 20)
	assert.Equal(t, 2, hlls.Len())
}

func TestHyperLogLogSet_Delete(t *testing.T) {
	hlls := NewHyperLogLogSet(14)

	hlls.Add("key1", "item1")
	hlls.Add("key2", "item2")

	assert.Equal(t, 2, hlls.Len())

	hlls.Delete("key1")

	assert.Equal(t, 1, hlls.Len())
	assert.Equal(t, uint64(0), hlls.Count("key1"))
	assert.Greater(t, hlls.Count("key2"), uint64(0))
}

func TestHyperLogLogSet_Keys(t *testing.T) {
	hlls := NewHyperLogLogSet(14)

	hlls.Add("bucket_a", "v1")
	hlls.Add("bucket_b", "v2")
	hlls.Add("bucket_c", "v3")

	keys := hlls.Keys()
	assert.Len(t, keys, 3)
	assert.ElementsMatch(t, []string{"bucket_a", "bucket_b", "bucket_c"}, keys)
}

func TestHyperLogLogSet_MergeInto(t *testing.T) {
	hlls := NewHyperLogLogSet(14)

	// Add to hourly bucket
	for i := 0; i < 100; i++ {
		hlls.Add("hour_1", fmt.Sprintf("visitor_%d", i))
	}
	for i := 50; i < 150; i++ {
		hlls.Add("hour_2", fmt.Sprintf("visitor_%d", i))
	}

	// Merge into daily bucket
	hlls.MergeInto("daily", "hour_1")
	hlls.MergeInto("daily", "hour_2")

	dailyCount := hlls.Count("daily")
	expected := 150 // 0-149 unique

	t.Logf("Daily count: expected ~%d, got %d", expected, dailyCount)

	assert.InDelta(t, expected, dailyCount, 15)
}

func BenchmarkHyperLogLog_Add(b *testing.B) {
	hll := NewHyperLogLog(14)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hll.Add(fmt.Sprintf("item_%d", i))
	}
}

func BenchmarkHyperLogLog_Count(b *testing.B) {
	hll := NewHyperLogLog(14)
	for i := 0; i < 100000; i++ {
		hll.Add(fmt.Sprintf("item_%d", i))
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hll.Count()
	}
}

func BenchmarkHyperLogLog_AddParallel(b *testing.B) {
	hll := NewHyperLogLog(14)
	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			hll.Add(fmt.Sprintf("item_%d", i))
			i++
		}
	})
}
