// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestVisitorTracker_BasicOperations(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	// Record some visits
	vt.RecordVisit("bucket1", "192.168.1.1")
	vt.RecordVisit("bucket1", "192.168.1.2")
	vt.RecordVisit("bucket1", "192.168.1.3")

	vt.RecordVisit("bucket2", "10.0.0.1")
	vt.RecordVisit("bucket2", "10.0.0.2")

	// Check counts
	count1 := vt.GetUniqueCount("bucket1")
	count2 := vt.GetUniqueCount("bucket2")

	assert.Equal(t, uint64(3), count1, "bucket1 should have 3 unique visitors")
	assert.Equal(t, uint64(2), count2, "bucket2 should have 2 unique visitors")

	// Non-existent bucket
	count3 := vt.GetUniqueCount("bucket3")
	assert.Equal(t, uint64(0), count3)
}

func TestVisitorTracker_Duplicates(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	// Record same IP multiple times
	for i := 0; i < 100; i++ {
		vt.RecordVisit("bucket", "192.168.1.1")
	}

	count := vt.GetUniqueCount("bucket")
	assert.Equal(t, uint64(1), count, "should count as 1 unique visitor")
}

func TestVisitorTracker_ManyVisitors(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	// Record many unique visitors
	n := 10000
	for i := 0; i < n; i++ {
		vt.RecordVisit("bucket", fmt.Sprintf("192.168.%d.%d", i/256, i%256))
	}

	count := vt.GetUniqueCount("bucket")

	// HyperLogLog should be accurate within ~5%
	errorRate := float64(int64(count)-int64(n)) / float64(n)
	if errorRate < 0 {
		errorRate = -errorRate
	}

	t.Logf("Expected %d, got %d (error: %.2f%%)", n, count, errorRate*100)
	assert.Less(t, errorRate, 0.05, "error rate should be less than 5%%")
}

func TestVisitorTracker_EmptyInputs(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	// Empty inputs should be ignored
	vt.RecordVisit("", "192.168.1.1")
	vt.RecordVisit("bucket", "")
	vt.RecordVisit("", "")

	assert.Equal(t, 0, vt.BucketCount())
}

func TestVisitorTracker_GetAllCounts(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	vt.RecordVisit("a", "1.1.1.1")
	vt.RecordVisit("b", "2.2.2.2")
	vt.RecordVisit("b", "3.3.3.3")
	vt.RecordVisit("c", "4.4.4.4")

	counts := vt.GetAllCounts()

	assert.Len(t, counts, 3)
	assert.Equal(t, uint64(1), counts["a"])
	assert.Equal(t, uint64(2), counts["b"])
	assert.Equal(t, uint64(1), counts["c"])
}

func TestVisitorTracker_DeleteBucket(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	vt.RecordVisit("keep", "1.1.1.1")
	vt.RecordVisit("delete", "2.2.2.2")

	assert.Equal(t, 2, vt.BucketCount())

	vt.DeleteBucket("delete")

	assert.Equal(t, 1, vt.BucketCount())
	assert.Equal(t, uint64(0), vt.GetUniqueCount("delete"))
	assert.Equal(t, uint64(1), vt.GetUniqueCount("keep"))
}

func TestVisitorTracker_Clear(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	vt.RecordVisit("a", "1.1.1.1")
	vt.RecordVisit("b", "2.2.2.2")

	assert.Equal(t, 2, vt.BucketCount())

	vt.Clear()

	assert.Equal(t, 0, vt.BucketCount())
}

func TestVisitorTracker_Snapshot(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	vt.RecordVisit("a", "1.1.1.1")
	vt.RecordVisit("a", "2.2.2.2")
	vt.RecordVisit("b", "3.3.3.3")

	snapshot := vt.Snapshot()

	assert.Len(t, snapshot.Buckets, 2)
	assert.Equal(t, uint64(2), snapshot.Buckets["a"])
	assert.Equal(t, uint64(1), snapshot.Buckets["b"])
	assert.Equal(t, uint64(3), snapshot.TotalCount)
	assert.False(t, snapshot.AsOf.IsZero())
}

func TestVisitorTracker_MemoryUsage(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)

	// Each bucket uses ~32KB (2 HLLs at 16KB each)
	vt.RecordVisit("a", "1.1.1.1")
	vt.RecordVisit("b", "2.2.2.2")
	vt.RecordVisit("c", "3.3.3.3")

	mem := vt.MemoryUsage()
	expected := 3 * 2 * 16384 // 3 buckets * 2 HLLs * 16KB

	assert.Equal(t, expected, mem)
}

func TestVisitorTracker_Concurrent(t *testing.T) {
	vt := NewVisitorTracker(time.Hour)
	var wg sync.WaitGroup

	// Concurrent writes
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				vt.RecordVisit(
					fmt.Sprintf("bucket_%d", n%10),
					fmt.Sprintf("192.168.%d.%d", n, j),
				)
			}
		}(i)
	}
	wg.Wait()

	// Should have 10 buckets
	assert.Equal(t, 10, vt.BucketCount())

	// Concurrent reads
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			vt.GetUniqueCount(fmt.Sprintf("bucket_%d", n%10))
			vt.GetAllCounts()
			vt.Snapshot()
		}(i)
	}
	wg.Wait()
}

func TestVisitorTracker_GlobalInstance(t *testing.T) {
	tracker1 := GetVisitorTracker()
	tracker2 := GetVisitorTracker()

	// Should return the same instance
	assert.Same(t, tracker1, tracker2)
}
