package usage

import (
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// VisitorTracker tracks unique visitors (IPs) per bucket using HyperLogLog.
// This provides approximate unique visitor counts with minimal memory overhead.
//
// Memory usage per bucket: ~16KB (precision 14)
// Error rate: ~0.8%
//
// Usage:
//
//	tracker := NewVisitorTracker()
//	tracker.RecordVisit("my-bucket", "192.168.1.1")
//	tracker.RecordVisit("my-bucket", "192.168.1.2")
//	count := tracker.GetUniqueCount("my-bucket") // ~2
type VisitorTracker struct {
	mu       sync.RWMutex
	hlls     map[string]*bucketHLL
	interval time.Duration
}

// bucketHLL holds HyperLogLogs for current and previous periods.
type bucketHLL struct {
	current  *utils.HyperLogLog
	previous *utils.HyperLogLog
	rollover time.Time
}

// NewVisitorTracker creates a new visitor tracker.
// The interval specifies how often the HLLs roll over (e.g., hourly, daily).
func NewVisitorTracker(interval time.Duration) *VisitorTracker {
	if interval <= 0 {
		interval = time.Hour
	}
	return &VisitorTracker{
		hlls:     make(map[string]*bucketHLL),
		interval: interval,
	}
}

// RecordVisit records a visit from clientIP to the specified bucket.
func (vt *VisitorTracker) RecordVisit(bucket, clientIP string) {
	if bucket == "" || clientIP == "" {
		return
	}

	vt.mu.Lock()
	defer vt.mu.Unlock()

	bh, exists := vt.hlls[bucket]
	if !exists {
		bh = &bucketHLL{
			current:  utils.NewHyperLogLog(14),
			previous: utils.NewHyperLogLog(14),
			rollover: time.Now().Add(vt.interval),
		}
		vt.hlls[bucket] = bh
	}

	// Check if we need to roll over
	if time.Now().After(bh.rollover) {
		bh.previous = bh.current
		bh.current = utils.NewHyperLogLog(14)
		bh.rollover = time.Now().Add(vt.interval)
	}

	bh.current.Add(clientIP)
}

// GetUniqueCount returns the estimated unique visitor count for a bucket
// in the current period.
func (vt *VisitorTracker) GetUniqueCount(bucket string) uint64 {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	bh, exists := vt.hlls[bucket]
	if !exists {
		return 0
	}

	return bh.current.Count()
}

// GetPreviousCount returns the estimated unique visitor count for a bucket
// in the previous period.
func (vt *VisitorTracker) GetPreviousCount(bucket string) uint64 {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	bh, exists := vt.hlls[bucket]
	if !exists {
		return 0
	}

	return bh.previous.Count()
}

// GetAllCounts returns unique visitor counts for all tracked buckets.
func (vt *VisitorTracker) GetAllCounts() map[string]uint64 {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	counts := make(map[string]uint64, len(vt.hlls))
	for bucket, bh := range vt.hlls {
		counts[bucket] = bh.current.Count()
	}
	return counts
}

// Clear removes all tracking data.
func (vt *VisitorTracker) Clear() {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	vt.hlls = make(map[string]*bucketHLL)
}

// DeleteBucket removes tracking data for a specific bucket.
func (vt *VisitorTracker) DeleteBucket(bucket string) {
	vt.mu.Lock()
	defer vt.mu.Unlock()

	delete(vt.hlls, bucket)
}

// BucketCount returns the number of buckets being tracked.
func (vt *VisitorTracker) BucketCount() int {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	return len(vt.hlls)
}

// MemoryUsage returns the approximate memory usage in bytes.
func (vt *VisitorTracker) MemoryUsage() int {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	// Each bucket has 2 HLLs, each ~16KB at precision 14
	return len(vt.hlls) * 2 * 16384
}

// VisitorSnapshot captures a point-in-time view of unique visitors.
type VisitorSnapshot struct {
	AsOf       time.Time
	Buckets    map[string]uint64
	TotalCount uint64
}

// Snapshot returns a point-in-time snapshot of all unique visitor counts.
func (vt *VisitorTracker) Snapshot() VisitorSnapshot {
	vt.mu.RLock()
	defer vt.mu.RUnlock()

	snapshot := VisitorSnapshot{
		AsOf:    time.Now(),
		Buckets: make(map[string]uint64, len(vt.hlls)),
	}

	for bucket, bh := range vt.hlls {
		count := bh.current.Count()
		snapshot.Buckets[bucket] = count
		snapshot.TotalCount += count
	}

	return snapshot
}

// Global visitor tracker instance (optional, can be overridden)
var globalVisitorTracker *VisitorTracker
var globalVisitorTrackerOnce sync.Once

// GetVisitorTracker returns the global visitor tracker instance.
func GetVisitorTracker() *VisitorTracker {
	globalVisitorTrackerOnce.Do(func() {
		globalVisitorTracker = NewVisitorTracker(time.Hour)
	})
	return globalVisitorTracker
}
