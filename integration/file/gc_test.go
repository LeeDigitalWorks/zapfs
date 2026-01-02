//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// computeChunkID computes the chunk ID (SHA-256) for data
func computeChunkID(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// =============================================================================
// GC Integration Tests
// =============================================================================

func TestGC_AdminEndpoints(t *testing.T) {
	t.Parallel()

	adminClient := testutil.NewAdminClient(t, testutil.Addrs.FileServer1Admin)

	// Test index stats endpoint
	stats := adminClient.MustGetIndexStats()
	t.Logf("Index stats: total=%d chunks (%d bytes), zero-ref=%d chunks (%d bytes)",
		stats.TotalChunks, stats.TotalBytes,
		stats.ZeroRefChunks, stats.ZeroRefBytes)

	assert.GreaterOrEqual(t, stats.TotalChunks, int64(0))
	assert.GreaterOrEqual(t, stats.TotalBytes, int64(0))

	// Test force GC endpoint (should work even with empty index)
	gcResp := adminClient.MustForceGC("")
	assert.Equal(t, "ok", gcResp.Status)
	t.Logf("Force GC ran %d workers", gcResp.WorkersRun)

	// Test chunk lookup for non-existent chunk
	_, err := adminClient.GetChunkInfo("nonexistent-chunk-id")
	assert.Error(t, err, "should return error for non-existent chunk")
}

func TestGC_ChunkBasedStorage(t *testing.T) {
	t.Parallel()

	fileClient := newFileClient(t, fileServer1Addr)
	adminClient := testutil.NewAdminClient(t, testutil.Addrs.FileServer1Admin)

	// Get initial stats
	initialStats := adminClient.MustGetIndexStats()
	t.Logf("Initial stats: %d chunks", initialStats.TotalChunks)

	// Upload a small object (will be one chunk)
	objectID := testutil.UniqueID("test-chunk")
	data := testutil.GenerateTestData(t, 1024) // 1KB
	chunkID := computeChunkID(data)

	resp := fileClient.PutObject(objectID, data)
	require.Equal(t, uint64(len(data)), resp.Size)
	t.Logf("Created object %s, expecting chunk %s", objectID, chunkID)

	// Verify chunk was created in index
	chunk, err := adminClient.GetChunkInfo(chunkID)
	require.NoError(t, err, "chunk should exist after PutObject")
	assert.Equal(t, uint32(1), chunk.RefCount, "new chunk should have RefCount=1")
	t.Logf("Chunk %s has RefCount=%d, Size=%d", chunkID, chunk.RefCount, chunk.Size)

	// Verify we can read the object back
	retrieved := fileClient.GetObject(objectID)
	assert.Equal(t, data, retrieved, "retrieved data should match original")

	// Verify stats increased
	newStats := adminClient.MustGetIndexStats()
	assert.Greater(t, newStats.TotalChunks, initialStats.TotalChunks, "should have more chunks")
	t.Logf("Stats after put: %d chunks (+%d)", newStats.TotalChunks, newStats.TotalChunks-initialStats.TotalChunks)
}

func TestGC_Deduplication(t *testing.T) {
	t.Parallel()

	fileClient := newFileClient(t, fileServer1Addr)
	adminClient := testutil.NewAdminClient(t, testutil.Addrs.FileServer1Admin)

	// Upload the same data twice (should deduplicate)
	data := testutil.GenerateTestData(t, 2048) // 2KB
	chunkID := computeChunkID(data)

	objectID1 := testutil.UniqueID("test-dedup-1")
	objectID2 := testutil.UniqueID("test-dedup-2")

	fileClient.PutObject(objectID1, data)
	fileClient.PutObject(objectID2, data)
	t.Logf("Created two objects with same data, chunk %s", chunkID)

	// Verify RefCount is 2 (deduplicated)
	chunk, err := adminClient.GetChunkInfo(chunkID)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), chunk.RefCount, "deduplicated chunk should have RefCount=2")
	t.Logf("Chunk has RefCount=%d (deduplicated)", chunk.RefCount)

	// Both objects should be readable
	assert.Equal(t, data, fileClient.GetObject(objectID1))
	assert.Equal(t, data, fileClient.GetObject(objectID2))
}

func TestGC_RefCountDecrement(t *testing.T) {
	t.Parallel()

	fileClient := newFileClient(t, fileServer1Addr)
	adminClient := testutil.NewAdminClient(t, testutil.Addrs.FileServer1Admin)

	// Upload an object
	objectID := testutil.UniqueID("test-gc-decrement")
	data := testutil.GenerateTestData(t, 1024)
	chunkID := computeChunkID(data)

	fileClient.PutObject(objectID, data)

	// Verify chunk exists with RefCount=1
	chunk := adminClient.MustGetChunkInfo(chunkID)
	assert.Equal(t, uint32(1), chunk.RefCount)

	// Decrement RefCount (simulates what metadata service does on delete)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	decResp, err := fileClient.FileServiceClient.DecrementRefCount(ctx, &file_pb.DecrementRefCountRequest{
		ChunkId: chunkID,
	})
	require.NoError(t, err)
	assert.True(t, decResp.Success)
	assert.Equal(t, uint32(0), decResp.NewRefCount)
	t.Logf("Decremented RefCount to %d", decResp.NewRefCount)

	// Verify ZeroRefSince is set
	chunk, err = adminClient.GetChunkInfo(chunkID)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), chunk.RefCount)
	assert.Greater(t, chunk.ZeroRefSince, int64(0), "ZeroRefSince should be set")
	t.Logf("Chunk has ZeroRefSince=%d", chunk.ZeroRefSince)

	// Force GC - chunk should be deleted
	adminClient.MustForceGC("")

	// Verify chunk is gone
	_, err = adminClient.GetChunkInfo(chunkID)
	assert.Error(t, err, "chunk should be deleted after GC")
	t.Logf("Chunk %s successfully garbage collected", chunkID)
}

// =============================================================================
// Concurrency Stress Tests
// =============================================================================

func TestGC_ConcurrentDeduplication(t *testing.T) {
	t.Parallel()

	const numGoroutines = 20
	const dataSize = 4096 // 4KB

	fileClient := newFileClient(t, fileServer1Addr)
	adminClient := testutil.NewAdminClient(t, testutil.Addrs.FileServer1Admin)

	// Generate data that all goroutines will upload (same content = same chunk)
	data := testutil.GenerateTestData(t, dataSize)
	chunkID := computeChunkID(data)
	t.Logf("Testing concurrent uploads of chunk %s with %d goroutines", chunkID, numGoroutines)

	// Track successful uploads
	var successCount atomic.Int32
	var wg sync.WaitGroup
	objectIDs := make([]string, numGoroutines)

	// Launch concurrent uploads
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		objectIDs[i] = testutil.UniqueID("test-concurrent")
		go func(idx int, objID string) {
			defer wg.Done()
			resp := fileClient.PutObject(objID, data)
			if resp.Size == uint64(dataSize) {
				successCount.Add(1)
			}
		}(i, objectIDs[i])
	}

	wg.Wait()
	t.Logf("Completed %d/%d uploads", successCount.Load(), numGoroutines)
	require.Equal(t, int32(numGoroutines), successCount.Load(), "all uploads should succeed")

	// Verify RefCount equals number of successful uploads
	chunk, err := adminClient.GetChunkInfo(chunkID)
	require.NoError(t, err, "chunk should exist")
	assert.Equal(t, uint32(numGoroutines), chunk.RefCount,
		"RefCount should equal number of concurrent uploads (deduplication)")
	t.Logf("Chunk %s has RefCount=%d (expected %d)", chunkID, chunk.RefCount, numGoroutines)

	// Verify all objects are readable
	for i := 0; i < numGoroutines; i++ {
		retrieved := fileClient.GetObject(objectIDs[i])
		assert.Equal(t, data, retrieved, "object %d should be readable", i)
	}
}

func TestGC_ConcurrentDecrements(t *testing.T) {
	t.Parallel()

	const numRefs = 10
	const dataSize = 2048

	fileClient := newFileClient(t, fileServer1Addr)
	adminClient := testutil.NewAdminClient(t, testutil.Addrs.FileServer1Admin)

	// Create multiple objects pointing to the same chunk
	data := testutil.GenerateTestData(t, dataSize)
	chunkID := computeChunkID(data)

	objectIDs := make([]string, numRefs)
	for i := 0; i < numRefs; i++ {
		objectIDs[i] = testutil.UniqueID("test-concurrent-dec")
		fileClient.PutObject(objectIDs[i], data)
	}

	// Verify initial RefCount
	chunk := adminClient.MustGetChunkInfo(chunkID)
	require.Equal(t, uint32(numRefs), chunk.RefCount, "should have %d refs", numRefs)
	t.Logf("Created %d objects, chunk %s has RefCount=%d", numRefs, chunkID, chunk.RefCount)

	// Concurrently decrement all refs
	var wg sync.WaitGroup
	var decrementErrors atomic.Int32

	for i := 0; i < numRefs; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			_, err := fileClient.FileServiceClient.DecrementRefCount(ctx, &file_pb.DecrementRefCountRequest{
				ChunkId: chunkID,
			})
			if err != nil {
				decrementErrors.Add(1)
			}
		}()
	}

	wg.Wait()
	require.Equal(t, int32(0), decrementErrors.Load(), "all decrements should succeed")

	// Verify RefCount is now 0
	chunk, err := adminClient.GetChunkInfo(chunkID)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), chunk.RefCount, "RefCount should be 0 after all decrements")
	assert.Greater(t, chunk.ZeroRefSince, int64(0), "ZeroRefSince should be set")
	t.Logf("After concurrent decrements: RefCount=%d, ZeroRefSince=%d", chunk.RefCount, chunk.ZeroRefSince)

	// Force GC and verify cleanup
	adminClient.MustForceGC("")

	_, err = adminClient.GetChunkInfo(chunkID)
	assert.Error(t, err, "chunk should be deleted after GC")
	t.Logf("Chunk %s successfully garbage collected after concurrent decrements", chunkID)
}

func TestGC_MixedConcurrentOperations(t *testing.T) {
	t.Parallel()

	const numWriters = 10
	const numDecrementers = 5
	const dataSize = 3072

	fileClient := newFileClient(t, fileServer1Addr)
	adminClient := testutil.NewAdminClient(t, testutil.Addrs.FileServer1Admin)

	// Generate shared data
	data := testutil.GenerateTestData(t, dataSize)
	chunkID := computeChunkID(data)

	// First, create some initial refs
	initialRefs := 5
	for range initialRefs {
		objID := testutil.UniqueID("test-mixed-init")
		fileClient.PutObject(objID, data)
	}

	chunk := adminClient.MustGetChunkInfo(chunkID)
	require.Equal(t, uint32(initialRefs), chunk.RefCount)
	t.Logf("Initial setup: %d refs for chunk %s", initialRefs, chunkID)

	// Now run mixed operations concurrently:
	// - Writers add new refs
	// - Decrementers remove refs
	var wg sync.WaitGroup
	var writeSuccesses, decrementSuccesses atomic.Int32

	// Start writers
	for i := 0; i < numWriters; i++ {
		wg.Add(1)
		objID := testutil.UniqueID("test-mixed-write")
		go func(id string) {
			defer wg.Done()
			resp := fileClient.PutObject(id, data)
			if resp.Size == uint64(dataSize) {
				writeSuccesses.Add(1)
			}
		}(objID)
	}

	// Start decrementers (slightly delayed to ensure some refs exist)
	time.Sleep(10 * time.Millisecond)
	for i := 0; i < numDecrementers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			resp, err := fileClient.FileServiceClient.DecrementRefCount(ctx, &file_pb.DecrementRefCountRequest{
				ChunkId: chunkID,
			})
			if err == nil && resp.Success {
				decrementSuccesses.Add(1)
			}
		}()
	}

	wg.Wait()

	writes := writeSuccesses.Load()
	decrements := decrementSuccesses.Load()
	t.Logf("Completed: %d writes, %d decrements", writes, decrements)

	// Verify final RefCount = initial + writes - decrements
	expectedRefCount := uint32(initialRefs) + uint32(writes) - uint32(decrements)
	chunk, err := adminClient.GetChunkInfo(chunkID)
	require.NoError(t, err)

	assert.Equal(t, expectedRefCount, chunk.RefCount,
		"RefCount should be initial(%d) + writes(%d) - decrements(%d) = %d",
		initialRefs, writes, decrements, expectedRefCount)
	t.Logf("Final RefCount=%d (expected %d)", chunk.RefCount, expectedRefCount)
}
