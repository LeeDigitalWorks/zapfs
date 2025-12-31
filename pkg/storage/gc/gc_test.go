package gc

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/index"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Test Helpers
// ============================================================================

func newTestGCWorker(t *testing.T, opts ...func(*GCWorkerConfig)) (*GCWorker, index.Indexer[types.ChunkID, types.Chunk], *backend.Manager) {
	t.Helper()

	chunkIdx, err := index.NewMemoryIndexer[types.ChunkID, types.Chunk]()
	require.NoError(t, err)

	mgr := backend.NewManager()
	err = mgr.AddMemory("test-backend")
	require.NoError(t, err)

	cfg := GCWorkerConfig{
		ChunkIdx:    chunkIdx,
		BackendID:   "test-backend",
		Interval:    100 * time.Millisecond,
		GracePeriod: 0, // No grace period for faster tests
		Concurrency: 2,
		Manager:     mgr,
	}

	for _, opt := range opts {
		opt(&cfg)
	}

	gc := NewGCWorkerWithConfig(cfg)

	t.Cleanup(func() {
		gc.Stop()
		chunkIdx.Close()
		mgr.Close()
	})

	return gc, chunkIdx, mgr
}

// ============================================================================
// Constructor Tests
// ============================================================================

func TestNewGCWorker(t *testing.T) {
	t.Parallel()

	chunkIdx, err := index.NewMemoryIndexer[types.ChunkID, types.Chunk]()
	require.NoError(t, err)
	defer chunkIdx.Close()

	mgr := backend.NewManager()
	defer mgr.Close()

	gc := NewGCWorker(chunkIdx, "backend-1", time.Minute, mgr)
	require.NotNil(t, gc)

	assert.Equal(t, "backend-1", gc.BackendID())
	assert.Equal(t, time.Minute, gc.interval)
	assert.Equal(t, DefaultGracePeriod, gc.gracePeriod)
	assert.Equal(t, 5, gc.concurrency) // Default
}

func TestNewGCWorkerWithConfig(t *testing.T) {
	t.Parallel()

	chunkIdx, err := index.NewMemoryIndexer[types.ChunkID, types.Chunk]()
	require.NoError(t, err)
	defer chunkIdx.Close()

	mgr := backend.NewManager()
	defer mgr.Close()

	cfg := GCWorkerConfig{
		ChunkIdx:    chunkIdx,
		BackendID:   "backend-2",
		Interval:    30 * time.Second,
		GracePeriod: 10 * time.Minute,
		Concurrency: 10,
		Manager:     mgr,
	}

	gc := NewGCWorkerWithConfig(cfg)
	require.NotNil(t, gc)

	assert.Equal(t, "backend-2", gc.BackendID())
	assert.Equal(t, 30*time.Second, gc.interval)
	assert.Equal(t, 10*time.Minute, gc.gracePeriod)
	assert.Equal(t, 10, gc.concurrency)
}

func TestNewGCWorkerWithConfig_Defaults(t *testing.T) {
	t.Parallel()

	chunkIdx, err := index.NewMemoryIndexer[types.ChunkID, types.Chunk]()
	require.NoError(t, err)
	defer chunkIdx.Close()

	mgr := backend.NewManager()
	defer mgr.Close()

	cfg := GCWorkerConfig{
		ChunkIdx:  chunkIdx,
		BackendID: "backend",
		Interval:  time.Minute,
		// GracePeriod: 0 (uses default)
		// Concurrency: 0 (uses default)
		Manager: mgr,
	}

	gc := NewGCWorkerWithConfig(cfg)
	require.NotNil(t, gc)

	assert.Equal(t, DefaultGracePeriod, gc.gracePeriod)
	assert.Equal(t, 5, gc.concurrency)
}

// ============================================================================
// BackendID Tests
// ============================================================================

func TestGCWorker_BackendID(t *testing.T) {
	t.Parallel()

	gc, _, _ := newTestGCWorker(t, func(cfg *GCWorkerConfig) {
		cfg.BackendID = "my-backend-id"
	})

	assert.Equal(t, "my-backend-id", gc.BackendID())
}

// ============================================================================
// Start/Stop Tests
// ============================================================================

func TestGCWorker_StartStop(t *testing.T) {
	t.Parallel()

	chunkIdx, err := index.NewMemoryIndexer[types.ChunkID, types.Chunk]()
	require.NoError(t, err)
	defer chunkIdx.Close()

	mgr := backend.NewManager()
	defer mgr.Close()

	gc := NewGCWorkerWithConfig(GCWorkerConfig{
		ChunkIdx:  chunkIdx,
		BackendID: "test",
		Interval:  50 * time.Millisecond,
		Manager:   mgr,
	})

	gc.Start()

	// Let it run a couple of cycles
	time.Sleep(150 * time.Millisecond)

	// Should not panic on stop
	gc.Stop()
}

func TestGCWorker_Start_ZeroInterval(t *testing.T) {
	t.Parallel()

	chunkIdx, err := index.NewMemoryIndexer[types.ChunkID, types.Chunk]()
	require.NoError(t, err)
	defer chunkIdx.Close()

	mgr := backend.NewManager()
	defer mgr.Close()

	gc := NewGCWorkerWithConfig(GCWorkerConfig{
		ChunkIdx:  chunkIdx,
		BackendID: "test",
		Interval:  0, // Zero interval
		Manager:   mgr,
	})

	// Start with zero interval should return immediately (no goroutine started)
	gc.Start()

	// Should not hang or panic
	gc.Stop()
}

// ============================================================================
// Run Tests
// ============================================================================

func TestGCWorker_Run_DeletesZeroRefChunks(t *testing.T) {
	t.Parallel()

	var mu sync.Mutex
	var deletedChunks []types.Chunk
	gc, chunkIdx, mgr := newTestGCWorker(t, func(cfg *GCWorkerConfig) {
		cfg.OnChunkDeleted = func(chunk types.Chunk) {
			mu.Lock()
			deletedChunks = append(deletedChunks, chunk)
			mu.Unlock()
		}
	})

	// Write some data to the backend
	ctx := t.Context()
	store, _ := mgr.Get("test-backend")

	// Create chunks in the index
	chunk1 := types.Chunk{
		ID:           "chunk-1",
		BackendID:    "test-backend",
		Path:         "chunk-1-path",
		Size:         100,
		RefCount:     0, // Zero ref - should be deleted
		ZeroRefSince: time.Now().Add(-10 * time.Minute).Unix(),
	}
	chunk2 := types.Chunk{
		ID:           "chunk-2",
		BackendID:    "test-backend",
		Path:         "chunk-2-path",
		Size:         200,
		RefCount:     1, // Has references - should NOT be deleted
		ZeroRefSince: 0,
	}
	chunk3 := types.Chunk{
		ID:           "chunk-3",
		BackendID:    "test-backend",
		Path:         "chunk-3-path",
		Size:         300,
		RefCount:     0, // Zero ref - should be deleted
		ZeroRefSince: time.Now().Add(-10 * time.Minute).Unix(),
	}

	require.NoError(t, chunkIdx.Put(chunk1.ID, chunk1))
	require.NoError(t, chunkIdx.Put(chunk2.ID, chunk2))
	require.NoError(t, chunkIdx.Put(chunk3.ID, chunk3))

	// Write chunk data to storage
	require.NoError(t, store.Write(ctx, chunk1.Path, strings.NewReader("data"), 4))
	require.NoError(t, store.Write(ctx, chunk2.Path, strings.NewReader("data"), 4))
	require.NoError(t, store.Write(ctx, chunk3.Path, strings.NewReader("data"), 4))

	// Run GC
	gc.RunWithGracePeriod(0) // Skip grace period

	// chunk1 and chunk3 should be deleted, chunk2 should remain
	_, err := chunkIdx.Get("chunk-1")
	assert.Error(t, err, "chunk-1 should be deleted")

	chunk2After, err := chunkIdx.Get("chunk-2")
	require.NoError(t, err, "chunk-2 should still exist")
	assert.Equal(t, uint32(1), chunk2After.RefCount)

	_, err = chunkIdx.Get("chunk-3")
	assert.Error(t, err, "chunk-3 should be deleted")

	// Verify callback was called
	assert.Len(t, deletedChunks, 2)
}

func TestGCWorker_Run_RespectsGracePeriod(t *testing.T) {
	t.Parallel()

	var deleted atomic.Int32
	gc, chunkIdx, mgr := newTestGCWorker(t, func(cfg *GCWorkerConfig) {
		cfg.GracePeriod = time.Hour // Long grace period
		cfg.OnChunkDeleted = func(chunk types.Chunk) {
			deleted.Add(1)
		}
	})

	ctx := t.Context()
	store, _ := mgr.Get("test-backend")

	// Chunk with ZeroRefSince set recently (within grace period)
	recentChunk := types.Chunk{
		ID:           "recent-chunk",
		BackendID:    "test-backend",
		Path:         "recent-path",
		Size:         100,
		RefCount:     0,
		ZeroRefSince: time.Now().Unix(), // Just now
	}

	// Chunk with ZeroRefSince set long ago (past grace period)
	oldChunk := types.Chunk{
		ID:           "old-chunk",
		BackendID:    "test-backend",
		Path:         "old-path",
		Size:         100,
		RefCount:     0,
		ZeroRefSince: time.Now().Add(-2 * time.Hour).Unix(), // 2 hours ago
	}

	require.NoError(t, chunkIdx.Put(recentChunk.ID, recentChunk))
	require.NoError(t, chunkIdx.Put(oldChunk.ID, oldChunk))
	require.NoError(t, store.Write(ctx, recentChunk.Path, strings.NewReader("data"), 4))
	require.NoError(t, store.Write(ctx, oldChunk.Path, strings.NewReader("data"), 4))

	// Run GC with the configured grace period
	gc.Run()

	// Recent chunk should still exist (within grace period)
	_, err := chunkIdx.Get("recent-chunk")
	assert.NoError(t, err, "recent chunk should still exist")

	// Old chunk should be deleted (past grace period)
	_, err = chunkIdx.Get("old-chunk")
	assert.Error(t, err, "old chunk should be deleted")

	assert.Equal(t, int32(1), deleted.Load())
}

func TestGCWorker_Run_OnlyDeletesMatchingBackend(t *testing.T) {
	t.Parallel()

	var deleted atomic.Int32
	gc, chunkIdx, mgr := newTestGCWorker(t, func(cfg *GCWorkerConfig) {
		cfg.BackendID = "test-backend"
		cfg.OnChunkDeleted = func(chunk types.Chunk) {
			deleted.Add(1)
		}
	})

	ctx := t.Context()
	store, _ := mgr.Get("test-backend")

	// Chunk on test-backend
	chunk1 := types.Chunk{
		ID:           "chunk-1",
		BackendID:    "test-backend",
		Path:         "chunk-1-path",
		Size:         100,
		RefCount:     0,
		ZeroRefSince: time.Now().Add(-10 * time.Minute).Unix(),
	}

	// Chunk on different backend
	chunk2 := types.Chunk{
		ID:           "chunk-2",
		BackendID:    "other-backend", // Different backend
		Path:         "chunk-2-path",
		Size:         100,
		RefCount:     0,
		ZeroRefSince: time.Now().Add(-10 * time.Minute).Unix(),
	}

	require.NoError(t, chunkIdx.Put(chunk1.ID, chunk1))
	require.NoError(t, chunkIdx.Put(chunk2.ID, chunk2))
	require.NoError(t, store.Write(ctx, chunk1.Path, strings.NewReader("data"), 4))

	// Run GC
	gc.RunWithGracePeriod(0)

	// Only chunk1 should be deleted (matches backend)
	_, err := chunkIdx.Get("chunk-1")
	assert.Error(t, err, "chunk-1 should be deleted")

	// chunk2 should still exist (different backend)
	_, err = chunkIdx.Get("chunk-2")
	assert.NoError(t, err, "chunk-2 should still exist (different backend)")

	assert.Equal(t, int32(1), deleted.Load())
}

func TestGCWorker_Run_ZeroRefSinceNotSet(t *testing.T) {
	t.Parallel()

	var deleted atomic.Int32
	gc, chunkIdx, mgr := newTestGCWorker(t, func(cfg *GCWorkerConfig) {
		cfg.GracePeriod = time.Minute // Non-zero grace period
		cfg.OnChunkDeleted = func(chunk types.Chunk) {
			deleted.Add(1)
		}
	})

	ctx := t.Context()
	store, _ := mgr.Get("test-backend")

	// Chunk with ZeroRefSince not set (legacy or race condition)
	chunk := types.Chunk{
		ID:           "legacy-chunk",
		BackendID:    "test-backend",
		Path:         "legacy-path",
		Size:         100,
		RefCount:     0,
		ZeroRefSince: 0, // Not set
	}

	require.NoError(t, chunkIdx.Put(chunk.ID, chunk))
	require.NoError(t, store.Write(ctx, chunk.Path, strings.NewReader("data"), 4))

	// Run GC with grace period
	gc.Run()

	// Chunk should NOT be deleted (ZeroRefSince not set = skipped)
	_, err := chunkIdx.Get("legacy-chunk")
	assert.NoError(t, err, "chunk with unset ZeroRefSince should be skipped")

	assert.Equal(t, int32(0), deleted.Load())
}

func TestGCWorker_Run_ForceImmediateDeletion(t *testing.T) {
	t.Parallel()

	var deleted atomic.Int32
	gc, chunkIdx, mgr := newTestGCWorker(t, func(cfg *GCWorkerConfig) {
		cfg.GracePeriod = time.Hour // Long grace period normally
		cfg.OnChunkDeleted = func(chunk types.Chunk) {
			deleted.Add(1)
		}
	})

	ctx := t.Context()
	store, _ := mgr.Get("test-backend")

	chunk := types.Chunk{
		ID:           "immediate-chunk",
		BackendID:    "test-backend",
		Path:         "immediate-path",
		Size:         100,
		RefCount:     0,
		ZeroRefSince: time.Now().Unix(), // Just now
	}

	require.NoError(t, chunkIdx.Put(chunk.ID, chunk))
	require.NoError(t, store.Write(ctx, chunk.Path, strings.NewReader("data"), 4))

	// Force immediate deletion (grace period = 0)
	gc.RunWithGracePeriod(0)

	// Chunk should be deleted even though it's recent
	_, err := chunkIdx.Get("immediate-chunk")
	assert.Error(t, err, "chunk should be deleted with grace period 0")

	assert.Equal(t, int32(1), deleted.Load())
}

// ============================================================================
// Concurrency Tests
// ============================================================================

func TestGCWorker_ConcurrentDeletion(t *testing.T) {
	t.Parallel()

	var deleted atomic.Int32
	gc, chunkIdx, mgr := newTestGCWorker(t, func(cfg *GCWorkerConfig) {
		cfg.Concurrency = 4
		cfg.OnChunkDeleted = func(chunk types.Chunk) {
			deleted.Add(1)
		}
	})

	ctx := t.Context()
	store, _ := mgr.Get("test-backend")

	// Create many chunks eligible for deletion
	for i := 0; i < 20; i++ {
		chunk := types.Chunk{
			ID:           types.ChunkID(fmt.Sprintf("chunk-%d", i)),
			BackendID:    "test-backend",
			Path:         fmt.Sprintf("chunk-%d-path", i),
			Size:         100,
			RefCount:     0,
			ZeroRefSince: time.Now().Add(-10 * time.Minute).Unix(),
		}
		require.NoError(t, chunkIdx.Put(chunk.ID, chunk))
		require.NoError(t, store.Write(ctx, chunk.Path, strings.NewReader("data"), 4))
	}

	// Run GC with concurrent workers
	gc.RunWithGracePeriod(0)

	// All chunks should be deleted
	assert.Equal(t, int32(20), deleted.Load())
}

// ============================================================================
// Edge Cases
// ============================================================================

func TestGCWorker_Run_EmptyIndex(t *testing.T) {
	t.Parallel()

	gc, _, _ := newTestGCWorker(t)

	// Run on empty index should not panic
	gc.Run()
}

func TestGCWorker_Run_BackendNotFound(t *testing.T) {
	t.Parallel()

	gc, chunkIdx, _ := newTestGCWorker(t)

	// Create chunk pointing to non-existent backend
	chunk := types.Chunk{
		ID:           "orphan-chunk",
		BackendID:    "non-existent-backend", // Different from "test-backend"
		Path:         "orphan-path",
		Size:         100,
		RefCount:     0,
		ZeroRefSince: time.Now().Add(-10 * time.Minute).Unix(),
	}
	require.NoError(t, chunkIdx.Put(chunk.ID, chunk))

	// Run GC - should not panic, chunk remains because it's filtered out
	gc.RunWithGracePeriod(0)

	// Chunk should still exist (different backend)
	_, err := chunkIdx.Get("orphan-chunk")
	assert.NoError(t, err)
}
