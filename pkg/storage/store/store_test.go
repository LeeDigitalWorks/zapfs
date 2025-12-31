package store

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Test Helpers
// ============================================================================

func newTestFileStore(t *testing.T) (*FileStore, *backend.Manager) {
	t.Helper()

	mgr := backend.NewManager()
	err := mgr.AddMemory("test-backend")
	require.NoError(t, err)

	cfg := Config{
		IndexKind: IndexKindMemory,
		Backends: []*types.Backend{
			{
				ID:         "test-backend",
				Type:       backend.StorageTypeMemory,
				TotalBytes: 1024 * 1024 * 1024, // 1GB capacity for testing
			},
		},
		ECScheme: types.ECScheme{
			DataShards:   4,
			ParityShards: 2,
		},
	}

	fs, err := NewFileStore(cfg, mgr)
	require.NoError(t, err)

	t.Cleanup(func() {
		fs.Close()
	})

	return fs, mgr
}

// ============================================================================
// NewFileStore Tests
// ============================================================================

func TestNewFileStore_Memory(t *testing.T) {
	t.Parallel()

	mgr := backend.NewManager()
	err := mgr.AddMemory("mem")
	require.NoError(t, err)

	cfg := Config{
		IndexKind: IndexKindMemory,
		Backends: []*types.Backend{
			{ID: "mem", Type: backend.StorageTypeMemory, TotalBytes: 1024 * 1024 * 1024},
		},
		ECScheme: types.ECScheme{
			DataShards:   4,
			ParityShards: 2,
		},
	}

	fs, err := NewFileStore(cfg, mgr)
	require.NoError(t, err)
	require.NotNil(t, fs)

	defer fs.Close()
}

func TestNewFileStore_LevelDB(t *testing.T) {
	t.Parallel()

	tmpDir := t.TempDir()

	mgr := backend.NewManager()
	err := mgr.AddMemory("mem")
	require.NoError(t, err)

	cfg := Config{
		IndexPath: tmpDir,
		IndexKind: IndexKindLevelDB,
		Backends: []*types.Backend{
			{ID: "mem", Type: backend.StorageTypeMemory, TotalBytes: 1024 * 1024 * 1024},
		},
		ECScheme: types.ECScheme{
			DataShards:   4,
			ParityShards: 2,
		},
	}

	fs, err := NewFileStore(cfg, mgr)
	require.NoError(t, err)
	require.NotNil(t, fs)

	defer fs.Close()
}

func TestNewFileStore_MultipleBackends(t *testing.T) {
	t.Parallel()

	mgr := backend.NewManager()
	err := mgr.AddMemory("backend-a")
	require.NoError(t, err)
	err = mgr.AddMemory("backend-b")
	require.NoError(t, err)

	cfg := Config{
		IndexKind: IndexKindMemory,
		Backends: []*types.Backend{
			{ID: "backend-a", Type: backend.StorageTypeMemory, TotalBytes: 1024 * 1024 * 1024},
			{ID: "backend-b", Type: backend.StorageTypeMemory, TotalBytes: 1024 * 1024 * 1024},
		},
		ECScheme: types.ECScheme{
			DataShards:   4,
			ParityShards: 2,
		},
	}

	fs, err := NewFileStore(cfg, mgr)
	require.NoError(t, err)
	defer fs.Close()

	// Verify both backends are registered
	backends := fs.ListBackends()
	assert.Len(t, backends, 2)
	assert.Contains(t, backends, "backend-a")
	assert.Contains(t, backends, "backend-b")
}

// ============================================================================
// Backend Management Tests
// ============================================================================

func TestFileStore_GetBackend(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)

	backend, ok := fs.GetBackend("test-backend")
	require.True(t, ok)
	require.NotNil(t, backend)
	assert.Equal(t, "test-backend", backend.ID)
}

func TestFileStore_GetBackend_NotFound(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)

	backend, ok := fs.GetBackend("nonexistent")
	assert.False(t, ok)
	assert.Nil(t, backend)
}

func TestFileStore_GetDefaultBackend(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)

	backend := fs.GetDefaultBackend()
	require.NotNil(t, backend)
	assert.Equal(t, "test-backend", backend.ID)
}

func TestFileStore_GetBackendStorage(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)

	storage, ok := fs.GetBackendStorage("test-backend")
	assert.True(t, ok)
	assert.NotNil(t, storage)
}

func TestFileStore_ListBackends(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)

	backends := fs.ListBackends()
	assert.Len(t, backends, 1)
	assert.Contains(t, backends, "test-backend")
}

func TestFileStore_AddBackend(t *testing.T) {
	t.Parallel()

	fs, mgr := newTestFileStore(t)

	// Add a new backend to the manager first
	err := mgr.AddMemory("new-backend")
	require.NoError(t, err)

	// Then add to FileStore
	fs.AddBackend(&types.Backend{ID: "new-backend", Type: backend.StorageTypeMemory})

	backends := fs.ListBackends()
	assert.Len(t, backends, 2)
	assert.Contains(t, backends, "new-backend")
}

func TestFileStore_RemoveBackend(t *testing.T) {
	t.Parallel()

	fs, mgr := newTestFileStore(t)

	// Add second backend
	err := mgr.AddMemory("to-remove")
	require.NoError(t, err)
	fs.AddBackend(&types.Backend{ID: "to-remove", Type: backend.StorageTypeMemory})

	// Verify it exists
	backends := fs.ListBackends()
	assert.Len(t, backends, 2)

	// Remove it
	fs.RemoveBackend("to-remove")

	backends = fs.ListBackends()
	assert.Len(t, backends, 1)
	assert.NotContains(t, backends, "to-remove")
}

// ============================================================================
// Object Operations Tests
// ============================================================================

func TestFileStore_PutObject_GetObject(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("hello world test data")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	// Verify object metadata
	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	assert.Equal(t, objID, retrieved.ID)
	assert.Equal(t, uint64(len(testData)), retrieved.Size)
	assert.NotEmpty(t, retrieved.ETag)
	assert.NotEmpty(t, retrieved.ChunkRefs)
}

func TestFileStore_PutObject_LargeObject(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	// Create data larger than one chunk (ChunkSize is typically 4MB or similar)
	// For testing, create enough data to span multiple chunks
	testData := bytes.Repeat([]byte("0123456789"), 1024*1024) // 10MB

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(testData)), retrieved.Size)
	// Should have multiple chunks
	assert.GreaterOrEqual(t, len(retrieved.ChunkRefs), 1)
}

func TestFileStore_PutObject_EmptyObject(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	obj := &types.ObjectRef{ID: objID}

	err := fs.PutObject(ctx, obj, bytes.NewReader([]byte{}))
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	assert.Equal(t, uint64(0), retrieved.Size)
	assert.Empty(t, retrieved.ChunkRefs)
}

func TestFileStore_GetObject_NotFound(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	_, err := fs.GetObject(ctx, uuid.New())
	require.Error(t, err)
}

func TestFileStore_DeleteObject(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("to be deleted")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	err = fs.DeleteObject(ctx, objID)
	require.NoError(t, err)

	// Object should still exist but be marked as deleted
	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	assert.True(t, retrieved.IsDeleted())
}

func TestFileStore_DeleteObject_NotFound(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	err := fs.DeleteObject(ctx, uuid.New())
	require.Error(t, err)
}

// ============================================================================
// Object Data Tests
// ============================================================================

func TestFileStore_GetObjectData(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("hello world object data")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	reader, err := fs.GetObjectData(ctx, objID)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFileStore_GetObjectData_NotFound(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	_, err := fs.GetObjectData(ctx, uuid.New())
	require.Error(t, err)
}

func TestFileStore_GetObjectData_Deleted(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("deleted object")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	err = fs.DeleteObject(ctx, objID)
	require.NoError(t, err)

	// GetObjectData should fail for deleted objects
	_, err = fs.GetObjectData(ctx, objID)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "deleted")
}

func TestFileStore_GetObjectRange(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("0123456789ABCDEFGHIJ")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	reader, err := fs.GetObjectRange(ctx, objID, 5, 10)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("56789ABCDE"), data)
}

// ============================================================================
// Chunk Operations Tests
// ============================================================================

func TestFileStore_GetChunk(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("chunk test data")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	require.NotEmpty(t, retrieved.ChunkRefs)

	chunkID := retrieved.ChunkRefs[0].ChunkID
	reader, err := fs.GetChunk(ctx, chunkID)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFileStore_GetChunk_NotFound(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	_, err := fs.GetChunk(ctx, types.ChunkID("nonexistent-chunk"))
	require.Error(t, err)
}

func TestFileStore_GetChunkRange(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("0123456789")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	require.NotEmpty(t, retrieved.ChunkRefs)

	chunkID := retrieved.ChunkRefs[0].ChunkID
	reader, err := fs.GetChunkRange(ctx, chunkID, 2, 5)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, []byte("23456"), data)
}

func TestFileStore_GetChunkInfo(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("chunk info test")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	require.NotEmpty(t, retrieved.ChunkRefs)

	chunkID := retrieved.ChunkRefs[0].ChunkID
	info, err := fs.GetChunkInfo(chunkID)
	require.NoError(t, err)

	assert.Equal(t, chunkID, info.ID)
	assert.Equal(t, "test-backend", info.BackendID)
	assert.Equal(t, uint64(len(testData)), info.Size)
	assert.Equal(t, uint32(1), info.RefCount)
}

// ============================================================================
// Deduplication Tests
// ============================================================================

func TestFileStore_Deduplication(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	// Write same data twice with different object IDs
	testData := []byte("deduplicated content")

	obj1 := &types.ObjectRef{ID: uuid.New()}
	err := fs.PutObject(ctx, obj1, bytes.NewReader(testData))
	require.NoError(t, err)

	obj2 := &types.ObjectRef{ID: uuid.New()}
	err = fs.PutObject(ctx, obj2, bytes.NewReader(testData))
	require.NoError(t, err)

	// Both should reference the same chunk
	retrieved1, _ := fs.GetObject(ctx, obj1.ID)
	retrieved2, _ := fs.GetObject(ctx, obj2.ID)

	assert.Equal(t, retrieved1.ChunkRefs[0].ChunkID, retrieved2.ChunkRefs[0].ChunkID)

	// Chunk should have RefCount = 2
	info, err := fs.GetChunkInfo(retrieved1.ChunkRefs[0].ChunkID)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), info.RefCount)
}

// ============================================================================
// RefCount Tests
// ============================================================================

func TestFileStore_IncrementRefCount(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("refcount test")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, _ := fs.GetObject(ctx, objID)
	chunkID := retrieved.ChunkRefs[0].ChunkID

	// Initial RefCount should be 1
	info, _ := fs.GetChunkInfo(chunkID)
	assert.Equal(t, uint32(1), info.RefCount)

	// Increment
	err = fs.IncrementRefCount(chunkID)
	require.NoError(t, err)

	info, _ = fs.GetChunkInfo(chunkID)
	assert.Equal(t, uint32(2), info.RefCount)
}

func TestFileStore_IncrementRefCount_NotFound(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)

	err := fs.IncrementRefCount(types.ChunkID("nonexistent"))
	assert.ErrorIs(t, err, ErrChunkNotFound)
}

func TestFileStore_DecrementRefCount(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("decrement test")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, _ := fs.GetObject(ctx, objID)
	chunkID := retrieved.ChunkRefs[0].ChunkID

	newCount, err := fs.DecrementRefCount(chunkID)
	require.NoError(t, err)
	assert.Equal(t, uint32(0), newCount)

	// Verify in index
	info, _ := fs.GetChunkInfo(chunkID)
	assert.Equal(t, uint32(0), info.RefCount)
	assert.NotZero(t, info.ZeroRefSince) // Should have GC timestamp
}

func TestFileStore_DecrementRefCount_NotFound(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)

	_, err := fs.DecrementRefCount(types.ChunkID("nonexistent"))
	assert.ErrorIs(t, err, ErrChunkNotFound)
}

func TestFileStore_DecrementRefCountCAS_Success(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("cas test")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, _ := fs.GetObject(ctx, objID)
	chunkID := retrieved.ChunkRefs[0].ChunkID

	// CAS with correct expected value
	newCount, success, err := fs.DecrementRefCountCAS(chunkID, 1, true)
	require.NoError(t, err)
	assert.True(t, success)
	assert.Equal(t, uint32(0), newCount)
}

func TestFileStore_DecrementRefCountCAS_Mismatch(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("cas mismatch test")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, _ := fs.GetObject(ctx, objID)
	chunkID := retrieved.ChunkRefs[0].ChunkID

	// CAS with wrong expected value
	currentCount, success, err := fs.DecrementRefCountCAS(chunkID, 5, true)
	require.NoError(t, err)
	assert.False(t, success)
	assert.Equal(t, uint32(1), currentCount) // Returns current count
}

func TestFileStore_DecrementRefCountBatch(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	// Create multiple objects with different chunks
	var chunkIDs []types.ChunkID
	for i := 0; i < 3; i++ {
		objID := uuid.New()
		testData := []byte(strings.Repeat("x", i+10)) // Different sizes for different chunks

		obj := &types.ObjectRef{ID: objID}
		err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
		require.NoError(t, err)

		retrieved, _ := fs.GetObject(ctx, objID)
		chunkIDs = append(chunkIDs, retrieved.ChunkRefs[0].ChunkID)
	}

	results, err := fs.DecrementRefCountBatch(chunkIDs)
	require.NoError(t, err)
	assert.Len(t, results, 3)

	// All should have RefCount = 0
	for _, newCount := range results {
		assert.Equal(t, uint32(0), newCount)
	}
}

// ============================================================================
// Concurrency Tests
// ============================================================================

func TestFileStore_ConcurrentPutObject(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			objID := uuid.New()
			testData := []byte(strings.Repeat("x", id+10))

			obj := &types.ObjectRef{ID: objID}
			err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
			assert.NoError(t, err)
		}(i)
	}

	wg.Wait()
}

func TestFileStore_ConcurrentRefCount(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	// Create an object to get a chunk
	objID := uuid.New()
	testData := []byte("concurrent refcount test")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
	require.NoError(t, err)

	retrieved, _ := fs.GetObject(ctx, objID)
	chunkID := retrieved.ChunkRefs[0].ChunkID

	var wg sync.WaitGroup

	// Concurrent increments
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fs.IncrementRefCount(chunkID)
		}()
	}

	wg.Wait()

	// Should have 1 + 50 = 51
	info, _ := fs.GetChunkInfo(chunkID)
	assert.Equal(t, uint32(51), info.RefCount)

	// Concurrent decrements
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fs.DecrementRefCount(chunkID)
		}()
	}

	wg.Wait()

	// Should be back to 1
	info, _ = fs.GetChunkInfo(chunkID)
	assert.Equal(t, uint32(1), info.RefCount)
}

// ============================================================================
// Close Tests
// ============================================================================

func TestFileStore_Close(t *testing.T) {
	t.Parallel()

	mgr := backend.NewManager()
	err := mgr.AddMemory("test")
	require.NoError(t, err)

	cfg := Config{
		IndexKind: IndexKindMemory,
		Backends: []*types.Backend{
			{ID: "test", Type: backend.StorageTypeMemory, TotalBytes: 1024 * 1024 * 1024},
		},
		ECScheme: types.ECScheme{DataShards: 4, ParityShards: 2},
	}

	fs, err := NewFileStore(cfg, mgr)
	require.NoError(t, err)

	err = fs.Close()
	assert.NoError(t, err)
}

// ============================================================================
// Stats Tests
// ============================================================================

func TestFileStore_GetIndexStats(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	// Create some objects
	for i := 0; i < 3; i++ {
		objID := uuid.New()
		testData := []byte(strings.Repeat("x", (i+1)*100))

		obj := &types.ObjectRef{ID: objID}
		err := fs.PutObject(ctx, obj, bytes.NewReader(testData))
		require.NoError(t, err)
	}

	stats, err := fs.GetIndexStats()
	require.NoError(t, err)

	// We should have some chunks tracked
	assert.GreaterOrEqual(t, stats.TotalChunks, int64(1))
	assert.GreaterOrEqual(t, stats.TotalBytes, int64(100))
}
