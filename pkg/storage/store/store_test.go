// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/compression"
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

// ============================================================================
// Compression Tests
// ============================================================================

func TestFileStore_PutObjectWithCompression_LZ4(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	// Compressible data (repeated patterns compress well)
	testData := bytes.Repeat([]byte("hello world compression test "), 1000)

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObjectWithCompression(ctx, obj, bytes.NewReader(testData), compression.LZ4)
	require.NoError(t, err)

	// Verify object metadata
	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	assert.Equal(t, objID, retrieved.ID)
	assert.Equal(t, uint64(len(testData)), retrieved.Size)
	require.NotEmpty(t, retrieved.ChunkRefs)

	// Verify compression metadata
	chunkRef := retrieved.ChunkRefs[0]
	assert.Equal(t, "lz4", chunkRef.Compression)
	assert.Greater(t, chunkRef.OriginalSize, uint64(0))
	// Compressed size should be less than original for compressible data
	assert.Less(t, chunkRef.Size, chunkRef.OriginalSize)

	// Verify data is correctly retrieved (decompressed)
	reader, err := fs.GetChunk(ctx, chunkRef.ChunkID)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFileStore_PutObjectWithCompression_ZSTD(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := bytes.Repeat([]byte("zstd compression works great "), 1000)

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObjectWithCompression(ctx, obj, bytes.NewReader(testData), compression.ZSTD)
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	require.NotEmpty(t, retrieved.ChunkRefs)

	chunkRef := retrieved.ChunkRefs[0]
	assert.Equal(t, "zstd", chunkRef.Compression)
	assert.Less(t, chunkRef.Size, chunkRef.OriginalSize)

	// Verify decompression
	reader, err := fs.GetChunk(ctx, chunkRef.ChunkID)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFileStore_PutObjectWithCompression_S2(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := bytes.Repeat([]byte("s2 is fast "), 1000)

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObjectWithCompression(ctx, obj, bytes.NewReader(testData), compression.S2)
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	require.NotEmpty(t, retrieved.ChunkRefs)

	chunkRef := retrieved.ChunkRefs[0]
	assert.Equal(t, "s2", chunkRef.Compression)
	assert.Less(t, chunkRef.Size, chunkRef.OriginalSize)

	// Verify decompression
	reader, err := fs.GetChunk(ctx, chunkRef.ChunkID)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFileStore_PutObjectWithCompression_None(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := []byte("no compression data")

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObjectWithCompression(ctx, obj, bytes.NewReader(testData), compression.None)
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	require.NotEmpty(t, retrieved.ChunkRefs)

	chunkRef := retrieved.ChunkRefs[0]
	// No compression should result in empty compression field or "none"
	assert.True(t, chunkRef.Compression == "" || chunkRef.Compression == "none")
	// Size should equal the data size
	assert.Equal(t, uint64(len(testData)), chunkRef.Size)
}

func TestFileStore_CompressionSkippedForIncompressibleData(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	// Random data doesn't compress well - CompressIfBeneficial should skip compression
	testData := make([]byte, 4096)
	for i := range testData {
		testData[i] = byte(i * 17) // Pseudo-random pattern
	}

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObjectWithCompression(ctx, obj, bytes.NewReader(testData), compression.LZ4)
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	require.NotEmpty(t, retrieved.ChunkRefs)

	// Even if compression was requested, it should be skipped if not beneficial
	// So the data should still be readable
	reader, err := fs.GetChunk(ctx, retrieved.ChunkRefs[0].ChunkID)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData, data)
}

func TestFileStore_GetChunkRange_WithCompression(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	objID := uuid.New()
	testData := bytes.Repeat([]byte("0123456789"), 100) // 1000 bytes of compressible data

	obj := &types.ObjectRef{ID: objID}
	err := fs.PutObjectWithCompression(ctx, obj, bytes.NewReader(testData), compression.LZ4)
	require.NoError(t, err)

	retrieved, err := fs.GetObject(ctx, objID)
	require.NoError(t, err)
	require.NotEmpty(t, retrieved.ChunkRefs)

	chunkID := retrieved.ChunkRefs[0].ChunkID

	// Read a range from the middle
	reader, err := fs.GetChunkRange(ctx, chunkID, 100, 50)
	require.NoError(t, err)
	defer reader.Close()

	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, testData[100:150], data)
}

func TestFileStore_Deduplication_WithCompression(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	// Write same data twice with compression
	testData := bytes.Repeat([]byte("deduplicated compressed content "), 100)

	obj1 := &types.ObjectRef{ID: uuid.New()}
	err := fs.PutObjectWithCompression(ctx, obj1, bytes.NewReader(testData), compression.LZ4)
	require.NoError(t, err)

	obj2 := &types.ObjectRef{ID: uuid.New()}
	err = fs.PutObjectWithCompression(ctx, obj2, bytes.NewReader(testData), compression.LZ4)
	require.NoError(t, err)

	// Both should reference the same chunk (deduplication based on original data hash)
	retrieved1, _ := fs.GetObject(ctx, obj1.ID)
	retrieved2, _ := fs.GetObject(ctx, obj2.ID)

	assert.Equal(t, retrieved1.ChunkRefs[0].ChunkID, retrieved2.ChunkRefs[0].ChunkID)
}

func TestFileStore_ConcurrentPutObjectWithCompression(t *testing.T) {
	t.Parallel()

	fs, _ := newTestFileStore(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	algos := []compression.Algorithm{compression.LZ4, compression.ZSTD, compression.S2, compression.None}

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			objID := uuid.New()
			testData := bytes.Repeat([]byte("concurrent compression test "), id+10)
			algo := algos[id%len(algos)]

			obj := &types.ObjectRef{ID: objID}
			err := fs.PutObjectWithCompression(ctx, obj, bytes.NewReader(testData), algo)
			assert.NoError(t, err)

			// Verify we can read it back
			reader, err := fs.GetObjectData(ctx, objID)
			if assert.NoError(t, err) {
				defer reader.Close()
				data, err := io.ReadAll(reader)
				assert.NoError(t, err)
				assert.Equal(t, testData, data)
			}
		}(i)
	}

	wg.Wait()
}
