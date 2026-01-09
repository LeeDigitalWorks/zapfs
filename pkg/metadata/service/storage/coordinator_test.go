// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/mocks/client"
	clientpkg "github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// groupChunksByID Tests
// =============================================================================

func TestGroupChunksByID(t *testing.T) {
	t.Run("empty refs", func(t *testing.T) {
		groups := groupChunksByID([]types.ChunkRef{})
		assert.Empty(t, groups)
	})

	t.Run("single chunk single replica", func(t *testing.T) {
		refs := []types.ChunkRef{
			{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-1:8081"},
		}
		groups := groupChunksByID(refs)

		require.Len(t, groups, 1)
		assert.Equal(t, types.ChunkID("chunk-1"), groups[0].chunkID)
		assert.Equal(t, uint64(0), groups[0].offset)
		assert.Len(t, groups[0].replicas, 1)
	})

	t.Run("single chunk multiple replicas", func(t *testing.T) {
		refs := []types.ChunkRef{
			{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-2:8081"},
			{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-3:8081"},
		}
		groups := groupChunksByID(refs)

		require.Len(t, groups, 1)
		assert.Equal(t, types.ChunkID("chunk-1"), groups[0].chunkID)
		assert.Len(t, groups[0].replicas, 3)
	})

	t.Run("multiple chunks sorted by offset", func(t *testing.T) {
		refs := []types.ChunkRef{
			{ChunkID: "chunk-3", Offset: 2000, Size: 1000, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-2", Offset: 1000, Size: 1000, FileServerAddr: "file-1:8081"},
		}
		groups := groupChunksByID(refs)

		require.Len(t, groups, 3)
		// Should be sorted by offset
		assert.Equal(t, types.ChunkID("chunk-1"), groups[0].chunkID)
		assert.Equal(t, uint64(0), groups[0].offset)
		assert.Equal(t, types.ChunkID("chunk-2"), groups[1].chunkID)
		assert.Equal(t, uint64(1000), groups[1].offset)
		assert.Equal(t, types.ChunkID("chunk-3"), groups[2].chunkID)
		assert.Equal(t, uint64(2000), groups[2].offset)
	})

	t.Run("multipart with replicas", func(t *testing.T) {
		refs := []types.ChunkRef{
			// Part 1 - 2 replicas
			{ChunkID: "chunk-1", Offset: 0, Size: 5 * 1024 * 1024, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-1", Offset: 0, Size: 5 * 1024 * 1024, FileServerAddr: "file-2:8081"},
			// Part 2 - 2 replicas
			{ChunkID: "chunk-2", Offset: 5 * 1024 * 1024, Size: 5 * 1024 * 1024, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-2", Offset: 5 * 1024 * 1024, Size: 5 * 1024 * 1024, FileServerAddr: "file-2:8081"},
			// Part 3 - 2 replicas
			{ChunkID: "chunk-3", Offset: 10 * 1024 * 1024, Size: 5 * 1024 * 1024, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-3", Offset: 10 * 1024 * 1024, Size: 5 * 1024 * 1024, FileServerAddr: "file-2:8081"},
		}
		groups := groupChunksByID(refs)

		require.Len(t, groups, 3)
		for i, g := range groups {
			assert.Len(t, g.replicas, 2, "group %d should have 2 replicas", i)
		}
		assert.Equal(t, uint64(0), groups[0].offset)
		assert.Equal(t, uint64(5*1024*1024), groups[1].offset)
		assert.Equal(t, uint64(10*1024*1024), groups[2].offset)
	})

	t.Run("skips refs with empty file server addr", func(t *testing.T) {
		refs := []types.ChunkRef{
			{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-2", Offset: 1000, Size: 1000, FileServerAddr: ""}, // Empty
			{ChunkID: "chunk-3", Offset: 2000, Size: 1000, FileServerAddr: "file-1:8081"},
		}
		groups := groupChunksByID(refs)

		require.Len(t, groups, 2)
		assert.Equal(t, types.ChunkID("chunk-1"), groups[0].chunkID)
		assert.Equal(t, types.ChunkID("chunk-3"), groups[1].chunkID)
	})

	t.Run("same chunk ID at different offsets - content deduplication", func(t *testing.T) {
		// This test verifies that when the same ChunkID appears at different offsets
		// (due to content-based deduplication), each offset is treated as a separate
		// read position. This happens when two parts of a multipart upload have
		// identical content.
		refs := []types.ChunkRef{
			// Part 1 at offset 0
			{ChunkID: "chunk-dedup", Offset: 0, Size: 5 * 1024 * 1024, FileServerAddr: "file-1:8081"},
			// Part 2 at offset 5MB has same content (same ChunkID)
			{ChunkID: "chunk-dedup", Offset: 5 * 1024 * 1024, Size: 5 * 1024 * 1024, FileServerAddr: "file-1:8081"},
		}
		groups := groupChunksByID(refs)

		// Should have 2 groups - one for each offset, even though ChunkID is the same
		require.Len(t, groups, 2, "same ChunkID at different offsets should create separate groups")
		assert.Equal(t, uint64(0), groups[0].offset)
		assert.Equal(t, uint64(5*1024*1024), groups[1].offset)
		// Both groups should reference the same chunk
		assert.Equal(t, types.ChunkID("chunk-dedup"), groups[0].chunkID)
		assert.Equal(t, types.ChunkID("chunk-dedup"), groups[1].chunkID)
	})

	t.Run("same chunk ID at different offsets with replicas", func(t *testing.T) {
		// Same as above but with replicas at each offset
		refs := []types.ChunkRef{
			// Part 1 at offset 0 - 2 replicas
			{ChunkID: "chunk-dedup", Offset: 0, Size: 5 * 1024 * 1024, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-dedup", Offset: 0, Size: 5 * 1024 * 1024, FileServerAddr: "file-2:8081"},
			// Part 2 at offset 5MB - 2 replicas (same content)
			{ChunkID: "chunk-dedup", Offset: 5 * 1024 * 1024, Size: 5 * 1024 * 1024, FileServerAddr: "file-1:8081"},
			{ChunkID: "chunk-dedup", Offset: 5 * 1024 * 1024, Size: 5 * 1024 * 1024, FileServerAddr: "file-2:8081"},
		}
		groups := groupChunksByID(refs)

		require.Len(t, groups, 2, "same ChunkID at different offsets should create separate groups")
		// Each group should have 2 replicas
		assert.Len(t, groups[0].replicas, 2, "offset 0 should have 2 replicas")
		assert.Len(t, groups[1].replicas, 2, "offset 5MB should have 2 replicas")
	})
}

// =============================================================================
// ReadObject Tests
// =============================================================================

func TestReadObject(t *testing.T) {
	t.Run("empty object returns nil", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		var buf bytes.Buffer
		err := coord.ReadObject(context.Background(), &ReadRequest{
			ChunkRefs: []types.ChunkRef{},
		}, &buf)

		assert.NoError(t, err)
		assert.Empty(t, buf.Bytes())
	})

	t.Run("single chunk success", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		chunkData := []byte("hello world")

		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-1", mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, writer clientpkg.ObjectWriter) {
				writer(chunkData)
			}).
			Return(nil)

		var buf bytes.Buffer
		err := coord.ReadObject(context.Background(), &ReadRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: uint64(len(chunkData)), FileServerAddr: "file-1:8081"},
			},
		}, &buf)

		assert.NoError(t, err)
		assert.Equal(t, chunkData, buf.Bytes())
	})

	t.Run("multipart object reads all chunks in order", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		chunk1Data := []byte("part1-")
		chunk2Data := []byte("part2-")
		chunk3Data := []byte("part3")

		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-1", mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, writer clientpkg.ObjectWriter) {
				writer(chunk1Data)
			}).
			Return(nil)

		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-2", mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, writer clientpkg.ObjectWriter) {
				writer(chunk2Data)
			}).
			Return(nil)

		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-3", mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, writer clientpkg.ObjectWriter) {
				writer(chunk3Data)
			}).
			Return(nil)

		var buf bytes.Buffer
		err := coord.ReadObject(context.Background(), &ReadRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: uint64(len(chunk1Data)), FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-2", Offset: uint64(len(chunk1Data)), Size: uint64(len(chunk2Data)), FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-3", Offset: uint64(len(chunk1Data) + len(chunk2Data)), Size: uint64(len(chunk3Data)), FileServerAddr: "file-1:8081"},
			},
		}, &buf)

		assert.NoError(t, err)
		assert.Equal(t, "part1-part2-part3", buf.String())
	})

	t.Run("failover to second replica", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		chunkData := []byte("fallback data")

		// First replica fails
		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-1", mock.Anything).
			Return(errors.New("connection refused"))

		// Second replica succeeds
		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-2:8081", "chunk-1", mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, writer clientpkg.ObjectWriter) {
				writer(chunkData)
			}).
			Return(nil)

		var buf bytes.Buffer
		err := coord.ReadObject(context.Background(), &ReadRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: uint64(len(chunkData)), FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-1", Offset: 0, Size: uint64(len(chunkData)), FileServerAddr: "file-2:8081"},
			},
		}, &buf)

		assert.NoError(t, err)
		assert.Equal(t, chunkData, buf.Bytes())
	})

	t.Run("all replicas fail returns error", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-1", mock.Anything).
			Return(errors.New("file-1 failed"))

		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-2:8081", "chunk-1", mock.Anything).
			Return(errors.New("file-2 failed"))

		var buf bytes.Buffer
		err := coord.ReadObject(context.Background(), &ReadRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: 100, FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-1", Offset: 0, Size: 100, FileServerAddr: "file-2:8081"},
			},
		}, &buf)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read chunk chunk-1 from any replica")
	})

	t.Run("second chunk fails after first succeeds", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		chunk1Data := []byte("success")

		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-1", mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, writer clientpkg.ObjectWriter) {
				writer(chunk1Data)
			}).
			Return(nil)

		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-2", mock.Anything).
			Return(errors.New("chunk-2 not found"))

		var buf bytes.Buffer
		err := coord.ReadObject(context.Background(), &ReadRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: uint64(len(chunk1Data)), FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-2", Offset: uint64(len(chunk1Data)), Size: 100, FileServerAddr: "file-1:8081"},
			},
		}, &buf)

		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to read chunk chunk-2")
	})

	t.Run("deduplicated chunk appears at multiple offsets", func(t *testing.T) {
		// This test verifies that when the same ChunkID appears at different offsets
		// (due to content-based deduplication, e.g., multipart upload where parts have
		// identical content), the chunk is read twice - once for each offset position.
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		chunkData := []byte("repeated-content-5MB")

		// The same chunk should be read twice - once for each offset
		mockFile.EXPECT().
			GetChunk(mock.Anything, "file-1:8081", "chunk-dedup", mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, writer clientpkg.ObjectWriter) {
				writer(chunkData)
			}).
			Return(nil).
			Times(2) // Should be called twice for the two different offsets

		var buf bytes.Buffer
		err := coord.ReadObject(context.Background(), &ReadRequest{
			ChunkRefs: []types.ChunkRef{
				// Same ChunkID at offset 0
				{ChunkID: "chunk-dedup", Offset: 0, Size: uint64(len(chunkData)), FileServerAddr: "file-1:8081"},
				// Same ChunkID at offset 20 (simulating 2nd part of multipart with identical content)
				{ChunkID: "chunk-dedup", Offset: uint64(len(chunkData)), Size: uint64(len(chunkData)), FileServerAddr: "file-1:8081"},
			},
		}, &buf)

		assert.NoError(t, err)
		// Output should contain the chunk data twice
		expected := append(chunkData, chunkData...)
		assert.Equal(t, expected, buf.Bytes())
	})
}

// =============================================================================
// ReadObjectRange Tests
// =============================================================================

func TestReadObjectRange(t *testing.T) {
	t.Run("empty object returns nil", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		var buf bytes.Buffer
		err := coord.ReadObjectRange(context.Background(), &ReadRangeRequest{
			ChunkRefs: []types.ChunkRef{},
			Offset:    0,
			Length:    100,
		}, &buf)

		assert.NoError(t, err)
		assert.Empty(t, buf.Bytes())
	})

	t.Run("range within single chunk", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		rangeData := []byte("llo wor")

		mockFile.EXPECT().
			GetChunkRange(mock.Anything, "file-1:8081", "chunk-1", uint64(2), uint64(7), mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, offset, length uint64, writer clientpkg.ObjectWriter) {
				writer(rangeData)
			}).
			Return(nil)

		var buf bytes.Buffer
		err := coord.ReadObjectRange(context.Background(), &ReadRangeRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: 11, FileServerAddr: "file-1:8081"}, // "hello world"
			},
			Offset: 2, // Start at "l"
			Length: 7, // End at "r"
		}, &buf)

		assert.NoError(t, err)
		assert.Equal(t, rangeData, buf.Bytes())
	})

	t.Run("range spanning multiple chunks", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		// Object layout:
		// Chunk 1: bytes 0-999 (size 1000)
		// Chunk 2: bytes 1000-1999 (size 1000)
		// Chunk 3: bytes 2000-2999 (size 1000)
		// Range request: bytes 500-2499 (should read from all 3 chunks)

		chunk1Part := []byte("chunk1-last-500-bytes")
		chunk2Full := []byte("chunk2-full-1000-bytes")
		chunk3Part := []byte("chunk3-first-500-bytes")

		// Chunk 1: offset 500 within chunk, length 500
		mockFile.EXPECT().
			GetChunkRange(mock.Anything, "file-1:8081", "chunk-1", uint64(500), uint64(500), mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, offset, length uint64, writer clientpkg.ObjectWriter) {
				writer(chunk1Part)
			}).
			Return(nil)

		// Chunk 2: offset 0 within chunk, length 1000 (full chunk)
		mockFile.EXPECT().
			GetChunkRange(mock.Anything, "file-1:8081", "chunk-2", uint64(0), uint64(1000), mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, offset, length uint64, writer clientpkg.ObjectWriter) {
				writer(chunk2Full)
			}).
			Return(nil)

		// Chunk 3: offset 0 within chunk, length 500
		mockFile.EXPECT().
			GetChunkRange(mock.Anything, "file-1:8081", "chunk-3", uint64(0), uint64(500), mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, offset, length uint64, writer clientpkg.ObjectWriter) {
				writer(chunk3Part)
			}).
			Return(nil)

		var buf bytes.Buffer
		err := coord.ReadObjectRange(context.Background(), &ReadRangeRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-2", Offset: 1000, Size: 1000, FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-3", Offset: 2000, Size: 1000, FileServerAddr: "file-1:8081"},
			},
			Offset: 500,
			Length: 2000,
		}, &buf)

		assert.NoError(t, err)
		expected := append(chunk1Part, chunk2Full...)
		expected = append(expected, chunk3Part...)
		assert.Equal(t, expected, buf.Bytes())
	})

	t.Run("range skips non-overlapping chunks", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		// Only chunk 2 should be read
		chunk2Data := []byte("only chunk 2")

		mockFile.EXPECT().
			GetChunkRange(mock.Anything, "file-1:8081", "chunk-2", uint64(100), uint64(200), mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, offset, length uint64, writer clientpkg.ObjectWriter) {
				writer(chunk2Data)
			}).
			Return(nil)

		var buf bytes.Buffer
		err := coord.ReadObjectRange(context.Background(), &ReadRangeRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-2", Offset: 1000, Size: 1000, FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-3", Offset: 2000, Size: 1000, FileServerAddr: "file-1:8081"},
			},
			Offset: 1100, // Start in chunk 2
			Length: 200,  // End in chunk 2
		}, &buf)

		assert.NoError(t, err)
		assert.Equal(t, chunk2Data, buf.Bytes())
	})

	t.Run("range failover to second replica", func(t *testing.T) {
		mockFile := mocks.NewMockFile(t)
		coord := &Coordinator{fileClientPool: mockFile}

		rangeData := []byte("fallback range")

		// First replica fails
		mockFile.EXPECT().
			GetChunkRange(mock.Anything, "file-1:8081", "chunk-1", uint64(0), uint64(100), mock.Anything).
			Return(errors.New("connection refused"))

		// Second replica succeeds
		mockFile.EXPECT().
			GetChunkRange(mock.Anything, "file-2:8081", "chunk-1", uint64(0), uint64(100), mock.Anything).
			Run(func(ctx context.Context, address string, chunkID string, offset, length uint64, writer clientpkg.ObjectWriter) {
				writer(rangeData)
			}).
			Return(nil)

		var buf bytes.Buffer
		err := coord.ReadObjectRange(context.Background(), &ReadRangeRequest{
			ChunkRefs: []types.ChunkRef{
				{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-1:8081"},
				{ChunkID: "chunk-1", Offset: 0, Size: 1000, FileServerAddr: "file-2:8081"},
			},
			Offset: 0,
			Length: 100,
		}, &buf)

		assert.NoError(t, err)
		assert.Equal(t, rangeData, buf.Bytes())
	})
}
