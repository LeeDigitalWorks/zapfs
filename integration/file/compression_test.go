//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"bytes"
	"context"
	"io"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Compression Tests
// =============================================================================

func TestPutGetObject_LZ4Compression(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-lz4")

	// Create compressible data (repeated patterns compress well)
	data := bytes.Repeat([]byte("compression test data for lz4 algorithm "), 10000)

	// Put with LZ4 compression
	resp := client.PutObject(objectID, data, testutil.WithCompression("lz4"))
	assert.Equal(t, uint64(len(data)), resp.Size, "size should match original")
	assert.Equal(t, testutil.ComputeETag(data), resp.Etag, "etag should match")

	// Verify compression metadata in chunks
	require.NotEmpty(t, resp.Chunks, "should have at least one chunk")
	for _, chunk := range resp.Chunks {
		assert.Equal(t, "lz4", chunk.Compression, "chunk should be lz4 compressed")
		assert.Greater(t, chunk.OriginalSize, uint64(0), "should have original size")
		assert.Less(t, chunk.Size, chunk.OriginalSize, "compressed should be smaller for compressible data")
	}

	// Get and verify data is correctly decompressed
	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved, "retrieved data should match original")

	// Cleanup
	client.DeleteObject(objectID)
}

func TestPutGetObject_ZSTDCompression(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-zstd")

	// Create compressible data
	data := bytes.Repeat([]byte("zstd compression works great for storage "), 10000)

	resp := client.PutObject(objectID, data, testutil.WithCompression("zstd"))
	assert.Equal(t, uint64(len(data)), resp.Size)

	require.NotEmpty(t, resp.Chunks)
	for _, chunk := range resp.Chunks {
		assert.Equal(t, "zstd", chunk.Compression)
		assert.Less(t, chunk.Size, chunk.OriginalSize)
	}

	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved)

	client.DeleteObject(objectID)
}

func TestPutGetObject_SnappyCompression(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-snappy")

	data := bytes.Repeat([]byte("snappy is fast for real-time compression "), 10000)

	resp := client.PutObject(objectID, data, testutil.WithCompression("snappy"))
	assert.Equal(t, uint64(len(data)), resp.Size)

	require.NotEmpty(t, resp.Chunks)
	for _, chunk := range resp.Chunks {
		assert.Equal(t, "snappy", chunk.Compression)
		assert.Less(t, chunk.Size, chunk.OriginalSize)
	}

	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved)

	client.DeleteObject(objectID)
}

func TestPutGetObject_NoCompression(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-nocomp")
	data := testutil.GenerateTestData(t, 10*1024) // 10KB

	// Put without compression
	resp := client.PutObject(objectID, data)
	assert.Equal(t, uint64(len(data)), resp.Size)

	require.NotEmpty(t, resp.Chunks)
	for _, chunk := range resp.Chunks {
		// No compression or "none"
		assert.True(t, chunk.Compression == "" || chunk.Compression == "none",
			"expected no compression, got: %s", chunk.Compression)
	}

	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved)

	client.DeleteObject(objectID)
}

func TestPutGetObject_CompressionSkippedForIncompressible(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-incompressible")

	// Random data doesn't compress well
	data := testutil.GenerateTestData(t, 10*1024) // 10KB random data

	resp := client.PutObject(objectID, data, testutil.WithCompression("lz4"))
	assert.Equal(t, uint64(len(data)), resp.Size)

	// Data should still be readable regardless of whether compression was applied
	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved)

	client.DeleteObject(objectID)
}

func TestPutGetObject_CompressionWithRange(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-comp-range")

	// Create compressible data
	data := bytes.Repeat([]byte("0123456789"), 10000) // 100KB

	resp := client.PutObject(objectID, data, testutil.WithCompression("lz4"))
	assert.Equal(t, uint64(len(data)), resp.Size)

	// Verify range reads work correctly with compression
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	offset := uint64(10000)
	length := uint64(5000)

	stream, err := client.FileServiceClient.GetObjectRange(ctx, &file_pb.GetObjectRangeRequest{
		ObjectId: objectID,
		Offset:   offset,
		Length:   length,
	})
	require.NoError(t, err)

	var rangeData []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		rangeData = append(rangeData, resp.Chunk...)
	}

	expectedRange := data[offset : offset+length]
	assert.Equal(t, expectedRange, rangeData, "range data should match")

	client.DeleteObject(objectID)
}

func TestPutGetObject_LargeCompressed(t *testing.T) {
	testutil.SkipIfShort(t)
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-large-comp")

	// Create large compressible data (10MB)
	data := bytes.Repeat([]byte("large object compression test with repeated patterns "), 200000)

	start := time.Now()
	resp := client.PutObject(objectID, data, testutil.WithCompression("zstd"))
	putDuration := time.Since(start)
	assert.Equal(t, uint64(len(data)), resp.Size)

	// Calculate compression ratio
	var totalCompressed, totalOriginal uint64
	for _, chunk := range resp.Chunks {
		totalCompressed += chunk.Size
		totalOriginal += chunk.OriginalSize
	}
	ratio := float64(totalOriginal) / float64(totalCompressed)
	t.Logf("Put %d bytes in %v, compression ratio: %.2fx (%d -> %d bytes)",
		len(data), putDuration, ratio, totalOriginal, totalCompressed)

	start = time.Now()
	retrieved := client.GetObject(objectID)
	getDuration := time.Since(start)
	assert.Equal(t, data, retrieved)
	t.Logf("Get %d bytes in %v", len(data), getDuration)

	client.DeleteObject(objectID)
}

func TestCompression_Deduplication(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)

	// Create same compressible data for two objects
	data := bytes.Repeat([]byte("deduplicated compressed content "), 5000)

	objectID1 := testutil.UniqueID("test-dedup-comp-1")
	objectID2 := testutil.UniqueID("test-dedup-comp-2")

	// Put both objects with same compression
	resp1 := client.PutObject(objectID1, data, testutil.WithCompression("lz4"))
	resp2 := client.PutObject(objectID2, data, testutil.WithCompression("lz4"))

	// Both should have same ETag (content hash)
	assert.Equal(t, resp1.Etag, resp2.Etag, "ETags should match for identical content")

	// Both should reference the same chunk IDs (deduplication)
	require.NotEmpty(t, resp1.Chunks)
	require.NotEmpty(t, resp2.Chunks)
	assert.Equal(t, len(resp1.Chunks), len(resp2.Chunks), "chunk count should match")

	for i := range resp1.Chunks {
		assert.Equal(t, resp1.Chunks[i].ChunkId, resp2.Chunks[i].ChunkId,
			"chunk %d should be deduplicated", i)
	}

	// Verify both can be read back
	retrieved1 := client.GetObject(objectID1)
	retrieved2 := client.GetObject(objectID2)
	assert.Equal(t, data, retrieved1)
	assert.Equal(t, data, retrieved2)

	client.DeleteObject(objectID1)
	client.DeleteObject(objectID2)
}

func TestCompression_LocalChunkInfo(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-chunk-info")

	data := bytes.Repeat([]byte("chunk info test with compression "), 5000)

	resp := client.PutObject(objectID, data, testutil.WithCompression("zstd"))
	require.NotEmpty(t, resp.Chunks)

	// Get local chunk info and verify compression metadata
	chunkID := resp.Chunks[0].ChunkId
	chunkInfo := client.GetLocalChunkOrFail(chunkID)

	assert.Equal(t, chunkID, chunkInfo.ChunkId)
	assert.Equal(t, "zstd", chunkInfo.Compression)
	assert.Greater(t, chunkInfo.OriginalSize, int64(0))
	assert.Less(t, chunkInfo.Size, chunkInfo.OriginalSize)

	client.DeleteObject(objectID)
}
