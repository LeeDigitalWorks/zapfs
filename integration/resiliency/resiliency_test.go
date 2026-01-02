//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package resiliency

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// computeChunkID computes the expected chunk ID for data (SHA-256 hash)
// Note: File servers use content-hash ChunkIDs, but the registry may use ObjectIDs
func computeChunkID(data []byte) string {
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}

// TestSingleFileUpload verifies that uploading a file creates a chunk on a file server
// and that the data can be retrieved correctly
func TestSingleFileUpload(t *testing.T) {
	s3 := newS3Client(t)
	file1 := newFileClient(t, fileServer1Addr)
	file2 := newFileClient(t, fileServer2Addr)

	// Create test bucket
	bucket := testutil.UniqueID("chunk-test")
	s3.CreateBucket(bucket)
	defer s3.DeleteBucket(bucket)

	// Count chunks before upload
	chunksBefore := len(file1.ListLocalChunks()) + len(file2.ListLocalChunks())

	// Upload small file
	data := testutil.GenerateTestData(t, 1024) // 1KB
	key := "test-object.txt"
	s3.PutObject(bucket, key, data)
	defer s3.DeleteObject(bucket, key)

	// Verify we can read the data back correctly via S3
	retrieved := s3.GetObject(bucket, key)
	assert.Equal(t, data, retrieved, "retrieved data should match uploaded data")

	// Compute expected chunk ID (content hash)
	expectedChunkID := computeChunkID(data)

	// Wait for chunk to appear on a file server
	WaitForCondition(t, 10*time.Second, 500*time.Millisecond, func() bool {
		chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
		chunk2, err2 := file2.GetLocalChunk(expectedChunkID)
		return (err1 == nil && chunk1 != nil) || (err2 == nil && chunk2 != nil)
	}, "chunk to appear on file server")

	// Verify total chunks increased
	chunksAfter := len(file1.ListLocalChunks()) + len(file2.ListLocalChunks())
	assert.Greater(t, chunksAfter, chunksBefore, "total chunks should increase after upload")

	// Verify the chunk info
	chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
	chunk2, err2 := file2.GetLocalChunk(expectedChunkID)

	var chunk *file_pb.LocalChunkInfo
	if err1 == nil && chunk1 != nil {
		chunk = chunk1
	} else if err2 == nil && chunk2 != nil {
		chunk = chunk2
	}

	require.NotNil(t, chunk, "chunk should exist on at least one file server")
	assert.Equal(t, int64(1024), chunk.Size, "chunk size should match")
}

// TestChunkDeduplication verifies that uploading duplicate content uses the same chunk
func TestChunkDeduplication(t *testing.T) {
	s3 := newS3Client(t)
	file1 := newFileClient(t, fileServer1Addr)
	file2 := newFileClient(t, fileServer2Addr)

	// Create test bucket
	bucket := testutil.UniqueID("dedup-test")
	s3.CreateBucket(bucket)
	defer s3.DeleteBucket(bucket)

	// Generate test data
	data := testutil.GenerateTestData(t, 2048) // 2KB
	expectedChunkID := computeChunkID(data)

	// Upload first object
	key1 := "object-1.txt"
	s3.PutObject(bucket, key1, data)
	defer s3.DeleteObject(bucket, key1)

	// Wait for chunk to appear on a file server
	WaitForCondition(t, 10*time.Second, 500*time.Millisecond, func() bool {
		chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
		chunk2, err2 := file2.GetLocalChunk(expectedChunkID)
		return (err1 == nil && chunk1 != nil) || (err2 == nil && chunk2 != nil)
	}, "chunk to appear on file server after first upload")

	// Count total chunks before second upload
	chunksBefore := len(file1.ListLocalChunks()) + len(file2.ListLocalChunks())

	// Upload second object with same content
	key2 := "object-2.txt"
	s3.PutObject(bucket, key2, data)
	defer s3.DeleteObject(bucket, key2)

	// Verify both objects are readable
	retrieved1 := s3.GetObject(bucket, key1)
	retrieved2 := s3.GetObject(bucket, key2)
	assert.Equal(t, data, retrieved1, "first object should match")
	assert.Equal(t, data, retrieved2, "second object should match")

	// Verify chunk count didn't increase (deduplication worked)
	chunksAfter := len(file1.ListLocalChunks()) + len(file2.ListLocalChunks())
	assert.Equal(t, chunksBefore, chunksAfter, "chunk count should not increase for duplicate content")
}

// TestChunkMetadata verifies the chunk metadata on file servers is correct
func TestChunkMetadata(t *testing.T) {
	s3 := newS3Client(t)
	file1 := newFileClient(t, fileServer1Addr)
	file2 := newFileClient(t, fileServer2Addr)

	bucket := testutil.UniqueID("metadata-test")
	s3.CreateBucket(bucket)
	defer s3.DeleteBucket(bucket)

	data := testutil.GenerateTestData(t, 4096) // 4KB
	key := "metadata-test.bin"
	expectedChunkID := computeChunkID(data)

	s3.PutObject(bucket, key, data)
	defer s3.DeleteObject(bucket, key)

	// Wait for chunk to appear on a file server
	WaitForCondition(t, 10*time.Second, 500*time.Millisecond, func() bool {
		chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
		chunk2, err2 := file2.GetLocalChunk(expectedChunkID)
		return (err1 == nil && chunk1 != nil) || (err2 == nil && chunk2 != nil)
	}, "chunk to appear on file server")

	// Get chunk info from whichever server has it
	chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
	chunk2, err2 := file2.GetLocalChunk(expectedChunkID)

	var chunk *file_pb.LocalChunkInfo
	if err1 == nil && chunk1 != nil {
		chunk = chunk1
	} else if err2 == nil && chunk2 != nil {
		chunk = chunk2
	}

	require.NotNil(t, chunk, "chunk should exist")
	assert.Equal(t, expectedChunkID, chunk.ChunkId)
	assert.Equal(t, int64(4096), chunk.Size)
	assert.NotEmpty(t, chunk.BackendId, "BackendId should be set")
	assert.NotEmpty(t, chunk.Path, "Path should be set")
	assert.Greater(t, chunk.CreatedAt, int64(0), "CreatedAt should be set")
}

// TestChunkPlacement verifies chunks are placed on file servers
func TestChunkPlacement(t *testing.T) {
	s3 := newS3Client(t)
	file1 := newFileClient(t, fileServer1Addr)
	file2 := newFileClient(t, fileServer2Addr)

	bucket := testutil.UniqueID("placement-test")
	s3.CreateBucket(bucket)
	defer s3.DeleteBucket(bucket)

	data := testutil.GenerateTestData(t, 1024)
	key := "placement-test.bin"
	expectedChunkID := computeChunkID(data)

	s3.PutObject(bucket, key, data)
	defer s3.DeleteObject(bucket, key)

	// Wait for chunk on file server
	WaitForCondition(t, 10*time.Second, 500*time.Millisecond, func() bool {
		chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
		chunk2, err2 := file2.GetLocalChunk(expectedChunkID)
		return (err1 == nil && chunk1 != nil) || (err2 == nil && chunk2 != nil)
	}, "chunk to appear on file server")

	// Verify data is still readable
	retrieved := s3.GetObject(bucket, key)
	assert.Equal(t, data, retrieved)
}

// TestChunkPersistence verifies chunks persist and data remains readable
func TestChunkPersistence(t *testing.T) {
	s3 := newS3Client(t)
	file1 := newFileClient(t, fileServer1Addr)
	file2 := newFileClient(t, fileServer2Addr)

	bucket := testutil.UniqueID("persist-test")
	s3.CreateBucket(bucket)
	defer s3.DeleteBucket(bucket)

	data := testutil.GenerateTestData(t, 1024)
	key := "persist-test.bin"
	expectedChunkID := computeChunkID(data)

	// Upload object
	s3.PutObject(bucket, key, data)

	// Wait for chunk to appear
	WaitForCondition(t, 10*time.Second, 500*time.Millisecond, func() bool {
		chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
		chunk2, err2 := file2.GetLocalChunk(expectedChunkID)
		return (err1 == nil && chunk1 != nil) || (err2 == nil && chunk2 != nil)
	}, "chunk to appear on file server")

	// Read multiple times to verify persistence
	for i := 0; i < 3; i++ {
		retrieved := s3.GetObject(bucket, key)
		assert.Equal(t, data, retrieved, "read %d should return correct data", i+1)
	}

	// Cleanup
	s3.DeleteObject(bucket, key)
}

// TestLocalChunkNotFound verifies GetLocalChunk returns NOT_FOUND for missing chunks
func TestLocalChunkNotFound(t *testing.T) {
	file1 := newFileClient(t, fileServer1Addr)

	// Query for non-existent chunk
	_, err := file1.GetLocalChunk("nonexistent-chunk-id-12345")
	require.Error(t, err)

	// Verify it's a NOT_FOUND error
	st, ok := status.FromError(err)
	require.True(t, ok, "error should be a gRPC status")
	assert.Equal(t, codes.NotFound, st.Code(), "should return NOT_FOUND")
}

// TestListLocalChunks verifies ListLocalChunks returns stored chunks
func TestListLocalChunks(t *testing.T) {
	s3 := newS3Client(t)
	file1 := newFileClient(t, fileServer1Addr)
	file2 := newFileClient(t, fileServer2Addr)

	bucket := testutil.UniqueID("list-chunks-test")
	s3.CreateBucket(bucket)
	defer s3.DeleteBucket(bucket)

	// Upload a few objects
	for i := 0; i < 3; i++ {
		data := testutil.GenerateTestData(t, 512+i*100)
		key := testutil.UniqueID("obj")
		s3.PutObject(bucket, key, data)
		defer s3.DeleteObject(bucket, key)
	}

	// Give time for chunks to be stored
	time.Sleep(2 * time.Second)

	// List chunks from both servers
	chunks1 := file1.ListLocalChunks()
	chunks2 := file2.ListLocalChunks()

	totalChunks := len(chunks1) + len(chunks2)
	t.Logf("File server 1 has %d chunks, file server 2 has %d chunks", len(chunks1), len(chunks2))

	// At least some chunks should exist
	assert.Greater(t, totalChunks, 0, "should have at least some chunks across servers")

	// Verify chunk info is populated
	for _, chunk := range chunks1 {
		assert.NotEmpty(t, chunk.ChunkId, "ChunkId should not be empty")
		assert.Greater(t, chunk.Size, int64(0), "Size should be positive")
		assert.NotEmpty(t, chunk.BackendId, "BackendId should not be empty")
	}
}

// TestDataIntegrity verifies that data is stored and retrieved correctly
func TestDataIntegrity(t *testing.T) {
	s3 := newS3Client(t)

	bucket := testutil.UniqueID("integrity-test")
	s3.CreateBucket(bucket)
	defer s3.DeleteBucket(bucket)

	// Test various sizes
	sizes := []int{
		1,           // 1 byte
		1024,        // 1 KB
		64 * 1024,   // 64 KB (streaming buffer size)
		100 * 1024,  // 100 KB
		1024 * 1024, // 1 MB
	}

	for _, size := range sizes {
		t.Run(testutil.UniqueID("size"), func(t *testing.T) {
			data := testutil.GenerateTestData(t, size)
			key := testutil.UniqueID("obj")

			// Upload
			s3.PutObject(bucket, key, data)

			// Retrieve and verify
			retrieved := s3.GetObject(bucket, key)
			require.Equal(t, len(data), len(retrieved), "size mismatch for %d byte object", size)
			assert.Equal(t, data, retrieved, "data mismatch for %d byte object", size)

			// Verify ETag matches MD5
			expectedETag := testutil.ComputeETag(data)
			head := s3.HeadObject(bucket, key)
			// ETags are often quoted
			actualETag := *head.ETag
			if len(actualETag) > 2 && actualETag[0] == '"' {
				actualETag = actualETag[1 : len(actualETag)-1]
			}
			assert.Equal(t, expectedETag, actualETag, "ETag should match MD5 for %d byte object", size)

			// Cleanup
			s3.DeleteObject(bucket, key)
		})
	}
}

// TestDeleteLocalChunk verifies the debug API can delete chunks
func TestDeleteLocalChunk(t *testing.T) {
	s3 := newS3Client(t)
	file1 := newFileClient(t, fileServer1Addr)
	file2 := newFileClient(t, fileServer2Addr)

	bucket := testutil.UniqueID("delete-chunk-test")
	s3.CreateBucket(bucket)
	defer s3.DeleteBucket(bucket)

	// Upload an object
	data := testutil.GenerateTestData(t, 1024)
	key := "delete-test.bin"
	expectedChunkID := computeChunkID(data)

	s3.PutObject(bucket, key, data)

	// Wait for chunk to appear on a file server
	WaitForCondition(t, 10*time.Second, 500*time.Millisecond, func() bool {
		chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
		chunk2, err2 := file2.GetLocalChunk(expectedChunkID)
		return (err1 == nil && chunk1 != nil) || (err2 == nil && chunk2 != nil)
	}, "chunk to appear on file server")

	// Find which server has the chunk
	chunk1, err1 := file1.GetLocalChunk(expectedChunkID)
	chunk2, err2 := file2.GetLocalChunk(expectedChunkID)

	var targetClient *testutil.FileClient
	if err1 == nil && chunk1 != nil {
		targetClient = file1
	} else if err2 == nil && chunk2 != nil {
		targetClient = file2
	} else {
		t.Skip("chunk not found on either file server (may have been placed elsewhere)")
	}

	// Delete the chunk via debug API
	resp := targetClient.DeleteLocalChunk(expectedChunkID)
	assert.True(t, resp.Success, "DeleteLocalChunk should succeed")
	assert.Empty(t, resp.Error, "Error should be empty on success")

	// Verify chunk is gone from that server
	_, err := targetClient.GetLocalChunk(expectedChunkID)
	require.Error(t, err, "chunk should no longer exist on server")

	// Cleanup - delete object (may fail if chunk is gone, that's ok)
	s3.DeleteObject(bucket, key)
}
