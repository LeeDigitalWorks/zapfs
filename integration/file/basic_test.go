//go:build integration

package file

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"zapfs/integration/testutil"
	"zapfs/proto/file_pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Basic Put/Get Tests
// =============================================================================

func TestPutGetObject_Small(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-small")
	data := testutil.GenerateTestData(t, 1024) // 1KB

	// Put
	resp := client.PutObject(objectID, data)
	assert.Equal(t, uint64(len(data)), resp.Size, "size should match")
	assert.Equal(t, testutil.ComputeETag(data), resp.Etag, "etag should match")

	// Get
	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved, "retrieved data should match original")

	// Cleanup
	deleteResp := client.DeleteObject(objectID)
	assert.Equal(t, int32(0), deleteResp.Status, "delete should succeed")
}

func TestPutGetObject_Medium(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-medium")
	data := testutil.GenerateTestData(t, 5*1024*1024) // 5MB

	resp := client.PutObject(objectID, data)
	assert.Equal(t, uint64(len(data)), resp.Size)
	assert.Equal(t, testutil.ComputeETag(data), resp.Etag)

	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved)

	client.DeleteObject(objectID)
}

func TestPutGetObject_Large(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-large")
	data := testutil.GenerateTestData(t, 50*1024*1024) // 50MB

	resp := client.PutObject(objectID, data)
	assert.Equal(t, uint64(len(data)), resp.Size)

	retrieved := client.GetObject(objectID)
	assert.Equal(t, len(data), len(retrieved), "size mismatch")
	assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved), "etag mismatch")

	client.DeleteObject(objectID)
}

func TestPutGetObject_VeryLarge(t *testing.T) {
	testutil.SkipIfShort(t)
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-verylarge")
	data := testutil.GenerateTestData(t, 100*1024*1024) // 100MB

	start := time.Now()
	resp := client.PutObject(objectID, data)
	putDuration := time.Since(start)
	assert.Equal(t, uint64(len(data)), resp.Size)
	t.Logf("Put 100MB in %v (%.2f MB/s)", putDuration, float64(100)/putDuration.Seconds())

	start = time.Now()
	retrieved := client.GetObject(objectID)
	getDuration := time.Since(start)
	assert.Equal(t, len(data), len(retrieved))
	assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved))
	t.Logf("Get 100MB in %v (%.2f MB/s)", getDuration, float64(100)/getDuration.Seconds())

	client.DeleteObject(objectID)
}

func TestGetObject_NotFound(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.FileServiceClient.GetObject(ctx, &file_pb.GetObjectRequest{
		ObjectId: "nonexistent-object-12345",
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	assert.Error(t, err, "should error on nonexistent object")
}

// =============================================================================
// Server Status Tests
// =============================================================================

func TestFileServerStatus(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	resp, err := client.FileServiceClient.FileServerStatus(ctx, &file_pb.FileServerStatusRequest{})
	require.NoError(t, err)

	assert.NotEmpty(t, resp.Backends, "should have at least one backend")
	assert.NotNil(t, resp.MemoryStatus)
	assert.Greater(t, resp.MemoryStatus.Goroutines, int32(0), "should have active goroutines")
}

func TestPing(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	resp := client.Ping()
	assert.Greater(t, resp.CurrentTime, uint64(0))
}

// =============================================================================
// Batch Operations Tests
// =============================================================================

func TestBatchDeleteObjects(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	numObjects := 5
	objectIDs := make([]string, numObjects)

	for i := 0; i < numObjects; i++ {
		objectIDs[i] = testutil.UniqueID(fmt.Sprintf("test-batch-%d", i))
		data := testutil.GenerateTestData(t, 1024)
		client.PutObject(objectIDs[i], data)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	resp, err := client.FileServiceClient.BatchDeleteObjects(ctx, &file_pb.BatchDeleteObjectsRequest{
		ObjectIds: objectIDs,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)

	// Verify objects are deleted
	for _, objectID := range objectIDs {
		ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
		stream, err := client.FileServiceClient.GetObject(ctx2, &file_pb.GetObjectRequest{
			ObjectId: objectID,
		})
		cancel2()

		if err == nil {
			_, recvErr := stream.Recv()
			assert.Error(t, recvErr, "object %s should be deleted", objectID)
		}
	}
}

// =============================================================================
// Range Read Tests
// =============================================================================

func TestGetObjectRange(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-range")
	data := testutil.GenerateTestData(t, 1*1024*1024) // 1MB

	client.PutObject(objectID, data)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	offset := uint64(100 * 1024)
	length := uint64(200 * 1024)

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

func TestGetObjectRange_EdgeCases(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-range-edge")
	data := testutil.GenerateTestData(t, 1*1024*1024) // 1MB

	client.PutObject(objectID, data)

	testCases := []struct {
		name     string
		offset   uint64
		length   uint64
		expected []byte
	}{
		{"first byte", 0, 1, data[0:1]},
		{"last byte", uint64(len(data) - 1), 1, data[len(data)-1:]},
		{"first 100 bytes", 0, 100, data[0:100]},
		{"last 100 bytes", uint64(len(data) - 100), 100, data[len(data)-100:]},
		{"middle 1KB", uint64(len(data)/2 - 512), 1024, data[len(data)/2-512 : len(data)/2+512]},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			stream, err := client.FileServiceClient.GetObjectRange(ctx, &file_pb.GetObjectRangeRequest{
				ObjectId: objectID,
				Offset:   tc.offset,
				Length:   tc.length,
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

			assert.Equal(t, tc.expected, rangeData)
		})
	}

	client.DeleteObject(objectID)
}
