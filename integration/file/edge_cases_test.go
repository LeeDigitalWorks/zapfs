//go:build integration

package file

import (
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
// Edge Case Tests
// =============================================================================

func TestPutGetObject_Empty(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-empty")
	data := []byte{}

	resp := client.PutObject(objectID, data)
	assert.Equal(t, uint64(0), resp.Size)

	retrieved := client.GetObject(objectID)
	assert.Len(t, retrieved, 0, "empty object should have zero length")

	client.DeleteObject(objectID)
}

func TestPutGetObject_SingleByte(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-singlebyte")
	data := []byte{0x42}

	resp := client.PutObject(objectID, data)
	assert.Equal(t, uint64(1), resp.Size)

	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved)

	client.DeleteObject(objectID)
}

func TestPutGetObject_ExactChunkBoundary(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)

	testCases := []struct {
		name string
		size int
	}{
		{"exactly 1 chunk (64KB)", 64 * 1024},
		{"exactly 2 chunks (128KB)", 128 * 1024},
		{"exactly 10 chunks (640KB)", 640 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objectID := testutil.UniqueID("test-boundary")
			data := testutil.GenerateTestData(t, tc.size)

			resp := client.PutObject(objectID, data)
			assert.Equal(t, uint64(tc.size), resp.Size)

			retrieved := client.GetObject(objectID)
			assert.Equal(t, data, retrieved)

			client.DeleteObject(objectID)
		})
	}
}

func TestPutGetObject_SpecialCharactersInID(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	data := testutil.GenerateTestData(t, 1024)

	testCases := []struct {
		name   string
		suffix string
	}{
		{"with slashes", "folder/subfolder/object"},
		{"with dashes", "my-object-with-dashes"},
		{"with underscores", "my_object_with_underscores"},
		{"with dots", "my.object.with.dots"},
		{"mixed", "path/to/my-object_v1.0"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objectID := testutil.UniqueID(tc.suffix)

			resp := client.PutObject(objectID, data)
			assert.Equal(t, uint64(len(data)), resp.Size, "put should succeed for objectID: %s", objectID)

			retrieved := client.GetObject(objectID)
			assert.Equal(t, data, retrieved)

			client.DeleteObject(objectID)
		})
	}
}

// =============================================================================
// Streaming Tests
// =============================================================================

func TestPutGetObject_StreamingChunks(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)

	testCases := []struct {
		name      string
		dataSize  int
		chunkSize int
	}{
		{"aligned 1MB chunks", 10 * 1024 * 1024, 1024 * 1024},
		{"small 4KB chunks", 1 * 1024 * 1024, 4 * 1024},
		{"large 512KB chunks", 5 * 1024 * 1024, 512 * 1024},
		{"unaligned chunks", 3*1024*1024 + 12345, 100 * 1024},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			objectID := testutil.UniqueID("test-stream")
			data := testutil.GenerateTestData(t, tc.dataSize)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			stream, err := client.FileServiceClient.PutObject(ctx)
			require.NoError(t, err)

			err = stream.Send(&file_pb.PutObjectRequest{
				Payload: &file_pb.PutObjectRequest_Meta{
					Meta: &file_pb.PutObjectMeta{
						ObjectId:  objectID,
						TotalSize: uint64(len(data)),
					},
				},
			})
			require.NoError(t, err)

			for offset := 0; offset < len(data); offset += tc.chunkSize {
				end := offset + tc.chunkSize
				if end > len(data) {
					end = len(data)
				}
				err = stream.Send(&file_pb.PutObjectRequest{
					Payload: &file_pb.PutObjectRequest_Chunk{
						Chunk: data[offset:end],
					},
				})
				require.NoError(t, err)
			}

			resp, err := stream.CloseAndRecv()
			require.NoError(t, err)
			assert.Equal(t, uint64(len(data)), resp.Size)

			retrieved := client.GetObject(objectID)
			assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved))

			client.DeleteObject(objectID)
		})
	}
}

// =============================================================================
// Cancellation Tests
// =============================================================================

func TestPutObject_CancelMidUpload(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-cancel")
	dataSize := 10 * 1024 * 1024
	data := testutil.GenerateTestData(t, dataSize)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.FileServiceClient.PutObject(ctx)
	require.NoError(t, err)

	err = stream.Send(&file_pb.PutObjectRequest{
		Payload: &file_pb.PutObjectRequest_Meta{
			Meta: &file_pb.PutObjectMeta{
				ObjectId:  objectID,
				TotalSize: uint64(len(data)),
			},
		},
	})
	require.NoError(t, err)

	chunkSize := 64 * 1024
	chunksSent := 0
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}

		err = stream.Send(&file_pb.PutObjectRequest{
			Payload: &file_pb.PutObjectRequest_Chunk{
				Chunk: data[offset:end],
			},
		})
		chunksSent++

		if offset > dataSize/4 {
			cancel()
			break
		}

		if err != nil {
			break
		}
	}

	t.Logf("Sent %d chunks before cancellation", chunksSent)

	_, err = stream.CloseAndRecv()
	assert.Error(t, err, "should error after cancellation")
}

func TestGetObject_CancelMidDownload(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-cancel-get")
	data := testutil.GenerateTestData(t, 10*1024*1024)

	client.PutObject(objectID, data)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	stream, err := client.FileServiceClient.GetObject(ctx, &file_pb.GetObjectRequest{
		ObjectId: objectID,
	})
	require.NoError(t, err)

	var downloaded []byte
	chunksReceived := 0
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			break
		}
		downloaded = append(downloaded, resp.Chunk...)
		chunksReceived++

		if len(downloaded) > len(data)/4 {
			cancel()
		}
	}

	t.Logf("Received %d chunks (%d bytes)", chunksReceived, len(downloaded))

	if len(downloaded) > 0 && len(downloaded) <= len(data) {
		assert.Equal(t, data[:len(downloaded)], downloaded, "partial data should match")
	}

	newFileClient(t, fileServer1Addr).DeleteObject(objectID)
}

// =============================================================================
// Connection Recovery Tests
// =============================================================================

func TestConnection_ReconnectAfterIdle(t *testing.T) {
	t.Parallel()

	client := newFileClient(t, fileServer1Addr)
	objectID := testutil.UniqueID("test-reconnect")
	data := testutil.GenerateTestData(t, 64*1024)

	resp := client.PutObject(objectID, data)
	assert.Equal(t, uint64(len(data)), resp.Size)

	time.Sleep(2 * time.Second)

	retrieved := client.GetObject(objectID)
	assert.Equal(t, data, retrieved)

	client.DeleteObject(objectID)
}
