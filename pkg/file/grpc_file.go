// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"io"
	"sync"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"github.com/google/uuid"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Namespace UUID for generating deterministic object UUIDs from string IDs
// This is a random UUID used as the namespace for UUID v5 generation
var objectIDNamespace = uuid.MustParse("6ba7b810-9dad-11d1-80b4-00c04fd430c8")

// streamBufferSize is the size of buffers used for streaming data (64KB)
const streamBufferSize = 64 * 1024

// Delete operation status codes for DeleteObjectResponse
const (
	DeleteStatusSuccess  = 0 // Operation completed successfully
	DeleteStatusNotFound = 1 // Object was not found
	DeleteStatusError    = 2 // Operation failed with error
)

// streamBufferPool provides pooled 64KB buffers for streaming operations.
// Avoids allocation on every GetObject/GetObjectRange call.
var streamBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, streamBufferSize)
		return &buf
	},
}

// getStreamBuffer gets a 64KB buffer from the pool.
func getStreamBuffer() *[]byte {
	return streamBufferPool.Get().(*[]byte)
}

// putStreamBuffer returns a buffer to the pool.
func putStreamBuffer(buf *[]byte) {
	streamBufferPool.Put(buf)
}

// uuidCache caches objectID -> UUID mappings to avoid repeated SHA-1 hashing.
// UUID v5 uses SHA-1 which is ~200ns per call; caching reduces this to ~35ns.
var uuidCache = utils.NewShardedMap[uuid.UUID]()

// objectIDToUUID converts a string object ID to a deterministic UUID.
// Uses UUID v5 (SHA-1 based) to ensure the same objectID always maps to the same UUID.
// Results are cached for performance.
func objectIDToUUID(objectID string) uuid.UUID {
	if cached, ok := uuidCache.Load(objectID); ok {
		return cached
	}
	id := uuid.NewSHA1(objectIDNamespace, []byte(objectID))
	uuidCache.Store(objectID, id)
	return id
}

// streamReader wraps the gRPC stream as an io.Reader.
// This allows streaming data directly to storage without buffering the entire file.
type streamReader struct {
	stream      file_pb.FileService_PutObjectServer
	currentData []byte
	done        bool
}

func newStreamReader(stream file_pb.FileService_PutObjectServer) *streamReader {
	return &streamReader{
		stream: stream,
	}
}

func (r *streamReader) Read(p []byte) (n int, err error) {
	// If we have leftover data from a previous chunk, use it first
	if len(r.currentData) > 0 {
		n = copy(p, r.currentData)
		r.currentData = r.currentData[n:]
		return n, nil
	}

	// If we're done, return EOF
	if r.done {
		return 0, io.EOF
	}

	// Receive next chunk from stream
	msg, err := r.stream.Recv()
	if err == io.EOF {
		r.done = true
		return 0, io.EOF
	}
	if err != nil {
		return 0, err
	}

	chunk := msg.GetChunk()
	if chunk == nil {
		// Empty message, try again
		return r.Read(p)
	}

	// Copy as much as we can to p
	n = copy(p, chunk)
	if n < len(chunk) {
		// Save the rest for the next Read call
		r.currentData = make([]byte, len(chunk)-n)
		copy(r.currentData, chunk[n:])
	}

	return n, nil
}

// PutObject receives a stream of chunks and writes them to local storage.
// Uses chunk-based storage with content-hash deduplication and reference counting.
// Each chunk is stored by its SHA-256 hash, enabling deduplication across objects.
func (fs *FileServer) PutObject(stream file_pb.FileService_PutObjectServer) error {
	ctx := stream.Context()

	// First message must be metadata
	firstMsg, err := stream.Recv()
	if err != nil {
		return status.Errorf(codes.InvalidArgument, "failed to receive metadata: %v", err)
	}

	meta := firstMsg.GetMeta()
	if meta == nil {
		return status.Error(codes.InvalidArgument, "first message must contain metadata")
	}

	objectID := meta.ObjectId

	logger.Debug().
		Str("object_id", objectID).
		Uint64("total_size", meta.TotalSize).
		Msg("PutObject started")

	// Create ObjectRef for chunk-based storage
	obj := types.ObjectRef{
		ID: objectIDToUUID(objectID),
	}

	// Create stream reader - wraps gRPC stream as io.Reader
	reader := newStreamReader(stream)

	// Use chunk-based storage: data is split into 64MB chunks,
	// each chunk identified by SHA-256 hash for deduplication,
	// with reference counting for GC
	if err := fs.store.PutObject(ctx, &obj, reader); err != nil {
		logger.Error().Err(err).Str("object_id", objectID).Msg("failed to write object")
		return status.Errorf(codes.Internal, "failed to write to storage: %v", err)
	}

	// Build chunk info response with actual content-hash based chunk IDs
	chunks := make([]*file_pb.ChunkInfo, 0, len(obj.ChunkRefs))
	for _, ref := range obj.ChunkRefs {
		chunks = append(chunks, &file_pb.ChunkInfo{
			ChunkId: string(ref.ChunkID),
			Size:    ref.Size,
			Offset:  ref.Offset,
		})
	}

	return stream.SendAndClose(&file_pb.PutObjectResponse{
		ObjectId:   objectID,
		Size:       obj.Size,
		Etag:       obj.ETag,
		ModifiedAt: timestamppb.Now(),
		Chunks:     chunks,
	})
}

// GetObject streams an object's data back to the caller.
// Reads from chunk-based storage, reassembling chunks in order.
func (fs *FileServer) GetObject(req *file_pb.GetObjectRequest, stream file_pb.FileService_GetObjectServer) error {
	ctx := stream.Context()
	objectID := req.ObjectId
	objUUID := objectIDToUUID(objectID)

	logger.Debug().Str("object_id", objectID).Msg("GetObject started")

	// Look up object in index
	obj, err := fs.store.GetObject(ctx, objUUID)
	if err != nil {
		return status.Errorf(codes.NotFound, "object not found: %s", objectID)
	}

	// Get pooled buffer for streaming
	bufPtr := getStreamBuffer()
	defer putStreamBuffer(bufPtr)
	buf := *bufPtr

	// Stream each chunk in order
	for _, chunkRef := range obj.ChunkRefs {
		reader, err := fs.store.GetChunk(ctx, chunkRef.ChunkID)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read chunk %s: %v", chunkRef.ChunkID, err)
		}

		// Stream this chunk's data
		for {
			n, readErr := reader.Read(buf)
			if n > 0 {
				if sendErr := stream.Send(&file_pb.GetObjectResponse{
					Chunk: buf[:n],
				}); sendErr != nil {
					reader.Close()
					return status.Errorf(codes.Internal, "failed to send chunk: %v", sendErr)
				}
			}

			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				reader.Close()
				return status.Errorf(codes.Internal, "failed to read chunk: %v", readErr)
			}
		}
		reader.Close()
	}

	logger.Debug().Str("object_id", objectID).Msg("GetObject completed")
	return nil
}

// GetObjectRange streams a range of an object's data.
// Handles range requests across chunk boundaries.
func (fs *FileServer) GetObjectRange(req *file_pb.GetObjectRangeRequest, stream file_pb.FileService_GetObjectRangeServer) error {
	ctx := stream.Context()
	objectID := req.ObjectId
	objUUID := objectIDToUUID(objectID)
	offset := req.Offset
	length := req.Length

	logger.Debug().
		Str("object_id", objectID).
		Uint64("offset", offset).
		Uint64("length", length).
		Msg("GetObjectRange started")

	// Look up object in index
	obj, err := fs.store.GetObject(ctx, objUUID)
	if err != nil {
		return status.Errorf(codes.NotFound, "object not found: %s", objectID)
	}

	// Find chunks that overlap with the requested range
	var bytesRemaining = length
	var currentOffset uint64 = 0

	// Get pooled buffer for streaming
	bufPtr := getStreamBuffer()
	defer putStreamBuffer(bufPtr)
	buf := *bufPtr

	for _, chunkRef := range obj.ChunkRefs {
		chunkStart := currentOffset
		chunkEnd := currentOffset + chunkRef.Size

		// Skip chunks before the range
		if chunkEnd <= offset {
			currentOffset = chunkEnd
			continue
		}

		// Stop if we've read enough
		if bytesRemaining == 0 {
			break
		}

		// Calculate how much to read from this chunk
		readStart := int64(0)
		if offset > chunkStart {
			readStart = int64(offset - chunkStart)
		}

		readLen := int64(chunkRef.Size) - readStart
		if uint64(readLen) > bytesRemaining {
			readLen = int64(bytesRemaining)
		}

		// Read the chunk range
		reader, err := fs.store.GetChunkRange(ctx, chunkRef.ChunkID, readStart, readLen)
		if err != nil {
			return status.Errorf(codes.Internal, "failed to read chunk %s: %v", chunkRef.ChunkID, err)
		}

		// Stream this chunk's data
		for {
			n, readErr := reader.Read(buf)
			if n > 0 {
				if sendErr := stream.Send(&file_pb.GetObjectResponse{
					Chunk: buf[:n],
				}); sendErr != nil {
					reader.Close()
					return status.Errorf(codes.Internal, "failed to send chunk: %v", sendErr)
				}
				bytesRemaining -= uint64(n)
			}

			if readErr == io.EOF {
				break
			}
			if readErr != nil {
				reader.Close()
				return status.Errorf(codes.Internal, "failed to read chunk: %v", readErr)
			}
		}
		reader.Close()

		currentOffset = chunkEnd
	}

	return nil
}

// DeleteObject marks an object as deleted in the index.
// The actual chunk data is cleaned up by GC after RefCounts reach 0.
// The caller (metadata service) is responsible for decrementing chunk RefCounts.
func (fs *FileServer) DeleteObject(ctx context.Context, req *file_pb.DeleteObjectRequest) (*file_pb.DeleteObjectResponse, error) {
	objectID := req.ObjectId
	objUUID := objectIDToUUID(objectID)

	logger.Debug().Str("object_id", objectID).Msg("DeleteObject started")

	// Mark object as deleted in the index
	if err := fs.store.DeleteObject(ctx, objUUID); err != nil {
		return &file_pb.DeleteObjectResponse{
			ObjectId: objectID,
			Status:   DeleteStatusNotFound,
			Error:    "object not found",
		}, nil
	}

	logger.Info().Str("object_id", objectID).Msg("DeleteObject completed")

	return &file_pb.DeleteObjectResponse{
		ObjectId:  objectID,
		Status:    DeleteStatusSuccess,
		DeletedAt: timestamppb.Now(),
	}, nil
}

// BatchDeleteObjects deletes multiple objects at once.
func (fs *FileServer) BatchDeleteObjects(ctx context.Context, req *file_pb.BatchDeleteObjectsRequest) (*file_pb.BatchDeleteObjectsResponse, error) {
	results := make([]*file_pb.DeleteObjectResponse, 0, len(req.ObjectIds))

	for _, objectID := range req.ObjectIds {
		resp, err := fs.DeleteObject(ctx, &file_pb.DeleteObjectRequest{
			ObjectId: objectID,
		})
		if err != nil {
			results = append(results, &file_pb.DeleteObjectResponse{
				ObjectId: objectID,
				Status:   DeleteStatusError,
				Error:    err.Error(),
			})
		} else {
			results = append(results, resp)
		}
	}

	return &file_pb.BatchDeleteObjectsResponse{
		Results: results,
	}, nil
}

// Note: DecrementRefCount and DecrementRefCountBatch RPCs have been removed.
// RefCount is now managed centrally in the metadata DB's chunk_registry table,
// not on individual file servers. See workspace/plans/chunk-registry-redesign.md.
