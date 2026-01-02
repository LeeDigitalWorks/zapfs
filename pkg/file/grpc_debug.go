// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ListLocalChunks streams all locally stored chunks.
// This is a debug/diagnostic RPC for testing and admin operations.
func (fs *FileServer) ListLocalChunks(_ *file_pb.ListLocalChunksRequest, stream file_pb.FileService_ListLocalChunksServer) error {
	logger.Debug().Msg("ListLocalChunks started")

	err := fs.store.IterateChunks(func(id types.ChunkID, chunk types.Chunk) error {
		return stream.Send(&file_pb.LocalChunkInfo{
			ChunkId:      string(id),
			Size:         int64(chunk.Size),
			CreatedAt:    chunk.CreatedAt,
			BackendId:    chunk.BackendID,
			Path:         chunk.Path,
			RefCount:     chunk.RefCount,
			ZeroRefSince: chunk.ZeroRefSince,
		})
	})
	if err != nil {
		logger.Error().Err(err).Msg("ListLocalChunks failed")
		return status.Errorf(codes.Internal, "failed to iterate chunks: %v", err)
	}

	logger.Debug().Msg("ListLocalChunks completed")
	return nil
}

// GetLocalChunk returns information about a specific locally stored chunk.
// This is a debug/diagnostic RPC for testing and admin operations.
func (fs *FileServer) GetLocalChunk(_ context.Context, req *file_pb.GetLocalChunkRequest) (*file_pb.LocalChunkInfo, error) {
	chunkID := types.ChunkID(req.GetChunkId())

	chunk, err := fs.store.GetChunkInfo(chunkID)
	if err != nil {
		logger.Debug().
			Str("chunk_id", string(chunkID)).
			Err(err).
			Msg("GetLocalChunk: chunk not found")
		return nil, status.Errorf(codes.NotFound, "chunk not found: %s", chunkID)
	}

	return &file_pb.LocalChunkInfo{
		ChunkId:      string(chunk.ID),
		Size:         int64(chunk.Size),
		CreatedAt:    chunk.CreatedAt,
		BackendId:    chunk.BackendID,
		Path:         chunk.Path,
		RefCount:     chunk.RefCount,
		ZeroRefSince: chunk.ZeroRefSince,
	}, nil
}

// DeleteLocalChunk deletes a chunk from local storage and index.
// WARNING: This is a debug/diagnostic RPC for testing only.
// It bypasses normal GC and ref counting - use with extreme caution.
func (fs *FileServer) DeleteLocalChunk(ctx context.Context, req *file_pb.DeleteLocalChunkRequest) (*file_pb.DeleteLocalChunkResponse, error) {
	chunkID := types.ChunkID(req.GetChunkId())

	logger.Warn().
		Str("chunk_id", string(chunkID)).
		Msg("DeleteLocalChunk: deleting chunk via debug API")

	// Get chunk info first to know which backend to delete from
	chunk, err := fs.store.GetChunkInfo(chunkID)
	if err != nil {
		return &file_pb.DeleteLocalChunkResponse{
			Success: false,
			Error:   "chunk not found in index",
		}, nil
	}

	// Delete from backend storage
	backend, ok := fs.store.GetBackendStorage(chunk.BackendID)
	if !ok {
		return &file_pb.DeleteLocalChunkResponse{
			Success: false,
			Error:   "backend not found: " + chunk.BackendID,
		}, nil
	}

	if err := backend.Delete(ctx, chunk.Path); err != nil {
		logger.Error().
			Err(err).
			Str("chunk_id", string(chunkID)).
			Str("backend_id", chunk.BackendID).
			Str("path", chunk.Path).
			Msg("DeleteLocalChunk: failed to delete from backend")
		return &file_pb.DeleteLocalChunkResponse{
			Success: false,
			Error:   "failed to delete from backend: " + err.Error(),
		}, nil
	}

	// Delete from index
	if err := fs.store.DeleteChunk(ctx, chunkID); err != nil {
		logger.Error().
			Err(err).
			Str("chunk_id", string(chunkID)).
			Msg("DeleteLocalChunk: failed to delete from index")
		return &file_pb.DeleteLocalChunkResponse{
			Success: false,
			Error:   "deleted from backend but failed to delete from index: " + err.Error(),
		}, nil
	}

	logger.Info().
		Str("chunk_id", string(chunkID)).
		Msg("DeleteLocalChunk: chunk deleted successfully")

	return &file_pb.DeleteLocalChunkResponse{
		Success: true,
	}, nil
}
