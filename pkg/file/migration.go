// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"
)

// streamChunkSize is the size of chunks sent during migration streaming
const streamChunkSize = 64 * 1024 // 64KB

// MigrateChunk transfers a chunk to a target file server.
// This is the entry point called by the manager or admin to initiate migration.
func (fs *FileServer) MigrateChunk(ctx context.Context, req *file_pb.MigrateChunkRequest) (*file_pb.MigrateChunkResponse, error) {
	start := time.Now()
	chunkID := types.ChunkID(req.GetChunkId())
	targetServer := req.GetTargetServer()

	logger.Info().
		Str("chunk_id", string(chunkID)).
		Str("target_server", targetServer).
		Bool("delete_after", req.GetDeleteAfterMigrate()).
		Msg("Starting chunk migration")

	// Get chunk info from local index
	chunk, err := fs.store.GetChunkInfo(chunkID)
	if err != nil {
		logger.Error().Err(err).Str("chunk_id", string(chunkID)).Msg("Chunk not found for migration")
		return &file_pb.MigrateChunkResponse{
			Success: false,
			Error:   fmt.Sprintf("chunk not found: %v", err),
		}, nil
	}

	// Get peer connection to target server
	peerClient, err := fs.peerPool.Get(ctx, targetServer)
	if err != nil {
		logger.Error().Err(err).Str("target_server", targetServer).Msg("Failed to connect to target server")
		return &file_pb.MigrateChunkResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to connect to target: %v", err),
		}, nil
	}

	// Open stream to send chunk
	stream, err := peerClient.ReceiveChunk(ctx)
	if err != nil {
		logger.Error().Err(err).Str("target_server", targetServer).Msg("Failed to open receive stream")
		return &file_pb.MigrateChunkResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to open stream: %v", err),
		}, nil
	}

	// Send metadata first
	targetBackendID := req.GetTargetBackendId()
	if targetBackendID == "" {
		targetBackendID = chunk.BackendID // Use same backend ID if not specified
	}

	meta := &file_pb.ReceiveChunkMeta{
		ChunkId:      string(chunkID),
		BackendId:    targetBackendID,
		Size:         chunk.Size,
		Checksum:     chunk.Checksum,
		SourceServer: fs.store.GetDefaultBackend().ID, // Source identifier
	}

	if err := stream.Send(&file_pb.ReceiveChunkRequest{
		Payload: &file_pb.ReceiveChunkRequest_Meta{Meta: meta},
	}); err != nil {
		logger.Error().Err(err).Msg("Failed to send chunk metadata")
		return &file_pb.MigrateChunkResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to send metadata: %v", err),
		}, nil
	}

	// Read chunk data from local storage and stream to target
	reader, err := fs.store.GetChunk(ctx, chunkID)
	if err != nil {
		logger.Error().Err(err).Str("chunk_id", string(chunkID)).Msg("Failed to read chunk data")
		return &file_pb.MigrateChunkResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to read chunk: %v", err),
		}, nil
	}
	defer reader.Close()

	buf := make([]byte, streamChunkSize)
	var bytesTransferred int64

	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&file_pb.ReceiveChunkRequest{
				Payload: &file_pb.ReceiveChunkRequest_Data{Data: buf[:n]},
			}); sendErr != nil {
				logger.Error().Err(sendErr).Msg("Failed to send chunk data")
				return &file_pb.MigrateChunkResponse{
					Success: false,
					Error:   fmt.Sprintf("failed to send data: %v", sendErr),
				}, nil
			}
			bytesTransferred += int64(n)
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error().Err(err).Msg("Error reading chunk data")
			return &file_pb.MigrateChunkResponse{
				Success: false,
				Error:   fmt.Sprintf("error reading chunk: %v", err),
			}, nil
		}
	}

	// Close stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to close stream")
		return &file_pb.MigrateChunkResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to close stream: %v", err),
		}, nil
	}

	if !resp.GetSuccess() {
		logger.Error().Str("error", resp.GetError()).Msg("Target server rejected chunk")
		return &file_pb.MigrateChunkResponse{
			Success: false,
			Error:   resp.GetError(),
		}, nil
	}

	duration := time.Since(start)

	// Delete source chunk if requested
	if req.GetDeleteAfterMigrate() {
		if err := fs.deleteChunkLocal(ctx, chunkID, chunk); err != nil {
			logger.Warn().Err(err).Str("chunk_id", string(chunkID)).Msg("Failed to delete source chunk after migration")
			// Don't fail the migration - chunk was successfully copied
		} else {
			logger.Info().Str("chunk_id", string(chunkID)).Msg("Deleted source chunk after migration")
		}
	}

	logger.Info().
		Str("chunk_id", string(chunkID)).
		Str("target_server", targetServer).
		Int64("bytes", bytesTransferred).
		Dur("duration", duration).
		Msg("Chunk migration completed")

	return &file_pb.MigrateChunkResponse{
		Success:          true,
		BytesTransferred: bytesTransferred,
		DurationMs:       duration.Milliseconds(),
	}, nil
}

// ReceiveChunk receives a chunk from a peer file server during migration.
func (fs *FileServer) ReceiveChunk(stream file_pb.FileService_ReceiveChunkServer) error {
	// First message should be metadata
	firstMsg, err := stream.Recv()
	if err != nil {
		return fmt.Errorf("failed to receive metadata: %w", err)
	}

	meta := firstMsg.GetMeta()
	if meta == nil {
		return fmt.Errorf("first message must be metadata")
	}

	chunkID := types.ChunkID(meta.GetChunkId())
	backendID := meta.GetBackendId()
	expectedSize := meta.GetSize()
	expectedChecksum := meta.GetChecksum()

	logger.Info().
		Str("chunk_id", string(chunkID)).
		Str("backend_id", backendID).
		Uint64("size", expectedSize).
		Str("source", meta.GetSourceServer()).
		Msg("Receiving migrated chunk")

	// Check if chunk already exists
	if _, err := fs.store.GetChunkInfo(chunkID); err == nil {
		logger.Info().Str("chunk_id", string(chunkID)).Msg("Chunk already exists, skipping")
		return stream.SendAndClose(&file_pb.ReceiveChunkResponse{
			Success: true,
			ChunkId: string(chunkID),
		})
	}

	// Get target backend
	_, ok := fs.store.GetBackend(backendID)
	if !ok {
		// Fall back to default backend
		defaultBackend := fs.store.GetDefaultBackend()
		if defaultBackend == nil {
			return stream.SendAndClose(&file_pb.ReceiveChunkResponse{
				Success: false,
				Error:   "no backend available",
				ChunkId: string(chunkID),
			})
		}
		backendID = defaultBackend.ID
	}

	// Pre-allocate buffer to avoid append() reallocations
	data := make([]byte, 0, expectedSize)
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error receiving data: %w", err)
		}

		chunk := msg.GetData()
		if chunk != nil {
			data = append(data, chunk...)
		}
	}

	// Verify size
	if uint64(len(data)) != expectedSize {
		return stream.SendAndClose(&file_pb.ReceiveChunkResponse{
			Success: false,
			Error:   fmt.Sprintf("size mismatch: expected %d, got %d", expectedSize, len(data)),
			ChunkId: string(chunkID),
		})
	}

	// Verify checksum if provided
	if expectedChecksum != "" {
		actualChecksum := types.ChunkIDFromBytes(data)
		if string(actualChecksum) != expectedChecksum && string(chunkID) != expectedChecksum {
			return stream.SendAndClose(&file_pb.ReceiveChunkResponse{
				Success: false,
				Error:   "checksum mismatch",
				ChunkId: string(chunkID),
			})
		}
	}

	// Write chunk to storage using existing WriteChunk
	if _, err := fs.store.WriteChunk(stream.Context(), chunkID, data, backendID); err != nil {
		logger.Error().Err(err).Str("chunk_id", string(chunkID)).Msg("Failed to write migrated chunk")
		return stream.SendAndClose(&file_pb.ReceiveChunkResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to write chunk: %v", err),
			ChunkId: string(chunkID),
		})
	}

	logger.Info().
		Str("chunk_id", string(chunkID)).
		Str("backend_id", backendID).
		Int("size", len(data)).
		Msg("Successfully received migrated chunk")

	return stream.SendAndClose(&file_pb.ReceiveChunkResponse{
		Success: true,
		ChunkId: string(chunkID),
	})
}

// GetChunk streams raw chunk data by ID (used for migration).
// Supports range reads via offset and length parameters.
func (fs *FileServer) GetChunk(req *file_pb.GetChunkRequest, stream file_pb.FileService_GetChunkServer) error {
	chunkID := types.ChunkID(req.GetChunkId())
	offset := req.GetOffset()
	length := req.GetLength()

	// Get chunk info
	chunk, err := fs.store.GetChunkInfo(chunkID)
	if err != nil {
		return fmt.Errorf("chunk not found: %w", err)
	}

	// Validate range parameters
	if offset < 0 {
		return fmt.Errorf("invalid offset: %d", offset)
	}
	if offset >= int64(chunk.Size) {
		return fmt.Errorf("offset %d exceeds chunk size %d", offset, chunk.Size)
	}

	// Calculate actual size to return
	actualSize := chunk.Size
	if offset > 0 || length > 0 {
		if length == 0 {
			// Read from offset to end
			actualSize = chunk.Size - uint64(offset)
		} else {
			// Read specified length
			maxAvailable := int64(chunk.Size) - offset
			if length > maxAvailable {
				length = maxAvailable
			}
			actualSize = uint64(length)
		}
	}

	// Send metadata first
	if err := stream.Send(&file_pb.GetChunkResponse{
		Payload: &file_pb.GetChunkResponse_Meta{
			Meta: &file_pb.GetChunkMeta{
				ChunkId:   string(chunkID),
				Size:      actualSize,
				Checksum:  chunk.Checksum,
				BackendId: chunk.BackendID,
			},
		},
	}); err != nil {
		return fmt.Errorf("failed to send metadata: %w", err)
	}

	// Stream chunk data (use range read if offset/length specified)
	var reader io.ReadCloser
	if offset > 0 || length > 0 {
		reader, err = fs.store.GetChunkRange(stream.Context(), chunkID, offset, int64(actualSize))
	} else {
		reader, err = fs.store.GetChunk(stream.Context(), chunkID)
	}
	if err != nil {
		return fmt.Errorf("failed to read chunk: %w", err)
	}
	defer reader.Close()

	buf := make([]byte, streamChunkSize)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			if sendErr := stream.Send(&file_pb.GetChunkResponse{
				Payload: &file_pb.GetChunkResponse_Data{Data: buf[:n]},
			}); sendErr != nil {
				return fmt.Errorf("failed to send data: %w", sendErr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("error reading chunk: %w", err)
		}
	}

	return nil
}

// deleteChunkLocal removes a chunk from local storage and index.
func (fs *FileServer) deleteChunkLocal(ctx context.Context, chunkID types.ChunkID, chunk *types.Chunk) error {
	// Delete from backend storage
	storage, ok := fs.store.GetBackendStorage(chunk.BackendID)
	if !ok {
		return fmt.Errorf("backend %s not found", chunk.BackendID)
	}

	if err := storage.Delete(ctx, chunk.Path); err != nil {
		return fmt.Errorf("failed to delete from storage: %w", err)
	}

	// Delete from index
	if err := fs.store.DeleteChunk(ctx, chunkID); err != nil {
		return fmt.Errorf("failed to delete from index: %w", err)
	}

	return nil
}
