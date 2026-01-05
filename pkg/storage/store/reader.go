// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/compression"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
)

// GetObjectData retrieves the full object data
func (fs *FileStore) GetObjectData(ctx context.Context, id uuid.UUID) (io.ReadCloser, error) {
	obj, err := fs.objectIdx.Get(id)
	if err != nil {
		return nil, err
	}

	if obj.IsDeleted() {
		return nil, fmt.Errorf("object deleted")
	}

	// For EC objects, use EC manager
	if obj.IsErasureCoded() {
		// Combine all EC groups
		var readers []io.Reader
		for _, groupID := range obj.ECGroupIDs {
			data, err := fs.ecManager.GetChunk(ctx, groupID)
			if err != nil {
				return nil, fmt.Errorf("read ec group %s: %w", groupID, err)
			}
			readers = append(readers, io.NopCloser(io.NewSectionReader(
				&bytesReaderAt{data}, 0, int64(len(data)),
			)))
		}
		return &multiReadCloser{readers: readers}, nil
	}

	// For regular chunks
	readers := make([]io.Reader, 0, len(obj.ChunkRefs))
	for _, ref := range obj.ChunkRefs {
		rc, err := fs.GetChunk(ctx, ref.ChunkID)
		if err != nil {
			return nil, fmt.Errorf("read chunk %s: %w", ref.ChunkID, err)
		}
		readers = append(readers, rc)
	}

	return &multiReadCloser{readers: readers}, nil
}

// GetObjectRange retrieves a range of object data
func (fs *FileStore) GetObjectRange(ctx context.Context, id uuid.UUID, offset, length int64) (io.ReadCloser, error) {
	obj, err := fs.objectIdx.Get(id)
	if err != nil {
		return nil, err
	}

	if obj.IsDeleted() {
		return nil, fmt.Errorf("object deleted")
	}

	// Simple implementation for regular chunks
	var readers []io.Reader
	var currentOffset int64
	remaining := length

	for _, ref := range obj.ChunkRefs {
		chunkSize := int64(ref.Size)

		// Skip chunks before the range
		if currentOffset+chunkSize <= offset {
			currentOffset += chunkSize
			continue
		}

		// Calculate read range within this chunk
		start := int64(0)
		if offset > currentOffset {
			start = offset - currentOffset
		}

		end := chunkSize
		if remaining < chunkSize-start {
			end = start + remaining
		}

		rc, err := fs.GetChunkRange(ctx, ref.ChunkID, start, end-start)
		if err != nil {
			return nil, err
		}
		readers = append(readers, rc)

		remaining -= (end - start)
		currentOffset += chunkSize

		if remaining <= 0 {
			break
		}
	}

	return &multiReadCloser{readers: readers}, nil
}

// bytesReaderAt wraps []byte for io.ReaderAt
type bytesReaderAt struct {
	data []byte
}

func (r *bytesReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n = copy(p, r.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
}

// multiReadCloser combines multiple readers into one ReadCloser
type multiReadCloser struct {
	readers []io.Reader
	idx     int
}

func (m *multiReadCloser) Read(p []byte) (n int, err error) {
	for m.idx < len(m.readers) {
		n, err = m.readers[m.idx].Read(p)
		if err == io.EOF {
			m.idx++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
	return 0, io.EOF
}

func (m *multiReadCloser) Close() error {
	for _, r := range m.readers {
		if rc, ok := r.(io.Closer); ok {
			rc.Close()
		}
	}
	return nil
}

// =============================================================================
// Chunk Read Operations
// =============================================================================

// GetObject retrieves object metadata by ID.
func (fs *FileStore) GetObject(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	obj, err := fs.objectIdx.Get(id)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// GetChunk reads a chunk from storage, decompressing if needed.
// Returns the original (uncompressed) data.
func (fs *FileStore) GetChunk(ctx context.Context, id types.ChunkID) (io.ReadCloser, error) {
	chunk, err := fs.chunkIdx.Get(id)
	if err != nil {
		return nil, err
	}

	store, ok := fs.manager.Get(chunk.BackendID)
	if !ok {
		return nil, fmt.Errorf("backend %s not found", chunk.BackendID)
	}

	reader, err := store.Read(ctx, chunk.Path)
	if err != nil {
		return nil, err
	}

	// If not compressed, return directly
	if !chunk.IsCompressed() {
		return reader, nil
	}

	// Read and decompress
	return fs.readAndDecompress(reader, chunk.Compression, id)
}

// GetChunkRange reads a range from a chunk, decompressing if needed.
// For compressed chunks, the entire chunk must be decompressed first.
func (fs *FileStore) GetChunkRange(ctx context.Context, id types.ChunkID, offset, length int64) (io.ReadCloser, error) {
	chunk, err := fs.chunkIdx.Get(id)
	if err != nil {
		return nil, err
	}

	store, ok := fs.manager.Get(chunk.BackendID)
	if !ok {
		return nil, fmt.Errorf("backend %s not found", chunk.BackendID)
	}

	// If not compressed, use efficient range read from backend
	if !chunk.IsCompressed() {
		return store.ReadRange(ctx, chunk.Path, offset, length)
	}

	// For compressed chunks, read and decompress entire chunk, then extract range
	reader, err := store.Read(ctx, chunk.Path)
	if err != nil {
		return nil, err
	}

	decompressed, err := fs.readAndDecompressBytes(reader, chunk.Compression, id)
	if err != nil {
		return nil, err
	}

	// Extract requested range
	dataLen := int64(len(decompressed))
	if offset >= dataLen {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}
	end := min(offset+length, dataLen)

	return io.NopCloser(bytes.NewReader(decompressed[offset:end])), nil
}

// GetChunkData reads a chunk and returns the decompressed data as bytes.
func (fs *FileStore) GetChunkData(ctx context.Context, id types.ChunkID) ([]byte, error) {
	reader, err := fs.GetChunk(ctx, id)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// GetChunkDataRange reads a range from a chunk and returns the data as bytes.
func (fs *FileStore) GetChunkDataRange(ctx context.Context, id types.ChunkID, offset, length int64) ([]byte, error) {
	reader, err := fs.GetChunkRange(ctx, id, offset, length)
	if err != nil {
		return nil, err
	}
	defer reader.Close()
	return io.ReadAll(reader)
}

// GetChunkInfo returns chunk metadata by ID.
func (fs *FileStore) GetChunkInfo(id types.ChunkID) (*types.Chunk, error) {
	chunk, err := fs.chunkIdx.Get(id)
	if err != nil {
		return nil, err
	}
	return &chunk, nil
}

// =============================================================================
// Decompression Helpers
// =============================================================================

// readAndDecompress reads compressed data and returns a decompressed ReadCloser.
func (fs *FileStore) readAndDecompress(reader io.ReadCloser, algo string, id types.ChunkID) (io.ReadCloser, error) {
	decompressed, err := fs.readAndDecompressBytes(reader, algo, id)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(decompressed)), nil
}

// readAndDecompressBytes reads compressed data and returns decompressed bytes.
func (fs *FileStore) readAndDecompressBytes(reader io.ReadCloser, algo string, id types.ChunkID) ([]byte, error) {
	compressedData, err := io.ReadAll(reader)
	reader.Close()
	if err != nil {
		return nil, fmt.Errorf("read compressed chunk: %w", err)
	}

	algorithm := compression.ParseAlgorithm(algo)
	decompressed, err := compression.Decompress(algorithm, compressedData)
	if err != nil {
		return nil, fmt.Errorf("decompress chunk %s: %w", id, err)
	}

	compression.RecordDecompression(algorithm, len(compressedData), len(decompressed))
	return decompressed, nil
}
