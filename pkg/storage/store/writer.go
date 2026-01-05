// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/compression"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// PutObject stores an object, chunking the data and writing to backends without compression.
func (fs *FileStore) PutObject(ctx context.Context, obj *types.ObjectRef, reader io.Reader) error {
	return fs.PutObjectWithCompression(ctx, obj, reader, compression.None)
}

// PutObjectWithCompression stores an object, chunking the data and writing to backends with optional compression.
// Each chunk is compressed independently using the specified algorithm.
func (fs *FileStore) PutObjectWithCompression(ctx context.Context, obj *types.ObjectRef, reader io.Reader, algo compression.Algorithm) error {
	var chunks []types.ChunkRef
	var totalSize uint64

	md5h := utils.Md5PoolGetHasher()
	defer utils.Md5PoolPutHasher(md5h)
	md5h.Reset()

	buf := make([]byte, types.ChunkSize)

	for {
		n, err := io.ReadFull(reader, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return fmt.Errorf("read: %w", err)
		}
		if n == 0 {
			break
		}

		chunkData := buf[:n]
		md5h.Write(chunkData)

		// Write chunk with compression
		chunkRef, err := fs.writeChunkWithCompression(ctx, chunkData, algo)
		if err != nil {
			return fmt.Errorf("write chunk: %w", err)
		}

		chunkRef.Offset = totalSize
		chunks = append(chunks, *chunkRef)
		totalSize += uint64(n)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}

	// Update object metadata
	obj.ChunkRefs = chunks
	obj.Size = totalSize
	obj.ETag = fmt.Sprintf("%x", md5h.Sum(nil))
	obj.CreatedAt = time.Now().Unix()

	return fs.objectIdx.Put(obj.ID, *obj)
}

// writeChunkWithCompression writes a single chunk to a backend with optional compression.
// The chunk ID is computed from the ORIGINAL (uncompressed) data to preserve deduplication.
// Compression is applied after hashing, before storage.
func (fs *FileStore) writeChunkWithCompression(ctx context.Context, data []byte, algo compression.Algorithm) (*types.ChunkRef, error) {
	// Compute chunk ID from ORIGINAL data (preserves deduplication)
	chunkID := types.ChunkIDFromBytes(data)

	// Check if chunk already exists (local deduplication)
	if ref := fs.getExistingChunkRef(chunkID); ref != nil {
		ChunkDedupeHits.Inc()
		ChunkOperations.WithLabelValues("deduplicate").Inc()
		return ref, nil
	}

	// Select backend
	backend, err := fs.placer.SelectBackend(ctx, uint64(len(data)), "")
	if err != nil {
		return nil, err
	}

	// Store chunk data with compression
	ref, err := fs.storeChunkData(ctx, chunkID, data, backend, algo)
	if err != nil {
		return nil, err
	}

	ChunkOperations.WithLabelValues("create").Inc()
	return ref, nil
}

// WriteChunk writes a chunk directly to storage (no compression).
// Used by migration to receive chunks from peer servers.
// The chunkID is provided by the caller (already known from source).
func (fs *FileStore) WriteChunk(ctx context.Context, chunkID types.ChunkID, data []byte, backendID string) (*types.ChunkRef, error) {
	// Check if chunk already exists (idempotent for migration)
	if ref := fs.getExistingChunkRef(chunkID); ref != nil {
		return ref, nil
	}

	// Get backend (use provided or fall back to placer)
	backend, ok := fs.backends[backendID]
	if !ok {
		var err error
		backend, err = fs.placer.SelectBackend(ctx, uint64(len(data)), "")
		if err != nil {
			return nil, err
		}
	}

	// Store chunk data without compression (migration preserves original format)
	ref, err := fs.storeChunkData(ctx, chunkID, data, backend, compression.None)
	if err != nil {
		return nil, err
	}

	ChunkOperations.WithLabelValues("receive").Inc()
	return ref, nil
}

// storeChunkData handles the common logic for compressing, writing, and indexing a chunk.
// Used by writeChunkWithCompression (PutObject flow) and WriteChunk (migration).
func (fs *FileStore) storeChunkData(ctx context.Context, chunkID types.ChunkID, data []byte, backend *types.Backend, algo compression.Algorithm) (*types.ChunkRef, error) {
	originalSize := uint64(len(data))

	// Compress data if algorithm specified
	dataToWrite, usedAlgo := fs.compressData(data, algo, originalSize)

	// Get backend store
	store, ok := fs.manager.Get(backend.ID)
	if !ok {
		return nil, fmt.Errorf("backend %s not found", backend.ID)
	}

	// Write to backend
	path := chunkID.FullPath("")
	if err := store.Write(ctx, path, bytes.NewReader(dataToWrite), int64(len(dataToWrite))); err != nil {
		return nil, err
	}

	// Determine sizes for index
	compressedSize := uint64(len(dataToWrite))
	storedOriginalSize := uint64(0)
	storedCompression := ""
	if usedAlgo != compression.None {
		storedOriginalSize = originalSize
		storedCompression = usedAlgo.String()
	}

	// Index chunk locally
	chunk := types.Chunk{
		ID:           chunkID,
		BackendID:    backend.ID,
		Path:         path,
		Size:         compressedSize,
		OriginalSize: storedOriginalSize,
		Compression:  storedCompression,
		CreatedAt:    time.Now().Unix(),
	}
	if err := fs.chunkIdx.PutSync(chunkID, chunk); err != nil {
		return nil, err
	}

	// Update metrics
	ChunkTotalCount.Inc()
	ChunkTotalBytes.Add(float64(compressedSize))

	return &types.ChunkRef{
		ChunkID:      chunkID,
		Size:         compressedSize,
		OriginalSize: storedOriginalSize,
		Compression:  storedCompression,
		BackendID:    backend.ID,
	}, nil
}

// compressData compresses data using the specified algorithm if beneficial.
// Returns the data to write and the algorithm that was actually used.
func (fs *FileStore) compressData(data []byte, algo compression.Algorithm, originalSize uint64) ([]byte, compression.Algorithm) {
	if algo == compression.None || algo == "" || !algo.IsValid() {
		return data, compression.None
	}

	dataToWrite, usedAlgo, err := compression.CompressIfBeneficial(algo, data)
	if err != nil {
		// On compression error, fall back to uncompressed
		return data, compression.None
	}

	// Record compression metrics
	if usedAlgo == compression.None {
		compression.CompressionSkipped.WithLabelValues(algo.String()).Inc()
	} else {
		compression.RecordCompression(usedAlgo, int(originalSize), len(dataToWrite), false)
	}

	return dataToWrite, usedAlgo
}

// getExistingChunkRef returns a ChunkRef if the chunk already exists in the index, nil otherwise.
func (fs *FileStore) getExistingChunkRef(chunkID types.ChunkID) *types.ChunkRef {
	existing, err := fs.chunkIdx.Get(chunkID)
	if err != nil {
		return nil
	}
	return &types.ChunkRef{
		ChunkID:      chunkID,
		Size:         existing.Size,
		OriginalSize: existing.GetOriginalSize(),
		Compression:  existing.Compression,
		BackendID:    existing.BackendID,
	}
}
