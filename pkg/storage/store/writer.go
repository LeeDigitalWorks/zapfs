// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/compression"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// ErrChunkNotFound is returned when a chunk doesn't exist
var ErrChunkNotFound = errors.New("chunk not found")

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
	// 1. Compute chunk ID from ORIGINAL data (preserves deduplication)
	chunkID := types.ChunkIDFromBytes(data)
	originalSize := uint64(len(data))

	// 2. Check if chunk already exists (local deduplication)
	if existing, err := fs.chunkIdx.Get(chunkID); err == nil {
		// Chunk exists locally - reuse it
		ChunkDedupeHits.Inc()
		ChunkOperations.WithLabelValues("deduplicate").Inc()

		return &types.ChunkRef{
			ChunkID:      chunkID,
			Size:         existing.Size,
			OriginalSize: existing.GetOriginalSize(),
			Compression:  existing.Compression,
			BackendID:    existing.BackendID,
		}, nil
	}

	// 3. Compress data if algorithm specified
	var dataToWrite []byte
	var usedAlgo compression.Algorithm
	var err error

	if algo != compression.None && algo != "" && algo.IsValid() {
		dataToWrite, usedAlgo, err = compression.CompressIfBeneficial(algo, data)
		if err != nil {
			return nil, fmt.Errorf("compress chunk: %w", err)
		}
		// Record compression metrics
		if usedAlgo == compression.None {
			compression.CompressionSkipped.WithLabelValues(algo.String()).Inc()
		} else {
			compression.RecordCompression(usedAlgo, int(originalSize), len(dataToWrite), false)
		}
	} else {
		dataToWrite = data
		usedAlgo = compression.None
	}

	// 4. Select backend
	backend, err := fs.placer.SelectBackend(ctx, uint64(len(dataToWrite)), "")
	if err != nil {
		return nil, err
	}

	store, ok := fs.manager.Get(backend.ID)
	if !ok {
		return nil, fmt.Errorf("backend %s not found", backend.ID)
	}

	// 5. Write to backend
	path := chunkID.FullPath("")
	if err := store.Write(ctx, path, bytes.NewReader(dataToWrite), int64(len(dataToWrite))); err != nil {
		return nil, err
	}

	// 6. Determine sizes for index
	compressedSize := uint64(len(dataToWrite))
	storedOriginalSize := uint64(0)
	storedCompression := ""
	if usedAlgo != compression.None {
		storedOriginalSize = originalSize
		storedCompression = usedAlgo.String()
	}

	// 7. Index chunk locally (RefCount managed centrally in chunk_registry)
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

	// 8. Update metrics for new chunk
	ChunkTotalCount.Inc()
	ChunkTotalBytes.Add(float64(compressedSize))
	ChunkOperations.WithLabelValues("create").Inc()

	return &types.ChunkRef{
		ChunkID:      chunkID,
		Size:         compressedSize,
		OriginalSize: storedOriginalSize,
		Compression:  storedCompression,
		BackendID:    backend.ID,
	}, nil
}
