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

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// ErrChunkNotFound is returned when a chunk doesn't exist
var ErrChunkNotFound = errors.New("chunk not found")

// PutObject stores an object, chunking the data and writing to backends
func (fs *FileStore) PutObject(ctx context.Context, obj *types.ObjectRef, reader io.Reader) error {
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

		// Write chunk
		chunkRef, err := fs.writeChunk(ctx, chunkData)
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

// writeChunk writes a single chunk to a backend.
// Deduplication is handled locally - if chunk already exists, reuse it.
// Note: RefCount is managed centrally in the metadata DB's chunk_registry table.
func (fs *FileStore) writeChunk(ctx context.Context, data []byte) (*types.ChunkRef, error) {
	chunkID := types.ChunkIDFromBytes(data)

	// Check if chunk already exists (local deduplication)
	if existing, err := fs.chunkIdx.Get(chunkID); err == nil {
		// Chunk exists locally - reuse it
		ChunkDedupeHits.Inc()
		ChunkOperations.WithLabelValues("deduplicate").Inc()

		return &types.ChunkRef{
			ChunkID:   chunkID,
			Size:      uint64(len(data)),
			BackendID: existing.BackendID,
		}, nil
	}

	// Select backend
	backend, err := fs.placer.SelectBackend(ctx, uint64(len(data)), "")
	if err != nil {
		return nil, err
	}

	store, ok := fs.manager.Get(backend.ID)
	if !ok {
		return nil, fmt.Errorf("backend %s not found", backend.ID)
	}

	// Write to backend
	path := chunkID.FullPath("")
	if err := store.Write(ctx, path, bytes.NewReader(data), int64(len(data))); err != nil {
		return nil, err
	}

	// Index chunk locally (RefCount managed centrally in chunk_registry)
	chunk := types.Chunk{
		ID:        chunkID,
		BackendID: backend.ID,
		Path:      path,
		Size:      uint64(len(data)),
		CreatedAt: time.Now().Unix(),
	}
	if err := fs.chunkIdx.PutSync(chunkID, chunk); err != nil {
		return nil, err
	}

	// Update metrics for new chunk
	ChunkTotalCount.Inc()
	ChunkTotalBytes.Add(float64(len(data)))
	ChunkOperations.WithLabelValues("create").Inc()

	return &types.ChunkRef{
		ChunkID:   chunkID,
		Size:      uint64(len(data)),
		BackendID: backend.ID,
	}, nil
}
