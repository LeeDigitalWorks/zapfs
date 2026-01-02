// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package ec

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/index"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/placer"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
)

// BlockSize is the default erasure coding block size
const BlockSize = 1 * 1024 * 1024 // 1MB

// ErasureCoder handles encoding/reconstructing data in shards
type ErasureCoder interface {
	// EncodeData splits data into data+parity shards
	EncodeData(data []byte) ([][]byte, error)
	// DecodeData reconstructs data from available shards (nil = missing)
	DecodeData(shards [][]byte) error
	// DataShards returns number of data shards
	DataShards() int
	// ParityShards returns number of parity shards
	ParityShards() int
}

// ECManager handles erasure-coded storage operations
type ECManager struct {
	chunkIdx index.Indexer[types.ChunkID, types.Chunk]
	ecIdx    index.Indexer[uuid.UUID, types.ECGroup]

	placer  placer.Placer
	manager *backend.Manager
	encoder ErasureCoder

	scheme    types.ECScheme
	blockSize int64
}

// NewECManager creates an EC manager with the given scheme
func NewECManager(
	chunkIdx index.Indexer[types.ChunkID, types.Chunk],
	ecIdx index.Indexer[uuid.UUID, types.ECGroup],
	placer placer.Placer,
	manager *backend.Manager,
	encoder ErasureCoder,
	scheme types.ECScheme,
) *ECManager {
	return &ECManager{
		chunkIdx:  chunkIdx,
		ecIdx:     ecIdx,
		placer:    placer,
		manager:   manager,
		encoder:   encoder,
		scheme:    scheme,
		blockSize: BlockSize,
	}
}

// WriteQuorum returns minimum backends needed for successful write
// This is DataShards + 1 (need at least data shards to reconstruct)
func (m *ECManager) WriteQuorum() int {
	return m.scheme.DataShards + 1
}

// ReadQuorum returns minimum backends needed for successful read
func (m *ECManager) ReadQuorum() int {
	return m.scheme.DataShards
}

// Encode reads from src, erasure-encodes in blocks, and writes to backends
// Returns the EC group ID.
func (m *ECManager) Encode(ctx context.Context, src io.Reader, size int64) (uuid.UUID, error) {
	totalShards := m.scheme.TotalShards()

	// Select backends for each shard
	backends, err := m.placer.SelectBackends(ctx, totalShards)
	if err != nil {
		return uuid.Nil, fmt.Errorf("select backends: %w", err)
	}

	// Get storage for each backend
	stores := make([]types.BackendStorage, totalShards)
	for i, b := range backends {
		store, ok := m.manager.Get(b.ID)
		if !ok {
			return uuid.Nil, fmt.Errorf("backend %s not found", b.ID)
		}
		stores[i] = store
	}

	// Create EC group
	groupID := uuid.New()
	ecGroup := types.ECGroup{
		ID:           groupID,
		Scheme:       m.scheme,
		OriginalSize: uint64(size),
		Shards:       make([]types.ECShard, totalShards),
	}

	// Initialize shard info
	for i, b := range backends {
		ecGroup.Shards[i] = types.ECShard{
			Index:     i,
			BackendID: b.ID,
		}
	}

	// Buffer for reading blocks
	buf := make([]byte, m.blockSize)
	var totalWritten int64

	// Shard writers - accumulate all shard data
	shardBuffers := make([]*bytes.Buffer, totalShards)
	for i := range shardBuffers {
		shardBuffers[i] = new(bytes.Buffer)
	}

	// Process data in blocks
	for {
		n, err := io.ReadFull(src, buf)
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			return uuid.Nil, fmt.Errorf("read: %w", err)
		}
		if n == 0 {
			break
		}

		// Encode this block
		shards, err := m.encoder.EncodeData(buf[:n])
		if err != nil {
			return uuid.Nil, fmt.Errorf("encode block: %w", err)
		}

		// Append to shard buffers
		for i, shard := range shards {
			shardBuffers[i].Write(shard)
		}

		totalWritten += int64(n)

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
	}

	// Write shards in parallel with quorum check
	var wg sync.WaitGroup
	var successCount int32
	errCh := make(chan error, totalShards)

	for i := range totalShards {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			data := shardBuffers[idx].Bytes()
			chunkID := types.ChunkIDFromBytes(data)
			path := chunkID.FullPath("")

			if err := stores[idx].Write(ctx, path, bytes.NewReader(data), int64(len(data))); err != nil {
				errCh <- fmt.Errorf("write shard %d: %w", idx, err)
				return
			}

			atomic.AddInt32(&successCount, 1)

			// Update shard info
			ecGroup.Shards[idx].ChunkID = chunkID
			ecGroup.Shards[idx].Size = uint64(len(data))

			// Index the chunk
			chunk := types.Chunk{
				ID:        chunkID,
				BackendID: backends[idx].ID,
				Path:      path,
				Size:      uint64(len(data)),
				CreatedAt: time.Now().Unix(),
				ECGroupID: groupID,
				ShardIdx:  idx,
				IsParity:  idx >= m.scheme.DataShards,
			}
			if err := m.chunkIdx.Put(chunkID, chunk); err != nil {
				errCh <- fmt.Errorf("index chunk %d: %w", idx, err)
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	// Check write quorum
	if int(successCount) < m.WriteQuorum() {
		// Collect errors for debugging
		var errs []error
		for err := range errCh {
			errs = append(errs, err)
		}
		return uuid.Nil, fmt.Errorf("write quorum not met: %d/%d succeeded, errors: %v",
			successCount, m.WriteQuorum(), errs)
	}

	// Store EC group
	if err := m.ecIdx.Put(groupID, ecGroup); err != nil {
		return uuid.Nil, fmt.Errorf("store ec group: %w", err)
	}

	return groupID, nil
}

// Decode reads shards from backends, reconstructs if needed, and writes to dst
func (m *ECManager) Decode(ctx context.Context, groupID uuid.UUID, dst io.Writer) error {
	ecGroup, err := m.ecIdx.Get(groupID)
	if err != nil {
		return fmt.Errorf("get ec group: %w", err)
	}

	totalShards := ecGroup.Scheme.TotalShards()
	shardData := make([][]byte, totalShards)

	// Read shards in parallel
	var wg sync.WaitGroup
	var mu sync.Mutex
	var readCount int32

	for _, shard := range ecGroup.Shards {
		if shard.ChunkID == "" {
			continue // Missing shard
		}

		wg.Add(1)
		go func(s types.ECShard) {
			defer wg.Done()

			store, ok := m.manager.Get(s.BackendID)
			if !ok {
				return
			}

			rc, err := store.Read(ctx, s.ChunkID.FullPath(""))
			if err != nil {
				return
			}
			defer rc.Close()

			data, err := io.ReadAll(rc)
			if err != nil {
				return
			}

			mu.Lock()
			shardData[s.Index] = data
			atomic.AddInt32(&readCount, 1)
			mu.Unlock()
		}(shard)
	}

	wg.Wait()

	// Check read quorum
	if int(readCount) < m.ReadQuorum() {
		return fmt.Errorf("read quorum not met: %d/%d available", readCount, m.ReadQuorum())
	}

	// Reconstruct if needed (some shards missing)
	if int(readCount) < totalShards {
		if err := m.encoder.DecodeData(shardData); err != nil {
			return fmt.Errorf("reconstruct: %w", err)
		}
	}

	// Write data shards to output
	// Need to decode block by block since shards contain multiple encoded blocks
	shardSize := len(shardData[0])
	blockShardSize := int(ceilDiv(m.blockSize, int64(m.scheme.DataShards)))
	numBlocks := ceilDiv(int64(shardSize), int64(blockShardSize))
	bytesWritten := int64(0)
	originalSize := int64(ecGroup.OriginalSize)

	for block := int64(0); block < numBlocks; block++ {
		offset := int(block) * blockShardSize
		end := offset + blockShardSize
		if end > shardSize {
			end = shardSize
		}

		// Collect this block's shards
		blockShards := make([][]byte, totalShards)
		for i, shard := range shardData {
			if shard != nil && offset < len(shard) {
				blockEnd := end
				if blockEnd > len(shard) {
					blockEnd = len(shard)
				}
				blockShards[i] = shard[offset:blockEnd]
			}
		}

		// Write data shards only (first DataShards)
		for i := 0; i < m.scheme.DataShards; i++ {
			if blockShards[i] == nil {
				continue
			}
			toWrite := blockShards[i]

			// Don't write past original size
			remaining := originalSize - bytesWritten
			if remaining <= 0 {
				break
			}
			if int64(len(toWrite)) > remaining {
				toWrite = toWrite[:remaining]
			}

			n, err := dst.Write(toWrite)
			if err != nil {
				return fmt.Errorf("write output: %w", err)
			}
			bytesWritten += int64(n)
		}
	}

	return nil
}

// PutChunk stores data with erasure coding (convenience method)
func (m *ECManager) PutChunk(ctx context.Context, data []byte) (uuid.UUID, error) {
	return m.Encode(ctx, bytes.NewReader(data), int64(len(data)))
}

// GetChunk retrieves and reconstructs data from an EC group (convenience method)
func (m *ECManager) GetChunk(ctx context.Context, groupID uuid.UUID) ([]byte, error) {
	var buf bytes.Buffer
	if err := m.Decode(ctx, groupID, &buf); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// ceilDiv returns ceiling division
func ceilDiv(a, b int64) int64 {
	return (a + b - 1) / b
}
