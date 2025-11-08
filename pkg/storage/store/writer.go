package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"zapfs/pkg/types"
	"zapfs/pkg/utils"
)

// ErrChunkNotFound is returned when a chunk doesn't exist
var ErrChunkNotFound = errors.New("chunk not found")

// lockForChunk returns the striped lock index for a given ChunkID
func (fs *FileStore) lockForChunk(chunkID types.ChunkID) *sync.Mutex {
	// Simple hash: sum of bytes mod numRefCountLocks
	var sum uint32
	for i := 0; i < len(chunkID) && i < 8; i++ {
		sum += uint32(chunkID[i])
	}
	return &fs.refCountLocks[sum%numRefCountLocks]
}

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

// writeChunk writes a single chunk to a backend
func (fs *FileStore) writeChunk(ctx context.Context, data []byte) (*types.ChunkRef, error) {
	chunkID := types.ChunkIDFromBytes(data)

	// Acquire striped lock for this chunk to ensure atomic RefCount updates
	lock := fs.lockForChunk(chunkID)
	lock.Lock()
	defer lock.Unlock()

	// Check if chunk already exists (deduplication)
	if existing, err := fs.chunkIdx.Get(chunkID); err == nil {
		// Track if transitioning from zero ref
		wasZeroRef := existing.RefCount == 0

		// Atomically increment ref count with durable write
		existing.RefCount++
		// Clear ZeroRefSince since we now have references again
		if wasZeroRef {
			existing.ZeroRefSince = 0
		}
		if err := fs.chunkIdx.PutSync(chunkID, existing); err != nil {
			return nil, fmt.Errorf("failed to update ref count: %w", err)
		}

		// Update metrics for deduplication
		ChunkDedupeHits.Inc()
		ChunkOperations.WithLabelValues("deduplicate").Inc()
		if wasZeroRef {
			ChunkZeroRefCount.Dec()
			ChunkZeroRefBytes.Sub(float64(existing.Size))
		}

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

	// Index chunk with durable write
	chunk := types.Chunk{
		ID:        chunkID,
		BackendID: backend.ID,
		Path:      path,
		Size:      uint64(len(data)),
		RefCount:  1,
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

// IncrementRefCount atomically increments a chunk's reference count
func (fs *FileStore) IncrementRefCount(chunkID types.ChunkID) error {
	lock := fs.lockForChunk(chunkID)
	lock.Lock()
	defer lock.Unlock()

	chunk, err := fs.chunkIdx.Get(chunkID)
	if err != nil {
		return ErrChunkNotFound
	}

	chunk.RefCount++
	return fs.chunkIdx.PutSync(chunkID, chunk)
}

// DecrementRefCount atomically decrements a chunk's reference count.
// Returns the new RefCount value. If RefCount reaches 0, the chunk
// becomes eligible for garbage collection.
func (fs *FileStore) DecrementRefCount(chunkID types.ChunkID) (uint32, error) {
	newCount, _, err := fs.DecrementRefCountCAS(chunkID, 0, false)
	return newCount, err
}

// DecrementRefCountCAS atomically decrements a chunk's reference count with
// compare-and-swap semantics.
// If useCAS is true and expectedRefCount doesn't match current RefCount,
// returns (currentRefCount, false, nil) without decrementing.
// Returns (newRefCount, true, nil) on successful decrement.
// Sets ZeroRefSince timestamp when RefCount reaches 0 for GC grace period.
func (fs *FileStore) DecrementRefCountCAS(chunkID types.ChunkID, expectedRefCount uint32, useCAS bool) (uint32, bool, error) {
	lock := fs.lockForChunk(chunkID)
	lock.Lock()
	defer lock.Unlock()

	chunk, err := fs.chunkIdx.Get(chunkID)
	if err != nil {
		return 0, false, ErrChunkNotFound
	}

	// CAS check: if expected doesn't match, return current without decrementing
	if useCAS && chunk.RefCount != expectedRefCount {
		return chunk.RefCount, false, nil
	}

	if chunk.RefCount > 0 {
		chunk.RefCount--
		// Set ZeroRefSince when RefCount reaches 0 (for GC grace period)
		if chunk.RefCount == 0 {
			chunk.ZeroRefSince = time.Now().Unix()
			// Update zero-ref metrics
			ChunkZeroRefCount.Inc()
			ChunkZeroRefBytes.Add(float64(chunk.Size))
		}
	}

	if err := fs.chunkIdx.PutSync(chunkID, chunk); err != nil {
		return 0, false, err
	}

	return chunk.RefCount, true, nil
}

// DecrementRefCountBatch atomically decrements multiple chunks' reference counts.
// Returns a map of chunkID -> new RefCount. Continues on individual errors.
func (fs *FileStore) DecrementRefCountBatch(chunkIDs []types.ChunkID) (map[types.ChunkID]uint32, error) {
	results := make(map[types.ChunkID]uint32, len(chunkIDs))
	var lastErr error

	for _, chunkID := range chunkIDs {
		newCount, err := fs.DecrementRefCount(chunkID)
		if err != nil {
			lastErr = err
			continue
		}
		results[chunkID] = newCount
	}

	return results, lastErr
}
