// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package store

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/ec"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/index"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/placer"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
	prometheusgo "github.com/prometheus/client_model/go"
)

// FileStore implements Store using indexed chunks on multiple backends
type FileStore struct {
	objectIdx index.Indexer[uuid.UUID, types.ObjectRef]
	chunkIdx  index.Indexer[types.ChunkID, types.Chunk]
	ecIdx     index.Indexer[uuid.UUID, types.ECGroup]

	backends     map[string]*types.Backend
	backendMutex sync.RWMutex

	manager   *backend.Manager
	placer    placer.Placer
	ecManager *ec.ECManager

	scheme types.ECScheme
}

// Config holds FileStore configuration
type Config struct {
	IndexPath string
	IndexKind IndexKind // memory or leveldb
	Backends  []*types.Backend
	ECScheme  types.ECScheme
}

// IndexKind specifies the index backend type
type IndexKind string

const (
	IndexKindMemory  IndexKind = "memory"
	IndexKindLevelDB IndexKind = "leveldb"
)

// NewFileStore creates a new FileStore
func NewFileStore(cfg Config, manager *backend.Manager) (*FileStore, error) {
	fs := &FileStore{
		backends: make(map[string]*types.Backend),
		manager:  manager,
		scheme:   cfg.ECScheme,
	}

	var err error

	switch cfg.IndexKind {
	case IndexKindLevelDB:
		fs.objectIdx, err = index.NewLevelDBIndexer[uuid.UUID, types.ObjectRef](
			cfg.IndexPath+"/objects", nil,
			func(k uuid.UUID) []byte { return k[:] },
			func(b []byte) (uuid.UUID, error) { return uuid.FromBytes(b) },
		)
		if err != nil {
			return nil, fmt.Errorf("create object index: %w", err)
		}

		fs.chunkIdx, err = index.NewLevelDBIndexer[types.ChunkID, types.Chunk](
			cfg.IndexPath+"/chunks", nil,
			func(k types.ChunkID) []byte { return []byte(k) },
			func(b []byte) (types.ChunkID, error) { return types.ChunkID(b), nil },
		)
		if err != nil {
			return nil, fmt.Errorf("create chunk index: %w", err)
		}

		fs.ecIdx, err = index.NewLevelDBIndexer[uuid.UUID, types.ECGroup](
			cfg.IndexPath+"/ecgroups", nil,
			func(k uuid.UUID) []byte { return k[:] },
			func(b []byte) (uuid.UUID, error) { return uuid.FromBytes(b) },
		)
		if err != nil {
			return nil, fmt.Errorf("create ec index: %w", err)
		}

	default: // IndexKindMemory
		fs.objectIdx, err = index.NewMemoryIndexer[uuid.UUID, types.ObjectRef]()
		if err != nil {
			return nil, fmt.Errorf("create object index: %w", err)
		}

		fs.chunkIdx, err = index.NewMemoryIndexer[types.ChunkID, types.Chunk]()
		if err != nil {
			return nil, fmt.Errorf("create chunk index: %w", err)
		}

		fs.ecIdx, err = index.NewMemoryIndexer[uuid.UUID, types.ECGroup]()
		if err != nil {
			return nil, fmt.Errorf("create ec index: %w", err)
		}
	}

	// Register backends
	for _, b := range cfg.Backends {
		fs.backends[b.ID] = b
	}

	// Initialize placer
	fs.placer = placer.NewRoundRobinPlacer(cfg.Backends)

	// Initialize EC manager
	rsEncoder, err := ec.NewReedSolomonCoder(cfg.ECScheme.DataShards, cfg.ECScheme.ParityShards)
	if err != nil {
		return nil, fmt.Errorf("create ec encoder: %w", err)
	}
	fs.ecManager = ec.NewECManager(fs.chunkIdx, fs.ecIdx, fs.placer, manager, rsEncoder, cfg.ECScheme)

	// Initialize metrics from existing index
	fs.initializeMetrics()

	return fs, nil
}

func (fs *FileStore) Close() error {
	if fs.objectIdx != nil {
		fs.objectIdx.Close()
	}
	if fs.chunkIdx != nil {
		fs.chunkIdx.Close()
	}
	if fs.ecIdx != nil {
		fs.ecIdx.Close()
	}

	return fs.manager.Close()
}

// GetBackend returns a backend by ID
func (fs *FileStore) GetBackend(id string) (*types.Backend, bool) {
	fs.backendMutex.RLock()
	defer fs.backendMutex.RUnlock()
	b, ok := fs.backends[id]
	return b, ok
}

// GetDefaultBackend returns the first available backend
func (fs *FileStore) GetDefaultBackend() *types.Backend {
	fs.backendMutex.RLock()
	defer fs.backendMutex.RUnlock()
	for _, b := range fs.backends {
		return b
	}
	return nil
}

// GetBackendStorage returns the storage interface for a backend
func (fs *FileStore) GetBackendStorage(id string) (types.BackendStorage, bool) {
	return fs.manager.Get(id)
}

// ListBackends returns all registered backend IDs
func (fs *FileStore) ListBackends() map[string]*types.Backend {
	fs.backendMutex.RLock()
	defer fs.backendMutex.RUnlock()
	// Return a copy to avoid concurrent modification
	result := make(map[string]*types.Backend, len(fs.backends))
	for k, v := range fs.backends {
		result[k] = v
	}
	return result
}

// AddBackend registers a new backend
func (fs *FileStore) AddBackend(b *types.Backend) {
	fs.backendMutex.Lock()
	defer fs.backendMutex.Unlock()
	fs.backends[b.ID] = b
	fs.refreshPlacer()
}

// RemoveBackend removes a backend
func (fs *FileStore) RemoveBackend(id string) {
	fs.backendMutex.Lock()
	defer fs.backendMutex.Unlock()
	delete(fs.backends, id)
	fs.refreshPlacer()
}

func (fs *FileStore) refreshPlacer() {
	backends := make([]*types.Backend, 0, len(fs.backends))
	for _, b := range fs.backends {
		backends = append(backends, b)
	}
	fs.placer.Refresh(backends)
}

// GetObject retrieves object metadata
func (fs *FileStore) GetObject(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	obj, err := fs.objectIdx.Get(id)
	if err != nil {
		return nil, err
	}
	return &obj, nil
}

// DeleteObject marks an object as deleted
func (fs *FileStore) DeleteObject(ctx context.Context, id uuid.UUID) error {
	obj, err := fs.objectIdx.Get(id)
	if err != nil {
		return err
	}
	obj.DeletedAt = 1 // Mark deleted
	return fs.objectIdx.Put(id, obj)
}

// GetChunk reads a chunk from storage
func (fs *FileStore) GetChunk(ctx context.Context, id types.ChunkID) (io.ReadCloser, error) {
	chunk, err := fs.chunkIdx.Get(id)
	if err != nil {
		return nil, err
	}

	store, ok := fs.manager.Get(chunk.BackendID)
	if !ok {
		return nil, fmt.Errorf("backend %s not found", chunk.BackendID)
	}

	return store.Read(ctx, chunk.Path)
}

// GetChunkRange reads a range from a chunk
func (fs *FileStore) GetChunkRange(ctx context.Context, id types.ChunkID, offset, length int64) (io.ReadCloser, error) {
	chunk, err := fs.chunkIdx.Get(id)
	if err != nil {
		return nil, err
	}

	store, ok := fs.manager.Get(chunk.BackendID)
	if !ok {
		return nil, fmt.Errorf("backend %s not found", chunk.BackendID)
	}

	return store.ReadRange(ctx, chunk.Path, offset, length)
}

// GetChunkInfo returns chunk metadata by ID (for admin/debugging)
func (fs *FileStore) GetChunkInfo(id types.ChunkID) (*types.Chunk, error) {
	chunk, err := fs.chunkIdx.Get(id)
	if err != nil {
		return nil, err
	}
	return &chunk, nil
}

// GetECGroup returns EC group metadata by ID (for admin/debugging)
func (fs *FileStore) GetECGroup(id uuid.UUID) (*types.ECGroup, error) {
	group, err := fs.ecIdx.Get(id)
	if err != nil {
		return nil, err
	}
	return &group, nil
}

// IndexStats holds statistics about the chunk index
type IndexStats struct {
	TotalChunks int64 `json:"total_chunks"`
	TotalBytes  int64 `json:"total_bytes"`
}

// initializeMetrics populates Prometheus metrics from the existing index.
// Called on startup to restore metrics after restart.
func (fs *FileStore) initializeMetrics() {
	var totalChunks, totalBytes float64

	fs.chunkIdx.Iterate(func(id types.ChunkID, chunk types.Chunk) error {
		totalChunks++
		totalBytes += float64(chunk.Size)
		return nil
	})

	ChunkTotalCount.Set(totalChunks)
	ChunkTotalBytes.Set(totalBytes)
}

// GetIndexStats returns statistics about the chunk index.
// Uses Prometheus metrics for O(1) performance instead of iterating.
func (fs *FileStore) GetIndexStats() (*IndexStats, error) {
	return &IndexStats{
		TotalChunks: int64(getGaugeValue(ChunkTotalCount)),
		TotalBytes:  int64(getGaugeValue(ChunkTotalBytes)),
	}, nil
}

// getGaugeValue extracts the current value from a prometheus Gauge
func getGaugeValue(g prometheus.Gauge) float64 {
	// Use the Write method to get the current metric value
	var m prometheusgo.Metric
	if err := g.Write(&m); err != nil {
		return 0
	}
	if m.Gauge != nil {
		return m.Gauge.GetValue()
	}
	return 0
}

// IterateChunks iterates over all chunks in the index.
// Used by reconciliation to compare local chunks with expected chunks.
func (fs *FileStore) IterateChunks(fn func(id types.ChunkID, chunk types.Chunk) error) error {
	return fs.chunkIdx.Iterate(fn)
}

// DeleteChunk removes a chunk from the index.
// Note: This does NOT delete the chunk from backend storage.
// Use with GetBackendStorage().Delete() to fully remove a chunk.
func (fs *FileStore) DeleteChunk(ctx context.Context, id types.ChunkID) error {
	return fs.chunkIdx.Delete(id)
}

// WriteChunk writes a chunk directly to storage.
// Used by migration to receive chunks from peer servers.
// Returns the chunk reference on success.
func (fs *FileStore) WriteChunk(ctx context.Context, chunkID types.ChunkID, data []byte, backendID string) (*types.ChunkRef, error) {
	// Check if chunk already exists
	if existing, err := fs.chunkIdx.Get(chunkID); err == nil {
		// Chunk exists - this is fine for migration (idempotent)
		return &types.ChunkRef{
			ChunkID:   chunkID,
			Size:      existing.Size,
			BackendID: existing.BackendID,
		}, nil
	}

	// Get backend
	backend, ok := fs.backends[backendID]
	if !ok {
		// Fall back to placer selection
		var err error
		backend, err = fs.placer.SelectBackend(ctx, uint64(len(data)), "")
		if err != nil {
			return nil, err
		}
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

	// Update metrics
	ChunkTotalCount.Inc()
	ChunkTotalBytes.Add(float64(len(data)))
	ChunkOperations.WithLabelValues("migrate_receive").Inc()

	return &types.ChunkRef{
		ChunkID:   chunkID,
		Size:      uint64(len(data)),
		BackendID: backend.ID,
	}, nil
}
