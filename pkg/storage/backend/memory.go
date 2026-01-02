// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// StorageTypeMemory is used for testing
const StorageTypeMemory types.StorageType = "memory"

func init() {
	Register(StorageTypeMemory, func(cfg types.BackendConfig) (types.BackendStorage, error) {
		return NewMemoryStorage(), nil
	})
}

// MemoryStorage is an in-memory backend for testing
type MemoryStorage struct {
	mu   sync.RWMutex
	data map[string][]byte
}

// NewMemoryStorage creates a new in-memory storage
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		data: make(map[string][]byte),
	}
}

func (m *MemoryStorage) Type() types.StorageType {
	return StorageTypeMemory
}

func (m *MemoryStorage) Write(ctx context.Context, key string, data io.Reader, size int64) error {
	buf, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = buf
	return nil
}

func (m *MemoryStorage) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MemoryStorage) ReadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("key not found: %s", key)
	}

	if offset >= int64(len(data)) {
		return io.NopCloser(bytes.NewReader(nil)), nil
	}

	end := offset + length
	if end > int64(len(data)) {
		end = int64(len(data))
	}

	return io.NopCloser(bytes.NewReader(data[offset:end])), nil
}

func (m *MemoryStorage) Delete(ctx context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MemoryStorage) Exists(ctx context.Context, key string) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	_, ok := m.data[key]
	return ok, nil
}

func (m *MemoryStorage) Size(ctx context.Context, key string) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	data, ok := m.data[key]
	if !ok {
		return 0, fmt.Errorf("key not found: %s", key)
	}
	return int64(len(data)), nil
}

func (m *MemoryStorage) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[string][]byte)
	return nil
}

// AddMemory is a convenience method to add a memory backend to the manager
func (mgr *Manager) AddMemory(id string) error {
	return mgr.Add(id, types.BackendConfig{
		Type: StorageTypeMemory,
	})
}
