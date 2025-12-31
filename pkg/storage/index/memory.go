package index

import (
	"fmt"
	"sync"
)

// MemoryIndexer is an in-memory implementation of Indexer for testing
type MemoryIndexer[K comparable, V any] struct {
	mu   sync.RWMutex
	data map[K]V
}

// NewMemoryIndexer creates a new in-memory indexer
func NewMemoryIndexer[K comparable, V any]() (*MemoryIndexer[K, V], error) {
	return &MemoryIndexer[K, V]{
		data: make(map[K]V),
	}, nil
}

func (m *MemoryIndexer[K, V]) Put(key K, value V) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = value
	return nil
}

func (m *MemoryIndexer[K, V]) Get(key K) (V, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.data[key]
	if !ok {
		var zero V
		return zero, fmt.Errorf("key not found")
	}
	return v, nil
}

func (m *MemoryIndexer[K, V]) Delete(key K) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
	return nil
}

func (m *MemoryIndexer[K, V]) Iterate(fn func(key K, value V) error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	for k, v := range m.data {
		if err := fn(k, v); err != nil {
			return err
		}
	}
	return nil
}

func (m *MemoryIndexer[K, V]) Stream(filter func(value V) bool) <-chan V {
	ch := make(chan V)
	go func() {
		defer close(ch)

		// Collect matching values under lock first to avoid holding
		// the lock while sending to channel (which could deadlock if
		// the consumer tries to call Delete/Put while we're blocked on send)
		m.mu.RLock()
		var values []V
		for _, v := range m.data {
			if filter(v) {
				values = append(values, v)
			}
		}
		m.mu.RUnlock()

		// Send values without holding the lock
		for _, v := range values {
			ch <- v
		}
	}()
	return ch
}

func (m *MemoryIndexer[K, V]) Close() error {
	return nil
}

func (m *MemoryIndexer[K, V]) Destroy() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = make(map[K]V)
	return nil
}

func (m *MemoryIndexer[K, V]) Sync() error {
	return nil
}

// PutSync is identical to Put for in-memory indexer (no disk to sync)
func (m *MemoryIndexer[K, V]) PutSync(key K, value V) error {
	return m.Put(key, value)
}

// DeleteSync is identical to Delete for in-memory indexer (no disk to sync)
func (m *MemoryIndexer[K, V]) DeleteSync(key K) error {
	return m.Delete(key)
}
