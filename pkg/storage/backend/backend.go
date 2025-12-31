// Package backend provides storage backend implementations.
// All backends implement types.BackendStorage interface.
package backend

import (
	"fmt"
	"sync"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// Registry holds registered backend factories
var (
	registryMu sync.RWMutex
	registry   = make(map[types.StorageType]Factory)
)

// Factory creates a BackendStorage from config
type Factory func(cfg types.BackendConfig) (types.BackendStorage, error)

// Register adds a factory for a storage type
func Register(t types.StorageType, f Factory) {
	registryMu.Lock()
	defer registryMu.Unlock()
	registry[t] = f
}

// New creates a BackendStorage from config
func New(cfg types.BackendConfig) (types.BackendStorage, error) {
	registryMu.RLock()
	f, ok := registry[cfg.Type]
	registryMu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("unknown storage type: %s", cfg.Type)
	}
	return f(cfg)
}

// Manager tracks multiple backends
type Manager struct {
	mu       sync.RWMutex
	backends map[string]types.BackendStorage
	configs  map[string]types.BackendConfig
}

// NewManager creates a backend manager
func NewManager() *Manager {
	return &Manager{
		backends: make(map[string]types.BackendStorage),
		configs:  make(map[string]types.BackendConfig),
	}
}

// Add creates and registers a backend
func (m *Manager) Add(id string, cfg types.BackendConfig) error {
	storage, err := New(cfg)
	if err != nil {
		return fmt.Errorf("create backend %s: %w", id, err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if old, exists := m.backends[id]; exists {
		old.Close()
	}

	m.backends[id] = storage
	m.configs[id] = cfg
	return nil
}

// Get retrieves a backend by ID
func (m *Manager) Get(id string) (types.BackendStorage, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	b, ok := m.backends[id]
	return b, ok
}

// Remove closes and removes a backend
func (m *Manager) Remove(id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if b, ok := m.backends[id]; ok {
		b.Close()
		delete(m.backends, id)
		delete(m.configs, id)
	}
	return nil
}

// List returns all backend IDs
func (m *Manager) List() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	ids := make([]string, 0, len(m.backends))
	for id := range m.backends {
		ids = append(ids, id)
	}
	return ids
}

// Close closes all backends
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, b := range m.backends {
		b.Close()
	}
	m.backends = make(map[string]types.BackendStorage)
	m.configs = make(map[string]types.BackendConfig)
	return nil
}
