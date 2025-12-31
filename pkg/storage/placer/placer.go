package placer

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// Placer selects backends for storing data
type Placer interface {
	// SelectBackend picks a backend for storing data of the given size
	SelectBackend(ctx context.Context, sizeBytes uint64, preferredID string) (*types.Backend, error)
	// SelectBackends picks n backends for storing shards (used for EC)
	SelectBackends(ctx context.Context, n int) ([]*types.Backend, error)
	// Refresh updates the list of available backends
	Refresh(backends []*types.Backend)
}

// RoundRobinPlacer distributes data across backends in round-robin fashion
type RoundRobinPlacer struct {
	mu       sync.RWMutex
	backends []*types.Backend
	idx      uint64
}

// NewRoundRobinPlacer creates a placer with the given backends
func NewRoundRobinPlacer(backends []*types.Backend) *RoundRobinPlacer {
	return &RoundRobinPlacer{backends: backends}
}

func (p *RoundRobinPlacer) Refresh(backends []*types.Backend) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.backends = backends
}

func (p *RoundRobinPlacer) SelectBackend(ctx context.Context, sizeBytes uint64, preferredID string) (*types.Backend, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	n := len(p.backends)
	if n == 0 {
		return nil, fmt.Errorf("no backends available")
	}

	// Try preferred backend first
	if preferredID != "" {
		for _, b := range p.backends {
			if b.ID == preferredID && !b.ReadOnly && b.TotalBytes-b.UsedBytes >= sizeBytes {
				return b, nil
			}
		}
	}

	// Round-robin selection
	for i := 0; i < n; i++ {
		idx := int(atomic.AddUint64(&p.idx, 1) % uint64(n))
		b := p.backends[idx]
		if b.ReadOnly || b.TotalBytes-b.UsedBytes < sizeBytes {
			continue
		}
		return b, nil
	}

	return nil, fmt.Errorf("no backends available with %d bytes free", sizeBytes)
}

func (p *RoundRobinPlacer) SelectBackends(ctx context.Context, n int) ([]*types.Backend, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if n > len(p.backends) {
		return nil, fmt.Errorf("need %d backends but only %d available", n, len(p.backends))
	}

	var result []*types.Backend
	startIdx := int(atomic.AddUint64(&p.idx, 1) % uint64(len(p.backends)))

	for i := 0; len(result) < n && i < len(p.backends)*2; i++ {
		b := p.backends[(startIdx+i)%len(p.backends)]
		if b.ReadOnly {
			continue
		}
		result = append(result, b)
	}

	if len(result) < n {
		return nil, fmt.Errorf("could only find %d writable backends, need %d", len(result), n)
	}

	return result, nil
}
