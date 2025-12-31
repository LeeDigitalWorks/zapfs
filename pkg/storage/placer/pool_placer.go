package placer

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
)

// PoolPlacer selects pools for storing data using weighted random selection
type PoolPlacer struct {
	mu      sync.RWMutex
	pools   *types.PoolSet
	targets []types.PoolTarget
	rng     *rand.Rand
}

// NewPoolPlacer creates a placer with the given pool set and targets
func NewPoolPlacer(pools *types.PoolSet, targets []types.PoolTarget) *PoolPlacer {
	return &PoolPlacer{
		pools:   pools,
		targets: targets,
		rng:     rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// SetTargets updates the placement targets
func (p *PoolPlacer) SetTargets(targets []types.PoolTarget) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.targets = targets
}

// SelectPool picks a pool for storing data using weighted random selection
func (p *PoolPlacer) SelectPool(ctx context.Context) (*types.StoragePool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return types.SelectPool(p.targets, p.pools, p.rng)
}

// SelectPoolWithPreference tries to use a preferred pool first
func (p *PoolPlacer) SelectPoolWithPreference(ctx context.Context, preferredID uuid.UUID) (*types.StoragePool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	// Try preferred pool first
	if preferredID != uuid.Nil {
		if pool, exists := p.pools.Get(preferredID); exists && pool.CanWrite() {
			return pool, nil
		}
	}

	// Fall back to weighted selection
	return types.SelectPool(p.targets, p.pools, p.rng)
}

// SelectPools picks n unique pools for replica/shard placement
func (p *PoolPlacer) SelectPools(ctx context.Context, n int) ([]*types.StoragePool, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return types.SelectPools(n, p.targets, p.pools, p.rng)
}

// GetPool retrieves a specific pool by ID
func (p *PoolPlacer) GetPool(id uuid.UUID) (*types.StoragePool, bool) {
	return p.pools.Get(id)
}

// GetPoolByName retrieves a specific pool by name
func (p *PoolPlacer) GetPoolByName(name string) (*types.StoragePool, bool) {
	return p.pools.GetByName(name)
}

// ListPools returns all available pools
func (p *PoolPlacer) ListPools() []*types.StoragePool {
	return p.pools.List()
}

// ListWritablePools returns pools that accept writes
func (p *PoolPlacer) ListWritablePools() []*types.StoragePool {
	return p.pools.ListWritable()
}

// WeightDistribution returns the expected distribution percentage for each pool
func (p *PoolPlacer) WeightDistribution() map[uuid.UUID]float64 {
	p.mu.RLock()
	defer p.mu.RUnlock()

	dist := make(map[uuid.UUID]float64)
	var totalWeight float64

	for _, t := range p.targets {
		pool, exists := p.pools.Get(t.PoolID)
		if !exists || !pool.CanWrite() {
			continue
		}

		weight := t.WeightOverride
		if weight == 0 {
			weight = pool.Weight
		}
		totalWeight += weight
	}

	if totalWeight == 0 {
		return dist
	}

	for _, t := range p.targets {
		pool, exists := p.pools.Get(t.PoolID)
		if !exists || !pool.CanWrite() {
			continue
		}

		weight := t.WeightOverride
		if weight == 0 {
			weight = pool.Weight
		}
		dist[t.PoolID] = (weight / totalWeight) * 100
	}

	return dist
}

// ProfilePlacer manages placement across multiple storage profiles
type ProfilePlacer struct {
	mu       sync.RWMutex
	pools    *types.PoolSet
	profiles map[string]*types.StorageProfile
	placers  map[string]*PoolPlacer
	rng      *rand.Rand
}

// NewProfilePlacer creates a placer that handles multiple profiles
func NewProfilePlacer(pools *types.PoolSet) *ProfilePlacer {
	return &ProfilePlacer{
		pools:    pools,
		profiles: make(map[string]*types.StorageProfile),
		placers:  make(map[string]*PoolPlacer),
		rng:      rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// AddProfile registers a storage profile
func (pp *ProfilePlacer) AddProfile(profile *types.StorageProfile) error {
	pp.mu.Lock()
	defer pp.mu.Unlock()

	// Validate all pool references exist
	for _, t := range profile.Pools {
		if _, exists := pp.pools.Get(t.PoolID); !exists {
			return fmt.Errorf("profile %q references unknown pool %s", profile.Name, t.PoolID)
		}
	}

	pp.profiles[profile.Name] = profile
	pp.placers[profile.Name] = NewPoolPlacer(pp.pools, profile.Pools)
	return nil
}

// RemoveProfile removes a storage profile
func (pp *ProfilePlacer) RemoveProfile(name string) {
	pp.mu.Lock()
	defer pp.mu.Unlock()
	delete(pp.profiles, name)
	delete(pp.placers, name)
}

// GetProfile retrieves a profile by name
func (pp *ProfilePlacer) GetProfile(name string) (*types.StorageProfile, bool) {
	pp.mu.RLock()
	defer pp.mu.RUnlock()
	p, ok := pp.profiles[name]
	return p, ok
}

// SelectPoolForProfile picks a pool for the given profile
func (pp *ProfilePlacer) SelectPoolForProfile(ctx context.Context, profileName string) (*types.StoragePool, error) {
	pp.mu.RLock()
	placer, exists := pp.placers[profileName]
	pp.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown storage profile: %s", profileName)
	}

	return placer.SelectPool(ctx)
}

// SelectPoolsForProfile picks n pools for the given profile (for replication/EC)
func (pp *ProfilePlacer) SelectPoolsForProfile(ctx context.Context, profileName string, n int) ([]*types.StoragePool, error) {
	pp.mu.RLock()
	placer, exists := pp.placers[profileName]
	pp.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("unknown storage profile: %s", profileName)
	}

	return placer.SelectPools(ctx, n)
}

// ListProfiles returns all profile names
func (pp *ProfilePlacer) ListProfiles() []string {
	pp.mu.RLock()
	defer pp.mu.RUnlock()

	names := make([]string, 0, len(pp.profiles))
	for name := range pp.profiles {
		names = append(names, name)
	}
	return names
}
