// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/google/uuid"
)

// StoragePool is a generic abstraction over different storage backends.
// Pools group one or more backends and provide capacity-based weighting
// for object placement decisions.
type StoragePool struct {
	// ID is the immutable unique identifier for this pool (UUID)
	ID uuid.UUID `json:"id"`

	// Name is a human-readable name (can be changed without breaking references)
	Name string `json:"name"`

	// Description provides additional context about this pool
	Description string `json:"description,omitempty"`

	// BackendType specifies the storage backend type (local, s3, ceph, etc.)
	BackendType StorageType `json:"backend_type"`

	// DiskType specifies the physical media type (ssd, hdd, nvme) - only for local backends
	DiskType MediaType `json:"disk_type,omitempty"`

	// Weight is the relative weight for placement selection.
	// Recommended: use capacity in TiB (e.g., 10.0 for 10 TiB)
	// Weights are normalized at runtime: percentage = weight / sum_of_all_weights
	Weight float64 `json:"weight"`

	// Backends contains the IDs of backends that belong to this pool
	Backends []string `json:"backends"`

	// Endpoint for remote backends (S3 URL, Ceph cluster, etc.)
	Endpoint string `json:"endpoint,omitempty"`

	// Region for cloud backends
	Region string `json:"region,omitempty"`

	// Bucket for S3-compatible backends
	Bucket string `json:"bucket,omitempty"`

	// StorageClass for cloud backends (e.g., GLACIER, STANDARD_IA)
	StorageClass string `json:"storage_class,omitempty"`

	// AccessKey and SecretKey for S3-compatible backends
	// Note: For production, prefer IAM roles or CredentialsRef
	AccessKey string `json:"access_key,omitempty"`
	SecretKey string `json:"secret_key,omitempty"`

	// Credentials reference (not the actual secret)
	CredentialsRef string `json:"credentials_ref,omitempty"`

	// ReadOnly marks the pool as read-only (no new writes)
	ReadOnly bool `json:"read_only,omitempty"`

	// Protected prevents accidental deletion (requires --force flag)
	Protected bool `json:"protected,omitempty"`

	// DeletionProtectionUntil provides time-based deletion lock
	DeletionProtectionUntil *time.Time `json:"deletion_protection_until,omitempty"`

	// Metadata for versioning and audit
	CreatedAt  time.Time `json:"created_at"`
	ModifiedAt time.Time `json:"modified_at"`
	ModifiedBy string    `json:"modified_by,omitempty"`
}

// NewStoragePool creates a new storage pool with a generated UUID
func NewStoragePool(name string, backendType StorageType, weight float64) *StoragePool {
	now := time.Now()
	return &StoragePool{
		ID:          uuid.New(),
		Name:        name,
		BackendType: backendType,
		Weight:      weight,
		CreatedAt:   now,
		ModifiedAt:  now,
	}
}

// CanDelete checks if the pool can be deleted
func (p *StoragePool) CanDelete() error {
	if p.Protected {
		return fmt.Errorf("pool %q is protected; use --force-remove to delete", p.Name)
	}
	if p.DeletionProtectionUntil != nil && time.Now().Before(*p.DeletionProtectionUntil) {
		return fmt.Errorf("pool %q is protected until %s", p.Name, p.DeletionProtectionUntil.Format(time.RFC3339))
	}
	return nil
}

// CanWrite checks if the pool accepts writes
func (p *StoragePool) CanWrite() bool {
	return !p.ReadOnly
}

// LifecycleRule defines when and how to transition objects between profiles
type LifecycleRule struct {
	// ID is a unique identifier for this rule
	ID string `json:"id"`

	// Enabled controls whether this rule is active
	Enabled bool `json:"enabled"`

	// TransitionAfterDays specifies days after creation to transition
	TransitionAfterDays int `json:"transition_after_days,omitempty"`

	// TransitionToProfile specifies the target storage profile
	TransitionToProfile string `json:"transition_to_profile,omitempty"`

	// ExpirationDays specifies days after creation to delete (0 = never)
	ExpirationDays int `json:"expiration_days,omitempty"`

	// Prefix filters objects by key prefix (empty = all objects)
	Prefix string `json:"prefix,omitempty"`

	// Tags filters objects by tags
	Tags map[string]string `json:"tags,omitempty"`
}

// PoolSet tracks a collection of storage pools
type PoolSet struct {
	mu    sync.RWMutex
	pools map[uuid.UUID]*StoragePool
	// Index by name for fast lookups
	byName map[string]uuid.UUID
}

// NewPoolSet creates an empty pool set
func NewPoolSet() *PoolSet {
	return &PoolSet{
		pools:  make(map[uuid.UUID]*StoragePool),
		byName: make(map[string]uuid.UUID),
	}
}

// Add adds or updates a pool in the set
func (ps *PoolSet) Add(p *StoragePool) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	// Check for name collision with different ID
	if existingID, exists := ps.byName[p.Name]; exists && existingID != p.ID {
		return fmt.Errorf("pool name %q already exists with different ID", p.Name)
	}

	// Remove old name mapping if name changed
	if existing, exists := ps.pools[p.ID]; exists && existing.Name != p.Name {
		delete(ps.byName, existing.Name)
	}

	ps.pools[p.ID] = p
	ps.byName[p.Name] = p.ID
	return nil
}

// Get retrieves a pool by ID
func (ps *PoolSet) Get(id uuid.UUID) (*StoragePool, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	p, ok := ps.pools[id]
	return p, ok
}

// GetByName retrieves a pool by name
func (ps *PoolSet) GetByName(name string) (*StoragePool, bool) {
	ps.mu.RLock()
	defer ps.mu.RUnlock()
	if id, exists := ps.byName[name]; exists {
		return ps.pools[id], true
	}
	return nil, false
}

// Remove removes a pool from the set
func (ps *PoolSet) Remove(id uuid.UUID) error {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	p, exists := ps.pools[id]
	if !exists {
		return fmt.Errorf("pool %s not found", id)
	}

	if err := p.CanDelete(); err != nil {
		return err
	}

	delete(ps.byName, p.Name)
	delete(ps.pools, id)
	return nil
}

// ForceRemove removes a pool ignoring protection flags
func (ps *PoolSet) ForceRemove(id uuid.UUID) {
	ps.mu.Lock()
	defer ps.mu.Unlock()

	if p, exists := ps.pools[id]; exists {
		delete(ps.byName, p.Name)
		delete(ps.pools, id)
	}
}

// List returns all pools
func (ps *PoolSet) List() []*StoragePool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	result := make([]*StoragePool, 0, len(ps.pools))
	for _, p := range ps.pools {
		result = append(result, p)
	}
	return result
}

// ListWritable returns pools that accept writes
func (ps *PoolSet) ListWritable() []*StoragePool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var result []*StoragePool
	for _, p := range ps.pools {
		if p.CanWrite() {
			result = append(result, p)
		}
	}
	return result
}

// ByType returns pools matching the given storage type
func (ps *PoolSet) ByType(t StorageType) []*StoragePool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var result []*StoragePool
	for _, p := range ps.pools {
		if p.BackendType == t {
			result = append(result, p)
		}
	}
	return result
}

// ByDiskType returns pools matching the given disk type
func (ps *PoolSet) ByDiskType(m MediaType) []*StoragePool {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var result []*StoragePool
	for _, p := range ps.pools {
		if p.DiskType == m {
			result = append(result, p)
		}
	}
	return result
}

// TotalWeight returns the sum of all pool weights
func (ps *PoolSet) TotalWeight() float64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var total float64
	for _, p := range ps.pools {
		total += p.Weight
	}
	return total
}

// TotalWritableWeight returns the sum of weights for writable pools
func (ps *PoolSet) TotalWritableWeight() float64 {
	ps.mu.RLock()
	defer ps.mu.RUnlock()

	var total float64
	for _, p := range ps.pools {
		if p.CanWrite() {
			total += p.Weight
		}
	}
	return total
}

// PoolTarget references a pool for placement with an optional weight override
type PoolTarget struct {
	// PoolID is the immutable reference to the pool
	PoolID uuid.UUID `json:"pool_id"`

	// WeightOverride optionally overrides the pool's default weight for this profile
	// If 0, uses the pool's configured weight
	WeightOverride float64 `json:"weight_override,omitempty"`
}

// SelectPool picks a pool from targets using weighted random selection
func SelectPool(targets []PoolTarget, pools *PoolSet, rng *rand.Rand) (*StoragePool, error) {
	if len(targets) == 0 {
		return nil, fmt.Errorf("no pool targets configured")
	}

	// Build list of available pools with their effective weights
	type weightedPool struct {
		pool   *StoragePool
		weight float64
	}

	var available []weightedPool
	var totalWeight float64

	for _, t := range targets {
		pool, exists := pools.Get(t.PoolID)
		if !exists {
			continue // Skip missing pools
		}
		if !pool.CanWrite() {
			continue // Skip read-only pools
		}

		weight := t.WeightOverride
		if weight == 0 {
			weight = pool.Weight
		}

		available = append(available, weightedPool{pool: pool, weight: weight})
		totalWeight += weight
	}

	if len(available) == 0 {
		return nil, fmt.Errorf("no writable pools available")
	}

	if totalWeight == 0 {
		// All weights zero, pick random
		return available[rng.Intn(len(available))].pool, nil
	}

	// Weighted random selection
	pick := rng.Float64() * totalWeight
	var cumulative float64
	for _, wp := range available {
		cumulative += wp.weight
		if pick < cumulative {
			return wp.pool, nil
		}
	}

	return available[len(available)-1].pool, nil
}

// SelectPools picks n unique pools for replica/shard placement
func SelectPools(n int, targets []PoolTarget, pools *PoolSet, rng *rand.Rand) ([]*StoragePool, error) {
	if n <= 0 {
		return nil, nil
	}

	// Build list of available pools with weights
	type weightedPool struct {
		pool   *StoragePool
		weight float64
		score  float64
	}

	var available []weightedPool
	for _, t := range targets {
		pool, exists := pools.Get(t.PoolID)
		if !exists || !pool.CanWrite() {
			continue
		}

		weight := t.WeightOverride
		if weight == 0 {
			weight = pool.Weight
		}

		available = append(available, weightedPool{
			pool:   pool,
			weight: weight,
			score:  rng.Float64() * (weight + 1), // Random score weighted by pool weight
		})
	}

	if len(available) < n {
		return nil, fmt.Errorf("need %d pools but only %d available", n, len(available))
	}

	// Sort by score descending (bubble sort for small n)
	for i := 0; i < len(available)-1; i++ {
		for j := i + 1; j < len(available); j++ {
			if available[j].score > available[i].score {
				available[i], available[j] = available[j], available[i]
			}
		}
	}

	result := make([]*StoragePool, n)
	for i := 0; i < n; i++ {
		result[i] = available[i].pool
	}
	return result, nil
}
