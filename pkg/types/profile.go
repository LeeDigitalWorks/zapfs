// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"github.com/google/uuid"
)

// StorageProfile defines how objects should be stored using the Pool abstraction.
// Profiles determine placement (which pools), durability (replication/EC), and compression.
// Lifecycle rules (transitions, expiration) are defined at the bucket level, not here.
type StorageProfile struct {
	// ID is the immutable unique identifier for this profile
	ID uuid.UUID `json:"id"`

	// Name is a human-readable identifier (e.g., "STANDARD", "COLD", "ARCHIVE")
	Name string `json:"name"`

	// Description provides additional context about this profile
	Description string `json:"description,omitempty"`

	// Pools defines which pools can be used and their weight overrides
	Pools []PoolTarget `json:"pools"`

	// Replication is the number of copies to maintain (mutually exclusive with EC)
	Replication int `json:"replication,omitempty"`

	// ECScheme defines erasure coding parameters (mutually exclusive with Replication)
	ECScheme *ECScheme `json:"ec_scheme,omitempty"`

	// Compression specifies the compression algorithm (none, lz4, zstd, snappy)
	Compression string `json:"compression,omitempty"`

	// Constraints for placement
	SpreadAcrossRacks bool `json:"spread_across_racks,omitempty"`
	SpreadAcrossDCs   bool `json:"spread_across_dcs,omitempty"`
}

// IsErasureCoded returns true if this profile uses EC instead of replication
func (p *StorageProfile) IsErasureCoded() bool {
	return p.ECScheme != nil && p.ECScheme.DataShards > 0
}

// RequiredPoolCount returns the minimum number of pools needed
func (p *StorageProfile) RequiredPoolCount() int {
	if p.IsErasureCoded() {
		return p.ECScheme.DataShards + p.ECScheme.ParityShards
	}
	if p.Replication > 0 {
		return p.Replication
	}
	return 1
}

// NewStorageProfile creates a new storage profile with generated UUID
func NewStorageProfile(name string) *StorageProfile {
	return &StorageProfile{
		ID:          uuid.New(),
		Name:        name,
		Replication: 1,
	}
}

// StandardProfile returns a basic profile for general-purpose storage
func StandardProfile(poolID uuid.UUID) *StorageProfile {
	return &StorageProfile{
		ID:          uuid.New(),
		Name:        "STANDARD",
		Description: "General-purpose storage with balanced performance",
		Pools:       []PoolTarget{{PoolID: poolID}},
		Replication: 1,
		Compression: "lz4",
	}
}

// ReplicatedProfile returns a profile with replication across pools
func ReplicatedProfile(name string, poolIDs []uuid.UUID, replication int) *StorageProfile {
	targets := make([]PoolTarget, len(poolIDs))
	for i, id := range poolIDs {
		targets[i] = PoolTarget{PoolID: id}
	}

	return &StorageProfile{
		ID:          uuid.New(),
		Name:        name,
		Pools:       targets,
		Replication: replication,
	}
}

// ECProfile returns a profile using erasure coding
func ECProfile(name string, poolIDs []uuid.UUID, dataShards, parityShards int) *StorageProfile {
	targets := make([]PoolTarget, len(poolIDs))
	for i, id := range poolIDs {
		targets[i] = PoolTarget{PoolID: id}
	}

	return &StorageProfile{
		ID:    uuid.New(),
		Name:  name,
		Pools: targets,
		ECScheme: &ECScheme{
			DataShards:   dataShards,
			ParityShards: parityShards,
		},
	}
}

// ColdProfile returns a profile suitable for infrequent access data
func ColdProfile(poolID uuid.UUID) *StorageProfile {
	return &StorageProfile{
		ID:          uuid.New(),
		Name:        "COLD",
		Description: "Infrequent access storage with high compression",
		Pools:       []PoolTarget{{PoolID: poolID}},
		Replication: 2,
		Compression: "zstd",
	}
}

// ArchiveProfile returns a profile for long-term archival storage
func ArchiveProfile(poolID uuid.UUID) *StorageProfile {
	return &StorageProfile{
		ID:          uuid.New(),
		Name:        "ARCHIVE",
		Description: "Long-term archival storage",
		Pools:       []PoolTarget{{PoolID: poolID}},
		Replication: 1,
		Compression: "zstd",
	}
}

// ProfileSet manages a collection of storage profiles
type ProfileSet struct {
	profiles map[string]*StorageProfile
	byID     map[uuid.UUID]*StorageProfile
}

// NewProfileSet creates an empty profile set
func NewProfileSet() *ProfileSet {
	return &ProfileSet{
		profiles: make(map[string]*StorageProfile),
		byID:     make(map[uuid.UUID]*StorageProfile),
	}
}

// Add adds or updates a profile
func (ps *ProfileSet) Add(p *StorageProfile) {
	// Remove old entry if name changed
	if existing, exists := ps.byID[p.ID]; exists && existing.Name != p.Name {
		delete(ps.profiles, existing.Name)
	}
	ps.profiles[p.Name] = p
	ps.byID[p.ID] = p
}

// Get retrieves a profile by name
func (ps *ProfileSet) Get(name string) (*StorageProfile, bool) {
	p, ok := ps.profiles[name]
	return p, ok
}

// GetByID retrieves a profile by ID
func (ps *ProfileSet) GetByID(id uuid.UUID) (*StorageProfile, bool) {
	p, ok := ps.byID[id]
	return p, ok
}

// Remove removes a profile by name
func (ps *ProfileSet) Remove(name string) {
	if p, exists := ps.profiles[name]; exists {
		delete(ps.byID, p.ID)
		delete(ps.profiles, name)
	}
}

// List returns all profiles
func (ps *ProfileSet) List() []*StorageProfile {
	result := make([]*StorageProfile, 0, len(ps.profiles))
	for _, p := range ps.profiles {
		result = append(result, p)
	}
	return result
}

// Names returns all profile names
func (ps *ProfileSet) Names() []string {
	names := make([]string, 0, len(ps.profiles))
	for name := range ps.profiles {
		names = append(names, name)
	}
	return names
}
