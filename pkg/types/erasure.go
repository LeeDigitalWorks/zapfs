// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// ECScheme defines erasure coding parameters
type ECScheme struct {
	DataShards   int `json:"data_shards"`   // Number of data shards (e.g., 10)
	ParityShards int `json:"parity_shards"` // Number of parity shards (e.g., 4)
}

// Common EC schemes
var (
	// EC10_4 is the default scheme: 10 data + 4 parity
	// Can tolerate 4 failures, 40% storage overhead
	EC10_4 = ECScheme{DataShards: 10, ParityShards: 4}

	// EC8_4 is more fault tolerant: 8 data + 4 parity
	// Can tolerate 4 failures, 50% storage overhead
	EC8_4 = ECScheme{DataShards: 8, ParityShards: 4}

	// EC4_2 for smaller deployments: 4 data + 2 parity
	// Can tolerate 2 failures, 50% storage overhead, needs only 6 backends
	EC4_2 = ECScheme{DataShards: 4, ParityShards: 2}

	// EC6_3 balanced: 6 data + 3 parity
	// Can tolerate 3 failures, 50% storage overhead
	EC6_3 = ECScheme{DataShards: 6, ParityShards: 3}

	// ECSchemes maps scheme names to predefined schemes
	ECSchemes = map[string]ECScheme{
		"4+2":  EC4_2,
		"6+3":  EC6_3,
		"8+4":  EC8_4,
		"10+4": EC10_4,
	}
)

// ParseECScheme parses an EC scheme string like "4+2" or "10+4"
// Returns the parsed scheme or an error if invalid
func ParseECScheme(s string) (ECScheme, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return EC4_2, nil // Default scheme
	}

	// Check predefined schemes first
	if scheme, ok := ECSchemes[s]; ok {
		return scheme, nil
	}

	// Parse custom "data+parity" format
	parts := strings.Split(s, "+")
	if len(parts) != 2 {
		return ECScheme{}, fmt.Errorf("invalid EC scheme format %q: expected 'data+parity' (e.g., '4+2')", s)
	}

	data, err := strconv.Atoi(strings.TrimSpace(parts[0]))
	if err != nil || data < 1 {
		return ECScheme{}, fmt.Errorf("invalid data shards in EC scheme %q: must be positive integer", s)
	}

	parity, err := strconv.Atoi(strings.TrimSpace(parts[1]))
	if err != nil || parity < 1 {
		return ECScheme{}, fmt.Errorf("invalid parity shards in EC scheme %q: must be positive integer", s)
	}

	return ECScheme{DataShards: data, ParityShards: parity}, nil
}

// String returns the scheme in "data+parity" format
func (e ECScheme) String() string {
	return fmt.Sprintf("%d+%d", e.DataShards, e.ParityShards)
}

// TotalShards returns total number of shards
func (e ECScheme) TotalShards() int {
	return e.DataShards + e.ParityShards
}

// Overhead returns the storage overhead ratio (e.g., 1.4 for 10+4)
func (e ECScheme) Overhead() float64 {
	if e.DataShards == 0 {
		return 0
	}
	return float64(e.TotalShards()) / float64(e.DataShards)
}

// FaultTolerance returns how many shards can be lost
func (e ECScheme) FaultTolerance() int {
	return e.ParityShards
}

// MinBackends returns minimum backends needed for this scheme
func (e ECScheme) MinBackends() int {
	// Need at least ParityShards+1 backends to survive losing ParityShards
	return e.ParityShards + 1
}

// ECGroup represents one erasure-coded stripe
// Contains references to all data + parity shards
type ECGroup struct {
	ID           uuid.UUID `json:"id"`            // Unique ID for this stripe
	ObjectID     uuid.UUID `json:"object_id"`     // Object this stripe belongs to
	Scheme       ECScheme  `json:"scheme"`        // EC parameters used
	OriginalSize uint64    `json:"original_size"` // Size before encoding
	Shards       []ECShard `json:"shards"`        // All shards (data first, then parity)
}

// ECShard represents one shard of an EC group
type ECShard struct {
	Index     int     `json:"index"`      // 0-based index (0..DataShards-1 = data, rest = parity)
	ChunkID   ChunkID `json:"chunk_id"`   // Reference to the stored chunk
	BackendID string  `json:"backend_id"` // Which backend stores this shard
	Size      uint64  `json:"size"`       // Size of this shard
}

// IsParity returns true if this is a parity shard
func (s ECShard) IsParity(scheme ECScheme) bool {
	return s.Index >= scheme.DataShards
}

// DataShards returns only the data shards from the group
func (g *ECGroup) DataShards() []ECShard {
	if len(g.Shards) < g.Scheme.DataShards {
		return g.Shards
	}
	return g.Shards[:g.Scheme.DataShards]
}

// ParityShards returns only the parity shards from the group
func (g *ECGroup) ParityShards() []ECShard {
	if len(g.Shards) <= g.Scheme.DataShards {
		return nil
	}
	return g.Shards[g.Scheme.DataShards:]
}

// AvailableShards counts shards that have valid chunk references
func (g *ECGroup) AvailableShards() int {
	count := 0
	for _, s := range g.Shards {
		if s.ChunkID != "" {
			count++
		}
	}
	return count
}

// CanRecover returns true if we have enough shards to recover the data
func (g *ECGroup) CanRecover() bool {
	return g.AvailableShards() >= g.Scheme.DataShards
}

// MissingShards returns indices of shards that are missing or unavailable
func (g *ECGroup) MissingShards() []int {
	var missing []int
	for i, s := range g.Shards {
		if s.ChunkID == "" {
			missing = append(missing, i)
		}
	}
	return missing
}
