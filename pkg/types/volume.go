// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"encoding/hex"
	"path/filepath"

	"github.com/LeeDigitalWorks/zapfs/pkg/utils"

	"github.com/google/uuid"
)

// ChunkSize is the default size for data chunks (64MB)
const ChunkSize = 64 * 1024 * 1024 // 64MB

// ChunkID uniquely identifies a chunk by its content hash
type ChunkID string

// ChunkIDFromBytes computes a ChunkID from data content
func ChunkIDFromBytes(data []byte) ChunkID {
	h := utils.Sha256PoolGetHasher()
	h.Write(data)
	sum := h.Sum(nil)
	utils.Sha256PoolPutHasher(h)
	return ChunkID(hex.EncodeToString(sum))
}

func (c ChunkID) String() string {
	return string(c)
}

// FullPath returns the full filesystem path for this chunk
func (c ChunkID) FullPath(base string) string {
	return filepath.Join(base, c.SubDirs(), c.String())
}

// SubDirs returns the subdirectory path (for sharding across directories)
func (c ChunkID) SubDirs() string {
	return filepath.Join(c.Prefix1(), c.Prefix2())
}

// Prefix1 returns the first 2 characters for directory sharding
func (c ChunkID) Prefix1() string {
	s := c.String()
	if len(s) < 2 {
		return s
	}
	return s[0:2]
}

// Prefix2 returns characters 3-4 for directory sharding
func (c ChunkID) Prefix2() string {
	s := c.String()
	if len(s) < 4 {
		if len(s) <= 2 {
			return ""
		}
		return s[2:]
	}
	return s[2:4]
}

// Chunk represents stored data with location information
type Chunk struct {
	ID           ChunkID   `json:"id"`
	BackendID    string    `json:"backend_id"` // Which backend this chunk is stored on
	Path         string    `json:"path"`       // Path/key within the backend
	Size         uint64    `json:"size"`
	Checksum     string    `json:"checksum"`
	RefCount     uint32    `json:"ref_count"`      // Reference count for deduplication
	ZeroRefSince int64     `json:"zero_ref_since"` // Unix timestamp when RefCount first became 0 (for GC grace period)
	ECGroupID    uuid.UUID `json:"ec_group_id,omitempty"`
	ShardIdx     int       `json:"shard_idx,omitempty"` // Index within EC group (0 = first data shard)
	IsParity     bool      `json:"is_parity,omitempty"` // True if this is a parity shard
}

// IsErasureCoded returns true if this chunk is part of an EC group
func (c *Chunk) IsErasureCoded() bool {
	return c.ECGroupID != uuid.Nil
}

// Volume represents a logical grouping of chunks
// A volume can be replicated or erasure-coded as a unit
type Volume struct {
	ID         uuid.UUID `json:"id"`
	BackendID  string    `json:"backend_id"`
	Collection string    `json:"collection,omitempty"` // Logical grouping (e.g., bucket name)
	Size       uint64    `json:"size"`                 // Current size
	MaxSize    uint64    `json:"max_size"`             // Maximum size before sealing
	Sealed     bool      `json:"sealed"`               // No more writes allowed
	ReadOnly   bool      `json:"read_only"`
	TTLSeconds uint32    `json:"ttl_seconds,omitempty"` // For time-limited volumes
}
