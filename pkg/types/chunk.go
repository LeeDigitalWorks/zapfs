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

// Chunk represents stored data with location information.
// Note: RefCount is managed centrally in the metadata DB's chunk_registry table,
// not on individual file servers. File servers only track chunk presence.
type Chunk struct {
	ID           ChunkID   `json:"id"`
	BackendID    string    `json:"backend_id"`              // Which backend this chunk is stored on
	Path         string    `json:"path"`                    // Path/key within the backend
	Size         uint64    `json:"size"`                    // Compressed size on disk
	OriginalSize uint64    `json:"original_size,omitempty"` // Original uncompressed size (0 = same as Size)
	Compression  string    `json:"compression,omitempty"`   // Compression algorithm: "none", "lz4", "zstd", "snappy"
	Checksum     string    `json:"checksum"`
	CreatedAt    int64     `json:"created_at"` // Unix timestamp when chunk was created (for reconciliation grace period)
	ECGroupID    uuid.UUID `json:"ec_group_id,omitempty"`
	ShardIdx     int       `json:"shard_idx,omitempty"` // Index within EC group (0 = first data shard)
	IsParity     bool      `json:"is_parity,omitempty"` // True if this is a parity shard
}

// GetOriginalSize returns the original uncompressed size.
// If OriginalSize is 0, returns Size (indicates no compression or legacy data).
func (c *Chunk) GetOriginalSize() uint64 {
	if c.OriginalSize > 0 {
		return c.OriginalSize
	}
	return c.Size
}

// IsCompressed returns true if the chunk is compressed.
func (c *Chunk) IsCompressed() bool {
	return c.Compression != "" && c.Compression != "none"
}

// IsErasureCoded returns true if this chunk is part of an EC group
func (c *Chunk) IsErasureCoded() bool {
	return c.ECGroupID != uuid.Nil
}
