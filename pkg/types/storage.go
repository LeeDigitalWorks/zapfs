// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package types

import (
	"context"
	"io"
)

// StorageType identifies the backend storage implementation
type StorageType string

const (
	StorageTypeLocal StorageType = "local" // Local filesystem
	StorageTypeS3    StorageType = "s3"    // S3-compatible
	StorageTypeCeph  StorageType = "ceph"  // Ceph RADOS
)

// MediaType describes the physical storage medium
type MediaType string

const (
	MediaTypeUnknown MediaType = ""
	MediaTypeSSD     MediaType = "ssd"
	MediaTypeHDD     MediaType = "hdd"
	MediaTypeNVMe    MediaType = "nvme"
)

// Backend represents a storage backend (local disk, S3 bucket, Ceph pool, etc.)
// This is the unit of placement and capacity tracking
type Backend struct {
	ID          string      `json:"id"`
	Type        StorageType `json:"type"`
	MediaType   MediaType   `json:"media_type,omitempty"` // Only relevant for local storage
	Endpoint    string      `json:"endpoint,omitempty"`   // For remote backends (S3 URL, Ceph cluster, etc.)
	Bucket      string      `json:"bucket,omitempty"`     // For object store backends
	Path        string      `json:"path,omitempty"`       // For local filesystem
	Region      string      `json:"region,omitempty"`     // For cloud backends
	TotalBytes  uint64      `json:"total_bytes"`
	UsedBytes   uint64      `json:"used_bytes"`
	ReadOnly    bool        `json:"read_only"`
	Credentials string      `json:"credentials,omitempty"` // Reference to credentials (not the actual secret)
}

// FreeBytes returns available capacity
func (b *Backend) FreeBytes() uint64 {
	if b.UsedBytes >= b.TotalBytes {
		return 0
	}
	return b.TotalBytes - b.UsedBytes
}

// UsagePercent returns capacity usage as percentage (0-100)
func (b *Backend) UsagePercent() float64 {
	if b.TotalBytes == 0 {
		return 0
	}
	return float64(b.UsedBytes) / float64(b.TotalBytes) * 100
}

// BackendStorage is the interface for reading/writing data to a backend
// Implementations: LocalStorage, S3Storage, CephStorage, etc.
type BackendStorage interface {
	// Type returns the storage type
	Type() StorageType

	// Write writes data to the backend, returns the key/path where it was stored
	Write(ctx context.Context, key string, data io.Reader, size int64) error

	// Read reads data from the backend
	Read(ctx context.Context, key string) (io.ReadCloser, error)

	// ReadRange reads a byte range from the backend
	ReadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error)

	// Delete removes data from the backend
	Delete(ctx context.Context, key string) error

	// Exists checks if a key exists
	Exists(ctx context.Context, key string) (bool, error)

	// Size returns the size of the stored data
	Size(ctx context.Context, key string) (int64, error)

	// Close releases any resources
	Close() error
}

// BackendConfig contains configuration for creating a backend storage instance
type BackendConfig struct {
	Type      StorageType       `json:"type"`
	Endpoint  string            `json:"endpoint,omitempty"`
	Bucket    string            `json:"bucket,omitempty"`
	Path      string            `json:"path,omitempty"`
	Region    string            `json:"region,omitempty"`
	AccessKey string            `json:"access_key,omitempty"`
	SecretKey string            `json:"secret_key,omitempty"`
	Options   map[string]string `json:"options,omitempty"`

	// DirectIO enables O_DIRECT for writes (Linux only).
	// Bypasses page cache for reduced memory pressure and more predictable latency.
	DirectIO bool `json:"direct_io,omitempty"`
}
