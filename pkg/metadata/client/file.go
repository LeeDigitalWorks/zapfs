// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"io"
)

// ObjectWriter is a callback that receives chunks of data from GetObject
type ObjectWriter func(chunk []byte) error

// ChunkInfo describes a chunk written by the file server
type ChunkInfo struct {
	ChunkID      string // SHA-256 content hash
	Size         uint64 // Compressed size on disk
	Offset       uint64 // Offset within the object
	OriginalSize uint64 // Original uncompressed size (0 = same as Size)
	Compression  string // Compression algorithm used ("none", "lz4", "zstd", "snappy")
}

// PutObjectResult contains the result of a PutObject operation
type PutObjectResult struct {
	ObjectID string
	Size     uint64
	ETag     string
	Chunks   []ChunkInfo // Actual chunk IDs (SHA-256 content hashes)
}

// DecrementRefCountResult contains the result of a DecrementRefCount operation
type DecrementRefCountResult struct {
	ChunkID     string
	NewRefCount uint32
	Success     bool
	Error       string
}

// File defines the file server operations needed by MetadataServer.
// This interface allows for easy mocking in tests while the production code
// uses FileClientPool which manages connection pooling.
type File interface {
	// PutObject streams data to a file server without compression
	PutObject(ctx context.Context, address string, objectID string, data io.Reader, totalSize uint64) (*PutObjectResult, error)

	// PutObjectWithCompression streams data to a file server with optional compression.
	// The compression algorithm is passed to the file server which compresses chunks before storage.
	// Supported algorithms: "none", "lz4", "zstd", "snappy"
	PutObjectWithCompression(ctx context.Context, address string, objectID string, data io.Reader, totalSize uint64, compression string) (*PutObjectResult, error)

	// GetObject retrieves an object from a file server by ObjectID
	// Deprecated: Use GetChunk for reading by chunk ID
	GetObject(ctx context.Context, address string, objectID string, writer ObjectWriter) (etag string, err error)

	// GetObjectRange retrieves a range of bytes from an object by ObjectID
	// Deprecated: Use GetChunkRange for reading by chunk ID
	GetObjectRange(ctx context.Context, address string, objectID string, offset, length uint64, writer ObjectWriter) (etag string, err error)

	// GetChunk retrieves chunk data by its SHA-256 content hash
	GetChunk(ctx context.Context, address string, chunkID string, writer ObjectWriter) error

	// GetChunkRange retrieves a range of bytes from a chunk by its SHA-256 content hash
	GetChunkRange(ctx context.Context, address string, chunkID string, offset, length uint64, writer ObjectWriter) error

	// Close closes all connections
	Close() error
}
