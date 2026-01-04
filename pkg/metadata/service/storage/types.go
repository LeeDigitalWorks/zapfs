// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package storage

import (
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// WriteRequest contains parameters for writing an object to storage
type WriteRequest struct {
	Bucket      string
	ObjectID    string
	Body        io.Reader
	Size        uint64
	ProfileName string
	Replication int // Number of replicas
}

// WriteResult contains the result of writing an object
type WriteResult struct {
	Size      uint64
	ETag      string // MD5 hash of the content
	ChunkRefs []types.ChunkRef
}

// ReadRequest contains parameters for reading an object from storage
type ReadRequest struct {
	ChunkRefs []types.ChunkRef
}

// ReadRangeRequest contains parameters for reading a range from storage
type ReadRangeRequest struct {
	ChunkRefs []types.ChunkRef
	Offset    uint64
	Length    uint64
}

// FederatedWriteRequest extends WriteRequest with federation parameters.
// Used for dual-write when a bucket is in MIGRATING mode with dual-write enabled.
type FederatedWriteRequest struct {
	WriteRequest

	// Key is the object key (used for external S3)
	Key string

	// ContentType is the MIME type for external S3
	ContentType string

	// ExternalBucket is the bucket name on external S3
	ExternalBucket string

	// ExternalEndpoint is the external S3 endpoint
	ExternalEndpoint string

	// ExternalRegion is the external S3 region
	ExternalRegion string

	// ExternalAccessKeyID is the access key for external S3
	ExternalAccessKeyID string

	// ExternalSecretAccessKey is the secret key for external S3
	ExternalSecretAccessKey string

	// ExternalPathStyle indicates if path-style addressing should be used
	ExternalPathStyle bool
}

// FederatedWriteResult extends WriteResult with external S3 info.
type FederatedWriteResult struct {
	WriteResult

	// ExternalETag is the ETag from external S3 (empty if write failed or disabled)
	ExternalETag string

	// ExternalVersionID is the version ID from external S3 (for versioned buckets)
	ExternalVersionID string

	// ExternalError contains any error from the external write (nil if succeeded)
	// Note: Local write is required to succeed; external errors are logged but not fatal.
	ExternalError error
}
