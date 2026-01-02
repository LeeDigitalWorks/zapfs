// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package object

import (
	"context"
)

// Service defines the interface for object operations.
// This separates business logic from HTTP handling.
type Service interface {
	// PutObject stores an object and returns the result.
	// For SSE-C/SSE-KMS, the body is encrypted before storage.
	PutObject(ctx context.Context, req *PutObjectRequest) (*PutObjectResult, error)

	// GetObject retrieves an object for reading.
	// Returns a ReadCloser that the caller must close.
	// For SSE-C/SSE-KMS encrypted objects, decryption is handled transparently.
	GetObject(ctx context.Context, req *GetObjectRequest) (*GetObjectResult, error)

	// HeadObject retrieves object metadata without the body.
	HeadObject(ctx context.Context, bucket, key string) (*HeadObjectResult, error)

	// DeleteObject soft-deletes an object.
	// Returns nil if the object doesn't exist (S3 compatibility).
	DeleteObject(ctx context.Context, bucket, key string) (*DeleteObjectResult, error)

	// DeleteObjectWithVersion handles versioned delete operations.
	// If versionID is empty and versioning is enabled, creates a delete marker.
	// If versionID is provided, permanently deletes that specific version.
	DeleteObjectWithVersion(ctx context.Context, bucket, key, versionID string) (*DeleteObjectResult, error)

	// DeleteObjects batch deletes multiple objects.
	DeleteObjects(ctx context.Context, req *DeleteObjectsRequest) (*DeleteObjectsResult, error)

	// CopyObject copies an object within or between buckets.
	// For server-side copy, no data transfer occurs.
	CopyObject(ctx context.Context, req *CopyObjectRequest) (*CopyObjectResult, error)

	// ListObjects lists objects using v1 API pagination.
	ListObjects(ctx context.Context, req *ListObjectsRequest) (*ListObjectsResult, error)

	// ListObjectsV2 lists objects using v2 API pagination.
	ListObjectsV2(ctx context.Context, req *ListObjectsV2Request) (*ListObjectsV2Result, error)
}
