// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package multipart

import (
	"context"
	"io"
)

// Service defines the interface for multipart upload operations.
// This separates business logic from HTTP handling.
type Service interface {
	// CreateUpload initiates a new multipart upload.
	CreateUpload(ctx context.Context, req *CreateUploadRequest) (*CreateUploadResult, error)

	// UploadPart uploads a part of a multipart upload.
	UploadPart(ctx context.Context, req *UploadPartRequest) (*UploadPartResult, error)

	// UploadPartCopy copies data from an existing object as a part.
	UploadPartCopy(ctx context.Context, req *UploadPartCopyRequest) (*UploadPartCopyResult, error)

	// CompleteUpload completes a multipart upload by assembling parts.
	CompleteUpload(ctx context.Context, req *CompleteUploadRequest) (*CompleteUploadResult, error)

	// AbortUpload aborts a multipart upload and cleans up parts.
	AbortUpload(ctx context.Context, bucket, key, uploadID string) error

	// ListParts lists the parts of a multipart upload.
	ListParts(ctx context.Context, req *ListPartsRequest) (*ListPartsResult, error)

	// ListUploads lists in-progress multipart uploads for a bucket.
	ListUploads(ctx context.Context, req *ListUploadsRequest) (*ListUploadsResult, error)
}

// PartEntry represents a part in a complete request
type PartEntry struct {
	PartNumber int
	ETag       string
}

// CreateUploadRequest contains parameters for initiating a multipart upload
type CreateUploadRequest struct {
	Bucket       string
	Key          string
	OwnerID      string
	ContentType  string
	StorageClass string

	// SSE-KMS encryption (optional)
	SSEKMS *SSEKMSParams
}

// SSEKMSParams contains SSE-KMS encryption parameters
type SSEKMSParams struct {
	KeyID   string // KMS key ID
	Context string // Optional encryption context
}

// CreateUploadResult contains the result of initiating a multipart upload
type CreateUploadResult struct {
	UploadID string

	// SSE-KMS response info (if encryption was applied)
	SSEAlgorithm  string
	SSEKMSKeyID   string
	SSEKMSContext string
}

// UploadPartRequest contains parameters for uploading a part
type UploadPartRequest struct {
	Bucket        string
	Key           string
	UploadID      string
	PartNumber    int
	Body          io.Reader
	ContentLength int64
}

// UploadPartResult contains the result of uploading a part
type UploadPartResult struct {
	ETag string
}

// UploadPartCopyRequest contains parameters for copying a part from an existing object
type UploadPartCopyRequest struct {
	Bucket       string
	Key          string
	UploadID     string
	PartNumber   int
	SourceBucket string
	SourceKey    string
	// SourceRange is optional, format: "bytes=start-end" (inclusive)
	// If empty, copy the entire source object
	SourceRange string
	// Conditional copy headers
	CopySourceIfMatch           string
	CopySourceIfNoneMatch       string
	CopySourceIfModifiedSince   *int64 // Unix timestamp
	CopySourceIfUnmodifiedSince *int64 // Unix timestamp
}

// UploadPartCopyResult contains the result of copying a part
type UploadPartCopyResult struct {
	ETag         string
	LastModified int64 // Unix nanoseconds
}

// CompleteUploadRequest contains parameters for completing a multipart upload
type CompleteUploadRequest struct {
	Bucket   string
	Key      string
	UploadID string
	Parts    []PartEntry
}

// CompleteUploadResult contains the result of completing a multipart upload
type CompleteUploadResult struct {
	Location string
	Bucket   string
	Key      string
	ETag     string

	// SSE-KMS response info (if encryption was applied)
	SSEAlgorithm  string
	SSEKMSKeyID   string
	SSEKMSContext string
}

// ListPartsRequest contains parameters for listing parts
type ListPartsRequest struct {
	Bucket           string
	Key              string
	UploadID         string
	PartNumberMarker int
	MaxParts         int
}

// ListPartsResult contains the result of listing parts
type ListPartsResult struct {
	Bucket               string
	Key                  string
	UploadID             string
	OwnerID              string
	StorageClass         string
	PartNumberMarker     int
	NextPartNumberMarker int
	MaxParts             int
	IsTruncated          bool
	Parts                []PartInfo
}

// PartInfo represents a part in list results
type PartInfo struct {
	PartNumber   int
	LastModified int64
	ETag         string
	Size         int64
}

// ListUploadsRequest contains parameters for listing uploads
type ListUploadsRequest struct {
	Bucket         string
	Prefix         string
	Delimiter      string
	KeyMarker      string
	UploadIDMarker string
	MaxUploads     int
}

// ListUploadsResult contains the result of listing uploads
type ListUploadsResult struct {
	Bucket             string
	KeyMarker          string
	UploadIDMarker     string
	NextKeyMarker      string
	NextUploadIDMarker string
	Delimiter          string
	Prefix             string
	MaxUploads         int
	IsTruncated        bool
	Uploads            []UploadInfo
	CommonPrefixes     []string
}

// UploadInfo represents an upload in list results
type UploadInfo struct {
	Key          string
	UploadID     string
	OwnerID      string
	StorageClass string
	Initiated    int64
}
