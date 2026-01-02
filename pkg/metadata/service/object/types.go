// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package object

import (
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// PutObjectRequest contains parameters for storing an object
type PutObjectRequest struct {
	Bucket        string
	Key           string
	Body          io.Reader
	ContentLength int64 // -1 if unknown (chunked encoding)
	StorageClass  string
	Owner         string

	// Encryption options (mutually exclusive)
	SSEC   *SSECParams
	SSEKMS *SSEKMSParams
}

// PutObjectResult contains the result of storing an object
type PutObjectResult struct {
	ETag      string
	VersionID string

	// Encryption metadata for response headers
	SSEAlgorithm      string
	SSECustomerKeyMD5 string
	SSEKMSKeyID       string
	SSEKMSContext     string
}

// GetObjectRequest contains parameters for retrieving an object
type GetObjectRequest struct {
	Bucket    string
	Key       string
	VersionID string     // Specific version to retrieve (empty for latest)
	Range     *ByteRange // nil for full object

	// For SSE-C encrypted objects
	SSEC *SSECParams

	// For SSE-KMS key validation (optional)
	SSEKMSKeyID string

	// Conditional headers
	IfMatch           string
	IfNoneMatch       string
	IfModifiedSince   *time.Time
	IfUnmodifiedSince *time.Time
}

// GetObjectResult contains the result of retrieving an object
type GetObjectResult struct {
	Object   *types.ObjectRef
	Body     io.ReadCloser // Caller must close
	Metadata *ObjectMetadata

	// Range info (populated if range request)
	Range        *ByteRange
	IsPartial    bool
	AcceptRanges string
}

// HeadObjectResult contains metadata for an object without the body
type HeadObjectResult struct {
	Object   *types.ObjectRef
	Metadata *ObjectMetadata
}

// ObjectMetadata contains object metadata for responses
type ObjectMetadata struct {
	ETag         string
	LastModified time.Time
	Size         uint64
	ContentType  string
	StorageClass string

	// Encryption metadata
	SSEAlgorithm      string
	SSECustomerKeyMD5 string
	SSEKMSKeyID       string
	SSEKMSContext     string
}

// DeleteObjectResult contains the result of deleting an object
type DeleteObjectResult struct {
	VersionID      string
	DeleteMarker   bool
	DeleteMarkerID string
}

// DeleteObjectsRequest contains parameters for batch delete
type DeleteObjectsRequest struct {
	Bucket  string
	Objects []DeleteObjectEntry
	Quiet   bool
}

// DeleteObjectEntry represents a single object to delete
type DeleteObjectEntry struct {
	Key       string
	VersionID string
}

// DeleteObjectsResult contains the result of batch delete
type DeleteObjectsResult struct {
	Deleted []DeletedObject
	Errors  []DeleteError
}

// DeletedObject represents a successfully deleted object
type DeletedObject struct {
	Key                   string
	VersionID             string
	DeleteMarker          bool
	DeleteMarkerVersionID string
}

// DeleteError represents a failed delete operation
type DeleteError struct {
	Key       string
	VersionID string
	Code      string
	Message   string
}

// CopyObjectRequest contains parameters for copying an object
type CopyObjectRequest struct {
	SourceBucket string
	SourceKey    string
	DestBucket   string
	DestKey      string

	// Directives
	MetadataDirective string // "COPY" or "REPLACE"
	TaggingDirective  string // "COPY" or "REPLACE"

	// Conditional headers for source
	CopySourceIfMatch           string
	CopySourceIfNoneMatch       string
	CopySourceIfModifiedSince   *time.Time
	CopySourceIfUnmodifiedSince *time.Time

	// SSE-KMS encryption for destination (optional)
	// If not specified and source is encrypted, copy the encryption
	// If not specified and bucket has default encryption, use that
	SSEKMS *SSEKMSParams
}

// CopyObjectResult contains the result of copying an object
type CopyObjectResult struct {
	ETag         string
	LastModified time.Time
	VersionID    string

	// SSE response info (if encryption was applied)
	SSEAlgorithm      string
	SSEKMSKeyID       string
	SSEKMSContext     string
	SSECustomerKeyMD5 string
}

// ListObjectsRequest contains parameters for listing objects (v1)
type ListObjectsRequest struct {
	Bucket    string
	Prefix    string
	Delimiter string
	Marker    string
	MaxKeys   int
}

// ListObjectsResult contains the result of listing objects (v1)
type ListObjectsResult struct {
	Name           string
	Prefix         string
	Marker         string
	NextMarker     string
	Delimiter      string
	MaxKeys        int
	IsTruncated    bool
	Contents       []ObjectEntry
	CommonPrefixes []string
}

// ListObjectsV2Request contains parameters for listing objects (v2)
type ListObjectsV2Request struct {
	Bucket            string
	Prefix            string
	Delimiter         string
	ContinuationToken string
	StartAfter        string
	MaxKeys           int
	FetchOwner        bool
}

// ListObjectsV2Result contains the result of listing objects (v2)
type ListObjectsV2Result struct {
	Name                  string
	Prefix                string
	Delimiter             string
	MaxKeys               int
	KeyCount              int
	IsTruncated           bool
	ContinuationToken     string
	NextContinuationToken string
	StartAfter            string
	Contents              []ObjectEntry
	CommonPrefixes        []string
}

// ObjectEntry represents an object in a list result
type ObjectEntry struct {
	Key          string
	LastModified time.Time
	ETag         string
	Size         uint64
	StorageClass string
	Owner        *ObjectOwner
}

// ObjectOwner represents the owner of an object
type ObjectOwner struct {
	ID          string
	DisplayName string
}

// ByteRange represents a byte range for partial content requests
type ByteRange struct {
	Start  uint64
	End    uint64 // Inclusive
	Length uint64
}

// SSECParams contains SSE-C encryption parameters
type SSECParams struct {
	Algorithm string // "AES256"
	Key       []byte // 32 bytes for AES-256
	KeyMD5    string // Base64-encoded MD5 of key
}

// SSEKMSParams contains SSE-KMS encryption parameters
type SSEKMSParams struct {
	KeyID   string
	Context string // JSON encryption context
}

// ConditionalResult represents the result of evaluating conditional headers
type ConditionalResult struct {
	ShouldProceed bool
	StatusCode    int  // HTTP status code if ShouldProceed is false
	NotModified   bool // true for 304 Not Modified
}
