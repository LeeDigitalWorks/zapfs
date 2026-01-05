// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package types

import "github.com/google/uuid"

// ObjectRef represents stored object metadata
// This is what the metadata service tracks for each object
type ObjectRef struct {
	ID          uuid.UUID `json:"id"`
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	Size        uint64    `json:"size"`
	Version     uint64    `json:"version"`
	ETag        string    `json:"etag"`
	ContentType string    `json:"content_type,omitempty"` // MIME type of the object
	CreatedAt   int64     `json:"created_at"`             // Unix timestamp
	DeletedAt   int64     `json:"deleted_at,omitempty"`
	TTL         uint32    `json:"ttl,omitempty"` // Seconds until expiration

	// Storage profile used for this object
	ProfileID string `json:"profile_id,omitempty"`

	// StorageClass is the S3 storage class (STANDARD, GLACIER, DEEP_ARCHIVE, etc.)
	StorageClass string `json:"storage_class,omitempty"`

	// Transition metadata (enterprise feature and federation)
	// TransitionedAt is when the object was transitioned to a different storage class (0 = not transitioned)
	TransitionedAt int64 `json:"transitioned_at,omitempty"`
	// TransitionedRef is the remote object reference. Usage depends on context:
	//
	// For lifecycle transitions (enterprise):
	//   - Format: "ab/cd/uuid" (tier backend path)
	//   - Set when object is transitioned to GLACIER, DEEP_ARCHIVE, etc.
	//   - ChunkRefs are cleared; data is in tier storage
	//
	// For federation/migration (community):
	//   - Format: "bucket/key" or "bucket/key?versionId=xxx"
	//   - Set when bucket is in PASSTHROUGH or MIGRATING mode
	//   - Points to object location in external S3
	//   - ChunkRefs empty = data only in external S3 (lazy migration pending)
	//   - ChunkRefs set = data ingested locally (migration complete for this object)
	TransitionedRef string `json:"transitioned_ref,omitempty"`

	// Restore metadata (enterprise feature - for archived objects)
	// RestoreStatus tracks the restore state: "", "pending", "in_progress", "completed"
	RestoreStatus string `json:"restore_status,omitempty"`
	// RestoreExpiryDate is when the restored copy expires (Unix nano, 0 = not restored)
	RestoreExpiryDate int64 `json:"restore_expiry_date,omitempty"`
	// RestoreTier is the retrieval tier used: "Expedited", "Standard", "Bulk"
	RestoreTier string `json:"restore_tier,omitempty"`
	// RestoreRequestedAt is when the restore was requested (Unix nano)
	RestoreRequestedAt int64 `json:"restore_requested_at,omitempty"`

	// Intelligent tiering access tracking
	// LastAccessedAt is updated on GET for intelligent tiering (Unix nano, 0 = never accessed, use CreatedAt)
	LastAccessedAt int64 `json:"last_accessed_at,omitempty"`

	// Storage location - one of these is set depending on storage mode
	ChunkRefs  []ChunkRef  `json:"chunk_refs,omitempty"`   // For simple/replicated storage
	ECGroupIDs []uuid.UUID `json:"ec_group_ids,omitempty"` // For erasure-coded storage

	// IsLatest indicates if this is the current version of the object
	IsLatest bool `json:"is_latest,omitempty"`

	// Encryption metadata
	// SSEAlgorithm: "AES256" for SSE-S3 or SSE-C, "aws:kms" for SSE-KMS, empty for no encryption
	SSEAlgorithm string `json:"sse_algorithm,omitempty"`
	// SSECustomerKeyMD5: MD5 hash of the customer-provided encryption key (for SSE-C validation)
	SSECustomerKeyMD5 string `json:"sse_customer_key_md5,omitempty"`
	// SSEKMSKeyID: KMS key ID used for encryption (for SSE-KMS)
	SSEKMSKeyID string `json:"sse_kms_key_id,omitempty"`
	// SSEKMSContext: KMS encryption context (for SSE-KMS)
	SSEKMSContext string `json:"sse_kms_context,omitempty"`
}

// ChunkRef references a chunk that stores part of an object
type ChunkRef struct {
	ChunkID        ChunkID `json:"chunk_id"`                   // SHA-256 hash of ORIGINAL (uncompressed) data
	Offset         uint64  `json:"offset"`                     // Offset within the object
	Size           uint64  `json:"size"`                       // Compressed size on disk
	OriginalSize   uint64  `json:"original_size,omitempty"`    // Original uncompressed size (0 means same as Size)
	Compression    string  `json:"compression,omitempty"`      // Compression algorithm: "none", "lz4", "zstd", "snappy"
	BackendID      string  `json:"backend_id"`                 // Which backend has this chunk
	FileServerAddr string  `json:"file_server_addr,omitempty"` // Address of file server hosting this chunk
}

// GetOriginalSize returns the original uncompressed size.
// If OriginalSize is 0, returns Size (indicates no compression or legacy data).
func (c *ChunkRef) GetOriginalSize() uint64 {
	if c.OriginalSize > 0 {
		return c.OriginalSize
	}
	return c.Size
}

// IsCompressed returns true if the chunk is compressed.
func (c *ChunkRef) IsCompressed() bool {
	return c.Compression != "" && c.Compression != "none"
}

// IsDeleted returns true if the object has been deleted
func (o *ObjectRef) IsDeleted() bool {
	return o.DeletedAt > 0
}

// IsErasureCoded returns true if the object uses EC storage
func (o *ObjectRef) IsErasureCoded() bool {
	return len(o.ECGroupIDs) > 0
}

// IsFederated returns true if the object has a federation reference.
// This means the object data exists (or existed) in an external S3 bucket.
func (o *ObjectRef) IsFederated() bool {
	return o.TransitionedRef != ""
}

// NeedsLazyMigration returns true if the object is federated but has no local chunks.
// This means the object data needs to be fetched from external S3 on read.
func (o *ObjectRef) NeedsLazyMigration() bool {
	return o.IsFederated() && len(o.ChunkRefs) == 0 && len(o.ECGroupIDs) == 0
}

// HasLocalData returns true if the object has local chunk data.
// For federated objects, this means the migration is complete for this object.
func (o *ObjectRef) HasLocalData() bool {
	return len(o.ChunkRefs) > 0 || len(o.ECGroupIDs) > 0
}
