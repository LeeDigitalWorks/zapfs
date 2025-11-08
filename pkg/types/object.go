package types

import "github.com/google/uuid"

// ObjectRef represents stored object metadata
// This is what the metadata service tracks for each object
type ObjectRef struct {
	ID        uuid.UUID `json:"id"`
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	Size      uint64    `json:"size"`
	Version   uint64    `json:"version"`
	ETag      string    `json:"etag"`
	CreatedAt int64     `json:"created_at"` // Unix timestamp
	DeletedAt int64     `json:"deleted_at,omitempty"`
	TTL       uint32    `json:"ttl,omitempty"` // Seconds until expiration

	// Storage profile used for this object (e.g., "STANDARD", "GLACIER")
	ProfileID string `json:"profile_id,omitempty"`

	// Storage location - one of these is set depending on storage mode
	ChunkRefs  []ChunkRef  `json:"chunk_refs,omitempty"`   // For simple/replicated storage
	ECGroupIDs []uuid.UUID `json:"ec_group_ids,omitempty"` // For erasure-coded storage

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
	ChunkID        ChunkID `json:"chunk_id"`
	Offset         uint64  `json:"offset"`                     // Offset within the object
	Size           uint64  `json:"size"`                       // Size of this chunk
	BackendID      string  `json:"backend_id"`                 // Which backend has this chunk
	FileServerAddr string  `json:"file_server_addr,omitempty"` // Address of file server hosting this chunk
}

// IsDeleted returns true if the object has been deleted
func (o *ObjectRef) IsDeleted() bool {
	return o.DeletedAt > 0
}

// IsErasureCoded returns true if the object uses EC storage
func (o *ObjectRef) IsErasureCoded() bool {
	return len(o.ECGroupIDs) > 0
}
