package types

import "github.com/google/uuid"

// MultipartUpload represents an in-progress multipart upload
type MultipartUpload struct {
	ID        uuid.UUID `json:"id"`
	UploadID  string    `json:"upload_id"` // S3 upload ID (base64-encoded UUID)
	Bucket    string    `json:"bucket"`
	Key       string    `json:"key"`
	OwnerID   string    `json:"owner_id"`
	Initiated int64     `json:"initiated"` // Unix nano timestamp

	// Metadata from the initiate request
	ContentType  string            `json:"content_type,omitempty"`
	StorageClass string            `json:"storage_class,omitempty"`
	Metadata     map[string]string `json:"metadata,omitempty"`
}

// MultipartPart represents a single part of a multipart upload
type MultipartPart struct {
	ID           uuid.UUID `json:"id"`
	UploadID     string    `json:"upload_id"`
	PartNumber   int       `json:"part_number"`
	Size         int64     `json:"size"`
	ETag         string    `json:"etag"`
	LastModified int64     `json:"last_modified"` // Unix nano timestamp

	// Storage location (same as object chunks)
	ChunkRefs []ChunkRef `json:"chunk_refs,omitempty"`
}
