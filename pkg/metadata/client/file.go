package client

import (
	"context"
	"io"
)

// ObjectWriter is a callback that receives chunks of data from GetObject
type ObjectWriter func(chunk []byte) error

// PutObjectResult contains the result of a PutObject operation
type PutObjectResult struct {
	ObjectID string
	Size     uint64
	ETag     string
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
	// PutObject streams data to a file server
	PutObject(ctx context.Context, address string, objectID string, data io.Reader, totalSize uint64) (*PutObjectResult, error)

	// GetObject retrieves an object from a file server
	GetObject(ctx context.Context, address string, objectID string, writer ObjectWriter) (etag string, err error)

	// GetObjectRange retrieves a range of bytes from an object
	GetObjectRange(ctx context.Context, address string, objectID string, offset, length uint64, writer ObjectWriter) (etag string, err error)

	// DecrementRefCount decrements a chunk's reference count on a file server
	DecrementRefCount(ctx context.Context, address string, chunkID string, expectedRefCount uint32) (*DecrementRefCountResult, error)

	// DecrementRefCountBatch decrements multiple chunks' reference counts
	DecrementRefCountBatch(ctx context.Context, address string, chunks []DecrementRefCountRequest) ([]*DecrementRefCountResult, error)

	// Close closes all connections
	Close() error
}

// DecrementRefCountRequest is a request to decrement a single chunk's ref count
type DecrementRefCountRequest struct {
	ChunkID          string
	ExpectedRefCount uint32
}
