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
