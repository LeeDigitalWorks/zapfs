package store

import (
	"context"
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
)

// Store handles chunk and object storage
type Store interface {
	io.Closer

	// Object operations
	GetObject(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error)
	PutObject(ctx context.Context, obj *types.ObjectRef, reader io.Reader) error
	DeleteObject(ctx context.Context, id uuid.UUID) error

	// Chunk operations
	GetChunk(ctx context.Context, id types.ChunkID) (io.ReadCloser, error)
	GetChunkRange(ctx context.Context, id types.ChunkID, offset, length int64) (io.ReadCloser, error)
}
