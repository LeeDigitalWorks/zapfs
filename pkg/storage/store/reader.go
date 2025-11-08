package store

import (
	"context"
	"fmt"
	"io"

	"github.com/google/uuid"
)

// GetObjectData retrieves the full object data
func (fs *FileStore) GetObjectData(ctx context.Context, id uuid.UUID) (io.ReadCloser, error) {
	obj, err := fs.objectIdx.Get(id)
	if err != nil {
		return nil, err
	}

	if obj.IsDeleted() {
		return nil, fmt.Errorf("object deleted")
	}

	// For EC objects, use EC manager
	if obj.IsErasureCoded() {
		// Combine all EC groups
		var readers []io.Reader
		for _, groupID := range obj.ECGroupIDs {
			data, err := fs.ecManager.GetChunk(ctx, groupID)
			if err != nil {
				return nil, fmt.Errorf("read ec group %s: %w", groupID, err)
			}
			readers = append(readers, io.NopCloser(io.NewSectionReader(
				&bytesReaderAt{data}, 0, int64(len(data)),
			)))
		}
		return &multiReadCloser{readers: readers}, nil
	}

	// For regular chunks
	readers := make([]io.Reader, 0, len(obj.ChunkRefs))
	for _, ref := range obj.ChunkRefs {
		rc, err := fs.GetChunk(ctx, ref.ChunkID)
		if err != nil {
			return nil, fmt.Errorf("read chunk %s: %w", ref.ChunkID, err)
		}
		readers = append(readers, rc)
	}

	return &multiReadCloser{readers: readers}, nil
}

// GetObjectRange retrieves a range of object data
func (fs *FileStore) GetObjectRange(ctx context.Context, id uuid.UUID, offset, length int64) (io.ReadCloser, error) {
	obj, err := fs.objectIdx.Get(id)
	if err != nil {
		return nil, err
	}

	if obj.IsDeleted() {
		return nil, fmt.Errorf("object deleted")
	}

	// Simple implementation for regular chunks
	var readers []io.Reader
	var currentOffset int64
	remaining := length

	for _, ref := range obj.ChunkRefs {
		chunkSize := int64(ref.Size)

		// Skip chunks before the range
		if currentOffset+chunkSize <= offset {
			currentOffset += chunkSize
			continue
		}

		// Calculate read range within this chunk
		start := int64(0)
		if offset > currentOffset {
			start = offset - currentOffset
		}

		end := chunkSize
		if remaining < chunkSize-start {
			end = start + remaining
		}

		rc, err := fs.GetChunkRange(ctx, ref.ChunkID, start, end-start)
		if err != nil {
			return nil, err
		}
		readers = append(readers, rc)

		remaining -= (end - start)
		currentOffset += chunkSize

		if remaining <= 0 {
			break
		}
	}

	return &multiReadCloser{readers: readers}, nil
}

// bytesReaderAt wraps []byte for io.ReaderAt
type bytesReaderAt struct {
	data []byte
}

func (r *bytesReaderAt) ReadAt(p []byte, off int64) (n int, err error) {
	if off >= int64(len(r.data)) {
		return 0, io.EOF
	}
	n = copy(p, r.data[off:])
	if n < len(p) {
		err = io.EOF
	}
	return
}

// multiReadCloser combines multiple readers into one ReadCloser
type multiReadCloser struct {
	readers []io.Reader
	idx     int
}

func (m *multiReadCloser) Read(p []byte) (n int, err error) {
	for m.idx < len(m.readers) {
		n, err = m.readers[m.idx].Read(p)
		if err == io.EOF {
			m.idx++
			if n > 0 {
				return n, nil
			}
			continue
		}
		return n, err
	}
	return 0, io.EOF
}

func (m *multiReadCloser) Close() error {
	for _, r := range m.readers {
		if rc, ok := r.(io.Closer); ok {
			rc.Close()
		}
	}
	return nil
}
