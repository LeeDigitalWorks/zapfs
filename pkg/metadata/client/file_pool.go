// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"fmt"
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/grpc/pool"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"google.golang.org/grpc"
)

// Verify FileClientPool implements File interface
var _ File = (*FileClientPool)(nil)

const (
	// DefaultChunkSize is the size of chunks sent over gRPC streams
	DefaultChunkSize = 64 * 1024 // 64KB
)

// FileClientPool manages gRPC connections to file servers.
// Uses the generic pool.Pool for connection management.
type FileClientPool struct {
	pool *pool.Pool[file_pb.FileServiceClient]
	opts pool.Options
}

// FileClientPoolConfig holds configuration for FileClientPool
type FileClientPoolConfig struct {
	// ConnsPerHost is the number of connections to maintain per file server
	ConnsPerHost int

	// DialOpts are additional gRPC dial options
	DialOpts []grpc.DialOption
}

// NewFileClientPool creates a new file client pool
func NewFileClientPool(cfg ...FileClientPoolConfig) *FileClientPool {
	opts := pool.DefaultOptions()

	if len(cfg) > 0 {
		if cfg[0].ConnsPerHost > 0 {
			opts.ConnsPerHost = cfg[0].ConnsPerHost
		}
		if len(cfg[0].DialOpts) > 0 {
			opts.DialOpts = append(opts.DialOpts, cfg[0].DialOpts...)
		}
	}

	return &FileClientPool{
		pool: pool.NewPool(fileClientFactory,
			pool.WithConnsPerHost(opts.ConnsPerHost),
			pool.WithDialOpts(opts.DialOpts...),
		),
		opts: opts,
	}
}

// fileClientFactory creates a file service client from a connection
func fileClientFactory(cc grpc.ClientConnInterface) file_pb.FileServiceClient {
	return file_pb.NewFileServiceClient(cc)
}

// GetClient returns a file service client for the given address.
// Creates connections lazily if they don't exist.
func (p *FileClientPool) GetClient(ctx context.Context, address string) (file_pb.FileServiceClient, error) {
	return p.pool.Get(ctx, address)
}

// PutObject streams data to a file server without compression.
// This is a convenience wrapper around PutObjectWithCompression with no compression.
func (p *FileClientPool) PutObject(
	ctx context.Context,
	address string,
	objectID string,
	data io.Reader,
	totalSize uint64,
) (*PutObjectResult, error) {
	return p.PutObjectWithCompression(ctx, address, objectID, data, totalSize, "")
}

// PutObjectWithCompression streams data to a file server with optional compression.
// The compression algorithm is passed to the file server which compresses chunks before storage.
// Supported algorithms: "none", "lz4", "zstd", "snappy", or empty string for no compression.
func (p *FileClientPool) PutObjectWithCompression(
	ctx context.Context,
	address string,
	objectID string,
	data io.Reader,
	totalSize uint64,
	compression string,
) (*PutObjectResult, error) {
	client, err := p.GetClient(ctx, address)
	if err != nil {
		return nil, err
	}

	// Open stream to file server
	stream, err := client.PutObject(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open PutObject stream: %w", err)
	}

	// Send metadata first (including compression algorithm)
	meta := &file_pb.PutObjectRequest{
		Payload: &file_pb.PutObjectRequest_Meta{
			Meta: &file_pb.PutObjectMeta{
				ObjectId:    objectID,
				TotalSize:   totalSize,
				Compression: compression,
			},
		},
	}
	if err := stream.Send(meta); err != nil {
		return nil, fmt.Errorf("failed to send metadata: %w", err)
	}

	// Stream data in chunks
	buf := make([]byte, DefaultChunkSize)
	for {
		n, err := data.Read(buf)
		if n > 0 {
			chunk := &file_pb.PutObjectRequest{
				Payload: &file_pb.PutObjectRequest_Chunk{
					Chunk: buf[:n],
				},
			}
			if sendErr := stream.Send(chunk); sendErr != nil {
				return nil, fmt.Errorf("failed to send chunk: %w", sendErr)
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read data: %w", err)
		}
	}

	// Close stream and get response
	resp, err := stream.CloseAndRecv()
	if err != nil {
		return nil, fmt.Errorf("failed to complete PutObject: %w", err)
	}

	// Extract chunk information from response (including compression metadata)
	chunks := make([]ChunkInfo, len(resp.Chunks))
	for i, c := range resp.Chunks {
		chunks[i] = ChunkInfo{
			ChunkID:      c.ChunkId,
			Size:         c.Size,
			Offset:       c.Offset,
			OriginalSize: c.OriginalSize,
			Compression:  c.Compression,
		}
	}

	return &PutObjectResult{
		ObjectID: resp.ObjectId,
		Size:     resp.Size,
		ETag:     resp.Etag,
		Chunks:   chunks,
	}, nil
}

// GetObject streams data from a file server to the provided writer.
func (p *FileClientPool) GetObject(
	ctx context.Context,
	address string,
	objectID string,
	writer ObjectWriter,
) (etag string, err error) {
	client, err := p.GetClient(ctx, address)
	if err != nil {
		return "", err
	}

	stream, err := client.GetObject(ctx, &file_pb.GetObjectRequest{
		ObjectId: objectID,
	})
	if err != nil {
		return "", fmt.Errorf("failed to open GetObject stream: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to receive chunk: %w", err)
		}

		if etag == "" && resp.Etag != "" {
			etag = resp.Etag
		}

		if err := writer(resp.Chunk); err != nil {
			return etag, fmt.Errorf("failed to write chunk: %w", err)
		}
	}

	return etag, nil
}

// GetObjectRange streams a range of data from a file server.
func (p *FileClientPool) GetObjectRange(
	ctx context.Context,
	address string,
	objectID string,
	offset, length uint64,
	writer ObjectWriter,
) (etag string, err error) {
	client, err := p.GetClient(ctx, address)
	if err != nil {
		return "", err
	}

	stream, err := client.GetObjectRange(ctx, &file_pb.GetObjectRangeRequest{
		ObjectId: objectID,
		Offset:   offset,
		Length:   length,
	})
	if err != nil {
		return "", fmt.Errorf("failed to open GetObjectRange stream: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("failed to receive chunk: %w", err)
		}

		if etag == "" && resp.Etag != "" {
			etag = resp.Etag
		}

		if err := writer(resp.Chunk); err != nil {
			return etag, fmt.Errorf("failed to write chunk: %w", err)
		}
	}

	return etag, nil
}

// GetChunk retrieves chunk data by its SHA-256 content hash.
// Uses the GetChunk RPC which is designed for direct chunk access.
func (p *FileClientPool) GetChunk(
	ctx context.Context,
	address string,
	chunkID string,
	writer ObjectWriter,
) error {
	client, err := p.GetClient(ctx, address)
	if err != nil {
		return err
	}

	stream, err := client.GetChunk(ctx, &file_pb.GetChunkRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return fmt.Errorf("failed to open GetChunk stream: %w", err)
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive chunk data: %w", err)
		}

		// Handle data payload (skip metadata)
		if data := resp.GetData(); data != nil {
			if err := writer(data); err != nil {
				return fmt.Errorf("failed to write chunk data: %w", err)
			}
		}
	}

	return nil
}

// GetChunkRange retrieves a range of bytes from a chunk by its SHA-256 content hash.
// Note: The GetChunk RPC doesn't support ranges directly, so we read the full chunk
// and skip bytes. For large chunks, this is inefficient but correct.
// TODO: Add range support to GetChunk RPC for efficiency.
func (p *FileClientPool) GetChunkRange(
	ctx context.Context,
	address string,
	chunkID string,
	offset, length uint64,
	writer ObjectWriter,
) error {
	client, err := p.GetClient(ctx, address)
	if err != nil {
		return err
	}

	stream, err := client.GetChunk(ctx, &file_pb.GetChunkRequest{
		ChunkId: chunkID,
	})
	if err != nil {
		return fmt.Errorf("failed to open GetChunk stream: %w", err)
	}

	var bytesSkipped, bytesWritten uint64
	remaining := length

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("failed to receive chunk data: %w", err)
		}

		data := resp.GetData()
		if data == nil {
			continue // Skip metadata messages
		}

		dataLen := uint64(len(data))

		// Skip bytes until we reach offset
		if bytesSkipped < offset {
			toSkip := offset - bytesSkipped
			if toSkip >= dataLen {
				bytesSkipped += dataLen
				continue
			}
			data = data[toSkip:]
			bytesSkipped = offset
			dataLen = uint64(len(data))
		}

		// Write up to remaining bytes
		if dataLen > remaining {
			data = data[:remaining]
		}

		if len(data) > 0 {
			if err := writer(data); err != nil {
				return fmt.Errorf("failed to write chunk data: %w", err)
			}
			bytesWritten += uint64(len(data))
			remaining -= uint64(len(data))
		}

		if remaining == 0 {
			break
		}
	}

	return nil
}

// DeleteObject deletes an object from a file server.
func (p *FileClientPool) DeleteObject(
	ctx context.Context,
	address string,
	objectID string,
) error {
	client, err := p.GetClient(ctx, address)
	if err != nil {
		return err
	}

	_, err = client.DeleteObject(ctx, &file_pb.DeleteObjectRequest{
		ObjectId: objectID,
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}

	return nil
}

// Close closes all client connections in the pool.
func (p *FileClientPool) Close() error {
	return p.pool.Close()
}

// Remove removes all connections for a specific file server address.
// Useful when a file server becomes unavailable.
func (p *FileClientPool) Remove(address string) {
	p.pool.Remove(address)
}
