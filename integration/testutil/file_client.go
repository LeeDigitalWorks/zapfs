//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"context"
	"io"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"

	"github.com/stretchr/testify/require"
)

// FileClient wraps file_pb.FileServiceClient with test helpers
type FileClient struct {
	file_pb.FileServiceClient
	t    *testing.T
	addr string
}

// NewFileClient creates a file service client with test helpers
func NewFileClient(t *testing.T, addr string) *FileClient {
	t.Helper()
	conn := NewGRPCConn(t, addr)
	return &FileClient{
		FileServiceClient: file_pb.NewFileServiceClient(conn),
		t:                 t,
		addr:              addr,
	}
}

// PutConfig configures a PutObject request
type PutConfig struct {
	BackendID string
	UseEC     bool
	ECScheme  *common_pb.ECScheme
}

// PutOption modifies PutConfig
type PutOption func(*PutConfig)

// WithErasureCoding enables erasure coding with the specified scheme
func WithErasureCoding(dataShards, parityShards int32) PutOption {
	return func(c *PutConfig) {
		c.UseEC = true
		c.ECScheme = &common_pb.ECScheme{
			DataShards:   dataShards,
			ParityShards: parityShards,
		}
	}
}

// PutObject uploads data to the file server
func (fc *FileClient) PutObject(objectID string, data []byte, opts ...PutOption) *file_pb.PutObjectResponse {
	fc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	config := &PutConfig{BackendID: "default"}
	for _, opt := range opts {
		opt(config)
	}

	stream, err := fc.FileServiceClient.PutObject(ctx)
	require.NoError(fc.t, err, "failed to open PutObject stream")

	// Send metadata
	meta := &file_pb.PutObjectRequest{
		Payload: &file_pb.PutObjectRequest_Meta{
			Meta: &file_pb.PutObjectMeta{
				ObjectId:           objectID,
				PreferredBackendId: config.BackendID,
				TotalSize:          uint64(len(data)),
				UseErasureCoding:   config.UseEC,
				EcScheme:           config.ECScheme,
			},
		},
	}
	err = stream.Send(meta)
	require.NoError(fc.t, err, "failed to send metadata")

	// Send data in chunks (64KB)
	chunkSize := 64 * 1024
	for offset := 0; offset < len(data); offset += chunkSize {
		end := offset + chunkSize
		if end > len(data) {
			end = len(data)
		}
		err = stream.Send(&file_pb.PutObjectRequest{
			Payload: &file_pb.PutObjectRequest_Chunk{
				Chunk: data[offset:end],
			},
		})
		require.NoError(fc.t, err, "failed to send chunk at offset %d", offset)
	}

	resp, err := stream.CloseAndRecv()
	require.NoError(fc.t, err, "failed to close stream and receive response")
	return resp
}

// GetObject retrieves data from the file server
func (fc *FileClient) GetObject(objectID string) []byte {
	fc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	stream, err := fc.FileServiceClient.GetObject(ctx, &file_pb.GetObjectRequest{
		ObjectId: objectID,
	})
	require.NoError(fc.t, err, "failed to open GetObject stream")

	var data []byte
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		require.NoError(fc.t, err, "failed to receive chunk")
		data = append(data, resp.Chunk...)
	}

	return data
}

// DeleteObject removes an object from the file server
func (fc *FileClient) DeleteObject(objectID string) *file_pb.DeleteObjectResponse {
	fc.t.Helper()

	ctx, cancel := WithShortTimeout(context.Background())
	defer cancel()

	resp, err := fc.FileServiceClient.DeleteObject(ctx, &file_pb.DeleteObjectRequest{
		ObjectId: objectID,
	})
	require.NoError(fc.t, err, "failed to delete object")
	return resp
}

// Ping pings the file server
func (fc *FileClient) Ping() *file_pb.PingResponse {
	fc.t.Helper()

	ctx, cancel := WithShortTimeout(context.Background())
	defer cancel()

	resp, err := fc.FileServiceClient.Ping(ctx, &file_pb.PingRequest{})
	require.NoError(fc.t, err, "failed to ping file server")
	return resp
}
