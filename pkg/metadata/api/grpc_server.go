// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"
)

// gRPC service method implementations
// These are defined in the proto for potential future cluster coordination use cases.
// Currently, all S3 operations go through the HTTP handlers directly.
// These stubs exist only to satisfy the gRPC interface - they are not used in production.

// Ping is a placeholder for health checks. Not currently supported.
// Health checks should use the HTTP /health endpoint instead.
func (ms *MetadataServer) Ping(ctx context.Context, req *metadata_pb.PingRequest) (*metadata_pb.PingResponse, error) {
	return &metadata_pb.PingResponse{}, nil
}

// GetObject is a placeholder. Not currently supported.
// Use the S3 HTTP API (GET /{bucket}/{key}) instead.
func (ms *MetadataServer) GetObject(ctx context.Context, req *metadata_pb.GetObjectRequest) (*metadata_pb.GetObjectResponse, error) {
	return &metadata_pb.GetObjectResponse{}, nil
}

// DeleteObject is a placeholder. Not currently supported.
// Use the S3 HTTP API (DELETE /{bucket}/{key}) instead.
func (ms *MetadataServer) DeleteObject(ctx context.Context, req *metadata_pb.DeleteObjectRequest) (*metadata_pb.DeleteObjectResponse, error) {
	return &metadata_pb.DeleteObjectResponse{}, nil
}

// ListObjects is a placeholder. Not currently supported.
// Use the S3 HTTP API (GET /{bucket}?list-type=2) instead.
func (ms *MetadataServer) ListObjects(ctx context.Context, req *metadata_pb.ListObjectsRequest) (*metadata_pb.ListObjectsResponse, error) {
	return &metadata_pb.ListObjectsResponse{}, nil
}

// CreateCollection is a placeholder. Not currently supported.
// Use the S3 HTTP API (PUT /{bucket}) instead.
func (ms *MetadataServer) CreateCollection(ctx context.Context, req *metadata_pb.CreateCollectionRequest) (*metadata_pb.CreateCollectionResponse, error) {
	return &metadata_pb.CreateCollectionResponse{}, nil
}

// DeleteCollection is a placeholder. Not currently supported.
// Use the S3 HTTP API (DELETE /{bucket}) instead.
func (ms *MetadataServer) DeleteCollection(ctx context.Context, req *metadata_pb.DeleteCollectionRequest) (*metadata_pb.DeleteCollectionResponse, error) {
	return &metadata_pb.DeleteCollectionResponse{}, nil
}

// ListCollections is a placeholder. Not currently supported.
// Use the S3 HTTP API (GET /) instead.
func (ms *MetadataServer) ListCollections(ctx context.Context, req *metadata_pb.ListCollectionsRequest) (*metadata_pb.ListCollectionsResponse, error) {
	return &metadata_pb.ListCollectionsResponse{}, nil
}

// StreamChunksForServer streams all chunk IDs expected on a file server.
// Used by Manager to coordinate file server reconciliation.
func (ms *MetadataServer) StreamChunksForServer(req *metadata_pb.StreamChunksForServerRequest, stream metadata_pb.MetadataService_StreamChunksForServerServer) error {
	serverID := req.GetServerId()
	if serverID == "" {
		return fmt.Errorf("server_id is required")
	}

	logger.Info().Str("server_id", serverID).Msg("streaming chunks for server reconciliation")

	// Query chunk_replicas table for all chunks on this server
	chunkIDs, err := ms.db.GetChunksByServer(stream.Context(), serverID)
	if err != nil {
		logger.Error().Err(err).Str("server_id", serverID).Msg("failed to get chunks for server")
		return err
	}

	logger.Info().Str("server_id", serverID).Int("chunk_count", len(chunkIDs)).Msg("found chunks for server")

	// Stream chunk IDs to client
	for _, chunkID := range chunkIDs {
		if err := stream.Send(&metadata_pb.ChunkIDResponse{ChunkId: chunkID}); err != nil {
			logger.Error().Err(err).Str("server_id", serverID).Msg("failed to send chunk ID")
			return err
		}
	}

	logger.Info().Str("server_id", serverID).Int("chunks_sent", len(chunkIDs)).Msg("finished streaming chunks for server")
	return nil
}

// GetChunkReplicas returns the servers that have a copy of a specific chunk.
// Used by Manager for re-replication of missing chunks.
func (ms *MetadataServer) GetChunkReplicas(ctx context.Context, req *metadata_pb.GetChunkReplicasRequest) (*metadata_pb.GetChunkReplicasResponse, error) {
	chunkID := req.GetChunkId()
	if chunkID == "" {
		return nil, fmt.Errorf("chunk_id is required")
	}

	replicas, err := ms.db.GetChunkReplicas(ctx, chunkID)
	if err != nil {
		logger.Error().Err(err).Str("chunk_id", chunkID).Msg("failed to get chunk replicas")
		return nil, err
	}

	resp := &metadata_pb.GetChunkReplicasResponse{
		Replicas: make([]*metadata_pb.ChunkReplicaInfo, len(replicas)),
	}
	for i, r := range replicas {
		resp.Replicas[i] = &metadata_pb.ChunkReplicaInfo{
			ServerId:  r.ServerID,
			BackendId: r.BackendID,
		}
	}

	return resp, nil
}
