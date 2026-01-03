// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"fmt"
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"
)

// GetExpectedChunks streams chunk IDs expected on a file server.
// The file server calls this to get the list of chunks it should have.
// Manager queries a metadata service and streams the results.
func (ms *ManagerServer) GetExpectedChunks(req *manager_pb.GetExpectedChunksRequest, stream manager_pb.ManagerService_GetExpectedChunksServer) error {
	serverID := req.GetServerId()
	if serverID == "" {
		return fmt.Errorf("server_id is required")
	}

	logger.Info().Str("server_id", serverID).Msg("file server requesting expected chunks for reconciliation")

	// Get a metadata service to query
	metadataAddr, err := ms.getMetadataServiceAddress()
	if err != nil {
		logger.Error().Err(err).Msg("no metadata service available for reconciliation")
		return err
	}

	// Get metadata client from pool
	metadataClient, err := ms.metadataClientPool.Get(stream.Context(), metadataAddr)
	if err != nil {
		logger.Error().Err(err).Str("metadata_addr", metadataAddr).Msg("failed to connect to metadata service")
		return err
	}

	// Stream chunks from metadata service
	metadataReq := &metadata_pb.StreamChunksForServerRequest{
		ServerId: serverID,
	}
	chunkStream, err := metadataClient.StreamChunksForServer(stream.Context(), metadataReq)
	if err != nil {
		logger.Error().Err(err).Str("server_id", serverID).Msg("failed to start chunk stream from metadata")
		return err
	}

	// Forward chunks to file server
	count := 0
	for {
		chunk, err := chunkStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Error().Err(err).Str("server_id", serverID).Int("chunks_sent", count).Msg("error receiving chunk from metadata")
			return err
		}

		if err := stream.Send(&manager_pb.ExpectedChunkResponse{
			ChunkId: chunk.GetChunkId(),
		}); err != nil {
			logger.Error().Err(err).Str("server_id", serverID).Int("chunks_sent", count).Msg("error sending chunk to file server")
			return err
		}
		count++
	}

	logger.Info().Str("server_id", serverID).Int("chunks_sent", count).Msg("finished streaming expected chunks to file server")
	return nil
}

// ReportReconciliation receives reconciliation results from a file server.
func (ms *ManagerServer) ReportReconciliation(ctx context.Context, req *manager_pb.ReconciliationReport) (*manager_pb.ReconciliationAck, error) {
	serverID := req.GetServerId()

	logger.Info().
		Str("server_id", serverID).
		Int64("total_chunks", req.GetTotalChunks()).
		Int64("expected_chunks", req.GetExpectedChunks()).
		Int64("orphans_deleted", req.GetOrphansDeleted()).
		Int64("orphans_skipped", req.GetOrphansSkipped()).
		Int("missing_chunks", len(req.GetMissingChunks())).
		Int64("duration_ms", req.GetDurationMs()).
		Msg("received reconciliation report from file server")

	// Log missing chunks if any (these need re-replication)
	if len(req.GetMissingChunks()) > 0 {
		logger.Warn().
			Str("server_id", serverID).
			Int("missing_count", len(req.GetMissingChunks())).
			Strs("missing_chunks", req.GetMissingChunks()[:min(10, len(req.GetMissingChunks()))]).
			Msg("file server reported missing chunks (need re-replication)")

		// TODO: Trigger re-replication for missing chunks
		// This would query chunk_replicas for other servers that have the chunk
		// and initiate a copy to this server
	}

	return &manager_pb.ReconciliationAck{
		Success: true,
		Message: fmt.Sprintf("reconciliation report received: %d orphans deleted, %d missing chunks",
			req.GetOrphansDeleted(), len(req.GetMissingChunks())),
	}, nil
}

// TriggerReconciliation manually triggers reconciliation for file servers.
// If server_id is empty, triggers for all servers.
func (ms *ManagerServer) TriggerReconciliation(ctx context.Context, req *manager_pb.TriggerReconciliationRequest) (*manager_pb.TriggerReconciliationResponse, error) {
	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		// TODO: Forward to leader
		return &manager_pb.TriggerReconciliationResponse{
			Success: false,
			Message: "not leader, reconciliation must be triggered on leader",
		}, nil
	}

	serverID := req.GetServerId()
	var serversTriggered []string

	ms.state.RLock()
	if serverID == "" {
		// Trigger for all file servers
		for id := range ms.state.FileServices {
			serversTriggered = append(serversTriggered, id)
		}
	} else {
		// Trigger for specific server
		if _, exists := ms.state.FileServices[serverID]; exists {
			serversTriggered = append(serversTriggered, serverID)
		}
	}
	ms.state.RUnlock()

	if len(serversTriggered) == 0 {
		return &manager_pb.TriggerReconciliationResponse{
			Success: false,
			Message: "no matching file servers found",
		}, nil
	}

	logger.Info().
		Int("server_count", len(serversTriggered)).
		Strs("servers", serversTriggered).
		Msg("triggering reconciliation for file servers")

	// TODO: Mark servers as RECONCILING and notify them
	// For now, we just log the request. The actual notification would be done
	// via heartbeat response or a push mechanism.

	return &manager_pb.TriggerReconciliationResponse{
		Success:          true,
		Message:          fmt.Sprintf("reconciliation triggered for %d servers", len(serversTriggered)),
		ServersTriggered: serversTriggered,
	}, nil
}

// getMetadataServiceAddress returns the address of an available metadata service.
func (ms *ManagerServer) getMetadataServiceAddress() (string, error) {
	ms.state.RLock()
	defer ms.state.RUnlock()

	for _, svc := range ms.state.MetadataServices {
		if svc.Status == ServiceActive && svc.Location != nil {
			return fmt.Sprintf("%s:%d", svc.Location.GetAddress(), svc.Location.GetGrpcPort()), nil
		}
	}

	return "", fmt.Errorf("no metadata service available")
}
