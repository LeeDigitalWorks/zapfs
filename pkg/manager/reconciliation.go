// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"
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

		// Trigger re-replication asynchronously
		go ms.triggerReReplication(context.Background(), serverID, req.GetMissingChunks())
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
		logger.Debug().Str("leader", leaderAddr).Msg("Forwarding TriggerReconciliation to leader")
		return ms.leaderForwarder.ForwardTriggerReconciliation(ctx, leaderAddr, req)
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

// triggerReReplication initiates re-replication for missing chunks on a server.
// For each missing chunk, it finds another server that has it and triggers a migration.
func (ms *ManagerServer) triggerReReplication(ctx context.Context, targetServerID string, missingChunks []string) {
	if len(missingChunks) == 0 {
		return
	}

	logger.Info().
		Str("target_server", targetServerID).
		Int("chunk_count", len(missingChunks)).
		Msg("starting re-replication for missing chunks")

	// Get metadata service address
	metadataAddr, err := ms.getMetadataServiceAddress()
	if err != nil {
		logger.Error().Err(err).Msg("cannot re-replicate: no metadata service available")
		return
	}

	// Get metadata client
	metadataClient, err := ms.metadataClientPool.Get(ctx, metadataAddr)
	if err != nil {
		logger.Error().Err(err).Str("metadata_addr", metadataAddr).Msg("failed to connect to metadata service for re-replication")
		return
	}

	// Process chunks with limited concurrency
	const maxConcurrent = 4
	sem := make(chan struct{}, maxConcurrent)
	var wg sync.WaitGroup

	var successCount, failCount int64
	var mu sync.Mutex

	for _, chunkID := range missingChunks {
		chunkID := chunkID // capture for goroutine

		wg.Add(1)
		sem <- struct{}{}

		go func() {
			defer wg.Done()
			defer func() { <-sem }()

			// Find replicas for this chunk
			resp, err := metadataClient.GetChunkReplicas(ctx, &metadata_pb.GetChunkReplicasRequest{
				ChunkId: chunkID,
			})
			if err != nil {
				logger.Error().Err(err).Str("chunk_id", chunkID).Msg("failed to get chunk replicas")
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			if len(resp.GetReplicas()) == 0 {
				logger.Warn().Str("chunk_id", chunkID).Msg("no replicas found for chunk - data may be lost")
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			// Find a source server (not the target)
			var sourceServer string
			for _, replica := range resp.GetReplicas() {
				if replica.GetServerId() != targetServerID {
					sourceServer = replica.GetServerId()
					break
				}
			}

			if sourceServer == "" {
				logger.Warn().Str("chunk_id", chunkID).Msg("no alternative source server found for chunk")
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			// Trigger migration from source to target
			if err := ms.migrateChunk(ctx, chunkID, sourceServer, targetServerID); err != nil {
				logger.Error().Err(err).
					Str("chunk_id", chunkID).
					Str("source", sourceServer).
					Str("target", targetServerID).
					Msg("failed to migrate chunk for re-replication")
				mu.Lock()
				failCount++
				mu.Unlock()
				return
			}

			mu.Lock()
			successCount++
			mu.Unlock()
		}()
	}

	wg.Wait()

	logger.Info().
		Str("target_server", targetServerID).
		Int64("success", successCount).
		Int64("failed", failCount).
		Msg("re-replication completed")
}

// migrateChunk tells a source server to copy a chunk to a target server.
func (ms *ManagerServer) migrateChunk(ctx context.Context, chunkID, sourceServer, targetServer string) error {
	// Create a context with timeout for the migration
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	// Get file client for source server
	sourceClient, err := fileClientPool.Get(ctx, sourceServer)
	if err != nil {
		return fmt.Errorf("connect to source server %s: %w", sourceServer, err)
	}

	// Use MigrateChunk RPC - don't delete after migrate since we're adding a replica
	resp, err := sourceClient.MigrateChunk(ctx, &file_pb.MigrateChunkRequest{
		ChunkId:            chunkID,
		TargetServer:       targetServer,
		DeleteAfterMigrate: false, // Keep source copy
	})
	if err != nil {
		return fmt.Errorf("migrate chunk: %w", err)
	}

	if !resp.GetSuccess() {
		return fmt.Errorf("migration failed: %s", resp.GetError())
	}

	logger.Debug().
		Str("chunk_id", chunkID).
		Str("source", sourceServer).
		Str("target", targetServer).
		Int64("bytes", resp.GetBytesTransferred()).
		Int64("duration_ms", resp.GetDurationMs()).
		Msg("chunk re-replicated successfully")

	return nil
}
