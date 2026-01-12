// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// collectionRecord holds bucket info discovered from metadata services
type collectionRecord struct {
	Name      string
	Owner     string
	CreatedAt time.Time
}

// metadataLister is an interface for querying collections (for testing)
type metadataLister interface {
	ListCollections(ctx context.Context, req *metadata_pb.ListCollectionsRequest, opts ...grpc.CallOption) (*metadata_pb.ListCollectionsResponse, error)
}

// queryMetadataForCollections queries a metadata service for all collections
func queryMetadataForCollections(ctx context.Context, client metadataLister) ([]collectionRecord, error) {
	var result []collectionRecord
	var continuationToken string

	for {
		resp, err := client.ListCollections(ctx, &metadata_pb.ListCollectionsRequest{
			MaxCollections:    1000,
			ContinuationToken: continuationToken,
		})
		if err != nil {
			return nil, fmt.Errorf("ListCollections: %w", err)
		}

		for _, col := range resp.GetCollections() {
			record := collectionRecord{
				Name: col.GetCollection(),
			}
			if col.GetCreatedAt() != nil {
				record.CreatedAt = col.GetCreatedAt().AsTime()
			}
			result = append(result, record)
		}

		continuationToken = resp.GetNextContinuationToken()
		if continuationToken == "" {
			break
		}
	}

	return result, nil
}

// mergeCollections combines collections from multiple sources, deduplicating by name
func mergeCollections(sources ...[]collectionRecord) map[string]collectionRecord {
	result := make(map[string]collectionRecord)
	for _, source := range sources {
		for _, col := range source {
			if existing, exists := result[col.Name]; !exists {
				result[col.Name] = col
			} else {
				// Keep the one with earlier creation time (if available)
				if !col.CreatedAt.IsZero() && (existing.CreatedAt.IsZero() || col.CreatedAt.Before(existing.CreatedAt)) {
					result[col.Name] = col
				}
			}
		}
	}
	return result
}

// RecoverCollections implements the disaster recovery RPC.
// It queries metadata services for their bucket lists and rebuilds the
// manager's collection registry through Raft.
func (ms *ManagerServer) RecoverCollections(ctx context.Context, req *manager_pb.RecoverCollectionsRequest) (*manager_pb.RecoverCollectionsResponse, error) {
	// Must be leader to apply Raft commands
	leaderAddr, err := forwardOrError(ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		return nil, fmt.Errorf("not the leader, connect to leader at %s", leaderAddr)
	}

	logger.Info().
		Strs("metadata_addresses", req.GetMetadataAddresses()).
		Bool("dry_run", req.GetDryRun()).
		Msg("starting collection recovery from metadata services")

	// Get metadata addresses - use provided or discover from registered services
	metadataAddrs := req.GetMetadataAddresses()
	if len(metadataAddrs) == 0 {
		ms.state.RLock()
		for _, svc := range ms.state.MetadataServices {
			if svc.Status == ServiceActive && svc.Location != nil {
				addr := fmt.Sprintf("%s:%d", svc.Location.GetAddress(), svc.Location.GetGrpcPort())
				metadataAddrs = append(metadataAddrs, addr)
			}
		}
		ms.state.RUnlock()
	}

	if len(metadataAddrs) == 0 {
		return &manager_pb.RecoverCollectionsResponse{
			Success: false,
			Message: "no metadata services available - provide --metadata-addrs or ensure services are registered",
		}, nil
	}

	// Query each metadata service
	var allCollections [][]collectionRecord
	for _, addr := range metadataAddrs {
		client, err := ms.metadataClientPool.Get(ctx, addr)
		if err != nil {
			logger.Warn().Err(err).Str("addr", addr).Msg("failed to connect to metadata service, skipping")
			continue
		}

		collections, err := queryMetadataForCollections(ctx, client)
		if err != nil {
			logger.Warn().Err(err).Str("addr", addr).Msg("failed to query collections, skipping")
			continue
		}

		logger.Info().Str("addr", addr).Int("count", len(collections)).Msg("queried metadata service")
		allCollections = append(allCollections, collections)
	}

	if len(allCollections) == 0 {
		return &manager_pb.RecoverCollectionsResponse{
			Success: false,
			Message: "failed to query any metadata service",
		}, nil
	}

	// Merge collections from all sources
	merged := mergeCollections(allCollections...)
	logger.Info().Int("total_unique", len(merged)).Msg("merged collections from all sources")

	// Check which collections need to be recovered
	var recovered, skipped, failed []string
	ms.state.RLock()
	existingCollections := make(map[string]bool, len(ms.state.Collections))
	for name := range ms.state.Collections {
		existingCollections[name] = true
	}
	ms.state.RUnlock()

	for name, col := range merged {
		if existingCollections[name] {
			skipped = append(skipped, name)
			continue
		}

		if req.GetDryRun() {
			recovered = append(recovered, name)
			continue
		}

		// Apply CreateCollection through Raft
		createdAt := col.CreatedAt
		if createdAt.IsZero() {
			createdAt = time.Now()
		}
		collection := &manager_pb.Collection{
			Name:      name,
			Owner:     col.Owner,
			CreatedAt: timestamppb.New(createdAt),
		}

		if err := ms.applyCommand(CommandCreateCollection, collection); err != nil {
			logger.Error().Err(err).Str("collection", name).Msg("failed to recover collection")
			failed = append(failed, name)
		} else {
			logger.Info().Str("collection", name).Msg("recovered collection")
			recovered = append(recovered, name)
		}
	}

	// If not dry run and we recovered something, take a snapshot
	if !req.GetDryRun() && len(recovered) > 0 {
		if err := ms.raftNode.Snapshot(); err != nil {
			logger.Warn().Err(err).Msg("failed to take snapshot after recovery")
		}

		// Clear recovery mode
		ms.ClearRecoveryMode()
	}

	message := fmt.Sprintf("recovery complete: %d recovered, %d skipped, %d failed",
		len(recovered), len(skipped), len(failed))
	if req.GetDryRun() {
		message = "[DRY RUN] " + message
	}

	logger.Info().
		Int("recovered", len(recovered)).
		Int("skipped", len(skipped)).
		Int("failed", len(failed)).
		Bool("dry_run", req.GetDryRun()).
		Msg("collection recovery finished")

	return &manager_pb.RecoverCollectionsResponse{
		Success:              len(failed) == 0,
		CollectionsRecovered: int32(len(recovered)),
		CollectionsSkipped:   int32(len(skipped)),
		CollectionsFailed:    int32(len(failed)),
		Message:              message,
		RecoveredNames:       recovered,
		SkippedNames:         skipped,
		FailedNames:          failed,
	}, nil
}
