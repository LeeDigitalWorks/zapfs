// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// CreateBackup exports the current Raft FSM state as a JSON snapshot.
// Enterprise feature - requires FeatureBackup license.
func (ms *ManagerServer) CreateBackup(ctx context.Context, req *manager_pb.CreateBackupRequest) (*manager_pb.CreateBackupResponse, error) {
	if !license.CheckBackup() {
		return nil, status.Error(codes.PermissionDenied, "backup requires enterprise license with FeatureBackup")
	}

	logger.Info().
		Str("description", req.GetDescription()).
		Msg("Creating manager backup")

	// Get snapshot from FSM (reuses existing Raft snapshot logic)
	snapshot, err := ms.Snapshot()
	if err != nil {
		logger.Error().Err(err).Msg("Failed to create FSM snapshot for backup")
		return &manager_pb.CreateBackupResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	// Serialize to JSON
	fsmSnap, ok := snapshot.(*fsmSnapshot)
	if !ok {
		return &manager_pb.CreateBackupResponse{
			Success: false,
			Error:   "unexpected snapshot type",
		}, nil
	}

	data, err := json.MarshalIndent(fsmSnap, "", "  ")
	if err != nil {
		logger.Error().Err(err).Msg("Failed to serialize FSM snapshot")
		return &manager_pb.CreateBackupResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	backupID := fmt.Sprintf("backup-%d", time.Now().Unix())

	logger.Info().
		Str("backup_id", backupID).
		Int("file_services", len(fsmSnap.FileServices)).
		Int("metadata_services", len(fsmSnap.MetadataServices)).
		Int("collections", len(fsmSnap.Collections)).
		Uint64("topology_version", fsmSnap.TopologyVersion).
		Uint64("collections_version", fsmSnap.CollectionsVersion).
		Int("snapshot_size_bytes", len(data)).
		Msg("Manager backup created successfully")

	return &manager_pb.CreateBackupResponse{
		Success:      true,
		BackupId:     backupID,
		SnapshotData: data,
	}, nil
}

// GetBackupStatus returns current FSM state info.
// Useful for monitoring and backup verification.
func (ms *ManagerServer) GetBackupStatus(ctx context.Context, req *manager_pb.GetBackupStatusRequest) (*manager_pb.GetBackupStatusResponse, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	return &manager_pb.GetBackupStatusResponse{
		TopologyVersion:       ms.topologyVersion,
		CollectionsVersion:    ms.collectionsVersion,
		FileServicesCount:     int32(len(ms.fileServices)),
		MetadataServicesCount: int32(len(ms.metadataServices)),
		CollectionsCount:      int32(len(ms.collections)),
		IsLeader:              ms.raftNode.IsLeader(),
	}, nil
}
