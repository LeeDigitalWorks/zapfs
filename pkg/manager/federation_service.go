// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// FederationService implements the gRPC FederationService for bucket federation and migration control.
// All write operations are forwarded to the Raft leader.
type FederationService struct {
	manager_pb.UnimplementedFederationServiceServer
	ms            *ManagerServer
	extS3Pool     *ExternalS3ClientPool
	federationCfg *FederationConfig
}

// FederationConfig holds federation configuration for the manager.
type FederationConfig struct {
	Enabled              bool
	ExternalTimeout      time.Duration
	ExternalMaxIdleConns int
}

// NewFederationService creates a new FederationService.
func NewFederationService(ms *ManagerServer, cfg *FederationConfig) *FederationService {
	var pool *ExternalS3ClientPool
	if cfg != nil && cfg.Enabled {
		pool = NewExternalS3ClientPool(cfg.ExternalTimeout, cfg.ExternalMaxIdleConns)
	}

	return &FederationService{
		ms:            ms,
		extS3Pool:     pool,
		federationCfg: cfg,
	}
}

// checkFederationEnabled returns an error if federation is not enabled.
func (s *FederationService) checkFederationEnabled() error {
	if s.federationCfg == nil || !s.federationCfg.Enabled {
		return status.Error(codes.FailedPrecondition, "federation is not enabled")
	}
	return nil
}

// RegisterBucket registers an existing external S3 bucket for federation.
func (s *FederationService) RegisterBucket(ctx context.Context, req *manager_pb.FederationRegisterBucketRequest) (*manager_pb.FederationRegisterBucketResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(s.ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Str("bucket", req.LocalBucket).Msg("Forwarding RegisterBucket to leader")
		return s.ms.leaderForwarder.ForwardFederationRegisterBucket(ctx, leaderAddr, req)
	}

	// Validate request
	if req.LocalBucket == "" {
		return nil, status.Error(codes.InvalidArgument, "local_bucket is required")
	}
	if req.External == nil {
		return nil, status.Error(codes.InvalidArgument, "external S3 config is required")
	}
	if req.Mode == manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_UNSPECIFIED ||
		req.Mode == manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_LOCAL {
		return nil, status.Error(codes.InvalidArgument, "mode must be PASSTHROUGH or MIGRATING")
	}

	// Verify external bucket exists and credentials are valid
	extCfg := &ExternalS3Config{
		Endpoint:        req.External.Endpoint,
		Region:          req.External.Region,
		AccessKeyID:     req.External.AccessKeyId,
		SecretAccessKey: req.External.SecretAccessKey,
		PathStyle:       req.External.PathStyle,
	}

	if err := s.extS3Pool.HeadBucket(ctx, extCfg, req.External.Bucket); err != nil {
		logger.Warn().Err(err).
			Str("bucket", req.External.Bucket).
			Str("endpoint", req.External.Endpoint).
			Msg("Failed to verify external bucket")
		return &manager_pb.FederationRegisterBucketResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to verify external bucket: %v", err),
		}, nil
	}

	// Count objects in external bucket for migration tracking
	var objectCount int64
	if req.Mode == manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_MIGRATING && req.StartActiveMigration {
		count, err := s.extS3Pool.CountObjects(ctx, extCfg, req.External.Bucket)
		if err != nil {
			logger.Warn().Err(err).Str("bucket", req.External.Bucket).Msg("Failed to count external objects")
			// Non-fatal - continue with 0 count
		} else {
			objectCount = count
		}
	}

	// Check if local bucket exists
	s.ms.state.RLock()
	existingCol, exists := s.ms.state.Collections[req.LocalBucket]
	s.ms.state.RUnlock()

	if exists {
		// Bucket exists - check if already federated
		if existingCol.BucketMode != manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_LOCAL &&
			existingCol.BucketMode != manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_UNSPECIFIED {
			return &manager_pb.FederationRegisterBucketResponse{
				Success: false,
				Error:   "bucket is already federated",
			}, nil
		}
	}

	// Create federation registration command
	fedReg := &FederationRegistration{
		LocalBucket:          req.LocalBucket,
		Mode:                 req.Mode,
		External:             req.External,
		ObjectsDiscovered:    objectCount,
		StartActiveMigration: req.StartActiveMigration,
		RegisteredAt:         time.Now().UnixNano(),
	}

	// Apply through Raft
	if err := s.ms.applyCommand(CommandRegisterFederation, fedReg); err != nil {
		return &manager_pb.FederationRegisterBucketResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	logger.Info().
		Str("local_bucket", req.LocalBucket).
		Str("external_bucket", req.External.Bucket).
		Str("mode", req.Mode.String()).
		Int64("object_count", objectCount).
		Msg("Registered federated bucket")

	return &manager_pb.FederationRegisterBucketResponse{
		Success:             true,
		ExternalObjectCount: objectCount,
	}, nil
}

// UnregisterBucket removes federation configuration from a bucket.
func (s *FederationService) UnregisterBucket(ctx context.Context, req *manager_pb.FederationUnregisterBucketRequest) (*manager_pb.FederationUnregisterBucketResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(s.ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Str("bucket", req.Bucket).Msg("Forwarding UnregisterBucket to leader")
		return s.ms.leaderForwarder.ForwardFederationUnregisterBucket(ctx, leaderAddr, req)
	}

	if req.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}

	// Check if bucket exists and is federated
	s.ms.state.RLock()
	col, exists := s.ms.state.Collections[req.Bucket]
	s.ms.state.RUnlock()

	if !exists {
		return nil, status.Errorf(codes.NotFound, "bucket %s not found", req.Bucket)
	}
	if col.BucketMode == manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_LOCAL ||
		col.BucketMode == manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_UNSPECIFIED {
		return nil, status.Errorf(codes.FailedPrecondition, "bucket %s is not federated", req.Bucket)
	}

	// Apply through Raft
	unreg := &FederationUnregistration{
		Bucket:          req.Bucket,
		DeleteExternal:  req.DeleteExternal,
		DeleteLocalData: req.DeleteLocalData,
		UnregisteredAt:  time.Now().UnixNano(),
	}

	if err := s.ms.applyCommand(CommandUnregisterFederation, unreg); err != nil {
		return &manager_pb.FederationUnregisterBucketResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	logger.Info().
		Str("bucket", req.Bucket).
		Bool("delete_external", req.DeleteExternal).
		Bool("delete_local_data", req.DeleteLocalData).
		Msg("Unregistered federated bucket")

	return &manager_pb.FederationUnregisterBucketResponse{
		Success: true,
	}, nil
}

// GetBucketStatus returns the federation status for a bucket.
func (s *FederationService) GetBucketStatus(ctx context.Context, req *manager_pb.FederationGetBucketStatusRequest) (*manager_pb.FederationGetBucketStatusResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	if req.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}

	// Read from local state (read ops don't need leader)
	s.ms.state.RLock()
	col, exists := s.ms.state.Collections[req.Bucket]
	fedInfo, fedExists := s.ms.state.FederationConfigs[req.Bucket]
	s.ms.state.RUnlock()

	if !exists {
		return nil, status.Errorf(codes.NotFound, "bucket %s not found", req.Bucket)
	}

	resp := &manager_pb.FederationGetBucketStatusResponse{
		Bucket: req.Bucket,
		Mode:   col.BucketMode,
	}

	if fedExists {
		resp.DualWriteEnabled = fedInfo.DualWriteEnabled
		resp.MigrationPaused = fedInfo.MigrationPaused
		resp.ObjectsDiscovered = fedInfo.ObjectsDiscovered
		resp.ObjectsSynced = fedInfo.ObjectsSynced
		resp.BytesSynced = fedInfo.BytesSynced
		resp.CreatedAt = fedInfo.CreatedAt
		resp.UpdatedAt = fedInfo.UpdatedAt

		if fedInfo.ObjectsDiscovered > 0 {
			resp.ProgressPercent = float64(fedInfo.ObjectsSynced) / float64(fedInfo.ObjectsDiscovered) * 100
		}

		// Return external config (with credentials redacted)
		resp.External = &manager_pb.ExternalS3Config{
			Endpoint:        fedInfo.External.Endpoint,
			Region:          fedInfo.External.Region,
			AccessKeyId:     "***REDACTED***",
			SecretAccessKey: "***REDACTED***",
			Bucket:          fedInfo.External.Bucket,
			PathStyle:       fedInfo.External.PathStyle,
		}
	}

	return resp, nil
}

// SetBucketMode changes the bucket mode (e.g., passthrough → migrating → local).
func (s *FederationService) SetBucketMode(ctx context.Context, req *manager_pb.FederationSetBucketModeRequest) (*manager_pb.FederationSetBucketModeResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(s.ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Str("bucket", req.Bucket).Msg("Forwarding SetBucketMode to leader")
		return s.ms.leaderForwarder.ForwardFederationSetBucketMode(ctx, leaderAddr, req)
	}

	if req.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}

	// Apply through Raft
	modeChange := &FederationModeChange{
		Bucket:                   req.Bucket,
		NewMode:                  req.Mode,
		DeleteExternalOnComplete: req.DeleteExternalOnComplete,
		ChangedAt:                time.Now().UnixNano(),
	}

	if err := s.ms.applyCommand(CommandSetFederationMode, modeChange); err != nil {
		return &manager_pb.FederationSetBucketModeResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	logger.Info().
		Str("bucket", req.Bucket).
		Str("new_mode", req.Mode.String()).
		Msg("Changed bucket federation mode")

	return &manager_pb.FederationSetBucketModeResponse{
		Success: true,
	}, nil
}

// PauseMigration pauses an active migration.
func (s *FederationService) PauseMigration(ctx context.Context, req *manager_pb.FederationPauseMigrationRequest) (*manager_pb.FederationPauseMigrationResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(s.ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		return s.ms.leaderForwarder.ForwardFederationPauseMigration(ctx, leaderAddr, req)
	}

	if req.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}

	pauseCmd := &FederationPauseResume{
		Bucket:    req.Bucket,
		Paused:    true,
		ChangedAt: time.Now().UnixNano(),
	}

	if err := s.ms.applyCommand(CommandPauseFederation, pauseCmd); err != nil {
		return &manager_pb.FederationPauseMigrationResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	logger.Info().Str("bucket", req.Bucket).Msg("Paused federation migration")

	return &manager_pb.FederationPauseMigrationResponse{
		Success: true,
	}, nil
}

// ResumeMigration resumes a paused migration.
func (s *FederationService) ResumeMigration(ctx context.Context, req *manager_pb.FederationResumeMigrationRequest) (*manager_pb.FederationResumeMigrationResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(s.ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		return s.ms.leaderForwarder.ForwardFederationResumeMigration(ctx, leaderAddr, req)
	}

	if req.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}

	resumeCmd := &FederationPauseResume{
		Bucket:    req.Bucket,
		Paused:    false,
		ChangedAt: time.Now().UnixNano(),
	}

	if err := s.ms.applyCommand(CommandResumeFederation, resumeCmd); err != nil {
		return &manager_pb.FederationResumeMigrationResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	logger.Info().Str("bucket", req.Bucket).Msg("Resumed federation migration")

	return &manager_pb.FederationResumeMigrationResponse{
		Success: true,
	}, nil
}

// SetDualWrite enables or disables dual-write for a bucket in migrating mode.
func (s *FederationService) SetDualWrite(ctx context.Context, req *manager_pb.FederationSetDualWriteRequest) (*manager_pb.FederationSetDualWriteResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(s.ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		return s.ms.leaderForwarder.ForwardFederationSetDualWrite(ctx, leaderAddr, req)
	}

	if req.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}

	dualWriteCmd := &FederationDualWrite{
		Bucket:    req.Bucket,
		Enabled:   req.Enabled,
		ChangedAt: time.Now().UnixNano(),
	}

	if err := s.ms.applyCommand(CommandSetFederationDualWrite, dualWriteCmd); err != nil {
		return &manager_pb.FederationSetDualWriteResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	logger.Info().Str("bucket", req.Bucket).Bool("enabled", req.Enabled).Msg("Set dual-write mode")

	return &manager_pb.FederationSetDualWriteResponse{
		Success: true,
	}, nil
}

// UpdateCredentials updates the external S3 credentials for a federated bucket.
func (s *FederationService) UpdateCredentials(ctx context.Context, req *manager_pb.FederationUpdateCredentialsRequest) (*manager_pb.FederationUpdateCredentialsResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(s.ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		return s.ms.leaderForwarder.ForwardFederationUpdateCredentials(ctx, leaderAddr, req)
	}

	if req.Bucket == "" {
		return nil, status.Error(codes.InvalidArgument, "bucket is required")
	}
	if req.AccessKeyId == "" || req.SecretAccessKey == "" {
		return nil, status.Error(codes.InvalidArgument, "credentials are required")
	}

	// Verify new credentials work
	s.ms.state.RLock()
	fedInfo, exists := s.ms.state.FederationConfigs[req.Bucket]
	s.ms.state.RUnlock()

	if !exists {
		return nil, status.Errorf(codes.NotFound, "bucket %s is not federated", req.Bucket)
	}

	// Test new credentials
	extCfg := &ExternalS3Config{
		Endpoint:        fedInfo.External.Endpoint,
		Region:          fedInfo.External.Region,
		AccessKeyID:     req.AccessKeyId,
		SecretAccessKey: req.SecretAccessKey,
		PathStyle:       fedInfo.External.PathStyle,
	}

	if err := s.extS3Pool.HeadBucket(ctx, extCfg, fedInfo.External.Bucket); err != nil {
		return &manager_pb.FederationUpdateCredentialsResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to verify new credentials: %v", err),
		}, nil
	}

	// Apply through Raft
	credUpdate := &FederationCredentialsUpdate{
		Bucket:          req.Bucket,
		AccessKeyID:     req.AccessKeyId,
		SecretAccessKey: req.SecretAccessKey,
		UpdatedAt:       time.Now().UnixNano(),
	}

	if err := s.ms.applyCommand(CommandUpdateFederationCredentials, credUpdate); err != nil {
		return &manager_pb.FederationUpdateCredentialsResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	logger.Info().Str("bucket", req.Bucket).Msg("Updated federation credentials")

	return &manager_pb.FederationUpdateCredentialsResponse{
		Success: true,
	}, nil
}

// ListFederatedBuckets returns all federated buckets.
func (s *FederationService) ListFederatedBuckets(ctx context.Context, req *manager_pb.FederationListBucketsRequest) (*manager_pb.FederationListBucketsResponse, error) {
	if err := s.checkFederationEnabled(); err != nil {
		return nil, err
	}

	s.ms.state.RLock()
	defer s.ms.state.RUnlock()

	var buckets []*manager_pb.FederationGetBucketStatusResponse

	for name, col := range s.ms.state.Collections {
		if col.BucketMode == manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_PASSTHROUGH ||
			col.BucketMode == manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_MIGRATING {

			resp := &manager_pb.FederationGetBucketStatusResponse{
				Bucket: name,
				Mode:   col.BucketMode,
			}

			if fedInfo, exists := s.ms.state.FederationConfigs[name]; exists {
				resp.DualWriteEnabled = fedInfo.DualWriteEnabled
				resp.MigrationPaused = fedInfo.MigrationPaused
				resp.ObjectsDiscovered = fedInfo.ObjectsDiscovered
				resp.ObjectsSynced = fedInfo.ObjectsSynced
				resp.BytesSynced = fedInfo.BytesSynced
				resp.CreatedAt = fedInfo.CreatedAt
				resp.UpdatedAt = fedInfo.UpdatedAt

				if fedInfo.ObjectsDiscovered > 0 {
					resp.ProgressPercent = float64(fedInfo.ObjectsSynced) / float64(fedInfo.ObjectsDiscovered) * 100
				}

				// Redact credentials
				resp.External = &manager_pb.ExternalS3Config{
					Endpoint:        fedInfo.External.Endpoint,
					Region:          fedInfo.External.Region,
					AccessKeyId:     "***REDACTED***",
					SecretAccessKey: "***REDACTED***",
					Bucket:          fedInfo.External.Bucket,
					PathStyle:       fedInfo.External.PathStyle,
				}
			}

			buckets = append(buckets, resp)
		}
	}

	return &manager_pb.FederationListBucketsResponse{
		Buckets: buckets,
	}, nil
}

// ===== Raft Command Types for Federation =====

// FederationRegistration is the Raft command for registering a federated bucket.
type FederationRegistration struct {
	LocalBucket          string                          `json:"local_bucket"`
	Mode                 manager_pb.FederationBucketMode `json:"mode"`
	External             *manager_pb.ExternalS3Config    `json:"external"`
	ObjectsDiscovered    int64                           `json:"objects_discovered"`
	StartActiveMigration bool                            `json:"start_active_migration"`
	RegisteredAt         int64                           `json:"registered_at"`
}

// FederationUnregistration is the Raft command for unregistering a federated bucket.
type FederationUnregistration struct {
	Bucket          string `json:"bucket"`
	DeleteExternal  bool   `json:"delete_external"`
	DeleteLocalData bool   `json:"delete_local_data"`
	UnregisteredAt  int64  `json:"unregistered_at"`
}

// FederationModeChange is the Raft command for changing bucket mode.
type FederationModeChange struct {
	Bucket                   string                          `json:"bucket"`
	NewMode                  manager_pb.FederationBucketMode `json:"new_mode"`
	DeleteExternalOnComplete bool                            `json:"delete_external_on_complete"`
	ChangedAt                int64                           `json:"changed_at"`
}

// FederationPauseResume is the Raft command for pausing/resuming migration.
type FederationPauseResume struct {
	Bucket    string `json:"bucket"`
	Paused    bool   `json:"paused"`
	ChangedAt int64  `json:"changed_at"`
}

// FederationDualWrite is the Raft command for enabling/disabling dual-write.
type FederationDualWrite struct {
	Bucket    string `json:"bucket"`
	Enabled   bool   `json:"enabled"`
	ChangedAt int64  `json:"changed_at"`
}

// FederationCredentialsUpdate is the Raft command for updating credentials.
type FederationCredentialsUpdate struct {
	Bucket          string `json:"bucket"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	UpdatedAt       int64  `json:"updated_at"`
}

// FederationInfo stores federation configuration in Raft state.
type FederationInfo struct {
	External          *manager_pb.ExternalS3Config `json:"external"`
	ObjectsDiscovered int64                        `json:"objects_discovered"`
	ObjectsSynced     int64                        `json:"objects_synced"`
	BytesSynced       int64                        `json:"bytes_synced"`
	MigrationPaused   bool                         `json:"migration_paused"`
	DualWriteEnabled  bool                         `json:"dual_write_enabled"`
	CreatedAt         int64                        `json:"created_at"`
	UpdatedAt         int64                        `json:"updated_at"`
}

// ===== Raft Command Constants for Federation =====

const (
	CommandRegisterFederation          CommandType = "register_federation"
	CommandUnregisterFederation        CommandType = "unregister_federation"
	CommandSetFederationMode           CommandType = "set_federation_mode"
	CommandPauseFederation             CommandType = "pause_federation"
	CommandResumeFederation            CommandType = "resume_federation"
	CommandSetFederationDualWrite      CommandType = "set_federation_dual_write"
	CommandUpdateFederationCredentials CommandType = "update_federation_credentials"
)
