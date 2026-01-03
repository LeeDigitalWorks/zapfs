// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"context"
	"io"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/storage/store"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// ReconciliationConfig holds configuration for the reconciliation service
type ReconciliationConfig struct {
	// ServerID is the unique identifier for this file server (e.g., "file-1:8001")
	ServerID string

	// GracePeriod is how long to wait before deleting orphan chunks
	// This protects chunks from in-flight uploads that haven't been registered yet
	GracePeriod time.Duration

	// Interval is how often to run periodic reconciliation (0 = disabled)
	Interval time.Duration

	// DryRun if true, only logs what would be deleted without actually deleting
	DryRun bool
}

// ReconciliationService handles chunk reconciliation with the manager
type ReconciliationService struct {
	config        ReconciliationConfig
	managerClient *client.ManagerClientPool
	store         *store.FileStore

	// State
	mu          sync.Mutex
	isRunning   bool
	lastRunTime time.Time
	lastReport  *ReconciliationReport

	// Lifecycle
	stopCh chan struct{}
	doneCh chan struct{}
}

// ReconciliationReport contains the results of a reconciliation run
type ReconciliationReport struct {
	ServerID       string
	TotalChunks    int64
	ExpectedChunks int64
	OrphansDeleted int64
	OrphansSkipped int64
	MissingChunks  []string
	Duration       time.Duration
	Error          error
}

// NewReconciliationService creates a new reconciliation service
func NewReconciliationService(config ReconciliationConfig, managerClient *client.ManagerClientPool, fileStore *store.FileStore) *ReconciliationService {
	return &ReconciliationService{
		config:        config,
		managerClient: managerClient,
		store:         fileStore,
		stopCh:        make(chan struct{}),
		doneCh:        make(chan struct{}),
	}
}

// Start begins periodic reconciliation (if interval > 0)
func (rs *ReconciliationService) Start(ctx context.Context) {
	if rs.config.Interval <= 0 {
		logger.Info().Msg("Periodic reconciliation disabled (interval=0)")
		close(rs.doneCh)
		return
	}

	go rs.runPeriodic(ctx)
}

// Stop halts periodic reconciliation
func (rs *ReconciliationService) Stop() {
	close(rs.stopCh)
	<-rs.doneCh
}

// RunOnce performs a single reconciliation run
func (rs *ReconciliationService) RunOnce(ctx context.Context) *ReconciliationReport {
	rs.mu.Lock()
	if rs.isRunning {
		rs.mu.Unlock()
		logger.Warn().Msg("Reconciliation already in progress, skipping")
		return nil
	}
	rs.isRunning = true
	rs.mu.Unlock()

	defer func() {
		rs.mu.Lock()
		rs.isRunning = false
		rs.mu.Unlock()
	}()

	start := time.Now()
	report := &ReconciliationReport{
		ServerID: rs.config.ServerID,
	}

	logger.Debug().
		Str("server_id", rs.config.ServerID).
		Dur("grace_period", rs.config.GracePeriod).
		Bool("dry_run", rs.config.DryRun).
		Msg("Starting chunk reconciliation")

	// Step 1: Get expected chunks from manager
	expectedChunks, err := rs.getExpectedChunks(ctx)
	if err != nil {
		report.Error = err
		report.Duration = time.Since(start)
		logger.Error().Err(err).Msg("Failed to get expected chunks from manager")
		return report
	}
	report.ExpectedChunks = int64(len(expectedChunks))

	logger.Debug().
		Int("expected_chunks", len(expectedChunks)).
		Msg("Received expected chunks from manager")

	// Step 2: Iterate local chunks and compare
	var totalChunks int64
	var orphansDeleted int64
	var orphansSkipped int64
	var missingChunks []string

	graceCutoff := time.Now().Add(-rs.config.GracePeriod).Unix()

	// Track which expected chunks we found locally
	foundChunks := make(map[string]bool)

	err = rs.store.IterateChunks(func(id types.ChunkID, chunk types.Chunk) error {
		totalChunks++
		chunkIDStr := string(id)

		if expectedChunks[chunkIDStr] {
			// Chunk is expected, mark as found
			foundChunks[chunkIDStr] = true
			return nil
		}

		// Chunk is not in expected set - it's an orphan
		// Check grace period based on chunk creation time
		if chunk.CreatedAt > graceCutoff {
			// Chunk is too new, skip deletion (might be from in-flight upload)
			orphansSkipped++
			logger.Debug().
				Str("chunk_id", chunkIDStr).
				Int64("created_at", chunk.CreatedAt).
				Int64("grace_cutoff", graceCutoff).
				Msg("Skipping orphan chunk (within grace period)")
			return nil
		}

		// Delete orphan chunk
		if rs.config.DryRun {
			logger.Info().
				Str("chunk_id", chunkIDStr).
				Uint64("size", chunk.Size).
				Msg("[DRY-RUN] Would delete orphan chunk")
			orphansDeleted++
		} else {
			if err := rs.deleteChunk(ctx, id, chunk); err != nil {
				logger.Warn().
					Err(err).
					Str("chunk_id", chunkIDStr).
					Msg("Failed to delete orphan chunk")
				// Continue with other chunks
			} else {
				orphansDeleted++
			}
		}

		return nil
	})

	if err != nil {
		report.Error = err
		report.Duration = time.Since(start)
		logger.Error().Err(err).Msg("Error iterating chunks")
		return report
	}

	// Step 3: Find missing chunks (expected but not on disk)
	for chunkID := range expectedChunks {
		if !foundChunks[chunkID] {
			missingChunks = append(missingChunks, chunkID)
		}
	}

	report.TotalChunks = totalChunks
	report.OrphansDeleted = orphansDeleted
	report.OrphansSkipped = orphansSkipped
	report.MissingChunks = missingChunks
	report.Duration = time.Since(start)

	logger.Info().
		Int64("total_chunks", totalChunks).
		Int64("expected_chunks", report.ExpectedChunks).
		Int64("orphans_deleted", orphansDeleted).
		Int64("orphans_skipped", orphansSkipped).
		Int("missing_chunks", len(missingChunks)).
		Dur("duration", report.Duration).
		Msg("Chunk reconciliation complete")

	// Step 4: Report results to manager
	if err := rs.reportToManager(ctx, report); err != nil {
		logger.Warn().Err(err).Msg("Failed to report reconciliation results to manager")
	}

	rs.mu.Lock()
	rs.lastRunTime = time.Now()
	rs.lastReport = report
	rs.mu.Unlock()

	return report
}

// getExpectedChunks streams expected chunk IDs from the manager
func (rs *ReconciliationService) getExpectedChunks(ctx context.Context) (map[string]bool, error) {
	expectedChunks := make(map[string]bool)

	req := &manager_pb.GetExpectedChunksRequest{
		ServerId: rs.config.ServerID,
	}

	stream, err := rs.managerClient.GetExpectedChunks(ctx, req)
	if err != nil {
		return nil, err
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		expectedChunks[resp.GetChunkId()] = true
	}

	return expectedChunks, nil
}

// deleteChunk removes a chunk from storage
func (rs *ReconciliationService) deleteChunk(ctx context.Context, id types.ChunkID, chunk types.Chunk) error {
	// Delete from backend storage
	storage, ok := rs.store.GetBackendStorage(chunk.BackendID)
	if !ok {
		logger.Warn().
			Str("chunk_id", string(id)).
			Str("backend_id", chunk.BackendID).
			Msg("Backend not found for chunk deletion")
		// Still try to delete from index
	} else {
		if err := storage.Delete(ctx, chunk.Path); err != nil {
			return err
		}
	}

	// Delete from index
	return rs.store.DeleteChunk(ctx, id)
}

// reportToManager sends reconciliation results to the manager
func (rs *ReconciliationService) reportToManager(ctx context.Context, report *ReconciliationReport) error {
	req := &manager_pb.ReconciliationReport{
		ServerId:       report.ServerID,
		TotalChunks:    report.TotalChunks,
		ExpectedChunks: report.ExpectedChunks,
		OrphansDeleted: report.OrphansDeleted,
		OrphansSkipped: report.OrphansSkipped,
		MissingChunks:  report.MissingChunks,
		DurationMs:     report.Duration.Milliseconds(),
	}

	_, err := rs.managerClient.ReportReconciliation(ctx, req)
	return err
}

// runPeriodic runs reconciliation on a schedule
func (rs *ReconciliationService) runPeriodic(ctx context.Context) {
	defer close(rs.doneCh)

	// Run once at startup after a short delay
	select {
	case <-time.After(30 * time.Second):
		rs.RunOnce(ctx)
	case <-rs.stopCh:
		return
	case <-ctx.Done():
		return
	}

	ticker := time.NewTicker(rs.config.Interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			rs.RunOnce(ctx)
		case <-rs.stopCh:
			return
		case <-ctx.Done():
			return
		}
	}
}

// GetLastReport returns the most recent reconciliation report
func (rs *ReconciliationService) GetLastReport() *ReconciliationReport {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.lastReport
}

// IsRunning returns true if reconciliation is currently in progress
func (rs *ReconciliationService) IsRunning() bool {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.isRunning
}
