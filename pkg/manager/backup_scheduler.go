// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// BackupSchedulerConfig configures automatic backup scheduling
type BackupSchedulerConfig struct {
	// Enabled flag
	Enabled bool

	// Schedule interval for automatic backups (default: 24h)
	Interval time.Duration

	// Directory to store backups (local path)
	// Future: support s3://bucket/prefix or gs://bucket/prefix
	DestinationDir string

	// Retention policy
	RetainCount int           // Number of backups to retain (default: 7)
	RetainDays  int           // Days to retain backups (default: 30)
	MaxSize     int64         // Max total backup size in bytes (0 = unlimited)

	// Backup naming
	Prefix string // Prefix for backup files (default: "manager")
}

// DefaultBackupSchedulerConfig returns the default backup scheduler configuration
func DefaultBackupSchedulerConfig() BackupSchedulerConfig {
	return BackupSchedulerConfig{
		Enabled:        false,
		Interval:       24 * time.Hour,
		DestinationDir: "/var/lib/zapfs/backups",
		RetainCount:    7,
		RetainDays:     30,
		MaxSize:        0,
		Prefix:         "manager",
	}
}

// BackupManifest describes a backup file
type BackupManifest struct {
	ID                 string    `json:"id"`
	CreatedAt          time.Time `json:"created_at"`
	TopologyVersion    uint64    `json:"topology_version"`
	CollectionsVersion uint64    `json:"collections_version"`
	FileServicesCount  int       `json:"file_services_count"`
	MetadataServices   int       `json:"metadata_services_count"`
	CollectionsCount   int       `json:"collections_count"`
	SizeBytes          int64     `json:"size_bytes"`
	Filename           string    `json:"filename"`
	Checksum           string    `json:"checksum,omitempty"`
	Description        string    `json:"description,omitempty"`
	Scheduled          bool      `json:"scheduled"` // true if created by scheduler
}

// BackupScheduler handles automatic periodic backups
type BackupScheduler struct {
	manager *ManagerServer
	config  BackupSchedulerConfig

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.RWMutex
	manifests []BackupManifest // Cached list of manifests
}

// NewBackupScheduler creates a new backup scheduler
func NewBackupScheduler(manager *ManagerServer, config BackupSchedulerConfig) *BackupScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &BackupScheduler{
		manager:   manager,
		config:    config,
		ctx:       ctx,
		cancel:    cancel,
		manifests: make([]BackupManifest, 0),
	}
}

// Start begins the backup scheduler loop
func (s *BackupScheduler) Start() error {
	if !s.config.Enabled {
		logger.Info().Msg("Backup scheduler disabled")
		return nil
	}

	// Check license
	if !license.CheckBackup() {
		logger.Warn().Msg("Backup scheduling requires enterprise license with FeatureBackup")
		return nil
	}

	// Create destination directory
	if err := os.MkdirAll(s.config.DestinationDir, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Load existing manifests
	if err := s.loadManifests(); err != nil {
		logger.Warn().Err(err).Msg("Failed to load existing backup manifests")
	}

	s.wg.Add(1)
	go s.scheduleLoop()

	logger.Info().
		Dur("interval", s.config.Interval).
		Str("destination", s.config.DestinationDir).
		Int("retain_count", s.config.RetainCount).
		Int("retain_days", s.config.RetainDays).
		Msg("Started backup scheduler")

	return nil
}

// Stop stops the backup scheduler and waits for completion
func (s *BackupScheduler) Stop() {
	s.cancel()
	s.wg.Wait()
	logger.Info().Msg("Stopped backup scheduler")
}

// scheduleLoop runs the periodic backup
func (s *BackupScheduler) scheduleLoop() {
	defer s.wg.Done()

	// Use jittered ticker to prevent thundering herd across instances
	// 10% jitter on 24h interval = ±2.4 hours
	tickCh, stopTicker := utils.JitteredTicker(s.config.Interval, 0.1)
	defer stopTicker()

	for {
		select {
		case <-tickCh:
			s.runScheduledBackup()
		case <-s.ctx.Done():
			return
		}
	}
}

// runScheduledBackup performs a scheduled backup
func (s *BackupScheduler) runScheduledBackup() {
	// Only leader should run backups
	if !s.manager.raftNode.IsLeader() {
		logger.Debug().Msg("Skipping scheduled backup - not leader")
		return
	}

	logger.Info().Msg("Starting scheduled backup")

	manifest, err := s.createBackup("scheduled backup")
	if err != nil {
		logger.Error().Err(err).Msg("Scheduled backup failed")
		return
	}

	logger.Info().
		Str("backup_id", manifest.ID).
		Str("filename", manifest.Filename).
		Int64("size_bytes", manifest.SizeBytes).
		Msg("Scheduled backup completed")

	// Apply retention policy
	s.applyRetentionPolicy()
}

// createBackup creates a new backup and returns its manifest
func (s *BackupScheduler) createBackup(description string) (*BackupManifest, error) {
	// Get FSM snapshot
	snapshot, err := s.manager.Snapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to create snapshot: %w", err)
	}

	fsmSnap, ok := snapshot.(*fsmSnapshot)
	if !ok {
		return nil, fmt.Errorf("unexpected snapshot type")
	}

	// Serialize to JSON
	data, err := json.MarshalIndent(fsmSnap, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to serialize snapshot: %w", err)
	}

	// Create filename
	now := time.Now().UTC()
	backupID := fmt.Sprintf("backup-%d", now.Unix())
	filename := fmt.Sprintf("%s-%s.json", s.config.Prefix, now.Format("2006-01-02-150405"))
	filepath := filepath.Join(s.config.DestinationDir, filename)

	// Write backup file
	if err := os.WriteFile(filepath, data, 0600); err != nil {
		return nil, fmt.Errorf("failed to write backup: %w", err)
	}

	// Create manifest
	manifest := BackupManifest{
		ID:                 backupID,
		CreatedAt:          now,
		TopologyVersion:    fsmSnap.TopologyVersion,
		CollectionsVersion: fsmSnap.CollectionsVersion,
		FileServicesCount:  len(fsmSnap.FileServices),
		MetadataServices:   len(fsmSnap.MetadataServices),
		CollectionsCount:   len(fsmSnap.Collections),
		SizeBytes:          int64(len(data)),
		Filename:           filename,
		Description:        description,
		Scheduled:          true,
	}

	// Save manifest
	s.mu.Lock()
	s.manifests = append(s.manifests, manifest)
	s.mu.Unlock()

	if err := s.saveManifests(); err != nil {
		logger.Warn().Err(err).Msg("Failed to save backup manifests")
	}

	return &manifest, nil
}

// applyRetentionPolicy removes old backups based on retention settings
func (s *BackupScheduler) applyRetentionPolicy() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.manifests) == 0 {
		return
	}

	// Sort by creation time (newest first)
	sort.Slice(s.manifests, func(i, j int) bool {
		return s.manifests[i].CreatedAt.After(s.manifests[j].CreatedAt)
	})

	var toDelete []BackupManifest
	var toKeep []BackupManifest

	now := time.Now()
	cutoffTime := now.AddDate(0, 0, -s.config.RetainDays)

	for i, m := range s.manifests {
		// Keep if within retention count
		if i < s.config.RetainCount {
			toKeep = append(toKeep, m)
			continue
		}

		// Delete if older than retention days
		if m.CreatedAt.Before(cutoffTime) {
			toDelete = append(toDelete, m)
		} else {
			toKeep = append(toKeep, m)
		}
	}

	// Delete old backups
	for _, m := range toDelete {
		filepath := filepath.Join(s.config.DestinationDir, m.Filename)
		if err := os.Remove(filepath); err != nil {
			logger.Warn().Err(err).Str("filename", m.Filename).Msg("Failed to delete old backup")
		} else {
			logger.Info().
				Str("backup_id", m.ID).
				Str("filename", m.Filename).
				Time("created_at", m.CreatedAt).
				Msg("Deleted old backup (retention policy)")
		}
	}

	s.manifests = toKeep

	if len(toDelete) > 0 {
		if err := s.saveManifests(); err != nil {
			logger.Warn().Err(err).Msg("Failed to save backup manifests after retention cleanup")
		}
	}
}

// loadManifests loads existing backup manifests from the destination directory
func (s *BackupScheduler) loadManifests() error {
	manifestPath := filepath.Join(s.config.DestinationDir, "manifests.json")

	data, err := os.ReadFile(manifestPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // No manifests yet
		}
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	return json.Unmarshal(data, &s.manifests)
}

// saveManifests saves the current manifests to disk
func (s *BackupScheduler) saveManifests() error {
	manifestPath := filepath.Join(s.config.DestinationDir, "manifests.json")

	s.mu.RLock()
	data, err := json.MarshalIndent(s.manifests, "", "  ")
	s.mu.RUnlock()

	if err != nil {
		return err
	}

	return os.WriteFile(manifestPath, data, 0600)
}

// ListBackups returns all known backups
func (s *BackupScheduler) ListBackups() []BackupManifest {
	s.mu.RLock()
	defer s.mu.RUnlock()

	result := make([]BackupManifest, len(s.manifests))
	copy(result, s.manifests)

	// Sort by creation time (newest first)
	sort.Slice(result, func(i, j int) bool {
		return result[i].CreatedAt.After(result[j].CreatedAt)
	})

	return result
}

// DeleteBackup deletes a backup by ID
func (s *BackupScheduler) DeleteBackup(backupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var found *BackupManifest
	var newManifests []BackupManifest

	for i := range s.manifests {
		if s.manifests[i].ID == backupID {
			found = &s.manifests[i]
		} else {
			newManifests = append(newManifests, s.manifests[i])
		}
	}

	if found == nil {
		return fmt.Errorf("backup not found: %s", backupID)
	}

	// Delete file
	filepath := filepath.Join(s.config.DestinationDir, found.Filename)
	if err := os.Remove(filepath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to delete backup file: %w", err)
	}

	s.manifests = newManifests

	return s.saveManifests()
}

// GetBackup returns a specific backup manifest
func (s *BackupScheduler) GetBackup(backupID string) (*BackupManifest, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for i := range s.manifests {
		if s.manifests[i].ID == backupID {
			return &s.manifests[i], nil
		}
	}

	return nil, fmt.Errorf("backup not found: %s", backupID)
}

// ===== gRPC handlers =====

// ListBackups returns all available backups
func (ms *ManagerServer) ListBackups(ctx context.Context, req *manager_pb.ListBackupsRequest) (*manager_pb.ListBackupsResponse, error) {
	if ms.backupScheduler == nil {
		return &manager_pb.ListBackupsResponse{
			Backups: []*manager_pb.BackupInfo{},
		}, nil
	}

	manifests := ms.backupScheduler.ListBackups()
	backups := make([]*manager_pb.BackupInfo, 0, len(manifests))

	for _, m := range manifests {
		backups = append(backups, &manager_pb.BackupInfo{
			BackupId:           m.ID,
			CreatedAt:          m.CreatedAt.Unix(),
			TopologyVersion:    m.TopologyVersion,
			CollectionsVersion: m.CollectionsVersion,
			FileServicesCount:  int32(m.FileServicesCount),
			MetadataServicesCount: int32(m.MetadataServices),
			CollectionsCount:   int32(m.CollectionsCount),
			SizeBytes:          m.SizeBytes,
			Filename:           m.Filename,
			Description:        m.Description,
			Scheduled:          m.Scheduled,
		})
	}

	return &manager_pb.ListBackupsResponse{
		Backups: backups,
	}, nil
}

// DeleteBackup deletes a backup by ID
func (ms *ManagerServer) DeleteBackup(ctx context.Context, req *manager_pb.DeleteBackupRequest) (*manager_pb.DeleteBackupResponse, error) {
	if !license.CheckBackup() {
		return nil, fmt.Errorf("backup requires enterprise license with FeatureBackup")
	}

	if ms.backupScheduler == nil {
		return &manager_pb.DeleteBackupResponse{
			Success: false,
			Error:   "backup scheduler not configured",
		}, nil
	}

	if err := ms.backupScheduler.DeleteBackup(req.GetBackupId()); err != nil {
		return &manager_pb.DeleteBackupResponse{
			Success: false,
			Error:   err.Error(),
		}, nil
	}

	logger.Info().Str("backup_id", req.GetBackupId()).Msg("Backup deleted")

	return &manager_pb.DeleteBackupResponse{
		Success: true,
	}, nil
}
