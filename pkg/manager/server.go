// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package manager provides the manager server for ZapFS cluster coordination.
//
// The manager server handles:
// - Service registry (file and metadata services)
// - Placement decisions (where to store chunks)
// - Raft consensus for high availability
// - IAM credential management and sync to metadata services
// - Collection (bucket) management
// - GC coordination
package manager

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/LeeDigitalWorks/zapfs/pkg/grpc/pool"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/iam_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"

	"google.golang.org/grpc"
)

// ClusterCapacity holds cached cluster-wide capacity metrics.
// Updated via heartbeats from file services (30s stale max).
type ClusterCapacity struct {
	TotalBytes   uint64    // Sum of all storage backend capacities
	UsedBytes    uint64    // Sum of all used storage
	UpdatedAt    time.Time // Last update time
	FileServices int       // Number of active file services
}

// ManagerServer is the main manager server that coordinates the ZapFS cluster.
// It implements both ManagerServiceServer (cluster management) and IAMServiceServer
// (credential sync to metadata services).
type ManagerServer struct {
	manager_pb.UnimplementedManagerServiceServer
	iam_pb.UnimplementedIAMServiceServer

	// Raft-replicated state (isolated for clean snapshots)
	state *FSMState

	// Multi-region support (enterprise)
	regionConfig *RegionConfig // Multi-region configuration (nil = single-region mode)
	regionClient *RegionClient // Cross-region forwarding client (nil = single-region mode)
	regionSyncer *RegionSyncer // Periodic cache sync from primary (nil = single-region mode)

	// Raft (embedded consensus)
	raftNode *RaftNode

	// License checker
	licenseChecker license.Checker

	// Cached cluster capacity (updated via heartbeats)
	clusterCapacity   ClusterCapacity
	clusterCapacityMu sync.RWMutex

	// IAM - centralized credential management
	iamService    *iam.Service            // Legacy service for TOML/config-based credentials
	raftCredStore *RaftCredentialStore    // Raft-backed credential store (primary)
	iamVersion    atomic.Uint64           // Unix timestamp in nanos
	iamSubsMu     sync.RWMutex            // Protects subscribers
	iamSubs       map[uint64]chan *iam_pb.CredentialEvent // Subscriber channels
	iamNextSubID  atomic.Uint64

	// Topology watch subscribers (PUSH-based updates)
	topoSubsMu    sync.RWMutex
	topoSubs      map[uint64]chan *manager_pb.TopologyEvent
	topoNextSubID atomic.Uint64

	// Collection watch subscribers (PUSH-based updates for multi-region sync)
	colSubsMu    sync.RWMutex
	colSubs      map[uint64]chan *manager_pb.CollectionEvent
	colNextSubID atomic.Uint64

	// Leader forwarding for transparent write routing
	leaderForwarder *LeaderForwarder

	// Data loss detection / recovery
	collectionsRecoveryRequired   bool // Set true if data loss detected, freezes bucket creates
	collectionsRecoveryRequiredMu sync.RWMutex

	// Metadata client pool for reconciliation queries
	metadataClientPool *pool.Pool[metadata_pb.MetadataServiceClient]

	// Backup scheduler (enterprise)
	backupScheduler *BackupScheduler

	// Shutdown
	shutdownCh chan struct{}
	shutdownWg sync.WaitGroup
}

// ServiceRegistration holds information about a registered service
type ServiceRegistration struct {
	ServiceType              manager_pb.ServiceType
	Location                 *common_pb.Location
	Status                   ServiceStatus
	RegisteredAt             time.Time
	LastHeartbeat            time.Time
	LastKnownTopologyVersion uint64
	StorageBackends          []*manager_pb.StorageBackend
}

// ServiceStatus represents the health status of a service
type ServiceStatus int

const (
	ServiceActive ServiceStatus = iota
	ServiceOffline
)

// NewManagerServer creates a new manager server with the given configuration
func NewManagerServer(regionID string, raftConfig *Config, leaderTimeout time.Duration, iamService *iam.Service, licenseChecker license.Checker, defaultNumReplicas uint32) (*ManagerServer, error) {
	if defaultNumReplicas == 0 {
		defaultNumReplicas = 3 // sensible default
	}
	// Create metadata client factory
	metadataClientFactory := func(cc grpc.ClientConnInterface) metadata_pb.MetadataServiceClient {
		return metadata_pb.NewMetadataServiceClient(cc)
	}

	ms := &ManagerServer{
		state:              NewFSMState(regionID, defaultNumReplicas),
		leaderForwarder:   NewLeaderForwarder(),
		iamService:        iamService,
		licenseChecker:    licenseChecker,
		iamSubs:           make(map[uint64]chan *iam_pb.CredentialEvent),
		topoSubs:          make(map[uint64]chan *manager_pb.TopologyEvent),
		colSubs:           make(map[uint64]chan *manager_pb.CollectionEvent),
		metadataClientPool: pool.NewPool(metadataClientFactory),
		shutdownCh:        make(chan struct{}),
	}

	// Initialize IAM version to current timestamp (nanoseconds)
	ms.iamVersion.Store(uint64(time.Now().UnixNano()))

	raftNode, err := NewRaftNode(ms, raftConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}
	ms.raftNode = raftNode

	// Initialize Raft-backed credential store
	ms.raftCredStore = NewRaftCredentialStore(ms)

	if err := raftNode.WaitForLeader(leaderTimeout); err != nil {
		logger.Warn().Err(err).Msg("No leader elected yet")
	}

	ms.shutdownWg.Add(1)
	go ms.healthCheckLoop()

	return ms, nil
}

// ConfigureMultiRegion sets up multi-region support for the manager server.
// This enables cross-region bucket forwarding and cache synchronization.
// This is an enterprise feature - in community edition, this is a no-op.
func (ms *ManagerServer) ConfigureMultiRegion(regionConfig *RegionConfig) error {
	if regionConfig == nil || !regionConfig.IsConfigured() {
		logger.Info().Msg("Multi-region not configured, running in single-region mode")
		return nil
	}

	if err := regionConfig.Validate(); err != nil {
		return fmt.Errorf("invalid region config: %w", err)
	}

	// Create region client for cross-region calls
	regionClient, err := NewRegionClient(regionConfig)
	if err != nil {
		return fmt.Errorf("failed to create region client: %w", err)
	}

	ms.regionConfig = regionConfig
	ms.regionClient = regionClient

	if regionConfig.IsPrimary() {
		logger.Info().Str("region", regionConfig.Name).Msg("Running as PRIMARY region")
	} else {
		logger.Info().
			Str("region", regionConfig.Name).
			Strs("primary_regions", regionConfig.PrimaryRegions).
			Msg("Running as SECONDARY region - bucket operations will be forwarded to primary")

		// Start cache syncer for secondary regions
		ms.regionSyncer = NewRegionSyncer(ms)
		ms.regionSyncer.Start()
	}

	return nil
}

// IsPrimaryRegion returns true if this manager is in the primary region.
// In single-region mode (no multi-region config), this always returns true.
func (ms *ManagerServer) IsPrimaryRegion() bool {
	if ms.regionConfig == nil {
		return true // Single-region mode
	}
	return ms.regionConfig.IsPrimary()
}

// ConfigureBackupScheduler sets up automatic backup scheduling.
// This is an enterprise feature requiring FeatureBackup license.
func (ms *ManagerServer) ConfigureBackupScheduler(config BackupSchedulerConfig) error {
	if !config.Enabled {
		logger.Info().Msg("Backup scheduler disabled")
		return nil
	}

	ms.backupScheduler = NewBackupScheduler(ms, config)
	if err := ms.backupScheduler.Start(); err != nil {
		return fmt.Errorf("failed to start backup scheduler: %w", err)
	}

	return nil
}

// checkDataLossCondition checks if metadata services have buckets but manager has none.
// This indicates potential data loss (Raft data corrupted/deleted but metadata DBs intact).
// If detected, sets collectionsRecoveryRequired to freeze bucket creates.
// IMPORTANT: Must be called while holding state lock.
func (ms *ManagerServer) checkDataLossCondition(serviceID string, metadataBucketCount uint64) {
	// Only check if manager has 0 collections and metadata has some
	if len(ms.state.Collections) == 0 && metadataBucketCount > 0 {
		ms.collectionsRecoveryRequiredMu.Lock()
		if !ms.collectionsRecoveryRequired {
			// First detection - log critical error and set flag
			logger.Error().
				Str("service_id", serviceID).
				Uint64("metadata_buckets", metadataBucketCount).
				Int("manager_collections", len(ms.state.Collections)).
				Msg("DATA LOSS DETECTED: metadata service has buckets but manager has 0 collections. " +
					"Bucket creates frozen until recovery. Use 'zapfs manager recover' to restore.")
			ms.collectionsRecoveryRequired = true
			ManagerDataLossDetected.Set(1)
			ManagerRecoveryRequired.Set(1)
		}
		ms.collectionsRecoveryRequiredMu.Unlock()
	}

	// Update collections metric
	ManagerCollectionsTotal.Set(float64(len(ms.state.Collections)))
}

// IsRecoveryRequired returns true if the manager detected data loss and
// bucket creates are frozen pending recovery.
func (ms *ManagerServer) IsRecoveryRequired() bool {
	ms.collectionsRecoveryRequiredMu.RLock()
	defer ms.collectionsRecoveryRequiredMu.RUnlock()
	return ms.collectionsRecoveryRequired
}

// ClearRecoveryMode clears the recovery required flag after successful recovery.
// This should only be called after explicit recovery operation.
func (ms *ManagerServer) ClearRecoveryMode() {
	ms.collectionsRecoveryRequiredMu.Lock()
	defer ms.collectionsRecoveryRequiredMu.Unlock()
	if ms.collectionsRecoveryRequired {
		ms.state.RLock()
		collCount := len(ms.state.Collections)
		ms.state.RUnlock()
		logger.Info().
			Int("collections", collCount).
			Msg("Recovery mode cleared - bucket creates enabled")
		ms.collectionsRecoveryRequired = false
		ManagerDataLossDetected.Set(0)
		ManagerRecoveryRequired.Set(0)
	}
}

// GetIAMService returns the IAM service
func (ms *ManagerServer) GetIAMService() *iam.Service {
	return ms.iamService
}

// GetRaftCredentialStore returns the Raft-backed credential store
func (ms *ManagerServer) GetRaftCredentialStore() *RaftCredentialStore {
	return ms.raftCredStore
}

// BootstrapIAMFromConfig bootstraps IAM credentials from the legacy config (TOML) into Raft.
// This should be called once on the leader after cluster initialization.
// If Raft already has IAM users, this is a no-op.
func (ms *ManagerServer) BootstrapIAMFromConfig(ctx context.Context) error {
	if ms.iamService == nil {
		logger.Info().Msg("No legacy IAM service configured, skipping bootstrap")
		return nil
	}

	// Check if Raft already has users
	if ms.raftCredStore.HasUsers() {
		logger.Info().Msg("IAM state exists in Raft, skipping TOML bootstrap")
		return nil
	}

	// Must be leader to bootstrap
	if !ms.raftNode.IsLeader() {
		logger.Info().Msg("Not leader, skipping IAM bootstrap")
		return nil
	}

	// Get all users from legacy service
	users, err := ms.iamService.ListUsers(ctx)
	if err != nil {
		return fmt.Errorf("failed to list users from legacy service: %w", err)
	}

	if len(users) == 0 {
		logger.Info().Msg("No users in legacy IAM config to bootstrap")
		return nil
	}

	logger.Info().Int("users", len(users)).Msg("Bootstrapping IAM from TOML config into Raft")

	for _, username := range users {
		identity, err := ms.iamService.GetUser(ctx, username)
		if err != nil {
			logger.Warn().Err(err).Str("user", username).Msg("Failed to get user from legacy service")
			continue
		}

		// Ensure credentials are encrypted before storing in Raft
		for _, cred := range identity.Credentials {
			if len(cred.EncryptedSecret) == 0 && cred.SecretKey != "" {
				if err := iam.SecureCredential(cred); err != nil {
					logger.Warn().Err(err).Str("user", username).Msg("Failed to encrypt credential")
					continue
				}
			}
		}

		if err := ms.raftCredStore.CreateUser(ctx, identity); err != nil {
			logger.Warn().Err(err).Str("user", username).Msg("Failed to create user in Raft")
			continue
		}

		logger.Info().Str("user", username).Int("credentials", len(identity.Credentials)).Msg("Bootstrapped user into Raft")
	}

	logger.Info().Int("users", len(users)).Msg("IAM bootstrap complete")
	return nil
}

// IsClusterReady returns true if the Raft cluster has an elected leader
// This is used for readiness checks - the service should not accept requests
// until the cluster is operational
func (ms *ManagerServer) IsClusterReady() bool {
	if ms.raftNode == nil {
		return false
	}
	return ms.raftNode.HasLeader()
}

// WaitForClusterReady blocks until the cluster has a leader or context is cancelled
func (ms *ManagerServer) WaitForClusterReady(timeout time.Duration) error {
	if ms.raftNode == nil {
		return fmt.Errorf("raft node not initialized")
	}
	return ms.raftNode.WaitForLeader(timeout)
}

// GetClusterState returns information about the current cluster state
func (ms *ManagerServer) GetClusterState() map[string]interface{} {
	state := map[string]interface{}{
		"has_leader": false,
		"is_leader":  false,
		"state":      "unknown",
		"leader":     "",
	}

	if ms.raftNode != nil {
		state["has_leader"] = ms.raftNode.HasLeader()
		state["is_leader"] = ms.raftNode.IsLeader()
		state["state"] = ms.raftNode.State()
		state["leader"] = ms.raftNode.Leader()
	}

	return state
}

// Shutdown gracefully shuts down the manager server
func (ms *ManagerServer) Shutdown() {
	if ms.backupScheduler != nil {
		ms.backupScheduler.Stop()
	}

	if ms.leaderForwarder != nil {
		ms.leaderForwarder.Close()
	}

	if ms.regionSyncer != nil {
		ms.regionSyncer.Stop()
	}

	if ms.regionClient != nil {
		ms.regionClient.Close()
	}

	if ms.raftNode != nil {
		ms.raftNode.Shutdown()
	}

	close(ms.shutdownCh)
	ms.shutdownWg.Wait()
}

// healthCheckLoop periodically checks the health of registered services
func (ms *ManagerServer) healthCheckLoop() {
	defer ms.shutdownWg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	timeout := 90 * time.Second

	for {
		select {
		case <-ticker.C:
			if ms.raftNode.IsLeader() {
				ms.checkServiceHealth(timeout)
			}
		case <-ms.shutdownCh:
			return
		}
	}
}

func (ms *ManagerServer) checkServiceHealth(timeout time.Duration) {
	ms.state.Lock()
	defer ms.state.Unlock()

	now := time.Now()
	var offlineServices []*manager_pb.ServiceInfo

	for id, reg := range ms.state.FileServices {
		if now.Sub(reg.LastHeartbeat) > timeout && reg.Status == ServiceActive {
			logger.Info().Msgf("File service %s is OFFLINE (no heartbeat for %v)", id, now.Sub(reg.LastHeartbeat))
			reg.Status = ServiceOffline
			offlineServices = append(offlineServices, &manager_pb.ServiceInfo{
				ServiceType: reg.ServiceType,
				Location:    reg.Location,
			})
		}
	}

	for id, reg := range ms.state.MetadataServices {
		if now.Sub(reg.LastHeartbeat) > timeout && reg.Status == ServiceActive {
			logger.Info().Msgf("Metadata service %s is OFFLINE (no heartbeat for %v)", id, now.Sub(reg.LastHeartbeat))
			reg.Status = ServiceOffline
			offlineServices = append(offlineServices, &manager_pb.ServiceInfo{
				ServiceType: reg.ServiceType,
				Location:    reg.Location,
			})
		}
	}

	if len(offlineServices) > 0 {
		ms.state.TopologyVersion++
		logger.Info().Msgf("Topology version updated to %d", ms.state.TopologyVersion)

		// Notify topology subscribers (PUSH update)
		ms.notifyTopologySubscribers(manager_pb.TopologyEvent_SERVICE_REMOVED, offlineServices)
	}
}

func (ms *ManagerServer) getRegistry(serviceType manager_pb.ServiceType) map[string]*ServiceRegistration {
	return ms.state.GetRegistry(serviceType)
}

// deriveServiceID creates a stable service identifier.
// Uses location.Node (node_id) if available for stable identification across restarts.
// Falls back to address for backward compatibility with older clients.
func deriveServiceID(serviceType manager_pb.ServiceType, location *common_pb.Location) string {
	// Prefer node_id for stable identification (doesn't change on container restart)
	if location.Node != "" {
		return fmt.Sprintf("%s:%s", serviceType.String(), location.Node)
	}
	// Fallback to address for backward compatibility
	return fmt.Sprintf("%s:%s", serviceType.String(), location.Address)
}

// ===== CLUSTER CAPACITY & LICENSE ENFORCEMENT =====

// updateClusterCapacity recalculates and caches cluster-wide capacity from file services.
// Should be called with state lock held (read lock is sufficient).
func (ms *ManagerServer) updateClusterCapacity() {
	var totalBytes, usedBytes uint64
	var activeServices int

	for _, reg := range ms.state.FileServices {
		if reg.Status != ServiceActive {
			continue
		}
		activeServices++

		for _, storageBackend := range reg.StorageBackends {
			for _, backend := range storageBackend.Backends {
				totalBytes += backend.TotalBytes
				usedBytes += backend.UsedBytes
			}
		}
	}

	ms.clusterCapacityMu.Lock()
	ms.clusterCapacity = ClusterCapacity{
		TotalBytes:   totalBytes,
		UsedBytes:    usedBytes,
		UpdatedAt:    time.Now(),
		FileServices: activeServices,
	}
	ms.clusterCapacityMu.Unlock()

	logger.Debug().
		Str("total", humanize.IBytes(totalBytes)).
		Str("used", humanize.IBytes(usedBytes)).
		Int("file_services", activeServices).
		Msg("Cluster capacity updated")
}

// GetClusterCapacity returns the cached cluster capacity metrics.
// This is O(1) and safe for frequent reads.
func (ms *ManagerServer) GetClusterCapacity() ClusterCapacity {
	ms.clusterCapacityMu.RLock()
	defer ms.clusterCapacityMu.RUnlock()
	return ms.clusterCapacity
}

// GetActiveFileServiceCount returns the number of active file services.
func (ms *ManagerServer) GetActiveFileServiceCount() int {
	ms.state.RLock()
	defer ms.state.RUnlock()

	count := 0
	for _, reg := range ms.state.FileServices {
		if reg.Status == ServiceActive {
			count++
		}
	}
	return count
}

// LogClusterCapacity logs the current cluster capacity in human-readable format.
func (ms *ManagerServer) LogClusterCapacity() {
	capacity := ms.GetClusterCapacity()
	freeBytes := capacity.TotalBytes - capacity.UsedBytes
	usagePercent := float64(0)
	if capacity.TotalBytes > 0 {
		usagePercent = float64(capacity.UsedBytes) / float64(capacity.TotalBytes) * 100
	}

	logger.Info().
		Str("total", humanize.IBytes(capacity.TotalBytes)).
		Str("used", humanize.IBytes(capacity.UsedBytes)).
		Str("free", humanize.IBytes(freeBytes)).
		Str("usage", fmt.Sprintf("%.1f%%", usagePercent)).
		Int("file_services", capacity.FileServices).
		Time("updated_at", capacity.UpdatedAt).
		Msg("Cluster capacity summary")
}
