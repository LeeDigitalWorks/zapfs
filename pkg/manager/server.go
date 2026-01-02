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
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dustin/go-humanize"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/iam_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/btree"
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

	mu               sync.RWMutex
	fileServices     map[string]*ServiceRegistration
	metadataServices map[string]*ServiceRegistration
	topologyVersion  uint64

	// Collections (storage namespaces / buckets)
	collections        map[string]*manager_pb.Collection
	collectionsVersion uint64

	// Optimized collection lookups
	collectionsByOwner   map[string][]string
	ownerCollectionCount map[string]int
	collectionsByTier    map[string][]string
	collectionsByTime    *btree.BTree

	// Configuration
	placementPolicy *manager_pb.PlacementPolicy
	regionID        string

	// Multi-region support (enterprise)
	regionConfig *RegionConfig  // Multi-region configuration (nil = single-region mode)
	regionClient *RegionClient  // Cross-region forwarding client (nil = single-region mode)
	regionSyncer *RegionSyncer  // Periodic cache sync from primary (nil = single-region mode)

	// Raft (embedded consensus)
	raftNode *RaftNode

	// License checker
	licenseChecker license.Checker

	// Cached cluster capacity (updated via heartbeats)
	clusterCapacity ClusterCapacity

	// IAM - centralized credential management
	iamService   *iam.Service
	iamVersion   atomic.Uint64                           // Unix timestamp in nanos
	iamSubsMu    sync.RWMutex                            // Protects subscribers
	iamSubs      map[uint64]chan *iam_pb.CredentialEvent // Subscriber channels
	iamNextSubID atomic.Uint64

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
	ms := &ManagerServer{
		regionID:             regionID,
		fileServices:         make(map[string]*ServiceRegistration),
		metadataServices:     make(map[string]*ServiceRegistration),
		collections:          make(map[string]*manager_pb.Collection),
		collectionsVersion:   1,
		collectionsByOwner:   make(map[string][]string),
		ownerCollectionCount: make(map[string]int),
		collectionsByTier:    make(map[string][]string),
		collectionsByTime:    btree.New(2),
		placementPolicy: &manager_pb.PlacementPolicy{
			NumReplicas: defaultNumReplicas,
		},
		topologyVersion:   1,
		leaderForwarder:   NewLeaderForwarder(),
		iamService:        iamService,
		licenseChecker:    licenseChecker,
		iamSubs:           make(map[uint64]chan *iam_pb.CredentialEvent),
		topoSubs:          make(map[uint64]chan *manager_pb.TopologyEvent),
		colSubs:           make(map[uint64]chan *manager_pb.CollectionEvent),
		shutdownCh:        make(chan struct{}),
	}

	// Initialize IAM version to current timestamp (nanoseconds)
	ms.iamVersion.Store(uint64(time.Now().UnixNano()))

	raftNode, err := NewRaftNode(ms, raftConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create raft node: %w", err)
	}
	ms.raftNode = raftNode

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

// GetIAMService returns the IAM service
func (ms *ManagerServer) GetIAMService() *iam.Service {
	return ms.iamService
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
	ms.mu.Lock()
	defer ms.mu.Unlock()

	now := time.Now()
	var offlineServices []*manager_pb.ServiceInfo

	for id, reg := range ms.fileServices {
		if now.Sub(reg.LastHeartbeat) > timeout && reg.Status == ServiceActive {
			logger.Info().Msgf("File service %s is OFFLINE (no heartbeat for %v)", id, now.Sub(reg.LastHeartbeat))
			reg.Status = ServiceOffline
			offlineServices = append(offlineServices, &manager_pb.ServiceInfo{
				ServiceType: reg.ServiceType,
				Location:    reg.Location,
			})
		}
	}

	for id, reg := range ms.metadataServices {
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
		ms.topologyVersion++
		logger.Info().Msgf("Topology version updated to %d", ms.topologyVersion)

		// Notify topology subscribers (PUSH update)
		ms.notifyTopologySubscribers(manager_pb.TopologyEvent_SERVICE_REMOVED, offlineServices)
	}
}

func (ms *ManagerServer) getRegistry(serviceType manager_pb.ServiceType) map[string]*ServiceRegistration {
	if serviceType == manager_pb.ServiceType_FILE_SERVICE {
		return ms.fileServices
	}
	return ms.metadataServices
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
// Should be called with ms.mu held (write lock).
func (ms *ManagerServer) updateClusterCapacity() {
	var totalBytes, usedBytes uint64
	var activeServices int

	for _, reg := range ms.fileServices {
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

	ms.clusterCapacity = ClusterCapacity{
		TotalBytes:   totalBytes,
		UsedBytes:    usedBytes,
		UpdatedAt:    time.Now(),
		FileServices: activeServices,
	}

	logger.Debug().
		Str("total", humanize.IBytes(totalBytes)).
		Str("used", humanize.IBytes(usedBytes)).
		Int("file_services", activeServices).
		Msg("Cluster capacity updated")
}

// GetClusterCapacity returns the cached cluster capacity metrics.
// This is O(1) and safe for frequent reads.
func (ms *ManagerServer) GetClusterCapacity() ClusterCapacity {
	ms.mu.RLock()
	defer ms.mu.RUnlock()
	return ms.clusterCapacity
}

// GetActiveFileServiceCount returns the number of active file services.
func (ms *ManagerServer) GetActiveFileServiceCount() int {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	count := 0
	for _, reg := range ms.fileServices {
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
