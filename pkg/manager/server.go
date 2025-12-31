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

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/iam_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/btree"
)

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

	// Raft (embedded consensus)
	raftNode *RaftNode

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
func NewManagerServer(regionID string, raftConfig *Config, leaderTimeout time.Duration, iamService *iam.Service) (*ManagerServer, error) {
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
			NumReplicas: 3,
		},
		topologyVersion:   1,
		leaderForwarder:   NewLeaderForwarder(),
		iamService:        iamService,
		iamSubs:           make(map[uint64]chan *iam_pb.CredentialEvent),
		topoSubs:          make(map[uint64]chan *manager_pb.TopologyEvent),
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
