package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/go-cmp/cmp"
	"github.com/hashicorp/raft"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/testing/protocmp"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Commands that go through Raft
type CommandType string

const (
	CommandRegisterService   CommandType = "register_service"
	CommandUnregisterService CommandType = "unregister_service"
	CommandUpdatePolicy      CommandType = "update_policy"
	CommandCreateCollection  CommandType = "create_collection"
	CommandDeleteCollection  CommandType = "delete_collection"
	CommandUpdateCollection  CommandType = "update_collection"
)

type RaftCommand struct {
	Type CommandType     `json:"type"`
	Data json.RawMessage `json:"data"`
}

// ===== RAFT FSM INTERFACE IMPLEMENTATION =====

// Apply applies a Raft log entry to the FSM (state machine)
func (ms *ManagerServer) Apply(l *raft.Log) interface{} {
	var cmd RaftCommand
	if err := json.Unmarshal(l.Data, &cmd); err != nil {
		logger.Error().Err(err).Msg("Failed to unmarshal raft command")
		return err
	}

	logger.Info().Str("type", string(cmd.Type)).Msg("Applying raft command")

	switch cmd.Type {
	case CommandRegisterService:
		return ms.applyRegisterService(cmd.Data)
	case CommandUnregisterService:
		return ms.applyUnregisterService(cmd.Data)
	case CommandUpdatePolicy:
		return ms.applyUpdatePolicy(cmd.Data)
	case CommandCreateCollection:
		return ms.applyCreateCollection(cmd.Data)
	case CommandDeleteCollection:
		return ms.applyDeleteCollection(cmd.Data)
	case CommandUpdateCollection:
		return ms.applyUpdateCollection(cmd.Data)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// Snapshot returns a snapshot of the FSM state
func (ms *ManagerServer) Snapshot() (raft.FSMSnapshot, error) {
	ms.mu.RLock()
	defer ms.mu.RUnlock()

	// Clone state for snapshot
	snapshot := &fsmSnapshot{
		FileServices:       make(map[string]*ServiceRegistration),
		MetadataServices:   make(map[string]*ServiceRegistration),
		Collections:        make(map[string]*manager_pb.Collection),
		TopologyVersion:    ms.topologyVersion,
		CollectionsVersion: ms.collectionsVersion,
		PlacementPolicy:    ms.placementPolicy,
		RegionID:           ms.regionID,
	}

	// Deep copy file services
	for k, v := range ms.fileServices {
		regCopy := *v
		snapshot.FileServices[k] = &regCopy
	}

	// Deep copy metadata services
	for k, v := range ms.metadataServices {
		regCopy := *v
		snapshot.MetadataServices[k] = &regCopy
	}

	// Deep copy collections
	for k, v := range ms.collections {
		colCopy := proto.Clone(v).(*manager_pb.Collection)
		snapshot.Collections[k] = colCopy
	}

	logger.Info().Msg("Created FSM snapshot")
	return snapshot, nil
}

// Restore restores the FSM from a snapshot
func (ms *ManagerServer) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.fileServices = snapshot.FileServices
	ms.metadataServices = snapshot.MetadataServices
	ms.collections = snapshot.Collections
	ms.topologyVersion = snapshot.TopologyVersion
	ms.collectionsVersion = snapshot.CollectionsVersion
	ms.placementPolicy = snapshot.PlacementPolicy
	ms.regionID = snapshot.RegionID

	// Rebuild optimized indexes from restored collections
	ms.collectionsByOwner = make(map[string][]string)
	ms.ownerCollectionCount = make(map[string]int)
	ms.collectionsByTier = make(map[string][]string)
	for _, col := range ms.collections {
		ms.addCollectionToIndexes(col)
	}

	logger.Info().
		Int("file_services", len(ms.fileServices)).
		Int("metadata_services", len(ms.metadataServices)).
		Int("collections", len(ms.collections)).
		Uint64("topology_version", ms.topologyVersion).
		Uint64("collections_version", ms.collectionsVersion).
		Msg("Restored state from snapshot")

	return nil
}

// ===== RAFT COMMAND HANDLERS =====

func (ms *ManagerServer) applyRegisterService(data json.RawMessage) interface{} {
	var req manager_pb.RegisterServiceRequest
	// Use protojson for protobuf messages with oneof fields
	if err := protojson.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	serviceID := deriveServiceID(req.ServiceType, req.Location)
	registry := ms.getRegistry(req.ServiceType)

	now := time.Now()
	registration := &ServiceRegistration{
		ServiceType:              req.ServiceType,
		Location:                 req.Location,
		Status:                   ServiceActive,
		RegisteredAt:             now,
		LastHeartbeat:            now,
		LastKnownTopologyVersion: ms.topologyVersion,
	}

	// Add service-specific metadata
	if req.ServiceType == manager_pb.ServiceType_FILE_SERVICE {
		if fileMetadata := req.GetFileService(); fileMetadata != nil {
			registration.StorageBackends = fileMetadata.GetStorageBackends()
		}
	}

	// Check if this is a new registration or a material change
	existing, exists := registry[serviceID]
	isNewOrChanged := !exists || registrationMateriallyChanged(existing, registration)

	if exists {
		// Preserve original registration time for re-registrations
		registration.RegisteredAt = existing.RegisteredAt
	}

	registry[serviceID] = registration

	// Only increment topology version if this is a new service or has material changes
	if isNewOrChanged {
		ms.topologyVersion++
		logger.Info().
			Str("service_id", serviceID).
			Str("type", req.ServiceType.String()).
			Uint64("topology_version", ms.topologyVersion).
			Bool("is_new", !exists).
			Msg("Service registered via Raft (topology changed)")

		// Notify topology subscribers (PUSH update)
		eventType := manager_pb.TopologyEvent_SERVICE_UPDATED
		if !exists {
			eventType = manager_pb.TopologyEvent_SERVICE_ADDED
		}
		ms.notifyTopologySubscribers(eventType, []*manager_pb.ServiceInfo{
			{
				ServiceType:   registration.ServiceType,
				Location:      registration.Location,
				LastHeartbeat: timestamppb.New(registration.LastHeartbeat),
			},
		})
	} else {
		logger.Debug().
			Str("service_id", serviceID).
			Str("type", req.ServiceType.String()).
			Msg("Service re-registered via Raft (no topology change)")
	}

	return nil
}

func (ms *ManagerServer) applyUnregisterService(data json.RawMessage) interface{} {
	var req manager_pb.UnregisterServiceRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	serviceID := deriveServiceID(req.ServiceType, req.Location)
	registry := ms.getRegistry(req.ServiceType)

	delete(registry, serviceID)
	ms.topologyVersion++

	logger.Info().
		Str("service_id", serviceID).
		Uint64("topology_version", ms.topologyVersion).
		Msg("Service unregistered via Raft")

	// Notify topology subscribers (PUSH update)
	ms.notifyTopologySubscribers(manager_pb.TopologyEvent_SERVICE_REMOVED, []*manager_pb.ServiceInfo{
		{
			ServiceType: req.ServiceType,
			Location:    req.Location,
		},
	})

	return nil
}

func (ms *ManagerServer) applyUpdatePolicy(data json.RawMessage) interface{} {
	var policy manager_pb.PlacementPolicy
	if err := json.Unmarshal(data, &policy); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	ms.placementPolicy = &policy
	ms.topologyVersion++

	logger.Info().
		Uint32("num_replicas", policy.NumReplicas).
		Uint64("topology_version", ms.topologyVersion).
		Msg("Placement policy updated via Raft")

	return nil
}

func (ms *ManagerServer) applyCreateCollection(data json.RawMessage) interface{} {
	var col manager_pb.Collection
	if err := json.Unmarshal(data, &col); err != nil {
		return err
	}

	ms.mu.Lock()

	// Check if already exists
	if _, exists := ms.collections[col.Name]; exists {
		ms.mu.Unlock()
		return fmt.Errorf("collection %s already exists", col.Name)
	}

	ms.collections[col.Name] = &col
	// CRITICAL: Update optimized lookup maps
	ms.addCollectionToIndexes(&col)
	ms.collectionsVersion++

	logger.Info().
		Str("collection", col.Name).
		Str("owner", col.Owner).
		Uint64("version", ms.collectionsVersion).
		Msg("Collection created via Raft")

	ms.mu.Unlock()

	// Notify collection subscribers (for multi-region cache sync)
	ms.notifyCollectionSubscribers(manager_pb.CollectionEvent_CREATED, []*manager_pb.Collection{&col})

	return nil
}

func (ms *ManagerServer) applyDeleteCollection(data json.RawMessage) interface{} {
	var req struct {
		Name string `json:"name"`
	}
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.mu.Lock()

	// Get collection before deletion to update indexes and notify
	var deletedCol *manager_pb.Collection
	if col, exists := ms.collections[req.Name]; exists {
		// Make a copy for notification (use proto.Clone to avoid copylocks)
		deletedCol = proto.Clone(col).(*manager_pb.Collection)
		deletedCol.IsDeleted = true
		// CRITICAL: Update optimized lookup maps
		ms.removeCollectionFromIndexes(col)
	}

	delete(ms.collections, req.Name)
	ms.collectionsVersion++

	logger.Info().
		Str("collection", req.Name).
		Uint64("version", ms.collectionsVersion).
		Msg("Collection deleted via Raft")

	ms.mu.Unlock()

	// Notify collection subscribers (for multi-region cache sync)
	if deletedCol != nil {
		ms.notifyCollectionSubscribers(manager_pb.CollectionEvent_DELETED, []*manager_pb.Collection{deletedCol})
	}

	return nil
}

func (ms *ManagerServer) applyUpdateCollection(data json.RawMessage) interface{} {
	var col manager_pb.Collection
	if err := json.Unmarshal(data, &col); err != nil {
		return err
	}

	ms.mu.Lock()
	defer ms.mu.Unlock()

	if existing, exists := ms.collections[col.Name]; exists {
		// Remove from old indexes if owner or tier changed
		if existing.Owner != col.Owner || existing.Tier != col.Tier {
			ms.removeCollectionFromIndexes(existing)
		}

		// Update fields (preserve creation time)
		col.CreatedAt = existing.CreatedAt
		ms.collections[col.Name] = &col

		// Add to new indexes if owner or tier changed
		if existing.Owner != col.Owner || existing.Tier != col.Tier {
			ms.addCollectionToIndexes(&col)
		}

		ms.collectionsVersion++

		logger.Info().
			Str("collection", col.Name).
			Str("tier", col.Tier).
			Uint64("version", ms.collectionsVersion).
			Msg("Collection updated via Raft")
	}

	return nil
}

// ===== HELPER: APPLY COMMAND THROUGH RAFT =====

func (ms *ManagerServer) applyCommand(cmdType CommandType, data interface{}) error {
	if !ms.raftNode.IsLeader() {
		return ErrNotLeader
	}

	// Use protojson for protobuf messages (handles oneof fields correctly)
	// Check if it's a protobuf message by trying to cast to proto.Message
	var dataBytes []byte
	var err error
	if pbMsg, ok := data.(proto.Message); ok {
		// It's a protobuf message, use protojson
		dataBytes, err = protojson.Marshal(pbMsg)
	} else {
		// Regular struct, use standard JSON
		dataBytes, err = json.Marshal(data)
	}
	if err != nil {
		return err
	}

	cmd := RaftCommand{
		Type: cmdType,
		Data: dataBytes,
	}

	cmdBytes, err := json.Marshal(cmd)
	if err != nil {
		return err
	}

	return ms.raftNode.Apply(cmdBytes, 10*time.Second)
}

// ===== HELPER FUNCTIONS =====

// registrationMateriallyChanged returns true if the new registration differs
// from the existing one in ways that affect cluster topology.
// This enables idempotent re-registration without causing topology version churn.
func registrationMateriallyChanged(existing, new *ServiceRegistration) bool {
	// Status change (e.g., offline -> active) is a material change
	if existing.Status != new.Status {
		return true
	}

	// Storage backends change is material for file services
	if !storageBackendsEqual(existing.StorageBackends, new.StorageBackends) {
		return true
	}

	return false
}

// backendCompareOpts defines comparison options for Backend messages.
// Ignores dynamic fields (used_bytes, total_bytes) that change frequently
// and shouldn't trigger topology version changes.
var backendCompareOpts = cmp.Options{
	protocmp.Transform(),
	protocmp.IgnoreFields(&common_pb.Backend{}, "used_bytes", "total_bytes"),
}

// storageBackendsEqual compares two slices of storage backends for equality.
// Returns true if they contain the same backends (order-independent).
// Uses protocmp to compare protobuf messages while ignoring dynamic fields.
func storageBackendsEqual(a, b []*manager_pb.StorageBackend) bool {
	if len(a) != len(b) {
		return false
	}

	// Build map of backends by ID for comparison
	aMap := make(map[string]*manager_pb.StorageBackend, len(a))
	for _, backend := range a {
		aMap[backend.Id] = backend
	}

	for _, bBackend := range b {
		aBackend, exists := aMap[bBackend.Id]
		if !exists {
			return false
		}

		// Use cmp.Equal with protocmp options to compare, ignoring dynamic fields
		if !cmp.Equal(aBackend, bBackend, backendCompareOpts...) {
			return false
		}
	}

	return true
}

// ===== FSM SNAPSHOT =====

type fsmSnapshot struct {
	FileServices       map[string]*ServiceRegistration   `json:"file_services"`
	MetadataServices   map[string]*ServiceRegistration   `json:"metadata_services"`
	TopologyVersion    uint64                            `json:"topology_version"`
	PlacementPolicy    *manager_pb.PlacementPolicy       `json:"placement_policy"`
	RegionID           string                            `json:"region_id"`
	Collections        map[string]*manager_pb.Collection `json:"collections"`
	CollectionsVersion uint64                            `json:"collections_version"`
}

func (f *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		if err := json.NewEncoder(sink).Encode(f); err != nil {
			return err
		}
		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
		return err
	}

	logger.Info().Msg("Persisted FSM snapshot")
	return nil
}

func (f *fsmSnapshot) Release() {}
