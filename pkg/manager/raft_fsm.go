// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"encoding/json"
	"fmt"
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
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
	CommandRegisterService     CommandType = "register_service"
	CommandUnregisterService   CommandType = "unregister_service"
	CommandUpdatePolicy        CommandType = "update_policy"
	CommandCreateCollection    CommandType = "create_collection"
	CommandDeleteCollection    CommandType = "delete_collection"
	CommandUpdateCollection    CommandType = "update_collection"
	CommandUpdateServiceStatus CommandType = "update_service_status"

	// IAM commands
	CommandIAMCreateUser   CommandType = "iam_create_user"
	CommandIAMUpdateUser   CommandType = "iam_update_user"
	CommandIAMDeleteUser   CommandType = "iam_delete_user"
	CommandIAMCreateKey    CommandType = "iam_create_key"
	CommandIAMDeleteKey    CommandType = "iam_delete_key"
	CommandIAMCreatePolicy CommandType = "iam_create_policy"
	CommandIAMDeletePolicy CommandType = "iam_delete_policy"
)

// ServiceStatusUpdate represents a status change for a service (goes through Raft)
type ServiceStatusUpdate struct {
	ServiceType manager_pb.ServiceType `json:"service_type"`
	ServiceID   string                 `json:"service_id"`
	NewStatus   ServiceStatus          `json:"new_status"`
}

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

	logger.Debug().Str("type", string(cmd.Type)).Msg("Applying raft command")

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
	case CommandUpdateServiceStatus:
		return ms.applyUpdateServiceStatus(cmd.Data)
	// IAM commands
	case CommandIAMCreateUser:
		return ms.applyIAMCreateUser(cmd.Data)
	case CommandIAMUpdateUser:
		return ms.applyIAMUpdateUser(cmd.Data)
	case CommandIAMDeleteUser:
		return ms.applyIAMDeleteUser(cmd.Data)
	case CommandIAMCreateKey:
		return ms.applyIAMCreateKey(cmd.Data)
	case CommandIAMDeleteKey:
		return ms.applyIAMDeleteKey(cmd.Data)
	case CommandIAMCreatePolicy:
		return ms.applyIAMCreatePolicy(cmd.Data)
	case CommandIAMDeletePolicy:
		return ms.applyIAMDeletePolicy(cmd.Data)
	// Federation commands
	case CommandRegisterFederation:
		return ms.applyRegisterFederation(cmd.Data)
	case CommandUnregisterFederation:
		return ms.applyUnregisterFederation(cmd.Data)
	case CommandSetFederationMode:
		return ms.applySetFederationMode(cmd.Data)
	case CommandPauseFederation:
		return ms.applyPauseFederation(cmd.Data)
	case CommandResumeFederation:
		return ms.applyResumeFederation(cmd.Data)
	case CommandSetFederationDualWrite:
		return ms.applySetFederationDualWrite(cmd.Data)
	case CommandUpdateFederationCredentials:
		return ms.applyUpdateFederationCredentials(cmd.Data)
	default:
		return fmt.Errorf("unknown command type: %s", cmd.Type)
	}
}

// Snapshot returns a snapshot of the FSM state
func (ms *ManagerServer) Snapshot() (raft.FSMSnapshot, error) {
	ms.state.RLock()
	defer ms.state.RUnlock()

	snapshot := ms.state.Snapshot()
	logger.Debug().Msg("Created FSM snapshot")
	return snapshot, nil
}

// Restore restores the FSM from a snapshot
func (ms *ManagerServer) Restore(rc io.ReadCloser) error {
	defer rc.Close()

	var snapshot fsmSnapshot
	if err := json.NewDecoder(rc).Decode(&snapshot); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	ms.state.Restore(&snapshot)

	logger.Info().
		Int("file_services", len(ms.state.FileServices)).
		Int("metadata_services", len(ms.state.MetadataServices)).
		Int("collections", len(ms.state.Collections)).
		Int("iam_users", len(ms.state.IAMUsers)).
		Int("iam_policies", len(ms.state.IAMPolicies)).
		Int("federation_configs", len(ms.state.FederationConfigs)).
		Uint64("topology_version", ms.state.TopologyVersion).
		Uint64("collections_version", ms.state.CollectionsVersion).
		Uint64("iam_version", ms.state.IAMVersion).
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

	ms.state.Lock()
	defer ms.state.Unlock()

	serviceID := deriveServiceID(req.ServiceType, req.Location)
	registry := ms.state.GetRegistry(req.ServiceType)

	now := time.Now()
	registration := &ServiceRegistration{
		ServiceType:              req.ServiceType,
		Location:                 req.Location,
		Status:                   ServiceActive,
		RegisteredAt:             now,
		LastHeartbeat:            now,
		LastKnownTopologyVersion: ms.state.TopologyVersion,
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
		ms.state.TopologyVersion++
		logger.Debug().
			Str("service_id", serviceID).
			Str("type", req.ServiceType.String()).
			Uint64("topology_version", ms.state.TopologyVersion).
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
	if err := protojson.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	serviceID := deriveServiceID(req.ServiceType, req.Location)
	registry := ms.state.GetRegistry(req.ServiceType)

	delete(registry, serviceID)
	ms.state.TopologyVersion++

	logger.Debug().
		Str("service_id", serviceID).
		Uint64("topology_version", ms.state.TopologyVersion).
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

func (ms *ManagerServer) applyUpdateServiceStatus(data json.RawMessage) interface{} {
	var update ServiceStatusUpdate
	if err := json.Unmarshal(data, &update); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	registry := ms.state.GetRegistry(update.ServiceType)
	reg, exists := registry[update.ServiceID]
	if !exists {
		logger.Warn().
			Str("service_id", update.ServiceID).
			Msg("Ignoring status update for unknown service")
		return nil
	}

	oldStatus := reg.Status
	if oldStatus == update.NewStatus {
		// No change needed
		return nil
	}

	reg.Status = update.NewStatus
	ms.state.TopologyVersion++

	logger.Debug().
		Str("service_id", update.ServiceID).
		Str("old_status", statusToString(oldStatus)).
		Str("new_status", statusToString(update.NewStatus)).
		Uint64("topology_version", ms.state.TopologyVersion).
		Msg("Service status updated via Raft")

	// Notify topology subscribers
	eventType := manager_pb.TopologyEvent_SERVICE_UPDATED
	switch update.NewStatus {
	case ServiceActive:
		eventType = manager_pb.TopologyEvent_SERVICE_ADDED // Semantically "came back online"
	case ServiceOffline:
		eventType = manager_pb.TopologyEvent_SERVICE_REMOVED // Semantically "went offline"
	}

	ms.notifyTopologySubscribers(eventType, []*manager_pb.ServiceInfo{
		{
			ServiceType:   reg.ServiceType,
			Location:      reg.Location,
			LastHeartbeat: timestamppb.New(reg.LastHeartbeat),
		},
	})

	return nil
}

// statusToString converts ServiceStatus to a string for logging
func statusToString(s ServiceStatus) string {
	switch s {
	case ServiceActive:
		return "active"
	case ServiceOffline:
		return "offline"
	default:
		return "unknown"
	}
}

func (ms *ManagerServer) applyUpdatePolicy(data json.RawMessage) interface{} {
	var policy manager_pb.PlacementPolicy
	if err := protojson.Unmarshal(data, &policy); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	ms.state.PlacementPolicy = &policy
	ms.state.TopologyVersion++

	logger.Debug().
		Uint32("num_replicas", policy.NumReplicas).
		Uint64("topology_version", ms.state.TopologyVersion).
		Msg("Placement policy updated via Raft")

	return nil
}

func (ms *ManagerServer) applyCreateCollection(data json.RawMessage) interface{} {
	var col manager_pb.Collection
	if err := protojson.Unmarshal(data, &col); err != nil {
		return err
	}

	ms.state.Lock()

	// Check if already exists
	if _, exists := ms.state.Collections[col.Name]; exists {
		ms.state.Unlock()
		return fmt.Errorf("collection %s already exists", col.Name)
	}

	ms.state.Collections[col.Name] = &col
	// CRITICAL: Update optimized lookup maps
	ms.state.AddCollectionToIndexes(&col)
	ms.state.CollectionsVersion++

	logger.Debug().
		Str("collection", col.Name).
		Str("owner", col.Owner).
		Uint64("version", ms.state.CollectionsVersion).
		Msg("Collection created via Raft")

	ms.state.Unlock()

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

	ms.state.Lock()

	// Get collection before deletion to update indexes and notify
	var deletedCol *manager_pb.Collection
	if col, exists := ms.state.Collections[req.Name]; exists {
		// Make a copy for notification (use proto.Clone to avoid copylocks)
		deletedCol = proto.Clone(col).(*manager_pb.Collection)
		deletedCol.IsDeleted = true
		// CRITICAL: Update optimized lookup maps
		ms.state.RemoveCollectionFromIndexes(col)
	}

	delete(ms.state.Collections, req.Name)
	ms.state.CollectionsVersion++

	logger.Debug().
		Str("collection", req.Name).
		Uint64("version", ms.state.CollectionsVersion).
		Msg("Collection deleted via Raft")

	ms.state.Unlock()

	// Notify collection subscribers (for multi-region cache sync)
	if deletedCol != nil {
		ms.notifyCollectionSubscribers(manager_pb.CollectionEvent_DELETED, []*manager_pb.Collection{deletedCol})
	}

	return nil
}

func (ms *ManagerServer) applyUpdateCollection(data json.RawMessage) interface{} {
	var col manager_pb.Collection
	if err := protojson.Unmarshal(data, &col); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	if existing, exists := ms.state.Collections[col.Name]; exists {
		// Remove from old indexes if owner or tier changed
		if existing.Owner != col.Owner || existing.Tier != col.Tier {
			ms.state.RemoveCollectionFromIndexes(existing)
		}

		// Update fields (preserve creation time)
		col.CreatedAt = existing.CreatedAt
		ms.state.Collections[col.Name] = &col

		// Add to new indexes if owner or tier changed
		if existing.Owner != col.Owner || existing.Tier != col.Tier {
			ms.state.AddCollectionToIndexes(&col)
		}

		ms.state.CollectionsVersion++

		logger.Debug().
			Str("collection", col.Name).
			Str("tier", col.Tier).
			Uint64("version", ms.state.CollectionsVersion).
			Msg("Collection updated via Raft")
	}

	return nil
}

// ===== IAM APPLY HANDLERS =====

// IAMCreateUserRequest is the Raft command data for creating a user.
type IAMCreateUserRequest struct {
	Identity *iam.Identity `json:"identity"`
}

// IAMDeleteUserRequest is the Raft command data for deleting a user.
type IAMDeleteUserRequest struct {
	UserName string `json:"user_name"`
}

// IAMCreateKeyRequest is the Raft command data for creating an access key.
type IAMCreateKeyRequest struct {
	UserName   string          `json:"user_name"`
	Credential *iam.Credential `json:"credential"`
}

// IAMDeleteKeyRequest is the Raft command data for deleting an access key.
type IAMDeleteKeyRequest struct {
	UserName  string `json:"user_name"`
	AccessKey string `json:"access_key"`
}

// IAMCreatePolicyRequest is the Raft command data for creating a policy.
type IAMCreatePolicyRequest struct {
	Policy *iam.Policy `json:"policy"`
}

// IAMDeletePolicyRequest is the Raft command data for deleting a policy.
type IAMDeletePolicyRequest struct {
	PolicyID string `json:"policy_id"`
}

func (ms *ManagerServer) applyIAMCreateUser(data json.RawMessage) interface{} {
	var req IAMCreateUserRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	if req.Identity == nil {
		return fmt.Errorf("identity is required")
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	// Check if user already exists
	if _, exists := ms.state.IAMUsers[req.Identity.Name]; exists {
		return fmt.Errorf("user %s already exists", req.Identity.Name)
	}

	ms.state.AddIAMUser(req.Identity)

	logger.Debug().
		Str("user", req.Identity.Name).
		Int("credentials", len(req.Identity.Credentials)).
		Uint64("iam_version", ms.state.IAMVersion).
		Msg("IAM user created via Raft")

	return nil
}

func (ms *ManagerServer) applyIAMUpdateUser(data json.RawMessage) interface{} {
	var req IAMCreateUserRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	if req.Identity == nil {
		return fmt.Errorf("identity is required")
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	// Check if user exists
	if _, exists := ms.state.IAMUsers[req.Identity.Name]; !exists {
		return fmt.Errorf("user %s not found", req.Identity.Name)
	}

	ms.state.UpdateIAMUser(req.Identity)

	logger.Debug().
		Str("user", req.Identity.Name).
		Uint64("iam_version", ms.state.IAMVersion).
		Msg("IAM user updated via Raft")

	return nil
}

func (ms *ManagerServer) applyIAMDeleteUser(data json.RawMessage) interface{} {
	var req IAMDeleteUserRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	if _, exists := ms.state.IAMUsers[req.UserName]; !exists {
		return fmt.Errorf("user %s not found", req.UserName)
	}

	ms.state.RemoveIAMUser(req.UserName)

	logger.Debug().
		Str("user", req.UserName).
		Uint64("iam_version", ms.state.IAMVersion).
		Msg("IAM user deleted via Raft")

	return nil
}

func (ms *ManagerServer) applyIAMCreateKey(data json.RawMessage) interface{} {
	var req IAMCreateKeyRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	identity, exists := ms.state.IAMUsers[req.UserName]
	if !exists {
		return fmt.Errorf("user %s not found", req.UserName)
	}

	// Check for duplicate access key
	for _, cred := range identity.Credentials {
		if cred.AccessKey == req.Credential.AccessKey {
			return fmt.Errorf("access key %s already exists", req.Credential.AccessKey)
		}
	}

	// Add the new credential
	identity.Credentials = append(identity.Credentials, req.Credential)

	// Update indexes
	ms.state.IAMCredentialIndex[req.Credential.AccessKey] = req.UserName
	ms.state.IAMVersion++

	logger.Debug().
		Str("user", req.UserName).
		Str("access_key", req.Credential.AccessKey).
		Uint64("iam_version", ms.state.IAMVersion).
		Msg("IAM access key created via Raft")

	return nil
}

func (ms *ManagerServer) applyIAMDeleteKey(data json.RawMessage) interface{} {
	var req IAMDeleteKeyRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	identity, exists := ms.state.IAMUsers[req.UserName]
	if !exists {
		return fmt.Errorf("user %s not found", req.UserName)
	}

	// Find and remove the credential
	found := false
	for i, cred := range identity.Credentials {
		if cred.AccessKey == req.AccessKey {
			identity.Credentials = append(identity.Credentials[:i], identity.Credentials[i+1:]...)
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("access key %s not found for user %s", req.AccessKey, req.UserName)
	}

	// Update indexes
	delete(ms.state.IAMCredentialIndex, req.AccessKey)
	ms.state.IAMVersion++

	logger.Debug().
		Str("user", req.UserName).
		Str("access_key", req.AccessKey).
		Uint64("iam_version", ms.state.IAMVersion).
		Msg("IAM access key deleted via Raft")

	return nil
}

func (ms *ManagerServer) applyIAMCreatePolicy(data json.RawMessage) interface{} {
	var req IAMCreatePolicyRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	if req.Policy == nil {
		return fmt.Errorf("policy is required")
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	if _, exists := ms.state.IAMPolicies[req.Policy.ID]; exists {
		return fmt.Errorf("policy %s already exists", req.Policy.ID)
	}

	ms.state.AddIAMPolicy(req.Policy)

	logger.Debug().
		Str("policy", req.Policy.ID).
		Uint64("iam_version", ms.state.IAMVersion).
		Msg("IAM policy created via Raft")

	return nil
}

func (ms *ManagerServer) applyIAMDeletePolicy(data json.RawMessage) interface{} {
	var req IAMDeletePolicyRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	if _, exists := ms.state.IAMPolicies[req.PolicyID]; !exists {
		return fmt.Errorf("policy %s not found", req.PolicyID)
	}

	ms.state.RemoveIAMPolicy(req.PolicyID)

	logger.Debug().
		Str("policy", req.PolicyID).
		Uint64("iam_version", ms.state.IAMVersion).
		Msg("IAM policy deleted via Raft")

	return nil
}

// ===== FEDERATION APPLY HANDLERS =====

func (ms *ManagerServer) applyRegisterFederation(data json.RawMessage) interface{} {
	var req FederationRegistration
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	// Create or update collection with federation mode
	col, exists := ms.state.Collections[req.LocalBucket]
	if !exists {
		// Create new collection for the federated bucket
		col = &manager_pb.Collection{
			Name:       req.LocalBucket,
			BucketMode: req.Mode,
			CreatedAt:  timestamppb.New(time.Unix(0, req.RegisteredAt)),
		}
		ms.state.Collections[req.LocalBucket] = col
		ms.state.AddCollectionToIndexes(col)
	} else {
		// Update existing collection's mode
		col.BucketMode = req.Mode
	}

	// Store federation config
	ms.state.FederationConfigs[req.LocalBucket] = &FederationInfo{
		External:          req.External,
		ObjectsDiscovered: req.ObjectsDiscovered,
		ObjectsSynced:     0,
		BytesSynced:       0,
		MigrationPaused:   false,
		DualWriteEnabled:  false,
		CreatedAt:         req.RegisteredAt,
		UpdatedAt:         req.RegisteredAt,
	}

	ms.state.CollectionsVersion++

	logger.Debug().
		Str("bucket", req.LocalBucket).
		Str("mode", req.Mode.String()).
		Int64("objects_discovered", req.ObjectsDiscovered).
		Uint64("collections_version", ms.state.CollectionsVersion).
		Msg("Federation registered via Raft")

	return nil
}

func (ms *ManagerServer) applyUnregisterFederation(data json.RawMessage) interface{} {
	var req FederationUnregistration
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	// Update collection mode back to local
	col, exists := ms.state.Collections[req.Bucket]
	if exists {
		col.BucketMode = manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_LOCAL
	}

	// Remove federation config
	delete(ms.state.FederationConfigs, req.Bucket)

	ms.state.CollectionsVersion++

	logger.Debug().
		Str("bucket", req.Bucket).
		Bool("delete_external", req.DeleteExternal).
		Bool("delete_local_data", req.DeleteLocalData).
		Uint64("collections_version", ms.state.CollectionsVersion).
		Msg("Federation unregistered via Raft")

	return nil
}

func (ms *ManagerServer) applySetFederationMode(data json.RawMessage) interface{} {
	var req FederationModeChange
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	col, exists := ms.state.Collections[req.Bucket]
	if !exists {
		return fmt.Errorf("bucket %s not found", req.Bucket)
	}

	oldMode := col.BucketMode
	col.BucketMode = req.NewMode

	// Update federation config if exists
	if fedInfo, exists := ms.state.FederationConfigs[req.Bucket]; exists {
		fedInfo.UpdatedAt = req.ChangedAt
	}

	// If transitioning to LOCAL mode, remove federation config
	if req.NewMode == manager_pb.FederationBucketMode_FEDERATION_BUCKET_MODE_LOCAL {
		delete(ms.state.FederationConfigs, req.Bucket)
	}

	ms.state.CollectionsVersion++

	logger.Debug().
		Str("bucket", req.Bucket).
		Str("old_mode", oldMode.String()).
		Str("new_mode", req.NewMode.String()).
		Uint64("collections_version", ms.state.CollectionsVersion).
		Msg("Federation mode changed via Raft")

	return nil
}

func (ms *ManagerServer) applyPauseFederation(data json.RawMessage) interface{} {
	var req FederationPauseResume
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	fedInfo, exists := ms.state.FederationConfigs[req.Bucket]
	if !exists {
		return fmt.Errorf("bucket %s is not federated", req.Bucket)
	}

	fedInfo.MigrationPaused = true
	fedInfo.UpdatedAt = req.ChangedAt

	logger.Debug().
		Str("bucket", req.Bucket).
		Msg("Federation migration paused via Raft")

	return nil
}

func (ms *ManagerServer) applyResumeFederation(data json.RawMessage) interface{} {
	var req FederationPauseResume
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	fedInfo, exists := ms.state.FederationConfigs[req.Bucket]
	if !exists {
		return fmt.Errorf("bucket %s is not federated", req.Bucket)
	}

	fedInfo.MigrationPaused = false
	fedInfo.UpdatedAt = req.ChangedAt

	logger.Debug().
		Str("bucket", req.Bucket).
		Msg("Federation migration resumed via Raft")

	return nil
}

func (ms *ManagerServer) applySetFederationDualWrite(data json.RawMessage) interface{} {
	var req FederationDualWrite
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	fedInfo, exists := ms.state.FederationConfigs[req.Bucket]
	if !exists {
		return fmt.Errorf("bucket %s is not federated", req.Bucket)
	}

	fedInfo.DualWriteEnabled = req.Enabled
	fedInfo.UpdatedAt = req.ChangedAt

	logger.Debug().
		Str("bucket", req.Bucket).
		Bool("dual_write", req.Enabled).
		Msg("Federation dual-write changed via Raft")

	return nil
}

func (ms *ManagerServer) applyUpdateFederationCredentials(data json.RawMessage) interface{} {
	var req FederationCredentialsUpdate
	if err := json.Unmarshal(data, &req); err != nil {
		return err
	}

	ms.state.Lock()
	defer ms.state.Unlock()

	fedInfo, exists := ms.state.FederationConfigs[req.Bucket]
	if !exists {
		return fmt.Errorf("bucket %s is not federated", req.Bucket)
	}

	fedInfo.External.AccessKeyId = req.AccessKeyID
	fedInfo.External.SecretAccessKey = req.SecretAccessKey
	fedInfo.UpdatedAt = req.UpdatedAt

	logger.Debug().
		Str("bucket", req.Bucket).
		Msg("Federation credentials updated via Raft")

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
	// IAM state
	IAMUsers    map[string]*iam.Identity `json:"iam_users,omitempty"`
	IAMPolicies map[string]*iam.Policy   `json:"iam_policies,omitempty"`
	IAMVersion  uint64                   `json:"iam_version,omitempty"`
	// Federation state
	FederationConfigs map[string]*FederationInfo `json:"federation_configs,omitempty"`
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

	logger.Debug().Msg("Persisted FSM snapshot")
	return nil
}

func (f *fsmSnapshot) Release() {}
