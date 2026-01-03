// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/btree"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrNotLeader          = status.Errorf(codes.FailedPrecondition, "not the leader")
	ErrRaftNotInitialized = status.Errorf(codes.FailedPrecondition, "raft not initialized")
)

// raftAddrToGrpcAddr converts a Raft address to a gRPC address.
// By convention, the gRPC port is Raft port - 1 (e.g., 8051 -> 8050).
func raftAddrToGrpcAddr(raftAddr string) string {
	// Parse host:port
	lastColon := strings.LastIndex(raftAddr, ":")
	if lastColon == -1 {
		return raftAddr // No port, return as-is
	}

	host := raftAddr[:lastColon]
	portStr := raftAddr[lastColon+1:]

	// Try to parse and decrement port
	var port int
	if _, err := fmt.Sscanf(portStr, "%d", &port); err == nil && port > 0 {
		return fmt.Sprintf("%s:%d", host, port-1)
	}

	return raftAddr // Can't parse, return as-is
}

func (ms *ManagerServer) RegisterService(ctx context.Context, req *manager_pb.RegisterServiceRequest) (*manager_pb.RegisterServiceResponse, error) {
	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Msg("Forwarding RegisterService to leader")
		return ms.leaderForwarder.ForwardRegisterService(ctx, leaderAddr, req)
	}

	// Apply through Raft
	if err := ms.applyCommand(CommandRegisterService, req); err != nil {
		return &manager_pb.RegisterServiceResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	// Get updated state after Raft apply
	ms.state.RLock()
	defer ms.state.RUnlock()

	// Return peer services
	peers := ms.getPeerServices(req.ServiceType)

	return &manager_pb.RegisterServiceResponse{
		Success:         true,
		Message:         "Service registered successfully",
		Version:         ms.state.TopologyVersion,
		PlacementPolicy: ms.state.PlacementPolicy,
		PeerServices:    peers,
	}, nil
}

func (ms *ManagerServer) UnregisterService(ctx context.Context, req *manager_pb.UnregisterServiceRequest) (*manager_pb.UnregisterServiceResponse, error) {
	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Msg("Forwarding UnregisterService to leader")
		return ms.leaderForwarder.ForwardUnregisterService(ctx, leaderAddr, req)
	}

	if err := ms.applyCommand(CommandUnregisterService, req); err != nil {
		return &manager_pb.UnregisterServiceResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	return &manager_pb.UnregisterServiceResponse{
		Success: true,
		Message: "Service unregistered successfully",
	}, nil
}

func (ms *ManagerServer) Heartbeat(ctx context.Context, req *manager_pb.HeartbeatRequest) (*manager_pb.HeartbeatResponse, error) {
	serviceID := deriveServiceID(req.ServiceType, req.Location)

	ms.state.Lock()
	registry := ms.getRegistry(req.ServiceType)

	reg, exists := registry[serviceID]
	if !exists {
		ms.state.Unlock()
		logger.Warn().
			Str("service_id", serviceID).
			Msg("Received heartbeat from unregistered service")
		return &manager_pb.HeartbeatResponse{
			TopologyChanged: false,
			TopologyVersion: ms.state.TopologyVersion,
		}, nil
	}

	reg.LastHeartbeat = time.Now()

	// Check if status change is needed (Offline → Active)
	needsStatusUpdate := reg.Status == ServiceOffline
	ms.state.Unlock()

	// Route status change through Raft for atomicity
	if needsStatusUpdate {
		logger.Info().
			Str("service_id", serviceID).
			Msg("Service came back online - updating via Raft")

		// Check if we're leader
		leaderAddr, err := forwardOrError(ms.raftNode)
		if err != nil {
			logger.Warn().Err(err).Str("service_id", serviceID).Msg("Failed to check leader for status update")
			// Continue anyway - status will be updated on next heartbeat
		} else if leaderAddr != "" {
			// We're not the leader - the actual status update should happen on leader
			// Heartbeats are processed locally, but status changes need leader consensus
			// This is expected: follower heartbeat updates LastHeartbeat locally, but
			// status recovery will only be durable when heartbeat reaches leader
			logger.Debug().Str("leader", leaderAddr).Msg("Not leader, skipping status update (will sync via heartbeat to leader)")
		} else {
			// We are the leader - apply through Raft
			statusUpdate := ServiceStatusUpdate{
				ServiceType: req.ServiceType,
				ServiceID:   serviceID,
				NewStatus:   ServiceActive,
			}
			if err := ms.applyCommand(CommandUpdateServiceStatus, statusUpdate); err != nil {
				logger.Warn().Err(err).Str("service_id", serviceID).Msg("Failed to apply status update via Raft")
			}
		}
	}

	// Re-acquire lock for the rest of the updates
	ms.state.Lock()
	defer ms.state.Unlock()

	// Re-fetch reg in case it was modified
	reg, exists = registry[serviceID]
	if !exists {
		return &manager_pb.HeartbeatResponse{
			TopologyChanged: false,
			TopologyVersion: ms.state.TopologyVersion,
		}, nil
	}

	// Track if we need to update capacity cache
	capacityUpdated := false

	if req.ServiceType == manager_pb.ServiceType_FILE_SERVICE {
		if fileMetadata := req.GetFileService(); fileMetadata != nil {
			if len(fileMetadata.StorageBackends) > 0 {
				reg.StorageBackends = fileMetadata.StorageBackends
				capacityUpdated = true
			}
		}
	}

	// Check for data loss condition from metadata service heartbeats
	if req.ServiceType == manager_pb.ServiceType_METADATA_SERVICE {
		if metadataInfo := req.GetMetadataService(); metadataInfo != nil {
			ms.checkDataLossCondition(serviceID, metadataInfo.BucketCount)
		}
	}

	// Update cached cluster capacity if storage info changed
	if capacityUpdated {
		ms.updateClusterCapacity()
	}

	topologyChanged := reg.LastKnownTopologyVersion < ms.state.TopologyVersion
	if topologyChanged {
		reg.LastKnownTopologyVersion = ms.state.TopologyVersion
	}

	logger.Debug().
		Str("service_id", serviceID).
		Uint64("client_version", req.Version).
		Uint64("current_version", ms.state.TopologyVersion).
		Bool("topology_changed", topologyChanged).
		Msg("Heartbeat received")

	return &manager_pb.HeartbeatResponse{
		TopologyChanged: topologyChanged,
		TopologyVersion: ms.state.TopologyVersion,
		PlacementPolicy: ms.state.PlacementPolicy,
	}, nil
}

// WatchTopology implements server-streaming PUSH-based topology updates.
// Works on any node (leader or follower) - events are broadcast when Raft applies changes.
func (ms *ManagerServer) WatchTopology(req *manager_pb.WatchTopologyRequest, stream manager_pb.ManagerService_WatchTopologyServer) error {
	// Create subscriber channel with buffer
	subID := ms.topoNextSubID.Add(1)
	eventCh := make(chan *manager_pb.TopologyEvent, 100)

	ms.topoSubsMu.Lock()
	ms.topoSubs[subID] = eventCh
	ms.topoSubsMu.Unlock()

	defer func() {
		ms.topoSubsMu.Lock()
		delete(ms.topoSubs, subID)
		close(eventCh)
		ms.topoSubsMu.Unlock()
		logger.Debug().Uint64("sub_id", subID).Msg("Topology subscriber disconnected")
	}()

	logger.Debug().
		Uint64("sub_id", subID).
		Uint64("client_version", req.CurrentVersion).
		Msg("New topology subscriber")

	// Send initial full sync
	if err := ms.sendFullTopology(stream, req.CurrentVersion); err != nil {
		return err
	}

	// Stream updates until client disconnects
	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				return nil // Channel closed, subscriber removed
			}
			if err := stream.Send(event); err != nil {
				logger.Warn().Err(err).Uint64("sub_id", subID).Msg("Failed to send topology event")
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// sendFullTopology sends a FULL_SYNC event with all active services
func (ms *ManagerServer) sendFullTopology(stream manager_pb.ManagerService_WatchTopologyServer, clientVersion uint64) error {
	ms.state.RLock()
	defer ms.state.RUnlock()

	// If client already has latest, send empty full sync
	if clientVersion == ms.state.TopologyVersion {
		return stream.Send(&manager_pb.TopologyEvent{
			Type:     manager_pb.TopologyEvent_FULL_SYNC,
			Version:  ms.state.TopologyVersion,
			Services: nil,
		})
	}

	// Collect all active services
	var services []*manager_pb.ServiceInfo

	for _, reg := range ms.state.FileServices {
		if reg.Status == ServiceActive {
			services = append(services, &manager_pb.ServiceInfo{
				ServiceType:   reg.ServiceType,
				Location:      reg.Location,
				LastHeartbeat: timestamppb.New(reg.LastHeartbeat),
			})
		}
	}

	for _, reg := range ms.state.MetadataServices {
		if reg.Status == ServiceActive {
			services = append(services, &manager_pb.ServiceInfo{
				ServiceType:   reg.ServiceType,
				Location:      reg.Location,
				LastHeartbeat: timestamppb.New(reg.LastHeartbeat),
			})
		}
	}

	logger.Debug().
		Uint64("client_version", clientVersion).
		Uint64("current_version", ms.state.TopologyVersion).
		Int("num_services", len(services)).
		Msg("Sending full topology sync")

	return stream.Send(&manager_pb.TopologyEvent{
		Type:     manager_pb.TopologyEvent_FULL_SYNC,
		Version:  ms.state.TopologyVersion,
		Services: services,
	})
}

// notifyTopologySubscribers broadcasts a topology event to all subscribers.
// Called after Raft Apply on both leader and followers.
func (ms *ManagerServer) notifyTopologySubscribers(eventType manager_pb.TopologyEvent_EventType, services []*manager_pb.ServiceInfo) {
	ms.topoSubsMu.RLock()
	defer ms.topoSubsMu.RUnlock()

	if len(ms.topoSubs) == 0 {
		return
	}

	event := &manager_pb.TopologyEvent{
		Type:     eventType,
		Version:  ms.state.TopologyVersion,
		Services: services,
	}

	for subID, ch := range ms.topoSubs {
		select {
		case ch <- event:
		default:
			logger.Warn().Uint64("sub_id", subID).Msg("Topology subscriber channel full, dropping event")
			TopologyEventsDropped.Inc()
		}
	}
}

// ===== COLLECTION WATCH (Multi-region cache sync) =====

// WatchCollections implements server-streaming PUSH-based collection updates.
// Used by secondary regions to keep their bucket cache synchronized with primary.
func (ms *ManagerServer) WatchCollections(req *manager_pb.WatchCollectionsRequest, stream manager_pb.ManagerService_WatchCollectionsServer) error {
	// Create subscriber channel with buffer
	subID := ms.colNextSubID.Add(1)
	eventCh := make(chan *manager_pb.CollectionEvent, 100)

	ms.colSubsMu.Lock()
	ms.colSubs[subID] = eventCh
	ms.colSubsMu.Unlock()

	defer func() {
		ms.colSubsMu.Lock()
		delete(ms.colSubs, subID)
		close(eventCh)
		ms.colSubsMu.Unlock()
		logger.Debug().Uint64("sub_id", subID).Msg("Collection subscriber disconnected")
	}()

	sinceTime := time.Time{}
	if req.GetSinceTime() != nil {
		sinceTime = req.GetSinceTime().AsTime()
	}

	logger.Debug().
		Uint64("sub_id", subID).
		Time("since_time", sinceTime).
		Msg("New collection subscriber")

	// Send initial sync - collections since the requested time
	if err := ms.sendCollectionSync(stream, sinceTime, req.IncludeTombstoned); err != nil {
		return err
	}

	// Stream updates until client disconnects
	for {
		select {
		case event, ok := <-eventCh:
			if !ok {
				return nil // Channel closed, subscriber removed
			}
			if err := stream.Send(event); err != nil {
				logger.Warn().Err(err).Uint64("sub_id", subID).Msg("Failed to send collection event")
				return err
			}
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// sendCollectionSync sends a FULL_SYNC event with collections since the given time.
func (ms *ManagerServer) sendCollectionSync(stream manager_pb.ManagerService_WatchCollectionsServer, sinceTime time.Time, includeTombstoned bool) error {
	ms.state.RLock()
	defer ms.state.RUnlock()

	var collections []*manager_pb.Collection

	// Use StreamCollectionsSince if we have a since_time, otherwise send all
	ms.StreamCollectionsSince(sinceTime, func(col *manager_pb.Collection) bool {
		if !includeTombstoned && col.DeletePending {
			return true // skip tombstoned
		}
		collections = append(collections, col)
		return true
	})

	return stream.Send(&manager_pb.CollectionEvent{
		Type:        manager_pb.CollectionEvent_FULL_SYNC,
		Collections: collections,
		Version:     ms.state.CollectionsVersion,
		Timestamp:   timestamppb.Now(),
	})
}

// notifyCollectionSubscribers broadcasts a collection event to all subscribers.
// Called after Raft Apply on both leader and followers.
func (ms *ManagerServer) notifyCollectionSubscribers(eventType manager_pb.CollectionEvent_EventType, collections []*manager_pb.Collection) {
	ms.colSubsMu.RLock()
	defer ms.colSubsMu.RUnlock()

	if len(ms.colSubs) == 0 {
		return
	}

	event := &manager_pb.CollectionEvent{
		Type:        eventType,
		Collections: collections,
		Version:     ms.state.CollectionsVersion,
		Timestamp:   timestamppb.Now(),
	}

	for subID, ch := range ms.colSubs {
		select {
		case ch <- event:
		default:
			logger.Warn().Uint64("sub_id", subID).Msg("Collection subscriber channel full, dropping event")
			CollectionEventsDropped.Inc()
		}
	}
}

func (ms *ManagerServer) GetReplicationTargets(ctx context.Context, req *manager_pb.GetReplicationTargetsRequest) (*manager_pb.GetReplicationTargetsResponse, error) {
	ms.state.RLock()
	defer ms.state.RUnlock()

	numReplicas := req.NumReplicas
	if numReplicas == 0 {
		numReplicas = ms.state.PlacementPolicy.NumReplicas
	}

	// Use placement algorithm
	targets := ms.selectReplicationTargets(req.FileSize, numReplicas, req.Tier)

	if len(targets) < int(numReplicas) {
		logger.Warn().
			Int("requested", int(numReplicas)).
			Int("found", len(targets)).
			Msg("Could not find enough replication targets")
	}

	return &manager_pb.GetReplicationTargetsResponse{
		Targets: targets,
	}, nil
}

func (ms *ManagerServer) RaftListClusterServers(ctx context.Context, req *manager_pb.RaftListClusterServersRequest) (*manager_pb.RaftListClusterServersResponse, error) {
	if ms.raftNode == nil {
		return nil, ErrRaftNotInitialized
	}

	config, err := ms.raftNode.GetConfiguration()
	if err != nil {
		return nil, err
	}

	leaderAddr := ms.raftNode.Leader()
	var servers []*manager_pb.RaftListClusterServersResponse_Server
	for _, srv := range config.Servers {
		servers = append(servers, &manager_pb.RaftListClusterServersResponse_Server{
			Id:       string(srv.ID),
			Address:  string(srv.Address),
			IsLeader: string(srv.Address) == leaderAddr,
		})
	}

	return &manager_pb.RaftListClusterServersResponse{
		Servers: servers,
	}, nil
}

func (ms *ManagerServer) RaftAddServer(ctx context.Context, req *manager_pb.RaftAddServerRequest) (*manager_pb.RaftAddServerResponse, error) {
	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Msg("Forwarding RaftAddServer to leader")
		return ms.leaderForwarder.ForwardRaftAddServer(ctx, leaderAddr, req)
	}

	var raftErr error
	if req.IsVoter {
		raftErr = ms.raftNode.AddVoter(req.Id, req.Address, 10*time.Second)
	} else {
		raftErr = ms.raftNode.AddNonvoter(req.Id, req.Address, 10*time.Second)
	}

	if raftErr != nil {
		return &manager_pb.RaftAddServerResponse{
			Success: false,
			Message: raftErr.Error(),
		}, raftErr
	}

	logger.Info().
		Str("id", req.Id).
		Str("address", req.Address).
		Bool("voter", req.IsVoter).
		Msg("Added Raft server")

	return &manager_pb.RaftAddServerResponse{
		Success: true,
		Message: "Server added successfully",
	}, nil
}

func (ms *ManagerServer) RaftRemoveServer(ctx context.Context, req *manager_pb.RaftRemoveServerRequest) (*manager_pb.RaftRemoveServerResponse, error) {
	// Forward to leader if not leader
	leaderAddr, err := forwardOrError(ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Msg("Forwarding RaftRemoveServer to leader")
		return ms.leaderForwarder.ForwardRaftRemoveServer(ctx, leaderAddr, req)
	}

	if err := ms.raftNode.RemoveServer(req.Id, 10*time.Second); err != nil {
		return &manager_pb.RaftRemoveServerResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	logger.Info().Str("id", req.Id).Msg("Removed Raft server")

	return &manager_pb.RaftRemoveServerResponse{
		Success: true,
		Message: "Server removed successfully",
	}, nil
}

func (ms *ManagerServer) Ping(ctx context.Context, req *manager_pb.PingRequest) (*manager_pb.PingResponse, error) {
	now := time.Now()

	return &manager_pb.PingResponse{
		StartTime:   timestamppb.New(now),
		CurrentTime: timestamppb.New(time.Now()),
		StopTime:    timestamppb.New(time.Now()),
		IsLeader:    ms.raftNode != nil && ms.raftNode.IsLeader(),
		Version:     "0.1.0",
	}, nil
}

func (ms *ManagerServer) getPeerServices(serviceType manager_pb.ServiceType) []*manager_pb.ServiceInfo {
	var peers []*manager_pb.ServiceInfo

	registry := ms.getRegistry(serviceType)
	for _, reg := range registry {
		if reg.Status == ServiceActive {
			info := &manager_pb.ServiceInfo{
				ServiceType:   reg.ServiceType,
				Location:      reg.Location,
				LastHeartbeat: timestamppb.New(reg.LastHeartbeat),
			}

			peers = append(peers, info)
		}
	}

	return peers
}

// ===== COLLECTION MANAGEMENT =====

// CreateCollection creates a new storage collection (similar to S3 bucket)
// This is a write operation that forwards to leader if not leader.
// In multi-region mode, secondary regions forward to the primary region.
func (ms *ManagerServer) CreateCollection(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
	// Forward to leader if not leader (within this region)
	leaderAddr, err := forwardOrError(ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Str("bucket", req.Name).Msg("Forwarding CreateCollection to leader")
		return ms.leaderForwarder.ForwardCreateCollection(ctx, leaderAddr, req)
	}

	// Check if bucket creates are frozen due to data loss detection
	ms.collectionsRecoveryRequiredMu.RLock()
	recoveryRequired := ms.collectionsRecoveryRequired
	ms.collectionsRecoveryRequiredMu.RUnlock()
	if recoveryRequired {
		logger.Warn().Str("bucket", req.Name).Msg("CreateCollection rejected: manager in recovery mode")
		return &manager_pb.CreateCollectionResponse{
			Success: false,
			Message: "manager in recovery mode - bucket creates frozen until data reconciliation",
		}, status.Error(codes.FailedPrecondition, "manager in recovery mode - bucket creates frozen until data reconciliation")
	}

	// We are the local leader. Check if we need to forward to primary region.
	// First do a fast local cache check - if bucket exists locally, reject immediately.
	ms.state.RLock()
	if col, exists := ms.state.Collections[req.Name]; exists {
		ms.state.RUnlock()
		return &manager_pb.CreateCollectionResponse{
			Success:    false,
			Message:    "collection already exists",
			Collection: col,
		}, status.Errorf(codes.AlreadyExists, "collection %s already exists", req.Name)
	}
	ms.state.RUnlock()

	// If not primary region, forward to primary region
	if ms.regionClient != nil && !ms.IsPrimaryRegion() {
		logger.Debug().Str("bucket", req.Name).Msg("Forwarding CreateCollection to primary region")
		resp, err := ms.regionClient.ForwardCreateCollection(ctx, req)
		if resp != nil || err != nil {
			// If primary returned a response (success or failure), update local cache on success
			if resp != nil && resp.Success && resp.Collection != nil {
				ms.state.Lock()
				ms.state.Collections[req.Name] = resp.Collection
				ms.addCollectionToIndexes(resp.Collection)
				ms.state.CollectionsVersion++
				ms.state.Unlock()
			}
			return resp, err
		}
		// resp == nil && err == nil means we became acting primary (handle locally)
		logger.Info().Str("bucket", req.Name).Msg("All higher-priority regions unavailable, handling as acting primary")
	}

	// Validate request
	if req.Name == "" {
		return nil, status.Error(codes.InvalidArgument, "collection name is required")
	}
	if req.Owner == "" {
		return nil, status.Error(codes.InvalidArgument, "owner is required")
	}

	// Create collection
	collection := &manager_pb.Collection{
		Name:             req.Name,
		Owner:            req.Owner,
		CreatedAt:        timestamppb.Now(),
		UpdatedAt:        timestamppb.Now(),
		CurrentObjects:   0,
		CurrentSizeBytes: 0,
		Tags:             req.Tags,
		Description:      req.Description,
	}

	// Apply through Raft
	if err := ms.applyCommand(CommandCreateCollection, collection); err != nil {
		return &manager_pb.CreateCollectionResponse{
			Success:    false,
			Message:    err.Error(),
			Collection: collection,
		}, err
	}

	return &manager_pb.CreateCollectionResponse{
		Success:    true,
		Message:    "Collection created successfully",
		Collection: collection,
	}, nil
}

// ListCollections lists all collections, optionally filtered by owner
// This is a read operation and can be served by any node (leader or follower)
func (ms *ManagerServer) ListCollections(req *manager_pb.ListCollectionsRequest, stream manager_pb.ManagerService_ListCollectionsServer) error {
	ms.state.RLock()
	defer ms.state.RUnlock()

	sinceTime := time.Time{}
	if req.GetSinceTime() != nil {
		sinceTime = req.GetSinceTime().AsTime()
	}

	// Stream collections created since the given time
	ms.StreamCollectionsSince(sinceTime, func(col *manager_pb.Collection) bool {
		if !req.IncludeTombstoned && col.DeletePending {
			return true // skip tombstoned
		}

		// Send collection
		if err := stream.Send(col); err != nil {
			logger.Error().
				Err(err).
				Str("collection", col.Name).
				Msg("Failed to send collection in stream")
			return false // stop on error
		}
		return true // continue
	})

	return nil
}

// StreamCollectionsSince streams collections created since a given timestamp
// Used for gRPC streaming API
func (ms *ManagerServer) StreamCollectionsSince(since time.Time, callback func(*manager_pb.Collection) bool) {
	ms.state.RLock()
	defer ms.state.RUnlock()

	// Create pivot item for range scan
	pivot := &collectionTimeItem{createdAt: since}

	// Ascend from pivot - visits items >= pivot in sorted order
	ms.state.CollectionsByTime.AscendGreaterOrEqual(pivot, func(item btree.Item) bool {
		ci := item.(*collectionTimeItem)
		return callback(ci.collection) // false stops iteration
	})
}

// StreamAllCollections streams all collections in time order
func (ms *ManagerServer) StreamAllCollections(callback func(*manager_pb.Collection) bool) {
	ms.state.RLock()
	defer ms.state.RUnlock()

	ms.state.CollectionsByTime.Ascend(func(item btree.Item) bool {
		ci := item.(*collectionTimeItem)
		return callback(ci.collection) // false stops iteration
	})
}

// GetCollection retrieves a specific collection by name
// This is a read operation and can be served by any node
func (ms *ManagerServer) GetCollection(ctx context.Context, req *manager_pb.GetCollectionRequest) (*manager_pb.GetCollectionResponse, error) {
	ms.state.RLock()
	defer ms.state.RUnlock()

	col, exists := ms.state.Collections[req.Name]
	if !exists {
		return nil, status.Errorf(codes.NotFound, "collection %s not found", req.Name)
	}

	return &manager_pb.GetCollectionResponse{
		Collection: col,
	}, nil
}

// DeleteCollection deletes a collection
// This is a write operation that forwards to leader if not leader.
// In multi-region mode, secondary regions forward to the primary region.
func (ms *ManagerServer) DeleteCollection(ctx context.Context, req *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error) {
	// Forward to leader if not leader (within this region)
	leaderAddr, err := forwardOrError(ms.raftNode)
	if err != nil {
		return nil, err
	}
	if leaderAddr != "" {
		logger.Debug().Str("leader", leaderAddr).Str("bucket", req.Name).Msg("Forwarding DeleteCollection to leader")
		return ms.leaderForwarder.ForwardDeleteCollection(ctx, leaderAddr, req)
	}

	// If not primary region, forward to primary region
	if ms.regionClient != nil && !ms.IsPrimaryRegion() {
		logger.Debug().Str("bucket", req.Name).Msg("Forwarding DeleteCollection to primary region")
		resp, err := ms.regionClient.ForwardDeleteCollection(ctx, req)
		if resp != nil || err != nil {
			// If primary returned a response (success or failure), update local cache on success
			if resp != nil && resp.Success {
				ms.state.Lock()
				if col, exists := ms.state.Collections[req.Name]; exists {
					ms.removeCollectionFromIndexes(col)
					delete(ms.state.Collections, req.Name)
					ms.state.CollectionsVersion++
				}
				ms.state.Unlock()
			}
			return resp, err
		}
		// resp == nil && err == nil means we became acting primary (handle locally)
		logger.Info().Str("bucket", req.Name).Msg("All higher-priority regions unavailable, handling as acting primary")
	}

	// Check if exists
	ms.state.RLock()
	col, exists := ms.state.Collections[req.Name]
	if !exists {
		ms.state.RUnlock()
		return nil, status.Errorf(codes.NotFound, "collection %s not found", req.Name)
	}

	// Optional: Check ownership
	if req.Owner != "" && col.Owner != req.Owner {
		ms.state.RUnlock()
		return nil, status.Errorf(codes.PermissionDenied, "not the owner of collection %s", req.Name)
	}
	ms.state.RUnlock()

	// Apply delete through Raft
	deleteReq := struct {
		Name string `json:"name"`
	}{Name: req.Name}

	if err := ms.applyCommand(CommandDeleteCollection, deleteReq); err != nil {
		return &manager_pb.DeleteCollectionResponse{
			Success: false,
			Message: err.Error(),
		}, err
	}

	return &manager_pb.DeleteCollectionResponse{
		Success: true,
		Message: "Collection deleted successfully",
	}, nil
}
