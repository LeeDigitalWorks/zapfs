// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package client

import (
	"context"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/grpc/pool"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"google.golang.org/grpc"
)

// Verify ManagerClientPool implements Manager interface
var _ Manager = (*ManagerClientPool)(nil)

// ManagerClientPool manages connections to manager servers with automatic
// failover and retry. Operations are sent to any available node, and the
// server's LeaderForwarder transparently routes writes to the leader.
type ManagerClientPool struct {
	cluster *pool.ClusterPool[manager_pb.ManagerServiceClient]
	opts    pool.ClusterOptions
}

// ManagerClientPoolConfig holds configuration for ManagerClientPool
type ManagerClientPoolConfig struct {
	// SeedAddrs are initial manager addresses to connect to
	SeedAddrs []string

	// DialTimeout for gRPC connections (default: 5s)
	DialTimeout time.Duration

	// RequestTimeout for individual requests (default: 10s)
	RequestTimeout time.Duration

	// MaxRetries for failed requests (default: 3)
	MaxRetries int

	// InitialBackoff is the initial backoff duration (default: 100ms)
	InitialBackoff time.Duration

	// MaxBackoff is the maximum backoff duration (default: 5s)
	MaxBackoff time.Duration
}

// NewManagerClientPool creates a new manager client pool
func NewManagerClientPool(cfg ManagerClientPoolConfig) *ManagerClientPool {
	// Apply defaults
	opts := pool.DefaultClusterOptions(cfg.SeedAddrs)

	if cfg.DialTimeout > 0 {
		opts.DialTimeout = cfg.DialTimeout
	}
	if cfg.RequestTimeout > 0 {
		opts.RequestTimeout = cfg.RequestTimeout
	}
	if cfg.MaxRetries > 0 {
		opts.MaxRetries = cfg.MaxRetries
	}
	if cfg.InitialBackoff > 0 {
		opts.InitialBackoff = cfg.InitialBackoff
	}
	if cfg.MaxBackoff > 0 {
		opts.MaxBackoff = cfg.MaxBackoff
	}

	// Create cluster pool with manager client factory
	cluster := pool.NewClusterPool(
		managerClientFactory,
		opts,
	)

	return &ManagerClientPool{
		cluster: cluster,
		opts:    opts,
	}
}

// managerClientFactory creates a manager service client from a connection
func managerClientFactory(cc grpc.ClientConnInterface) manager_pb.ManagerServiceClient {
	return manager_pb.NewManagerServiceClient(cc)
}

// Close closes all connections in the pool
func (p *ManagerClientPool) Close() error {
	return p.cluster.Close()
}

// executeOnAny executes an operation on any node with retries
func (p *ManagerClientPool) executeOnAny(ctx context.Context, fn func(client manager_pb.ManagerServiceClient) error) error {
	return p.cluster.ExecuteOnAny(ctx, fn)
}

// UpdateNodes updates the list of known manager nodes.
// This can be called when receiving topology updates.
func (p *ManagerClientPool) UpdateNodes(addrs []string) {
	p.cluster.UpdateNodes(addrs)
}

// ========== Write Operations (Server Forwards to Leader) ==========

// RegisterService registers a service with the manager cluster.
// The server transparently forwards to leader if needed.
func (p *ManagerClientPool) RegisterService(ctx context.Context, req *manager_pb.RegisterServiceRequest) (*manager_pb.RegisterServiceResponse, error) {
	var resp *manager_pb.RegisterServiceResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.RegisterService(reqCtx, req)
		return err
	})
	return resp, err
}

// UnregisterService unregisters a service from the manager cluster.
// The server transparently forwards to leader if needed.
func (p *ManagerClientPool) UnregisterService(ctx context.Context, req *manager_pb.UnregisterServiceRequest) (*manager_pb.UnregisterServiceResponse, error) {
	var resp *manager_pb.UnregisterServiceResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.UnregisterService(reqCtx, req)
		return err
	})
	return resp, err
}

// CreateCollection creates a collection/bucket.
// The server transparently forwards to leader if needed.
func (p *ManagerClientPool) CreateCollection(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
	var resp *manager_pb.CreateCollectionResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.CreateCollection(reqCtx, req)
		return err
	})
	return resp, err
}

// DeleteCollection deletes a collection/bucket.
// The server transparently forwards to leader if needed.
func (p *ManagerClientPool) DeleteCollection(ctx context.Context, req *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error) {
	var resp *manager_pb.DeleteCollectionResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.DeleteCollection(reqCtx, req)
		return err
	})
	return resp, err
}

// RaftAddServer adds a server to the Raft cluster.
// The server transparently forwards to leader if needed.
func (p *ManagerClientPool) RaftAddServer(ctx context.Context, req *manager_pb.RaftAddServerRequest) (*manager_pb.RaftAddServerResponse, error) {
	var resp *manager_pb.RaftAddServerResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.RaftAddServer(reqCtx, req)
		return err
	})
	return resp, err
}

// RaftRemoveServer removes a server from the Raft cluster.
// The server transparently forwards to leader if needed.
func (p *ManagerClientPool) RaftRemoveServer(ctx context.Context, req *manager_pb.RaftRemoveServerRequest) (*manager_pb.RaftRemoveServerResponse, error) {
	var resp *manager_pb.RaftRemoveServerResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.RaftRemoveServer(reqCtx, req)
		return err
	})
	return resp, err
}

// ========== Read Operations (Any Node) ==========

// GetReplicationTargets gets placement targets for data (any node)
func (p *ManagerClientPool) GetReplicationTargets(ctx context.Context, req *manager_pb.GetReplicationTargetsRequest) (*manager_pb.GetReplicationTargetsResponse, error) {
	var resp *manager_pb.GetReplicationTargetsResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.GetReplicationTargets(reqCtx, req)
		return err
	})
	return resp, err
}

// WatchTopology subscribes to topology updates (any node - PUSH-based streaming)
// Returns a streaming client that receives TopologyEvent messages.
// Caller is responsible for handling the stream and reconnection.
func (p *ManagerClientPool) WatchTopology(ctx context.Context, req *manager_pb.WatchTopologyRequest) (manager_pb.ManagerService_WatchTopologyClient, error) {
	client, _, err := p.cluster.GetAny(ctx)
	if err != nil {
		return nil, err
	}
	return client.WatchTopology(ctx, req)
}

// GetCollection gets a collection by name (any node)
func (p *ManagerClientPool) GetCollection(ctx context.Context, req *manager_pb.GetCollectionRequest) (*manager_pb.GetCollectionResponse, error) {
	var resp *manager_pb.GetCollectionResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.GetCollection(reqCtx, req)
		return err
	})
	return resp, err
}

// Ping checks connectivity to a manager node (any node)
func (p *ManagerClientPool) Ping(ctx context.Context, req *manager_pb.PingRequest) (*manager_pb.PingResponse, error) {
	var resp *manager_pb.PingResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.Ping(reqCtx, req)
		return err
	})
	return resp, err
}

// RaftListClusterServers lists Raft cluster servers (any node)
func (p *ManagerClientPool) RaftListClusterServers(ctx context.Context, req *manager_pb.RaftListClusterServersRequest) (*manager_pb.RaftListClusterServersResponse, error) {
	var resp *manager_pb.RaftListClusterServersResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.RaftListClusterServers(reqCtx, req)
		return err
	})
	return resp, err
}

// Heartbeat sends a heartbeat to the manager (any node)
func (p *ManagerClientPool) Heartbeat(ctx context.Context, req *manager_pb.HeartbeatRequest) (*manager_pb.HeartbeatResponse, error) {
	var resp *manager_pb.HeartbeatResponse
	err := p.executeOnAny(ctx, func(client manager_pb.ManagerServiceClient) error {
		reqCtx, cancel := context.WithTimeout(ctx, p.opts.RequestTimeout)
		defer cancel()
		var err error
		resp, err = client.Heartbeat(reqCtx, req)
		return err
	})
	return resp, err
}

// ListCollections streams collections from manager (any node)
// Note: This returns a streaming client, caller is responsible for handling the stream
func (p *ManagerClientPool) ListCollections(ctx context.Context, req *manager_pb.ListCollectionsRequest) (manager_pb.ManagerService_ListCollectionsClient, error) {
	client, _, err := p.cluster.GetAny(ctx)
	if err != nil {
		return nil, err
	}
	return client.ListCollections(ctx, req)
}

// WatchCollections subscribes to collection updates (any node - PUSH-based streaming)
// Returns a streaming client that receives CollectionEvent messages.
// Caller is responsible for handling the stream and reconnection.
func (p *ManagerClientPool) WatchCollections(ctx context.Context, req *manager_pb.WatchCollectionsRequest) (manager_pb.ManagerService_WatchCollectionsClient, error) {
	client, _, err := p.cluster.GetAny(ctx)
	if err != nil {
		return nil, err
	}
	return client.WatchCollections(ctx, req)
}
