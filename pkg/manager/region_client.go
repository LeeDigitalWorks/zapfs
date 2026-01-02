//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package manager

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// RegionClient manages gRPC connections to remote region managers.
// It uses ManagerClientPool for connection pooling and leader-aware routing
// within each remote region.
type RegionClient struct {
	config  *RegionConfig
	timeout time.Duration

	mu      sync.RWMutex
	clients map[string]*client.ManagerClientPool // region name -> client pool
}

// NewRegionClient creates a new cross-region client.
func NewRegionClient(config *RegionConfig) (*RegionClient, error) {
	if config == nil || !config.IsConfigured() {
		return nil, nil // Not configured = single-region mode
	}

	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid region config: %w", err)
	}

	return &RegionClient{
		config:  config,
		timeout: config.GetCrossRegionTimeout(),
		clients: make(map[string]*client.ManagerClientPool),
	}, nil
}

// GetClient returns a manager client pool for the specified region.
// Creates and caches the connection if not already connected.
func (rc *RegionClient) GetClient(ctx context.Context, regionName string) (*client.ManagerClientPool, error) {
	if rc == nil {
		return nil, fmt.Errorf("region client not initialized")
	}

	// Fast path: check cache
	rc.mu.RLock()
	pool, ok := rc.clients[regionName]
	rc.mu.RUnlock()
	if ok {
		return pool, nil
	}

	// Slow path: create connection pool
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// Double-check after acquiring write lock
	if pool, ok := rc.clients[regionName]; ok {
		return pool, nil
	}

	addrs := rc.config.GetPeerAddresses(regionName)
	if len(addrs) == 0 {
		return nil, fmt.Errorf("no addresses configured for region %q", regionName)
	}

	// Create a ManagerClientPool for the region
	// This provides connection pooling, retries, and leader-aware routing within the region
	pool = client.NewManagerClientPool(client.ManagerClientPoolConfig{
		SeedAddrs:      addrs,
		DialTimeout:    5 * time.Second,
		RequestTimeout: rc.timeout,
		MaxRetries:     2,
	})

	// Test connection with Ping
	pingCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	_, err := pool.Ping(pingCtx, &manager_pb.PingRequest{})
	cancel()

	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("failed to connect to region %q: %w", regionName, err)
	}

	rc.clients[regionName] = pool
	logger.Info().Str("region", regionName).Strs("addrs", addrs).Msg("connected to remote region manager cluster")
	return pool, nil
}

// IsRegionHealthy checks if the given region is reachable.
func (rc *RegionClient) IsRegionHealthy(ctx context.Context, regionName string) bool {
	if rc == nil {
		return false
	}

	// For our own region, always return true
	if regionName == rc.config.Name {
		return true
	}

	pool, err := rc.GetClient(ctx, regionName)
	if err != nil {
		return false
	}

	// Use a short timeout for health checks
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	// Simple health check using Ping RPC
	_, err = pool.Ping(ctx, &manager_pb.PingRequest{})
	return err == nil
}

// ForwardCreateCollection forwards a CreateCollection request to the primary region.
// Returns (nil, nil) if this region is the primary (signal to handle locally).
func (rc *RegionClient) ForwardCreateCollection(ctx context.Context, req *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
	if rc == nil || !rc.config.IsConfigured() {
		return nil, nil // Single-region mode, handle locally
	}

	// Try each region in priority order
	for _, region := range rc.config.PrimaryRegions {
		if region == rc.config.Name {
			// We're in the priority list and all higher-priority regions failed.
			// We become acting primary - signal to handle locally.
			return nil, nil
		}

		client, err := rc.GetClient(ctx, region)
		if err != nil {
			logger.Warn().Str("region", region).Err(err).Msg("cannot connect to region, trying next")
			continue
		}

		// Forward with timeout
		forwardCtx, cancel := context.WithTimeout(ctx, rc.timeout)
		resp, err := client.CreateCollection(forwardCtx, req)
		cancel()

		if err != nil {
			logger.Warn().Str("region", region).Err(err).Msg("forward CreateCollection failed, trying next")
			continue
		}

		logger.Debug().Str("region", region).Str("bucket", req.Name).Bool("success", resp.Success).Msg("forwarded CreateCollection")
		return resp, nil
	}

	return nil, fmt.Errorf("all primary regions unavailable")
}

// ForwardDeleteCollection forwards a DeleteCollection request to the primary region.
// Returns (nil, nil) if this region is the primary (signal to handle locally).
func (rc *RegionClient) ForwardDeleteCollection(ctx context.Context, req *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error) {
	if rc == nil || !rc.config.IsConfigured() {
		return nil, nil // Single-region mode, handle locally
	}

	// Try each region in priority order
	for _, region := range rc.config.PrimaryRegions {
		if region == rc.config.Name {
			return nil, nil // Handle locally
		}

		client, err := rc.GetClient(ctx, region)
		if err != nil {
			logger.Warn().Str("region", region).Err(err).Msg("cannot connect to region, trying next")
			continue
		}

		forwardCtx, cancel := context.WithTimeout(ctx, rc.timeout)
		resp, err := client.DeleteCollection(forwardCtx, req)
		cancel()

		if err != nil {
			logger.Warn().Str("region", region).Err(err).Msg("forward DeleteCollection failed, trying next")
			continue
		}

		logger.Debug().Str("region", region).Str("bucket", req.Name).Bool("success", resp.Success).Msg("forwarded DeleteCollection")
		return resp, nil
	}

	return nil, fmt.Errorf("all primary regions unavailable")
}

// ListPrimaryCollections retrieves all collections from the current primary region.
// Used for cache synchronization.
func (rc *RegionClient) ListPrimaryCollections(ctx context.Context) (manager_pb.ManagerService_ListCollectionsClient, error) {
	if rc == nil || !rc.config.IsConfigured() {
		return nil, fmt.Errorf("region client not configured")
	}

	// Find current primary
	for _, region := range rc.config.PrimaryRegions {
		if region == rc.config.Name {
			return nil, fmt.Errorf("this is the primary region")
		}

		client, err := rc.GetClient(ctx, region)
		if err != nil {
			logger.Warn().Str("region", region).Err(err).Msg("cannot connect to region for sync, trying next")
			continue
		}

		stream, err := client.ListCollections(ctx, &manager_pb.ListCollectionsRequest{})
		if err != nil {
			logger.Warn().Str("region", region).Err(err).Msg("ListCollections failed, trying next")
			continue
		}

		return stream, nil
	}

	return nil, fmt.Errorf("no healthy primary region for sync")
}

// WatchPrimaryCollections opens a long-lived stream to watch collection changes from the primary region.
// Returns the stream and the name of the connected region.
// The caller is responsible for handling reconnection on stream errors.
func (rc *RegionClient) WatchPrimaryCollections(ctx context.Context, req *manager_pb.WatchCollectionsRequest) (manager_pb.ManagerService_WatchCollectionsClient, string, error) {
	if rc == nil || !rc.config.IsConfigured() {
		return nil, "", fmt.Errorf("region client not configured")
	}

	// Try each primary region in priority order
	for _, region := range rc.config.PrimaryRegions {
		if region == rc.config.Name {
			return nil, "", fmt.Errorf("this is the primary region")
		}

		client, err := rc.GetClient(ctx, region)
		if err != nil {
			logger.Warn().Str("region", region).Err(err).Msg("cannot connect to region for watch, trying next")
			continue
		}

		stream, err := client.WatchCollections(ctx, req)
		if err != nil {
			logger.Warn().Str("region", region).Err(err).Msg("WatchCollections failed, trying next")
			continue
		}

		logger.Info().Str("region", region).Msg("connected to primary region for collection watch")
		return stream, region, nil
	}

	return nil, "", fmt.Errorf("no healthy primary region for watch")
}

// Close closes all client connections.
func (rc *RegionClient) Close() error {
	if rc == nil {
		return nil
	}

	rc.mu.Lock()
	defer rc.mu.Unlock()

	var lastErr error
	for name, pool := range rc.clients {
		if err := pool.Close(); err != nil {
			logger.Warn().Str("region", name).Err(err).Msg("error closing region client")
			lastErr = err
		}
	}
	rc.clients = make(map[string]*client.ManagerClientPool)

	return lastErr
}
