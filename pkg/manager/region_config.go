//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package manager provides the region configuration for multi-region deployments.
// This enterprise feature enables cross-region bucket cache synchronization and
// forwarding of bucket mutations to the primary region.
package manager

import (
	"context"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
)

// RegionConfig holds multi-region configuration for enterprise deployments.
// In a multi-region setup, one region is the primary (source of truth for bucket ownership)
// and secondary regions forward bucket mutations to the primary.
type RegionConfig struct {
	// Name is this region's identifier (e.g., "us-east-1", "eu-west-1")
	Name string `mapstructure:"name"`

	// PrimaryRegions is an ordered list of region names for failover.
	// The first region in the list is the "true" primary when healthy.
	// If the first region is unavailable, requests fail over to the next in the list.
	PrimaryRegions []string `mapstructure:"primary_regions"`

	// Peers contains connection info for all known regions.
	Peers []RegionPeer `mapstructure:"peers"`

	// Cache holds cache synchronization settings.
	Cache RegionCacheConfig `mapstructure:"cache"`
}

// RegionPeer represents a remote region's manager cluster.
type RegionPeer struct {
	// Name is the region identifier (must match a name in PrimaryRegions).
	Name string `mapstructure:"name"`

	// ManagerAddresses are the gRPC addresses of managers in this region.
	// Multiple addresses provide redundancy (any can be used to reach the region).
	ManagerAddresses []string `mapstructure:"manager_addresses"`
}

// RegionCacheConfig holds cache synchronization settings.
type RegionCacheConfig struct {
	// SyncInterval is how often secondary regions sync bucket cache from primary (seconds).
	// Default: 30 seconds.
	SyncInterval int `mapstructure:"sync_interval"`

	// CrossRegionTimeout is the timeout for cross-region gRPC calls (milliseconds).
	// Default: 5000ms.
	CrossRegionTimeout int `mapstructure:"cross_region_timeout"`
}

// IsPrimary returns true if this region is the current primary.
// A region is primary if it's first in the PrimaryRegions list.
func (c *RegionConfig) IsPrimary() bool {
	if c == nil || len(c.PrimaryRegions) == 0 {
		return true // No multi-region config = single region = primary
	}
	return c.PrimaryRegions[0] == c.Name
}

// IsConfigured returns true if multi-region is configured.
func (c *RegionConfig) IsConfigured() bool {
	return c != nil && len(c.PrimaryRegions) > 0 && len(c.Peers) > 0
}

// GetSyncInterval returns the cache sync interval as a Duration.
func (c *RegionConfig) GetSyncInterval() time.Duration {
	if c == nil || c.Cache.SyncInterval <= 0 {
		return 30 * time.Second // Default
	}
	return time.Duration(c.Cache.SyncInterval) * time.Second
}

// GetCrossRegionTimeout returns the cross-region call timeout as a Duration.
func (c *RegionConfig) GetCrossRegionTimeout() time.Duration {
	if c == nil || c.Cache.CrossRegionTimeout <= 0 {
		return 5 * time.Second // Default
	}
	return time.Duration(c.Cache.CrossRegionTimeout) * time.Millisecond
}

// GetPeerAddresses returns the manager addresses for a given region.
// Returns nil if the region is not found.
func (c *RegionConfig) GetPeerAddresses(regionName string) []string {
	if c == nil {
		return nil
	}
	for _, peer := range c.Peers {
		if peer.Name == regionName {
			return peer.ManagerAddresses
		}
	}
	return nil
}

// GetCurrentPrimary returns the first healthy region from the priority list.
// Returns an error if no primary regions are configured.
// The isHealthy function is called for each region to check availability.
func (c *RegionConfig) GetCurrentPrimary(ctx context.Context, isHealthy func(ctx context.Context, region string) bool) (string, error) {
	if c == nil || len(c.PrimaryRegions) == 0 {
		return c.Name, nil // Single-region mode
	}

	for _, region := range c.PrimaryRegions {
		if isHealthy(ctx, region) {
			return region, nil
		}
		logger.Warn().Str("region", region).Msg("region unhealthy, trying next in priority list")
	}

	return "", fmt.Errorf("no healthy primary region available")
}

// Validate checks the configuration for errors.
func (c *RegionConfig) Validate() error {
	if c == nil {
		return nil // Not configured is valid (single-region mode)
	}

	if c.Name == "" {
		return fmt.Errorf("region.name is required when multi-region is configured")
	}

	if len(c.PrimaryRegions) == 0 {
		return fmt.Errorf("region.primary_regions is required when multi-region is configured")
	}

	// Check that all primary regions have peer config
	for _, region := range c.PrimaryRegions {
		if region == c.Name {
			continue // Don't need peer config for self
		}
		if addrs := c.GetPeerAddresses(region); len(addrs) == 0 {
			return fmt.Errorf("no peer config found for primary region %q", region)
		}
	}

	return nil
}
