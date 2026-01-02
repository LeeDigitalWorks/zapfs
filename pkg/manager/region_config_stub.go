//go:build !enterprise

// Package manager provides stub region configuration for community edition.
// Multi-region support is an enterprise feature.
package manager

import (
	"context"
	"time"
)

// RegionConfig is a stub for community edition.
// Multi-region support requires an enterprise license.
type RegionConfig struct {
	Name           string
	PrimaryRegions []string // Stub field for compatibility
	Peers          []RegionPeer
	Cache          RegionCacheConfig
}

// RegionPeer is a stub for community edition.
type RegionPeer struct {
	Name             string
	ManagerAddresses []string
}

// RegionCacheConfig is a stub for community edition.
type RegionCacheConfig struct {
	SyncInterval       int
	CrossRegionTimeout int
}

// IsPrimary always returns true in community edition (single-region mode).
func (c *RegionConfig) IsPrimary() bool {
	return true
}

// IsConfigured always returns false in community edition.
func (c *RegionConfig) IsConfigured() bool {
	return false
}

// GetSyncInterval returns the default sync interval.
func (c *RegionConfig) GetSyncInterval() time.Duration {
	return 30 * time.Second
}

// GetCrossRegionTimeout returns the default timeout.
func (c *RegionConfig) GetCrossRegionTimeout() time.Duration {
	return 5 * time.Second
}

// GetPeerAddresses always returns nil in community edition.
func (c *RegionConfig) GetPeerAddresses(_ string) []string {
	return nil
}

// GetCurrentPrimary returns this region's name in community edition.
func (c *RegionConfig) GetCurrentPrimary(_ context.Context, _ func(context.Context, string) bool) (string, error) {
	if c == nil {
		return "", nil
	}
	return c.Name, nil
}

// Validate always succeeds in community edition.
func (c *RegionConfig) Validate() error {
	return nil
}
