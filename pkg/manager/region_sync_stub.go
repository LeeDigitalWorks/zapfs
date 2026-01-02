//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

// RegionSyncer is a stub for community edition.
// Multi-region cache sync requires an enterprise license.
type RegionSyncer struct{}

// NewRegionSyncer returns nil in community edition.
func NewRegionSyncer(_ *ManagerServer) *RegionSyncer {
	return nil
}

// Start is a no-op in community edition.
func (rs *RegionSyncer) Start() {}

// Stop is a no-op in community edition.
func (rs *RegionSyncer) Stop() {}
