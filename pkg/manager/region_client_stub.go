//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"errors"

	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
)

// ErrMultiRegionNotAvailable is returned when multi-region features are used in community edition.
var ErrMultiRegionNotAvailable = errors.New("multi-region support requires enterprise license")

// RegionClient is a stub for community edition.
type RegionClient struct{}

// NewRegionClient returns nil in community edition (multi-region not supported).
func NewRegionClient(_ *RegionConfig) (*RegionClient, error) {
	return nil, nil
}

// GetClient always returns an error in community edition.
func (rc *RegionClient) GetClient(_ context.Context, _ string) (interface{}, error) {
	return nil, ErrMultiRegionNotAvailable
}

// IsRegionHealthy always returns false in community edition.
func (rc *RegionClient) IsRegionHealthy(_ context.Context, _ string) bool {
	return false
}

// ForwardCreateCollection returns nil in community edition (handle locally).
func (rc *RegionClient) ForwardCreateCollection(_ context.Context, _ *manager_pb.CreateCollectionRequest) (*manager_pb.CreateCollectionResponse, error) {
	return nil, nil
}

// ForwardDeleteCollection returns nil in community edition (handle locally).
func (rc *RegionClient) ForwardDeleteCollection(_ context.Context, _ *manager_pb.DeleteCollectionRequest) (*manager_pb.DeleteCollectionResponse, error) {
	return nil, nil
}

// ListPrimaryCollections always returns an error in community edition.
func (rc *RegionClient) ListPrimaryCollections(_ context.Context) (manager_pb.ManagerService_ListCollectionsClient, error) {
	return nil, ErrMultiRegionNotAvailable
}

// Close is a no-op in community edition.
func (rc *RegionClient) Close() error {
	return nil
}
