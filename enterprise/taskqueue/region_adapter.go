//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package taskqueue

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/manager"
)

// RegionConfigAdapter adapts manager.RegionConfig to implement RegionEndpoints.
type RegionConfigAdapter struct {
	config *manager.RegionConfig
}

// NewRegionConfigAdapter creates a new adapter for the region config.
func NewRegionConfigAdapter(config *manager.RegionConfig) *RegionConfigAdapter {
	return &RegionConfigAdapter{config: config}
}

// GetS3Endpoint returns the S3 endpoint for the specified region.
func (a *RegionConfigAdapter) GetS3Endpoint(region string) string {
	if a.config == nil {
		return ""
	}
	return a.config.GetPeerS3Endpoint(region)
}
