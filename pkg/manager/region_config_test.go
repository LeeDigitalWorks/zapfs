//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package manager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegionConfig_IsPrimary(t *testing.T) {
	tests := []struct {
		name     string
		config   *RegionConfig
		expected bool
	}{
		{
			name:     "nil config",
			config:   nil,
			expected: true,
		},
		{
			name:     "empty primary regions",
			config:   &RegionConfig{Name: "us-east-1"},
			expected: true,
		},
		{
			name: "is first in list (primary)",
			config: &RegionConfig{
				Name:           "us-east-1",
				PrimaryRegions: []string{"us-east-1", "eu-west-1"},
			},
			expected: true,
		},
		{
			name: "not first in list (secondary)",
			config: &RegionConfig{
				Name:           "eu-west-1",
				PrimaryRegions: []string{"us-east-1", "eu-west-1"},
			},
			expected: false,
		},
		{
			name: "third in list (secondary)",
			config: &RegionConfig{
				Name:           "ap-southeast-1",
				PrimaryRegions: []string{"us-east-1", "eu-west-1", "ap-southeast-1"},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.IsPrimary()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRegionConfig_IsConfigured(t *testing.T) {
	tests := []struct {
		name     string
		config   *RegionConfig
		expected bool
	}{
		{
			name:     "nil config",
			config:   nil,
			expected: false,
		},
		{
			name:     "empty config",
			config:   &RegionConfig{},
			expected: false,
		},
		{
			name: "no peers",
			config: &RegionConfig{
				Name:           "us-east-1",
				PrimaryRegions: []string{"us-east-1"},
			},
			expected: false,
		},
		{
			name: "fully configured",
			config: &RegionConfig{
				Name:           "eu-west-1",
				PrimaryRegions: []string{"us-east-1", "eu-west-1"},
				Peers: []RegionPeer{
					{Name: "us-east-1", ManagerAddresses: []string{"manager1:8050"}},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.IsConfigured()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRegionConfig_GetSyncInterval(t *testing.T) {
	tests := []struct {
		name     string
		config   *RegionConfig
		expected time.Duration
	}{
		{
			name:     "nil config",
			config:   nil,
			expected: 30 * time.Second,
		},
		{
			name:     "zero interval",
			config:   &RegionConfig{Cache: RegionCacheConfig{SyncInterval: 0}},
			expected: 30 * time.Second,
		},
		{
			name:     "custom interval",
			config:   &RegionConfig{Cache: RegionCacheConfig{SyncInterval: 60}},
			expected: 60 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetSyncInterval()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRegionConfig_GetCrossRegionTimeout(t *testing.T) {
	tests := []struct {
		name     string
		config   *RegionConfig
		expected time.Duration
	}{
		{
			name:     "nil config",
			config:   nil,
			expected: 5 * time.Second,
		},
		{
			name:     "zero timeout",
			config:   &RegionConfig{Cache: RegionCacheConfig{CrossRegionTimeout: 0}},
			expected: 5 * time.Second,
		},
		{
			name:     "custom timeout",
			config:   &RegionConfig{Cache: RegionCacheConfig{CrossRegionTimeout: 10000}},
			expected: 10 * time.Second,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.GetCrossRegionTimeout()
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRegionConfig_GetPeerAddresses(t *testing.T) {
	config := &RegionConfig{
		Name:           "eu-west-1",
		PrimaryRegions: []string{"us-east-1", "eu-west-1"},
		Peers: []RegionPeer{
			{Name: "us-east-1", ManagerAddresses: []string{"manager1:8050", "manager2:8050"}},
			{Name: "eu-west-1", ManagerAddresses: []string{"manager3:8050"}},
		},
	}

	tests := []struct {
		name       string
		regionName string
		expected   []string
	}{
		{
			name:       "existing region",
			regionName: "us-east-1",
			expected:   []string{"manager1:8050", "manager2:8050"},
		},
		{
			name:       "own region",
			regionName: "eu-west-1",
			expected:   []string{"manager3:8050"},
		},
		{
			name:       "non-existing region",
			regionName: "ap-southeast-1",
			expected:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := config.GetPeerAddresses(tt.regionName)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestRegionConfig_GetCurrentPrimary(t *testing.T) {
	config := &RegionConfig{
		Name:           "eu-west-1",
		PrimaryRegions: []string{"us-east-1", "eu-west-1", "ap-southeast-1"},
		Peers: []RegionPeer{
			{Name: "us-east-1", ManagerAddresses: []string{"manager1:8050"}},
			{Name: "ap-southeast-1", ManagerAddresses: []string{"manager2:8050"}},
		},
	}

	tests := []struct {
		name        string
		healthyFn   func(ctx context.Context, region string) bool
		expected    string
		expectError bool
	}{
		{
			name: "first region healthy",
			healthyFn: func(ctx context.Context, region string) bool {
				return region == "us-east-1"
			},
			expected:    "us-east-1",
			expectError: false,
		},
		{
			name: "first unhealthy, second healthy",
			healthyFn: func(ctx context.Context, region string) bool {
				return region == "eu-west-1"
			},
			expected:    "eu-west-1",
			expectError: false,
		},
		{
			name: "all unhealthy",
			healthyFn: func(ctx context.Context, region string) bool {
				return false
			},
			expected:    "",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := config.GetCurrentPrimary(context.Background(), tt.healthyFn)
			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

func TestRegionConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      *RegionConfig
		expectError bool
	}{
		{
			name:        "nil config",
			config:      nil,
			expectError: false,
		},
		{
			name:        "empty name",
			config:      &RegionConfig{PrimaryRegions: []string{"us-east-1"}},
			expectError: true,
		},
		{
			name:        "empty primary regions",
			config:      &RegionConfig{Name: "eu-west-1"},
			expectError: true,
		},
		{
			name: "missing peer config for primary region",
			config: &RegionConfig{
				Name:           "eu-west-1",
				PrimaryRegions: []string{"us-east-1", "eu-west-1"},
				Peers:          []RegionPeer{}, // Missing us-east-1 peer config
			},
			expectError: true,
		},
		{
			name: "valid config",
			config: &RegionConfig{
				Name:           "eu-west-1",
				PrimaryRegions: []string{"us-east-1", "eu-west-1"},
				Peers: []RegionPeer{
					{Name: "us-east-1", ManagerAddresses: []string{"manager1:8050"}},
				},
			},
			expectError: false,
		},
		{
			name: "self is primary (no peer config needed)",
			config: &RegionConfig{
				Name:           "us-east-1",
				PrimaryRegions: []string{"us-east-1", "eu-west-1"},
				Peers: []RegionPeer{
					{Name: "eu-west-1", ManagerAddresses: []string{"manager2:8050"}},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
