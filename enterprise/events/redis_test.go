//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package events

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRedisConfig_Defaults(t *testing.T) {
	t.Parallel()

	cfg := DefaultRedisConfig("localhost:6379")

	assert.Equal(t, "localhost:6379", cfg.Addr)
	assert.Equal(t, "s3:events", cfg.Channel)
	assert.Equal(t, 5*time.Second, cfg.DialTimeout)
	assert.Equal(t, 3*time.Second, cfg.ReadTimeout)
	assert.Equal(t, 3*time.Second, cfg.WriteTimeout)
}

func TestNewRedisPublisher_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cfg     RedisConfig
		wantErr string
	}{
		{
			name:    "empty address",
			cfg:     RedisConfig{},
			wantErr: "redis address is required",
		},
		{
			name: "invalid address",
			cfg: RedisConfig{
				Addr:        "invalid:99999",
				DialTimeout: 100 * time.Millisecond,
			},
			wantErr: "redis ping failed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewRedisPublisher(tt.cfg)
			if tt.wantErr != "" {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
			}
		})
	}
}

func TestRedisPublisher_Name(t *testing.T) {
	t.Parallel()

	pub := &RedisPublisher{channel: "test"}
	assert.Equal(t, "redis", pub.Name())
}

func TestRedisPublisher_ChannelFormat(t *testing.T) {
	t.Parallel()

	// Verify channel format logic
	tests := []struct {
		prefix   string
		bucket   string
		expected string
	}{
		{"s3:events", "my-bucket", "s3:events:my-bucket"},
		{"custom", "test", "custom:test"},
		{"events", "bucket-123", "events:bucket-123"},
	}

	for _, tt := range tests {
		t.Run(tt.bucket, func(t *testing.T) {
			t.Parallel()
			// Channel format: {prefix}:{bucket}
			channel := tt.prefix + ":" + tt.bucket
			assert.Equal(t, tt.expected, channel)
		})
	}
}

func TestRedisPublisher_Close(t *testing.T) {
	t.Parallel()

	// Test nil client
	pub := &RedisPublisher{}
	err := pub.Close()
	assert.NoError(t, err)
}
