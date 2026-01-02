// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package events provides S3 event notification infrastructure.
//
// Community edition:
// - Events are queued via the taskqueue but not delivered
// - Provides the Emitter interface for S3 handlers to call
//
// Enterprise edition:
// - EventHandler processes queued events
// - Redis, Kafka, and Webhook publishers deliver events
package events

import (
	"time"
)

// Config holds event notification configuration.
type Config struct {
	// Enabled controls whether event emission is active.
	// When false, Emitter.Emit() is a no-op.
	Enabled bool `mapstructure:"enabled"`

	// Redis publisher configuration (Enterprise)
	Redis RedisConfig `mapstructure:"redis"`

	// Kafka publisher configuration (Enterprise)
	Kafka KafkaConfig `mapstructure:"kafka"`

	// Webhook publisher configuration (Enterprise)
	Webhook WebhookConfig `mapstructure:"webhook"`
}

// RedisConfig holds Redis publisher settings.
type RedisConfig struct {
	// Enabled activates the Redis publisher.
	Enabled bool `mapstructure:"enabled"`

	// Addr is the Redis server address (e.g., "localhost:6379").
	Addr string `mapstructure:"addr"`

	// Password for Redis authentication (optional).
	Password string `mapstructure:"password"`

	// DB is the Redis database number (default: 0).
	DB int `mapstructure:"db"`

	// Channel is the channel prefix for publishing events.
	// Events are published to "{channel}:{bucket}" (default: "s3:events").
	Channel string `mapstructure:"channel"`

	// PoolSize is the maximum number of connections (default: 10).
	PoolSize int `mapstructure:"pool_size"`
}

// KafkaConfig holds Kafka publisher settings.
type KafkaConfig struct {
	// Enabled activates the Kafka publisher.
	Enabled bool `mapstructure:"enabled"`

	// Brokers is the list of Kafka broker addresses.
	Brokers []string `mapstructure:"brokers"`

	// Topic is the Kafka topic for events (default: "s3-events").
	Topic string `mapstructure:"topic"`

	// RequiredAcks: 0=none, 1=leader, -1=all (default: 1).
	RequiredAcks int `mapstructure:"required_acks"`

	// Compression: "none", "gzip", "snappy", "lz4", "zstd" (default: "snappy").
	Compression string `mapstructure:"compression"`

	// BatchSize is the maximum messages per batch (default: 100).
	BatchSize int `mapstructure:"batch_size"`

	// BatchTimeout is the maximum time to wait for a batch (default: 1s).
	BatchTimeout time.Duration `mapstructure:"batch_timeout"`
}

// WebhookConfig holds Webhook publisher settings.
type WebhookConfig struct {
	// Enabled activates the Webhook publisher.
	Enabled bool `mapstructure:"enabled"`

	// Timeout for HTTP requests (default: 30s).
	Timeout time.Duration `mapstructure:"timeout"`

	// MaxRetries for failed deliveries (default: 3).
	MaxRetries int `mapstructure:"max_retries"`

	// RetryDelay between retry attempts (default: 1s).
	RetryDelay time.Duration `mapstructure:"retry_delay"`

	// UserAgent for HTTP requests (default: "ZapFS/1.0").
	UserAgent string `mapstructure:"user_agent"`
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		Enabled: false,
		Redis: RedisConfig{
			Enabled:  false,
			Addr:     "localhost:6379",
			DB:       0,
			Channel:  "s3:events",
			PoolSize: 10,
		},
		Kafka: KafkaConfig{
			Enabled:      false,
			Topic:        "s3-events",
			RequiredAcks: 1,
			Compression:  "snappy",
			BatchSize:    100,
			BatchTimeout: time.Second,
		},
		Webhook: WebhookConfig{
			Enabled:    false,
			Timeout:    30 * time.Second,
			MaxRetries: 3,
			RetryDelay: time.Second,
			UserAgent:  "ZapFS/1.0",
		},
	}
}

// Validate checks the config and applies defaults for invalid values.
func (c *Config) Validate() {
	// Redis defaults
	if c.Redis.Addr == "" {
		c.Redis.Addr = "localhost:6379"
	}
	if c.Redis.Channel == "" {
		c.Redis.Channel = "s3:events"
	}
	if c.Redis.PoolSize <= 0 {
		c.Redis.PoolSize = 10
	}

	// Kafka defaults
	if c.Kafka.Topic == "" {
		c.Kafka.Topic = "s3-events"
	}
	if c.Kafka.RequiredAcks < -1 || c.Kafka.RequiredAcks > 1 {
		c.Kafka.RequiredAcks = 1
	}
	if c.Kafka.Compression == "" {
		c.Kafka.Compression = "snappy"
	}
	if c.Kafka.BatchSize <= 0 {
		c.Kafka.BatchSize = 100
	}
	if c.Kafka.BatchTimeout <= 0 {
		c.Kafka.BatchTimeout = time.Second
	}

	// Webhook defaults
	if c.Webhook.Timeout <= 0 {
		c.Webhook.Timeout = 30 * time.Second
	}
	if c.Webhook.MaxRetries < 0 {
		c.Webhook.MaxRetries = 3
	}
	if c.Webhook.RetryDelay <= 0 {
		c.Webhook.RetryDelay = time.Second
	}
	if c.Webhook.UserAgent == "" {
		c.Webhook.UserAgent = "ZapFS/1.0"
	}
}

// HasPublishers returns true if at least one publisher is enabled.
func (c *Config) HasPublishers() bool {
	return c.Redis.Enabled || c.Kafka.Enabled || c.Webhook.Enabled
}
