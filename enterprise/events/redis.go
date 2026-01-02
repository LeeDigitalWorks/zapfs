//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package events

import (
	"context"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/events"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"

	"github.com/redis/go-redis/v9"
)

// RedisPublisher publishes events to Redis Pub/Sub.
type RedisPublisher struct {
	client  *redis.Client
	channel string // Channel prefix (e.g., "s3:events")
}

// RedisConfig configures the Redis publisher.
type RedisConfig struct {
	// Addr is the Redis server address (e.g., "localhost:6379").
	Addr string

	// Password is the Redis password (optional).
	Password string

	// DB is the Redis database number (default 0).
	DB int

	// Channel is the Pub/Sub channel prefix (default "s3:events").
	// Events are published to "{channel}:{bucket}".
	Channel string

	// DialTimeout is the connection timeout (default 5s).
	DialTimeout time.Duration

	// ReadTimeout is the read timeout (default 3s).
	ReadTimeout time.Duration

	// WriteTimeout is the write timeout (default 3s).
	WriteTimeout time.Duration
}

// DefaultRedisConfig returns a RedisConfig with sensible defaults.
func DefaultRedisConfig(addr string) RedisConfig {
	return RedisConfig{
		Addr:         addr,
		Channel:      "s3:events",
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	}
}

// NewRedisPublisher creates a new Redis publisher.
func NewRedisPublisher(cfg RedisConfig) (*RedisPublisher, error) {
	if cfg.Addr == "" {
		return nil, fmt.Errorf("redis address is required")
	}

	if cfg.Channel == "" {
		cfg.Channel = "s3:events"
	}

	client := redis.NewClient(&redis.Options{
		Addr:         cfg.Addr,
		Password:     cfg.Password,
		DB:           cfg.DB,
		DialTimeout:  cfg.DialTimeout,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		return nil, fmt.Errorf("redis ping failed: %w", err)
	}

	logger.Info().
		Str("addr", cfg.Addr).
		Str("channel", cfg.Channel).
		Msg("redis event publisher connected")

	return &RedisPublisher{
		client:  client,
		channel: cfg.Channel,
	}, nil
}

// Name returns the publisher identifier.
func (p *RedisPublisher) Name() string {
	return "redis"
}

// Publish sends an event to Redis Pub/Sub.
// Events are published to the channel "{prefix}:{bucket}".
func (p *RedisPublisher) Publish(ctx context.Context, bucket string, eventData []byte) error {
	start := time.Now()
	channel := fmt.Sprintf("%s:%s", p.channel, bucket)

	result := p.client.Publish(ctx, channel, eventData)
	if err := result.Err(); err != nil {
		return fmt.Errorf("redis publish: %w", err)
	}

	events.EventsDeliveryDuration.WithLabelValues("redis").Observe(time.Since(start).Seconds())

	logger.Debug().
		Str("channel", channel).
		Int64("subscribers", result.Val()).
		Msg("published event to redis")

	return nil
}

// Close closes the Redis connection.
func (p *RedisPublisher) Close() error {
	if p.client != nil {
		return p.client.Close()
	}
	return nil
}
