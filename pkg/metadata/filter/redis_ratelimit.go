// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog/log"
)

// RedisRateLimiter implements distributed rate limiting using Redis and GCRA.
//
// GCRA (Generic Cell Rate Algorithm) provides smooth rate limiting without
// fixed time windows. It tracks the "theoretical arrival time" (TAT) for
// each key, allowing requests only when the TAT has passed.
//
// This implementation uses a Lua script for atomic operations, ensuring
// correctness under concurrent access from multiple servers.
//
// Usage:
//
//	limiter := NewRedisRateLimiter(redisClient, RedisRateLimitConfig{
//	    KeyPrefix: "ratelimit:",
//	    DefaultRPS: 100,
//	    DefaultBurst: 200,
//	})
//
//	allowed, err := limiter.Allow(ctx, "user:123", 1)
type RedisRateLimiter struct {
	client *redis.Client
	config RedisRateLimitConfig
}

// RedisRateLimitConfig configures the Redis rate limiter.
type RedisRateLimitConfig struct {
	// Enabled activates distributed rate limiting via Redis.
	// When false, only local in-memory rate limiting is used.
	Enabled bool `mapstructure:"enabled"`

	// Redis connection settings
	Addr     string `mapstructure:"addr"`
	Password string `mapstructure:"password"`
	DB       int    `mapstructure:"db"`
	PoolSize int    `mapstructure:"pool_size"`

	// Rate limiting settings
	KeyPrefix    string        `mapstructure:"key_prefix"`
	DefaultRPS   int64         `mapstructure:"default_rps"`
	DefaultBurst int64         `mapstructure:"default_burst"`
	KeyTTL       time.Duration `mapstructure:"key_ttl"`

	// Fallback behavior when Redis is unavailable
	FailOpen bool `mapstructure:"fail_open"`
}

// DefaultRedisRateLimitConfig returns sensible defaults.
func DefaultRedisRateLimitConfig() RedisRateLimitConfig {
	return RedisRateLimitConfig{
		Addr:         "localhost:6379",
		DB:           0,
		PoolSize:     10,
		KeyPrefix:    "zapfs:ratelimit:",
		DefaultRPS:   100,
		DefaultBurst: 200,
		KeyTTL:       time.Hour,
		FailOpen:     true, // Allow requests if Redis is down
	}
}

// NewRedisRateLimiter creates a new Redis-backed rate limiter.
func NewRedisRateLimiter(cfg RedisRateLimitConfig) (*RedisRateLimiter, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.Addr,
		Password: cfg.Password,
		DB:       cfg.DB,
		PoolSize: cfg.PoolSize,
	})

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis connection failed: %w", err)
	}

	return &RedisRateLimiter{
		client: client,
		config: cfg,
	}, nil
}

// NewRedisRateLimiterWithClient creates a rate limiter with an existing Redis client.
func NewRedisRateLimiterWithClient(client *redis.Client, cfg RedisRateLimitConfig) *RedisRateLimiter {
	return &RedisRateLimiter{
		client: client,
		config: cfg,
	}
}

// gcraScript is a Lua script implementing GCRA for atomic rate limiting.
// Returns: allowed (1 or 0), remaining tokens, reset time in ms
//
// GCRA uses a "theoretical arrival time" (TAT) approach:
// - TAT represents when the bucket will be full again
// - Each request moves TAT forward by the emission interval (1/rate seconds)
// - Requests are allowed if TAT <= now + burst_offset
var gcraScript = redis.NewScript(`
local key = KEYS[1]
local now = tonumber(ARGV[1])        -- Current time in microseconds
local burst = tonumber(ARGV[2])      -- Burst size (max tokens)
local rate = tonumber(ARGV[3])       -- Tokens per second
local cost = tonumber(ARGV[4])       -- Cost of this request (tokens)
local ttl = tonumber(ARGV[5])        -- Key TTL in seconds

-- Emission interval: time between token additions (microseconds per token)
local emission_interval = 1000000 / rate

-- Burst offset: maximum time TAT can be ahead of now
local burst_offset = burst * emission_interval

-- Get current TAT (theoretical arrival time)
local tat = redis.call("GET", key)
if tat then
    tat = tonumber(tat)
else
    tat = now
end

-- Calculate new TAT if we allow this request
local new_tat = tat + (cost * emission_interval)

-- Check if request is allowed
-- Request is allowed if new_tat <= now + burst_offset
local allow_at = now + burst_offset
if new_tat > allow_at then
    -- Request denied - return current state
    local remaining = math.max(0, math.floor((allow_at - tat) / emission_interval))
    local reset_after = math.ceil((tat - now) / 1000) -- Convert to ms
    return {0, remaining, reset_after}
end

-- Request allowed - update TAT
-- If tat is in the past, set it to now before adding cost
if tat < now then
    new_tat = now + (cost * emission_interval)
end

redis.call("SET", key, new_tat, "EX", ttl)

-- Calculate remaining tokens
local remaining = math.max(0, math.floor((allow_at - new_tat) / emission_interval))
local reset_after = math.ceil((new_tat - now) / 1000) -- Convert to ms

return {1, remaining, reset_after}
`)

// RateLimitResult contains the result of a rate limit check.
type RateLimitResult struct {
	Allowed    bool
	Remaining  int64         // Remaining tokens
	ResetAfter time.Duration // Time until bucket refills
}

// Allow checks if a request should be allowed for the given key.
// Returns the result and any error (Redis connection issues, etc.).
//
// The cost parameter allows variable-cost operations (e.g., bandwidth limiting
// where cost = bytes transferred).
func (r *RedisRateLimiter) Allow(ctx context.Context, key string, cost int64) (RateLimitResult, error) {
	return r.AllowN(ctx, key, cost, r.config.DefaultRPS, r.config.DefaultBurst)
}

// AllowN checks rate limit with custom rate and burst values.
func (r *RedisRateLimiter) AllowN(ctx context.Context, key string, cost, rate, burst int64) (RateLimitResult, error) {
	fullKey := r.config.KeyPrefix + key
	now := time.Now().UnixMicro()
	ttlSeconds := int64(r.config.KeyTTL.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 3600 // Default 1 hour
	}

	result, err := gcraScript.Run(ctx, r.client, []string{fullKey},
		now, burst, rate, cost, ttlSeconds,
	).Int64Slice()

	if err != nil {
		log.Warn().Err(err).Str("key", key).Msg("Redis rate limit check failed")
		if r.config.FailOpen {
			return RateLimitResult{Allowed: true, Remaining: burst}, nil
		}
		return RateLimitResult{Allowed: false}, err
	}

	return RateLimitResult{
		Allowed:    result[0] == 1,
		Remaining:  result[1],
		ResetAfter: time.Duration(result[2]) * time.Millisecond,
	}, nil
}

// AllowBatch checks multiple keys atomically using pipelining.
// Returns results in the same order as keys.
func (r *RedisRateLimiter) AllowBatch(ctx context.Context, keys []string, cost int64) ([]RateLimitResult, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	now := time.Now().UnixMicro()
	ttlSeconds := int64(r.config.KeyTTL.Seconds())
	if ttlSeconds < 1 {
		ttlSeconds = 3600
	}

	pipe := r.client.Pipeline()
	cmds := make([]*redis.Cmd, len(keys))

	for i, key := range keys {
		fullKey := r.config.KeyPrefix + key
		cmds[i] = gcraScript.Run(ctx, pipe, []string{fullKey},
			now, r.config.DefaultBurst, r.config.DefaultRPS, cost, ttlSeconds,
		)
	}

	_, err := pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		log.Warn().Err(err).Msg("Redis batch rate limit check failed")
		if r.config.FailOpen {
			results := make([]RateLimitResult, len(keys))
			for i := range results {
				results[i] = RateLimitResult{Allowed: true, Remaining: r.config.DefaultBurst}
			}
			return results, nil
		}
		return nil, err
	}

	results := make([]RateLimitResult, len(keys))
	for i, cmd := range cmds {
		vals, err := cmd.Int64Slice()
		if err != nil {
			if r.config.FailOpen {
				results[i] = RateLimitResult{Allowed: true, Remaining: r.config.DefaultBurst}
			} else {
				results[i] = RateLimitResult{Allowed: false}
			}
			continue
		}
		results[i] = RateLimitResult{
			Allowed:    vals[0] == 1,
			Remaining:  vals[1],
			ResetAfter: time.Duration(vals[2]) * time.Millisecond,
		}
	}

	return results, nil
}

// Reset clears the rate limit state for a key.
func (r *RedisRateLimiter) Reset(ctx context.Context, key string) error {
	fullKey := r.config.KeyPrefix + key
	return r.client.Del(ctx, fullKey).Err()
}

// Close closes the Redis connection.
func (r *RedisRateLimiter) Close() error {
	return r.client.Close()
}

// Stats returns current statistics about the rate limiter.
type RedisRateLimiterStats struct {
	Connected bool
	PoolStats *redis.PoolStats
}

// Stats returns current connection statistics.
func (r *RedisRateLimiter) Stats() RedisRateLimiterStats {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	connected := r.client.Ping(ctx).Err() == nil
	poolStats := r.client.PoolStats()

	return RedisRateLimiterStats{
		Connected: connected,
		PoolStats: poolStats,
	}
}
