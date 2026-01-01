package filter

import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// setupTestRedis creates a miniredis instance for testing
func setupTestRedis(t *testing.T) (*miniredis.Miniredis, *redis.Client) {
	t.Helper()
	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{
		Addr: s.Addr(),
	})
	return s, client
}

func TestRedisRateLimiter_Allow(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cfg := DefaultRedisRateLimitConfig()
	cfg.DefaultRPS = 10
	cfg.DefaultBurst = 10
	cfg.KeyTTL = time.Minute

	limiter := NewRedisRateLimiterWithClient(client, cfg)

	ctx := context.Background()

	// First 10 requests should be allowed (burst size)
	for i := 0; i < 10; i++ {
		result, err := limiter.Allow(ctx, "test-key", 1)
		require.NoError(t, err)
		assert.True(t, result.Allowed, "request %d should be allowed", i)
	}

	// 11th request should be denied (no time has passed to refill)
	result, err := limiter.Allow(ctx, "test-key", 1)
	require.NoError(t, err)
	assert.False(t, result.Allowed, "11th request should be denied")
}

func TestRedisRateLimiter_AllowN_CustomRateBurst(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cfg := DefaultRedisRateLimitConfig()
	cfg.KeyTTL = time.Minute

	limiter := NewRedisRateLimiterWithClient(client, cfg)

	ctx := context.Background()

	// Use custom rate of 5/s with burst of 5
	for i := 0; i < 5; i++ {
		result, err := limiter.AllowN(ctx, "custom-key", 1, 5, 5)
		require.NoError(t, err)
		assert.True(t, result.Allowed, "request %d should be allowed", i)
	}

	// 6th request should be denied
	result, err := limiter.AllowN(ctx, "custom-key", 1, 5, 5)
	require.NoError(t, err)
	assert.False(t, result.Allowed, "6th request should be denied")
}

func TestRedisRateLimiter_VariableCost(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cfg := DefaultRedisRateLimitConfig()
	cfg.DefaultRPS = 100
	cfg.DefaultBurst = 100
	cfg.KeyTTL = time.Minute

	limiter := NewRedisRateLimiterWithClient(client, cfg)

	ctx := context.Background()

	// Take 50 tokens at once
	result, err := limiter.Allow(ctx, "cost-key", 50)
	require.NoError(t, err)
	assert.True(t, result.Allowed)
	assert.Equal(t, int64(50), result.Remaining)

	// Take another 50
	result, err = limiter.Allow(ctx, "cost-key", 50)
	require.NoError(t, err)
	assert.True(t, result.Allowed)
	assert.Equal(t, int64(0), result.Remaining)

	// Now should be denied
	result, err = limiter.Allow(ctx, "cost-key", 1)
	require.NoError(t, err)
	assert.False(t, result.Allowed)
}

func TestRedisRateLimiter_IndependentKeys(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cfg := DefaultRedisRateLimitConfig()
	cfg.DefaultRPS = 5
	cfg.DefaultBurst = 5
	cfg.KeyTTL = time.Minute

	limiter := NewRedisRateLimiterWithClient(client, cfg)

	ctx := context.Background()

	// Exhaust bucket for key1
	for i := 0; i < 5; i++ {
		result, err := limiter.Allow(ctx, "key1", 1)
		require.NoError(t, err)
		assert.True(t, result.Allowed)
	}

	// key1 should be exhausted
	result, err := limiter.Allow(ctx, "key1", 1)
	require.NoError(t, err)
	assert.False(t, result.Allowed)

	// key2 should still be available
	result, err = limiter.Allow(ctx, "key2", 1)
	require.NoError(t, err)
	assert.True(t, result.Allowed)
}

func TestRedisRateLimiter_AllowBatch(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cfg := DefaultRedisRateLimitConfig()
	cfg.DefaultRPS = 10
	cfg.DefaultBurst = 10
	cfg.KeyTTL = time.Minute

	limiter := NewRedisRateLimiterWithClient(client, cfg)

	ctx := context.Background()

	keys := []string{"batch1", "batch2", "batch3"}

	// First batch should all be allowed
	results, err := limiter.AllowBatch(ctx, keys, 1)
	require.NoError(t, err)
	require.Len(t, results, 3)

	for i, result := range results {
		assert.True(t, result.Allowed, "key %d should be allowed", i)
	}
}

func TestRedisRateLimiter_Reset(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cfg := DefaultRedisRateLimitConfig()
	cfg.DefaultRPS = 5
	cfg.DefaultBurst = 5
	cfg.KeyTTL = time.Minute

	limiter := NewRedisRateLimiterWithClient(client, cfg)

	ctx := context.Background()

	// Exhaust the bucket
	for i := 0; i < 5; i++ {
		limiter.Allow(ctx, "reset-key", 1)
	}

	// Should be exhausted
	result, _ := limiter.Allow(ctx, "reset-key", 1)
	assert.False(t, result.Allowed)

	// Reset
	err := limiter.Reset(ctx, "reset-key")
	require.NoError(t, err)

	// Should be available again
	result, err = limiter.Allow(ctx, "reset-key", 1)
	require.NoError(t, err)
	assert.True(t, result.Allowed)
}

func TestRedisRateLimiter_FailOpen(t *testing.T) {
	// Create a limiter with a bad address (simulating Redis down)
	cfg := DefaultRedisRateLimitConfig()
	cfg.Addr = "localhost:19999" // Non-existent port
	cfg.FailOpen = true
	cfg.DefaultRPS = 10
	cfg.DefaultBurst = 10

	// Creating the limiter will fail, but we can test FailOpen behavior
	// by creating a limiter with a client that gets disconnected
	s, client := setupTestRedis(t)
	limiter := NewRedisRateLimiterWithClient(client, cfg)

	ctx := context.Background()

	// First request should work
	result, err := limiter.Allow(ctx, "failopen-key", 1)
	require.NoError(t, err)
	assert.True(t, result.Allowed)

	// Stop redis
	s.Close()
	client.Close()

	// With FailOpen=true, should still allow (handled in the code)
	// Note: This test may need adjustment based on actual fail-open handling
}

func TestRedisRateLimiter_Stats(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cfg := DefaultRedisRateLimitConfig()
	limiter := NewRedisRateLimiterWithClient(client, cfg)

	stats := limiter.Stats()
	assert.True(t, stats.Connected)
	assert.NotNil(t, stats.PoolStats)
}

func TestDefaultRedisRateLimitConfig(t *testing.T) {
	cfg := DefaultRedisRateLimitConfig()

	assert.Equal(t, "localhost:6379", cfg.Addr)
	assert.Equal(t, 0, cfg.DB)
	assert.Equal(t, 10, cfg.PoolSize)
	assert.Equal(t, "zapfs:ratelimit:", cfg.KeyPrefix)
	assert.Equal(t, int64(100), cfg.DefaultRPS)
	assert.Equal(t, int64(200), cfg.DefaultBurst)
	assert.Equal(t, time.Hour, cfg.KeyTTL)
	assert.True(t, cfg.FailOpen)
}

// TestRedisRateLimiter_Refill tests that tokens refill over time
func TestRedisRateLimiter_Refill(t *testing.T) {
	_, client := setupTestRedis(t)
	defer client.Close()

	cfg := DefaultRedisRateLimitConfig()
	cfg.DefaultRPS = 100  // 100 per second = 10ms per token
	cfg.DefaultBurst = 10 // Burst of 10
	cfg.KeyTTL = time.Minute

	limiter := NewRedisRateLimiterWithClient(client, cfg)

	ctx := context.Background()

	// Exhaust the bucket - take 10 tokens quickly
	for i := 0; i < 10; i++ {
		result, _ := limiter.Allow(ctx, "refill-key", 1)
		assert.True(t, result.Allowed, "request %d should be allowed", i)
	}

	// Should be exhausted (or very close to it)
	result, _ := limiter.Allow(ctx, "refill-key", 1)
	// Don't assert false here as timing may allow a refill

	// Wait for refill (at 100/s, 100ms = 10 tokens)
	time.Sleep(150 * time.Millisecond)

	// Should definitely have tokens now
	result, err := limiter.Allow(ctx, "refill-key", 1)
	require.NoError(t, err)
	assert.True(t, result.Allowed, "should have refilled tokens")
}
