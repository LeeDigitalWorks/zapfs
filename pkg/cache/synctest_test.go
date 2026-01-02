// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCacheExpiry_GetAfterExpiry verifies entries expire correctly.
func TestCacheExpiry_GetAfterExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		expiry := 100 * time.Millisecond

		c := New[string, string](ctx, WithExpiry[string, string](expiry))
		defer c.Stop()

		// Set a value
		c.Set("key1", "value1")

		// Immediately accessible
		val, ok := c.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", val)

		// Wait for half the expiry time - still accessible
		time.Sleep(50 * time.Millisecond)
		val, ok = c.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", val)

		// Wait past expiry (but Get refreshes lastAccess, so need full expiry from last access)
		time.Sleep(expiry + 10*time.Millisecond)
		_, ok = c.Get("key1")
		assert.False(t, ok, "entry should be expired")
	})
}

// TestCacheExpiry_CleanupTimer verifies background cleanup runs on schedule.
func TestCacheExpiry_CleanupTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		expiry := 50 * time.Millisecond

		c := New[string, string](ctx, WithExpiry[string, string](expiry))
		defer c.Stop()

		// Set multiple values
		c.Set("key1", "value1")
		c.Set("key2", "value2")
		c.Set("key3", "value3")

		// All accessible immediately
		_, ok1 := c.Get("key1")
		_, ok2 := c.Get("key2")
		_, ok3 := c.Get("key3")
		assert.True(t, ok1 && ok2 && ok3)

		// Wait for expiry + cleanup cycle to run
		// Cleanup runs after expiry duration
		time.Sleep(expiry + 10*time.Millisecond)

		// Entries should be expired on Get (passive expiry)
		_, ok1 = c.Get("key1")
		_, ok2 = c.Get("key2")
		_, ok3 = c.Get("key3")
		assert.False(t, ok1 || ok2 || ok3, "all entries should be expired")

		// Wait for cleanup to actually remove them from internal map
		time.Sleep(expiry)

		// Verify size is 0 after cleanup
		assert.Equal(t, 0, c.Size())
	})
}

// TestCacheExpiry_AccessRefreshesExpiry verifies that Get refreshes the TTL.
func TestCacheExpiry_AccessRefreshesExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		expiry := 100 * time.Millisecond

		c := New[string, string](ctx, WithExpiry[string, string](expiry))
		defer c.Stop()

		c.Set("key1", "value1")

		// Access repeatedly, keeping it alive
		for range 5 {
			time.Sleep(50 * time.Millisecond) // Less than expiry
			val, ok := c.Get("key1")
			assert.True(t, ok, "entry should still be accessible")
			assert.Equal(t, "value1", val)
		}

		// Total time: 250ms, but entry is still valid because each access refreshes TTL
		val, ok := c.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", val)

		// Now wait without accessing
		time.Sleep(expiry + 10*time.Millisecond)
		_, ok = c.Get("key1")
		assert.False(t, ok, "entry should be expired after no access")
	})
}

// TestCacheExpiry_MultipleEntriesDifferentTimes verifies entries expire independently.
func TestCacheExpiry_MultipleEntriesDifferentTimes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		expiry := 100 * time.Millisecond

		c := New[string, string](ctx, WithExpiry[string, string](expiry))
		defer c.Stop()

		// Set key1 now
		c.Set("key1", "value1")

		// Wait 50ms, then set key2
		time.Sleep(50 * time.Millisecond)
		c.Set("key2", "value2")

		// After another 60ms (110ms total for key1, 60ms for key2)
		time.Sleep(60 * time.Millisecond)

		// key1 should be expired, key2 should still be valid
		_, ok1 := c.Get("key1")
		val2, ok2 := c.Get("key2")

		assert.False(t, ok1, "key1 should be expired")
		assert.True(t, ok2, "key2 should still be valid")
		assert.Equal(t, "value2", val2)
	})
}

// TestCacheNoExpiry verifies cache works without expiry.
func TestCacheNoExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		c := New[string, string](ctx) // No expiry option
		// No Stop() needed - no cleanup timer

		c.Set("key1", "value1")

		// Wait arbitrary time
		time.Sleep(1 * time.Second)

		// Still accessible
		val, ok := c.Get("key1")
		assert.True(t, ok)
		assert.Equal(t, "value1", val)
	})
}

// TestCacheStop verifies Stop() prevents further cleanup.
func TestCacheStop(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		expiry := 50 * time.Millisecond

		c := New[string, string](ctx, WithExpiry[string, string](expiry))

		c.Set("key1", "value1")

		// Stop immediately
		c.Stop()

		// Wait past expiry - no cleanup should run, but Get still checks expiry
		time.Sleep(expiry + 10*time.Millisecond)

		// Get should still report expired (passive check)
		_, ok := c.Get("key1")
		assert.False(t, ok, "entry should be expired on Get")
	})
}

// TestCacheMaxSize verifies LRU eviction.
func TestCacheMaxSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()

		// Small cache: 64 shards * 1 entry per shard = 64 max
		c := New[int, string](ctx, WithMaxSize[int, string](64))

		// Fill cache beyond capacity
		for i := range 100 {
			c.Set(i, "value")
		}

		// Some entries should have been evicted
		assert.Less(t, c.Size(), 100)
	})
}

// TestCacheGetOrLoad verifies load function integration.
func TestCacheGetOrLoad(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		loadCount := 0

		c := New[string, string](ctx,
			WithExpiry[string, string](100*time.Millisecond),
			WithLoadFunc(func(ctx context.Context, key string) (string, error) {
				loadCount++
				return "loaded-" + key, nil
			}),
		)
		defer c.Stop()

		// First access triggers load
		val, err := c.GetOrLoad(ctx, "key1")
		require.NoError(t, err)
		assert.Equal(t, "loaded-key1", val)
		assert.Equal(t, 1, loadCount)

		// Second access uses cache
		val, err = c.GetOrLoad(ctx, "key1")
		require.NoError(t, err)
		assert.Equal(t, "loaded-key1", val)
		assert.Equal(t, 1, loadCount) // Still 1

		// Wait for expiry
		time.Sleep(110 * time.Millisecond)

		// After expiry, should reload
		val, err = c.GetOrLoad(ctx, "key1")
		require.NoError(t, err)
		assert.Equal(t, "loaded-key1", val)
		assert.Equal(t, 2, loadCount)
	})
}

// TestCacheCleanupReschedules verifies cleanup timer reschedules itself.
func TestCacheCleanupReschedules(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := context.Background()
		expiry := 50 * time.Millisecond

		c := New[string, string](ctx, WithExpiry[string, string](expiry))
		defer c.Stop()

		// Set key1, wait for cleanup
		c.Set("key1", "value1")
		time.Sleep(expiry + 10*time.Millisecond)

		// Key1 expired
		_, ok := c.Get("key1")
		assert.False(t, ok)

		// Wait for cleanup to run and reschedule
		time.Sleep(expiry)

		// Set key2 after first cleanup cycle
		c.Set("key2", "value2")
		val, ok := c.Get("key2")
		assert.True(t, ok)
		assert.Equal(t, "value2", val)

		// Wait for second cleanup cycle
		time.Sleep(expiry + 10*time.Millisecond)

		// key2 should now be expired
		_, ok = c.Get("key2")
		assert.False(t, ok)
	})
}
