// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package placer

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// RoundRobinPlacer Tests
// ============================================================================

func TestNewRoundRobinPlacer(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 0},
	}

	placer := NewRoundRobinPlacer(backends)
	require.NotNil(t, placer)
	assert.Len(t, placer.backends, 2)
}

func TestRoundRobinPlacer_SelectBackend_RoundRobin(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 0},
		{ID: "b3", TotalBytes: 1000, UsedBytes: 0},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	// Track selections
	counts := make(map[string]int)
	for i := 0; i < 9; i++ {
		b, err := placer.SelectBackend(ctx, 100, "")
		require.NoError(t, err)
		counts[b.ID]++
	}

	// Each backend should be selected 3 times
	assert.Equal(t, 3, counts["b1"])
	assert.Equal(t, 3, counts["b2"])
	assert.Equal(t, 3, counts["b3"])
}

func TestRoundRobinPlacer_SelectBackend_PreferredBackend(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 0},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	// Selecting with preferred should return preferred backend
	b, err := placer.SelectBackend(ctx, 100, "b2")
	require.NoError(t, err)
	assert.Equal(t, "b2", b.ID)

	// Second call should still prefer b2
	b, err = placer.SelectBackend(ctx, 100, "b2")
	require.NoError(t, err)
	assert.Equal(t, "b2", b.ID)
}

func TestRoundRobinPlacer_SelectBackend_PreferredNotAvailable(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 900, ReadOnly: true}, // Read only
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	// Preferred b2 is read-only, should fall back to round-robin
	b, err := placer.SelectBackend(ctx, 100, "b2")
	require.NoError(t, err)
	assert.Equal(t, "b1", b.ID)
}

func TestRoundRobinPlacer_SelectBackend_InsufficientSpace(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 950}, // Only 50 bytes free
		{ID: "b2", TotalBytes: 1000, UsedBytes: 800}, // 200 bytes free
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	// Need 100 bytes, b1 doesn't have enough
	b, err := placer.SelectBackend(ctx, 100, "")
	require.NoError(t, err)
	assert.Equal(t, "b2", b.ID)
}

func TestRoundRobinPlacer_SelectBackend_NoBackends(t *testing.T) {
	t.Parallel()

	placer := NewRoundRobinPlacer(nil)
	ctx := context.Background()

	_, err := placer.SelectBackend(ctx, 100, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no backends available")
}

func TestRoundRobinPlacer_SelectBackend_AllReadOnly(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0, ReadOnly: true},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 0, ReadOnly: true},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	_, err := placer.SelectBackend(ctx, 100, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no backends available")
}

func TestRoundRobinPlacer_SelectBackend_AllFull(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 1000},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 990},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	_, err := placer.SelectBackend(ctx, 100, "")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "100 bytes free")
}

func TestRoundRobinPlacer_SelectBackends_Success(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 0},
		{ID: "b3", TotalBytes: 1000, UsedBytes: 0},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	selected, err := placer.SelectBackends(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, selected, 2)

	// Selected backends should be different
	assert.NotEqual(t, selected[0].ID, selected[1].ID)
}

func TestRoundRobinPlacer_SelectBackends_NotEnough(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	_, err := placer.SelectBackends(ctx, 3)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "need 3 backends but only 1 available")
}

func TestRoundRobinPlacer_SelectBackends_SkipsReadOnly(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0, ReadOnly: true},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 0},
		{ID: "b3", TotalBytes: 1000, UsedBytes: 0},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	selected, err := placer.SelectBackends(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, selected, 2)

	// b1 (read-only) should not be selected
	for _, b := range selected {
		assert.NotEqual(t, "b1", b.ID)
	}
}

func TestRoundRobinPlacer_SelectBackends_NotEnoughWritable(t *testing.T) {
	t.Parallel()

	// NOTE: Current implementation allows duplicates when there aren't
	// enough unique writable backends. This test verifies the behavior
	// when ALL backends are read-only.
	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0, ReadOnly: true},
		{ID: "b2", TotalBytes: 1000, UsedBytes: 0, ReadOnly: true},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	_, err := placer.SelectBackends(ctx, 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "could only find 0 writable")
}

func TestRoundRobinPlacer_Refresh(t *testing.T) {
	t.Parallel()

	initial := []*types.Backend{
		{ID: "b1", TotalBytes: 1000, UsedBytes: 0},
	}

	placer := NewRoundRobinPlacer(initial)

	newBackends := []*types.Backend{
		{ID: "b2", TotalBytes: 2000, UsedBytes: 0},
		{ID: "b3", TotalBytes: 3000, UsedBytes: 0},
	}

	placer.Refresh(newBackends)

	ctx := context.Background()
	b, err := placer.SelectBackend(ctx, 100, "")
	require.NoError(t, err)
	assert.NotEqual(t, "b1", b.ID) // b1 no longer exists
}

func TestRoundRobinPlacer_Concurrent(t *testing.T) {
	t.Parallel()

	backends := []*types.Backend{
		{ID: "b1", TotalBytes: 1000000, UsedBytes: 0},
		{ID: "b2", TotalBytes: 1000000, UsedBytes: 0},
		{ID: "b3", TotalBytes: 1000000, UsedBytes: 0},
	}

	placer := NewRoundRobinPlacer(backends)
	ctx := context.Background()

	var wg sync.WaitGroup
	var counts sync.Map
	var errors int64

	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b, err := placer.SelectBackend(ctx, 100, "")
			if err != nil {
				atomic.AddInt64(&errors, 1)
				return
			}
			val, _ := counts.LoadOrStore(b.ID, new(int64))
			atomic.AddInt64(val.(*int64), 1)
		}()
	}

	wg.Wait()
	assert.Equal(t, int64(0), errors)

	// Verify all backends were selected
	var total int64
	counts.Range(func(_, value interface{}) bool {
		total += atomic.LoadInt64(value.(*int64))
		return true
	})
	assert.Equal(t, int64(100), total)
}

// ============================================================================
// Interface Compliance Tests
// ============================================================================

func TestRoundRobinPlacer_ImplementsPlacer(t *testing.T) {
	t.Parallel()

	var _ Placer = (*RoundRobinPlacer)(nil)
}
