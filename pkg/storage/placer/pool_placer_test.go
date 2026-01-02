// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package placer

import (
	"context"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// Helper Functions
// ============================================================================

func createTestPool(id uuid.UUID, name string, weight float64, writable bool) *types.StoragePool {
	return &types.StoragePool{
		ID:       id,
		Name:     name,
		Weight:   weight,
		ReadOnly: !writable,
	}
}

func createTestPoolSet(pools ...*types.StoragePool) *types.PoolSet {
	ps := types.NewPoolSet()
	for _, p := range pools {
		ps.Add(p)
	}
	return ps
}

// ============================================================================
// PoolPlacer Tests
// ============================================================================

func TestNewPoolPlacer(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 0},
	}

	placer := NewPoolPlacer(pools, targets)
	require.NotNil(t, placer)
	assert.NotNil(t, placer.rng)
}

func TestPoolPlacer_SelectPool_SinglePool(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 0},
	}

	placer := NewPoolPlacer(pools, targets)
	ctx := context.Background()

	// Should always return the only pool
	pool, err := placer.SelectPool(ctx)
	require.NoError(t, err)
	assert.Equal(t, pool1ID, pool.ID)
}

func TestPoolPlacer_SelectPoolWithPreference_Preferred(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 1.0, true),
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 0},
		{PoolID: pool2ID, WeightOverride: 0},
	}

	placer := NewPoolPlacer(pools, targets)
	ctx := context.Background()

	// Should return preferred pool
	pool, err := placer.SelectPoolWithPreference(ctx, pool2ID)
	require.NoError(t, err)
	assert.Equal(t, pool2ID, pool.ID)
}

func TestPoolPlacer_SelectPoolWithPreference_PreferredReadOnly(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 1.0, false), // Read-only
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 0},
		{PoolID: pool2ID, WeightOverride: 0},
	}

	placer := NewPoolPlacer(pools, targets)
	ctx := context.Background()

	// Preferred is read-only, should fall back
	pool, err := placer.SelectPoolWithPreference(ctx, pool2ID)
	require.NoError(t, err)
	assert.Equal(t, pool1ID, pool.ID)
}

func TestPoolPlacer_SelectPoolWithPreference_NilPreferred(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 0},
	}

	placer := NewPoolPlacer(pools, targets)
	ctx := context.Background()

	// No preference, should use weighted selection
	pool, err := placer.SelectPoolWithPreference(ctx, uuid.Nil)
	require.NoError(t, err)
	assert.Equal(t, pool1ID, pool.ID)
}

func TestPoolPlacer_SelectPools_Success(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pool3ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 1.0, true),
		createTestPool(pool3ID, "pool3", 1.0, true),
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 0},
		{PoolID: pool2ID, WeightOverride: 0},
		{PoolID: pool3ID, WeightOverride: 0},
	}

	placer := NewPoolPlacer(pools, targets)
	ctx := context.Background()

	selected, err := placer.SelectPools(ctx, 2)
	require.NoError(t, err)
	assert.Len(t, selected, 2)
}

func TestPoolPlacer_SetTargets(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 1.0, true),
	)

	initialTargets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 1.0},
	}

	placer := NewPoolPlacer(pools, initialTargets)

	// Update targets
	newTargets := []types.PoolTarget{
		{PoolID: pool2ID, WeightOverride: 1.0},
	}
	placer.SetTargets(newTargets)

	// Verify targets changed
	assert.Len(t, placer.targets, 1)
	assert.Equal(t, pool2ID, placer.targets[0].PoolID)
}

func TestPoolPlacer_GetPool(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
	)

	placer := NewPoolPlacer(pools, nil)

	// Get existing pool
	pool, exists := placer.GetPool(pool1ID)
	assert.True(t, exists)
	assert.Equal(t, pool1ID, pool.ID)

	// Get non-existing pool
	_, exists = placer.GetPool(uuid.New())
	assert.False(t, exists)
}

func TestPoolPlacer_GetPoolByName(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "my-pool", 1.0, true),
	)

	placer := NewPoolPlacer(pools, nil)

	// Get by name
	pool, exists := placer.GetPoolByName("my-pool")
	assert.True(t, exists)
	assert.Equal(t, pool1ID, pool.ID)

	// Get non-existing name
	_, exists = placer.GetPoolByName("unknown")
	assert.False(t, exists)
}

func TestPoolPlacer_ListPools(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 1.0, true),
	)

	placer := NewPoolPlacer(pools, nil)

	list := placer.ListPools()
	assert.Len(t, list, 2)
}

func TestPoolPlacer_ListWritablePools(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 1.0, false), // Read-only
	)

	placer := NewPoolPlacer(pools, nil)

	list := placer.ListWritablePools()
	assert.Len(t, list, 1)
	assert.Equal(t, pool1ID, list[0].ID)
}

func TestPoolPlacer_WeightDistribution(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 3.0, true),
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 0}, // Uses pool weight: 1.0
		{PoolID: pool2ID, WeightOverride: 0}, // Uses pool weight: 3.0
	}

	placer := NewPoolPlacer(pools, targets)

	dist := placer.WeightDistribution()
	assert.Len(t, dist, 2)

	// pool1: 1.0 / 4.0 = 25%
	// pool2: 3.0 / 4.0 = 75%
	assert.InDelta(t, 25.0, dist[pool1ID], 0.1)
	assert.InDelta(t, 75.0, dist[pool2ID], 0.1)
}

func TestPoolPlacer_WeightDistribution_WithOverrides(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 1.0, true),
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 2.0}, // Override
		{PoolID: pool2ID, WeightOverride: 2.0}, // Override
	}

	placer := NewPoolPlacer(pools, targets)

	dist := placer.WeightDistribution()
	// Both have same weight override, should be 50/50
	assert.InDelta(t, 50.0, dist[pool1ID], 0.1)
	assert.InDelta(t, 50.0, dist[pool2ID], 0.1)
}

func TestPoolPlacer_WeightDistribution_Empty(t *testing.T) {
	t.Parallel()

	pools := createTestPoolSet()
	placer := NewPoolPlacer(pools, nil)

	dist := placer.WeightDistribution()
	assert.Empty(t, dist)
}

func TestPoolPlacer_WeightDistribution_AllReadOnly(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, false), // Read-only
	)

	targets := []types.PoolTarget{
		{PoolID: pool1ID, WeightOverride: 0},
	}

	placer := NewPoolPlacer(pools, targets)

	dist := placer.WeightDistribution()
	assert.Empty(t, dist)
}

// ============================================================================
// ProfilePlacer Tests
// ============================================================================

func TestNewProfilePlacer(t *testing.T) {
	t.Parallel()

	pools := createTestPoolSet()
	placer := NewProfilePlacer(pools)

	require.NotNil(t, placer)
	assert.NotNil(t, placer.profiles)
	assert.NotNil(t, placer.placers)
}

func TestProfilePlacer_AddProfile_Success(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
	)

	placer := NewProfilePlacer(pools)

	profile := &types.StorageProfile{
		Name: "standard",
		Pools: []types.PoolTarget{
			{PoolID: pool1ID, WeightOverride: 0},
		},
	}

	err := placer.AddProfile(profile)
	require.NoError(t, err)

	// Verify profile was added
	p, exists := placer.GetProfile("standard")
	assert.True(t, exists)
	assert.Equal(t, "standard", p.Name)
}

func TestProfilePlacer_AddProfile_UnknownPool(t *testing.T) {
	t.Parallel()

	pools := createTestPoolSet()
	placer := NewProfilePlacer(pools)

	profile := &types.StorageProfile{
		Name: "standard",
		Pools: []types.PoolTarget{
			{PoolID: uuid.New(), WeightOverride: 0}, // Unknown pool
		},
	}

	err := placer.AddProfile(profile)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown pool")
}

func TestProfilePlacer_RemoveProfile(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
	)

	placer := NewProfilePlacer(pools)

	profile := &types.StorageProfile{
		Name: "standard",
		Pools: []types.PoolTarget{
			{PoolID: pool1ID, WeightOverride: 0},
		},
	}

	_ = placer.AddProfile(profile)
	placer.RemoveProfile("standard")

	_, exists := placer.GetProfile("standard")
	assert.False(t, exists)
}

func TestProfilePlacer_GetProfile_NotFound(t *testing.T) {
	t.Parallel()

	pools := createTestPoolSet()
	placer := NewProfilePlacer(pools)

	_, exists := placer.GetProfile("unknown")
	assert.False(t, exists)
}

func TestProfilePlacer_SelectPoolForProfile_Success(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
	)

	placer := NewProfilePlacer(pools)

	profile := &types.StorageProfile{
		Name: "standard",
		Pools: []types.PoolTarget{
			{PoolID: pool1ID, WeightOverride: 0},
		},
	}
	_ = placer.AddProfile(profile)

	ctx := context.Background()
	pool, err := placer.SelectPoolForProfile(ctx, "standard")
	require.NoError(t, err)
	assert.Equal(t, pool1ID, pool.ID)
}

func TestProfilePlacer_SelectPoolForProfile_UnknownProfile(t *testing.T) {
	t.Parallel()

	pools := createTestPoolSet()
	placer := NewProfilePlacer(pools)

	ctx := context.Background()
	_, err := placer.SelectPoolForProfile(ctx, "unknown")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown storage profile")
}

func TestProfilePlacer_SelectPoolsForProfile_Success(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pool2ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
		createTestPool(pool2ID, "pool2", 1.0, true),
	)

	placer := NewProfilePlacer(pools)

	profile := &types.StorageProfile{
		Name: "replicated",
		Pools: []types.PoolTarget{
			{PoolID: pool1ID, WeightOverride: 0},
			{PoolID: pool2ID, WeightOverride: 0},
		},
	}
	_ = placer.AddProfile(profile)

	ctx := context.Background()
	selected, err := placer.SelectPoolsForProfile(ctx, "replicated", 2)
	require.NoError(t, err)
	assert.Len(t, selected, 2)
}

func TestProfilePlacer_SelectPoolsForProfile_UnknownProfile(t *testing.T) {
	t.Parallel()

	pools := createTestPoolSet()
	placer := NewProfilePlacer(pools)

	ctx := context.Background()
	_, err := placer.SelectPoolsForProfile(ctx, "unknown", 2)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown storage profile")
}

func TestProfilePlacer_ListProfiles(t *testing.T) {
	t.Parallel()

	pool1ID := uuid.New()
	pools := createTestPoolSet(
		createTestPool(pool1ID, "pool1", 1.0, true),
	)

	placer := NewProfilePlacer(pools)

	profile1 := &types.StorageProfile{
		Name:  "standard",
		Pools: []types.PoolTarget{{PoolID: pool1ID}},
	}
	profile2 := &types.StorageProfile{
		Name:  "premium",
		Pools: []types.PoolTarget{{PoolID: pool1ID}},
	}
	_ = placer.AddProfile(profile1)
	_ = placer.AddProfile(profile2)

	names := placer.ListProfiles()
	assert.Len(t, names, 2)
	assert.Contains(t, names, "standard")
	assert.Contains(t, names, "premium")
}

func TestProfilePlacer_ListProfiles_Empty(t *testing.T) {
	t.Parallel()

	pools := createTestPoolSet()
	placer := NewProfilePlacer(pools)

	names := placer.ListProfiles()
	assert.Empty(t, names)
}
