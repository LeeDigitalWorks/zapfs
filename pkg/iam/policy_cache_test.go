// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// callCounter wraps atomic counters for tracking mock call counts
type callCounter struct {
	userPolicyCalls  atomic.Int64
	groupPolicyCalls atomic.Int64
	userGroupsCalls  atomic.Int64
}

func TestCachedPolicyEvaluator_CachesUserPolicies(t *testing.T) {
	policyStore := NewMockPolicyStore(t)
	groupStore := NewMockGroupStore(t)
	counter := &callCounter{}

	policies := []*Policy{FullAccessPolicy()}
	policyStore.EXPECT().GetUserPolicies(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userPolicyCalls.Add(1) }).
		Return(policies, nil)
	groupStore.EXPECT().GetUserGroups(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userGroupsCalls.Add(1) }).
		Return([]string{}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore,
		WithPolicyCacheTTL(time.Minute),
		WithPolicyCacheSize(100),
	)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// First call should hit the store
	result1 := cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, DecisionAllow, result1)
	assert.Equal(t, int64(1), counter.userPolicyCalls.Load())

	// Second call should use cache
	result2 := cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, DecisionAllow, result2)
	assert.Equal(t, int64(1), counter.userPolicyCalls.Load()) // Still 1, from cache

	// Third call should still use cache
	result3 := cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, DecisionAllow, result3)
	assert.Equal(t, int64(1), counter.userPolicyCalls.Load())
}

func TestCachedPolicyEvaluator_CachesGroupPolicies(t *testing.T) {
	policyStore := NewMockPolicyStore(t)
	groupStore := NewMockGroupStore(t)
	counter := &callCounter{}

	userPolicies := []*Policy{}
	groupPolicies := []*Policy{ReadOnlyPolicy()}

	policyStore.EXPECT().GetUserPolicies(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userPolicyCalls.Add(1) }).
		Return(userPolicies, nil)
	policyStore.EXPECT().GetGroupPolicies(mock.Anything, "developers").
		Run(func(_ context.Context, _ string) { counter.groupPolicyCalls.Add(1) }).
		Return(groupPolicies, nil)
	groupStore.EXPECT().GetUserGroups(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userGroupsCalls.Add(1) }).
		Return([]string{"developers"}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// First call
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), counter.groupPolicyCalls.Load())
	assert.Equal(t, int64(1), counter.userGroupsCalls.Load())

	// Second call should use cache for both groups and group policies
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), counter.groupPolicyCalls.Load())
	assert.Equal(t, int64(1), counter.userGroupsCalls.Load())
}

func TestCachedPolicyEvaluator_InvalidateUser(t *testing.T) {
	policyStore := NewMockPolicyStore(t)
	groupStore := NewMockGroupStore(t)
	counter := &callCounter{}

	policies := []*Policy{FullAccessPolicy()}
	policyStore.EXPECT().GetUserPolicies(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userPolicyCalls.Add(1) }).
		Return(policies, nil)
	groupStore.EXPECT().GetUserGroups(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userGroupsCalls.Add(1) }).
		Return([]string{}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// First call
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), counter.userPolicyCalls.Load())

	// Second call uses cache
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), counter.userPolicyCalls.Load())

	// Invalidate cache
	cpe.InvalidateUser("testuser")

	// Third call should hit store again
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(2), counter.userPolicyCalls.Load())
}

func TestCachedPolicyEvaluator_InvalidateGroup(t *testing.T) {
	policyStore := NewMockPolicyStore(t)
	groupStore := NewMockGroupStore(t)
	counter := &callCounter{}

	policyStore.EXPECT().GetUserPolicies(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userPolicyCalls.Add(1) }).
		Return([]*Policy{}, nil)
	policyStore.EXPECT().GetGroupPolicies(mock.Anything, "admin").
		Run(func(_ context.Context, _ string) { counter.groupPolicyCalls.Add(1) }).
		Return([]*Policy{FullAccessPolicy()}, nil)
	groupStore.EXPECT().GetUserGroups(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userGroupsCalls.Add(1) }).
		Return([]string{"admin"}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// First call
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), counter.groupPolicyCalls.Load())

	// Second call uses cache
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), counter.groupPolicyCalls.Load())

	// Invalidate group cache
	cpe.InvalidateGroup("admin")

	// Third call should hit store for group policies again
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(2), counter.groupPolicyCalls.Load())
}

func TestCachedPolicyEvaluator_InvalidateAll(t *testing.T) {
	policyStore := NewMockPolicyStore(t)
	groupStore := NewMockGroupStore(t)
	counter := &callCounter{}

	policyStore.EXPECT().GetUserPolicies(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userPolicyCalls.Add(1) }).
		Return([]*Policy{FullAccessPolicy()}, nil)
	groupStore.EXPECT().GetUserGroups(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userGroupsCalls.Add(1) }).
		Return([]string{}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// Populate cache
	cpe.Evaluate(context.Background(), identity, evalCtx)
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), counter.userPolicyCalls.Load())

	// Clear all caches
	cpe.InvalidateAll()
	stats := cpe.Stats()
	assert.Equal(t, 0, stats.UserPolicyCacheSize)
	assert.Equal(t, 0, stats.GroupPolicyCacheSize)
	assert.Equal(t, 0, stats.UserGroupsCacheSize)

	// Next call should hit store
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(2), counter.userPolicyCalls.Load())
}

func TestCachedPolicyEvaluator_Stats(t *testing.T) {
	policyStore := NewMockPolicyStore(t)
	groupStore := NewMockGroupStore(t)

	policyStore.EXPECT().GetUserPolicies(mock.Anything, mock.Anything).
		Return([]*Policy{FullAccessPolicy()}, nil)
	policyStore.EXPECT().GetGroupPolicies(mock.Anything, mock.Anything).
		Return([]*Policy{}, nil)
	groupStore.EXPECT().GetUserGroups(mock.Anything, mock.Anything).
		Return([]string{"group1"}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// Evaluate for multiple users
	cpe.Evaluate(context.Background(), &Identity{Name: "user1"}, evalCtx)
	cpe.Evaluate(context.Background(), &Identity{Name: "user2"}, evalCtx)
	cpe.Evaluate(context.Background(), &Identity{Name: "user3"}, evalCtx)

	stats := cpe.Stats()
	assert.Equal(t, 3, stats.UserPolicyCacheSize)
	assert.Equal(t, 1, stats.GroupPolicyCacheSize) // All users in same group
	assert.Equal(t, 3, stats.UserGroupsCacheSize)
}

func TestCachedPolicyEvaluator_WithHooks(t *testing.T) {
	policyStore := NewMockPolicyStore(t)
	groupStore := NewMockGroupStore(t)
	counter := &callCounter{}

	// These won't be called due to hook short-circuit, but we need to set them up
	// with Maybe() to avoid assertion failures
	policyStore.EXPECT().GetUserPolicies(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userPolicyCalls.Add(1) }).
		Return([]*Policy{}, nil).Maybe()
	groupStore.EXPECT().GetUserGroups(mock.Anything, "testuser").
		Run(func(_ context.Context, _ string) { counter.userGroupsCalls.Add(1) }).
		Return([]string{}, nil).Maybe()

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	// Add a hook that always allows
	hookCalled := false
	cpe.hooks = PolicyEvaluatorHooks{
		BeforeEvaluate: func(ctx context.Context, identity *Identity, evalCtx *PolicyEvaluationContext) PolicyDecision {
			hookCalled = true
			return DecisionAllow
		},
	}

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	result := cpe.Evaluate(context.Background(), identity, evalCtx)

	assert.True(t, hookCalled)
	assert.Equal(t, DecisionAllow, result)
	// Policy store should not be called because hook short-circuited
	assert.Equal(t, int64(0), counter.userPolicyCalls.Load())
}
