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

// MockPolicyStoreForCache implements PolicyStore for testing cache behavior
type MockPolicyStoreForCache struct {
	mock.Mock
	userPolicyCalls  atomic.Int64
	groupPolicyCalls atomic.Int64
}

func (m *MockPolicyStoreForCache) GetUserPolicies(ctx context.Context, username string) ([]*Policy, error) {
	m.userPolicyCalls.Add(1)
	args := m.Called(ctx, username)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*Policy), args.Error(1)
}

func (m *MockPolicyStoreForCache) GetGroupPolicies(ctx context.Context, groupName string) ([]*Policy, error) {
	m.groupPolicyCalls.Add(1)
	args := m.Called(ctx, groupName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]*Policy), args.Error(1)
}

func (m *MockPolicyStoreForCache) GetRolePolicies(ctx context.Context, roleName string) ([]*Policy, error) {
	return nil, nil
}

// MockGroupStoreForCache implements GroupStore for testing
type MockGroupStoreForCache struct {
	mock.Mock
	calls atomic.Int64
}

func (m *MockGroupStoreForCache) GetUserGroups(ctx context.Context, username string) ([]string, error) {
	m.calls.Add(1)
	args := m.Called(ctx, username)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).([]string), args.Error(1)
}

func TestCachedPolicyEvaluator_CachesUserPolicies(t *testing.T) {
	policyStore := &MockPolicyStoreForCache{}
	groupStore := &MockGroupStoreForCache{}

	policies := []*Policy{FullAccessPolicy()}
	policyStore.On("GetUserPolicies", mock.Anything, "testuser").Return(policies, nil)
	groupStore.On("GetUserGroups", mock.Anything, "testuser").Return([]string{}, nil)

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
	assert.Equal(t, int64(1), policyStore.userPolicyCalls.Load())

	// Second call should use cache
	result2 := cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, DecisionAllow, result2)
	assert.Equal(t, int64(1), policyStore.userPolicyCalls.Load()) // Still 1, from cache

	// Third call should still use cache
	result3 := cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, DecisionAllow, result3)
	assert.Equal(t, int64(1), policyStore.userPolicyCalls.Load())
}

func TestCachedPolicyEvaluator_CachesGroupPolicies(t *testing.T) {
	policyStore := &MockPolicyStoreForCache{}
	groupStore := &MockGroupStoreForCache{}

	userPolicies := []*Policy{}
	groupPolicies := []*Policy{ReadOnlyPolicy()}

	policyStore.On("GetUserPolicies", mock.Anything, "testuser").Return(userPolicies, nil)
	policyStore.On("GetGroupPolicies", mock.Anything, "developers").Return(groupPolicies, nil)
	groupStore.On("GetUserGroups", mock.Anything, "testuser").Return([]string{"developers"}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// First call
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), policyStore.groupPolicyCalls.Load())
	assert.Equal(t, int64(1), groupStore.calls.Load())

	// Second call should use cache for both groups and group policies
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), policyStore.groupPolicyCalls.Load())
	assert.Equal(t, int64(1), groupStore.calls.Load())
}

func TestCachedPolicyEvaluator_InvalidateUser(t *testing.T) {
	policyStore := &MockPolicyStoreForCache{}
	groupStore := &MockGroupStoreForCache{}

	policies := []*Policy{FullAccessPolicy()}
	policyStore.On("GetUserPolicies", mock.Anything, "testuser").Return(policies, nil)
	groupStore.On("GetUserGroups", mock.Anything, "testuser").Return([]string{}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// First call
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), policyStore.userPolicyCalls.Load())

	// Second call uses cache
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), policyStore.userPolicyCalls.Load())

	// Invalidate cache
	cpe.InvalidateUser("testuser")

	// Third call should hit store again
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(2), policyStore.userPolicyCalls.Load())
}

func TestCachedPolicyEvaluator_InvalidateGroup(t *testing.T) {
	policyStore := &MockPolicyStoreForCache{}
	groupStore := &MockGroupStoreForCache{}

	policyStore.On("GetUserPolicies", mock.Anything, "testuser").Return([]*Policy{}, nil)
	policyStore.On("GetGroupPolicies", mock.Anything, "admin").Return([]*Policy{FullAccessPolicy()}, nil)
	groupStore.On("GetUserGroups", mock.Anything, "testuser").Return([]string{"admin"}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// First call
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), policyStore.groupPolicyCalls.Load())

	// Second call uses cache
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), policyStore.groupPolicyCalls.Load())

	// Invalidate group cache
	cpe.InvalidateGroup("admin")

	// Third call should hit store for group policies again
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(2), policyStore.groupPolicyCalls.Load())
}

func TestCachedPolicyEvaluator_InvalidateAll(t *testing.T) {
	policyStore := &MockPolicyStoreForCache{}
	groupStore := &MockGroupStoreForCache{}

	policyStore.On("GetUserPolicies", mock.Anything, "testuser").Return([]*Policy{FullAccessPolicy()}, nil)
	groupStore.On("GetUserGroups", mock.Anything, "testuser").Return([]string{}, nil)

	cpe := NewCachedPolicyEvaluator(policyStore, groupStore)
	defer cpe.Stop()

	identity := &Identity{Name: "testuser"}
	evalCtx := BuildEvaluationContext("s3:GetObject", "bucket", "key")

	// Populate cache
	cpe.Evaluate(context.Background(), identity, evalCtx)
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(1), policyStore.userPolicyCalls.Load())

	// Clear all caches
	cpe.InvalidateAll()
	stats := cpe.Stats()
	assert.Equal(t, 0, stats.UserPolicyCacheSize)
	assert.Equal(t, 0, stats.GroupPolicyCacheSize)
	assert.Equal(t, 0, stats.UserGroupsCacheSize)

	// Next call should hit store
	cpe.Evaluate(context.Background(), identity, evalCtx)
	assert.Equal(t, int64(2), policyStore.userPolicyCalls.Load())
}

func TestCachedPolicyEvaluator_Stats(t *testing.T) {
	policyStore := &MockPolicyStoreForCache{}
	groupStore := &MockGroupStoreForCache{}

	policyStore.On("GetUserPolicies", mock.Anything, mock.Anything).Return([]*Policy{FullAccessPolicy()}, nil)
	policyStore.On("GetGroupPolicies", mock.Anything, mock.Anything).Return([]*Policy{}, nil)
	groupStore.On("GetUserGroups", mock.Anything, mock.Anything).Return([]string{"group1"}, nil)

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
	policyStore := &MockPolicyStoreForCache{}
	groupStore := &MockGroupStoreForCache{}

	policyStore.On("GetUserPolicies", mock.Anything, "testuser").Return([]*Policy{}, nil)
	groupStore.On("GetUserGroups", mock.Anything, "testuser").Return([]string{}, nil)

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
	assert.Equal(t, int64(0), policyStore.userPolicyCalls.Load())
}
