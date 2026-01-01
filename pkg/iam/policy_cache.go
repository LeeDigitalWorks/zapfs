package iam

import (
	"context"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
)

// CachedPolicyEvaluator wraps PolicyEvaluator with LRU caching for policies.
// This significantly reduces database/store load on the hot authentication path.
//
// Cache behavior:
//   - User policies are cached by username
//   - Group memberships are cached by username
//   - Group policies are cached by group name
//   - All caches have configurable TTL and max size
//
// Usage:
//
//	evaluator := iam.NewCachedPolicyEvaluator(policyStore, groupStore,
//	    iam.WithPolicyCacheTTL(5*time.Minute),
//	    iam.WithPolicyCacheSize(10000),
//	)
type CachedPolicyEvaluator struct {
	*PolicyEvaluator

	// Caches
	userPolicyCache  *cache.Cache[string, []*Policy]
	groupPolicyCache *cache.Cache[string, []*Policy]
	userGroupsCache  *cache.Cache[string, []string]

	// Config
	ttl time.Duration
}

// CachedPolicyEvaluatorOption configures a CachedPolicyEvaluator.
type CachedPolicyEvaluatorOption func(*cachedEvaluatorConfig)

type cachedEvaluatorConfig struct {
	ttl     time.Duration
	maxSize int
}

// WithPolicyCacheTTL sets the TTL for cached policies.
func WithPolicyCacheTTL(ttl time.Duration) CachedPolicyEvaluatorOption {
	return func(c *cachedEvaluatorConfig) {
		c.ttl = ttl
	}
}

// WithPolicyCacheSize sets the maximum number of entries per cache.
func WithPolicyCacheSize(maxSize int) CachedPolicyEvaluatorOption {
	return func(c *cachedEvaluatorConfig) {
		c.maxSize = maxSize
	}
}

// NewCachedPolicyEvaluator creates a policy evaluator with caching.
func NewCachedPolicyEvaluator(
	policyStore PolicyStore,
	groupStore GroupStore,
	opts ...CachedPolicyEvaluatorOption,
) *CachedPolicyEvaluator {
	// Default config
	cfg := &cachedEvaluatorConfig{
		ttl:     5 * time.Minute,
		maxSize: 10000,
	}

	for _, opt := range opts {
		opt(cfg)
	}

	ctx := context.Background()

	cpe := &CachedPolicyEvaluator{
		PolicyEvaluator: NewPolicyEvaluator(policyStore, groupStore),
		ttl:             cfg.ttl,
		userPolicyCache: cache.New[string, []*Policy](ctx,
			cache.WithExpiry[string, []*Policy](cfg.ttl),
			cache.WithMaxSize[string, []*Policy](cfg.maxSize),
		),
		groupPolicyCache: cache.New[string, []*Policy](ctx,
			cache.WithExpiry[string, []*Policy](cfg.ttl),
			cache.WithMaxSize[string, []*Policy](cfg.maxSize),
		),
		userGroupsCache: cache.New[string, []string](ctx,
			cache.WithExpiry[string, []string](cfg.ttl),
			cache.WithMaxSize[string, []string](cfg.maxSize),
		),
	}

	return cpe
}

// Evaluate checks if the identity is allowed to perform the action.
// Uses cached policies when available.
func (cpe *CachedPolicyEvaluator) Evaluate(ctx context.Context, identity *Identity, evalCtx *PolicyEvaluationContext) PolicyDecision {
	// Call BeforeEvaluate hook
	if cpe.hooks.BeforeEvaluate != nil {
		if decision := cpe.hooks.BeforeEvaluate(ctx, identity, evalCtx); decision != DecisionNotApplicable {
			return decision
		}
	}

	// Collect all applicable policies (using cache)
	policies, err := cpe.collectPoliciesCached(ctx, identity)
	if err != nil {
		return DecisionDeny
	}

	// Apply OnPolicyLoad hook
	if cpe.hooks.OnPolicyLoad != nil {
		policies = cpe.hooks.OnPolicyLoad(ctx, policies)
	}

	// Evaluate all policies
	decision := cpe.evaluatePolicies(policies, evalCtx)

	// Call AfterEvaluate hook
	if cpe.hooks.AfterEvaluate != nil {
		decision = cpe.hooks.AfterEvaluate(ctx, identity, evalCtx, decision)
	}

	return decision
}

// collectPoliciesCached gathers all policies with caching
func (cpe *CachedPolicyEvaluator) collectPoliciesCached(ctx context.Context, identity *Identity) ([]*Policy, error) {
	if cpe.policyStore == nil {
		return nil, nil
	}

	var allPolicies []*Policy

	// Get user policies (cached)
	userPolicies, err := cpe.getUserPoliciesCached(ctx, identity.Name)
	if err != nil {
		return nil, err
	}
	allPolicies = append(allPolicies, userPolicies...)

	// Get group policies (cached)
	if cpe.groupStore != nil {
		groups, err := cpe.getUserGroupsCached(ctx, identity.Name)
		if err == nil {
			for _, group := range groups {
				groupPolicies, err := cpe.getGroupPoliciesCached(ctx, group)
				if err == nil {
					allPolicies = append(allPolicies, groupPolicies...)
				}
			}
		}
	}

	return allPolicies, nil
}

// getUserPoliciesCached returns cached user policies or fetches from store
func (cpe *CachedPolicyEvaluator) getUserPoliciesCached(ctx context.Context, username string) ([]*Policy, error) {
	// Check cache first
	if policies, ok := cpe.userPolicyCache.Get(username); ok {
		return policies, nil
	}

	// Cache miss - fetch from store
	policies, err := cpe.policyStore.GetUserPolicies(ctx, username)
	if err != nil {
		return nil, err
	}

	// Store in cache
	cpe.userPolicyCache.Set(username, policies)
	return policies, nil
}

// getGroupPoliciesCached returns cached group policies or fetches from store
func (cpe *CachedPolicyEvaluator) getGroupPoliciesCached(ctx context.Context, groupName string) ([]*Policy, error) {
	// Check cache first
	if policies, ok := cpe.groupPolicyCache.Get(groupName); ok {
		return policies, nil
	}

	// Cache miss - fetch from store
	policies, err := cpe.policyStore.GetGroupPolicies(ctx, groupName)
	if err != nil {
		return nil, err
	}

	// Store in cache
	cpe.groupPolicyCache.Set(groupName, policies)
	return policies, nil
}

// getUserGroupsCached returns cached user groups or fetches from store
func (cpe *CachedPolicyEvaluator) getUserGroupsCached(ctx context.Context, username string) ([]string, error) {
	// Check cache first
	if groups, ok := cpe.userGroupsCache.Get(username); ok {
		return groups, nil
	}

	// Cache miss - fetch from store
	groups, err := cpe.groupStore.GetUserGroups(ctx, username)
	if err != nil {
		return nil, err
	}

	// Store in cache
	cpe.userGroupsCache.Set(username, groups)
	return groups, nil
}

// InvalidateUser removes all cached data for a user.
// Call this when a user's policies or group memberships change.
func (cpe *CachedPolicyEvaluator) InvalidateUser(username string) {
	cpe.userPolicyCache.Delete(username)
	cpe.userGroupsCache.Delete(username)
}

// InvalidateGroup removes cached policies for a group.
// Call this when a group's policies change.
func (cpe *CachedPolicyEvaluator) InvalidateGroup(groupName string) {
	cpe.groupPolicyCache.Delete(groupName)
}

// InvalidateAll clears all caches.
// Call this when there's a bulk policy change.
func (cpe *CachedPolicyEvaluator) InvalidateAll() {
	cpe.userPolicyCache.Clear()
	cpe.groupPolicyCache.Clear()
	cpe.userGroupsCache.Clear()
}

// CacheStats returns statistics about the policy caches.
type CacheStats struct {
	UserPolicyCacheSize  int
	GroupPolicyCacheSize int
	UserGroupsCacheSize  int
}

// Stats returns current cache statistics.
func (cpe *CachedPolicyEvaluator) Stats() CacheStats {
	return CacheStats{
		UserPolicyCacheSize:  cpe.userPolicyCache.Size(),
		GroupPolicyCacheSize: cpe.groupPolicyCache.Size(),
		UserGroupsCacheSize:  cpe.userGroupsCache.Size(),
	}
}

// Stop cleans up cache resources.
func (cpe *CachedPolicyEvaluator) Stop() {
	cpe.userPolicyCache.Stop()
	cpe.groupPolicyCache.Stop()
	cpe.userGroupsCache.Stop()
}
