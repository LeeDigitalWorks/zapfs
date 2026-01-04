// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"sync"
)

// PolicyStore defines the interface for retrieving IAM policies.
// Implement this interface to integrate with your policy storage backend.
type PolicyStore interface {
	// GetUserPolicies returns all policies attached to a user (inline + managed)
	GetUserPolicies(ctx context.Context, username string) ([]*Policy, error)

	// GetGroupPolicies returns all policies attached to a group
	GetGroupPolicies(ctx context.Context, groupName string) ([]*Policy, error)

	// GetRolePolicies returns all policies attached to a role
	GetRolePolicies(ctx context.Context, roleName string) ([]*Policy, error)
}

// GroupStore defines the interface for user-group membership lookups.
type GroupStore interface {
	// GetUserGroups returns the groups a user belongs to
	GetUserGroups(ctx context.Context, username string) ([]string, error)
}

// PolicyEvaluator evaluates IAM policies for authorization decisions.
// It combines policies from users, groups, and roles.
//
// Usage:
//
//	evaluator := iam.NewPolicyEvaluator(policyStore, groupStore)
//	decision := evaluator.Evaluate(ctx, identity, evalCtx)
type PolicyEvaluator struct {
	policyStore PolicyStore
	groupStore  GroupStore

	// Optional hooks for extending evaluation logic
	hooks PolicyEvaluatorHooks
}

// PolicyEvaluatorHooks allows customization of policy evaluation.
// All hooks are optional - nil hooks are skipped.
type PolicyEvaluatorHooks struct {
	// BeforeEvaluate is called before policy evaluation starts.
	// Return DecisionDeny or DecisionAllow to short-circuit evaluation.
	// Return DecisionNotApplicable to continue normal evaluation.
	BeforeEvaluate func(ctx context.Context, identity *Identity, evalCtx *PolicyEvaluationContext) PolicyDecision

	// AfterEvaluate is called after policy evaluation completes.
	// Can be used for logging, metrics, or overriding decisions.
	AfterEvaluate func(ctx context.Context, identity *Identity, evalCtx *PolicyEvaluationContext, decision PolicyDecision) PolicyDecision

	// OnPolicyLoad is called when policies are loaded for evaluation.
	// Can be used for caching or transformation.
	OnPolicyLoad func(ctx context.Context, policies []*Policy) []*Policy
}

// NewPolicyEvaluator creates a new policy evaluator
func NewPolicyEvaluator(policyStore PolicyStore, groupStore GroupStore) *PolicyEvaluator {
	return &PolicyEvaluator{
		policyStore: policyStore,
		groupStore:  groupStore,
	}
}

// WithHooks returns a copy of the evaluator with the given hooks
func (e *PolicyEvaluator) WithHooks(hooks PolicyEvaluatorHooks) *PolicyEvaluator {
	return &PolicyEvaluator{
		policyStore: e.policyStore,
		groupStore:  e.groupStore,
		hooks:       hooks,
	}
}

// Evaluate checks if the identity is allowed to perform the action.
// Returns DecisionAllow, DecisionDeny, or DecisionNotApplicable.
//
// Evaluation order (AWS IAM logic):
// 1. Explicit deny in any policy → Deny
// 2. Explicit allow in any policy → Allow
// 3. No matching policy → NotApplicable (implicit deny at authorization layer)
func (e *PolicyEvaluator) Evaluate(ctx context.Context, identity *Identity, evalCtx *PolicyEvaluationContext) PolicyDecision {
	// Call BeforeEvaluate hook
	if e.hooks.BeforeEvaluate != nil {
		if decision := e.hooks.BeforeEvaluate(ctx, identity, evalCtx); decision != DecisionNotApplicable {
			return decision
		}
	}

	// Collect all applicable policies
	policies, err := e.collectPolicies(ctx, identity)
	if err != nil {
		// On error, fail closed (deny)
		return DecisionDeny
	}

	// Apply OnPolicyLoad hook
	if e.hooks.OnPolicyLoad != nil {
		policies = e.hooks.OnPolicyLoad(ctx, policies)
	}

	// Evaluate all policies
	decision := e.evaluatePolicies(policies, evalCtx)

	// Call AfterEvaluate hook
	if e.hooks.AfterEvaluate != nil {
		decision = e.hooks.AfterEvaluate(ctx, identity, evalCtx, decision)
	}

	return decision
}

// collectPolicies gathers all policies that apply to the identity
func (e *PolicyEvaluator) collectPolicies(ctx context.Context, identity *Identity) ([]*Policy, error) {
	if e.policyStore == nil {
		return nil, nil
	}

	var allPolicies []*Policy
	var mu sync.Mutex
	var wg sync.WaitGroup
	var firstErr error

	// Get user policies
	wg.Add(1)
	go func() {
		defer wg.Done()
		policies, err := e.policyStore.GetUserPolicies(ctx, identity.Name)
		if err != nil {
			mu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			mu.Unlock()
			return
		}
		mu.Lock()
		allPolicies = append(allPolicies, policies...)
		mu.Unlock()
	}()

	// Get group policies
	if e.groupStore != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			groups, err := e.groupStore.GetUserGroups(ctx, identity.Name)
			if err != nil {
				return // Non-fatal, continue without group policies
			}
			for _, group := range groups {
				policies, err := e.policyStore.GetGroupPolicies(ctx, group)
				if err != nil {
					continue
				}
				mu.Lock()
				allPolicies = append(allPolicies, policies...)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return allPolicies, firstErr
}

// evaluatePolicies evaluates a list of policies and returns the combined decision
func (e *PolicyEvaluator) evaluatePolicies(policies []*Policy, ctx *PolicyEvaluationContext) PolicyDecision {
	var hasAllow bool

	for _, policy := range policies {
		if policy == nil {
			continue
		}
		decision := policy.Evaluate(ctx)
		switch decision {
		case DecisionDeny:
			return DecisionDeny // Explicit deny always wins
		case DecisionAllow:
			hasAllow = true
		}
	}

	if hasAllow {
		return DecisionAllow
	}
	return DecisionNotApplicable
}

// IsAllowed is a convenience method that returns true only for explicit allow
func (e *PolicyEvaluator) IsAllowed(ctx context.Context, identity *Identity, evalCtx *PolicyEvaluationContext) bool {
	return e.Evaluate(ctx, identity, evalCtx) == DecisionAllow
}

// BuildEvaluationContext creates a PolicyEvaluationContext from common parameters
func BuildEvaluationContext(action, bucket, key string) *PolicyEvaluationContext {
	return &PolicyEvaluationContext{
		Action:   action,
		Bucket:   bucket,
		Key:      key,
		Resource: BuildResourceARN(bucket, key),
	}
}

// BuildResourceARN constructs an S3 resource ARN
func BuildResourceARN(bucket, key string) string {
	if bucket == "" {
		return "arn:aws:s3:::*"
	}
	if key == "" {
		return "arn:aws:s3:::" + bucket
	}
	return "arn:aws:s3:::" + bucket + "/" + key
}

// Common predefined policies for convenience

// ReadOnlyPolicy returns a policy that allows read-only access to all S3 resources
func ReadOnlyPolicy() *Policy {
	return &Policy{
		Version: "2012-10-17",
		Statements: []PolicyStatement{
			{
				Sid:       "ReadOnlyAccess",
				Effect:    EffectAllow,
				Actions:   StringOrSlice{"s3:Get*", "s3:List*", "s3:Head*"},
				Resources: StringOrSlice{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
			},
		},
	}
}

// FullAccessPolicy returns a policy that allows full access to all S3 resources
func FullAccessPolicy() *Policy {
	return &Policy{
		Version: "2012-10-17",
		Statements: []PolicyStatement{
			{
				Sid:       "FullAccess",
				Effect:    EffectAllow,
				Actions:   StringOrSlice{"s3:*"},
				Resources: StringOrSlice{"arn:aws:s3:::*", "arn:aws:s3:::*/*"},
			},
		},
	}
}

// BucketAccessPolicy returns a policy granting specified actions on a bucket
func BucketAccessPolicy(bucket string, actions ...string) *Policy {
	if len(actions) == 0 {
		actions = []string{"s3:*"}
	}
	return &Policy{
		Version: "2012-10-17",
		Statements: []PolicyStatement{
			{
				Effect:  EffectAllow,
				Actions: StringOrSlice(actions),
				Resources: StringOrSlice{
					"arn:aws:s3:::" + bucket,
					"arn:aws:s3:::" + bucket + "/*",
				},
			},
		},
	}
}
