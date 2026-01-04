// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"sync"
)

// Compile-time interface verification
var (
	_ PolicyStore = (*MemoryPolicyStore)(nil)
	_ GroupStore  = (*MemoryPolicyStore)(nil)
)

// MemoryPolicyStore is an in-memory implementation of PolicyStore and GroupStore.
// Useful for development, testing, or simple deployments.
//
// Usage:
//
//	store := iam.NewMemoryPolicyStore()
//	store.AttachUserPolicy("admin", iam.FullAccessPolicy())
//	store.AddUserToGroup("alice", "developers")
//	store.AttachGroupPolicy("developers", iam.BucketAccessPolicy("dev-bucket"))
type MemoryPolicyStore struct {
	mu sync.RWMutex

	// User policies (username -> policies)
	userPolicies map[string][]*Policy

	// Group policies (groupName -> policies)
	groupPolicies map[string][]*Policy

	// Role policies (roleName -> policies)
	rolePolicies map[string][]*Policy

	// User groups (username -> groupNames)
	userGroups map[string][]string
}

// NewMemoryPolicyStore creates a new in-memory policy store.
// Returns concrete type to allow access to management methods (AttachUserPolicy, AddUserToGroup, etc.)
// which are needed for test setup and simple deployments.
func NewMemoryPolicyStore() *MemoryPolicyStore {
	return &MemoryPolicyStore{
		userPolicies:  make(map[string][]*Policy),
		groupPolicies: make(map[string][]*Policy),
		rolePolicies:  make(map[string][]*Policy),
		userGroups:    make(map[string][]string),
	}
}

// --- PolicyStore implementation ---

func (s *MemoryPolicyStore) GetUserPolicies(ctx context.Context, username string) ([]*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	policies := s.userPolicies[username]
	// Return a copy to prevent external modification
	result := make([]*Policy, len(policies))
	copy(result, policies)
	return result, nil
}

func (s *MemoryPolicyStore) GetGroupPolicies(ctx context.Context, groupName string) ([]*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	policies := s.groupPolicies[groupName]
	result := make([]*Policy, len(policies))
	copy(result, policies)
	return result, nil
}

func (s *MemoryPolicyStore) GetRolePolicies(ctx context.Context, roleName string) ([]*Policy, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	policies := s.rolePolicies[roleName]
	result := make([]*Policy, len(policies))
	copy(result, policies)
	return result, nil
}

// --- GroupStore implementation ---

func (s *MemoryPolicyStore) GetUserGroups(ctx context.Context, username string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups := s.userGroups[username]
	result := make([]string, len(groups))
	copy(result, groups)
	return result, nil
}

// --- Management methods ---

// AttachUserPolicy attaches a policy to a user
func (s *MemoryPolicyStore) AttachUserPolicy(username string, policy *Policy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userPolicies[username] = append(s.userPolicies[username], policy)
}

// DetachUserPolicy removes a policy from a user by policy ID
func (s *MemoryPolicyStore) DetachUserPolicy(username, policyID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	policies := s.userPolicies[username]
	for i, p := range policies {
		if p.ID == policyID {
			s.userPolicies[username] = append(policies[:i], policies[i+1:]...)
			return
		}
	}
}

// SetUserPolicies replaces all policies for a user
func (s *MemoryPolicyStore) SetUserPolicies(username string, policies []*Policy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userPolicies[username] = policies
}

// ClearUserPolicies removes all policies from a user
func (s *MemoryPolicyStore) ClearUserPolicies(username string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.userPolicies, username)
}

// AttachGroupPolicy attaches a policy to a group
func (s *MemoryPolicyStore) AttachGroupPolicy(groupName string, policy *Policy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupPolicies[groupName] = append(s.groupPolicies[groupName], policy)
}

// DetachGroupPolicy removes a policy from a group by policy ID
func (s *MemoryPolicyStore) DetachGroupPolicy(groupName, policyID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	policies := s.groupPolicies[groupName]
	for i, p := range policies {
		if p.ID == policyID {
			s.groupPolicies[groupName] = append(policies[:i], policies[i+1:]...)
			return
		}
	}
}

// SetGroupPolicies replaces all policies for a group
func (s *MemoryPolicyStore) SetGroupPolicies(groupName string, policies []*Policy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.groupPolicies[groupName] = policies
}

// AttachRolePolicy attaches a policy to a role
func (s *MemoryPolicyStore) AttachRolePolicy(roleName string, policy *Policy) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.rolePolicies[roleName] = append(s.rolePolicies[roleName], policy)
}

// AddUserToGroup adds a user to a group
func (s *MemoryPolicyStore) AddUserToGroup(username, groupName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if already in group
	for _, g := range s.userGroups[username] {
		if g == groupName {
			return
		}
	}
	s.userGroups[username] = append(s.userGroups[username], groupName)
}

// RemoveUserFromGroup removes a user from a group
func (s *MemoryPolicyStore) RemoveUserFromGroup(username, groupName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	groups := s.userGroups[username]
	for i, g := range groups {
		if g == groupName {
			s.userGroups[username] = append(groups[:i], groups[i+1:]...)
			return
		}
	}
}

// SetUserGroups replaces all groups for a user
func (s *MemoryPolicyStore) SetUserGroups(username string, groups []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userGroups[username] = groups
}

// CreateGroup creates a new group (no-op for memory store, group exists when it has policies)
func (s *MemoryPolicyStore) CreateGroup(groupName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.groupPolicies[groupName]; !exists {
		s.groupPolicies[groupName] = []*Policy{}
	}
}

// DeleteGroup removes a group and its policies
func (s *MemoryPolicyStore) DeleteGroup(groupName string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.groupPolicies, groupName)

	// Remove group from all users
	for username, groups := range s.userGroups {
		for i, g := range groups {
			if g == groupName {
				s.userGroups[username] = append(groups[:i], groups[i+1:]...)
				break
			}
		}
	}
}

// ListGroups returns all group names
func (s *MemoryPolicyStore) ListGroups() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	groups := make([]string, 0, len(s.groupPolicies))
	for g := range s.groupPolicies {
		groups = append(groups, g)
	}
	return groups
}

// ListGroupMembers returns all users in a group
func (s *MemoryPolicyStore) ListGroupMembers(groupName string) []string {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var members []string
	for username, groups := range s.userGroups {
		for _, g := range groups {
			if g == groupName {
				members = append(members, username)
				break
			}
		}
	}
	return members
}
