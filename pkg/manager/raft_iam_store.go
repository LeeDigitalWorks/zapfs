// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
)

var (
	// ErrUserNotFound is returned when a user is not found.
	ErrUserNotFound = errors.New("user not found")
	// ErrUserExists is returned when trying to create a user that already exists.
	ErrUserExists = errors.New("user already exists")
	// ErrCredentialNotFound is returned when a credential is not found.
	ErrCredentialNotFound = errors.New("credential not found")
)

// RaftCredentialStore implements iam.CredentialStore backed by Raft consensus.
// All mutations go through Raft, ensuring credentials are consistent across the cluster.
// Credentials are stored with encrypted secrets (EncryptedSecret field).
type RaftCredentialStore struct {
	ms *ManagerServer
}

// NewRaftCredentialStore creates a new RaftCredentialStore.
func NewRaftCredentialStore(ms *ManagerServer) *RaftCredentialStore {
	return &RaftCredentialStore{ms: ms}
}

// GetCredential retrieves a credential by access key.
// Returns the identity and credential if found, or ErrCredentialNotFound if not.
func (s *RaftCredentialStore) GetCredential(ctx context.Context, accessKey string) (*iam.Identity, *iam.Credential, error) {
	s.ms.state.RLock()
	defer s.ms.state.RUnlock()

	userName, exists := s.ms.state.GetIAMUserByAccessKey(accessKey)
	if !exists {
		return nil, nil, ErrCredentialNotFound
	}

	identity, exists := s.ms.state.GetIAMUser(userName)
	if !exists {
		return nil, nil, ErrCredentialNotFound
	}

	// Find the specific credential
	for _, cred := range identity.Credentials {
		if cred.AccessKey == accessKey {
			return identity, cred, nil
		}
	}

	return nil, nil, ErrCredentialNotFound
}

// ListCredentials returns all credentials (for streaming to metadata services).
func (s *RaftCredentialStore) ListCredentials(ctx context.Context) ([]*iam.Identity, error) {
	s.ms.state.RLock()
	defer s.ms.state.RUnlock()

	identities := make([]*iam.Identity, 0, len(s.ms.state.IAMUsers))
	for _, identity := range s.ms.state.IAMUsers {
		// Return a copy to avoid concurrent modification issues
		identities = append(identities, copyIdentity(identity))
	}

	return identities, nil
}

// CreateUser creates a new user with the given identity.
// Credentials should have EncryptedSecret set (call iam.SecureCredential before).
func (s *RaftCredentialStore) CreateUser(ctx context.Context, identity *iam.Identity) error {
	if identity == nil || identity.Name == "" {
		return errors.New("identity with name is required")
	}

	// Check if user already exists (optimistic check before Raft)
	s.ms.state.RLock()
	_, exists := s.ms.state.GetIAMUser(identity.Name)
	s.ms.state.RUnlock()
	if exists {
		return ErrUserExists
	}

	// Apply through Raft
	req := IAMCreateUserRequest{Identity: identity}
	return s.ms.applyCommand(CommandIAMCreateUser, req)
}

// UpdateUser updates an existing user.
func (s *RaftCredentialStore) UpdateUser(ctx context.Context, identity *iam.Identity) error {
	if identity == nil || identity.Name == "" {
		return errors.New("identity with name is required")
	}

	// Check if user exists (optimistic check before Raft)
	s.ms.state.RLock()
	_, exists := s.ms.state.GetIAMUser(identity.Name)
	s.ms.state.RUnlock()
	if !exists {
		return ErrUserNotFound
	}

	// Apply through Raft
	req := IAMCreateUserRequest{Identity: identity}
	return s.ms.applyCommand(CommandIAMUpdateUser, req)
}

// DeleteUser deletes a user by name.
func (s *RaftCredentialStore) DeleteUser(ctx context.Context, userName string) error {
	if userName == "" {
		return errors.New("user name is required")
	}

	// Check if user exists (optimistic check before Raft)
	s.ms.state.RLock()
	_, exists := s.ms.state.GetIAMUser(userName)
	s.ms.state.RUnlock()
	if !exists {
		return ErrUserNotFound
	}

	// Apply through Raft
	req := IAMDeleteUserRequest{UserName: userName}
	return s.ms.applyCommand(CommandIAMDeleteUser, req)
}

// GetUser retrieves a user by name.
func (s *RaftCredentialStore) GetUser(ctx context.Context, userName string) (*iam.Identity, error) {
	s.ms.state.RLock()
	defer s.ms.state.RUnlock()

	identity, exists := s.ms.state.GetIAMUser(userName)
	if !exists {
		return nil, ErrUserNotFound
	}

	return copyIdentity(identity), nil
}

// CreateAccessKey creates a new access key for a user.
// The credential should have EncryptedSecret set.
func (s *RaftCredentialStore) CreateAccessKey(ctx context.Context, userName string, credential *iam.Credential) error {
	if userName == "" {
		return errors.New("user name is required")
	}
	if credential == nil || credential.AccessKey == "" {
		return errors.New("credential with access key is required")
	}

	// Set creation time if not set
	if credential.CreatedAt.IsZero() {
		credential.CreatedAt = time.Now()
	}

	// Check if user exists
	s.ms.state.RLock()
	_, exists := s.ms.state.GetIAMUser(userName)
	s.ms.state.RUnlock()
	if !exists {
		return ErrUserNotFound
	}

	// Apply through Raft
	req := IAMCreateKeyRequest{
		UserName:   userName,
		Credential: credential,
	}
	return s.ms.applyCommand(CommandIAMCreateKey, req)
}

// DeleteAccessKey deletes an access key from a user.
func (s *RaftCredentialStore) DeleteAccessKey(ctx context.Context, userName, accessKey string) error {
	if userName == "" {
		return errors.New("user name is required")
	}
	if accessKey == "" {
		return errors.New("access key is required")
	}

	// Apply through Raft
	req := IAMDeleteKeyRequest{
		UserName:  userName,
		AccessKey: accessKey,
	}
	return s.ms.applyCommand(CommandIAMDeleteKey, req)
}

// CreatePolicy creates a new policy.
func (s *RaftCredentialStore) CreatePolicy(ctx context.Context, policy *iam.Policy) error {
	if policy == nil || policy.ID == "" {
		return errors.New("policy with ID is required")
	}

	// Apply through Raft
	req := IAMCreatePolicyRequest{Policy: policy}
	return s.ms.applyCommand(CommandIAMCreatePolicy, req)
}

// DeletePolicy deletes a policy.
func (s *RaftCredentialStore) DeletePolicy(ctx context.Context, policyID string) error {
	if policyID == "" {
		return errors.New("policy ID is required")
	}

	// Apply through Raft
	req := IAMDeletePolicyRequest{PolicyID: policyID}
	return s.ms.applyCommand(CommandIAMDeletePolicy, req)
}

// GetPolicy retrieves a policy by ID.
func (s *RaftCredentialStore) GetPolicy(ctx context.Context, policyID string) (*iam.Policy, error) {
	s.ms.state.RLock()
	defer s.ms.state.RUnlock()

	policy, exists := s.ms.state.GetIAMPolicy(policyID)
	if !exists {
		return nil, fmt.Errorf("policy %s not found", policyID)
	}

	return copyPolicy(policy), nil
}

// HasUsers returns true if there are any users in the store.
func (s *RaftCredentialStore) HasUsers() bool {
	s.ms.state.RLock()
	defer s.ms.state.RUnlock()
	return s.ms.state.HasIAMUsers()
}

// GetVersion returns the current IAM version counter.
func (s *RaftCredentialStore) GetVersion() uint64 {
	s.ms.state.RLock()
	defer s.ms.state.RUnlock()
	return s.ms.state.GetIAMVersion()
}

// ListUsers returns the names of all users.
func (s *RaftCredentialStore) ListUsers(ctx context.Context) ([]string, error) {
	s.ms.state.RLock()
	defer s.ms.state.RUnlock()

	users := make([]string, 0, len(s.ms.state.IAMUsers))
	for name := range s.ms.state.IAMUsers {
		users = append(users, name)
	}

	return users, nil
}
