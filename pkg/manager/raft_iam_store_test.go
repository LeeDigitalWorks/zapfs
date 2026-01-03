// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFSMState_IAMOperations(t *testing.T) {
	state := NewFSMState("test-region", 3)

	// Test adding a user
	identity := &iam.Identity{
		Name: "testuser",
		Account: &iam.Account{
			DisplayName:  "Test User",
			EmailAddress: "test@example.com",
			ID:           "user-123",
		},
		Credentials: []*iam.Credential{
			{
				AccessKey:       "AKIAIOSFODNN7EXAMPLE",
				EncryptedSecret: []byte("encrypted-secret"),
				Status:          "Active",
				CreatedAt:       time.Now(),
			},
		},
	}

	// Add user
	state.Lock()
	state.AddIAMUser(identity)
	state.Unlock()

	// Verify user was added
	state.RLock()
	retrieved, exists := state.GetIAMUser("testuser")
	state.RUnlock()

	require.True(t, exists)
	assert.Equal(t, "testuser", retrieved.Name)
	assert.Equal(t, "Test User", retrieved.Account.DisplayName)
	assert.Len(t, retrieved.Credentials, 1)

	// Verify credential index
	state.RLock()
	userName, exists := state.GetIAMUserByAccessKey("AKIAIOSFODNN7EXAMPLE")
	state.RUnlock()

	require.True(t, exists)
	assert.Equal(t, "testuser", userName)

	// Test version increment
	state.RLock()
	version := state.GetIAMVersion()
	state.RUnlock()
	assert.Equal(t, uint64(2), version) // 1 from init + 1 from AddIAMUser
}

func TestFSMState_IAMUpdate(t *testing.T) {
	state := NewFSMState("test-region", 3)

	// Add initial user
	identity := &iam.Identity{
		Name: "testuser",
		Credentials: []*iam.Credential{
			{
				AccessKey:       "AKIAOLD",
				EncryptedSecret: []byte("old-secret"),
				Status:          "Active",
			},
		},
	}

	state.Lock()
	state.AddIAMUser(identity)
	state.Unlock()

	// Update user with new credential
	updatedIdentity := &iam.Identity{
		Name: "testuser",
		Credentials: []*iam.Credential{
			{
				AccessKey:       "AKIANEW",
				EncryptedSecret: []byte("new-secret"),
				Status:          "Active",
			},
		},
	}

	state.Lock()
	state.UpdateIAMUser(updatedIdentity)
	state.Unlock()

	// Verify old access key is removed from index
	state.RLock()
	_, oldExists := state.GetIAMUserByAccessKey("AKIAOLD")
	_, newExists := state.GetIAMUserByAccessKey("AKIANEW")
	state.RUnlock()

	assert.False(t, oldExists, "old access key should be removed from index")
	assert.True(t, newExists, "new access key should be in index")
}

func TestFSMState_IAMDelete(t *testing.T) {
	state := NewFSMState("test-region", 3)

	// Add user
	identity := &iam.Identity{
		Name: "testuser",
		Credentials: []*iam.Credential{
			{AccessKey: "AKIATEST"},
		},
	}

	state.Lock()
	state.AddIAMUser(identity)
	state.Unlock()

	// Delete user
	state.Lock()
	state.RemoveIAMUser("testuser")
	state.Unlock()

	// Verify user is removed
	state.RLock()
	_, exists := state.GetIAMUser("testuser")
	_, credExists := state.GetIAMUserByAccessKey("AKIATEST")
	state.RUnlock()

	assert.False(t, exists, "user should be removed")
	assert.False(t, credExists, "credential index should be cleared")
}

func TestFSMState_IAMPolicy(t *testing.T) {
	state := NewFSMState("test-region", 3)

	policy := &iam.Policy{
		Version: "2012-10-17",
		ID:      "test-policy",
		Statements: []iam.PolicyStatement{
			{
				Sid:       "AllowS3",
				Effect:    iam.EffectAllow,
				Actions:   iam.StringOrSlice{"s3:*"},
				Resources: iam.StringOrSlice{"*"},
			},
		},
	}

	// Add policy
	state.Lock()
	state.AddIAMPolicy(policy)
	state.Unlock()

	// Verify policy was added
	state.RLock()
	retrieved, exists := state.GetIAMPolicy("test-policy")
	state.RUnlock()

	require.True(t, exists)
	assert.Equal(t, "test-policy", retrieved.ID)
	assert.Len(t, retrieved.Statements, 1)

	// Delete policy
	state.Lock()
	state.RemoveIAMPolicy("test-policy")
	state.Unlock()

	state.RLock()
	_, exists = state.GetIAMPolicy("test-policy")
	state.RUnlock()

	assert.False(t, exists)
}

func TestFSMState_IAMSnapshotRestore(t *testing.T) {
	state := NewFSMState("test-region", 3)

	// Add users and policies
	state.Lock()
	state.AddIAMUser(&iam.Identity{
		Name: "user1",
		Credentials: []*iam.Credential{
			{AccessKey: "AKIA1", EncryptedSecret: []byte("secret1")},
		},
	})
	state.AddIAMUser(&iam.Identity{
		Name: "user2",
		Credentials: []*iam.Credential{
			{AccessKey: "AKIA2", EncryptedSecret: []byte("secret2")},
		},
	})
	state.AddIAMPolicy(&iam.Policy{
		ID:      "policy1",
		Version: "2012-10-17",
	})

	// Take snapshot
	snapshot := state.Snapshot()
	state.Unlock()

	// Create new state and restore
	newState := NewFSMState("new-region", 3)
	newState.Lock()
	newState.Restore(snapshot)
	newState.Unlock()

	// Verify users restored
	newState.RLock()
	user1, exists1 := newState.GetIAMUser("user1")
	user2, exists2 := newState.GetIAMUser("user2")
	policy, policyExists := newState.GetIAMPolicy("policy1")

	userName1, idx1 := newState.GetIAMUserByAccessKey("AKIA1")
	userName2, idx2 := newState.GetIAMUserByAccessKey("AKIA2")
	newState.RUnlock()

	require.True(t, exists1)
	require.True(t, exists2)
	require.True(t, policyExists)

	assert.Equal(t, "user1", user1.Name)
	assert.Equal(t, "user2", user2.Name)
	assert.Equal(t, "policy1", policy.ID)

	// Verify indexes were rebuilt
	assert.True(t, idx1)
	assert.True(t, idx2)
	assert.Equal(t, "user1", userName1)
	assert.Equal(t, "user2", userName2)
}

func TestCopyIdentity(t *testing.T) {
	original := &iam.Identity{
		Name:     "testuser",
		Disabled: false,
		Account: &iam.Account{
			DisplayName:  "Test",
			EmailAddress: "test@test.com",
			ID:           "id-123",
		},
		Credentials: []*iam.Credential{
			{
				AccessKey:       "AKIATEST",
				EncryptedSecret: []byte("secret"),
				Status:          "Active",
				CreatedAt:       time.Now(),
			},
		},
	}

	copy := copyIdentity(original)

	// Verify it's a deep copy
	assert.Equal(t, original.Name, copy.Name)
	assert.Equal(t, original.Account.DisplayName, copy.Account.DisplayName)
	assert.Equal(t, original.Credentials[0].AccessKey, copy.Credentials[0].AccessKey)

	// Modify original and verify copy is unchanged
	original.Name = "modified"
	original.Account.DisplayName = "Modified"
	original.Credentials[0].AccessKey = "MODIFIED"

	assert.Equal(t, "testuser", copy.Name)
	assert.Equal(t, "Test", copy.Account.DisplayName)
	assert.Equal(t, "AKIATEST", copy.Credentials[0].AccessKey)
}

func TestCopyPolicy(t *testing.T) {
	original := &iam.Policy{
		Version: "2012-10-17",
		ID:      "test-policy",
		Statements: []iam.PolicyStatement{
			{
				Sid:       "Stmt1",
				Effect:    iam.EffectAllow,
				Actions:   iam.StringOrSlice{"s3:GetObject", "s3:PutObject"},
				Resources: iam.StringOrSlice{"arn:aws:s3:::bucket/*"},
				Condition: map[string]iam.Condition{
					"StringEquals": {
						"s3:prefix": iam.StringOrSlice{"home/"},
					},
				},
			},
		},
	}

	copy := copyPolicy(original)

	// Verify it's a deep copy
	assert.Equal(t, original.ID, copy.ID)
	assert.Equal(t, original.Version, copy.Version)
	require.Len(t, copy.Statements, 1)
	assert.Equal(t, original.Statements[0].Sid, copy.Statements[0].Sid)
	assert.Equal(t, original.Statements[0].Actions, copy.Statements[0].Actions)

	// Modify original and verify copy is unchanged
	original.ID = "modified"
	original.Statements[0].Actions = append(original.Statements[0].Actions, "s3:DeleteObject")

	assert.Equal(t, "test-policy", copy.ID)
	assert.Len(t, copy.Statements[0].Actions, 2)
}
