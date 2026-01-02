// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// MemoryStore Tests
// =============================================================================

func TestMemoryStore_CreateUser(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		identity    *Identity
		wantErr     error
		description string
	}{
		{
			name: "create user successfully",
			identity: &Identity{
				Name: "testuser",
				Credentials: []*Credential{
					{AccessKey: "AKIA123", SecretKey: "secret123", Status: "Active"},
				},
			},
			wantErr:     nil,
			description: "should create a new user",
		},
		{
			name: "create user with multiple credentials",
			identity: &Identity{
				Name: "multikey",
				Credentials: []*Credential{
					{AccessKey: "AKIA001", SecretKey: "secret1", Status: "Active"},
					{AccessKey: "AKIA002", SecretKey: "secret2", Status: "Active"},
				},
			},
			wantErr:     nil,
			description: "should create user with multiple access keys",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemoryStore()
			ctx := context.Background()

			err := store.CreateUser(ctx, tt.identity)
			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)

			// Verify user exists
			got, err := store.GetUser(ctx, tt.identity.Name)
			require.NoError(t, err)
			assert.Equal(t, tt.identity.Name, got.Name)
			assert.Len(t, got.Credentials, len(tt.identity.Credentials))
		})
	}
}

func TestMemoryStore_CreateUser_Duplicate(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	ctx := context.Background()

	identity := &Identity{
		Name:        "duplicate",
		Credentials: []*Credential{{AccessKey: "AKIA999", SecretKey: "secret"}},
	}

	err := store.CreateUser(ctx, identity)
	require.NoError(t, err)

	// Try to create again
	err = store.CreateUser(ctx, identity)
	assert.ErrorIs(t, err, ErrUserAlreadyExists)
}

func TestMemoryStore_GetUserByAccessKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accessKey string
		setup     func(*MemoryStore)
		wantErr   error
		wantUser  string
	}{
		{
			name:      "find existing user",
			accessKey: "AKIAFOUND",
			setup: func(s *MemoryStore) {
				s.CreateUser(context.Background(), &Identity{
					Name:        "founduser",
					Credentials: []*Credential{{AccessKey: "AKIAFOUND", SecretKey: "secret"}},
				})
			},
			wantErr:  nil,
			wantUser: "founduser",
		},
		{
			name:      "access key not found",
			accessKey: "AKIANOTFOUND",
			setup:     func(s *MemoryStore) {},
			wantErr:   ErrAccessKeyNotFound,
			wantUser:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemoryStore()
			tt.setup(store)

			identity, cred, err := store.GetUserByAccessKey(context.Background(), tt.accessKey)

			if tt.wantErr != nil {
				assert.ErrorIs(t, err, tt.wantErr)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantUser, identity.Name)
			assert.Equal(t, tt.accessKey, cred.AccessKey)
		})
	}
}

func TestMemoryStore_DeleteUser(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	ctx := context.Background()

	// Create user
	identity := &Identity{
		Name:        "todelete",
		Credentials: []*Credential{{AccessKey: "AKIADELETE", SecretKey: "secret"}},
	}
	err := store.CreateUser(ctx, identity)
	require.NoError(t, err)

	// Delete user
	err = store.DeleteUser(ctx, "todelete")
	require.NoError(t, err)

	// Verify user is gone
	_, err = store.GetUser(ctx, "todelete")
	assert.ErrorIs(t, err, ErrUserNotFound)

	// Verify access key is gone
	_, _, err = store.GetUserByAccessKey(ctx, "AKIADELETE")
	assert.ErrorIs(t, err, ErrAccessKeyNotFound)
}

func TestMemoryStore_Concurrent(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	ctx := context.Background()

	var wg sync.WaitGroup
	numGoroutines := 100

	// Concurrent writes
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			identity := &Identity{
				Name:        GenerateAccessKey(), // Use unique name
				Credentials: []*Credential{{AccessKey: GenerateAccessKey(), SecretKey: GenerateSecretKey()}},
			}
			store.CreateUser(ctx, identity)
		}(i)
	}

	wg.Wait()

	users, err := store.ListUsers(ctx)
	require.NoError(t, err)
	assert.Len(t, users, numGoroutines)
}

// =============================================================================
// Manager Tests
// =============================================================================

func TestManager_LookupByAccessKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		accessKey string
		setup     func(*MemoryStore)
		wantFound bool
		wantUser  string
	}{
		{
			name:      "cache miss then hit",
			accessKey: "AKIACACHE",
			setup: func(s *MemoryStore) {
				s.CreateUser(context.Background(), &Identity{
					Name:        "cacheduser",
					Credentials: []*Credential{{AccessKey: "AKIACACHE", SecretKey: "secret", Status: "Active"}},
				})
			},
			wantFound: true,
			wantUser:  "cacheduser",
		},
		{
			name:      "not found",
			accessKey: "AKIANOTEXIST",
			setup:     func(s *MemoryStore) {},
			wantFound: false,
			wantUser:  "",
		},
		{
			name:      "disabled user not found",
			accessKey: "AKIADISABLED",
			setup: func(s *MemoryStore) {
				s.CreateUser(context.Background(), &Identity{
					Name:        "disableduser",
					Disabled:    true,
					Credentials: []*Credential{{AccessKey: "AKIADISABLED", SecretKey: "secret", Status: "Active"}},
				})
			},
			wantFound: false,
			wantUser:  "",
		},
		{
			name:      "inactive credential not found",
			accessKey: "AKIAINACTIVE",
			setup: func(s *MemoryStore) {
				s.CreateUser(context.Background(), &Identity{
					Name:        "inactiveuser",
					Credentials: []*Credential{{AccessKey: "AKIAINACTIVE", SecretKey: "secret", Status: "Inactive"}},
				})
			},
			wantFound: false,
			wantUser:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			store := NewMemoryStore()
			tt.setup(store)

			manager := NewManagerWithCache(store, 100, time.Minute)
			defer manager.Stop()

			// First lookup (cache miss)
			identity, _, found := manager.LookupByAccessKey(context.Background(), tt.accessKey)
			assert.Equal(t, tt.wantFound, found)
			if tt.wantFound {
				assert.Equal(t, tt.wantUser, identity.Name)

				// Second lookup (cache hit)
				identity2, _, found2 := manager.LookupByAccessKey(context.Background(), tt.accessKey)
				assert.True(t, found2)
				assert.Equal(t, tt.wantUser, identity2.Name)

				// Verify cache has entry
				assert.Equal(t, 1, manager.CacheSize())
			}
		})
	}
}

func TestManager_InvalidateAccessKey(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	ctx := context.Background()

	store.CreateUser(ctx, &Identity{
		Name:        "cachetest",
		Credentials: []*Credential{{AccessKey: "AKIAINVALIDATE", SecretKey: "secret", Status: "Active"}},
	})

	manager := NewManagerWithCache(store, 100, time.Minute)
	defer manager.Stop()

	// Populate cache
	_, _, found := manager.LookupByAccessKey(ctx, "AKIAINVALIDATE")
	require.True(t, found)
	assert.Equal(t, 1, manager.CacheSize())

	// Invalidate
	manager.InvalidateAccessKey("AKIAINVALIDATE")
	assert.Equal(t, 0, manager.CacheSize())
}

func TestManager_InvalidateCache(t *testing.T) {
	t.Parallel()

	store := NewMemoryStore()
	ctx := context.Background()

	numUsers := 10
	accessKeys := make([]string, numUsers)
	for i := 0; i < numUsers; i++ {
		accessKey := GenerateAccessKey()
		accessKeys[i] = accessKey
		store.CreateUser(ctx, &Identity{
			Name:        GenerateAccessKey(),
			Credentials: []*Credential{{AccessKey: accessKey, SecretKey: GenerateSecretKey(), Status: "Active"}},
		})
	}

	manager := NewManagerWithCache(store, 100, time.Minute)
	defer manager.Stop()

	// Populate cache by looking up each access key
	for _, ak := range accessKeys {
		_, _, found := manager.LookupByAccessKey(ctx, ak)
		require.True(t, found, "should find user with access key %s", ak)
	}
	// Cache should have some entries
	assert.Greater(t, manager.CacheSize(), 0, "cache should have entries")

	// Clear all
	manager.InvalidateCache()
	assert.Equal(t, 0, manager.CacheSize(), "cache should be empty after invalidation")
}

// =============================================================================
// Policy Evaluation Tests
// =============================================================================

func TestPolicy_Evaluate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		policy   *Policy
		ctx      *PolicyEvaluationContext
		expected PolicyDecision
	}{
		{
			name:   "allow all s3 actions",
			policy: FullAccessPolicy(),
			ctx: &PolicyEvaluationContext{
				Action:   "s3:GetObject",
				Resource: "arn:aws:s3:::mybucket/mykey",
			},
			expected: DecisionAllow,
		},
		{
			name:   "read only policy allows get",
			policy: ReadOnlyPolicy(),
			ctx: &PolicyEvaluationContext{
				Action:   "s3:GetObject",
				Resource: "arn:aws:s3:::mybucket/mykey",
			},
			expected: DecisionAllow,
		},
		{
			name:   "read only policy denies put",
			policy: ReadOnlyPolicy(),
			ctx: &PolicyEvaluationContext{
				Action:   "s3:PutObject",
				Resource: "arn:aws:s3:::mybucket/mykey",
			},
			expected: DecisionNotApplicable,
		},
		{
			name:   "bucket access policy",
			policy: BucketAccessPolicy("allowed-bucket", "s3:GetObject", "s3:PutObject"),
			ctx: &PolicyEvaluationContext{
				Action:   "s3:GetObject",
				Resource: "arn:aws:s3:::allowed-bucket/key",
			},
			expected: DecisionAllow,
		},
		{
			name:   "bucket access policy wrong bucket",
			policy: BucketAccessPolicy("allowed-bucket"),
			ctx: &PolicyEvaluationContext{
				Action:   "s3:GetObject",
				Resource: "arn:aws:s3:::other-bucket/key",
			},
			expected: DecisionNotApplicable,
		},
		{
			name: "explicit deny wins",
			policy: &Policy{
				Version: "2012-10-17",
				Statements: []PolicyStatement{
					{Effect: EffectAllow, Actions: StringOrSlice{"s3:*"}, Resources: StringOrSlice{"*"}},
					{Effect: EffectDeny, Actions: StringOrSlice{"s3:DeleteObject"}, Resources: StringOrSlice{"*"}},
				},
			},
			ctx: &PolicyEvaluationContext{
				Action:   "s3:DeleteObject",
				Resource: "arn:aws:s3:::bucket/key",
			},
			expected: DecisionDeny,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.policy.Evaluate(tt.ctx)
			assert.Equal(t, tt.expected, result, "policy evaluation mismatch")
		})
	}
}

func TestPolicyStatement_MatchesConditions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		conditions map[string]Condition
		ctx        *PolicyEvaluationContext
		expected   bool
	}{
		{
			name:       "no conditions always matches",
			conditions: nil,
			ctx:        &PolicyEvaluationContext{},
			expected:   true,
		},
		{
			name: "string equals matches",
			conditions: map[string]Condition{
				"StringEquals": {"aws:username": {"admin"}},
			},
			ctx:      &PolicyEvaluationContext{UserName: "admin"},
			expected: true,
		},
		{
			name: "string equals no match",
			conditions: map[string]Condition{
				"StringEquals": {"aws:username": {"admin"}},
			},
			ctx:      &PolicyEvaluationContext{UserName: "user"},
			expected: false,
		},
		{
			name: "bool condition matches",
			conditions: map[string]Condition{
				"Bool": {"aws:SecureTransport": {"true"}},
			},
			ctx:      &PolicyEvaluationContext{IsHTTPS: true},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			stmt := PolicyStatement{
				Effect:    EffectAllow,
				Actions:   StringOrSlice{"s3:*"},
				Resources: StringOrSlice{"*"},
				Condition: tt.conditions,
			}
			result := stmt.matchesConditions(tt.ctx)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// IAM Config Tests
// =============================================================================

func TestLoadFromConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		config      IAMConfig
		wantErr     bool
		wantUsers   int
		checkAccess func(*testing.T, *Service)
	}{
		{
			name:      "default config",
			config:    DefaultIAMConfig(),
			wantErr:   false,
			wantUsers: 2,
			checkAccess: func(t *testing.T, svc *Service) {
				// Admin should have full access
				identity, _, found := svc.LookupByAccessKey(context.Background(), "test-access-key")
				require.True(t, found)
				assert.Equal(t, "admin", identity.Name)

				allowed := svc.IsAllowed(context.Background(), identity, "s3:DeleteObject", "any-bucket", "any-key")
				assert.True(t, allowed, "admin should have full access")

				// Developer should be read-only
				devIdentity, _, found := svc.LookupByAccessKey(context.Background(), "dev-access-key")
				require.True(t, found)

				readAllowed := svc.IsAllowed(context.Background(), devIdentity, "s3:GetObject", "bucket", "key")
				assert.True(t, readAllowed, "developer should have read access")

				writeAllowed := svc.IsAllowed(context.Background(), devIdentity, "s3:PutObject", "bucket", "key")
				assert.False(t, writeAllowed, "developer should not have write access")
			},
		},
		{
			name: "custom users and groups",
			config: IAMConfig{
				Users: []UserConfig{
					{Name: "alice", AccessKey: "AKIAALICE", SecretKey: "alicesecret", Groups: []string{"writers"}},
					{Name: "bob", AccessKey: "AKIABOB", SecretKey: "bobsecret", Policies: []string{"CustomPolicy"}},
				},
				Groups: []GroupConfig{
					{Name: "writers", Policies: []string{"WritePolicy"}},
				},
				Policies: []PolicyConfig{
					{Name: "WritePolicy", Document: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:PutObject","Resource":"*"}]}`},
					{Name: "CustomPolicy", Document: `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:ListBucket","Resource":"*"}]}`},
				},
			},
			wantErr:   false,
			wantUsers: 2,
			checkAccess: func(t *testing.T, svc *Service) {
				// Alice (via group) should have write access
				aliceIdentity, _, found := svc.LookupByAccessKey(context.Background(), "AKIAALICE")
				require.True(t, found)
				writeAllowed := svc.IsAllowed(context.Background(), aliceIdentity, "s3:PutObject", "bucket", "key")
				assert.True(t, writeAllowed, "alice should have write via group")

				// Bob should have list access
				bobIdentity, _, found := svc.LookupByAccessKey(context.Background(), "AKIABOB")
				require.True(t, found)
				listAllowed := svc.IsAllowed(context.Background(), bobIdentity, "s3:ListBucket", "bucket", "")
				assert.True(t, listAllowed, "bob should have list access")
			},
		},
		{
			name: "invalid policy document",
			config: IAMConfig{
				Policies: []PolicyConfig{
					{Name: "BadPolicy", Document: "not valid json"},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			svc, err := LoadFromConfig(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.NotNil(t, svc)

			users, err := svc.ListUsers(context.Background())
			require.NoError(t, err)
			assert.Len(t, users, tt.wantUsers)

			if tt.checkAccess != nil {
				tt.checkAccess(t, svc)
			}
		})
	}
}

// =============================================================================
// Service Tests
// =============================================================================

func TestService_CreateAccessKey(t *testing.T) {
	t.Parallel()

	svc, err := NewServiceWithDefaults()
	require.NoError(t, err)

	ctx := context.Background()

	// Create new access key for admin
	cred, err := svc.CreateAccessKey(ctx, "admin")
	require.NoError(t, err)
	require.NotNil(t, cred)

	assert.NotEmpty(t, cred.AccessKey)
	assert.NotEmpty(t, cred.SecretKey)
	assert.True(t, len(cred.AccessKey) >= 16)
	assert.True(t, len(cred.SecretKey) >= 30)

	// Should be able to lookup with new key
	identity, _, found := svc.LookupByAccessKey(ctx, cred.AccessKey)
	assert.True(t, found)
	assert.Equal(t, "admin", identity.Name)
}

func TestService_DeleteAccessKey(t *testing.T) {
	t.Parallel()

	svc, err := NewServiceWithDefaults()
	require.NoError(t, err)

	ctx := context.Background()

	// Create new access key
	cred, err := svc.CreateAccessKey(ctx, "admin")
	require.NoError(t, err)

	// Verify it works
	_, _, found := svc.LookupByAccessKey(ctx, cred.AccessKey)
	require.True(t, found)

	// Delete it
	err = svc.DeleteAccessKey(ctx, "admin", cred.AccessKey)
	require.NoError(t, err)

	// Should no longer work
	_, _, found = svc.LookupByAccessKey(ctx, cred.AccessKey)
	assert.False(t, found)
}

// =============================================================================
// STS Tests
// =============================================================================

func TestSTSService_AssumeRole(t *testing.T) {
	t.Parallel()

	cfg := DefaultIAMConfig()
	cfg.EnableSTS = true
	svc, err := LoadFromConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, svc.STS())

	ctx := context.Background()

	// Get admin identity
	adminIdentity, _, found := svc.LookupByAccessKey(ctx, "test-access-key")
	require.True(t, found)

	// Assume role
	sessionCred, err := svc.AssumeRole(ctx, adminIdentity, AssumeRoleInput{
		RoleARN:         "arn:aws:iam::123456789012:role/TestRole",
		RoleSessionName: "test-session",
		DurationSeconds: 3600,
	})
	require.NoError(t, err)
	require.NotNil(t, sessionCred)

	assert.NotEmpty(t, sessionCred.AccessKeyID)
	assert.NotEmpty(t, sessionCred.SecretAccessKey)
	assert.NotEmpty(t, sessionCred.SessionToken)
	assert.False(t, sessionCred.IsExpired())
	assert.Equal(t, "admin", sessionCred.SourceIdentity)

	// Validate session token
	validated, err := svc.ValidateSessionToken(ctx, sessionCred.SessionToken)
	require.NoError(t, err)
	assert.Equal(t, sessionCred.AccessKeyID, validated.AccessKeyID)
}

func TestSTSService_GetSessionToken(t *testing.T) {
	t.Parallel()

	cfg := DefaultIAMConfig()
	cfg.EnableSTS = true
	svc, err := LoadFromConfig(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	identity, _, _ := svc.LookupByAccessKey(ctx, "dev-access-key")
	require.NotNil(t, identity)

	cred, err := svc.GetSessionToken(ctx, identity, GetSessionCredentialsInput{
		DurationSeconds: 900, // 15 min
	})
	require.NoError(t, err)
	require.NotNil(t, cred)

	assert.True(t, cred.Expiration.After(time.Now()))
	assert.True(t, cred.Expiration.Before(time.Now().Add(20*time.Minute)))
}

// =============================================================================
// KMS Tests
// =============================================================================

func TestKMSService_EncryptDecrypt(t *testing.T) {
	t.Parallel()

	cfg := DefaultIAMConfig()
	cfg.EnableKMS = true
	svc, err := LoadFromConfig(cfg)
	require.NoError(t, err)
	require.NotNil(t, svc.KMS())

	ctx := context.Background()

	// Create key
	keyMeta, err := svc.KMSCreateKey(ctx, CreateKeyInput{
		Description: "Test key",
	})
	require.NoError(t, err)
	require.NotNil(t, keyMeta)

	keyID := keyMeta.KeyID

	// Encrypt
	plaintext := []byte("secret data to encrypt")
	ciphertext, err := svc.KMSEncrypt(ctx, keyID, plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, ciphertext)

	// Decrypt
	decrypted, err := svc.KMSDecrypt(ctx, keyID, ciphertext)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestKMSService_GenerateDataKey(t *testing.T) {
	t.Parallel()

	cfg := DefaultIAMConfig()
	cfg.EnableKMS = true
	svc, err := LoadFromConfig(cfg)
	require.NoError(t, err)

	ctx := context.Background()

	// Create master key
	keyMeta, err := svc.KMSCreateKey(ctx, CreateKeyInput{})
	require.NoError(t, err)

	// Generate data key
	plainKey, encryptedKey, err := svc.KMSGenerateDataKey(ctx, keyMeta.KeyID, "AES_256")
	require.NoError(t, err)

	assert.Len(t, plainKey, 32) // AES-256
	assert.NotEqual(t, plainKey, encryptedKey)

	// Encrypted key should be decryptable
	decryptedKey, err := svc.KMSDecrypt(ctx, keyMeta.KeyID, encryptedKey)
	require.NoError(t, err)
	assert.Equal(t, plainKey, decryptedKey)
}

// =============================================================================
// Helper Function Tests
// =============================================================================

func TestGenerateAccessKey(t *testing.T) {
	t.Parallel()

	keys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		key := GenerateAccessKey()
		assert.True(t, len(key) >= 16, "access key should be at least 16 chars")
		assert.False(t, keys[key], "access keys should be unique")
		keys[key] = true
	}
}

func TestGenerateSecretKey(t *testing.T) {
	t.Parallel()

	keys := make(map[string]bool)
	for i := 0; i < 100; i++ {
		key := GenerateSecretKey()
		assert.True(t, len(key) >= 30, "secret key should be at least 30 chars")
		assert.False(t, keys[key], "secret keys should be unique")
		keys[key] = true
	}
}

func TestCredential_IsActive(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cred     Credential
		expected bool
	}{
		{
			name:     "active status",
			cred:     Credential{Status: "Active"},
			expected: true,
		},
		{
			name:     "inactive status",
			cred:     Credential{Status: "Inactive"},
			expected: false,
		},
		{
			name:     "empty status (defaults to active)",
			cred:     Credential{Status: ""},
			expected: true,
		},
		{
			name: "expired",
			cred: Credential{
				Status:    "Active",
				ExpiresAt: func() *time.Time { t := time.Now().Add(-time.Hour); return &t }(),
			},
			expected: false,
		},
		{
			name: "not yet expired",
			cred: Credential{
				Status:    "Active",
				ExpiresAt: func() *time.Time { t := time.Now().Add(time.Hour); return &t }(),
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result := tt.cred.IsActive()
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildResourceARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		bucket   string
		key      string
		expected string
	}{
		{"", "", "arn:aws:s3:::*"},
		{"mybucket", "", "arn:aws:s3:::mybucket"},
		{"mybucket", "mykey", "arn:aws:s3:::mybucket/mykey"},
		{"mybucket", "path/to/object", "arn:aws:s3:::mybucket/path/to/object"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			t.Parallel()
			result := BuildResourceARN(tt.bucket, tt.key)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// =============================================================================
// Crypto Tests
// =============================================================================

func TestMasterKey_EncryptDecrypt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		plaintext string
	}{
		{"short", "hello"},
		{"medium", "this is a secret key 1234567890"},
		{"long", "a very long secret that spans multiple blocks and should still work correctly"},
		{"empty", ""},
		{"special", "!@#$%^&*()_+{}|:<>?"},
	}

	// Generate a test master key
	keyBytes, err := GenerateMasterKey()
	require.NoError(t, err)
	mk, err := NewMasterKey(keyBytes)
	require.NoError(t, err)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Encrypt
			ciphertext, err := mk.Encrypt([]byte(tt.plaintext))
			require.NoError(t, err)
			assert.NotEqual(t, tt.plaintext, string(ciphertext))

			// Decrypt
			decrypted, err := mk.Decrypt(ciphertext)
			require.NoError(t, err)
			assert.Equal(t, tt.plaintext, string(decrypted))
		})
	}
}

func TestMasterKey_DifferentCiphertexts(t *testing.T) {
	t.Parallel()

	keyBytes, _ := GenerateMasterKey()
	mk, _ := NewMasterKey(keyBytes)

	plaintext := []byte("same plaintext")

	// Encrypt same data twice
	ct1, _ := mk.Encrypt(plaintext)
	ct2, _ := mk.Encrypt(plaintext)

	// Should produce different ciphertexts (due to random nonce)
	assert.NotEqual(t, ct1, ct2)

	// But both should decrypt to same plaintext
	pt1, _ := mk.Decrypt(ct1)
	pt2, _ := mk.Decrypt(ct2)
	assert.Equal(t, pt1, pt2)
}

func TestSecureCredential(t *testing.T) {
	t.Parallel()

	cred := &Credential{
		AccessKey: "AKIATEST123",
		SecretKey: "mySecretKey12345",
		Status:    "Active",
	}

	// Secure the credential
	err := SecureCredential(cred)
	require.NoError(t, err)

	// SecretKey should be cleared
	assert.Empty(t, cred.SecretKey)
	// EncryptedSecret should be set
	assert.NotEmpty(t, cred.EncryptedSecret)

	// Unsecure to get it back
	err = UnsecureCredential(cred)
	require.NoError(t, err)

	assert.Equal(t, "mySecretKey12345", cred.SecretKey)
}

func TestDeriveSigningKey(t *testing.T) {
	t.Parallel()

	// Known test vectors from AWS documentation
	secretKey := "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"
	date := "20130524"
	region := "us-east-1"
	service := "s3"

	signingKey := DeriveSigningKey(secretKey, date, region, service)
	assert.NotEmpty(t, signingKey)
	assert.Len(t, signingKey, 32) // SHA256 output
}

func TestVerifySignature(t *testing.T) {
	t.Parallel()

	secretKey := "mySecretKey"
	date := "20231227"
	region := "us-west-2"
	service := "s3"
	stringToSign := "AWS4-HMAC-SHA256\n20231227T120000Z\n..."

	signingKey := DeriveSigningKey(secretKey, date, region, service)
	signature := ComputeSignature(signingKey, stringToSign)

	// Valid signature
	assert.True(t, VerifySignature(secretKey, stringToSign, signature, date, region, service))

	// Invalid signature
	assert.False(t, VerifySignature(secretKey, stringToSign, "invalidsig", date, region, service))

	// Wrong secret
	assert.False(t, VerifySignature("wrongKey", stringToSign, signature, date, region, service))
}
