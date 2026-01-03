//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package kms

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Provider Tests
// =============================================================================

func TestNewProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		cfg         Config
		expectError string
	}{
		{
			name: "aws provider without config",
			cfg: Config{
				Provider: "aws",
				AWS:      nil,
			},
			expectError: "AWS KMS configuration required",
		},
		{
			name: "vault provider without config",
			cfg: Config{
				Provider: "vault",
				Vault:    nil,
			},
			expectError: "vault configuration required",
		},
		{
			name: "unsupported provider",
			cfg: Config{
				Provider: "unknown",
			},
			expectError: "unsupported KMS provider: unknown",
		},
		{
			name: "empty provider",
			cfg: Config{
				Provider: "",
			},
			expectError: "unsupported KMS provider:",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ctx := context.Background()
			_, err := NewProvider(ctx, tc.cfg)

			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.expectError)
		})
	}
}

// =============================================================================
// Adapter Tests
// =============================================================================

func TestNewAdapter(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{}
	defaultKeyID := "test-default-key"

	adapter := NewAdapter(provider, defaultKeyID)

	assert.NotNil(t, adapter)
	assert.Equal(t, defaultKeyID, adapter.DefaultKeyID())
	assert.Equal(t, provider, adapter.Provider())
}

func TestAdapter_Encrypt(t *testing.T) {
	t.Parallel()

	adapter := MockAdapter()
	ctx := context.Background()

	tests := []struct {
		name      string
		keyID     string
		plaintext []byte
	}{
		{
			name:      "with explicit key ID",
			keyID:     "my-key",
			plaintext: []byte("secret data"),
		},
		{
			name:      "with empty key ID uses default",
			keyID:     "",
			plaintext: []byte("secret data"),
		},
		{
			name:      "empty plaintext",
			keyID:     "my-key",
			plaintext: []byte{},
		},
		{
			name:      "large plaintext",
			keyID:     "my-key",
			plaintext: make([]byte, 1024),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			ciphertext, err := adapter.Encrypt(ctx, tc.keyID, tc.plaintext)
			require.NoError(t, err)
			assert.Len(t, ciphertext, len(tc.plaintext))

			// Verify we can decrypt back
			decrypted, err := adapter.Decrypt(ctx, tc.keyID, ciphertext)
			require.NoError(t, err)
			assert.Equal(t, tc.plaintext, decrypted)
		})
	}
}

func TestAdapter_Decrypt(t *testing.T) {
	t.Parallel()

	adapter := MockAdapter()
	ctx := context.Background()

	// First encrypt something
	plaintext := []byte("test data for decryption")
	ciphertext, err := adapter.Encrypt(ctx, "key-1", plaintext)
	require.NoError(t, err)

	tests := []struct {
		name       string
		keyID      string
		ciphertext []byte
	}{
		{
			name:       "with explicit key ID",
			keyID:      "key-1",
			ciphertext: ciphertext,
		},
		{
			name:       "with empty key ID uses default",
			keyID:      "",
			ciphertext: ciphertext,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			decrypted, err := adapter.Decrypt(ctx, tc.keyID, tc.ciphertext)
			require.NoError(t, err)
			assert.Equal(t, plaintext, decrypted)
		})
	}
}

func TestAdapter_GenerateDataKey(t *testing.T) {
	t.Parallel()

	adapter := MockAdapter()
	ctx := context.Background()

	tests := []struct {
		name    string
		keyID   string
		keySpec string
	}{
		{
			name:    "AES 256",
			keyID:   "master-key",
			keySpec: "AES_256",
		},
		{
			name:    "with empty key ID",
			keyID:   "",
			keySpec: "AES_256",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			plaintext, ciphertext, err := adapter.GenerateDataKey(ctx, tc.keyID, tc.keySpec)
			require.NoError(t, err)
			assert.Len(t, plaintext, 32)
			assert.Len(t, ciphertext, 32)

			// Verify the ciphertext decrypts to plaintext
			decrypted, err := adapter.Decrypt(ctx, tc.keyID, ciphertext)
			require.NoError(t, err)
			assert.Equal(t, plaintext, decrypted)
		})
	}
}

func TestAdapter_GetKey(t *testing.T) {
	t.Parallel()

	adapter := MockAdapter()
	ctx := context.Background()

	t.Run("get key by ID", func(t *testing.T) {
		t.Parallel()

		metadata, err := adapter.GetKey(ctx, "my-key-id")
		require.NoError(t, err)
		assert.Equal(t, "my-key-id", metadata.KeyID)
		assert.Equal(t, KeyStateEnabled, metadata.KeyState)
		assert.Equal(t, "mock", metadata.Provider)
	})

	t.Run("get key with empty ID uses default", func(t *testing.T) {
		t.Parallel()

		metadata, err := adapter.GetKey(ctx, "")
		require.NoError(t, err)
		assert.Equal(t, "test-key", metadata.KeyID) // default key from MockAdapter
	})

	t.Run("cache hit", func(t *testing.T) {
		t.Parallel()

		localAdapter := MockAdapter()

		// First call - populates cache
		metadata1, err := localAdapter.GetKey(ctx, "cached-key")
		require.NoError(t, err)

		// Second call - should hit cache
		metadata2, err := localAdapter.GetKey(ctx, "cached-key")
		require.NoError(t, err)

		// Should return the same cached object
		assert.Equal(t, metadata1.KeyID, metadata2.KeyID)
	})
}

func TestAdapter_ListKeys(t *testing.T) {
	t.Parallel()

	adapter := MockAdapter()
	ctx := context.Background()

	keys := adapter.ListKeys(ctx)
	assert.Contains(t, keys, "test-key")
}

func TestAdapter_CreateKey_NotSupported(t *testing.T) {
	t.Parallel()

	// mockProvider doesn't implement KeyCreator
	adapter := MockAdapter()
	ctx := context.Background()

	_, err := adapter.CreateKey(ctx, iam.CreateKeyInput{
		Description: "test key",
	})

	assert.ErrorIs(t, err, ErrNotSupported)
}

func TestAdapter_EnableDisableKey_NotSupported(t *testing.T) {
	t.Parallel()

	// mockProvider doesn't implement KeyManager
	adapter := MockAdapter()
	ctx := context.Background()

	t.Run("enable not supported", func(t *testing.T) {
		t.Parallel()
		err := adapter.EnableKey(ctx, "key-1")
		assert.ErrorIs(t, err, ErrNotSupported)
	})

	t.Run("disable not supported", func(t *testing.T) {
		t.Parallel()
		err := adapter.DisableKey(ctx, "key-1")
		assert.ErrorIs(t, err, ErrNotSupported)
	})
}

func TestAdapter_Close(t *testing.T) {
	t.Parallel()

	adapter := MockAdapter()
	err := adapter.Close()
	assert.NoError(t, err)
}

// =============================================================================
// IsConfigured Tests
// =============================================================================

func TestIsConfigured(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      ExternalKMSConfig
		expected bool
	}{
		{
			name:     "empty config",
			cfg:      ExternalKMSConfig{},
			expected: false,
		},
		{
			name: "provider only no details",
			cfg: ExternalKMSConfig{
				Provider: "aws",
			},
			expected: false,
		},
		{
			name: "aws with config",
			cfg: ExternalKMSConfig{
				Provider: "aws",
				AWS: &AWSConfig{
					Region: "us-east-1",
				},
			},
			expected: true,
		},
		{
			name: "vault without address",
			cfg: ExternalKMSConfig{
				Provider: "vault",
				Vault:    &VaultConfig{},
			},
			expected: false,
		},
		{
			name: "vault with address",
			cfg: ExternalKMSConfig{
				Provider: "vault",
				Vault: &VaultConfig{
					Address: "https://vault.example.com:8200",
				},
			},
			expected: true,
		},
		{
			name: "unknown provider",
			cfg: ExternalKMSConfig{
				Provider: "unknown",
			},
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := IsConfigured(tc.cfg)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// =============================================================================
// MockAdapter Tests
// =============================================================================

func TestMockAdapter(t *testing.T) {
	t.Parallel()

	adapter := MockAdapter()

	assert.NotNil(t, adapter)
	assert.Equal(t, "test-key", adapter.DefaultKeyID())
	assert.Equal(t, "mock", adapter.Provider().Name())
}

// =============================================================================
// mockProvider Tests
// =============================================================================

func TestMockProvider(t *testing.T) {
	t.Parallel()

	provider := &mockProvider{}
	ctx := context.Background()

	t.Run("name", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, "mock", provider.Name())
	})

	t.Run("encrypt decrypt roundtrip", func(t *testing.T) {
		t.Parallel()

		plaintext := []byte("hello world")
		ciphertext, err := provider.Encrypt(ctx, "key", plaintext)
		require.NoError(t, err)

		// Ciphertext should be different from plaintext
		assert.NotEqual(t, plaintext, ciphertext)

		decrypted, err := provider.Decrypt(ctx, "key", ciphertext)
		require.NoError(t, err)
		assert.Equal(t, plaintext, decrypted)
	})

	t.Run("generate data key", func(t *testing.T) {
		t.Parallel()

		plaintext, ciphertext, err := provider.GenerateDataKey(ctx, "key", "AES_256")
		require.NoError(t, err)

		assert.Len(t, plaintext, 32)
		assert.Len(t, ciphertext, 32)

		// Verify the key is deterministic (for testing)
		for i := 0; i < 32; i++ {
			assert.Equal(t, byte(i), plaintext[i])
		}
	})

	t.Run("describe key", func(t *testing.T) {
		t.Parallel()

		metadata, err := provider.DescribeKey(ctx, "my-key")
		require.NoError(t, err)

		assert.Equal(t, "my-key", metadata.KeyID)
		assert.Equal(t, "mock:key/my-key", metadata.ARN)
		assert.Equal(t, KeyStateEnabled, metadata.KeyState)
		assert.Equal(t, "ENCRYPT_DECRYPT", metadata.KeyUsage)
		assert.Equal(t, "MOCK", metadata.Origin)
		assert.Equal(t, "mock", metadata.Provider)
		assert.WithinDuration(t, time.Now(), metadata.CreationDate, time.Second)
	})

	t.Run("list keys", func(t *testing.T) {
		t.Parallel()

		keys, err := provider.ListKeys(ctx)
		require.NoError(t, err)
		assert.Equal(t, []string{"test-key"}, keys)
	})

	t.Run("close", func(t *testing.T) {
		t.Parallel()

		err := provider.Close()
		assert.NoError(t, err)
	})
}

// =============================================================================
// Full Provider with KeyCreator and KeyManager Tests
// =============================================================================

// fullMockProvider implements Provider, KeyCreator, and KeyManager for testing
type fullMockProvider struct {
	mockProvider
	keys        map[string]*KeyMetadata
	createErr   error
	enableErr   error
	disableErr  error
	describeErr error
}

func newFullMockProvider() *fullMockProvider {
	return &fullMockProvider{
		keys: make(map[string]*KeyMetadata),
	}
}

func (f *fullMockProvider) CreateKey(ctx context.Context, input CreateKeyInput) (*KeyMetadata, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}

	keyID := "generated-key-id"
	metadata := &KeyMetadata{
		KeyID:        keyID,
		ARN:          "mock:key/" + keyID,
		CreationDate: time.Now(),
		Description:  input.Description,
		KeyState:     KeyStateEnabled,
		KeyUsage:     "ENCRYPT_DECRYPT",
		Origin:       "MOCK",
		Provider:     "mock",
	}
	f.keys[keyID] = metadata
	return metadata, nil
}

func (f *fullMockProvider) EnableKey(ctx context.Context, keyID string) error {
	if f.enableErr != nil {
		return f.enableErr
	}
	if meta, ok := f.keys[keyID]; ok {
		meta.KeyState = KeyStateEnabled
	}
	return nil
}

func (f *fullMockProvider) DisableKey(ctx context.Context, keyID string) error {
	if f.disableErr != nil {
		return f.disableErr
	}
	if meta, ok := f.keys[keyID]; ok {
		meta.KeyState = KeyStateDisabled
	}
	return nil
}

func (f *fullMockProvider) ScheduleKeyDeletion(ctx context.Context, keyID string, pendingDays int) error {
	if meta, ok := f.keys[keyID]; ok {
		meta.KeyState = KeyStatePendingDeletion
		deletionDate := time.Now().AddDate(0, 0, pendingDays)
		meta.DeletionDate = &deletionDate
	}
	return nil
}

func (f *fullMockProvider) DescribeKey(ctx context.Context, keyID string) (*KeyMetadata, error) {
	if f.describeErr != nil {
		return nil, f.describeErr
	}
	if meta, ok := f.keys[keyID]; ok {
		return meta, nil
	}
	return f.mockProvider.DescribeKey(ctx, keyID)
}

func TestAdapter_WithFullProvider(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("create key", func(t *testing.T) {
		t.Parallel()

		provider := newFullMockProvider()
		adapter := NewAdapter(provider, "default-key")

		metadata, err := adapter.CreateKey(ctx, iam.CreateKeyInput{
			Description: "Test key for encryption",
		})

		require.NoError(t, err)
		assert.Equal(t, "generated-key-id", metadata.KeyID)
		assert.Equal(t, "Test key for encryption", metadata.Description)
		assert.Equal(t, KeyStateEnabled, metadata.KeyState)
	})

	t.Run("create key error", func(t *testing.T) {
		t.Parallel()

		provider := newFullMockProvider()
		provider.createErr = errors.New("creation failed")
		adapter := NewAdapter(provider, "default-key")

		_, err := adapter.CreateKey(ctx, iam.CreateKeyInput{})
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "creation failed")
	})

	t.Run("enable key", func(t *testing.T) {
		t.Parallel()

		provider := newFullMockProvider()
		adapter := NewAdapter(provider, "default-key")

		// Create a key first
		meta, _ := adapter.CreateKey(ctx, iam.CreateKeyInput{})

		// Disable then enable
		err := adapter.DisableKey(ctx, meta.KeyID)
		require.NoError(t, err)

		err = adapter.EnableKey(ctx, meta.KeyID)
		require.NoError(t, err)
	})

	t.Run("disable key", func(t *testing.T) {
		t.Parallel()

		provider := newFullMockProvider()
		adapter := NewAdapter(provider, "default-key")

		// Create a key first
		meta, _ := adapter.CreateKey(ctx, iam.CreateKeyInput{})

		err := adapter.DisableKey(ctx, meta.KeyID)
		require.NoError(t, err)
	})

	t.Run("enable key error", func(t *testing.T) {
		t.Parallel()

		provider := newFullMockProvider()
		provider.enableErr = errors.New("enable failed")
		adapter := NewAdapter(provider, "default-key")

		err := adapter.EnableKey(ctx, "some-key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "enable failed")
	})

	t.Run("disable key error", func(t *testing.T) {
		t.Parallel()

		provider := newFullMockProvider()
		provider.disableErr = errors.New("disable failed")
		adapter := NewAdapter(provider, "default-key")

		err := adapter.DisableKey(ctx, "some-key")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "disable failed")
	})

	t.Run("cache invalidation on disable", func(t *testing.T) {
		t.Parallel()

		provider := newFullMockProvider()
		adapter := NewAdapter(provider, "default-key")

		// Create and cache a key
		meta, _ := adapter.CreateKey(ctx, iam.CreateKeyInput{})
		_, _ = adapter.GetKey(ctx, meta.KeyID)

		// Disable should invalidate cache
		err := adapter.DisableKey(ctx, meta.KeyID)
		require.NoError(t, err)

		// Next GetKey should fetch fresh data
		updated, err := adapter.GetKey(ctx, meta.KeyID)
		require.NoError(t, err)
		assert.Equal(t, KeyStateDisabled, updated.KeyState)
	})

	t.Run("cache invalidation on enable", func(t *testing.T) {
		t.Parallel()

		provider := newFullMockProvider()
		adapter := NewAdapter(provider, "default-key")

		// Create and cache a key
		meta, _ := adapter.CreateKey(ctx, iam.CreateKeyInput{})
		_, _ = adapter.GetKey(ctx, meta.KeyID)
		_ = adapter.DisableKey(ctx, meta.KeyID)

		// Enable should invalidate cache
		err := adapter.EnableKey(ctx, meta.KeyID)
		require.NoError(t, err)

		// Next GetKey should fetch fresh data
		updated, err := adapter.GetKey(ctx, meta.KeyID)
		require.NoError(t, err)
		assert.Equal(t, KeyStateEnabled, updated.KeyState)
	})
}

// =============================================================================
// Error Handling Tests
// =============================================================================

// errorProvider is a provider that returns errors for testing
type errorProvider struct {
	mockProvider
	encryptErr  error
	decryptErr  error
	generateErr error
	describeErr error
	listErr     error
}

func (e *errorProvider) Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error) {
	if e.encryptErr != nil {
		return nil, e.encryptErr
	}
	return e.mockProvider.Encrypt(ctx, keyID, plaintext)
}

func (e *errorProvider) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	if e.decryptErr != nil {
		return nil, e.decryptErr
	}
	return e.mockProvider.Decrypt(ctx, keyID, ciphertext)
}

func (e *errorProvider) GenerateDataKey(ctx context.Context, keyID string, keySpec string) ([]byte, []byte, error) {
	if e.generateErr != nil {
		return nil, nil, e.generateErr
	}
	return e.mockProvider.GenerateDataKey(ctx, keyID, keySpec)
}

func (e *errorProvider) DescribeKey(ctx context.Context, keyID string) (*KeyMetadata, error) {
	if e.describeErr != nil {
		return nil, e.describeErr
	}
	return e.mockProvider.DescribeKey(ctx, keyID)
}

func (e *errorProvider) ListKeys(ctx context.Context) ([]string, error) {
	if e.listErr != nil {
		return nil, e.listErr
	}
	return e.mockProvider.ListKeys(ctx)
}

func TestAdapter_ErrorHandling(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	t.Run("encrypt error", func(t *testing.T) {
		t.Parallel()

		provider := &errorProvider{encryptErr: errors.New("encryption failed")}
		adapter := NewAdapter(provider, "default")

		_, err := adapter.Encrypt(ctx, "key", []byte("data"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "encryption failed")
	})

	t.Run("decrypt error", func(t *testing.T) {
		t.Parallel()

		provider := &errorProvider{decryptErr: errors.New("decryption failed")}
		adapter := NewAdapter(provider, "default")

		_, err := adapter.Decrypt(ctx, "key", []byte("data"))
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "decryption failed")
	})

	t.Run("generate data key error", func(t *testing.T) {
		t.Parallel()

		provider := &errorProvider{generateErr: errors.New("generation failed")}
		adapter := NewAdapter(provider, "default")

		_, _, err := adapter.GenerateDataKey(ctx, "key", "AES_256")
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "generation failed")
	})

	t.Run("describe key error", func(t *testing.T) {
		t.Parallel()

		provider := &errorProvider{describeErr: ErrKeyNotFound}
		adapter := NewAdapter(provider, "default")

		_, err := adapter.GetKey(ctx, "nonexistent")
		assert.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("list keys error returns nil", func(t *testing.T) {
		t.Parallel()

		provider := &errorProvider{listErr: errors.New("list failed")}
		adapter := NewAdapter(provider, "default")

		keys := adapter.ListKeys(ctx)
		assert.Nil(t, keys)
	})
}
