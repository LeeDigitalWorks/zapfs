//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package kms

import (
	"context"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
)

// Adapter wraps an external KMS Provider to provide an interface compatible
// with the encryption handler's KMSProvider interface.
type Adapter struct {
	provider     Provider
	defaultKeyID string
	mu           sync.RWMutex
	keyCache     map[string]*iam.KeyMetadata // Cache key metadata
}

// NewAdapter creates a new KMS adapter wrapping the given provider.
func NewAdapter(provider Provider, defaultKeyID string) *Adapter {
	return &Adapter{
		provider:     provider,
		defaultKeyID: defaultKeyID,
		keyCache:     make(map[string]*iam.KeyMetadata),
	}
}

// CreateKey creates a new key in the external KMS.
func (a *Adapter) CreateKey(ctx context.Context, input iam.CreateKeyInput) (*iam.KeyMetadata, error) {
	creator, ok := a.provider.(KeyCreator)
	if !ok {
		return nil, ErrNotSupported
	}

	// Types are now unified, can pass directly
	return creator.CreateKey(ctx, input)
}

// GetKey retrieves key metadata from the external KMS.
func (a *Adapter) GetKey(ctx context.Context, keyID string) (*iam.KeyMetadata, error) {
	if keyID == "" {
		keyID = a.defaultKeyID
	}

	// Check cache first
	a.mu.RLock()
	if cached, ok := a.keyCache[keyID]; ok {
		a.mu.RUnlock()
		return cached, nil
	}
	a.mu.RUnlock()

	// Fetch from provider - types are now unified, no conversion needed
	km, err := a.provider.DescribeKey(ctx, keyID)
	if err != nil {
		return nil, err
	}

	// Cache the result
	a.mu.Lock()
	a.keyCache[keyID] = km
	a.mu.Unlock()

	return km, nil
}

// DisableKey disables a key in the external KMS.
func (a *Adapter) DisableKey(ctx context.Context, keyID string) error {
	manager, ok := a.provider.(KeyManager)
	if !ok {
		return ErrNotSupported
	}

	err := manager.DisableKey(ctx, keyID)
	if err != nil {
		return err
	}

	// Invalidate cache
	a.mu.Lock()
	delete(a.keyCache, keyID)
	a.mu.Unlock()

	return nil
}

// EnableKey enables a key in the external KMS.
func (a *Adapter) EnableKey(ctx context.Context, keyID string) error {
	manager, ok := a.provider.(KeyManager)
	if !ok {
		return ErrNotSupported
	}

	err := manager.EnableKey(ctx, keyID)
	if err != nil {
		return err
	}

	// Invalidate cache
	a.mu.Lock()
	delete(a.keyCache, keyID)
	a.mu.Unlock()

	return nil
}

// Encrypt encrypts data using the external KMS.
func (a *Adapter) Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error) {
	if keyID == "" {
		keyID = a.defaultKeyID
	}
	return a.provider.Encrypt(ctx, keyID, plaintext)
}

// Decrypt decrypts data using the external KMS.
func (a *Adapter) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	if keyID == "" {
		keyID = a.defaultKeyID
	}
	return a.provider.Decrypt(ctx, keyID, ciphertext)
}

// GenerateDataKey generates a data encryption key using the external KMS.
func (a *Adapter) GenerateDataKey(ctx context.Context, keyID string, keySpec string) (plaintext, ciphertext []byte, err error) {
	if keyID == "" {
		keyID = a.defaultKeyID
	}
	return a.provider.GenerateDataKey(ctx, keyID, keySpec)
}

// ListKeys returns all key IDs from the external KMS.
func (a *Adapter) ListKeys(ctx context.Context) []string {
	keys, err := a.provider.ListKeys(ctx)
	if err != nil {
		return nil
	}
	return keys
}

// DefaultKeyID returns the default key ID for this adapter.
func (a *Adapter) DefaultKeyID() string {
	return a.defaultKeyID
}

// Provider returns the underlying KMS provider.
func (a *Adapter) Provider() Provider {
	return a.provider
}

// Close closes the underlying provider.
func (a *Adapter) Close() error {
	return a.provider.Close()
}

// ExternalKMSConfig holds configuration for external KMS providers.
// This is the runtime config struct populated from TOML/CLI flags.
type ExternalKMSConfig struct {
	Provider     string       // "aws" or "vault"
	DefaultKeyID string       // Default key to use for operations
	AWS          *AWSConfig   // AWS KMS config (if Provider == "aws")
	Vault        *VaultConfig // Vault config (if Provider == "vault")
}

// NewExternalKMS creates an external KMS adapter from configuration.
// This is the main entry point for initializing external KMS.
func NewExternalKMS(ctx context.Context, cfg ExternalKMSConfig) (*Adapter, error) {
	provider, err := NewProvider(ctx, Config{
		Provider:     cfg.Provider,
		AWS:          cfg.AWS,
		Vault:        cfg.Vault,
		DefaultKeyID: cfg.DefaultKeyID,
	})
	if err != nil {
		return nil, err
	}

	return NewAdapter(provider, cfg.DefaultKeyID), nil
}

// IsConfigured returns true if external KMS is configured.
func IsConfigured(cfg ExternalKMSConfig) bool {
	if cfg.Provider == "" {
		return false
	}
	switch cfg.Provider {
	case "aws":
		return cfg.AWS != nil
	case "vault":
		return cfg.Vault != nil && cfg.Vault.Address != ""
	default:
		return false
	}
}

// MockAdapter creates a mock adapter for testing.
func MockAdapter() *Adapter {
	return &Adapter{
		provider:     &mockProvider{},
		defaultKeyID: "test-key",
		keyCache:     make(map[string]*iam.KeyMetadata),
	}
}

// mockProvider is a simple in-memory provider for testing.
type mockProvider struct{}

func (m *mockProvider) Name() string { return "mock" }

func (m *mockProvider) Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error) {
	// Simple XOR "encryption" for testing only
	result := make([]byte, len(plaintext))
	for i, b := range plaintext {
		result[i] = b ^ 0x42
	}
	return result, nil
}

func (m *mockProvider) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	// Simple XOR "decryption" for testing only
	result := make([]byte, len(ciphertext))
	for i, b := range ciphertext {
		result[i] = b ^ 0x42
	}
	return result, nil
}

func (m *mockProvider) GenerateDataKey(ctx context.Context, keyID string, keySpec string) ([]byte, []byte, error) {
	// Return a fixed key for testing
	plaintext := make([]byte, 32)
	for i := range plaintext {
		plaintext[i] = byte(i)
	}
	ciphertext, _ := m.Encrypt(ctx, keyID, plaintext)
	return plaintext, ciphertext, nil
}

func (m *mockProvider) DescribeKey(ctx context.Context, keyID string) (*KeyMetadata, error) {
	return &KeyMetadata{
		KeyID:        keyID,
		ARN:          "mock:key/" + keyID,
		CreationDate: time.Now(),
		KeyState:     KeyStateEnabled,
		KeyUsage:     "ENCRYPT_DECRYPT",
		Origin:       "MOCK",
		Provider:     "mock",
	}, nil
}

func (m *mockProvider) ListKeys(ctx context.Context) ([]string, error) {
	return []string{"test-key"}, nil
}

func (m *mockProvider) Close() error { return nil }
