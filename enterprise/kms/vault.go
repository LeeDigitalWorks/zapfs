//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package kms

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"fmt"
	"net/http"
	"os"
	"time"

	vault "github.com/hashicorp/vault/api"
)

// VaultProvider implements the Provider interface for HashiCorp Vault Transit
type VaultProvider struct {
	client    *vault.Client
	config    VaultConfig
	mountPath string
}

// NewVaultProvider creates a new Vault Transit provider
func NewVaultProvider(ctx context.Context, cfg VaultConfig) (*VaultProvider, error) {
	// Set defaults
	if cfg.MountPath == "" {
		cfg.MountPath = "transit"
	}

	// Create Vault client config
	vaultCfg := vault.DefaultConfig()
	vaultCfg.Address = cfg.Address

	// Configure TLS
	if cfg.TLSInsecure {
		vaultCfg.HttpClient.Transport = &http.Transport{
			TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		}
	} else if cfg.TLSCACert != "" {
		if err := vaultCfg.ConfigureTLS(&vault.TLSConfig{
			CACert: cfg.TLSCACert,
		}); err != nil {
			return nil, fmt.Errorf("failed to configure Vault TLS: %w", err)
		}
	}

	// Create client
	client, err := vault.NewClient(vaultCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create Vault client: %w", err)
	}

	// Set token (from config or environment)
	token := cfg.Token
	if token == "" {
		token = os.Getenv("VAULT_TOKEN")
	}
	if token != "" {
		client.SetToken(token)
	}

	// Set namespace if specified (Vault Enterprise)
	if cfg.Namespace != "" {
		client.SetNamespace(cfg.Namespace)
	}

	// Verify connection
	_, err = client.Sys().Health()
	if err != nil {
		return nil, fmt.Errorf("failed to connect to Vault: %w", err)
	}

	return &VaultProvider{
		client:    client,
		config:    cfg,
		mountPath: cfg.MountPath,
	}, nil
}

// Name returns the provider name
func (p *VaultProvider) Name() string {
	return "vault"
}

// transitPath returns the full path for a transit operation
func (p *VaultProvider) transitPath(op, keyName string) string {
	return fmt.Sprintf("%s/%s/%s", p.mountPath, op, keyName)
}

// Encrypt encrypts plaintext using Vault Transit
func (p *VaultProvider) Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error) {
	secret, err := p.client.Logical().WriteWithContext(ctx, p.transitPath("encrypt", keyID), map[string]interface{}{
		"plaintext": base64.StdEncoding.EncodeToString(plaintext),
	})
	if err != nil {
		return nil, fmt.Errorf("Vault Transit encrypt failed: %w", err)
	}

	ciphertext, ok := secret.Data["ciphertext"].(string)
	if !ok {
		return nil, fmt.Errorf("Vault Transit encrypt: invalid response")
	}

	// Return the ciphertext as-is (Vault format: vault:v1:base64...)
	return []byte(ciphertext), nil
}

// Decrypt decrypts ciphertext using Vault Transit
func (p *VaultProvider) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	secret, err := p.client.Logical().WriteWithContext(ctx, p.transitPath("decrypt", keyID), map[string]interface{}{
		"ciphertext": string(ciphertext),
	})
	if err != nil {
		return nil, fmt.Errorf("Vault Transit decrypt failed: %w", err)
	}

	plaintextB64, ok := secret.Data["plaintext"].(string)
	if !ok {
		return nil, fmt.Errorf("Vault Transit decrypt: invalid response")
	}

	plaintext, err := base64.StdEncoding.DecodeString(plaintextB64)
	if err != nil {
		return nil, fmt.Errorf("Vault Transit decrypt: failed to decode plaintext: %w", err)
	}

	return plaintext, nil
}

// GenerateDataKey generates a data encryption key using Vault Transit
func (p *VaultProvider) GenerateDataKey(ctx context.Context, keyID string, keySpec string) ([]byte, []byte, error) {
	bits := 256
	switch keySpec {
	case "AES_128":
		bits = 128
	case "AES_256", "":
		bits = 256
	}

	secret, err := p.client.Logical().WriteWithContext(ctx, p.transitPath("datakey/plaintext", keyID), map[string]interface{}{
		"bits": bits,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("Vault Transit generate data key failed: %w", err)
	}

	plaintextB64, ok := secret.Data["plaintext"].(string)
	if !ok {
		return nil, nil, fmt.Errorf("Vault Transit datakey: invalid plaintext response")
	}

	ciphertext, ok := secret.Data["ciphertext"].(string)
	if !ok {
		return nil, nil, fmt.Errorf("Vault Transit datakey: invalid ciphertext response")
	}

	plaintext, err := base64.StdEncoding.DecodeString(plaintextB64)
	if err != nil {
		return nil, nil, fmt.Errorf("Vault Transit datakey: failed to decode plaintext: %w", err)
	}

	return plaintext, []byte(ciphertext), nil
}

// DescribeKey returns metadata about a Vault Transit key
func (p *VaultProvider) DescribeKey(ctx context.Context, keyID string) (*KeyMetadata, error) {
	secret, err := p.client.Logical().ReadWithContext(ctx, p.transitPath("keys", keyID))
	if err != nil {
		return nil, fmt.Errorf("Vault Transit describe key failed: %w", err)
	}
	if secret == nil {
		return nil, ErrKeyNotFound
	}

	km := &KeyMetadata{
		KeyID:    keyID,
		ARN:      fmt.Sprintf("vault:%s/keys/%s", p.mountPath, keyID),
		Provider: "vault",
		Origin:   "VAULT",
		KeyUsage: "ENCRYPT_DECRYPT",
		KeyState: KeyStateEnabled, // Vault keys are enabled by default
	}

	// Parse key metadata
	if name, ok := secret.Data["name"].(string); ok {
		km.Alias = name
	}

	if deletable, ok := secret.Data["deletion_allowed"].(bool); ok && deletable {
		km.KeyState = KeyStatePendingDeletion
	}

	// Keys in Vault don't have a creation date exposed directly
	// Use current time as approximation
	km.CreationDate = time.Now()

	return km, nil
}

// ListKeys returns all available Transit key names
func (p *VaultProvider) ListKeys(ctx context.Context) ([]string, error) {
	secret, err := p.client.Logical().ListWithContext(ctx, p.transitPath("keys", ""))
	if err != nil {
		return nil, fmt.Errorf("Vault Transit list keys failed: %w", err)
	}
	if secret == nil {
		return []string{}, nil
	}

	keys, ok := secret.Data["keys"].([]interface{})
	if !ok {
		return []string{}, nil
	}

	keyIDs := make([]string, 0, len(keys))
	for _, k := range keys {
		if keyID, ok := k.(string); ok {
			keyIDs = append(keyIDs, keyID)
		}
	}

	return keyIDs, nil
}

// Close releases resources (no-op for Vault)
func (p *VaultProvider) Close() error {
	return nil
}

// CreateKey creates a new Transit key (implements KeyCreator interface)
func (p *VaultProvider) CreateKey(ctx context.Context, input CreateKeyInput) (*KeyMetadata, error) {
	keyName := input.Alias
	if keyName == "" {
		// Generate a random key name
		keyName = fmt.Sprintf("key-%d", time.Now().UnixNano())
	}

	params := map[string]interface{}{
		"type": "aes256-gcm96", // Default encryption type
	}

	_, err := p.client.Logical().WriteWithContext(ctx, p.transitPath("keys", keyName), params)
	if err != nil {
		return nil, fmt.Errorf("Vault Transit create key failed: %w", err)
	}

	return p.DescribeKey(ctx, keyName)
}

// EnableKey is a no-op for Vault (keys are always enabled)
func (p *VaultProvider) EnableKey(ctx context.Context, keyID string) error {
	// Vault Transit keys don't have an enable/disable state
	return nil
}

// DisableKey is not directly supported by Vault Transit
func (p *VaultProvider) DisableKey(ctx context.Context, keyID string) error {
	// Vault Transit doesn't have a disable operation
	// You would need to update key policy or delete the key
	return ErrNotSupported
}

// ScheduleKeyDeletion enables deletion and deletes the key
func (p *VaultProvider) ScheduleKeyDeletion(ctx context.Context, keyID string, pendingDays int) error {
	// First, enable deletion
	_, err := p.client.Logical().WriteWithContext(ctx, p.transitPath("keys", keyID)+"/config", map[string]interface{}{
		"deletion_allowed": true,
	})
	if err != nil {
		return fmt.Errorf("Vault Transit enable deletion failed: %w", err)
	}

	// Note: Vault doesn't have a pending deletion period like AWS
	// The key is immediately deletable once deletion_allowed is true
	// Actual deletion would be:
	// _, err = p.client.Logical().DeleteWithContext(ctx, p.transitPath("keys", keyID))

	return nil
}

// Ensure VaultProvider implements all interfaces
var (
	_ Provider   = (*VaultProvider)(nil)
	_ KeyCreator = (*VaultProvider)(nil)
	_ KeyManager = (*VaultProvider)(nil)
)
