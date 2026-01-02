//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package kms provides external Key Management Service integration.
// This package is only available in the enterprise edition of ZapFS.
//
// Supported KMS providers:
// - AWS KMS
// - HashiCorp Vault Transit
// - Google Cloud KMS (planned)
// - Azure Key Vault (planned)
package kms

import (
	"context"
	"errors"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
)

// Re-export types from pkg/iam for convenience within this package.
// This allows external KMS code to use kms.KeyMetadata etc. while
// the actual type definition lives in pkg/iam.
type (
	KeyState       = iam.KeyState
	KeyMetadata    = iam.KeyMetadata
	CreateKeyInput = iam.CreateKeyInput
)

// Re-export error variables from pkg/iam for convenience.
var (
	ErrKeyNotFound     = iam.ErrKeyNotFound
	ErrKeyDisabled     = iam.ErrKeyDisabled
	ErrInvalidKeyState = iam.ErrInvalidKeyState
	ErrDecryptFailed   = iam.ErrDecryptFailed
	ErrNotSupported    = iam.ErrNotSupported
)

// Re-export KeyState constants from pkg/iam for convenience.
const (
	KeyStateEnabled         = iam.KeyStateEnabled
	KeyStateDisabled        = iam.KeyStateDisabled
	KeyStatePendingDeletion = iam.KeyStatePendingDeletion
)

// Provider defines the interface for KMS providers
type Provider interface {
	// Name returns the provider name (aws, vault, gcp, azure)
	Name() string

	// Encrypt encrypts plaintext using the specified key
	Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error)

	// Decrypt decrypts ciphertext using the specified key
	Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error)

	// GenerateDataKey generates a data encryption key wrapped by the master key
	// Returns (plaintext DEK, encrypted DEK, error)
	GenerateDataKey(ctx context.Context, keyID string, keySpec string) ([]byte, []byte, error)

	// DescribeKey returns metadata about a key
	DescribeKey(ctx context.Context, keyID string) (*KeyMetadata, error)

	// ListKeys returns all available key IDs
	ListKeys(ctx context.Context) ([]string, error)

	// Close releases any resources held by the provider
	Close() error
}

// KeyCreator is an optional interface for providers that support key creation
type KeyCreator interface {
	CreateKey(ctx context.Context, input CreateKeyInput) (*KeyMetadata, error)
}

// KeyManager is an optional interface for providers that support key management
type KeyManager interface {
	EnableKey(ctx context.Context, keyID string) error
	DisableKey(ctx context.Context, keyID string) error
	ScheduleKeyDeletion(ctx context.Context, keyID string, pendingDays int) error
}

// Config holds common configuration for KMS providers
type Config struct {
	// Provider type: aws, vault, gcp, azure
	Provider string `json:"provider"`

	// AWS KMS configuration
	AWS *AWSConfig `json:"aws,omitempty"`

	// HashiCorp Vault configuration
	Vault *VaultConfig `json:"vault,omitempty"`

	// Default key ID to use when not specified
	DefaultKeyID string `json:"default_key_id"`
}

// AWSConfig holds AWS KMS configuration
type AWSConfig struct {
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id,omitempty"`
	SecretAccessKey string `json:"secret_access_key,omitempty"`
	Endpoint        string `json:"endpoint,omitempty"` // For LocalStack/testing
	RoleARN         string `json:"role_arn,omitempty"` // For cross-account access
}

// VaultConfig holds HashiCorp Vault Transit configuration
type VaultConfig struct {
	Address     string `json:"address"`               // Vault server address
	Token       string `json:"token,omitempty"`       // Vault token (or use VAULT_TOKEN env)
	MountPath   string `json:"mount_path,omitempty"`  // Transit mount path (default: transit)
	Namespace   string `json:"namespace,omitempty"`   // Vault namespace (enterprise)
	TLSCACert   string `json:"tls_ca_cert,omitempty"` // CA cert for TLS verification
	TLSInsecure bool   `json:"tls_insecure,omitempty"`
}

// NewProvider creates a new KMS provider based on configuration
func NewProvider(ctx context.Context, cfg Config) (Provider, error) {
	switch cfg.Provider {
	case "aws":
		if cfg.AWS == nil {
			return nil, errors.New("AWS KMS configuration required")
		}
		return NewAWSProvider(ctx, *cfg.AWS)
	case "vault":
		if cfg.Vault == nil {
			return nil, errors.New("vault configuration required")
		}
		return NewVaultProvider(ctx, *cfg.Vault)
	default:
		return nil, errors.New("unsupported KMS provider: " + cfg.Provider)
	}
}
