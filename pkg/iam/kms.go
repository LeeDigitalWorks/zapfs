// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"io"
	"sync"
	"time"
)

// KMS errors
var (
	ErrKeyNotFound     = errors.New("key not found")
	ErrKeyDisabled     = errors.New("key is disabled")
	ErrInvalidKeyState = errors.New("invalid key state")
	ErrDecryptFailed   = errors.New("decryption failed")
	ErrNotSupported    = errors.New("operation not supported by this provider")
)

// KeyState represents the state of a KMS key
type KeyState string

const (
	KeyStateEnabled         KeyState = "Enabled"
	KeyStateDisabled        KeyState = "Disabled"
	KeyStatePendingDeletion KeyState = "PendingDeletion"
)

// KeyMetadata holds metadata about a KMS key.
// This struct is used by both internal KMS (for testing) and external KMS providers.
type KeyMetadata struct {
	KeyID                string
	ARN                  string // Provider-specific ARN or identifier
	Alias                string // Human-readable alias (optional)
	CreationDate         time.Time
	Description          string
	KeyState             KeyState
	KeyUsage             string // ENCRYPT_DECRYPT, SIGN_VERIFY
	Origin               string // AWS_KMS, EXTERNAL, VAULT, etc.
	Provider             string // aws, vault, gcp, azure (for external KMS)
	DeletionDate         *time.Time
	EncryptionAlgorithms []string
}

// Key represents a KMS key (simplified for local development)
type Key struct {
	Metadata KeyMetadata
	keyData  []byte // The actual key material (32 bytes for AES-256)
}

// KMSService provides Key Management Service functionality
// This is a simplified implementation for local development
type KMSService struct {
	mu   sync.RWMutex
	keys map[string]*Key // keyID -> Key
}

// NewKMSService creates a new KMS service
func NewKMSService() *KMSService {
	return &KMSService{
		keys: make(map[string]*Key),
	}
}

// CreateKeyInput contains parameters for CreateKey.
// Used by both internal KMS and external KMS providers.
type CreateKeyInput struct {
	Description string
	KeyUsage    string            // Default: ENCRYPT_DECRYPT
	Origin      string            // Default: AWS_KMS
	Alias       string            // Human-readable alias (optional)
	Tags        map[string]string // Key-value tags (optional, for AWS/cloud providers)
}

// CreateKey creates a new KMS key
func (k *KMSService) CreateKey(ctx context.Context, input CreateKeyInput) (*KeyMetadata, error) {
	k.mu.Lock()
	defer k.mu.Unlock()

	keyID := generateKeyID()

	// Generate key material
	keyData := make([]byte, 32) // AES-256
	if _, err := rand.Read(keyData); err != nil {
		return nil, err
	}

	keyUsage := input.KeyUsage
	if keyUsage == "" {
		keyUsage = "ENCRYPT_DECRYPT"
	}

	origin := input.Origin
	if origin == "" {
		origin = "AWS_KMS"
	}

	key := &Key{
		Metadata: KeyMetadata{
			KeyID:                keyID,
			ARN:                  "arn:aws:kms:local:000000000000:key/" + keyID,
			CreationDate:         time.Now(),
			Description:          input.Description,
			KeyState:             KeyStateEnabled,
			KeyUsage:             keyUsage,
			Origin:               origin,
			EncryptionAlgorithms: []string{"SYMMETRIC_DEFAULT", "AES_256"},
		},
		keyData: keyData,
	}

	k.keys[keyID] = key
	return &key.Metadata, nil
}

// GetKey retrieves key metadata
func (k *KMSService) GetKey(ctx context.Context, keyID string) (*KeyMetadata, error) {
	k.mu.RLock()
	defer k.mu.RUnlock()

	key, exists := k.keys[keyID]
	if !exists {
		return nil, ErrKeyNotFound
	}

	return &key.Metadata, nil
}

// DisableKey disables a key
func (k *KMSService) DisableKey(ctx context.Context, keyID string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	key, exists := k.keys[keyID]
	if !exists {
		return ErrKeyNotFound
	}

	key.Metadata.KeyState = KeyStateDisabled
	return nil
}

// EnableKey enables a key
func (k *KMSService) EnableKey(ctx context.Context, keyID string) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	key, exists := k.keys[keyID]
	if !exists {
		return ErrKeyNotFound
	}

	if key.Metadata.KeyState == KeyStatePendingDeletion {
		return ErrInvalidKeyState
	}

	key.Metadata.KeyState = KeyStateEnabled
	return nil
}

// Encrypt encrypts data using a KMS key
func (k *KMSService) Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error) {
	k.mu.RLock()
	key, exists := k.keys[keyID]
	k.mu.RUnlock()

	if !exists {
		return nil, ErrKeyNotFound
	}

	if key.Metadata.KeyState != KeyStateEnabled {
		return nil, ErrKeyDisabled
	}

	// AES-GCM encryption
	block, err := aes.NewCipher(key.keyData)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts data using a KMS key
func (k *KMSService) Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error) {
	k.mu.RLock()
	key, exists := k.keys[keyID]
	k.mu.RUnlock()

	if !exists {
		return nil, ErrKeyNotFound
	}

	if key.Metadata.KeyState != KeyStateEnabled {
		return nil, ErrKeyDisabled
	}

	// AES-GCM decryption
	block, err := aes.NewCipher(key.keyData)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, ErrDecryptFailed
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, ErrDecryptFailed
	}

	return plaintext, nil
}

// GenerateDataKey generates a data encryption key
func (k *KMSService) GenerateDataKey(ctx context.Context, keyID string, keySpec string) (plaintext, ciphertext []byte, err error) {
	k.mu.RLock()
	key, exists := k.keys[keyID]
	k.mu.RUnlock()

	if !exists {
		return nil, nil, ErrKeyNotFound
	}

	if key.Metadata.KeyState != KeyStateEnabled {
		return nil, nil, ErrKeyDisabled
	}

	// Generate data key
	var keySize int
	switch keySpec {
	case "AES_128":
		keySize = 16
	case "AES_256", "":
		keySize = 32
	default:
		keySize = 32
	}

	plaintext = make([]byte, keySize)
	if _, err := rand.Read(plaintext); err != nil {
		return nil, nil, err
	}

	// Encrypt the data key with the master key
	ciphertext, err = k.Encrypt(ctx, keyID, plaintext)
	if err != nil {
		return nil, nil, err
	}

	return plaintext, ciphertext, nil
}

// ListKeys returns all key IDs
func (k *KMSService) ListKeys(ctx context.Context) []string {
	k.mu.RLock()
	defer k.mu.RUnlock()

	keyIDs := make([]string, 0, len(k.keys))
	for id := range k.keys {
		keyIDs = append(keyIDs, id)
	}
	return keyIDs
}

// generateKeyID generates a random key ID
func generateKeyID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return base64.RawURLEncoding.EncodeToString(b)
}
