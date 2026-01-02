// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"os"
	"sync"
)

// MasterKey provides encryption/decryption for secret keys at rest.
// The master key should come from environment variables or a secrets manager.
type MasterKey struct {
	key []byte
}

var (
	globalMasterKey     *MasterKey
	globalMasterKeyOnce sync.Once
	globalMasterKeyErr  error
)

// GetMasterKey returns the global master key, loading from environment if needed.
// The key is expected in ZAPFS_IAM_MASTER_KEY environment variable (base64 encoded).
func GetMasterKey() (*MasterKey, error) {
	globalMasterKeyOnce.Do(func() {
		keyStr := os.Getenv("ZAPFS_IAM_MASTER_KEY")
		if keyStr == "" {
			// For development, generate a random key (warn that it won't persist)
			key := make([]byte, 32)
			if _, err := io.ReadFull(rand.Reader, key); err != nil {
				globalMasterKeyErr = fmt.Errorf("failed to generate master key: %w", err)
				return
			}
			globalMasterKey = &MasterKey{key: key}
			// Note: In dev mode, secrets won't survive restart
			return
		}

		key, err := base64.StdEncoding.DecodeString(keyStr)
		if err != nil {
			globalMasterKeyErr = fmt.Errorf("invalid ZAPFS_IAM_MASTER_KEY: must be base64 encoded: %w", err)
			return
		}
		if len(key) != 32 {
			globalMasterKeyErr = fmt.Errorf("ZAPFS_IAM_MASTER_KEY must be 32 bytes (AES-256), got %d", len(key))
			return
		}
		globalMasterKey = &MasterKey{key: key}
	})

	return globalMasterKey, globalMasterKeyErr
}

// NewMasterKey creates a master key from raw bytes (for testing)
func NewMasterKey(key []byte) (*MasterKey, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("master key must be 32 bytes for AES-256, got %d", len(key))
	}
	return &MasterKey{key: key}, nil
}

// GenerateMasterKey generates a new random master key
func GenerateMasterKey() ([]byte, error) {
	key := make([]byte, 32)
	if _, err := io.ReadFull(rand.Reader, key); err != nil {
		return nil, err
	}
	return key, nil
}

// Encrypt encrypts data using AES-256-GCM
func (m *MasterKey) Encrypt(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(m.key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Prepend nonce to ciphertext
	ciphertext := aesGCM.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts data using AES-256-GCM
func (m *MasterKey) Decrypt(ciphertext []byte) ([]byte, error) {
	block, err := aes.NewCipher(m.key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := aesGCM.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, encryptedData := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := aesGCM.Open(nil, nonce, encryptedData, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}

// EncryptCredentialSecret encrypts a secret key for storage
func EncryptCredentialSecret(secretKey string) ([]byte, error) {
	mk, err := GetMasterKey()
	if err != nil {
		return nil, err
	}
	return mk.Encrypt([]byte(secretKey))
}

// DecryptCredentialSecret decrypts a stored secret key
func DecryptCredentialSecret(encrypted []byte) (string, error) {
	if len(encrypted) == 0 {
		return "", fmt.Errorf("no encrypted data")
	}
	mk, err := GetMasterKey()
	if err != nil {
		return "", err
	}
	plaintext, err := mk.Decrypt(encrypted)
	if err != nil {
		return "", err
	}
	return string(plaintext), nil
}

// DeriveSigningKey derives an HMAC signing key from the secret key.
// This follows AWS S3 signature v4 derivation:
//
//	kDate = HMAC("AWS4" + secretKey, date)
//	kRegion = HMAC(kDate, region)
//	kService = HMAC(kRegion, service)
//	kSigning = HMAC(kService, "aws4_request")
//
// For simplicity, we use a single-step derivation for local signing key.
func DeriveSigningKey(secretKey, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

// ComputeSignature computes an HMAC-SHA256 signature
func ComputeSignature(signingKey []byte, stringToSign string) string {
	sig := hmacSHA256(signingKey, []byte(stringToSign))
	return fmt.Sprintf("%x", sig)
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// VerifySignature verifies an S3 signature
func VerifySignature(secretKey, stringToSign, providedSignature, date, region, service string) bool {
	signingKey := DeriveSigningKey(secretKey, date, region, service)
	expectedSig := ComputeSignature(signingKey, stringToSign)
	return hmac.Equal([]byte(expectedSig), []byte(providedSignature))
}

// SecureCredential encrypts the secret key in a credential for storage.
// After calling this, SecretKey will be empty and EncryptedSecret will be set.
func SecureCredential(cred *Credential) error {
	if cred.SecretKey == "" {
		return nil // Already secured or no secret
	}

	encrypted, err := EncryptCredentialSecret(cred.SecretKey)
	if err != nil {
		return err
	}

	cred.EncryptedSecret = encrypted
	cred.SecretKey = "" // Clear plaintext
	return nil
}

// UnsecureCredential decrypts the secret key in a credential.
// This should only be used when the secret key is actually needed (signature verification).
func UnsecureCredential(cred *Credential) error {
	if len(cred.EncryptedSecret) == 0 {
		return nil // Not encrypted
	}

	secretKey, err := DecryptCredentialSecret(cred.EncryptedSecret)
	if err != nil {
		return err
	}

	cred.SecretKey = secretKey
	return nil
}
