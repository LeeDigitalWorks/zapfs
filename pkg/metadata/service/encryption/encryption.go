// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package encryption provides encryption/decryption services for SSE-C and SSE-KMS.
package encryption

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
)

// KMSProvider defines the interface for KMS operations.
// Both internal (iam.KMSService) and external (kms.Adapter) providers implement this.
type KMSProvider interface {
	// GetKey retrieves key metadata
	GetKey(ctx context.Context, keyID string) (*iam.KeyMetadata, error)
	// Encrypt encrypts data using the specified key
	Encrypt(ctx context.Context, keyID string, plaintext []byte) ([]byte, error)
	// Decrypt decrypts data using the specified key
	Decrypt(ctx context.Context, keyID string, ciphertext []byte) ([]byte, error)
	// GenerateDataKey generates a data encryption key wrapped by the master key
	GenerateDataKey(ctx context.Context, keyID string, keySpec string) (plaintext, ciphertext []byte, err error)
}

// Handler provides encryption/decryption services
type Handler struct {
	kmsProvider KMSProvider // For SSE-KMS (may be nil if KMS not available)
}

// NewHandler creates a new encryption handler with an internal KMS service.
// For external KMS, use NewHandlerWithProvider.
func NewHandler(kmsService *iam.KMSService) *Handler {
	if kmsService == nil {
		return &Handler{kmsProvider: nil}
	}
	return &Handler{
		kmsProvider: kmsService,
	}
}

// NewHandlerWithProvider creates a new encryption handler with a custom KMS provider.
// Use this for external KMS providers (AWS KMS, Vault, etc.).
func NewHandlerWithProvider(provider KMSProvider) *Handler {
	return &Handler{
		kmsProvider: provider,
	}
}

// Encrypt encrypts data using the specified parameters.
// Returns the encrypted data and metadata for storage.
func (h *Handler) Encrypt(ctx context.Context, plaintext []byte, params *Params) (*Result, error) {
	if params == nil || (!params.IsSSEC() && !params.IsSSEKMS()) {
		return &Result{Ciphertext: plaintext, Metadata: nil}, nil
	}

	if params.IsSSEC() {
		return h.encryptSSEC(plaintext, params.SSEC)
	}

	return h.encryptSSEKMS(ctx, plaintext, params.SSEKMS)
}

// Decrypt decrypts data using the specified parameters.
// For SSE-C, the key must be provided in params.SSEC.Key.
// For SSE-KMS, the DEK is decrypted using KMS.
func (h *Handler) Decrypt(ctx context.Context, ciphertext []byte, metadata *Metadata, params *Params) ([]byte, error) {
	if metadata == nil {
		return ciphertext, nil
	}

	switch metadata.Algorithm {
	case "AES256":
		if params == nil || params.SSEC == nil || params.SSEC.Key == nil {
			return nil, fmt.Errorf("SSE-C key required for decryption")
		}
		return DecryptSSEC(ciphertext, params.SSEC.Key)

	case "aws:kms":
		return h.decryptSSEKMS(ctx, ciphertext, metadata)

	default:
		return ciphertext, nil
	}
}

// encryptSSEC encrypts data using SSE-C (AES-256-CBC)
func (h *Handler) encryptSSEC(plaintext []byte, params *SSECParams) (*Result, error) {
	ciphertext, err := EncryptSSEC(plaintext, params.Key)
	if err != nil {
		return nil, err
	}

	return &Result{
		Ciphertext: ciphertext,
		Metadata: &Metadata{
			Algorithm:      params.Algorithm,
			CustomerKeyMD5: params.KeyMD5,
		},
	}, nil
}

// encryptSSEKMS encrypts data using SSE-KMS (AES-256-GCM with KMS-managed DEK)
func (h *Handler) encryptSSEKMS(ctx context.Context, plaintext []byte, params *SSEKMSParams) (*Result, error) {
	if h.kmsProvider == nil {
		return nil, fmt.Errorf("KMS service not available")
	}

	// Validate KMS key exists
	_, err := h.kmsProvider.GetKey(ctx, params.KeyID)
	if err != nil {
		if err == iam.ErrKeyNotFound {
			return nil, fmt.Errorf("KMS key not found: %s", params.KeyID)
		}
		if err == iam.ErrKeyDisabled {
			return nil, fmt.Errorf("KMS key disabled: %s", params.KeyID)
		}
		return nil, fmt.Errorf("failed to get KMS key: %w", err)
	}

	// Generate data encryption key (DEK)
	dekPlaintext, dekCiphertext, err := h.kmsProvider.GenerateDataKey(ctx, params.KeyID, "AES_256")
	if err != nil {
		return nil, fmt.Errorf("failed to generate data key: %w", err)
	}

	// Encrypt data using DEK
	ciphertext, err := EncryptKMSDEK(plaintext, dekPlaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to encrypt with DEK: %w", err)
	}

	// Store encrypted DEK in metadata
	dekCiphertextBase64 := base64.StdEncoding.EncodeToString(dekCiphertext)

	return &Result{
		Ciphertext: ciphertext,
		Metadata: &Metadata{
			Algorithm:     "aws:kms",
			KMSKeyID:      params.KeyID,
			KMSContext:    params.Context,
			DEKCiphertext: dekCiphertextBase64,
		},
	}, nil
}

// decryptSSEKMS decrypts data using SSE-KMS
func (h *Handler) decryptSSEKMS(ctx context.Context, ciphertext []byte, metadata *Metadata) ([]byte, error) {
	if h.kmsProvider == nil {
		return nil, fmt.Errorf("KMS service not available")
	}

	if metadata.DEKCiphertext == "" {
		return nil, fmt.Errorf("missing encrypted DEK in metadata")
	}

	// Decode encrypted DEK
	dekCiphertext, err := base64.StdEncoding.DecodeString(metadata.DEKCiphertext)
	if err != nil {
		return nil, fmt.Errorf("failed to decode encrypted DEK: %w", err)
	}

	// Decrypt DEK using KMS
	dekPlaintext, err := h.kmsProvider.Decrypt(ctx, metadata.KMSKeyID, dekCiphertext)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt DEK: %w", err)
	}

	// Decrypt data using DEK
	plaintext, err := DecryptKMSDEK(ciphertext, dekPlaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt data: %w", err)
	}

	return plaintext, nil
}

// HasKMS returns true if KMS is available
func (h *Handler) HasKMS() bool {
	return h.kmsProvider != nil
}

// DEKResult contains the result of generating a data encryption key
type DEKResult struct {
	DEKPlaintext  []byte // Plaintext DEK (32 bytes for AES-256)
	DEKCiphertext string // Base64-encoded encrypted DEK
}

// GenerateDEK generates a data encryption key using KMS.
// This is used for multipart uploads where the DEK is generated at upload initiation
// and reused for each part.
func (h *Handler) GenerateDEK(ctx context.Context, keyID string) (*DEKResult, error) {
	if h.kmsProvider == nil {
		return nil, fmt.Errorf("KMS service not available")
	}

	// Generate data encryption key
	dekPlaintext, dekCiphertext, err := h.kmsProvider.GenerateDataKey(ctx, keyID, "AES_256")
	if err != nil {
		return nil, fmt.Errorf("failed to generate data key: %w", err)
	}

	return &DEKResult{
		DEKPlaintext:  dekPlaintext,
		DEKCiphertext: base64.StdEncoding.EncodeToString(dekCiphertext),
	}, nil
}

// DecryptDEK decrypts an encrypted DEK using KMS.
// This is used for multipart uploads to decrypt the DEK for each part.
func (h *Handler) DecryptDEK(ctx context.Context, keyID, dekCiphertextBase64 string) ([]byte, error) {
	if h.kmsProvider == nil {
		return nil, fmt.Errorf("KMS service not available")
	}

	dekCiphertext, err := base64.StdEncoding.DecodeString(dekCiphertextBase64)
	if err != nil {
		return nil, fmt.Errorf("failed to decode encrypted DEK: %w", err)
	}

	dekPlaintext, err := h.kmsProvider.Decrypt(ctx, keyID, dekCiphertext)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt DEK: %w", err)
	}

	return dekPlaintext, nil
}

// ValidateSSECKey validates an SSE-C key and returns its MD5 hash
func ValidateSSECKey(key []byte, providedMD5 string) (string, error) {
	if len(key) != 32 {
		return "", fmt.Errorf("SSE-C key must be 32 bytes for AES-256, got %d", len(key))
	}

	// Calculate MD5 of key
	keyMD5Hash := md5.Sum(key)
	keyMD5 := base64.StdEncoding.EncodeToString(keyMD5Hash[:])

	// Validate provided MD5 matches
	if providedMD5 != "" && providedMD5 != keyMD5 {
		return "", fmt.Errorf("key MD5 mismatch")
	}

	return keyMD5, nil
}

// EncryptSSEC encrypts data using SSE-C (AES-256-CBC).
// AWS S3 uses CBC mode for SSE-C compatibility.
// Returns encrypted data with IV prepended.
func EncryptSSEC(plaintext []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("SSE-C key must be 32 bytes for AES-256, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Generate random IV (16 bytes for AES-128/256 block size)
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("failed to generate IV: %w", err)
	}

	// Pad plaintext to block size (PKCS#7 padding)
	padding := aes.BlockSize - (len(plaintext) % aes.BlockSize)
	paddedPlaintext := make([]byte, len(plaintext)+padding)
	copy(paddedPlaintext, plaintext)
	for i := len(plaintext); i < len(paddedPlaintext); i++ {
		paddedPlaintext[i] = byte(padding)
	}

	// Encrypt using CBC mode
	mode := cipher.NewCBCEncrypter(block, iv)
	ciphertext := make([]byte, len(paddedPlaintext))
	mode.CryptBlocks(ciphertext, paddedPlaintext)

	// Prepend IV to ciphertext
	result := make([]byte, len(iv)+len(ciphertext))
	copy(result, iv)
	copy(result[len(iv):], ciphertext)

	return result, nil
}

// DecryptSSEC decrypts data using SSE-C (AES-256-CBC).
// Expects encrypted data with IV prepended.
func DecryptSSEC(ciphertext []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("SSE-C key must be 32 bytes for AES-256, got %d", len(key))
	}

	if len(ciphertext) < aes.BlockSize {
		return nil, fmt.Errorf("ciphertext too short (must include IV)")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Extract IV and encrypted data
	iv := ciphertext[:aes.BlockSize]
	encryptedData := ciphertext[aes.BlockSize:]

	// Validate encrypted data length is multiple of block size
	if len(encryptedData)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("encrypted data length must be multiple of block size")
	}

	// Decrypt using CBC mode
	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(encryptedData))
	mode.CryptBlocks(plaintext, encryptedData)

	// Remove PKCS#7 padding
	if len(plaintext) == 0 {
		return nil, fmt.Errorf("decrypted data is empty")
	}
	padding := int(plaintext[len(plaintext)-1])
	if padding > aes.BlockSize || padding == 0 {
		return nil, fmt.Errorf("invalid padding")
	}
	if padding > len(plaintext) {
		return nil, fmt.Errorf("padding exceeds data length")
	}

	// Validate padding bytes
	for i := len(plaintext) - padding; i < len(plaintext); i++ {
		if plaintext[i] != byte(padding) {
			return nil, fmt.Errorf("invalid padding")
		}
	}

	return plaintext[:len(plaintext)-padding], nil
}

// EncryptKMSDEK encrypts data using a KMS-generated data encryption key (DEK).
// Uses AES-256-GCM.
func EncryptKMSDEK(plaintext []byte, dek []byte) ([]byte, error) {
	if len(dek) != 32 {
		return nil, fmt.Errorf("DEK must be 32 bytes for AES-256, got %d", len(dek))
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// DecryptKMSDEK decrypts data using a KMS-generated data encryption key (DEK).
// Uses AES-256-GCM.
func DecryptKMSDEK(ciphertext []byte, dek []byte) ([]byte, error) {
	if len(dek) != 32 {
		return nil, fmt.Errorf("DEK must be 32 bytes for AES-256, got %d", len(dek))
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}

	return plaintext, nil
}

// ParseStoredKMSContext parses the stored KMS context which contains both
// the user-provided context and the encrypted DEK.
// Format: "context|dekCiphertextBase64" or "|dekCiphertextBase64"
func ParseStoredKMSContext(stored string) (context, dekCiphertext string) {
	if stored == "" {
		return "", ""
	}
	parts := strings.SplitN(stored, "|", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return stored, ""
}

// BuildStoredKMSContext builds the stored KMS context string.
func BuildStoredKMSContext(context, dekCiphertext string) string {
	if dekCiphertext == "" {
		return context
	}
	return context + "|" + dekCiphertext
}
