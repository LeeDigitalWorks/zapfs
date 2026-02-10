// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
)

const encryptedPrefix = "enc:"

// encryptSecret encrypts a plaintext string using AES-256-GCM.
// Returns a base64-encoded ciphertext prefixed with "enc:".
// Empty strings are returned as-is.
// If key is nil or empty, the plaintext is returned unchanged (no encryption).
func encryptSecret(key []byte, plaintext string) (string, error) {
	if plaintext == "" {
		return "", nil
	}
	if len(key) == 0 {
		return plaintext, nil // No key configured
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create gcm: %w", err)
	}

	nonce := make([]byte, aesGCM.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return "", fmt.Errorf("generate nonce: %w", err)
	}

	ciphertext := aesGCM.Seal(nonce, nonce, []byte(plaintext), nil)
	return encryptedPrefix + base64.StdEncoding.EncodeToString(ciphertext), nil
}

// decryptSecret decrypts an "enc:"-prefixed base64-encoded AES-256-GCM ciphertext.
// If the string doesn't have the prefix, returns it as-is (backward compatible
// with unencrypted legacy data).
// If key is nil or empty, the ciphertext is returned unchanged.
func decryptSecret(key []byte, ciphertext string) (string, error) {
	if ciphertext == "" {
		return "", nil
	}
	if len(key) == 0 {
		return ciphertext, nil // No key configured
	}

	if !strings.HasPrefix(ciphertext, encryptedPrefix) {
		return ciphertext, nil // Not encrypted (legacy data)
	}

	data, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(ciphertext, encryptedPrefix))
	if err != nil {
		return "", fmt.Errorf("decode base64: %w", err)
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return "", fmt.Errorf("create cipher: %w", err)
	}

	aesGCM, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("create gcm: %w", err)
	}

	nonceSize := aesGCM.NonceSize()
	if len(data) < nonceSize {
		return "", fmt.Errorf("ciphertext too short")
	}

	nonce, encrypted := data[:nonceSize], data[nonceSize:]
	plaintext, err := aesGCM.Open(nil, nonce, encrypted, nil)
	if err != nil {
		return "", fmt.Errorf("decrypt: %w", err)
	}

	return string(plaintext), nil
}
