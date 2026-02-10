// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncryptDecryptSecret(t *testing.T) {
	key := make([]byte, 32)
	key[0] = 1 // Non-zero key for testing

	plaintext := "my-secret-access-key"

	encrypted, err := encryptSecret(key, plaintext)
	require.NoError(t, err)
	assert.NotEqual(t, plaintext, encrypted)
	assert.True(t, len(encrypted) > 0)
	assert.Contains(t, encrypted, encryptedPrefix)

	decrypted, err := decryptSecret(key, encrypted)
	require.NoError(t, err)
	assert.Equal(t, plaintext, decrypted)
}

func TestEncryptSecret_EmptyInput(t *testing.T) {
	key := make([]byte, 32)

	encrypted, err := encryptSecret(key, "")
	require.NoError(t, err)
	assert.Equal(t, "", encrypted)
}

func TestEncryptSecret_NilKey(t *testing.T) {
	// No key = passthrough (backward compat)
	encrypted, err := encryptSecret(nil, "my-secret")
	require.NoError(t, err)
	assert.Equal(t, "my-secret", encrypted)
}

func TestEncryptSecret_EmptyKey(t *testing.T) {
	// Empty key = passthrough (backward compat)
	encrypted, err := encryptSecret([]byte{}, "my-secret")
	require.NoError(t, err)
	assert.Equal(t, "my-secret", encrypted)
}

func TestDecryptSecret_EmptyInput(t *testing.T) {
	key := make([]byte, 32)

	decrypted, err := decryptSecret(key, "")
	require.NoError(t, err)
	assert.Equal(t, "", decrypted)
}

func TestDecryptSecret_LegacyUnencrypted(t *testing.T) {
	key := make([]byte, 32)

	// No "enc:" prefix = legacy unencrypted data, returned as-is
	decrypted, err := decryptSecret(key, "plaintext-secret")
	require.NoError(t, err)
	assert.Equal(t, "plaintext-secret", decrypted)
}

func TestDecryptSecret_NilKey(t *testing.T) {
	// No key = passthrough (backward compat)
	decrypted, err := decryptSecret(nil, "enc:somedata")
	require.NoError(t, err)
	assert.Equal(t, "enc:somedata", decrypted)
}

func TestDecryptSecret_EmptyKey(t *testing.T) {
	// Empty key = passthrough (backward compat)
	decrypted, err := decryptSecret([]byte{}, "enc:somedata")
	require.NoError(t, err)
	assert.Equal(t, "enc:somedata", decrypted)
}

func TestEncryptSecret_DifferentNonces(t *testing.T) {
	key := make([]byte, 32)
	key[0] = 2

	enc1, err := encryptSecret(key, "same-plaintext")
	require.NoError(t, err)

	enc2, err := encryptSecret(key, "same-plaintext")
	require.NoError(t, err)

	// Same plaintext should produce different ciphertexts (random nonce)
	assert.NotEqual(t, enc1, enc2)

	// Both should decrypt to the same value
	dec1, err := decryptSecret(key, enc1)
	require.NoError(t, err)
	dec2, err := decryptSecret(key, enc2)
	require.NoError(t, err)
	assert.Equal(t, dec1, dec2)
	assert.Equal(t, "same-plaintext", dec1)
}

func TestDecryptSecret_InvalidBase64(t *testing.T) {
	key := make([]byte, 32)

	_, err := decryptSecret(key, "enc:not-valid-base64!!!")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decode base64")
}

func TestDecryptSecret_CiphertextTooShort(t *testing.T) {
	key := make([]byte, 32)

	// Valid base64 but too short for nonce + ciphertext
	_, err := decryptSecret(key, "enc:AAAA")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "ciphertext too short")
}

func TestDecryptSecret_WrongKey(t *testing.T) {
	key1 := make([]byte, 32)
	key1[0] = 1
	key2 := make([]byte, 32)
	key2[0] = 2

	encrypted, err := encryptSecret(key1, "my-secret")
	require.NoError(t, err)

	_, err = decryptSecret(key2, encrypted)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "decrypt")
}
