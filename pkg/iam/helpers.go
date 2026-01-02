// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package iam

import (
	"crypto/rand"
	"encoding/hex"
)

// GenerateAccessKey generates a new AWS-style access key ID.
// Format: AKIA + 16 hex characters (20 chars total)
func GenerateAccessKey() string {
	b := make([]byte, 10)
	rand.Read(b)
	return "AKIA" + hex.EncodeToString(b)[:16]
}

// GenerateSecretKey generates a new secret access key.
// Returns a 40-character hex string.
func GenerateSecretKey() string {
	b := make([]byte, 30)
	rand.Read(b)
	return hex.EncodeToString(b)[:40]
}

// generateID generates a random ID for internal use.
func generateID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}
