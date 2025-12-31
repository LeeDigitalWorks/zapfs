//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package license

// ProductionPublicKeys contains the RSA public keys used to verify license signatures.
// Multiple keys are supported to enable key rotation without breaking existing licenses.
//
// Key rotation process:
// 1. Generate new key pair (e.g., v2)
// 2. Add v2 public key to this map
// 3. Release new ZapFS binary
// 4. Start signing new licenses with v2 (update DefaultKeyID in portal)
// 5. Old licenses (signed with v1) continue to work
// 6. Eventually remove v1 when all licenses have been renewed
//
// IMPORTANT: The private keys must be kept secure on the license generation server.
// Only the public keys are embedded here.
var ProductionPublicKeys = map[string][]byte{
	// v1: Initial production key
	// TODO: Replace with actual production public key before first release
	// Generate with: license.GenerateKeyPair()
	"v1": nil, // Placeholder - set to nil until production key is generated
}

// GetProductionPublicKey returns the public key for the given key ID.
// Returns nil if the key ID is not found.
func GetProductionPublicKey(keyID string) []byte {
	return ProductionPublicKeys[keyID]
}

// GetDefaultPublicKey returns the default public key (v1).
// Returns nil if no default key is configured.
func GetDefaultPublicKey() []byte {
	return ProductionPublicKeys[DefaultKeyID]
}

// HasProductionKeys returns true if any production public keys are configured.
func HasProductionKeys() bool {
	for _, key := range ProductionPublicKeys {
		if len(key) > 0 {
			return true
		}
	}
	return false
}
