//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package license

import (
	"crypto/rand"
	"crypto/rsa"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// Generator creates signed license keys.
// This should only be used by ZapFS internally to issue licenses.
type Generator struct {
	privateKey *rsa.PrivateKey
	keyID      string // Key ID to include in JWT header for key rotation support
}

// NewGenerator creates a license generator with the given RSA private key.
// Uses the default key ID for the "kid" header in generated tokens.
func NewGenerator(privateKeyPEM []byte) (*Generator, error) {
	return NewGeneratorWithKeyID(privateKeyPEM, DefaultKeyID)
}

// NewGeneratorWithKeyID creates a license generator with a specific key ID.
// The key ID is included in the JWT header ("kid") to support key rotation.
// When verifying licenses, the Manager uses this key ID to select the correct public key.
func NewGeneratorWithKeyID(privateKeyPEM []byte, keyID string) (*Generator, error) {
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	if keyID == "" {
		keyID = DefaultKeyID
	}

	return &Generator{
		privateKey: privateKey,
		keyID:      keyID,
	}, nil
}

// LicenseRequest contains the parameters for generating a new license.
type LicenseRequest struct {
	CustomerID   string
	CustomerName string
	Features     []Feature
	Tier         string
	ValidDays    int
}

// Generate creates a signed license key from the request.
func (g *Generator) Generate(req LicenseRequest) (string, error) {
	// Generate a unique license ID
	idBytes := make([]byte, 8)
	if _, err := rand.Read(idBytes); err != nil {
		return "", fmt.Errorf("failed to generate license ID: %w", err)
	}
	licenseID := hex.EncodeToString(idBytes)

	now := time.Now()
	expiresAt := now.Add(time.Duration(req.ValidDays) * 24 * time.Hour)

	claims := licenseClaims{
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:    "zapfs",
			Subject:   req.CustomerID,
			IssuedAt:  jwt.NewNumericDate(now),
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			ID:        licenseID,
		},
		CustomerID:   req.CustomerID,
		CustomerName: req.CustomerName,
		Features:     req.Features,
		LicenseID:    licenseID,
		Tier:         req.Tier,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)

	// Add key ID to header for key rotation support
	token.Header["kid"] = g.keyID

	signedToken, err := token.SignedString(g.privateKey)
	if err != nil {
		return "", fmt.Errorf("failed to sign license: %w", err)
	}

	return signedToken, nil
}

// GenerateKeyPair generates a new RSA key pair for license signing.
// Returns (privateKeyPEM, publicKeyPEM, error).
// This should be used once to generate the key pair, then the keys should be stored securely.
func GenerateKeyPair() ([]byte, []byte, error) {
	privateKey, err := rsa.GenerateKey(rand.Reader, 4096)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to generate key pair: %w", err)
	}

	privateKeyPEM := encodePrivateKeyToPEM(privateKey)
	publicKeyPEM := encodePublicKeyToPEM(&privateKey.PublicKey)

	return privateKeyPEM, publicKeyPEM, nil
}
