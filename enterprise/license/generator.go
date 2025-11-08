//go:build enterprise

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
}

// NewGenerator creates a license generator with the given RSA private key.
func NewGenerator(privateKeyPEM []byte) (*Generator, error) {
	privateKey, err := jwt.ParseRSAPrivateKeyFromPEM(privateKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key: %w", err)
	}

	return &Generator{
		privateKey: privateKey,
	}, nil
}

// LicenseRequest contains the parameters for generating a new license.
type LicenseRequest struct {
	CustomerID    string
	CustomerName  string
	Features      []Feature
	MaxNodes      int
	MaxCapacityTB int
	Tier          string
	ValidDays     int
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
		CustomerID:    req.CustomerID,
		CustomerName:  req.CustomerName,
		Features:      req.Features,
		MaxNodes:      req.MaxNodes,
		MaxCapacityTB: req.MaxCapacityTB,
		LicenseID:     licenseID,
		Tier:          req.Tier,
	}

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
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
