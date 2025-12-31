//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package license provides enterprise license key validation and feature gating.
//
// License keys are cryptographically signed JWT tokens that contain:
// - Customer identification
// - Enabled feature flags
// - Capacity limits (nodes, storage)
// - Expiration date
//
// License validation is performed offline using RSA public key verification.
// No "phone home" or network connectivity is required.
package license

import (
	"crypto/rsa"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// License represents a validated enterprise license.
type License struct {
	// CustomerID is the unique identifier for the customer
	CustomerID string `json:"customer_id"`

	// CustomerName is the display name of the customer
	CustomerName string `json:"customer_name"`

	// Features is the list of enabled enterprise features
	Features []Feature `json:"features"`

	// MaxNodes is the maximum number of nodes allowed (0 = unlimited)
	MaxNodes int `json:"max_nodes"`

	// MaxCapacityTB is the maximum storage capacity in TB (0 = unlimited)
	MaxCapacityTB int `json:"max_capacity_tb"`

	// IssuedAt is when the license was issued
	IssuedAt time.Time `json:"issued_at"`

	// ExpiresAt is when the license expires
	ExpiresAt time.Time `json:"expires_at"`

	// LicenseID is the unique identifier for this license
	LicenseID string `json:"license_id"`

	// Tier is the license tier (e.g., "standard", "premium", "enterprise")
	Tier string `json:"tier"`
}

// IsExpired returns true if the license has expired.
func (l *License) IsExpired() bool {
	return time.Now().After(l.ExpiresAt)
}

// DaysUntilExpiry returns the number of days until the license expires.
// Returns negative values if already expired.
func (l *License) DaysUntilExpiry() int {
	return int(time.Until(l.ExpiresAt).Hours() / 24)
}

// HasFeature returns true if the license includes the specified feature.
func (l *License) HasFeature(feature Feature) bool {
	for _, f := range l.Features {
		if f == feature {
			return true
		}
	}
	return false
}

// Validate checks if the license is valid for use.
func (l *License) Validate() error {
	if l.IsExpired() {
		return ErrLicenseExpired
	}
	if l.CustomerID == "" {
		return ErrInvalidLicense
	}
	return nil
}

// licenseClaims represents the JWT claims in a license key.
type licenseClaims struct {
	jwt.RegisteredClaims
	CustomerID    string    `json:"cid"`
	CustomerName  string    `json:"cnm"`
	Features      []Feature `json:"ftr"`
	MaxNodes      int       `json:"mxn"`
	MaxCapacityTB int       `json:"mxc"`
	LicenseID     string    `json:"lid"`
	Tier          string    `json:"tier"`
}

// Manager handles license validation and feature checks.
// The license field uses atomic.Pointer for lock-free reads and safe hot-reloading.
type Manager struct {
	license   atomic.Pointer[License]
	publicKey *rsa.PublicKey // Single key mode (for backward compatibility)

	// Multi-key support for key rotation
	publicKeys map[string]*rsa.PublicKey // keyID -> publicKey

	// For file-based license reloading
	mu          sync.Mutex // protects licenseFile and reload operations
	licenseFile string
}

// Common errors
var (
	ErrNoLicense             = errors.New("no license loaded")
	ErrLicenseExpired        = errors.New("license has expired")
	ErrInvalidLicense        = errors.New("invalid license")
	ErrFeatureDisabled       = errors.New("feature not enabled by license")
	ErrInvalidKey            = errors.New("invalid license key")
	ErrNodeLimitExceeded     = errors.New("node limit exceeded")
	ErrCapacityLimitExceeded = errors.New("capacity limit exceeded")
)

// NewManager creates a new license manager with the given RSA public key.
// The public key is used to verify license signatures.
// For multi-key support (key rotation), use NewManagerWithKeys instead.
func NewManager(publicKeyPEM []byte) (*Manager, error) {
	publicKey, err := jwt.ParseRSAPublicKeyFromPEM(publicKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	return &Manager{
		publicKey: publicKey,
	}, nil
}

// NewManagerWithKeys creates a license manager that supports multiple public keys.
// This enables key rotation - new licenses can be signed with a new key while
// old licenses signed with previous keys remain valid.
//
// The keyID in the JWT header ("kid") is used to select the appropriate public key.
// If no "kid" header is present, the default key ID is used.
func NewManagerWithKeys(publicKeys map[string][]byte) (*Manager, error) {
	if len(publicKeys) == 0 {
		return nil, errors.New("at least one public key is required")
	}

	parsedKeys := make(map[string]*rsa.PublicKey, len(publicKeys))
	for keyID, keyPEM := range publicKeys {
		if len(keyPEM) == 0 {
			continue // Skip nil/empty keys (placeholder for future keys)
		}
		publicKey, err := jwt.ParseRSAPublicKeyFromPEM(keyPEM)
		if err != nil {
			return nil, fmt.Errorf("failed to parse public key %s: %w", keyID, err)
		}
		parsedKeys[keyID] = publicKey
	}

	if len(parsedKeys) == 0 {
		return nil, errors.New("no valid public keys found")
	}

	return &Manager{
		publicKeys: parsedKeys,
	}, nil
}

// NewManagerWithProductionKeys creates a license manager using the embedded production keys.
// Returns an error if no production keys are configured.
func NewManagerWithProductionKeys() (*Manager, error) {
	if !HasProductionKeys() {
		return nil, errors.New("no production public keys configured")
	}
	return NewManagerWithKeys(ProductionPublicKeys)
}

// getPublicKeyForToken returns the appropriate public key for verifying the token.
// It checks the "kid" (key ID) header to support key rotation.
func (m *Manager) getPublicKeyForToken(token *jwt.Token) (*rsa.PublicKey, error) {
	// Single-key mode (backward compatibility)
	if m.publicKey != nil {
		return m.publicKey, nil
	}

	// Multi-key mode - look up by kid header
	if len(m.publicKeys) == 0 {
		return nil, errors.New("no public keys configured")
	}

	// Get key ID from header, default to "v1" if not present
	keyID := DefaultKeyID
	if kid, ok := token.Header["kid"].(string); ok && kid != "" {
		keyID = kid
	}

	publicKey, exists := m.publicKeys[keyID]
	if !exists {
		return nil, fmt.Errorf("unknown key ID: %s", keyID)
	}

	return publicKey, nil
}

// LoadLicense loads and validates a license key.
// The license key is a JWT signed with the corresponding RSA private key.
// This method is safe to call concurrently - license updates are atomic.
//
// If the manager was created with multiple keys (NewManagerWithKeys), the "kid"
// header in the JWT is used to select the appropriate public key. If no "kid"
// header is present, the default key ID ("v1") is used.
func (m *Manager) LoadLicense(licenseKey string) error {
	// Parse and validate the JWT
	token, err := jwt.ParseWithClaims(licenseKey, &licenseClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Verify signing method
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.getPublicKeyForToken(token)
	})

	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalidKey, err)
	}

	claims, ok := token.Claims.(*licenseClaims)
	if !ok || !token.Valid {
		return ErrInvalidKey
	}

	// Convert claims to License
	license := &License{
		CustomerID:    claims.CustomerID,
		CustomerName:  claims.CustomerName,
		Features:      claims.Features,
		MaxNodes:      claims.MaxNodes,
		MaxCapacityTB: claims.MaxCapacityTB,
		LicenseID:     claims.LicenseID,
		Tier:          claims.Tier,
	}

	if claims.IssuedAt != nil {
		license.IssuedAt = claims.IssuedAt.Time
	}
	if claims.ExpiresAt != nil {
		license.ExpiresAt = claims.ExpiresAt.Time
	}

	// Validate the license
	if err := license.Validate(); err != nil {
		return err
	}

	// Atomic store - safe for concurrent readers
	m.license.Store(license)
	return nil
}

// LoadLicenseFromFile loads a license from a file path.
// The file should contain only the JWT license key.
func (m *Manager) LoadLicenseFromFile(path string) error {
	m.mu.Lock()
	m.licenseFile = path
	m.mu.Unlock()

	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read license file: %w", err)
	}

	return m.LoadLicense(string(data))
}

// ReloadLicense reloads the license from the previously configured file.
// Returns an error if no license file was configured or if reload fails.
// On failure, the existing license remains active.
func (m *Manager) ReloadLicense() error {
	m.mu.Lock()
	path := m.licenseFile
	m.mu.Unlock()

	if path == "" {
		return errors.New("no license file configured")
	}

	return m.LoadLicenseFromFile(path)
}

// WatchLicenseFile starts watching the license file for changes and reloads automatically.
// Returns a stop function that should be called to stop watching.
// The stop function is idempotent and safe to call multiple times.
// If no license file is configured, this is a no-op.
func (m *Manager) WatchLicenseFile(checkInterval time.Duration, onReload func(error)) (stop func()) {
	m.mu.Lock()
	path := m.licenseFile
	m.mu.Unlock()

	if path == "" {
		return func() {}
	}

	stopCh := make(chan struct{})
	var stopOnce sync.Once
	var lastModTime time.Time

	// Get initial mod time
	if info, err := os.Stat(path); err == nil {
		lastModTime = info.ModTime()
	}

	go func() {
		ticker := time.NewTicker(checkInterval)
		defer ticker.Stop()

		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				info, err := os.Stat(path)
				if err != nil {
					continue
				}

				if info.ModTime().After(lastModTime) {
					lastModTime = info.ModTime()
					err := m.ReloadLicense()
					if onReload != nil {
						onReload(err)
					}
				}
			}
		}
	}()

	return func() {
		stopOnce.Do(func() {
			close(stopCh)
		})
	}
}

// GetLicense returns the currently loaded license, or nil if none is loaded.
// This method is lock-free and safe for concurrent access.
func (m *Manager) GetLicense() *License {
	return m.license.Load()
}

// IsLicensed returns true if a valid license is loaded.
// This method is lock-free and safe for concurrent access.
func (m *Manager) IsLicensed() bool {
	license := m.license.Load()
	return license != nil && !license.IsExpired()
}

// CheckFeature returns nil if the feature is enabled, or an error if not.
// This method is lock-free and safe for concurrent access.
func (m *Manager) CheckFeature(feature Feature) error {
	license := m.license.Load()
	if license == nil {
		return ErrNoLicense
	}
	if license.IsExpired() {
		return ErrLicenseExpired
	}
	if !license.HasFeature(feature) {
		return fmt.Errorf("%w: %s", ErrFeatureDisabled, feature)
	}
	return nil
}

// RequireFeature panics if the feature is not enabled.
// Use this for features that should never be called without proper license checks.
func (m *Manager) RequireFeature(feature Feature) {
	if err := m.CheckFeature(feature); err != nil {
		panic(fmt.Sprintf("license check failed: %v", err))
	}
}

// CheckNodeLimit returns nil if adding a node is within limits.
// This method is lock-free and safe for concurrent access.
func (m *Manager) CheckNodeLimit(currentNodes int) error {
	license := m.license.Load()
	if license == nil {
		return ErrNoLicense
	}
	if license.MaxNodes > 0 && currentNodes >= license.MaxNodes {
		return fmt.Errorf("%w: limit is %d nodes", ErrNodeLimitExceeded, license.MaxNodes)
	}
	return nil
}

// CheckCapacityLimit returns nil if the capacity is within limits.
// This method is lock-free and safe for concurrent access.
func (m *Manager) CheckCapacityLimit(currentCapacityTB int) error {
	license := m.license.Load()
	if license == nil {
		return ErrNoLicense
	}
	if license.MaxCapacityTB > 0 && currentCapacityTB >= license.MaxCapacityTB {
		return fmt.Errorf("%w: limit is %d TB", ErrCapacityLimitExceeded, license.MaxCapacityTB)
	}
	return nil
}

// Info returns license information as a JSON-serializable map.
// Useful for API endpoints that display license status.
// This method is lock-free and safe for concurrent access.
func (m *Manager) Info() map[string]interface{} {
	license := m.license.Load()
	if license == nil {
		return map[string]interface{}{
			"licensed": false,
			"edition":  "community",
			"features": []string{},
		}
	}

	features := make([]string, len(license.Features))
	for i, f := range license.Features {
		features[i] = string(f)
	}

	return map[string]interface{}{
		"licensed":        true,
		"edition":         "enterprise",
		"tier":            license.Tier,
		"customer_id":     license.CustomerID,
		"customer_name":   license.CustomerName,
		"license_id":      license.LicenseID,
		"features":        features,
		"max_nodes":       license.MaxNodes,
		"max_capacity_tb": license.MaxCapacityTB,
		"issued_at":       license.IssuedAt,
		"expires_at":      license.ExpiresAt,
		"days_remaining":  license.DaysUntilExpiry(),
		"expired":         license.IsExpired(),
	}
}

// MarshalJSON implements json.Marshaler for License.
func (l *License) MarshalJSON() ([]byte, error) {
	type Alias License
	return json.Marshal(&struct {
		*Alias
		DaysRemaining int  `json:"days_remaining"`
		Expired       bool `json:"expired"`
	}{
		Alias:         (*Alias)(l),
		DaysRemaining: l.DaysUntilExpiry(),
		Expired:       l.IsExpired(),
	})
}
