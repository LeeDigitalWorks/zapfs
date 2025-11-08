//go:build enterprise

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

// Feature represents an enterprise feature that can be enabled by license.
type Feature string

const (
	// FeatureAuditLog enables audit logging and compliance features
	FeatureAuditLog Feature = "audit_log"

	// FeatureLDAP enables LDAP/Active Directory integration
	FeatureLDAP Feature = "ldap"

	// FeatureOIDC enables OpenID Connect SSO integration
	FeatureOIDC Feature = "oidc"

	// FeatureKMS enables external KMS integration (AWS KMS, Vault, etc.)
	FeatureKMS Feature = "kms"

	// FeatureMultiRegion enables cross-region replication
	FeatureMultiRegion Feature = "multi_region"

	// FeatureObjectLock enables S3 Object Lock (WORM) compliance
	FeatureObjectLock Feature = "object_lock"

	// FeatureLifecycle enables advanced lifecycle policies
	FeatureLifecycle Feature = "lifecycle"

	// FeatureMultiTenancy enables multi-tenant isolation and quotas
	FeatureMultiTenancy Feature = "multi_tenancy"

	// FeatureAdvancedMetrics enables advanced observability features
	FeatureAdvancedMetrics Feature = "advanced_metrics"
)

// AllFeatures returns all available enterprise features.
func AllFeatures() []Feature {
	return []Feature{
		FeatureAuditLog,
		FeatureLDAP,
		FeatureOIDC,
		FeatureKMS,
		FeatureMultiRegion,
		FeatureObjectLock,
		FeatureLifecycle,
		FeatureMultiTenancy,
		FeatureAdvancedMetrics,
	}
}

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
	publicKey *rsa.PublicKey

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
func NewManager(publicKeyPEM []byte) (*Manager, error) {
	publicKey, err := jwt.ParseRSAPublicKeyFromPEM(publicKeyPEM)
	if err != nil {
		return nil, fmt.Errorf("failed to parse public key: %w", err)
	}

	return &Manager{
		publicKey: publicKey,
	}, nil
}

// LoadLicense loads and validates a license key.
// The license key is a JWT signed with the corresponding RSA private key.
// This method is safe to call concurrently - license updates are atomic.
func (m *Manager) LoadLicense(licenseKey string) error {
	// Parse and validate the JWT
	token, err := jwt.ParseWithClaims(licenseKey, &licenseClaims{}, func(token *jwt.Token) (interface{}, error) {
		// Verify signing method
		if _, ok := token.Method.(*jwt.SigningMethodRSA); !ok {
			return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
		}
		return m.publicKey, nil
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
