//go:build !enterprise

// Package license provides license checking stubs for community edition.
// When built without the "enterprise" build tag, all license checks
// return community/unlicensed status.
package license

import (
	"errors"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

// Feature represents an enterprise feature.
type Feature string

// License feature constants (defined but not usable in community edition).
const (
	FeatureAuditLog        Feature = "audit_log"
	FeatureLDAP            Feature = "ldap"
	FeatureOIDC            Feature = "oidc"
	FeatureKMS             Feature = "kms"
	FeatureMultiRegion     Feature = "multi_region"
	FeatureObjectLock      Feature = "object_lock"
	FeatureLifecycle       Feature = "lifecycle"
	FeatureMultiTenancy    Feature = "multi_tenancy"
	FeatureAdvancedMetrics Feature = "advanced_metrics"
)

// Common errors
var (
	ErrNoLicense             = errors.New("enterprise license required")
	ErrLicenseExpired        = errors.New("license has expired")
	ErrInvalidLicense        = errors.New("invalid license")
	ErrFeatureDisabled       = errors.New("feature not enabled by license")
	ErrInvalidKey            = errors.New("invalid license key")
	ErrNodeLimitExceeded     = errors.New("node limit exceeded")
	ErrCapacityLimitExceeded = errors.New("capacity limit exceeded")
	ErrCommunityEdition      = errors.New("enterprise features not available in community edition")
)

// License is a stub for community edition.
type License struct{}

// Manager is a stub license manager for community edition.
type Manager struct{}

// Config holds license configuration (stub for community edition).
type Config struct {
	LicenseKey           string
	LicenseFile          string
	PublicKey            []byte
	PublicKeyFile        string
	WarnDaysBeforeExpiry int
}

// NewManager returns a stub manager for community edition.
func NewManager(_ []byte) (*Manager, error) {
	return &Manager{}, nil
}

// LoadLicense always returns an error in community edition.
func (m *Manager) LoadLicense(_ string) error {
	return ErrCommunityEdition
}

// GetLicense always returns nil in community edition.
func (m *Manager) GetLicense() *License {
	return nil
}

// IsLicensed always returns false in community edition.
func (m *Manager) IsLicensed() bool {
	return false
}

// CheckFeature always returns an error in community edition.
func (m *Manager) CheckFeature(_ Feature) error {
	return ErrCommunityEdition
}

// RequireFeature panics in community edition.
func (m *Manager) RequireFeature(feature Feature) {
	panic("enterprise feature " + string(feature) + " not available in community edition")
}

// CheckNodeLimit always succeeds in community edition (no limits).
func (m *Manager) CheckNodeLimit(_ int) error {
	return nil
}

// CheckCapacityLimit always succeeds in community edition (no limits).
func (m *Manager) CheckCapacityLimit(_ int) error {
	return nil
}

// Info returns community edition info.
func (m *Manager) Info() map[string]interface{} {
	return map[string]interface{}{
		"licensed": false,
		"edition":  "community",
		"features": []string{},
	}
}

// AllFeatures returns all enterprise features.
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

// Initialize is a no-op in community edition.
func Initialize(_ Config) error {
	return nil
}

// GetManager returns nil in community edition.
func GetManager() *Manager {
	return nil
}

// RegisterMetrics is a no-op in community edition.
func RegisterMetrics(_ prometheus.Registerer) error {
	return nil
}

// StartMetricsUpdater is a no-op in community edition.
func StartMetricsUpdater(_ time.Duration) {}

// IsEnterprise returns false (this is the community build).
func IsEnterprise() bool {
	return false
}

// Edition returns "community".
func Edition() string {
	return "community"
}
