//go:build enterprise

package license

import (
	"os"
	"sync"
	"time"
	"zapfs/pkg/logger"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	globalManager *Manager
	globalMetrics *Metrics
	globalOnce    sync.Once
)

// Config holds license configuration options.
type Config struct {
	// LicenseKey is the JWT license key (can be loaded from env or file)
	LicenseKey string

	// LicenseFile is the path to a file containing the license key
	LicenseFile string

	// PublicKey is the PEM-encoded RSA public key for verification
	PublicKey []byte

	// PublicKeyFile is the path to the public key file
	PublicKeyFile string

	// WarnDaysBeforeExpiry logs a warning when license is about to expire
	WarnDaysBeforeExpiry int
}

// Initialize sets up the global license manager and metrics.
// This should be called early in main() before using any enterprise features.
//
// A public key must be provided via cfg.PublicKey or cfg.PublicKeyFile.
// Without a valid public key, license verification cannot proceed.
func Initialize(cfg Config) error {
	var initErr error

	globalOnce.Do(func() {
		// Determine public key source - required for enterprise features
		publicKey := cfg.PublicKey
		if len(publicKey) == 0 && cfg.PublicKeyFile != "" {
			var err error
			publicKey, err = os.ReadFile(cfg.PublicKeyFile)
			if err != nil {
				initErr = err
				return
			}
		}

		// If no public key provided, we can't verify licenses
		// Still create manager/metrics but license loading will fail
		if len(publicKey) == 0 {
			logger.Debug().Msg("No license public key configured, enterprise features unavailable")
			return
		}

		// Create manager
		manager, err := NewManager(publicKey)
		if err != nil {
			initErr = err
			return
		}
		globalManager = manager

		// Create metrics
		globalMetrics = NewMetrics(manager)

		// Load license key
		licenseKey := cfg.LicenseKey
		if licenseKey == "" && cfg.LicenseFile != "" {
			data, err := os.ReadFile(cfg.LicenseFile)
			if err != nil {
				logger.Warn().Err(err).Str("file", cfg.LicenseFile).Msg("Failed to read license file")
			} else {
				licenseKey = string(data)
			}
		}
		if licenseKey == "" {
			// Try environment variable
			licenseKey = os.Getenv("ZAPFS_LICENSE_KEY")
		}

		if licenseKey != "" {
			if err := manager.LoadLicense(licenseKey); err != nil {
				logger.Warn().Err(err).Msg("Failed to load license key")
			} else {
				license := manager.GetLicense()
				logger.Info().
					Str("customer", license.CustomerName).
					Str("tier", license.Tier).
					Int("days_remaining", license.DaysUntilExpiry()).
					Msg("Enterprise license loaded")

				// Warn if expiring soon
				warnDays := cfg.WarnDaysBeforeExpiry
				if warnDays == 0 {
					warnDays = 14
				}
				if license.DaysUntilExpiry() <= warnDays {
					logger.Warn().
						Int("days_remaining", license.DaysUntilExpiry()).
						Time("expires_at", license.ExpiresAt).
						Msg("License expiring soon! Please renew.")
				}
			}
		} else {
			logger.Info().Msg("No enterprise license found, running in community mode")
		}

		// Update metrics
		globalMetrics.Update()
	})

	return initErr
}

// GetManager returns the global license manager.
// Returns nil if Initialize() hasn't been called.
func GetManager() *Manager {
	return globalManager
}

// GetMetrics returns the global license metrics.
// Returns nil if Initialize() hasn't been called.
func GetMetrics() *Metrics {
	return globalMetrics
}

// RegisterMetrics registers license metrics with a Prometheus registerer.
func RegisterMetrics(reg prometheus.Registerer) error {
	if globalMetrics == nil {
		return nil
	}
	for _, c := range globalMetrics.Collectors() {
		if err := reg.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// StartMetricsUpdater starts a goroutine that periodically updates license metrics.
// This ensures expiry countdown is accurate.
func StartMetricsUpdater(interval time.Duration) {
	if globalMetrics == nil {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for range ticker.C {
			globalMetrics.Update()
		}
	}()
}

// IsEnterprise returns true (this is the enterprise build).
func IsEnterprise() bool {
	return true
}

// Edition returns "enterprise".
func Edition() string {
	return "enterprise"
}
