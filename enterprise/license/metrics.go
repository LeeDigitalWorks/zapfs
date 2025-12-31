//go:build enterprise

// Copyright 2025 ZapInvest, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package license

import (
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics provides Prometheus metrics for license monitoring.
type Metrics struct {
	// licenseInfo is a gauge that exports license metadata as labels
	licenseInfo *prometheus.GaugeVec

	// licenseExpirySeconds is a gauge showing seconds until license expiry
	// Negative values indicate an expired license
	licenseExpirySeconds prometheus.Gauge

	// licenseValid is 1 if license is valid, 0 otherwise
	licenseValid prometheus.Gauge

	// featureEnabled exports each feature's enabled state (1=enabled, 0=disabled)
	featureEnabled *prometheus.GaugeVec

	// nodeLimitTotal is the maximum number of nodes allowed
	nodeLimitTotal prometheus.Gauge

	// capacityLimitTB is the maximum capacity in TB allowed
	capacityLimitTB prometheus.Gauge

	manager *Manager
}

// NewMetrics creates a new Metrics instance bound to the given license manager.
func NewMetrics(m *Manager) *Metrics {
	metrics := &Metrics{
		manager: m,

		licenseInfo: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "zapfs",
				Subsystem: "license",
				Name:      "info",
				Help:      "License information with labels for customer, tier, and license ID. Value is always 1.",
			},
			[]string{"customer_id", "customer_name", "tier", "license_id", "edition"},
		),

		licenseExpirySeconds: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "zapfs",
				Subsystem: "license",
				Name:      "expiry_seconds",
				Help:      "Seconds until license expires. Negative if expired.",
			},
		),

		licenseValid: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "zapfs",
				Subsystem: "license",
				Name:      "valid",
				Help:      "1 if license is valid and not expired, 0 otherwise.",
			},
		),

		featureEnabled: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace: "zapfs",
				Subsystem: "license",
				Name:      "feature_enabled",
				Help:      "1 if the feature is enabled by license, 0 otherwise.",
			},
			[]string{"feature"},
		),

		nodeLimitTotal: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "zapfs",
				Subsystem: "license",
				Name:      "node_limit_total",
				Help:      "Maximum number of nodes allowed by license. 0 means unlimited.",
			},
		),

		capacityLimitTB: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace: "zapfs",
				Subsystem: "license",
				Name:      "capacity_limit_tb",
				Help:      "Maximum storage capacity in TB allowed by license. 0 means unlimited.",
			},
		),
	}

	return metrics
}

// Collectors returns all Prometheus collectors for registration.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.licenseInfo,
		m.licenseExpirySeconds,
		m.licenseValid,
		m.featureEnabled,
		m.nodeLimitTotal,
		m.capacityLimitTB,
	}
}

// Update refreshes all metrics based on current license state.
// Call this periodically or after license changes.
func (m *Metrics) Update() {
	license := m.manager.GetLicense()

	// Reset info gauge (we'll set new labels)
	m.licenseInfo.Reset()

	if license == nil {
		// No license loaded - community edition
		m.licenseInfo.WithLabelValues("", "", "", "", "community").Set(1)
		m.licenseExpirySeconds.Set(0)
		m.licenseValid.Set(0)
		m.nodeLimitTotal.Set(0)
		m.capacityLimitTB.Set(0)

		// All features disabled
		for _, f := range AllFeatures() {
			m.featureEnabled.WithLabelValues(string(f)).Set(0)
		}
		return
	}

	// License loaded
	m.licenseInfo.WithLabelValues(
		license.CustomerID,
		license.CustomerName,
		license.Tier,
		license.LicenseID,
		"enterprise",
	).Set(1)

	// Calculate expiry in seconds (uses time.Until internally)
	expirySeconds := float64(license.DaysUntilExpiry()) * 86400 // Convert days to seconds
	m.licenseExpirySeconds.Set(expirySeconds)

	// Valid if not expired
	if license.IsExpired() {
		m.licenseValid.Set(0)
	} else {
		m.licenseValid.Set(1)
	}

	// Limits
	m.nodeLimitTotal.Set(float64(license.MaxNodes))
	m.capacityLimitTB.Set(float64(license.MaxCapacityTB))

	// Feature states
	for _, f := range AllFeatures() {
		if license.HasFeature(f) {
			m.featureEnabled.WithLabelValues(string(f)).Set(1)
		} else {
			m.featureEnabled.WithLabelValues(string(f)).Set(0)
		}
	}
}
