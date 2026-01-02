// Copyright 2025 ZapFS Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the LICENSE file.

// Package license provides a license checking interface for feature gating.
// The default implementation (noopChecker) returns false for all features.
// Enterprise builds inject a real implementation at startup.
package license

import "errors"

// ErrNotLicensed is returned when a feature is not licensed.
var ErrNotLicensed = errors.New("feature requires enterprise license")

// Feature represents a licensable feature.
type Feature string

// Enterprise features that require a license.
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

// Checker is the interface for license validation.
type Checker interface {
	// CheckFeature returns nil if the feature is licensed, error otherwise.
	CheckFeature(feature Feature) error

	// IsLicensed returns true if any valid license is present.
	IsLicensed() bool

	// Info returns license metadata (customer, expiry, features, etc.)
	Info() map[string]interface{}
}

// noopChecker is the default implementation that denies all features.
type noopChecker struct{}

func (noopChecker) CheckFeature(Feature) error {
	return ErrNotLicensed
}

func (noopChecker) IsLicensed() bool {
	return false
}

func (noopChecker) Info() map[string]interface{} {
	return map[string]interface{}{
		"licensed": false,
		"edition":  "community",
		"features": []string{},
	}
}

// Global checker instance. Default is noopChecker.
var checker Checker = noopChecker{}

// SetChecker sets the global license checker.
// This is called by enterprise/license during initialization.
func SetChecker(c Checker) {
	if c != nil {
		checker = c
	}
}

// GetChecker returns the current license checker.
func GetChecker() Checker {
	return checker
}

// CheckFeature checks if a feature is licensed using the global checker.
func CheckFeature(feature Feature) error {
	return checker.CheckFeature(feature)
}

// IsLicensed returns true if any valid license is present.
func IsLicensed() bool {
	return checker.IsLicensed()
}

// Info returns license metadata from the global checker.
func Info() map[string]interface{} {
	return checker.Info()
}

// Convenience functions for common feature checks.

// CheckAuditLog checks if audit logging feature is licensed.
func CheckAuditLog() bool {
	return checker.CheckFeature(FeatureAuditLog) == nil
}

// CheckKMS checks if KMS integration feature is licensed.
func CheckKMS() bool {
	return checker.CheckFeature(FeatureKMS) == nil
}

// CheckLifecycle checks if lifecycle feature is licensed.
func CheckLifecycle() bool {
	return checker.CheckFeature(FeatureLifecycle) == nil
}

// CheckMultiRegion checks if multi-region/replication feature is licensed.
func CheckMultiRegion() bool {
	return checker.CheckFeature(FeatureMultiRegion) == nil
}

// CheckObjectLock checks if object lock feature is licensed.
func CheckObjectLock() bool {
	return checker.CheckFeature(FeatureObjectLock) == nil
}
