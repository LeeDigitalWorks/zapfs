// Copyright 2025 ZapFS Authors. All rights reserved.
// Use of this source code is governed by the Apache License 2.0
// that can be found in the LICENSE file.

// Package license provides a license checking interface for feature gating.
// The default implementation (noopChecker) returns false for all features.
// Enterprise builds inject a real implementation at startup.
package license

import (
	"errors"
	"sync"
)

// ErrNotLicensed is returned when a feature is not licensed.
var ErrNotLicensed = errors.New("feature requires enterprise license")

// Feature represents a licensable feature.
type Feature string

// Enterprise features that require a license.
const (
	FeatureAccessLog       Feature = "access_log"
	FeatureEvents          Feature = "events"
	FeatureLDAP            Feature = "ldap"
	FeatureOIDC            Feature = "oidc"
	FeatureKMS             Feature = "kms"
	FeatureMultiRegion     Feature = "multi_region"
	FeatureObjectLock      Feature = "object_lock"
	FeatureLifecycle       Feature = "lifecycle"
	FeatureAdvancedMetrics Feature = "advanced_metrics"
	FeatureBackup          Feature = "backup"
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
// Access is protected by checkerMu for thread safety during testing.
var (
	checker   Checker = noopChecker{}
	checkerMu sync.RWMutex
)

// SetChecker sets the global license checker.
// This is called by enterprise/license during initialization.
func SetChecker(c Checker) {
	checkerMu.Lock()
	defer checkerMu.Unlock()
	if c != nil {
		checker = c
	}
}

// GetChecker returns the current license checker.
func GetChecker() Checker {
	checkerMu.RLock()
	defer checkerMu.RUnlock()
	return checker
}

// CheckFeature checks if a feature is licensed using the global checker.
func CheckFeature(feature Feature) error {
	checkerMu.RLock()
	c := checker
	checkerMu.RUnlock()
	return c.CheckFeature(feature)
}

// IsLicensed returns true if any valid license is present.
func IsLicensed() bool {
	checkerMu.RLock()
	c := checker
	checkerMu.RUnlock()
	return c.IsLicensed()
}

// Info returns license metadata from the global checker.
func Info() map[string]interface{} {
	checkerMu.RLock()
	c := checker
	checkerMu.RUnlock()
	return c.Info()
}

// Convenience functions for common feature checks.

// CheckAccessLog checks if access logging feature is licensed.
func CheckAccessLog() bool {
	return CheckFeature(FeatureAccessLog) == nil
}

// CheckKMS checks if KMS integration feature is licensed.
func CheckKMS() bool {
	return CheckFeature(FeatureKMS) == nil
}

// CheckLifecycle checks if lifecycle feature is licensed.
func CheckLifecycle() bool {
	return CheckFeature(FeatureLifecycle) == nil
}

// CheckMultiRegion checks if multi-region/replication feature is licensed.
func CheckMultiRegion() bool {
	return CheckFeature(FeatureMultiRegion) == nil
}

// CheckObjectLock checks if object lock feature is licensed.
func CheckObjectLock() bool {
	return CheckFeature(FeatureObjectLock) == nil
}

// CheckEvents checks if event notifications feature is licensed.
func CheckEvents() bool {
	return CheckFeature(FeatureEvents) == nil
}

// CheckLDAP checks if LDAP integration feature is licensed.
func CheckLDAP() bool {
	return CheckFeature(FeatureLDAP) == nil
}

// CheckBackup checks if backup/restore feature is licensed.
func CheckBackup() bool {
	return CheckFeature(FeatureBackup) == nil
}
