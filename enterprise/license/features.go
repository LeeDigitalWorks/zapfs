// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package license provides enterprise license types.
package license

import (
	"github.com/golang-jwt/jwt/v5"

	pkglicense "github.com/LeeDigitalWorks/zapfs/pkg/license"
)

// Claims represents the JWT claims in a license key.
type Claims struct {
	jwt.RegisteredClaims
	CustomerID   string    `json:"cid"`
	CustomerName string    `json:"cnm"`
	Features     []Feature `json:"ftr"`
	LicenseID    string    `json:"lid"`
	Tier         string    `json:"tier"`
}

// DefaultKeyID is the default key ID used for signing new licenses.
// This should be updated when rotating to a new key.
const DefaultKeyID = "v1"

// Feature is an alias to pkg/license.Feature to ensure consistency.
// All feature constants are defined in pkg/license as the single source of truth.
type Feature = pkglicense.Feature

// Re-export feature constants from pkg/license for convenience.
// This ensures enterprise/license and pkg/license stay in sync.
const (
	FeatureAuditLog        = pkglicense.FeatureAuditLog
	FeatureEvents          = pkglicense.FeatureEvents
	FeatureLDAP            = pkglicense.FeatureLDAP
	FeatureOIDC            = pkglicense.FeatureOIDC
	FeatureKMS             = pkglicense.FeatureKMS
	FeatureMultiRegion     = pkglicense.FeatureMultiRegion
	FeatureObjectLock      = pkglicense.FeatureObjectLock
	FeatureLifecycle       = pkglicense.FeatureLifecycle
	FeatureMultiTenancy    = pkglicense.FeatureMultiTenancy
	FeatureAdvancedMetrics = pkglicense.FeatureAdvancedMetrics
)

// AllFeatures returns all available enterprise features.
func AllFeatures() []Feature {
	return []Feature{
		FeatureAuditLog,
		FeatureEvents,
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
