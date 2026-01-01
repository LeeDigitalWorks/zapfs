// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package license provides enterprise license types.
package license

import "github.com/golang-jwt/jwt/v5"

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
