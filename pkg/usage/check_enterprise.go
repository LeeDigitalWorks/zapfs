//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package usage

import "github.com/LeeDigitalWorks/zapfs/enterprise/license"

// IsUsageReportingEnabled checks if usage reporting is enabled via license.
// Returns true if the license includes FeatureAdvancedMetrics or FeatureMultiTenancy.
func IsUsageReportingEnabled() bool {
	mgr := license.GetManager()
	if mgr == nil {
		return false
	}

	// Check for either AdvancedMetrics or MultiTenancy feature
	return mgr.CheckFeature(license.FeatureAdvancedMetrics) == nil ||
		mgr.CheckFeature(license.FeatureMultiTenancy) == nil
}
