//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

// IsUsageReportingEnabled always returns false in community edition.
func IsUsageReportingEnabled() bool {
	return false
}
