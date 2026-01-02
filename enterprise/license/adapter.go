//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package license

import (
	pkglicense "github.com/LeeDigitalWorks/zapfs/pkg/license"
)

// managerAdapter adapts *Manager to implement pkglicense.Checker.
type managerAdapter struct {
	manager *Manager
}

// CheckFeature implements pkglicense.Checker.
func (a *managerAdapter) CheckFeature(feature pkglicense.Feature) error {
	if a.manager == nil {
		return pkglicense.ErrNotLicensed
	}
	// Feature is a type alias to pkglicense.Feature, so no conversion needed
	return a.manager.CheckFeature(feature)
}

// IsLicensed implements pkglicense.Checker.
func (a *managerAdapter) IsLicensed() bool {
	if a.manager == nil {
		return false
	}
	return a.manager.IsLicensed()
}

// Info implements pkglicense.Checker.
func (a *managerAdapter) Info() map[string]interface{} {
	if a.manager == nil {
		return map[string]interface{}{
			"licensed": false,
			"edition":  "enterprise",
			"features": []string{},
		}
	}
	return a.manager.Info()
}

// injectChecker registers the manager as the global license checker.
func injectChecker(manager *Manager) {
	pkglicense.SetChecker(&managerAdapter{manager: manager})
}
