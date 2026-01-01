//go:build enterprise

package api

import (
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// checkReplicationLicense checks if replication feature is licensed.
func checkReplicationLicense() bool {
	mgr := license.GetManager()
	return mgr != nil && mgr.CheckFeature(license.FeatureMultiRegion) == nil
}

// checkLifecycleLicense checks if lifecycle feature is licensed.
// Lifecycle rules, intelligent tiering, and object restore require this license.
func checkLifecycleLicense() bool {
	mgr := license.GetManager()
	return mgr != nil && mgr.CheckFeature(license.FeatureLifecycle) == nil
}

// checkObjectLockLicense checks if object lock feature is licensed.
// S3 Object Lock (WORM) compliance requires this license.
func checkObjectLockLicense() bool {
	mgr := license.GetManager()
	return mgr != nil && mgr.CheckFeature(license.FeatureObjectLock) == nil
}

// checkAuditLogLicense checks if audit log feature is licensed.
// Bucket logging and audit trails require this license.
func checkAuditLogLicense() bool {
	mgr := license.GetManager()
	return mgr != nil && mgr.CheckFeature(license.FeatureAuditLog) == nil
}

// validateReplicationConfig validates a replication configuration.
func validateReplicationConfig(config *s3types.ReplicationConfiguration) error {
	if len(config.Rules) == 0 {
		return fmt.Errorf("replication configuration must have at least one rule")
	}

	if len(config.Rules) > 1000 {
		return fmt.Errorf("replication configuration cannot have more than 1000 rules")
	}

	for i, rule := range config.Rules {
		if rule.ID == "" {
			return fmt.Errorf("rule %d: ID is required", i)
		}

		if len(rule.ID) > 255 {
			return fmt.Errorf("rule %d: ID cannot exceed 255 characters", i)
		}

		if rule.Status != "Enabled" && rule.Status != "Disabled" {
			return fmt.Errorf("rule %d: status must be Enabled or Disabled", i)
		}

		if rule.Destination.Bucket == "" {
			return fmt.Errorf("rule %d: destination bucket is required", i)
		}
	}

	return nil
}
