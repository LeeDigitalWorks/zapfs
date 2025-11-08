package types

import (
	"fmt"
	"math"
	"strings"

	"github.com/google/uuid"
)

// ConfigValidationError represents a configuration validation error
type ConfigValidationError struct {
	Field   string
	Message string
}

func (e ConfigValidationError) Error() string {
	return fmt.Sprintf("%s: %s", e.Field, e.Message)
}

// ConfigValidationResult contains the results of configuration validation
type ConfigValidationResult struct {
	Valid    bool
	Errors   []ConfigValidationError
	Warnings []string
}

// AddError adds an error to the result
func (r *ConfigValidationResult) AddError(field, message string) {
	r.Valid = false
	r.Errors = append(r.Errors, ConfigValidationError{Field: field, Message: message})
}

// AddWarning adds a warning to the result
func (r *ConfigValidationResult) AddWarning(message string) {
	r.Warnings = append(r.Warnings, message)
}

// ValidatePool validates a storage pool configuration
func ValidatePool(pool *StoragePool) *ConfigValidationResult {
	result := &ConfigValidationResult{Valid: true}

	if pool.ID == uuid.Nil {
		result.AddError("id", "pool ID cannot be nil")
	}

	if strings.TrimSpace(pool.Name) == "" {
		result.AddError("name", "pool name cannot be empty")
	}

	if pool.Weight < 0 {
		result.AddError("weight", "weight cannot be negative")
	} else if pool.Weight == 0 {
		result.AddWarning(fmt.Sprintf("pool %q has zero weight and will not receive any data", pool.Name))
	}

	switch pool.BackendType {
	case StorageTypeLocal:
		// Local backend validation
		if len(pool.Backends) == 0 {
			result.AddWarning(fmt.Sprintf("pool %q has no backends configured", pool.Name))
		}
	case StorageTypeS3:
		// Cloud backend validation
		if pool.Endpoint == "" && pool.BackendType != StorageTypeS3 {
			result.AddWarning(fmt.Sprintf("pool %q has no endpoint configured", pool.Name))
		}
	case StorageTypeCeph:
		if pool.Endpoint == "" {
			result.AddError("endpoint", "Ceph backend requires endpoint")
		}
	case "":
		result.AddError("backend_type", "backend type cannot be empty")
	}

	return result
}

// ValidatePoolSet validates a collection of pools
func ValidatePoolSet(ps *PoolSet) *ConfigValidationResult {
	result := &ConfigValidationResult{Valid: true}

	pools := ps.List()
	if len(pools) == 0 {
		result.AddError("pools", "at least one pool must be configured")
		return result
	}

	// Check for duplicate names (shouldn't happen with PoolSet, but validate anyway)
	names := make(map[string]bool)
	for _, p := range pools {
		if names[p.Name] {
			result.AddError("pools", fmt.Sprintf("duplicate pool name: %s", p.Name))
		}
		names[p.Name] = true

		// Validate each pool
		poolResult := ValidatePool(p)
		if !poolResult.Valid {
			for _, err := range poolResult.Errors {
				result.AddError(fmt.Sprintf("pool[%s].%s", p.Name, err.Field), err.Message)
			}
		}
		result.Warnings = append(result.Warnings, poolResult.Warnings...)
	}

	// Check total weight
	totalWeight := ps.TotalWritableWeight()
	if totalWeight == 0 {
		result.AddError("pools", "total weight of writable pools is zero")
	}

	return result
}

// ValidateStorageProfile validates a storage profile configuration
func ValidateStorageProfile(profile *StorageProfile, pools *PoolSet) *ConfigValidationResult {
	result := &ConfigValidationResult{Valid: true}

	if strings.TrimSpace(profile.Name) == "" {
		result.AddError("name", "profile name cannot be empty")
	}

	if len(profile.Pools) == 0 {
		result.AddError("pools", "profile must reference at least one pool")
	}

	// Validate pool references
	for i, target := range profile.Pools {
		pool, exists := pools.Get(target.PoolID)
		if !exists {
			result.AddError(
				fmt.Sprintf("pools[%d]", i),
				fmt.Sprintf("references unknown pool ID: %s", target.PoolID),
			)
			continue
		}

		if target.WeightOverride < 0 {
			result.AddError(
				fmt.Sprintf("pools[%d].weight_override", i),
				"weight override cannot be negative",
			)
		}

		if pool.ReadOnly {
			result.AddWarning(fmt.Sprintf("pool %q is read-only", pool.Name))
		}
	}

	// Validate replication settings
	if profile.Replication > 0 && profile.Replication > len(profile.Pools) {
		result.AddWarning(fmt.Sprintf(
			"replication factor %d exceeds number of pools %d",
			profile.Replication, len(profile.Pools),
		))
	}

	return result
}

// WeightChange represents a change in pool weight
type WeightChange struct {
	PoolID    uuid.UUID
	PoolName  string
	OldWeight float64
	NewWeight float64
}

// ChangePercent returns the percentage change
func (wc WeightChange) ChangePercent() float64 {
	if wc.OldWeight == 0 {
		if wc.NewWeight == 0 {
			return 0
		}
		return 100 // Went from 0 to something = 100% increase
	}
	return ((wc.NewWeight - wc.OldWeight) / wc.OldWeight) * 100
}

// IsHighImpact returns true if the weight change exceeds the threshold
func (wc WeightChange) IsHighImpact(thresholdPercent float64) bool {
	return math.Abs(wc.ChangePercent()) > thresholdPercent
}

// ImpactAnalysis contains the impact of configuration changes
type ImpactAnalysis struct {
	// AffectedProfiles lists profiles that would be affected
	AffectedProfiles []string

	// WeightChanges lists changes to pool weights
	WeightChanges []WeightChange

	// NewPools lists pools that would be added
	NewPools []string

	// RemovedPools lists pools that would be removed
	RemovedPools []string

	// BreakingChanges lists critical issues
	BreakingChanges []string

	// HasHighImpactChanges indicates significant weight redistribution
	HasHighImpactChanges bool
}

// AnalyzePoolSetChange compares old and new pool sets
func AnalyzePoolSetChange(old, new *PoolSet, thresholdPercent float64) *ImpactAnalysis {
	analysis := &ImpactAnalysis{}

	oldPools := make(map[uuid.UUID]*StoragePool)
	for _, p := range old.List() {
		oldPools[p.ID] = p
	}

	newPools := make(map[uuid.UUID]*StoragePool)
	for _, p := range new.List() {
		newPools[p.ID] = p
	}

	// Find new pools
	for id, p := range newPools {
		if _, exists := oldPools[id]; !exists {
			analysis.NewPools = append(analysis.NewPools, p.Name)
		}
	}

	// Find removed pools and weight changes
	for id, oldPool := range oldPools {
		newPool, exists := newPools[id]
		if !exists {
			analysis.RemovedPools = append(analysis.RemovedPools, oldPool.Name)
			continue
		}

		if oldPool.Weight != newPool.Weight {
			change := WeightChange{
				PoolID:    id,
				PoolName:  oldPool.Name,
				OldWeight: oldPool.Weight,
				NewWeight: newPool.Weight,
			}
			analysis.WeightChanges = append(analysis.WeightChanges, change)

			if change.IsHighImpact(thresholdPercent) {
				analysis.HasHighImpactChanges = true
			}
		}
	}

	return analysis
}

// ValidateConfigChange validates a configuration change before applying
func ValidateConfigChange(old, new *PoolSet, profiles map[string]*StorageProfile) *ConfigValidationResult {
	result := &ConfigValidationResult{Valid: true}

	// Validate new config standalone
	newResult := ValidatePoolSet(new)
	if !newResult.Valid {
		result.Valid = false
		result.Errors = append(result.Errors, newResult.Errors...)
	}
	result.Warnings = append(result.Warnings, newResult.Warnings...)

	// Check that no profiles reference removed pools
	removedPools := make(map[uuid.UUID]bool)
	oldPools := make(map[uuid.UUID]*StoragePool)
	for _, p := range old.List() {
		oldPools[p.ID] = p
	}
	for _, p := range new.List() {
		delete(oldPools, p.ID)
	}
	for id := range oldPools {
		removedPools[id] = true
	}

	for name, profile := range profiles {
		for _, target := range profile.Pools {
			if removedPools[target.PoolID] {
				result.AddError(
					"pools",
					fmt.Sprintf("cannot remove pool %s: referenced by profile %q", target.PoolID, name),
				)
			}
		}
	}

	return result
}
