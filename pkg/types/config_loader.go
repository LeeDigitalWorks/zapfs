package types

import (
	"encoding/json"
	"fmt"
	"os"
)

// PoolsConfig represents the JSON structure for pool configuration
type PoolsConfig struct {
	Pools []*StoragePool `json:"pools"`
}

// ProfilesConfig represents the JSON structure for profile configuration
type ProfilesConfig struct {
	Profiles []*StorageProfile `json:"profiles"`
}

// CombinedConfig represents a combined pools and profiles configuration
type CombinedConfig struct {
	Pools    []*StoragePool    `json:"pools"`
	Profiles []*StorageProfile `json:"profiles"`
}

// LoadPoolsFromFile loads storage pools from a JSON file
func LoadPoolsFromFile(path string) (*PoolSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read pools file: %w", err)
	}

	var cfg PoolsConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse pools JSON: %w", err)
	}

	ps := NewPoolSet()
	for _, pool := range cfg.Pools {
		if err := ps.Add(pool); err != nil {
			return nil, fmt.Errorf("add pool %q: %w", pool.Name, err)
		}
	}

	return ps, nil
}

// LoadProfilesFromFile loads storage profiles from a JSON file
func LoadProfilesFromFile(path string, pools *PoolSet) (*ProfileSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read profiles file: %w", err)
	}

	var cfg ProfilesConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse profiles JSON: %w", err)
	}

	ps := NewProfileSet()
	for _, profile := range cfg.Profiles {
		// Validate pool references if pools provided
		if pools != nil {
			result := ValidateStorageProfile(profile, pools)
			if !result.Valid {
				return nil, fmt.Errorf("invalid profile %q: %v", profile.Name, result.Errors)
			}
		}
		ps.Add(profile)
	}

	return ps, nil
}

// LoadCombinedConfigFromFile loads both pools and profiles from a single JSON file
func LoadCombinedConfigFromFile(path string) (*PoolSet, *ProfileSet, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, nil, fmt.Errorf("read config file: %w", err)
	}

	var cfg CombinedConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, nil, fmt.Errorf("parse config JSON: %w", err)
	}

	// Load pools first
	pools := NewPoolSet()
	for _, pool := range cfg.Pools {
		if err := pools.Add(pool); err != nil {
			return nil, nil, fmt.Errorf("add pool %q: %w", pool.Name, err)
		}
	}

	// Load profiles with validation
	profiles := NewProfileSet()
	for _, profile := range cfg.Profiles {
		result := ValidateStorageProfile(profile, pools)
		if !result.Valid {
			return nil, nil, fmt.Errorf("invalid profile %q: %v", profile.Name, result.Errors)
		}
		profiles.Add(profile)
	}

	return pools, profiles, nil
}

// SavePoolsToFile saves storage pools to a JSON file
func SavePoolsToFile(path string, pools *PoolSet) error {
	cfg := PoolsConfig{
		Pools: pools.List(),
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal pools: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write pools file: %w", err)
	}

	return nil
}

// SaveProfilesToFile saves storage profiles to a JSON file
func SaveProfilesToFile(path string, profiles *ProfileSet) error {
	cfg := ProfilesConfig{
		Profiles: profiles.List(),
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal profiles: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write profiles file: %w", err)
	}

	return nil
}

// SaveCombinedConfigToFile saves both pools and profiles to a single JSON file
func SaveCombinedConfigToFile(path string, pools *PoolSet, profiles *ProfileSet) error {
	cfg := CombinedConfig{
		Pools:    pools.List(),
		Profiles: profiles.List(),
	}

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal config: %w", err)
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return fmt.Errorf("write config file: %w", err)
	}

	return nil
}
