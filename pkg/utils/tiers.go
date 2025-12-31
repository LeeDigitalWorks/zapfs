package utils

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"

	"github.com/spf13/viper"
)

type RateLimitTier struct {
	Name              string
	MaxReadRPS        int
	MaxWriteRPS       int
	MaxListRPS        int
	MaxReadBandwidth  int64
	MaxWriteBandwidth int64
	MaxObjects        int64
	MaxSizeBytes      int64
}

// Tier name constants for type safety and easier code reviews
const (
	RateLimitTierStandard   = "standard"
	RateLimitTierPremium    = "premium"
	RateLimitTierRestricted = "restricted"
	RateLimitTierBlocked    = "blocked"
)

// Default tier configurations
var (
	DefaultTierStandard = RateLimitTier{
		Name:              RateLimitTierStandard,
		MaxReadRPS:        800,
		MaxWriteRPS:       800,
		MaxListRPS:        100,
		MaxReadBandwidth:  1 << 30, // 1 GB/s
		MaxWriteBandwidth: 1 << 30, // 1 GB/s
		MaxObjects:        0,       // unlimited
		MaxSizeBytes:      0,       // unlimited
	}
)

// GetDefaultTiers returns the built-in tier definitions
func GetDefaultTiers() map[string]RateLimitTier {
	return map[string]RateLimitTier{
		RateLimitTierStandard: DefaultTierStandard,
	}
}

// TierConfig holds tier configuration loaded from tiers.toml
type TierConfig struct {
	DefaultTier     string
	Tiers           map[string]RateLimitTier
	CollectionTiers map[string]string // collection -> tier overrides
}

// LoadTierConfig loads tier configuration from tiers.toml
// Returns config with defaults if file not found
func LoadTierConfig() *TierConfig {
	// Start with defaults
	config := &TierConfig{
		DefaultTier:     RateLimitTierStandard,
		Tiers:           GetDefaultTiers(),
		CollectionTiers: make(map[string]string),
	}

	// Load tiers.toml separately
	if !LoadConfiguration("tiers", false) {
		logger.Info().Msg("tiers.toml not found, using built-in defaults")
		return config
	}

	// Override default tier if specified in config
	if defaultTier := viper.GetString("default_tier"); defaultTier != "" {
		config.DefaultTier = defaultTier
	}

	// Load tier definitions
	type TierConfigRaw struct {
		Name              string `mapstructure:"name"`
		MaxReadRPS        int    `mapstructure:"max_read_rps"`
		MaxWriteRPS       int    `mapstructure:"max_write_rps"`
		MaxListRPS        int    `mapstructure:"max_list_rps"`
		MaxReadBandwidth  int64  `mapstructure:"max_read_bandwidth"`
		MaxWriteBandwidth int64  `mapstructure:"max_write_bandwidth"`
		MaxObjects        int64  `mapstructure:"max_objects"`
		MaxSizeBytes      int64  `mapstructure:"max_size_bytes"`
	}

	var tierConfigs []TierConfigRaw
	if err := viper.UnmarshalKey("tiers", &tierConfigs); err == nil && len(tierConfigs) > 0 {
		// Merge with defaults (config overrides defaults)
		for _, tc := range tierConfigs {
			tier := RateLimitTier(tc)
			config.Tiers[tier.Name] = tier
			logger.Info().
				Str("tier", tier.Name).
				Int("max_read_rps", tier.MaxReadRPS).
				Int("max_write_rps", tier.MaxWriteRPS).
				Msg("Loaded tier definition from config")
		}
	}

	// Load collection-to-tier overrides
	collectionTiers := viper.GetStringMapString("collection_tiers")
	if len(collectionTiers) > 0 {
		config.CollectionTiers = collectionTiers
		logger.Info().
			Int("count", len(collectionTiers)).
			Msg("Loaded collection tier overrides")
	}

	logger.Info().
		Str("default_tier", config.DefaultTier).
		Int("tier_count", len(config.Tiers)).
		Int("override_count", len(config.CollectionTiers)).
		Msg("Tier configuration loaded")

	return config
}
