package usage

import (
	"time"
)

// Config holds usage reporting configuration.
type Config struct {
	// Enabled controls whether usage collection is active.
	// In enterprise builds, this defaults to true if licensed.
	// In community builds, this is always false.
	Enabled bool `mapstructure:"enabled"`

	// RetentionDays is how long raw usage events are kept before purging.
	// Daily aggregates are kept indefinitely.
	// Default: 30 days.
	RetentionDays int `mapstructure:"retention_days"`

	// CompactOnStartup forces retention cleanup to run immediately on service start.
	// Useful for reclaiming space after lowering RetentionDays.
	// Default: false.
	CompactOnStartup bool `mapstructure:"compact_on_startup"`

	// FlushSize is the number of events to buffer before flushing to DB.
	// Default: 1000.
	FlushSize int `mapstructure:"flush_size"`

	// FlushInterval is how often to flush buffered events regardless of count.
	// Default: 10 seconds.
	FlushInterval time.Duration `mapstructure:"flush_interval"`

	// AggregationTime is the UTC hour to run daily aggregation (0-23).
	// Default: 0 (midnight UTC).
	AggregationTime int `mapstructure:"aggregation_time"`

	// ReportExpiry is how long completed reports are kept before cleanup.
	// Default: 24 hours.
	ReportExpiry time.Duration `mapstructure:"report_expiry"`
}

// DefaultConfig returns a Config with default values.
func DefaultConfig() Config {
	return Config{
		Enabled:          false, // Overridden by enterprise license check
		RetentionDays:    30,
		CompactOnStartup: false,
		FlushSize:        1000,
		FlushInterval:    10 * time.Second,
		AggregationTime:  0,
		ReportExpiry:     24 * time.Hour,
	}
}

// Validate checks the config for invalid values and applies defaults.
func (c *Config) Validate() {
	if c.RetentionDays <= 0 {
		c.RetentionDays = 30
	}
	if c.FlushSize <= 0 {
		c.FlushSize = 1000
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 10 * time.Second
	}
	if c.AggregationTime < 0 || c.AggregationTime > 23 {
		c.AggregationTime = 0
	}
	if c.ReportExpiry <= 0 {
		c.ReportExpiry = 24 * time.Hour
	}
}

// RetentionDuration returns RetentionDays as a time.Duration.
func (c *Config) RetentionDuration() time.Duration {
	return time.Duration(c.RetentionDays) * 24 * time.Hour
}
