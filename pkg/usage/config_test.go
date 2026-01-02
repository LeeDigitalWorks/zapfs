// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

import (
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	if cfg.Enabled {
		t.Error("Enabled should be false by default")
	}
	if cfg.RetentionDays != 30 {
		t.Errorf("RetentionDays = %d, want 30", cfg.RetentionDays)
	}
	if cfg.CompactOnStartup {
		t.Error("CompactOnStartup should be false by default")
	}
	if cfg.FlushSize != 1000 {
		t.Errorf("FlushSize = %d, want 1000", cfg.FlushSize)
	}
	if cfg.FlushInterval != 10*time.Second {
		t.Errorf("FlushInterval = %v, want 10s", cfg.FlushInterval)
	}
	if cfg.AggregationTime != 0 {
		t.Errorf("AggregationTime = %d, want 0", cfg.AggregationTime)
	}
	if cfg.ReportExpiry != 24*time.Hour {
		t.Errorf("ReportExpiry = %v, want 24h", cfg.ReportExpiry)
	}
}

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name     string
		config   Config
		expected Config
	}{
		{
			name:   "zero values get defaults",
			config: Config{},
			expected: Config{
				RetentionDays:   30,
				FlushSize:       1000,
				FlushInterval:   10 * time.Second,
				AggregationTime: 0,
				ReportExpiry:    24 * time.Hour,
			},
		},
		{
			name: "negative values get defaults",
			config: Config{
				RetentionDays:   -1,
				FlushSize:       -1,
				FlushInterval:   -1,
				AggregationTime: -1,
				ReportExpiry:    -1,
			},
			expected: Config{
				RetentionDays:   30,
				FlushSize:       1000,
				FlushInterval:   10 * time.Second,
				AggregationTime: 0,
				ReportExpiry:    24 * time.Hour,
			},
		},
		{
			name: "out of range aggregation time",
			config: Config{
				RetentionDays:   30,
				FlushSize:       1000,
				FlushInterval:   10 * time.Second,
				AggregationTime: 25, // > 23
				ReportExpiry:    24 * time.Hour,
			},
			expected: Config{
				RetentionDays:   30,
				FlushSize:       1000,
				FlushInterval:   10 * time.Second,
				AggregationTime: 0, // reset to default
				ReportExpiry:    24 * time.Hour,
			},
		},
		{
			name: "valid custom values preserved",
			config: Config{
				RetentionDays:   7,
				FlushSize:       500,
				FlushInterval:   5 * time.Second,
				AggregationTime: 3,
				ReportExpiry:    12 * time.Hour,
			},
			expected: Config{
				RetentionDays:   7,
				FlushSize:       500,
				FlushInterval:   5 * time.Second,
				AggregationTime: 3,
				ReportExpiry:    12 * time.Hour,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := tt.config
			cfg.Validate()

			if cfg.RetentionDays != tt.expected.RetentionDays {
				t.Errorf("RetentionDays = %d, want %d", cfg.RetentionDays, tt.expected.RetentionDays)
			}
			if cfg.FlushSize != tt.expected.FlushSize {
				t.Errorf("FlushSize = %d, want %d", cfg.FlushSize, tt.expected.FlushSize)
			}
			if cfg.FlushInterval != tt.expected.FlushInterval {
				t.Errorf("FlushInterval = %v, want %v", cfg.FlushInterval, tt.expected.FlushInterval)
			}
			if cfg.AggregationTime != tt.expected.AggregationTime {
				t.Errorf("AggregationTime = %d, want %d", cfg.AggregationTime, tt.expected.AggregationTime)
			}
			if cfg.ReportExpiry != tt.expected.ReportExpiry {
				t.Errorf("ReportExpiry = %v, want %v", cfg.ReportExpiry, tt.expected.ReportExpiry)
			}
		})
	}
}

func TestRetentionDuration(t *testing.T) {
	tests := []struct {
		days     int
		expected time.Duration
	}{
		{1, 24 * time.Hour},
		{7, 7 * 24 * time.Hour},
		{30, 30 * 24 * time.Hour},
	}

	for _, tt := range tests {
		cfg := Config{RetentionDays: tt.days}
		if got := cfg.RetentionDuration(); got != tt.expected {
			t.Errorf("RetentionDuration(%d days) = %v, want %v", tt.days, got, tt.expected)
		}
	}
}
