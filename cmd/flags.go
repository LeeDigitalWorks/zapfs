// Package cmd provides CLI command implementations for ZapFS services.
// This file contains reusable helpers for configuration loading with CLI flag precedence.
package cmd

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// FlagLoader provides methods for loading configuration values with CLI flag precedence.
// When a CLI flag is explicitly set, it takes precedence over config file and env vars.
// Otherwise, viper's standard priority applies: env > config file > default.
type FlagLoader struct {
	cmd *cobra.Command
}

// NewFlagLoader creates a FlagLoader for the given cobra command.
func NewFlagLoader(cmd *cobra.Command) *FlagLoader {
	return &FlagLoader{cmd: cmd}
}

// String returns CLI flag value if explicitly set, otherwise viper value.
func (f *FlagLoader) String(flagName string) string {
	if f.cmd.Flags().Changed(flagName) {
		val, _ := f.cmd.Flags().GetString(flagName)
		return val
	}
	return viper.GetString(flagName)
}

// Int returns CLI flag value if explicitly set, otherwise viper value.
func (f *FlagLoader) Int(flagName string) int {
	if f.cmd.Flags().Changed(flagName) {
		val, _ := f.cmd.Flags().GetInt(flagName)
		return val
	}
	return viper.GetInt(flagName)
}

// Int64 returns CLI flag value if explicitly set, otherwise viper value.
func (f *FlagLoader) Int64(flagName string) int64 {
	if f.cmd.Flags().Changed(flagName) {
		val, _ := f.cmd.Flags().GetInt64(flagName)
		return val
	}
	return viper.GetInt64(flagName)
}

// Bool returns CLI flag value if explicitly set, otherwise viper value.
func (f *FlagLoader) Bool(flagName string) bool {
	if f.cmd.Flags().Changed(flagName) {
		val, _ := f.cmd.Flags().GetBool(flagName)
		return val
	}
	return viper.GetBool(flagName)
}

// Duration returns CLI flag value if explicitly set, otherwise viper value.
func (f *FlagLoader) Duration(flagName string) time.Duration {
	if f.cmd.Flags().Changed(flagName) {
		val, _ := f.cmd.Flags().GetDuration(flagName)
		return val
	}
	return viper.GetDuration(flagName)
}

// StringSlice returns CLI flag value if explicitly set, otherwise viper value.
func (f *FlagLoader) StringSlice(flagName string) []string {
	if f.cmd.Flags().Changed(flagName) {
		val, _ := f.cmd.Flags().GetStringSlice(flagName)
		return val
	}
	return viper.GetStringSlice(flagName)
}

// Uint32 returns CLI flag value if explicitly set, otherwise viper value.
func (f *FlagLoader) Uint32(flagName string) uint32 {
	if f.cmd.Flags().Changed(flagName) {
		val, _ := f.cmd.Flags().GetUint32(flagName)
		return val
	}
	return viper.GetUint32(flagName)
}
