// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"os"
	"time"

	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/debug"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"

	"github.com/rs/zerolog/log"
	"github.com/spf13/cobra"
)

var rootCmd = &cobra.Command{
	Use:   "zapfs",
	Short: "ZapFS - A distributed file system",
	Long: `ZapFS is a distributed file system with S3-compatible API.
It consists of volume servers for data storage, metadata servers for file metadata,
and manager servers for cluster coordination.`,
	PersistentPreRun: initializeLicense,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

func init() {
	rootCmd.PersistentFlags().StringVar(&utils.ConfigurationFileDirectory, "config_dir", ".", "Directory for configuration files")

	// Enterprise license flags
	rootCmd.PersistentFlags().String("license_key", "", "Enterprise license key (or set ZAPFS_LICENSE_KEY)")
	rootCmd.PersistentFlags().String("license_file", "", "Path to file containing enterprise license key")
}

// initializeLicense sets up enterprise license (no-op in community edition)
func initializeLicense(cmd *cobra.Command, args []string) {
	licenseKey, _ := cmd.Flags().GetString("license_key")
	licenseFile, _ := cmd.Flags().GetString("license_file")

	cfg := license.Config{
		LicenseKey:           licenseKey,
		LicenseFile:          licenseFile,
		WarnDaysBeforeExpiry: 14,
	}

	if err := license.Initialize(cfg); err != nil {
		log.Warn().Err(err).Msg("Failed to initialize license")
	}

	// Register license metrics with debug server
	if err := license.RegisterMetrics(debug.Registry()); err != nil {
		log.Debug().Err(err).Msg("Failed to register license metrics")
	}

	// Start periodic metric updates (every minute)
	license.StartMetricsUpdater(1 * time.Minute)
}

func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
