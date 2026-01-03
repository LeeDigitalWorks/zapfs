//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package cmd

import (
	"context"
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/enterprise/kms"
	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/encryption"
	"github.com/spf13/viper"
)

// getKMSString returns config value from CLI flag (kms_key) or TOML ([kms] key).
func getKMSString(key string) string {
	// Try CLI flag first (kms_key)
	if v := viper.GetString("kms_" + key); v != "" {
		return v
	}
	// Fall back to TOML nested config (kms.key)
	return viper.GetString("kms." + key)
}

// getKMSBool returns config value from CLI flag (kms_key) or TOML ([kms] key).
func getKMSBool(key string) bool {
	if viper.IsSet("kms_" + key) {
		return viper.GetBool("kms_" + key)
	}
	return viper.GetBool("kms." + key)
}

// initializeExternalKMS creates an external KMS provider from configuration.
// This is only available in the enterprise edition with FeatureKMS license.
// Configuration can come from CLI flags (--kms_provider) or TOML config ([kms] provider).
func initializeExternalKMS(ctx context.Context) (encryption.KMSProvider, error) {
	provider := getKMSString("provider")
	if provider == "" {
		return nil, nil // External KMS not configured
	}

	// Check license
	mgr := license.GetManager()
	if mgr == nil {
		return nil, fmt.Errorf("external KMS integration requires enterprise license")
	}
	if err := mgr.CheckFeature(license.FeatureKMS); err != nil {
		return nil, fmt.Errorf("external KMS integration requires enterprise license with FeatureKMS: %w", err)
	}

	defaultKeyID := getKMSString("default_key_id")
	if defaultKeyID == "" {
		return nil, fmt.Errorf("kms.default_key_id is required when using external KMS")
	}

	logger.Info().
		Str("provider", provider).
		Str("default_key_id", defaultKeyID).
		Msg("initializing external KMS")

	var cfg kms.ExternalKMSConfig
	cfg.Provider = provider
	cfg.DefaultKeyID = defaultKeyID

	switch provider {
	case "aws":
		cfg.AWS = &kms.AWSConfig{
			Region:          getKMSString("aws.region"),
			AccessKeyID:     getKMSString("aws.access_key_id"),
			SecretAccessKey: getKMSString("aws.secret_access_key"),
			Endpoint:        getKMSString("aws.endpoint"),
			RoleARN:         getKMSString("aws.role_arn"),
		}

		// Default region
		if cfg.AWS.Region == "" {
			cfg.AWS.Region = "us-east-1"
		}

		logger.Info().
			Str("region", cfg.AWS.Region).
			Bool("has_explicit_creds", cfg.AWS.AccessKeyID != "").
			Bool("has_endpoint", cfg.AWS.Endpoint != "").
			Bool("has_role_arn", cfg.AWS.RoleARN != "").
			Msg("configured AWS KMS")

	case "vault":
		cfg.Vault = &kms.VaultConfig{
			Address:     getKMSString("vault.address"),
			Token:       getKMSString("vault.token"),
			MountPath:   getKMSString("vault.mount_path"),
			Namespace:   getKMSString("vault.namespace"),
			TLSCACert:   getKMSString("vault.tls_ca_cert"),
			TLSInsecure: getKMSBool("vault.tls_insecure"),
		}

		if cfg.Vault.Address == "" {
			return nil, fmt.Errorf("kms.vault.address is required for Vault KMS provider")
		}

		logger.Info().
			Str("address", cfg.Vault.Address).
			Str("mount_path", cfg.Vault.MountPath).
			Bool("has_namespace", cfg.Vault.Namespace != "").
			Bool("tls_insecure", cfg.Vault.TLSInsecure).
			Msg("configured Vault KMS")

	default:
		return nil, fmt.Errorf("unsupported KMS provider: %s (supported: aws, vault)", provider)
	}

	adapter, err := kms.NewExternalKMS(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize external KMS: %w", err)
	}

	logger.Info().
		Str("provider", provider).
		Msg("external KMS initialized successfully")

	return adapter, nil
}

// externalKMSEnabled returns true if external KMS is available in this build.
func externalKMSEnabled() bool {
	return true
}
