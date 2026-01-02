//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package cmd

import (
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/enterprise/ldap"
	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/spf13/viper"
)

// getLDAPString returns config value from CLI flag (ldap_key) or TOML ([ldap] key).
func getLDAPString(key string) string {
	// Try CLI flag first (ldap_key)
	if v := viper.GetString("ldap_" + key); v != "" {
		return v
	}
	// Fall back to TOML nested config (ldap.key)
	return viper.GetString("ldap." + key)
}

// getLDAPBool returns config value from CLI flag (ldap_key) or TOML ([ldap] key).
func getLDAPBool(key string) bool {
	if viper.IsSet("ldap_" + key) {
		return viper.GetBool("ldap_" + key)
	}
	return viper.GetBool("ldap." + key)
}

// getLDAPInt returns config value from CLI flag (ldap_key) or TOML ([ldap] key).
func getLDAPInt(key string) int {
	if viper.IsSet("ldap_" + key) {
		return viper.GetInt("ldap_" + key)
	}
	if viper.IsSet("ldap." + key) {
		return viper.GetInt("ldap." + key)
	}
	return 0
}

// getLDAPDuration returns config value from CLI flag (ldap_key) or TOML ([ldap] key).
func getLDAPDuration(key string) time.Duration {
	if viper.IsSet("ldap_" + key) {
		return viper.GetDuration("ldap_" + key)
	}
	return viper.GetDuration("ldap." + key)
}

// initializeLDAPBackedIAM creates an IAM service backed by LDAP.
// This is only available in the enterprise edition with FeatureLDAP license.
// Configuration can come from CLI flags (--ldap_bind_dn) or TOML config ([ldap] bind_dn).
func initializeLDAPBackedIAM(ldapURL string) (*iam.Service, error) {
	// Check license
	mgr := license.GetManager()
	if mgr == nil {
		return nil, fmt.Errorf("LDAP integration requires enterprise license")
	}
	if err := mgr.CheckFeature(license.FeatureLDAP); err != nil {
		return nil, fmt.Errorf("LDAP integration requires enterprise license with FeatureLDAP: %w", err)
	}

	logger.Info().Str("url", ldapURL).Msg("initializing LDAP-backed IAM")

	// Create in-memory credential store for S3 access keys (LDAP doesn't store AWS-style credentials)
	credStore := iam.NewMemoryStore()

	// Create in-memory policy/group store for IAM policies
	policyStore := iam.NewMemoryPolicyStore()

	// Build group policy map from config (try both flat and nested keys)
	groupPolicyMap := make(map[string][]string)
	if viper.IsSet("ldap_group_policy_map") {
		if err := viper.UnmarshalKey("ldap_group_policy_map", &groupPolicyMap); err != nil {
			logger.Warn().Err(err).Msg("failed to parse ldap_group_policy_map, using empty map")
		}
	} else if viper.IsSet("ldap.group_policy_map") {
		if err := viper.UnmarshalKey("ldap.group_policy_map", &groupPolicyMap); err != nil {
			logger.Warn().Err(err).Msg("failed to parse ldap.group_policy_map, using empty map")
		}
	}

	// Get config values with fallback from CLI flags to TOML
	bindDN := getLDAPString("bind_dn")
	bindPass := getLDAPString("bind_pass")
	baseDN := getLDAPString("base_dn")
	userFilter := getLDAPString("user_filter")
	usernameAttr := getLDAPString("username_attr")
	emailAttr := getLDAPString("email_attr")
	groupAttr := getLDAPString("group_attr")
	requiredGroup := getLDAPString("required_group")
	startTLS := getLDAPBool("start_tls")
	poolSize := getLDAPInt("pool_size")
	timeout := getLDAPDuration("timeout")

	// Create LDAP store wrapping the credential store
	ldapStore, err := ldap.NewStore(ldap.Config{
		ServerURL:      ldapURL,
		BindDN:         bindDN,
		BindPass:       bindPass,
		BaseDN:         baseDN,
		UserFilter:     userFilter,
		UsernameAttr:   usernameAttr,
		EmailAttr:      emailAttr,
		GroupAttr:      groupAttr,
		RequiredGroup:  requiredGroup,
		StartTLS:       startTLS,
		PoolSize:       poolSize,
		Timeout:        timeout,
		GroupPolicyMap: groupPolicyMap,
	}, credStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create LDAP store: %w", err)
	}

	logger.Info().
		Str("base_dn", baseDN).
		Bool("start_tls", startTLS).
		Int("pool_size", poolSize).
		Msg("LDAP connection established")

	// Create IAM service with LDAP store for credentials, memory store for policies
	return iam.NewService(iam.ServiceConfig{
		CredentialStore: ldapStore,
		PolicyStore:     policyStore,
		GroupStore:      policyStore, // MemoryPolicyStore implements both PolicyStore and GroupStore
		CacheMaxItems:   100000,
		CacheTTL:        5 * time.Minute,
	})
}

// ldapEnabled returns true if LDAP is available in this build.
func ldapEnabled() bool {
	return true
}
