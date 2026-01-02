//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package cmd

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/LeeDigitalWorks/zapfs/enterprise/oidc"
	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/spf13/viper"
)

// getOIDCString returns config value from CLI flag (oidc_key) or TOML ([oidc] key).
func getOIDCString(key string) string {
	// Try CLI flag first (oidc_key)
	if v := viper.GetString("oidc_" + key); v != "" {
		return v
	}
	// Fall back to TOML nested config (oidc.key)
	return viper.GetString("oidc." + key)
}

// getOIDCStringSlice returns config value from CLI flag (oidc_key) or TOML ([oidc] key).
func getOIDCStringSlice(key string) []string {
	if viper.IsSet("oidc_" + key) {
		return viper.GetStringSlice("oidc_" + key)
	}
	return viper.GetStringSlice("oidc." + key)
}

// initializeOIDCBackedIAM creates an IAM service backed by OIDC.
// This is only available in the enterprise edition with FeatureOIDC license.
// Configuration can come from CLI flags (--oidc_issuer) or TOML config ([oidc] issuer).
func initializeOIDCBackedIAM(issuer string) (*iam.Service, *oidc.HTTPHandler, error) {
	// Check license
	mgr := license.GetManager()
	if mgr == nil {
		return nil, nil, fmt.Errorf("OIDC integration requires enterprise license")
	}
	if err := mgr.CheckFeature(license.FeatureOIDC); err != nil {
		return nil, nil, fmt.Errorf("OIDC integration requires enterprise license with FeatureOIDC: %w", err)
	}

	logger.Info().Str("issuer", issuer).Msg("initializing OIDC-backed IAM")

	// Create in-memory credential store for S3 access keys (OIDC doesn't store AWS-style credentials)
	credStore := iam.NewMemoryStore()

	// Create in-memory policy/group store for IAM policies
	policyStore := iam.NewMemoryPolicyStore()

	// Build group policy map from config (try both flat and nested keys)
	groupPolicyMap := make(map[string][]string)
	if viper.IsSet("oidc_group_policy_map") {
		if err := viper.UnmarshalKey("oidc_group_policy_map", &groupPolicyMap); err != nil {
			logger.Warn().Err(err).Msg("failed to parse oidc_group_policy_map, using empty map")
		}
	} else if viper.IsSet("oidc.group_policy_map") {
		if err := viper.UnmarshalKey("oidc.group_policy_map", &groupPolicyMap); err != nil {
			logger.Warn().Err(err).Msg("failed to parse oidc.group_policy_map, using empty map")
		}
	}

	// Get config values with fallback from CLI flags to TOML
	clientID := getOIDCString("client_id")
	clientSecret := getOIDCString("client_secret")
	redirectURL := getOIDCString("redirect_url")
	scopes := getOIDCStringSlice("scopes")
	usernameClaim := getOIDCString("username_claim")
	groupsClaim := getOIDCString("groups_claim")
	requiredGroups := getOIDCStringSlice("required_groups")
	allowedDomains := getOIDCStringSlice("allowed_domains")

	// Set defaults
	if len(scopes) == 0 {
		scopes = []string{"openid", "email", "profile"}
	}
	if usernameClaim == "" {
		usernameClaim = "email"
	}

	// Create OIDC store
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	oidcStore, err := oidc.NewStore(ctx, oidc.Config{
		Issuer:         issuer,
		ClientID:       clientID,
		ClientSecret:   clientSecret,
		RedirectURL:    redirectURL,
		Scopes:         scopes,
		UsernameClaim:  usernameClaim,
		GroupsClaim:    groupsClaim,
		RequiredGroups: requiredGroups,
		AllowedDomains: allowedDomains,
		GroupPolicyMap: groupPolicyMap,
	}, credStore)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create OIDC store: %w", err)
	}

	logger.Info().
		Str("issuer", issuer).
		Str("client_id", clientID).
		Str("redirect_url", redirectURL).
		Strs("scopes", scopes).
		Msg("OIDC provider configured")

	// Create IAM service with OIDC store for credentials, memory store for policies
	iamService, err := iam.NewService(iam.ServiceConfig{
		CredentialStore: oidcStore,
		PolicyStore:     policyStore,
		GroupStore:      policyStore, // MemoryPolicyStore implements both PolicyStore and GroupStore
		CacheMaxItems:   100000,
		CacheTTL:        5 * time.Minute,
	})
	if err != nil {
		return nil, nil, err
	}

	// Create HTTP handler for OIDC endpoints
	oidcHandler := oidc.NewHTTPHandler(oidcStore)

	return iamService, oidcHandler, nil
}

// registerOIDCHandlers registers OIDC HTTP endpoints on the admin server mux.
func registerOIDCHandlers(mux *http.ServeMux, handler http.Handler) {
	if handler != nil {
		mux.Handle("/v1/oidc/", handler)
		logger.Info().Msg("OIDC endpoints registered at /v1/oidc/{login,callback}")
	}
}

// oidcEnabled returns true if OIDC is available in this build.
func oidcEnabled() bool {
	return true
}
