//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"fmt"
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
)

// initializeOIDCBackedIAM returns an error in community edition.
// OIDC integration requires the enterprise edition with FeatureOIDC license.
func initializeOIDCBackedIAM(issuer string) (*iam.Service, http.Handler, error) {
	return nil, nil, fmt.Errorf("OIDC integration requires enterprise edition")
}

// registerOIDCHandlers is a no-op in community edition.
func registerOIDCHandlers(mux *http.ServeMux, handler http.Handler) {
	// No-op in community edition
}

// oidcEnabled returns false in community edition.
func oidcEnabled() bool {
	return false
}
