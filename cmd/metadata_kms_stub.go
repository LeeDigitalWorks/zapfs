//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/encryption"
)

// initializeExternalKMS returns nil in community builds.
// External KMS requires the enterprise edition.
func initializeExternalKMS(ctx context.Context) (encryption.KMSProvider, error) {
	// Return nil - external KMS is not available in community edition
	// This is not an error; KMS is an optional feature
	return nil, nil
}

// externalKMSEnabled returns false in community builds.
func externalKMSEnabled() bool {
	return false
}
