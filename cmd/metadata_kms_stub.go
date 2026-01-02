//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
)

// externalKMSAdapter is a placeholder type for community builds.
// External KMS is only available in enterprise edition.
type externalKMSAdapter struct{}

// initializeExternalKMS returns an error in community builds.
// External KMS requires the enterprise edition.
func initializeExternalKMS(ctx context.Context) (*externalKMSAdapter, error) {
	return nil, fmt.Errorf("external KMS integration requires enterprise edition")
}

// externalKMSEnabled returns false in community builds.
func externalKMSEnabled() bool {
	return false
}
