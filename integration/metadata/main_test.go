//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package metadata

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain sets up and tears down the test suite
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
