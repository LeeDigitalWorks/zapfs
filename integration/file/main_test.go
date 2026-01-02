//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"go.uber.org/goleak"
)

// Test configuration - use testutil.Addrs directly
var (
	fileServer1Addr = testutil.Addrs.FileServer1
	fileServer2Addr = testutil.Addrs.FileServer2
)

// TestMain sets up and tears down the test suite
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newFileClient creates a file client for testing
func newFileClient(t *testing.T, addr string) *testutil.FileClient {
	return testutil.NewFileClient(t, addr)
}
