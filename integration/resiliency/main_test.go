//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package resiliency provides integration tests for file server resiliency,
// chunk registry consistency, and reconciliation behavior.
package resiliency

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"go.uber.org/goleak"
)

// Test addresses - use testutil.Addrs directly
var (
	fileServer1Addr = testutil.Addrs.FileServer1
	fileServer2Addr = testutil.Addrs.FileServer2
)

// TestMain sets up and tears down the test suite
func TestMain(m *testing.M) {
	// Ignore HTTP transport goroutines from S3 client keep-alive connections
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

// newFileClient creates a file client for testing
func newFileClient(t *testing.T, addr string) *testutil.FileClient {
	return testutil.NewFileClient(t, addr)
}

// newS3Client creates an S3 client for testing
func newS3Client(t *testing.T) *testutil.S3Client {
	return testutil.NewS3Client(t, testutil.DefaultS3Config())
}
