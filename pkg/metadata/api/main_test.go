//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		// Ignore the shared time ticker goroutine in filter_ratelimit.go
		// This is an intentional package-level goroutine that updates cached time
		goleak.IgnoreTopFunction("github.com/LeeDigitalWorks/zapfs/pkg/metadata/filter.init.1.func1"),
	)
}
