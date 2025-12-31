//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package license

import "sync"

// ResetForTesting resets the global state for testing.
// This allows Initialize() to be called again in tests.
// WARNING: This is not thread-safe and should only be used in test setup.
func ResetForTesting() {
	globalManager = nil
	globalMetrics = nil
	globalOnce = sync.Once{}
}
