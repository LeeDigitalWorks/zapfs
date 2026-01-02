// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package bucket

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
