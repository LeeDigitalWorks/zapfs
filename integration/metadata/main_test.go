//go:build integration

package metadata

import (
	"testing"

	"go.uber.org/goleak"
)

// TestMain sets up and tears down the test suite
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}
