//go:build integration

package metadata

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"go.uber.org/goleak"
)

// Test configuration
var (
	metadataServerAddr = testutil.Addrs.MetadataServer
)

// TestMain sets up and tears down the test suite
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newMetadataClient creates a metadata client for testing
func newMetadataClient(t *testing.T) *testutil.MetadataClient {
	return testutil.NewMetadataClient(t, metadataServerAddr)
}
