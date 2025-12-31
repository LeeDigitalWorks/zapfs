//go:build integration

package manager

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"go.uber.org/goleak"
)

// Test configuration
var (
	managerServer1Addr = testutil.Addrs.ManagerServer1
	managerServer2Addr = testutil.Addrs.ManagerServer2
	managerServer3Addr = testutil.Addrs.ManagerServer3
)

// TestMain sets up and tears down the test suite
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// newManagerClient creates a manager client for testing
func newManagerClient(t *testing.T, addr string) *testutil.ManagerClient {
	return testutil.NewManagerClient(t, addr)
}
