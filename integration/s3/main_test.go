//go:build integration

package s3

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"go.uber.org/goleak"
)

// Test configuration
var s3Config = testutil.DefaultS3Config()

// TestMain sets up and tears down the test suite
func TestMain(m *testing.M) {
	// Ignore HTTP transport goroutines from keep-alive connections
	goleak.VerifyTestMain(m,
		goleak.IgnoreTopFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	)
}

// newS3Client creates an S3 client for testing
func newS3Client(t *testing.T) *testutil.S3Client {
	return testutil.NewS3Client(t, s3Config)
}

// uniqueBucket generates a unique bucket name
func uniqueBucket(prefix string) string {
	return testutil.UniqueID(prefix)
}

// uniqueKey generates a unique object key
func uniqueKey(prefix string) string {
	return testutil.UniqueID(prefix)
}
