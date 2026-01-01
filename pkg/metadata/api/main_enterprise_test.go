//go:build enterprise

package api

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/LeeDigitalWorks/zapfs/enterprise/license/testdata"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// Initialize a test license with all features for enterprise tests
	initTestLicense()

	goleak.VerifyTestMain(m,
		// Ignore the shared time ticker goroutine in filter_ratelimit.go
		// This is an intentional package-level goroutine that updates cached time
		goleak.IgnoreTopFunction("github.com/LeeDigitalWorks/zapfs/pkg/metadata/filter.init.0.func1"),
	)
}

// initTestLicense sets up a test license with all enterprise features enabled.
// This allows enterprise handler tests to pass license checks.
func initTestLicense() {
	// Reset so we can call Initialize (which uses sync.Once)
	license.ResetForTesting()

	// Generate a test license with all features
	generator, err := license.NewGenerator(testdata.TestPrivateKeyPEM)
	if err != nil {
		panic("failed to create test license generator: " + err.Error())
	}

	licenseKey, err := generator.Generate(license.LicenseRequest{
		CustomerID:    "test_customer",
		CustomerName:  "ZapFS Test Suite",
		Features:      license.AllFeatures(),
		Tier:          "enterprise",
		ValidDays:     365,
	})
	if err != nil {
		panic("failed to generate test license: " + err.Error())
	}

	// Use the standard Initialize function with test keys
	if err := license.Initialize(license.Config{
		PublicKey:  testdata.TestPublicKeyPEM,
		LicenseKey: licenseKey,
	}); err != nil {
		panic("failed to initialize test license: " + err.Error())
	}
}
