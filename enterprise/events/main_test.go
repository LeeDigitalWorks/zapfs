//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package events

import (
	"os"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	"github.com/LeeDigitalWorks/zapfs/enterprise/license/testdata"
)

func TestMain(m *testing.M) {
	// Set up test license with all features enabled for testing
	setupTestLicense()
	os.Exit(m.Run())
}

func setupTestLicense() {
	// Reset global state to allow re-initialization
	license.ResetForTesting()

	// Create generator with test keys
	generator, err := license.NewGenerator(testdata.TestPrivateKeyPEM)
	if err != nil {
		panic("failed to create license generator: " + err.Error())
	}

	// Generate a test license with all features
	licenseKey, err := generator.Generate(license.LicenseRequest{
		CustomerID:   "test_events",
		CustomerName: "Event Tests",
		Features:     license.AllFeatures(),
		Tier:         "enterprise",
		ValidDays:    365,
	})
	if err != nil {
		panic("failed to generate test license: " + err.Error())
	}

	// Initialize the license system with test keys and license
	err = license.Initialize(license.Config{
		PublicKey:  testdata.TestPublicKeyPEM,
		LicenseKey: licenseKey,
	})
	if err != nil {
		panic("failed to initialize license: " + err.Error())
	}
}
