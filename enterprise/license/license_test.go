//go:build enterprise

package license

import (
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"zapfs/enterprise/license/testdata"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLicenseGeneration(t *testing.T) {
	t.Parallel()

	// Generate a test key pair
	privateKeyPEM, publicKeyPEM, err := GenerateKeyPair()
	require.NoError(t, err)

	// Create generator and manager
	generator, err := NewGenerator(privateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(publicKeyPEM)
	require.NoError(t, err)

	// Generate a license
	licenseKey, err := generator.Generate(LicenseRequest{
		CustomerID:    "cust_123",
		CustomerName:  "Acme Corp",
		Features:      []Feature{FeatureAuditLog, FeatureLDAP, FeatureKMS},
		MaxNodes:      10,
		MaxCapacityTB: 100,
		Tier:          "premium",
		ValidDays:     365,
	})
	require.NoError(t, err)
	assert.NotEmpty(t, licenseKey)

	// Load the license
	err = manager.LoadLicense(licenseKey)
	require.NoError(t, err)

	// Verify license details
	license := manager.GetLicense()
	require.NotNil(t, license)

	assert.Equal(t, "cust_123", license.CustomerID)
	assert.Equal(t, "Acme Corp", license.CustomerName)
	assert.Equal(t, "premium", license.Tier)
	assert.Equal(t, 10, license.MaxNodes)
	assert.Equal(t, 100, license.MaxCapacityTB)
	assert.Len(t, license.Features, 3)
	assert.True(t, license.HasFeature(FeatureAuditLog))
	assert.True(t, license.HasFeature(FeatureLDAP))
	assert.True(t, license.HasFeature(FeatureKMS))
	assert.False(t, license.HasFeature(FeatureMultiRegion))
	assert.False(t, license.IsExpired())
	assert.Greater(t, license.DaysUntilExpiry(), 360)
}

func TestLicenseExpiration(t *testing.T) {
	t.Parallel()

	privateKeyPEM, publicKeyPEM, err := GenerateKeyPair()
	require.NoError(t, err)

	generator, err := NewGenerator(privateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(publicKeyPEM)
	require.NoError(t, err)

	// Generate an expired license (valid for 0 days means it expires immediately)
	// We need to test with a license that has already expired
	// Since we can't easily generate an expired JWT, we test the License struct directly
	expiredLicense := &License{
		CustomerID: "cust_expired",
		ExpiresAt:  time.Now().Add(-24 * time.Hour), // Expired yesterday
	}

	assert.True(t, expiredLicense.IsExpired())
	assert.Less(t, expiredLicense.DaysUntilExpiry(), 0)
	assert.Error(t, expiredLicense.Validate())

	// Valid license
	validLicense := &License{
		CustomerID: "cust_valid",
		ExpiresAt:  time.Now().Add(30 * 24 * time.Hour), // 30 days from now
	}

	assert.False(t, validLicense.IsExpired())
	assert.Greater(t, validLicense.DaysUntilExpiry(), 25)
	assert.NoError(t, validLicense.Validate())

	// License without customer ID
	invalidLicense := &License{
		ExpiresAt: time.Now().Add(30 * 24 * time.Hour),
	}
	assert.Error(t, invalidLicense.Validate())

	_ = manager // silence unused
	_ = generator
}

func TestFeatureCheck(t *testing.T) {
	t.Parallel()

	privateKeyPEM, publicKeyPEM, err := GenerateKeyPair()
	require.NoError(t, err)

	generator, err := NewGenerator(privateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(publicKeyPEM)
	require.NoError(t, err)

	// No license loaded
	err = manager.CheckFeature(FeatureAuditLog)
	assert.ErrorIs(t, err, ErrNoLicense)

	// Load license with specific features
	licenseKey, err := generator.Generate(LicenseRequest{
		CustomerID:   "cust_123",
		CustomerName: "Test Corp",
		Features:     []Feature{FeatureAuditLog, FeatureLDAP},
		Tier:         "standard",
		ValidDays:    30,
	})
	require.NoError(t, err)

	err = manager.LoadLicense(licenseKey)
	require.NoError(t, err)

	// Check enabled features
	assert.NoError(t, manager.CheckFeature(FeatureAuditLog))
	assert.NoError(t, manager.CheckFeature(FeatureLDAP))

	// Check disabled features
	assert.ErrorIs(t, manager.CheckFeature(FeatureKMS), ErrFeatureDisabled)
	assert.ErrorIs(t, manager.CheckFeature(FeatureMultiRegion), ErrFeatureDisabled)
}

func TestNodeAndCapacityLimits(t *testing.T) {
	t.Parallel()

	privateKeyPEM, publicKeyPEM, err := GenerateKeyPair()
	require.NoError(t, err)

	generator, err := NewGenerator(privateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(publicKeyPEM)
	require.NoError(t, err)

	// Load license with limits
	licenseKey, err := generator.Generate(LicenseRequest{
		CustomerID:    "cust_123",
		CustomerName:  "Limited Corp",
		Features:      []Feature{FeatureAuditLog},
		MaxNodes:      5,
		MaxCapacityTB: 50,
		Tier:          "standard",
		ValidDays:     30,
	})
	require.NoError(t, err)

	err = manager.LoadLicense(licenseKey)
	require.NoError(t, err)

	// Check node limits
	assert.NoError(t, manager.CheckNodeLimit(0))
	assert.NoError(t, manager.CheckNodeLimit(4))
	assert.ErrorIs(t, manager.CheckNodeLimit(5), ErrNodeLimitExceeded)
	assert.ErrorIs(t, manager.CheckNodeLimit(10), ErrNodeLimitExceeded)

	// Check capacity limits
	assert.NoError(t, manager.CheckCapacityLimit(0))
	assert.NoError(t, manager.CheckCapacityLimit(49))
	assert.ErrorIs(t, manager.CheckCapacityLimit(50), ErrCapacityLimitExceeded)
	assert.ErrorIs(t, manager.CheckCapacityLimit(100), ErrCapacityLimitExceeded)
}

func TestLicenseInfo(t *testing.T) {
	t.Parallel()

	privateKeyPEM, publicKeyPEM, err := GenerateKeyPair()
	require.NoError(t, err)

	manager, err := NewManager(publicKeyPEM)
	require.NoError(t, err)

	// No license loaded
	info := manager.Info()
	assert.False(t, info["licensed"].(bool))
	assert.Equal(t, "community", info["edition"])

	// Load license
	generator, err := NewGenerator(privateKeyPEM)
	require.NoError(t, err)

	licenseKey, err := generator.Generate(LicenseRequest{
		CustomerID:   "cust_123",
		CustomerName: "Info Corp",
		Features:     []Feature{FeatureAuditLog},
		Tier:         "premium",
		ValidDays:    30,
	})
	require.NoError(t, err)

	err = manager.LoadLicense(licenseKey)
	require.NoError(t, err)

	// With license loaded
	info = manager.Info()
	assert.True(t, info["licensed"].(bool))
	assert.Equal(t, "enterprise", info["edition"])
	assert.Equal(t, "premium", info["tier"])
	assert.Equal(t, "Info Corp", info["customer_name"])
	assert.False(t, info["expired"].(bool))
}

func TestInvalidLicenseKey(t *testing.T) {
	t.Parallel()

	_, publicKeyPEM, err := GenerateKeyPair()
	require.NoError(t, err)

	manager, err := NewManager(publicKeyPEM)
	require.NoError(t, err)

	// Invalid JWT
	err = manager.LoadLicense("not-a-valid-jwt")
	assert.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidKey)

	// JWT signed with different key
	differentPrivateKey, _, err := GenerateKeyPair()
	require.NoError(t, err)

	differentGenerator, err := NewGenerator(differentPrivateKey)
	require.NoError(t, err)

	wrongLicense, err := differentGenerator.Generate(LicenseRequest{
		CustomerID:   "cust_123",
		CustomerName: "Wrong Corp",
		Features:     []Feature{FeatureAuditLog},
		ValidDays:    30,
	})
	require.NoError(t, err)

	err = manager.LoadLicense(wrongLicense)
	assert.Error(t, err)
}

func TestAllFeatures(t *testing.T) {
	features := AllFeatures()
	assert.Len(t, features, 9)
	assert.Contains(t, features, FeatureAuditLog)
	assert.Contains(t, features, FeatureLDAP)
	assert.Contains(t, features, FeatureOIDC)
	assert.Contains(t, features, FeatureKMS)
	assert.Contains(t, features, FeatureMultiRegion)
	assert.Contains(t, features, FeatureObjectLock)
	assert.Contains(t, features, FeatureLifecycle)
	assert.Contains(t, features, FeatureMultiTenancy)
	assert.Contains(t, features, FeatureAdvancedMetrics)
}

func TestTestdataKeys(t *testing.T) {
	t.Parallel()

	// Test that the testdata keys work correctly
	generator, err := NewGenerator(testdata.TestPrivateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(testdata.TestPublicKeyPEM)
	require.NoError(t, err)

	// Generate and load a license
	licenseKey, err := generator.Generate(LicenseRequest{
		CustomerID:   "test_customer",
		CustomerName: "Test Company",
		Features:     []Feature{FeatureAuditLog},
		Tier:         "test",
		ValidDays:    30,
	})
	require.NoError(t, err)

	err = manager.LoadLicense(licenseKey)
	require.NoError(t, err)

	license := manager.GetLicense()
	require.NotNil(t, license)
	assert.Equal(t, "test_customer", license.CustomerID)
	assert.Equal(t, "Test Company", license.CustomerName)
}

func TestLoadLicenseFromFile(t *testing.T) {
	t.Parallel()

	generator, err := NewGenerator(testdata.TestPrivateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(testdata.TestPublicKeyPEM)
	require.NoError(t, err)

	// Generate a license
	licenseKey, err := generator.Generate(LicenseRequest{
		CustomerID:   "file_test",
		CustomerName: "File Test Corp",
		Features:     []Feature{FeatureAuditLog, FeatureLDAP},
		Tier:         "standard",
		ValidDays:    365,
	})
	require.NoError(t, err)

	// Write to temp file
	tmpDir := t.TempDir()
	licensePath := filepath.Join(tmpDir, "license.jwt")
	err = os.WriteFile(licensePath, []byte(licenseKey), 0600)
	require.NoError(t, err)

	// Load from file
	err = manager.LoadLicenseFromFile(licensePath)
	require.NoError(t, err)

	license := manager.GetLicense()
	require.NotNil(t, license)
	assert.Equal(t, "file_test", license.CustomerID)
}

func TestReloadLicense(t *testing.T) {
	t.Parallel()

	generator, err := NewGenerator(testdata.TestPrivateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(testdata.TestPublicKeyPEM)
	require.NoError(t, err)

	// Create initial license
	license1, err := generator.Generate(LicenseRequest{
		CustomerID:   "v1",
		CustomerName: "Version 1",
		Features:     []Feature{FeatureAuditLog},
		Tier:         "basic",
		ValidDays:    30,
	})
	require.NoError(t, err)

	// Write to temp file and load
	tmpDir := t.TempDir()
	licensePath := filepath.Join(tmpDir, "license.jwt")
	err = os.WriteFile(licensePath, []byte(license1), 0600)
	require.NoError(t, err)

	err = manager.LoadLicenseFromFile(licensePath)
	require.NoError(t, err)

	// Verify initial license
	current := manager.GetLicense()
	require.NotNil(t, current)
	assert.Equal(t, "v1", current.CustomerID)

	// Create updated license
	license2, err := generator.Generate(LicenseRequest{
		CustomerID:   "v2",
		CustomerName: "Version 2",
		Features:     []Feature{FeatureAuditLog, FeatureLDAP, FeatureKMS},
		Tier:         "premium",
		ValidDays:    365,
	})
	require.NoError(t, err)

	// Overwrite file
	err = os.WriteFile(licensePath, []byte(license2), 0600)
	require.NoError(t, err)

	// Reload
	err = manager.ReloadLicense()
	require.NoError(t, err)

	// Verify updated license
	current = manager.GetLicense()
	require.NotNil(t, current)
	assert.Equal(t, "v2", current.CustomerID)
	assert.Equal(t, "premium", current.Tier)
	assert.True(t, current.HasFeature(FeatureKMS))
}

func TestConcurrentAccess(t *testing.T) {
	t.Parallel()

	generator, err := NewGenerator(testdata.TestPrivateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(testdata.TestPublicKeyPEM)
	require.NoError(t, err)

	// Generate a license
	licenseKey, err := generator.Generate(LicenseRequest{
		CustomerID:   "concurrent_test",
		CustomerName: "Concurrent Corp",
		Features:     []Feature{FeatureAuditLog, FeatureLDAP},
		MaxNodes:     100,
		Tier:         "enterprise",
		ValidDays:    365,
	})
	require.NoError(t, err)

	err = manager.LoadLicense(licenseKey)
	require.NoError(t, err)

	// Hammer it with concurrent reads and writes
	var wg sync.WaitGroup
	var readCount atomic.Int64
	var writeCount atomic.Int64

	// Start readers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 1000; j++ {
				license := manager.GetLicense()
				if license != nil {
					_ = license.CustomerID
					_ = manager.IsLicensed()
					_ = manager.CheckFeature(FeatureAuditLog)
					_ = manager.CheckNodeLimit(50)
					readCount.Add(1)
				}
			}
		}()
	}

	// Start writers (reload)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = manager.LoadLicense(licenseKey)
				writeCount.Add(1)
			}
		}()
	}

	wg.Wait()

	// Should complete without race conditions
	assert.Greater(t, readCount.Load(), int64(5000))
	assert.Greater(t, writeCount.Load(), int64(100))
}

func TestWatchLicenseFile(t *testing.T) {
	t.Parallel()

	generator, err := NewGenerator(testdata.TestPrivateKeyPEM)
	require.NoError(t, err)

	manager, err := NewManager(testdata.TestPublicKeyPEM)
	require.NoError(t, err)

	// Create initial license
	license1, err := generator.Generate(LicenseRequest{
		CustomerID:   "watch_v1",
		CustomerName: "Watch Test V1",
		Features:     []Feature{FeatureAuditLog},
		Tier:         "basic",
		ValidDays:    30,
	})
	require.NoError(t, err)

	// Write to temp file and load
	tmpDir := t.TempDir()
	licensePath := filepath.Join(tmpDir, "license.jwt")
	err = os.WriteFile(licensePath, []byte(license1), 0600)
	require.NoError(t, err)

	err = manager.LoadLicenseFromFile(licensePath)
	require.NoError(t, err)

	// Track reloads
	var reloadCount atomic.Int64
	var lastErr atomic.Pointer[error]

	// Start watching with short interval for testing
	stop := manager.WatchLicenseFile(50*time.Millisecond, func(err error) {
		reloadCount.Add(1)
		if err != nil {
			lastErr.Store(&err)
		}
	})
	defer stop()

	// Wait a bit
	time.Sleep(100 * time.Millisecond)

	// Verify initial state
	assert.Equal(t, "watch_v1", manager.GetLicense().CustomerID)

	// Update the file
	license2, err := generator.Generate(LicenseRequest{
		CustomerID:   "watch_v2",
		CustomerName: "Watch Test V2",
		Features:     []Feature{FeatureAuditLog, FeatureLDAP},
		Tier:         "premium",
		ValidDays:    365,
	})
	require.NoError(t, err)

	// Touch file to update mod time and write new content
	time.Sleep(100 * time.Millisecond) // Ensure mod time changes
	err = os.WriteFile(licensePath, []byte(license2), 0600)
	require.NoError(t, err)

	// Wait for watcher to pick up change
	time.Sleep(200 * time.Millisecond)

	// Verify reload happened
	assert.GreaterOrEqual(t, reloadCount.Load(), int64(1))
	assert.Equal(t, "watch_v2", manager.GetLicense().CustomerID)
	assert.True(t, manager.GetLicense().HasFeature(FeatureLDAP))

	// Stop watcher
	stop()

	// Verify no panic on double stop
	stop()
}
