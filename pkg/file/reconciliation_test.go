// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package file

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReconciliationConfig(t *testing.T) {
	config := ReconciliationConfig{
		ServerID:    "file-1:8001",
		GracePeriod: 2 * time.Hour,
		Interval:    6 * time.Hour,
		DryRun:      false,
	}

	assert.Equal(t, "file-1:8001", config.ServerID)
	assert.Equal(t, 2*time.Hour, config.GracePeriod)
	assert.Equal(t, 6*time.Hour, config.Interval)
	assert.False(t, config.DryRun)
}

func TestReconciliationReport(t *testing.T) {
	report := &ReconciliationReport{
		ServerID:       "file-1:8001",
		TotalChunks:    1000,
		ExpectedChunks: 950,
		OrphansDeleted: 40,
		OrphansSkipped: 10,
		MissingChunks:  []string{"chunk-1", "chunk-2"},
		Duration:       5 * time.Second,
	}

	assert.Equal(t, "file-1:8001", report.ServerID)
	assert.Equal(t, int64(1000), report.TotalChunks)
	assert.Equal(t, int64(950), report.ExpectedChunks)
	assert.Equal(t, int64(40), report.OrphansDeleted)
	assert.Equal(t, int64(10), report.OrphansSkipped)
	assert.Equal(t, 2, len(report.MissingChunks))
	assert.Equal(t, 5*time.Second, report.Duration)
	assert.Nil(t, report.Error)
}

func TestReconciliationService_DisabledWhenIntervalZero(t *testing.T) {
	config := ReconciliationConfig{
		ServerID:    "file-1:8001",
		GracePeriod: 2 * time.Hour,
		Interval:    0, // Disabled
	}

	// Create service without manager client or store (won't be used since interval=0)
	svc := NewReconciliationService(config, nil, nil)

	// Start should return immediately since interval=0
	// The doneCh should be closed immediately
	svc.Start(nil)

	select {
	case <-svc.doneCh:
		// Expected - doneCh should be closed since periodic is disabled
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Expected doneCh to be closed for disabled reconciliation")
	}
}

func TestReconciliationService_IsRunningFlag(t *testing.T) {
	config := ReconciliationConfig{
		ServerID:    "file-1:8001",
		GracePeriod: 2 * time.Hour,
		Interval:    0, // Disabled
	}

	svc := NewReconciliationService(config, nil, nil)

	// Initially not running
	assert.False(t, svc.IsRunning())

	// GetLastReport should return nil initially
	assert.Nil(t, svc.GetLastReport())
}
