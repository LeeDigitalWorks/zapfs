// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReportReconciliation(t *testing.T) {
	ms := &ManagerServer{
		state: NewFSMState("test-region", 3),
	}

	ctx := context.Background()
	req := &manager_pb.ReconciliationReport{
		ServerId:       "file-1:8001",
		TotalChunks:    1000,
		ExpectedChunks: 950,
		OrphansDeleted: 40,
		OrphansSkipped: 10,
		MissingChunks:  []string{"chunk-1", "chunk-2"},
		DurationMs:     5000,
	}

	resp, err := ms.ReportReconciliation(ctx, req)
	require.NoError(t, err)
	assert.True(t, resp.Success)
	assert.Contains(t, resp.Message, "40 orphans deleted")
	assert.Contains(t, resp.Message, "2 missing chunks")
}

// Note: TriggerReconciliation tests require a raftNode to check for leadership
// These tests are better suited for integration tests

func TestGetMetadataServiceAddress_NoServices(t *testing.T) {
	ms := &ManagerServer{
		state: NewFSMState("test-region", 3),
	}

	addr, err := ms.getMetadataServiceAddress()
	assert.Error(t, err)
	assert.Empty(t, addr)
	assert.Contains(t, err.Error(), "no metadata service available")
}
