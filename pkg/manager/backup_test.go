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

func TestCreateBackupRequiresLicense(t *testing.T) {
	// Create a minimal manager server with nil raftNode
	// The license check happens before raft access, so this is safe
	ms := &ManagerServer{
		fileServices:     make(map[string]*ServiceRegistration),
		metadataServices: make(map[string]*ServiceRegistration),
		collections:      make(map[string]*manager_pb.Collection),
	}

	// Without license, CreateBackup should fail with permission denied
	resp, err := ms.CreateBackup(context.Background(), &manager_pb.CreateBackupRequest{})

	// Should return permission denied error
	require.Error(t, err)
	assert.Contains(t, err.Error(), "PermissionDenied")
	assert.Contains(t, err.Error(), "enterprise license")
	assert.Nil(t, resp)
}
