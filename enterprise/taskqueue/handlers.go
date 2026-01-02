//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package taskqueue provides enterprise task handlers.
// Core taskqueue functionality is in pkg/taskqueue.
package taskqueue

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// Enterprise task types (requires license)
const (
	TaskTypeReplication taskqueue.TaskType = "replication" // CRR object replication
	TaskTypeAuditLog    taskqueue.TaskType = "audit_log"   // Ship audit logs
	TaskTypeWebhook     taskqueue.TaskType = "webhook"     // Deliver webhooks
)

// Dependencies required by enterprise handlers.
type Dependencies struct {
	// ObjectReader for reading objects from local storage
	ObjectReader ObjectReader

	// RegionEndpoints for getting S3 endpoints per region
	RegionEndpoints RegionEndpoints

	// ReplicationCredentials for authenticating to remote regions
	ReplicationCredentials ReplicationCredentials
}

// EnterpriseHandlers returns all enterprise task handlers.
// Returns empty slice if dependencies are not provided.
func EnterpriseHandlers(deps Dependencies) []taskqueue.Handler {
	handlers := []taskqueue.Handler{}

	// Only add replication handler if dependencies are configured
	if deps.ObjectReader != nil && deps.RegionEndpoints != nil {
		handlers = append(handlers, NewReplicationHandler(ReplicationHandlerConfig{
			ObjectReader: deps.ObjectReader,
			Endpoints:    deps.RegionEndpoints,
			Credentials:  deps.ReplicationCredentials,
		}))
	}

	return handlers
}
