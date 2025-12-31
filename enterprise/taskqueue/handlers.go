//go:build enterprise

// Copyright 2025 ZapInvest, Inc. All rights reserved.
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
	// TODO: Add actual dependencies as handlers are implemented
	// RegionClient RegionClient
	// FileClient   client.File
	// AuditStore   AuditStore
	// HTTPClient   *http.Client
}

// EnterpriseHandlers returns all enterprise task handlers.
// Returns nil if dependencies are not provided.
func EnterpriseHandlers(deps Dependencies) []taskqueue.Handler {
	return []taskqueue.Handler{
		NewReplicationHandler(),
		// TODO: Add more handlers as implemented
		// NewAuditHandler(deps.AuditStore),
		// NewWebhookHandler(deps.HTTPClient),
	}
}
