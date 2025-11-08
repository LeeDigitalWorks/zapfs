//go:build enterprise

// Package taskqueue provides enterprise task handlers.
// Core taskqueue functionality is in pkg/taskqueue.
package taskqueue

import (
	"zapfs/pkg/taskqueue"
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
