//go:build !enterprise

// Package taskqueue provides stubs for enterprise-only task handlers.
// Core taskqueue functionality is available in pkg/taskqueue.
package taskqueue

import (
	"errors"

	"zapfs/pkg/taskqueue"
)

var ErrEnterpriseRequired = errors.New("enterprise task types require enterprise edition")

// Enterprise task types (stubs)
const (
	TaskTypeReplication taskqueue.TaskType = "replication"
	TaskTypeAuditLog    taskqueue.TaskType = "audit_log"
	TaskTypeWebhook     taskqueue.TaskType = "webhook"
)

// Dependencies is a stub.
type Dependencies struct{}

// EnterpriseHandlers returns nil in community edition.
func EnterpriseHandlers(deps Dependencies) []taskqueue.Handler {
	return nil
}

// ReplicationPayload is a stub.
type ReplicationPayload struct {
	SourceRegion string `json:"source_region"`
	SourceBucket string `json:"source_bucket"`
	SourceKey    string `json:"source_key"`
	DestRegion   string `json:"dest_region"`
	DestBucket   string `json:"dest_bucket"`
	Operation    string `json:"operation"`
}

// NewReplicationTask returns an error in community edition.
func NewReplicationTask(payload ReplicationPayload) (*taskqueue.Task, error) {
	return nil, ErrEnterpriseRequired
}
