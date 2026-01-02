//go:build !enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package taskqueue provides stubs for enterprise-only task handlers.
// Core taskqueue functionality is available in pkg/taskqueue.
package taskqueue

import (
	"context"
	"errors"
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/manager"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

var ErrEnterpriseRequired = errors.New("enterprise task types require enterprise edition")

// Enterprise task types (stubs)
const (
	TaskTypeReplication taskqueue.TaskType = "replication"
	TaskTypeAuditLog    taskqueue.TaskType = "audit_log"
	TaskTypeWebhook     taskqueue.TaskType = "webhook"
)

// ObjectReader provides read access to objects in local storage.
type ObjectReader interface {
	ReadObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectMeta, error)
}

// ObjectMeta contains metadata about an object.
type ObjectMeta struct {
	Size        int64
	ContentType string
	ETag        string
	Metadata    map[string]string
}

// RegionEndpoints provides S3 endpoints for remote regions.
type RegionEndpoints interface {
	GetS3Endpoint(region string) string
}

// ReplicationCredentials provides credentials for cross-region replication.
type ReplicationCredentials struct {
	AccessKeyID     string
	SecretAccessKey string
}

// Dependencies is required by enterprise handlers.
type Dependencies struct {
	ObjectReader           ObjectReader
	RegionEndpoints        RegionEndpoints
	ReplicationCredentials ReplicationCredentials
}

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

// NewObjectServiceAdapter returns nil in community edition.
func NewObjectServiceAdapter(service object.Service) ObjectReader {
	return nil
}

// NewRegionConfigAdapter returns nil in community edition.
func NewRegionConfigAdapter(config *manager.RegionConfig) RegionEndpoints {
	return nil
}
