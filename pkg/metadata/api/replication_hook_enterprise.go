//go:build enterprise

// Copyright 2025 ZapInvest, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package api

import (
	"context"

	"github.com/LeeDigitalWorks/zapfs/enterprise/license"
	enttaskqueue "github.com/LeeDigitalWorks/zapfs/enterprise/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// CRRHook queues replication tasks for objects in buckets with replication enabled.
type CRRHook struct {
	queue       taskqueue.Queue
	localRegion string
}

// NewCRRHook creates a new cross-region replication hook.
func NewCRRHook(queue taskqueue.Queue, localRegion string) *CRRHook {
	return &CRRHook{
		queue:       queue,
		localRegion: localRegion,
	}
}

// AfterPutObject is called after an object is successfully stored.
// It queues replication tasks if the bucket has replication configured.
func (h *CRRHook) AfterPutObject(ctx context.Context, bucket *s3types.Bucket, key, etag string, size int64) {
	if h.queue == nil || bucket == nil {
		return
	}

	// Check if bucket has replication configured
	if bucket.ReplicationConfig == nil {
		return
	}

	// Check license for replication feature
	mgr := license.GetManager()
	if mgr == nil || mgr.CheckFeature(license.FeatureMultiRegion) != nil {
		logger.Debug().Msg("CRR skipped: not licensed")
		return
	}

	// Process each replication rule
	for _, rule := range bucket.ReplicationConfig.Rules {
		if rule.Status != s3types.ReplicationRuleStatusEnabled {
			continue
		}

		// Check if key matches the rule filter
		if !matchesReplicationRule(key, &rule) {
			continue
		}

		// Determine destination
		destBucket := extractBucketName(rule.Destination.Bucket)
		if destBucket == "" {
			destBucket = bucket.Name
		}

		destRegion := "default" // TODO: Parse from destination ARN
		if rule.Destination.StorageClass != "" {
			// Could use storage class to determine region
		}

		// Create replication task
		task, err := enttaskqueue.NewReplicationTask(enttaskqueue.ReplicationPayload{
			SourceRegion: h.localRegion,
			SourceBucket: bucket.Name,
			SourceKey:    key,
			SourceETag:   etag,
			SourceSize:   size,
			DestRegion:   destRegion,
			DestBucket:   destBucket,
			Operation:    "PUT",
		})
		if err != nil {
			logger.Error().Err(err).
				Str("bucket", bucket.Name).
				Str("key", key).
				Msg("failed to create replication task")
			continue
		}

		// Enqueue
		if err := h.queue.Enqueue(ctx, task); err != nil {
			logger.Error().Err(err).
				Str("bucket", bucket.Name).
				Str("key", key).
				Str("dest_region", destRegion).
				Msg("failed to queue replication task")
			continue
		}

		logger.Debug().
			Str("task_id", task.ID).
			Str("bucket", bucket.Name).
			Str("key", key).
			Str("dest_region", destRegion).
			Str("dest_bucket", destBucket).
			Msg("queued CRR task")
	}
}

// AfterDeleteObject queues delete replication if configured.
func (h *CRRHook) AfterDeleteObject(ctx context.Context, bucket *s3types.Bucket, key string) {
	if h.queue == nil || bucket == nil || bucket.ReplicationConfig == nil {
		return
	}

	mgr := license.GetManager()
	if mgr == nil || mgr.CheckFeature(license.FeatureMultiRegion) != nil {
		return
	}

	for _, rule := range bucket.ReplicationConfig.Rules {
		if rule.Status != s3types.ReplicationRuleStatusEnabled {
			continue
		}

		// Check if delete marker replication is enabled
		if rule.DeleteMarkerReplication == nil || rule.DeleteMarkerReplication.Status != "Enabled" {
			continue
		}

		if !matchesReplicationRule(key, &rule) {
			continue
		}

		destBucket := extractBucketName(rule.Destination.Bucket)
		if destBucket == "" {
			destBucket = bucket.Name
		}

		task, err := enttaskqueue.NewReplicationTask(enttaskqueue.ReplicationPayload{
			SourceRegion: h.localRegion,
			SourceBucket: bucket.Name,
			SourceKey:    key,
			DestRegion:   "default",
			DestBucket:   destBucket,
			Operation:    "DELETE",
		})
		if err != nil {
			continue
		}

		h.queue.Enqueue(ctx, task)
	}
}

// matchesReplicationRule checks if a key matches the rule's filter.
func matchesReplicationRule(key string, rule *s3types.ReplicationRule) bool {
	// Check prefix (deprecated but still supported)
	if rule.Prefix != "" {
		if len(key) < len(rule.Prefix) || key[:len(rule.Prefix)] != rule.Prefix {
			return false
		}
	}

	// Check filter
	if rule.Filter != nil {
		if rule.Filter.Prefix != "" {
			if len(key) < len(rule.Filter.Prefix) || key[:len(rule.Filter.Prefix)] != rule.Filter.Prefix {
				return false
			}
		}
		// TODO: Check tags
	}

	return true
}

// extractBucketName extracts bucket name from ARN or returns as-is.
func extractBucketName(bucketOrARN string) string {
	// ARN format: arn:aws:s3:::bucket-name
	if len(bucketOrARN) > 13 && bucketOrARN[:13] == "arn:aws:s3:::" {
		return bucketOrARN[13:]
	}
	return bucketOrARN
}
