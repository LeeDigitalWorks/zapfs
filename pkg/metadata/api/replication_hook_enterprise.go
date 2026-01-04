//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
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

// TagLoader is a function that loads tags for an object.
// Returns nil if object has no tags or an error occurs.
type TagLoader func(ctx context.Context, bucket, key string) (map[string]string, error)

// CRRHook queues replication tasks for objects in buckets with replication enabled.
type CRRHook struct {
	queue       taskqueue.Queue
	localRegion string
	tagLoader   TagLoader // Optional: loads object tags for tag filtering
}

// NewCRRHook creates a new cross-region replication hook.
func NewCRRHook(queue taskqueue.Queue, localRegion string) *CRRHook {
	return &CRRHook{
		queue:       queue,
		localRegion: localRegion,
	}
}

// SetTagLoader sets the function used to load object tags for tag filtering.
// If not set, rules with tag filters will be skipped.
func (h *CRRHook) SetTagLoader(loader TagLoader) {
	h.tagLoader = loader
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

	// Load tags lazily (only if any rule has tag filters)
	var objectTags map[string]string
	var tagsLoaded bool

	// Process each replication rule
	for _, rule := range bucket.ReplicationConfig.Rules {
		if rule.Status != s3types.ReplicationRuleStatusEnabled {
			continue
		}

		// Load tags if this rule has tag filters and we haven't loaded yet
		if !tagsLoaded && ruleHasTagFilter(&rule) {
			if h.tagLoader != nil {
				var err error
				objectTags, err = h.tagLoader(ctx, bucket.Name, key)
				if err != nil {
					logger.Warn().Err(err).
						Str("bucket", bucket.Name).
						Str("key", key).
						Msg("failed to load tags for replication filter")
				}
			}
			tagsLoaded = true
		}

		// Check if key matches the rule filter
		if !matchesReplicationRule(key, &rule, objectTags) {
			continue
		}

		// Determine destination
		destBucket := extractBucketName(rule.Destination.Bucket)
		if destBucket == "" {
			destBucket = bucket.Name
		}

		// TODO: Parse region from destination ARN; storage class could inform region
		destRegion := "default"
		_ = rule.Destination.StorageClass // Reserved for future region mapping

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

	// Load tags lazily (only if any rule has tag filters)
	var objectTags map[string]string
	var tagsLoaded bool

	for _, rule := range bucket.ReplicationConfig.Rules {
		if rule.Status != s3types.ReplicationRuleStatusEnabled {
			continue
		}

		// Check if delete marker replication is enabled
		if rule.DeleteMarkerReplication == nil || rule.DeleteMarkerReplication.Status != "Enabled" {
			continue
		}

		// Load tags if this rule has tag filters and we haven't loaded yet
		if !tagsLoaded && ruleHasTagFilter(&rule) {
			if h.tagLoader != nil {
				var err error
				objectTags, err = h.tagLoader(ctx, bucket.Name, key)
				if err != nil {
					logger.Warn().Err(err).
						Str("bucket", bucket.Name).
						Str("key", key).
						Msg("failed to load tags for replication filter")
				}
			}
			tagsLoaded = true
		}

		if !matchesReplicationRule(key, &rule, objectTags) {
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

// ruleHasTagFilter returns true if the rule has any tag-based filtering.
func ruleHasTagFilter(rule *s3types.ReplicationRule) bool {
	if rule.Filter == nil {
		return false
	}
	if rule.Filter.Tag != nil {
		return true
	}
	if rule.Filter.And != nil && len(rule.Filter.And.Tags) > 0 {
		return true
	}
	return false
}

// matchesReplicationRule checks if a key matches the rule's filter.
func matchesReplicationRule(key string, rule *s3types.ReplicationRule, objectTags map[string]string) bool {
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

		// Check single tag filter
		if rule.Filter.Tag != nil {
			if !matchTag(objectTags, rule.Filter.Tag.Key, rule.Filter.Tag.Value) {
				return false
			}
		}

		// Check And filter (prefix + multiple tags)
		if rule.Filter.And != nil {
			// Check prefix in And filter
			if rule.Filter.And.Prefix != "" {
				if len(key) < len(rule.Filter.And.Prefix) || key[:len(rule.Filter.And.Prefix)] != rule.Filter.And.Prefix {
					return false
				}
			}
			// Check all tags in And filter (all must match)
			for _, tag := range rule.Filter.And.Tags {
				if !matchTag(objectTags, tag.Key, tag.Value) {
					return false
				}
			}
		}
	}

	return true
}

// matchTag checks if objectTags contains the specified key-value pair.
func matchTag(objectTags map[string]string, key, value string) bool {
	if objectTags == nil {
		return false
	}
	v, exists := objectTags[key]
	return exists && v == value
}

// extractBucketName extracts bucket name from ARN or returns as-is.
func extractBucketName(bucketOrARN string) string {
	// ARN format: arn:aws:s3:::bucket-name
	if len(bucketOrARN) > 13 && bucketOrARN[:13] == "arn:aws:s3:::" {
		return bucketOrARN[13:]
	}
	return bucketOrARN
}
