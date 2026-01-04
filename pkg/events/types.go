// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
)

// S3Event represents an S3 event notification in AWS format.
// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-content-structure.html
type S3Event struct {
	Records []S3EventRecord `json:"Records"`
}

// S3EventRecord represents a single event within an S3 notification.
type S3EventRecord struct {
	EventVersion string    `json:"eventVersion"`
	EventSource  string    `json:"eventSource"`
	AWSRegion    string    `json:"awsRegion"`
	EventTime    time.Time `json:"eventTime"`
	EventName    string    `json:"eventName"`

	UserIdentity      S3UserIdentity      `json:"userIdentity"`
	RequestParameters S3RequestParameters `json:"requestParameters"`
	ResponseElements  S3ResponseElements  `json:"responseElements"`
	S3                S3Entity            `json:"s3"`
}

// S3UserIdentity identifies the user who made the request.
type S3UserIdentity struct {
	PrincipalID string `json:"principalId"`
}

// S3RequestParameters contains request metadata.
type S3RequestParameters struct {
	SourceIPAddress string `json:"sourceIPAddress"`
}

// S3ResponseElements contains response metadata.
type S3ResponseElements struct {
	RequestID string `json:"x-amz-request-id"`
	HostID    string `json:"x-amz-id-2"`
}

// S3Entity contains the S3-specific event data.
type S3Entity struct {
	SchemaVersion   string         `json:"s3SchemaVersion"`
	ConfigurationID string         `json:"configurationId"`
	Bucket          S3BucketEntity `json:"bucket"`
	Object          S3ObjectEntity `json:"object"`
}

// S3BucketEntity contains bucket information.
type S3BucketEntity struct {
	Name          string              `json:"name"`
	OwnerIdentity S3BucketOwnerEntity `json:"ownerIdentity"`
	ARN           string              `json:"arn"`
}

// S3BucketOwnerEntity contains bucket owner information.
type S3BucketOwnerEntity struct {
	PrincipalID string `json:"principalId"`
}

// S3ObjectEntity contains object information.
type S3ObjectEntity struct {
	Key       string `json:"key"`
	Size      int64  `json:"size"`
	ETag      string `json:"eTag"`
	VersionID string `json:"versionId,omitempty"`
	Sequencer string `json:"sequencer"`
}

// EventType categorizes S3 events.
type EventType string

// S3 event type constants.
// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html
const (
	// Object created events
	EventObjectCreated               EventType = "s3:ObjectCreated:*"
	EventObjectCreatedPut            EventType = "s3:ObjectCreated:Put"
	EventObjectCreatedPost           EventType = "s3:ObjectCreated:Post"
	EventObjectCreatedCopy           EventType = "s3:ObjectCreated:Copy"
	EventObjectCreatedCompleteUpload EventType = "s3:ObjectCreated:CompleteMultipartUpload"

	// Object removed events
	EventObjectRemoved             EventType = "s3:ObjectRemoved:*"
	EventObjectRemovedDelete       EventType = "s3:ObjectRemoved:Delete"
	EventObjectRemovedDeleteMarker EventType = "s3:ObjectRemoved:DeleteMarkerCreated"

	// Object restore events (Enterprise - Glacier-like)
	EventObjectRestore          EventType = "s3:ObjectRestore:*"
	EventObjectRestorePost      EventType = "s3:ObjectRestore:Post"
	EventObjectRestoreCompleted EventType = "s3:ObjectRestore:Completed"
	EventObjectRestoreDelete    EventType = "s3:ObjectRestore:Delete"

	// Object tagging events
	EventObjectTagging       EventType = "s3:ObjectTagging:*"
	EventObjectTaggingPut    EventType = "s3:ObjectTagging:Put"
	EventObjectTaggingDelete EventType = "s3:ObjectTagging:Delete"

	// Object ACL events
	EventObjectACLPut EventType = "s3:ObjectAcl:Put"

	// Lifecycle events
	EventLifecycleExpiration             EventType = "s3:LifecycleExpiration:*"
	EventLifecycleExpirationDelete       EventType = "s3:LifecycleExpiration:Delete"
	EventLifecycleExpirationDeleteMarker EventType = "s3:LifecycleExpiration:DeleteMarkerCreated"
	EventLifecycleTransition             EventType = "s3:LifecycleTransition"

	// Replication events (Enterprise)
	EventReplication                EventType = "s3:Replication:*"
	EventReplicationCompleted       EventType = "s3:Replication:OperationCompletedReplication"
	EventReplicationFailed          EventType = "s3:Replication:OperationFailedReplication"
	EventReplicationMissedThreshold EventType = "s3:Replication:OperationMissedThreshold"
)

// BuildS3Event converts an EventPayload to an S3Event for delivery.
func BuildS3Event(payload *taskqueue.EventPayload, region, configID string) *S3Event {
	return &S3Event{
		Records: []S3EventRecord{
			{
				EventVersion: "2.1",
				EventSource:  "zapfs:s3",
				AWSRegion:    region,
				EventTime:    time.UnixMilli(payload.Timestamp),
				EventName:    payload.EventName,
				UserIdentity: S3UserIdentity{
					PrincipalID: payload.OwnerID,
				},
				RequestParameters: S3RequestParameters{
					SourceIPAddress: payload.SourceIP,
				},
				ResponseElements: S3ResponseElements{
					RequestID: payload.RequestID,
					HostID:    "", // ZapFS doesn't use x-amz-id-2
				},
				S3: S3Entity{
					SchemaVersion:   "1.0",
					ConfigurationID: configID,
					Bucket: S3BucketEntity{
						Name: payload.Bucket,
						OwnerIdentity: S3BucketOwnerEntity{
							PrincipalID: payload.OwnerID,
						},
						ARN: "arn:aws:s3:::" + payload.Bucket,
					},
					Object: S3ObjectEntity{
						Key:       payload.Key,
						Size:      payload.Size,
						ETag:      payload.ETag,
						VersionID: payload.VersionID,
						Sequencer: payload.Sequencer,
					},
				},
			},
		},
	}
}

// MatchesEventType checks if an event name matches an event type pattern.
// Supports wildcard matching (e.g., "s3:ObjectCreated:*" matches "s3:ObjectCreated:Put").
func MatchesEventType(pattern EventType, eventName string) bool {
	patternStr := string(pattern)

	// Exact match
	if patternStr == eventName {
		return true
	}

	// Wildcard match
	if len(patternStr) > 0 && patternStr[len(patternStr)-1] == '*' {
		prefix := patternStr[:len(patternStr)-1]
		return len(eventName) >= len(prefix) && eventName[:len(prefix)] == prefix
	}

	return false
}

// MatchesFilterRules checks if an object key matches the filter rules.
// Returns true if no rules are specified or if the key matches all rules.
func MatchesFilterRules(key string, prefix, suffix string) bool {
	if prefix != "" && (len(key) < len(prefix) || key[:len(prefix)] != prefix) {
		return false
	}
	if suffix != "" && (len(key) < len(suffix) || key[len(key)-len(suffix):] != suffix) {
		return false
	}
	return true
}
