// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import "encoding/xml"

// ReplicationConfiguration defines cross-region replication rules for a bucket.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_ReplicationConfiguration.html
type ReplicationConfiguration struct {
	XMLName xml.Name          `xml:"ReplicationConfiguration"`
	Role    string            `xml:"Role"` // IAM role ARN (optional for ZapFS)
	Rules   []ReplicationRule `xml:"Rule"`
}

// ReplicationRule defines a single replication rule.
type ReplicationRule struct {
	ID       string                 `xml:"ID,omitempty"`
	Priority int                    `xml:"Priority,omitempty"`
	Status   ReplicationRuleStatus  `xml:"Status"`
	Filter   *ReplicationRuleFilter `xml:"Filter,omitempty"`

	// Prefix is deprecated but still supported
	Prefix string `xml:"Prefix,omitempty"`

	// Destination configuration
	Destination ReplicationDestination `xml:"Destination"`

	// Optional features
	DeleteMarkerReplication   *DeleteMarkerReplication   `xml:"DeleteMarkerReplication,omitempty"`
	ExistingObjectReplication *ExistingObjectReplication `xml:"ExistingObjectReplication,omitempty"`
	SourceSelectionCriteria   *SourceSelectionCriteria   `xml:"SourceSelectionCriteria,omitempty"`
}

// ReplicationRuleStatus indicates if a rule is enabled.
type ReplicationRuleStatus string

const (
	ReplicationRuleStatusEnabled  ReplicationRuleStatus = "Enabled"
	ReplicationRuleStatusDisabled ReplicationRuleStatus = "Disabled"
)

// ReplicationRuleFilter defines which objects are replicated.
type ReplicationRuleFilter struct {
	Prefix string          `xml:"Prefix,omitempty"`
	Tag    *Tag            `xml:"Tag,omitempty"`
	And    *ReplicationAnd `xml:"And,omitempty"`
}

// ReplicationAnd combines multiple filter conditions.
type ReplicationAnd struct {
	Prefix string `xml:"Prefix,omitempty"`
	Tags   []Tag  `xml:"Tag,omitempty"`
}

// ReplicationDestination defines where objects are replicated to.
type ReplicationDestination struct {
	// Bucket ARN or bucket name (ZapFS also accepts just the bucket name)
	Bucket string `xml:"Bucket"`

	// Optional: Account ID of destination bucket owner
	Account string `xml:"Account,omitempty"`

	// Optional: Storage class for replicated objects
	StorageClass string `xml:"StorageClass,omitempty"`

	// Optional: Encryption configuration
	EncryptionConfiguration *EncryptionConfiguration `xml:"EncryptionConfiguration,omitempty"`

	// Optional: Replica modifications
	ReplicaModifications *ReplicaModifications `xml:"ReplicaModifications,omitempty"`

	// Optional: Access control translation
	AccessControlTranslation *AccessControlTranslation `xml:"AccessControlTranslation,omitempty"`

	// Optional: Metrics configuration
	Metrics *ReplicationMetrics `xml:"Metrics,omitempty"`

	// Optional: Replication time control
	ReplicationTime *ReplicationTime `xml:"ReplicationTime,omitempty"`
}

// EncryptionConfiguration specifies encryption for replicated objects.
type EncryptionConfiguration struct {
	ReplicaKmsKeyID string `xml:"ReplicaKmsKeyID,omitempty"`
}

// ReplicaModifications controls replication of metadata changes.
type ReplicaModifications struct {
	Status string `xml:"Status"` // "Enabled" or "Disabled"
}

// AccessControlTranslation controls ownership translation.
type AccessControlTranslation struct {
	Owner string `xml:"Owner"` // "Destination"
}

// ReplicationMetrics contains metrics configuration.
type ReplicationMetrics struct {
	Status         string                     `xml:"Status"` // "Enabled" or "Disabled"
	EventThreshold *ReplicationEventThreshold `xml:"EventThreshold,omitempty"`
}

// ReplicationEventThreshold defines when events are generated.
type ReplicationEventThreshold struct {
	Minutes int `xml:"Minutes"`
}

// ReplicationTime defines replication time control settings.
type ReplicationTime struct {
	Status string                `xml:"Status"` // "Enabled" or "Disabled"
	Time   *ReplicationTimeValue `xml:"Time,omitempty"`
}

// ReplicationTimeValue defines the replication time target.
type ReplicationTimeValue struct {
	Minutes int `xml:"Minutes"`
}

// DeleteMarkerReplication controls delete marker replication.
type DeleteMarkerReplication struct {
	Status string `xml:"Status"` // "Enabled" or "Disabled"
}

// ExistingObjectReplication controls replication of existing objects.
type ExistingObjectReplication struct {
	Status string `xml:"Status"` // "Enabled" or "Disabled"
}

// SourceSelectionCriteria defines additional source selection criteria.
type SourceSelectionCriteria struct {
	SseKmsEncryptedObjects *SseKmsEncryptedObjects `xml:"SseKmsEncryptedObjects,omitempty"`
	ReplicaModifications   *ReplicaModifications   `xml:"ReplicaModifications,omitempty"`
}

// SseKmsEncryptedObjects controls replication of KMS-encrypted objects.
type SseKmsEncryptedObjects struct {
	Status string `xml:"Status"` // "Enabled" or "Disabled"
}
