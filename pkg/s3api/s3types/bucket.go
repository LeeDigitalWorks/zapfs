// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import (
	"time"

	"github.com/google/uuid"
)

// Bucket represents an S3 bucket with all its configuration.
type Bucket struct {
	ID         uuid.UUID
	Name       string
	OwnerID    string
	CreateTime time.Time
	Versioning Versioning
	BucketMode BucketMode

	// Location/region constraint
	Location string

	// Core configurations
	LifecyclePolicy   *Lifecycle
	Logging           *BucketLoggingStatus
	Policy            *BucketPolicy
	ACL               *AccessControlList
	ReplicationConfig *ReplicationConfiguration // Enterprise: cross-region replication

	// Additional S3 configurations
	CORS               *CORSConfiguration          // Cross-origin resource sharing
	Tagging            *TagSet                     // Bucket tags
	Encryption         *ServerSideEncryptionConfig // Default encryption
	Website            *WebsiteConfiguration       // Static website hosting
	ObjectLockConfig   *ObjectLockConfiguration    // WORM compliance
	NotificationConfig *NotificationConfiguration  // Event notifications
	Accelerate         *AccelerateConfiguration    // Transfer acceleration
	RequestPayment     *RequestPaymentConfig       // Requester pays
	OwnershipControls  *OwnershipControls          // Object ownership
	PublicAccessBlock  *PublicAccessBlockConfig    // Block public access
}
