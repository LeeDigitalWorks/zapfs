// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3action

// This file represents the S3 actions that are used to handle S3 requests.
// https://docs.aws.amazon.com/AmazonS3/latest/API/API_Operations_Amazon_Simple_Storage_Service.html
type Action int

// OperationType classifies S3 actions by their effect on data.
// Useful for rate limiting, auditing, read-only access policies, etc.
type OperationType int

const (
	OpRead  OperationType = iota // GET, HEAD - retrieving data
	OpWrite                      // PUT, POST, DELETE - modifying data
	OpList                       // List operations - enumeration (typically more expensive)
)

func (o OperationType) String() string {
	switch o {
	case OpRead:
		return "read"
	case OpWrite:
		return "write"
	case OpList:
		return "list"
	default:
		return "unknown"
	}
}

// ResourceType indicates whether an action operates on a bucket or object.
// Useful for IAM policy evaluation and resource-level permissions.
type ResourceType int

const (
	ResourceBucket  ResourceType = iota // Bucket-level operations
	ResourceObject                      // Object-level operations
	ResourceService                     // Service-level operations (e.g., ListBuckets)
)

func (r ResourceType) String() string {
	switch r {
	case ResourceBucket:
		return "bucket"
	case ResourceObject:
		return "object"
	case ResourceService:
		return "service"
	default:
		return "unknown"
	}
}

// Removed the following actions:
// - GetBucketLifecycle
// - GetBucketNotification
// - ListDirectoryBuckets
// - PutBucketLifecycle
// - PutBucketNotification
// Added the following actions:
// - OptionsPreflight
// - PostObject
const (
	AbortMultipartUpload Action = iota
	CompleteMultipartUpload
	CopyObject
	CreateBucket
	CreateMultipartUpload
	CreateSession
	DeleteBucket
	DeleteBucketAnalyticsConfiguration
	DeleteBucketCors
	DeleteBucketEncryption
	DeleteBucketIntelligentTieringConfiguration
	DeleteBucketInventoryConfiguration
	DeleteBucketLifecycle
	DeleteBucketMetricsConfiguration
	DeleteBucketOwnershipControls
	DeleteBucketPolicy
	DeleteBucketReplication
	DeleteBucketTagging
	DeleteBucketWebsite
	DeleteObject
	DeleteObjects
	DeleteObjectTagging
	DeletePublicAccessBlock
	GetBucketAccelerateConfiguration
	GetBucketAcl
	GetBucketAnalyticsConfiguration
	GetBucketCors
	GetBucketEncryption
	GetBucketIntelligentTieringConfiguration
	GetBucketInventoryConfiguration
	GetBucketLifecycleConfiguration
	GetBucketLocation
	GetBucketLogging
	GetBucketMetricsConfiguration
	GetBucketNotificationConfiguration
	GetBucketOwnershipControls
	GetBucketPolicy
	GetBucketPolicyStatus
	GetBucketReplication
	GetBucketRequestPayment
	GetBucketTagging
	GetBucketVersioning
	GetBucketWebsite
	GetObject
	GetObjectAcl
	GetObjectAttributes
	GetObjectLegalHold
	GetObjectLockConfiguration
	GetObjectRetention
	GetObjectTagging
	GetObjectTorrent
	GetPublicAccessBlock
	HeadBucket
	HeadObject
	ListBucketAnalyticsConfigurations
	ListBucketIntelligentTieringConfigurations
	ListBucketInventoryConfigurations
	ListBucketMetricsConfigurations
	ListBuckets
	ListMultipartUploads
	ListObjects
	ListObjectsV2
	ListObjectVersions
	ListParts
	OptionsPreflight
	PostObject
	PutBucketAccelerateConfiguration
	PutBucketAcl
	PutBucketAnalyticsConfiguration
	PutBucketCors
	PutBucketEncryption
	PutBucketIntelligentTieringConfiguration
	PutBucketInventoryConfiguration
	PutBucketLifecycleConfiguration
	PutBucketLogging
	PutBucketMetricsConfiguration
	PutBucketNotificationConfiguration
	PutBucketOwnershipControls
	PutBucketPolicy
	PutBucketReplication
	PutBucketRequestPayment
	PutBucketTagging
	PutBucketVersioning
	PutBucketWebsite
	PutObject
	PutObjectAcl
	PutObjectLegalHold
	PutObjectLockConfiguration
	PutObjectRetention
	PutObjectTagging
	PutPublicAccessBlock
	RestoreObject
	SelectObjectContent
	UploadPart
	UploadPartCopy
	WriteGetObjectResponse
	Unknown
)

var actionNames = map[Action]string{
	AbortMultipartUpload:               "s3:AbortMultipartUpload",
	CompleteMultipartUpload:            "s3:CompleteMultipartUpload",
	CopyObject:                         "s3:CopyObject",
	CreateBucket:                       "s3:CreateBucket",
	CreateMultipartUpload:              "s3:CreateMultipartUpload",
	CreateSession:                      "s3:CreateSession",
	DeleteBucket:                       "s3:DeleteBucket",
	DeleteBucketAnalyticsConfiguration: "s3:DeleteBucketAnalyticsConfiguration",
	DeleteBucketCors:                   "s3:DeleteBucketCors",
	DeleteBucketEncryption:             "s3:DeleteBucketEncryption",
	DeleteBucketIntelligentTieringConfiguration: "s3:DeleteBucketIntelligentTieringConfiguration",
	DeleteBucketInventoryConfiguration:          "s3:DeleteBucketInventoryConfiguration",
	DeleteBucketLifecycle:                       "s3:DeleteBucketLifecycle",
	DeleteBucketMetricsConfiguration:            "s3:DeleteBucketMetricsConfiguration",
	DeleteBucketOwnershipControls:               "s3:DeleteBucketOwnershipControls",
	DeleteBucketPolicy:                          "s3:DeleteBucketPolicy",
	DeleteBucketReplication:                     "s3:DeleteBucketReplication",
	DeleteBucketTagging:                         "s3:DeleteBucketTagging",
	DeleteBucketWebsite:                         "s3:DeleteBucketWebsite",
	DeleteObject:                                "s3:DeleteObject",
	DeleteObjects:                               "s3:DeleteObjects",
	DeleteObjectTagging:                         "s3:DeleteObjectTagging",
	DeletePublicAccessBlock:                     "s3:DeletePublicAccessBlock",
	GetBucketAccelerateConfiguration:            "s3:GetBucketAccelerateConfiguration",
	GetBucketAcl:                                "s3:GetBucketAcl",
	GetBucketAnalyticsConfiguration:             "s3:GetBucketAnalyticsConfiguration",
	GetBucketCors:                               "s3:GetBucketCors",
	GetBucketEncryption:                         "s3:GetBucketEncryption",
	GetBucketIntelligentTieringConfiguration:    "s3:GetBucketIntelligentTieringConfiguration",
	GetBucketInventoryConfiguration:             "s3:GetBucketInventoryConfiguration",
	GetBucketLifecycleConfiguration:             "s3:GetBucketLifecycleConfiguration",
	GetBucketLocation:                           "s3:GetBucketLocation",
	GetBucketLogging:                            "s3:GetBucketLogging",
	GetBucketMetricsConfiguration:               "s3:GetBucketMetricsConfiguration",
	GetBucketNotificationConfiguration:          "s3:GetBucketNotificationConfiguration",
	GetBucketOwnershipControls:                  "s3:GetBucketOwnershipControls",
	GetBucketPolicy:                             "s3:GetBucketPolicy",
	GetBucketPolicyStatus:                       "s3:GetBucketPolicyStatus",
	GetBucketReplication:                        "s3:GetBucketReplication",
	GetBucketRequestPayment:                     "s3:GetBucketRequestPayment",
	GetBucketTagging:                            "s3:GetBucketTagging",
	GetBucketVersioning:                         "s3:GetBucketVersioning",
	GetBucketWebsite:                            "s3:GetBucketWebsite",
	GetObject:                                   "s3:GetObject",
	GetObjectAcl:                                "s3:GetObjectAcl",
	GetObjectAttributes:                         "s3:GetObjectAttributes",
	GetObjectLegalHold:                          "s3:GetObjectLegalHold",
	GetObjectLockConfiguration:                  "s3:GetObjectLockConfiguration",
	GetObjectRetention:                          "s3:GetObjectRetention",
	GetObjectTagging:                            "s3:GetObjectTagging",
	GetObjectTorrent:                            "s3:GetObjectTorrent",
	GetPublicAccessBlock:                        "s3:GetPublicAccessBlock",
	HeadBucket:                                  "s3:HeadBucket",
	HeadObject:                                  "s3:HeadObject",
	ListBucketAnalyticsConfigurations:           "s3:ListBucketAnalyticsConfigurations",
	ListBucketIntelligentTieringConfigurations:  "s3:ListBucketIntelligentTieringConfigurations",
	ListBucketInventoryConfigurations:           "s3:ListBucketInventoryConfigurations",
	ListBucketMetricsConfigurations:             "s3:ListBucketMetricsConfigurations",
	ListBuckets:                                 "s3:ListBuckets",
	ListMultipartUploads:                        "s3:ListMultipartUploads",
	ListObjects:                                 "s3:ListObjects",
	ListObjectsV2:                               "s3:ListObjectsV2",
	ListObjectVersions:                          "s3:ListObjectVersions",
	ListParts:                                   "s3:ListParts",
	OptionsPreflight:                            "s3:OptionsPreflight",
	PostObject:                                  "s3:PostObject",
	PutBucketAccelerateConfiguration:            "s3:PutBucketAccelerateConfiguration",
	PutBucketAcl:                                "s3:PutBucketAcl",
	PutBucketAnalyticsConfiguration:             "s3:PutBucketAnalyticsConfiguration",
	PutBucketCors:                               "s3:PutBucketCors",
	PutBucketEncryption:                         "s3:PutBucketEncryption",
	PutBucketIntelligentTieringConfiguration:    "s3:PutBucketIntelligentTieringConfiguration",
	PutBucketInventoryConfiguration:             "s3:PutBucketInventoryConfiguration",
	PutBucketLifecycleConfiguration:             "s3:PutBucketLifecycleConfiguration",
	PutBucketLogging:                            "s3:PutBucketLogging",
	PutBucketMetricsConfiguration:               "s3:PutBucketMetricsConfiguration",
	PutBucketNotificationConfiguration:          "s3:PutBucketNotificationConfiguration",
	PutBucketOwnershipControls:                  "s3:PutBucketOwnershipControls",
	PutBucketPolicy:                             "s3:PutBucketPolicy",
	PutBucketReplication:                        "s3:PutBucketReplication",
	PutBucketRequestPayment:                     "s3:PutBucketRequestPayment",
	PutBucketTagging:                            "s3:PutBucketTagging",
	PutBucketVersioning:                         "s3:PutBucketVersioning",
	PutBucketWebsite:                            "s3:PutBucketWebsite",
	PutObject:                                   "s3:PutObject",
	PutObjectAcl:                                "s3:PutObjectAcl",
	PutObjectLegalHold:                          "s3:PutObjectLegalHold",
	PutObjectLockConfiguration:                  "s3:PutObjectLockConfiguration",
	PutObjectRetention:                          "s3:PutObjectRetention",
	PutObjectTagging:                            "s3:PutObjectTagging",
	PutPublicAccessBlock:                        "s3:PutPublicAccessBlock",
	RestoreObject:                               "s3:RestoreObject",
	SelectObjectContent:                         "s3:SelectObjectContent",
	UploadPart:                                  "s3:UploadPart",
	UploadPartCopy:                              "s3:UploadPartCopy",
	WriteGetObjectResponse:                      "s3:WriteGetObjectResponse",
	Unknown:                                     "Unknown",
}

func (a Action) String() string {
	if name, ok := actionNames[a]; ok {
		return name
	}
	return actionNames[Unknown]
}

func ParseAction(name string) Action {
	for action, actionName := range actionNames {
		if actionName == name {
			return action
		}
	}
	return Unknown
}

// OperationType returns the operation classification for this action.
// Used for rate limiting, read-only policies, and auditing.
func (a Action) OperationType() OperationType {
	switch a {
	// List operations (enumeration, typically more expensive)
	case ListBuckets,
		ListObjects,
		ListObjectsV2,
		ListObjectVersions,
		ListMultipartUploads,
		ListParts,
		ListBucketAnalyticsConfigurations,
		ListBucketIntelligentTieringConfigurations,
		ListBucketInventoryConfigurations,
		ListBucketMetricsConfigurations:
		return OpList

	// Write operations (modify state)
	case PutObject,
		PostObject,
		CopyObject,
		DeleteObject,
		DeleteObjects,
		CreateBucket,
		DeleteBucket,
		CreateMultipartUpload,
		CompleteMultipartUpload,
		AbortMultipartUpload,
		UploadPart,
		UploadPartCopy,
		PutBucketAcl,
		PutBucketAccelerateConfiguration,
		PutBucketAnalyticsConfiguration,
		PutBucketCors,
		PutBucketEncryption,
		PutBucketIntelligentTieringConfiguration,
		PutBucketInventoryConfiguration,
		PutBucketLifecycleConfiguration,
		PutBucketLogging,
		PutBucketMetricsConfiguration,
		PutBucketNotificationConfiguration,
		PutBucketOwnershipControls,
		PutBucketPolicy,
		PutBucketReplication,
		PutBucketRequestPayment,
		PutBucketTagging,
		PutBucketVersioning,
		PutBucketWebsite,
		PutObjectAcl,
		PutObjectLegalHold,
		PutObjectLockConfiguration,
		PutObjectRetention,
		PutObjectTagging,
		PutPublicAccessBlock,
		DeleteBucketAnalyticsConfiguration,
		DeleteBucketCors,
		DeleteBucketEncryption,
		DeleteBucketIntelligentTieringConfiguration,
		DeleteBucketInventoryConfiguration,
		DeleteBucketLifecycle,
		DeleteBucketMetricsConfiguration,
		DeleteBucketOwnershipControls,
		DeleteBucketPolicy,
		DeleteBucketReplication,
		DeleteBucketTagging,
		DeleteBucketWebsite,
		DeleteObjectTagging,
		DeletePublicAccessBlock,
		RestoreObject,
		SelectObjectContent,
		WriteGetObjectResponse:
		return OpWrite

	// Read operations (default - does not modify state)
	default:
		return OpRead
	}
}

// ResourceType returns whether this action operates on a bucket, object, or service.
// Useful for IAM policy resource matching.
func (a Action) ResourceType() ResourceType {
	switch a {
	// Service-level operations
	case ListBuckets:
		return ResourceService

	// Object-level operations
	case GetObject,
		HeadObject,
		PutObject,
		PostObject,
		CopyObject,
		DeleteObject,
		DeleteObjects,
		GetObjectAcl,
		PutObjectAcl,
		GetObjectTagging,
		PutObjectTagging,
		DeleteObjectTagging,
		GetObjectAttributes,
		GetObjectLegalHold,
		PutObjectLegalHold,
		GetObjectRetention,
		PutObjectRetention,
		GetObjectTorrent,
		RestoreObject,
		SelectObjectContent,
		CreateMultipartUpload,
		CompleteMultipartUpload,
		AbortMultipartUpload,
		UploadPart,
		UploadPartCopy,
		ListParts,
		WriteGetObjectResponse:
		return ResourceObject

	// Bucket-level operations (default)
	default:
		return ResourceBucket
	}
}

// IsReadOnly returns true if this action only reads data without modification.
// Useful for implementing read-only access policies.
func (a Action) IsReadOnly() bool {
	return a.OperationType() != OpWrite
}

// IsBucketLevel returns true if this action operates at the bucket level.
func (a Action) IsBucketLevel() bool {
	return a.ResourceType() == ResourceBucket
}

// IsObjectLevel returns true if this action operates on objects.
func (a Action) IsObjectLevel() bool {
	return a.ResourceType() == ResourceObject
}

// RequiresObjectKey returns true if this action requires an object key in the request.
func (a Action) RequiresObjectKey() bool {
	return a.ResourceType() == ResourceObject
}

// IsMultipartOperation returns true if this action is part of multipart upload.
func (a Action) IsMultipartOperation() bool {
	switch a {
	case CreateMultipartUpload,
		CompleteMultipartUpload,
		AbortMultipartUpload,
		UploadPart,
		UploadPartCopy,
		ListParts,
		ListMultipartUploads:
		return true
	default:
		return false
	}
}

// IsACLOperation returns true if this action deals with ACLs.
func (a Action) IsACLOperation() bool {
	switch a {
	case GetBucketAcl,
		PutBucketAcl,
		GetObjectAcl,
		PutObjectAcl:
		return true
	default:
		return false
	}
}

// IsPolicyOperation returns true if this action deals with bucket policies.
func (a Action) IsPolicyOperation() bool {
	switch a {
	case GetBucketPolicy,
		PutBucketPolicy,
		DeleteBucketPolicy,
		GetBucketPolicyStatus:
		return true
	default:
		return false
	}
}

// IsTaggingOperation returns true if this action deals with tagging.
func (a Action) IsTaggingOperation() bool {
	switch a {
	case GetBucketTagging,
		PutBucketTagging,
		DeleteBucketTagging,
		GetObjectTagging,
		PutObjectTagging,
		DeleteObjectTagging:
		return true
	default:
		return false
	}
}

// IsVersioningOperation returns true if this action deals with versioning.
func (a Action) IsVersioningOperation() bool {
	switch a {
	case GetBucketVersioning,
		PutBucketVersioning,
		ListObjectVersions:
		return true
	default:
		return false
	}
}

// IsEncryptionOperation returns true if this action deals with encryption settings.
func (a Action) IsEncryptionOperation() bool {
	switch a {
	case GetBucketEncryption,
		PutBucketEncryption,
		DeleteBucketEncryption:
		return true
	default:
		return false
	}
}

// IsReplicationOperation returns true if this action deals with replication.
func (a Action) IsReplicationOperation() bool {
	switch a {
	case GetBucketReplication,
		PutBucketReplication,
		DeleteBucketReplication:
		return true
	default:
		return false
	}
}

// IsLifecycleOperation returns true if this action deals with lifecycle configuration.
func (a Action) IsLifecycleOperation() bool {
	switch a {
	case GetBucketLifecycleConfiguration,
		PutBucketLifecycleConfiguration,
		DeleteBucketLifecycle:
		return true
	default:
		return false
	}
}

// IsObjectLockOperation returns true if this action deals with Object Lock.
func (a Action) IsObjectLockOperation() bool {
	switch a {
	case GetObjectLockConfiguration,
		PutObjectLockConfiguration,
		GetObjectLegalHold,
		PutObjectLegalHold,
		GetObjectRetention,
		PutObjectRetention:
		return true
	default:
		return false
	}
}
