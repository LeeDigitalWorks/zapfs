package config

import (
	"context"

	"zapfs/pkg/s3api/s3types"
)

// Service defines the interface for bucket/object configuration operations.
// This separates business logic from HTTP handling.
type Service interface {
	// Tagging operations
	GetBucketTagging(ctx context.Context, bucket string) (*s3types.TagSet, error)
	SetBucketTagging(ctx context.Context, bucket string, tags *s3types.TagSet) error
	DeleteBucketTagging(ctx context.Context, bucket string) error

	GetObjectTagging(ctx context.Context, bucket, key string) (*GetObjectTaggingResult, error)
	SetObjectTagging(ctx context.Context, bucket, key string, tags *s3types.TagSet) (*SetObjectTaggingResult, error)
	DeleteObjectTagging(ctx context.Context, bucket, key string) (*DeleteObjectTaggingResult, error)

	// ACL operations
	GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error)
	SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error

	GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, error)
	SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error

	// Policy operations
	GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, error)
	SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error
	DeleteBucketPolicy(ctx context.Context, bucket string) error

	// CORS operations
	GetBucketCORS(ctx context.Context, bucket string) (*s3types.CORSConfiguration, error)
	SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error
	DeleteBucketCORS(ctx context.Context, bucket string) error

	// Website operations
	GetBucketWebsite(ctx context.Context, bucket string) (*s3types.WebsiteConfiguration, error)
	SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error
	DeleteBucketWebsite(ctx context.Context, bucket string) error

	// Versioning operations
	GetBucketVersioning(ctx context.Context, bucket string) (*s3types.VersioningConfiguration, error)
	SetBucketVersioning(ctx context.Context, bucket string, versioning *s3types.VersioningConfiguration) error

	// Lifecycle operations
	GetBucketLifecycle(ctx context.Context, bucket string) (*s3types.Lifecycle, error)
	SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error
	DeleteBucketLifecycle(ctx context.Context, bucket string) error

	// Encryption operations
	GetBucketEncryption(ctx context.Context, bucket string) (*s3types.ServerSideEncryptionConfig, error)
	SetBucketEncryption(ctx context.Context, bucket string, encryption *s3types.ServerSideEncryptionConfig) error
	DeleteBucketEncryption(ctx context.Context, bucket string) error

	// Object Lock operations
	GetObjectLockConfiguration(ctx context.Context, bucket string) (*s3types.ObjectLockConfiguration, error)
	SetObjectLockConfiguration(ctx context.Context, bucket string, config *s3types.ObjectLockConfiguration) error

	// Replication operations (enterprise)
	GetBucketReplication(ctx context.Context, bucket string) (*s3types.ReplicationConfiguration, error)
	SetBucketReplication(ctx context.Context, bucket string, replication *s3types.ReplicationConfiguration) error
	DeleteBucketReplication(ctx context.Context, bucket string) error
}
