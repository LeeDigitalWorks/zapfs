// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"context"
	"errors"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// Config holds configuration for the config service
type Config struct {
	DB          db.DB
	BucketStore BucketStore
}

// BucketStore is the interface for bucket existence checks
type BucketStore interface {
	GetBucket(name string) (s3types.Bucket, bool)
	SetBucket(name string, bucket s3types.Bucket)
	GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, bool)
	UpdateBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error
}

// serviceImpl implements the Service interface
type serviceImpl struct {
	db          db.DB
	bucketStore BucketStore
}

// NewService creates a new config service
func NewService(cfg Config) (Service, error) {
	if cfg.DB == nil {
		return nil, errors.New("DB is required")
	}
	if cfg.BucketStore == nil {
		return nil, errors.New("BucketStore is required")
	}

	return &serviceImpl{
		db:          cfg.DB,
		bucketStore: cfg.BucketStore,
	}, nil
}

// ============================================================================
// Bucket Tagging
// ============================================================================

func (s *serviceImpl) GetBucketTagging(ctx context.Context, bucket string) (*s3types.TagSet, error) {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Check cache first
	if bucketInfo.Tagging != nil {
		return bucketInfo.Tagging, nil
	}

	// Cache miss - fetch from DB
	tagSet, err := s.db.GetBucketTagging(ctx, bucket)
	if err != nil {
		if errors.Is(err, db.ErrTaggingNotFound) {
			return nil, &Error{Code: ErrCodeNoSuchTagSet, Message: "no tagging configured"}
		}
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to get tagging", Err: err}
	}

	// Update cache
	bucketInfo.Tagging = tagSet
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return tagSet, nil
}

func (s *serviceImpl) SetBucketTagging(ctx context.Context, bucket string, tags *s3types.TagSet) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Validate tags
	if err := validateTagSet(tags); err != nil {
		return err
	}

	// Write to DB
	if err := s.db.SetBucketTagging(ctx, bucket, tags); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to set tagging", Err: err}
	}

	// Update cache
	bucketInfo.Tagging = tags
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

func (s *serviceImpl) DeleteBucketTagging(ctx context.Context, bucket string) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Delete from DB
	if err := s.db.DeleteBucketTagging(ctx, bucket); err != nil && !errors.Is(err, db.ErrTaggingNotFound) {
		return &Error{Code: ErrCodeInternalError, Message: "failed to delete tagging", Err: err}
	}

	// Update cache
	bucketInfo.Tagging = nil
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

// ============================================================================
// Object Tagging
// ============================================================================

func (s *serviceImpl) GetObjectTagging(ctx context.Context, bucket, key string) (*GetObjectTaggingResult, error) {
	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}
	if objRef.IsDeleted() {
		return nil, &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}

	tagSet, err := s.db.GetObjectTagging(ctx, bucket, key)
	if err != nil {
		if errors.Is(err, db.ErrTaggingNotFound) {
			// Return empty tag set for objects without tags
			return &GetObjectTaggingResult{
				VersionID: objRef.ID.String(),
				Tags:      &s3types.TagSet{Tags: []s3types.Tag{}},
			}, nil
		}
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to get tagging", Err: err}
	}

	return &GetObjectTaggingResult{
		VersionID: objRef.ID.String(),
		Tags:      tagSet,
	}, nil
}

func (s *serviceImpl) SetObjectTagging(ctx context.Context, bucket, key string, tags *s3types.TagSet) (*SetObjectTaggingResult, error) {
	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}
	if objRef.IsDeleted() {
		return nil, &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}

	// Validate tags
	if err := validateTagSet(tags); err != nil {
		return nil, err
	}

	if err := s.db.SetObjectTagging(ctx, bucket, key, tags); err != nil {
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to set tagging", Err: err}
	}

	return &SetObjectTaggingResult{
		VersionID: objRef.ID.String(),
	}, nil
}

func (s *serviceImpl) DeleteObjectTagging(ctx context.Context, bucket, key string) (*DeleteObjectTaggingResult, error) {
	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}
	if objRef.IsDeleted() {
		return nil, &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}

	if err := s.db.DeleteObjectTagging(ctx, bucket, key); err != nil && !errors.Is(err, db.ErrTaggingNotFound) {
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to delete tagging", Err: err}
	}

	return &DeleteObjectTaggingResult{
		VersionID: objRef.ID.String(),
	}, nil
}

// ============================================================================
// Bucket ACL
// ============================================================================

func (s *serviceImpl) GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error) {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	if bucketInfo.ACL != nil {
		return bucketInfo.ACL, nil
	}

	// Return default private ACL
	return s3types.NewPrivateACL(bucketInfo.OwnerID, bucketInfo.OwnerID), nil
}

func (s *serviceImpl) SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Update cache
	bucketInfo.ACL = acl
	s.bucketStore.SetBucket(bucket, bucketInfo)

	// Store in DB
	if err := s.db.SetBucketACL(ctx, bucket, acl); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to set ACL", Err: err}
	}

	return nil
}

// ============================================================================
// Object ACL
// ============================================================================

func (s *serviceImpl) GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, error) {
	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		return nil, &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}
	if objRef.IsDeleted() {
		return nil, &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}

	acl, err := s.db.GetObjectACL(ctx, bucket, key)
	if err != nil || acl == nil {
		// Return default private ACL using bucket owner
		bucketInfo, exists := s.bucketStore.GetBucket(bucket)
		ownerID := ""
		if exists {
			ownerID = bucketInfo.OwnerID
		}
		return s3types.NewPrivateACL(ownerID, ownerID), nil
	}

	return acl, nil
}

func (s *serviceImpl) SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error {
	// Verify object exists
	objRef, err := s.db.GetObject(ctx, bucket, key)
	if err != nil {
		return &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}
	if objRef.IsDeleted() {
		return &Error{Code: ErrCodeNoSuchKey, Message: "object not found"}
	}

	if err := s.db.SetObjectACL(ctx, bucket, key, acl); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to set ACL", Err: err}
	}

	return nil
}

// ============================================================================
// Bucket Policy
// ============================================================================

func (s *serviceImpl) GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, error) {
	policy, exists := s.bucketStore.GetBucketPolicy(ctx, bucket)
	if !exists {
		// Check if bucket exists
		if _, bucketExists := s.bucketStore.GetBucket(bucket); !bucketExists {
			return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
		}
		return nil, &Error{Code: ErrCodeNoSuchBucketPolicy, Message: "no policy configured"}
	}

	// Bucket exists but may have nil policy (e.g., after deletion)
	if policy == nil {
		return nil, &Error{Code: ErrCodeNoSuchBucketPolicy, Message: "no policy configured"}
	}

	return policy, nil
}

func (s *serviceImpl) SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error {
	// Verify bucket exists
	if _, exists := s.bucketStore.GetBucket(bucket); !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Validate policy
	if err := validatePolicy(policy); err != nil {
		return err
	}

	// Store in DB
	if err := s.db.SetBucketPolicy(ctx, bucket, policy); err != nil {
		// Log the underlying error for debugging
		logger.Error().Err(err).Str("bucket", bucket).Msg("SetBucketPolicy DB error")
		return &Error{Code: ErrCodeInternalError, Message: "failed to set policy", Err: err}
	}

	// Update cache (non-fatal if fails)
	_ = s.bucketStore.UpdateBucketPolicy(ctx, bucket, policy)

	return nil
}

func (s *serviceImpl) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	// Verify bucket exists
	if _, exists := s.bucketStore.GetBucket(bucket); !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	if err := s.db.DeleteBucketPolicy(ctx, bucket); err != nil && !errors.Is(err, db.ErrPolicyNotFound) {
		return &Error{Code: ErrCodeInternalError, Message: "failed to delete policy", Err: err}
	}

	// Update cache (non-fatal if fails)
	_ = s.bucketStore.UpdateBucketPolicy(ctx, bucket, nil)

	return nil
}

// ============================================================================
// CORS
// ============================================================================

func (s *serviceImpl) GetBucketCORS(ctx context.Context, bucket string) (*s3types.CORSConfiguration, error) {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Check cache first
	if bucketInfo.CORS != nil {
		return bucketInfo.CORS, nil
	}

	// Cache miss - fetch from DB
	cors, err := s.db.GetBucketCORS(ctx, bucket)
	if err != nil {
		if errors.Is(err, db.ErrCORSNotFound) {
			return nil, &Error{Code: ErrCodeNoSuchCORSConfiguration, Message: "no CORS configuration"}
		}
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to get CORS", Err: err}
	}

	// Update cache
	bucketInfo.CORS = cors
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return cors, nil
}

func (s *serviceImpl) SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Validate CORS
	if err := validateCORS(cors); err != nil {
		return err
	}

	// Write to DB
	if err := s.db.SetBucketCORS(ctx, bucket, cors); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to set CORS", Err: err}
	}

	// Update cache
	bucketInfo.CORS = cors
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

func (s *serviceImpl) DeleteBucketCORS(ctx context.Context, bucket string) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Delete from DB
	if err := s.db.DeleteBucketCORS(ctx, bucket); err != nil && !errors.Is(err, db.ErrCORSNotFound) {
		return &Error{Code: ErrCodeInternalError, Message: "failed to delete CORS", Err: err}
	}

	// Update cache
	bucketInfo.CORS = nil
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

// ============================================================================
// Website
// ============================================================================

func (s *serviceImpl) GetBucketWebsite(ctx context.Context, bucket string) (*s3types.WebsiteConfiguration, error) {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Check cache first
	if bucketInfo.Website != nil {
		return bucketInfo.Website, nil
	}

	// Cache miss - fetch from DB
	website, err := s.db.GetBucketWebsite(ctx, bucket)
	if err != nil {
		if errors.Is(err, db.ErrWebsiteNotFound) {
			return nil, &Error{Code: ErrCodeNoSuchWebsiteConfiguration, Message: "no website configuration"}
		}
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to get website", Err: err}
	}

	// Update cache
	bucketInfo.Website = website
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return website, nil
}

func (s *serviceImpl) SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Validate website configuration
	if err := validateWebsite(website); err != nil {
		return err
	}

	// Write to DB
	if err := s.db.SetBucketWebsite(ctx, bucket, website); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to set website", Err: err}
	}

	// Update cache
	bucketInfo.Website = website
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

func (s *serviceImpl) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Delete from DB
	if err := s.db.DeleteBucketWebsite(ctx, bucket); err != nil && !errors.Is(err, db.ErrWebsiteNotFound) {
		return &Error{Code: ErrCodeInternalError, Message: "failed to delete website", Err: err}
	}

	// Update cache
	bucketInfo.Website = nil
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

// ============================================================================
// Versioning
// ============================================================================

func (s *serviceImpl) GetBucketVersioning(ctx context.Context, bucket string) (*s3types.VersioningConfiguration, error) {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	result := &s3types.VersioningConfiguration{
		Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/",
	}

	// Only include Status if versioning was ever enabled
	if bucketInfo.Versioning != "" && bucketInfo.Versioning != s3types.VersioningDisabled {
		result.Status = string(bucketInfo.Versioning)
	}

	return result, nil
}

func (s *serviceImpl) SetBucketVersioning(ctx context.Context, bucket string, versioning *s3types.VersioningConfiguration) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Validate
	if versioning.Status != "" && versioning.Status != "Enabled" && versioning.Status != "Suspended" {
		return &Error{Code: ErrCodeMalformedXML, Message: "invalid versioning status"}
	}

	// Update DB
	if err := s.db.UpdateBucketVersioning(ctx, bucket, versioning.Status); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to update versioning", Err: err}
	}

	// Update cache
	bucketInfo.Versioning = s3types.Versioning(versioning.Status)
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

// ============================================================================
// Lifecycle
// ============================================================================

func (s *serviceImpl) GetBucketLifecycle(ctx context.Context, bucket string) (*s3types.Lifecycle, error) {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Check cache first
	if bucketInfo.LifecyclePolicy != nil {
		return bucketInfo.LifecyclePolicy, nil
	}

	// Cache miss - fetch from DB
	lifecycle, err := s.db.GetBucketLifecycle(ctx, bucket)
	if err != nil {
		if errors.Is(err, db.ErrLifecycleNotFound) {
			return nil, &Error{Code: ErrCodeNoSuchLifecycleConfiguration, Message: "no lifecycle configuration"}
		}
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to get lifecycle", Err: err}
	}

	// Update cache
	bucketInfo.LifecyclePolicy = lifecycle
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return lifecycle, nil
}

func (s *serviceImpl) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Validate lifecycle
	if err := validateLifecycle(lifecycle); err != nil {
		return err
	}

	// Write to DB
	if err := s.db.SetBucketLifecycle(ctx, bucket, lifecycle); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to set lifecycle", Err: err}
	}

	// Update cache
	bucketInfo.LifecyclePolicy = lifecycle
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

func (s *serviceImpl) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Delete from DB
	if err := s.db.DeleteBucketLifecycle(ctx, bucket); err != nil && !errors.Is(err, db.ErrLifecycleNotFound) {
		return &Error{Code: ErrCodeInternalError, Message: "failed to delete lifecycle", Err: err}
	}

	// Update cache
	bucketInfo.LifecyclePolicy = nil
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

// ============================================================================
// Encryption
// ============================================================================

func (s *serviceImpl) GetBucketEncryption(ctx context.Context, bucket string) (*s3types.ServerSideEncryptionConfig, error) {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Check cache first
	if bucketInfo.Encryption != nil {
		return bucketInfo.Encryption, nil
	}

	// Cache miss - fetch from DB
	config, err := s.db.GetBucketEncryption(ctx, bucket)
	if err != nil {
		if errors.Is(err, db.ErrEncryptionNotFound) {
			return nil, &Error{Code: ErrCodeServerSideEncryptionConfigurationNotFound, Message: "no encryption configuration"}
		}
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to get encryption", Err: err}
	}

	// Update cache
	bucketInfo.Encryption = config
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return config, nil
}

func (s *serviceImpl) SetBucketEncryption(ctx context.Context, bucket string, encryption *s3types.ServerSideEncryptionConfig) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Validate encryption config
	if err := validateEncryption(encryption); err != nil {
		return err
	}

	if err := s.db.SetBucketEncryption(ctx, bucket, encryption); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to set encryption", Err: err}
	}

	// Update cache
	bucketInfo.Encryption = encryption
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

func (s *serviceImpl) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Delete from DB
	if err := s.db.DeleteBucketEncryption(ctx, bucket); err != nil && !errors.Is(err, db.ErrEncryptionNotFound) {
		return &Error{Code: ErrCodeInternalError, Message: "failed to delete encryption", Err: err}
	}

	// Update cache
	bucketInfo.Encryption = nil
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

// ============================================================================
// Object Lock
// ============================================================================

func (s *serviceImpl) GetObjectLockConfiguration(ctx context.Context, bucket string) (*s3types.ObjectLockConfiguration, error) {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Check cache first
	if bucketInfo.ObjectLockConfig != nil {
		return bucketInfo.ObjectLockConfig, nil
	}

	// Cache miss - fetch from DB
	config, err := s.db.GetObjectLockConfiguration(ctx, bucket)
	if err != nil {
		if errors.Is(err, db.ErrObjectLockNotFound) {
			return nil, &Error{Code: ErrCodeObjectLockConfigurationNotFound, Message: "no object lock configuration"}
		}
		return nil, &Error{Code: ErrCodeInternalError, Message: "failed to get object lock config", Err: err}
	}

	// Update cache
	bucketInfo.ObjectLockConfig = config
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return config, nil
}

func (s *serviceImpl) SetObjectLockConfiguration(ctx context.Context, bucket string, config *s3types.ObjectLockConfiguration) error {
	bucketInfo, exists := s.bucketStore.GetBucket(bucket)
	if !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Validate object lock config
	if err := validateObjectLockConfig(config); err != nil {
		return err
	}

	if err := s.db.SetObjectLockConfiguration(ctx, bucket, config); err != nil {
		return &Error{Code: ErrCodeInternalError, Message: "failed to set object lock config", Err: err}
	}

	// Update cache
	bucketInfo.ObjectLockConfig = config
	s.bucketStore.SetBucket(bucket, bucketInfo)

	return nil
}

// ============================================================================
// Replication (Enterprise)
// ============================================================================

func (s *serviceImpl) GetBucketReplication(ctx context.Context, bucket string) (*s3types.ReplicationConfiguration, error) {
	// Verify bucket exists
	if _, exists := s.bucketStore.GetBucket(bucket); !exists {
		return nil, &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Replication is enterprise-only, return not found for open source
	return nil, &Error{Code: ErrCodeReplicationConfigurationNotFound, Message: "replication not configured"}
}

func (s *serviceImpl) SetBucketReplication(ctx context.Context, bucket string, replication *s3types.ReplicationConfiguration) error {
	// Verify bucket exists
	if _, exists := s.bucketStore.GetBucket(bucket); !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Replication is enterprise-only
	return &Error{Code: ErrCodeInternalError, Message: "replication requires enterprise edition"}
}

func (s *serviceImpl) DeleteBucketReplication(ctx context.Context, bucket string) error {
	// Verify bucket exists
	if _, exists := s.bucketStore.GetBucket(bucket); !exists {
		return &Error{Code: ErrCodeNoSuchBucket, Message: "bucket not found"}
	}

	// Replication is enterprise-only, return success for delete (idempotent)
	return nil
}

// ============================================================================
// Validation Helpers
// ============================================================================

func validateTagSet(tags *s3types.TagSet) error {
	if tags == nil {
		return &Error{Code: ErrCodeMalformedXML, Message: "empty tag set"}
	}

	// Maximum 50 tags
	if len(tags.Tags) > 50 {
		return &Error{Code: ErrCodeInvalidTag, Message: "too many tags (max 50)"}
	}

	seen := make(map[string]bool)
	for _, tag := range tags.Tags {
		// Key is required
		if tag.Key == "" {
			return &Error{Code: ErrCodeInvalidTag, Message: "tag key is required"}
		}

		// Key length: 1-128 characters
		if len(tag.Key) > 128 {
			return &Error{Code: ErrCodeInvalidTag, Message: "tag key too long (max 128)"}
		}

		// Value length: 0-256 characters
		if len(tag.Value) > 256 {
			return &Error{Code: ErrCodeInvalidTag, Message: "tag value too long (max 256)"}
		}

		// Keys must be unique
		if seen[tag.Key] {
			return &Error{Code: ErrCodeInvalidTag, Message: "duplicate tag key"}
		}
		seen[tag.Key] = true
	}

	return nil
}

func validatePolicy(policy *s3types.BucketPolicy) error {
	if policy == nil {
		return &Error{Code: ErrCodeMalformedPolicy, Message: "empty policy"}
	}

	// Validate version
	if policy.Version != "" && policy.Version != "2012-10-17" && policy.Version != "2008-10-17" {
		return &Error{Code: ErrCodeMalformedPolicy, Message: "invalid policy version"}
	}

	// Must have statements
	if len(policy.Statements) == 0 {
		return &Error{Code: ErrCodeMalformedPolicy, Message: "policy must have statements"}
	}

	for _, stmt := range policy.Statements {
		// Must have Effect
		if stmt.Effect != s3types.EffectAllow && stmt.Effect != s3types.EffectDeny {
			return &Error{Code: ErrCodeMalformedPolicy, Message: "statement must have valid Effect"}
		}

		// Must have Action or NotAction
		if len(stmt.Actions) == 0 && len(stmt.NotActions) == 0 {
			return &Error{Code: ErrCodeMalformedPolicy, Message: "statement must have Action or NotAction"}
		}

		// Must have Resource or NotResource
		if len(stmt.Resources) == 0 && len(stmt.NotResources) == 0 {
			return &Error{Code: ErrCodeMalformedPolicy, Message: "statement must have Resource or NotResource"}
		}
	}

	return nil
}

func validateCORS(cors *s3types.CORSConfiguration) error {
	if cors == nil || len(cors.Rules) == 0 {
		return &Error{Code: ErrCodeMalformedXML, Message: "empty CORS configuration"}
	}

	for _, rule := range cors.Rules {
		if len(rule.AllowedOrigins) == 0 {
			return &Error{Code: ErrCodeMalformedXML, Message: "CORS rule must have AllowedOrigin"}
		}
		if len(rule.AllowedMethods) == 0 {
			return &Error{Code: ErrCodeMalformedXML, Message: "CORS rule must have AllowedMethod"}
		}

		// Validate methods
		for _, method := range rule.AllowedMethods {
			switch method {
			case "GET", "PUT", "POST", "DELETE", "HEAD":
				// valid
			default:
				return &Error{Code: ErrCodeMalformedXML, Message: "invalid CORS method: " + method}
			}
		}
	}

	return nil
}

func validateWebsite(website *s3types.WebsiteConfiguration) error {
	if website == nil {
		return &Error{Code: ErrCodeMalformedXML, Message: "empty website configuration"}
	}

	hasIndex := website.IndexDocument != nil && website.IndexDocument.Suffix != ""
	hasRedirect := website.RedirectAllRequestsTo != nil && website.RedirectAllRequestsTo.HostName != ""

	// Must have either IndexDocument OR RedirectAllRequestsTo
	if !hasIndex && !hasRedirect {
		return &Error{Code: ErrCodeMalformedXML, Message: "website must have IndexDocument or RedirectAllRequestsTo"}
	}

	// Can't have both RedirectAllRequestsTo and other elements
	if hasRedirect && (website.IndexDocument != nil || website.ErrorDocument != nil || website.RoutingRules != nil) {
		return &Error{Code: ErrCodeMalformedXML, Message: "RedirectAllRequestsTo cannot be combined with other elements"}
	}

	// Validate routing rules if present
	if website.RoutingRules != nil {
		for _, rule := range website.RoutingRules.Rules {
			if rule.Redirect == nil {
				return &Error{Code: ErrCodeMalformedXML, Message: "routing rule must have Redirect"}
			}
		}
	}

	return nil
}

func validateLifecycle(lifecycle *s3types.Lifecycle) error {
	if lifecycle == nil || len(lifecycle.Rules) == 0 {
		return &Error{Code: ErrCodeMalformedXML, Message: "empty lifecycle configuration"}
	}

	ruleIDs := make(map[string]bool)
	for _, rule := range lifecycle.Rules {
		// Status must be Enabled or Disabled
		if rule.Status != s3types.LifecycleStatusEnabled && rule.Status != s3types.LifecycleStatusDisabled {
			return &Error{Code: ErrCodeMalformedXML, Message: "rule status must be Enabled or Disabled"}
		}

		// Rule ID must be unique if present
		if rule.ID != nil && *rule.ID != "" {
			if ruleIDs[*rule.ID] {
				return &Error{Code: ErrCodeMalformedXML, Message: "duplicate rule ID"}
			}
			ruleIDs[*rule.ID] = true
		}

		// Must have at least one action
		hasAction := rule.Expiration != nil ||
			rule.Transitions != nil ||
			rule.AbortIncompleteMultipartUpload != nil ||
			rule.NoncurrentVersionExpiration != nil ||
			rule.NoncurrentVersionTransition != nil

		if !hasAction {
			return &Error{Code: ErrCodeMalformedXML, Message: "lifecycle rule must have at least one action"}
		}
	}

	return nil
}

func validateEncryption(config *s3types.ServerSideEncryptionConfig) error {
	if config == nil || len(config.Rules) == 0 {
		return &Error{Code: ErrCodeMalformedXML, Message: "empty encryption configuration"}
	}

	for _, rule := range config.Rules {
		if rule.ApplyServerSideEncryptionByDefault == nil {
			return &Error{Code: ErrCodeMalformedXML, Message: "rule must have ApplyServerSideEncryptionByDefault"}
		}

		algo := rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm
		if algo != "AES256" && algo != "aws:kms" {
			return &Error{Code: ErrCodeMalformedXML, Message: "invalid encryption algorithm"}
		}
	}

	return nil
}

func validateObjectLockConfig(config *s3types.ObjectLockConfiguration) error {
	if config == nil {
		return &Error{Code: ErrCodeMalformedXML, Message: "empty object lock configuration"}
	}

	if config.ObjectLockEnabled != "Enabled" {
		return &Error{Code: ErrCodeMalformedXML, Message: "ObjectLockEnabled must be Enabled"}
	}

	// Validate rule if present
	if config.Rule != nil && config.Rule.DefaultRetention != nil {
		ret := config.Rule.DefaultRetention

		// Mode must be GOVERNANCE or COMPLIANCE
		if ret.Mode != "GOVERNANCE" && ret.Mode != "COMPLIANCE" {
			return &Error{Code: ErrCodeMalformedXML, Message: "retention mode must be GOVERNANCE or COMPLIANCE"}
		}

		// Must have Days or Years (not both)
		if (ret.Days == 0 && ret.Years == 0) || (ret.Days > 0 && ret.Years > 0) {
			return &Error{Code: ErrCodeMalformedXML, Message: "retention must have Days or Years (not both)"}
		}
	}

	return nil
}
