// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

// Package memory provides an in-memory implementation of db.DB for testing.
// This implementation stores data in maps and is suitable for unit tests
// where fast, isolated testing is needed without a real database.
package memory

import (
	"context"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
)

// DB is an in-memory database implementation for testing.
// It stores data in maps and provides basic CRUD operations.
type DB struct {
	mu sync.RWMutex

	buckets           map[string]*types.BucketInfo
	objects           map[string]*types.ObjectRef // key: bucket/key
	uploads           map[string]*types.MultipartUpload
	parts             map[string][]*types.MultipartPart // key: uploadID
	acls              map[string]*s3types.AccessControlList
	policies          map[string]*s3types.BucketPolicy
	cors              map[string]*s3types.CORSConfiguration
	websites          map[string]*s3types.WebsiteConfiguration
	tags              map[string]*s3types.TagSet
	encrypt           map[string]*s3types.ServerSideEncryptionConfig
	lifecycle         map[string]*s3types.Lifecycle
	objLock           map[string]*s3types.ObjectLockConfiguration
	retention         map[string]*s3types.ObjectLockRetention
	legalHold         map[string]*s3types.ObjectLockLegalHold
	publicAccessBlock map[string]*s3types.PublicAccessBlockConfig
	ownershipControls map[string]*s3types.OwnershipControls
	logging           map[string]*db.BucketLoggingConfig
}

// New creates a new in-memory database for testing.
func New() *DB {
	return &DB{
		buckets:           make(map[string]*types.BucketInfo),
		objects:           make(map[string]*types.ObjectRef),
		uploads:           make(map[string]*types.MultipartUpload),
		parts:             make(map[string][]*types.MultipartPart),
		acls:              make(map[string]*s3types.AccessControlList),
		policies:          make(map[string]*s3types.BucketPolicy),
		cors:              make(map[string]*s3types.CORSConfiguration),
		websites:          make(map[string]*s3types.WebsiteConfiguration),
		tags:              make(map[string]*s3types.TagSet),
		encrypt:           make(map[string]*s3types.ServerSideEncryptionConfig),
		lifecycle:         make(map[string]*s3types.Lifecycle),
		objLock:           make(map[string]*s3types.ObjectLockConfiguration),
		retention:         make(map[string]*s3types.ObjectLockRetention),
		legalHold:         make(map[string]*s3types.ObjectLockLegalHold),
		publicAccessBlock: make(map[string]*s3types.PublicAccessBlockConfig),
		ownershipControls: make(map[string]*s3types.OwnershipControls),
		logging:           make(map[string]*db.BucketLoggingConfig),
	}
}

func objectKey(bucket, key string) string {
	return bucket + "/" + key
}

// ============================================================================
// Object Operations
// ============================================================================

func (d *DB) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Generate ID if not set
	if obj.ID == uuid.Nil {
		obj.ID = uuid.New()
	}
	if obj.CreatedAt == 0 {
		obj.CreatedAt = time.Now().UnixNano()
	}

	// Make a copy
	copy := *obj
	d.objects[objectKey(obj.Bucket, obj.Key)] = &copy
	return nil
}

func (d *DB) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	obj, ok := d.objects[objectKey(bucket, key)]
	if !ok {
		return nil, db.ErrObjectNotFound
	}
	// Return a copy (including soft-deleted objects for test verification)
	copy := *obj
	return &copy, nil
}

func (d *DB) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	for _, obj := range d.objects {
		if obj.ID == id {
			copy := *obj
			return &copy, nil
		}
	}
	return nil, db.ErrObjectNotFound
}

func (d *DB) DeleteObject(ctx context.Context, bucket, key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.objects, objectKey(bucket, key))
	return nil
}

func (d *DB) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	k := objectKey(bucket, key)
	obj, ok := d.objects[k]
	if !ok {
		return nil // S3 returns success for non-existent objects
	}
	obj.DeletedAt = deletedAt
	return nil
}

func (d *DB) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	// Use ListObjectsV2 internally for consistency
	result, err := d.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKeys: limit,
	})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (d *DB) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var objects []*types.ObjectRef
	prefixMap := make(map[string]bool)

	// Determine start marker
	startAfter := params.StartAfter
	if params.ContinuationToken != "" {
		startAfter = params.ContinuationToken
	}
	if params.Marker != "" {
		startAfter = params.Marker
	}

	for _, obj := range d.objects {
		if obj.Bucket != params.Bucket {
			continue
		}
		if params.Prefix != "" && !strings.HasPrefix(obj.Key, params.Prefix) {
			continue
		}
		if obj.DeletedAt > 0 {
			continue
		}

		// Handle marker
		if startAfter != "" && obj.Key <= startAfter {
			continue
		}

		// Handle delimiter for common prefixes
		if params.Delimiter != "" {
			suffix := obj.Key[len(params.Prefix):]
			delimIdx := strings.Index(suffix, params.Delimiter)
			if delimIdx >= 0 {
				prefix := params.Prefix + suffix[:delimIdx+1]
				prefixMap[prefix] = true
				continue // Don't include this object, it becomes a common prefix
			}
		}

		copy := *obj
		objects = append(objects, &copy)
	}

	// Sort objects by key
	sort.Slice(objects, func(i, j int) bool {
		return objects[i].Key < objects[j].Key
	})

	// Extract common prefixes
	var commonPrefixes []string
	for prefix := range prefixMap {
		commonPrefixes = append(commonPrefixes, prefix)
	}
	sort.Strings(commonPrefixes)

	// Apply max keys limit
	maxKeys := params.MaxKeys
	if maxKeys == 0 {
		maxKeys = 1000
	}

	isTruncated := len(objects) > maxKeys
	if isTruncated {
		objects = objects[:maxKeys]
	}

	result := &db.ListObjectsResult{
		Objects:        objects,
		CommonPrefixes: commonPrefixes,
		IsTruncated:    isTruncated,
	}

	if isTruncated && len(objects) > 0 {
		lastKey := objects[len(objects)-1].Key
		result.NextMarker = lastKey
		result.NextContinuationToken = lastKey
	}

	return result, nil
}

func (d *DB) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var result []*types.ObjectRef
	for _, obj := range d.objects {
		if obj.DeletedAt > 0 && obj.DeletedAt < olderThan {
			copy := *obj
			result = append(result, &copy)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

// ============================================================================
// Bucket Operations
// ============================================================================

func (d *DB) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if bucket.ID == uuid.Nil {
		bucket.ID = uuid.New()
	}
	copy := *bucket
	d.buckets[bucket.Name] = &copy
	return nil
}

func (d *DB) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	bucket, ok := d.buckets[name]
	if !ok {
		return nil, db.ErrBucketNotFound
	}
	copy := *bucket
	return &copy, nil
}

func (d *DB) DeleteBucket(ctx context.Context, name string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.buckets, name)
	return nil
}

func (d *DB) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var buckets []*types.BucketInfo
	for _, bucket := range d.buckets {
		if params.OwnerID != "" && bucket.OwnerID != params.OwnerID {
			continue
		}
		if params.Prefix != "" && !strings.HasPrefix(bucket.Name, params.Prefix) {
			continue
		}
		if params.BucketRegion != "" && bucket.Region != params.BucketRegion {
			continue
		}
		// Handle continuation token (bucket name to start after)
		if params.ContinuationToken != "" && bucket.Name <= params.ContinuationToken {
			continue
		}
		copy := *bucket
		buckets = append(buckets, &copy)
	}

	// Sort buckets by name
	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	// Apply max buckets limit
	maxBuckets := params.MaxBuckets
	if maxBuckets == 0 {
		maxBuckets = 1000
	}

	isTruncated := len(buckets) > maxBuckets
	if isTruncated {
		buckets = buckets[:maxBuckets]
	}

	result := &db.ListBucketsResult{
		Buckets:     buckets,
		IsTruncated: isTruncated,
	}

	if isTruncated && len(buckets) > 0 {
		result.NextContinuationToken = buckets[len(buckets)-1].Name
	}

	return result, nil
}

func (d *DB) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	b, ok := d.buckets[bucket]
	if !ok {
		return db.ErrBucketNotFound
	}
	b.Versioning = versioning
	return nil
}

// ============================================================================
// Version Operations
// ============================================================================

func (d *DB) ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, versionIDMarker, delimiter string, maxKeys int) ([]*types.ObjectVersion, bool, string, string, error) {
	// Simplified - returns empty for non-versioned buckets
	return nil, false, "", "", nil
}

func (d *DB) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*types.ObjectRef, error) {
	// For non-versioned, return the current object if versionID is null
	if versionID == "" || versionID == "null" {
		return d.GetObject(ctx, bucket, key)
	}
	return nil, db.ErrObjectNotFound
}

func (d *DB) DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error {
	return d.DeleteObject(ctx, bucket, key)
}

func (d *DB) PutDeleteMarker(ctx context.Context, bucket, key, ownerID string) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	k := objectKey(bucket, key)
	obj, ok := d.objects[k]
	if ok {
		obj.DeletedAt = time.Now().UnixNano()
	}
	return uuid.New().String(), nil
}

// ============================================================================
// Multipart Operations
// ============================================================================

func (d *DB) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if upload.UploadID == "" {
		upload.UploadID = uuid.New().String()
	}
	copy := *upload
	d.uploads[upload.UploadID] = &copy
	return nil
}

func (d *DB) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	upload, ok := d.uploads[uploadID]
	if !ok {
		return nil, db.ErrUploadNotFound
	}
	copy := *upload
	return &copy, nil
}

func (d *DB) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.uploads, uploadID)
	delete(d.parts, uploadID)
	return nil
}

func (d *DB) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	var result []*types.MultipartUpload
	for _, upload := range d.uploads {
		if upload.Bucket == bucket {
			copy := *upload
			result = append(result, &copy)
		}
	}
	return result, false, nil
}

func (d *DB) PutPart(ctx context.Context, part *types.MultipartPart) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	// Check if part already exists and replace it
	parts := d.parts[part.UploadID]
	found := false
	for i, p := range parts {
		if p.PartNumber == part.PartNumber {
			copy := *part
			parts[i] = &copy
			found = true
			break
		}
	}
	if !found {
		copy := *part
		d.parts[part.UploadID] = append(d.parts[part.UploadID], &copy)
	}
	return nil
}

func (d *DB) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	parts := d.parts[uploadID]
	for _, part := range parts {
		if part.PartNumber == partNumber {
			copy := *part
			return &copy, nil
		}
	}
	return nil, db.ErrPartNotFound
}

func (d *DB) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	parts := d.parts[uploadID]
	var result []*types.MultipartPart
	for _, part := range parts {
		if part.PartNumber > partNumberMarker {
			copy := *part
			result = append(result, &copy)
		}
	}

	// Sort by part number
	sort.Slice(result, func(i, j int) bool {
		return result[i].PartNumber < result[j].PartNumber
	})

	isTruncated := false
	if maxParts > 0 && len(result) > maxParts {
		result = result[:maxParts]
		isTruncated = true
	}

	return result, isTruncated, nil
}

func (d *DB) DeleteParts(ctx context.Context, uploadID string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.parts, uploadID)
	return nil
}

// ============================================================================
// ACL Operations
// ============================================================================

func (d *DB) GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	acl, ok := d.acls["bucket:"+bucket]
	if !ok {
		return nil, db.ErrACLNotFound
	}
	return acl, nil
}

func (d *DB) SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.acls["bucket:"+bucket] = acl
	return nil
}

func (d *DB) GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	acl, ok := d.acls["object:"+objectKey(bucket, key)]
	if !ok {
		return nil, db.ErrACLNotFound
	}
	return acl, nil
}

func (d *DB) SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.acls["object:"+objectKey(bucket, key)] = acl
	return nil
}

// ============================================================================
// Policy Operations
// ============================================================================

func (d *DB) GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	policy, ok := d.policies[bucket]
	if !ok {
		return nil, db.ErrPolicyNotFound
	}
	return policy, nil
}

func (d *DB) SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.policies[bucket] = policy
	return nil
}

func (d *DB) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.policies, bucket)
	return nil
}

// ============================================================================
// CORS Operations
// ============================================================================

func (d *DB) GetBucketCORS(ctx context.Context, bucket string) (*s3types.CORSConfiguration, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	cors, ok := d.cors[bucket]
	if !ok {
		return nil, db.ErrCORSNotFound
	}
	return cors, nil
}

func (d *DB) SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.cors[bucket] = cors
	return nil
}

func (d *DB) DeleteBucketCORS(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.cors, bucket)
	return nil
}

// ============================================================================
// Website Operations
// ============================================================================

func (d *DB) GetBucketWebsite(ctx context.Context, bucket string) (*s3types.WebsiteConfiguration, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	website, ok := d.websites[bucket]
	if !ok {
		return nil, db.ErrWebsiteNotFound
	}
	return website, nil
}

func (d *DB) SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.websites[bucket] = website
	return nil
}

func (d *DB) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.websites, bucket)
	return nil
}

// ============================================================================
// Tagging Operations
// ============================================================================

func (d *DB) GetBucketTagging(ctx context.Context, bucket string) (*s3types.TagSet, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tags, ok := d.tags["bucket:"+bucket]
	if !ok {
		return nil, db.ErrTaggingNotFound
	}
	return tags, nil
}

func (d *DB) SetBucketTagging(ctx context.Context, bucket string, tagSet *s3types.TagSet) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.tags["bucket:"+bucket] = tagSet
	return nil
}

func (d *DB) DeleteBucketTagging(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.tags, "bucket:"+bucket)
	return nil
}

func (d *DB) GetObjectTagging(ctx context.Context, bucket, key string) (*s3types.TagSet, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	tags, ok := d.tags["object:"+objectKey(bucket, key)]
	if !ok {
		return nil, db.ErrTaggingNotFound
	}
	return tags, nil
}

func (d *DB) SetObjectTagging(ctx context.Context, bucket, key string, tagSet *s3types.TagSet) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.tags["object:"+objectKey(bucket, key)] = tagSet
	return nil
}

func (d *DB) DeleteObjectTagging(ctx context.Context, bucket, key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.tags, "object:"+objectKey(bucket, key))
	return nil
}

// ============================================================================
// Encryption Operations
// ============================================================================

func (d *DB) GetBucketEncryption(ctx context.Context, bucket string) (*s3types.ServerSideEncryptionConfig, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	config, ok := d.encrypt[bucket]
	if !ok {
		return nil, db.ErrEncryptionNotFound
	}
	return config, nil
}

func (d *DB) SetBucketEncryption(ctx context.Context, bucket string, config *s3types.ServerSideEncryptionConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.encrypt[bucket] = config
	return nil
}

func (d *DB) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.encrypt, bucket)
	return nil
}

// ============================================================================
// Lifecycle Operations
// ============================================================================

func (d *DB) GetBucketLifecycle(ctx context.Context, bucket string) (*s3types.Lifecycle, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	lc, ok := d.lifecycle[bucket]
	if !ok {
		return nil, db.ErrLifecycleNotFound
	}
	return lc, nil
}

func (d *DB) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.lifecycle[bucket] = lifecycle
	return nil
}

func (d *DB) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.lifecycle, bucket)
	return nil
}

// ============================================================================
// Object Lock Operations
// ============================================================================

func (d *DB) GetObjectLockConfiguration(ctx context.Context, bucket string) (*s3types.ObjectLockConfiguration, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	config, ok := d.objLock[bucket]
	if !ok {
		return nil, db.ErrObjectLockNotFound
	}
	return config, nil
}

func (d *DB) SetObjectLockConfiguration(ctx context.Context, bucket string, config *s3types.ObjectLockConfiguration) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.objLock[bucket] = config
	return nil
}

func (d *DB) GetObjectRetention(ctx context.Context, bucket, key string) (*s3types.ObjectLockRetention, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	ret, ok := d.retention[objectKey(bucket, key)]
	if !ok {
		return nil, db.ErrRetentionNotFound
	}
	return ret, nil
}

func (d *DB) SetObjectRetention(ctx context.Context, bucket, key string, retention *s3types.ObjectLockRetention) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.retention[objectKey(bucket, key)] = retention
	return nil
}

func (d *DB) GetObjectLegalHold(ctx context.Context, bucket, key string) (*s3types.ObjectLockLegalHold, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	hold, ok := d.legalHold[objectKey(bucket, key)]
	if !ok {
		return nil, db.ErrLegalHoldNotFound
	}
	return hold, nil
}

func (d *DB) SetObjectLegalHold(ctx context.Context, bucket, key string, legalHold *s3types.ObjectLockLegalHold) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.legalHold[objectKey(bucket, key)] = legalHold
	return nil
}

// ============================================================================
// Public Access Block Operations
// ============================================================================

func (d *DB) GetPublicAccessBlock(ctx context.Context, bucket string) (*s3types.PublicAccessBlockConfig, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	config, ok := d.publicAccessBlock[bucket]
	if !ok {
		return nil, db.ErrPublicAccessBlockNotFound
	}
	return config, nil
}

func (d *DB) SetPublicAccessBlock(ctx context.Context, bucket string, config *s3types.PublicAccessBlockConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.publicAccessBlock[bucket] = config
	return nil
}

func (d *DB) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.publicAccessBlock[bucket]; !ok {
		return db.ErrPublicAccessBlockNotFound
	}
	delete(d.publicAccessBlock, bucket)
	return nil
}

// ============================================================================
// Ownership Controls Operations
// ============================================================================

func (d *DB) GetOwnershipControls(ctx context.Context, bucket string) (*s3types.OwnershipControls, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	controls, ok := d.ownershipControls[bucket]
	if !ok {
		return nil, db.ErrOwnershipControlsNotFound
	}
	return controls, nil
}

func (d *DB) SetOwnershipControls(ctx context.Context, bucket string, controls *s3types.OwnershipControls) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.ownershipControls[bucket] = controls
	return nil
}

func (d *DB) DeleteOwnershipControls(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if _, ok := d.ownershipControls[bucket]; !ok {
		return db.ErrOwnershipControlsNotFound
	}
	delete(d.ownershipControls, bucket)
	return nil
}

// ============================================================================
// Logging Configuration
// ============================================================================

func (d *DB) GetBucketLogging(ctx context.Context, bucket string) (*db.BucketLoggingConfig, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	config, ok := d.logging[bucket]
	if !ok {
		return nil, nil
	}
	return config, nil
}

func (d *DB) SetBucketLogging(ctx context.Context, config *db.BucketLoggingConfig) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	if config.TargetBucket == "" {
		delete(d.logging, config.SourceBucket)
		return nil
	}

	if config.ID == "" {
		config.ID = uuid.New().String()
	}
	now := time.Now()
	if config.CreatedAt.IsZero() {
		config.CreatedAt = now
	}
	config.UpdatedAt = now

	d.logging[config.SourceBucket] = config
	return nil
}

func (d *DB) DeleteBucketLogging(ctx context.Context, bucket string) error {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.logging, bucket)
	return nil
}

func (d *DB) ListLoggingConfigs(ctx context.Context) ([]*db.BucketLoggingConfig, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()

	configs := make([]*db.BucketLoggingConfig, 0, len(d.logging))
	for _, config := range d.logging {
		configs = append(configs, config)
	}
	return configs, nil
}

// ============================================================================
// Lifecycle Scan Store (stub for in-memory testing)
// ============================================================================

func (d *DB) GetScanState(ctx context.Context, bucket string) (*db.LifecycleScanState, error) {
	return nil, db.ErrLifecycleScanStateNotFound
}

func (d *DB) UpdateScanState(ctx context.Context, state *db.LifecycleScanState) error {
	return nil
}

func (d *DB) ListBucketsWithLifecycle(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (d *DB) GetBucketsNeedingScan(ctx context.Context, minAge time.Duration, limit int) ([]string, error) {
	return nil, nil
}

func (d *DB) ResetScanState(ctx context.Context, bucket string) error {
	return nil
}

// ============================================================================
// Transaction Support
// ============================================================================

func (d *DB) WithTx(ctx context.Context, fn func(tx db.TxStore) error) error {
	// In-memory DB is already atomic via mutex, just run directly
	return fn(d)
}

// ============================================================================
// Migrations
// ============================================================================

func (d *DB) Migrate(ctx context.Context) error {
	// No-op for in-memory database
	return nil
}

// ============================================================================
// Close
// ============================================================================

func (d *DB) Close() error {
	return nil
}

// Ensure DB implements db.DB interface
var _ db.DB = (*DB)(nil)
