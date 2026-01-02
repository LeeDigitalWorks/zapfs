package db

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
)

// Common errors
var (
	ErrObjectNotFound = fmt.Errorf("object not found")
	ErrBucketNotFound = fmt.Errorf("bucket not found")
)

// Driver identifies a database driver type
type Driver string

const (
	DriverVitess    Driver = "vitess"
	DriverMySQL     Driver = "mysql"
	DriverPostgres  Driver = "postgres"
	DriverCockroach Driver = "cockroachdb"
)

// Config holds database configuration
type Config struct {
	// Driver specifies the database backend (memory, vitess, mysql, postgres, cockroachdb)
	Driver Driver

	// DSN is the data source name for SQL databases
	// Format varies by driver:
	//   - vitess/mysql: "user:pass@tcp(host:port)/database"
	//   - postgres: "postgres://user:pass@host:port/database?sslmode=disable"
	//   - cockroachdb: "postgres://user:pass@host:port/database?sslmode=disable"
	DSN string

	// Connection pool settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime int // seconds
	ConnMaxIdleTime int // seconds
}

// DefaultConfig returns a Config with sensible defaults for the given driver
func DefaultConfig(driver Driver) Config {
	return Config{
		Driver:          driver,
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 300, // 5 minutes
		ConnMaxIdleTime: 60,  // 1 minute
	}
}

// DB is the main database interface for the metadata service
type DB interface {
	// Object operations
	ObjectStore

	// Bucket operations
	BucketStore

	// Multipart upload operations
	MultipartStore

	// Version operations
	VersionStore

	// ACL operations
	ACLStore

	// Policy operations
	PolicyStore

	// CORS operations
	CORSStore

	// Website operations
	WebsiteStore

	// Tagging operations
	TaggingStore

	// Encryption operations
	EncryptionStore

	// Lifecycle operations
	LifecycleStore

	// Lifecycle scanner state
	LifecycleScanStore

	// Object Lock operations
	ObjectLockStore

	// Public Access Block operations
	PublicAccessBlockStore

	// Ownership Controls operations
	OwnershipControlsStore

	// Bucket Logging operations (access log configuration)
	LoggingStore

	// Transaction support - executes fn within a transaction.
	// If fn returns an error, the transaction is rolled back.
	// If fn returns nil, the transaction is committed.
	WithTx(ctx context.Context, fn func(tx TxStore) error) error

	// Migrations
	Migrate(ctx context.Context) error

	// Close closes the database connection
	Close() error
}

// TxStore provides transactional access to object and bucket stores.
// All operations within a transaction are atomic.
type TxStore interface {
	ObjectStore
	BucketStore
	MultipartStore
}

// ObjectStore provides CRUD operations for object metadata
type ObjectStore interface {
	// PutObject stores or updates object metadata
	PutObject(ctx context.Context, obj *types.ObjectRef) error

	// GetObject retrieves object metadata by bucket and key
	GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error)

	// GetObjectByID retrieves object metadata by ID
	GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error)

	// DeleteObject removes object metadata (hard delete)
	DeleteObject(ctx context.Context, bucket, key string) error

	// MarkObjectDeleted sets the deleted timestamp (soft delete)
	MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error

	// ListObjects returns objects in a bucket with optional prefix filter.
	// This is a convenience method that wraps ListObjectsV2 for simple use cases.
	// For full pagination support, use ListObjectsV2 directly.
	ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error)

	// ListObjectsV2 returns objects with full pagination support
	ListObjectsV2(ctx context.Context, params *ListObjectsParams) (*ListObjectsResult, error)

	// ListDeletedObjects returns soft-deleted objects for GC
	ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error)
}

// ListObjectsParams contains parameters for ListObjectsV2
type ListObjectsParams struct {
	Bucket            string
	Prefix            string
	Delimiter         string
	MaxKeys           int
	Marker            string // For ListObjects v1
	ContinuationToken string // For ListObjects v2
	StartAfter        string // For ListObjects v2
	FetchOwner        bool
}

// ListObjectsResult contains the result of ListObjectsV2
type ListObjectsResult struct {
	Objects               []*types.ObjectRef
	CommonPrefixes        []string // Aggregated prefixes when delimiter is used
	IsTruncated           bool
	NextMarker            string // For v1
	NextContinuationToken string // For v2
}

// ListBucketsParams contains parameters for ListBuckets
type ListBucketsParams struct {
	OwnerID           string
	Prefix            string // Filter by bucket name prefix
	BucketRegion      string // Filter by bucket region
	MaxBuckets        int    // Pagination limit (1-10000)
	ContinuationToken string // Pagination token
}

// ListBucketsResult contains the result of ListBuckets
type ListBucketsResult struct {
	Buckets               []*types.BucketInfo
	IsTruncated           bool
	NextContinuationToken string
}

// BucketStore provides CRUD operations for bucket metadata
type BucketStore interface {
	// CreateBucket creates a new bucket
	CreateBucket(ctx context.Context, bucket *types.BucketInfo) error

	// GetBucket retrieves bucket metadata by name
	GetBucket(ctx context.Context, name string) (*types.BucketInfo, error)

	// DeleteBucket removes a bucket (must be empty)
	DeleteBucket(ctx context.Context, name string) error

	// ListBuckets returns buckets with full pagination support
	ListBuckets(ctx context.Context, params *ListBucketsParams) (*ListBucketsResult, error)

	// UpdateBucketVersioning updates the versioning state of a bucket
	UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error
}

// VersionStore provides operations for object versioning
type VersionStore interface {
	// ListObjectVersions lists all versions of objects in a bucket
	// Returns: versions, isTruncated, nextKeyMarker, nextVersionIDMarker, error
	ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, versionIDMarker, delimiter string, maxKeys int) ([]*types.ObjectVersion, bool, string, string, error)

	// GetObjectVersion retrieves a specific version of an object
	GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*types.ObjectRef, error)

	// DeleteObjectVersion permanently deletes a specific version
	DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error

	// PutDeleteMarker creates a delete marker for an object (versioning enabled)
	PutDeleteMarker(ctx context.Context, bucket, key, ownerID string) (versionID string, err error)
}

// MultipartStore provides operations for multipart uploads
type MultipartStore interface {
	// CreateMultipartUpload creates a new multipart upload
	CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error

	// GetMultipartUpload retrieves a multipart upload by upload ID
	GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error)

	// DeleteMultipartUpload removes a multipart upload and all its parts
	DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error

	// ListMultipartUploads lists in-progress uploads for a bucket
	ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error)

	// PutPart stores a part of a multipart upload
	PutPart(ctx context.Context, part *types.MultipartPart) error

	// GetPart retrieves a specific part
	GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error)

	// ListParts lists all parts for an upload
	ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error)

	// DeleteParts removes all parts for an upload
	DeleteParts(ctx context.Context, uploadID string) error
}

// Common multipart errors
var (
	ErrUploadNotFound = fmt.Errorf("multipart upload not found")
	ErrPartNotFound   = fmt.Errorf("part not found")
)

// ACLStore provides operations for access control lists
type ACLStore interface {
	// GetBucketACL retrieves the ACL for a bucket
	GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error)

	// SetBucketACL stores the ACL for a bucket
	SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error

	// GetObjectACL retrieves the ACL for an object
	GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, error)

	// SetObjectACL stores the ACL for an object
	SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error
}

// PolicyStore provides operations for bucket policies
type PolicyStore interface {
	// GetBucketPolicy retrieves the policy for a bucket
	GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, error)

	// SetBucketPolicy stores the policy for a bucket
	SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error

	// DeleteBucketPolicy removes the policy for a bucket
	DeleteBucketPolicy(ctx context.Context, bucket string) error
}

// Common ACL/Policy errors
var (
	ErrACLNotFound    = fmt.Errorf("ACL not found")
	ErrPolicyNotFound = fmt.Errorf("policy not found")
)

// CORSStore provides operations for CORS configurations
type CORSStore interface {
	// GetBucketCORS retrieves the CORS configuration for a bucket
	GetBucketCORS(ctx context.Context, bucket string) (*s3types.CORSConfiguration, error)

	// SetBucketCORS stores the CORS configuration for a bucket
	SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error

	// DeleteBucketCORS removes the CORS configuration for a bucket
	DeleteBucketCORS(ctx context.Context, bucket string) error
}

// WebsiteStore provides operations for website configurations
type WebsiteStore interface {
	// GetBucketWebsite retrieves the website configuration for a bucket
	GetBucketWebsite(ctx context.Context, bucket string) (*s3types.WebsiteConfiguration, error)

	// SetBucketWebsite stores the website configuration for a bucket
	SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error

	// DeleteBucketWebsite removes the website configuration for a bucket
	DeleteBucketWebsite(ctx context.Context, bucket string) error
}

// Common CORS/Website errors
var (
	ErrCORSNotFound    = fmt.Errorf("CORS configuration not found")
	ErrWebsiteNotFound = fmt.Errorf("website configuration not found")
)

// TaggingStore provides operations for resource tagging
type TaggingStore interface {
	// GetBucketTagging retrieves the tag set for a bucket
	GetBucketTagging(ctx context.Context, bucket string) (*s3types.TagSet, error)

	// SetBucketTagging stores the tag set for a bucket
	SetBucketTagging(ctx context.Context, bucket string, tagSet *s3types.TagSet) error

	// DeleteBucketTagging removes the tag set for a bucket
	DeleteBucketTagging(ctx context.Context, bucket string) error

	// GetObjectTagging retrieves the tag set for an object
	GetObjectTagging(ctx context.Context, bucket, key string) (*s3types.TagSet, error)

	// SetObjectTagging stores the tag set for an object
	SetObjectTagging(ctx context.Context, bucket, key string, tagSet *s3types.TagSet) error

	// DeleteObjectTagging removes the tag set for an object
	DeleteObjectTagging(ctx context.Context, bucket, key string) error
}

// EncryptionStore provides operations for bucket encryption configurations
type EncryptionStore interface {
	// GetBucketEncryption retrieves the encryption config for a bucket
	GetBucketEncryption(ctx context.Context, bucket string) (*s3types.ServerSideEncryptionConfig, error)

	// SetBucketEncryption stores the encryption config for a bucket
	SetBucketEncryption(ctx context.Context, bucket string, config *s3types.ServerSideEncryptionConfig) error

	// DeleteBucketEncryption removes the encryption config for a bucket
	DeleteBucketEncryption(ctx context.Context, bucket string) error
}

// Common Tagging/Encryption errors
var (
	ErrTaggingNotFound    = fmt.Errorf("tagging not found")
	ErrEncryptionNotFound = fmt.Errorf("encryption configuration not found")
)

// LifecycleStore provides operations for lifecycle configurations
type LifecycleStore interface {
	// GetBucketLifecycle retrieves the lifecycle config for a bucket
	GetBucketLifecycle(ctx context.Context, bucket string) (*s3types.Lifecycle, error)

	// SetBucketLifecycle stores the lifecycle config for a bucket
	SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error

	// DeleteBucketLifecycle removes the lifecycle config for a bucket
	DeleteBucketLifecycle(ctx context.Context, bucket string) error
}

// LifecycleScanState tracks scan progress for a bucket
type LifecycleScanState struct {
	Bucket            string
	LastKey           string
	LastVersionID     string
	ScanStartedAt     time.Time
	ScanCompletedAt   time.Time
	ObjectsScanned    int
	ActionsEnqueued   int
	LastError         string
	ConsecutiveErrors int
}

// LifecycleScanStore tracks lifecycle scanner progress
type LifecycleScanStore interface {
	// GetScanState retrieves scan state for a bucket
	GetScanState(ctx context.Context, bucket string) (*LifecycleScanState, error)

	// UpdateScanState updates scan progress (upsert)
	UpdateScanState(ctx context.Context, state *LifecycleScanState) error

	// ListBucketsWithLifecycle returns buckets that have lifecycle configs
	ListBucketsWithLifecycle(ctx context.Context) ([]string, error)

	// GetBucketsNeedingScan returns buckets that need scanning
	// (completed scan older than minAge, or never scanned)
	GetBucketsNeedingScan(ctx context.Context, minAge time.Duration, limit int) ([]string, error)

	// ResetScanState clears scan state for a bucket (for fresh start)
	ResetScanState(ctx context.Context, bucket string) error
}

// ObjectLockStore provides operations for Object Lock (WORM) configurations
type ObjectLockStore interface {
	// GetObjectLockConfiguration retrieves the Object Lock config for a bucket
	GetObjectLockConfiguration(ctx context.Context, bucket string) (*s3types.ObjectLockConfiguration, error)

	// SetObjectLockConfiguration stores the Object Lock config for a bucket
	SetObjectLockConfiguration(ctx context.Context, bucket string, config *s3types.ObjectLockConfiguration) error

	// GetObjectRetention retrieves the retention settings for an object
	GetObjectRetention(ctx context.Context, bucket, key string) (*s3types.ObjectLockRetention, error)

	// SetObjectRetention stores the retention settings for an object
	SetObjectRetention(ctx context.Context, bucket, key string, retention *s3types.ObjectLockRetention) error

	// GetObjectLegalHold retrieves the legal hold status for an object
	GetObjectLegalHold(ctx context.Context, bucket, key string) (*s3types.ObjectLockLegalHold, error)

	// SetObjectLegalHold stores the legal hold status for an object
	SetObjectLegalHold(ctx context.Context, bucket, key string, legalHold *s3types.ObjectLockLegalHold) error
}

// PublicAccessBlockStore provides operations for public access block configurations
type PublicAccessBlockStore interface {
	// GetPublicAccessBlock retrieves the public access block config for a bucket
	GetPublicAccessBlock(ctx context.Context, bucket string) (*s3types.PublicAccessBlockConfig, error)

	// SetPublicAccessBlock stores the public access block config for a bucket
	SetPublicAccessBlock(ctx context.Context, bucket string, config *s3types.PublicAccessBlockConfig) error

	// DeletePublicAccessBlock removes the public access block config for a bucket
	DeletePublicAccessBlock(ctx context.Context, bucket string) error
}

// OwnershipControlsStore provides operations for bucket ownership controls
type OwnershipControlsStore interface {
	// GetOwnershipControls retrieves the ownership controls for a bucket
	GetOwnershipControls(ctx context.Context, bucket string) (*s3types.OwnershipControls, error)

	// SetOwnershipControls stores the ownership controls for a bucket
	SetOwnershipControls(ctx context.Context, bucket string, controls *s3types.OwnershipControls) error

	// DeleteOwnershipControls removes the ownership controls for a bucket
	DeleteOwnershipControls(ctx context.Context, bucket string) error
}

// BucketLoggingConfig holds the logging configuration for a bucket.
type BucketLoggingConfig struct {
	ID           string
	SourceBucket string
	TargetBucket string
	TargetPrefix string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// LoggingStore provides operations for bucket access logging configuration.
type LoggingStore interface {
	// GetBucketLogging retrieves the logging configuration for a bucket.
	// Returns nil, nil if no logging is configured.
	GetBucketLogging(ctx context.Context, bucket string) (*BucketLoggingConfig, error)

	// SetBucketLogging stores the logging configuration for a bucket.
	// If config.TargetBucket is empty, logging is disabled for the bucket.
	SetBucketLogging(ctx context.Context, config *BucketLoggingConfig) error

	// DeleteBucketLogging removes the logging configuration for a bucket.
	DeleteBucketLogging(ctx context.Context, bucket string) error

	// ListLoggingConfigs returns all bucket logging configurations.
	ListLoggingConfigs(ctx context.Context) ([]*BucketLoggingConfig, error)
}

// Common Lifecycle/ObjectLock errors
var (
	ErrLifecycleNotFound         = fmt.Errorf("lifecycle configuration not found")
	ErrLifecycleScanStateNotFound = fmt.Errorf("lifecycle scan state not found")
	ErrObjectLockNotFound        = fmt.Errorf("object lock configuration not found")
	ErrRetentionNotFound         = fmt.Errorf("retention not found")
	ErrLegalHoldNotFound         = fmt.Errorf("legal hold not found")
	ErrPublicAccessBlockNotFound = fmt.Errorf("public access block configuration not found")
	ErrOwnershipControlsNotFound = fmt.Errorf("ownership controls not found")
)

// ============================================================================
// JSON Serialization Helpers (for SQL backends)
// ============================================================================

// ObjectRefJSON is used for JSON serialization in SQL databases
type ObjectRefJSON struct {
	ID         string `json:"id"`
	Bucket     string `json:"bucket"`
	Key        string `json:"key"`
	Size       uint64 `json:"size"`
	Version    uint64 `json:"version"`
	ETag       string `json:"etag"`
	CreatedAt  int64  `json:"created_at"`
	DeletedAt  int64  `json:"deleted_at,omitempty"`
	ProfileID  string `json:"profile_id,omitempty"`
	ChunkRefs  string `json:"chunk_refs,omitempty"`   // JSON-encoded []ChunkRef
	ECGroupIDs string `json:"ec_group_ids,omitempty"` // JSON-encoded []uuid.UUID
}

// ObjectRefToJSON converts ObjectRef to JSON-serializable form
func ObjectRefToJSON(obj *types.ObjectRef) (*ObjectRefJSON, error) {
	j := &ObjectRefJSON{
		ID:        obj.ID.String(),
		Bucket:    obj.Bucket,
		Key:       obj.Key,
		Size:      obj.Size,
		Version:   obj.Version,
		ETag:      obj.ETag,
		CreatedAt: obj.CreatedAt,
		DeletedAt: obj.DeletedAt,
	}

	if len(obj.ChunkRefs) > 0 {
		data, err := json.Marshal(obj.ChunkRefs)
		if err != nil {
			return nil, fmt.Errorf("marshal chunk refs: %w", err)
		}
		j.ChunkRefs = string(data)
	}

	if len(obj.ECGroupIDs) > 0 {
		data, err := json.Marshal(obj.ECGroupIDs)
		if err != nil {
			return nil, fmt.Errorf("marshal ec group ids: %w", err)
		}
		j.ECGroupIDs = string(data)
	}

	return j, nil
}

// ObjectRefFromJSON converts JSON form back to ObjectRef
func ObjectRefFromJSON(j *ObjectRefJSON) (*types.ObjectRef, error) {
	id, err := uuid.Parse(j.ID)
	if err != nil {
		return nil, fmt.Errorf("parse object id: %w", err)
	}

	obj := &types.ObjectRef{
		ID:        id,
		Bucket:    j.Bucket,
		Key:       j.Key,
		Size:      j.Size,
		Version:   j.Version,
		ETag:      j.ETag,
		CreatedAt: j.CreatedAt,
		DeletedAt: j.DeletedAt,
	}

	if j.ChunkRefs != "" {
		if err := json.Unmarshal([]byte(j.ChunkRefs), &obj.ChunkRefs); err != nil {
			return nil, fmt.Errorf("unmarshal chunk refs: %w", err)
		}
	}

	if j.ECGroupIDs != "" {
		if err := json.Unmarshal([]byte(j.ECGroupIDs), &obj.ECGroupIDs); err != nil {
			return nil, fmt.Errorf("unmarshal ec group ids: %w", err)
		}
	}

	return obj, nil
}
