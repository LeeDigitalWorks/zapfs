// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package db

import (
	"context"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

// Metrics for database operations
var (
	dbQueryDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "zapfs_db_query_duration_seconds",
			Help:    "Duration of database operations in seconds",
			Buckets: []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
		},
		[]string{"operation", "status"},
	)

	dbQueryTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "zapfs_db_queries_total",
			Help: "Total number of database operations",
		},
		[]string{"operation", "status"},
	)

	dbConnectionsActive = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "zapfs_db_connections_active",
			Help: "Number of active database connections",
		},
	)

	dbConnectionsIdle = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "zapfs_db_connections_idle",
			Help: "Number of idle database connections",
		},
	)

	dbConnectionsTotal = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "zapfs_db_connections_total",
			Help: "Total number of database connections (active + idle)",
		},
	)
)

func init() {
	prometheus.MustRegister(
		dbQueryDuration,
		dbQueryTotal,
		dbConnectionsActive,
		dbConnectionsIdle,
		dbConnectionsTotal,
	)
}

// DBMetrics returns the Prometheus collectors for DB metrics
func DBMetrics() []prometheus.Collector {
	return []prometheus.Collector{
		dbQueryDuration,
		dbQueryTotal,
		dbConnectionsActive,
		dbConnectionsIdle,
		dbConnectionsTotal,
	}
}

// UpdateConnectionMetrics updates connection pool metrics from sql.DBStats
func UpdateConnectionMetrics(inUse, idle, total int) {
	dbConnectionsActive.Set(float64(inUse))
	dbConnectionsIdle.Set(float64(idle))
	dbConnectionsTotal.Set(float64(total))
}

// recordMetric records timing and status for an operation
func recordMetric(operation string, start time.Time, err error) {
	duration := time.Since(start).Seconds()
	status := "success"
	if err != nil {
		status = "error"
	}
	dbQueryDuration.WithLabelValues(operation, status).Observe(duration)
	dbQueryTotal.WithLabelValues(operation, status).Inc()
}

// MetricsDB wraps a DB implementation and adds metrics instrumentation
type MetricsDB struct {
	db DB
}

// NewMetricsDB creates a new metrics-instrumented DB wrapper
func NewMetricsDB(db DB) *MetricsDB {
	return &MetricsDB{db: db}
}

// Unwrap returns the underlying DB implementation
func (m *MetricsDB) Unwrap() DB {
	return m.db
}

// Close closes the database connection
func (m *MetricsDB) Close() error {
	return m.db.Close()
}

// Migrate runs database migrations
func (m *MetricsDB) Migrate(ctx context.Context) error {
	start := time.Now()
	err := m.db.Migrate(ctx)
	recordMetric("migrate", start, err)
	return err
}

// WithTx executes fn within a transaction
func (m *MetricsDB) WithTx(ctx context.Context, fn func(tx TxStore) error) error {
	start := time.Now()
	err := m.db.WithTx(ctx, func(tx TxStore) error {
		return fn(&metricsTxStore{tx: tx})
	})
	recordMetric("transaction", start, err)
	return err
}

// ============================================================================
// ObjectStore implementation
// ============================================================================

func (m *MetricsDB) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	start := time.Now()
	err := m.db.PutObject(ctx, obj)
	recordMetric("put_object", start, err)
	return err
}

func (m *MetricsDB) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	start := time.Now()
	obj, err := m.db.GetObject(ctx, bucket, key)
	recordMetric("get_object", start, err)
	return obj, err
}

func (m *MetricsDB) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	start := time.Now()
	obj, err := m.db.GetObjectByID(ctx, id)
	recordMetric("get_object_by_id", start, err)
	return obj, err
}

func (m *MetricsDB) DeleteObject(ctx context.Context, bucket, key string) error {
	start := time.Now()
	err := m.db.DeleteObject(ctx, bucket, key)
	recordMetric("delete_object", start, err)
	return err
}

func (m *MetricsDB) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	start := time.Now()
	err := m.db.MarkObjectDeleted(ctx, bucket, key, deletedAt)
	recordMetric("mark_object_deleted", start, err)
	return err
}

func (m *MetricsDB) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	start := time.Now()
	objs, err := m.db.ListObjects(ctx, bucket, prefix, limit)
	recordMetric("list_objects", start, err)
	return objs, err
}

func (m *MetricsDB) ListObjectsV2(ctx context.Context, params *ListObjectsParams) (*ListObjectsResult, error) {
	start := time.Now()
	result, err := m.db.ListObjectsV2(ctx, params)
	recordMetric("list_objects_v2", start, err)
	return result, err
}

func (m *MetricsDB) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	start := time.Now()
	objs, err := m.db.ListDeletedObjects(ctx, olderThan, limit)
	recordMetric("list_deleted_objects", start, err)
	return objs, err
}

// ============================================================================
// BucketStore implementation
// ============================================================================

func (m *MetricsDB) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	start := time.Now()
	err := m.db.CreateBucket(ctx, bucket)
	recordMetric("create_bucket", start, err)
	return err
}

func (m *MetricsDB) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	start := time.Now()
	bucket, err := m.db.GetBucket(ctx, name)
	recordMetric("get_bucket", start, err)
	return bucket, err
}

func (m *MetricsDB) DeleteBucket(ctx context.Context, name string) error {
	start := time.Now()
	err := m.db.DeleteBucket(ctx, name)
	recordMetric("delete_bucket", start, err)
	return err
}

func (m *MetricsDB) ListBuckets(ctx context.Context, params *ListBucketsParams) (*ListBucketsResult, error) {
	start := time.Now()
	result, err := m.db.ListBuckets(ctx, params)
	recordMetric("list_buckets", start, err)
	return result, err
}

func (m *MetricsDB) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	start := time.Now()
	err := m.db.UpdateBucketVersioning(ctx, bucket, versioning)
	recordMetric("update_bucket_versioning", start, err)
	return err
}

// ============================================================================
// MultipartStore implementation
// ============================================================================

func (m *MetricsDB) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	start := time.Now()
	err := m.db.CreateMultipartUpload(ctx, upload)
	recordMetric("create_multipart_upload", start, err)
	return err
}

func (m *MetricsDB) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	start := time.Now()
	upload, err := m.db.GetMultipartUpload(ctx, bucket, key, uploadID)
	recordMetric("get_multipart_upload", start, err)
	return upload, err
}

func (m *MetricsDB) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	start := time.Now()
	err := m.db.DeleteMultipartUpload(ctx, bucket, key, uploadID)
	recordMetric("delete_multipart_upload", start, err)
	return err
}

func (m *MetricsDB) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	start := time.Now()
	uploads, truncated, err := m.db.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, maxUploads)
	recordMetric("list_multipart_uploads", start, err)
	return uploads, truncated, err
}

func (m *MetricsDB) PutPart(ctx context.Context, part *types.MultipartPart) error {
	start := time.Now()
	err := m.db.PutPart(ctx, part)
	recordMetric("put_part", start, err)
	return err
}

func (m *MetricsDB) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	start := time.Now()
	part, err := m.db.GetPart(ctx, uploadID, partNumber)
	recordMetric("get_part", start, err)
	return part, err
}

func (m *MetricsDB) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	start := time.Now()
	parts, truncated, err := m.db.ListParts(ctx, uploadID, partNumberMarker, maxParts)
	recordMetric("list_parts", start, err)
	return parts, truncated, err
}

func (m *MetricsDB) DeleteParts(ctx context.Context, uploadID string) error {
	start := time.Now()
	err := m.db.DeleteParts(ctx, uploadID)
	recordMetric("delete_parts", start, err)
	return err
}

// ============================================================================
// VersionStore implementation
// ============================================================================

func (m *MetricsDB) ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, versionIDMarker, delimiter string, maxKeys int) ([]*types.ObjectVersion, bool, string, string, error) {
	start := time.Now()
	versions, truncated, nextKey, nextVersion, err := m.db.ListObjectVersions(ctx, bucket, prefix, keyMarker, versionIDMarker, delimiter, maxKeys)
	recordMetric("list_object_versions", start, err)
	return versions, truncated, nextKey, nextVersion, err
}

func (m *MetricsDB) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*types.ObjectRef, error) {
	start := time.Now()
	obj, err := m.db.GetObjectVersion(ctx, bucket, key, versionID)
	recordMetric("get_object_version", start, err)
	return obj, err
}

func (m *MetricsDB) DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error {
	start := time.Now()
	err := m.db.DeleteObjectVersion(ctx, bucket, key, versionID)
	recordMetric("delete_object_version", start, err)
	return err
}

func (m *MetricsDB) PutDeleteMarker(ctx context.Context, bucket, key, ownerID string) (string, error) {
	start := time.Now()
	versionID, err := m.db.PutDeleteMarker(ctx, bucket, key, ownerID)
	recordMetric("put_delete_marker", start, err)
	return versionID, err
}

// ============================================================================
// ACLStore implementation
// ============================================================================

func (m *MetricsDB) GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error) {
	start := time.Now()
	acl, err := m.db.GetBucketACL(ctx, bucket)
	recordMetric("get_bucket_acl", start, err)
	return acl, err
}

func (m *MetricsDB) SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error {
	start := time.Now()
	err := m.db.SetBucketACL(ctx, bucket, acl)
	recordMetric("set_bucket_acl", start, err)
	return err
}

func (m *MetricsDB) GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, error) {
	start := time.Now()
	acl, err := m.db.GetObjectACL(ctx, bucket, key)
	recordMetric("get_object_acl", start, err)
	return acl, err
}

func (m *MetricsDB) SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error {
	start := time.Now()
	err := m.db.SetObjectACL(ctx, bucket, key, acl)
	recordMetric("set_object_acl", start, err)
	return err
}

// ============================================================================
// PolicyStore implementation
// ============================================================================

func (m *MetricsDB) GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, error) {
	start := time.Now()
	policy, err := m.db.GetBucketPolicy(ctx, bucket)
	recordMetric("get_bucket_policy", start, err)
	return policy, err
}

func (m *MetricsDB) SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error {
	start := time.Now()
	err := m.db.SetBucketPolicy(ctx, bucket, policy)
	recordMetric("set_bucket_policy", start, err)
	return err
}

func (m *MetricsDB) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteBucketPolicy(ctx, bucket)
	recordMetric("delete_bucket_policy", start, err)
	return err
}

// ============================================================================
// CORSStore implementation
// ============================================================================

func (m *MetricsDB) GetBucketCORS(ctx context.Context, bucket string) (*s3types.CORSConfiguration, error) {
	start := time.Now()
	cors, err := m.db.GetBucketCORS(ctx, bucket)
	recordMetric("get_bucket_cors", start, err)
	return cors, err
}

func (m *MetricsDB) SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error {
	start := time.Now()
	err := m.db.SetBucketCORS(ctx, bucket, cors)
	recordMetric("set_bucket_cors", start, err)
	return err
}

func (m *MetricsDB) DeleteBucketCORS(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteBucketCORS(ctx, bucket)
	recordMetric("delete_bucket_cors", start, err)
	return err
}

// ============================================================================
// WebsiteStore implementation
// ============================================================================

func (m *MetricsDB) GetBucketWebsite(ctx context.Context, bucket string) (*s3types.WebsiteConfiguration, error) {
	start := time.Now()
	website, err := m.db.GetBucketWebsite(ctx, bucket)
	recordMetric("get_bucket_website", start, err)
	return website, err
}

func (m *MetricsDB) SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error {
	start := time.Now()
	err := m.db.SetBucketWebsite(ctx, bucket, website)
	recordMetric("set_bucket_website", start, err)
	return err
}

func (m *MetricsDB) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteBucketWebsite(ctx, bucket)
	recordMetric("delete_bucket_website", start, err)
	return err
}

// ============================================================================
// TaggingStore implementation
// ============================================================================

func (m *MetricsDB) GetBucketTagging(ctx context.Context, bucket string) (*s3types.TagSet, error) {
	start := time.Now()
	tags, err := m.db.GetBucketTagging(ctx, bucket)
	recordMetric("get_bucket_tagging", start, err)
	return tags, err
}

func (m *MetricsDB) SetBucketTagging(ctx context.Context, bucket string, tagSet *s3types.TagSet) error {
	start := time.Now()
	err := m.db.SetBucketTagging(ctx, bucket, tagSet)
	recordMetric("set_bucket_tagging", start, err)
	return err
}

func (m *MetricsDB) DeleteBucketTagging(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteBucketTagging(ctx, bucket)
	recordMetric("delete_bucket_tagging", start, err)
	return err
}

func (m *MetricsDB) GetObjectTagging(ctx context.Context, bucket, key string) (*s3types.TagSet, error) {
	start := time.Now()
	tags, err := m.db.GetObjectTagging(ctx, bucket, key)
	recordMetric("get_object_tagging", start, err)
	return tags, err
}

func (m *MetricsDB) SetObjectTagging(ctx context.Context, bucket, key string, tagSet *s3types.TagSet) error {
	start := time.Now()
	err := m.db.SetObjectTagging(ctx, bucket, key, tagSet)
	recordMetric("set_object_tagging", start, err)
	return err
}

func (m *MetricsDB) DeleteObjectTagging(ctx context.Context, bucket, key string) error {
	start := time.Now()
	err := m.db.DeleteObjectTagging(ctx, bucket, key)
	recordMetric("delete_object_tagging", start, err)
	return err
}

// ============================================================================
// EncryptionStore implementation
// ============================================================================

func (m *MetricsDB) GetBucketEncryption(ctx context.Context, bucket string) (*s3types.ServerSideEncryptionConfig, error) {
	start := time.Now()
	config, err := m.db.GetBucketEncryption(ctx, bucket)
	recordMetric("get_bucket_encryption", start, err)
	return config, err
}

func (m *MetricsDB) SetBucketEncryption(ctx context.Context, bucket string, config *s3types.ServerSideEncryptionConfig) error {
	start := time.Now()
	err := m.db.SetBucketEncryption(ctx, bucket, config)
	recordMetric("set_bucket_encryption", start, err)
	return err
}

func (m *MetricsDB) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteBucketEncryption(ctx, bucket)
	recordMetric("delete_bucket_encryption", start, err)
	return err
}

// ============================================================================
// LifecycleStore implementation
// ============================================================================

func (m *MetricsDB) GetBucketLifecycle(ctx context.Context, bucket string) (*s3types.Lifecycle, error) {
	start := time.Now()
	lifecycle, err := m.db.GetBucketLifecycle(ctx, bucket)
	recordMetric("get_bucket_lifecycle", start, err)
	return lifecycle, err
}

func (m *MetricsDB) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error {
	start := time.Now()
	err := m.db.SetBucketLifecycle(ctx, bucket, lifecycle)
	recordMetric("set_bucket_lifecycle", start, err)
	return err
}

func (m *MetricsDB) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteBucketLifecycle(ctx, bucket)
	recordMetric("delete_bucket_lifecycle", start, err)
	return err
}

// ============================================================================
// LifecycleScanStore implementation
// ============================================================================

func (m *MetricsDB) GetScanState(ctx context.Context, bucket string) (*LifecycleScanState, error) {
	start := time.Now()
	state, err := m.db.GetScanState(ctx, bucket)
	recordMetric("get_scan_state", start, err)
	return state, err
}

func (m *MetricsDB) UpdateScanState(ctx context.Context, state *LifecycleScanState) error {
	start := time.Now()
	err := m.db.UpdateScanState(ctx, state)
	recordMetric("update_scan_state", start, err)
	return err
}

func (m *MetricsDB) ListBucketsWithLifecycle(ctx context.Context) ([]string, error) {
	start := time.Now()
	buckets, err := m.db.ListBucketsWithLifecycle(ctx)
	recordMetric("list_buckets_with_lifecycle", start, err)
	return buckets, err
}

func (m *MetricsDB) GetBucketsNeedingScan(ctx context.Context, minAge time.Duration, limit int) ([]string, error) {
	start := time.Now()
	buckets, err := m.db.GetBucketsNeedingScan(ctx, minAge, limit)
	recordMetric("get_buckets_needing_scan", start, err)
	return buckets, err
}

func (m *MetricsDB) ResetScanState(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.ResetScanState(ctx, bucket)
	recordMetric("reset_scan_state", start, err)
	return err
}

// ============================================================================
// ObjectLockStore implementation
// ============================================================================

func (m *MetricsDB) GetObjectLockConfiguration(ctx context.Context, bucket string) (*s3types.ObjectLockConfiguration, error) {
	start := time.Now()
	config, err := m.db.GetObjectLockConfiguration(ctx, bucket)
	recordMetric("get_object_lock_configuration", start, err)
	return config, err
}

func (m *MetricsDB) SetObjectLockConfiguration(ctx context.Context, bucket string, config *s3types.ObjectLockConfiguration) error {
	start := time.Now()
	err := m.db.SetObjectLockConfiguration(ctx, bucket, config)
	recordMetric("set_object_lock_configuration", start, err)
	return err
}

func (m *MetricsDB) GetObjectRetention(ctx context.Context, bucket, key string) (*s3types.ObjectLockRetention, error) {
	start := time.Now()
	retention, err := m.db.GetObjectRetention(ctx, bucket, key)
	recordMetric("get_object_retention", start, err)
	return retention, err
}

func (m *MetricsDB) SetObjectRetention(ctx context.Context, bucket, key string, retention *s3types.ObjectLockRetention) error {
	start := time.Now()
	err := m.db.SetObjectRetention(ctx, bucket, key, retention)
	recordMetric("set_object_retention", start, err)
	return err
}

func (m *MetricsDB) GetObjectLegalHold(ctx context.Context, bucket, key string) (*s3types.ObjectLockLegalHold, error) {
	start := time.Now()
	legalHold, err := m.db.GetObjectLegalHold(ctx, bucket, key)
	recordMetric("get_object_legal_hold", start, err)
	return legalHold, err
}

func (m *MetricsDB) SetObjectLegalHold(ctx context.Context, bucket, key string, legalHold *s3types.ObjectLockLegalHold) error {
	start := time.Now()
	err := m.db.SetObjectLegalHold(ctx, bucket, key, legalHold)
	recordMetric("set_object_legal_hold", start, err)
	return err
}

// ============================================================================
// PublicAccessBlockStore implementation
// ============================================================================

func (m *MetricsDB) GetPublicAccessBlock(ctx context.Context, bucket string) (*s3types.PublicAccessBlockConfig, error) {
	start := time.Now()
	config, err := m.db.GetPublicAccessBlock(ctx, bucket)
	recordMetric("get_public_access_block", start, err)
	return config, err
}

func (m *MetricsDB) SetPublicAccessBlock(ctx context.Context, bucket string, config *s3types.PublicAccessBlockConfig) error {
	start := time.Now()
	err := m.db.SetPublicAccessBlock(ctx, bucket, config)
	recordMetric("set_public_access_block", start, err)
	return err
}

func (m *MetricsDB) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeletePublicAccessBlock(ctx, bucket)
	recordMetric("delete_public_access_block", start, err)
	return err
}

// ============================================================================
// OwnershipControlsStore implementation
// ============================================================================

func (m *MetricsDB) GetOwnershipControls(ctx context.Context, bucket string) (*s3types.OwnershipControls, error) {
	start := time.Now()
	controls, err := m.db.GetOwnershipControls(ctx, bucket)
	recordMetric("get_ownership_controls", start, err)
	return controls, err
}

func (m *MetricsDB) SetOwnershipControls(ctx context.Context, bucket string, controls *s3types.OwnershipControls) error {
	start := time.Now()
	err := m.db.SetOwnershipControls(ctx, bucket, controls)
	recordMetric("set_ownership_controls", start, err)
	return err
}

func (m *MetricsDB) DeleteOwnershipControls(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteOwnershipControls(ctx, bucket)
	recordMetric("delete_ownership_controls", start, err)
	return err
}

// ============================================================================
// LoggingStore implementation
// ============================================================================

func (m *MetricsDB) GetBucketLogging(ctx context.Context, bucket string) (*BucketLoggingConfig, error) {
	start := time.Now()
	config, err := m.db.GetBucketLogging(ctx, bucket)
	recordMetric("get_bucket_logging", start, err)
	return config, err
}

func (m *MetricsDB) SetBucketLogging(ctx context.Context, config *BucketLoggingConfig) error {
	start := time.Now()
	err := m.db.SetBucketLogging(ctx, config)
	recordMetric("set_bucket_logging", start, err)
	return err
}

func (m *MetricsDB) DeleteBucketLogging(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteBucketLogging(ctx, bucket)
	recordMetric("delete_bucket_logging", start, err)
	return err
}

func (m *MetricsDB) ListLoggingConfigs(ctx context.Context) ([]*BucketLoggingConfig, error) {
	start := time.Now()
	configs, err := m.db.ListLoggingConfigs(ctx)
	recordMetric("list_logging_configs", start, err)
	return configs, err
}

// ============================================================================
// NotificationStore implementation
// ============================================================================

func (m *MetricsDB) GetNotificationConfiguration(ctx context.Context, bucket string) (*s3types.NotificationConfiguration, error) {
	start := time.Now()
	config, err := m.db.GetNotificationConfiguration(ctx, bucket)
	recordMetric("get_notification_configuration", start, err)
	return config, err
}

func (m *MetricsDB) SetNotificationConfiguration(ctx context.Context, bucket string, config *s3types.NotificationConfiguration) error {
	start := time.Now()
	err := m.db.SetNotificationConfiguration(ctx, bucket, config)
	recordMetric("set_notification_configuration", start, err)
	return err
}

func (m *MetricsDB) DeleteNotificationConfiguration(ctx context.Context, bucket string) error {
	start := time.Now()
	err := m.db.DeleteNotificationConfiguration(ctx, bucket)
	recordMetric("delete_notification_configuration", start, err)
	return err
}

// ============================================================================
// metricsTxStore wraps TxStore with metrics
// ============================================================================

type metricsTxStore struct {
	tx TxStore
}

func (m *metricsTxStore) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	start := time.Now()
	err := m.tx.PutObject(ctx, obj)
	recordMetric("tx_put_object", start, err)
	return err
}

func (m *metricsTxStore) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	start := time.Now()
	obj, err := m.tx.GetObject(ctx, bucket, key)
	recordMetric("tx_get_object", start, err)
	return obj, err
}

func (m *metricsTxStore) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	start := time.Now()
	obj, err := m.tx.GetObjectByID(ctx, id)
	recordMetric("tx_get_object_by_id", start, err)
	return obj, err
}

func (m *metricsTxStore) DeleteObject(ctx context.Context, bucket, key string) error {
	start := time.Now()
	err := m.tx.DeleteObject(ctx, bucket, key)
	recordMetric("tx_delete_object", start, err)
	return err
}

func (m *metricsTxStore) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	start := time.Now()
	err := m.tx.MarkObjectDeleted(ctx, bucket, key, deletedAt)
	recordMetric("tx_mark_object_deleted", start, err)
	return err
}

func (m *metricsTxStore) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	start := time.Now()
	objs, err := m.tx.ListObjects(ctx, bucket, prefix, limit)
	recordMetric("tx_list_objects", start, err)
	return objs, err
}

func (m *metricsTxStore) ListObjectsV2(ctx context.Context, params *ListObjectsParams) (*ListObjectsResult, error) {
	start := time.Now()
	result, err := m.tx.ListObjectsV2(ctx, params)
	recordMetric("tx_list_objects_v2", start, err)
	return result, err
}

func (m *metricsTxStore) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	start := time.Now()
	objs, err := m.tx.ListDeletedObjects(ctx, olderThan, limit)
	recordMetric("tx_list_deleted_objects", start, err)
	return objs, err
}

func (m *metricsTxStore) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	start := time.Now()
	err := m.tx.CreateBucket(ctx, bucket)
	recordMetric("tx_create_bucket", start, err)
	return err
}

func (m *metricsTxStore) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	start := time.Now()
	bucket, err := m.tx.GetBucket(ctx, name)
	recordMetric("tx_get_bucket", start, err)
	return bucket, err
}

func (m *metricsTxStore) DeleteBucket(ctx context.Context, name string) error {
	start := time.Now()
	err := m.tx.DeleteBucket(ctx, name)
	recordMetric("tx_delete_bucket", start, err)
	return err
}

func (m *metricsTxStore) ListBuckets(ctx context.Context, params *ListBucketsParams) (*ListBucketsResult, error) {
	start := time.Now()
	result, err := m.tx.ListBuckets(ctx, params)
	recordMetric("tx_list_buckets", start, err)
	return result, err
}

func (m *metricsTxStore) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	start := time.Now()
	err := m.tx.UpdateBucketVersioning(ctx, bucket, versioning)
	recordMetric("tx_update_bucket_versioning", start, err)
	return err
}

func (m *metricsTxStore) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	start := time.Now()
	err := m.tx.CreateMultipartUpload(ctx, upload)
	recordMetric("tx_create_multipart_upload", start, err)
	return err
}

func (m *metricsTxStore) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	start := time.Now()
	upload, err := m.tx.GetMultipartUpload(ctx, bucket, key, uploadID)
	recordMetric("tx_get_multipart_upload", start, err)
	return upload, err
}

func (m *metricsTxStore) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	start := time.Now()
	err := m.tx.DeleteMultipartUpload(ctx, bucket, key, uploadID)
	recordMetric("tx_delete_multipart_upload", start, err)
	return err
}

func (m *metricsTxStore) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	start := time.Now()
	uploads, truncated, err := m.tx.ListMultipartUploads(ctx, bucket, prefix, keyMarker, uploadIDMarker, maxUploads)
	recordMetric("tx_list_multipart_uploads", start, err)
	return uploads, truncated, err
}

func (m *metricsTxStore) PutPart(ctx context.Context, part *types.MultipartPart) error {
	start := time.Now()
	err := m.tx.PutPart(ctx, part)
	recordMetric("tx_put_part", start, err)
	return err
}

func (m *metricsTxStore) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	start := time.Now()
	part, err := m.tx.GetPart(ctx, uploadID, partNumber)
	recordMetric("tx_get_part", start, err)
	return part, err
}

func (m *metricsTxStore) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	start := time.Now()
	parts, truncated, err := m.tx.ListParts(ctx, uploadID, partNumberMarker, maxParts)
	recordMetric("tx_list_parts", start, err)
	return parts, truncated, err
}

func (m *metricsTxStore) DeleteParts(ctx context.Context, uploadID string) error {
	start := time.Now()
	err := m.tx.DeleteParts(ctx, uploadID)
	recordMetric("tx_delete_parts", start, err)
	return err
}
