package vitess

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	_ "github.com/go-sql-driver/mysql" // MySQL driver for Vitess
	"github.com/google/uuid"
)

const (
	defaultMaxOpenConns    = 25
	defaultMaxIdleConns    = 5
	defaultConnMaxLifetime = 5 * time.Minute
	defaultConnMaxIdleTime = 1 * time.Minute
)

// Config holds Vitess connection configuration
type Config struct {
	// DSN is the data source name (e.g., "user:pass@tcp(vtgate:3306)/keyspace")
	DSN string

	// Connection pool settings
	MaxOpenConns    int
	MaxIdleConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(dsn string) Config {
	return Config{
		DSN:             dsn,
		MaxOpenConns:    defaultMaxOpenConns,
		MaxIdleConns:    defaultMaxIdleConns,
		ConnMaxLifetime: defaultConnMaxLifetime,
		ConnMaxIdleTime: defaultConnMaxIdleTime,
	}
}

// Vitess implements db.DB using Vitess as the backing store
type Vitess struct {
	db     *sql.DB
	config Config
}

// NewVitess creates a new Vitess-backed database
func NewVitess(cfg Config) (db.DB, error) {
	sqlDB, err := sql.Open("mysql", cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Configure connection pool
	sqlDB.SetMaxOpenConns(cfg.MaxOpenConns)
	sqlDB.SetMaxIdleConns(cfg.MaxIdleConns)
	sqlDB.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := sqlDB.PingContext(ctx); err != nil {
		sqlDB.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return &Vitess{
		db:     sqlDB,
		config: cfg,
	}, nil
}

// Close closes the database connection
func (v *Vitess) Close() error {
	return v.db.Close()
}

// SqlDB returns the underlying *sql.DB for use with taskqueue.DBQueue
func (v *Vitess) SqlDB() *sql.DB {
	return v.db
}

// WithTx executes fn within a database transaction.
// If fn returns an error, the transaction is rolled back.
// If fn returns nil, the transaction is committed.
func (v *Vitess) WithTx(ctx context.Context, fn func(tx db.TxStore) error) error {
	sqlTx, err := v.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin transaction: %w", err)
	}

	txStore := &vitessTx{tx: sqlTx}

	if err := fn(txStore); err != nil {
		if rbErr := sqlTx.Rollback(); rbErr != nil {
			return fmt.Errorf("rollback failed: %v (original error: %w)", rbErr, err)
		}
		return err
	}

	if err := sqlTx.Commit(); err != nil {
		return fmt.Errorf("commit transaction: %w", err)
	}

	return nil
}

// vitessTx wraps a sql.Tx to implement db.TxStore
type vitessTx struct {
	tx *sql.Tx
}

// ============================================================================
// Transaction Object Operations
// ============================================================================

func (t *vitessTx) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	chunkRefsJSON, err := json.Marshal(obj.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	ecGroupIDsJSON, err := json.Marshal(obj.ECGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal ec group ids: %w", err)
	}

	// Convert IsLatest bool to int for MySQL
	isLatest := 0
	if obj.IsLatest {
		isLatest = 1
	}

	if obj.IsLatest {
		// Versioning mode: mark old versions as not latest, then insert new
		_, err = t.tx.ExecContext(ctx, `
			UPDATE objects SET is_latest = 0 WHERE bucket = ? AND object_key = ? AND is_latest = 1
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("mark old versions: %w", err)
		}

		// Insert new version
		_, err = t.tx.ExecContext(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			obj.ID.String(),
			obj.Bucket,
			obj.Key,
			obj.Size,
			obj.Version,
			obj.ETag,
			obj.CreatedAt,
			obj.DeletedAt,
			obj.TTL,
			obj.ProfileID,
			string(chunkRefsJSON),
			string(ecGroupIDsJSON),
			isLatest,
			obj.SSEAlgorithm,
			obj.SSECustomerKeyMD5,
			obj.SSEKMSKeyID,
			obj.SSEKMSContext,
		)
	} else {
		// Non-versioning mode: delete old, then insert new (replace behavior)
		_, err = t.tx.ExecContext(ctx, `
			DELETE FROM objects WHERE bucket = ? AND object_key = ?
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("delete old object: %w", err)
		}

		_, err = t.tx.ExecContext(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
		`,
			obj.ID.String(),
			obj.Bucket,
			obj.Key,
			obj.Size,
			obj.Version,
			obj.ETag,
			obj.CreatedAt,
			obj.DeletedAt,
			obj.TTL,
			obj.ProfileID,
			string(chunkRefsJSON),
			string(ecGroupIDsJSON),
			obj.SSEAlgorithm,
			obj.SSECustomerKeyMD5,
			obj.SSEKMSKeyID,
			obj.SSEKMSContext,
		)
	}
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (t *vitessTx) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ? AND object_key = ? AND is_latest = 1
	`, bucket, key)
	return scanObject(row)
}

func (t *vitessTx) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE id = ?
	`, id.String())
	return scanObject(row)
}

func (t *vitessTx) DeleteObject(ctx context.Context, bucket, key string) error {
	_, err := t.tx.ExecContext(ctx, `
		DELETE FROM objects WHERE bucket = ? AND object_key = ?
	`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (t *vitessTx) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	result, err := t.tx.ExecContext(ctx, `
		UPDATE objects SET deleted_at = ? WHERE bucket = ? AND object_key = ? AND deleted_at = 0
	`, deletedAt, bucket, key)
	if err != nil {
		return fmt.Errorf("mark object deleted: %w", err)
	}
	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

func (t *vitessTx) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	// Use ListObjectsV2 internally for consistency
	result, err := t.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKeys: limit,
	})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (t *vitessTx) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	marker := params.ContinuationToken
	if marker == "" {
		marker = params.Marker
	}
	if marker == "" {
		marker = params.StartAfter
	}

	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ?
		  AND object_key LIKE ?
		  AND deleted_at = 0
		  AND is_latest = 1
	`
	args := []any{params.Bucket, params.Prefix + "%"}

	if marker != "" {
		query += " AND object_key > ?"
		args = append(args, marker)
	}

	query += " ORDER BY object_key"

	fetchLimit := params.MaxKeys + 1
	if fetchLimit > 0 {
		query += " LIMIT ?"
		args = append(args, fetchLimit)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list objects v2: %w", err)
	}
	defer rows.Close()

	objects, err := scanObjects(rows)
	if err != nil {
		return nil, err
	}

	result := &db.ListObjectsResult{
		CommonPrefixes: make([]string, 0),
	}

	if params.Delimiter != "" {
		seenPrefixes := make(map[string]bool)
		filteredObjects := make([]*types.ObjectRef, 0, len(objects))

		for _, obj := range objects {
			afterPrefix := obj.Key[len(params.Prefix):]
			idx := strings.Index(afterPrefix, params.Delimiter)
			if idx >= 0 {
				commonPrefix := params.Prefix + afterPrefix[:idx+len(params.Delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
			} else {
				filteredObjects = append(filteredObjects, obj)
			}
		}
		objects = filteredObjects
	}

	totalItems := len(objects) + len(result.CommonPrefixes)
	if totalItems > params.MaxKeys {
		result.IsTruncated = true
		if len(objects) > params.MaxKeys {
			objects = objects[:params.MaxKeys]
		}
		if len(objects) > 0 {
			result.NextContinuationToken = objects[len(objects)-1].Key
			result.NextMarker = result.NextContinuationToken
		}
	}

	result.Objects = objects
	return result, nil
}

func (t *vitessTx) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE deleted_at > 0 AND deleted_at < ?
		ORDER BY deleted_at
	`
	args := []any{olderThan}

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deleted objects: %w", err)
	}
	defer rows.Close()

	return scanObjects(rows)
}

// ============================================================================
// Transaction Bucket Operations
// ============================================================================

func (t *vitessTx) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	_, err := t.tx.ExecContext(ctx, `
		INSERT INTO buckets (id, name, owner_id, region, created_at, default_profile_id, versioning)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		bucket.ID.String(),
		bucket.Name,
		bucket.OwnerID,
		bucket.Region,
		bucket.CreatedAt,
		bucket.DefaultProfileID,
		bucket.Versioning,
	)
	if err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}

func (t *vitessTx) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, name, owner_id, region, created_at, default_profile_id, versioning
		FROM buckets
		WHERE name = ?
	`, name)

	var bucket types.BucketInfo
	var idStr string
	err := row.Scan(
		&idStr,
		&bucket.Name,
		&bucket.OwnerID,
		&bucket.Region,
		&bucket.CreatedAt,
		&bucket.DefaultProfileID,
		&bucket.Versioning,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrBucketNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan bucket: %w", err)
	}

	bucket.ID, err = uuid.Parse(idStr)
	if err != nil {
		return nil, fmt.Errorf("parse bucket id: %w", err)
	}

	return &bucket, nil
}

func (t *vitessTx) DeleteBucket(ctx context.Context, name string) error {
	_, err := t.tx.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}
	return nil
}

func (t *vitessTx) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	// Build query with filters
	query := `
		SELECT id, name, owner_id, region, created_at, default_profile_id, versioning
		FROM buckets
		WHERE owner_id = ?
	`
	args := []any{params.OwnerID}

	// Add prefix filter
	if params.Prefix != "" {
		query += " AND name LIKE ?"
		args = append(args, params.Prefix+"%")
	}

	// Add region filter
	if params.BucketRegion != "" {
		query += " AND region = ?"
		args = append(args, params.BucketRegion)
	}

	// Add continuation token (pagination marker)
	if params.ContinuationToken != "" {
		query += " AND name > ?"
		args = append(args, params.ContinuationToken)
	}

	// Order by name for consistent pagination
	query += " ORDER BY name"

	// Determine fetch limit (fetch one extra to detect truncation)
	maxBuckets := params.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = 10000
	}
	fetchLimit := maxBuckets + 1
	query += " LIMIT ?"
	args = append(args, fetchLimit)

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets v2: %w", err)
	}
	defer rows.Close()

	var buckets []*types.BucketInfo
	for rows.Next() {
		var bucket types.BucketInfo
		var idStr string
		var region, profileID, versioning sql.NullString

		err := rows.Scan(
			&idStr,
			&bucket.Name,
			&bucket.OwnerID,
			&region,
			&bucket.CreatedAt,
			&profileID,
			&versioning,
		)
		if err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}

		bucket.ID, _ = uuid.Parse(idStr)
		bucket.Region = region.String
		bucket.DefaultProfileID = profileID.String
		bucket.Versioning = versioning.String

		buckets = append(buckets, &bucket)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list buckets v2: %w", err)
	}

	// Check for truncation
	result := &db.ListBucketsResult{
		Buckets: buckets,
	}
	if len(buckets) > maxBuckets {
		result.IsTruncated = true
		result.Buckets = buckets[:maxBuckets]
		// Next continuation token is the name of the last bucket returned
		if len(result.Buckets) > 0 {
			result.NextContinuationToken = result.Buckets[len(result.Buckets)-1].Name
		}
	}

	return result, nil
}

func (t *vitessTx) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	_, err := t.tx.ExecContext(ctx, `
		UPDATE buckets SET versioning = ? WHERE name = ?
	`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}

// ============================================================================
// Transaction Multipart Operations
// ============================================================================

func (t *vitessTx) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	metadataJSON, err := json.Marshal(upload.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = t.tx.ExecContext(ctx, `
		INSERT INTO multipart_uploads (id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		upload.ID.String(),
		upload.UploadID,
		upload.Bucket,
		upload.Key,
		upload.OwnerID,
		upload.Initiated,
		upload.ContentType,
		upload.StorageClass,
		string(metadataJSON),
	)
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	return nil
}

func (t *vitessTx) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata
		FROM multipart_uploads
		WHERE upload_id = ? AND bucket = ? AND object_key = ?
	`, uploadID, bucket, key)

	return scanMultipartUpload(row)
}

func (t *vitessTx) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	_, err := t.tx.ExecContext(ctx, `
		DELETE FROM multipart_uploads WHERE upload_id = ? AND bucket = ? AND object_key = ?
	`, uploadID, bucket, key)
	if err != nil {
		return fmt.Errorf("delete multipart upload: %w", err)
	}
	return nil
}

func (t *vitessTx) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	query := `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata
		FROM multipart_uploads
		WHERE bucket = ?
	`
	args := []any{bucket}

	if prefix != "" {
		query += " AND object_key LIKE ?"
		args = append(args, prefix+"%")
	}

	if keyMarker != "" {
		if uploadIDMarker != "" {
			query += " AND (object_key > ? OR (object_key = ? AND upload_id > ?))"
			args = append(args, keyMarker, keyMarker, uploadIDMarker)
		} else {
			query += " AND object_key > ?"
			args = append(args, keyMarker)
		}
	}

	query += " ORDER BY object_key, upload_id"

	if maxUploads > 0 {
		query += " LIMIT ?"
		args = append(args, maxUploads+1)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []*types.MultipartUpload
	for rows.Next() {
		upload, err := scanMultipartUpload(rows)
		if err != nil {
			return nil, false, err
		}
		uploads = append(uploads, upload)
	}

	isTruncated := false
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
		isTruncated = true
	}

	return uploads, isTruncated, nil
}

func (t *vitessTx) PutPart(ctx context.Context, part *types.MultipartPart) error {
	chunkRefsJSON, err := json.Marshal(part.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	_, err = t.tx.ExecContext(ctx, `
		INSERT INTO multipart_parts (id, upload_id, part_number, size, etag, last_modified, chunk_refs)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			size = VALUES(size),
			etag = VALUES(etag),
			last_modified = VALUES(last_modified),
			chunk_refs = VALUES(chunk_refs)
	`,
		part.ID.String(),
		part.UploadID,
		part.PartNumber,
		part.Size,
		part.ETag,
		part.LastModified,
		string(chunkRefsJSON),
	)
	if err != nil {
		return fmt.Errorf("put part: %w", err)
	}
	return nil
}

func (t *vitessTx) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	row := t.tx.QueryRowContext(ctx, `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = ? AND part_number = ?
	`, uploadID, partNumber)

	return scanPart(row)
}

func (t *vitessTx) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	query := `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = ? AND part_number > ?
		ORDER BY part_number
	`
	args := []any{uploadID, partNumberMarker}

	if maxParts > 0 {
		query += " LIMIT ?"
		args = append(args, maxParts+1)
	}

	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list parts: %w", err)
	}
	defer rows.Close()

	var parts []*types.MultipartPart
	for rows.Next() {
		part, err := scanPart(rows)
		if err != nil {
			return nil, false, err
		}
		parts = append(parts, part)
	}

	isTruncated := false
	if maxParts > 0 && len(parts) > maxParts {
		parts = parts[:maxParts]
		isTruncated = true
	}

	return parts, isTruncated, nil
}

func (t *vitessTx) DeleteParts(ctx context.Context, uploadID string) error {
	_, err := t.tx.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID)
	if err != nil {
		return fmt.Errorf("delete parts: %w", err)
	}
	return nil
}

// Migrate runs database migrations
func (v *Vitess) Migrate(ctx context.Context) error {
	// Ensure schema_migrations table exists
	_, err := v.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INT PRIMARY KEY,
			applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)
	`)
	if err != nil {
		return fmt.Errorf("create schema_migrations table: %w", err)
	}

	// Use the migrations framework
	return db.RunMigrations(ctx, &vitessMigrator{db: v.db})
}

// vitessMigrator implements db.Migrator for Vitess
type vitessMigrator struct {
	db *sql.DB
}

func (m *vitessMigrator) CurrentVersion(ctx context.Context) (int, error) {
	var version int
	err := m.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(version), 0) FROM schema_migrations
	`).Scan(&version)
	if err != nil {
		return 0, fmt.Errorf("get current version: %w", err)
	}
	return version, nil
}

func (m *vitessMigrator) Apply(ctx context.Context, migration db.Migration) error {
	// Split migration SQL into individual statements (MySQL driver doesn't support multi-statement by default)
	statements := splitSQLStatements(migration.SQL)
	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt == "" || strings.HasPrefix(stmt, "--") {
			continue
		}
		if _, err := m.db.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("execute migration SQL: %w", err)
		}
	}
	return nil
}

// splitSQLStatements splits SQL content into individual statements
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip empty lines and comments
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")

		// If line ends with semicolon, it's end of statement
		if strings.HasSuffix(trimmed, ";") {
			statements = append(statements, current.String())
			current.Reset()
		}
	}

	// Handle any remaining content
	if current.Len() > 0 {
		statements = append(statements, current.String())
	}

	return statements
}

func (m *vitessMigrator) SetVersion(ctx context.Context, version int) error {
	_, err := m.db.ExecContext(ctx, `
		INSERT INTO schema_migrations (version) VALUES (?)
	`, version)
	if err != nil {
		return fmt.Errorf("record migration version: %w", err)
	}
	return nil
}

// ============================================================================
// Object Operations
// ============================================================================

func (v *Vitess) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	chunkRefsJSON, err := json.Marshal(obj.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk_refs: %w", err)
	}

	ecGroupIDsJSON, err := json.Marshal(obj.ECGroupIDs)
	if err != nil {
		return fmt.Errorf("marshal ec_group_ids: %w", err)
	}

	// Convert IsLatest bool to int for MySQL
	isLatest := 0
	if obj.IsLatest {
		isLatest = 1
	}

	if obj.IsLatest {
		// Versioning mode: mark old versions as not latest, then insert new
		_, err = v.db.ExecContext(ctx, `
			UPDATE objects SET is_latest = 0 WHERE bucket = ? AND object_key = ? AND is_latest = 1
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("mark old versions: %w", err)
		}

		// Insert new version
		_, err = v.db.ExecContext(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			obj.ID.String(),
			obj.Bucket,
			obj.Key,
			obj.Size,
			obj.Version,
			obj.ETag,
			obj.CreatedAt,
			obj.DeletedAt,
			obj.TTL,
			obj.ProfileID,
			chunkRefsJSON,
			ecGroupIDsJSON,
			isLatest,
			obj.SSEAlgorithm,
			obj.SSECustomerKeyMD5,
			obj.SSEKMSKeyID,
			obj.SSEKMSContext,
		)
	} else {
		// Non-versioning mode: delete old versions, then insert new (replace behavior)
		_, err = v.db.ExecContext(ctx, `
			DELETE FROM objects WHERE bucket = ? AND object_key = ?
		`, obj.Bucket, obj.Key)
		if err != nil {
			return fmt.Errorf("delete old object: %w", err)
		}

		// Insert new object
		_, err = v.db.ExecContext(ctx, `
			INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
		`,
			obj.ID.String(),
			obj.Bucket,
			obj.Key,
			obj.Size,
			obj.Version,
			obj.ETag,
			obj.CreatedAt,
			obj.DeletedAt,
			obj.TTL,
			obj.ProfileID,
			chunkRefsJSON,
			ecGroupIDsJSON,
			obj.SSEAlgorithm,
			obj.SSECustomerKeyMD5,
			obj.SSEKMSKeyID,
			obj.SSEKMSContext,
		)
	}
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (v *Vitess) GetObject(ctx context.Context, bucket, key string) (*types.ObjectRef, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ? AND object_key = ? AND is_latest = 1 AND deleted_at = 0
	`, bucket, key)

	return scanObject(row)
}

func (v *Vitess) GetObjectByID(ctx context.Context, id uuid.UUID) (*types.ObjectRef, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE id = ?
	`, id.String())

	return scanObject(row)
}

func (v *Vitess) DeleteObject(ctx context.Context, bucket, key string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM objects WHERE bucket = ? AND object_key = ?
	`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

func (v *Vitess) MarkObjectDeleted(ctx context.Context, bucket, key string, deletedAt int64) error {
	result, err := v.db.ExecContext(ctx, `
		UPDATE objects SET deleted_at = ? WHERE bucket = ? AND object_key = ? AND deleted_at = 0
	`, deletedAt, bucket, key)
	if err != nil {
		return fmt.Errorf("mark object deleted: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

func (v *Vitess) ListObjects(ctx context.Context, bucket, prefix string, limit int) ([]*types.ObjectRef, error) {
	// Use ListObjectsV2 internally for consistency
	result, err := v.ListObjectsV2(ctx, &db.ListObjectsParams{
		Bucket:  bucket,
		Prefix:  prefix,
		MaxKeys: limit,
	})
	if err != nil {
		return nil, err
	}
	return result.Objects, nil
}

func (v *Vitess) ListObjectsV2(ctx context.Context, params *db.ListObjectsParams) (*db.ListObjectsResult, error) {
	// Determine marker from params
	marker := params.ContinuationToken
	if marker == "" {
		marker = params.Marker
	}
	if marker == "" {
		marker = params.StartAfter
	}

	// Build efficient query with proper indexes
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ?
		  AND object_key LIKE ?
		  AND deleted_at = 0
		  AND is_latest = 1
	`
	args := []any{params.Bucket, params.Prefix + "%"}

	// Add marker condition for pagination (uses index efficiently)
	if marker != "" {
		query += " AND object_key > ?"
		args = append(args, marker)
	}

	query += " ORDER BY object_key"

	// Fetch one extra to detect truncation
	fetchLimit := params.MaxKeys + 1
	if fetchLimit > 0 {
		query += " LIMIT ?"
		args = append(args, fetchLimit)
	}

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list objects v2: %w", err)
	}
	defer rows.Close()

	objects, err := scanObjects(rows)
	if err != nil {
		return nil, err
	}

	result := &db.ListObjectsResult{
		CommonPrefixes: make([]string, 0),
	}

	// Handle delimiter for folder simulation
	if params.Delimiter != "" {
		seenPrefixes := make(map[string]bool)
		filteredObjects := make([]*types.ObjectRef, 0, len(objects))

		for _, obj := range objects {
			// Get the part of the key after the prefix
			afterPrefix := obj.Key[len(params.Prefix):]

			// Check if delimiter exists in remaining key
			idx := strings.Index(afterPrefix, params.Delimiter)
			if idx >= 0 {
				// This is a "folder" - extract common prefix
				commonPrefix := params.Prefix + afterPrefix[:idx+len(params.Delimiter)]
				if !seenPrefixes[commonPrefix] {
					seenPrefixes[commonPrefix] = true
					result.CommonPrefixes = append(result.CommonPrefixes, commonPrefix)
				}
			} else {
				// Regular object
				filteredObjects = append(filteredObjects, obj)
			}
		}
		objects = filteredObjects
	}

	// Check truncation
	totalItems := len(objects) + len(result.CommonPrefixes)
	if totalItems > params.MaxKeys {
		result.IsTruncated = true
		// Trim to MaxKeys
		if len(objects) > params.MaxKeys {
			objects = objects[:params.MaxKeys]
		}
		// Set continuation token
		if len(objects) > 0 {
			result.NextContinuationToken = objects[len(objects)-1].Key
			result.NextMarker = result.NextContinuationToken
		}
	}

	result.Objects = objects
	return result, nil
}

func (v *Vitess) ListDeletedObjects(ctx context.Context, olderThan int64, limit int) ([]*types.ObjectRef, error) {
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE deleted_at > 0 AND deleted_at < ?
		ORDER BY deleted_at
	`
	args := []any{olderThan}

	if limit > 0 {
		query += " LIMIT ?"
		args = append(args, limit)
	}

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list deleted objects: %w", err)
	}
	defer rows.Close()

	return scanObjects(rows)
}

// ============================================================================
// Bucket Operations
// ============================================================================

func (v *Vitess) CreateBucket(ctx context.Context, bucket *types.BucketInfo) error {
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO buckets (id, name, owner_id, region, created_at, default_profile_id, versioning)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`,
		bucket.ID.String(),
		bucket.Name,
		bucket.OwnerID,
		bucket.Region,
		bucket.CreatedAt,
		bucket.DefaultProfileID,
		bucket.Versioning,
	)
	if err != nil {
		return fmt.Errorf("create bucket: %w", err)
	}
	return nil
}

func (v *Vitess) GetBucket(ctx context.Context, name string) (*types.BucketInfo, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, name, owner_id, region, created_at, default_profile_id, versioning
		FROM buckets
		WHERE name = ?
	`, name)

	var bucket types.BucketInfo
	var idStr string
	var region, profileID, versioning sql.NullString

	err := row.Scan(
		&idStr,
		&bucket.Name,
		&bucket.OwnerID,
		&region,
		&bucket.CreatedAt,
		&profileID,
		&versioning,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrBucketNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket: %w", err)
	}

	bucket.ID, _ = uuid.Parse(idStr)
	bucket.Region = region.String
	bucket.DefaultProfileID = profileID.String
	bucket.Versioning = versioning.String

	return &bucket, nil
}

func (v *Vitess) DeleteBucket(ctx context.Context, name string) error {
	result, err := v.db.ExecContext(ctx, `DELETE FROM buckets WHERE name = ?`, name)
	if err != nil {
		return fmt.Errorf("delete bucket: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return db.ErrBucketNotFound
	}
	return nil
}

func (v *Vitess) ListBuckets(ctx context.Context, params *db.ListBucketsParams) (*db.ListBucketsResult, error) {
	// Build query with filters
	query := `
		SELECT id, name, owner_id, region, created_at, default_profile_id, versioning
		FROM buckets
		WHERE owner_id = ?
	`
	args := []any{params.OwnerID}

	// Add prefix filter
	if params.Prefix != "" {
		query += " AND name LIKE ?"
		args = append(args, params.Prefix+"%")
	}

	// Add region filter
	if params.BucketRegion != "" {
		query += " AND region = ?"
		args = append(args, params.BucketRegion)
	}

	// Add continuation token (pagination marker)
	if params.ContinuationToken != "" {
		query += " AND name > ?"
		args = append(args, params.ContinuationToken)
	}

	// Order by name for consistent pagination
	query += " ORDER BY name"

	// Determine fetch limit (fetch one extra to detect truncation)
	maxBuckets := params.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = 10000
	}
	fetchLimit := maxBuckets + 1
	query += " LIMIT ?"
	args = append(args, fetchLimit)

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("list buckets v2: %w", err)
	}
	defer rows.Close()

	var buckets []*types.BucketInfo
	for rows.Next() {
		var bucket types.BucketInfo
		var idStr string
		var region, profileID, versioning sql.NullString

		err := rows.Scan(
			&idStr,
			&bucket.Name,
			&bucket.OwnerID,
			&region,
			&bucket.CreatedAt,
			&profileID,
			&versioning,
		)
		if err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}

		bucket.ID, _ = uuid.Parse(idStr)
		bucket.Region = region.String
		bucket.DefaultProfileID = profileID.String
		bucket.Versioning = versioning.String

		buckets = append(buckets, &bucket)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list buckets v2: %w", err)
	}

	// Check for truncation
	result := &db.ListBucketsResult{
		Buckets: buckets,
	}
	if len(buckets) > maxBuckets {
		result.IsTruncated = true
		result.Buckets = buckets[:maxBuckets]
		// Next continuation token is the name of the last bucket returned
		if len(result.Buckets) > 0 {
			result.NextContinuationToken = result.Buckets[len(result.Buckets)-1].Name
		}
	}

	return result, nil
}

// ============================================================================
// Helpers
// ============================================================================

// scanner is an interface for sql.Row and sql.Rows
type scanner interface {
	Scan(dest ...any) error
}

func scanObject(s scanner) (*types.ObjectRef, error) {
	var obj types.ObjectRef
	var idStr string
	var profileID sql.NullString
	var chunkRefsJSON, ecGroupIDsJSON []byte
	var isLatest int
	var sseAlgorithm, sseCustomerKeyMD5, sseKMSKeyID sql.NullString
	var sseKMSContext sql.NullString

	err := s.Scan(
		&idStr,
		&obj.Bucket,
		&obj.Key,
		&obj.Size,
		&obj.Version,
		&obj.ETag,
		&obj.CreatedAt,
		&obj.DeletedAt,
		&obj.TTL,
		&profileID,
		&chunkRefsJSON,
		&ecGroupIDsJSON,
		&isLatest,
		&sseAlgorithm,
		&sseCustomerKeyMD5,
		&sseKMSKeyID,
		&sseKMSContext,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrObjectNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan object: %w", err)
	}

	obj.ID, _ = uuid.Parse(idStr)
	obj.ProfileID = profileID.String
	obj.IsLatest = isLatest == 1

	if len(chunkRefsJSON) > 0 && string(chunkRefsJSON) != "null" {
		if err := json.Unmarshal(chunkRefsJSON, &obj.ChunkRefs); err != nil {
			return nil, fmt.Errorf("unmarshal chunk_refs: %w", err)
		}
	}

	if len(ecGroupIDsJSON) > 0 && string(ecGroupIDsJSON) != "null" {
		if err := json.Unmarshal(ecGroupIDsJSON, &obj.ECGroupIDs); err != nil {
			return nil, fmt.Errorf("unmarshal ec_group_ids: %w", err)
		}
	}

	// Set encryption fields
	obj.SSEAlgorithm = sseAlgorithm.String
	obj.SSECustomerKeyMD5 = sseCustomerKeyMD5.String
	obj.SSEKMSKeyID = sseKMSKeyID.String
	obj.SSEKMSContext = sseKMSContext.String

	return &obj, nil
}

func scanObjects(rows *sql.Rows) ([]*types.ObjectRef, error) {
	var objects []*types.ObjectRef
	for rows.Next() {
		obj, err := scanObject(rows)
		if err != nil {
			return nil, err
		}
		objects = append(objects, obj)
	}
	return objects, rows.Err()
}

// ============================================================================
// Multipart Upload Operations
// ============================================================================

func (v *Vitess) CreateMultipartUpload(ctx context.Context, upload *types.MultipartUpload) error {
	metadataJSON, err := json.Marshal(upload.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	_, err = v.db.ExecContext(ctx, `
		INSERT INTO multipart_uploads (id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		upload.ID.String(),
		upload.UploadID,
		upload.Bucket,
		upload.Key,
		upload.OwnerID,
		upload.Initiated,
		upload.ContentType,
		upload.StorageClass,
		string(metadataJSON),
	)
	if err != nil {
		return fmt.Errorf("create multipart upload: %w", err)
	}
	return nil
}

func (v *Vitess) GetMultipartUpload(ctx context.Context, bucket, key, uploadID string) (*types.MultipartUpload, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata
		FROM multipart_uploads
		WHERE upload_id = ? AND bucket = ? AND object_key = ?
	`, uploadID, bucket, key)

	return scanMultipartUpload(row)
}

func (v *Vitess) DeleteMultipartUpload(ctx context.Context, bucket, key, uploadID string) error {
	// Parts will be deleted by CASCADE
	_, err := v.db.ExecContext(ctx, `
		DELETE FROM multipart_uploads WHERE upload_id = ? AND bucket = ? AND object_key = ?
	`, uploadID, bucket, key)
	if err != nil {
		return fmt.Errorf("delete multipart upload: %w", err)
	}
	return nil
}

func (v *Vitess) ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker string, maxUploads int) ([]*types.MultipartUpload, bool, error) {
	query := `
		SELECT id, upload_id, bucket, object_key, owner_id, initiated, content_type, storage_class, metadata
		FROM multipart_uploads
		WHERE bucket = ?
	`
	args := []any{bucket}

	if prefix != "" {
		query += " AND object_key LIKE ?"
		args = append(args, prefix+"%")
	}

	if keyMarker != "" {
		if uploadIDMarker != "" {
			query += " AND (object_key > ? OR (object_key = ? AND upload_id > ?))"
			args = append(args, keyMarker, keyMarker, uploadIDMarker)
		} else {
			query += " AND object_key > ?"
			args = append(args, keyMarker)
		}
	}

	query += " ORDER BY object_key, upload_id"

	if maxUploads > 0 {
		query += " LIMIT ?"
		args = append(args, maxUploads+1)
	}

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list multipart uploads: %w", err)
	}
	defer rows.Close()

	var uploads []*types.MultipartUpload
	for rows.Next() {
		upload, err := scanMultipartUpload(rows)
		if err != nil {
			return nil, false, err
		}
		uploads = append(uploads, upload)
	}

	isTruncated := false
	if maxUploads > 0 && len(uploads) > maxUploads {
		uploads = uploads[:maxUploads]
		isTruncated = true
	}

	return uploads, isTruncated, nil
}

func (v *Vitess) PutPart(ctx context.Context, part *types.MultipartPart) error {
	chunkRefsJSON, err := json.Marshal(part.ChunkRefs)
	if err != nil {
		return fmt.Errorf("marshal chunk refs: %w", err)
	}

	_, err = v.db.ExecContext(ctx, `
		INSERT INTO multipart_parts (id, upload_id, part_number, size, etag, last_modified, chunk_refs)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			size = VALUES(size),
			etag = VALUES(etag),
			last_modified = VALUES(last_modified),
			chunk_refs = VALUES(chunk_refs)
	`,
		part.ID.String(),
		part.UploadID,
		part.PartNumber,
		part.Size,
		part.ETag,
		part.LastModified,
		string(chunkRefsJSON),
	)
	if err != nil {
		return fmt.Errorf("put part: %w", err)
	}
	return nil
}

func (v *Vitess) GetPart(ctx context.Context, uploadID string, partNumber int) (*types.MultipartPart, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = ? AND part_number = ?
	`, uploadID, partNumber)

	return scanPart(row)
}

func (v *Vitess) ListParts(ctx context.Context, uploadID string, partNumberMarker, maxParts int) ([]*types.MultipartPart, bool, error) {
	query := `
		SELECT id, upload_id, part_number, size, etag, last_modified, chunk_refs
		FROM multipart_parts
		WHERE upload_id = ? AND part_number > ?
		ORDER BY part_number
	`
	args := []any{uploadID, partNumberMarker}

	if maxParts > 0 {
		query += " LIMIT ?"
		args = append(args, maxParts+1)
	}

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, fmt.Errorf("list parts: %w", err)
	}
	defer rows.Close()

	var parts []*types.MultipartPart
	for rows.Next() {
		part, err := scanPart(rows)
		if err != nil {
			return nil, false, err
		}
		parts = append(parts, part)
	}

	isTruncated := false
	if maxParts > 0 && len(parts) > maxParts {
		parts = parts[:maxParts]
		isTruncated = true
	}

	return parts, isTruncated, nil
}

func (v *Vitess) DeleteParts(ctx context.Context, uploadID string) error {
	_, err := v.db.ExecContext(ctx, `DELETE FROM multipart_parts WHERE upload_id = ?`, uploadID)
	if err != nil {
		return fmt.Errorf("delete parts: %w", err)
	}
	return nil
}

// Scanner interface for multipart
type multipartScanner interface {
	Scan(dest ...any) error
}

func scanMultipartUpload(scanner multipartScanner) (*types.MultipartUpload, error) {
	var upload types.MultipartUpload
	var idStr string
	var contentType, storageClass sql.NullString
	var metadataJSON []byte

	err := scanner.Scan(
		&idStr,
		&upload.UploadID,
		&upload.Bucket,
		&upload.Key,
		&upload.OwnerID,
		&upload.Initiated,
		&contentType,
		&storageClass,
		&metadataJSON,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrUploadNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan multipart upload: %w", err)
	}

	upload.ID, _ = uuid.Parse(idStr)
	upload.ContentType = contentType.String
	upload.StorageClass = storageClass.String

	if len(metadataJSON) > 0 && string(metadataJSON) != "null" {
		if err := json.Unmarshal(metadataJSON, &upload.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &upload, nil
}

func scanPart(scanner multipartScanner) (*types.MultipartPart, error) {
	var part types.MultipartPart
	var idStr string
	var chunkRefsJSON []byte

	err := scanner.Scan(
		&idStr,
		&part.UploadID,
		&part.PartNumber,
		&part.Size,
		&part.ETag,
		&part.LastModified,
		&chunkRefsJSON,
	)
	if err == sql.ErrNoRows {
		return nil, db.ErrPartNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("scan part: %w", err)
	}

	part.ID, _ = uuid.Parse(idStr)

	if len(chunkRefsJSON) > 0 && string(chunkRefsJSON) != "null" {
		if err := json.Unmarshal(chunkRefsJSON, &part.ChunkRefs); err != nil {
			return nil, fmt.Errorf("unmarshal chunk refs: %w", err)
		}
	}

	return &part, nil
}

// ============================================================================
// Versioning Operations
// ============================================================================

func (v *Vitess) UpdateBucketVersioning(ctx context.Context, bucket string, versioning string) error {
	_, err := v.db.ExecContext(ctx, `
		UPDATE buckets SET versioning = ? WHERE name = ?
	`, versioning, bucket)
	if err != nil {
		return fmt.Errorf("update bucket versioning: %w", err)
	}
	return nil
}

func (v *Vitess) ListObjectVersions(ctx context.Context, bucket, prefix, keyMarker, versionIDMarker, delimiter string, maxKeys int) ([]*types.ObjectVersion, bool, string, string, error) {
	query := `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, profile_id, is_latest
		FROM objects
		WHERE bucket = ?
	`
	args := []any{bucket}

	if prefix != "" {
		query += " AND object_key LIKE ?"
		args = append(args, prefix+"%")
	}

	if keyMarker != "" {
		if versionIDMarker != "" {
			query += " AND (object_key > ? OR (object_key = ? AND id > ?))"
			args = append(args, keyMarker, keyMarker, versionIDMarker)
		} else {
			query += " AND object_key > ?"
			args = append(args, keyMarker)
		}
	}

	query += " ORDER BY object_key, created_at DESC"

	if maxKeys > 0 {
		query += " LIMIT ?"
		args = append(args, maxKeys+1)
	}

	rows, err := v.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, "", "", fmt.Errorf("list object versions: %w", err)
	}
	defer rows.Close()

	var versions []*types.ObjectVersion
	for rows.Next() {
		var idStr string
		var bucketName, key, etag string
		var size int64
		var version uint64
		var createdAt, deletedAt int64
		var profileID sql.NullString
		var isLatest int

		err := rows.Scan(&idStr, &bucketName, &key, &size, &version, &etag, &createdAt, &deletedAt, &profileID, &isLatest)
		if err != nil {
			return nil, false, "", "", fmt.Errorf("scan object version: %w", err)
		}

		versions = append(versions, &types.ObjectVersion{
			Key:            key,
			VersionID:      idStr,
			IsLatest:       isLatest == 1,
			IsDeleteMarker: deletedAt > 0,
			LastModified:   createdAt,
			ETag:           etag,
			Size:           size,
			StorageClass:   "STANDARD",
		})
	}

	// Check truncation
	isTruncated := false
	var nextKeyMarker, nextVersionIDMarker string
	if maxKeys > 0 && len(versions) > maxKeys {
		versions = versions[:maxKeys]
		isTruncated = true
		if len(versions) > 0 {
			last := versions[len(versions)-1]
			nextKeyMarker = last.Key
			nextVersionIDMarker = last.VersionID
		}
	}

	return versions, isTruncated, nextKeyMarker, nextVersionIDMarker, nil
}

func (v *Vitess) GetObjectVersion(ctx context.Context, bucket, key, versionID string) (*types.ObjectRef, error) {
	row := v.db.QueryRowContext(ctx, `
		SELECT id, bucket, object_key, size, version, etag, created_at, deleted_at, ttl, profile_id, chunk_refs, ec_group_ids, is_latest, sse_algorithm, sse_customer_key_md5, sse_kms_key_id, sse_kms_context
		FROM objects
		WHERE bucket = ? AND object_key = ? AND id = ?
	`, bucket, key, versionID)

	return scanObject(row)
}

func (v *Vitess) DeleteObjectVersion(ctx context.Context, bucket, key, versionID string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM objects WHERE bucket = ? AND object_key = ? AND id = ?
	`, bucket, key, versionID)
	if err != nil {
		return fmt.Errorf("delete object version: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrObjectNotFound
	}
	return nil
}

func (v *Vitess) PutDeleteMarker(ctx context.Context, bucket, key, ownerID string) (string, error) {
	id := uuid.New()
	now := time.Now().UnixNano()

	// Mark existing versions as not latest
	_, err := v.db.ExecContext(ctx, `
		UPDATE objects SET is_latest = 0 WHERE bucket = ? AND object_key = ? AND is_latest = 1
	`, bucket, key)
	if err != nil {
		return "", fmt.Errorf("mark old versions not latest: %w", err)
	}

	// Insert delete marker with is_latest = 1
	// A delete marker is an object with size=0, empty etag, and deleted_at > 0
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO objects (id, bucket, object_key, size, version, etag, created_at, deleted_at, is_latest)
		VALUES (?, ?, ?, 0, 1, '', ?, ?, 1)
	`, id.String(), bucket, key, now, now)
	if err != nil {
		return "", fmt.Errorf("put delete marker: %w", err)
	}

	return id.String(), nil
}

// ============================================================================
// ACL Operations
// ============================================================================

func (v *Vitess) GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error) {
	var aclJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT acl_json FROM bucket_acls WHERE bucket = ?
	`, bucket).Scan(&aclJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrACLNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket ACL: %w", err)
	}

	var acl s3types.AccessControlList
	if err := json.Unmarshal(aclJSON, &acl); err != nil {
		return nil, fmt.Errorf("unmarshal ACL: %w", err)
	}
	return &acl, nil
}

func (v *Vitess) SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error {
	aclJSON, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("marshal ACL: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_acls (bucket, acl_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE acl_json = VALUES(acl_json), updated_at = VALUES(updated_at)
	`, bucket, string(aclJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket ACL: %w", err)
	}
	return nil
}

func (v *Vitess) GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, error) {
	var aclJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT acl_json FROM object_acls WHERE bucket = ? AND object_key = ?
	`, bucket, key).Scan(&aclJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrACLNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object ACL: %w", err)
	}

	var acl s3types.AccessControlList
	if err := json.Unmarshal(aclJSON, &acl); err != nil {
		return nil, fmt.Errorf("unmarshal ACL: %w", err)
	}
	return &acl, nil
}

func (v *Vitess) SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error {
	aclJSON, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("marshal ACL: %w", err)
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO object_acls (id, bucket, object_key, acl_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE acl_json = VALUES(acl_json), updated_at = VALUES(updated_at)
	`, id, bucket, key, string(aclJSON), now, now)
	if err != nil {
		return fmt.Errorf("set object ACL: %w", err)
	}
	return nil
}

// ============================================================================
// Policy Operations
// ============================================================================

func (v *Vitess) GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, error) {
	var policyJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT policy_json FROM bucket_policies WHERE bucket = ?
	`, bucket).Scan(&policyJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrPolicyNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket policy: %w", err)
	}

	var policy s3types.BucketPolicy
	if err := json.Unmarshal(policyJSON, &policy); err != nil {
		return nil, fmt.Errorf("unmarshal policy: %w", err)
	}
	return &policy, nil
}

func (v *Vitess) SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_policies (bucket, policy_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE policy_json = VALUES(policy_json), updated_at = VALUES(updated_at)
	`, bucket, string(policyJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket policy: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_policies WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket policy: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrPolicyNotFound
	}
	return nil
}

// ============================================================================
// CORS Operations
// ============================================================================

func (v *Vitess) GetBucketCORS(ctx context.Context, bucket string) (*s3types.CORSConfiguration, error) {
	var corsJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT cors_json FROM bucket_cors WHERE bucket = ?
	`, bucket).Scan(&corsJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrCORSNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket CORS: %w", err)
	}

	var cors s3types.CORSConfiguration
	if err := json.Unmarshal(corsJSON, &cors); err != nil {
		return nil, fmt.Errorf("unmarshal CORS: %w", err)
	}
	return &cors, nil
}

func (v *Vitess) SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error {
	corsJSON, err := json.Marshal(cors)
	if err != nil {
		return fmt.Errorf("marshal CORS: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_cors (bucket, cors_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE cors_json = VALUES(cors_json), updated_at = VALUES(updated_at)
	`, bucket, string(corsJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket CORS: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteBucketCORS(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_cors WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket CORS: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrCORSNotFound
	}
	return nil
}

// ============================================================================
// Website Operations
// ============================================================================

func (v *Vitess) GetBucketWebsite(ctx context.Context, bucket string) (*s3types.WebsiteConfiguration, error) {
	var websiteJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT website_json FROM bucket_website WHERE bucket = ?
	`, bucket).Scan(&websiteJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrWebsiteNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket website: %w", err)
	}

	var website s3types.WebsiteConfiguration
	if err := json.Unmarshal(websiteJSON, &website); err != nil {
		return nil, fmt.Errorf("unmarshal website: %w", err)
	}
	return &website, nil
}

func (v *Vitess) SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error {
	websiteJSON, err := json.Marshal(website)
	if err != nil {
		return fmt.Errorf("marshal website: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_website (bucket, website_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE website_json = VALUES(website_json), updated_at = VALUES(updated_at)
	`, bucket, string(websiteJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket website: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_website WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket website: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrWebsiteNotFound
	}
	return nil
}

// ============================================================================
// Tagging Operations
// ============================================================================

func (v *Vitess) GetBucketTagging(ctx context.Context, bucket string) (*s3types.TagSet, error) {
	var tagsJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT tags_json FROM bucket_tagging WHERE bucket = ?
	`, bucket).Scan(&tagsJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrTaggingNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket tagging: %w", err)
	}

	var tagSet s3types.TagSet
	if err := json.Unmarshal(tagsJSON, &tagSet); err != nil {
		return nil, fmt.Errorf("unmarshal tagging: %w", err)
	}
	return &tagSet, nil
}

func (v *Vitess) SetBucketTagging(ctx context.Context, bucket string, tagSet *s3types.TagSet) error {
	tagsJSON, err := json.Marshal(tagSet)
	if err != nil {
		return fmt.Errorf("marshal tagging: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_tagging (bucket, tags_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE tags_json = VALUES(tags_json), updated_at = VALUES(updated_at)
	`, bucket, string(tagsJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket tagging: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteBucketTagging(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_tagging WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket tagging: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrTaggingNotFound
	}
	return nil
}

func (v *Vitess) GetObjectTagging(ctx context.Context, bucket, key string) (*s3types.TagSet, error) {
	var tagsJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT tags_json FROM object_tagging WHERE bucket = ? AND object_key = ?
	`, bucket, key).Scan(&tagsJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrTaggingNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object tagging: %w", err)
	}

	var tagSet s3types.TagSet
	if err := json.Unmarshal(tagsJSON, &tagSet); err != nil {
		return nil, fmt.Errorf("unmarshal tagging: %w", err)
	}
	return &tagSet, nil
}

func (v *Vitess) SetObjectTagging(ctx context.Context, bucket, key string, tagSet *s3types.TagSet) error {
	tagsJSON, err := json.Marshal(tagSet)
	if err != nil {
		return fmt.Errorf("marshal tagging: %w", err)
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO object_tagging (id, bucket, object_key, tags_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE tags_json = VALUES(tags_json), updated_at = VALUES(updated_at)
	`, id, bucket, key, string(tagsJSON), now, now)
	if err != nil {
		return fmt.Errorf("set object tagging: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteObjectTagging(ctx context.Context, bucket, key string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM object_tagging WHERE bucket = ? AND object_key = ?
	`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object tagging: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrTaggingNotFound
	}
	return nil
}

// ============================================================================
// Encryption Operations
// ============================================================================

func (v *Vitess) GetBucketEncryption(ctx context.Context, bucket string) (*s3types.ServerSideEncryptionConfig, error) {
	var encryptionJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT encryption_json FROM bucket_encryption WHERE bucket = ?
	`, bucket).Scan(&encryptionJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrEncryptionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket encryption: %w", err)
	}

	var config s3types.ServerSideEncryptionConfig
	if err := json.Unmarshal(encryptionJSON, &config); err != nil {
		return nil, fmt.Errorf("unmarshal encryption: %w", err)
	}
	return &config, nil
}

func (v *Vitess) SetBucketEncryption(ctx context.Context, bucket string, config *s3types.ServerSideEncryptionConfig) error {
	encryptionJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal encryption: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_encryption (bucket, encryption_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE encryption_json = VALUES(encryption_json), updated_at = VALUES(updated_at)
	`, bucket, string(encryptionJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket encryption: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_encryption WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket encryption: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrEncryptionNotFound
	}
	return nil
}

// ============================================================================
// Lifecycle Operations
// ============================================================================

func (v *Vitess) GetBucketLifecycle(ctx context.Context, bucket string) (*s3types.Lifecycle, error) {
	var lifecycleJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT lifecycle_json FROM bucket_lifecycle WHERE bucket = ?
	`, bucket).Scan(&lifecycleJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrLifecycleNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket lifecycle: %w", err)
	}

	var lifecycle s3types.Lifecycle
	if err := json.Unmarshal(lifecycleJSON, &lifecycle); err != nil {
		return nil, fmt.Errorf("unmarshal lifecycle: %w", err)
	}
	return &lifecycle, nil
}

func (v *Vitess) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error {
	lifecycleJSON, err := json.Marshal(lifecycle)
	if err != nil {
		return fmt.Errorf("marshal lifecycle: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_lifecycle (bucket, lifecycle_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE lifecycle_json = VALUES(lifecycle_json), updated_at = VALUES(updated_at)
	`, bucket, string(lifecycleJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket lifecycle: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_lifecycle WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket lifecycle: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrLifecycleNotFound
	}
	return nil
}

// ============================================================================
// Lifecycle Scanner State Operations
// ============================================================================

func (v *Vitess) GetScanState(ctx context.Context, bucket string) (*db.LifecycleScanState, error) {
	var state db.LifecycleScanState
	var scanStartedAt, scanCompletedAt int64
	var lastError sql.NullString

	err := v.db.QueryRowContext(ctx, `
		SELECT bucket, last_key, last_version_id, scan_started_at, scan_completed_at,
		       objects_scanned, actions_enqueued, last_error, consecutive_errors
		FROM lifecycle_scan_state WHERE bucket = ?
	`, bucket).Scan(
		&state.Bucket, &state.LastKey, &state.LastVersionID,
		&scanStartedAt, &scanCompletedAt,
		&state.ObjectsScanned, &state.ActionsEnqueued,
		&lastError, &state.ConsecutiveErrors,
	)

	if err == sql.ErrNoRows {
		return nil, db.ErrLifecycleScanStateNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get scan state: %w", err)
	}

	state.ScanStartedAt = time.Unix(0, scanStartedAt)
	state.ScanCompletedAt = time.Unix(0, scanCompletedAt)
	if lastError.Valid {
		state.LastError = lastError.String
	}

	return &state, nil
}

func (v *Vitess) UpdateScanState(ctx context.Context, state *db.LifecycleScanState) error {
	scanStartedAt := state.ScanStartedAt.UnixNano()
	scanCompletedAt := state.ScanCompletedAt.UnixNano()

	var lastError sql.NullString
	if state.LastError != "" {
		lastError = sql.NullString{String: state.LastError, Valid: true}
	}

	_, err := v.db.ExecContext(ctx, `
		INSERT INTO lifecycle_scan_state
			(bucket, last_key, last_version_id, scan_started_at, scan_completed_at,
			 objects_scanned, actions_enqueued, last_error, consecutive_errors)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			last_key = VALUES(last_key),
			last_version_id = VALUES(last_version_id),
			scan_started_at = VALUES(scan_started_at),
			scan_completed_at = VALUES(scan_completed_at),
			objects_scanned = VALUES(objects_scanned),
			actions_enqueued = VALUES(actions_enqueued),
			last_error = VALUES(last_error),
			consecutive_errors = VALUES(consecutive_errors)
	`, state.Bucket, state.LastKey, state.LastVersionID,
		scanStartedAt, scanCompletedAt,
		state.ObjectsScanned, state.ActionsEnqueued,
		lastError, state.ConsecutiveErrors)

	if err != nil {
		return fmt.Errorf("update scan state: %w", err)
	}
	return nil
}

func (v *Vitess) ListBucketsWithLifecycle(ctx context.Context) ([]string, error) {
	rows, err := v.db.QueryContext(ctx, `
		SELECT bucket FROM bucket_lifecycle
	`)
	if err != nil {
		return nil, fmt.Errorf("list buckets with lifecycle: %w", err)
	}
	defer rows.Close()

	var buckets []string
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		buckets = append(buckets, bucket)
	}
	return buckets, rows.Err()
}

func (v *Vitess) GetBucketsNeedingScan(ctx context.Context, minAge time.Duration, limit int) ([]string, error) {
	// Get buckets with lifecycle config that either:
	// 1. Have never been scanned (no entry in lifecycle_scan_state)
	// 2. Were last scanned more than minAge ago
	cutoff := time.Now().Add(-minAge).UnixNano()

	rows, err := v.db.QueryContext(ctx, `
		SELECT bl.bucket
		FROM bucket_lifecycle bl
		LEFT JOIN lifecycle_scan_state lss ON bl.bucket = lss.bucket
		WHERE lss.bucket IS NULL
		   OR lss.scan_completed_at < ?
		   OR lss.scan_completed_at = 0
		ORDER BY COALESCE(lss.scan_completed_at, 0) ASC
		LIMIT ?
	`, cutoff, limit)
	if err != nil {
		return nil, fmt.Errorf("get buckets needing scan: %w", err)
	}
	defer rows.Close()

	var buckets []string
	for rows.Next() {
		var bucket string
		if err := rows.Scan(&bucket); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		buckets = append(buckets, bucket)
	}
	return buckets, rows.Err()
}

func (v *Vitess) ResetScanState(ctx context.Context, bucket string) error {
	_, err := v.db.ExecContext(ctx, `
		DELETE FROM lifecycle_scan_state WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("reset scan state: %w", err)
	}
	return nil
}

// ============================================================================
// Object Lock Operations
// ============================================================================

func (v *Vitess) GetObjectLockConfiguration(ctx context.Context, bucket string) (*s3types.ObjectLockConfiguration, error) {
	var configJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT config_json FROM bucket_object_lock WHERE bucket = ?
	`, bucket).Scan(&configJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrObjectLockNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object lock config: %w", err)
	}

	var config s3types.ObjectLockConfiguration
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return nil, fmt.Errorf("unmarshal object lock config: %w", err)
	}
	return &config, nil
}

func (v *Vitess) SetObjectLockConfiguration(ctx context.Context, bucket string, config *s3types.ObjectLockConfiguration) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal object lock config: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_object_lock (bucket, config_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE config_json = VALUES(config_json), updated_at = VALUES(updated_at)
	`, bucket, string(configJSON), now, now)
	if err != nil {
		return fmt.Errorf("set object lock config: %w", err)
	}
	return nil
}

func (v *Vitess) GetObjectRetention(ctx context.Context, bucket, key string) (*s3types.ObjectLockRetention, error) {
	var mode, retainUntilDate string
	err := v.db.QueryRowContext(ctx, `
		SELECT mode, retain_until_date FROM object_retention WHERE bucket = ? AND object_key = ?
	`, bucket, key).Scan(&mode, &retainUntilDate)

	if err == sql.ErrNoRows {
		return nil, db.ErrRetentionNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object retention: %w", err)
	}

	return &s3types.ObjectLockRetention{
		Mode:            mode,
		RetainUntilDate: retainUntilDate,
	}, nil
}

func (v *Vitess) SetObjectRetention(ctx context.Context, bucket, key string, retention *s3types.ObjectLockRetention) error {
	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO object_retention (id, bucket, object_key, mode, retain_until_date, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE mode = VALUES(mode), retain_until_date = VALUES(retain_until_date), updated_at = VALUES(updated_at)
	`, id, bucket, key, retention.Mode, retention.RetainUntilDate, now, now)
	if err != nil {
		return fmt.Errorf("set object retention: %w", err)
	}
	return nil
}

func (v *Vitess) GetObjectLegalHold(ctx context.Context, bucket, key string) (*s3types.ObjectLockLegalHold, error) {
	var status string
	err := v.db.QueryRowContext(ctx, `
		SELECT status FROM object_legal_hold WHERE bucket = ? AND object_key = ?
	`, bucket, key).Scan(&status)

	if err == sql.ErrNoRows {
		return nil, db.ErrLegalHoldNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get object legal hold: %w", err)
	}

	return &s3types.ObjectLockLegalHold{
		Status: status,
	}, nil
}

func (v *Vitess) SetObjectLegalHold(ctx context.Context, bucket, key string, legalHold *s3types.ObjectLockLegalHold) error {
	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO object_legal_hold (id, bucket, object_key, status, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE status = VALUES(status), updated_at = VALUES(updated_at)
	`, id, bucket, key, legalHold.Status, now, now)
	if err != nil {
		return fmt.Errorf("set object legal hold: %w", err)
	}
	return nil
}

// ============================================================================
// Public Access Block Operations
// ============================================================================

func (v *Vitess) GetPublicAccessBlock(ctx context.Context, bucket string) (*s3types.PublicAccessBlockConfig, error) {
	var blockPublicAcls, ignorePublicAcls, blockPublicPolicy, restrictPublicBuckets bool
	err := v.db.QueryRowContext(ctx, `
		SELECT block_public_acls, ignore_public_acls, block_public_policy, restrict_public_buckets
		FROM bucket_public_access_block WHERE bucket = ?
	`, bucket).Scan(&blockPublicAcls, &ignorePublicAcls, &blockPublicPolicy, &restrictPublicBuckets)

	if err == sql.ErrNoRows {
		return nil, db.ErrPublicAccessBlockNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get public access block: %w", err)
	}

	return &s3types.PublicAccessBlockConfig{
		BlockPublicAcls:       blockPublicAcls,
		IgnorePublicAcls:      ignorePublicAcls,
		BlockPublicPolicy:     blockPublicPolicy,
		RestrictPublicBuckets: restrictPublicBuckets,
	}, nil
}

func (v *Vitess) SetPublicAccessBlock(ctx context.Context, bucket string, config *s3types.PublicAccessBlockConfig) error {
	now := time.Now().UnixNano()
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO bucket_public_access_block (bucket, block_public_acls, ignore_public_acls, block_public_policy, restrict_public_buckets, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			block_public_acls = VALUES(block_public_acls),
			ignore_public_acls = VALUES(ignore_public_acls),
			block_public_policy = VALUES(block_public_policy),
			restrict_public_buckets = VALUES(restrict_public_buckets),
			updated_at = VALUES(updated_at)
	`, bucket, config.BlockPublicAcls, config.IgnorePublicAcls, config.BlockPublicPolicy, config.RestrictPublicBuckets, now, now)
	if err != nil {
		return fmt.Errorf("set public access block: %w", err)
	}
	return nil
}

func (v *Vitess) DeletePublicAccessBlock(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_public_access_block WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete public access block: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrPublicAccessBlockNotFound
	}
	return nil
}

// ============================================================================
// Ownership Controls Operations
// ============================================================================

func (v *Vitess) GetOwnershipControls(ctx context.Context, bucket string) (*s3types.OwnershipControls, error) {
	var objectOwnership string
	err := v.db.QueryRowContext(ctx, `
		SELECT object_ownership FROM bucket_ownership_controls WHERE bucket = ?
	`, bucket).Scan(&objectOwnership)

	if err == sql.ErrNoRows {
		return nil, db.ErrOwnershipControlsNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get ownership controls: %w", err)
	}

	return &s3types.OwnershipControls{
		Rules: []s3types.OwnershipControlsRule{
			{ObjectOwnership: objectOwnership},
		},
	}, nil
}

func (v *Vitess) SetOwnershipControls(ctx context.Context, bucket string, controls *s3types.OwnershipControls) error {
	if len(controls.Rules) == 0 {
		return fmt.Errorf("ownership controls must have at least one rule")
	}

	objectOwnership := controls.Rules[0].ObjectOwnership
	now := time.Now().UnixNano()
	_, err := v.db.ExecContext(ctx, `
		INSERT INTO bucket_ownership_controls (bucket, object_ownership, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE object_ownership = VALUES(object_ownership), updated_at = VALUES(updated_at)
	`, bucket, objectOwnership, now, now)
	if err != nil {
		return fmt.Errorf("set ownership controls: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteOwnershipControls(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_ownership_controls WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete ownership controls: %w", err)
	}

	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrOwnershipControlsNotFound
	}
	return nil
}

