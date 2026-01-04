// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package sql

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/google/uuid"
)

// ============================================================================
// ACL Operations
// ============================================================================

func (s *Store) GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error) {
	var aclJSON []byte
	err := s.QueryRow(ctx, `SELECT acl_json FROM bucket_acls WHERE bucket = $1`, bucket).Scan(&aclJSON)
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

func (s *Store) SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error {
	aclJSON, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("marshal ACL: %w", err)
	}

	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_acls (bucket, acl_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4)%s
	`, s.dialect.UpsertSuffix("bucket", []string{"acl_json", "updated_at"}))

	_, err = s.Exec(ctx, query, bucket, string(aclJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket ACL: %w", err)
	}
	return nil
}

func (s *Store) GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, error) {
	var aclJSON []byte
	err := s.QueryRow(ctx, `SELECT acl_json FROM object_acls WHERE bucket = $1 AND object_key = $2`, bucket, key).Scan(&aclJSON)
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

func (s *Store) SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error {
	aclJSON, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("marshal ACL: %w", err)
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO object_acls (id, bucket, object_key, acl_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)%s
	`, s.dialect.UpsertSuffix("bucket, object_key", []string{"acl_json", "updated_at"}))

	_, err = s.Exec(ctx, query, id, bucket, key, string(aclJSON), now, now)
	if err != nil {
		return fmt.Errorf("set object ACL: %w", err)
	}
	return nil
}

// ============================================================================
// Policy Operations
// ============================================================================

func (s *Store) GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, error) {
	var policyJSON []byte
	err := s.QueryRow(ctx, `SELECT policy_json FROM bucket_policies WHERE bucket = $1`, bucket).Scan(&policyJSON)
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

func (s *Store) SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}

	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_policies (bucket, policy_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4)%s
	`, s.dialect.UpsertSuffix("bucket", []string{"policy_json", "updated_at"}))

	// Note: Pass now twice for created_at and updated_at since MySQL requires separate arguments for each ?
	_, err = s.Exec(ctx, query, bucket, string(policyJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket policy: %w", err)
	}
	return nil
}

func (s *Store) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	result, err := s.Exec(ctx, `DELETE FROM bucket_policies WHERE bucket = $1`, bucket)
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

func (s *Store) GetBucketCORS(ctx context.Context, bucket string) (*s3types.CORSConfiguration, error) {
	var corsJSON []byte
	err := s.QueryRow(ctx, `SELECT cors_json FROM bucket_cors WHERE bucket = $1`, bucket).Scan(&corsJSON)
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

func (s *Store) SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error {
	corsJSON, err := json.Marshal(cors)
	if err != nil {
		return fmt.Errorf("marshal CORS: %w", err)
	}

	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_cors (bucket, cors_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4)%s
	`, s.dialect.UpsertSuffix("bucket", []string{"cors_json", "updated_at"}))

	_, err = s.Exec(ctx, query, bucket, string(corsJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket CORS: %w", err)
	}
	return nil
}

func (s *Store) DeleteBucketCORS(ctx context.Context, bucket string) error {
	result, err := s.Exec(ctx, `DELETE FROM bucket_cors WHERE bucket = $1`, bucket)
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

func (s *Store) GetBucketWebsite(ctx context.Context, bucket string) (*s3types.WebsiteConfiguration, error) {
	var websiteJSON []byte
	err := s.QueryRow(ctx, `SELECT website_json FROM bucket_website WHERE bucket = $1`, bucket).Scan(&websiteJSON)
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

func (s *Store) SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error {
	websiteJSON, err := json.Marshal(website)
	if err != nil {
		return fmt.Errorf("marshal website: %w", err)
	}

	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_website (bucket, website_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4)%s
	`, s.dialect.UpsertSuffix("bucket", []string{"website_json", "updated_at"}))

	_, err = s.Exec(ctx, query, bucket, string(websiteJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket website: %w", err)
	}
	return nil
}

func (s *Store) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	result, err := s.Exec(ctx, `DELETE FROM bucket_website WHERE bucket = $1`, bucket)
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

func (s *Store) GetBucketTagging(ctx context.Context, bucket string) (*s3types.TagSet, error) {
	var tagsJSON []byte
	err := s.QueryRow(ctx, `SELECT tags_json FROM bucket_tagging WHERE bucket = $1`, bucket).Scan(&tagsJSON)
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

func (s *Store) SetBucketTagging(ctx context.Context, bucket string, tagSet *s3types.TagSet) error {
	tagsJSON, err := json.Marshal(tagSet)
	if err != nil {
		return fmt.Errorf("marshal tagging: %w", err)
	}

	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_tagging (bucket, tags_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4)%s
	`, s.dialect.UpsertSuffix("bucket", []string{"tags_json", "updated_at"}))

	_, err = s.Exec(ctx, query, bucket, string(tagsJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket tagging: %w", err)
	}
	return nil
}

func (s *Store) DeleteBucketTagging(ctx context.Context, bucket string) error {
	result, err := s.Exec(ctx, `DELETE FROM bucket_tagging WHERE bucket = $1`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket tagging: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrTaggingNotFound
	}
	return nil
}

func (s *Store) GetObjectTagging(ctx context.Context, bucket, key string) (*s3types.TagSet, error) {
	var tagsJSON []byte
	err := s.QueryRow(ctx, `SELECT tags_json FROM object_tagging WHERE bucket = $1 AND object_key = $2`, bucket, key).Scan(&tagsJSON)
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

func (s *Store) SetObjectTagging(ctx context.Context, bucket, key string, tagSet *s3types.TagSet) error {
	tagsJSON, err := json.Marshal(tagSet)
	if err != nil {
		return fmt.Errorf("marshal tagging: %w", err)
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO object_tagging (id, bucket, object_key, tags_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)%s
	`, s.dialect.UpsertSuffix("bucket, object_key", []string{"tags_json", "updated_at"}))

	_, err = s.Exec(ctx, query, id, bucket, key, string(tagsJSON), now, now)
	if err != nil {
		return fmt.Errorf("set object tagging: %w", err)
	}
	return nil
}

func (s *Store) DeleteObjectTagging(ctx context.Context, bucket, key string) error {
	result, err := s.Exec(ctx, `DELETE FROM object_tagging WHERE bucket = $1 AND object_key = $2`, bucket, key)
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

func (s *Store) GetBucketEncryption(ctx context.Context, bucket string) (*s3types.ServerSideEncryptionConfig, error) {
	var encryptionJSON []byte
	err := s.QueryRow(ctx, `SELECT encryption_json FROM bucket_encryption WHERE bucket = $1`, bucket).Scan(&encryptionJSON)
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

func (s *Store) SetBucketEncryption(ctx context.Context, bucket string, config *s3types.ServerSideEncryptionConfig) error {
	encryptionJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal encryption: %w", err)
	}

	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_encryption (bucket, encryption_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4)%s
	`, s.dialect.UpsertSuffix("bucket", []string{"encryption_json", "updated_at"}))

	_, err = s.Exec(ctx, query, bucket, string(encryptionJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket encryption: %w", err)
	}
	return nil
}

func (s *Store) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	result, err := s.Exec(ctx, `DELETE FROM bucket_encryption WHERE bucket = $1`, bucket)
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

func (s *Store) GetBucketLifecycle(ctx context.Context, bucket string) (*s3types.Lifecycle, error) {
	var lifecycleJSON []byte
	err := s.QueryRow(ctx, `SELECT lifecycle_json FROM bucket_lifecycle WHERE bucket = $1`, bucket).Scan(&lifecycleJSON)
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

func (s *Store) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error {
	lifecycleJSON, err := json.Marshal(lifecycle)
	if err != nil {
		return fmt.Errorf("marshal lifecycle: %w", err)
	}

	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_lifecycle (bucket, lifecycle_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4)%s
	`, s.dialect.UpsertSuffix("bucket", []string{"lifecycle_json", "updated_at"}))

	_, err = s.Exec(ctx, query, bucket, string(lifecycleJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket lifecycle: %w", err)
	}
	return nil
}

func (s *Store) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	result, err := s.Exec(ctx, `DELETE FROM bucket_lifecycle WHERE bucket = $1`, bucket)
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
// Replication Operations
// ============================================================================

func (s *Store) GetReplicationConfiguration(ctx context.Context, bucket string) (*s3types.ReplicationConfiguration, error) {
	var configJSON []byte
	err := s.QueryRow(ctx, `SELECT config FROM bucket_replication WHERE bucket = $1`, bucket).Scan(&configJSON)
	if err == sql.ErrNoRows {
		return nil, db.ErrReplicationNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get replication config: %w", err)
	}

	var config s3types.ReplicationConfiguration
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return nil, fmt.Errorf("unmarshal replication config: %w", err)
	}
	return &config, nil
}

func (s *Store) SetReplicationConfiguration(ctx context.Context, bucket string, config *s3types.ReplicationConfiguration) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal replication config: %w", err)
	}

	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_replication (bucket, config, created_at, updated_at)
		VALUES ($1, $2, $3, $4)%s
	`, s.dialect.UpsertSuffix("bucket", []string{"config", "updated_at"}))

	_, err = s.Exec(ctx, query, bucket, string(configJSON), now, now)
	if err != nil {
		return fmt.Errorf("set replication config: %w", err)
	}
	return nil
}

func (s *Store) DeleteReplicationConfiguration(ctx context.Context, bucket string) error {
	result, err := s.Exec(ctx, `DELETE FROM bucket_replication WHERE bucket = $1`, bucket)
	if err != nil {
		return fmt.Errorf("delete replication config: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrReplicationNotFound
	}
	return nil
}

// ============================================================================
// Intelligent Tiering Operations
// ============================================================================

func (s *Store) GetIntelligentTieringConfiguration(ctx context.Context, bucket, configID string) (*s3types.IntelligentTieringConfiguration, error) {
	var configJSON []byte
	err := s.QueryRow(ctx, `SELECT config_json FROM bucket_intelligent_tiering WHERE bucket = $1 AND config_id = $2`, bucket, configID).Scan(&configJSON)
	if err == sql.ErrNoRows {
		return nil, db.ErrIntelligentTieringNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get intelligent tiering config: %w", err)
	}

	var config s3types.IntelligentTieringConfiguration
	if err := json.Unmarshal(configJSON, &config); err != nil {
		return nil, fmt.Errorf("unmarshal intelligent tiering config: %w", err)
	}
	return &config, nil
}

func (s *Store) PutIntelligentTieringConfiguration(ctx context.Context, bucket string, config *s3types.IntelligentTieringConfiguration) error {
	configJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal intelligent tiering config: %w", err)
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()
	query := fmt.Sprintf(`
		INSERT INTO bucket_intelligent_tiering (id, bucket, config_id, config_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)%s
	`, s.dialect.UpsertSuffix("bucket, config_id", []string{"config_json", "updated_at"}))

	_, err = s.Exec(ctx, query, id, bucket, config.ID, string(configJSON), now, now)
	if err != nil {
		return fmt.Errorf("set intelligent tiering config: %w", err)
	}
	return nil
}

func (s *Store) DeleteIntelligentTieringConfiguration(ctx context.Context, bucket, configID string) error {
	result, err := s.Exec(ctx, `DELETE FROM bucket_intelligent_tiering WHERE bucket = $1 AND config_id = $2`, bucket, configID)
	if err != nil {
		return fmt.Errorf("delete intelligent tiering config: %w", err)
	}
	affected, _ := result.RowsAffected()
	if affected == 0 {
		return db.ErrIntelligentTieringNotFound
	}
	return nil
}

func (s *Store) ListIntelligentTieringConfigurations(ctx context.Context, bucket string) ([]*s3types.IntelligentTieringConfiguration, error) {
	rows, err := s.Query(ctx, `SELECT config_json FROM bucket_intelligent_tiering WHERE bucket = $1 ORDER BY config_id`, bucket)
	if err != nil {
		return nil, fmt.Errorf("list intelligent tiering configs: %w", err)
	}
	defer rows.Close()

	var configs []*s3types.IntelligentTieringConfiguration
	for rows.Next() {
		var configJSON []byte
		if err := rows.Scan(&configJSON); err != nil {
			return nil, fmt.Errorf("scan intelligent tiering config: %w", err)
		}

		var config s3types.IntelligentTieringConfiguration
		if err := json.Unmarshal(configJSON, &config); err != nil {
			return nil, fmt.Errorf("unmarshal intelligent tiering config: %w", err)
		}
		configs = append(configs, &config)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}
	return configs, nil
}
