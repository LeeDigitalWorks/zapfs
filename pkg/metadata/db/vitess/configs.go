// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

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

func (v *Vitess) GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error) {
	var aclJSON []byte
	err := v.db.QueryRowContext(ctx, `
		SELECT acl_json FROM bucket_acls WHERE bucket = ?
	`, bucket).Scan(&aclJSON)

	if err == sql.ErrNoRows {
		return nil, db.ErrACLNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("get bucket acl: %w", err)
	}

	var acl s3types.AccessControlList
	if err := json.Unmarshal(aclJSON, &acl); err != nil {
		return nil, fmt.Errorf("unmarshal bucket acl: %w", err)
	}
	return &acl, nil
}

func (v *Vitess) SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error {
	aclJSON, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("marshal bucket acl: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_acls (bucket, acl_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE acl_json = VALUES(acl_json), updated_at = VALUES(updated_at)
	`, bucket, string(aclJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket acl: %w", err)
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
		return nil, fmt.Errorf("get object acl: %w", err)
	}

	var acl s3types.AccessControlList
	if err := json.Unmarshal(aclJSON, &acl); err != nil {
		return nil, fmt.Errorf("unmarshal object acl: %w", err)
	}
	return &acl, nil
}

func (v *Vitess) SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error {
	aclJSON, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("marshal object acl: %w", err)
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO object_acls (id, bucket, object_key, acl_json, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE acl_json = VALUES(acl_json), updated_at = VALUES(updated_at)
	`, id, bucket, key, string(aclJSON), now, now)
	if err != nil {
		return fmt.Errorf("set object acl: %w", err)
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
		return nil, fmt.Errorf("unmarshal bucket policy: %w", err)
	}
	return &policy, nil
}

func (v *Vitess) SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal bucket policy: %w", err)
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
		return nil, fmt.Errorf("get bucket cors: %w", err)
	}

	var cors s3types.CORSConfiguration
	if err := json.Unmarshal(corsJSON, &cors); err != nil {
		return nil, fmt.Errorf("unmarshal bucket cors: %w", err)
	}
	return &cors, nil
}

func (v *Vitess) SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error {
	corsJSON, err := json.Marshal(cors)
	if err != nil {
		return fmt.Errorf("marshal bucket cors: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = v.db.ExecContext(ctx, `
		INSERT INTO bucket_cors (bucket, cors_json, created_at, updated_at)
		VALUES (?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE cors_json = VALUES(cors_json), updated_at = VALUES(updated_at)
	`, bucket, string(corsJSON), now, now)
	if err != nil {
		return fmt.Errorf("set bucket cors: %w", err)
	}
	return nil
}

func (v *Vitess) DeleteBucketCORS(ctx context.Context, bucket string) error {
	result, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_cors WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket cors: %w", err)
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
		return nil, fmt.Errorf("unmarshal bucket website: %w", err)
	}
	return &website, nil
}

func (v *Vitess) SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error {
	websiteJSON, err := json.Marshal(website)
	if err != nil {
		return fmt.Errorf("marshal bucket website: %w", err)
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
		return nil, fmt.Errorf("unmarshal bucket tagging: %w", err)
	}
	return &tagSet, nil
}

func (v *Vitess) SetBucketTagging(ctx context.Context, bucket string, tagSet *s3types.TagSet) error {
	tagsJSON, err := json.Marshal(tagSet)
	if err != nil {
		return fmt.Errorf("marshal bucket tagging: %w", err)
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
	_, err := v.db.ExecContext(ctx, `
		DELETE FROM bucket_tagging WHERE bucket = ?
	`, bucket)
	if err != nil {
		return fmt.Errorf("delete bucket tagging: %w", err)
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
		return nil, fmt.Errorf("unmarshal object tagging: %w", err)
	}
	return &tagSet, nil
}

func (v *Vitess) SetObjectTagging(ctx context.Context, bucket, key string, tagSet *s3types.TagSet) error {
	tagsJSON, err := json.Marshal(tagSet)
	if err != nil {
		return fmt.Errorf("marshal object tagging: %w", err)
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
	_, err := v.db.ExecContext(ctx, `
		DELETE FROM object_tagging WHERE bucket = ? AND object_key = ?
	`, bucket, key)
	if err != nil {
		return fmt.Errorf("delete object tagging: %w", err)
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
		return nil, fmt.Errorf("unmarshal bucket encryption: %w", err)
	}
	return &config, nil
}

func (v *Vitess) SetBucketEncryption(ctx context.Context, bucket string, config *s3types.ServerSideEncryptionConfig) error {
	encryptionJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal bucket encryption: %w", err)
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
		return nil, fmt.Errorf("unmarshal bucket lifecycle: %w", err)
	}
	return &lifecycle, nil
}

func (v *Vitess) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error {
	lifecycleJSON, err := json.Marshal(lifecycle)
	if err != nil {
		return fmt.Errorf("marshal bucket lifecycle: %w", err)
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
