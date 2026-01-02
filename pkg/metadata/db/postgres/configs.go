// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

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

func (p *Postgres) GetBucketACL(ctx context.Context, bucket string) (*s3types.AccessControlList, error) {
	var aclJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT acl_json FROM bucket_acls WHERE bucket = $1
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

func (p *Postgres) SetBucketACL(ctx context.Context, bucket string, acl *s3types.AccessControlList) error {
	aclJSON, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("marshal ACL: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO bucket_acls (bucket, acl_json, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (bucket) DO UPDATE SET acl_json = EXCLUDED.acl_json, updated_at = EXCLUDED.updated_at
	`, bucket, string(aclJSON), now)
	if err != nil {
		return fmt.Errorf("set bucket ACL: %w", err)
	}
	return nil
}

func (p *Postgres) GetObjectACL(ctx context.Context, bucket, key string) (*s3types.AccessControlList, error) {
	var aclJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT acl_json FROM object_acls WHERE bucket = $1 AND object_key = $2
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

func (p *Postgres) SetObjectACL(ctx context.Context, bucket, key string, acl *s3types.AccessControlList) error {
	aclJSON, err := json.Marshal(acl)
	if err != nil {
		return fmt.Errorf("marshal ACL: %w", err)
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO object_acls (id, bucket, object_key, acl_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5)
		ON CONFLICT (bucket, object_key) DO UPDATE SET acl_json = EXCLUDED.acl_json, updated_at = EXCLUDED.updated_at
	`, id, bucket, key, string(aclJSON), now)
	if err != nil {
		return fmt.Errorf("set object ACL: %w", err)
	}
	return nil
}

// ============================================================================
// Policy Operations
// ============================================================================

func (p *Postgres) GetBucketPolicy(ctx context.Context, bucket string) (*s3types.BucketPolicy, error) {
	var policyJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT policy_json FROM bucket_policies WHERE bucket = $1
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

func (p *Postgres) SetBucketPolicy(ctx context.Context, bucket string, policy *s3types.BucketPolicy) error {
	policyJSON, err := json.Marshal(policy)
	if err != nil {
		return fmt.Errorf("marshal policy: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO bucket_policies (bucket, policy_json, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (bucket) DO UPDATE SET policy_json = EXCLUDED.policy_json, updated_at = EXCLUDED.updated_at
	`, bucket, string(policyJSON), now)
	if err != nil {
		return fmt.Errorf("set bucket policy: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteBucketPolicy(ctx context.Context, bucket string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM bucket_policies WHERE bucket = $1
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

func (p *Postgres) GetBucketCORS(ctx context.Context, bucket string) (*s3types.CORSConfiguration, error) {
	var corsJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT cors_json FROM bucket_cors WHERE bucket = $1
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

func (p *Postgres) SetBucketCORS(ctx context.Context, bucket string, cors *s3types.CORSConfiguration) error {
	corsJSON, err := json.Marshal(cors)
	if err != nil {
		return fmt.Errorf("marshal CORS: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO bucket_cors (bucket, cors_json, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (bucket) DO UPDATE SET cors_json = EXCLUDED.cors_json, updated_at = EXCLUDED.updated_at
	`, bucket, string(corsJSON), now)
	if err != nil {
		return fmt.Errorf("set bucket CORS: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteBucketCORS(ctx context.Context, bucket string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM bucket_cors WHERE bucket = $1
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

func (p *Postgres) GetBucketWebsite(ctx context.Context, bucket string) (*s3types.WebsiteConfiguration, error) {
	var websiteJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT website_json FROM bucket_website WHERE bucket = $1
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

func (p *Postgres) SetBucketWebsite(ctx context.Context, bucket string, website *s3types.WebsiteConfiguration) error {
	websiteJSON, err := json.Marshal(website)
	if err != nil {
		return fmt.Errorf("marshal website: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO bucket_website (bucket, website_json, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (bucket) DO UPDATE SET website_json = EXCLUDED.website_json, updated_at = EXCLUDED.updated_at
	`, bucket, string(websiteJSON), now)
	if err != nil {
		return fmt.Errorf("set bucket website: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteBucketWebsite(ctx context.Context, bucket string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM bucket_website WHERE bucket = $1
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

func (p *Postgres) GetBucketTagging(ctx context.Context, bucket string) (*s3types.TagSet, error) {
	var tagsJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT tags_json FROM bucket_tagging WHERE bucket = $1
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

func (p *Postgres) SetBucketTagging(ctx context.Context, bucket string, tagSet *s3types.TagSet) error {
	tagsJSON, err := json.Marshal(tagSet)
	if err != nil {
		return fmt.Errorf("marshal tagging: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO bucket_tagging (bucket, tags_json, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (bucket) DO UPDATE SET tags_json = EXCLUDED.tags_json, updated_at = EXCLUDED.updated_at
	`, bucket, string(tagsJSON), now)
	if err != nil {
		return fmt.Errorf("set bucket tagging: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteBucketTagging(ctx context.Context, bucket string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM bucket_tagging WHERE bucket = $1
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

func (p *Postgres) GetObjectTagging(ctx context.Context, bucket, key string) (*s3types.TagSet, error) {
	var tagsJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT tags_json FROM object_tagging WHERE bucket = $1 AND object_key = $2
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

func (p *Postgres) SetObjectTagging(ctx context.Context, bucket, key string, tagSet *s3types.TagSet) error {
	tagsJSON, err := json.Marshal(tagSet)
	if err != nil {
		return fmt.Errorf("marshal tagging: %w", err)
	}

	id := uuid.New().String()
	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO object_tagging (id, bucket, object_key, tags_json, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $5)
		ON CONFLICT (bucket, object_key) DO UPDATE SET tags_json = EXCLUDED.tags_json, updated_at = EXCLUDED.updated_at
	`, id, bucket, key, string(tagsJSON), now)
	if err != nil {
		return fmt.Errorf("set object tagging: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteObjectTagging(ctx context.Context, bucket, key string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM object_tagging WHERE bucket = $1 AND object_key = $2
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

func (p *Postgres) GetBucketEncryption(ctx context.Context, bucket string) (*s3types.ServerSideEncryptionConfig, error) {
	var encryptionJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT encryption_json FROM bucket_encryption WHERE bucket = $1
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

func (p *Postgres) SetBucketEncryption(ctx context.Context, bucket string, config *s3types.ServerSideEncryptionConfig) error {
	encryptionJSON, err := json.Marshal(config)
	if err != nil {
		return fmt.Errorf("marshal encryption: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO bucket_encryption (bucket, encryption_json, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (bucket) DO UPDATE SET encryption_json = EXCLUDED.encryption_json, updated_at = EXCLUDED.updated_at
	`, bucket, string(encryptionJSON), now)
	if err != nil {
		return fmt.Errorf("set bucket encryption: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteBucketEncryption(ctx context.Context, bucket string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM bucket_encryption WHERE bucket = $1
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

func (p *Postgres) GetBucketLifecycle(ctx context.Context, bucket string) (*s3types.Lifecycle, error) {
	var lifecycleJSON []byte
	err := p.db.QueryRowContext(ctx, `
		SELECT lifecycle_json FROM bucket_lifecycle WHERE bucket = $1
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

func (p *Postgres) SetBucketLifecycle(ctx context.Context, bucket string, lifecycle *s3types.Lifecycle) error {
	lifecycleJSON, err := json.Marshal(lifecycle)
	if err != nil {
		return fmt.Errorf("marshal lifecycle: %w", err)
	}

	now := time.Now().UnixNano()
	_, err = p.db.ExecContext(ctx, `
		INSERT INTO bucket_lifecycle (bucket, lifecycle_json, created_at, updated_at)
		VALUES ($1, $2, $3, $3)
		ON CONFLICT (bucket) DO UPDATE SET lifecycle_json = EXCLUDED.lifecycle_json, updated_at = EXCLUDED.updated_at
	`, bucket, string(lifecycleJSON), now)
	if err != nil {
		return fmt.Errorf("set bucket lifecycle: %w", err)
	}
	return nil
}

func (p *Postgres) DeleteBucketLifecycle(ctx context.Context, bucket string) error {
	result, err := p.db.ExecContext(ctx, `
		DELETE FROM bucket_lifecycle WHERE bucket = $1
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
