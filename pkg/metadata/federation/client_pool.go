// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package federation

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3client"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ExternalS3Config holds configuration for connecting to an external S3 service.
type ExternalS3Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	Bucket          string
	PathStyle       bool
}

// toPoolConfig converts to the shared s3client.Config.
func (c *ExternalS3Config) toPoolConfig() *s3client.Config {
	return &s3client.Config{
		Endpoint:        c.Endpoint,
		Region:          c.Region,
		AccessKeyID:     c.AccessKeyID,
		SecretAccessKey: c.SecretAccessKey,
		PathStyle:       c.PathStyle,
	}
}

// ClientPool manages a pool of S3 clients for different external endpoints.
// This is used by the Metadata service for proxying requests and lazy migration.
type ClientPool struct {
	*s3client.Pool
}

// NewClientPool creates a new client pool with the given timeout and max idle connections.
func NewClientPool(timeout time.Duration, maxIdleConns int) *ClientPool {
	return &ClientPool{
		Pool: s3client.NewPool(timeout, maxIdleConns),
	}
}

// GetClient returns an S3 client configured for the given external S3 config.
// This is a convenience wrapper that converts ExternalS3Config to the shared s3client.Config.
func (p *ClientPool) GetClient(ctx context.Context, cfg *ExternalS3Config) (*s3.Client, error) {
	return p.Pool.GetClient(ctx, cfg.toPoolConfig())
}

// GetObject retrieves an object from the external S3 bucket.
func (p *ClientPool) GetObject(ctx context.Context, cfg *ExternalS3Config, key string) (*s3.GetObjectOutput, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	output, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object %s: %w", key, err)
	}

	return output, nil
}

// GetObjectWithVersion retrieves a specific version of an object from external S3.
func (p *ClientPool) GetObjectWithVersion(ctx context.Context, cfg *ExternalS3Config, key, versionID string) (*s3.GetObjectOutput, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	input := &s3.GetObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(key),
	}
	if versionID != "" {
		input.VersionId = aws.String(versionID)
	}

	output, err := client.GetObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("get object %s (version %s): %w", key, versionID, err)
	}

	return output, nil
}

// HeadObject checks if an object exists and returns its metadata.
func (p *ClientPool) HeadObject(ctx context.Context, cfg *ExternalS3Config, key string) (*s3.HeadObjectOutput, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	output, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("head object %s: %w", key, err)
	}

	return output, nil
}

// PutObject uploads an object to the external S3 bucket.
func (p *ClientPool) PutObject(ctx context.Context, cfg *ExternalS3Config, key string, body io.Reader, contentLength int64, contentType string) (*s3.PutObjectOutput, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	input := &s3.PutObjectInput{
		Bucket:        aws.String(cfg.Bucket),
		Key:           aws.String(key),
		Body:          body,
		ContentLength: aws.Int64(contentLength),
	}
	if contentType != "" {
		input.ContentType = aws.String(contentType)
	}

	output, err := client.PutObject(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("put object %s: %w", key, err)
	}

	return output, nil
}

// DeleteObject deletes an object from the external S3 bucket.
func (p *ClientPool) DeleteObject(ctx context.Context, cfg *ExternalS3Config, key string) (*s3.DeleteObjectOutput, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	output, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("delete object %s: %w", key, err)
	}

	return output, nil
}

// ListObjects lists objects in the external S3 bucket.
func (p *ClientPool) ListObjects(ctx context.Context, cfg *ExternalS3Config, prefix, startAfter string, maxKeys int32) (*s3.ListObjectsV2Output, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(cfg.Bucket),
		MaxKeys: aws.Int32(maxKeys),
	}
	if prefix != "" {
		input.Prefix = aws.String(prefix)
	}
	if startAfter != "" {
		input.StartAfter = aws.String(startAfter)
	}

	output, err := client.ListObjectsV2(ctx, input)
	if err != nil {
		return nil, fmt.Errorf("list objects: %w", err)
	}

	return output, nil
}

// CopyObject copies an object within the external S3 bucket.
func (p *ClientPool) CopyObject(ctx context.Context, cfg *ExternalS3Config, sourceKey, destKey string) (*s3.CopyObjectOutput, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return nil, fmt.Errorf("get client: %w", err)
	}

	copySource := fmt.Sprintf("%s/%s", cfg.Bucket, sourceKey)
	output, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     aws.String(cfg.Bucket),
		Key:        aws.String(destKey),
		CopySource: aws.String(copySource),
	})
	if err != nil {
		return nil, fmt.Errorf("copy object %s to %s: %w", sourceKey, destKey, err)
	}

	return output, nil
}
