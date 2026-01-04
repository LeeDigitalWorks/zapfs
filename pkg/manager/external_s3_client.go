// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"fmt"
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

// ExternalS3ClientPool manages a pool of S3 clients for different endpoints.
// This is used by the Manager to verify external bucket credentials and perform
// bucket operations during federation registration.
type ExternalS3ClientPool struct {
	*s3client.Pool
}

// NewExternalS3ClientPool creates a new client pool with the given timeout and max idle connections.
func NewExternalS3ClientPool(timeout time.Duration, maxIdleConns int) *ExternalS3ClientPool {
	return &ExternalS3ClientPool{
		Pool: s3client.NewPool(timeout, maxIdleConns),
	}
}

// HeadBucket checks if a bucket exists and the credentials are valid.
// Returns nil if successful, error otherwise.
func (p *ExternalS3ClientPool) HeadBucket(ctx context.Context, cfg *ExternalS3Config, bucket string) error {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return fmt.Errorf("get client: %w", err)
	}

	_, err = client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("head bucket %s: %w", bucket, err)
	}

	return nil
}

// DeleteBucket deletes a bucket on the external S3 service.
// Returns nil if successful or if bucket doesn't exist (404).
func (p *ExternalS3ClientPool) DeleteBucket(ctx context.Context, cfg *ExternalS3Config, bucket string) error {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return fmt.Errorf("get client: %w", err)
	}

	_, err = client.DeleteBucket(ctx, &s3.DeleteBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("delete bucket %s: %w", bucket, err)
	}

	return nil
}

// ListObjects lists objects in a bucket (for discovery during migration).
// Returns object keys and a continuation token for pagination.
func (p *ExternalS3ClientPool) ListObjects(ctx context.Context, cfg *ExternalS3Config, bucket, prefix, startAfter string, maxKeys int32) ([]string, string, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return nil, "", fmt.Errorf("get client: %w", err)
	}

	input := &s3.ListObjectsV2Input{
		Bucket:  aws.String(bucket),
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
		return nil, "", fmt.Errorf("list objects: %w", err)
	}

	var keys []string
	for _, obj := range output.Contents {
		if obj.Key != nil {
			keys = append(keys, *obj.Key)
		}
	}

	var nextToken string
	if output.NextContinuationToken != nil {
		nextToken = *output.NextContinuationToken
	}

	return keys, nextToken, nil
}

// CountObjects returns the number of objects in a bucket.
// This is a rough count for migration progress estimation.
func (p *ExternalS3ClientPool) CountObjects(ctx context.Context, cfg *ExternalS3Config, bucket string) (int64, error) {
	client, err := p.Pool.GetClient(ctx, cfg.toPoolConfig())
	if err != nil {
		return 0, fmt.Errorf("get client: %w", err)
	}

	var count int64
	var continuationToken *string

	for {
		input := &s3.ListObjectsV2Input{
			Bucket:            aws.String(bucket),
			MaxKeys:           aws.Int32(1000),
			ContinuationToken: continuationToken,
		}

		output, err := client.ListObjectsV2(ctx, input)
		if err != nil {
			return count, fmt.Errorf("list objects: %w", err)
		}

		count += int64(len(output.Contents))

		if output.IsTruncated == nil || !*output.IsTruncated {
			break
		}
		continuationToken = output.NextContinuationToken
	}

	return count, nil
}
