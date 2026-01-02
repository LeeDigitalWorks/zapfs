// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package backend

import (
	"bytes"
	"context"
	"fmt"
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func init() {
	Register(types.StorageTypeS3, NewS3)
}

// S3 implements BackendStorage for S3-compatible storage
type S3 struct {
	client *s3.Client
	bucket string
}

// NewS3 creates an S3 backend
func NewS3(cfg types.BackendConfig) (types.BackendStorage, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("bucket required for S3 backend")
	}

	opts := []func(*config.LoadOptions) error{}

	if cfg.Region != "" {
		opts = append(opts, config.WithRegion(cfg.Region))
	}

	if cfg.AccessKey != "" && cfg.SecretKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, ""),
		))
	}

	awsCfg, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}

	// Build S3 client options
	s3Opts := []func(*s3.Options){}
	if cfg.Endpoint != "" {
		s3Opts = append(s3Opts, func(o *s3.Options) {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
			o.UsePathStyle = true
		})
	}

	return &S3{
		client: s3.NewFromConfig(awsCfg, s3Opts...),
		bucket: cfg.Bucket,
	}, nil
}

func (s *S3) Type() types.StorageType {
	return types.StorageTypeS3
}

func (s *S3) Write(ctx context.Context, key string, data io.Reader, size int64) error {
	// For simplicity, buffer the data (production should use multipart for large objects)
	buf, err := io.ReadAll(data)
	if err != nil {
		return fmt.Errorf("read data: %w", err)
	}

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(buf),
		ContentLength: aws.Int64(int64(len(buf))),
	})
	if err != nil {
		return fmt.Errorf("put object: %w", err)
	}
	return nil
}

func (s *S3) Read(ctx context.Context, key string) (io.ReadCloser, error) {
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, fmt.Errorf("get object: %w", err)
	}
	return out.Body, nil
}

func (s *S3) ReadRange(ctx context.Context, key string, offset, length int64) (io.ReadCloser, error) {
	rangeStr := fmt.Sprintf("bytes=%d-%d", offset, offset+length-1)
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Range:  aws.String(rangeStr),
	})
	if err != nil {
		return nil, fmt.Errorf("get object range: %w", err)
	}
	return out.Body, nil
}

func (s *S3) Delete(ctx context.Context, key string) error {
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("delete object: %w", err)
	}
	return nil
}

func (s *S3) Exists(ctx context.Context, key string) (bool, error) {
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		// Check if it's a not found error
		return false, nil // Simplified - should check actual error type
	}
	return true, nil
}

func (s *S3) Size(ctx context.Context, key string) (int64, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return 0, fmt.Errorf("head object: %w", err)
	}
	if out.ContentLength == nil {
		return 0, nil
	}
	return *out.ContentLength, nil
}

func (s *S3) Close() error {
	return nil
}
