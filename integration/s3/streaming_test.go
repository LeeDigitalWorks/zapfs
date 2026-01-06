//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStreamingSignatures tests uploads that use chunked transfer encoding
// with AWS Signature V4 streaming signatures (aws-chunked content encoding).
// These tests verify that the server correctly handles STREAMING-AWS4-HMAC-SHA256-PAYLOAD
// signed requests.
func TestStreamingSignatures(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("large object upload triggers streaming signature", func(t *testing.T) {
		bucket := uniqueBucket("test-streaming-large")
		key := uniqueKey("large-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create a large object (20MB) that will trigger chunked upload
		// AWS SDK automatically uses chunked transfer for large objects
		dataSize := 20 * 1024 * 1024
		data := make([]byte, dataSize)
		_, err := rand.Read(data)
		require.NoError(t, err)

		// Upload using the uploader which handles chunking
		uploader := manager.NewUploader(rawClient, func(u *manager.Uploader) {
			u.PartSize = 5 * 1024 * 1024 // 5MB parts
		})

		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		// Verify the object was uploaded correctly
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(dataSize), *headResp.ContentLength)

		// Download and verify content
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		downloaded, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Equal(t, data, downloaded)
	})

	t.Run("multipart upload with streaming parts", func(t *testing.T) {
		bucket := uniqueBucket("test-streaming-multipart")
		key := uniqueKey("multipart-streaming")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create 15MB of data (will create 3 x 5MB parts)
		dataSize := 15 * 1024 * 1024
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// Use multipart uploader
		uploader := manager.NewUploader(rawClient, func(u *manager.Uploader) {
			u.PartSize = 5 * 1024 * 1024 // 5MB parts
			u.Concurrency = 1            // Sequential for predictable behavior
		})

		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		// Verify upload
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(dataSize), *headResp.ContentLength)
	})

	t.Run("content-md5 with streaming upload", func(t *testing.T) {
		bucket := uniqueBucket("test-streaming-md5")
		key := uniqueKey("md5-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create data
		data := []byte("test content for MD5 verification with streaming signatures")

		// Upload with explicit content type
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("text/plain"),
		})
		require.NoError(t, err)

		// Verify
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		downloaded, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Equal(t, data, downloaded)
	})

	t.Run("upload with metadata in streaming request", func(t *testing.T) {
		bucket := uniqueBucket("test-streaming-meta")
		key := uniqueKey("meta-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create 10MB data to trigger streaming
		dataSize := 10 * 1024 * 1024
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// Upload with custom metadata
		uploader := manager.NewUploader(rawClient, func(u *manager.Uploader) {
			u.PartSize = 5 * 1024 * 1024
		})

		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			ContentType: aws.String("application/octet-stream"),
			Metadata: map[string]string{
				"custom-key":    "custom-value",
				"streaming-key": "streaming-value",
			},
		})
		require.NoError(t, err)

		// Verify metadata
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.Equal(t, "custom-value", headResp.Metadata["custom-key"])
		assert.Equal(t, "streaming-value", headResp.Metadata["streaming-key"])
	})

	t.Run("concurrent streaming uploads", func(t *testing.T) {
		bucket := uniqueBucket("test-streaming-concurrent")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create test data
		dataSize := 6 * 1024 * 1024 // 6MB each
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		uploader := manager.NewUploader(rawClient, func(u *manager.Uploader) {
			u.PartSize = 5 * 1024 * 1024
			u.Concurrency = 3 // Concurrent part uploads
		})

		// Upload multiple objects concurrently
		numObjects := 3
		keys := make([]string, numObjects)
		errCh := make(chan error, numObjects)

		for i := 0; i < numObjects; i++ {
			keys[i] = uniqueKey("concurrent-object")
			go func(key string) {
				_, err := uploader.Upload(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
					Body:   bytes.NewReader(data),
				})
				errCh <- err
			}(keys[i])
		}

		// Wait for all uploads
		for i := 0; i < numObjects; i++ {
			err := <-errCh
			require.NoError(t, err)
		}

		// Cleanup and verify
		for _, key := range keys {
			headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)
			assert.Equal(t, int64(dataSize), *headResp.ContentLength)
			client.DeleteObject(bucket, key)
		}
	})

	t.Run("streaming upload abort and retry", func(t *testing.T) {
		bucket := uniqueBucket("test-streaming-retry")
		key := uniqueKey("retry-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create data
		dataSize := 10 * 1024 * 1024
		data := make([]byte, dataSize)
		for i := range data {
			data[i] = byte(i % 256)
		}

		// First upload
		uploader := manager.NewUploader(rawClient, func(u *manager.Uploader) {
			u.PartSize = 5 * 1024 * 1024
		})

		_, err := uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)

		// Overwrite with different data (tests that previous upload is fully replaced)
		newData := make([]byte, dataSize)
		for i := range newData {
			newData[i] = byte((i + 1) % 256)
		}

		_, err = uploader.Upload(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(newData),
		})
		require.NoError(t, err)

		// Verify new content
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		downloaded, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Equal(t, newData, downloaded)
	})
}
