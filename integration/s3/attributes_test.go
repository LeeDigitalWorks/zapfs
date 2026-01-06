//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetObjectAttributes(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("get ETag attribute", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-attrs-etag")
		key := uniqueKey("test-object")
		data := []byte("test content for attributes")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, data)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Get object attributes - requesting ETag
		resp, err := rawClient.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ObjectAttributes: []s3types.ObjectAttributes{
				s3types.ObjectAttributesEtag,
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.ETag, "should have ETag")
	})

	t.Run("get ObjectSize attribute", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-attrs-size")
		key := uniqueKey("test-object")
		data := []byte("test content with known size")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, data)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Get object attributes - requesting ObjectSize
		resp, err := rawClient.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ObjectAttributes: []s3types.ObjectAttributes{
				s3types.ObjectAttributesObjectSize,
			},
		})
		require.NoError(t, err)
		assert.NotNil(t, resp.ObjectSize)
		assert.Equal(t, int64(len(data)), *resp.ObjectSize)
	})

	t.Run("get StorageClass attribute", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-attrs-class")
		key := uniqueKey("test-object")
		data := []byte("test content")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, data)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Get object attributes - requesting StorageClass
		resp, err := rawClient.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ObjectAttributes: []s3types.ObjectAttributes{
				s3types.ObjectAttributesStorageClass,
			},
		})
		require.NoError(t, err)
		// Storage class should be set (e.g., STANDARD)
		assert.NotEmpty(t, resp.StorageClass)
	})

	t.Run("get multiple attributes", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-attrs-multi")
		key := uniqueKey("test-object")
		data := []byte("test content for multiple attributes")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, data)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Get object attributes - requesting multiple
		resp, err := rawClient.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ObjectAttributes: []s3types.ObjectAttributes{
				s3types.ObjectAttributesEtag,
				s3types.ObjectAttributesObjectSize,
				s3types.ObjectAttributesStorageClass,
			},
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.ETag)
		if resp.ObjectSize != nil {
			assert.Equal(t, int64(len(data)), *resp.ObjectSize)
		}
		// Storage class may or may not be returned
	})

	t.Run("get attributes for non-existent object", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-attrs-404")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Get object attributes for non-existent key
		_, err := rawClient.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucket),
			Key:    aws.String("non-existent-key"),
			ObjectAttributes: []s3types.ObjectAttributes{
				s3types.ObjectAttributesEtag,
			},
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NoSuchKey")
	})

	t.Run("get checksum attribute", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-attrs-checksum")
		key := uniqueKey("test-object")
		data := []byte("test content for checksum")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, data)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Get object attributes - requesting Checksum
		resp, err := rawClient.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ObjectAttributes: []s3types.ObjectAttributes{
				s3types.ObjectAttributesChecksum,
			},
		})
		require.NoError(t, err)
		// Checksum may or may not be present depending on how object was uploaded
		// Just verify the call succeeds
		_ = resp
	})

	t.Run("get ObjectParts for multipart object", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-attrs-parts")
		key := uniqueKey("multipart-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create a multipart upload
		createResp, err := rawClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		uploadID := createResp.UploadId

		// Upload two parts (minimum 5MB each for real S3, but our test server may allow smaller)
		partData := make([]byte, 5*1024*1024) // 5MB
		for i := range partData {
			partData[i] = byte(i % 256)
		}

		part1Resp, err := rawClient.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       bytes.NewReader(partData),
		})
		require.NoError(t, err)

		part2Resp, err := rawClient.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(2),
			Body:       bytes.NewReader(partData),
		})
		require.NoError(t, err)

		// Complete multipart upload
		_, err = rawClient.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
			MultipartUpload: &s3types.CompletedMultipartUpload{
				Parts: []s3types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: part1Resp.ETag},
					{PartNumber: aws.Int32(2), ETag: part2Resp.ETag},
				},
			},
		})
		require.NoError(t, err)

		// Get object attributes - requesting ObjectParts
		resp, err := rawClient.GetObjectAttributes(ctx, &s3.GetObjectAttributesInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ObjectAttributes: []s3types.ObjectAttributes{
				s3types.ObjectAttributesObjectParts,
				s3types.ObjectAttributesObjectSize,
			},
		})
		require.NoError(t, err)
		assert.NotNil(t, resp.ObjectSize)
		assert.Equal(t, int64(10*1024*1024), *resp.ObjectSize)
		// ObjectParts may or may not be populated depending on implementation
	})
}
