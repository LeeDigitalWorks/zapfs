//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyObjectMetadataDirective(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("copy with COPY metadata directive (default)", func(t *testing.T) {
		bucket := uniqueBucket("test-copy-meta-copy")
		srcKey := uniqueKey("source-object")
		dstKey := uniqueKey("dest-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, srcKey)
			client.DeleteObject(bucket, dstKey)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put source object with custom metadata and content-type
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(srcKey),
			Body:        strings.NewReader("source content"),
			ContentType: aws.String("text/plain"),
			Metadata: map[string]string{
				"custom-key":  "custom-value",
				"another-key": "another-value",
			},
		})
		require.NoError(t, err)

		// Copy with COPY directive (default - metadata is preserved)
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String(dstKey),
			CopySource:        aws.String(bucket + "/" + srcKey),
			MetadataDirective: s3types.MetadataDirectiveCopy,
		})
		require.NoError(t, err)

		// Verify destination has same metadata
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		assert.Equal(t, "text/plain", *headResp.ContentType)
		assert.Equal(t, "custom-value", headResp.Metadata["custom-key"])
		assert.Equal(t, "another-value", headResp.Metadata["another-key"])
	})

	t.Run("copy with REPLACE metadata directive", func(t *testing.T) {
		bucket := uniqueBucket("test-copy-meta-replace")
		srcKey := uniqueKey("source-object")
		dstKey := uniqueKey("dest-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, srcKey)
			client.DeleteObject(bucket, dstKey)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put source object with custom metadata
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(srcKey),
			Body:        strings.NewReader("source content"),
			ContentType: aws.String("text/plain"),
			Metadata: map[string]string{
				"original-key": "original-value",
			},
		})
		require.NoError(t, err)

		// Copy with REPLACE directive and new metadata
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String(dstKey),
			CopySource:        aws.String(bucket + "/" + srcKey),
			MetadataDirective: s3types.MetadataDirectiveReplace,
			ContentType:       aws.String("application/json"),
			Metadata: map[string]string{
				"new-key": "new-value",
			},
		})
		require.NoError(t, err)

		// Verify destination has new metadata, not source metadata
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		assert.Equal(t, "application/json", *headResp.ContentType)
		assert.Equal(t, "new-value", headResp.Metadata["new-key"])
		_, hasOriginal := headResp.Metadata["original-key"]
		assert.False(t, hasOriginal, "should not have original metadata key")
	})

	t.Run("copy to same key with REPLACE (update metadata)", func(t *testing.T) {
		bucket := uniqueBucket("test-copy-same-key")
		key := uniqueKey("same-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object with initial metadata
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			Body:        strings.NewReader("content"),
			ContentType: aws.String("text/plain"),
			Metadata: map[string]string{
				"version": "1",
			},
		})
		require.NoError(t, err)

		// Copy to same key with REPLACE to update metadata
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String(key),
			CopySource:        aws.String(bucket + "/" + key),
			MetadataDirective: s3types.MetadataDirectiveReplace,
			ContentType:       aws.String("text/plain"),
			Metadata: map[string]string{
				"version": "2",
			},
		})
		require.NoError(t, err)

		// Verify metadata was updated
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.Equal(t, "2", headResp.Metadata["version"])
	})

	t.Run("cross-bucket copy with metadata", func(t *testing.T) {
		srcBucket := uniqueBucket("test-copy-src-bucket")
		dstBucket := uniqueBucket("test-copy-dst-bucket")
		key := uniqueKey("object")
		client.CreateBucket(srcBucket)
		client.CreateBucket(dstBucket)
		defer func() {
			client.DeleteObject(srcBucket, key)
			client.DeleteObject(dstBucket, key)
			client.DeleteBucket(srcBucket)
			client.DeleteBucket(dstBucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put source object
		content := "cross-bucket content"
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:      aws.String(srcBucket),
			Key:         aws.String(key),
			Body:        strings.NewReader(content),
			ContentType: aws.String("text/html"),
			Metadata: map[string]string{
				"source-bucket": srcBucket,
			},
		})
		require.NoError(t, err)

		// Copy to different bucket
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:     aws.String(dstBucket),
			Key:        aws.String(key),
			CopySource: aws.String(srcBucket + "/" + key),
		})
		require.NoError(t, err)

		// Verify content and metadata in destination
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(dstBucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		body, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Equal(t, content, string(body))
		assert.Equal(t, "text/html", *getResp.ContentType)
		assert.Equal(t, srcBucket, getResp.Metadata["source-bucket"])
	})

	t.Run("copy with storage class change", func(t *testing.T) {
		bucket := uniqueBucket("test-copy-storage-class")
		srcKey := uniqueKey("source-object")
		dstKey := uniqueKey("dest-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, srcKey)
			client.DeleteObject(bucket, dstKey)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put source object
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Body:   strings.NewReader("content"),
		})
		require.NoError(t, err)

		// Copy with different storage class
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:       aws.String(bucket),
			Key:          aws.String(dstKey),
			CopySource:   aws.String(bucket + "/" + srcKey),
			StorageClass: s3types.StorageClassStandardIa,
		})
		require.NoError(t, err)

		// Verify storage class
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		// Note: Storage class might be "STANDARD_IA" or implementation-specific
		assert.NotEmpty(t, headResp.StorageClass)
	})
}

func TestCopyObjectRange(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("UploadPartCopy with range", func(t *testing.T) {
		bucket := uniqueBucket("test-part-copy-range")
		srcKey := uniqueKey("source-large")
		dstKey := uniqueKey("dest-multipart")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, srcKey)
			client.DeleteObject(bucket, dstKey)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create a source object (10MB)
		srcData := make([]byte, 10*1024*1024)
		for i := range srcData {
			srcData[i] = byte(i % 256)
		}
		client.PutObject(bucket, srcKey, srcData)

		// Create multipart upload
		createResp, err := rawClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		uploadID := createResp.UploadId

		defer func() {
			// Abort if not completed
			rawClient.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(dstKey),
				UploadId: uploadID,
			})
		}()

		// Copy first 5MB as part 1
		part1Resp, err := rawClient.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(dstKey),
			UploadId:        uploadID,
			PartNumber:      aws.Int32(1),
			CopySource:      aws.String(bucket + "/" + srcKey),
			CopySourceRange: aws.String("bytes=0-5242879"), // First 5MB
		})
		require.NoError(t, err)

		// Copy last 5MB as part 2
		part2Resp, err := rawClient.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(dstKey),
			UploadId:        uploadID,
			PartNumber:      aws.Int32(2),
			CopySource:      aws.String(bucket + "/" + srcKey),
			CopySourceRange: aws.String("bytes=5242880-10485759"), // Last 5MB
		})
		require.NoError(t, err)

		// Complete multipart upload
		_, err = rawClient.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(dstKey),
			UploadId: uploadID,
			MultipartUpload: &s3types.CompletedMultipartUpload{
				Parts: []s3types.CompletedPart{
					{PartNumber: aws.Int32(1), ETag: part1Resp.CopyPartResult.ETag},
					{PartNumber: aws.Int32(2), ETag: part2Resp.CopyPartResult.ETag},
				},
			},
		})
		require.NoError(t, err)

		// Verify the copied object
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		assert.Equal(t, int64(10*1024*1024), *headResp.ContentLength)
	})
}
