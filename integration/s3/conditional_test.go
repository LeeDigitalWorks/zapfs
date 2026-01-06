//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConditionalRequests(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("GetObject with If-Match - matching ETag", func(t *testing.T) {
		bucket := uniqueBucket("test-if-match")
		key := uniqueKey("test-object")
		data := []byte("test content for conditional request")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object and get ETag
		putResp, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(string(data)),
		})
		require.NoError(t, err)
		etag := *putResp.ETag

		// Get with matching If-Match should succeed
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:  aws.String(bucket),
			Key:     aws.String(key),
			IfMatch: aws.String(etag),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()
		assert.Equal(t, etag, *getResp.ETag)
	})

	t.Run("GetObject with If-Match - non-matching ETag", func(t *testing.T) {
		bucket := uniqueBucket("test-if-match-fail")
		key := uniqueKey("test-object")
		data := []byte("test content")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object
		client.PutObject(bucket, key, data)

		// Get with non-matching If-Match should fail with 412
		_, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:  aws.String(bucket),
			Key:     aws.String(key),
			IfMatch: aws.String("\"non-matching-etag\""),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PreconditionFailed")
	})

	t.Run("GetObject with If-None-Match - non-matching ETag", func(t *testing.T) {
		bucket := uniqueBucket("test-if-none-match")
		key := uniqueKey("test-object")
		data := []byte("test content")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object
		client.PutObject(bucket, key, data)

		// Get with non-matching If-None-Match should succeed
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			IfNoneMatch: aws.String("\"non-matching-etag\""),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()
		assert.NotEmpty(t, *getResp.ETag)
	})

	t.Run("GetObject with If-None-Match - matching ETag returns 304", func(t *testing.T) {
		bucket := uniqueBucket("test-if-none-match-304")
		key := uniqueKey("test-object")
		data := []byte("test content for 304")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object and get ETag
		putResp, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(string(data)),
		})
		require.NoError(t, err)
		etag := *putResp.ETag

		// Get with matching If-None-Match should return 304 Not Modified
		_, err = rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:      aws.String(bucket),
			Key:         aws.String(key),
			IfNoneMatch: aws.String(etag),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NotModified")
	})

	t.Run("GetObject with If-Modified-Since - object not modified", func(t *testing.T) {
		bucket := uniqueBucket("test-if-modified")
		key := uniqueKey("test-object")
		data := []byte("test content")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object
		client.PutObject(bucket, key, data)

		// Wait a moment to ensure time difference
		time.Sleep(100 * time.Millisecond)

		// Get with If-Modified-Since in the future should return 304
		futureTime := time.Now().Add(1 * time.Hour)
		_, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(key),
			IfModifiedSince: aws.Time(futureTime),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "NotModified")
	})

	t.Run("GetObject with If-Modified-Since - object modified", func(t *testing.T) {
		bucket := uniqueBucket("test-if-modified-ok")
		key := uniqueKey("test-object")
		data := []byte("test content")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object
		client.PutObject(bucket, key, data)

		// Get with If-Modified-Since in the past should succeed
		pastTime := time.Now().Add(-1 * time.Hour)
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:          aws.String(bucket),
			Key:             aws.String(key),
			IfModifiedSince: aws.Time(pastTime),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()
		assert.NotEmpty(t, *getResp.ETag)
	})

	t.Run("HeadObject with If-Match", func(t *testing.T) {
		bucket := uniqueBucket("test-head-if-match")
		key := uniqueKey("test-object")
		data := []byte("test content")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object and get ETag
		putResp, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   strings.NewReader(string(data)),
		})
		require.NoError(t, err)
		etag := *putResp.ETag

		// Head with matching If-Match should succeed
		headResp, err := rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:  aws.String(bucket),
			Key:     aws.String(key),
			IfMatch: aws.String(etag),
		})
		require.NoError(t, err)
		assert.Equal(t, etag, *headResp.ETag)

		// Head with non-matching If-Match should fail
		_, err = rawClient.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket:  aws.String(bucket),
			Key:     aws.String(key),
			IfMatch: aws.String("\"wrong-etag\""),
		})
		require.Error(t, err)
	})

	t.Run("CopyObject with x-amz-copy-source-if-match", func(t *testing.T) {
		bucket := uniqueBucket("test-copy-if-match")
		srcKey := uniqueKey("source-object")
		dstKey := uniqueKey("dest-object")
		data := []byte("test content for copy")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, srcKey)
			client.DeleteObject(bucket, dstKey)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put source object
		putResp, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Body:   strings.NewReader(string(data)),
		})
		require.NoError(t, err)
		etag := *putResp.ETag

		// Copy with matching If-Match should succeed
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:            aws.String(bucket),
			Key:               aws.String(dstKey),
			CopySource:        aws.String(bucket + "/" + srcKey),
			CopySourceIfMatch: aws.String(etag),
		})
		require.NoError(t, err)

		// Verify copy succeeded
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()
	})

	t.Run("CopyObject with x-amz-copy-source-if-none-match", func(t *testing.T) {
		bucket := uniqueBucket("test-copy-if-none-match")
		srcKey := uniqueKey("source-object")
		dstKey := uniqueKey("dest-object")
		data := []byte("test content")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, srcKey)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put source object
		putResp, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Body:   strings.NewReader(string(data)),
		})
		require.NoError(t, err)
		etag := *putResp.ETag

		// Copy with matching If-None-Match should fail (412)
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:                aws.String(bucket),
			Key:                   aws.String(dstKey),
			CopySource:            aws.String(bucket + "/" + srcKey),
			CopySourceIfNoneMatch: aws.String(etag),
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "PreconditionFailed")
	})
}
