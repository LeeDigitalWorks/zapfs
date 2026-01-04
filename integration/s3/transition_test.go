//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestStorageClassHeader tests that storage class is correctly returned in responses
func TestStorageClassHeader(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-storage-class")
	key := uniqueKey("object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	// Put object with STANDARD storage class (default)
	data := []byte("test data for storage class")
	client.PutObject(bucket, key, data)
	defer client.DeleteObject(bucket, key)

	// List objects and verify storage class is returned
	ctx := context.Background()
	listResp, err := client.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
	})
	require.NoError(t, err)
	require.Len(t, listResp.Contents, 1)

	// StorageClass should be STANDARD for new objects
	assert.Equal(t, s3types.ObjectStorageClassStandard, listResp.Contents[0].StorageClass)

	// Head object - STANDARD class should not have x-amz-storage-class header
	// (S3 convention: only non-STANDARD classes include the header)
	headResp, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	// For STANDARD, StorageClass might be empty or STANDARD depending on implementation
	// AWS S3 typically doesn't return it for STANDARD
	if headResp.StorageClass != "" {
		assert.Equal(t, s3types.StorageClassStandard, headResp.StorageClass)
	}
}

// TestPutObjectWithStorageClass tests putting objects with different storage classes
func TestPutObjectWithStorageClass(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-put-storage-class")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	tests := []struct {
		name         string
		storageClass s3types.StorageClass
	}{
		{"STANDARD", s3types.StorageClassStandard},
		// Note: GLACIER and DEEP_ARCHIVE require special backend configuration
		// which may not be available in all test environments
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			key := uniqueKey("object-" + string(tc.storageClass))
			data := []byte("test data")

			ctx := context.Background()
			_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:       aws.String(bucket),
				Key:          aws.String(key),
				Body:         bytes.NewReader(data),
				StorageClass: tc.storageClass,
			})
			require.NoError(t, err)
			defer client.DeleteObject(bucket, key)

			// Verify the object is accessible
			getResp, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
			})
			require.NoError(t, err)
			defer getResp.Body.Close()
		})
	}
}

// TestInvalidStorageClass tests that invalid storage classes are rejected
func TestInvalidStorageClass(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-invalid-class")
	key := uniqueKey("object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	ctx := context.Background()
	_, err := client.Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		Body:         bytes.NewReader([]byte("test")),
		StorageClass: s3types.StorageClass("INVALID_CLASS"),
	})
	// Should fail with InvalidStorageClass error
	assert.Error(t, err)
}

// TestTransitionedObjectAccess tests accessing objects that have been transitioned
// NOTE: This test requires:
// 1. An enterprise license with FeatureLifecycle enabled
// 2. A tier backend configured (e.g., MinIO for GLACIER simulation)
// 3. A lifecycle policy that transitions objects
//
// This test is currently a placeholder that documents the expected behavior.
// Uncomment and modify when tier infrastructure is available.
/*
func TestTransitionedObjectAccess(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-transitioned")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	// 1. Put an object
	key := uniqueKey("to-transition")
	data := []byte("data to be transitioned to cold storage")
	client.PutObject(bucket, key, data)

	// 2. Set lifecycle policy to transition immediately (or use admin API)
	// This would require lifecycle policy configuration

	// 3. Wait for transition to complete (or trigger manually)

	// 4. Verify object can still be accessed (for non-archive tiers)
	ctx := context.Background()
	getResp, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	defer getResp.Body.Close()

	retrieved, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	assert.Equal(t, data, retrieved)

	// 5. Verify storage class is updated
	assert.NotEqual(t, s3types.StorageClassStandard, getResp.StorageClass)
}
*/

// TestGlacierObjectRequiresRestore tests that GLACIER objects require restore before access
// NOTE: This test requires a GLACIER tier backend and enterprise license.
// When infrastructure is available, this test verifies InvalidObjectState is returned.
/*
func TestGlacierObjectRequiresRestore(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-glacier")
	key := uniqueKey("glacier-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	// 1. Put and transition object to GLACIER
	// (requires lifecycle policy or admin API)

	// 2. Try to GET - should fail with InvalidObjectState
	ctx := context.Background()
	_, err := client.Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.Error(t, err)
	// Verify it's an InvalidObjectState error
	// var apiErr smithy.APIError
	// require.True(t, errors.As(err, &apiErr))
	// assert.Equal(t, "InvalidObjectState", apiErr.ErrorCode())
}
*/
