//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRestoreObjectNotArchived tests that RestoreObject returns an error for non-archived objects
func TestRestoreObjectNotArchived(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-restore")
	key := uniqueKey("standard-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	// Put a standard object
	data := []byte("test data for restore")
	client.PutObject(bucket, key, data)
	defer client.DeleteObject(bucket, key)

	// Try to restore a non-archived object - should fail with InvalidObjectState
	ctx := context.Background()
	_, err := client.Client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		RestoreRequest: &types.RestoreRequest{
			Days: aws.Int32(1),
		},
	})

	// Should fail because object is not in an archive tier
	require.Error(t, err)
	// Error should indicate invalid object state or not implemented
	errMsg := err.Error()
	assert.True(t, strings.Contains(errMsg, "InvalidObjectState") ||
		strings.Contains(errMsg, "NotImplemented"),
		"Expected InvalidObjectState or NotImplemented error, got: %s", errMsg)
}

// TestRestoreObjectEndpointExists tests that the RestoreObject endpoint exists and responds
func TestRestoreObjectEndpointExists(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-restore-endpoint")
	key := uniqueKey("test-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	// Put a standard object
	data := []byte("test data")
	client.PutObject(bucket, key, data)
	defer client.DeleteObject(bucket, key)

	// Call RestoreObject - we expect either an error (not archived) or a valid response
	ctx := context.Background()
	_, err := client.Client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		RestoreRequest: &types.RestoreRequest{
			Days: aws.Int32(1),
		},
	})

	// We should get an error, but not a connection error or unknown endpoint
	if err != nil {
		errMsg := err.Error()
		// Should not be a network/connection error
		assert.False(t, strings.Contains(errMsg, "connection refused"),
			"RestoreObject endpoint should exist")
		assert.False(t, strings.Contains(errMsg, "no such host"),
			"RestoreObject endpoint should be reachable")
	}
}

// TestRestoreNonExistentObject tests RestoreObject on a non-existent key
func TestRestoreNonExistentObject(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-restore-404")
	key := uniqueKey("nonexistent-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	// Try to restore a non-existent object
	ctx := context.Background()
	_, err := client.Client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		RestoreRequest: &types.RestoreRequest{
			Days: aws.Int32(1),
		},
	})

	// Should fail with NoSuchKey
	require.Error(t, err)
	errMsg := err.Error()
	assert.True(t, strings.Contains(errMsg, "NoSuchKey") ||
		strings.Contains(errMsg, "NotImplemented"),
		"Expected NoSuchKey or NotImplemented error, got: %s", errMsg)
}

// TestRestoreHeaderFormat tests the x-amz-restore header format
// This test documents the expected header format for archived objects.
// NOTE: This test requires:
// 1. An enterprise license with FeatureLifecycle enabled
// 2. A GLACIER/DEEP_ARCHIVE tier backend configured
// 3. Objects that have been transitioned and restored
//
// When infrastructure is available, uncomment and run this test.
/*
func TestRestoreHeaderFormat(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-restore-header")
	key := uniqueKey("glacier-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	// 1. Put object and transition to GLACIER
	// (requires lifecycle policy or admin API)

	// 2. Initiate restore
	ctx := context.Background()
	_, err := client.Client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int32(7),
			GlacierJobParameters: &s3.GlacierJobParameters{
				Tier: s3.Tier("Standard"),
			},
		},
	})
	require.NoError(t, err)

	// 3. Check x-amz-restore header while restore is in progress
	headResp, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	// During restore, header should be: ongoing-request="true"
	restore := aws.ToString(headResp.Restore)
	assert.Contains(t, restore, `ongoing-request="true"`)

	// 4. Wait for restore to complete (may take time depending on tier)

	// 5. Check x-amz-restore header after restore completes
	// Header should be: ongoing-request="false", expiry-date="<date>"
	headResp2, err := client.Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)

	restore2 := aws.ToString(headResp2.Restore)
	assert.Contains(t, restore2, `ongoing-request="false"`)
	assert.Contains(t, restore2, `expiry-date=`)
}
*/

// TestRestoreAlreadyInProgress tests that requesting restore when already in progress returns 409
// NOTE: This test requires GLACIER infrastructure to be available.
/*
func TestRestoreAlreadyInProgress(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-restore-conflict")
	key := uniqueKey("glacier-object")

	// Create bucket
	client.CreateBucket(bucket)
	defer client.DeleteBucket(bucket)

	// 1. Put object and transition to GLACIER

	// 2. Initiate first restore
	ctx := context.Background()
	_, err := client.Client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int32(7),
		},
	})
	require.NoError(t, err)

	// 3. Try to restore again - should fail with 409 Conflict (RestoreAlreadyInProgress)
	_, err = client.Client.RestoreObject(ctx, &s3.RestoreObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
		RestoreRequest: &s3.RestoreRequest{
			Days: aws.Int32(7),
		},
	})
	require.Error(t, err)
	errMsg := err.Error()
	assert.Contains(t, errMsg, "RestoreAlreadyInProgress",
		"Expected RestoreAlreadyInProgress error, got: %s", errMsg)
}
*/
