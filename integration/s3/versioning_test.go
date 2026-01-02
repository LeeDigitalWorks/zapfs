//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVersioning(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	// Use the underlying AWS SDK client for versioning operations
	rawClient := client.Client

	t.Run("enable versioning", func(t *testing.T) {
		bucket := uniqueBucket("test-versioning-enable")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Get versioning status (initially disabled/not set)
		getResp, err := rawClient.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		assert.Empty(t, getResp.Status, "versioning should be empty initially")

		// Enable versioning
		_, err = rawClient.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		})
		require.NoError(t, err)

		// Verify it's enabled
		getResp, err = rawClient.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		assert.Equal(t, s3types.BucketVersioningStatusEnabled, getResp.Status)
	})

	t.Run("suspend versioning", func(t *testing.T) {
		bucket := uniqueBucket("test-versioning-suspend")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Enable versioning first
		_, err := rawClient.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		})
		require.NoError(t, err)

		// Suspend versioning
		_, err = rawClient.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusSuspended,
			},
		})
		require.NoError(t, err)

		// Verify it's suspended
		getResp, err := rawClient.GetBucketVersioning(ctx, &s3.GetBucketVersioningInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		assert.Equal(t, s3types.BucketVersioningStatusSuspended, getResp.Status)
	})

	t.Run("multiple versions of same object", func(t *testing.T) {
		bucket := uniqueBucket("test-versioning-multi")
		key := uniqueKey("versioned-object")
		client.CreateBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Enable versioning
		_, err := rawClient.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		})
		require.NoError(t, err)

		// Put first version
		version1Data := []byte("version 1 content")
		putResp1, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(version1Data),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, putResp1.VersionId, "should have version ID")
		version1ID := *putResp1.VersionId

		// Put second version
		version2Data := []byte("version 2 content - updated")
		putResp2, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(version2Data),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, putResp2.VersionId)
		version2ID := *putResp2.VersionId

		assert.NotEqual(t, version1ID, version2ID, "version IDs should be different")

		// Get latest version (should be version 2)
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()
		assert.Equal(t, version2ID, *getResp.VersionId)

		body, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Equal(t, version2Data, body)

		// Get specific version (version 1)
		getResp1, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(key),
			VersionId: aws.String(version1ID),
		})
		require.NoError(t, err)
		defer getResp1.Body.Close()

		body1, err := io.ReadAll(getResp1.Body)
		require.NoError(t, err)
		assert.Equal(t, version1Data, body1)

		// Cleanup - delete all versions
		_, err = rawClient.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(key),
			VersionId: aws.String(version1ID),
		})
		require.NoError(t, err)
		_, err = rawClient.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(key),
			VersionId: aws.String(version2ID),
		})
		require.NoError(t, err)
		client.DeleteBucket(bucket)
	})

	t.Run("list object versions", func(t *testing.T) {
		bucket := uniqueBucket("test-versioning-list")
		key := uniqueKey("list-versions-obj")
		client.CreateBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Enable versioning
		_, err := rawClient.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		})
		require.NoError(t, err)

		// Create 3 versions
		var versionIDs []string
		for i := 0; i < 3; i++ {
			data := []byte("version content " + string(rune('A'+i)))
			resp, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(bucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader(data),
			})
			require.NoError(t, err)
			versionIDs = append(versionIDs, *resp.VersionId)
		}

		// List versions
		listResp, err := rawClient.ListObjectVersions(ctx, &s3.ListObjectVersionsInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		assert.Len(t, listResp.Versions, 3, "should have 3 versions")

		// Verify all our version IDs are present
		foundVersions := make(map[string]bool)
		for _, v := range listResp.Versions {
			if *v.Key == key {
				foundVersions[*v.VersionId] = true
			}
		}
		for _, vid := range versionIDs {
			assert.True(t, foundVersions[vid], "version %s should be in list", vid)
		}

		// Cleanup
		for _, vid := range versionIDs {
			_, err = rawClient.DeleteObject(ctx, &s3.DeleteObjectInput{
				Bucket:    aws.String(bucket),
				Key:       aws.String(key),
				VersionId: aws.String(vid),
			})
			require.NoError(t, err)
		}
		client.DeleteBucket(bucket)
	})

	t.Run("delete marker", func(t *testing.T) {
		bucket := uniqueBucket("test-versioning-delete-marker")
		key := uniqueKey("delete-marker-obj")
		client.CreateBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Enable versioning
		_, err := rawClient.PutBucketVersioning(ctx, &s3.PutBucketVersioningInput{
			Bucket: aws.String(bucket),
			VersioningConfiguration: &s3types.VersioningConfiguration{
				Status: s3types.BucketVersioningStatusEnabled,
			},
		})
		require.NoError(t, err)

		// Put an object
		data := []byte("test content")
		putResp, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   bytes.NewReader(data),
		})
		require.NoError(t, err)
		originalVersionID := *putResp.VersionId

		// Delete without version ID (creates delete marker)
		delResp, err := rawClient.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.NotNil(t, delResp.VersionId, "should return delete marker version ID")
		assert.True(t, *delResp.DeleteMarker, "should be marked as delete marker")
		deleteMarkerVersionID := *delResp.VersionId

		// Try to get the object (should fail with 404)
		_, err = rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		assert.Error(t, err, "should get error for deleted object")

		// Get the original version directly (should still work)
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(key),
			VersionId: aws.String(originalVersionID),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		body, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Equal(t, data, body)

		// Cleanup - delete the delete marker and original version
		_, err = rawClient.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(key),
			VersionId: aws.String(deleteMarkerVersionID),
		})
		require.NoError(t, err)
		_, err = rawClient.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(key),
			VersionId: aws.String(originalVersionID),
		})
		require.NoError(t, err)
		client.DeleteBucket(bucket)
	})
}
