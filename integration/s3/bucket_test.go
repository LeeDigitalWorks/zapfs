//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"sort"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketCRUD(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)

	tests := []struct {
		name        string
		bucket      string
		setup       func(*testing.T, *testutil.S3Client, string) // Setup before test
		test        func(*testing.T, *testutil.S3Client, string) // Test function
		cleanup     func(*testing.T, *testutil.S3Client, string) // Cleanup after test
		expectError bool
	}{
		{
			name:   "create bucket - success",
			bucket: uniqueBucket("test-create"),
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
				// Verify bucket exists
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := c.Client.HeadBucket(ctx, &s3.HeadBucketInput{
					Bucket: aws.String(bucket),
				})
				assert.NoError(t, err, "bucket should exist after creation")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "create bucket - already exists",
			bucket: uniqueBucket("test-exists"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := c.Client.CreateBucket(ctx, &s3.CreateBucketInput{
					Bucket: aws.String(bucket),
				})
				assert.Error(t, err, "creating duplicate bucket should error")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "delete bucket - success",
			bucket: uniqueBucket("test-delete"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.DeleteBucket(bucket)
				// Verify bucket is deleted
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := c.Client.HeadBucket(ctx, &s3.HeadBucketInput{
					Bucket: aws.String(bucket),
				})
				assert.Error(t, err, "bucket should not exist after deletion")
			},
		},
		{
			name:   "delete bucket - not empty",
			bucket: uniqueBucket("test-notempty"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
				c.PutObject(bucket, "test-key", []byte("test data"))
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := c.Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
					Bucket: aws.String(bucket),
				})
				assert.Error(t, err, "deleting non-empty bucket should error")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.DeleteObject(bucket, "test-key")
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "delete bucket - not found",
			bucket: "nonexistent-bucket-" + testutil.UniqueID(""),
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := c.Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
					Bucket: aws.String(bucket),
				})
				assert.Error(t, err, "deleting nonexistent bucket should error")
			},
		},
		{
			name:   "head bucket - success",
			bucket: uniqueBucket("test-head"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				resp, err := c.Client.HeadBucket(ctx, &s3.HeadBucketInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err)
				assert.NotNil(t, resp)
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "head bucket - not found",
			bucket: "nonexistent-bucket-head-" + testutil.UniqueID(""),
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				_, err := c.Client.HeadBucket(ctx, &s3.HeadBucketInput{
					Bucket: aws.String(bucket),
				})
				assert.Error(t, err, "head on nonexistent bucket should error")
			},
		},
		{
			name:   "list buckets - includes created buckets",
			bucket: uniqueBucket("test-list"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				// Create multiple buckets
				for i := 0; i < 3; i++ {
					b := uniqueBucket("test-list")
					c.CreateBucket(b)
					// Store bucket names for cleanup
					t.Cleanup(func() {
						c.DeleteBucket(b)
					})
				}
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				resp := c.ListBuckets()
				require.NotNil(t, resp)
				assert.GreaterOrEqual(t, len(resp.Buckets), 4, "should have at least 4 buckets")

				// Check our bucket is in the list
				bucketNames := make(map[string]bool)
				for _, b := range resp.Buckets {
					bucketNames[*b.Name] = true
				}
				assert.True(t, bucketNames[bucket], "bucket %s should be in list", bucket)
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.DeleteBucket(bucket)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Setup
			if tc.setup != nil {
				tc.setup(t, client, tc.bucket)
			}

			// Test
			tc.test(t, client, tc.bucket)

			// Cleanup
			if tc.cleanup != nil {
				tc.cleanup(t, client, tc.bucket)
			}
		})
	}
}

// TestListBucketsPagination tests ListBuckets pagination with max-buckets parameter.
func TestListBucketsPagination(t *testing.T) {
	t.Parallel()
	client := newS3Client(t)

	// Create multiple buckets with a common prefix
	prefix := "pagination-test-" + testutil.UniqueID("")[:8] + "-"
	bucketNames := make([]string, 5)
	for i := range bucketNames {
		bucketNames[i] = prefix + string('a'+rune(i)) // alphabetical ordering
		client.CreateBucket(bucketNames[i])
		t.Cleanup(func(name string) func() {
			return func() { client.DeleteBucket(name) }
		}(bucketNames[i]))
	}
	sort.Strings(bucketNames) // Ensure we know the expected order

	t.Run("list all buckets without pagination", func(t *testing.T) {
		resp := client.ListBuckets()
		require.NotNil(t, resp)

		// Check all our buckets are in the list
		found := make(map[string]bool)
		for _, b := range resp.Buckets {
			if contains(bucketNames, *b.Name) {
				found[*b.Name] = true
			}
		}
		assert.Len(t, found, 5, "should find all 5 created buckets")
	})

	t.Run("list buckets with max-buckets limit", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Request only 2 buckets at a time
		resp, err := client.Client.ListBuckets(ctx, &s3.ListBucketsInput{
			MaxBuckets: aws.Int32(2),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.LessOrEqual(t, len(resp.Buckets), 2, "should return at most 2 buckets")
	})

	t.Run("list buckets with prefix filter", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		resp, err := client.Client.ListBuckets(ctx, &s3.ListBucketsInput{
			Prefix: aws.String(prefix),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)

		// Should only return buckets with our prefix
		for _, b := range resp.Buckets {
			assert.Contains(t, *b.Name, prefix[:len(prefix)-1],
				"bucket %s should have prefix %s", *b.Name, prefix)
		}
		assert.GreaterOrEqual(t, len(resp.Buckets), 5, "should find at least our 5 buckets with prefix")
	})
}

// TestGetBucketLocation tests GetBucketLocation returns proper region.
func TestGetBucketLocation(t *testing.T) {
	t.Parallel()
	client := newS3Client(t)

	bucket := uniqueBucket("location-test")
	client.CreateBucket(bucket)
	t.Cleanup(func() { client.DeleteBucket(bucket) })

	t.Run("get location of existing bucket", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		require.NotNil(t, resp)
		// Location can be empty for us-east-1 or contain a region name
		// Just verify the API succeeds
	})

	t.Run("get location of non-existent bucket", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Client.GetBucketLocation(ctx, &s3.GetBucketLocationInput{
			Bucket: aws.String("nonexistent-bucket-" + testutil.UniqueID("")),
		})
		require.Error(t, err, "should fail for non-existent bucket")
	})
}

// contains checks if a string slice contains a value.
func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}
