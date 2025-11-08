//go:build integration

package s3

import (
	"context"
	"testing"
	"time"

	"zapfs/integration/testutil"

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
