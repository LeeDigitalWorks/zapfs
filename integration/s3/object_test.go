//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"strings"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestObjectCRUD(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)

	tests := []struct {
		name      string
		bucket    string
		key       string
		data      []byte
		dataSize  int                                                          // Size in bytes (0 means use data length)
		setup     func(*testing.T, *testutil.S3Client, string)                 // Setup before test
		test      func(*testing.T, *testutil.S3Client, string, string, []byte) // Test function
		cleanup   func(*testing.T, *testutil.S3Client, string, string)         // Cleanup after test
		skipShort bool                                                         // Skip in short mode
	}{
		{
			name:     "put and get - small object (1KB)",
			bucket:   uniqueBucket("test-putget"),
			key:      uniqueKey("small-object"),
			dataSize: 1024,
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				// Put
				resp := c.PutObject(bucket, key, data)
				assert.NotNil(t, resp)
				assert.NotEmpty(t, resp.ETag)

				// Get
				retrieved := c.GetObject(bucket, key)
				assert.Equal(t, data, retrieved, "retrieved data should match original")
				assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved), "ETags should match")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "put and get - medium object (5MB)",
			bucket:   uniqueBucket("test-medium"),
			key:      uniqueKey("medium-object"),
			dataSize: 5 * 1024 * 1024,
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				resp := c.PutObject(bucket, key, data)
				assert.NotNil(t, resp)

				retrieved := c.GetObject(bucket, key)
				assert.Equal(t, len(data), len(retrieved))
				assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved))
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "put and get - large object (50MB)",
			bucket:   uniqueBucket("test-large"),
			key:      uniqueKey("large-object"),
			dataSize: 50 * 1024 * 1024,
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				start := time.Now()
				resp := c.PutObject(bucket, key, data)
				putDuration := time.Since(start)
				assert.NotNil(t, resp)
				t.Logf("Put 50MB in %v (%.2f MB/s)", putDuration, float64(50)/putDuration.Seconds())

				start = time.Now()
				retrieved := c.GetObject(bucket, key)
				getDuration := time.Since(start)
				assert.Equal(t, len(data), len(retrieved))
				assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved))
				t.Logf("Get 50MB in %v (%.2f MB/s)", getDuration, float64(50)/getDuration.Seconds())
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
			skipShort: true,
		},
		{
			name:   "put and get - empty object",
			bucket: uniqueBucket("test-empty"),
			key:    uniqueKey("empty-object"),
			data:   []byte{},
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				c.PutObject(bucket, key, data)

				resp := c.HeadObject(bucket, key)
				assert.Equal(t, int64(0), *resp.ContentLength)

				retrieved := c.GetObject(bucket, key)
				assert.Equal(t, data, retrieved)
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "put and get - overwrite existing object",
			bucket:   uniqueBucket("test-overwrite"),
			key:      uniqueKey("overwrite-object"),
			dataSize: 1024,
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				// First write
				data1 := []byte("first version")
				c.PutObject(bucket, key, data1)

				// Verify first version
				retrieved1 := c.GetObject(bucket, key)
				assert.Equal(t, data1, retrieved1)

				// Overwrite
				data2 := []byte("second version - updated")
				c.PutObject(bucket, key, data2)

				// Verify second version
				retrieved2 := c.GetObject(bucket, key)
				assert.Equal(t, data2, retrieved2)
				assert.NotEqual(t, data1, retrieved2, "should have different content")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "put and get - special characters in key",
			bucket:   uniqueBucket("test-special"),
			key:      "path/with spaces/file.txt",
			dataSize: 512,
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				c.PutObject(bucket, key, data)
				retrieved := c.GetObject(bucket, key)
				assert.Equal(t, data, retrieved)
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "get object - not found",
			bucket: uniqueBucket("test-notfound"),
			key:    "nonexistent-key",
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				_, err := c.Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				assert.Error(t, err, "getting nonexistent object should error")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "head object - success",
			bucket:   uniqueBucket("test-head"),
			key:      uniqueKey("head-object"),
			dataSize: 1024,
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				c.PutObject(bucket, key, data)

				resp := c.HeadObject(bucket, key)
				assert.Equal(t, int64(len(data)), *resp.ContentLength)
				assert.NotNil(t, resp.ETag)
				assert.NotNil(t, resp.LastModified)
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "head object - not found",
			bucket: uniqueBucket("test-head-notfound"),
			key:    "nonexistent-key",
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				_, err := c.Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				assert.Error(t, err, "head on nonexistent object should error")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "delete object - success",
			bucket:   uniqueBucket("test-delete"),
			key:      uniqueKey("delete-object"),
			dataSize: 1024,
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				c.PutObject(bucket, key, data)
				c.DeleteObject(bucket, key)

				// Verify object is deleted
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				_, err := c.Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				assert.Error(t, err, "object should not exist after deletion")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "delete object - idempotent (nonexistent)",
			bucket: uniqueBucket("test-delete-idem"),
			key:    "nonexistent-key",
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data []byte) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				_, err := c.Client.DeleteObject(ctx, &s3.DeleteObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				// S3 delete is idempotent - should not error
				assert.NoError(t, err, "deleting nonexistent object should succeed")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteBucket(bucket)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipShort {
				testutil.SkipIfShort(t)
			}
			t.Parallel()

			// Generate test data if needed
			var data []byte
			if tc.data != nil {
				data = tc.data
			} else if tc.dataSize > 0 {
				data = testutil.GenerateTestData(t, tc.dataSize)
			} else {
				data = testutil.GenerateTestData(t, 1024) // Default 1KB
			}

			// Setup
			if tc.setup != nil {
				tc.setup(t, client, tc.bucket)
			}

			// Test
			tc.test(t, client, tc.bucket, tc.key, data)

			// Cleanup
			if tc.cleanup != nil {
				tc.cleanup(t, client, tc.bucket, tc.key)
			}
		})
	}
}

// =============================================================================
// SSE-C Encryption Tests (Table-Driven)
// =============================================================================

func TestObjectSSEC(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)

	// Generate a valid SSE-C key (32 bytes for AES-256)
	ssecKey := make([]byte, 32)
	_, err := rand.Read(ssecKey)
	require.NoError(t, err)

	tests := []struct {
		name      string
		bucket    string
		key       string
		data      []byte
		dataSize  int
		test      func(*testing.T, *testutil.S3Client, string, string, []byte, []byte)
		cleanup   func(*testing.T, *testutil.S3Client, string, string)
		skipShort bool
	}{
		{
			name:     "put and get with SSE-C - small object",
			bucket:   uniqueBucket("test-ssec"),
			key:      uniqueKey("ssec-object"),
			dataSize: 1024,
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data, ssecKey []byte) {
				// Put with SSE-C
				resp := c.PutObjectWithOptions(bucket, key, data, testutil.WithSSECustomerKey(ssecKey))
				assert.NotNil(t, resp)
				assert.NotEmpty(t, resp.ETag)

				// Verify SSE-C response headers
				assert.NotNil(t, resp.SSECustomerAlgorithm)
				assert.Equal(t, "AES256", *resp.SSECustomerAlgorithm)
				assert.NotNil(t, resp.SSECustomerKeyMD5)

				// Get with SSE-C (must use same key)
				retrieved := c.GetObjectWithOptions(bucket, key, testutil.WithSSECustomerKeyForGet(ssecKey))
				assert.Equal(t, data, retrieved, "retrieved data should match original")
				assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved), "ETags should match")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "put and get with SSE-C - medium object",
			bucket:   uniqueBucket("test-ssec-medium"),
			key:      uniqueKey("ssec-medium"),
			dataSize: 5 * 1024 * 1024,
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data, ssecKey []byte) {
				resp := c.PutObjectWithOptions(bucket, key, data, testutil.WithSSECustomerKey(ssecKey))
				assert.NotNil(t, resp)

				retrieved := c.GetObjectWithOptions(bucket, key, testutil.WithSSECustomerKeyForGet(ssecKey))
				assert.Equal(t, len(data), len(retrieved))
				assert.Equal(t, testutil.ComputeETag(data), testutil.ComputeETag(retrieved))
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "put with SSE-C, get without key - should fail",
			bucket:   uniqueBucket("test-ssec-missing-key"),
			key:      uniqueKey("ssec-missing"),
			dataSize: 1024,
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data, ssecKey []byte) {
				// Put with SSE-C
				c.PutObjectWithOptions(bucket, key, data, testutil.WithSSECustomerKey(ssecKey))

				// Try to get without SSE-C key - should fail
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				_, err := c.Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				assert.Error(t, err, "getting SSE-C encrypted object without key should error")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "put with SSE-C, get with wrong key - should fail",
			bucket:   uniqueBucket("test-ssec-wrong-key"),
			key:      uniqueKey("ssec-wrong"),
			dataSize: 1024,
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data, ssecKey []byte) {
				// Put with SSE-C
				c.PutObjectWithOptions(bucket, key, data, testutil.WithSSECustomerKey(ssecKey))

				// Generate wrong key
				wrongKey := make([]byte, 32)
				_, err := rand.Read(wrongKey)
				require.NoError(t, err)

				// Try to get with wrong key - should fail
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				wrongKeyMD5Hash := md5.Sum(wrongKey)
				_, err = c.Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket:               aws.String(bucket),
					Key:                  aws.String(key),
					SSECustomerAlgorithm: aws.String("AES256"),
					SSECustomerKey:       aws.String(base64.StdEncoding.EncodeToString(wrongKey)),
					SSECustomerKeyMD5:    aws.String(base64.StdEncoding.EncodeToString(wrongKeyMD5Hash[:])),
				})
				assert.Error(t, err, "getting SSE-C encrypted object with wrong key should error")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "head object with SSE-C - success",
			bucket:   uniqueBucket("test-ssec-head"),
			key:      uniqueKey("ssec-head"),
			dataSize: 1024,
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data, ssecKey []byte) {
				// Put with SSE-C
				c.PutObjectWithOptions(bucket, key, data, testutil.WithSSECustomerKey(ssecKey))

				// Head with SSE-C key
				resp := c.HeadObjectWithOptions(bucket, key, testutil.WithSSECustomerKeyForHead(ssecKey))
				assert.Equal(t, int64(len(data)), *resp.ContentLength)
				assert.NotNil(t, resp.ETag)

				// Verify SSE-C response headers
				assert.NotNil(t, resp.SSECustomerAlgorithm)
				assert.Equal(t, "AES256", *resp.SSECustomerAlgorithm)
				assert.NotNil(t, resp.SSECustomerKeyMD5)
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
		{
			name:     "head object with SSE-C - missing key should fail",
			bucket:   uniqueBucket("test-ssec-head-missing"),
			key:      uniqueKey("ssec-head-missing"),
			dataSize: 1024,
			test: func(t *testing.T, c *testutil.S3Client, bucket, key string, data, ssecKey []byte) {
				// Put with SSE-C
				c.PutObjectWithOptions(bucket, key, data, testutil.WithSSECustomerKey(ssecKey))

				// Try to head without SSE-C key - should fail
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				_, err := c.Client.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				assert.Error(t, err, "heading SSE-C encrypted object without key should error")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket, key string) {
				c.DeleteObject(bucket, key)
				c.DeleteBucket(bucket)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipShort {
				testutil.SkipIfShort(t)
			}
			t.Parallel()

			// Generate test data if needed
			var data []byte
			if tc.data != nil {
				data = tc.data
			} else if tc.dataSize > 0 {
				data = testutil.GenerateTestData(t, tc.dataSize)
			} else {
				data = testutil.GenerateTestData(t, 1024)
			}

			// Setup bucket
			client.CreateBucket(tc.bucket)

			// Test
			tc.test(t, client, tc.bucket, tc.key, data, ssecKey)

			// Cleanup
			if tc.cleanup != nil {
				tc.cleanup(t, client, tc.bucket, tc.key)
			} else {
				// Default cleanup
				client.DeleteObject(tc.bucket, tc.key)
				client.DeleteBucket(tc.bucket)
			}
		})
	}
}

// =============================================================================
// List Objects Tests (Table-Driven)
// =============================================================================

func TestListObjects(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)

	tests := []struct {
		name    string
		bucket  string
		setup   func(*testing.T, *testutil.S3Client, string)
		test    func(*testing.T, *testutil.S3Client, string)
		cleanup func(*testing.T, *testutil.S3Client, string)
	}{
		{
			name:   "list objects - all objects",
			bucket: uniqueBucket("test-list"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
				keys := []string{"file1.txt", "file2.txt", "folder/file3.txt", "folder/subfolder/file4.txt"}
				for _, key := range keys {
					c.PutObject(bucket, key, []byte("test data for "+key))
				}
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				resp := c.ListObjects(bucket)
				assert.Equal(t, 4, len(resp.Contents), "should have 4 objects")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				resp := c.ListObjects(bucket)
				for _, obj := range resp.Contents {
					c.DeleteObject(bucket, *obj.Key)
				}
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "list objects - with prefix",
			bucket: uniqueBucket("test-list-prefix"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
				c.PutObject(bucket, "a/1.txt", []byte("a1"))
				c.PutObject(bucket, "a/2.txt", []byte("a2"))
				c.PutObject(bucket, "b/1.txt", []byte("b1"))
				c.PutObject(bucket, "b/2.txt", []byte("b2"))
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				resp := c.ListObjects(bucket, testutil.WithS3Prefix("a/"))
				assert.Equal(t, 2, len(resp.Contents), "should have 2 objects with prefix a/")

				for _, obj := range resp.Contents {
					assert.True(t, strings.HasPrefix(*obj.Key, "a/"), "key should have prefix a/")
				}
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				resp := c.ListObjects(bucket)
				for _, obj := range resp.Contents {
					c.DeleteObject(bucket, *obj.Key)
				}
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "list objects - with delimiter",
			bucket: uniqueBucket("test-list-delim"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
				c.PutObject(bucket, "root.txt", []byte("root"))
				c.PutObject(bucket, "folder1/file1.txt", []byte("f1"))
				c.PutObject(bucket, "folder1/file2.txt", []byte("f2"))
				c.PutObject(bucket, "folder2/file1.txt", []byte("f1"))
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				resp, err := c.Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
					Bucket:    aws.String(bucket),
					Delimiter: aws.String("/"),
				})
				require.NoError(t, err)

				// Should have 1 object (root.txt) and 2 common prefixes (folder1/, folder2/)
				assert.Equal(t, 1, len(resp.Contents), "should have 1 root object")
				assert.Equal(t, 2, len(resp.CommonPrefixes), "should have 2 common prefixes")
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				resp := c.ListObjects(bucket)
				for _, obj := range resp.Contents {
					c.DeleteObject(bucket, *obj.Key)
				}
				c.DeleteBucket(bucket)
			},
		},
		{
			name:   "list objects - pagination",
			bucket: uniqueBucket("test-list-page"),
			setup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				c.CreateBucket(bucket)
				// Create many objects
				for i := 0; i < 10; i++ {
					key := uniqueKey("obj")
					c.PutObject(bucket, key, []byte("data"))
				}
			},
			test: func(t *testing.T, c *testutil.S3Client, bucket string) {
				resp := c.ListObjects(bucket, testutil.WithS3MaxKeys(3))
				assert.LessOrEqual(t, len(resp.Contents), 3, "should have at most 3 objects")

				if *resp.IsTruncated {
					t.Logf("List was truncated, continuation token: %s", *resp.NextContinuationToken)
				}
			},
			cleanup: func(t *testing.T, c *testutil.S3Client, bucket string) {
				resp := c.ListObjects(bucket)
				for _, obj := range resp.Contents {
					c.DeleteObject(bucket, *obj.Key)
				}
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

// =============================================================================
// Concurrent Access Tests
// =============================================================================

func TestConcurrentPutGetObjects(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-concurrent")

	client.CreateBucket(bucket)
	defer func() {
		resp := client.ListObjects(bucket)
		for _, obj := range resp.Contents {
			client.DeleteObject(bucket, *obj.Key)
		}
		client.DeleteBucket(bucket)
	}()

	numObjects := 10
	done := make(chan bool, numObjects)

	for i := 0; i < numObjects; i++ {
		go func(idx int) {
			key := uniqueKey("concurrent")
			data := testutil.GenerateTestData(t, 1024)

			client.PutObject(bucket, key, data)
			retrieved := client.GetObject(bucket, key)

			if !bytes.Equal(data, retrieved) {
				t.Errorf("data mismatch for object %d", idx)
			}

			done <- true
		}(i)
	}

	for i := 0; i < numObjects; i++ {
		<-done
	}
}
