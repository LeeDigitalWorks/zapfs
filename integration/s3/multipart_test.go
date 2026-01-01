//go:build integration

package s3

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// =============================================================================
// Multipart Upload Tests
// =============================================================================

func TestMultipartUpload(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)

	tests := []struct {
		name      string
		bucket    string
		key       string
		partSize  int // Size of each part in bytes
		numParts  int
		test      func(*testing.T, *s3.Client, string, string, int, int)
		skipShort bool
	}{
		{
			name:     "basic multipart upload - 3 parts",
			bucket:   uniqueBucket("test-mpu"),
			key:      uniqueKey("multipart-object"),
			partSize: 5 * 1024 * 1024, // 5MB minimum
			numParts: 3,
			test: func(t *testing.T, c *s3.Client, bucket, key string, partSize, numParts int) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				// Create multipart upload
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId
				t.Logf("Created multipart upload: %s", uploadID)

				// Upload parts
				var completedParts []types.CompletedPart
				var allData []byte

				for i := 1; i <= numParts; i++ {
					partData := testutil.GenerateTestData(t, partSize)
					allData = append(allData, partData...)

					uploadPartResp, err := c.UploadPart(ctx, &s3.UploadPartInput{
						Bucket:     aws.String(bucket),
						Key:        aws.String(key),
						UploadId:   aws.String(uploadID),
						PartNumber: aws.Int32(int32(i)),
						Body:       bytes.NewReader(partData),
					})
					require.NoError(t, err)
					t.Logf("Uploaded part %d: ETag=%s", i, *uploadPartResp.ETag)

					completedParts = append(completedParts, types.CompletedPart{
						ETag:       uploadPartResp.ETag,
						PartNumber: aws.Int32(int32(i)),
					})
				}

				// Complete multipart upload
				completeResp, err := c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(key),
					UploadId: aws.String(uploadID),
					MultipartUpload: &types.CompletedMultipartUpload{
						Parts: completedParts,
					},
				})
				require.NoError(t, err)
				t.Logf("Completed multipart upload: ETag=%s", *completeResp.ETag)

				// Verify the uploaded object
				getResp, err := c.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				defer getResp.Body.Close()

				retrieved, err := io.ReadAll(getResp.Body)
				require.NoError(t, err)
				assert.Equal(t, allData, retrieved)
			},
		},
		{
			name:     "abort multipart upload",
			bucket:   uniqueBucket("test-mpu-abort"),
			key:      uniqueKey("abort-object"),
			partSize: 5 * 1024 * 1024,
			numParts: 2,
			test: func(t *testing.T, c *s3.Client, bucket, key string, partSize, numParts int) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				// Create multipart upload
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId

				// Upload one part
				partData := testutil.GenerateTestData(t, partSize)
				_, err = c.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(bucket),
					Key:        aws.String(key),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(1),
					Body:       bytes.NewReader(partData),
				})
				require.NoError(t, err)

				// Abort the upload
				_, err = c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(key),
					UploadId: aws.String(uploadID),
				})
				require.NoError(t, err)

				// Verify upload is aborted (listing should not include it)
				listResp, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err)

				for _, upload := range listResp.Uploads {
					assert.NotEqual(t, uploadID, *upload.UploadId, "aborted upload should not appear in list")
				}
			},
		},
		{
			name:     "list parts",
			bucket:   uniqueBucket("test-mpu-listparts"),
			key:      uniqueKey("listparts-object"),
			partSize: 5 * 1024 * 1024,
			numParts: 3,
			test: func(t *testing.T, c *s3.Client, bucket, key string, partSize, numParts int) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				// Create multipart upload
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(key),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId

				// Upload parts
				for i := 1; i <= numParts; i++ {
					partData := testutil.GenerateTestData(t, partSize)
					_, err := c.UploadPart(ctx, &s3.UploadPartInput{
						Bucket:     aws.String(bucket),
						Key:        aws.String(key),
						UploadId:   aws.String(uploadID),
						PartNumber: aws.Int32(int32(i)),
						Body:       bytes.NewReader(partData),
					})
					require.NoError(t, err)
				}

				// List parts
				listResp, err := c.ListParts(ctx, &s3.ListPartsInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(key),
					UploadId: aws.String(uploadID),
				})
				require.NoError(t, err)

				assert.Len(t, listResp.Parts, numParts)
				for i, part := range listResp.Parts {
					assert.Equal(t, int32(i+1), *part.PartNumber)
					assert.NotEmpty(t, *part.ETag)
					assert.Equal(t, int64(partSize), *part.Size)
				}

				// Cleanup: abort the upload
				_, _ = c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(key),
					UploadId: aws.String(uploadID),
				})
			},
		},
		{
			name:     "list multipart uploads",
			bucket:   uniqueBucket("test-mpu-list"),
			key:      uniqueKey("list-object"),
			partSize: 5 * 1024 * 1024,
			numParts: 1,
			test: func(t *testing.T, c *s3.Client, bucket, key string, partSize, numParts int) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				// Create multiple multipart uploads
				var uploadIDs []string
				for i := 0; i < 3; i++ {
					createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
						Bucket: aws.String(bucket),
						Key:    aws.String(fmt.Sprintf("%s-%d", key, i)),
					})
					require.NoError(t, err)
					uploadIDs = append(uploadIDs, *createResp.UploadId)
				}

				// List uploads
				listResp, err := c.ListMultipartUploads(ctx, &s3.ListMultipartUploadsInput{
					Bucket: aws.String(bucket),
				})
				require.NoError(t, err)

				assert.GreaterOrEqual(t, len(listResp.Uploads), 3)

				// Cleanup: abort all uploads
				for i, uploadID := range uploadIDs {
					_, _ = c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
						Bucket:   aws.String(bucket),
						Key:      aws.String(fmt.Sprintf("%s-%d", key, i)),
						UploadId: aws.String(uploadID),
					})
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipShort {
				testutil.SkipIfShort(t)
			}
			t.Parallel()

			// Setup bucket
			client.CreateBucket(tc.bucket)
			defer client.DeleteBucket(tc.bucket)

			// Run test
			tc.test(t, client.Client, tc.bucket, tc.key, tc.partSize, tc.numParts)

			// Cleanup: delete any objects created
			listResp := client.ListObjects(tc.bucket)
			for _, obj := range listResp.Contents {
				client.DeleteObject(tc.bucket, *obj.Key)
			}
		})
	}
}

// =============================================================================
// UploadPartCopy Tests
// =============================================================================

func TestUploadPartCopy(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)

	tests := []struct {
		name      string
		bucket    string
		test      func(*testing.T, *s3.Client, string)
		skipShort bool
	}{
		{
			name:   "upload part copy - full object",
			bucket: uniqueBucket("test-partcopy"),
			test: func(t *testing.T, c *s3.Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				// Create source object (must be at least 5MB for part copy)
				sourceKey := uniqueKey("source")
				sourceData := testutil.GenerateTestData(t, 5*1024*1024)
				_, err := c.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(sourceKey),
					Body:   bytes.NewReader(sourceData),
				})
				require.NoError(t, err)

				// Create multipart upload for destination
				destKey := uniqueKey("dest")
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId

				// Copy source object as a part
				copySource := bucket + "/" + sourceKey
				copyResp, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:     aws.String(bucket),
					Key:        aws.String(destKey),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(1),
					CopySource: aws.String(copySource),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, *copyResp.CopyPartResult.ETag)
				t.Logf("Copied part: ETag=%s", *copyResp.CopyPartResult.ETag)

				// Complete multipart upload
				_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(destKey),
					UploadId: aws.String(uploadID),
					MultipartUpload: &types.CompletedMultipartUpload{
						Parts: []types.CompletedPart{
							{
								ETag:       copyResp.CopyPartResult.ETag,
								PartNumber: aws.Int32(1),
							},
						},
					},
				})
				require.NoError(t, err)

				// Verify destination matches source
				getResp, err := c.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				defer getResp.Body.Close()

				destData, err := io.ReadAll(getResp.Body)
				require.NoError(t, err)
				assert.Equal(t, sourceData, destData)
			},
		},
		{
			name:   "upload part copy - with byte range",
			bucket: uniqueBucket("test-partcopy-range"),
			test: func(t *testing.T, c *s3.Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				// Create source object (10MB)
				sourceKey := uniqueKey("source")
				sourceData := testutil.GenerateTestData(t, 10*1024*1024)
				_, err := c.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(sourceKey),
					Body:   bytes.NewReader(sourceData),
				})
				require.NoError(t, err)

				// Create multipart upload
				destKey := uniqueKey("dest")
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId

				// Copy first 5MB as part 1
				copySource := bucket + "/" + sourceKey
				part1End := 5*1024*1024 - 1
				copyResp1, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:          aws.String(bucket),
					Key:             aws.String(destKey),
					UploadId:        aws.String(uploadID),
					PartNumber:      aws.Int32(1),
					CopySource:      aws.String(copySource),
					CopySourceRange: aws.String(fmt.Sprintf("bytes=0-%d", part1End)),
				})
				require.NoError(t, err)

				// Copy second 5MB as part 2
				part2Start := 5 * 1024 * 1024
				part2End := 10*1024*1024 - 1
				copyResp2, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:          aws.String(bucket),
					Key:             aws.String(destKey),
					UploadId:        aws.String(uploadID),
					PartNumber:      aws.Int32(2),
					CopySource:      aws.String(copySource),
					CopySourceRange: aws.String(fmt.Sprintf("bytes=%d-%d", part2Start, part2End)),
				})
				require.NoError(t, err)

				// Complete upload
				_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(destKey),
					UploadId: aws.String(uploadID),
					MultipartUpload: &types.CompletedMultipartUpload{
						Parts: []types.CompletedPart{
							{ETag: copyResp1.CopyPartResult.ETag, PartNumber: aws.Int32(1)},
							{ETag: copyResp2.CopyPartResult.ETag, PartNumber: aws.Int32(2)},
						},
					},
				})
				require.NoError(t, err)

				// Verify destination matches source
				getResp, err := c.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				defer getResp.Body.Close()

				destData, err := io.ReadAll(getResp.Body)
				require.NoError(t, err)
				assert.Equal(t, sourceData, destData)
			},
			skipShort: true,
		},
		{
			name:   "upload part copy - conditional (If-Match success)",
			bucket: uniqueBucket("test-partcopy-ifmatch"),
			test: func(t *testing.T, c *s3.Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				// Create source object
				sourceKey := uniqueKey("source")
				sourceData := testutil.GenerateTestData(t, 5*1024*1024)
				putResp, err := c.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(sourceKey),
					Body:   bytes.NewReader(sourceData),
				})
				require.NoError(t, err)
				sourceETag := *putResp.ETag

				// Create multipart upload
				destKey := uniqueKey("dest")
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId

				// Copy with matching ETag - should succeed
				copySource := bucket + "/" + sourceKey
				copyResp, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:              aws.String(bucket),
					Key:                 aws.String(destKey),
					UploadId:            aws.String(uploadID),
					PartNumber:          aws.Int32(1),
					CopySource:          aws.String(copySource),
					CopySourceIfMatch:   aws.String(sourceETag),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, *copyResp.CopyPartResult.ETag)

				// Cleanup
				_, _ = c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(destKey),
					UploadId: aws.String(uploadID),
				})
			},
		},
		{
			name:   "upload part copy - conditional (If-Match failure)",
			bucket: uniqueBucket("test-partcopy-ifmatch-fail"),
			test: func(t *testing.T, c *s3.Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				// Create source object
				sourceKey := uniqueKey("source")
				sourceData := testutil.GenerateTestData(t, 5*1024*1024)
				_, err := c.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(sourceKey),
					Body:   bytes.NewReader(sourceData),
				})
				require.NoError(t, err)

				// Create multipart upload
				destKey := uniqueKey("dest")
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId

				// Copy with non-matching ETag - should fail
				copySource := bucket + "/" + sourceKey
				_, err = c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:            aws.String(bucket),
					Key:               aws.String(destKey),
					UploadId:          aws.String(uploadID),
					PartNumber:        aws.Int32(1),
					CopySource:        aws.String(copySource),
					CopySourceIfMatch: aws.String("\"wrong-etag\""),
				})
				assert.Error(t, err, "copy with wrong ETag should fail")

				// Cleanup
				_, _ = c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(destKey),
					UploadId: aws.String(uploadID),
				})
			},
		},
		{
			name:   "upload part copy - source not found",
			bucket: uniqueBucket("test-partcopy-notfound"),
			test: func(t *testing.T, c *s3.Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
				defer cancel()

				// Create multipart upload
				destKey := uniqueKey("dest")
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId

				// Try to copy from non-existent source
				copySource := bucket + "/nonexistent-key"
				_, err = c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:     aws.String(bucket),
					Key:        aws.String(destKey),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(1),
					CopySource: aws.String(copySource),
				})
				assert.Error(t, err, "copy from non-existent source should fail")

				// Cleanup
				_, _ = c.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(destKey),
					UploadId: aws.String(uploadID),
				})
			},
		},
		{
			name:   "upload part copy - mixed regular and copy parts",
			bucket: uniqueBucket("test-partcopy-mixed"),
			test: func(t *testing.T, c *s3.Client, bucket string) {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
				defer cancel()

				// Create source object for part copy
				sourceKey := uniqueKey("source")
				sourceData := testutil.GenerateTestData(t, 5*1024*1024)
				_, err := c.PutObject(ctx, &s3.PutObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(sourceKey),
					Body:   bytes.NewReader(sourceData),
				})
				require.NoError(t, err)

				// Create multipart upload
				destKey := uniqueKey("dest")
				createResp, err := c.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				uploadID := *createResp.UploadId

				// Part 1: Regular upload
				part1Data := testutil.GenerateTestData(t, 5*1024*1024)
				part1Resp, err := c.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(bucket),
					Key:        aws.String(destKey),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(1),
					Body:       bytes.NewReader(part1Data),
				})
				require.NoError(t, err)

				// Part 2: Copy from source
				copySource := bucket + "/" + sourceKey
				part2Resp, err := c.UploadPartCopy(ctx, &s3.UploadPartCopyInput{
					Bucket:     aws.String(bucket),
					Key:        aws.String(destKey),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(2),
					CopySource: aws.String(copySource),
				})
				require.NoError(t, err)

				// Part 3: Regular upload
				part3Data := testutil.GenerateTestData(t, 5*1024*1024)
				part3Resp, err := c.UploadPart(ctx, &s3.UploadPartInput{
					Bucket:     aws.String(bucket),
					Key:        aws.String(destKey),
					UploadId:   aws.String(uploadID),
					PartNumber: aws.Int32(3),
					Body:       bytes.NewReader(part3Data),
				})
				require.NoError(t, err)

				// Complete upload
				_, err = c.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
					Bucket:   aws.String(bucket),
					Key:      aws.String(destKey),
					UploadId: aws.String(uploadID),
					MultipartUpload: &types.CompletedMultipartUpload{
						Parts: []types.CompletedPart{
							{ETag: part1Resp.ETag, PartNumber: aws.Int32(1)},
							{ETag: part2Resp.CopyPartResult.ETag, PartNumber: aws.Int32(2)},
							{ETag: part3Resp.ETag, PartNumber: aws.Int32(3)},
						},
					},
				})
				require.NoError(t, err)

				// Verify the final object size
				headResp, err := c.HeadObject(ctx, &s3.HeadObjectInput{
					Bucket: aws.String(bucket),
					Key:    aws.String(destKey),
				})
				require.NoError(t, err)
				expectedSize := int64(len(part1Data) + len(sourceData) + len(part3Data))
				assert.Equal(t, expectedSize, *headResp.ContentLength)
			},
			skipShort: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skipShort {
				testutil.SkipIfShort(t)
			}
			t.Parallel()

			// Setup bucket
			client.CreateBucket(tc.bucket)
			defer func() {
				// Cleanup: delete all objects and bucket
				listResp := client.ListObjects(tc.bucket)
				for _, obj := range listResp.Contents {
					client.DeleteObject(tc.bucket, *obj.Key)
				}
				client.DeleteBucket(tc.bucket)
			}()

			// Run test
			tc.test(t, client.Client, tc.bucket)
		})
	}
}

// =============================================================================
// Multipart ETag Calculation Tests
// =============================================================================

func TestMultipartETag(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	bucket := uniqueBucket("test-mpu-etag")
	client.CreateBucket(bucket)
	defer func() {
		listResp := client.ListObjects(bucket)
		for _, obj := range listResp.Contents {
			client.DeleteObject(bucket, *obj.Key)
		}
		client.DeleteBucket(bucket)
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	key := uniqueKey("etag-test")
	partSize := 5 * 1024 * 1024
	numParts := 3

	// Create multipart upload
	createResp, err := client.Client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	require.NoError(t, err)
	uploadID := *createResp.UploadId

	// Upload parts and compute expected ETag
	var completedParts []types.CompletedPart
	var partETags []string

	for i := 1; i <= numParts; i++ {
		partData := testutil.GenerateTestData(t, partSize)

		uploadPartResp, err := client.Client.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   aws.String(uploadID),
			PartNumber: aws.Int32(int32(i)),
			Body:       bytes.NewReader(partData),
		})
		require.NoError(t, err)

		etag := strings.Trim(*uploadPartResp.ETag, "\"")
		partETags = append(partETags, etag)
		completedParts = append(completedParts, types.CompletedPart{
			ETag:       uploadPartResp.ETag,
			PartNumber: aws.Int32(int32(i)),
		})
	}

	// Complete upload
	completeResp, err := client.Client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(key),
		UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: completedParts,
		},
	})
	require.NoError(t, err)

	// Verify multipart ETag format: "hash-numparts"
	finalETag := strings.Trim(*completeResp.ETag, "\"")
	parts := strings.Split(finalETag, "-")
	require.Len(t, parts, 2, "multipart ETag should have format 'hash-numparts'")

	partCount, err := strconv.Atoi(parts[1])
	require.NoError(t, err)
	assert.Equal(t, numParts, partCount, "part count in ETag should match")

	// Verify the hash part (MD5 of concatenated part ETags)
	var etagConcat string
	for _, etag := range partETags {
		etagConcat += etag
	}
	expectedHash := md5.Sum([]byte(etagConcat))
	expectedHashHex := hex.EncodeToString(expectedHash[:])
	assert.Equal(t, expectedHashHex, parts[0], "ETag hash should be MD5 of concatenated part ETags")
}
