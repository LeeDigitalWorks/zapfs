//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketTagging(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("put and get bucket tagging", func(t *testing.T) {
		bucket := uniqueBucket("test-bucket-tagging")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put bucket tagging
		tags := []s3types.Tag{
			{Key: aws.String("Environment"), Value: aws.String("Production")},
			{Key: aws.String("Team"), Value: aws.String("Engineering")},
			{Key: aws.String("CostCenter"), Value: aws.String("12345")},
		}

		_, err := rawClient.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
			Bucket: aws.String(bucket),
			Tagging: &s3types.Tagging{
				TagSet: tags,
			},
		})
		require.NoError(t, err)

		// Get bucket tagging
		resp, err := rawClient.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		require.Len(t, resp.TagSet, 3)

		// Verify tags (order may vary)
		tagMap := make(map[string]string)
		for _, tag := range resp.TagSet {
			tagMap[*tag.Key] = *tag.Value
		}
		assert.Equal(t, "Production", tagMap["Environment"])
		assert.Equal(t, "Engineering", tagMap["Team"])
		assert.Equal(t, "12345", tagMap["CostCenter"])
	})

	t.Run("delete bucket tagging", func(t *testing.T) {
		bucket := uniqueBucket("test-bucket-tagging-delete")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put bucket tagging
		_, err := rawClient.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
			Bucket: aws.String(bucket),
			Tagging: &s3types.Tagging{
				TagSet: []s3types.Tag{
					{Key: aws.String("Key1"), Value: aws.String("Value1")},
				},
			},
		})
		require.NoError(t, err)

		// Verify tagging exists
		_, err = rawClient.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		// Delete bucket tagging
		_, err = rawClient.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		// Verify tagging is gone
		_, err = rawClient.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
			Bucket: aws.String(bucket),
		})
		require.Error(t, err, "should get error when tagging doesn't exist")
	})

	t.Run("replace bucket tagging", func(t *testing.T) {
		bucket := uniqueBucket("test-bucket-tagging-replace")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketTagging(ctx, &s3.DeleteBucketTaggingInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put initial tagging
		_, err := rawClient.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
			Bucket: aws.String(bucket),
			Tagging: &s3types.Tagging{
				TagSet: []s3types.Tag{
					{Key: aws.String("Initial"), Value: aws.String("Tag")},
				},
			},
		})
		require.NoError(t, err)

		// Replace with new tagging
		_, err = rawClient.PutBucketTagging(ctx, &s3.PutBucketTaggingInput{
			Bucket: aws.String(bucket),
			Tagging: &s3types.Tagging{
				TagSet: []s3types.Tag{
					{Key: aws.String("Replaced"), Value: aws.String("NewTag")},
				},
			},
		})
		require.NoError(t, err)

		// Verify new tagging replaced old
		resp, err := rawClient.GetBucketTagging(ctx, &s3.GetBucketTaggingInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		require.Len(t, resp.TagSet, 1)
		assert.Equal(t, "Replaced", *resp.TagSet[0].Key)
		assert.Equal(t, "NewTag", *resp.TagSet[0].Value)
	})
}

func TestObjectTagging(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("put and get object tagging", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-tagging")
		key := uniqueKey("tagged-object")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, []byte("test content"))
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object tagging
		tags := []s3types.Tag{
			{Key: aws.String("Project"), Value: aws.String("ZapFS")},
			{Key: aws.String("Status"), Value: aws.String("Active")},
		}

		_, err := rawClient.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Tagging: &s3types.Tagging{
				TagSet: tags,
			},
		})
		require.NoError(t, err)

		// Get object tagging
		resp, err := rawClient.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		require.Len(t, resp.TagSet, 2)

		// Verify tags
		tagMap := make(map[string]string)
		for _, tag := range resp.TagSet {
			tagMap[*tag.Key] = *tag.Value
		}
		assert.Equal(t, "ZapFS", tagMap["Project"])
		assert.Equal(t, "Active", tagMap["Status"])
	})

	t.Run("delete object tagging", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-tagging-delete")
		key := uniqueKey("tagged-object")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, []byte("test content"))
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object tagging
		_, err := rawClient.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Tagging: &s3types.Tagging{
				TagSet: []s3types.Tag{
					{Key: aws.String("ToDelete"), Value: aws.String("Yes")},
				},
			},
		})
		require.NoError(t, err)

		// Delete object tagging
		_, err = rawClient.DeleteObjectTagging(ctx, &s3.DeleteObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Verify tagging is gone (empty tag set)
		resp, err := rawClient.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.Empty(t, resp.TagSet, "tag set should be empty after delete")
	})

	t.Run("put object with tagging header", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-tagging-header")
		key := uniqueKey("tagged-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put object with tagging header (URL-encoded key=value pairs)
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:  aws.String(bucket),
			Key:     aws.String(key),
			Body:    nil,
			Tagging: aws.String("Key1=Value1&Key2=Value2"),
		})
		require.NoError(t, err)

		// Get object tagging
		resp, err := rawClient.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		require.Len(t, resp.TagSet, 2)

		// Verify tags
		tagMap := make(map[string]string)
		for _, tag := range resp.TagSet {
			tagMap[*tag.Key] = *tag.Value
		}
		assert.Equal(t, "Value1", tagMap["Key1"])
		assert.Equal(t, "Value2", tagMap["Key2"])
	})

	t.Run("copy object with tagging directive COPY", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-tagging-copy")
		srcKey := uniqueKey("source-tagged")
		dstKey := uniqueKey("dest-tagged")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, srcKey)
			client.DeleteObject(bucket, dstKey)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put source object
		client.PutObject(bucket, srcKey, []byte("source content"))

		// Tag source object
		_, err := rawClient.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Tagging: &s3types.Tagging{
				TagSet: []s3types.Tag{
					{Key: aws.String("SourceTag"), Value: aws.String("Copied")},
				},
			},
		})
		require.NoError(t, err)

		// Copy with COPY tagging directive (default)
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:           aws.String(bucket),
			Key:              aws.String(dstKey),
			CopySource:       aws.String(bucket + "/" + srcKey),
			TaggingDirective: s3types.TaggingDirectiveCopy,
		})
		require.NoError(t, err)

		// Verify destination has same tags
		resp, err := rawClient.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		require.Len(t, resp.TagSet, 1)
		assert.Equal(t, "SourceTag", *resp.TagSet[0].Key)
		assert.Equal(t, "Copied", *resp.TagSet[0].Value)
	})

	t.Run("copy object with tagging directive REPLACE", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-tagging-replace")
		srcKey := uniqueKey("source-tagged")
		dstKey := uniqueKey("dest-tagged")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, srcKey)
			client.DeleteObject(bucket, dstKey)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Put source object
		client.PutObject(bucket, srcKey, []byte("source content"))

		// Tag source object
		_, err := rawClient.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(srcKey),
			Tagging: &s3types.Tagging{
				TagSet: []s3types.Tag{
					{Key: aws.String("SourceTag"), Value: aws.String("Original")},
				},
			},
		})
		require.NoError(t, err)

		// Copy with REPLACE tagging directive and new tags
		_, err = rawClient.CopyObject(ctx, &s3.CopyObjectInput{
			Bucket:           aws.String(bucket),
			Key:              aws.String(dstKey),
			CopySource:       aws.String(bucket + "/" + srcKey),
			TaggingDirective: s3types.TaggingDirectiveReplace,
			Tagging:          aws.String("NewTag=Replaced"),
		})
		require.NoError(t, err)

		// Verify destination has new tags, not source tags
		resp, err := rawClient.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(dstKey),
		})
		require.NoError(t, err)
		require.Len(t, resp.TagSet, 1)
		assert.Equal(t, "NewTag", *resp.TagSet[0].Key)
		assert.Equal(t, "Replaced", *resp.TagSet[0].Value)
	})
}
