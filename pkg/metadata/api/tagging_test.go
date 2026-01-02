// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBucketTaggingHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		createBucket bool
		setupTags    bool
		expectedCode int
	}{
		{
			name:         "bucket not found",
			bucket:       "nonexistent-bucket",
			createBucket: false,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "bucket with no tags",
			bucket:       "test-bucket",
			createBucket: true,
			setupTags:    false,
			expectedCode: http.StatusNotFound, // NoSuchTagSet error
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			if tc.createBucket {
				err := srv.db.CreateBucket(ctx, &types.BucketInfo{
					Name:    tc.bucket,
					OwnerID: "test-owner",
				})
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, "", "test-owner")
			w := httptest.NewRecorder()

			srv.GetBucketTaggingHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)
		})
	}
}

func TestPutBucketTaggingHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		tags         []s3types.Tag
		expectedCode int
		verifyCount  int
	}{
		{
			name:   "successfully set tags",
			bucket: "test-bucket",
			tags: []s3types.Tag{
				{Key: "env", Value: "production"},
				{Key: "team", Value: "backend"},
			},
			expectedCode: http.StatusNoContent, // S3 returns 204
			verifyCount:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Add bucket to cache (handlers check cache first)
			srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})

			// Create tagging request
			tagging := s3types.TagSet{Tags: tc.tags}
			body, _ := xml.Marshal(tagging)

			d := createTestData(tc.bucket, "", "test-owner")
			d.Req = httptest.NewRequest("PUT", "/"+tc.bucket+"?tagging", bytes.NewReader(body))
			w := httptest.NewRecorder()

			srv.PutBucketTaggingHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			// Verify tags were stored
			storedTags, err := srv.db.GetBucketTagging(ctx, tc.bucket)
			require.NoError(t, err)
			assert.Len(t, storedTags.Tags, tc.verifyCount)
		})
	}
}

func TestDeleteBucketTaggingHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		setupTags    bool
		expectedCode int
		verifyDelete bool
	}{
		{
			name:         "delete existing tags",
			bucket:       "test-bucket",
			setupTags:    true,
			expectedCode: http.StatusNoContent,
			verifyDelete: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Add bucket to cache (handlers check cache first)
			srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})

			if tc.setupTags {
				tags := &s3types.TagSet{
					Tags: []s3types.Tag{
						{Key: "env", Value: "production"},
					},
				}
				err = srv.db.SetBucketTagging(ctx, tc.bucket, tags)
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, "", "test-owner")
			w := httptest.NewRecorder()

			srv.DeleteBucketTaggingHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.verifyDelete {
				_, err = srv.db.GetBucketTagging(ctx, tc.bucket)
				assert.Error(t, err) // Should return not found
			}
		})
	}
}

func TestGetObjectTaggingHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		key          string
		createObject bool
		expectedCode int
	}{
		{
			name:         "object not found",
			bucket:       "test-bucket",
			key:          "nonexistent.txt",
			createObject: false,
			expectedCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			d := createTestData(tc.bucket, tc.key, "test-owner")
			w := httptest.NewRecorder()

			srv.GetObjectTaggingHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)
		})
	}
}

func TestPutObjectTaggingHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		key          string
		tags         []s3types.Tag
		expectedCode int
		verifyKey    string
		verifyValue  string
	}{
		{
			name:   "successfully set object tags",
			bucket: "test-bucket",
			key:    "test.txt",
			tags: []s3types.Tag{
				{Key: "status", Value: "processed"},
			},
			expectedCode: http.StatusOK,
			verifyKey:    "status",
			verifyValue:  "processed",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Create object
			err = srv.db.PutObject(ctx, &types.ObjectRef{
				Bucket: tc.bucket,
				Key:    tc.key,
				Size:   100,
				ETag:   "abc123",
			})
			require.NoError(t, err)

			// Create tagging request
			tagging := s3types.TagSet{Tags: tc.tags}
			body, _ := xml.Marshal(tagging)

			d := createTestData(tc.bucket, tc.key, "test-owner")
			d.Req = httptest.NewRequest("PUT", "/"+tc.bucket+"/"+tc.key+"?tagging", bytes.NewReader(body))
			w := httptest.NewRecorder()

			srv.PutObjectTaggingHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			// Verify tags were stored
			storedTags, err := srv.db.GetObjectTagging(ctx, tc.bucket, tc.key)
			require.NoError(t, err)
			assert.Len(t, storedTags.Tags, len(tc.tags))
			if len(storedTags.Tags) > 0 {
				assert.Equal(t, tc.verifyKey, storedTags.Tags[0].Key)
				assert.Equal(t, tc.verifyValue, storedTags.Tags[0].Value)
			}
		})
	}
}

func TestDeleteObjectTaggingHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		key          string
		setupTags    bool
		expectedCode int
		verifyDelete bool
	}{
		{
			name:         "delete existing object tags",
			bucket:       "test-bucket",
			key:          "test.txt",
			setupTags:    true,
			expectedCode: http.StatusNoContent,
			verifyDelete: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Create object
			err = srv.db.PutObject(ctx, &types.ObjectRef{
				Bucket: tc.bucket,
				Key:    tc.key,
				Size:   100,
				ETag:   "abc123",
			})
			require.NoError(t, err)

			if tc.setupTags {
				tags := &s3types.TagSet{
					Tags: []s3types.Tag{
						{Key: "status", Value: "processed"},
					},
				}
				err = srv.db.SetObjectTagging(ctx, tc.bucket, tc.key, tags)
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, tc.key, "test-owner")
			w := httptest.NewRecorder()

			srv.DeleteObjectTaggingHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.verifyDelete {
				_, err = srv.db.GetObjectTagging(ctx, tc.bucket, tc.key)
				assert.Error(t, err)
			}
		})
	}
}
