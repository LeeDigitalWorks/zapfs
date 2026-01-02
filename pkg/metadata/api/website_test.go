// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBucketWebsiteHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupWebsite   *s3types.WebsiteConfiguration
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "no website configured",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			expectedStatus: http.StatusNotFound, // NoSuchWebsiteConfiguration
		},
		{
			name:   "website exists",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupWebsite: &s3types.WebsiteConfiguration{
				IndexDocument: &s3types.IndexDocument{Suffix: "index.html"},
				ErrorDocument: &s3types.ErrorDocument{Key: "error.html"},
			},
			expectedStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup bucket in cache
			if tc.setupBucket != nil {
				err := srv.db.CreateBucket(ctx, tc.setupBucket)
				require.NoError(t, err)
				srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
					Name:       tc.setupBucket.Name,
					OwnerID:    tc.setupBucket.OwnerID,
					CreateTime: time.Unix(0, tc.setupBucket.CreatedAt),
				})
			}

			// Setup website in DB
			if tc.setupWebsite != nil {
				err := srv.db.SetBucketWebsite(ctx, tc.bucket, tc.setupWebsite)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?website", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetBucketWebsiteHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.WebsiteConfiguration
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				if tc.setupWebsite.IndexDocument != nil {
					assert.Equal(t, tc.setupWebsite.IndexDocument.Suffix, result.IndexDocument.Suffix)
				}
			}
		})
	}
}

func TestPutBucketWebsiteHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		requestBody    string
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			requestBody:    `<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "valid website config",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration>
  <IndexDocument>
    <Suffix>index.html</Suffix>
  </IndexDocument>
  <ErrorDocument>
    <Key>error.html</Key>
  </ErrorDocument>
</WebsiteConfiguration>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "redirect all requests",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration>
  <RedirectAllRequestsTo>
    <HostName>example.com</HostName>
    <Protocol>https</Protocol>
  </RedirectAllRequestsTo>
</WebsiteConfiguration>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "malformed XML",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody:    `not valid xml`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:   "empty body",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody:    ``,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup bucket
			if tc.setupBucket != nil {
				err := srv.db.CreateBucket(ctx, tc.setupBucket)
				require.NoError(t, err)
				srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
					Name:       tc.setupBucket.Name,
					OwnerID:    tc.setupBucket.OwnerID,
					CreateTime: time.Unix(0, tc.setupBucket.CreatedAt),
				})
			}

			// Create request
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?website", strings.NewReader(tc.requestBody))
			req.Header.Set("Content-Type", "application/xml")
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.PutBucketWebsiteHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify website was stored
			if tc.expectedStatus == http.StatusOK {
				website, err := srv.db.GetBucketWebsite(ctx, tc.bucket)
				require.NoError(t, err)
				assert.NotNil(t, website)
			}
		})
	}
}

func TestDeleteBucketWebsiteHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupWebsite   bool
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "delete existing website",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupWebsite:   true,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:   "delete non-existing website (idempotent)",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupWebsite:   false,
			expectedStatus: http.StatusNoContent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup bucket
			if tc.setupBucket != nil {
				err := srv.db.CreateBucket(ctx, tc.setupBucket)
				require.NoError(t, err)
				srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
					Name:       tc.setupBucket.Name,
					OwnerID:    tc.setupBucket.OwnerID,
					CreateTime: time.Unix(0, tc.setupBucket.CreatedAt),
				})
			}

			// Setup website
			if tc.setupWebsite {
				website := &s3types.WebsiteConfiguration{
					IndexDocument: &s3types.IndexDocument{Suffix: "index.html"},
				}
				err := srv.db.SetBucketWebsite(ctx, tc.bucket, website)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("DELETE", "/"+tc.bucket+"?website", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.DeleteBucketWebsiteHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}
