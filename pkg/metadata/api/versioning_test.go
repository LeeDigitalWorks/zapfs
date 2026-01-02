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

func TestGetBucketVersioningHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name             string
		bucket           string
		setupBucket      *types.BucketInfo
		expectedStatus   int
		expectedEnabled  bool
		expectedResponse string // "Enabled", "Suspended", or ""
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent-bucket",
			// No setup bucket
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "versioning not set (empty)",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			expectedStatus:   http.StatusOK,
			expectedResponse: "",
		},
		{
			name:   "versioning enabled",
			bucket: "versioned-bucket",
			setupBucket: &types.BucketInfo{
				ID:         uuid.New(),
				Name:       "versioned-bucket",
				OwnerID:    "test-owner",
				Versioning: "Enabled",
				CreatedAt:  time.Now().UnixNano(),
			},
			expectedStatus:   http.StatusOK,
			expectedResponse: "Enabled",
		},
		{
			name:   "versioning suspended",
			bucket: "suspended-bucket",
			setupBucket: &types.BucketInfo{
				ID:         uuid.New(),
				Name:       "suspended-bucket",
				OwnerID:    "test-owner",
				Versioning: "Suspended",
				CreatedAt:  time.Now().UnixNano(),
			},
			expectedStatus:   http.StatusOK,
			expectedResponse: "Suspended",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup bucket if provided - add to both DB and cache
			if tc.setupBucket != nil {
				err := srv.db.CreateBucket(ctx, tc.setupBucket)
				require.NoError(t, err)
				// Also add to bucket store cache (service layer checks cache)
				srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
					Name:       tc.setupBucket.Name,
					OwnerID:    tc.setupBucket.OwnerID,
					CreateTime: time.Unix(0, tc.setupBucket.CreatedAt),
					Versioning: s3types.Versioning(tc.setupBucket.Versioning),
				})
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?versioning", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetBucketVersioningHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.VersioningConfiguration
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)

				assert.Equal(t, tc.expectedResponse, result.Status)
			}
		})
	}
}

func TestPutBucketVersioningHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		requestBody    string
		expectedStatus int
		verifyState    string // expected versioning state after handler
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent-bucket",
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Enabled</Status>
</VersioningConfiguration>`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "enable versioning",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Enabled</Status>
</VersioningConfiguration>`,
			expectedStatus: http.StatusOK,
			verifyState:    "Enabled",
		},
		{
			name:   "suspend versioning",
			bucket: "versioned-bucket",
			setupBucket: &types.BucketInfo{
				ID:         uuid.New(),
				Name:       "versioned-bucket",
				OwnerID:    "test-owner",
				Versioning: "Enabled",
				CreatedAt:  time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Suspended</Status>
</VersioningConfiguration>`,
			expectedStatus: http.StatusOK,
			verifyState:    "Suspended",
		},
		{
			name:   "MFA delete not supported",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>Enabled</Status>
  <MfaDelete>Enabled</MfaDelete>
</VersioningConfiguration>`,
			expectedStatus: http.StatusNotImplemented,
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
			name:   "invalid status value",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Status>InvalidValue</Status>
</VersioningConfiguration>`,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup bucket if provided - add to both DB and cache
			if tc.setupBucket != nil {
				err := srv.db.CreateBucket(ctx, tc.setupBucket)
				require.NoError(t, err)
				// Also add to bucket store cache (service layer checks cache)
				srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
					Name:       tc.setupBucket.Name,
					OwnerID:    tc.setupBucket.OwnerID,
					CreateTime: time.Unix(0, tc.setupBucket.CreatedAt),
					Versioning: s3types.Versioning(tc.setupBucket.Versioning),
				})
			}

			// Create request
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?versioning", strings.NewReader(tc.requestBody))
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

			srv.PutBucketVersioningHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify state if expected
			if tc.verifyState != "" && tc.setupBucket != nil {
				bucket, err := srv.db.GetBucket(ctx, tc.bucket)
				require.NoError(t, err)
				assert.Equal(t, tc.verifyState, bucket.Versioning)
			}
		})
	}
}

func TestListObjectVersionsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		bucket            string
		setupBucket       *types.BucketInfo
		queryParams       map[string]string
		expectedStatus    int
		expectedVersions  int
		expectedMarkers   int
		expectedTruncated bool
	}{
		{
			name:   "empty bucket returns empty list",
			bucket: "empty-bucket",
			setupBucket: &types.BucketInfo{
				ID:         uuid.New(),
				Name:       "empty-bucket",
				OwnerID:    "test-owner",
				Versioning: "Enabled",
				CreatedAt:  time.Now().UnixNano(),
			},
			expectedStatus:   http.StatusOK,
			expectedVersions: 0,
			expectedMarkers:  0,
		},
		{
			name:   "with prefix filter",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:         uuid.New(),
				Name:       "test-bucket",
				OwnerID:    "test-owner",
				Versioning: "Enabled",
				CreatedAt:  time.Now().UnixNano(),
			},
			queryParams:      map[string]string{"prefix": "folder/"},
			expectedStatus:   http.StatusOK,
			expectedVersions: 0,
		},
		{
			name:   "with max-keys limit",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:         uuid.New(),
				Name:       "test-bucket",
				OwnerID:    "test-owner",
				Versioning: "Enabled",
				CreatedAt:  time.Now().UnixNano(),
			},
			queryParams:    map[string]string{"max-keys": "5"},
			expectedStatus: http.StatusOK,
		},
		{
			name:   "with delimiter for folder simulation",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:         uuid.New(),
				Name:       "test-bucket",
				OwnerID:    "test-owner",
				Versioning: "Enabled",
				CreatedAt:  time.Now().UnixNano(),
			},
			queryParams:    map[string]string{"delimiter": "/"},
			expectedStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup bucket if provided - add to both DB and cache
			if tc.setupBucket != nil {
				err := srv.db.CreateBucket(ctx, tc.setupBucket)
				require.NoError(t, err)
				// Also add to bucket store cache
				srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
					Name:       tc.setupBucket.Name,
					OwnerID:    tc.setupBucket.OwnerID,
					CreateTime: time.Unix(0, tc.setupBucket.CreatedAt),
					Versioning: s3types.Versioning(tc.setupBucket.Versioning),
				})
			}

			// Create request with query params
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?versions", nil)
			if tc.queryParams != nil {
				q := req.URL.Query()
				for k, v := range tc.queryParams {
					q.Set(k, v)
				}
				req.URL.RawQuery = q.Encode()
			}

			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.ListObjectVersionsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.ListVersionsResult
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)

				assert.Equal(t, tc.bucket, result.Name)
				assert.Equal(t, tc.expectedVersions, len(result.Versions))
				assert.Equal(t, tc.expectedMarkers, len(result.DeleteMarkers))
				assert.Equal(t, tc.expectedTruncated, result.IsTruncated)
			}
		})
	}
}
