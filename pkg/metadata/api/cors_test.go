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

func TestGetBucketCorsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupCORS      *s3types.CORSConfiguration
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "no CORS configured",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			expectedStatus: http.StatusNotFound, // NoSuchCORSConfiguration
		},
		{
			name:   "CORS exists",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupCORS: &s3types.CORSConfiguration{
				Rules: []s3types.CORSRule{
					{
						AllowedOrigins: []string{"https://example.com"},
						AllowedMethods: []string{"GET", "PUT"},
						AllowedHeaders: []string{"*"},
						MaxAgeSeconds:  3600,
					},
				},
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

			// Setup CORS in DB
			if tc.setupCORS != nil {
				err := srv.db.SetBucketCORS(ctx, tc.bucket, tc.setupCORS)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?cors", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetBucketCorsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.CORSConfiguration
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				assert.Len(t, result.Rules, len(tc.setupCORS.Rules))
			}
		})
	}
}

func TestPutBucketCorsHandler(t *testing.T) {
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
			requestBody:    `<CORSConfiguration><CORSRule><AllowedOrigin>*</AllowedOrigin><AllowedMethod>GET</AllowedMethod></CORSRule></CORSConfiguration>`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "valid CORS",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration>
  <CORSRule>
    <AllowedOrigin>https://example.com</AllowedOrigin>
    <AllowedMethod>GET</AllowedMethod>
    <AllowedMethod>PUT</AllowedMethod>
    <AllowedHeader>*</AllowedHeader>
    <MaxAgeSeconds>3600</MaxAgeSeconds>
  </CORSRule>
</CORSConfiguration>`,
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
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?cors", strings.NewReader(tc.requestBody))
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

			srv.PutBucketCorsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify CORS was stored
			if tc.expectedStatus == http.StatusOK {
				cors, err := srv.db.GetBucketCORS(ctx, tc.bucket)
				require.NoError(t, err)
				assert.NotNil(t, cors)
			}
		})
	}
}

func TestDeleteBucketCorsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupCORS      bool
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "delete existing CORS",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupCORS:      true,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:   "delete non-existing CORS (idempotent)",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupCORS:      false,
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

			// Setup CORS
			if tc.setupCORS {
				cors := &s3types.CORSConfiguration{
					Rules: []s3types.CORSRule{
						{
							AllowedOrigins: []string{"*"},
							AllowedMethods: []string{"GET"},
						},
					},
				}
				err := srv.db.SetBucketCORS(ctx, tc.bucket, cors)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("DELETE", "/"+tc.bucket+"?cors", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.DeleteBucketCorsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}

func TestMatchCORSOrigin(t *testing.T) {
	tests := []struct {
		name     string
		allowed  []string
		origin   string
		expected bool
	}{
		{"exact match", []string{"https://example.com"}, "https://example.com", true},
		{"no match", []string{"https://example.com"}, "https://other.com", false},
		{"wildcard allows all", []string{"*"}, "https://anything.com", true},
		{"wildcard suffix", []string{"*.example.com"}, "https://sub.example.com", true},
		{"wildcard suffix no match", []string{"*.example.com"}, "https://example.com", false},
		{"multiple allowed", []string{"https://a.com", "https://b.com"}, "https://b.com", true},
		{"http wildcard suffix", []string{"http://*.example.com"}, "http://app.example.com", true},
		{"empty allowed", []string{}, "https://example.com", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchCORSOrigin(tc.allowed, tc.origin)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMatchCORSMethod(t *testing.T) {
	tests := []struct {
		name     string
		allowed  []string
		method   string
		expected bool
	}{
		{"GET allowed", []string{"GET", "PUT"}, "GET", true},
		{"PUT allowed", []string{"GET", "PUT"}, "PUT", true},
		{"POST not allowed", []string{"GET", "PUT"}, "POST", false},
		{"empty allowed", []string{}, "GET", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchCORSMethod(tc.allowed, tc.method)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMatchCORSHeaders(t *testing.T) {
	tests := []struct {
		name           string
		allowedHeaders []string
		requestHeaders string
		expected       bool
	}{
		{"no headers requested", []string{"Content-Type"}, "", true},
		{"wildcard allows all", []string{"*"}, "Content-Type, X-Custom", true},
		{"specific header allowed", []string{"Content-Type"}, "Content-Type", true},
		{"case insensitive match", []string{"content-type"}, "Content-Type", true},
		{"multiple headers allowed", []string{"Content-Type", "X-Custom"}, "Content-Type, X-Custom", true},
		{"header not allowed", []string{"Content-Type"}, "X-Custom", false},
		{"partial match fails", []string{"Content-Type"}, "Content-Type, X-Custom", false},
		{"no allowed headers", []string{}, "Content-Type", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchCORSHeaders(tc.allowedHeaders, tc.requestHeaders)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestOptionsPreflightHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                string
		bucket              string
		setupBucket         *types.BucketInfo
		setupCORS           *s3types.CORSConfiguration
		origin              string
		requestMethod       string
		requestHeaders      string
		expectedStatus      int
		expectedAllowOrigin string
	}{
		{
			name:           "no origin header",
			bucket:         "test-bucket",
			origin:         "",
			requestMethod:  "GET",
			expectedStatus: http.StatusForbidden,
		},
		{
			name:           "no request method header",
			bucket:         "test-bucket",
			origin:         "https://example.com",
			requestMethod:  "",
			expectedStatus: http.StatusForbidden,
		},
		{
			name:   "no CORS config",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			origin:         "https://example.com",
			requestMethod:  "GET",
			expectedStatus: http.StatusForbidden,
		},
		{
			name:   "matching rule",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupCORS: &s3types.CORSConfiguration{
				Rules: []s3types.CORSRule{
					{
						AllowedOrigins: []string{"https://example.com"},
						AllowedMethods: []string{"GET", "PUT"},
						AllowedHeaders: []string{"*"},
						MaxAgeSeconds:  3600,
					},
				},
			},
			origin:              "https://example.com",
			requestMethod:       "GET",
			expectedStatus:      http.StatusOK,
			expectedAllowOrigin: "https://example.com",
		},
		{
			name:   "origin not allowed",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupCORS: &s3types.CORSConfiguration{
				Rules: []s3types.CORSRule{
					{
						AllowedOrigins: []string{"https://allowed.com"},
						AllowedMethods: []string{"GET"},
					},
				},
			},
			origin:         "https://notallowed.com",
			requestMethod:  "GET",
			expectedStatus: http.StatusForbidden,
		},
		{
			name:   "method not allowed",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupCORS: &s3types.CORSConfiguration{
				Rules: []s3types.CORSRule{
					{
						AllowedOrigins: []string{"https://example.com"},
						AllowedMethods: []string{"GET"},
					},
				},
			},
			origin:         "https://example.com",
			requestMethod:  "DELETE",
			expectedStatus: http.StatusForbidden,
		},
		{
			name:   "headers not allowed",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupCORS: &s3types.CORSConfiguration{
				Rules: []s3types.CORSRule{
					{
						AllowedOrigins: []string{"https://example.com"},
						AllowedMethods: []string{"GET"},
						AllowedHeaders: []string{"Content-Type"},
					},
				},
			},
			origin:         "https://example.com",
			requestMethod:  "GET",
			requestHeaders: "X-Custom-Header",
			expectedStatus: http.StatusForbidden,
		},
		{
			name:   "wildcard origin",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupCORS: &s3types.CORSConfiguration{
				Rules: []s3types.CORSRule{
					{
						AllowedOrigins: []string{"*"},
						AllowedMethods: []string{"GET"},
					},
				},
			},
			origin:              "https://any-origin.com",
			requestMethod:       "GET",
			expectedStatus:      http.StatusOK,
			expectedAllowOrigin: "https://any-origin.com",
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

			// Setup CORS
			if tc.setupCORS != nil {
				err := srv.db.SetBucketCORS(ctx, tc.bucket, tc.setupCORS)
				require.NoError(t, err)
			}

			// Create OPTIONS request
			req := httptest.NewRequest("OPTIONS", "/"+tc.bucket+"/object", nil)
			if tc.origin != "" {
				req.Header.Set("Origin", tc.origin)
			}
			if tc.requestMethod != "" {
				req.Header.Set("Access-Control-Request-Method", tc.requestMethod)
			}
			if tc.requestHeaders != "" {
				req.Header.Set("Access-Control-Request-Headers", tc.requestHeaders)
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

			srv.OptionsPreflightHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Equal(t, tc.expectedAllowOrigin, w.Header().Get("Access-Control-Allow-Origin"))
				assert.NotEmpty(t, w.Header().Get("Access-Control-Allow-Methods"))
				assert.Equal(t, "Origin, Access-Control-Request-Method, Access-Control-Request-Headers", w.Header().Get("Vary"))
			}
		})
	}
}
