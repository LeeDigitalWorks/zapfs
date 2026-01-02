//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

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

func TestGetBucketEncryptionHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		bucket          string
		setupBucket     *types.BucketInfo
		setupEncryption *s3types.ServerSideEncryptionConfig
		expectedStatus  int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "no encryption configured",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			expectedStatus: http.StatusNotFound, // ServerSideEncryptionConfigurationNotFoundError
		},
		{
			name:   "AES256 encryption configured",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupEncryption: &s3types.ServerSideEncryptionConfig{
				Rules: []s3types.ServerSideEncryptionRule{
					{
						ApplyServerSideEncryptionByDefault: &s3types.EncryptionByDefault{
							SSEAlgorithm: "AES256",
						},
					},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:   "KMS encryption configured",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupEncryption: &s3types.ServerSideEncryptionConfig{
				Rules: []s3types.ServerSideEncryptionRule{
					{
						ApplyServerSideEncryptionByDefault: &s3types.EncryptionByDefault{
							SSEAlgorithm:   "aws:kms",
							KMSMasterKeyID: "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012",
						},
						BucketKeyEnabled: true,
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

			// Setup encryption in DB
			if tc.setupEncryption != nil {
				err := srv.db.SetBucketEncryption(ctx, tc.bucket, tc.setupEncryption)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?encryption", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetBucketEncryptionHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.ServerSideEncryptionConfig
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				assert.Len(t, result.Rules, len(tc.setupEncryption.Rules))
			}
		})
	}
}

func TestPutBucketEncryptionHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		requestBody    string
		expectedStatus int
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent-bucket",
			requestBody: `<ServerSideEncryptionConfiguration>
				<Rule><ApplyServerSideEncryptionByDefault><SSEAlgorithm>AES256</SSEAlgorithm></ApplyServerSideEncryptionByDefault></Rule>
			</ServerSideEncryptionConfiguration>`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "valid AES256 encryption",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>AES256</SSEAlgorithm>
    </ApplyServerSideEncryptionByDefault>
  </Rule>
</ServerSideEncryptionConfiguration>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "valid KMS encryption",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<ServerSideEncryptionConfiguration>
  <Rule>
    <ApplyServerSideEncryptionByDefault>
      <SSEAlgorithm>aws:kms</SSEAlgorithm>
      <KMSMasterKeyID>arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012</KMSMasterKeyID>
    </ApplyServerSideEncryptionByDefault>
    <BucketKeyEnabled>true</BucketKeyEnabled>
  </Rule>
</ServerSideEncryptionConfiguration>`,
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
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?encryption", strings.NewReader(tc.requestBody))
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

			srv.PutBucketEncryptionHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify encryption was stored
			if tc.expectedStatus == http.StatusOK {
				enc, err := srv.db.GetBucketEncryption(ctx, tc.bucket)
				require.NoError(t, err)
				assert.NotNil(t, enc)
			}
		})
	}
}

func TestDeleteBucketEncryptionHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		bucket          string
		setupBucket     *types.BucketInfo
		setupEncryption bool
		expectedStatus  int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "delete existing encryption",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupEncryption: true,
			expectedStatus:  http.StatusNoContent,
		},
		{
			name:   "delete non-existing encryption (idempotent)",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupEncryption: false,
			expectedStatus:  http.StatusNoContent,
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

			// Setup encryption
			if tc.setupEncryption {
				enc := &s3types.ServerSideEncryptionConfig{
					Rules: []s3types.ServerSideEncryptionRule{
						{
							ApplyServerSideEncryptionByDefault: &s3types.EncryptionByDefault{
								SSEAlgorithm: "AES256",
							},
						},
					},
				}
				err := srv.db.SetBucketEncryption(ctx, tc.bucket, enc)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("DELETE", "/"+tc.bucket+"?encryption", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.DeleteBucketEncryptionHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}
