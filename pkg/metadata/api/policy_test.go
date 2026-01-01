package api

import (
	"context"
	"encoding/json"
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

func TestGetBucketPolicyHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupPolicy    *s3types.BucketPolicy
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "no policy configured",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			// Per AWS S3 spec, GetBucketPolicy returns 404/NoSuchBucketPolicy when no policy exists
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "policy exists",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupPolicy: &s3types.BucketPolicy{
				Version: "2012-10-17",
				Statements: []s3types.Statement{
					{
						Sid:       "AllowRead",
						Effect:    "Allow",
						Principal: &s3types.Principal{AWS: []string{"*"}},
						Actions:   []s3types.S3Action{s3types.S3ActionGetObject},
						Resources: []string{"arn:aws:s3:::test-bucket/*"},
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

			// Setup policy - must set in both DB and bucket cache
			if tc.setupPolicy != nil {
				err := srv.db.SetBucketPolicy(ctx, tc.bucket, tc.setupPolicy)
				require.NoError(t, err)
				// Also update the bucket cache's policy
				err = srv.bucketStore.UpdateBucketPolicy(ctx, tc.bucket, tc.setupPolicy)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?policy", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetBucketPolicyHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK && tc.setupPolicy != nil {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/json")

				var result s3types.BucketPolicy
				err := json.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				assert.Equal(t, tc.setupPolicy.Version, result.Version)
				assert.Len(t, result.Statements, len(tc.setupPolicy.Statements))
			}
		})
	}
}

func TestPutBucketPolicyHandler(t *testing.T) {
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
			requestBody:    `{"Version":"2012-10-17","Statement":[]}`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "valid policy",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `{
				"Version": "2012-10-17",
				"Statement": [{
					"Sid": "AllowRead",
					"Effect": "Allow",
					"Principal": {"AWS": ["*"]},
					"Action": ["s3:GetObject"],
					"Resource": ["arn:aws:s3:::test-bucket/*"]
				}]
			}`,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:   "malformed JSON",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody:    `not valid json`,
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
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?policy", strings.NewReader(tc.requestBody))
			req.Header.Set("Content-Type", "application/json")
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.PutBucketPolicyHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify policy was stored
			if tc.expectedStatus == http.StatusNoContent {
				policy, err := srv.db.GetBucketPolicy(ctx, tc.bucket)
				require.NoError(t, err)
				assert.NotNil(t, policy)
			}
		})
	}
}

func TestDeleteBucketPolicyHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupPolicy    bool
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "delete existing policy",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupPolicy:    true,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:   "delete non-existing policy (idempotent)",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupPolicy:    false,
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

			// Setup policy
			if tc.setupPolicy {
				policy := &s3types.BucketPolicy{
					Version:    "2012-10-17",
					Statements: []s3types.Statement{},
				}
				err := srv.db.SetBucketPolicy(ctx, tc.bucket, policy)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("DELETE", "/"+tc.bucket+"?policy", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.DeleteBucketPolicyHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}
