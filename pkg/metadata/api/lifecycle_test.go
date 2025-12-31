//go:build enterprise

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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBucketLifecycleConfigurationHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupLifecycle *s3types.Lifecycle
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "no lifecycle configured",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			expectedStatus: http.StatusNotFound, // NoSuchLifecycleConfiguration
		},
		{
			name:   "lifecycle exists - expiration rule",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupLifecycle: &s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("delete-old-files"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("logs/"),
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(30),
						},
					},
				},
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:   "lifecycle exists - transition rule",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupLifecycle: &s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("archive-rule"),
						Status: s3types.LifecycleStatusEnabled,
						Transitions: []*s3types.LifecycleTransition{
							{
								Days:         aws.Int64(90),
								StorageClass: aws.String("GLACIER"),
							},
						},
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

			// Setup lifecycle in DB
			if tc.setupLifecycle != nil {
				err := srv.db.SetBucketLifecycle(ctx, tc.bucket, tc.setupLifecycle)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?lifecycle", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetBucketLifecycleConfigurationHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.Lifecycle
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				assert.Len(t, result.Rules, len(tc.setupLifecycle.Rules))
			}
		})
	}
}

func TestPutBucketLifecycleConfigurationHandler(t *testing.T) {
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
			requestBody: `<LifecycleConfiguration>
				<Rule><ID>test</ID><Status>Enabled</Status></Rule>
			</LifecycleConfiguration>`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "valid expiration rule",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>delete-old-logs</ID>
    <Status>Enabled</Status>
    <Filter>
      <Prefix>logs/</Prefix>
    </Filter>
    <LifecycleExpiration>
      <Days>30</Days>
    </LifecycleExpiration>
  </Rule>
</LifecycleConfiguration>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "valid transition rule",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>archive-to-glacier</ID>
    <Status>Enabled</Status>
    <Transition>
      <Days>90</Days>
      <StorageClass>GLACIER</StorageClass>
    </Transition>
  </Rule>
</LifecycleConfiguration>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "abort incomplete multipart upload",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration>
  <Rule>
    <ID>abort-incomplete-mpu</ID>
    <Status>Enabled</Status>
    <AbortIncompleteMultipartUpload>
      <DaysAfterInitiation>7</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
</LifecycleConfiguration>`,
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
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?lifecycle", strings.NewReader(tc.requestBody))
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

			srv.PutBucketLifecycleConfigurationHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify lifecycle was stored
			if tc.expectedStatus == http.StatusOK {
				lifecycle, err := srv.db.GetBucketLifecycle(ctx, tc.bucket)
				require.NoError(t, err)
				assert.NotNil(t, lifecycle)
			}
		})
	}
}

func TestDeleteBucketLifecycleHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupLifecycle bool
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "delete existing lifecycle",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupLifecycle: true,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:   "delete non-existing lifecycle (idempotent)",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupLifecycle: false,
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

			// Setup lifecycle
			if tc.setupLifecycle {
				lifecycle := &s3types.Lifecycle{
					Rules: []s3types.LifecycleRule{
						{
							ID:     aws.String("test-rule"),
							Status: s3types.LifecycleStatusEnabled,
						},
					},
				}
				err := srv.db.SetBucketLifecycle(ctx, tc.bucket, lifecycle)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("DELETE", "/"+tc.bucket+"?lifecycle", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.DeleteBucketLifecycleHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}
