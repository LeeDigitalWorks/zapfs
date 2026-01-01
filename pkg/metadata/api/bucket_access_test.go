package api

import (
	"context"
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

func TestGetPublicAccessBlockHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupConfig    *s3types.PublicAccessBlockConfig
		expectedStatus int
	}{
		{
			name:   "config not found",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			// Per AWS S3 spec, returns NoSuchPublicAccessBlockConfiguration (404)
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "config exists",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupConfig: &s3types.PublicAccessBlockConfig{
				BlockPublicAcls:       true,
				IgnorePublicAcls:      true,
				BlockPublicPolicy:     true,
				RestrictPublicBuckets: true,
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

			// Setup config
			if tc.setupConfig != nil {
				err := srv.db.SetPublicAccessBlock(ctx, tc.bucket, tc.setupConfig)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?publicAccessBlock", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetPublicAccessBlockHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK && tc.setupConfig != nil {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")
				assert.Contains(t, w.Body.String(), "BlockPublicAcls")
			}
		})
	}
}

func TestPutPublicAccessBlockHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		requestBody    string
		expectedStatus int
	}{
		{
			name:   "valid config",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<PublicAccessBlockConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<BlockPublicAcls>true</BlockPublicAcls>
	<IgnorePublicAcls>true</IgnorePublicAcls>
	<BlockPublicPolicy>true</BlockPublicPolicy>
	<RestrictPublicBuckets>true</RestrictPublicBuckets>
</PublicAccessBlockConfiguration>`,
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
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?publicAccessBlock", strings.NewReader(tc.requestBody))
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

			srv.PutPublicAccessBlockHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify config was stored
			if tc.expectedStatus == http.StatusOK {
				config, err := srv.db.GetPublicAccessBlock(ctx, tc.bucket)
				require.NoError(t, err)
				assert.True(t, config.BlockPublicAcls)
			}
		})
	}
}

func TestDeletePublicAccessBlockHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupConfig    bool
		expectedStatus int
	}{
		{
			name:   "delete existing config",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupConfig:    true,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:   "delete non-existing config (idempotent)",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupConfig:    false,
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

			// Setup config
			if tc.setupConfig {
				err := srv.db.SetPublicAccessBlock(ctx, tc.bucket, &s3types.PublicAccessBlockConfig{
					BlockPublicAcls: true,
				})
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("DELETE", "/"+tc.bucket+"?publicAccessBlock", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.DeletePublicAccessBlockHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}

func TestGetBucketOwnershipControlsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupControls  *s3types.OwnershipControls
		expectedStatus int
	}{
		{
			name:   "controls not found",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			// Per AWS S3 spec, returns OwnershipControlsNotFoundError (404)
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "controls exist",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupControls: &s3types.OwnershipControls{
				Rules: []s3types.OwnershipControlsRule{
					{ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced},
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

			// Setup controls
			if tc.setupControls != nil {
				err := srv.db.SetOwnershipControls(ctx, tc.bucket, tc.setupControls)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?ownershipControls", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetBucketOwnershipControlsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK && tc.setupControls != nil {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")
				assert.Contains(t, w.Body.String(), "ObjectOwnership")
			}
		})
	}
}

func TestPutBucketOwnershipControlsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		requestBody    string
		expectedStatus int
	}{
		{
			name:   "valid BucketOwnerEnforced",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Rule>
		<ObjectOwnership>BucketOwnerEnforced</ObjectOwnership>
	</Rule>
</OwnershipControls>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "valid BucketOwnerPreferred",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Rule>
		<ObjectOwnership>BucketOwnerPreferred</ObjectOwnership>
	</Rule>
</OwnershipControls>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "valid ObjectWriter",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Rule>
		<ObjectOwnership>ObjectWriter</ObjectOwnership>
	</Rule>
</OwnershipControls>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "invalid ObjectOwnership value",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
	<Rule>
		<ObjectOwnership>InvalidValue</ObjectOwnership>
	</Rule>
</OwnershipControls>`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:   "empty rules",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<OwnershipControls xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
</OwnershipControls>`,
			expectedStatus: http.StatusBadRequest,
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
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?ownershipControls", strings.NewReader(tc.requestBody))
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

			srv.PutBucketOwnershipControlsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify controls were stored for successful cases
			if tc.expectedStatus == http.StatusOK {
				controls, err := srv.db.GetOwnershipControls(ctx, tc.bucket)
				require.NoError(t, err)
				assert.Len(t, controls.Rules, 1)
			}
		})
	}
}

func TestDeleteBucketOwnershipControlsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupControls  bool
		expectedStatus int
	}{
		{
			name:   "delete existing controls",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupControls:  true,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:   "delete non-existing controls (idempotent)",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupControls:  false,
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

			// Setup controls
			if tc.setupControls {
				err := srv.db.SetOwnershipControls(ctx, tc.bucket, &s3types.OwnershipControls{
					Rules: []s3types.OwnershipControlsRule{
						{ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced},
					},
				})
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("DELETE", "/"+tc.bucket+"?ownershipControls", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.DeleteBucketOwnershipControlsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}
