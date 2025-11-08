package api

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"zapfs/pkg/metadata/data"
	"zapfs/pkg/s3api/s3types"
	"zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetObjectLockConfigurationHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		setupBucket    *types.BucketInfo
		setupLock      *s3types.ObjectLockConfiguration
		expectedStatus int
	}{
		{
			name:           "bucket not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "no lock configured",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			expectedStatus: http.StatusNotFound, // ObjectLockConfigurationNotFoundError
		},
		{
			name:   "lock configured - enabled only",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupLock: &s3types.ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
			},
			expectedStatus: http.StatusOK,
		},
		{
			name:   "lock configured - with default retention",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupLock: &s3types.ObjectLockConfiguration{
				ObjectLockEnabled: "Enabled",
				Rule: &s3types.ObjectLockRule{
					DefaultRetention: &s3types.DefaultRetention{
						Mode: "GOVERNANCE",
						Days: 30,
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

			// Setup lock config in DB
			if tc.setupLock != nil {
				err := srv.db.SetObjectLockConfiguration(ctx, tc.bucket, tc.setupLock)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?object-lock", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetObjectLockConfigurationHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.ObjectLockConfiguration
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				assert.Equal(t, tc.setupLock.ObjectLockEnabled, result.ObjectLockEnabled)
			}
		})
	}
}

func TestPutObjectLockConfigurationHandler(t *testing.T) {
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
			requestBody: `<ObjectLockConfiguration>
				<ObjectLockEnabled>Enabled</ObjectLockEnabled>
			</ObjectLockConfiguration>`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "valid - enable only",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
</ObjectLockConfiguration>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "valid - with governance retention",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
  <Rule>
    <DefaultRetention>
      <Mode>GOVERNANCE</Mode>
      <Days>30</Days>
    </DefaultRetention>
  </Rule>
</ObjectLockConfiguration>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "valid - with compliance retention years",
			bucket: "test-bucket",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<ObjectLockConfiguration>
  <ObjectLockEnabled>Enabled</ObjectLockEnabled>
  <Rule>
    <DefaultRetention>
      <Mode>COMPLIANCE</Mode>
      <Years>1</Years>
    </DefaultRetention>
  </Rule>
</ObjectLockConfiguration>`,
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
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"?object-lock", strings.NewReader(tc.requestBody))
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

			srv.PutObjectLockConfigurationHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify config was stored
			if tc.expectedStatus == http.StatusOK {
				cfg, err := srv.db.GetObjectLockConfiguration(ctx, tc.bucket)
				require.NoError(t, err)
				assert.NotNil(t, cfg)
			}
		})
	}
}

func TestGetObjectRetentionHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		bucket          string
		key             string
		setupBucket     *types.BucketInfo
		setupObject     *types.ObjectRef
		setupRetention  *s3types.ObjectLockRetention
		expectedStatus  int
		expectedMode    string
	}{
		{
			name:   "object not found",
			bucket: "test-bucket",
			key:    "nonexistent-key",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "no retention set",
			bucket: "test-bucket",
			key:    "test-key.txt",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupObject: &types.ObjectRef{
				ID:     uuid.New(),
				Bucket: "test-bucket",
				Key:    "test-key.txt",
				Size:   100,
				ETag:   "abc123",
			},
			expectedStatus: http.StatusNotFound, // NoSuchObjectLockConfiguration
		},
		{
			name:   "retention exists",
			bucket: "test-bucket",
			key:    "test-key.txt",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupObject: &types.ObjectRef{
				ID:     uuid.New(),
				Bucket: "test-bucket",
				Key:    "test-key.txt",
				Size:   100,
				ETag:   "abc123",
			},
			setupRetention: &s3types.ObjectLockRetention{
				Mode:            "GOVERNANCE",
				RetainUntilDate: time.Now().Add(24 * time.Hour).Format(time.RFC3339),
			},
			expectedStatus: http.StatusOK,
			expectedMode:   "GOVERNANCE",
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

			// Setup object
			if tc.setupObject != nil {
				err := srv.db.PutObject(ctx, tc.setupObject)
				require.NoError(t, err)
			}

			// Setup retention
			if tc.setupRetention != nil {
				err := srv.db.SetObjectRetention(ctx, tc.bucket, tc.key, tc.setupRetention)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"/"+tc.key+"?retention", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					Key:     tc.key,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetObjectRetentionHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.ObjectLockRetention
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				assert.Equal(t, tc.expectedMode, result.Mode)
			}
		})
	}
}

func TestGetObjectLegalHoldHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		key            string
		setupBucket    *types.BucketInfo
		setupObject    *types.ObjectRef
		setupLegalHold *s3types.ObjectLockLegalHold
		expectedStatus int
		expectedHold   string
	}{
		{
			name:   "object not found",
			bucket: "test-bucket",
			key:    "nonexistent-key",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "no legal hold set - returns OFF",
			bucket: "test-bucket",
			key:    "test-key.txt",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupObject: &types.ObjectRef{
				ID:     uuid.New(),
				Bucket: "test-bucket",
				Key:    "test-key.txt",
				Size:   100,
				ETag:   "abc123",
			},
			expectedStatus: http.StatusOK,
			expectedHold:   "OFF",
		},
		{
			name:   "legal hold ON",
			bucket: "test-bucket",
			key:    "test-key.txt",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupObject: &types.ObjectRef{
				ID:     uuid.New(),
				Bucket: "test-bucket",
				Key:    "test-key.txt",
				Size:   100,
				ETag:   "abc123",
			},
			setupLegalHold: &s3types.ObjectLockLegalHold{Status: "ON"},
			expectedStatus: http.StatusOK,
			expectedHold:   "ON",
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

			// Setup object
			if tc.setupObject != nil {
				err := srv.db.PutObject(ctx, tc.setupObject)
				require.NoError(t, err)
			}

			// Setup legal hold
			if tc.setupLegalHold != nil {
				err := srv.db.SetObjectLegalHold(ctx, tc.bucket, tc.key, tc.setupLegalHold)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"/"+tc.key+"?legal-hold", nil)
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					Key:     tc.key,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.GetObjectLegalHoldHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.ObjectLockLegalHold
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				assert.Equal(t, tc.expectedHold, result.Status)
			}
		})
	}
}

func TestPutObjectLegalHoldHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		key            string
		setupBucket    *types.BucketInfo
		setupObject    *types.ObjectRef
		requestBody    string
		expectedStatus int
	}{
		{
			name:   "object not found",
			bucket: "test-bucket",
			key:    "nonexistent-key",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			requestBody:    `<LegalHold><Status>ON</Status></LegalHold>`,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:   "set legal hold ON",
			bucket: "test-bucket",
			key:    "test-key.txt",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupObject: &types.ObjectRef{
				ID:     uuid.New(),
				Bucket: "test-bucket",
				Key:    "test-key.txt",
				Size:   100,
				ETag:   "abc123",
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<LegalHold>
  <Status>ON</Status>
</LegalHold>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "set legal hold OFF",
			bucket: "test-bucket",
			key:    "test-key.txt",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupObject: &types.ObjectRef{
				ID:     uuid.New(),
				Bucket: "test-bucket",
				Key:    "test-key.txt",
				Size:   100,
				ETag:   "abc123",
			},
			requestBody: `<?xml version="1.0" encoding="UTF-8"?>
<LegalHold>
  <Status>OFF</Status>
</LegalHold>`,
			expectedStatus: http.StatusOK,
		},
		{
			name:   "invalid status",
			bucket: "test-bucket",
			key:    "test-key.txt",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupObject: &types.ObjectRef{
				ID:     uuid.New(),
				Bucket: "test-bucket",
				Key:    "test-key.txt",
				Size:   100,
				ETag:   "abc123",
			},
			requestBody:    `<LegalHold><Status>INVALID</Status></LegalHold>`,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:   "malformed XML",
			bucket: "test-bucket",
			key:    "test-key.txt",
			setupBucket: &types.BucketInfo{
				ID:        uuid.New(),
				Name:      "test-bucket",
				OwnerID:   "test-owner",
				CreatedAt: time.Now().UnixNano(),
			},
			setupObject: &types.ObjectRef{
				ID:     uuid.New(),
				Bucket: "test-bucket",
				Key:    "test-key.txt",
				Size:   100,
				ETag:   "abc123",
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

			// Setup object
			if tc.setupObject != nil {
				err := srv.db.PutObject(ctx, tc.setupObject)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"/"+tc.key+"?legal-hold", strings.NewReader(tc.requestBody))
			req.Header.Set("Content-Type", "application/xml")
			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					Key:     tc.key,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.PutObjectLegalHoldHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify legal hold was stored
			if tc.expectedStatus == http.StatusOK {
				hold, err := srv.db.GetObjectLegalHold(ctx, tc.bucket, tc.key)
				require.NoError(t, err)
				assert.NotNil(t, hold)
			}
		})
	}
}
