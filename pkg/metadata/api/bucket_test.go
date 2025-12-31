package api

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	clientmocks "github.com/LeeDigitalWorks/zapfs/mocks/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// createTestData creates test data for a handler
func createTestData(bucket, key, ownerID string) *data.Data {
	req := httptest.NewRequest("GET", "/", nil)
	return &data.Data{
		Ctx: context.Background(),
		Req: req,
		S3Info: &data.S3Info{
			Bucket:  bucket,
			Key:     key,
			OwnerID: ownerID,
		},
	}
}

func TestListBucketsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		setupBuckets   []*types.BucketInfo // buckets to add to DB before test
		ownerID        string
		queryParams    map[string]string // query parameters
		expectedStatus int
		expectedCount  int
		checkTruncated bool
		hasToken       bool // whether response should have continuation token
	}{
		{
			name:           "empty bucket list",
			setupBuckets:   nil,
			ownerID:        "test-owner",
			queryParams:    nil,
			expectedStatus: http.StatusOK,
			expectedCount:  0,
		},
		{
			name: "returns only owned buckets",
			setupBuckets: []*types.BucketInfo{
				{ID: uuid.New(), Name: "bucket-1", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "bucket-2", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "other-owner-bucket", OwnerID: "other-owner", CreatedAt: time.Now().UnixNano()},
			},
			ownerID:        "test-owner",
			queryParams:    nil,
			expectedStatus: http.StatusOK,
			expectedCount:  2,
		},
		{
			name: "different owner sees their buckets",
			setupBuckets: []*types.BucketInfo{
				{ID: uuid.New(), Name: "bucket-1", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "other-owner-bucket", OwnerID: "other-owner", CreatedAt: time.Now().UnixNano()},
			},
			ownerID:        "other-owner",
			queryParams:    nil,
			expectedStatus: http.StatusOK,
			expectedCount:  1,
		},
		{
			name: "pagination with max-buckets",
			setupBuckets: []*types.BucketInfo{
				{ID: uuid.New(), Name: "bucket-1", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "bucket-2", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "bucket-3", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
			},
			ownerID:        "test-owner",
			queryParams:    map[string]string{"max-buckets": "2"},
			expectedStatus: http.StatusOK,
			expectedCount:  2,
			checkTruncated: true,
			hasToken:       true,
		},
		{
			name: "prefix filtering",
			setupBuckets: []*types.BucketInfo{
				{ID: uuid.New(), Name: "test-bucket-1", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "test-bucket-2", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "other-bucket", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
			},
			ownerID:        "test-owner",
			queryParams:    map[string]string{"prefix": "test-"},
			expectedStatus: http.StatusOK,
			expectedCount:  2,
		},
		{
			name: "region filtering",
			setupBuckets: []*types.BucketInfo{
				{ID: uuid.New(), Name: "bucket-1", OwnerID: "test-owner", Region: "us-east-1", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "bucket-2", OwnerID: "test-owner", Region: "us-west-2", CreatedAt: time.Now().UnixNano()},
			},
			ownerID:        "test-owner",
			queryParams:    map[string]string{"bucket-region": "us-east-1"},
			expectedStatus: http.StatusOK,
			expectedCount:  1,
		},
		{
			name: "continuation token pagination",
			setupBuckets: []*types.BucketInfo{
				{ID: uuid.New(), Name: "bucket-a", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "bucket-b", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
				{ID: uuid.New(), Name: "bucket-c", OwnerID: "test-owner", CreatedAt: time.Now().UnixNano()},
			},
			ownerID:        "test-owner",
			queryParams:    map[string]string{"max-buckets": "1", "continuation-token": "bucket-a"},
			expectedStatus: http.StatusOK,
			expectedCount:  1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)

			// Setup buckets in DB
			ctx := context.Background()
			for _, bucket := range tc.setupBuckets {
				err := srv.db.CreateBucket(ctx, bucket)
				require.NoError(t, err)
			}

			// Create request with query params
			req := httptest.NewRequest("GET", "/", nil)
			if tc.queryParams != nil {
				q := req.URL.Query()
				for k, v := range tc.queryParams {
					q.Set(k, v)
				}
				req.URL.RawQuery = q.Encode()
			}

			d := &data.Data{
				Ctx:    ctx,
				Req:    req,
				S3Info: &data.S3Info{OwnerID: tc.ownerID},
			}
			w := httptest.NewRecorder()

			srv.ListBucketsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
			assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

			var result s3types.ListAllMyBucketsResult
			err := xml.Unmarshal(w.Body.Bytes(), &result)
			require.NoError(t, err)

			assert.Equal(t, tc.ownerID, result.Owner.ID)
			assert.Len(t, result.Buckets.Buckets, tc.expectedCount)

			if tc.checkTruncated {
				// If we expect truncation, check for continuation token
				if tc.hasToken {
					assert.NotEmpty(t, result.ContinuationToken, "should have continuation token when truncated")
				}
			}

			// Verify prefix is echoed back if provided
			if prefix, ok := tc.queryParams["prefix"]; ok {
				assert.Equal(t, prefix, result.Prefix)
			}
		})
	}
}

func TestHeadBucketHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		expectedStatus int
	}{
		{
			name:           "empty bucket name returns bad request",
			bucket:         "",
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:           "nonexistent bucket returns not found",
			bucket:         "nonexistent-bucket",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			d := createTestData(tc.bucket, "", "test-owner")
			w := httptest.NewRecorder()

			srv.HeadBucketHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}

func TestGetBucketLocationHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		expectedStatus int
	}{
		{
			name:           "empty bucket name returns not found",
			bucket:         "",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			d := createTestData(tc.bucket, "", "test-owner")
			w := httptest.NewRecorder()

			srv.GetBucketLocationHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}

func TestCreateBucketHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		ownerID        string
		mockResponse   *manager_pb.CreateCollectionResponse
		mockError      error
		expectedStatus int
		verifyCache    bool // whether to verify bucket was added to cache
	}{
		{
			name:    "success creates bucket",
			bucket:  "new-bucket",
			ownerID: "test-owner",
			mockResponse: &manager_pb.CreateCollectionResponse{
				Success: true,
			},
			expectedStatus: http.StatusOK,
			verifyCache:    true,
		},
		{
			name:    "bucket already exists owned by other user",
			bucket:  "existing-bucket",
			ownerID: "test-owner",
			mockResponse: &manager_pb.CreateCollectionResponse{
				Success: false,
				Message: "bucket already exists",
				Collection: &manager_pb.Collection{
					Name:  "existing-bucket",
					Owner: "other-owner",
				},
			},
			expectedStatus: http.StatusConflict,
			verifyCache:    false,
		},
		{
			name:    "bucket already owned by you",
			bucket:  "my-bucket",
			ownerID: "test-owner",
			mockResponse: &manager_pb.CreateCollectionResponse{
				Success: false,
				Message: "bucket already exists",
				Collection: &manager_pb.Collection{
					Name:  "my-bucket",
					Owner: "test-owner",
				},
			},
			expectedStatus: http.StatusConflict,
			verifyCache:    false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockMgr := clientmocks.NewMockManager(t)
			mockMgr.EXPECT().
				CreateCollection(mock.Anything, mock.MatchedBy(func(req *manager_pb.CreateCollectionRequest) bool {
					return req.Name == tc.bucket && req.Owner == tc.ownerID
				})).
				Return(tc.mockResponse, tc.mockError)

			srv := newTestServer(t, WithManagerClient(mockMgr))

			d := createTestData(tc.bucket, "", tc.ownerID)
			d.Req = httptest.NewRequest("PUT", "/"+tc.bucket, nil)
			w := httptest.NewRecorder()

			srv.CreateBucketHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.verifyCache {
				bucket, exists := srv.bucketStore.GetBucket(tc.bucket)
				assert.True(t, exists)
				assert.NotNil(t, bucket)
			}
		})
	}
}

func TestDeleteBucketHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		ownerID        string
		setupBucket    bool // whether to add bucket to cache first
		mockResponse   *manager_pb.DeleteCollectionResponse
		mockError      error
		expectedStatus int
	}{
		{
			name:        "success deletes bucket",
			bucket:      "delete-me",
			ownerID:     "test-owner",
			setupBucket: true,
			mockResponse: &manager_pb.DeleteCollectionResponse{
				Success: true,
			},
			expectedStatus: http.StatusNoContent,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			mockMgr := clientmocks.NewMockManager(t)
			mockMgr.EXPECT().
				DeleteCollection(mock.Anything, mock.MatchedBy(func(req *manager_pb.DeleteCollectionRequest) bool {
					return req.Name == tc.bucket
				})).
				Return(tc.mockResponse, tc.mockError)

			srv := newTestServer(t, WithManagerClient(mockMgr))

			if tc.setupBucket {
				srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
					Name:    tc.bucket,
					OwnerID: tc.ownerID,
				})
			}

			d := createTestData(tc.bucket, "", tc.ownerID)
			d.Req = httptest.NewRequest("DELETE", "/"+tc.bucket, nil)
			w := httptest.NewRecorder()

			srv.DeleteBucketHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}
