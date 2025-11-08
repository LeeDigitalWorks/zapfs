package bucket

import (
	"context"
	"errors"
	"testing"
	"time"

	cachemocks "zapfs/mocks/cache"
	clientmocks "zapfs/mocks/client"
	dbmocks "zapfs/mocks/db"
	"zapfs/pkg/cache"
	"zapfs/pkg/metadata/db"
	"zapfs/pkg/s3api/s3types"
	"zapfs/pkg/types"
	"zapfs/proto/manager_pb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupConfig func(t *testing.T) Config
		wantErr     bool
		errContains string
	}{
		{
			name: "missing DB returns error",
			setupConfig: func(t *testing.T) Config {
				return Config{
					DB:                nil,
					ManagerClient:     clientmocks.NewMockManager(t),
					GlobalBucketCache: &cache.GlobalBucketCache{},
				}
			},
			wantErr:     true,
			errContains: "DB is required",
		},
		{
			name: "missing ManagerClient returns error",
			setupConfig: func(t *testing.T) Config {
				return Config{
					DB:                dbmocks.NewMockDB(t),
					ManagerClient:     nil,
					GlobalBucketCache: &cache.GlobalBucketCache{},
				}
			},
			wantErr:     true,
			errContains: "ManagerClient is required",
		},
		{
			name: "missing GlobalBucketCache returns error",
			setupConfig: func(t *testing.T) Config {
				return Config{
					DB:                dbmocks.NewMockDB(t),
					ManagerClient:     clientmocks.NewMockManager(t),
					GlobalBucketCache: nil,
				}
			},
			wantErr:     true,
			errContains: "GlobalBucketCache is required",
		},
		{
			name: "valid config succeeds",
			setupConfig: func(t *testing.T) Config {
				mockMgr := clientmocks.NewMockManager(t)
				mockMgr.EXPECT().Close().Return(nil).Maybe()
				return Config{
					DB:                dbmocks.NewMockDB(t),
					ManagerClient:     mockMgr,
					GlobalBucketCache: &cache.GlobalBucketCache{},
				}
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.setupConfig(t)
			svc, err := NewService(cfg)

			if tc.wantErr {
				assert.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, svc)
		})
	}
}

func TestCreateBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *CreateBucketRequest
		setupMocks  func(*dbmocks.MockDB, *clientmocks.MockManager, *cache.GlobalBucketCache)
		wantErr     bool
		wantErrCode ErrorCode
	}{
		{
			name: "successful creation",
			req: &CreateBucketRequest{
				Bucket:  "new-bucket",
				OwnerID: "user-123",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockMgr *clientmocks.MockManager, gbc *cache.GlobalBucketCache) {
				mockMgr.EXPECT().CreateCollection(mock.Anything, &manager_pb.CreateCollectionRequest{
					Name:  "new-bucket",
					Owner: "user-123",
				}).Return(&manager_pb.CreateCollectionResponse{
					Success: true,
				}, nil)
				mockDB.EXPECT().CreateBucket(mock.Anything, mock.MatchedBy(func(bi *types.BucketInfo) bool {
					return bi.Name == "new-bucket" && bi.OwnerID == "user-123"
				})).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "bucket already exists different owner",
			req: &CreateBucketRequest{
				Bucket:  "existing-bucket",
				OwnerID: "user-123",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockMgr *clientmocks.MockManager, gbc *cache.GlobalBucketCache) {
				gbc.Set("existing-bucket", "other-user")
			},
			wantErr:     true,
			wantErrCode: ErrCodeBucketAlreadyExists,
		},
		{
			name: "bucket already owned by same user",
			req: &CreateBucketRequest{
				Bucket:  "my-bucket",
				OwnerID: "user-123",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockMgr *clientmocks.MockManager, gbc *cache.GlobalBucketCache) {
				gbc.Set("my-bucket", "user-123")
			},
			wantErr:     true,
			wantErrCode: ErrCodeBucketAlreadyOwnedByYou,
		},
		{
			name: "manager error",
			req: &CreateBucketRequest{
				Bucket:  "new-bucket",
				OwnerID: "user-123",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockMgr *clientmocks.MockManager, gbc *cache.GlobalBucketCache) {
				mockMgr.EXPECT().CreateCollection(mock.Anything, mock.Anything).
					Return(nil, errors.New("connection failed"))
			},
			wantErr:     true,
			wantErrCode: ErrCodeInternalError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockMgr := clientmocks.NewMockManager(t)
			mockMgr.EXPECT().Close().Return(nil).Maybe()

			// Create global bucket cache with mock
			ctx := context.Background()
			mockBCC := cachemocks.NewMockBucketCacheClient(t)
			gbc := cache.NewGlobalBucketCache(ctx, mockBCC)

			tc.setupMocks(mockDB, mockMgr, gbc)

			svc, err := NewService(Config{
				DB:                mockDB,
				ManagerClient:     mockMgr,
				GlobalBucketCache: gbc,
			})
			require.NoError(t, err)

			result, err := svc.CreateBucket(ctx, tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				var bucketErr *Error
				assert.True(t, errors.As(err, &bucketErr))
				assert.Equal(t, tc.wantErrCode, bucketErr.Code)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)
		})
	}
}

func TestDeleteBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		setupMocks  func(*dbmocks.MockDB, *clientmocks.MockManager, *cache.GlobalBucketCache)
		wantErr     bool
		wantErrCode ErrorCode
	}{
		{
			name:   "successful deletion",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockMgr *clientmocks.MockManager, gbc *cache.GlobalBucketCache) {
				mockDB.EXPECT().ListObjects(mock.Anything, "test-bucket", "", 1).Return([]*types.ObjectRef{}, nil)
				mockMgr.EXPECT().DeleteCollection(mock.Anything, &manager_pb.DeleteCollectionRequest{
					Name: "test-bucket",
				}).Return(&manager_pb.DeleteCollectionResponse{Success: true}, nil)
				mockDB.EXPECT().DeleteBucket(mock.Anything, "test-bucket").Return(nil)
				gbc.Set("test-bucket", "owner")
			},
			wantErr: false,
		},
		{
			name:   "bucket not empty",
			bucket: "non-empty-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockMgr *clientmocks.MockManager, gbc *cache.GlobalBucketCache) {
				mockDB.EXPECT().ListObjects(mock.Anything, "non-empty-bucket", "", 1).
					Return([]*types.ObjectRef{{Key: "some-object"}}, nil)
			},
			wantErr:     true,
			wantErrCode: ErrCodeBucketNotEmpty,
		},
		{
			name:   "bucket doesn't exist in manager",
			bucket: "nonexistent-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockMgr *clientmocks.MockManager, gbc *cache.GlobalBucketCache) {
				mockDB.EXPECT().ListObjects(mock.Anything, "nonexistent-bucket", "", 1).Return([]*types.ObjectRef{}, nil)
				mockMgr.EXPECT().DeleteCollection(mock.Anything, mock.Anything).
					Return(&manager_pb.DeleteCollectionResponse{Success: false}, nil)
			},
			wantErr:     true,
			wantErrCode: ErrCodeNoSuchBucket,
		},
		{
			name:   "empty bucket name",
			bucket: "",
			setupMocks: func(mockDB *dbmocks.MockDB, mockMgr *clientmocks.MockManager, gbc *cache.GlobalBucketCache) {
				// No mocks needed - fails early
			},
			wantErr:     true,
			wantErrCode: ErrCodeNoSuchBucket,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockMgr := clientmocks.NewMockManager(t)
			mockMgr.EXPECT().Close().Return(nil).Maybe()

			ctx := context.Background()
			mockBCC := cachemocks.NewMockBucketCacheClient(t)
			gbc := cache.NewGlobalBucketCache(ctx, mockBCC)

			tc.setupMocks(mockDB, mockMgr, gbc)

			svc, err := NewService(Config{
				DB:                mockDB,
				ManagerClient:     mockMgr,
				GlobalBucketCache: gbc,
			})
			require.NoError(t, err)

			err = svc.DeleteBucket(ctx, tc.bucket)

			if tc.wantErr {
				assert.Error(t, err)
				var bucketErr *Error
				assert.True(t, errors.As(err, &bucketErr))
				assert.Equal(t, tc.wantErrCode, bucketErr.Code)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestHeadBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		setupMocks  func(*cache.GlobalBucketCache, *cache.BucketStore)
		wantErr     bool
		wantErrCode ErrorCode
		checkResult func(*testing.T, *HeadBucketResult)
	}{
		{
			name:   "bucket exists",
			bucket: "my-bucket",
			setupMocks: func(gbc *cache.GlobalBucketCache, bs *cache.BucketStore) {
				gbc.Set("my-bucket", "owner-123")
				bs.SetBucket("my-bucket", s3types.Bucket{
					Name:     "my-bucket",
					OwnerID:  "owner-123",
					Location: "us-east-1",
				})
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *HeadBucketResult) {
				assert.True(t, result.Exists)
				assert.Equal(t, "owner-123", result.OwnerID)
				assert.Equal(t, "us-east-1", result.Location)
			},
		},
		{
			name:   "bucket not found",
			bucket: "nonexistent",
			setupMocks: func(gbc *cache.GlobalBucketCache, bs *cache.BucketStore) {
				// Bucket not in cache
			},
			wantErr:     true,
			wantErrCode: ErrCodeNoSuchBucket,
		},
		{
			name:   "empty bucket name",
			bucket: "",
			setupMocks: func(gbc *cache.GlobalBucketCache, bs *cache.BucketStore) {
				// No setup needed
			},
			wantErr:     true,
			wantErrCode: ErrCodeNoSuchBucket,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockMgr := clientmocks.NewMockManager(t)
			mockMgr.EXPECT().Close().Return(nil).Maybe()

			ctx := context.Background()
			mockBCC := cachemocks.NewMockBucketCacheClient(t)
			gbc := cache.NewGlobalBucketCache(ctx, mockBCC)

			bucketsCache := cache.New(ctx, cache.WithMaxSize[string, s3types.Bucket](100))
			bs := cache.NewBucketStore(bucketsCache)

			tc.setupMocks(gbc, bs)

			svc, err := NewService(Config{
				DB:                mockDB,
				ManagerClient:     mockMgr,
				GlobalBucketCache: gbc,
				BucketStore:       bs,
			})
			require.NoError(t, err)

			result, err := svc.HeadBucket(ctx, tc.bucket)

			if tc.wantErr {
				assert.Error(t, err)
				var bucketErr *Error
				assert.True(t, errors.As(err, &bucketErr))
				assert.Equal(t, tc.wantErrCode, bucketErr.Code)
				return
			}

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

func TestGetBucketLocation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		setupMocks  func(*cache.GlobalBucketCache, *cache.BucketStore)
		wantErr     bool
		wantErrCode ErrorCode
		wantLoc     string
	}{
		{
			name:   "bucket with location",
			bucket: "my-bucket",
			setupMocks: func(gbc *cache.GlobalBucketCache, bs *cache.BucketStore) {
				gbc.Set("my-bucket", "owner")
				bs.SetBucket("my-bucket", s3types.Bucket{
					Name:     "my-bucket",
					Location: "eu-west-1",
				})
			},
			wantErr: false,
			wantLoc: "eu-west-1",
		},
		{
			name:   "bucket with empty location (us-east-1)",
			bucket: "my-bucket",
			setupMocks: func(gbc *cache.GlobalBucketCache, bs *cache.BucketStore) {
				gbc.Set("my-bucket", "owner")
				bs.SetBucket("my-bucket", s3types.Bucket{
					Name:     "my-bucket",
					Location: "",
				})
			},
			wantErr: false,
			wantLoc: "",
		},
		{
			name:   "bucket not found",
			bucket: "nonexistent",
			setupMocks: func(gbc *cache.GlobalBucketCache, bs *cache.BucketStore) {
				// Not in cache
			},
			wantErr:     true,
			wantErrCode: ErrCodeNoSuchBucket,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockMgr := clientmocks.NewMockManager(t)
			mockMgr.EXPECT().Close().Return(nil).Maybe()

			ctx := context.Background()
			mockBCC := cachemocks.NewMockBucketCacheClient(t)
			gbc := cache.NewGlobalBucketCache(ctx, mockBCC)

			bucketsCache := cache.New(ctx, cache.WithMaxSize[string, s3types.Bucket](100))
			bs := cache.NewBucketStore(bucketsCache)

			tc.setupMocks(gbc, bs)

			svc, err := NewService(Config{
				DB:                mockDB,
				ManagerClient:     mockMgr,
				GlobalBucketCache: gbc,
				BucketStore:       bs,
			})
			require.NoError(t, err)

			result, err := svc.GetBucketLocation(ctx, tc.bucket)

			if tc.wantErr {
				assert.Error(t, err)
				var bucketErr *Error
				assert.True(t, errors.As(err, &bucketErr))
				assert.Equal(t, tc.wantErrCode, bucketErr.Code)
				return
			}

			assert.NoError(t, err)
			assert.Equal(t, tc.wantLoc, result.Location)
		})
	}
}

func TestListBuckets(t *testing.T) {
	t.Parallel()

	now := time.Now()

	tests := []struct {
		name        string
		req         *ListBucketsRequest
		setupMock   func(*dbmocks.MockDB)
		wantErr     bool
		checkResult func(*testing.T, *ListBucketsResult)
	}{
		{
			name: "successful list",
			req: &ListBucketsRequest{
				OwnerID:    "user-123",
				MaxBuckets: 100,
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListBuckets(mock.Anything, &db.ListBucketsParams{
					OwnerID:    "user-123",
					MaxBuckets: 100,
				}).Return(&db.ListBucketsResult{
					Buckets: []*types.BucketInfo{
						{Name: "bucket-1", CreatedAt: now.UnixNano(), Region: "us-east-1"},
						{Name: "bucket-2", CreatedAt: now.UnixNano(), Region: "eu-west-1"},
					},
					IsTruncated: false,
				}, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *ListBucketsResult) {
				assert.Len(t, result.Buckets, 2)
				assert.Equal(t, "bucket-1", result.Buckets[0].Name)
				assert.Equal(t, "bucket-2", result.Buckets[1].Name)
				assert.False(t, result.IsTruncated)
			},
		},
		{
			name: "with prefix filter",
			req: &ListBucketsRequest{
				OwnerID: "user-123",
				Prefix:  "prod-",
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListBuckets(mock.Anything, mock.MatchedBy(func(p *db.ListBucketsParams) bool {
					return p.Prefix == "prod-"
				})).Return(&db.ListBucketsResult{
					Buckets: []*types.BucketInfo{
						{Name: "prod-bucket", CreatedAt: now.UnixNano()},
					},
				}, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *ListBucketsResult) {
				assert.Len(t, result.Buckets, 1)
				assert.Equal(t, "prod-", result.Prefix)
			},
		},
		{
			name: "max buckets defaults to 10000",
			req: &ListBucketsRequest{
				OwnerID:    "user-123",
				MaxBuckets: 0,
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListBuckets(mock.Anything, mock.MatchedBy(func(p *db.ListBucketsParams) bool {
					return p.MaxBuckets == 10000
				})).Return(&db.ListBucketsResult{}, nil)
			},
			wantErr: false,
		},
		{
			name: "max buckets capped at 10000",
			req: &ListBucketsRequest{
				OwnerID:    "user-123",
				MaxBuckets: 50000,
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListBuckets(mock.Anything, mock.MatchedBy(func(p *db.ListBucketsParams) bool {
					return p.MaxBuckets == 10000
				})).Return(&db.ListBucketsResult{}, nil)
			},
			wantErr: false,
		},
		{
			name: "paginated result",
			req: &ListBucketsRequest{
				OwnerID:    "user-123",
				MaxBuckets: 10,
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListBuckets(mock.Anything, mock.Anything).Return(&db.ListBucketsResult{
					Buckets: []*types.BucketInfo{
						{Name: "bucket-1", CreatedAt: now.UnixNano()},
					},
					IsTruncated:           true,
					NextContinuationToken: "token-123",
				}, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *ListBucketsResult) {
				assert.True(t, result.IsTruncated)
				assert.Equal(t, "token-123", result.NextContinuationToken)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockMgr := clientmocks.NewMockManager(t)
			mockMgr.EXPECT().Close().Return(nil).Maybe()

			tc.setupMock(mockDB)

			ctx := context.Background()
			mockBCC := cachemocks.NewMockBucketCacheClient(t)
			gbc := cache.NewGlobalBucketCache(ctx, mockBCC)

			svc, err := NewService(Config{
				DB:                mockDB,
				ManagerClient:     mockMgr,
				GlobalBucketCache: gbc,
			})
			require.NoError(t, err)

			result, err := svc.ListBuckets(ctx, tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

func TestErrorToS3Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errCode  ErrorCode
		expected string
	}{
		{"no such bucket", ErrCodeNoSuchBucket, "NoSuchBucket"},
		{"bucket already exists", ErrCodeBucketAlreadyExists, "BucketAlreadyExists"},
		{"bucket already owned by you", ErrCodeBucketAlreadyOwnedByYou, "BucketAlreadyOwnedByYou"},
		{"bucket not empty", ErrCodeBucketNotEmpty, "BucketNotEmpty"},
		{"internal error", ErrCodeInternalError, "InternalError"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := &Error{Code: tc.errCode, Message: "test"}
			s3Err := err.ToS3Error()
			assert.Equal(t, tc.expected, s3Err.Code())
		})
	}
}
