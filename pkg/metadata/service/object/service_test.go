package object

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	cachemocks "zapfs/mocks/cache"
	dbmocks "zapfs/mocks/db"
	"zapfs/pkg/cache"
	"zapfs/pkg/metadata/db"
	"zapfs/pkg/metadata/service/storage"
	"zapfs/pkg/s3api/s3types"
	"zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockStorage implements storage.Coordinator for testing
type mockStorage struct {
	writeResult *storage.WriteResult
	writeErr    error
	readData    []byte
	readErr     error
}

func (m *mockStorage) WriteObject(ctx context.Context, req *storage.WriteRequest) (*storage.WriteResult, error) {
	if m.writeErr != nil {
		return nil, m.writeErr
	}
	if m.writeResult != nil {
		return m.writeResult, nil
	}
	// Default successful write
	return &storage.WriteResult{
		Size: req.Size,
		ChunkRefs: []types.ChunkRef{
			{
				ChunkID:        types.ChunkID(req.ObjectID),
				Size:           req.Size,
				FileServerAddr: "localhost:9000",
				BackendID:      "backend-1",
			},
		},
	}, nil
}

func (m *mockStorage) ReadObject(ctx context.Context, req *storage.ReadRequest, w io.Writer) error {
	if m.readErr != nil {
		return m.readErr
	}
	if m.readData != nil {
		_, err := w.Write(m.readData)
		return err
	}
	return nil
}

func (m *mockStorage) ReadObjectRange(ctx context.Context, req *storage.ReadRangeRequest, w io.Writer) error {
	if m.readErr != nil {
		return m.readErr
	}
	if m.readData != nil {
		start := req.Offset
		end := min(req.Offset+req.Length, uint64(len(m.readData)))
		_, err := w.Write(m.readData[start:end])
		return err
	}
	return nil
}

func (m *mockStorage) ReadObjectToBuffer(ctx context.Context, chunkRefs []types.ChunkRef) ([]byte, error) {
	if m.readErr != nil {
		return nil, m.readErr
	}
	return m.readData, nil
}

func (m *mockStorage) ReportOrphanChunks(ctx context.Context, bucket, objectID string, targets []types.ChunkRef) {
	// No-op for testing
}

// newServiceWithMockDB creates a service for testing with a mock DB
func newServiceWithMockDB(t *testing.T, mockDB *dbmocks.MockDB) Service {
	t.Helper()
	profiles := types.NewProfileSet()
	profiles.Add(&types.StorageProfile{Name: "STANDARD", Replication: 2})

	svc, err := NewService(Config{
		DB:             mockDB,
		Storage:        &storage.Coordinator{},
		Profiles:       profiles,
		DefaultProfile: "STANDARD",
	})
	require.NoError(t, err)
	return svc
}

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
				profiles := types.NewProfileSet()
				profiles.Add(&types.StorageProfile{Name: "STANDARD"})
				return Config{
					DB:       nil,
					Storage:  &storage.Coordinator{},
					Profiles: profiles,
				}
			},
			wantErr:     true,
			errContains: "DB is required",
		},
		{
			name: "missing Profiles returns error",
			setupConfig: func(t *testing.T) Config {
				return Config{
					DB:       dbmocks.NewMockDB(t),
					Storage:  &storage.Coordinator{},
					Profiles: nil,
				}
			},
			wantErr:     true,
			errContains: "Profiles is required",
		},
		{
			name: "valid config succeeds",
			setupConfig: func(t *testing.T) Config {
				profiles := types.NewProfileSet()
				profiles.Add(&types.StorageProfile{Name: "STANDARD"})
				return Config{
					DB:             dbmocks.NewMockDB(t),
					Storage:        &storage.Coordinator{},
					Profiles:       profiles,
					DefaultProfile: "STANDARD",
				}
			},
			wantErr: false,
		},
		{
			name: "empty default profile uses STANDARD",
			setupConfig: func(t *testing.T) Config {
				profiles := types.NewProfileSet()
				profiles.Add(&types.StorageProfile{Name: "STANDARD"})
				return Config{
					DB:             dbmocks.NewMockDB(t),
					Storage:        &storage.Coordinator{},
					Profiles:       profiles,
					DefaultProfile: "",
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

			// Special check for default profile
			if tc.name == "empty default profile uses STANDARD" {
				impl := svc.(*serviceImpl)
				assert.Equal(t, "STANDARD", impl.defaultProfile)
			}
		})
	}
}

func TestHeadObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		key         string
		setupMock   func(*dbmocks.MockDB)
		wantErr     bool
		wantErrCode ErrorCode
		checkResult func(*testing.T, *HeadObjectResult)
	}{
		{
			name:   "object found",
			bucket: "test-bucket",
			key:    "test-key",
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "test-key").
					Return(&types.ObjectRef{
						ID:        uuid.New(),
						Bucket:    "test-bucket",
						Key:       "test-key",
						Size:      1024,
						ETag:      "abc123",
						ProfileID: "STANDARD",
						CreatedAt: time.Now().UnixNano(),
					}, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *HeadObjectResult) {
				assert.Equal(t, "abc123", result.Metadata.ETag)
				assert.Equal(t, uint64(1024), result.Metadata.Size)
			},
		},
		{
			name:   "object not found",
			bucket: "test-bucket",
			key:    "nonexistent",
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "nonexistent").
					Return(nil, db.ErrObjectNotFound)
			},
			wantErr:     true,
			wantErrCode: ErrCodeNotFound,
		},
		{
			name:   "deleted object returns not found",
			bucket: "test-bucket",
			key:    "deleted-key",
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "deleted-key").
					Return(&types.ObjectRef{
						ID:        uuid.New(),
						Bucket:    "test-bucket",
						Key:       "deleted-key",
						Size:      1024,
						ETag:      "abc123",
						DeletedAt: time.Now().Unix(), // Marked as deleted
					}, nil)
			},
			wantErr:     true,
			wantErrCode: ErrCodeNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			tc.setupMock(mockDB)

			svc := newServiceWithMockDB(t, mockDB)
			result, err := svc.HeadObject(context.Background(), tc.bucket, tc.key)

			if tc.wantErr {
				assert.Error(t, err)
				var objErr *Error
				assert.True(t, errors.As(err, &objErr))
				assert.Equal(t, tc.wantErrCode, objErr.Code)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

func TestDeleteObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		bucket    string
		key       string
		setupMock func(*dbmocks.MockDB)
		wantErr   bool
	}{
		{
			name:   "successful delete",
			bucket: "test-bucket",
			key:    "test-key",
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "test-key").
					Return(&types.ObjectRef{
						ID:     uuid.New(),
						Bucket: "test-bucket",
						Key:    "test-key",
						Size:   1024,
					}, nil)
				mockDB.EXPECT().MarkObjectDeleted(mock.Anything, "test-bucket", "test-key", mock.AnythingOfType("int64")).
					Return(nil)
			},
			wantErr: false,
		},
		{
			name:   "delete nonexistent object succeeds (S3 behavior)",
			bucket: "test-bucket",
			key:    "nonexistent",
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "nonexistent").
					Return(nil, db.ErrObjectNotFound)
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			tc.setupMock(mockDB)

			svc := newServiceWithMockDB(t, mockDB)
			result, err := svc.DeleteObject(context.Background(), tc.bucket, tc.key)

			if tc.wantErr {
				assert.Error(t, err)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, result)
		})
	}
}

func TestDeleteObjectWithCRRHook(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	mockDB := dbmocks.NewMockDB(t)

	objRef := &types.ObjectRef{
		ID:     uuid.New(),
		Bucket: "test-bucket",
		Key:    "test-key",
		Size:   1024,
	}

	mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "test-key").
		Return(objRef, nil)
	mockDB.EXPECT().MarkObjectDeleted(mock.Anything, "test-bucket", "test-key", mock.AnythingOfType("int64")).
		Return(nil)

	profiles := types.NewProfileSet()
	profiles.Add(&types.StorageProfile{Name: "STANDARD", Replication: 2})

	// Create bucket store with mock
	mockBucketCacheClient := cachemocks.NewMockBucketCacheClient(t)
	bucketsCache := cache.New(ctx, cache.WithMaxSize[string, s3types.Bucket](100))
	bucketStore := cache.NewBucketStore(bucketsCache)
	bucketStore.SetBucket("test-bucket", s3types.Bucket{Name: "test-bucket", OwnerID: "owner"})

	// Create mock CRR hook
	crrHookCalled := false
	mockCRRHook := &mockCRRHookImpl{
		afterDeleteFn: func(ctx context.Context, bucketInfo any, key string) {
			crrHookCalled = true
			assert.Equal(t, "test-key", key)
		},
	}

	svc, err := NewService(Config{
		DB:             mockDB,
		Storage:        &storage.Coordinator{},
		Profiles:       profiles,
		DefaultProfile: "STANDARD",
		BucketStore:    bucketStore,
		CRRHook:        mockCRRHook,
	})
	require.NoError(t, err)

	_, err = svc.DeleteObject(ctx, "test-bucket", "test-key")
	assert.NoError(t, err)
	assert.True(t, crrHookCalled, "CRR hook should be called")

	// Suppress unused variable warning
	_ = mockBucketCacheClient
}

func TestDeleteObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		objects        []DeleteObjectEntry
		setupMock      func(*dbmocks.MockDB)
		wantDeletedLen int
		wantErrorsLen  int
		checkErrors    func(*testing.T, []DeleteError)
	}{
		{
			name: "successful batch delete",
			objects: []DeleteObjectEntry{
				{Key: "key1"},
				{Key: "key2"},
				{Key: "key3"},
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				// GetObject is called first to collect chunk refs
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "key1").Return(&types.ObjectRef{Key: "key1"}, nil)
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "key2").Return(&types.ObjectRef{Key: "key2"}, nil)
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "key3").Return(&types.ObjectRef{Key: "key3"}, nil)
				mockDB.EXPECT().MarkObjectDeleted(mock.Anything, "test-bucket", "key1", mock.AnythingOfType("int64")).Return(nil)
				mockDB.EXPECT().MarkObjectDeleted(mock.Anything, "test-bucket", "key2", mock.AnythingOfType("int64")).Return(nil)
				mockDB.EXPECT().MarkObjectDeleted(mock.Anything, "test-bucket", "key3", mock.AnythingOfType("int64")).Return(nil)
			},
			wantDeletedLen: 3,
			wantErrorsLen:  0,
		},
		{
			name: "partial failure",
			objects: []DeleteObjectEntry{
				{Key: "key1"},
				{Key: "key2"},
				{Key: "key3"},
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				// GetObject is called first to collect chunk refs
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "key1").Return(&types.ObjectRef{Key: "key1"}, nil)
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "key2").Return(&types.ObjectRef{Key: "key2"}, nil)
				mockDB.EXPECT().GetObject(mock.Anything, "test-bucket", "key3").Return(&types.ObjectRef{Key: "key3"}, nil)
				mockDB.EXPECT().MarkObjectDeleted(mock.Anything, "test-bucket", "key1", mock.AnythingOfType("int64")).Return(nil)
				mockDB.EXPECT().MarkObjectDeleted(mock.Anything, "test-bucket", "key2", mock.AnythingOfType("int64")).Return(errors.New("database error"))
				mockDB.EXPECT().MarkObjectDeleted(mock.Anything, "test-bucket", "key3", mock.AnythingOfType("int64")).Return(nil)
			},
			wantDeletedLen: 2,
			wantErrorsLen:  1,
			checkErrors: func(t *testing.T, errs []DeleteError) {
				assert.Equal(t, "key2", errs[0].Key)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			tc.setupMock(mockDB)

			svc := newServiceWithMockDB(t, mockDB)
			result, err := svc.DeleteObjects(context.Background(), &DeleteObjectsRequest{
				Bucket:  "test-bucket",
				Objects: tc.objects,
			})

			assert.NoError(t, err) // Operation itself always succeeds
			assert.Len(t, result.Deleted, tc.wantDeletedLen)
			assert.Len(t, result.Errors, tc.wantErrorsLen)

			if tc.checkErrors != nil {
				tc.checkErrors(t, result.Errors)
			}
		})
	}
}

func TestCopyObject(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *CopyObjectRequest
		setupMock   func(*dbmocks.MockDB)
		wantErr     bool
		wantErrCode ErrorCode
		checkResult func(*testing.T, *CopyObjectResult)
	}{
		{
			name: "successful copy",
			req: &CopyObjectRequest{
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
				DestBucket:   "dest-bucket",
				DestKey:      "dest-key",
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").
					Return(&types.ObjectRef{
						ID:        uuid.New(),
						Bucket:    "src-bucket",
						Key:       "src-key",
						Size:      1024,
						ETag:      "abc123",
						ProfileID: "STANDARD",
						CreatedAt: time.Now().UnixNano(),
						ChunkRefs: []types.ChunkRef{{ChunkID: "chunk1", Size: 1024}},
					}, nil)
				mockDB.EXPECT().PutObject(mock.Anything, mock.MatchedBy(func(obj *types.ObjectRef) bool {
					return obj.Bucket == "dest-bucket" && obj.Key == "dest-key" && obj.ETag == "abc123"
				})).Return(nil)
				mockDB.EXPECT().GetObjectTagging(mock.Anything, "src-bucket", "src-key").
					Return(nil, db.ErrObjectNotFound)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *CopyObjectResult) {
				assert.Equal(t, "abc123", result.ETag)
			},
		},
		{
			name: "source not found",
			req: &CopyObjectRequest{
				SourceBucket: "src-bucket",
				SourceKey:    "nonexistent",
				DestBucket:   "dest-bucket",
				DestKey:      "dest-key",
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "nonexistent").
					Return(nil, db.ErrObjectNotFound)
			},
			wantErr:     true,
			wantErrCode: ErrCodeNotFound,
		},
		{
			name: "copy source if-match precondition failed",
			req: &CopyObjectRequest{
				SourceBucket:      "src-bucket",
				SourceKey:         "src-key",
				DestBucket:        "dest-bucket",
				DestKey:           "dest-key",
				CopySourceIfMatch: "wrong-etag",
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").
					Return(&types.ObjectRef{
						ID:        uuid.New(),
						Bucket:    "src-bucket",
						Key:       "src-key",
						Size:      1024,
						ETag:      "abc123",
						CreatedAt: time.Now().UnixNano(),
					}, nil)
			},
			wantErr:     true,
			wantErrCode: ErrCodePreconditionFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			tc.setupMock(mockDB)

			svc := newServiceWithMockDB(t, mockDB)
			result, err := svc.CopyObject(context.Background(), tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				var objErr *Error
				assert.True(t, errors.As(err, &objErr))
				assert.Equal(t, tc.wantErrCode, objErr.Code)
				return
			}

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

func TestCopyObjectWithTags(t *testing.T) {
	t.Parallel()

	mockDB := dbmocks.NewMockDB(t)

	srcObj := &types.ObjectRef{
		ID:        uuid.New(),
		Bucket:    "src-bucket",
		Key:       "src-key",
		Size:      1024,
		ETag:      "abc123",
		ProfileID: "STANDARD",
		CreatedAt: time.Now().UnixNano(),
	}

	srcTags := &s3types.TagSet{
		Tags: []s3types.Tag{{Key: "env", Value: "prod"}},
	}

	mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").Return(srcObj, nil)
	mockDB.EXPECT().PutObject(mock.Anything, mock.Anything).Return(nil)
	mockDB.EXPECT().GetObjectTagging(mock.Anything, "src-bucket", "src-key").Return(srcTags, nil)
	mockDB.EXPECT().SetObjectTagging(mock.Anything, "dest-bucket", "dest-key", srcTags).Return(nil)

	svc := newServiceWithMockDB(t, mockDB)
	result, err := svc.CopyObject(context.Background(), &CopyObjectRequest{
		SourceBucket:     "src-bucket",
		SourceKey:        "src-key",
		DestBucket:       "dest-bucket",
		DestKey:          "dest-key",
		TaggingDirective: "COPY",
	})

	assert.NoError(t, err)
	assert.NotNil(t, result)
}

func TestListObjects(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *ListObjectsRequest
		setupMock   func(*dbmocks.MockDB)
		checkResult func(*testing.T, *ListObjectsResult)
	}{
		{
			name: "list objects with prefix and delimiter",
			req: &ListObjectsRequest{
				Bucket:    "test-bucket",
				Prefix:    "prefix/",
				Delimiter: "/",
				MaxKeys:   100,
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListObjectsV2(mock.Anything, &db.ListObjectsParams{
					Bucket:     "test-bucket",
					Prefix:     "prefix/",
					Delimiter:  "/",
					MaxKeys:    100,
					StartAfter: "",
				}).Return(&db.ListObjectsResult{
					Objects: []*types.ObjectRef{
						{Key: "prefix/file1.txt", Size: 100, ETag: "etag1", CreatedAt: time.Now().UnixNano()},
						{Key: "prefix/file2.txt", Size: 200, ETag: "etag2", CreatedAt: time.Now().UnixNano()},
					},
					CommonPrefixes: []string{"prefix/subdir/"},
					IsTruncated:    false,
				}, nil)
			},
			checkResult: func(t *testing.T, result *ListObjectsResult) {
				assert.Equal(t, "test-bucket", result.Name)
				assert.Len(t, result.Contents, 2)
				assert.Len(t, result.CommonPrefixes, 1)
				assert.False(t, result.IsTruncated)
			},
		},
		{
			name: "max keys defaults to 1000",
			req: &ListObjectsRequest{
				Bucket:  "test-bucket",
				MaxKeys: 0,
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListObjectsV2(mock.Anything, mock.MatchedBy(func(p *db.ListObjectsParams) bool {
					return p.MaxKeys == 1000
				})).Return(&db.ListObjectsResult{}, nil)
			},
			checkResult: func(t *testing.T, result *ListObjectsResult) {
				assert.NotNil(t, result)
			},
		},
		{
			name: "max keys capped at 1000",
			req: &ListObjectsRequest{
				Bucket:  "test-bucket",
				MaxKeys: 5000,
			},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListObjectsV2(mock.Anything, mock.MatchedBy(func(p *db.ListObjectsParams) bool {
					return p.MaxKeys == 1000
				})).Return(&db.ListObjectsResult{}, nil)
			},
			checkResult: func(t *testing.T, result *ListObjectsResult) {
				assert.NotNil(t, result)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			tc.setupMock(mockDB)

			svc := newServiceWithMockDB(t, mockDB)
			result, err := svc.ListObjects(context.Background(), tc.req)

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

func TestListObjectsV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *ListObjectsV2Request
		setupMock   func(*dbmocks.MockDB)
		checkResult func(*testing.T, *ListObjectsV2Result)
	}{
		{
			name: "list objects v2 with pagination",
			req:  &ListObjectsV2Request{Bucket: "test-bucket"},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListObjectsV2(mock.Anything, &db.ListObjectsParams{
					Bucket:            "test-bucket",
					Prefix:            "",
					Delimiter:         "",
					MaxKeys:           1000,
					ContinuationToken: "",
					StartAfter:        "",
					FetchOwner:        false,
				}).Return(&db.ListObjectsResult{
					Objects: []*types.ObjectRef{
						{Key: "file1.txt", Size: 100, ETag: "etag1", CreatedAt: time.Now().UnixNano()},
					},
					IsTruncated:           true,
					NextContinuationToken: "token123",
				}, nil)
			},
			checkResult: func(t *testing.T, result *ListObjectsV2Result) {
				assert.Equal(t, "test-bucket", result.Name)
				assert.Len(t, result.Contents, 1)
				assert.True(t, result.IsTruncated)
				assert.Equal(t, "token123", result.NextContinuationToken)
			},
		},
		{
			name: "with fetch owner",
			req:  &ListObjectsV2Request{Bucket: "test-bucket", FetchOwner: true},
			setupMock: func(mockDB *dbmocks.MockDB) {
				mockDB.EXPECT().ListObjectsV2(mock.Anything, mock.MatchedBy(func(p *db.ListObjectsParams) bool {
					return p.FetchOwner == true
				})).Return(&db.ListObjectsResult{
					Objects: []*types.ObjectRef{
						{Key: "file1.txt", Size: 100, ETag: "etag1", CreatedAt: time.Now().UnixNano()},
					},
				}, nil)
			},
			checkResult: func(t *testing.T, result *ListObjectsV2Result) {
				assert.NotNil(t, result.Contents[0].Owner)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			tc.setupMock(mockDB)

			svc := newServiceWithMockDB(t, mockDB)
			result, err := svc.ListObjectsV2(context.Background(), tc.req)

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
		{"not found", ErrCodeNotFound, "NoSuchKey"},
		{"precondition failed", ErrCodePreconditionFailed, "PreconditionFailed"},
		{"invalid encryption", ErrCodeInvalidEncryption, "InvalidEncryptionAlgorithmError"},
		{"incomplete body", ErrCodeIncompleteBody, "IncompleteBody"},
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

// mockCRRHookImpl implements CRRHook for testing
type mockCRRHookImpl struct {
	afterPutFn    func(ctx context.Context, bucketInfo any, key, etag string, size int64)
	afterDeleteFn func(ctx context.Context, bucketInfo any, key string)
}

func (m *mockCRRHookImpl) AfterPutObject(ctx context.Context, bucketInfo any, key, etag string, size int64) {
	if m.afterPutFn != nil {
		m.afterPutFn(ctx, bucketInfo, key, etag, size)
	}
}

func (m *mockCRRHookImpl) AfterDeleteObject(ctx context.Context, bucketInfo any, key string) {
	if m.afterDeleteFn != nil {
		m.afterDeleteFn(ctx, bucketInfo, key)
	}
}

func TestCheckConditionalHeaders(t *testing.T) {
	t.Parallel()

	mockDB := dbmocks.NewMockDB(t)
	svc := newServiceWithMockDB(t, mockDB)
	impl := svc.(*serviceImpl)

	now := time.Now()
	etag := "abc123"
	hourAgo := now.Add(-time.Hour)
	hourAhead := now.Add(time.Hour)

	tests := []struct {
		name              string
		req               *GetObjectRequest
		wantShouldProceed bool
		wantStatusCode    int
		wantNotModified   bool
	}{
		{
			name:              "no conditions - should proceed",
			req:               &GetObjectRequest{},
			wantShouldProceed: true,
		},
		{
			name:              "if-match matches - should proceed",
			req:               &GetObjectRequest{IfMatch: "abc123"},
			wantShouldProceed: true,
		},
		{
			name:              "if-match doesn't match - 412",
			req:               &GetObjectRequest{IfMatch: "wrong-etag"},
			wantShouldProceed: false,
			wantStatusCode:    412,
		},
		{
			name:              "if-none-match matches - 304",
			req:               &GetObjectRequest{IfNoneMatch: "abc123"},
			wantShouldProceed: false,
			wantStatusCode:    304,
			wantNotModified:   true,
		},
		{
			name:              "if-none-match doesn't match - should proceed",
			req:               &GetObjectRequest{IfNoneMatch: "different-etag"},
			wantShouldProceed: true,
		},
		{
			name:              "if-modified-since not modified - 304",
			req:               &GetObjectRequest{IfModifiedSince: &hourAhead},
			wantShouldProceed: false,
			wantStatusCode:    304,
			wantNotModified:   true,
		},
		{
			name:              "if-modified-since modified - should proceed",
			req:               &GetObjectRequest{IfModifiedSince: &hourAgo},
			wantShouldProceed: true,
		},
		{
			name:              "if-unmodified-since modified - 412",
			req:               &GetObjectRequest{IfUnmodifiedSince: &hourAgo},
			wantShouldProceed: false,
			wantStatusCode:    412,
		},
		{
			name:              "if-unmodified-since not modified - should proceed",
			req:               &GetObjectRequest{IfUnmodifiedSince: &hourAhead},
			wantShouldProceed: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := impl.checkConditionalHeaders(tc.req, etag, now)

			assert.Equal(t, tc.wantShouldProceed, result.ShouldProceed)
			if !tc.wantShouldProceed {
				assert.Equal(t, tc.wantStatusCode, result.StatusCode)
				assert.Equal(t, tc.wantNotModified, result.NotModified)
			}
		})
	}
}

func TestPutObjectValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *PutObjectRequest
		wantErr     bool
		wantErrCode ErrorCode
	}{
		{
			name: "invalid storage class",
			req: &PutObjectRequest{
				Bucket:       "test-bucket",
				Key:          "test-key",
				Body:         bytes.NewReader([]byte("test")),
				StorageClass: "INVALID_CLASS",
			},
			wantErr:     true,
			wantErrCode: ErrCodeInvalidStorageClass,
		},
		{
			name: "both SSE-C and SSE-KMS not allowed",
			req: &PutObjectRequest{
				Bucket: "test-bucket",
				Key:    "test-key",
				Body:   bytes.NewReader([]byte("test")),
				SSEC: &SSECParams{
					Algorithm: "AES256",
					Key:       make([]byte, 32),
					KeyMD5:    "abc",
				},
				SSEKMS: &SSEKMSParams{
					KeyID: "key-123",
				},
			},
			wantErr:     true,
			wantErrCode: ErrCodeInvalidEncryption,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			svc := newServiceWithMockDB(t, mockDB)

			_, err := svc.PutObject(context.Background(), tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				var objErr *Error
				assert.True(t, errors.As(err, &objErr))
				assert.Equal(t, tc.wantErrCode, objErr.Code)
				return
			}

			assert.NoError(t, err)
		})
	}
}
