package multipart_test

import (
	"bytes"
	"context"
	"errors"
	"testing"

	dbmocks "github.com/LeeDigitalWorks/zapfs/mocks/db"
	mpmocks "github.com/LeeDigitalWorks/zapfs/mocks/multipart"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/multipart"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/storage"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// ============================================================================
// NewService Tests
// ============================================================================

func TestNewService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupConfig func(t *testing.T) multipart.Config
		wantErr     bool
		errContains string
	}{
		{
			name: "missing DB returns error",
			setupConfig: func(t *testing.T) multipart.Config {
				return multipart.Config{
					DB:       nil,
					Storage:  mpmocks.NewMockStorage(t),
					Profiles: types.NewProfileSet(),
				}
			},
			wantErr:     true,
			errContains: "DB is required",
		},
		{
			name: "missing Storage returns error",
			setupConfig: func(t *testing.T) multipart.Config {
				return multipart.Config{
					DB:       dbmocks.NewMockDB(t),
					Storage:  nil,
					Profiles: types.NewProfileSet(),
				}
			},
			wantErr:     true,
			errContains: "Storage is required",
		},
		{
			name: "valid config succeeds",
			setupConfig: func(t *testing.T) multipart.Config {
				return multipart.Config{
					DB:       dbmocks.NewMockDB(t),
					Storage:  mpmocks.NewMockStorage(t),
					Profiles: types.NewProfileSet(),
				}
			},
			wantErr: false,
		},
		{
			name: "uses default profile when not specified",
			setupConfig: func(t *testing.T) multipart.Config {
				return multipart.Config{
					DB:             dbmocks.NewMockDB(t),
					Storage:        mpmocks.NewMockStorage(t),
					Profiles:       types.NewProfileSet(),
					DefaultProfile: "", // Should default to "STANDARD"
				}
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.setupConfig(t)
			svc, err := multipart.NewService(cfg)

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

// ============================================================================
// CreateUpload Tests
// ============================================================================

func TestCreateUpload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *multipart.CreateUploadRequest
		setupMocks  func(*dbmocks.MockDB, *mpmocks.MockStorage)
		wantErr     bool
		wantErrCode multipart.ErrorCode
	}{
		{
			name: "successful create",
			req: &multipart.CreateUploadRequest{
				Bucket:  "test-bucket",
				Key:     "test-key",
				OwnerID: "owner-123",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().CreateMultipartUpload(mock.Anything, mock.AnythingOfType("*types.MultipartUpload")).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "with storage class",
			req: &multipart.CreateUploadRequest{
				Bucket:       "test-bucket",
				Key:          "test-key",
				OwnerID:      "owner-123",
				StorageClass: "STANDARD_IA",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().CreateMultipartUpload(mock.Anything, mock.MatchedBy(func(u *types.MultipartUpload) bool {
					return u.StorageClass == "STANDARD_IA"
				})).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "db error",
			req: &multipart.CreateUploadRequest{
				Bucket:  "test-bucket",
				Key:     "test-key",
				OwnerID: "owner-123",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().CreateMultipartUpload(mock.Anything, mock.Anything).Return(errors.New("db error"))
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInternalError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockStorage := mpmocks.NewMockStorage(t)

			tc.setupMocks(mockDB, mockStorage)

			profiles := types.NewProfileSet()
			profiles.Add(&types.StorageProfile{Name: "STANDARD", Replication: 2})

			svc, err := multipart.NewService(multipart.Config{
				DB:             mockDB,
				Storage:        mockStorage,
				Profiles:       profiles,
				DefaultProfile: "STANDARD",
			})
			require.NoError(t, err)

			result, err := svc.CreateUpload(context.Background(), tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				var mpErr *multipart.Error
				assert.True(t, errors.As(err, &mpErr))
				assert.Equal(t, tc.wantErrCode, mpErr.Code)
				return
			}

			assert.NoError(t, err)
			assert.NotEmpty(t, result.UploadID)
		})
	}
}

// ============================================================================
// UploadPart Tests
// ============================================================================

func TestUploadPart(t *testing.T) {
	t.Parallel()

	uploadID := "test-upload-id"
	uploadUUID := uuid.New()

	tests := []struct {
		name        string
		req         *multipart.UploadPartRequest
		setupMocks  func(*dbmocks.MockDB, *mpmocks.MockStorage)
		wantErr     bool
		wantErrCode multipart.ErrorCode
	}{
		{
			name: "successful upload",
			req: &multipart.UploadPartRequest{
				Bucket:        "test-bucket",
				Key:           "test-key",
				UploadID:      uploadID,
				PartNumber:    1,
				Body:          bytes.NewReader([]byte("test data")),
				ContentLength: 9,
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					Bucket:       "test-bucket",
					Key:          "test-key",
					StorageClass: "STANDARD",
				}, nil)

				mockStorage.EXPECT().WriteObject(mock.Anything, mock.AnythingOfType("*storage.WriteRequest")).Return(&storage.WriteResult{
					Size: 9,
					ChunkRefs: []types.ChunkRef{
						{ChunkID: "chunk-1", Size: 9, FileServerAddr: "localhost:9000"},
					},
				}, nil)

				mockDB.EXPECT().PutPart(mock.Anything, mock.AnythingOfType("*types.MultipartPart")).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "invalid part number - too low",
			req: &multipart.UploadPartRequest{
				Bucket:        "test-bucket",
				Key:           "test-key",
				UploadID:      uploadID,
				PartNumber:    0,
				Body:          bytes.NewReader([]byte("test")),
				ContentLength: 4,
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				// No mocks needed - validation fails first
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInvalidArgument,
		},
		{
			name: "invalid part number - too high",
			req: &multipart.UploadPartRequest{
				Bucket:        "test-bucket",
				Key:           "test-key",
				UploadID:      uploadID,
				PartNumber:    10001,
				Body:          bytes.NewReader([]byte("test")),
				ContentLength: 4,
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				// No mocks needed - validation fails first
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInvalidArgument,
		},
		{
			name: "upload not found",
			req: &multipart.UploadPartRequest{
				Bucket:        "test-bucket",
				Key:           "test-key",
				UploadID:      "nonexistent",
				PartNumber:    1,
				Body:          bytes.NewReader([]byte("test")),
				ContentLength: 4,
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", "nonexistent").Return(nil, db.ErrUploadNotFound)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeNoSuchUpload,
		},
		{
			name: "storage write error",
			req: &multipart.UploadPartRequest{
				Bucket:        "test-bucket",
				Key:           "test-key",
				UploadID:      uploadID,
				PartNumber:    1,
				Body:          bytes.NewReader([]byte("test")),
				ContentLength: 4,
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockStorage.EXPECT().WriteObject(mock.Anything, mock.Anything).Return(nil, errors.New("storage error"))
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInternalError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockStorage := mpmocks.NewMockStorage(t)

			tc.setupMocks(mockDB, mockStorage)

			profiles := types.NewProfileSet()
			profiles.Add(&types.StorageProfile{Name: "STANDARD", Replication: 2})

			svc, err := multipart.NewService(multipart.Config{
				DB:             mockDB,
				Storage:        mockStorage,
				Profiles:       profiles,
				DefaultProfile: "STANDARD",
			})
			require.NoError(t, err)

			result, err := svc.UploadPart(context.Background(), tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				var mpErr *multipart.Error
				assert.True(t, errors.As(err, &mpErr))
				assert.Equal(t, tc.wantErrCode, mpErr.Code)
				return
			}

			assert.NoError(t, err)
			assert.NotEmpty(t, result.ETag)
		})
	}
}

// ============================================================================
// AbortUpload Tests
// ============================================================================

func TestAbortUpload(t *testing.T) {
	t.Parallel()

	uploadID := "test-upload-id"
	uploadUUID := uuid.New()

	tests := []struct {
		name        string
		bucket      string
		key         string
		uploadID    string
		setupMocks  func(*dbmocks.MockDB, *mpmocks.MockStorage)
		wantErr     bool
		wantErrCode multipart.ErrorCode
	}{
		{
			name:     "successful abort",
			bucket:   "test-bucket",
			key:      "test-key",
			uploadID: uploadID,
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", uploadID).Return(&types.MultipartUpload{
					ID:       uploadUUID,
					UploadID: uploadID,
				}, nil)
				mockDB.EXPECT().DeleteMultipartUpload(mock.Anything, "test-bucket", "test-key", uploadID).Return(nil)
			},
			wantErr: false,
		},
		{
			name:     "upload not found",
			bucket:   "test-bucket",
			key:      "test-key",
			uploadID: "nonexistent",
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", "nonexistent").Return(nil, db.ErrUploadNotFound)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeNoSuchUpload,
		},
		{
			name:     "delete error",
			bucket:   "test-bucket",
			key:      "test-key",
			uploadID: uploadID,
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", uploadID).Return(&types.MultipartUpload{
					ID:       uploadUUID,
					UploadID: uploadID,
				}, nil)
				mockDB.EXPECT().DeleteMultipartUpload(mock.Anything, "test-bucket", "test-key", uploadID).Return(errors.New("db error"))
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInternalError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockStorage := mpmocks.NewMockStorage(t)

			tc.setupMocks(mockDB, mockStorage)

			profiles := types.NewProfileSet()
			profiles.Add(&types.StorageProfile{Name: "STANDARD", Replication: 2})

			svc, err := multipart.NewService(multipart.Config{
				DB:             mockDB,
				Storage:        mockStorage,
				Profiles:       profiles,
				DefaultProfile: "STANDARD",
			})
			require.NoError(t, err)

			err = svc.AbortUpload(context.Background(), tc.bucket, tc.key, tc.uploadID)

			if tc.wantErr {
				assert.Error(t, err)
				var mpErr *multipart.Error
				assert.True(t, errors.As(err, &mpErr))
				assert.Equal(t, tc.wantErrCode, mpErr.Code)
				return
			}

			assert.NoError(t, err)
		})
	}
}

// ============================================================================
// ListParts Tests
// ============================================================================

func TestListParts(t *testing.T) {
	t.Parallel()

	uploadID := "test-upload-id"
	uploadUUID := uuid.New()

	tests := []struct {
		name        string
		req         *multipart.ListPartsRequest
		setupMocks  func(*dbmocks.MockDB, *mpmocks.MockStorage)
		wantErr     bool
		wantErrCode multipart.ErrorCode
		checkResult func(*testing.T, *multipart.ListPartsResult)
	}{
		{
			name: "successful list",
			req: &multipart.ListPartsRequest{
				Bucket:   "test-bucket",
				Key:      "test-key",
				UploadID: uploadID,
				MaxParts: 100,
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					OwnerID:      "owner-123",
					StorageClass: "STANDARD",
				}, nil)
				mockDB.EXPECT().ListParts(mock.Anything, uploadID, 0, 100).Return([]*types.MultipartPart{
					{PartNumber: 1, ETag: "etag1", Size: 100},
					{PartNumber: 2, ETag: "etag2", Size: 200},
				}, false, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *multipart.ListPartsResult) {
				assert.Len(t, result.Parts, 2)
				assert.Equal(t, 1, result.Parts[0].PartNumber)
				assert.Equal(t, 2, result.Parts[1].PartNumber)
				assert.False(t, result.IsTruncated)
			},
		},
		{
			name: "upload not found",
			req: &multipart.ListPartsRequest{
				Bucket:   "test-bucket",
				Key:      "test-key",
				UploadID: "nonexistent",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", "nonexistent").Return(nil, db.ErrUploadNotFound)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeNoSuchUpload,
		},
		{
			name: "defaults max parts to 1000",
			req: &multipart.ListPartsRequest{
				Bucket:   "test-bucket",
				Key:      "test-key",
				UploadID: uploadID,
				MaxParts: 0, // Should default to 1000
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "test-bucket", "test-key", uploadID).Return(&types.MultipartUpload{
					ID:       uploadUUID,
					UploadID: uploadID,
				}, nil)
				mockDB.EXPECT().ListParts(mock.Anything, uploadID, 0, 1000).Return([]*types.MultipartPart{}, false, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *multipart.ListPartsResult) {
				assert.Equal(t, 1000, result.MaxParts)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockStorage := mpmocks.NewMockStorage(t)

			tc.setupMocks(mockDB, mockStorage)

			profiles := types.NewProfileSet()

			svc, err := multipart.NewService(multipart.Config{
				DB:             mockDB,
				Storage:        mockStorage,
				Profiles:       profiles,
				DefaultProfile: "STANDARD",
			})
			require.NoError(t, err)

			result, err := svc.ListParts(context.Background(), tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				var mpErr *multipart.Error
				assert.True(t, errors.As(err, &mpErr))
				assert.Equal(t, tc.wantErrCode, mpErr.Code)
				return
			}

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

// ============================================================================
// ListUploads Tests
// ============================================================================

func TestListUploads(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         *multipart.ListUploadsRequest
		setupMocks  func(*dbmocks.MockDB, *mpmocks.MockStorage)
		wantErr     bool
		wantErrCode multipart.ErrorCode
		checkResult func(*testing.T, *multipart.ListUploadsResult)
	}{
		{
			name: "successful list",
			req: &multipart.ListUploadsRequest{
				Bucket:     "test-bucket",
				MaxUploads: 100,
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().ListMultipartUploads(mock.Anything, "test-bucket", "", "", "", 100).Return([]*types.MultipartUpload{
					{Key: "key1", UploadID: "upload1"},
					{Key: "key2", UploadID: "upload2"},
				}, false, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *multipart.ListUploadsResult) {
				assert.Len(t, result.Uploads, 2)
				assert.Equal(t, "key1", result.Uploads[0].Key)
				assert.Equal(t, "key2", result.Uploads[1].Key)
				assert.False(t, result.IsTruncated)
			},
		},
		{
			name: "with prefix filter",
			req: &multipart.ListUploadsRequest{
				Bucket:     "test-bucket",
				Prefix:     "folder/",
				MaxUploads: 100,
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().ListMultipartUploads(mock.Anything, "test-bucket", "folder/", "", "", 100).Return([]*types.MultipartUpload{
					{Key: "folder/file1", UploadID: "upload1"},
				}, false, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *multipart.ListUploadsResult) {
				assert.Len(t, result.Uploads, 1)
				assert.Equal(t, "folder/file1", result.Uploads[0].Key)
			},
		},
		{
			name: "defaults max uploads to 1000",
			req: &multipart.ListUploadsRequest{
				Bucket:     "test-bucket",
				MaxUploads: 0, // Should default to 1000
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().ListMultipartUploads(mock.Anything, "test-bucket", "", "", "", 1000).Return([]*types.MultipartUpload{}, false, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, result *multipart.ListUploadsResult) {
				assert.Equal(t, 1000, result.MaxUploads)
			},
		},
		{
			name: "db error",
			req: &multipart.ListUploadsRequest{
				Bucket: "test-bucket",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().ListMultipartUploads(mock.Anything, "test-bucket", "", "", "", 1000).Return(nil, false, errors.New("db error"))
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInternalError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockStorage := mpmocks.NewMockStorage(t)

			tc.setupMocks(mockDB, mockStorage)

			profiles := types.NewProfileSet()

			svc, err := multipart.NewService(multipart.Config{
				DB:             mockDB,
				Storage:        mockStorage,
				Profiles:       profiles,
				DefaultProfile: "STANDARD",
			})
			require.NoError(t, err)

			result, err := svc.ListUploads(context.Background(), tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				var mpErr *multipart.Error
				assert.True(t, errors.As(err, &mpErr))
				assert.Equal(t, tc.wantErrCode, mpErr.Code)
				return
			}

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

// ============================================================================
// Error Mapping Tests
// ============================================================================

func TestErrorToS3Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errCode  multipart.ErrorCode
		expected string
	}{
		{"no such upload", multipart.ErrCodeNoSuchUpload, "NoSuchUpload"},
		{"invalid part", multipart.ErrCodeInvalidPart, "InvalidPart"},
		{"invalid part order", multipart.ErrCodeInvalidPartOrder, "InvalidPartOrder"},
		{"entity too small", multipart.ErrCodeEntityTooSmall, "EntityTooSmall"},
		{"invalid argument", multipart.ErrCodeInvalidArgument, "InvalidArgument"},
		{"internal error", multipart.ErrCodeInternalError, "InternalError"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := &multipart.Error{Code: tc.errCode, Message: "test"}
			s3Err := err.ToS3Error()
			assert.Equal(t, tc.expected, s3Err.Code())
		})
	}
}
