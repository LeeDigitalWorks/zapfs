// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package multipart_test

import (
	"bytes"
	"context"
	"errors"
	"io"
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
// UploadPartCopy Tests
// ============================================================================

func TestUploadPartCopy(t *testing.T) {
	t.Parallel()

	uploadID := "test-upload-id"
	uploadUUID := uuid.New()
	srcObjectID := uuid.New()

	tests := []struct {
		name        string
		req         *multipart.UploadPartCopyRequest
		setupMocks  func(*dbmocks.MockDB, *mpmocks.MockStorage)
		wantErr     bool
		wantErrCode multipart.ErrorCode
	}{
		{
			name: "successful copy - full object",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   1,
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").Return(&types.ObjectRef{
					ID:        srcObjectID,
					Bucket:    "src-bucket",
					Key:       "src-key",
					Size:      1024,
					ETag:      "abc123",
					CreatedAt: 1000000000000000000,
					ChunkRefs: []types.ChunkRef{{ChunkID: "chunk-1", Size: 1024}},
				}, nil)

				// Mock streaming read - write data to the writer
				mockStorage.EXPECT().ReadObject(mock.Anything, mock.AnythingOfType("*storage.ReadRequest"), mock.Anything).
					Run(func(ctx context.Context, req *storage.ReadRequest, w io.Writer) {
						w.Write([]byte("test data for copy"))
					}).Return(nil)

				// WriteObject must consume from the pipe to allow the goroutine to complete
				mockStorage.EXPECT().WriteObject(mock.Anything, mock.AnythingOfType("*storage.WriteRequest")).
					Run(func(ctx context.Context, req *storage.WriteRequest) {
						// Drain the body (pipe reader) to let the writing goroutine complete
						io.Copy(io.Discard, req.Body)
					}).Return(&storage.WriteResult{
					Size:      1024,
					ChunkRefs: []types.ChunkRef{{ChunkID: "new-chunk-1", Size: 1024}},
				}, nil)

				mockDB.EXPECT().PutPart(mock.Anything, mock.AnythingOfType("*types.MultipartPart")).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "successful copy - with range",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   2,
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
				SourceRange:  "bytes=0-499",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").Return(&types.ObjectRef{
					ID:        srcObjectID,
					Bucket:    "src-bucket",
					Key:       "src-key",
					Size:      1024,
					ETag:      "abc123",
					CreatedAt: 1000000000000000000,
					ChunkRefs: []types.ChunkRef{{ChunkID: "chunk-1", Size: 1024}},
				}, nil)

				// Mock streaming range read
				mockStorage.EXPECT().ReadObjectRange(mock.Anything, mock.MatchedBy(func(req *storage.ReadRangeRequest) bool {
					return req.Offset == 0 && req.Length == 500
				}), mock.Anything).
					Run(func(ctx context.Context, req *storage.ReadRangeRequest, w io.Writer) {
						w.Write([]byte("partial data"))
					}).Return(nil)

				// WriteObject must consume from the pipe
				mockStorage.EXPECT().WriteObject(mock.Anything, mock.MatchedBy(func(req *storage.WriteRequest) bool {
					return req.Size == 500
				})).Run(func(ctx context.Context, req *storage.WriteRequest) {
					io.Copy(io.Discard, req.Body)
				}).Return(&storage.WriteResult{
					Size:      500,
					ChunkRefs: []types.ChunkRef{{ChunkID: "new-chunk-1", Size: 500}},
				}, nil)

				mockDB.EXPECT().PutPart(mock.Anything, mock.AnythingOfType("*types.MultipartPart")).Return(nil)
			},
			wantErr: false,
		},
		{
			name: "invalid part number - too low",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   0,
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				// Validation fails first, no mocks needed
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInvalidArgument,
		},
		{
			name: "invalid part number - too high",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   10001,
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				// Validation fails first
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInvalidArgument,
		},
		{
			name: "upload not found",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     "nonexistent",
				PartNumber:   1,
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", "nonexistent").Return(nil, db.ErrUploadNotFound)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeNoSuchUpload,
		},
		{
			name: "source object not found",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   1,
				SourceBucket: "src-bucket",
				SourceKey:    "nonexistent",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "nonexistent").Return(nil, db.ErrObjectNotFound)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeNoSuchUpload, // Uses NoSuchUpload for source not found
		},
		{
			name: "source object deleted",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   1,
				SourceBucket: "src-bucket",
				SourceKey:    "deleted-key",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				// Return a deleted object (DeletedAt > 0)
				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "deleted-key").Return(&types.ObjectRef{
					ID:        srcObjectID,
					Bucket:    "src-bucket",
					Key:       "deleted-key",
					DeletedAt: 1000000000000000000, // Marked as deleted
				}, nil)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeNoSuchUpload,
		},
		{
			name: "precondition failed - If-Match",
			req: &multipart.UploadPartCopyRequest{
				Bucket:            "dest-bucket",
				Key:               "dest-key",
				UploadID:          uploadID,
				PartNumber:        1,
				SourceBucket:      "src-bucket",
				SourceKey:         "src-key",
				CopySourceIfMatch: "wrong-etag",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").Return(&types.ObjectRef{
					ID:        srcObjectID,
					Bucket:    "src-bucket",
					Key:       "src-key",
					Size:      1024,
					ETag:      "correct-etag",
					CreatedAt: 1000000000000000000,
				}, nil)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodePreconditionFailed,
		},
		{
			name: "precondition failed - If-None-Match",
			req: &multipart.UploadPartCopyRequest{
				Bucket:                "dest-bucket",
				Key:                   "dest-key",
				UploadID:              uploadID,
				PartNumber:            1,
				SourceBucket:          "src-bucket",
				SourceKey:             "src-key",
				CopySourceIfNoneMatch: "matching-etag",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").Return(&types.ObjectRef{
					ID:        srcObjectID,
					Bucket:    "src-bucket",
					Key:       "src-key",
					Size:      1024,
					ETag:      "matching-etag",
					CreatedAt: 1000000000000000000,
				}, nil)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodePreconditionFailed,
		},
		{
			name: "invalid source range format",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   1,
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
				SourceRange:  "invalid-range",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").Return(&types.ObjectRef{
					ID:        srcObjectID,
					Bucket:    "src-bucket",
					Key:       "src-key",
					Size:      1024,
					ETag:      "abc123",
					CreatedAt: 1000000000000000000,
				}, nil)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInvalidArgument,
		},
		{
			name: "source range out of bounds",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   1,
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
				SourceRange:  "bytes=2000-3000", // Object is only 1024 bytes
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").Return(&types.ObjectRef{
					ID:        srcObjectID,
					Bucket:    "src-bucket",
					Key:       "src-key",
					Size:      1024,
					ETag:      "abc123",
					CreatedAt: 1000000000000000000,
				}, nil)
			},
			wantErr:     true,
			wantErrCode: multipart.ErrCodeInvalidArgument,
		},
		{
			name: "storage write error",
			req: &multipart.UploadPartCopyRequest{
				Bucket:       "dest-bucket",
				Key:          "dest-key",
				UploadID:     uploadID,
				PartNumber:   1,
				SourceBucket: "src-bucket",
				SourceKey:    "src-key",
			},
			setupMocks: func(mockDB *dbmocks.MockDB, mockStorage *mpmocks.MockStorage) {
				mockDB.EXPECT().GetMultipartUpload(mock.Anything, "dest-bucket", "dest-key", uploadID).Return(&types.MultipartUpload{
					ID:           uploadUUID,
					UploadID:     uploadID,
					StorageClass: "STANDARD",
				}, nil)

				mockDB.EXPECT().GetObject(mock.Anything, "src-bucket", "src-key").Return(&types.ObjectRef{
					ID:        srcObjectID,
					Bucket:    "src-bucket",
					Key:       "src-key",
					Size:      1024,
					ETag:      "abc123",
					CreatedAt: 1000000000000000000,
					ChunkRefs: []types.ChunkRef{{ChunkID: "chunk-1", Size: 1024}},
				}, nil)

				mockStorage.EXPECT().ReadObject(mock.Anything, mock.Anything, mock.Anything).
					Run(func(ctx context.Context, req *storage.ReadRequest, w io.Writer) {
						w.Write([]byte("test data"))
					}).Return(nil)

				// WriteObject must consume from the pipe before returning error
				mockStorage.EXPECT().WriteObject(mock.Anything, mock.Anything).
					Run(func(ctx context.Context, req *storage.WriteRequest) {
						io.Copy(io.Discard, req.Body)
					}).Return(nil, errors.New("write error"))
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

			result, err := svc.UploadPartCopy(context.Background(), tc.req)

			if tc.wantErr {
				assert.Error(t, err)
				var mpErr *multipart.Error
				assert.True(t, errors.As(err, &mpErr))
				assert.Equal(t, tc.wantErrCode, mpErr.Code)
				return
			}

			assert.NoError(t, err)
			assert.NotEmpty(t, result.ETag)
			assert.NotZero(t, result.LastModified)
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
				// AbortUpload lists parts to get chunk refs for cleanup
				mockDB.EXPECT().ListParts(mock.Anything, uploadID, 0, 10000).Return([]*types.MultipartPart{}, false, nil)
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
				// AbortUpload lists parts to get chunk refs for cleanup
				mockDB.EXPECT().ListParts(mock.Anything, uploadID, 0, 10000).Return([]*types.MultipartPart{}, false, nil)
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
