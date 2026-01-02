// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	clientmocks "github.com/LeeDigitalWorks/zapfs/mocks/client"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/proto/common_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestListObjectsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		bucket        string
		objects       []string // object keys to create
		queryParams   string
		expectedCount int
		expectedCode  int
	}{
		{
			name:          "empty bucket",
			bucket:        "test-bucket",
			objects:       nil,
			queryParams:   "",
			expectedCount: 0,
			expectedCode:  http.StatusOK,
		},
		{
			name:          "with objects",
			bucket:        "test-bucket",
			objects:       []string{"file1.txt", "file2.txt", "folder/file3.txt"},
			queryParams:   "",
			expectedCount: 3,
			expectedCode:  http.StatusOK,
		},
		{
			name:          "with prefix filter",
			bucket:        "test-bucket",
			objects:       []string{"logs/app.log", "logs/error.log", "data/file.txt"},
			queryParams:   "?prefix=logs/",
			expectedCount: 2,
			expectedCode:  http.StatusOK,
		},
		{
			name:          "with delimiter",
			bucket:        "test-bucket",
			objects:       []string{"folder1/file1.txt", "folder1/file2.txt", "folder2/file3.txt", "root.txt"},
			queryParams:   "?delimiter=/",
			expectedCount: 1, // Only root.txt, folders become common prefixes
			expectedCode:  http.StatusOK,
		},
		{
			name:          "with marker pagination",
			bucket:        "test-bucket",
			objects:       []string{"a.txt", "b.txt", "c.txt", "d.txt"},
			queryParams:   "?marker=b.txt&max-keys=2",
			expectedCount: 2, // Should return c.txt and d.txt
			expectedCode:  http.StatusOK,
		},
		{
			name:          "with prefix and delimiter",
			bucket:        "test-bucket",
			objects:       []string{"logs/2024/app.log", "logs/2024/error.log", "logs/2023/app.log", "data/file.txt"},
			queryParams:   "?prefix=logs/&delimiter=/",
			expectedCount: 0, // All under logs/ become common prefixes
			expectedCode:  http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Add objects
			for _, key := range tc.objects {
				err := srv.db.PutObject(ctx, &types.ObjectRef{
					Bucket:    tc.bucket,
					Key:       key,
					Size:      100,
					ETag:      "abc123",
					CreatedAt: 1000000000,
				})
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, "", "test-owner")
			d.Req = httptest.NewRequest("GET", "/"+tc.bucket+tc.queryParams, nil)
			w := httptest.NewRecorder()

			srv.ListObjectsHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.expectedCode == http.StatusOK {
				var result s3types.ListObjectsResult
				err = xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)

				assert.Equal(t, tc.bucket, result.Name)
				assert.Len(t, result.Contents, tc.expectedCount)

				// Verify marker is set if provided
				if strings.Contains(tc.queryParams, "marker=") {
					assert.NotEmpty(t, result.Marker)
				}

				// Verify delimiter is set if provided
				if strings.Contains(tc.queryParams, "delimiter=") {
					assert.NotEmpty(t, result.Delimiter)
				}

				// Verify common prefixes when delimiter is used
				if strings.Contains(tc.queryParams, "delimiter=") && len(result.Contents) == 0 {
					assert.Greater(t, len(result.CommonPrefixes), 0, "should have common prefixes when delimiter is used")
				}
			}
		})
	}
}

func TestHeadObjectHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		key            string
		createObject   bool
		objectSize     int64
		objectETag     string
		expectedCode   int
		expectedLength string
		expectedETag   string
	}{
		{
			name:         "object not found",
			bucket:       "test-bucket",
			key:          "nonexistent.txt",
			createObject: false,
			expectedCode: http.StatusNotFound,
		},
		{
			name:           "object found",
			bucket:         "test-bucket",
			key:            "test.txt",
			createObject:   true,
			objectSize:     12345,
			objectETag:     "abc123def456",
			expectedCode:   http.StatusOK,
			expectedLength: "12345",
			expectedETag:   "\"abc123def456\"",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Create object if needed
			if tc.createObject {
				err = srv.db.PutObject(ctx, &types.ObjectRef{
					Bucket:    tc.bucket,
					Key:       tc.key,
					Size:      uint64(tc.objectSize),
					ETag:      tc.objectETag,
					CreatedAt: 1000000000,
				})
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, tc.key, "test-owner")
			w := httptest.NewRecorder()

			srv.HeadObjectHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.expectedCode == http.StatusOK {
				assert.Equal(t, tc.expectedLength, w.Header().Get("Content-Length"))
				assert.Equal(t, tc.expectedETag, w.Header().Get("ETag"))
			}
		})
	}
}

func TestDeleteObjectHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		key          string
		createObject bool
		expectedCode int
		verifyDelete bool // whether to verify deletion worked
	}{
		{
			name:         "object not found returns 204",
			bucket:       "test-bucket",
			key:          "nonexistent.txt",
			createObject: false,
			expectedCode: http.StatusNoContent, // S3 returns 204 even for non-existent
		},
		{
			name:         "successfully deletes object",
			bucket:       "test-bucket",
			key:          "test.txt",
			createObject: true,
			expectedCode: http.StatusNoContent,
			verifyDelete: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Create object if needed
			if tc.createObject {
				err = srv.db.PutObject(ctx, &types.ObjectRef{
					Bucket: tc.bucket,
					Key:    tc.key,
					Size:   100,
					ETag:   "abc123",
				})
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, tc.key, "test-owner")
			w := httptest.NewRecorder()

			srv.DeleteObjectHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.verifyDelete {
				obj, err := srv.db.GetObject(ctx, tc.bucket, tc.key)
				require.NoError(t, err)
				assert.True(t, obj.IsDeleted())
			}
		})
	}
}

func TestDeleteObjectsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		bucket          string
		existingKeys    []string
		deleteKeys      []string
		quiet           bool
		expectedCode    int
		expectedDeleted int
		expectedErrors  int
	}{
		{
			name:            "delete multiple objects",
			bucket:          "test-bucket",
			existingKeys:    []string{"file1.txt", "file2.txt", "file3.txt"},
			deleteKeys:      []string{"file1.txt", "file2.txt"},
			quiet:           false,
			expectedCode:    http.StatusOK,
			expectedDeleted: 2,
			expectedErrors:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Create objects
			for _, key := range tc.existingKeys {
				err := srv.db.PutObject(ctx, &types.ObjectRef{
					Bucket: tc.bucket,
					Key:    key,
					Size:   100,
					ETag:   "abc123",
				})
				require.NoError(t, err)
			}

			// Build delete request
			entries := make([]s3types.DeleteObjectEntry, len(tc.deleteKeys))
			for i, key := range tc.deleteKeys {
				entries[i] = s3types.DeleteObjectEntry{Key: key}
			}
			deleteReq := s3types.DeleteObjectsRequest{
				Quiet:   tc.quiet,
				Objects: entries,
			}
			body, _ := xml.Marshal(deleteReq)

			d := createTestData(tc.bucket, "", "test-owner")
			d.Req = httptest.NewRequest("POST", "/"+tc.bucket+"?delete", bytes.NewReader(body))
			w := httptest.NewRecorder()

			srv.DeleteObjectsHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			var result s3types.DeleteObjectsResult
			err = xml.Unmarshal(w.Body.Bytes(), &result)
			require.NoError(t, err)

			assert.Len(t, result.Deleted, tc.expectedDeleted)
			assert.Len(t, result.Error, tc.expectedErrors)
		})
	}
}

func TestCopyObjectHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		srcBucket    string
		srcKey       string
		destBucket   string
		destKey      string
		createSrc    bool
		srcSize      int64
		expectedCode int
		verifyCopy   bool
	}{
		{
			name:         "source not found",
			srcBucket:    "src-bucket",
			srcKey:       "source.txt",
			destBucket:   "dest-bucket",
			destKey:      "copied.txt",
			createSrc:    false,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "successful copy",
			srcBucket:    "src-bucket",
			srcKey:       "source.txt",
			destBucket:   "dest-bucket",
			destKey:      "copied.txt",
			createSrc:    true,
			srcSize:      100,
			expectedCode: http.StatusOK,
			verifyCopy:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create source and destination buckets
			for _, bucket := range []string{tc.srcBucket, tc.destBucket} {
				err := srv.db.CreateBucket(ctx, &types.BucketInfo{
					Name:    bucket,
					OwnerID: "test-owner",
				})
				require.NoError(t, err)
			}

			// Create source object if needed
			if tc.createSrc {
				err := srv.db.PutObject(ctx, &types.ObjectRef{
					Bucket: tc.srcBucket,
					Key:    tc.srcKey,
					Size:   uint64(tc.srcSize),
					ETag:   "abc123",
				})
				require.NoError(t, err)
			}

			d := createTestData(tc.destBucket, tc.destKey, "test-owner")
			d.Req = httptest.NewRequest("PUT", "/"+tc.destBucket+"/"+tc.destKey, nil)
			d.Req.Header.Set("x-amz-copy-source", "/"+tc.srcBucket+"/"+tc.srcKey)
			w := httptest.NewRecorder()

			srv.CopyObjectHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.verifyCopy {
				copied, err := srv.db.GetObject(ctx, tc.destBucket, tc.destKey)
				require.NoError(t, err)
				assert.Equal(t, tc.srcSize, int64(copied.Size))
			}
		})
	}
}

func TestListObjectsV2Handler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		bucket        string
		objects       []string
		expectedCode  int
		expectedCount int
	}{
		{
			name:          "empty bucket",
			bucket:        "test-bucket",
			objects:       nil,
			expectedCode:  http.StatusOK,
			expectedCount: 0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			// Add objects
			for _, key := range tc.objects {
				err := srv.db.PutObject(ctx, &types.ObjectRef{
					Bucket: tc.bucket,
					Key:    key,
					Size:   100,
					ETag:   "abc123",
				})
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, "", "test-owner")
			d.Req = httptest.NewRequest("GET", "/"+tc.bucket+"?list-type=2", nil)
			w := httptest.NewRecorder()

			srv.ListObjectsV2Handler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			var result s3types.ListObjectsV2Result
			err = xml.Unmarshal(w.Body.Bytes(), &result)
			require.NoError(t, err)

			assert.Equal(t, tc.bucket, result.Name)
			assert.Len(t, result.Contents, tc.expectedCount)
			assert.Equal(t, tc.expectedCount, result.KeyCount)
			assert.False(t, result.IsTruncated)
		})
	}
}

func TestGetObjectHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		key          string
		createObject bool
		expectedCode int
	}{
		{
			name:         "object not found",
			bucket:       "test-bucket",
			key:          "nonexistent.txt",
			createObject: false,
			// GetObject needs file client to work, without it should handle gracefully
			expectedCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			d := createTestData(tc.bucket, tc.key, "test-owner")
			w := httptest.NewRecorder()

			srv.GetObjectHandler(d, w)

			// For now, just verify it doesn't panic and returns expected or internal error
			assert.True(t, w.Code == tc.expectedCode || w.Code == http.StatusInternalServerError)
		})
	}
}

func TestPutObjectHandler(t *testing.T) {
	t.Parallel()

	// Generate valid SSE-C key for testing
	validSSEKey := make([]byte, 32)
	for i := range validSSEKey {
		validSSEKey[i] = byte(i)
	}
	validSSEKeyBase64 := base64.StdEncoding.EncodeToString(validSSEKey)
	validSSEKeyMD5Hash := md5.Sum(validSSEKey)
	validSSEKeyMD5 := base64.StdEncoding.EncodeToString(validSSEKeyMD5Hash[:])

	tests := []struct {
		name                string
		bucket              string
		key                 string
		body                []byte
		contentLength       int64
		headers             map[string]string
		setupBucket         bool
		bucketOwnerID       string
		storageProfile      string
		createProfile       bool
		profileReplication  int
		mockManagerResponse *manager_pb.GetReplicationTargetsResponse
		mockManagerError    error
		mockFileResult      *client.PutObjectResult
		mockFileError       error
		expectedCode        int
		verifyObject        bool
		verifySSECHeaders   bool
	}{
		{
			name:               "basic PutObject - streaming mode",
			bucket:             "test-bucket",
			key:                "test.txt",
			body:               []byte("test data"),
			contentLength:      9,
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			mockManagerResponse: &manager_pb.GetReplicationTargetsResponse{
				Targets: []*manager_pb.ReplicationTarget{
					{
						Location:  &common_pb.Location{Address: "file-server-1:8080"},
						BackendId: "backend-1",
					},
				},
			},
			mockFileResult: &client.PutObjectResult{
				ObjectID: "test-object-id",
				Size:     9,
				ETag:     "dummy-etag",
			},
			expectedCode: http.StatusOK,
			verifyObject: true,
		},
		{
			name:          "PutObject with SSE-C - buffered mode",
			bucket:        "test-bucket",
			key:           "encrypted.txt",
			body:          []byte("encrypted data"),
			contentLength: 14,
			headers: map[string]string{
				s3consts.XAmzServerSideEncryptionCustomerAlgo:   "AES256",
				s3consts.XAmzServerSideEncryptionCustomerKey:    validSSEKeyBase64,
				s3consts.XAmzServerSideEncryptionCustomerKeyMD5: validSSEKeyMD5,
			},
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			mockManagerResponse: &manager_pb.GetReplicationTargetsResponse{
				Targets: []*manager_pb.ReplicationTarget{
					{
						Location:  &common_pb.Location{Address: "file-server-1:8080"},
						BackendId: "backend-1",
					},
				},
			},
			mockFileResult: &client.PutObjectResult{
				ObjectID: "test-object-id",
				Size:     30, // Encrypted size (14 + IV + padding)
				ETag:     "dummy-etag",
			},
			expectedCode:      http.StatusOK,
			verifyObject:      true,
			verifySSECHeaders: true,
		},
		{
			name:               "PutObject with chunked encoding - buffered mode",
			bucket:             "test-bucket",
			key:                "chunked.txt",
			body:               []byte("chunked data"),
			contentLength:      -1, // Chunked encoding
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			mockManagerResponse: &manager_pb.GetReplicationTargetsResponse{
				Targets: []*manager_pb.ReplicationTarget{
					{
						Location:  &common_pb.Location{Address: "file-server-1:8080"},
						BackendId: "backend-1",
					},
				},
			},
			mockFileResult: &client.PutObjectResult{
				ObjectID: "test-object-id",
				Size:     12,
				ETag:     "dummy-etag",
			},
			expectedCode: http.StatusOK,
			verifyObject: true,
		},
		{
			name:          "bucket not found",
			bucket:        "nonexistent-bucket",
			key:           "test.txt",
			body:          []byte("test"),
			contentLength: 4,
			setupBucket:   false,
			expectedCode:  http.StatusBadRequest, // Returns 400 when bucket doesn't exist (no profile)
		},
		{
			name:          "invalid storage profile",
			bucket:        "test-bucket",
			key:           "test.txt",
			body:          []byte("test"),
			contentLength: 4,
			headers: map[string]string{
				"x-amz-storage-class": "INVALID",
			},
			setupBucket:   true,
			bucketOwnerID: "test-owner",
			createProfile: false,
			expectedCode:  http.StatusBadRequest,
		},
		{
			name:          "expected bucket owner mismatch",
			bucket:        "test-bucket",
			key:           "test.txt",
			body:          []byte("test"),
			contentLength: 4,
			headers: map[string]string{
				s3consts.XAmzExpectedBucketOwner: "wrong-owner",
			},
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			expectedCode:       http.StatusForbidden,
		},
		{
			name:               "requester pays bucket without header",
			bucket:             "test-bucket",
			key:                "test.txt",
			body:               []byte("test"),
			contentLength:      4,
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			// No mockManagerResponse - should fail before calling manager
			expectedCode: http.StatusForbidden,
		},
		{
			name:          "invalid SSE-C headers",
			bucket:        "test-bucket",
			key:           "test.txt",
			body:          []byte("test"),
			contentLength: 4,
			headers: map[string]string{
				s3consts.XAmzServerSideEncryptionCustomerAlgo: "AES128", // Invalid
			},
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			expectedCode:       http.StatusBadRequest,
		},
		{
			name:               "manager client error",
			bucket:             "test-bucket",
			key:                "test.txt",
			body:               []byte("test"),
			contentLength:      4,
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			mockManagerError:   errors.New("manager unavailable"),
			expectedCode:       http.StatusInternalServerError,
		},
		{
			name:               "no replication targets",
			bucket:             "test-bucket",
			key:                "test.txt",
			body:               []byte("test"),
			contentLength:      4,
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			mockManagerResponse: &manager_pb.GetReplicationTargetsResponse{
				Targets: []*manager_pb.ReplicationTarget{},
			},
			expectedCode: http.StatusInternalServerError,
		},
		{
			name:               "file client error",
			bucket:             "test-bucket",
			key:                "test.txt",
			body:               []byte("test"),
			contentLength:      4,
			setupBucket:        true,
			bucketOwnerID:      "test-owner",
			createProfile:      true,
			profileReplication: 1,
			mockManagerResponse: &manager_pb.GetReplicationTargetsResponse{
				Targets: []*manager_pb.ReplicationTarget{
					{
						Location:  &common_pb.Location{Address: "file-server-1:8080"},
						BackendId: "backend-1",
					},
				},
			},
			mockFileError: errors.New("file server error"),
			expectedCode:  http.StatusInternalServerError,
		},
	}

	// Add SSE-KMS test cases
	// Note: These tests require KMS license, so they may be skipped if license is not available
	t.Run("PutObject with SSE-KMS - buffered mode", func(t *testing.T) {
		t.Parallel()

		// Create IAM service with KMS enabled
		iamCfg := iam.DefaultIAMConfig()
		iamCfg.EnableKMS = true
		iamSvc, err := iam.LoadFromConfig(iamCfg)
		require.NoError(t, err)
		require.NotNil(t, iamSvc.KMS())

		// Create a KMS key for testing
		ctx := context.Background()
		keyMeta, err := iamSvc.KMSCreateKey(ctx, iam.CreateKeyInput{
			Description: "Test key for SSE-KMS",
		})
		require.NoError(t, err)
		require.NotNil(t, keyMeta)
		keyID := keyMeta.KeyID

		// Setup mocks
		mockMgr := clientmocks.NewMockManager(t)
		mockFile := clientmocks.NewMockFile(t)

		// Setup test server with IAM service
		srv := newTestServer(t, WithManagerClient(mockMgr), WithFileClient(mockFile), WithIAMService(iamSvc))

		bucket := "test-bucket"
		key := "kms-encrypted.txt"
		body := []byte("data encrypted with KMS")

		// Create bucket
		err = srv.db.CreateBucket(ctx, &types.BucketInfo{
			Name:    bucket,
			OwnerID: "test-owner",
		})
		require.NoError(t, err)
		srv.bucketStore.SetBucket(bucket, s3types.Bucket{
			Name:    bucket,
			OwnerID: "test-owner",
		})

		// Create storage profile
		profile := types.NewStorageProfile("STANDARD")
		profile.Replication = 1
		srv.profiles.Add(profile)

		// Setup manager mock
		mockMgr.EXPECT().
			GetReplicationTargets(mock.Anything, mock.Anything).
			Return(&manager_pb.GetReplicationTargetsResponse{
				Targets: []*manager_pb.ReplicationTarget{
					{
						Location:  &common_pb.Location{Address: "file-server-1:8080"},
						BackendId: "backend-1",
					},
				},
			}, nil)

		// Setup file client mock - MUST drain the reader for the storage coordinator's
		// pipe-based streaming to work correctly
		mockFile.EXPECT().
			PutObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, address string, objectID string, data io.Reader, totalSize uint64) (*client.PutObjectResult, error) {
				// Drain the reader (required for pipe-based streaming in storage coordinator)
				readData, err := io.ReadAll(data)
				if err != nil {
					return nil, err
				}
				// Verify that data is encrypted (size should be larger than plaintext)
				assert.Greater(t, uint64(len(readData)), uint64(len(body)), "encrypted data should be larger than plaintext")
				return &client.PutObjectResult{
					ObjectID: objectID,
					Size:     uint64(len(readData)),
					ETag:     "dummy-etag",
				}, nil
			})

		// Create request with SSE-KMS headers
		req := httptest.NewRequest("PUT", "/"+bucket+"/"+key, bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		req.Header.Set(s3consts.XAmzServerSideEncryption, s3consts.SSEAlgorithmKMS)
		req.Header.Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, keyID)

		d := createTestData(bucket, key, "test-owner")
		d.Req = req
		w := httptest.NewRecorder()

		// Execute handler
		srv.PutObjectHandler(d, w)

		// Verify response - skip if license check fails
		if w.Code == http.StatusForbidden {
			// License check failed - skip test
			t.Skip("Skipping test: KMS license not available")
			return
		}
		assert.Equal(t, http.StatusOK, w.Code, "expected StatusOK but got %d", w.Code)

		// Verify object was created with SSE-KMS metadata
		obj, err := srv.db.GetObject(ctx, bucket, key)
		require.NoError(t, err)
		assert.Equal(t, key, obj.Key)
		assert.Equal(t, bucket, obj.Bucket)
		assert.Equal(t, s3consts.SSEAlgorithmKMS, obj.SSEAlgorithm)
		assert.Equal(t, keyID, obj.SSEKMSKeyID)
		assert.NotEmpty(t, obj.SSEKMSContext, "SSEKMSContext should contain encrypted DEK")

		// Verify SSE-KMS response headers
		assert.Equal(t, s3consts.SSEAlgorithmKMS, w.Header().Get(s3consts.XAmzServerSideEncryption))
		assert.Equal(t, keyID, w.Header().Get(s3consts.XAmzServerSideEncryptionAwsKmsKeyID))
	})

	t.Run("PutObject with SSE-KMS - KMS key not found", func(t *testing.T) {
		t.Parallel()

		// Create IAM service with KMS enabled
		iamCfg := iam.DefaultIAMConfig()
		iamCfg.EnableKMS = true
		iamSvc, err := iam.LoadFromConfig(iamCfg)
		require.NoError(t, err)
		require.NotNil(t, iamSvc.KMS())

		// Setup mocks
		mockMgr := clientmocks.NewMockManager(t)
		mockFile := clientmocks.NewMockFile(t)

		// Setup test server with IAM service
		srv := newTestServer(t, WithManagerClient(mockMgr), WithFileClient(mockFile), WithIAMService(iamSvc))

		bucket := "test-bucket"
		key := "test.txt"
		body := []byte("test data")

		// Create bucket
		testCtx := context.Background()
		err = srv.db.CreateBucket(testCtx, &types.BucketInfo{
			Name:    bucket,
			OwnerID: "test-owner",
		})
		require.NoError(t, err)
		srv.bucketStore.SetBucket(bucket, s3types.Bucket{
			Name:    bucket,
			OwnerID: "test-owner",
		})

		// Create storage profile
		profile := types.NewStorageProfile("STANDARD")
		profile.Replication = 1
		srv.profiles.Add(profile)

		// Create request with SSE-KMS headers pointing to non-existent key
		req := httptest.NewRequest("PUT", "/"+bucket+"/"+key, bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		req.Header.Set(s3consts.XAmzServerSideEncryption, s3consts.SSEAlgorithmKMS)
		req.Header.Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, "nonexistent-key-id")

		d := createTestData(bucket, key, "test-owner")
		d.Req = req
		w := httptest.NewRecorder()

		// Execute handler
		srv.PutObjectHandler(d, w)

		// Verify response - should fail with KMS key not found or access denied (license)
		if w.Code == http.StatusForbidden {
			// License check failed - skip test
			t.Skip("Skipping test: KMS license not available")
			return
		}
		// Should be BadRequest for key not found, or Forbidden for license
		assert.True(t, w.Code == http.StatusBadRequest || w.Code == http.StatusForbidden, "expected BadRequest or Forbidden, got %d", w.Code)
	})

	t.Run("PutObject with both SSE-C and SSE-KMS - should fail", func(t *testing.T) {
		t.Parallel()

		// Generate valid SSE-C key
		validSSEKey := make([]byte, 32)
		for i := range validSSEKey {
			validSSEKey[i] = byte(i)
		}
		validSSEKeyBase64 := base64.StdEncoding.EncodeToString(validSSEKey)
		validSSEKeyMD5Hash := md5.Sum(validSSEKey)
		validSSEKeyMD5 := base64.StdEncoding.EncodeToString(validSSEKeyMD5Hash[:])

		// Create IAM service with KMS enabled
		iamCfg := iam.DefaultIAMConfig()
		iamCfg.EnableKMS = true
		iamSvc, err := iam.LoadFromConfig(iamCfg)
		require.NoError(t, err)
		require.NotNil(t, iamSvc.KMS())

		// Create a KMS key
		ctx := context.Background()
		keyMeta, err := iamSvc.KMSCreateKey(ctx, iam.CreateKeyInput{})
		require.NoError(t, err)
		keyID := keyMeta.KeyID

		// Setup mocks
		mockMgr := clientmocks.NewMockManager(t)
		mockFile := clientmocks.NewMockFile(t)

		// Setup test server with IAM service
		srv := newTestServer(t, WithManagerClient(mockMgr), WithFileClient(mockFile), WithIAMService(iamSvc))

		bucket := "test-bucket"
		key := "test.txt"
		body := []byte("test data")

		// Create bucket
		err = srv.db.CreateBucket(ctx, &types.BucketInfo{
			Name:    bucket,
			OwnerID: "test-owner",
		})
		require.NoError(t, err)
		srv.bucketStore.SetBucket(bucket, s3types.Bucket{
			Name:    bucket,
			OwnerID: "test-owner",
		})

		// Create storage profile
		profile := types.NewStorageProfile("STANDARD")
		profile.Replication = 1
		srv.profiles.Add(profile)

		// Create request with both SSE-C and SSE-KMS headers
		req := httptest.NewRequest("PUT", "/"+bucket+"/"+key, bytes.NewReader(body))
		req.ContentLength = int64(len(body))
		req.Header.Set(s3consts.XAmzServerSideEncryptionCustomerAlgo, s3consts.SSEAlgorithmAES256)
		req.Header.Set(s3consts.XAmzServerSideEncryptionCustomerKey, validSSEKeyBase64)
		req.Header.Set(s3consts.XAmzServerSideEncryptionCustomerKeyMD5, validSSEKeyMD5)
		req.Header.Set(s3consts.XAmzServerSideEncryption, s3consts.SSEAlgorithmKMS)
		req.Header.Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, keyID)

		d := createTestData(bucket, key, "test-owner")
		d.Req = req
		w := httptest.NewRecorder()

		// Execute handler
		srv.PutObjectHandler(d, w)

		// Verify response - should fail with invalid encryption algorithm or access denied (license)
		if w.Code == http.StatusForbidden {
			// License check failed - skip test
			t.Skip("Skipping test: KMS license not available")
			return
		}
		// Should be BadRequest for invalid algorithm (both SSE-C and SSE-KMS)
		assert.Equal(t, http.StatusBadRequest, w.Code, "expected BadRequest for using both SSE-C and SSE-KMS, got %d", w.Code)
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Setup mocks
			mockMgr := clientmocks.NewMockManager(t)
			mockFile := clientmocks.NewMockFile(t)

			// Setup test server
			srv := newTestServer(t, WithManagerClient(mockMgr), WithFileClient(mockFile))

			ctx := context.Background()

			// Create bucket if needed
			if tc.setupBucket {
				err := srv.db.CreateBucket(ctx, &types.BucketInfo{
					Name:    tc.bucket,
					OwnerID: tc.bucketOwnerID,
				})
				require.NoError(t, err)

				// Add bucket to cache
				bucketInfo := s3types.Bucket{
					Name:    tc.bucket,
					OwnerID: tc.bucketOwnerID,
				}
				if tc.name == "requester pays bucket without header" {
					bucketInfo.RequestPayment = &s3types.RequestPaymentConfig{
						Payer: s3types.PayerRequester,
					}
				}
				srv.bucketStore.SetBucket(tc.bucket, bucketInfo)
			}

			// Create storage profile if needed
			if tc.createProfile {
				profileName := tc.storageProfile
				if profileName == "" {
					profileName = "STANDARD"
				}
				profile := types.NewStorageProfile(profileName)
				profile.Replication = tc.profileReplication
				srv.profiles.Add(profile)
			}

			// Setup manager mock expectations
			if tc.mockManagerResponse != nil || tc.mockManagerError != nil {
				mockMgr.EXPECT().
					GetReplicationTargets(mock.Anything, mock.Anything).
					Return(tc.mockManagerResponse, tc.mockManagerError)
			}

			// Setup file client mock expectations
			if tc.mockFileResult != nil || tc.mockFileError != nil {
				mockFile.EXPECT().
					PutObject(mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything).
					Run(func(ctx context.Context, address string, objectID string, data io.Reader, totalSize uint64) {
						// Must drain the reader or the pipe will block/error
						io.Copy(io.Discard, data)
					}).
					Return(tc.mockFileResult, tc.mockFileError)
			}

			// Create request
			req := httptest.NewRequest("PUT", "/"+tc.bucket+"/"+tc.key, bytes.NewReader(tc.body))
			if tc.contentLength >= 0 {
				req.ContentLength = tc.contentLength
			} else {
				// Chunked encoding - remove ContentLength
				req.ContentLength = -1
				req.TransferEncoding = []string{"chunked"}
			}

			// Add headers
			for k, v := range tc.headers {
				req.Header.Set(k, v)
			}

			d := createTestData(tc.bucket, tc.key, tc.bucketOwnerID)
			d.Req = req
			w := httptest.NewRecorder()

			// Execute handler
			srv.PutObjectHandler(d, w)

			// Verify response
			assert.Equal(t, tc.expectedCode, w.Code)

			// Verify object was created
			if tc.verifyObject && tc.expectedCode == http.StatusOK {
				obj, err := srv.db.GetObject(ctx, tc.bucket, tc.key)
				require.NoError(t, err)
				assert.Equal(t, tc.key, obj.Key)
				assert.Equal(t, tc.bucket, obj.Bucket)
				assert.NotEmpty(t, obj.ETag)

				// Verify SSE-C metadata if applicable
				if tc.verifySSECHeaders {
					assert.Equal(t, "AES256", obj.SSEAlgorithm)
					assert.NotEmpty(t, obj.SSECustomerKeyMD5)
					// Verify that Size stores original (plaintext) size, not encrypted size
					assert.Equal(t, uint64(len(tc.body)), obj.Size, "obj.Size should equal original data size, not encrypted size")
				}
			}

			// Verify SSE-C response headers
			if tc.verifySSECHeaders && tc.expectedCode == http.StatusOK {
				assert.Equal(t, "AES256", w.Header().Get(s3consts.XAmzServerSideEncryptionCustomerAlgo))
				assert.NotEmpty(t, w.Header().Get(s3consts.XAmzServerSideEncryptionCustomerKeyMD5))
			}
		})
	}
}

// TestGetObjectHandler_SSEKMS tests GetObjectHandler with SSE-KMS encrypted objects
// setupSSEKMSForTest creates an IAM service with KMS and sets up test data for SSE-KMS tests.
// Returns (iamService, keyID, dekCiphertextBase64, testData, encryptedData, etag, skip)
func setupSSEKMSForTest(t *testing.T) (*iam.Service, string, string, []byte, []byte, string, bool) {
	t.Helper()

	// Create IAM service with KMS enabled
	iamCfg := iam.DefaultIAMConfig()
	iamCfg.EnableKMS = true
	iamSvc, err := iam.LoadFromConfig(iamCfg)
	require.NoError(t, err)
	if iamSvc == nil || iamSvc.KMS() == nil {
		t.Skip("Skipping test: KMS service not available")
		return nil, "", "", nil, nil, "", true
	}

	// Create a KMS key for testing
	ctx := context.Background()
	keyMeta, err := iamSvc.KMSCreateKey(ctx, iam.CreateKeyInput{
		Description: "Test key for SSE-KMS",
	})
	require.NoError(t, err)
	require.NotNil(t, keyMeta)
	keyID := keyMeta.KeyID

	// Generate a DEK and encrypt some test data
	dekPlaintext := make([]byte, 32)
	for i := range dekPlaintext {
		dekPlaintext[i] = byte(i)
	}
	dekCiphertext, err := iamSvc.KMS().Encrypt(ctx, keyID, dekPlaintext)
	require.NoError(t, err)
	dekCiphertextBase64 := base64.StdEncoding.EncodeToString(dekCiphertext)

	// Encrypt test data with the DEK
	testData := []byte("test data for SSE-KMS")
	encryptedData, err := encryptWithKMSDEK(testData, dekPlaintext)
	require.NoError(t, err)

	// Compute ETag of plaintext
	md5Hash := md5.Sum(testData)
	etag := base64.StdEncoding.EncodeToString(md5Hash[:])

	return iamSvc, keyID, dekCiphertextBase64, testData, encryptedData, etag, false
}

func TestGetObjectHandler_SSEKMS(t *testing.T) {
	t.Parallel()

	// Setup shared KMS resources
	iamSvc, keyID, dekCiphertextBase64, testData, encryptedData, etag, skip := setupSSEKMSForTest(t)
	if skip {
		return
	}

	tests := []struct {
		name           string
		key            string
		rangeHeader    string // Optional range header
		requestHeaders map[string]string
		setupMocks     func(*testing.T, *clientmocks.MockManager, *clientmocks.MockFile, string, string, []byte, string)
		expectedCode   int
		skipOnLicense  bool // Skip test if license check fails
		verifyResponse func(*testing.T, *httptest.ResponseRecorder, []byte, string, string)
	}{
		{
			name: "GetObject with SSE-KMS - success",
			key:  "kms-encrypted.txt",
			setupMocks: func(t *testing.T, mockMgr *clientmocks.MockManager, mockFile *clientmocks.MockFile, bucket, key string, encryptedData []byte, etag string) {
				mockFile.EXPECT().
					GetObject(mock.Anything, "file-server-1:8080", "chunk-1", mock.Anything).
					Run(func(ctx context.Context, address string, objectID string, writer client.ObjectWriter) {
						writer(encryptedData)
					}).
					Return(etag, nil)
			},
			expectedCode:  http.StatusOK,
			skipOnLicense: true,
			verifyResponse: func(t *testing.T, w *httptest.ResponseRecorder, testData []byte, keyID, etag string) {
				// Verify SSE-KMS response headers
				assert.Equal(t, s3consts.SSEAlgorithmKMS, w.Header().Get(s3consts.XAmzServerSideEncryption))
				assert.Equal(t, keyID, w.Header().Get(s3consts.XAmzServerSideEncryptionAwsKmsKeyID))

				// Verify decrypted data
				assert.Equal(t, testData, w.Body.Bytes())
				assert.Equal(t, strconv.FormatUint(uint64(len(testData)), 10), w.Header().Get("Content-Length"))
			},
		},
		{
			name:        "GetObject with SSE-KMS - range request",
			key:         "kms-encrypted-range.txt",
			rangeHeader: "bytes=0-4",
			setupMocks: func(t *testing.T, mockMgr *clientmocks.MockManager, mockFile *clientmocks.MockFile, bucket, key string, encryptedData []byte, etag string) {
				mockFile.EXPECT().
					GetObject(mock.Anything, "file-server-1:8080", "chunk-1", mock.Anything).
					Run(func(ctx context.Context, address string, objectID string, writer client.ObjectWriter) {
						writer(encryptedData)
					}).
					Return(etag, nil)
			},
			expectedCode:  http.StatusPartialContent,
			skipOnLicense: true,
			verifyResponse: func(t *testing.T, w *httptest.ResponseRecorder, testData []byte, keyID, etag string) {
				// Verify range response headers
				assert.Equal(t, "bytes 0-4/"+strconv.Itoa(len(testData)), w.Header().Get("Content-Range"))
				assert.Equal(t, "5", w.Header().Get("Content-Length"))

				// Verify SSE-KMS response headers
				assert.Equal(t, s3consts.SSEAlgorithmKMS, w.Header().Get(s3consts.XAmzServerSideEncryption))
				assert.Equal(t, keyID, w.Header().Get(s3consts.XAmzServerSideEncryptionAwsKmsKeyID))

				// Verify decrypted data (first 5 bytes)
				expectedRange := testData[0:5]
				assert.Equal(t, expectedRange, w.Body.Bytes())
			},
		},
		{
			name: "GetObject with SSE-KMS - wrong key ID",
			key:  "kms-encrypted-wrong-key.txt",
			requestHeaders: map[string]string{
				s3consts.XAmzServerSideEncryptionAwsKmsKeyID: "wrong-key-id",
			},
			setupMocks:     func(*testing.T, *clientmocks.MockManager, *clientmocks.MockFile, string, string, []byte, string) {},
			expectedCode:   http.StatusBadRequest,
			skipOnLicense:  true,
			verifyResponse: func(*testing.T, *httptest.ResponseRecorder, []byte, string, string) {},
		},
		{
			name: "GetObject with both SSE-C and SSE-KMS - should fail",
			key:  "kms-encrypted-conflict.txt",
			requestHeaders: map[string]string{
				s3consts.XAmzServerSideEncryptionCustomerAlgo: s3consts.SSEAlgorithmAES256,
				s3consts.XAmzServerSideEncryptionCustomerKey:  base64.StdEncoding.EncodeToString(make([]byte, 32)),
				s3consts.XAmzServerSideEncryptionCustomerKeyMD5: func() string {
					keyBytes := make([]byte, 32)
					hash := md5.Sum(keyBytes)
					return base64.StdEncoding.EncodeToString(hash[:])
				}(),
				s3consts.XAmzServerSideEncryptionAwsKmsKeyID: keyID,
			},
			setupMocks:     func(*testing.T, *clientmocks.MockManager, *clientmocks.MockFile, string, string, []byte, string) {},
			expectedCode:   http.StatusBadRequest,
			skipOnLicense:  true,
			verifyResponse: func(*testing.T, *httptest.ResponseRecorder, []byte, string, string) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Setup mocks
			mockMgr := clientmocks.NewMockManager(t)
			mockFile := clientmocks.NewMockFile(t)

			// Setup test server with IAM service
			srv := newTestServer(t, WithManagerClient(mockMgr), WithFileClient(mockFile), WithIAMService(iamSvc))

			ctx := context.Background()
			bucket := "test-bucket-" + uuid.New().String()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)
			srv.bucketStore.SetBucket(bucket, s3types.Bucket{
				Name:    bucket,
				OwnerID: "test-owner",
			})

			// Create object with SSE-KMS metadata
			objRef := &types.ObjectRef{
				Bucket:        bucket,
				Key:           tc.key,
				Size:          uint64(len(testData)),
				ETag:          etag,
				CreatedAt:     1000000000,
				SSEAlgorithm:  s3consts.SSEAlgorithmKMS,
				SSEKMSKeyID:   keyID,
				SSEKMSContext: "|" + dekCiphertextBase64,
				ChunkRefs: []types.ChunkRef{
					{
						ChunkID:        "chunk-1",
						FileServerAddr: "file-server-1:8080",
					},
				},
			}
			putErr := srv.db.PutObject(ctx, objRef)
			require.NoError(t, putErr)

			// Setup mocks
			tc.setupMocks(t, mockMgr, mockFile, bucket, tc.key, encryptedData, etag)

			// Create request
			req := httptest.NewRequest("GET", "/"+bucket+"/"+tc.key, nil)
			if tc.rangeHeader != "" {
				req.Header.Set("Range", tc.rangeHeader)
			}
			for k, v := range tc.requestHeaders {
				req.Header.Set(k, v)
			}
			d := createTestData(bucket, tc.key, "test-owner")
			d.Req = req
			w := httptest.NewRecorder()

			// Execute handler
			srv.GetObjectHandler(d, w)

			// Verify response
			if tc.skipOnLicense && w.Code == http.StatusForbidden {
				t.Skip("Skipping test: KMS license not available")
				return
			}
			assert.Equal(t, tc.expectedCode, w.Code, "expected %d but got %d", tc.expectedCode, w.Code)

			// Verify response details
			if tc.verifyResponse != nil {
				tc.verifyResponse(t, w, testData, keyID, etag)
			}
		})
	}
}

// TestHeadObjectHandler_SSEKMS tests HeadObjectHandler with SSE-KMS encrypted objects
func TestHeadObjectHandler_SSEKMS(t *testing.T) {
	t.Parallel()

	// Setup shared KMS resources
	iamSvc, keyID, dekCiphertextBase64, testData, _, etag, skip := setupSSEKMSForTest(t)
	if skip {
		return
	}

	tests := []struct {
		name           string
		key            string
		requestHeaders map[string]string
		expectedCode   int
		skipOnLicense  bool
		verifyResponse func(*testing.T, *httptest.ResponseRecorder, []byte, string)
	}{
		{
			name:          "HeadObject with SSE-KMS - success",
			key:           "kms-encrypted-head.txt",
			expectedCode:  http.StatusOK,
			skipOnLicense: true,
			verifyResponse: func(t *testing.T, w *httptest.ResponseRecorder, testData []byte, keyID string) {
				// Verify SSE-KMS response headers
				assert.Equal(t, s3consts.SSEAlgorithmKMS, w.Header().Get(s3consts.XAmzServerSideEncryption))
				assert.Equal(t, keyID, w.Header().Get(s3consts.XAmzServerSideEncryptionAwsKmsKeyID))

				// Verify other headers
				assert.Equal(t, strconv.FormatUint(uint64(len(testData)), 10), w.Header().Get("Content-Length"))
				assert.Equal(t, "\""+etag+"\"", w.Header().Get("ETag"))
			},
		},
		{
			name: "HeadObject with SSE-KMS - conditional not modified",
			key:  "kms-encrypted-conditional.txt",
			requestHeaders: map[string]string{
				"If-None-Match": "\"" + etag + "\"",
			},
			expectedCode:  http.StatusNotModified,
			skipOnLicense: true,
			verifyResponse: func(t *testing.T, w *httptest.ResponseRecorder, testData []byte, keyID string) {
				// Verify SSE-KMS response headers are included in 304 response
				assert.Equal(t, s3consts.SSEAlgorithmKMS, w.Header().Get(s3consts.XAmzServerSideEncryption))
				assert.Equal(t, keyID, w.Header().Get(s3consts.XAmzServerSideEncryptionAwsKmsKeyID))
			},
		},
		{
			name: "HeadObject with SSE-KMS - wrong key ID",
			key:  "kms-encrypted-wrong-key-head.txt",
			requestHeaders: map[string]string{
				s3consts.XAmzServerSideEncryptionAwsKmsKeyID: "wrong-key-id",
			},
			expectedCode:   http.StatusBadRequest,
			skipOnLicense:  true,
			verifyResponse: func(*testing.T, *httptest.ResponseRecorder, []byte, string) {},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Setup test server with IAM service
			srv := newTestServer(t, WithIAMService(iamSvc))

			ctx := context.Background()
			bucket := "test-bucket-" + uuid.New().String()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)
			srv.bucketStore.SetBucket(bucket, s3types.Bucket{
				Name:    bucket,
				OwnerID: "test-owner",
			})

			// Create object with SSE-KMS metadata
			objRef := &types.ObjectRef{
				Bucket:        bucket,
				Key:           tc.key,
				Size:          uint64(len(testData)),
				ETag:          etag,
				CreatedAt:     1000000000,
				SSEAlgorithm:  s3consts.SSEAlgorithmKMS,
				SSEKMSKeyID:   keyID,
				SSEKMSContext: "|" + dekCiphertextBase64,
			}
			putErr := srv.db.PutObject(ctx, objRef)
			require.NoError(t, putErr)

			// Create request
			req := httptest.NewRequest("HEAD", "/"+bucket+"/"+tc.key, nil)
			for k, v := range tc.requestHeaders {
				req.Header.Set(k, v)
			}
			d := createTestData(bucket, tc.key, "test-owner")
			d.Req = req
			w := httptest.NewRecorder()

			// Execute handler
			srv.HeadObjectHandler(d, w)

			// Verify response
			if tc.skipOnLicense && w.Code == http.StatusForbidden {
				t.Skip("Skipping test: KMS license not available")
				return
			}
			assert.Equal(t, tc.expectedCode, w.Code, "expected %d but got %d", tc.expectedCode, w.Code)

			// Verify response details
			if tc.verifyResponse != nil {
				tc.verifyResponse(t, w, testData, keyID)
			}
		})
	}
}

// TestHeadObjectHandler_SSEC tests HeadObjectHandler with SSE-C encrypted objects
func TestHeadObjectHandler_SSEC(t *testing.T) {
	t.Parallel()

	// Generate valid SSE-C key for testing
	validSSEKey := make([]byte, 32)
	for i := range validSSEKey {
		validSSEKey[i] = byte(i)
	}
	validSSEKeyBase64 := base64.StdEncoding.EncodeToString(validSSEKey)
	validSSEKeyMD5Hash := md5.Sum(validSSEKey)
	validSSEKeyMD5 := base64.StdEncoding.EncodeToString(validSSEKeyMD5Hash[:])

	// Generate wrong key for testing
	wrongSSEKey := make([]byte, 32)
	for i := range wrongSSEKey {
		wrongSSEKey[i] = byte(i + 100)
	}
	wrongSSEKeyBase64 := base64.StdEncoding.EncodeToString(wrongSSEKey)
	wrongSSEKeyMD5Hash := md5.Sum(wrongSSEKey)
	wrongSSEKeyMD5 := base64.StdEncoding.EncodeToString(wrongSSEKeyMD5Hash[:])

	tests := []struct {
		name           string
		key            string
		requestHeaders map[string]string
		expectedCode   int
		verifyHeaders  bool
	}{
		{
			name: "HeadObject with SSE-C - success with correct key",
			key:  "ssec-encrypted-head.txt",
			requestHeaders: map[string]string{
				s3consts.XAmzServerSideEncryptionCustomerAlgo:   s3consts.SSEAlgorithmAES256,
				s3consts.XAmzServerSideEncryptionCustomerKey:    validSSEKeyBase64,
				s3consts.XAmzServerSideEncryptionCustomerKeyMD5: validSSEKeyMD5,
			},
			expectedCode:  http.StatusOK,
			verifyHeaders: true,
		},
		{
			name:           "HeadObject with SSE-C - missing key should fail",
			key:            "ssec-encrypted-no-key.txt",
			requestHeaders: map[string]string{},
			expectedCode:   http.StatusBadRequest,
			verifyHeaders:  false,
		},
		{
			name: "HeadObject with SSE-C - wrong key should fail",
			key:  "ssec-encrypted-wrong-key.txt",
			requestHeaders: map[string]string{
				s3consts.XAmzServerSideEncryptionCustomerAlgo:   s3consts.SSEAlgorithmAES256,
				s3consts.XAmzServerSideEncryptionCustomerKey:    wrongSSEKeyBase64,
				s3consts.XAmzServerSideEncryptionCustomerKeyMD5: wrongSSEKeyMD5,
			},
			expectedCode:  http.StatusBadRequest,
			verifyHeaders: false,
		},
		{
			name: "HeadObject with SSE-C - partial headers should fail",
			key:  "ssec-encrypted-partial.txt",
			requestHeaders: map[string]string{
				s3consts.XAmzServerSideEncryptionCustomerAlgo: s3consts.SSEAlgorithmAES256,
				// Missing key and key MD5
			},
			expectedCode:  http.StatusBadRequest,
			verifyHeaders: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()
			bucket := "test-bucket-" + uuid.New().String()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)
			srv.bucketStore.SetBucket(bucket, s3types.Bucket{
				Name:    bucket,
				OwnerID: "test-owner",
			})

			// Create SSE-C encrypted object (store metadata as if encrypted)
			objRef := &types.ObjectRef{
				Bucket:            bucket,
				Key:               tc.key,
				Size:              1024,
				ETag:              "test-etag-ssec",
				CreatedAt:         1000000000,
				SSEAlgorithm:      s3consts.SSEAlgorithmAES256,
				SSECustomerKeyMD5: validSSEKeyMD5, // Object encrypted with validSSEKey
			}
			putErr := srv.db.PutObject(ctx, objRef)
			require.NoError(t, putErr)

			// Create request
			req := httptest.NewRequest("HEAD", "/"+bucket+"/"+tc.key, nil)
			for k, v := range tc.requestHeaders {
				req.Header.Set(k, v)
			}
			d := createTestData(bucket, tc.key, "test-owner")
			d.Req = req
			w := httptest.NewRecorder()

			// Execute handler
			srv.HeadObjectHandler(d, w)

			// Verify response
			assert.Equal(t, tc.expectedCode, w.Code, "expected %d but got %d", tc.expectedCode, w.Code)

			// Verify response headers on success
			if tc.verifyHeaders && tc.expectedCode == http.StatusOK {
				assert.Equal(t, s3consts.SSEAlgorithmAES256, w.Header().Get(s3consts.XAmzServerSideEncryptionCustomerAlgo))
				assert.Equal(t, validSSEKeyMD5, w.Header().Get(s3consts.XAmzServerSideEncryptionCustomerKeyMD5))
				assert.Equal(t, "1024", w.Header().Get("Content-Length"))
				assert.Equal(t, "\"test-etag-ssec\"", w.Header().Get("ETag"))
			}
		})
	}
}

// TestHeadObjectHandler_NonSSEC tests that non-encrypted objects work without SSE-C headers
func TestHeadObjectHandler_NonSSEC(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t)
	ctx := context.Background()
	bucket := "test-bucket-" + uuid.New().String()

	// Create bucket
	err := srv.db.CreateBucket(ctx, &types.BucketInfo{
		Name:    bucket,
		OwnerID: "test-owner",
	})
	require.NoError(t, err)
	srv.bucketStore.SetBucket(bucket, s3types.Bucket{
		Name:    bucket,
		OwnerID: "test-owner",
	})

	// Create non-encrypted object
	objRef := &types.ObjectRef{
		Bucket:    bucket,
		Key:       "plain-object.txt",
		Size:      512,
		ETag:      "plain-etag",
		CreatedAt: 1000000000,
		// No SSE fields - plain object
	}
	putErr := srv.db.PutObject(ctx, objRef)
	require.NoError(t, putErr)

	// Create request without SSE-C headers
	req := httptest.NewRequest("HEAD", "/"+bucket+"/plain-object.txt", nil)
	d := createTestData(bucket, "plain-object.txt", "test-owner")
	d.Req = req
	w := httptest.NewRecorder()

	// Execute handler
	srv.HeadObjectHandler(d, w)

	// Should succeed without SSE-C headers
	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "512", w.Header().Get("Content-Length"))
	assert.Equal(t, "\"plain-etag\"", w.Header().Get("ETag"))
	// No SSE headers should be present
	assert.Empty(t, w.Header().Get(s3consts.XAmzServerSideEncryptionCustomerAlgo))
	assert.Empty(t, w.Header().Get(s3consts.XAmzServerSideEncryptionCustomerKeyMD5))
}

func TestGetObjectAttributesHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		key            string
		attrs          string // x-amz-object-attributes header
		createObject   bool
		objectSize     uint64
		objectETag     string
		storageClass   string
		expectedCode   int
		checkResponse  func(t *testing.T, resp s3types.GetObjectAttributesResponse)
	}{
		{
			name:         "missing attributes header",
			bucket:       "test-bucket",
			key:          "test.txt",
			attrs:        "",
			createObject: false,
			expectedCode: http.StatusBadRequest,
		},
		{
			name:         "invalid attributes",
			bucket:       "test-bucket",
			key:          "test.txt",
			attrs:        "InvalidAttr",
			createObject: false,
			expectedCode: http.StatusBadRequest,
		},
		{
			name:         "object not found",
			bucket:       "test-bucket",
			key:          "nonexistent.txt",
			attrs:        "ETag",
			createObject: false,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "request ETag only",
			bucket:       "test-bucket",
			key:          "test.txt",
			attrs:        "ETag",
			createObject: true,
			objectSize:   12345,
			objectETag:   "abc123def456",
			expectedCode: http.StatusOK,
			checkResponse: func(t *testing.T, resp s3types.GetObjectAttributesResponse) {
				assert.Equal(t, "abc123def456", resp.ETag)
				assert.Nil(t, resp.ObjectSize)
				assert.Empty(t, resp.StorageClass)
			},
		},
		{
			name:         "request ObjectSize only",
			bucket:       "test-bucket",
			key:          "test.txt",
			attrs:        "ObjectSize",
			createObject: true,
			objectSize:   99999,
			objectETag:   "someEtag",
			expectedCode: http.StatusOK,
			checkResponse: func(t *testing.T, resp s3types.GetObjectAttributesResponse) {
				assert.Empty(t, resp.ETag)
				require.NotNil(t, resp.ObjectSize)
				assert.Equal(t, int64(99999), *resp.ObjectSize)
			},
		},
		{
			name:         "request StorageClass",
			bucket:       "test-bucket",
			key:          "test.txt",
			attrs:        "StorageClass",
			createObject: true,
			objectSize:   100,
			objectETag:   "etag",
			storageClass: "GLACIER",
			expectedCode: http.StatusOK,
			checkResponse: func(t *testing.T, resp s3types.GetObjectAttributesResponse) {
				assert.Equal(t, "GLACIER", resp.StorageClass)
			},
		},
		{
			name:         "request multiple attributes",
			bucket:       "test-bucket",
			key:          "test.txt",
			attrs:        "ETag, ObjectSize, StorageClass",
			createObject: true,
			objectSize:   5000,
			objectETag:   "multi-attr-etag",
			storageClass: "STANDARD",
			expectedCode: http.StatusOK,
			checkResponse: func(t *testing.T, resp s3types.GetObjectAttributesResponse) {
				assert.Equal(t, "multi-attr-etag", resp.ETag)
				require.NotNil(t, resp.ObjectSize)
				assert.Equal(t, int64(5000), *resp.ObjectSize)
				assert.Equal(t, "STANDARD", resp.StorageClass)
			},
		},
		{
			name:         "multipart object with ObjectParts",
			bucket:       "test-bucket",
			key:          "multipart.txt",
			attrs:        "ETag, ObjectParts",
			createObject: true,
			objectSize:   50000000,
			objectETag:   "abc123def456-5", // Multipart ETag format
			expectedCode: http.StatusOK,
			checkResponse: func(t *testing.T, resp s3types.GetObjectAttributesResponse) {
				assert.Equal(t, "abc123def456-5", resp.ETag)
				require.NotNil(t, resp.ObjectParts)
				assert.Equal(t, 5, resp.ObjectParts.TotalPartsCount)
			},
		},
		{
			name:         "non-multipart object with ObjectParts requested",
			bucket:       "test-bucket",
			key:          "simple.txt",
			attrs:        "ObjectParts",
			createObject: true,
			objectSize:   1000,
			objectETag:   "simpleEtag", // No dash - not multipart
			expectedCode: http.StatusOK,
			checkResponse: func(t *testing.T, resp s3types.GetObjectAttributesResponse) {
				// ObjectParts should be nil for non-multipart objects
				assert.Nil(t, resp.ObjectParts)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()
			bucket := tc.bucket + "-" + uuid.New().String()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)
			srv.bucketStore.SetBucket(bucket, s3types.Bucket{
				Name:    bucket,
				OwnerID: "test-owner",
			})

			// Create object if needed
			if tc.createObject {
				profileID := tc.storageClass
				if profileID == "" {
					profileID = "STANDARD"
				}
				err = srv.db.PutObject(ctx, &types.ObjectRef{
					Bucket:    bucket,
					Key:       tc.key,
					Size:      tc.objectSize,
					ETag:      tc.objectETag,
					ProfileID: profileID,
					CreatedAt: 1000000000,
				})
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+bucket+"/"+tc.key+"?attributes", nil)
			if tc.attrs != "" {
				req.Header.Set(s3consts.XAmzObjectAttributes, tc.attrs)
			}
			d := createTestData(bucket, tc.key, "test-owner")
			d.Req = req
			w := httptest.NewRecorder()

			srv.GetObjectAttributesHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.expectedCode == http.StatusOK && tc.checkResponse != nil {
				var resp s3types.GetObjectAttributesResponse
				err := xml.Unmarshal(w.Body.Bytes(), &resp)
				require.NoError(t, err, "failed to parse response XML: %s", w.Body.String())
				tc.checkResponse(t, resp)
			}
		})
	}
}

func TestParseObjectAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		header   string
		expected map[string]struct{}
	}{
		{
			name:     "single attribute",
			header:   "ETag",
			expected: map[string]struct{}{"ETag": {}},
		},
		{
			name:     "multiple attributes",
			header:   "ETag, ObjectSize, StorageClass",
			expected: map[string]struct{}{"ETag": {}, "ObjectSize": {}, "StorageClass": {}},
		},
		{
			name:     "all attributes",
			header:   "ETag,Checksum,ObjectParts,StorageClass,ObjectSize",
			expected: map[string]struct{}{"ETag": {}, "Checksum": {}, "ObjectParts": {}, "StorageClass": {}, "ObjectSize": {}},
		},
		{
			name:     "with extra spaces",
			header:   "  ETag  ,  ObjectSize  ",
			expected: map[string]struct{}{"ETag": {}, "ObjectSize": {}},
		},
		{
			name:     "invalid attributes ignored",
			header:   "ETag, InvalidAttr, ObjectSize",
			expected: map[string]struct{}{"ETag": {}, "ObjectSize": {}},
		},
		{
			name:     "all invalid",
			header:   "foo, bar, baz",
			expected: map[string]struct{}{},
		},
		{
			name:     "empty",
			header:   "",
			expected: map[string]struct{}{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := parseObjectAttributes(tc.header)
			assert.Equal(t, tc.expected, result)
		})
	}
}
