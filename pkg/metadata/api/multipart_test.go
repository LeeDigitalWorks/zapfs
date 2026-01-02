// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

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

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateMultipartUploadHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		key            string
		contentType    string
		storageClass   string
		expectedStatus int
	}{
		{
			name:           "success - basic upload",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "success - with content type",
			bucket:         "test-bucket",
			key:            "image.png",
			contentType:    "image/png",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "success - with storage class",
			bucket:         "test-bucket",
			key:            "large-file.zip",
			storageClass:   "REDUCED_REDUNDANCY",
			expectedStatus: http.StatusOK,
		},
		{
			name:           "success - nested key",
			bucket:         "test-bucket",
			key:            "folder/subfolder/file.txt",
			expectedStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create request
			req := httptest.NewRequest("POST", "/"+tc.bucket+"/"+tc.key+"?uploads", nil)
			if tc.contentType != "" {
				req.Header.Set("Content-Type", tc.contentType)
			}
			if tc.storageClass != "" {
				req.Header.Set("x-amz-storage-class", tc.storageClass)
			}

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

			srv.CreateMultipartUploadHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				assert.Contains(t, w.Header().Get("Content-Type"), "application/xml")

				var result s3types.InitiateMultipartUploadResult
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)

				assert.Equal(t, tc.bucket, result.Bucket)
				assert.Equal(t, tc.key, result.Key)
				assert.NotEmpty(t, result.UploadID, "should return upload ID")
			}
		})
	}
}

func TestAbortMultipartUploadHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		key            string
		uploadID       string
		setupUpload    bool
		expectedStatus int
	}{
		{
			name:           "success - abort existing upload",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "test-upload-id",
			setupUpload:    true,
			expectedStatus: http.StatusNoContent,
		},
		{
			name:           "upload not found",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "nonexistent-upload",
			setupUpload:    false,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "missing upload ID",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "",
			setupUpload:    false,
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup upload if needed
			if tc.setupUpload {
				upload := &types.MultipartUpload{
					ID:           uuid.New(),
					UploadID:     tc.uploadID,
					Bucket:       tc.bucket,
					Key:          tc.key,
					OwnerID:      "test-owner",
					Initiated:    time.Now().UnixNano(),
					StorageClass: "STANDARD",
				}
				err := srv.db.CreateMultipartUpload(ctx, upload)
				require.NoError(t, err)
			}

			// Create request
			url := "/" + tc.bucket + "/" + tc.key
			if tc.uploadID != "" {
				url += "?uploadId=" + tc.uploadID
			}
			req := httptest.NewRequest("DELETE", url, nil)

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

			srv.AbortMultipartUploadHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			// Verify upload was deleted
			if tc.expectedStatus == http.StatusNoContent {
				_, err := srv.db.GetMultipartUpload(ctx, tc.bucket, tc.key, tc.uploadID)
				assert.Error(t, err, "upload should be deleted")
			}
		})
	}
}

func TestListPartsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		key            string
		uploadID       string
		setupUpload    bool
		setupParts     int // number of parts to create
		queryParams    map[string]string
		expectedStatus int
		expectedParts  int
	}{
		{
			name:           "success - list parts",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "test-upload-id",
			setupUpload:    true,
			setupParts:     3,
			expectedStatus: http.StatusOK,
			expectedParts:  3,
		},
		{
			name:           "success - empty parts",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "test-upload-id",
			setupUpload:    true,
			setupParts:     0,
			expectedStatus: http.StatusOK,
			expectedParts:  0,
		},
		{
			name:           "upload not found",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "nonexistent-upload",
			setupUpload:    false,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "missing upload ID",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "",
			setupUpload:    false,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:        "with max-parts limit",
			bucket:      "test-bucket",
			key:         "test-object.txt",
			uploadID:    "test-upload-id",
			setupUpload: true,
			setupParts:  5,
			queryParams: map[string]string{
				"max-parts": "2",
			},
			expectedStatus: http.StatusOK,
			expectedParts:  2, // memory DB correctly implements limit
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup upload if needed
			if tc.setupUpload {
				upload := &types.MultipartUpload{
					ID:           uuid.New(),
					UploadID:     tc.uploadID,
					Bucket:       tc.bucket,
					Key:          tc.key,
					OwnerID:      "test-owner",
					Initiated:    time.Now().UnixNano(),
					StorageClass: "STANDARD",
				}
				err := srv.db.CreateMultipartUpload(ctx, upload)
				require.NoError(t, err)

				// Create parts
				for i := 1; i <= tc.setupParts; i++ {
					part := &types.MultipartPart{
						ID:           uuid.New(),
						UploadID:     tc.uploadID,
						PartNumber:   i,
						Size:         int64(1024 * i),
						ETag:         "test-etag-" + string(rune('a'+i-1)),
						LastModified: time.Now().UnixNano(),
					}
					err := srv.db.PutPart(ctx, part)
					require.NoError(t, err)
				}
			}

			// Create request
			url := "/" + tc.bucket + "/" + tc.key
			if tc.uploadID != "" {
				url += "?uploadId=" + tc.uploadID
			}
			req := httptest.NewRequest("GET", url, nil)

			// Add query params
			if tc.queryParams != nil {
				q := req.URL.Query()
				for k, v := range tc.queryParams {
					q.Set(k, v)
				}
				req.URL.RawQuery = q.Encode()
			}

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

			srv.ListPartsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				var result s3types.ListPartsResult
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)

				assert.Equal(t, tc.bucket, result.Bucket)
				assert.Equal(t, tc.key, result.Key)
				assert.Equal(t, tc.uploadID, result.UploadID)
				assert.Len(t, result.Parts, tc.expectedParts)
			}
		})
	}
}

func TestListMultipartUploadsHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		bucket          string
		setupUploads    []*types.MultipartUpload
		queryParams     map[string]string
		expectedStatus  int
		expectedUploads int
	}{
		{
			name:            "empty list",
			bucket:          "test-bucket",
			setupUploads:    nil,
			expectedStatus:  http.StatusOK,
			expectedUploads: 0,
		},
		{
			name:   "list all uploads",
			bucket: "test-bucket",
			setupUploads: []*types.MultipartUpload{
				{ID: uuid.New(), UploadID: "upload-1", Bucket: "test-bucket", Key: "file1.txt", OwnerID: "test-owner", Initiated: time.Now().UnixNano()},
				{ID: uuid.New(), UploadID: "upload-2", Bucket: "test-bucket", Key: "file2.txt", OwnerID: "test-owner", Initiated: time.Now().UnixNano()},
			},
			expectedStatus:  http.StatusOK,
			expectedUploads: 2,
		},
		{
			name:   "only this bucket's uploads",
			bucket: "test-bucket",
			setupUploads: []*types.MultipartUpload{
				{ID: uuid.New(), UploadID: "upload-1", Bucket: "test-bucket", Key: "file1.txt", OwnerID: "test-owner", Initiated: time.Now().UnixNano()},
				{ID: uuid.New(), UploadID: "upload-2", Bucket: "other-bucket", Key: "file2.txt", OwnerID: "test-owner", Initiated: time.Now().UnixNano()},
			},
			expectedStatus:  http.StatusOK,
			expectedUploads: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup uploads
			for _, upload := range tc.setupUploads {
				err := srv.db.CreateMultipartUpload(ctx, upload)
				require.NoError(t, err)
			}

			// Create request
			req := httptest.NewRequest("GET", "/"+tc.bucket+"?uploads", nil)

			// Add query params
			if tc.queryParams != nil {
				q := req.URL.Query()
				for k, v := range tc.queryParams {
					q.Set(k, v)
				}
				req.URL.RawQuery = q.Encode()
			}

			d := &data.Data{
				Ctx: ctx,
				Req: req,
				S3Info: &data.S3Info{
					Bucket:  tc.bucket,
					OwnerID: "test-owner",
				},
			}
			w := httptest.NewRecorder()

			srv.ListMultipartUploadsHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)

			if tc.expectedStatus == http.StatusOK {
				var result s3types.ListMultipartUploadsResult
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)

				assert.Equal(t, tc.bucket, result.Bucket)
				assert.Len(t, result.Uploads, tc.expectedUploads)
			}
		})
	}
}

func TestCompleteMultipartUploadHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		bucket         string
		key            string
		uploadID       string
		setupUpload    bool
		setupParts     []int // part numbers to create
		requestParts   []s3types.CompletePart
		expectedStatus int
	}{
		{
			name:           "upload not found",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "nonexistent-upload",
			setupUpload:    false,
			expectedStatus: http.StatusNotFound,
		},
		{
			name:           "missing upload ID",
			bucket:         "test-bucket",
			key:            "test-object.txt",
			uploadID:       "",
			setupUpload:    false,
			expectedStatus: http.StatusBadRequest,
		},
		{
			name:        "invalid part - not uploaded",
			bucket:      "test-bucket",
			key:         "test-object.txt",
			uploadID:    "test-upload-id",
			setupUpload: true,
			setupParts:  []int{1, 2}, // Only parts 1 and 2 uploaded
			requestParts: []s3types.CompletePart{
				{PartNumber: 1, ETag: "etag-1"},
				{PartNumber: 3, ETag: "etag-3"}, // Part 3 not uploaded
			},
			expectedStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Setup upload if needed
			if tc.setupUpload {
				upload := &types.MultipartUpload{
					ID:           uuid.New(),
					UploadID:     tc.uploadID,
					Bucket:       tc.bucket,
					Key:          tc.key,
					OwnerID:      "test-owner",
					Initiated:    time.Now().UnixNano(),
					StorageClass: "STANDARD",
				}
				err := srv.db.CreateMultipartUpload(ctx, upload)
				require.NoError(t, err)

				// Create parts
				for _, partNum := range tc.setupParts {
					part := &types.MultipartPart{
						ID:           uuid.New(),
						UploadID:     tc.uploadID,
						PartNumber:   partNum,
						Size:         1024,
						ETag:         "etag-" + string(rune('0'+partNum)),
						LastModified: time.Now().UnixNano(),
					}
					err := srv.db.PutPart(ctx, part)
					require.NoError(t, err)
				}
			}

			// Build request body
			requestBody := s3types.CompleteMultipartUploadRequest{
				Parts: tc.requestParts,
			}
			bodyXML, _ := xml.Marshal(requestBody)

			// Create request
			url := "/" + tc.bucket + "/" + tc.key
			if tc.uploadID != "" {
				url += "?uploadId=" + tc.uploadID
			}
			req := httptest.NewRequest("POST", url, strings.NewReader(string(bodyXML)))
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

			srv.CompleteMultipartUploadHandler(d, w)

			assert.Equal(t, tc.expectedStatus, w.Code)
		})
	}
}
