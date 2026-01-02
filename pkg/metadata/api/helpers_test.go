// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"crypto/md5"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"

	"github.com/stretchr/testify/assert"
)

func TestParseRangeHeader(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rangeHeader  string
		objectSize   uint64
		expectValid  bool
		expectOffset uint64
		expectLength uint64
	}{
		{
			name:        "no range header",
			rangeHeader: "",
			objectSize:  1000,
			expectValid: false,
		},
		{
			name:        "invalid prefix",
			rangeHeader: "invalid=0-100",
			objectSize:  1000,
			expectValid: false,
		},
		{
			name:         "closed range - valid",
			rangeHeader:  "bytes=0-499",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 0,
			expectLength: 500,
		},
		{
			name:         "closed range - middle",
			rangeHeader:  "bytes=100-199",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 100,
			expectLength: 100,
		},
		{
			name:         "closed range - end",
			rangeHeader:  "bytes=900-999",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 900,
			expectLength: 100,
		},
		{
			name:         "closed range - exceeds size",
			rangeHeader:  "bytes=900-1500",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 900,
			expectLength: 100, // Capped at object size
		},
		{
			name:         "open-ended range - from start",
			rangeHeader:  "bytes=0-",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 0,
			expectLength: 1000,
		},
		{
			name:         "open-ended range - from middle",
			rangeHeader:  "bytes=500-",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 500,
			expectLength: 500,
		},
		{
			name:        "open-ended range - beyond size",
			rangeHeader: "bytes=1500-",
			objectSize:  1000,
			expectValid: false, // Range not satisfiable
		},
		{
			name:         "suffix range - valid",
			rangeHeader:  "bytes=-100",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 900,
			expectLength: 100,
		},
		{
			name:         "suffix range - larger than object",
			rangeHeader:  "bytes=-1500",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 0,
			expectLength: 1000, // Returns entire object
		},
		{
			name:         "suffix range - exact object size",
			rangeHeader:  "bytes=-1000",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 0,
			expectLength: 1000,
		},
		{
			name:        "invalid range - start > end",
			rangeHeader: "bytes=500-400",
			objectSize:  1000,
			expectValid: false,
		},
		{
			name:        "invalid range - negative start",
			rangeHeader: "bytes=-100-200",
			objectSize:  1000,
			expectValid: false,
		},
		{
			name:        "invalid range - malformed",
			rangeHeader: "bytes=abc-def",
			objectSize:  1000,
			expectValid: false,
		},
		{
			name:        "invalid range - missing parts",
			rangeHeader: "bytes=",
			objectSize:  1000,
			expectValid: false,
		},
		{
			name:        "invalid range - single value",
			rangeHeader: "bytes=100",
			objectSize:  1000,
			expectValid: false,
		},
		{
			name:        "empty object - valid range",
			rangeHeader: "bytes=0-0",
			objectSize:  0,
			expectValid: false, // Range not satisfiable for empty object
		},
		{
			name:         "single byte object - valid range",
			rangeHeader:  "bytes=0-0",
			objectSize:   1,
			expectValid:  true,
			expectOffset: 0,
			expectLength: 1,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			offset, length, valid := parseRangeHeader(tc.rangeHeader, tc.objectSize)

			assert.Equal(t, tc.expectValid, valid, "validity mismatch")
			if tc.expectValid {
				assert.Equal(t, tc.expectOffset, offset, "offset mismatch")
				assert.Equal(t, tc.expectLength, length, "length mismatch")
			}
		})
	}
}

func TestCheckConditionalHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		ifMatch           string
		ifNoneMatch       string
		ifModifiedSince   string
		ifUnmodifiedSince string
		objectETag        string
		objectModified    time.Time
		expectProceed     bool
		expectStatus      int
		expectErrCode     s3err.ErrorCode
	}{
		{
			name:           "no conditional headers",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  true,
		},
		{
			name:           "If-Match matches",
			ifMatch:        "abc123",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  true,
		},
		{
			name:           "If-Match with quotes matches",
			ifMatch:        "\"abc123\"",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  true,
		},
		{
			name:           "If-Match does not match",
			ifMatch:        "xyz789",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  false,
			expectStatus:   http.StatusPreconditionFailed,
			expectErrCode:  s3err.ErrPreconditionFailed,
		},
		{
			name:           "If-None-Match does not match (object exists)",
			ifNoneMatch:    "xyz789",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  true,
		},
		{
			name:           "If-None-Match matches (return 304)",
			ifNoneMatch:    "abc123",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  false,
			expectStatus:   http.StatusNotModified,
			expectErrCode:  s3err.ErrNotModified,
		},
		{
			name:            "If-Modified-Since - object not modified",
			ifModifiedSince: "Mon, 02 Jan 2024 00:00:00 GMT",
			objectETag:      "abc123",
			objectModified:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:   false,
			expectStatus:    http.StatusNotModified,
			expectErrCode:   s3err.ErrNotModified,
		},
		{
			name:            "If-Modified-Since - object modified",
			ifModifiedSince: "Mon, 01 Jan 2024 00:00:00 GMT",
			objectETag:      "abc123",
			objectModified:  time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			expectProceed:   true,
		},
		{
			name:              "If-Unmodified-Since - object not modified",
			ifUnmodifiedSince: "Mon, 02 Jan 2024 00:00:00 GMT",
			objectETag:        "abc123",
			objectModified:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:     true,
		},
		{
			name:              "If-Unmodified-Since - object modified",
			ifUnmodifiedSince: "Mon, 01 Jan 2024 00:00:00 GMT",
			objectETag:        "abc123",
			objectModified:    time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			expectProceed:     false,
			expectStatus:      http.StatusPreconditionFailed,
			expectErrCode:     s3err.ErrPreconditionFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("GET", "/bucket/key", nil)
			if tc.ifMatch != "" {
				req.Header.Set("If-Match", tc.ifMatch)
			}
			if tc.ifNoneMatch != "" {
				req.Header.Set("If-None-Match", tc.ifNoneMatch)
			}
			if tc.ifModifiedSince != "" {
				req.Header.Set("If-Modified-Since", tc.ifModifiedSince)
			}
			if tc.ifUnmodifiedSince != "" {
				req.Header.Set("If-Unmodified-Since", tc.ifUnmodifiedSince)
			}

			shouldProceed, statusCode, errCode := checkConditionalHeaders(req, tc.objectETag, tc.objectModified)

			assert.Equal(t, tc.expectProceed, shouldProceed, "shouldProceed mismatch")
			if !tc.expectProceed {
				assert.Equal(t, tc.expectStatus, statusCode, "statusCode mismatch")
				assert.Equal(t, tc.expectErrCode, errCode, "errCode mismatch")
			}
		})
	}
}

func TestCheckExpectedBucketOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		headerValue   string
		bucketOwnerID string
		expectError   bool
		expectErrCode s3err.ErrorCode
	}{
		{
			name:          "no header provided",
			headerValue:   "",
			bucketOwnerID: "owner-123",
			expectError:   false,
		},
		{
			name:          "header matches owner",
			headerValue:   "owner-123",
			bucketOwnerID: "owner-123",
			expectError:   false,
		},
		{
			name:          "header does not match owner",
			headerValue:   "owner-456",
			bucketOwnerID: "owner-123",
			expectError:   true,
			expectErrCode: s3err.ErrAccessDenied,
		},
		{
			name:          "case sensitive match",
			headerValue:   "Owner-123",
			bucketOwnerID: "owner-123",
			expectError:   true,
			expectErrCode: s3err.ErrAccessDenied,
		},
		{
			name:          "empty bucket owner with header",
			headerValue:   "owner-123",
			bucketOwnerID: "",
			expectError:   true,
			expectErrCode: s3err.ErrAccessDenied,
		},
		{
			name:          "empty header value with owner",
			headerValue:   "",
			bucketOwnerID: "owner-123",
			expectError:   false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("GET", "/bucket/key", nil)
			if tc.headerValue != "" {
				req.Header.Set(s3consts.XAmzExpectedBucketOwner, tc.headerValue)
			}

			errCode := checkExpectedBucketOwner(req, tc.bucketOwnerID)

			if tc.expectError {
				assert.NotNil(t, errCode, "expected error code but got nil")
				if errCode != nil {
					assert.Equal(t, tc.expectErrCode, *errCode, "error code mismatch")
				}
			} else {
				assert.Nil(t, errCode, "expected no error but got: %v", errCode)
			}
		})
	}
}

func TestCheckRequestPayer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		headerValue    string
		requestPayment *s3types.RequestPaymentConfig
		expectError    bool
		expectErrCode  s3err.ErrorCode
	}{
		{
			name:           "no request payment config",
			headerValue:    "",
			requestPayment: nil,
			expectError:    false,
		},
		{
			name:        "bucket owner pays - header not required",
			headerValue: "",
			requestPayment: &s3types.RequestPaymentConfig{
				Payer: s3types.PayerBucketOwner,
			},
			expectError: false,
		},
		{
			name:        "bucket owner pays - header provided anyway",
			headerValue: "requester",
			requestPayment: &s3types.RequestPaymentConfig{
				Payer: s3types.PayerBucketOwner,
			},
			expectError: false,
		},
		{
			name:        "requester pays - header missing",
			headerValue: "",
			requestPayment: &s3types.RequestPaymentConfig{
				Payer: s3types.PayerRequester,
			},
			expectError:   true,
			expectErrCode: s3err.ErrAccessDenied,
		},
		{
			name:        "requester pays - header with correct value",
			headerValue: "requester",
			requestPayment: &s3types.RequestPaymentConfig{
				Payer: s3types.PayerRequester,
			},
			expectError: false,
		},
		{
			name:        "requester pays - header with uppercase value",
			headerValue: "REQUESTER",
			requestPayment: &s3types.RequestPaymentConfig{
				Payer: s3types.PayerRequester,
			},
			expectError: false, // Case-insensitive per AWS spec
		},
		{
			name:        "requester pays - header with mixed case value",
			headerValue: "Requester",
			requestPayment: &s3types.RequestPaymentConfig{
				Payer: s3types.PayerRequester,
			},
			expectError: false, // Case-insensitive per AWS spec
		},
		{
			name:        "requester pays - header with invalid value",
			headerValue: "bucket-owner",
			requestPayment: &s3types.RequestPaymentConfig{
				Payer: s3types.PayerRequester,
			},
			expectError:   true,
			expectErrCode: s3err.ErrAccessDenied,
		},
		{
			name:        "requester pays - header with empty value",
			headerValue: "",
			requestPayment: &s3types.RequestPaymentConfig{
				Payer: s3types.PayerRequester,
			},
			expectError:   true,
			expectErrCode: s3err.ErrAccessDenied,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("GET", "/bucket/key", nil)
			if tc.headerValue != "" {
				req.Header.Set(s3consts.XAmzRequestPayer, tc.headerValue)
			}

			errCode := checkRequestPayer(req, tc.requestPayment)

			if tc.expectError {
				assert.NotNil(t, errCode, "expected error code but got nil")
				if errCode != nil {
					assert.Equal(t, tc.expectErrCode, *errCode, "error code mismatch")
				}
			} else {
				assert.Nil(t, errCode, "expected no error but got: %v", errCode)
			}
		})
	}
}

func TestCheckCopySourceConditionalHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		ifMatch           string
		ifNoneMatch       string
		ifModifiedSince   string
		ifUnmodifiedSince string
		objectETag        string
		objectModified    time.Time
		expectProceed     bool
		expectStatus      int
		expectErrCode     s3err.ErrorCode
	}{
		{
			name:           "no conditional headers",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  true,
		},
		{
			name:           "copy-source-if-match matches",
			ifMatch:        "abc123",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  true,
		},
		{
			name:           "copy-source-if-match does not match",
			ifMatch:        "xyz789",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  false,
			expectStatus:   http.StatusPreconditionFailed,
			expectErrCode:  s3err.ErrPreconditionFailed,
		},
		{
			name:           "copy-source-if-none-match does not match",
			ifNoneMatch:    "xyz789",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  true,
		},
		{
			name:           "copy-source-if-none-match matches (return 412)",
			ifNoneMatch:    "abc123",
			objectETag:     "abc123",
			objectModified: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:  false,
			expectStatus:   http.StatusPreconditionFailed,
			expectErrCode:  s3err.ErrPreconditionFailed,
		},
		{
			name:            "copy-source-if-modified-since - object not modified",
			ifModifiedSince: "Mon, 02 Jan 2024 00:00:00 GMT",
			objectETag:      "abc123",
			objectModified:  time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:   false,
			expectStatus:    http.StatusPreconditionFailed,
			expectErrCode:   s3err.ErrPreconditionFailed,
		},
		{
			name:            "copy-source-if-modified-since - object modified",
			ifModifiedSince: "Mon, 01 Jan 2024 00:00:00 GMT",
			objectETag:      "abc123",
			objectModified:  time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			expectProceed:   true,
		},
		{
			name:              "copy-source-if-unmodified-since - object not modified",
			ifUnmodifiedSince: "Mon, 02 Jan 2024 00:00:00 GMT",
			objectETag:        "abc123",
			objectModified:    time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			expectProceed:     true,
		},
		{
			name:              "copy-source-if-unmodified-since - object modified",
			ifUnmodifiedSince: "Mon, 01 Jan 2024 00:00:00 GMT",
			objectETag:        "abc123",
			objectModified:    time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
			expectProceed:     false,
			expectStatus:      http.StatusPreconditionFailed,
			expectErrCode:     s3err.ErrPreconditionFailed,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("PUT", "/bucket/key", nil)
			if tc.ifMatch != "" {
				req.Header.Set(s3consts.XAmzCopySourceIfMatch, tc.ifMatch)
			}
			if tc.ifNoneMatch != "" {
				req.Header.Set(s3consts.XAmzCopySourceIfNoneMatch, tc.ifNoneMatch)
			}
			if tc.ifModifiedSince != "" {
				req.Header.Set(s3consts.XAmzCopySourceIfModifiedSince, tc.ifModifiedSince)
			}
			if tc.ifUnmodifiedSince != "" {
				req.Header.Set(s3consts.XAmzCopySourceIfUnmodifiedSince, tc.ifUnmodifiedSince)
			}

			shouldProceed, statusCode, errCode := checkCopySourceConditionalHeaders(req, tc.objectETag, tc.objectModified)

			assert.Equal(t, tc.expectProceed, shouldProceed, "shouldProceed mismatch")
			if !tc.expectProceed {
				assert.Equal(t, tc.expectStatus, statusCode, "statusCode mismatch")
				assert.Equal(t, tc.expectErrCode, errCode, "errCode mismatch")
			}
		})
	}
}

func TestCheckExpectedSourceBucketOwner(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		headerValue   string
		sourceOwnerID string
		expectError   bool
		expectErrCode s3err.ErrorCode
	}{
		{
			name:          "no header provided",
			headerValue:   "",
			sourceOwnerID: "owner-123",
			expectError:   false,
		},
		{
			name:          "header matches owner",
			headerValue:   "owner-123",
			sourceOwnerID: "owner-123",
			expectError:   false,
		},
		{
			name:          "header does not match owner",
			headerValue:   "owner-456",
			sourceOwnerID: "owner-123",
			expectError:   true,
			expectErrCode: s3err.ErrAccessDenied,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("PUT", "/bucket/key", nil)
			if tc.headerValue != "" {
				req.Header.Set(s3consts.XAmzSourceExpectedBucketOwner, tc.headerValue)
			}

			errCode := checkExpectedSourceBucketOwner(req, tc.sourceOwnerID)

			if tc.expectError {
				assert.NotNil(t, errCode, "expected error code but got nil")
				if errCode != nil {
					assert.Equal(t, tc.expectErrCode, *errCode, "error code mismatch")
				}
			} else {
				assert.Nil(t, errCode, "expected no error but got: %v", errCode)
			}
		})
	}
}

func TestParseCopySourceRange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rangeHeader  string
		objectSize   uint64
		expectValid  bool
		expectOffset uint64
		expectLength uint64
	}{
		{
			name:         "valid range",
			rangeHeader:  "bytes=0-499",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 0,
			expectLength: 500,
		},
		{
			name:         "middle range",
			rangeHeader:  "bytes=100-199",
			objectSize:   1000,
			expectValid:  true,
			expectOffset: 100,
			expectLength: 100,
		},
		{
			name:        "invalid range",
			rangeHeader: "bytes=500-400",
			objectSize:  1000,
			expectValid: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			offset, length, valid := parseCopySourceRange(tc.rangeHeader, tc.objectSize)

			assert.Equal(t, tc.expectValid, valid, "validity mismatch")
			if tc.expectValid {
				assert.Equal(t, tc.expectOffset, offset, "offset mismatch")
				assert.Equal(t, tc.expectLength, length, "length mismatch")
			}
		})
	}
}

func TestParseSSECHeaders(t *testing.T) {
	t.Parallel()

	// Generate a valid 32-byte key for testing
	validKey := make([]byte, 32)
	for i := range validKey {
		validKey[i] = byte(i)
	}
	validKeyBase64 := base64.StdEncoding.EncodeToString(validKey)
	validKeyMD5Hash := md5.Sum(validKey)
	validKeyMD5 := base64.StdEncoding.EncodeToString(validKeyMD5Hash[:])

	tests := []struct {
		name          string
		algorithm     string
		keyBase64     string
		keyMD5        string
		expectError   bool
		expectErrCode s3err.ErrorCode
	}{
		{
			name:        "no headers - returns nil",
			algorithm:   "",
			keyBase64:   "",
			keyMD5:      "",
			expectError: false,
		},
		{
			name:          "missing algorithm",
			algorithm:     "",
			keyBase64:     validKeyBase64,
			keyMD5:        validKeyMD5,
			expectError:   true,
			expectErrCode: s3err.ErrSSECustomerKeyMissing,
		},
		{
			name:          "missing key",
			algorithm:     s3consts.SSEAlgorithmAES256,
			keyBase64:     "",
			keyMD5:        validKeyMD5,
			expectError:   true,
			expectErrCode: s3err.ErrSSECustomerKeyMissing,
		},
		{
			name:          "missing MD5",
			algorithm:     s3consts.SSEAlgorithmAES256,
			keyBase64:     validKeyBase64,
			keyMD5:        "",
			expectError:   true,
			expectErrCode: s3err.ErrSSECustomerKeyMissing,
		},
		{
			name:          "invalid algorithm",
			algorithm:     "AES128",
			keyBase64:     validKeyBase64,
			keyMD5:        validKeyMD5,
			expectError:   true,
			expectErrCode: s3err.ErrInvalidEncryptionAlgorithm,
		},
		{
			name:          "invalid base64 key",
			algorithm:     s3consts.SSEAlgorithmAES256,
			keyBase64:     "not-valid-base64!!!",
			keyMD5:        validKeyMD5,
			expectError:   true,
			expectErrCode: s3err.ErrInvalidEncryptionKey,
		},
		{
			name:          "key too short (16 bytes)",
			algorithm:     s3consts.SSEAlgorithmAES256,
			keyBase64:     base64.StdEncoding.EncodeToString(make([]byte, 16)),
			keyMD5:        validKeyMD5,
			expectError:   true,
			expectErrCode: s3err.ErrInvalidEncryptionKey,
		},
		{
			name:          "key too long (64 bytes)",
			algorithm:     s3consts.SSEAlgorithmAES256,
			keyBase64:     base64.StdEncoding.EncodeToString(make([]byte, 64)),
			keyMD5:        validKeyMD5,
			expectError:   true,
			expectErrCode: s3err.ErrInvalidEncryptionKey,
		},
		{
			name:          "MD5 mismatch",
			algorithm:     s3consts.SSEAlgorithmAES256,
			keyBase64:     validKeyBase64,
			keyMD5:        "invalid-md5-hash",
			expectError:   true,
			expectErrCode: s3err.ErrSSECustomerKeyMD5Mismatch,
		},
		{
			name:        "valid SSE-C headers",
			algorithm:   s3consts.SSEAlgorithmAES256,
			keyBase64:   validKeyBase64,
			keyMD5:      validKeyMD5,
			expectError: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("PUT", "/bucket/key", nil)
			if tc.algorithm != "" {
				req.Header.Set(s3consts.XAmzServerSideEncryptionCustomerAlgo, tc.algorithm)
			}
			if tc.keyBase64 != "" {
				req.Header.Set(s3consts.XAmzServerSideEncryptionCustomerKey, tc.keyBase64)
			}
			if tc.keyMD5 != "" {
				req.Header.Set(s3consts.XAmzServerSideEncryptionCustomerKeyMD5, tc.keyMD5)
			}

			headers, errCode := parseSSECHeaders(req)

			if tc.expectError {
				assert.NotNil(t, errCode, "expected error code but got nil")
				if tc.expectErrCode != 0 {
					assert.NotNil(t, errCode, "expected error code but got nil")
					assert.Equal(t, tc.expectErrCode, *errCode, "error code mismatch")
				}
				assert.Nil(t, headers, "expected nil headers on error")
			} else {
				if tc.algorithm == "" && tc.keyBase64 == "" && tc.keyMD5 == "" {
					// No headers case
					assert.Nil(t, headers, "expected nil headers when no SSE-C headers present")
					assert.Nil(t, errCode, "expected no error when no SSE-C headers present")
				} else {
					// Valid headers case
					assert.Nil(t, errCode, "expected no error but got: %v", errCode)
					assert.NotNil(t, headers, "expected headers but got nil")
					if headers != nil {
						assert.Equal(t, tc.algorithm, headers.Algorithm)
						assert.Equal(t, tc.keyMD5, headers.KeyMD5)
						assert.Equal(t, 32, len(headers.Key), "key should be 32 bytes")
					}
				}
			}
		})
	}
}

func TestEncryptDecryptWithSSEC(t *testing.T) {
	t.Parallel()

	// Generate a valid 32-byte key
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i)
	}

	tests := []struct {
		name      string
		plaintext []byte
		key       []byte
		expectErr bool
	}{
		{
			name:      "empty plaintext",
			plaintext: []byte{},
			key:       key,
			expectErr: false,
		},
		{
			name:      "small plaintext (1 byte)",
			plaintext: []byte{0x01},
			key:       key,
			expectErr: false,
		},
		{
			name:      "exact block size (16 bytes)",
			plaintext: make([]byte, 16),
			key:       key,
			expectErr: false,
		},
		{
			name:      "multiple blocks (100 bytes)",
			plaintext: make([]byte, 100),
			key:       key,
			expectErr: false,
		},
		{
			name:      "large plaintext (10KB)",
			plaintext: make([]byte, 10*1024),
			key:       key,
			expectErr: false,
		},
		{
			name:      "key too short",
			plaintext: []byte("test"),
			key:       make([]byte, 16),
			expectErr: true,
		},
		{
			name:      "key too long",
			plaintext: []byte("test"),
			key:       make([]byte, 64),
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Encrypt
			ciphertext, err := encryptWithSSEC(tc.plaintext, tc.key)
			if tc.expectErr {
				assert.Error(t, err, "expected encryption error")
				return
			}

			assert.NoError(t, err, "encryption should succeed")
			assert.NotNil(t, ciphertext, "ciphertext should not be nil")
			assert.Greater(t, len(ciphertext), len(tc.plaintext), "ciphertext should be larger than plaintext (includes IV and padding)")

			// Verify IV is prepended (first 16 bytes)
			assert.GreaterOrEqual(t, len(ciphertext), 16, "ciphertext should include IV")

			// Decrypt
			decrypted, err := decryptWithSSEC(ciphertext, tc.key)
			assert.NoError(t, err, "decryption should succeed")
			assert.Equal(t, tc.plaintext, decrypted, "decrypted text should match original")

			// Test that different keys produce different ciphertexts
			if len(tc.key) == 32 && len(tc.plaintext) > 0 {
				differentKey := make([]byte, 32)
				copy(differentKey, tc.key)
				differentKey[0] ^= 0xFF // Flip first bit

				encrypted2, err := encryptWithSSEC(tc.plaintext, differentKey)
				assert.NoError(t, err)
				assert.NotEqual(t, ciphertext, encrypted2, "different keys should produce different ciphertexts")

				// Decrypting with wrong key should fail
				_, err = decryptWithSSEC(ciphertext, differentKey)
				assert.Error(t, err, "decryption with wrong key should fail")
			}
		})
	}
}

func TestDecryptWithSSEC_InvalidInput(t *testing.T) {
	t.Parallel()

	key := make([]byte, 32)

	tests := []struct {
		name       string
		ciphertext []byte
		key        []byte
		expectErr  bool
	}{
		{
			name:       "ciphertext too short (no IV)",
			ciphertext: make([]byte, 10),
			key:        key,
			expectErr:  true,
		},
		{
			name:       "ciphertext only IV (no data)",
			ciphertext: make([]byte, 16),
			key:        key,
			expectErr:  true,
		},
		{
			name:       "ciphertext not multiple of block size",
			ciphertext: make([]byte, 17), // IV (16) + 1 byte (invalid)
			key:        key,
			expectErr:  true,
		},
		{
			name:       "key too short",
			ciphertext: make([]byte, 32),
			key:        make([]byte, 16),
			expectErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := decryptWithSSEC(tc.ciphertext, tc.key)
			assert.Error(t, err, "expected decryption error")
		})
	}
}

func TestParseSSEKMSHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		algorithm     string
		keyID         string
		context       string
		expectError   bool
		expectErrCode *s3err.ErrorCode
		skipLicense   bool // Skip if license check would fail
	}{
		{
			name:        "no headers - returns nil",
			algorithm:   "",
			keyID:       "",
			context:     "",
			expectError: false,
		},
		{
			name:        "algorithm aws:kms without key ID",
			algorithm:   s3consts.SSEAlgorithmKMS,
			keyID:       "",
			context:     "",
			expectError: true,
			// May return ErrKMSAccessDenied (license check) or ErrKMSKeyNotFound (validation)
			// Both are acceptable errors - don't check specific code
			expectErrCode: nil,
		},
		{
			name:        "key ID without algorithm (should assume aws:kms)",
			algorithm:   "",
			keyID:       "test-key-id",
			context:     "",
			expectError: false,
			skipLicense: true, // License check may fail
		},
		{
			name:        "invalid algorithm (not aws:kms)",
			algorithm:   "AES256",
			keyID:       "test-key-id",
			context:     "",
			expectError: true,
			// May return ErrKMSAccessDenied (license check) or ErrInvalidEncryptionAlgorithm (validation)
			// Both are acceptable errors - don't check specific code
			expectErrCode: nil,
		},
		{
			name:        "key ID with wrong algorithm",
			algorithm:   "AES256",
			keyID:       "test-key-id",
			context:     "",
			expectError: true,
			// May return ErrKMSAccessDenied (license check) or ErrInvalidEncryptionAlgorithm (validation)
			// Both are acceptable errors - don't check specific code
			expectErrCode: nil,
		},
		{
			name:        "valid SSE-KMS headers with algorithm",
			algorithm:   s3consts.SSEAlgorithmKMS,
			keyID:       "test-key-id",
			context:     "",
			expectError: false,
			skipLicense: true, // License check may fail
		},
		{
			name:        "valid SSE-KMS headers with context",
			algorithm:   s3consts.SSEAlgorithmKMS,
			keyID:       "test-key-id",
			context:     `{"bucket":"test-bucket"}`,
			expectError: false,
			skipLicense: true, // License check may fail
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("PUT", "/bucket/key", nil)
			if tc.algorithm != "" {
				req.Header.Set(s3consts.XAmzServerSideEncryption, tc.algorithm)
			}
			if tc.keyID != "" {
				req.Header.Set(s3consts.XAmzServerSideEncryptionAwsKmsKeyID, tc.keyID)
			}
			if tc.context != "" {
				req.Header.Set(s3consts.XAmzServerSideEncryptionContext, tc.context)
			}

			headers, errCode := parseSSEKMSHeaders(req)

			if tc.expectError {
				assert.NotNil(t, errCode, "expected error code but got nil")
				if tc.expectErrCode != nil && errCode != nil {
					// Compare error code values
					assert.Equal(t, *tc.expectErrCode, *errCode, "error code mismatch")
				}
				// If expectErrCode is nil, we accept any error code (e.g., license vs validation error)
				assert.Nil(t, headers, "expected nil headers on error")
			} else {
				if tc.skipLicense {
					// License check may fail, so we might get ErrKMSAccessDenied
					// This is acceptable for unit tests without license setup
					if errCode != nil && *errCode == s3err.ErrKMSAccessDenied {
						t.Skip("Skipping test: KMS license not available")
						return
					}
				}
				if tc.algorithm == "" && tc.keyID == "" && tc.context == "" {
					// No headers case
					assert.Nil(t, headers, "expected nil headers when no SSE-KMS headers present")
					assert.Nil(t, errCode, "expected no error when no SSE-KMS headers present")
				} else {
					// Valid headers case
					assert.Nil(t, errCode, "expected no error but got: %v", errCode)
					assert.NotNil(t, headers, "expected headers but got nil")
					if headers != nil {
						if tc.algorithm != "" {
							assert.Equal(t, tc.algorithm, headers.Algorithm)
						} else {
							assert.Equal(t, s3consts.SSEAlgorithmKMS, headers.Algorithm, "should default to aws:kms when key ID provided")
						}
						assert.Equal(t, tc.keyID, headers.KeyID)
						assert.Equal(t, tc.context, headers.Context)
					}
				}
			}
		})
	}
}

func TestEncryptDecryptWithKMSDEK(t *testing.T) {
	t.Parallel()

	// Generate a valid 32-byte DEK (data encryption key)
	dek := make([]byte, 32)
	for i := range dek {
		dek[i] = byte(i)
	}

	tests := []struct {
		name      string
		plaintext []byte
		dek       []byte
		expectErr bool
	}{
		{
			name:      "empty plaintext",
			plaintext: []byte{},
			dek:       dek,
			expectErr: false,
		},
		{
			name:      "small plaintext (1 byte)",
			plaintext: []byte{0x01},
			dek:       dek,
			expectErr: false,
		},
		{
			name:      "exact block size (16 bytes)",
			plaintext: make([]byte, 16),
			dek:       dek,
			expectErr: false,
		},
		{
			name:      "multiple blocks (100 bytes)",
			plaintext: make([]byte, 100),
			dek:       dek,
			expectErr: false,
		},
		{
			name:      "large plaintext (10KB)",
			plaintext: make([]byte, 10*1024),
			dek:       dek,
			expectErr: false,
		},
		{
			name:      "key too short",
			plaintext: []byte("test"),
			dek:       make([]byte, 16),
			expectErr: true,
		},
		{
			name:      "key too long",
			plaintext: []byte("test"),
			dek:       make([]byte, 64),
			expectErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Encrypt
			ciphertext, err := encryptWithKMSDEK(tc.plaintext, tc.dek)
			if tc.expectErr {
				assert.Error(t, err, "expected encryption error")
				return
			}

			assert.NoError(t, err, "encryption should succeed")
			assert.NotNil(t, ciphertext, "ciphertext should not be nil")
			// GCM mode prepends nonce, so ciphertext should be larger
			assert.Greater(t, len(ciphertext), len(tc.plaintext), "ciphertext should be larger than plaintext (includes nonce)")

			// Decrypt
			decrypted, err := decryptWithKMSDEK(ciphertext, tc.dek)
			assert.NoError(t, err, "decryption should succeed")
			// Handle empty slice vs nil - both are valid empty results
			if len(tc.plaintext) == 0 {
				assert.True(t, len(decrypted) == 0, "decrypted should be empty for empty plaintext")
			} else {
				assert.Equal(t, tc.plaintext, decrypted, "decrypted text should match original")
			}

			// Test that different DEKs produce different ciphertexts
			if len(tc.dek) == 32 && len(tc.plaintext) > 0 {
				differentDEK := make([]byte, 32)
				copy(differentDEK, tc.dek)
				differentDEK[0] ^= 0xFF // Flip first bit

				encrypted2, err := encryptWithKMSDEK(tc.plaintext, differentDEK)
				assert.NoError(t, err)
				assert.NotEqual(t, ciphertext, encrypted2, "different DEKs should produce different ciphertexts")

				// Decrypting with wrong DEK should fail
				_, err = decryptWithKMSDEK(ciphertext, differentDEK)
				assert.Error(t, err, "decryption with wrong DEK should fail")
			}
		})
	}
}

func TestDecryptWithKMSDEK_InvalidInput(t *testing.T) {
	t.Parallel()

	dek := make([]byte, 32)
	for i := range dek {
		dek[i] = byte(i)
	}

	tests := []struct {
		name       string
		ciphertext []byte
		dek        []byte
		expectErr  bool
	}{
		{
			name:       "ciphertext too short (no nonce)",
			ciphertext: make([]byte, 5),
			dek:        dek,
			expectErr:  true,
		},
		{
			name:       "ciphertext with invalid nonce size",
			ciphertext: make([]byte, 10), // Less than GCM nonce size (12 bytes)
			dek:        dek,
			expectErr:  true,
		},
		{
			name:       "dek too short",
			ciphertext: make([]byte, 50),
			dek:        make([]byte, 16),
			expectErr:  true,
		},
		{
			name:       "dek too long",
			ciphertext: make([]byte, 50),
			dek:        make([]byte, 64),
			expectErr:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// For invalid ciphertext, try to decrypt
			if len(tc.ciphertext) > 0 && len(tc.dek) == 32 {
				// Create a valid-looking ciphertext structure but with wrong content
				_, err := decryptWithKMSDEK(tc.ciphertext, tc.dek)
				assert.Error(t, err, "expected decryption error")
			} else {
				// For invalid DEK, try to encrypt first
				_, err := encryptWithKMSDEK([]byte("test"), tc.dek)
				assert.Error(t, err, "expected encryption error")
			}
		})
	}
}
