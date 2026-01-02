// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package signature

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// signV2Request signs a request using AWS Signature V2
func signV2Request(req *http.Request, accessKey, secretKey string) {
	// Set Date header
	date := time.Now().UTC().Format(time.RFC1123)
	req.Header.Set("Date", date)

	// Build string to sign
	stringToSign := buildV2StringToSign(req, date, false, "")

	// Calculate signature
	h := hmac.New(sha1.New, []byte(secretKey))
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	// Set Authorization header
	req.Header.Set("Authorization", "AWS "+accessKey+":"+signature)
}

func buildV2StringToSign(req *http.Request, dateOrExpires string, isPresigned bool, amzHeaders string) string {
	contentMD5 := req.Header.Get("Content-MD5")
	contentType := req.Header.Get("Content-Type")

	// For presigned, use expires instead of date
	date := dateOrExpires
	if !isPresigned && req.Header.Get("X-Amz-Date") != "" {
		date = ""
	}

	// Build canonicalized resource
	resource := req.URL.Path
	if resource == "" {
		resource = "/"
	}

	return req.Method + "\n" +
		contentMD5 + "\n" +
		contentType + "\n" +
		date + "\n" +
		amzHeaders +
		resource
}

func TestV2Verifier_VerifyRequest(t *testing.T) {
	t.Parallel()

	manager := createTestManager(t)
	verifier := NewV2Verifier(manager)

	tests := []struct {
		name           string
		method         string
		path           string
		headers        map[string]string
		accessKey      string
		secretKey      string
		expectedErr    s3err.ErrorCode
		expectIdentity bool
	}{
		{
			name:           "valid GET request",
			method:         "GET",
			path:           "/test-bucket/test-key",
			headers:        map[string]string{},
			accessKey:      testAccessKey,
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrNone,
			expectIdentity: true,
		},
		{
			name:           "valid PUT request",
			method:         "PUT",
			path:           "/test-bucket/test-key",
			headers:        map[string]string{"Content-Type": "application/octet-stream"},
			accessKey:      testAccessKey,
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrNone,
			expectIdentity: true,
		},
		{
			name:           "valid DELETE request",
			method:         "DELETE",
			path:           "/test-bucket/test-key",
			headers:        map[string]string{},
			accessKey:      testAccessKey,
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrNone,
			expectIdentity: true,
		},
		{
			name:           "valid request with bucket only",
			method:         "GET",
			path:           "/test-bucket",
			headers:        map[string]string{},
			accessKey:      testAccessKey,
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrNone,
			expectIdentity: true,
		},
		{
			name:           "invalid access key",
			method:         "GET",
			path:           "/test-bucket/test-key",
			headers:        map[string]string{},
			accessKey:      "INVALIDACCESSKEY123",
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrInvalidAccessKeyID,
			expectIdentity: false,
		},
		{
			name:           "wrong secret key",
			method:         "GET",
			path:           "/test-bucket/test-key",
			headers:        map[string]string{},
			accessKey:      testAccessKey,
			secretKey:      "wrongsecretkey123456789012345678901234",
			expectedErr:    s3err.ErrSignatureDoesNotMatch,
			expectIdentity: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Create request
			reqURL := "http://s3.amazonaws.com" + tt.path
			req := httptest.NewRequest(tt.method, reqURL, nil)
			req.Host = "s3.amazonaws.com"

			// Set custom headers
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			// Sign the request
			signV2Request(req, tt.accessKey, tt.secretKey)

			// Verify request
			identity, errCode := verifier.VerifyRequest(req)

			assert.Equal(t, tt.expectedErr, errCode, "error code mismatch")

			if tt.expectIdentity {
				require.NotNil(t, identity, "expected identity")
				assert.Equal(t, "testuser", identity.Name)
			} else {
				assert.Nil(t, identity, "expected no identity")
			}
		})
	}
}

func TestV2Verifier_ExtractV2AuthInfo(t *testing.T) {
	t.Parallel()

	verifier := NewV2Verifier(nil)

	tests := []struct {
		name         string
		authHeader   string
		queryParams  map[string]string
		expectError  bool
		expectAccess string
		isPresigned  bool
	}{
		{
			name:         "valid authorization header",
			authHeader:   "AWS AKIAIOSFODNN7EXAMPLE:signature123",
			queryParams:  nil,
			expectError:  false,
			expectAccess: "AKIAIOSFODNN7EXAMPLE",
			isPresigned:  false,
		},
		{
			name:         "valid presigned URL",
			authHeader:   "",
			queryParams:  map[string]string{"AWSAccessKeyId": "AKIAIOSFODNN7EXAMPLE", "Signature": "sig123", "Expires": "1234567890"},
			expectError:  false,
			expectAccess: "AKIAIOSFODNN7EXAMPLE",
			isPresigned:  true,
		},
		{
			name:        "missing authorization header",
			authHeader:  "",
			queryParams: nil,
			expectError: true,
		},
		{
			name:        "invalid authorization prefix",
			authHeader:  "AWS4-HMAC-SHA256 Credential=...",
			queryParams: nil,
			expectError: true,
		},
		{
			name:        "missing signature in header",
			authHeader:  "AWS AKIAIOSFODNN7EXAMPLE",
			queryParams: nil,
			expectError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reqURL := "http://s3.amazonaws.com/bucket"
			if tt.queryParams != nil {
				reqURL += "?"
				first := true
				for k, v := range tt.queryParams {
					if !first {
						reqURL += "&"
					}
					reqURL += k + "=" + v
					first = false
				}
			}

			req := httptest.NewRequest("GET", reqURL, nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}

			auth, err := verifier.extractV2AuthInfo(req)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, auth)
			} else {
				require.NoError(t, err)
				require.NotNil(t, auth)
				assert.Equal(t, tt.expectAccess, auth.accessKey)
				assert.Equal(t, tt.isPresigned, auth.isPresigned)
			}
		})
	}
}

func TestV2Verifier_BuildStringToSign(t *testing.T) {
	t.Parallel()

	verifier := NewV2Verifier(nil)

	tests := []struct {
		name          string
		method        string
		path          string
		headers       map[string]string
		date          string
		isPresigned   bool
		expires       string
		expectContain []string
	}{
		{
			name:          "simple GET request",
			method:        "GET",
			path:          "/bucket/key",
			headers:       map[string]string{"Date": "Mon, 02 Jan 2006 15:04:05 GMT"},
			date:          "Mon, 02 Jan 2006 15:04:05 GMT",
			isPresigned:   false,
			expires:       "",
			expectContain: []string{"GET", "/bucket/key", "Mon, 02 Jan 2006 15:04:05 GMT"},
		},
		{
			name:          "PUT with Content-Type",
			method:        "PUT",
			path:          "/bucket/key",
			headers:       map[string]string{"Date": "Mon, 02 Jan 2006 15:04:05 GMT", "Content-Type": "text/plain"},
			date:          "Mon, 02 Jan 2006 15:04:05 GMT",
			isPresigned:   false,
			expires:       "",
			expectContain: []string{"PUT", "text/plain", "/bucket/key"},
		},
		{
			name:          "PUT with Content-MD5",
			method:        "PUT",
			path:          "/bucket/key",
			headers:       map[string]string{"Date": "Mon, 02 Jan 2006 15:04:05 GMT", "Content-MD5": "d41d8cd98f00b204e9800998ecf8427e"},
			date:          "Mon, 02 Jan 2006 15:04:05 GMT",
			isPresigned:   false,
			expires:       "",
			expectContain: []string{"PUT", "d41d8cd98f00b204e9800998ecf8427e", "/bucket/key"},
		},
		{
			name:          "presigned uses expires",
			method:        "GET",
			path:          "/bucket/key",
			headers:       map[string]string{},
			date:          "",
			isPresigned:   true,
			expires:       "1234567890",
			expectContain: []string{"GET", "1234567890", "/bucket/key"},
		},
		{
			name:          "x-amz-date clears date",
			method:        "GET",
			path:          "/bucket/key",
			headers:       map[string]string{"Date": "Mon, 02 Jan 2006 15:04:05 GMT", "X-Amz-Date": "20130524T000000Z"},
			date:          "",
			isPresigned:   false,
			expires:       "",
			expectContain: []string{"GET", "\n\n\n", "/bucket/key"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, "http://s3.amazonaws.com"+tt.path, nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			auth := &v2AuthInfo{
				isPresigned: tt.isPresigned,
				expires:     tt.expires,
			}

			result := verifier.buildStringToSign(req, auth)

			for _, expected := range tt.expectContain {
				assert.Contains(t, result, expected, "string to sign should contain: %s", expected)
			}
		})
	}
}

func TestV2Verifier_BuildCanonicalizedAmzHeaders(t *testing.T) {
	t.Parallel()

	verifier := NewV2Verifier(nil)

	tests := []struct {
		name          string
		headers       map[string]string
		expectContain []string
		expectEmpty   bool
	}{
		{
			name:        "no x-amz headers",
			headers:     map[string]string{"Content-Type": "text/plain"},
			expectEmpty: true,
		},
		{
			name:          "single x-amz header",
			headers:       map[string]string{"X-Amz-Date": "20130524T000000Z"},
			expectContain: []string{"x-amz-date:20130524T000000Z"},
			expectEmpty:   false,
		},
		{
			name:          "multiple x-amz headers sorted",
			headers:       map[string]string{"X-Amz-Meta-Name": "test", "X-Amz-Date": "20130524T000000Z"},
			expectContain: []string{"x-amz-date:20130524T000000Z", "x-amz-meta-name:test"},
			expectEmpty:   false,
		},
		{
			name:          "headers lowercased",
			headers:       map[string]string{"X-AMZ-Meta-NAME": "test"},
			expectContain: []string{"x-amz-meta-name:test"},
			expectEmpty:   false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("GET", "http://s3.amazonaws.com/bucket", nil)
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			result := verifier.buildCanonicalizedAmzHeaders(req)

			if tt.expectEmpty {
				assert.Empty(t, result)
			} else {
				for _, expected := range tt.expectContain {
					assert.Contains(t, result, expected)
				}
				// Should end with newline if not empty
				assert.True(t, len(result) > 0 && result[len(result)-1] == '\n')
			}
		})
	}
}

func TestV2Verifier_BuildCanonicalizedResource(t *testing.T) {
	t.Parallel()

	verifier := NewV2Verifier(nil)

	tests := []struct {
		name     string
		path     string
		query    string
		expected string
	}{
		{
			name:     "simple path",
			path:     "/bucket/key",
			query:    "",
			expected: "/bucket/key",
		},
		{
			name:     "bucket only",
			path:     "/bucket",
			query:    "",
			expected: "/bucket",
		},
		{
			name:     "root path",
			path:     "/",
			query:    "",
			expected: "/",
		},
		{
			name:     "empty path defaults to /",
			path:     "",
			query:    "",
			expected: "/",
		},
		{
			name:     "with acl subresource",
			path:     "/bucket",
			query:    "acl",
			expected: "/bucket?acl",
		},
		{
			name:     "with versioning subresource",
			path:     "/bucket",
			query:    "versioning",
			expected: "/bucket?versioning",
		},
		{
			name:     "with versionId value",
			path:     "/bucket/key",
			query:    "versionId=123456",
			expected: "/bucket/key?versionId=123456",
		},
		{
			name:     "ignores non-subresource query params",
			path:     "/bucket",
			query:    "prefix=test&delimiter=/",
			expected: "/bucket",
		},
		{
			name:     "multiple subresources",
			path:     "/bucket",
			query:    "acl&versioning",
			expected: "/bucket?acl&versioning",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			urlStr := "http://s3.amazonaws.com" + tt.path
			if tt.query != "" {
				urlStr += "?" + tt.query
			}

			req := httptest.NewRequest("GET", urlStr, nil)

			result := verifier.buildCanonicalizedResource(req)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestV2Verifier_CalculateSignature(t *testing.T) {
	t.Parallel()

	verifier := NewV2Verifier(nil)

	tests := []struct {
		name         string
		secretKey    string
		stringToSign string
	}{
		{
			name:         "simple signature",
			secretKey:    testSecretKey,
			stringToSign: "GET\n\n\nMon, 02 Jan 2006 15:04:05 GMT\n/bucket/key",
		},
		{
			name:         "empty string to sign",
			secretKey:    testSecretKey,
			stringToSign: "",
		},
		{
			name:         "complex string",
			secretKey:    testSecretKey,
			stringToSign: "PUT\nd41d8cd98f00b204e9800998ecf8427e\napplication/octet-stream\nMon, 02 Jan 2006 15:04:05 GMT\nx-amz-meta-name:test\n/bucket/key",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sig := verifier.calculateSignature(tt.secretKey, tt.stringToSign)

			// Signature should be base64 encoded
			decoded, err := base64.StdEncoding.DecodeString(sig)
			require.NoError(t, err, "signature should be valid base64")

			// HMAC-SHA1 produces 20 bytes
			assert.Len(t, decoded, 20, "decoded signature should be 20 bytes (SHA1)")

			// Same inputs should produce same signature
			sig2 := verifier.calculateSignature(tt.secretKey, tt.stringToSign)
			assert.Equal(t, sig, sig2, "same inputs should produce same signature")

			// Different secret key should produce different signature
			sigDifferent := verifier.calculateSignature("differentsecretkey12345678901234", tt.stringToSign)
			assert.NotEqual(t, sig, sigDifferent, "different key should produce different signature")
		})
	}
}

func TestV2Verifier_PresignedURL_Expired(t *testing.T) {
	t.Parallel()

	manager := createTestManager(t)
	verifier := NewV2Verifier(manager)

	// Create a presigned URL that expired in the past
	expiredTime := time.Now().Add(-1 * time.Hour).Unix()
	expiresStr := strconv.FormatInt(expiredTime, 10)

	// Build the string to sign for presigned
	stringToSign := "GET\n\n\n" + expiresStr + "\n/bucket/key"

	// Calculate signature
	h := hmac.New(sha1.New, []byte(testSecretKey))
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	reqURL := "http://s3.amazonaws.com/bucket/key?AWSAccessKeyId=" + testAccessKey +
		"&Expires=" + expiresStr +
		"&Signature=" + signature

	req := httptest.NewRequest("GET", reqURL, nil)
	req.Host = "s3.amazonaws.com"

	identity, errCode := verifier.VerifyRequest(req)

	assert.Equal(t, s3err.ErrExpiredPresignRequest, errCode)
	assert.Nil(t, identity)
}

func TestV2Verifier_PresignedURL_Valid(t *testing.T) {
	t.Parallel()

	manager := createTestManager(t)
	verifier := NewV2Verifier(manager)

	// Create a presigned URL that expires in the future
	futureTime := time.Now().Add(1 * time.Hour).Unix()
	expiresStr := strconv.FormatInt(futureTime, 10)

	// Build the string to sign for presigned
	stringToSign := "GET\n\n\n" + expiresStr + "\n/bucket/key"

	// Calculate signature
	h := hmac.New(sha1.New, []byte(testSecretKey))
	h.Write([]byte(stringToSign))
	signature := base64.StdEncoding.EncodeToString(h.Sum(nil))

	// Build URL with properly encoded signature (base64 may contain +, /, =)
	reqURL := "http://s3.amazonaws.com/bucket/key?" +
		"AWSAccessKeyId=" + testAccessKey +
		"&Expires=" + expiresStr +
		"&Signature=" + url.QueryEscape(signature)

	req := httptest.NewRequest("GET", reqURL, nil)
	req.Host = "s3.amazonaws.com"

	identity, errCode := verifier.VerifyRequest(req)

	assert.Equal(t, s3err.ErrNone, errCode)
	require.NotNil(t, identity)
	assert.Equal(t, "testuser", identity.Name)
}

func TestConstantTimeCompareV2(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        string
		b        string
		expected bool
	}{
		{
			name:     "equal signatures",
			a:        "YWJjMTIzNDU2Nzg5MA==",
			b:        "YWJjMTIzNDU2Nzg5MA==",
			expected: true,
		},
		{
			name:     "different signatures",
			a:        "YWJjMTIzNDU2Nzg5MA==",
			b:        "eHl6OTg3NjU0MzIxMA==",
			expected: false,
		},
		{
			name:     "different lengths",
			a:        "short",
			b:        "longerstring",
			expected: false,
		},
		{
			name:     "empty strings",
			a:        "",
			b:        "",
			expected: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := constantTimeCompareV2(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// mockCredentialStoreForV2 - reuse the one from v4_test.go
// Note: Since both files are in the same package, the mockCredentialStore
// from v4_test.go is available here.
