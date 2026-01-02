// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package signature

import (
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	mocks "github.com/LeeDigitalWorks/zapfs/mocks/iam"
	iampkg "github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// Test credentials - use AWS example keys for predictable signatures
const (
	testAccessKey = "AKIAIOSFODNN7EXAMPLE"
	testSecretKey = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
	testRegion    = "us-east-1"
	testService   = "s3"
)

// testIdentity is the identity used in tests
var testIdentity = &iampkg.Identity{
	Name: "testuser",
	Account: &iampkg.Account{
		ID:          "123456789012",
		DisplayName: "Test User",
	},
	Disabled: false,
}

// testCredential is the credential used in tests
var testCredential = &iampkg.Credential{
	AccessKey: testAccessKey,
	SecretKey: testSecretKey,
	Status:    "Active",
}

// createTestManager creates an IAM manager with mocked credentials using mockery
func createTestManager(t *testing.T) *iampkg.Manager {
	mockStore := mocks.NewMockCredentialStore(t)

	// Set up expectation for GetUserByAccessKey
	mockStore.EXPECT().
		GetUserByAccessKey(mock.Anything, testAccessKey).
		Return(testIdentity, testCredential, nil).
		Maybe()

	// Set up expectation for unknown access keys
	mockStore.EXPECT().
		GetUserByAccessKey(mock.Anything, mock.MatchedBy(func(key string) bool {
			return key != testAccessKey
		})).
		Return(nil, nil, iampkg.ErrAccessKeyNotFound).
		Maybe()

	return iampkg.NewManager(mockStore)
}

// signV4Request signs a request using AWS Signature V4
func signV4Request(req *http.Request, accessKey, secretKey, region, service string, signedHeaders []string) {
	// Use current time for signing
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format(Iso8601BasicFormat)

	// Set required headers
	req.Header.Set("X-Amz-Date", amzDate)
	if req.Host == "" {
		req.Host = req.URL.Host
	}

	// Calculate payload hash
	payloadHash := HashedEmptyPayload
	req.Header.Set("X-Amz-Content-Sha256", payloadHash)

	// Build canonical request
	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Build canonical query string
	canonicalQuery := buildTestCanonicalQueryString(req.URL.Query())

	// Build canonical headers
	canonicalHeaders := ""
	for _, h := range signedHeaders {
		var val string
		if h == "host" {
			val = req.Host
		} else {
			val = req.Header.Get(h)
		}
		canonicalHeaders += h + ":" + val + "\n"
	}

	signedHeadersStr := ""
	for i, h := range signedHeaders {
		if i > 0 {
			signedHeadersStr += ";"
		}
		signedHeadersStr += h
	}

	canonicalRequest := req.Method + "\n" +
		canonicalURI + "\n" +
		canonicalQuery + "\n" +
		canonicalHeaders + "\n" +
		signedHeadersStr + "\n" +
		payloadHash

	// Hash canonical request
	h := sha256.New()
	h.Write([]byte(canonicalRequest))
	hashedCanonicalRequest := hex.EncodeToString(h.Sum(nil))

	// Build credential scope
	credentialScope := dateStamp + "/" + region + "/" + service + "/aws4_request"

	// Build string to sign
	stringToSign := AuthHeaderV4 + "\n" +
		amzDate + "\n" +
		credentialScope + "\n" +
		hashedCanonicalRequest

	// Derive signing key
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	// Calculate signature
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	// Build Authorization header
	authHeader := AuthHeaderV4 + " " +
		"Credential=" + accessKey + "/" + credentialScope + ", " +
		"SignedHeaders=" + signedHeadersStr + ", " +
		"Signature=" + signature

	req.Header.Set("Authorization", authHeader)
}

func buildTestCanonicalQueryString(query url.Values) string {
	if len(query) == 0 {
		return ""
	}

	keys := make([]string, 0, len(query))
	for k := range query {
		if k != "X-Amz-Signature" {
			keys = append(keys, k)
		}
	}

	// Simple sort for test
	for i := 0; i < len(keys); i++ {
		for j := i + 1; j < len(keys); j++ {
			if keys[i] > keys[j] {
				keys[i], keys[j] = keys[j], keys[i]
			}
		}
	}

	result := ""
	for i, k := range keys {
		if i > 0 {
			result += "&"
		}
		result += url.QueryEscape(k) + "=" + url.QueryEscape(query.Get(k))
	}
	return result
}

func TestV4Verifier_VerifyRequest(t *testing.T) {
	t.Parallel()

	manager := createTestManager(t)
	verifier := NewV4Verifier(manager)

	tests := []struct {
		name           string
		method         string
		path           string
		headers        map[string]string
		signedHeaders  []string
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
			signedHeaders:  []string{"host", "x-amz-content-sha256", "x-amz-date"},
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
			signedHeaders:  []string{"content-type", "host", "x-amz-content-sha256", "x-amz-date"},
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
			signedHeaders:  []string{"host", "x-amz-content-sha256", "x-amz-date"},
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
			signedHeaders:  []string{"host", "x-amz-content-sha256", "x-amz-date"},
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
			signedHeaders:  []string{"host", "x-amz-content-sha256", "x-amz-date"},
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
			signedHeaders:  []string{"host", "x-amz-content-sha256", "x-amz-date"},
			accessKey:      testAccessKey,
			secretKey:      "wrongsecretkey123456789012345678901234",
			expectedErr:    s3err.ErrSignatureDoesNotMatch,
			expectIdentity: false,
		},
	}

	for _, tt := range tests {
		tt := tt // capture range variable
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
			signV4Request(req, tt.accessKey, tt.secretKey, testRegion, testService, tt.signedHeaders)

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

func TestV4Verifier_ExtractAuthInfo(t *testing.T) {
	t.Parallel()

	verifier := NewV4Verifier(nil)

	tests := []struct {
		name          string
		authHeader    string
		amzDate       string
		expectError   bool
		expectAccess  string
		expectRegion  string
		expectService string
	}{
		{
			name:          "valid authorization header",
			authHeader:    "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host;x-amz-date, Signature=abcd1234",
			amzDate:       "20130524T000000Z",
			expectError:   false,
			expectAccess:  "AKIAIOSFODNN7EXAMPLE",
			expectRegion:  "us-east-1",
			expectService: "s3",
		},
		{
			name:        "missing authorization header",
			authHeader:  "",
			amzDate:     "20130524T000000Z",
			expectError: true,
		},
		{
			name:        "invalid authorization prefix",
			authHeader:  "AWS AccessKey:Signature",
			amzDate:     "20130524T000000Z",
			expectError: true,
		},
		{
			name:        "missing x-amz-date header",
			authHeader:  "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, SignedHeaders=host, Signature=abcd1234",
			amzDate:     "",
			expectError: true,
		},
		{
			name:        "invalid credential format",
			authHeader:  "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/invalid, SignedHeaders=host, Signature=abcd1234",
			amzDate:     "20130524T000000Z",
			expectError: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("GET", "http://s3.amazonaws.com/bucket", nil)
			if tt.authHeader != "" {
				req.Header.Set("Authorization", tt.authHeader)
			}
			if tt.amzDate != "" {
				req.Header.Set("X-Amz-Date", tt.amzDate)
			}

			auth, err := verifier.extractAuthInfo(req)

			if tt.expectError {
				assert.Error(t, err)
				assert.Nil(t, auth)
			} else {
				require.NoError(t, err)
				require.NotNil(t, auth)
				assert.Equal(t, tt.expectAccess, auth.accessKey)
				assert.Equal(t, tt.expectRegion, auth.region)
				assert.Equal(t, tt.expectService, auth.service)
			}
		})
	}
}

func TestV4Verifier_BuildCanonicalRequest(t *testing.T) {
	t.Parallel()

	verifier := NewV4Verifier(nil)

	tests := []struct {
		name          string
		method        string
		path          string
		query         string
		headers       map[string]string
		host          string
		signedHeaders []string
		expectContain []string
	}{
		{
			name:          "simple GET request",
			method:        "GET",
			path:          "/bucket/key",
			query:         "",
			headers:       map[string]string{"X-Amz-Content-Sha256": HashedEmptyPayload},
			host:          "s3.amazonaws.com",
			signedHeaders: []string{"host", "x-amz-content-sha256"},
			expectContain: []string{"GET", "/bucket/key", "host:s3.amazonaws.com"},
		},
		{
			name:          "GET with query parameters",
			method:        "GET",
			path:          "/bucket",
			query:         "list-type=2&prefix=test",
			headers:       map[string]string{"X-Amz-Content-Sha256": HashedEmptyPayload},
			host:          "s3.amazonaws.com",
			signedHeaders: []string{"host", "x-amz-content-sha256"},
			expectContain: []string{"GET", "/bucket", "list-type=2", "prefix=test"},
		},
		{
			name:          "PUT with content-type",
			method:        "PUT",
			path:          "/bucket/key",
			query:         "",
			headers:       map[string]string{"Content-Type": "text/plain", "X-Amz-Content-Sha256": HashedEmptyPayload},
			host:          "s3.amazonaws.com",
			signedHeaders: []string{"content-type", "host", "x-amz-content-sha256"},
			expectContain: []string{"PUT", "/bucket/key", "content-type:text/plain"},
		},
		{
			name:          "empty path defaults to /",
			method:        "GET",
			path:          "",
			query:         "",
			headers:       map[string]string{"X-Amz-Content-Sha256": HashedEmptyPayload},
			host:          "s3.amazonaws.com",
			signedHeaders: []string{"host", "x-amz-content-sha256"},
			expectContain: []string{"GET\n/\n"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			urlStr := "http://" + tt.host + tt.path
			if tt.query != "" {
				urlStr += "?" + tt.query
			}

			req := httptest.NewRequest(tt.method, urlStr, nil)
			req.Host = tt.host
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			canonical, err := verifier.buildCanonicalRequest(req, tt.signedHeaders)
			require.NoError(t, err)

			for _, expected := range tt.expectContain {
				assert.Contains(t, canonical, expected, "canonical request should contain: %s", expected)
			}
		})
	}
}

func TestV4Verifier_DeriveSigningKey(t *testing.T) {
	t.Parallel()

	verifier := NewV4Verifier(nil)

	tests := []struct {
		name      string
		secretKey string
		date      string
		region    string
		service   string
	}{
		{
			name:      "standard signing key derivation",
			secretKey: testSecretKey,
			date:      "20130524",
			region:    "us-east-1",
			service:   "s3",
		},
		{
			name:      "different region",
			secretKey: testSecretKey,
			date:      "20231215",
			region:    "eu-west-1",
			service:   "s3",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			key := verifier.deriveSigningKey(tt.secretKey, tt.date, tt.region, tt.service)

			// Key should be 32 bytes (SHA256 output)
			assert.Len(t, key, 32, "signing key should be 32 bytes")

			// Same inputs should produce same key
			key2 := verifier.deriveSigningKey(tt.secretKey, tt.date, tt.region, tt.service)
			assert.Equal(t, key, key2, "same inputs should produce same key")

			// Different date should produce different key
			keyDifferentDate := verifier.deriveSigningKey(tt.secretKey, "20991231", tt.region, tt.service)
			assert.NotEqual(t, key, keyDifferentDate, "different date should produce different key")
		})
	}
}

func TestV4Verifier_CalculateSignature(t *testing.T) {
	t.Parallel()

	verifier := NewV4Verifier(nil)

	tests := []struct {
		name         string
		signingKey   []byte
		stringToSign string
	}{
		{
			name:         "simple signature",
			signingKey:   []byte("test-signing-key-32bytes-000000"),
			stringToSign: "AWS4-HMAC-SHA256\n20130524T000000Z\n20130524/us-east-1/s3/aws4_request\nhashedcanonicalrequest",
		},
		{
			name:         "empty string to sign",
			signingKey:   []byte("test-signing-key-32bytes-000000"),
			stringToSign: "",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sig := verifier.calculateSignature(tt.signingKey, tt.stringToSign)

			// Signature should be hex-encoded (64 chars for SHA256)
			assert.Len(t, sig, 64, "signature should be 64 hex characters")

			// Should only contain hex characters
			for _, c := range sig {
				assert.True(t, (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'),
					"signature should only contain hex characters")
			}

			// Same inputs should produce same signature
			sig2 := verifier.calculateSignature(tt.signingKey, tt.stringToSign)
			assert.Equal(t, sig, sig2, "same inputs should produce same signature")
		})
	}
}

func TestV4Verifier_BuildCanonicalQueryString(t *testing.T) {
	t.Parallel()

	verifier := NewV4Verifier(nil)

	tests := []struct {
		name     string
		query    url.Values
		expected string
	}{
		{
			name:     "empty query",
			query:    url.Values{},
			expected: "",
		},
		{
			name:     "single parameter",
			query:    url.Values{"prefix": {"test"}},
			expected: "prefix=test",
		},
		{
			name:     "multiple parameters sorted",
			query:    url.Values{"prefix": {"test"}, "delimiter": {"/"}},
			expected: "delimiter=%2F&prefix=test",
		},
		{
			name:     "excludes X-Amz-Signature",
			query:    url.Values{"prefix": {"test"}, "X-Amz-Signature": {"abc123"}},
			expected: "prefix=test",
		},
		{
			name:     "special characters encoded",
			query:    url.Values{"key": {"hello world"}},
			expected: "key=hello+world",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := verifier.buildCanonicalQueryString(tt.query)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestV4Verifier_BuildCanonicalHeaders(t *testing.T) {
	t.Parallel()

	verifier := NewV4Verifier(nil)

	tests := []struct {
		name          string
		host          string
		headers       map[string]string
		signedHeaders []string
		expectContain []string
	}{
		{
			name:          "host header from r.Host",
			host:          "s3.amazonaws.com",
			headers:       map[string]string{},
			signedHeaders: []string{"host"},
			expectContain: []string{"host:s3.amazonaws.com"},
		},
		{
			name:          "multiple headers sorted",
			host:          "s3.amazonaws.com",
			headers:       map[string]string{"X-Amz-Date": "20130524T000000Z", "Content-Type": "text/plain"},
			signedHeaders: []string{"content-type", "host", "x-amz-date"},
			expectContain: []string{"content-type:text/plain", "host:s3.amazonaws.com", "x-amz-date:20130524T000000Z"},
		},
		{
			name:          "trailing newline",
			host:          "s3.amazonaws.com",
			headers:       map[string]string{},
			signedHeaders: []string{"host"},
			expectContain: []string{"host:s3.amazonaws.com\n"},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("GET", "http://"+tt.host+"/bucket", nil)
			req.Host = tt.host
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			result, sortedHeaders := verifier.buildCanonicalHeaders(req, tt.signedHeaders)

			for _, expected := range tt.expectContain {
				assert.Contains(t, result, expected)
			}

			// Verify sorted headers are returned in alphabetical order
			for i := 1; i < len(sortedHeaders); i++ {
				assert.True(t, sortedHeaders[i-1] <= sortedHeaders[i],
					"sorted headers should be in alphabetical order")
			}
		})
	}
}

func TestConstantTimeCompare(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		a        string
		b        string
		expected bool
	}{
		{
			name:     "equal strings",
			a:        "abc123",
			b:        "abc123",
			expected: true,
		},
		{
			name:     "different strings",
			a:        "abc123",
			b:        "xyz789",
			expected: false,
		},
		{
			name:     "different lengths",
			a:        "short",
			b:        "longer string",
			expected: false,
		},
		{
			name:     "empty strings",
			a:        "",
			b:        "",
			expected: true,
		},
		{
			name:     "one empty",
			a:        "notempty",
			b:        "",
			expected: false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			result := constantTimeCompare(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// signStreamingV4Request signs a streaming request using AWS Signature V4
// This mimics how the AWS SDK signs chunked uploads with STREAMING-AWS4-HMAC-SHA256-PAYLOAD
func signStreamingV4Request(req *http.Request, accessKey, secretKey, region, service string, signedHeaders []string, decodedLength int64) {
	// Use current time for signing
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format(Iso8601BasicFormat)

	// Set required headers for streaming
	req.Header.Set("X-Amz-Date", amzDate)
	req.Header.Set("X-Amz-Content-Sha256", StreamingPayload)
	req.Header.Set("Content-Encoding", "aws-chunked")
	req.Header.Set("X-Amz-Decoded-Content-Length", strconv.FormatInt(decodedLength, 10))

	if req.Host == "" {
		req.Host = req.URL.Host
	}

	// Sort signed headers for consistency
	sortedHeaders := make([]string, len(signedHeaders))
	copy(sortedHeaders, signedHeaders)
	sort.Strings(sortedHeaders)

	// Build canonical request
	canonicalURI := req.URL.Path
	if canonicalURI == "" {
		canonicalURI = "/"
	}

	// Build canonical query string
	canonicalQuery := buildTestCanonicalQueryString(req.URL.Query())

	// Build canonical headers
	canonicalHeaders := ""
	for _, h := range sortedHeaders {
		var val string
		switch h {
		case "host":
			val = req.Host
		case "content-length":
			val = strconv.FormatInt(req.ContentLength, 10)
		default:
			val = req.Header.Get(h)
		}
		canonicalHeaders += h + ":" + val + "\n"
	}

	signedHeadersStr := strings.Join(sortedHeaders, ";")

	canonicalRequest := req.Method + "\n" +
		canonicalURI + "\n" +
		canonicalQuery + "\n" +
		canonicalHeaders + "\n" +
		signedHeadersStr + "\n" +
		StreamingPayload

	// Hash canonical request
	h := sha256.New()
	h.Write([]byte(canonicalRequest))
	hashedCanonicalRequest := hex.EncodeToString(h.Sum(nil))

	// Build credential scope
	credentialScope := dateStamp + "/" + region + "/" + service + "/aws4_request"

	// Build string to sign
	stringToSign := AuthHeaderV4 + "\n" +
		amzDate + "\n" +
		credentialScope + "\n" +
		hashedCanonicalRequest

	// Derive signing key
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	// Calculate signature
	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	// Build Authorization header
	authHeader := AuthHeaderV4 + " " +
		"Credential=" + accessKey + "/" + credentialScope + ", " +
		"SignedHeaders=" + signedHeadersStr + ", " +
		"Signature=" + signature

	req.Header.Set("Authorization", authHeader)
}

func TestV4Verifier_VerifyStreamingRequest(t *testing.T) {
	t.Parallel()

	manager := createTestManager(t)
	verifier := NewV4Verifier(manager)

	tests := []struct {
		name           string
		method         string
		path           string
		contentLength  int64 // Total size including chunk overhead
		decodedLength  int64 // Actual object size
		headers        map[string]string
		signedHeaders  []string
		accessKey      string
		secretKey      string
		expectedErr    s3err.ErrorCode
		expectIdentity bool
	}{
		{
			name:          "valid streaming PUT request",
			method:        "PUT",
			path:          "/test-bucket/test-key",
			contentLength: 1048756, // Includes chunk signature overhead
			decodedLength: 1048576, // 1 MiB actual data
			headers:       map[string]string{},
			signedHeaders: []string{
				"content-encoding",
				"content-length",
				"host",
				"x-amz-content-sha256",
				"x-amz-date",
				"x-amz-decoded-content-length",
			},
			accessKey:      testAccessKey,
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrNone,
			expectIdentity: true,
		},
		{
			name:          "streaming request with storage class",
			method:        "PUT",
			path:          "/test-bucket/test-key",
			contentLength: 2097332,
			decodedLength: 2097152, // 2 MiB
			headers: map[string]string{
				"X-Amz-Storage-Class": "STANDARD",
			},
			signedHeaders: []string{
				"content-encoding",
				"content-length",
				"host",
				"x-amz-content-sha256",
				"x-amz-date",
				"x-amz-decoded-content-length",
				"x-amz-storage-class",
			},
			accessKey:      testAccessKey,
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrNone,
			expectIdentity: true,
		},
		{
			name:          "streaming request unsorted headers (client bug simulation)",
			method:        "PUT",
			path:          "/test-bucket/test-key",
			contentLength: 1048756,
			decodedLength: 1048576,
			headers:       map[string]string{},
			// Headers in wrong order - our code should still sort them
			signedHeaders: []string{
				"x-amz-date",
				"host",
				"content-encoding",
				"x-amz-content-sha256",
				"content-length",
				"x-amz-decoded-content-length",
			},
			accessKey:      testAccessKey,
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrNone,
			expectIdentity: true,
		},
		{
			name:          "invalid access key",
			method:        "PUT",
			path:          "/test-bucket/test-key",
			contentLength: 1048756,
			decodedLength: 1048576,
			headers:       map[string]string{},
			signedHeaders: []string{
				"content-encoding",
				"content-length",
				"host",
				"x-amz-content-sha256",
				"x-amz-date",
				"x-amz-decoded-content-length",
			},
			accessKey:      "INVALIDACCESSKEY123",
			secretKey:      testSecretKey,
			expectedErr:    s3err.ErrInvalidAccessKeyID,
			expectIdentity: false,
		},
		{
			name:          "wrong secret key",
			method:        "PUT",
			path:          "/test-bucket/test-key",
			contentLength: 1048756,
			decodedLength: 1048576,
			headers:       map[string]string{},
			signedHeaders: []string{
				"content-encoding",
				"content-length",
				"host",
				"x-amz-content-sha256",
				"x-amz-date",
				"x-amz-decoded-content-length",
			},
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
			req.ContentLength = tt.contentLength

			// Set custom headers
			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			// Sign the request
			signStreamingV4Request(req, tt.accessKey, tt.secretKey, testRegion, testService, tt.signedHeaders, tt.decodedLength)

			// Verify request
			result, errCode := verifier.VerifyStreamingRequest(req)

			assert.Equal(t, tt.expectedErr, errCode, "error code mismatch")

			if tt.expectIdentity {
				require.NotNil(t, result, "expected result")
				require.NotNil(t, result.Identity, "expected identity")
				assert.Equal(t, "testuser", result.Identity.Name)
				assert.NotEmpty(t, result.SigningKey, "signing key should be set")
				assert.NotEmpty(t, result.SeedSignature, "seed signature should be set")
				assert.NotEmpty(t, result.Timestamp, "timestamp should be set")
				assert.Equal(t, testRegion, result.Region)
				assert.Equal(t, testService, result.Service)
			} else {
				assert.Nil(t, result, "expected no result")
			}
		})
	}
}

func TestV4Verifier_ContentLengthHandling(t *testing.T) {
	t.Parallel()

	verifier := NewV4Verifier(nil)

	tests := []struct {
		name                 string
		contentLengthHeader  string
		requestContentLength int64
		signedHeaders        []string
		expectInCanonical    bool
		expectValue          string
	}{
		{
			name:                 "content-length from header",
			contentLengthHeader:  "12345",
			requestContentLength: 12345,
			signedHeaders:        []string{"content-length", "host"},
			expectInCanonical:    true,
			expectValue:          "12345",
		},
		{
			name:                 "content-length from r.ContentLength when header missing",
			contentLengthHeader:  "", // Header not set
			requestContentLength: 67890,
			signedHeaders:        []string{"content-length", "host"},
			expectInCanonical:    true,
			expectValue:          "67890",
		},
		{
			name:                 "content-length not signed",
			contentLengthHeader:  "12345",
			requestContentLength: 12345,
			signedHeaders:        []string{"host"},
			expectInCanonical:    false,
			expectValue:          "",
		},
		{
			name:                 "zero content-length",
			contentLengthHeader:  "0",
			requestContentLength: 0,
			signedHeaders:        []string{"content-length", "host"},
			expectInCanonical:    true,
			expectValue:          "0",
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("PUT", "http://s3.amazonaws.com/bucket/key", nil)
			req.Host = "s3.amazonaws.com"
			req.ContentLength = tt.requestContentLength

			if tt.contentLengthHeader != "" {
				req.Header.Set("Content-Length", tt.contentLengthHeader)
			}

			result, sortedHeaders := verifier.buildCanonicalHeaders(req, tt.signedHeaders)

			if tt.expectInCanonical {
				assert.Contains(t, result, "content-length:"+tt.expectValue,
					"canonical headers should contain content-length")
				assert.Contains(t, sortedHeaders, "content-length",
					"sorted headers should include content-length")
			} else {
				assert.NotContains(t, result, "content-length:",
					"canonical headers should not contain content-length")
			}
		})
	}
}

func TestV4Verifier_SignedHeadersSorting(t *testing.T) {
	t.Parallel()

	verifier := NewV4Verifier(nil)

	tests := []struct {
		name           string
		signedHeaders  []string
		expectedOrder  []string
		headers        map[string]string
	}{
		{
			name:          "already sorted headers",
			signedHeaders: []string{"content-encoding", "host", "x-amz-date"},
			expectedOrder: []string{"content-encoding", "host", "x-amz-date"},
			headers: map[string]string{
				"Content-Encoding": "aws-chunked",
				"X-Amz-Date":       "20231215T000000Z",
			},
		},
		{
			name:          "unsorted headers",
			signedHeaders: []string{"x-amz-date", "host", "content-encoding"},
			expectedOrder: []string{"content-encoding", "host", "x-amz-date"},
			headers: map[string]string{
				"Content-Encoding": "aws-chunked",
				"X-Amz-Date":       "20231215T000000Z",
			},
		},
		{
			name:          "reverse sorted headers",
			signedHeaders: []string{"z-custom", "x-amz-date", "host", "content-type", "accept"},
			expectedOrder: []string{"accept", "content-type", "host", "x-amz-date", "z-custom"},
			headers: map[string]string{
				"Accept":       "application/json",
				"Content-Type": "application/octet-stream",
				"X-Amz-Date":   "20231215T000000Z",
				"Z-Custom":     "value",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest("PUT", "http://s3.amazonaws.com/bucket/key", nil)
			req.Host = "s3.amazonaws.com"

			for k, v := range tt.headers {
				req.Header.Set(k, v)
			}

			_, sortedHeaders := verifier.buildCanonicalHeaders(req, tt.signedHeaders)

			assert.Equal(t, tt.expectedOrder, sortedHeaders,
				"headers should be sorted alphabetically")
		})
	}
}

func TestV4Verifier_StreamingSignatureWithTrailers(t *testing.T) {
	t.Parallel()

	manager := createTestManager(t)
	verifier := NewV4Verifier(manager)

	// Test with trailing checksum headers (STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER)
	// Note: This tests the initial request verification only, not the trailer signature
	req := httptest.NewRequest("PUT", "http://s3.amazonaws.com/test-bucket/test-key", nil)
	req.Host = "s3.amazonaws.com"
	req.ContentLength = 1048756
	req.Header.Set("X-Amz-Trailer", "x-amz-checksum-crc32c")

	signedHeaders := []string{
		"content-encoding",
		"content-length",
		"host",
		"x-amz-content-sha256",
		"x-amz-date",
		"x-amz-decoded-content-length",
		"x-amz-trailer",
	}

	signStreamingV4Request(req, testAccessKey, testSecretKey, testRegion, testService, signedHeaders, 1048576)

	result, errCode := verifier.VerifyStreamingRequest(req)

	assert.Equal(t, s3err.ErrNone, errCode)
	require.NotNil(t, result)
	assert.NotNil(t, result.Identity)
	assert.NotEmpty(t, result.SigningKey)
}
