// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package signature

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// AWS Signature Version 4 implementation following:
// https://docs.aws.amazon.com/general/latest/gr/signature-version-4.html

// V4Verifier verifies AWS Signature Version 4 authentication
type V4Verifier struct {
	iamManager *iam.Manager
}

// NewV4Verifier creates a new signature v4 verifier
func NewV4Verifier(iamManager *iam.Manager) *V4Verifier {
	return &V4Verifier{
		iamManager: iamManager,
	}
}

// authInfo contains parsed authentication information from request
type authInfo struct {
	accessKey       string
	date            string // YYYYMMDD format from credential scope
	timestamp       string // Full ISO8601 timestamp (YYYYMMDDTHHMMSSZ)
	region          string
	service         string
	signedHeaders   []string
	signature       string
	credentialScope string
}

// VerifyRequest verifies AWS Signature V4 for a request
// Returns the authenticated identity or an error code
func (v *V4Verifier) VerifyRequest(r *http.Request) (*iam.Identity, s3err.ErrorCode) {
	// 1. Extract authentication info from Authorization header or query string
	auth, err := v.extractAuthInfo(r)
	if err != nil {
		return nil, s3err.ErrAccessDenied
	}

	// 2. Lookup credentials by access key
	identity, credential, found := v.iamManager.LookupByAccessKey(r.Context(), auth.accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// 3. Build canonical request
	canonicalReq, err := v.buildCanonicalRequest(r, auth.signedHeaders)
	if err != nil {
		return nil, s3err.ErrSignatureDoesNotMatch
	}

	// 4. Build string to sign
	stringToSign := v.buildStringToSign(auth, canonicalReq)

	// 5. Calculate expected signature
	signingKey := v.deriveSigningKey(credential.SecretKey, auth.date, auth.region, auth.service)
	expectedSig := v.calculateSignature(signingKey, stringToSign)

	// 6. Compare signatures (constant time to prevent timing attacks)
	if !constantTimeCompare(auth.signature, expectedSig) {
		return nil, s3err.ErrSignatureDoesNotMatch
	}

	return identity, s3err.ErrNone
}

// extractAuthInfo parses authentication info from Authorization header or query params
func (v *V4Verifier) extractAuthInfo(r *http.Request) (*authInfo, error) {
	// Check for query-based presigned URL first
	if accessKey := r.URL.Query().Get("X-Amz-Credential"); accessKey != "" {
		return v.extractPresignedAuthInfo(r)
	}

	// Parse Authorization header: "AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=..."
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, AuthHeaderV4) {
		return nil, fmt.Errorf("invalid authorization header")
	}

	parts := strings.Split(strings.TrimPrefix(authHeader, AuthHeaderV4+" "), ", ")
	auth := &authInfo{}

	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			continue
		}

		switch kv[0] {
		case "Credential":
			// Format: accessKey/date/region/service/aws4_request
			credParts := strings.Split(kv[1], "/")
			if len(credParts) != 5 {
				return nil, fmt.Errorf("invalid credential format")
			}
			auth.accessKey = credParts[0]
			auth.date = credParts[1]
			auth.region = credParts[2]
			auth.service = credParts[3]
			auth.credentialScope = strings.Join(credParts[1:], "/")

		case "SignedHeaders":
			auth.signedHeaders = strings.Split(kv[1], ";")

		case "Signature":
			auth.signature = kv[1]
		}
	}

	if auth.accessKey == "" || auth.signature == "" {
		return nil, fmt.Errorf("missing required auth fields")
	}

	// Get timestamp from X-Amz-Date header (required for signature verification)
	auth.timestamp = r.Header.Get("X-Amz-Date")
	if auth.timestamp == "" {
		// Fall back to Date header if X-Amz-Date is not present
		if dateHeader := r.Header.Get("Date"); dateHeader != "" {
			// Parse HTTP date format and convert to ISO8601
			if t, err := time.Parse(time.RFC1123, dateHeader); err == nil {
				auth.timestamp = t.UTC().Format(Iso8601BasicFormat)
			}
		}
	}
	if auth.timestamp == "" {
		return nil, fmt.Errorf("missing X-Amz-Date header")
	}

	return auth, nil
}

// extractPresignedAuthInfo parses presigned URL query parameters
func (v *V4Verifier) extractPresignedAuthInfo(r *http.Request) (*authInfo, error) {
	q := r.URL.Query()

	// Parse credential: accessKey/date/region/service/aws4_request
	credStr := q.Get("X-Amz-Credential")
	credParts := strings.Split(credStr, "/")
	if len(credParts) != 5 {
		return nil, fmt.Errorf("invalid credential format")
	}

	// Get timestamp from X-Amz-Date query parameter
	timestamp := q.Get("X-Amz-Date")
	if timestamp == "" {
		return nil, fmt.Errorf("missing X-Amz-Date parameter")
	}

	auth := &authInfo{
		accessKey:       credParts[0],
		date:            credParts[1],
		timestamp:       timestamp,
		region:          credParts[2],
		service:         credParts[3],
		credentialScope: strings.Join(credParts[1:], "/"),
		signedHeaders:   strings.Split(q.Get("X-Amz-SignedHeaders"), ";"),
		signature:       q.Get("X-Amz-Signature"),
	}

	// Validate expiration
	if expiresStr := q.Get("X-Amz-Expires"); expiresStr != "" {
		signTime, err := time.Parse(Iso8601BasicFormat, timestamp)
		if err == nil {
			// Check expiration (expires is in seconds)
			expires, _ := strconv.ParseInt(expiresStr, 10, 64)
			if time.Since(signTime) > time.Duration(expires)*time.Second {
				return nil, fmt.Errorf("presigned URL expired")
			}
		}
	}

	return auth, nil
}

// buildCanonicalRequest creates the canonical request string per AWS spec
func (v *V4Verifier) buildCanonicalRequest(r *http.Request, signedHeaders []string) (string, error) {
	// 1. HTTP Method
	method := r.Method

	// 2. Canonical URI (URL-encoded path)
	// AWS Signature V4 requires the canonical URI to be URL-encoded.
	// Go's HTTP server automatically decodes req.URL.Path, so we need to use
	// req.URL.RawPath if available (contains the original encoded path), or
	// re-encode each path segment.
	canonicalURI := r.URL.RawPath
	if canonicalURI == "" {
		// RawPath not available, need to re-encode the path
		// Split path into segments and encode each segment separately
		// (slashes should remain as path separators)
		path := r.URL.Path
		if path == "" {
			path = "/"
		}
		canonicalURI = encodeCanonicalURI(path)
	} else {
		// Use RawPath but ensure it starts with /
		if canonicalURI == "" {
			canonicalURI = "/"
		}
	}

	// 3. Canonical Query String (sorted)
	canonicalQuery := v.buildCanonicalQueryString(r.URL.Query())

	// 4. Canonical Headers (sorted, lowercase)
	canonicalHeaders, sortedSignedHeaders := v.buildCanonicalHeaders(r, signedHeaders)

	// 5. Signed Headers (sorted, lowercase, semicolon-separated)
	// Must be in the same order as they appear in the canonical headers
	signedHeadersStr := strings.Join(sortedSignedHeaders, ";")

	// 6. Hashed Payload
	hashedPayload := r.Header.Get("X-Amz-Content-Sha256")
	if hashedPayload == "" {
		hashedPayload = HashedEmptyPayload
	}

	// Canonical request format:
	// HTTPMethod + "\n" +
	// CanonicalURI + "\n" +
	// CanonicalQueryString + "\n" +
	// CanonicalHeaders + "\n" +
	// SignedHeaders + "\n" +
	// HashedPayload
	canonical := strings.Join([]string{
		method,
		canonicalURI,
		canonicalQuery,
		canonicalHeaders,
		signedHeadersStr,
		hashedPayload,
	}, "\n")

	return canonical, nil
}

// buildCanonicalQueryString creates sorted canonical query string
func (v *V4Verifier) buildCanonicalQueryString(query url.Values) string {
	// For presigned URLs, exclude signature from canonical query
	filtered := url.Values{}
	for k, vals := range query {
		if k == "X-Amz-Signature" {
			continue
		}
		filtered[k] = vals
	}

	// Sort keys
	keys := make([]string, 0, len(filtered))
	for k := range filtered {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	// Build query string
	var parts []string
	for _, k := range keys {
		vals := filtered[k]
		sort.Strings(vals)
		for _, v := range vals {
			parts = append(parts, fmt.Sprintf("%s=%s", url.QueryEscape(k), url.QueryEscape(v)))
		}
	}

	return strings.Join(parts, "&")
}

// buildCanonicalHeaders creates sorted canonical headers string and returns
// the sorted list of header names for use in the signed headers list.
func (v *V4Verifier) buildCanonicalHeaders(r *http.Request, signedHeaders []string) (string, []string) {
	headers := make(map[string][]string)

	// Collect values for signed headers
	for _, h := range signedHeaders {
		h = strings.ToLower(strings.TrimSpace(h))

		// Special case: Host header is stored in r.Host, not r.Header
		if h == "host" {
			if r.Host != "" {
				headers[h] = []string{r.Host}
			}
			continue
		}

		// Special case: Content-Length is stored in r.ContentLength by Go's HTTP server
		// and may not be present in r.Header for some request types.
		// For streaming uploads with Content-Encoding: aws-chunked, Content-Length
		// represents the total size including chunk metadata.
		if h == "content-length" {
			// First try to get from header (preferred, as it's the original value)
			if vals := r.Header.Values(h); len(vals) > 0 {
				headers[h] = vals
			} else if r.ContentLength >= 0 {
				// Fall back to r.ContentLength if header not present
				headers[h] = []string{strconv.FormatInt(r.ContentLength, 10)}
			}
			continue
		}

		if vals := r.Header.Values(h); len(vals) > 0 {
			headers[h] = vals
		}
	}

	// Sort header names
	names := make([]string, 0, len(headers))
	for name := range headers {
		names = append(names, name)
	}
	sort.Strings(names)

	// Build canonical headers
	var parts []string
	for _, name := range names {
		vals := headers[name]
		// Trim and join multiple values
		trimmed := make([]string, len(vals))
		for i, v := range vals {
			trimmed[i] = strings.TrimSpace(v)
		}
		parts = append(parts, fmt.Sprintf("%s:%s", name, strings.Join(trimmed, ",")))
	}

	return strings.Join(parts, "\n") + "\n", names
}

// buildStringToSign creates the string to sign per AWS spec
func (v *V4Verifier) buildStringToSign(auth *authInfo, canonicalRequest string) string {
	// Hash the canonical request
	h := utils.Sha256PoolGetHasher()
	h.Write([]byte(canonicalRequest))
	hashedRequest := hex.EncodeToString(h.Sum(nil))
	utils.Sha256PoolPutHasher(h)

	// String to sign format:
	// Algorithm + "\n" +
	// RequestDateTime + "\n" +
	// CredentialScope + "\n" +
	// HashedCanonicalRequest
	return strings.Join([]string{
		AuthHeaderV4,
		auth.timestamp,
		auth.credentialScope,
		hashedRequest,
	}, "\n")
}

// deriveSigningKey derives the signing key using HMAC-SHA256 chain
func (v *V4Verifier) deriveSigningKey(secretKey, date, region, service string) []byte {
	// kSecret = "AWS4" + SecretKey
	// kDate = HMAC("AWS4" + SecretKey, Date)
	// kRegion = HMAC(kDate, Region)
	// kService = HMAC(kRegion, Service)
	// kSigning = HMAC(kService, "aws4_request")

	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	return kSigning
}

// calculateSignature computes the final signature
func (v *V4Verifier) calculateSignature(signingKey []byte, stringToSign string) string {
	signature := hmacSHA256(signingKey, []byte(stringToSign))
	return hex.EncodeToString(signature)
}

// hmacSHA256 computes HMAC-SHA256
func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}

// constantTimeCompare performs constant-time string comparison to prevent timing attacks
func constantTimeCompare(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}

// StreamingAuthResult contains the authentication result and signing context
// needed for verifying chunked upload signatures.
type StreamingAuthResult struct {
	Identity      *iam.Identity
	SigningKey    []byte // Derived signing key for chunk verification
	SeedSignature string // Initial request signature (seed for chunk chain)
	Timestamp     string // ISO8601 timestamp from request
	Region        string
	Service       string
}

// VerifyStreamingRequest verifies the initial request signature for streaming uploads
// and returns the signing context needed for chunk-level verification.
func (v *V4Verifier) VerifyStreamingRequest(r *http.Request) (*StreamingAuthResult, s3err.ErrorCode) {
	// 1. Extract authentication info
	auth, err := v.extractAuthInfo(r)
	if err != nil {
		return nil, s3err.ErrAccessDenied
	}

	// 2. Lookup credentials by access key
	identity, credential, found := v.iamManager.LookupByAccessKey(r.Context(), auth.accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// 3. Build canonical request
	canonicalReq, err := v.buildCanonicalRequest(r, auth.signedHeaders)
	if err != nil {
		return nil, s3err.ErrSignatureDoesNotMatch
	}

	// 4. Build string to sign
	stringToSign := v.buildStringToSign(auth, canonicalReq)

	// 5. Derive signing key
	signingKey := v.deriveSigningKey(credential.SecretKey, auth.date, auth.region, auth.service)

	// 6. Calculate expected signature
	expectedSig := v.calculateSignature(signingKey, stringToSign)

	// 7. Compare signatures (constant time)
	if !constantTimeCompare(auth.signature, expectedSig) {
		return nil, s3err.ErrSignatureDoesNotMatch
	}

	return &StreamingAuthResult{
		Identity:      identity,
		SigningKey:    signingKey,
		SeedSignature: auth.signature,
		Timestamp:     auth.timestamp,
		Region:        auth.region,
		Service:       auth.service,
	}, s3err.ErrNone
}
