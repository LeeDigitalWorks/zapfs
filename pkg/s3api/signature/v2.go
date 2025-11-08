package signature

import (
	"crypto/hmac"
	"crypto/sha1"
	"crypto/subtle"
	"encoding/base64"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"zapfs/pkg/iam"
	"zapfs/pkg/s3api/s3consts"
	"zapfs/pkg/s3api/s3err"
)

// AWS Signature Version 2 implementation following:
// https://docs.aws.amazon.com/AmazonS3/latest/userguide/RESTAuthentication.html

// V2Verifier verifies AWS Signature Version 2 authentication
type V2Verifier struct {
	iamManager *iam.Manager
}

// NewV2Verifier creates a new signature v2 verifier
func NewV2Verifier(iamManager *iam.Manager) *V2Verifier {
	return &V2Verifier{
		iamManager: iamManager,
	}
}

// v2AuthInfo contains parsed v2 authentication information from request
type v2AuthInfo struct {
	accessKey   string
	signature   string
	isPresigned bool
	expires     string
}

// VerifyRequest verifies AWS Signature V2 for a request
// Returns the authenticated identity or an error code
func (v *V2Verifier) VerifyRequest(r *http.Request) (*iam.Identity, s3err.ErrorCode) {
	// 1. Extract authentication info from Authorization header or query string
	auth, err := v.extractV2AuthInfo(r)
	if err != nil {
		return nil, s3err.ErrAccessDenied
	}

	// 2. Check if presigned URL has expired
	if auth.isPresigned && auth.expires != "" {
		var expiresTime int64
		fmt.Sscanf(auth.expires, "%d", &expiresTime)
		if time.Now().Unix() > expiresTime {
			return nil, s3err.ErrExpiredPresignRequest
		}
	}

	// 3. Lookup credentials by access key
	identity, credential, found := v.iamManager.LookupByAccessKey(r.Context(), auth.accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// 4. Build string to sign
	stringToSign := v.buildStringToSign(r, auth)

	// 5. Calculate expected signature
	expectedSig := v.calculateSignature(credential.SecretKey, stringToSign)

	// 6. Compare signatures (constant time to prevent timing attacks)
	if !constantTimeCompareV2(auth.signature, expectedSig) {
		return nil, s3err.ErrSignatureDoesNotMatch
	}

	return identity, s3err.ErrNone
}

// extractV2AuthInfo parses authentication info from Authorization header or query params
func (v *V2Verifier) extractV2AuthInfo(r *http.Request) (*v2AuthInfo, error) {
	// Check for query-based presigned URL first
	if accessKey := r.URL.Query().Get("AWSAccessKeyId"); accessKey != "" {
		return &v2AuthInfo{
			accessKey:   accessKey,
			signature:   r.URL.Query().Get("Signature"),
			isPresigned: true,
			expires:     r.URL.Query().Get("Expires"),
		}, nil
	}

	// Parse Authorization header: "AWS AccessKeyId:Signature"
	authHeader := r.Header.Get("Authorization")
	if !strings.HasPrefix(authHeader, AuthHeaderV2+" ") {
		return nil, fmt.Errorf("invalid authorization header")
	}

	// Extract "AccessKeyId:Signature"
	authValue := strings.TrimPrefix(authHeader, AuthHeaderV2+" ")
	parts := strings.SplitN(authValue, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid authorization format")
	}

	return &v2AuthInfo{
		accessKey:   parts[0],
		signature:   parts[1],
		isPresigned: false,
	}, nil
}

// buildStringToSign creates the string to sign per AWS Signature V2 spec
// Format:
// HTTP-Verb + "\n" +
// Content-MD5 + "\n" +
// Content-Type + "\n" +
// Date + "\n" +
// CanonicalizedAmzHeaders +
// CanonicalizedResource
func (v *V2Verifier) buildStringToSign(r *http.Request, auth *v2AuthInfo) string {
	// 1. HTTP Verb
	verb := r.Method

	// 2. Content-MD5
	contentMD5 := r.Header.Get("Content-MD5")

	// 3. Content-Type
	contentType := r.Header.Get("Content-Type")

	// 4. Date - use Expires for presigned URLs, otherwise use Date or x-amz-date
	var date string
	if auth.isPresigned {
		date = auth.expires
	} else {
		// x-amz-date takes precedence over Date header
		if amzDate := r.Header.Get(s3consts.XAmzDate); amzDate != "" {
			date = ""
		} else {
			date = r.Header.Get("Date")
		}
	}

	// 5. Canonicalized AMZ Headers
	canonicalizedAmzHeaders := v.buildCanonicalizedAmzHeaders(r)

	// 6. Canonicalized Resource
	canonicalizedResource := v.buildCanonicalizedResource(r)

	// Build string to sign
	return strings.Join([]string{
		verb,
		contentMD5,
		contentType,
		date,
		canonicalizedAmzHeaders + canonicalizedResource,
	}, "\n")
}

// buildCanonicalizedAmzHeaders creates canonicalized x-amz-* headers
// All x-amz-* headers are included in the string to sign in sorted order
func (v *V2Verifier) buildCanonicalizedAmzHeaders(r *http.Request) string {
	// Collect all x-amz-* headers (case-insensitive)
	amzHeaders := make(map[string][]string)
	for name, values := range r.Header {
		lowerName := strings.ToLower(name)
		if strings.HasPrefix(lowerName, "x-amz-") {
			amzHeaders[lowerName] = values
		}
	}

	// If no x-amz-* headers, return empty string
	if len(amzHeaders) == 0 {
		return ""
	}

	// Sort header names
	names := make([]string, 0, len(amzHeaders))
	for name := range amzHeaders {
		names = append(names, name)
	}
	sort.Strings(names)

	// Build canonical string
	var parts []string
	for _, name := range names {
		values := amzHeaders[name]
		// Join multiple values with comma, trim whitespace
		trimmed := make([]string, len(values))
		for i, v := range values {
			trimmed[i] = strings.TrimSpace(v)
		}
		parts = append(parts, fmt.Sprintf("%s:%s", name, strings.Join(trimmed, ",")))
	}

	return strings.Join(parts, "\n") + "\n"
}

// buildCanonicalizedResource creates the canonicalized resource string
// Format: /bucket/key with subresources
func (v *V2Verifier) buildCanonicalizedResource(r *http.Request) string {
	// Start with the URI path
	// AWS Signature V2 also requires URL-encoded paths for signature calculation
	// Use RawPath if available, otherwise re-encode
	resource := r.URL.RawPath
	if resource == "" {
		// RawPath not available, re-encode the path
		path := r.URL.Path
		if path == "" {
			path = "/"
		}
		resource = encodeCanonicalURI(path)
	} else {
		if resource == "" {
			resource = "/"
		}
	}

	// Add sub-resources from query string in sorted order
	// Only specific query parameters are included as sub-resources
	subResources := []string{
		"acl", "lifecycle", "location", "logging", "notification",
		"partNumber", "policy", "requestPayment", "torrent",
		"uploadId", "uploads", "versionId", "versioning",
		"versions", "website", "delete", "cors", "restore",
		"tagging", "replication", "metrics", "analytics",
		"inventory", "select", "select-type",
	}

	query := r.URL.Query()
	var foundSubResources []string

	for _, subRes := range subResources {
		if query.Has(subRes) {
			val := query.Get(subRes)
			if val != "" {
				foundSubResources = append(foundSubResources, fmt.Sprintf("%s=%s", subRes, val))
			} else {
				foundSubResources = append(foundSubResources, subRes)
			}
		}
	}

	// Add sub-resources to resource with "?" separator
	if len(foundSubResources) > 0 {
		resource += "?" + strings.Join(foundSubResources, "&")
	}

	return resource
}

// calculateSignature computes the HMAC-SHA1 signature for V2
func (v *V2Verifier) calculateSignature(secretKey, stringToSign string) string {
	h := hmac.New(sha1.New, []byte(secretKey))
	h.Write([]byte(stringToSign))
	signature := h.Sum(nil)
	return base64.StdEncoding.EncodeToString(signature)
}

// constantTimeCompareV2 performs constant-time string comparison to prevent timing attacks
func constantTimeCompareV2(a, b string) bool {
	return subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
