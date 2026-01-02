// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package signature

import (
	"context"
	"crypto/subtle"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// PostPolicyVerifier verifies AWS Signature V4 for POST form uploads
type PostPolicyVerifier struct {
	iamManager *iam.Manager
	region     string
}

// NewPostPolicyVerifier creates a new POST policy verifier
func NewPostPolicyVerifier(iamManager *iam.Manager, region string) *PostPolicyVerifier {
	if region == "" {
		region = "us-east-1"
	}
	return &PostPolicyVerifier{
		iamManager: iamManager,
		region:     region,
	}
}

// PostFormData contains the parsed form fields from a POST upload
type PostFormData struct {
	// Required fields
	Key       string // Object key (may contain ${filename} substitution)
	Policy    string // Base64-encoded policy document
	Signature string // x-amz-signature
	Algorithm string // x-amz-algorithm (must be AWS4-HMAC-SHA256)
	Date      string // x-amz-date (ISO8601 format)
	Credential string // x-amz-credential (accessKey/date/region/s3/aws4_request)

	// Optional fields
	ACL                  string
	ContentType          string
	ContentDisposition   string
	ContentEncoding      string
	CacheControl         string
	Expires              string
	SuccessActionRedirect string
	SuccessActionStatus  int
	Tagging              string

	// Custom x-amz-meta-* headers
	Metadata map[string]string

	// File content (populated by caller)
	Filename    string
	FileSize    int64
}

// PostPolicyResult contains the result of a successful POST policy verification
type PostPolicyResult struct {
	Identity    *iam.Identity
	Key         string            // Final object key after ${filename} substitution
	ContentType string
	ACL         string
	Metadata    map[string]string
}

// PolicyDocument represents the decoded POST policy
type PolicyDocument struct {
	Expiration string          `json:"expiration"`
	Conditions []interface{}   `json:"conditions"`
}

// VerifyPostForm verifies the POST form data against the policy
func (v *PostPolicyVerifier) VerifyPostForm(ctx context.Context, form *PostFormData, bucket string) (*PostPolicyResult, s3err.ErrorCode) {
	// 1. Validate required fields
	if form.Policy == "" {
		return nil, s3err.ErrMissingFields
	}
	if form.Signature == "" {
		return nil, s3err.ErrMissingFields
	}
	if form.Credential == "" {
		return nil, s3err.ErrMissingFields
	}
	if form.Algorithm != "AWS4-HMAC-SHA256" {
		return nil, s3err.ErrSignatureVersionNotSupported
	}

	// 2. Parse credential to extract access key
	accessKey, credDate, credRegion, err := parseCredential(form.Credential)
	if err != nil {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// 3. Look up credentials
	identity, credential, found := v.iamManager.LookupByAccessKey(ctx, accessKey)
	if !found {
		return nil, s3err.ErrInvalidAccessKeyID
	}

	// 4. Decode and parse policy
	policyBytes, err := base64.StdEncoding.DecodeString(form.Policy)
	if err != nil {
		return nil, s3err.ErrMalformedPOSTRequest
	}

	var policy PolicyDocument
	if err := json.Unmarshal(policyBytes, &policy); err != nil {
		return nil, s3err.ErrMalformedPOSTRequest
	}

	// 5. Check policy expiration
	expTime, err := time.Parse(time.RFC3339, policy.Expiration)
	if err != nil {
		// Try alternate format
		expTime, err = time.Parse("2006-01-02T15:04:05Z", policy.Expiration)
		if err != nil {
			return nil, s3err.ErrMalformedPOSTRequest
		}
	}
	if time.Now().After(expTime) {
		return nil, s3err.ErrExpiredPresignRequest
	}

	// 6. Calculate expected signature
	signingKey := deriveSigningKey(credential.SecretKey, credDate, credRegion, "s3")
	expectedSig := calculatePostSignature(signingKey, form.Policy)

	// 7. Compare signatures
	if subtle.ConstantTimeCompare([]byte(form.Signature), []byte(expectedSig)) != 1 {
		return nil, s3err.ErrSignatureDoesNotMatch
	}

	// 8. Validate policy conditions
	finalKey := form.Key
	if strings.Contains(form.Key, "${filename}") && form.Filename != "" {
		finalKey = strings.ReplaceAll(form.Key, "${filename}", form.Filename)
	}

	if err := v.validateConditions(policy.Conditions, form, bucket, finalKey); err != nil {
		return nil, s3err.ErrPostPolicyConditionInvalidFormat
	}

	return &PostPolicyResult{
		Identity:    identity,
		Key:         finalKey,
		ContentType: form.ContentType,
		ACL:         form.ACL,
		Metadata:    form.Metadata,
	}, s3err.ErrNone
}

// parseCredential parses the x-amz-credential value
// Format: accessKey/date/region/service/aws4_request
func parseCredential(credential string) (accessKey, date, region string, err error) {
	parts := strings.Split(credential, "/")
	if len(parts) != 5 {
		return "", "", "", fmt.Errorf("invalid credential format")
	}
	if parts[3] != "s3" || parts[4] != "aws4_request" {
		return "", "", "", fmt.Errorf("invalid credential scope")
	}
	return parts[0], parts[1], parts[2], nil
}

// deriveSigningKey derives the signing key for V4 signature
func deriveSigningKey(secretKey, date, region, service string) []byte {
	kDate := hmacSHA256([]byte("AWS4"+secretKey), []byte(date))
	kRegion := hmacSHA256(kDate, []byte(region))
	kService := hmacSHA256(kRegion, []byte(service))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))
	return kSigning
}

// calculatePostSignature calculates the signature for POST policy
func calculatePostSignature(signingKey []byte, policy string) string {
	signature := hmacSHA256(signingKey, []byte(policy))
	return hex.EncodeToString(signature)
}

// Note: hmacSHA256 is defined in v4.go

// validateConditions validates the policy conditions against form data
func (v *PostPolicyVerifier) validateConditions(conditions []interface{}, form *PostFormData, bucket, key string) error {
	for _, cond := range conditions {
		switch c := cond.(type) {
		case map[string]interface{}:
			// Exact match condition: {"field": "value"}
			for field, value := range c {
				if err := v.validateExactMatch(field, value, form, bucket, key); err != nil {
					return err
				}
			}
		case []interface{}:
			// Array condition: ["operator", "field", "value"] or ["starts-with", "$field", "prefix"]
			if len(c) != 3 {
				return fmt.Errorf("invalid condition array length")
			}
			operator, ok := c[0].(string)
			if !ok {
				return fmt.Errorf("invalid condition operator")
			}
			field, ok := c[1].(string)
			if !ok {
				return fmt.Errorf("invalid condition field")
			}
			value := c[2]

			if err := v.validateArrayCondition(operator, field, value, form, bucket, key); err != nil {
				return err
			}
		}
	}
	return nil
}

// validateExactMatch validates an exact match condition
func (v *PostPolicyVerifier) validateExactMatch(field string, value interface{}, form *PostFormData, bucket, key string) error {
	strValue, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid condition value type")
	}

	var formValue string
	switch strings.ToLower(field) {
	case "bucket":
		formValue = bucket
	case "key":
		formValue = key
	case "acl":
		formValue = form.ACL
	case "content-type":
		formValue = form.ContentType
	case "x-amz-algorithm":
		formValue = form.Algorithm
	case "x-amz-credential":
		formValue = form.Credential
	case "x-amz-date":
		formValue = form.Date
	case "success_action_redirect":
		formValue = form.SuccessActionRedirect
	case "success_action_status":
		formValue = strconv.Itoa(form.SuccessActionStatus)
	default:
		if strings.HasPrefix(strings.ToLower(field), "x-amz-meta-") {
			metaKey := strings.TrimPrefix(strings.ToLower(field), "x-amz-meta-")
			formValue = form.Metadata[metaKey]
		} else {
			// Unknown field - ignore or return error based on strictness
			return nil
		}
	}

	if formValue != strValue {
		return fmt.Errorf("condition mismatch for %s: expected %s, got %s", field, strValue, formValue)
	}
	return nil
}

// validateArrayCondition validates an array condition
func (v *PostPolicyVerifier) validateArrayCondition(operator, field string, value interface{}, form *PostFormData, bucket, key string) error {
	// Remove $ prefix from field name
	fieldName := strings.TrimPrefix(field, "$")

	switch operator {
	case "eq":
		return v.validateExactMatch(fieldName, value, form, bucket, key)

	case "starts-with":
		strValue, ok := value.(string)
		if !ok {
			return fmt.Errorf("invalid starts-with value type")
		}

		var formValue string
		switch strings.ToLower(fieldName) {
		case "key":
			formValue = key
		case "content-type":
			formValue = form.ContentType
		case "acl":
			formValue = form.ACL
		default:
			if strings.HasPrefix(strings.ToLower(fieldName), "x-amz-meta-") {
				metaKey := strings.TrimPrefix(strings.ToLower(fieldName), "x-amz-meta-")
				formValue = form.Metadata[metaKey]
			} else {
				return nil
			}
		}

		// Empty prefix allows any value
		if strValue != "" && !strings.HasPrefix(formValue, strValue) {
			return fmt.Errorf("starts-with condition failed for %s", fieldName)
		}
		return nil

	case "content-length-range":
		// value should be two numbers: min, max
		values, ok := value.([]interface{})
		if !ok {
			// Try to get min/max from the condition array itself
			// Format: ["content-length-range", min, max]
			return nil
		}
		if len(values) != 2 {
			return fmt.Errorf("invalid content-length-range format")
		}

		min, err := toInt64(values[0])
		if err != nil {
			return fmt.Errorf("invalid min value")
		}
		max, err := toInt64(values[1])
		if err != nil {
			return fmt.Errorf("invalid max value")
		}

		if form.FileSize < min || form.FileSize > max {
			return fmt.Errorf("file size %d outside range [%d, %d]", form.FileSize, min, max)
		}
		return nil

	default:
		return fmt.Errorf("unknown operator: %s", operator)
	}
}

// toInt64 converts interface{} to int64
func toInt64(v interface{}) (int64, error) {
	switch n := v.(type) {
	case float64:
		return int64(n), nil
	case int:
		return int64(n), nil
	case int64:
		return n, nil
	case string:
		return strconv.ParseInt(n, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert to int64")
	}
}
