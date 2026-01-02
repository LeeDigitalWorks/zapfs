// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/rand"
	"encoding/base64"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/license"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/multipart"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// parseRangeHeader parses the HTTP Range header and returns offset and length.
// Returns (0, 0, false) if no valid range is found.
// Format: "bytes=start-end", "bytes=start-", or "bytes=-suffix"
func parseRangeHeader(rangeHeader string, objectSize uint64) (offset, length uint64, valid bool) {
	if rangeHeader == "" {
		return 0, 0, false
	}

	// Range header format: "bytes=start-end" or "bytes=start-" or "bytes=-suffix"
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		return 0, 0, false
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.Split(rangeSpec, "-")
	if len(parts) != 2 {
		return 0, 0, false
	}

	var start, end int64
	var err error

	if parts[0] == "" {
		// Suffix range: "bytes=-suffix"
		suffix, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || suffix <= 0 {
			return 0, 0, false
		}
		if uint64(suffix) >= objectSize {
			// Request entire object
			return 0, objectSize, true
		}
		offset = objectSize - uint64(suffix)
		length = uint64(suffix)
		return offset, length, true
	}

	// Parse start position
	start, err = strconv.ParseInt(parts[0], 10, 64)
	if err != nil || start < 0 {
		return 0, 0, false
	}

	if parts[1] == "" {
		// Open-ended range: "bytes=start-"
		if uint64(start) >= objectSize {
			return 0, 0, false // Range not satisfiable
		}
		offset = uint64(start)
		length = objectSize - offset
		return offset, length, true
	}

	// Closed range: "bytes=start-end"
	end, err = strconv.ParseInt(parts[1], 10, 64)
	if err != nil || end < start {
		return 0, 0, false
	}

	offset = uint64(start)
	// end is inclusive, so length = end - start + 1, but capped at object size
	requestedLength := uint64(end) - offset + 1

	// When objectSize is MaxUint64 (sentinel for "unknown"), just return the requested range
	// The service layer will validate and adjust when it knows the actual size
	if objectSize == ^uint64(0) {
		return offset, requestedLength, true
	}

	if offset >= objectSize {
		return 0, 0, false // Range not satisfiable
	}
	if offset+requestedLength > objectSize {
		length = objectSize - offset
	} else {
		length = requestedLength
	}

	return offset, length, true
}

// checkConditionalHeaders validates conditional request headers against object metadata.
// Returns (shouldProceed, statusCode, errorCode).
// If shouldProceed is false, the handler should return immediately with the given status code.
// errorCode will be empty if shouldProceed is true.
func checkConditionalHeaders(req *http.Request, etag string, lastModified time.Time) (shouldProceed bool, statusCode int, errCode s3err.ErrorCode) {
	// If-Match: ETag must match (return 412 if it doesn't)
	if ifMatch := req.Header.Get("If-Match"); ifMatch != "" {
		// Remove quotes if present
		ifMatch = strings.Trim(ifMatch, "\"")
		objectETag := strings.Trim(etag, "\"")
		if ifMatch != objectETag {
			return false, http.StatusPreconditionFailed, s3err.ErrPreconditionFailed
		}
	}

	// If-None-Match: ETag must not match (return 304 if it matches, for GET/HEAD)
	if ifNoneMatch := req.Header.Get("If-None-Match"); ifNoneMatch != "" {
		// Remove quotes if present
		ifNoneMatch = strings.Trim(ifNoneMatch, "\"")
		objectETag := strings.Trim(etag, "\"")
		if ifNoneMatch == objectETag {
			// For GET/HEAD, return 304 Not Modified
			return false, http.StatusNotModified, s3err.ErrNotModified
		}
	}

	// If-Modified-Since: Return 304 if object hasn't been modified since this date
	if ifModifiedSince := req.Header.Get("If-Modified-Since"); ifModifiedSince != "" {
		ifModifiedSinceTime, err := time.Parse(http.TimeFormat, ifModifiedSince)
		if err == nil {
			// Compare times (ignore sub-second precision)
			if !lastModified.After(ifModifiedSinceTime) {
				return false, http.StatusNotModified, s3err.ErrNotModified
			}
		}
	}

	// If-Unmodified-Since: Return 412 if object has been modified since this date
	if ifUnmodifiedSince := req.Header.Get("If-Unmodified-Since"); ifUnmodifiedSince != "" {
		ifUnmodifiedSinceTime, err := time.Parse(http.TimeFormat, ifUnmodifiedSince)
		if err == nil {
			// Compare times (ignore sub-second precision)
			if lastModified.After(ifUnmodifiedSinceTime) {
				return false, http.StatusPreconditionFailed, s3err.ErrPreconditionFailed
			}
		}
	}

	return true, 0, s3err.ErrNone
}

// checkExpectedBucketOwner validates the x-amz-expected-bucket-owner header.
// Returns an error if the header is provided and doesn't match the bucket owner.
func checkExpectedBucketOwner(req *http.Request, bucketOwnerID string) *s3err.ErrorCode {
	expectedOwner := req.Header.Get(s3consts.XAmzExpectedBucketOwner)
	if expectedOwner == "" {
		// Header not provided - no validation needed
		return nil
	}

	if expectedOwner != bucketOwnerID {
		// Owner mismatch - return error
		err := s3err.ErrAccessDenied
		return &err
	}

	// Owner matches
	return nil
}

// checkRequestPayer validates the x-amz-request-payer header for requester pays buckets.
// Returns an error if the bucket requires requester pays but the header is missing or invalid.
func checkRequestPayer(req *http.Request, requestPayment *s3types.RequestPaymentConfig) *s3err.ErrorCode {
	// If bucket doesn't have requester pays enabled, header is optional
	if requestPayment == nil || requestPayment.Payer != s3types.PayerRequester {
		return nil
	}

	// Bucket requires requester pays - header must be present with value "requester"
	requestPayer := req.Header.Get(s3consts.XAmzRequestPayer)
	if requestPayer == "" {
		// Header missing - return error
		err := s3err.ErrAccessDenied
		return &err
	}

	// Header value must be "requester" (case-insensitive per AWS spec)
	if strings.ToLower(requestPayer) != "requester" {
		// Invalid header value - return error
		err := s3err.ErrAccessDenied
		return &err
	}

	// Valid requester pays header
	return nil
}

// checkCopySourceConditionalHeaders validates conditional headers for copy source object.
// Returns (shouldProceed, statusCode, errorCode).
// If shouldProceed is false, the handler should return immediately with the given status code.
func checkCopySourceConditionalHeaders(req *http.Request, etag string, lastModified time.Time) (shouldProceed bool, statusCode int, errCode s3err.ErrorCode) {
	// x-amz-copy-source-if-match: ETag must match (return 412 if it doesn't)
	if ifMatch := req.Header.Get(s3consts.XAmzCopySourceIfMatch); ifMatch != "" {
		ifMatch = strings.Trim(ifMatch, "\"")
		objectETag := strings.Trim(etag, "\"")
		if ifMatch != objectETag {
			return false, http.StatusPreconditionFailed, s3err.ErrPreconditionFailed
		}
	}

	// x-amz-copy-source-if-none-match: ETag must not match (return 412 if it matches)
	if ifNoneMatch := req.Header.Get(s3consts.XAmzCopySourceIfNoneMatch); ifNoneMatch != "" {
		ifNoneMatch = strings.Trim(ifNoneMatch, "\"")
		objectETag := strings.Trim(etag, "\"")
		if ifNoneMatch == objectETag {
			return false, http.StatusPreconditionFailed, s3err.ErrPreconditionFailed
		}
	}

	// x-amz-copy-source-if-modified-since: Return 412 if object hasn't been modified since this date
	if ifModifiedSince := req.Header.Get(s3consts.XAmzCopySourceIfModifiedSince); ifModifiedSince != "" {
		ifModifiedSinceTime, err := time.Parse(http.TimeFormat, ifModifiedSince)
		if err == nil {
			if !lastModified.After(ifModifiedSinceTime) {
				return false, http.StatusPreconditionFailed, s3err.ErrPreconditionFailed
			}
		}
	}

	// x-amz-copy-source-if-unmodified-since: Return 412 if object has been modified since this date
	if ifUnmodifiedSince := req.Header.Get(s3consts.XAmzCopySourceIfUnmodifiedSince); ifUnmodifiedSince != "" {
		ifUnmodifiedSinceTime, err := time.Parse(http.TimeFormat, ifUnmodifiedSince)
		if err == nil {
			if lastModified.After(ifUnmodifiedSinceTime) {
				return false, http.StatusPreconditionFailed, s3err.ErrPreconditionFailed
			}
		}
	}

	return true, 0, s3err.ErrNone
}

// parseCopySourceRange parses the x-amz-copy-source-range header for partial copy operations.
// Returns (offset, length, valid).
// Format: "bytes=start-end" (same as Range header)
func parseCopySourceRange(rangeHeader string, objectSize uint64) (offset, length uint64, valid bool) {
	return parseRangeHeader(rangeHeader, objectSize)
}

// checkExpectedSourceBucketOwner validates the x-amz-source-expected-bucket-owner header.
// Returns an error if the header is provided and doesn't match the source bucket owner.
func checkExpectedSourceBucketOwner(req *http.Request, sourceBucketOwnerID string) *s3err.ErrorCode {
	expectedOwner := req.Header.Get(s3consts.XAmzSourceExpectedBucketOwner)
	if expectedOwner == "" {
		// Header not provided - no validation needed
		return nil
	}

	if expectedOwner != sourceBucketOwnerID {
		// Owner mismatch - return error
		err := s3err.ErrAccessDenied
		return &err
	}

	// Owner matches
	return nil
}

// buildACLResponse converts an ACL to an AccessControlPolicy response.
func buildACLResponse(acl *s3types.AccessControlList) *s3types.AccessControlPolicy {
	return acl.ToAccessControlPolicy()
}

// parseACLXML parses ACL XML from request body.
func parseACLXML(body []byte, defaultOwnerID string) (*s3types.AccessControlList, error) {
	var policy s3types.AccessControlPolicy
	if err := xml.Unmarshal(body, &policy); err != nil {
		return nil, err
	}

	acl := &s3types.AccessControlList{
		Owner: policy.Owner,
	}

	if acl.Owner.ID == "" {
		acl.Owner.ID = defaultOwnerID
		acl.Owner.DisplayName = defaultOwnerID
	}

	for _, grant := range policy.AccessControlList.Grants {
		g := s3types.Grant{
			Permission: grant.Permission,
			Grantee: s3types.Grantee{
				ID:          grant.Grantee.ID,
				DisplayName: grant.Grantee.DisplayName,
				URI:         grant.Grantee.URI,
			},
		}

		// Determine grantee type from xsi:type
		switch grant.Grantee.XsiType {
		case "CanonicalUser":
			g.Grantee.Type = s3types.GranteeTypeCanonicalUser
		case "Group":
			g.Grantee.Type = s3types.GranteeTypeGroup
		case "AmazonCustomerByEmail":
			g.Grantee.Type = s3types.GranteeTypeEmail
		}

		acl.Grants = append(acl.Grants, g)
	}

	return acl, nil
}

// EncryptionMetadata contains encryption information for an object
type EncryptionMetadata struct {
	SSEAlgorithm      string // "AES256" or "aws:kms"
	SSECustomerKeyMD5 string // For SSE-C validation
	SSEKMSKeyID       string // For SSE-KMS
	SSEKMSContext     string // For SSE-KMS (encryption context JSON)
}

// SSECHeaders contains parsed SSE-C headers from a request
type SSECHeaders struct {
	Algorithm string // "AES256"
	Key       []byte // Decoded customer key (32 bytes)
	KeyMD5    string // MD5 hash of the key
}

// parseSSECHeaders parses and validates SSE-C headers from a request.
// Returns (headers, errorCode). If errorCode is not nil, the handler should return it.
func parseSSECHeaders(req *http.Request) (*SSECHeaders, *s3err.ErrorCode) {
	algorithm := req.Header.Get(s3consts.XAmzServerSideEncryptionCustomerAlgo)
	keyBase64 := req.Header.Get(s3consts.XAmzServerSideEncryptionCustomerKey)
	keyMD5 := req.Header.Get(s3consts.XAmzServerSideEncryptionCustomerKeyMD5)

	// If no SSE-C headers are present, return nil (no encryption)
	if algorithm == "" && keyBase64 == "" && keyMD5 == "" {
		return nil, nil
	}

	// All three headers must be present for SSE-C
	if algorithm == "" {
		err := s3err.ErrSSECustomerKeyMissing
		return nil, &err
	}
	if keyBase64 == "" {
		err := s3err.ErrSSECustomerKeyMissing
		return nil, &err
	}
	if keyMD5 == "" {
		err := s3err.ErrSSECustomerKeyMissing
		return nil, &err
	}

	// Validate algorithm
	if algorithm != s3consts.SSEAlgorithmAES256 {
		logger.Warn().Str("algorithm", algorithm).Msg("invalid SSE-C algorithm")
		err := s3err.ErrInvalidEncryptionAlgorithm
		return nil, &err
	}

	// Decode base64 key
	key, err := base64.StdEncoding.DecodeString(keyBase64)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to decode SSE-C key")
		errCode := s3err.ErrInvalidEncryptionKey
		return nil, &errCode
	}

	// Validate key length (must be 32 bytes for AES-256)
	if len(key) != 32 {
		logger.Warn().Int("key_length", len(key)).Msg("invalid SSE-C key length")
		errCode := s3err.ErrInvalidEncryptionKey
		return nil, &errCode
	}

	// Validate MD5 hash
	keyMD5Hash := md5.Sum(key)
	keyMD5Expected := base64.StdEncoding.EncodeToString(keyMD5Hash[:])
	if keyMD5 != keyMD5Expected {
		logger.Warn().
			Str("provided_md5", keyMD5).
			Str("calculated_md5", keyMD5Expected).
			Msg("SSE-C key MD5 mismatch")
		err := s3err.ErrSSECustomerKeyMD5Mismatch
		return nil, &err
	}

	return &SSECHeaders{
		Algorithm: algorithm,
		Key:       key,
		KeyMD5:    keyMD5,
	}, nil
}

// encryptWithKMSDEK encrypts data using a KMS-generated data encryption key (DEK).
// Uses AES-256-GCM (same as KMS service).
func encryptWithKMSDEK(plaintext []byte, dek []byte) ([]byte, error) {
	if len(dek) != 32 {
		return nil, fmt.Errorf("DEK must be 32 bytes for AES-256, got %d", len(dek))
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	ciphertext := gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// decryptWithKMSDEK decrypts data using a KMS-generated data encryption key (DEK).
// Uses AES-256-GCM (same as KMS service).
func decryptWithKMSDEK(ciphertext []byte, dek []byte) ([]byte, error) {
	if len(dek) != 32 {
		return nil, fmt.Errorf("DEK must be 32 bytes for AES-256, got %d", len(dek))
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonceSize := gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext too short")
	}

	nonce, ciphertext := ciphertext[:nonceSize], ciphertext[nonceSize:]
	plaintext, err := gcm.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}

	return plaintext, nil
}

// encryptWithSSEC encrypts data using SSE-C (AES-256-CBC).
// AWS S3 uses CBC mode for SSE-C compatibility.
// Returns encrypted data with IV prepended.
func encryptWithSSEC(plaintext []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("SSE-C key must be 32 bytes for AES-256, got %d", len(key))
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Generate random IV (16 bytes for AES-128/256 block size)
	iv := make([]byte, aes.BlockSize)
	if _, err := io.ReadFull(rand.Reader, iv); err != nil {
		return nil, fmt.Errorf("failed to generate IV: %w", err)
	}

	// Pad plaintext to block size (PKCS#7 padding)
	padding := aes.BlockSize - (len(plaintext) % aes.BlockSize)
	paddedPlaintext := make([]byte, len(plaintext)+padding)
	copy(paddedPlaintext, plaintext)
	for i := len(plaintext); i < len(paddedPlaintext); i++ {
		paddedPlaintext[i] = byte(padding)
	}

	// Encrypt using CBC mode
	mode := cipher.NewCBCEncrypter(block, iv)
	ciphertext := make([]byte, len(paddedPlaintext))
	mode.CryptBlocks(ciphertext, paddedPlaintext)

	// Prepend IV to ciphertext
	result := make([]byte, len(iv)+len(ciphertext))
	copy(result, iv)
	copy(result[len(iv):], ciphertext)

	return result, nil
}

// decryptWithSSEC decrypts data using SSE-C (AES-256-CBC).
// Expects encrypted data with IV prepended.
func decryptWithSSEC(ciphertext []byte, key []byte) ([]byte, error) {
	if len(key) != 32 {
		return nil, fmt.Errorf("SSE-C key must be 32 bytes for AES-256, got %d", len(key))
	}

	if len(ciphertext) < aes.BlockSize {
		return nil, fmt.Errorf("ciphertext too short (must include IV)")
	}

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	// Extract IV and encrypted data
	iv := ciphertext[:aes.BlockSize]
	encryptedData := ciphertext[aes.BlockSize:]

	// Validate encrypted data length is multiple of block size
	if len(encryptedData)%aes.BlockSize != 0 {
		return nil, fmt.Errorf("encrypted data length must be multiple of block size")
	}

	// Decrypt using CBC mode
	mode := cipher.NewCBCDecrypter(block, iv)
	plaintext := make([]byte, len(encryptedData))
	mode.CryptBlocks(plaintext, encryptedData)

	// Remove PKCS#7 padding
	if len(plaintext) == 0 {
		return nil, fmt.Errorf("decrypted data is empty")
	}
	padding := int(plaintext[len(plaintext)-1])
	if padding > aes.BlockSize || padding == 0 {
		return nil, fmt.Errorf("invalid padding")
	}
	if padding > len(plaintext) {
		return nil, fmt.Errorf("padding exceeds data length")
	}

	// Validate padding bytes
	for i := len(plaintext) - padding; i < len(plaintext); i++ {
		if plaintext[i] != byte(padding) {
			return nil, fmt.Errorf("invalid padding")
		}
	}

	return plaintext[:len(plaintext)-padding], nil
}

// SSEKMSHeaders contains parsed SSE-KMS headers from a request
type SSEKMSHeaders struct {
	Algorithm string // "aws:kms"
	KeyID     string // KMS key ID
	Context   string // Optional encryption context (JSON string)
}

// validateReplicationConfig validates a replication configuration.
func validateReplicationConfig(config *s3types.ReplicationConfiguration) error {
	if len(config.Rules) == 0 {
		return fmt.Errorf("replication configuration must have at least one rule")
	}

	if len(config.Rules) > 1000 {
		return fmt.Errorf("replication configuration cannot have more than 1000 rules")
	}

	for i, rule := range config.Rules {
		if rule.ID == "" {
			return fmt.Errorf("rule %d: ID is required", i)
		}

		if len(rule.ID) > 255 {
			return fmt.Errorf("rule %d: ID cannot exceed 255 characters", i)
		}

		if rule.Status != "Enabled" && rule.Status != "Disabled" {
			return fmt.Errorf("rule %d: status must be Enabled or Disabled", i)
		}

		if rule.Destination.Bucket == "" {
			return fmt.Errorf("rule %d: destination bucket is required", i)
		}
	}

	return nil
}

// parseSSEKMSHeaders parses and validates SSE-KMS headers from a request.
// Returns (headers, errorCode). If errorCode is not nil, the handler should return it.
// Requires enterprise license with FeatureKMS.
func parseSSEKMSHeaders(req *http.Request) (*SSEKMSHeaders, *s3err.ErrorCode) {
	algorithm := req.Header.Get(s3consts.XAmzServerSideEncryption)
	keyID := req.Header.Get(s3consts.XAmzServerSideEncryptionAwsKmsKeyID)
	context := req.Header.Get(s3consts.XAmzServerSideEncryptionContext)

	// If no SSE-KMS headers are present, return nil (no encryption)
	// Check this BEFORE license check to avoid unnecessary license errors
	if algorithm == "" && keyID == "" && context == "" {
		return nil, nil
	}

	// Check license only if SSE-KMS headers are present
	if !license.CheckKMS() {
		err := s3err.ErrKMSAccessDenied
		return nil, &err
	}

	// Algorithm must be "aws:kms" for SSE-KMS
	if algorithm != "" && algorithm != s3consts.SSEAlgorithmKMS {
		logger.Warn().Str("algorithm", algorithm).Msg("invalid SSE-KMS algorithm")
		err := s3err.ErrInvalidEncryptionAlgorithm
		return nil, &err
	}

	// If algorithm is specified, key ID is required
	if algorithm == s3consts.SSEAlgorithmKMS && keyID == "" {
		err := s3err.ErrKMSKeyNotFound
		return nil, &err
	}

	// If key ID is specified, algorithm must be "aws:kms"
	if keyID != "" && algorithm != "" && algorithm != s3consts.SSEAlgorithmKMS {
		logger.Warn().Str("algorithm", algorithm).Str("key_id", keyID).Msg("key ID provided but algorithm is not aws:kms")
		err := s3err.ErrInvalidEncryptionAlgorithm
		return nil, &err
	}

	// If key ID is specified without algorithm, assume aws:kms
	if keyID != "" && algorithm == "" {
		algorithm = s3consts.SSEAlgorithmKMS
	}

	// If algorithm is aws:kms, key ID is required
	if algorithm == s3consts.SSEAlgorithmKMS && keyID == "" {
		err := s3err.ErrKMSKeyNotFound
		return nil, &err
	}

	return &SSEKMSHeaders{
		Algorithm: algorithm,
		KeyID:     keyID,
		Context:   context,
	}, nil
}

// getBucketDefaultEncryption extracts SSE-KMS parameters from bucket default encryption config.
// Returns nil if no default encryption is configured or if it's AES256 (handled by S3 service).
// Only returns params for SSE-KMS encryption.
func getBucketDefaultEncryption(encConfig *s3types.ServerSideEncryptionConfig) *object.SSEKMSParams {
	if encConfig == nil || len(encConfig.Rules) == 0 {
		return nil
	}

	// Use the first rule (S3 only supports one rule)
	rule := encConfig.Rules[0]
	if rule.ApplyServerSideEncryptionByDefault == nil {
		return nil
	}

	defaultEnc := rule.ApplyServerSideEncryptionByDefault

	// Only handle SSE-KMS (aws:kms), not AES256
	// AES256 is S3-managed encryption which we don't implement yet
	if defaultEnc.SSEAlgorithm != s3consts.SSEAlgorithmKMS {
		return nil
	}

	// Check license for KMS
	if !license.CheckKMS() {
		logger.Warn().Msg("bucket has SSE-KMS default encryption but KMS license not available")
		return nil
	}

	if defaultEnc.KMSMasterKeyID == "" {
		logger.Warn().Msg("bucket has SSE-KMS default encryption but no key ID configured")
		return nil
	}

	return &object.SSEKMSParams{
		KeyID: defaultEnc.KMSMasterKeyID,
	}
}

// getBucketDefaultEncryptionForMultipart extracts SSE-KMS parameters from bucket default encryption config
// for multipart uploads. Returns nil if no default encryption is configured or if it's AES256.
func getBucketDefaultEncryptionForMultipart(encConfig *s3types.ServerSideEncryptionConfig) *multipart.SSEKMSParams {
	if encConfig == nil || len(encConfig.Rules) == 0 {
		return nil
	}

	// Use the first rule (S3 only supports one rule)
	rule := encConfig.Rules[0]
	if rule.ApplyServerSideEncryptionByDefault == nil {
		return nil
	}

	defaultEnc := rule.ApplyServerSideEncryptionByDefault

	// Only handle SSE-KMS (aws:kms), not AES256
	if defaultEnc.SSEAlgorithm != s3consts.SSEAlgorithmKMS {
		return nil
	}

	// Check license for KMS
	if !license.CheckKMS() {
		logger.Warn().Msg("bucket has SSE-KMS default encryption but KMS license not available")
		return nil
	}

	if defaultEnc.KMSMasterKeyID == "" {
		logger.Warn().Msg("bucket has SSE-KMS default encryption but no key ID configured")
		return nil
	}

	return &multipart.SSEKMSParams{
		KeyID: defaultEnc.KMSMasterKeyID,
	}
}
