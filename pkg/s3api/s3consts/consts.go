// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3consts

// http://docs.aws.amazon.com/AmazonS3/latest/dev/UploadingObjects.html
const (
	// MaxObjectSize is the maximum object size per PUT request (5GiB)
	MaxObjectSize = 1024 * 1024 * 1024 * 5
	// MaxPartID is the maximum Part ID for multipart upload (10000)
	// Acceptable values range from 1 to 10000 inclusive
	MaxPartID = 10000

	// --- Core request / tracing ---
	XAmzDate        = "x-amz-date"
	XAmzRequestID   = "x-amz-request-id"
	XAmzId2         = "x-amz-id-2"
	XAmzSecurityTok = "x-amz-security-token"

	// --- Authorization ---
	XAmzAlgorithm     = "x-amz-algorithm"
	XAmzCredential    = "x-amz-credential"
	XAmzSignedHeaders = "x-amz-signedheaders"
	XAmzSignature     = "x-amz-signature"
	XAmzExpires       = "x-amz-expires"

	// --- Content / payload ---
	XAmzContentSHA256 = "x-amz-content-sha256"
	XAmzDecodedLength = "x-amz-decoded-content-length"
	XAmzTrailer       = "x-amz-trailer"

	// --- ACL ---
	XAmzACL = "x-amz-acl"

	XAmzGrantRead        = "x-amz-grant-read"
	XAmzGrantWrite       = "x-amz-grant-write"
	XAmzGrantReadACP     = "x-amz-grant-read-acp"
	XAmzGrantWriteACP    = "x-amz-grant-write-acp"
	XAmzGrantFullControl = "x-amz-grant-full-control"

	// --- Metadata ---
	XAmzMetaPrefix = "x-amz-meta-"

	// --- Storage class ---
	XAmzStorageClass = "x-amz-storage-class"

	// --- Website ---
	XAmzWebsiteRedirectLocation = "x-amz-website-redirect-location"

	// --- Tagging ---
	XAmzTagging      = "x-amz-tagging"
	XAmzTaggingCount = "x-amz-tagging-count"

	// --- Object Lock / Retention ---
	XAmzObjectLockMode            = "x-amz-object-lock-mode"
	XAmzObjectLockRetainUntilDate = "x-amz-object-lock-retain-until-date"
	XAmzObjectLockLegalHold       = "x-amz-object-lock-legal-hold"

	// --- Server-side encryption (SSE) ---
	XAmzServerSideEncryption               = "x-amz-server-side-encryption"
	XAmzServerSideEncryptionAwsKmsKeyID    = "x-amz-server-side-encryption-aws-kms-key-id"
	XAmzServerSideEncryptionContext        = "x-amz-server-side-encryption-context"
	XAmzServerSideEncryptionCustomerAlgo   = "x-amz-server-side-encryption-customer-algorithm"
	XAmzServerSideEncryptionCustomerKey    = "x-amz-server-side-encryption-customer-key"
	XAmzServerSideEncryptionCustomerKeyMD5 = "x-amz-server-side-encryption-customer-key-md5"

	// --- Copy source ---
	XAmzCopySource                  = "x-amz-copy-source"
	XAmzCopySourceRange             = "x-amz-copy-source-range"
	XAmzCopySourceIfMatch           = "x-amz-copy-source-if-match"
	XAmzCopySourceIfNoneMatch       = "x-amz-copy-source-if-none-match"
	XAmzCopySourceIfModifiedSince   = "x-amz-copy-source-if-modified-since"
	XAmzCopySourceIfUnmodifiedSince = "x-amz-copy-source-if-unmodified-since"
	XAmzMetadataDirective           = "x-amz-metadata-directive"
	XAmzTaggingDirective            = "x-amz-tagging-directive"

	// --- Multipart upload ---
	XAmzUploadID   = "x-amz-upload-id"
	XAmzPartNumber = "x-amz-part-number"

	// --- Request payer ---
	XAmzRequestPayer = "x-amz-request-payer"

	// --- Replication ---
	XAmzReplicationStatus = "x-amz-replication-status"

	// --- Restore / Glacier ---
	XAmzRestore       = "x-amz-restore"
	XAmzRestoreExpiry = "x-amz-restore-expiry"

	// --- Checksum (newer S3) ---
	XAmzChecksumCRC32  = "x-amz-checksum-crc32"
	XAmzChecksumCRC32C = "x-amz-checksum-crc32c"
	XAmzChecksumSHA1   = "x-amz-checksum-sha1"
	XAmzChecksumSHA256 = "x-amz-checksum-sha256"

	// --- Expected bucket owner ---
	XAmzExpectedBucketOwner = "x-amz-expected-bucket-owner"

	// --- Versioning ---
	XAmzVersionID       = "x-amz-version-id"
	XAmzDeleteMarker    = "x-amz-delete-marker"
	XAmzMFA             = "x-amz-mfa"
	XAmzBypassGovernace = "x-amz-bypass-governance-retention"

	// --- Object attributes ---
	XAmzObjectAttributes = "x-amz-object-attributes"
	XAmzMaxParts         = "x-amz-max-parts"
	XAmzPartNumberMarker = "x-amz-part-number-marker"

	// --- Copy source SSE-C ---
	XAmzCopySourceServerSideEncryptionCustomerAlgo   = "x-amz-copy-source-server-side-encryption-customer-algorithm"
	XAmzCopySourceServerSideEncryptionCustomerKey    = "x-amz-copy-source-server-side-encryption-customer-key"
	XAmzCopySourceServerSideEncryptionCustomerKeyMD5 = "x-amz-copy-source-server-side-encryption-customer-key-MD5"

	// --- Bucket key ---
	XAmzServerSideEncryptionBucketKeyEnabled = "x-amz-server-side-encryption-bucket-key-enabled"

	// --- Checksum mode ---
	XAmzChecksumMode      = "x-amz-checksum-mode"
	XAmzChecksumAlgorithm = "x-amz-checksum-algorithm"
	XAmzSdkChecksumAlgo   = "x-amz-sdk-checksum-algorithm"

	// --- Object expiration / archive ---
	XAmzExpiration     = "x-amz-expiration"
	XAmzArchiveStatus  = "x-amz-archive-status"
	XAmzRestoreRequest = "x-amz-restore-request"

	// --- Intelligent tiering ---
	XAmzIntelligentTieringConfig = "x-amz-intelligent-tiering-configuration"

	// --- Missing parts ---
	XAmzMissingMeta = "x-amz-missing-meta"

	// --- MP upload abort date ---
	XAmzAbortDate   = "x-amz-abort-date"
	XAmzAbortRuleID = "x-amz-abort-rule-id"

	// --- Object ownership ---
	XAmzObjectOwnership           = "x-amz-object-ownership"
	XAmzSourceExpectedBucketOwner = "x-amz-source-expected-bucket-owner"
)

// Storage class values
const (
	StorageClassStandard           = "STANDARD"
	StorageClassReducedRedundancy  = "REDUCED_REDUNDANCY"
	StorageClassStandardIA         = "STANDARD_IA"
	StorageClassOnezoneIA          = "ONEZONE_IA"
	StorageClassIntelligentTiering = "INTELLIGENT_TIERING"
	StorageClassGlacier            = "GLACIER"
	StorageClassGlacierIR          = "GLACIER_IR"
	StorageClassDeepArchive        = "DEEP_ARCHIVE"
)

// SSE algorithm values
const (
	SSEAlgorithmAES256  = "AES256"
	SSEAlgorithmKMS     = "aws:kms"
	SSEAlgorithmKMSDSSE = "aws:kms:dsse"
)

// Checksum algorithm values
const (
	ChecksumAlgoCRC32  = "CRC32"
	ChecksumAlgoCRC32C = "CRC32C"
	ChecksumAlgoSHA1   = "SHA1"
	ChecksumAlgoSHA256 = "SHA256"
)
