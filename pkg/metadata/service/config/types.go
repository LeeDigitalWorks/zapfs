package config

import (
	"zapfs/pkg/s3api/s3err"
	"zapfs/pkg/s3api/s3types"

	"github.com/google/uuid"
)

// GetObjectTaggingResult contains the result with version info and tags
type GetObjectTaggingResult struct {
	VersionID string
	Tags      *s3types.TagSet
}

// SetObjectTaggingResult contains the result with version info
type SetObjectTaggingResult struct {
	VersionID string
}

// DeleteObjectTaggingResult contains the result with version info
type DeleteObjectTaggingResult struct {
	VersionID string
}

// Error codes for config operations
type ErrorCode int

const (
	ErrCodeNone ErrorCode = iota
	ErrCodeNoSuchBucket
	ErrCodeNoSuchKey
	ErrCodeNoSuchTagSet
	ErrCodeNoSuchCORSConfiguration
	ErrCodeNoSuchWebsiteConfiguration
	ErrCodeNoSuchLifecycleConfiguration
	ErrCodeNoSuchBucketPolicy
	ErrCodeServerSideEncryptionConfigurationNotFound
	ErrCodeObjectLockConfigurationNotFound
	ErrCodeReplicationConfigurationNotFound
	ErrCodeInvalidTag
	ErrCodeMalformedXML
	ErrCodeMalformedACL
	ErrCodeMalformedPolicy
	ErrCodeInternalError
)

// Error represents a config service error with an error code
type Error struct {
	Code      ErrorCode
	Message   string
	Err       error
	VersionID uuid.UUID // For object operations
}

func (e *Error) Error() string {
	if e.Err != nil {
		return e.Message + ": " + e.Err.Error()
	}
	return e.Message
}

func (e *Error) Unwrap() error {
	return e.Err
}

// ToS3Error converts a config error to an S3 error code
func (e *Error) ToS3Error() s3err.ErrorCode {
	switch e.Code {
	case ErrCodeNoSuchBucket:
		return s3err.ErrNoSuchBucket
	case ErrCodeNoSuchKey:
		return s3err.ErrNoSuchKey
	case ErrCodeNoSuchTagSet:
		return s3err.ErrNoSuchTagSet
	case ErrCodeNoSuchCORSConfiguration:
		return s3err.ErrNoSuchCORSConfiguration
	case ErrCodeNoSuchWebsiteConfiguration:
		return s3err.ErrNoSuchWebsiteConfiguration
	case ErrCodeNoSuchLifecycleConfiguration:
		return s3err.ErrNoSuchLifecycleConfiguration
	case ErrCodeNoSuchBucketPolicy:
		return s3err.ErrNoSuchBucketPolicy
	case ErrCodeServerSideEncryptionConfigurationNotFound:
		return s3err.ErrNoSuchBucketEncryptionConfiguration
	case ErrCodeObjectLockConfigurationNotFound:
		return s3err.ErrNoSuchObjectLockConfiguration
	case ErrCodeReplicationConfigurationNotFound:
		return s3err.ErrReplicationConfigurationNotFoundError
	case ErrCodeInvalidTag:
		return s3err.ErrInvalidTag
	case ErrCodeMalformedXML:
		return s3err.ErrMalformedXML
	case ErrCodeMalformedACL:
		return s3err.ErrMalformedACLError
	case ErrCodeMalformedPolicy:
		return s3err.ErrMalformedPolicy
	default:
		return s3err.ErrInternalError
	}
}
