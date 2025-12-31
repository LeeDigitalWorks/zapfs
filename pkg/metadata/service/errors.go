package service

import (
	"fmt"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// ErrorCode represents a domain-level error code
type ErrorCode int

const (
	ErrCodeNone ErrorCode = iota
	ErrCodeNotFound
	ErrCodeAlreadyExists
	ErrCodeAccessDenied
	ErrCodeValidation
	ErrCodePreconditionFailed
	ErrCodeNotModified
	ErrCodeRangeNotSatisfiable
	ErrCodeBucketNotEmpty
	ErrCodeIncompleteBody
	ErrCodeInvalidStorageClass
	ErrCodeInvalidEncryption
	ErrCodeKMSError
	ErrCodeInternalError
)

// ServiceError represents a domain-level error that can be mapped to S3 errors
type ServiceError struct {
	Code    ErrorCode
	Message string
	Err     error
}

func (e *ServiceError) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *ServiceError) Unwrap() error {
	return e.Err
}

// ToS3Error converts a ServiceError to an S3 error code
func (e *ServiceError) ToS3Error() s3err.ErrorCode {
	switch e.Code {
	case ErrCodeNotFound:
		return s3err.ErrNoSuchKey
	case ErrCodeAlreadyExists:
		return s3err.ErrBucketAlreadyExists
	case ErrCodeAccessDenied:
		return s3err.ErrAccessDenied
	case ErrCodeValidation:
		return s3err.ErrInvalidArgument
	case ErrCodePreconditionFailed:
		return s3err.ErrPreconditionFailed
	case ErrCodeNotModified:
		return s3err.ErrNotModified
	case ErrCodeRangeNotSatisfiable:
		return s3err.ErrInvalidRange
	case ErrCodeBucketNotEmpty:
		return s3err.ErrBucketNotEmpty
	case ErrCodeIncompleteBody:
		return s3err.ErrIncompleteBody
	case ErrCodeInvalidStorageClass:
		return s3err.ErrInvalidStorageClass
	case ErrCodeInvalidEncryption:
		return s3err.ErrInvalidEncryptionAlgorithm
	case ErrCodeKMSError:
		return s3err.ErrKMSAccessDenied
	default:
		return s3err.ErrInternalError
	}
}

// Error constructors for convenience

func NewNotFoundError(resource string) *ServiceError {
	return &ServiceError{
		Code:    ErrCodeNotFound,
		Message: fmt.Sprintf("%s not found", resource),
	}
}

func NewAlreadyExistsError(resource string) *ServiceError {
	return &ServiceError{
		Code:    ErrCodeAlreadyExists,
		Message: fmt.Sprintf("%s already exists", resource),
	}
}

func NewAccessDeniedError(reason string) *ServiceError {
	return &ServiceError{
		Code:    ErrCodeAccessDenied,
		Message: reason,
	}
}

func NewValidationError(reason string) *ServiceError {
	return &ServiceError{
		Code:    ErrCodeValidation,
		Message: reason,
	}
}

func NewPreconditionFailedError(reason string) *ServiceError {
	return &ServiceError{
		Code:    ErrCodePreconditionFailed,
		Message: reason,
	}
}

func NewInternalError(err error) *ServiceError {
	return &ServiceError{
		Code:    ErrCodeInternalError,
		Message: "internal error",
		Err:     err,
	}
}

// IsNotFound checks if an error is a not found error
func IsNotFound(err error) bool {
	if svcErr, ok := err.(*ServiceError); ok {
		return svcErr.Code == ErrCodeNotFound
	}
	return false
}

// IsPreconditionFailed checks if an error is a precondition failed error
func IsPreconditionFailed(err error) bool {
	if svcErr, ok := err.(*ServiceError); ok {
		return svcErr.Code == ErrCodePreconditionFailed
	}
	return false
}
