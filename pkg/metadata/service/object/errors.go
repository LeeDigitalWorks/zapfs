// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package object

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
	ErrCodeIncompleteBody
	ErrCodeInvalidStorageClass
	ErrCodeInvalidEncryption
	ErrCodeKMSError
	ErrCodeKMSKeyNotFound
	ErrCodeInternalError
)

// Error represents a domain-level error
type Error struct {
	Code    ErrorCode
	Message string
	Err     error
}

func (e *Error) Error() string {
	if e.Err != nil {
		return fmt.Sprintf("%s: %v", e.Message, e.Err)
	}
	return e.Message
}

func (e *Error) Unwrap() error {
	return e.Err
}

// ToS3Error converts an Error to an S3 error code
func (e *Error) ToS3Error() s3err.ErrorCode {
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
	case ErrCodeIncompleteBody:
		return s3err.ErrIncompleteBody
	case ErrCodeInvalidStorageClass:
		return s3err.ErrInvalidStorageClass
	case ErrCodeInvalidEncryption:
		return s3err.ErrInvalidEncryptionAlgorithm
	case ErrCodeKMSError:
		return s3err.ErrKMSAccessDenied
	case ErrCodeKMSKeyNotFound:
		return s3err.ErrKMSKeyNotFound
	default:
		return s3err.ErrInternalError
	}
}

// Error constructors

func newNotFoundError(resource string) *Error {
	return &Error{
		Code:    ErrCodeNotFound,
		Message: fmt.Sprintf("%s not found", resource),
	}
}

func newValidationError(msg string) *Error {
	return &Error{
		Code:    ErrCodeValidation,
		Message: msg,
	}
}

func newInternalError(err error) *Error {
	return &Error{
		Code:    ErrCodeInternalError,
		Message: "internal error",
		Err:     err,
	}
}

// IsNotFound checks if an error is a not found error
func IsNotFound(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.Code == ErrCodeNotFound
	}
	return false
}

// IsNotModified checks if an error is a not modified error
func IsNotModified(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.Code == ErrCodeNotModified
	}
	return false
}

// IsPreconditionFailed checks if an error is a precondition failed error
func IsPreconditionFailed(err error) bool {
	if e, ok := err.(*Error); ok {
		return e.Code == ErrCodePreconditionFailed
	}
	return false
}
