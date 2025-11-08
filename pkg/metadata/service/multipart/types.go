package multipart

import (
	"zapfs/pkg/s3api/s3err"
)

// Error codes for multipart operations
type ErrorCode int

const (
	ErrCodeNone ErrorCode = iota
	ErrCodeNoSuchUpload
	ErrCodeInvalidPart
	ErrCodeInvalidPartOrder
	ErrCodeEntityTooSmall
	ErrCodeInvalidArgument
	ErrCodeInternalError
)

// Error represents a multipart service error with an error code
type Error struct {
	Code    ErrorCode
	Message string
	Err     error
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

// ToS3Error converts a multipart error to an S3 error code
func (e *Error) ToS3Error() s3err.ErrorCode {
	switch e.Code {
	case ErrCodeNoSuchUpload:
		return s3err.ErrNoSuchUpload
	case ErrCodeInvalidPart:
		return s3err.ErrInvalidPart
	case ErrCodeInvalidPartOrder:
		return s3err.ErrInvalidPartOrder
	case ErrCodeEntityTooSmall:
		return s3err.ErrEntityTooSmall
	case ErrCodeInvalidArgument:
		return s3err.ErrInvalidArgument
	default:
		return s3err.ErrInternalError
	}
}
