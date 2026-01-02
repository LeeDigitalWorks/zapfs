// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package bucket

import (
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// CreateBucketRequest contains parameters for creating a bucket
type CreateBucketRequest struct {
	Bucket   string
	OwnerID  string
	ACL      string // Canned ACL: private, public-read, etc.
	Location string // Location constraint (region)
}

// CreateBucketResult contains the result of creating a bucket
type CreateBucketResult struct {
	Location string
}

// HeadBucketResult contains metadata for a bucket
type HeadBucketResult struct {
	Exists   bool
	OwnerID  string
	Location string
}

// GetBucketLocationResult contains the bucket's location
type GetBucketLocationResult struct {
	Location string
}

// ListBucketsRequest contains parameters for listing buckets
type ListBucketsRequest struct {
	OwnerID           string
	Prefix            string
	BucketRegion      string
	MaxBuckets        int
	ContinuationToken string
}

// ListBucketsResult contains the result of listing buckets
type ListBucketsResult struct {
	Buckets               []BucketInfo
	NextContinuationToken string
	IsTruncated           bool
	Prefix                string
}

// BucketInfo represents a bucket in list results
type BucketInfo struct {
	Name         string
	CreationDate time.Time
	Region       string
}

// Error codes for bucket operations
type ErrorCode int

const (
	ErrCodeNone ErrorCode = iota
	ErrCodeNoSuchBucket
	ErrCodeBucketAlreadyExists
	ErrCodeBucketAlreadyOwnedByYou
	ErrCodeBucketNotEmpty
	ErrCodeInternalError
)

// Error represents a bucket service error with an error code
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

// ToS3Error converts a bucket error to an S3 error code
func (e *Error) ToS3Error() s3err.ErrorCode {
	switch e.Code {
	case ErrCodeNoSuchBucket:
		return s3err.ErrNoSuchBucket
	case ErrCodeBucketAlreadyExists:
		return s3err.ErrBucketAlreadyExists
	case ErrCodeBucketAlreadyOwnedByYou:
		return s3err.ErrBucketAlreadyOwnedByYou
	case ErrCodeBucketNotEmpty:
		return s3err.ErrBucketNotEmpty
	default:
		return s3err.ErrInternalError
	}
}
