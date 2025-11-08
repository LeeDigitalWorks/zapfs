package bucket

import (
	"context"
)

// Service defines the interface for bucket operations.
// This separates business logic from HTTP handling.
type Service interface {
	// CreateBucket creates a new bucket.
	// Returns ErrBucketAlreadyExists if bucket exists with different owner.
	// Returns ErrBucketAlreadyOwnedByYou if bucket exists with same owner.
	CreateBucket(ctx context.Context, req *CreateBucketRequest) (*CreateBucketResult, error)

	// DeleteBucket deletes an empty bucket.
	// Returns ErrBucketNotEmpty if bucket contains objects.
	// Returns ErrNoSuchBucket if bucket doesn't exist.
	DeleteBucket(ctx context.Context, bucket string) error

	// HeadBucket checks if a bucket exists and returns its metadata.
	// Returns ErrNoSuchBucket if bucket doesn't exist.
	HeadBucket(ctx context.Context, bucket string) (*HeadBucketResult, error)

	// GetBucketLocation returns the region where the bucket resides.
	// Returns ErrNoSuchBucket if bucket doesn't exist.
	GetBucketLocation(ctx context.Context, bucket string) (*GetBucketLocationResult, error)

	// ListBuckets lists all buckets owned by a user.
	ListBuckets(ctx context.Context, req *ListBucketsRequest) (*ListBucketsResult, error)
}
