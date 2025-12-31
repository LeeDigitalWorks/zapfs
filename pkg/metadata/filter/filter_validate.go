package filter

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3action"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/utils"
)

// BucketChecker checks if a bucket exists
type BucketChecker interface {
	Get(bucket string) (owner string, exists bool)
}

type ValidationFilter struct {
	bucketChecker BucketChecker
}

func NewValidationFilter(bucketChecker BucketChecker) *ValidationFilter {
	return &ValidationFilter{
		bucketChecker: bucketChecker,
	}
}

func (f *ValidationFilter) Type() string {
	return "validation"
}

func (f *ValidationFilter) Run(d *data.Data) (Response, error) {
	if d.Ctx.Err() != nil {
		return nil, d.Ctx.Err()
	}

	if d.S3Info.Action == s3action.ListBuckets {
		return Next{}, nil
	}

	if utils.ValidateBucketName(d.S3Info.Bucket) != nil {
		return End{}, s3err.ErrInvalidBucketName
	}

	// Blocks operations on non-existent buckets
	if _, exists := f.bucketChecker.Get(d.S3Info.Bucket); !exists && d.S3Info.Action != s3action.CreateBucket {
		return End{}, s3err.ErrNoSuchBucket
	}

	if len(d.S3Info.Key) > 1024 {
		return End{}, s3err.ErrKeyTooLong
	}

	return Next{}, nil
}
