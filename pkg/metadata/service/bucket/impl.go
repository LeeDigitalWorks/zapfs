package bucket

import (
	"context"
	"errors"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/cache"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/uuid"
)

// Config holds configuration for the bucket service
type Config struct {
	DB                db.DB
	ManagerClient     client.Manager
	GlobalBucketCache *cache.GlobalBucketCache
	BucketStore       *cache.BucketStore
}

// serviceImpl implements the Service interface
type serviceImpl struct {
	db                db.DB
	managerClient     client.Manager
	globalBucketCache *cache.GlobalBucketCache
	bucketStore       *cache.BucketStore
}

// NewService creates a new bucket service
func NewService(cfg Config) (Service, error) {
	if cfg.DB == nil {
		return nil, errors.New("DB is required")
	}
	if cfg.ManagerClient == nil {
		return nil, errors.New("ManagerClient is required")
	}
	if cfg.GlobalBucketCache == nil {
		return nil, errors.New("GlobalBucketCache is required")
	}

	return &serviceImpl{
		db:                cfg.DB,
		managerClient:     cfg.ManagerClient,
		globalBucketCache: cfg.GlobalBucketCache,
		bucketStore:       cfg.BucketStore,
	}, nil
}

func (s *serviceImpl) CreateBucket(ctx context.Context, req *CreateBucketRequest) (*CreateBucketResult, error) {
	// Check if bucket already exists
	owner, exists := s.globalBucketCache.Get(req.Bucket)
	if exists {
		if owner == req.OwnerID {
			return nil, &Error{
				Code:    ErrCodeBucketAlreadyOwnedByYou,
				Message: "bucket already owned by you",
			}
		}
		return nil, &Error{
			Code:    ErrCodeBucketAlreadyExists,
			Message: "bucket already exists",
		}
	}

	// Create bucket in manager
	resp, err := s.managerClient.CreateCollection(ctx, &manager_pb.CreateCollectionRequest{
		Name:  req.Bucket,
		Owner: req.OwnerID,
	})
	if err != nil {
		logger.Error().Err(err).Str("bucket", req.Bucket).Msg("failed to create bucket in manager")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to create bucket",
			Err:     err,
		}
	}

	if !resp.Success {
		// Check if bucket was created by someone else
		if resp.Collection != nil && resp.Collection.Owner == req.OwnerID {
			return nil, &Error{
				Code:    ErrCodeBucketAlreadyOwnedByYou,
				Message: "bucket already owned by you",
			}
		}
		return nil, &Error{
			Code:    ErrCodeBucketAlreadyExists,
			Message: "bucket already exists",
		}
	}

	// Update global bucket cache
	s.globalBucketCache.Set(req.Bucket, req.OwnerID)

	// Store bucket in local DB
	now := time.Now()
	bucketInfo := &types.BucketInfo{
		ID:        uuid.New(),
		Name:      req.Bucket,
		OwnerID:   req.OwnerID,
		Region:    req.Location,
		CreatedAt: now.UnixNano(),
	}
	if err := s.db.CreateBucket(ctx, bucketInfo); err != nil {
		// Non-fatal: bucket was created in manager
		logger.Warn().Err(err).Str("bucket", req.Bucket).Msg("failed to store bucket in local DB (non-fatal)")
	}

	// Update local bucket cache
	if s.bucketStore != nil {
		s.bucketStore.SetBucket(req.Bucket, s3types.Bucket{
			ID:         bucketInfo.ID,
			Name:       req.Bucket,
			CreateTime: now,
			OwnerID:    req.OwnerID,
			Location:   req.Location,
		})
	}

	return &CreateBucketResult{
		Location: req.Location,
	}, nil
}

func (s *serviceImpl) DeleteBucket(ctx context.Context, bucket string) error {
	if bucket == "" {
		return &Error{
			Code:    ErrCodeNoSuchBucket,
			Message: "bucket name is required",
		}
	}

	// Check if bucket is empty
	objects, err := s.db.ListObjects(ctx, bucket, "", 1)
	if err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to check if bucket is empty")
		return &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to check if bucket is empty",
			Err:     err,
		}
	}
	if len(objects) > 0 {
		return &Error{
			Code:    ErrCodeBucketNotEmpty,
			Message: "bucket is not empty",
		}
	}

	// Delete from manager
	resp, err := s.managerClient.DeleteCollection(ctx, &manager_pb.DeleteCollectionRequest{
		Name: bucket,
	})
	if err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to delete bucket from manager")
		return &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to delete bucket",
			Err:     err,
		}
	}

	if !resp.Success {
		return &Error{
			Code:    ErrCodeNoSuchBucket,
			Message: "bucket does not exist",
		}
	}

	// Remove from caches
	s.globalBucketCache.Delete(bucket)
	if s.bucketStore != nil {
		s.bucketStore.DeleteBucket(bucket)
	}

	// Remove from local DB (best-effort)
	if err := s.db.DeleteBucket(ctx, bucket); err != nil {
		logger.Warn().Err(err).Str("bucket", bucket).Msg("failed to delete bucket from local DB (non-fatal)")
	}

	return nil
}

func (s *serviceImpl) HeadBucket(ctx context.Context, bucket string) (*HeadBucketResult, error) {
	if bucket == "" {
		return nil, &Error{
			Code:    ErrCodeNoSuchBucket,
			Message: "bucket name is required",
		}
	}

	// Check global cache first
	owner, exists := s.globalBucketCache.Get(bucket)
	if !exists {
		return nil, &Error{
			Code:    ErrCodeNoSuchBucket,
			Message: "bucket does not exist",
		}
	}

	result := &HeadBucketResult{
		Exists:  true,
		OwnerID: owner,
	}

	// Get additional info from local cache
	if s.bucketStore != nil {
		if bucketInfo, ok := s.bucketStore.GetBucket(bucket); ok {
			result.Location = bucketInfo.Location
			result.OwnerID = bucketInfo.OwnerID
		}
	}

	return result, nil
}

func (s *serviceImpl) GetBucketLocation(ctx context.Context, bucket string) (*GetBucketLocationResult, error) {
	if bucket == "" {
		return nil, &Error{
			Code:    ErrCodeNoSuchBucket,
			Message: "bucket name is required",
		}
	}

	// Check if bucket exists
	_, exists := s.globalBucketCache.Get(bucket)
	if !exists {
		return nil, &Error{
			Code:    ErrCodeNoSuchBucket,
			Message: "bucket does not exist",
		}
	}

	// Get location from bucket store
	location := ""
	if s.bucketStore != nil {
		if bucketInfo, ok := s.bucketStore.GetBucket(bucket); ok {
			location = bucketInfo.Location
		}
	}

	return &GetBucketLocationResult{
		Location: location,
	}, nil
}

func (s *serviceImpl) ListBuckets(ctx context.Context, req *ListBucketsRequest) (*ListBucketsResult, error) {
	// Set defaults
	maxBuckets := req.MaxBuckets
	if maxBuckets <= 0 {
		maxBuckets = 10000
	}
	if maxBuckets > 10000 {
		maxBuckets = 10000
	}

	// Query database
	listResult, err := s.db.ListBuckets(ctx, &db.ListBucketsParams{
		OwnerID:           req.OwnerID,
		Prefix:            req.Prefix,
		BucketRegion:      req.BucketRegion,
		MaxBuckets:        maxBuckets,
		ContinuationToken: req.ContinuationToken,
	})
	if err != nil {
		logger.Error().Err(err).Str("owner", req.OwnerID).Msg("failed to list buckets")
		return nil, &Error{
			Code:    ErrCodeInternalError,
			Message: "failed to list buckets",
			Err:     err,
		}
	}

	// Convert to response format
	buckets := make([]BucketInfo, 0, len(listResult.Buckets))
	for _, bi := range listResult.Buckets {
		buckets = append(buckets, BucketInfo{
			Name:         bi.Name,
			CreationDate: time.Unix(0, bi.CreatedAt).UTC(),
			Region:       bi.Region,
		})
	}

	return &ListBucketsResult{
		Buckets:               buckets,
		NextContinuationToken: listResult.NextContinuationToken,
		IsTruncated:           listResult.IsTruncated,
		Prefix:                req.Prefix,
	}, nil
}
