package cache

import (
	"context"
	"iter"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
)

// BucketIterFunc returns an iterator that yields buckets from the database.
// This is more memory-efficient than returning slices as it streams results.
type BucketIterFunc func(ctx context.Context) iter.Seq2[*types.BucketInfo, error]

// BucketLoader loads buckets from the database into the BucketStore cache.
// It runs periodically to keep the cache in sync with the database.
type BucketLoader struct {
	iterFunc    BucketIterFunc
	bucketStore *BucketStore
}

// NewBucketLoader creates a new BucketLoader.
// The iterFunc should return an iterator that yields buckets from the database.
func NewBucketLoader(iterFunc BucketIterFunc, bucketStore *BucketStore) *BucketLoader {
	return &BucketLoader{
		iterFunc:    iterFunc,
		bucketStore: bucketStore,
	}
}

// LoadBuckets runs a background loop that periodically refreshes the bucket cache.
// It loads all buckets from the database and populates the BucketStore.
// Config fields (Tagging, CORS, etc.) are loaded lazily on first access.
func (bl *BucketLoader) LoadBuckets(ctx context.Context, refreshInterval time.Duration) {
	// Initial load with jittered delay to prevent all instances loading simultaneously
	initialDelay := utils.JitterUp(time.Second, 2.0) // 1-3 seconds
	select {
	case <-time.After(initialDelay):
	case <-ctx.Done():
		return
	}

	if err := bl.refreshBuckets(ctx); err != nil {
		logger.Error().Err(err).Msg("error during initial bucket cache load")
	} else {
		logger.Info().Msg("bucket cache initial load complete")
	}

	// Use jittered ticker to prevent thundering herd across metadata servers
	// 10% jitter on 1 minute = ±6 seconds
	tickCh, stopTicker := utils.JitteredTicker(refreshInterval, 0.1)
	defer stopTicker()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tickCh:
			if err := bl.refreshBuckets(ctx); err != nil {
				logger.Error().Err(err).Msg("error refreshing bucket cache")
			}
		}
	}
}

// refreshBuckets fetches all buckets from DB and updates the cache.
// Existing config fields in cached buckets are preserved.
func (bl *BucketLoader) refreshBuckets(ctx context.Context) error {
	startTime := time.Now()
	var totalLoaded int

	// Iterate through all buckets using the iterator
	for info, err := range bl.iterFunc(ctx) {
		if err != nil {
			return err
		}

		// Check if bucket already exists in cache
		existing, exists := bl.bucketStore.GetBucket(info.Name)
		if exists {
			// Update basic fields but preserve config fields
			existing.ID = info.ID
			existing.Name = info.Name
			existing.OwnerID = info.OwnerID
			existing.CreateTime = time.Unix(0, info.CreatedAt)
			existing.Versioning = s3types.Versioning(info.Versioning)
			existing.Location = info.Region
			bl.bucketStore.SetBucket(info.Name, existing)
		} else {
			// New bucket - add to cache with basic info
			bucket := s3types.Bucket{
				ID:         info.ID,
				Name:       info.Name,
				OwnerID:    info.OwnerID,
				CreateTime: time.Unix(0, info.CreatedAt),
				Versioning: s3types.Versioning(info.Versioning),
				Location:   info.Region,
			}
			bl.bucketStore.SetBucket(info.Name, bucket)
		}
		totalLoaded++
	}

	// Mark cache as loaded (for IsReady checks)
	bl.bucketStore.cache.MarkLoaded()

	logger.Debug().
		Int("buckets", totalLoaded).
		Dur("duration", time.Since(startTime)).
		Msg("bucket cache refresh complete")

	return nil
}
