// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"context"
	"io"
	"sync/atomic"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/utils"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// BucketCacheClient defines the manager operations needed by GlobalBucketCache.
// This is a subset of manager_pb.ManagerServiceClient, allowing the use of
// ManagerClientPool (which provides leader-aware routing) or direct proto clients.
type BucketCacheClient interface {
	ListCollections(ctx context.Context, req *manager_pb.ListCollectionsRequest) (manager_pb.ManagerService_ListCollectionsClient, error)
	GetCollection(ctx context.Context, req *manager_pb.GetCollectionRequest) (*manager_pb.GetCollectionResponse, error)
}

type GlobalBucketCache struct {
	cache           *Cache[string, string]
	lastRefreshTime atomic.Pointer[time.Time]
	managerClient   BucketCacheClient
}

func NewGlobalBucketCache(ctx context.Context, managerClient BucketCacheClient) *GlobalBucketCache {
	lookupFunc := func(Ctx context.Context, bucket string) (string, error) {
		return lookupBucketOwner(Ctx, managerClient, bucket)
	}
	return &GlobalBucketCache{
		cache: New(ctx,
			WithLoadFunc(lookupFunc),
		),
		managerClient: managerClient,
	}
}

func (gbc *GlobalBucketCache) Get(bucket string) (string, bool) {
	return gbc.cache.Get(bucket)
}

func (gbc *GlobalBucketCache) Set(bucket string, owner string) {
	gbc.cache.Set(bucket, owner)
}

func (gbc *GlobalBucketCache) Delete(bucket string) {
	gbc.cache.Delete(bucket)
}

func (gbc *GlobalBucketCache) IsReady() bool {
	return gbc.cache.HasLoaded()
}

func (gbc *GlobalBucketCache) LoadBuckets(ctx context.Context, refreshInterval time.Duration) {
	// Initial load
	if err := gbc.refreshBuckets(ctx); err != nil {
		logger.Error().Err(err).Msg("error refreshing global buckets cache")
	}

	// Use jittered ticker to prevent thundering herd across metadata servers
	// 10% jitter spreads out refreshes
	tickCh, stopTicker := utils.JitteredTicker(refreshInterval, 0.1)
	defer stopTicker()

	for {
		select {
		case <-ctx.Done():
			return
		case <-tickCh:
			if err := gbc.refreshBuckets(ctx); err != nil {
				logger.Error().Err(err).Msg("error refreshing global buckets cache")
			}
		}
	}
}

func (gbc *GlobalBucketCache) refreshBuckets(ctx context.Context) error {
	return gbc.cache.Load(func(yield func(Entity[string, string], error) bool) {
		startTime := time.Now().UTC()

		req := &manager_pb.ListCollectionsRequest{}
		if lastRefresh := gbc.lastRefreshTime.Load(); lastRefresh != nil {
			req.SinceTime = timestamppb.New(lastRefresh.Add(-time.Minute))
		}
		stream, err := gbc.managerClient.ListCollections(ctx, req)
		if err != nil {
			yield(Entity[string, string]{}, err)
			return
		}

		for {
			collection, err := stream.Recv()
			if err == io.EOF {
				gbc.lastRefreshTime.Store(&startTime)
				return
			}
			if err != nil {
				yield(Entity[string, string]{}, err)
				return
			}

			entity := Entity[string, string]{
				Key:       collection.Name,
				Value:     collection.Owner,
				IsDeleted: collection.IsDeleted,
			}
			if !yield(entity, nil) {
				return
			}
		}
	})
}

func lookupBucketOwner(ctx context.Context, managerClient BucketCacheClient, bucket string) (string, error) {
	req := &manager_pb.GetCollectionRequest{
		Name: bucket,
	}
	resp, err := managerClient.GetCollection(ctx, req)
	if err != nil {
		return "", err
	}
	return resp.Collection.Owner, nil
}
