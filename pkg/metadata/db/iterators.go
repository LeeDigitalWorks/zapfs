// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package db

import (
	"context"
	"iter"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// IterBuckets returns an iterator that yields all buckets from the database.
// This is more memory-efficient than ListBuckets as it streams results.
// The iterator handles pagination internally.
func IterBuckets(ctx context.Context, store BucketStore, params *ListBucketsParams) iter.Seq2[*types.BucketInfo, error] {
	return func(yield func(*types.BucketInfo, error) bool) {
		p := &ListBucketsParams{}
		if params != nil {
			*p = *params
		}
		if p.MaxBuckets <= 0 {
			p.MaxBuckets = 1000
		}

		for {
			result, err := store.ListBuckets(ctx, p)
			if err != nil {
				yield(nil, err)
				return
			}

			for _, bucket := range result.Buckets {
				if !yield(bucket, nil) {
					return
				}
			}

			if !result.IsTruncated {
				return
			}
			p.ContinuationToken = result.NextContinuationToken
		}
	}
}

// IterObjects returns an iterator that yields all objects from a bucket.
// This is more memory-efficient than ListObjectsV2 as it streams results.
// The iterator handles pagination internally.
func IterObjects(ctx context.Context, store ObjectStore, params *ListObjectsParams) iter.Seq2[*types.ObjectRef, error] {
	return func(yield func(*types.ObjectRef, error) bool) {
		p := &ListObjectsParams{}
		if params != nil {
			*p = *params
		}
		if p.MaxKeys <= 0 {
			p.MaxKeys = 1000
		}

		for {
			result, err := store.ListObjectsV2(ctx, p)
			if err != nil {
				yield(nil, err)
				return
			}

			for _, obj := range result.Objects {
				if !yield(obj, nil) {
					return
				}
			}

			if !result.IsTruncated {
				return
			}
			p.ContinuationToken = result.NextContinuationToken
		}
	}
}
