//go:build !enterprise

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// CRRHook is a stub for community edition.
type CRRHook struct{}

// NewCRRHook returns nil in community edition.
func NewCRRHook(queue any, localRegion string) *CRRHook {
	return nil
}

func (h *CRRHook) AfterPutObject(ctx context.Context, bucket *s3types.Bucket, key, etag string, size int64) {
}

func (h *CRRHook) AfterDeleteObject(ctx context.Context, bucket *s3types.Bucket, key string) {
}
