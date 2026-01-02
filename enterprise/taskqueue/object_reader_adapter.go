//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package taskqueue

import (
	"context"
	"io"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/object"
)

// ObjectServiceAdapter adapts object.Service to implement ObjectReader.
type ObjectServiceAdapter struct {
	service object.Service
}

// NewObjectServiceAdapter creates a new adapter for the object service.
func NewObjectServiceAdapter(service object.Service) *ObjectServiceAdapter {
	return &ObjectServiceAdapter{service: service}
}

// ReadObject reads an object's content from local storage.
func (a *ObjectServiceAdapter) ReadObject(ctx context.Context, bucket, key string) (io.ReadCloser, *ObjectMeta, error) {
	result, err := a.service.GetObject(ctx, &object.GetObjectRequest{
		Bucket: bucket,
		Key:    key,
	})
	if err != nil {
		return nil, nil, err
	}

	meta := &ObjectMeta{
		Size:        int64(result.Metadata.Size),
		ContentType: result.Metadata.ContentType,
		ETag:        result.Metadata.ETag,
	}

	return result.Body, meta, nil
}
