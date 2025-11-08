package api

import (
	"context"

	"zapfs/pkg/metadata/service/object"
	"zapfs/pkg/s3api/s3types"
)

// crrHookAdapter adapts api.CRRHook to implement object.CRRHook interface.
// This allows the existing CRRHook implementations to work with the service layer.
type crrHookAdapter struct {
	hook *CRRHook
}

// adaptCRRHook creates an adapter that implements object.CRRHook.
// Returns nil if hook is nil.
func adaptCRRHook(hook *CRRHook) object.CRRHook {
	if hook == nil {
		return nil
	}
	return &crrHookAdapter{hook: hook}
}

func (a *crrHookAdapter) AfterPutObject(ctx context.Context, bucketInfo interface{}, key, etag string, size int64) {
	if bucket, ok := bucketInfo.(*s3types.Bucket); ok {
		a.hook.AfterPutObject(ctx, bucket, key, etag, size)
	}
}

func (a *crrHookAdapter) AfterDeleteObject(ctx context.Context, bucketInfo interface{}, key string) {
	if bucket, ok := bucketInfo.(*s3types.Bucket); ok {
		a.hook.AfterDeleteObject(ctx, bucket, key)
	}
}
