//go:build integration

package testutil

import (
	"context"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"

	"github.com/stretchr/testify/require"
)

// MetadataClient wraps metadata_pb.MetadataServiceClient with test helpers
type MetadataClient struct {
	metadata_pb.MetadataServiceClient
	t    *testing.T
	addr string
}

// NewMetadataClient creates a metadata service client with test helpers
func NewMetadataClient(t *testing.T, addr string) *MetadataClient {
	t.Helper()
	conn := NewGRPCConn(t, addr)
	return &MetadataClient{
		MetadataServiceClient: metadata_pb.NewMetadataServiceClient(conn),
		t:                     t,
		addr:                  addr,
	}
}

// Ping pings the metadata server
func (mc *MetadataClient) Ping() *metadata_pb.PingResponse {
	mc.t.Helper()

	ctx, cancel := WithShortTimeout(context.Background())
	defer cancel()

	resp, err := mc.MetadataServiceClient.Ping(ctx, &metadata_pb.PingRequest{})
	require.NoError(mc.t, err, "failed to ping metadata server")
	return resp
}

// CreateCollection creates a new collection
func (mc *MetadataClient) CreateCollection(collection string) *metadata_pb.CreateCollectionResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.MetadataServiceClient.CreateCollection(ctx, &metadata_pb.CreateCollectionRequest{
		Collection: collection,
	})
	require.NoError(mc.t, err, "failed to create collection %s", collection)
	return resp
}

// DeleteCollection deletes a collection
func (mc *MetadataClient) DeleteCollection(collection string) *metadata_pb.DeleteCollectionResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.MetadataServiceClient.DeleteCollection(ctx, &metadata_pb.DeleteCollectionRequest{
		Collection: collection,
	})
	require.NoError(mc.t, err, "failed to delete collection %s", collection)
	return resp
}

// ListCollections lists all collections
func (mc *MetadataClient) ListCollections() *metadata_pb.ListCollectionsResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.MetadataServiceClient.ListCollections(ctx, &metadata_pb.ListCollectionsRequest{})
	require.NoError(mc.t, err, "failed to list collections")
	return resp
}

// GetObject retrieves object metadata
func (mc *MetadataClient) GetObject(collection, key string) *metadata_pb.GetObjectResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.MetadataServiceClient.GetObject(ctx, &metadata_pb.GetObjectRequest{
		Collection: collection,
		Key:        key,
	})
	require.NoError(mc.t, err, "failed to get object %s/%s", collection, key)
	return resp
}

// DeleteObject deletes an object
func (mc *MetadataClient) DeleteObject(collection, key string) *metadata_pb.DeleteObjectResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	resp, err := mc.MetadataServiceClient.DeleteObject(ctx, &metadata_pb.DeleteObjectRequest{
		Collection: collection,
		Key:        key,
	})
	require.NoError(mc.t, err, "failed to delete object %s/%s", collection, key)
	return resp
}

// ListObjects lists objects in a collection
func (mc *MetadataClient) ListObjects(collection string, opts ...ListObjectsOption) *metadata_pb.ListObjectsResponse {
	mc.t.Helper()

	ctx, cancel := WithTimeout(context.Background())
	defer cancel()

	req := &metadata_pb.ListObjectsRequest{
		Collection: collection,
	}

	for _, opt := range opts {
		opt(req)
	}

	resp, err := mc.MetadataServiceClient.ListObjects(ctx, req)
	require.NoError(mc.t, err, "failed to list objects in %s", collection)
	return resp
}

// ListObjectsOption modifies ListObjectsRequest
type ListObjectsOption func(*metadata_pb.ListObjectsRequest)

// WithPrefix sets the prefix filter
func WithPrefix(prefix string) ListObjectsOption {
	return func(r *metadata_pb.ListObjectsRequest) {
		r.Prefix = prefix
	}
}

// WithMaxKeys sets the max keys
func WithMaxKeys(max int32) ListObjectsOption {
	return func(r *metadata_pb.ListObjectsRequest) {
		r.MaxKeys = max
	}
}

// WithContinuationToken sets the continuation token for pagination
func WithContinuationToken(token string) ListObjectsOption {
	return func(r *metadata_pb.ListObjectsRequest) {
		r.ContinuationToken = token
	}
}
