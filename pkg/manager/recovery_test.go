// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/LeeDigitalWorks/zapfs/proto/metadata_pb"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockMetadataLister implements metadataLister for testing
type mockMetadataLister struct {
	collections []*metadata_pb.CollectionInfo
	err         error
}

func (m *mockMetadataLister) ListCollections(ctx context.Context, req *metadata_pb.ListCollectionsRequest, opts ...grpc.CallOption) (*metadata_pb.ListCollectionsResponse, error) {
	if m.err != nil {
		return nil, m.err
	}
	return &metadata_pb.ListCollectionsResponse{
		Collections: m.collections,
	}, nil
}

func TestQueryMetadataForCollections(t *testing.T) {
	tests := []struct {
		name        string
		collections []*metadata_pb.CollectionInfo
		wantCount   int
	}{
		{
			name: "returns all collections",
			collections: []*metadata_pb.CollectionInfo{
				{Collection: "bucket1"},
				{Collection: "bucket2"},
			},
			wantCount: 2,
		},
		{
			name:        "empty collections",
			collections: nil,
			wantCount:   0,
		},
		{
			name: "collections with creation time",
			collections: []*metadata_pb.CollectionInfo{
				{Collection: "bucket1", CreatedAt: timestamppb.Now()},
				{Collection: "bucket2"},
			},
			wantCount: 2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockMetadataLister{collections: tc.collections}
			result, err := queryMetadataForCollections(context.Background(), client)
			require.NoError(t, err)
			assert.Len(t, result, tc.wantCount)
		})
	}
}

func TestMergeCollections(t *testing.T) {
	tests := []struct {
		name     string
		inputs   [][]collectionRecord
		wantKeys []string
	}{
		{
			name: "deduplicates same bucket from multiple sources",
			inputs: [][]collectionRecord{
				{{Name: "bucket1", Owner: "user1"}},
				{{Name: "bucket1", Owner: "user1"}, {Name: "bucket2", Owner: "user2"}},
			},
			wantKeys: []string{"bucket1", "bucket2"},
		},
		{
			name: "empty inputs",
			inputs: [][]collectionRecord{
				{},
				{},
			},
			wantKeys: []string{},
		},
		{
			name: "single source",
			inputs: [][]collectionRecord{
				{{Name: "bucket1"}, {Name: "bucket2"}, {Name: "bucket3"}},
			},
			wantKeys: []string{"bucket1", "bucket2", "bucket3"},
		},
		{
			name:     "no sources",
			inputs:   [][]collectionRecord{},
			wantKeys: []string{},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := mergeCollections(tc.inputs...)
			assert.Len(t, result, len(tc.wantKeys))
			for _, key := range tc.wantKeys {
				_, exists := result[key]
				assert.True(t, exists, "expected key %s", key)
			}
		})
	}
}

func TestMergeCollections_KeepsEarlierCreationTime(t *testing.T) {
	earlier := collectionRecord{
		Name:      "bucket1",
		Owner:     "user1",
		CreatedAt: mustParseTime("2024-01-01T00:00:00Z"),
	}
	later := collectionRecord{
		Name:      "bucket1",
		Owner:     "user1",
		CreatedAt: mustParseTime("2024-06-01T00:00:00Z"),
	}

	// Test: later comes first, earlier should win
	result := mergeCollections([]collectionRecord{later}, []collectionRecord{earlier})
	assert.Equal(t, earlier.CreatedAt, result["bucket1"].CreatedAt)

	// Test: earlier comes first, earlier should still be kept
	result = mergeCollections([]collectionRecord{earlier}, []collectionRecord{later})
	assert.Equal(t, earlier.CreatedAt, result["bucket1"].CreatedAt)
}

func mustParseTime(s string) (t time.Time) {
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		panic(err)
	}
	return t
}
