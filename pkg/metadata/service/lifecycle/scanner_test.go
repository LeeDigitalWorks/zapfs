// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package lifecycle

import (
	"testing"
	"time"

	dbmocks "github.com/LeeDigitalWorks/zapfs/mocks/db"
	mockqueue "github.com/LeeDigitalWorks/zapfs/mocks/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestScanner_listObjects_WithVersioning(t *testing.T) {
	mockDB := dbmocks.NewMockDB(t)
	mockQueue := mockqueue.NewMockQueue(t)

	scanner := NewScanner(mockDB, mockQueue, DefaultConfig())

	now := time.Now()
	oneDayAgo := now.Add(-24 * time.Hour)
	twoDaysAgo := now.Add(-48 * time.Hour)

	// Setup: ListObjectVersions returns versions with proper IsLatest flags
	mockDB.EXPECT().ListObjectVersions(
		mock.Anything, // ctx
		"test-bucket", // bucket
		"",            // prefix
		"",            // keyMarker
		"",            // versionIDMarker
		"",            // delimiter
		1000,          // limit
	).Return([]*types.ObjectVersion{
		{
			Key:            "file1.txt",
			VersionID:      "version-1",
			IsLatest:       true,
			IsDeleteMarker: false,
			LastModified:   now.UnixNano(),
			Size:           100,
			ETag:           "etag1",
		},
		{
			Key:            "file1.txt",
			VersionID:      "version-0",
			IsLatest:       false, // Previous version
			IsDeleteMarker: false,
			LastModified:   oneDayAgo.UnixNano(),
			Size:           90,
			ETag:           "etag0",
		},
		{
			Key:            "file2.txt",
			VersionID:      "version-2",
			IsLatest:       true,
			IsDeleteMarker: true, // Delete marker
			LastModified:   now.UnixNano(),
			Size:           0,
		},
		{
			Key:            "file2.txt",
			VersionID:      "version-1",
			IsLatest:       false,
			IsDeleteMarker: false,
			LastModified:   twoDaysAgo.UnixNano(),
			Size:           200,
			ETag:           "etag2",
		},
	}, false, "", "", nil)

	// Call listObjects
	states, nextMarker, err := scanner.listObjects("test-bucket", "", 1000)

	assert.NoError(t, err)
	assert.Empty(t, nextMarker)
	assert.Len(t, states, 4)

	// Verify first version - current version of file1.txt
	assert.Equal(t, "file1.txt", states[0].Name)
	assert.Equal(t, "version-1", states[0].VersionID)
	assert.True(t, states[0].IsLatest)
	assert.False(t, states[0].DeleteMarker)
	assert.Equal(t, int64(100), states[0].Size)

	// Verify second version - previous version of file1.txt
	assert.Equal(t, "file1.txt", states[1].Name)
	assert.Equal(t, "version-0", states[1].VersionID)
	assert.False(t, states[1].IsLatest) // Not latest!
	assert.False(t, states[1].DeleteMarker)
	assert.Equal(t, int64(90), states[1].Size)

	// Verify third version - delete marker for file2.txt
	assert.Equal(t, "file2.txt", states[2].Name)
	assert.Equal(t, "version-2", states[2].VersionID)
	assert.True(t, states[2].IsLatest)
	assert.True(t, states[2].DeleteMarker) // Delete marker!

	// Verify fourth version - previous version of file2.txt (before delete)
	assert.Equal(t, "file2.txt", states[3].Name)
	assert.Equal(t, "version-1", states[3].VersionID)
	assert.False(t, states[3].IsLatest)
	assert.False(t, states[3].DeleteMarker)
}

func TestScanner_listObjects_NonVersionedBucket(t *testing.T) {
	mockDB := dbmocks.NewMockDB(t)
	mockQueue := mockqueue.NewMockQueue(t)

	scanner := NewScanner(mockDB, mockQueue, DefaultConfig())

	now := time.Now()

	// For non-versioned buckets, ListObjectVersions returns single versions with IsLatest=true
	mockDB.EXPECT().ListObjectVersions(
		mock.Anything,
		"non-versioned-bucket",
		"", "", "", "",
		1000,
	).Return([]*types.ObjectVersion{
		{
			Key:            "file1.txt",
			VersionID:      "id-1",
			IsLatest:       true, // Always true for non-versioned
			IsDeleteMarker: false,
			LastModified:   now.UnixNano(),
			Size:           100,
		},
		{
			Key:            "file2.txt",
			VersionID:      "id-2",
			IsLatest:       true,
			IsDeleteMarker: false,
			LastModified:   now.UnixNano(),
			Size:           200,
		},
	}, false, "", "", nil)

	states, _, err := scanner.listObjects("non-versioned-bucket", "", 1000)

	assert.NoError(t, err)
	assert.Len(t, states, 2)

	// All objects should have IsLatest=true in non-versioned bucket
	assert.True(t, states[0].IsLatest)
	assert.True(t, states[1].IsLatest)
	assert.False(t, states[0].DeleteMarker)
	assert.False(t, states[1].DeleteMarker)
}

func TestScanner_EvalNoncurrentVersions(t *testing.T) {
	// This test verifies that the lifecycle evaluator correctly handles
	// noncurrent versions when IsLatest=false is properly set.

	now := time.Now()
	thirtyDaysAgo := now.Add(-30 * 24 * time.Hour)

	// Create a lifecycle rule that expires noncurrent versions after 7 days
	ruleID := "expire-noncurrent"
	days := int64(7)
	lifecycle := &s3types.Lifecycle{
		Rules: []s3types.LifecycleRule{
			{
				ID:     &ruleID,
				Status: s3types.LifecycleStatusEnabled,
				NoncurrentVersionExpiration: &s3types.LifecycleNoncurrentVersionExpiration{
					Days: &days,
				},
			},
		},
	}

	evaluator := s3types.NewEvaluator(lifecycle)

	// Test: Current version should NOT be expired
	currentVersion := s3types.ObjectState{
		Name:     "file.txt",
		IsLatest: true,
		ModTime:  thirtyDaysAgo, // Even if old, current version shouldn't be deleted by noncurrent rule
	}
	event := evaluator.Eval(currentVersion, now)
	assert.Equal(t, s3types.NoneAction, event.Action, "Current version should not be expired by noncurrent rule")

	// Test: Noncurrent version older than 7 days SHOULD be expired
	noncurrentVersion := s3types.ObjectState{
		Name:     "file.txt",
		IsLatest: false, // This is the key - properly marked as not latest
		ModTime:  thirtyDaysAgo,
	}
	event = evaluator.Eval(noncurrentVersion, now)
	assert.Equal(t, s3types.DeleteVersionAction, event.Action, "Noncurrent version should be expired")
}

func TestLifecycleHasTagFilter(t *testing.T) {
	tests := []struct {
		name     string
		lc       *s3types.Lifecycle
		expected bool
	}{
		{
			name:     "nil lifecycle",
			lc:       nil,
			expected: false,
		},
		{
			name: "no tag filter",
			lc: &s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: ptrString("logs/"),
						},
					},
				},
			},
			expected: false,
		},
		{
			name: "single tag filter",
			lc: &s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Tag: &s3types.Tag{Key: "env", Value: "dev"},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "And filter with tags",
			lc: &s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							And: &s3types.LifecycleRuleAndOperator{
								Prefix: ptrString("logs/"),
								Tags:   []*s3types.Tag{{Key: "env", Value: "prod"}},
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "disabled rule with tag filter",
			lc: &s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						Status: s3types.LifecycleStatusDisabled,
						Filter: &s3types.LifecycleFilter{
							Tag: &s3types.Tag{Key: "env", Value: "dev"},
						},
					},
				},
			},
			expected: false, // Disabled rules don't count
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := lifecycleHasTagFilter(tc.lc)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestTagSetToQueryString(t *testing.T) {
	tests := []struct {
		name     string
		tagSet   *s3types.TagSet
		expected string
	}{
		{
			name:     "nil tagset",
			tagSet:   nil,
			expected: "",
		},
		{
			name:     "empty tagset",
			tagSet:   &s3types.TagSet{Tags: []s3types.Tag{}},
			expected: "",
		},
		{
			name: "single tag",
			tagSet: &s3types.TagSet{
				Tags: []s3types.Tag{{Key: "env", Value: "prod"}},
			},
			expected: "env=prod",
		},
		{
			name: "multiple tags",
			tagSet: &s3types.TagSet{
				Tags: []s3types.Tag{
					{Key: "env", Value: "prod"},
					{Key: "team", Value: "eng"},
				},
			},
			// Note: url.Values.Encode() sorts keys alphabetically
			expected: "env=prod&team=eng",
		},
		{
			name: "tag with special characters",
			tagSet: &s3types.TagSet{
				Tags: []s3types.Tag{{Key: "path", Value: "/data/files"}},
			},
			expected: "path=%2Fdata%2Ffiles",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := tagSetToQueryString(tc.tagSet)
			assert.Equal(t, tc.expected, result)
		})
	}
}

// Helper function to create string pointer
func ptrString(s string) *string {
	return &s
}
