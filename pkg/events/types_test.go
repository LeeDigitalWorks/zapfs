package events

import (
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/stretchr/testify/assert"
)

func TestMatchesEventType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		pattern   EventType
		eventName string
		expected  bool
	}{
		{
			name:      "exact match",
			pattern:   EventObjectCreatedPut,
			eventName: "s3:ObjectCreated:Put",
			expected:  true,
		},
		{
			name:      "exact match - no match",
			pattern:   EventObjectCreatedPut,
			eventName: "s3:ObjectCreated:Post",
			expected:  false,
		},
		{
			name:      "wildcard match - ObjectCreated",
			pattern:   EventObjectCreated,
			eventName: "s3:ObjectCreated:Put",
			expected:  true,
		},
		{
			name:      "wildcard match - ObjectCreated Post",
			pattern:   EventObjectCreated,
			eventName: "s3:ObjectCreated:Post",
			expected:  true,
		},
		{
			name:      "wildcard match - ObjectCreated Copy",
			pattern:   EventObjectCreated,
			eventName: "s3:ObjectCreated:Copy",
			expected:  true,
		},
		{
			name:      "wildcard match - ObjectCreated CompleteMultipartUpload",
			pattern:   EventObjectCreated,
			eventName: "s3:ObjectCreated:CompleteMultipartUpload",
			expected:  true,
		},
		{
			name:      "wildcard no match - different category",
			pattern:   EventObjectCreated,
			eventName: "s3:ObjectRemoved:Delete",
			expected:  false,
		},
		{
			name:      "wildcard match - ObjectRemoved",
			pattern:   EventObjectRemoved,
			eventName: "s3:ObjectRemoved:Delete",
			expected:  true,
		},
		{
			name:      "wildcard match - ObjectRemoved DeleteMarker",
			pattern:   EventObjectRemoved,
			eventName: "s3:ObjectRemoved:DeleteMarkerCreated",
			expected:  true,
		},
		{
			name:      "wildcard match - Replication",
			pattern:   EventReplication,
			eventName: "s3:Replication:OperationCompletedReplication",
			expected:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := MatchesEventType(tc.pattern, tc.eventName)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMatchesFilterRules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		prefix   string
		suffix   string
		expected bool
	}{
		{
			name:     "no filters",
			key:      "images/photo.jpg",
			prefix:   "",
			suffix:   "",
			expected: true,
		},
		{
			name:     "prefix match",
			key:      "images/photo.jpg",
			prefix:   "images/",
			suffix:   "",
			expected: true,
		},
		{
			name:     "prefix no match",
			key:      "documents/file.pdf",
			prefix:   "images/",
			suffix:   "",
			expected: false,
		},
		{
			name:     "suffix match",
			key:      "images/photo.jpg",
			prefix:   "",
			suffix:   ".jpg",
			expected: true,
		},
		{
			name:     "suffix no match",
			key:      "images/photo.jpg",
			prefix:   "",
			suffix:   ".png",
			expected: false,
		},
		{
			name:     "prefix and suffix match",
			key:      "images/photo.jpg",
			prefix:   "images/",
			suffix:   ".jpg",
			expected: true,
		},
		{
			name:     "prefix match suffix no match",
			key:      "images/photo.jpg",
			prefix:   "images/",
			suffix:   ".png",
			expected: false,
		},
		{
			name:     "prefix no match suffix match",
			key:      "documents/photo.jpg",
			prefix:   "images/",
			suffix:   ".jpg",
			expected: false,
		},
		{
			name:     "key shorter than prefix",
			key:      "a",
			prefix:   "images/",
			suffix:   "",
			expected: false,
		},
		{
			name:     "key shorter than suffix",
			key:      "a",
			prefix:   "",
			suffix:   ".jpg",
			expected: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			result := MatchesFilterRules(tc.key, tc.prefix, tc.suffix)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestBuildS3Event(t *testing.T) {
	t.Parallel()

	payload := &taskqueue.EventPayload{
		EventName: "s3:ObjectCreated:Put",
		Bucket:    "my-bucket",
		Key:       "path/to/object.txt",
		Size:      1024,
		ETag:      "d41d8cd98f00b204e9800998ecf8427e",
		VersionID: "v1",
		OwnerID:   "owner123",
		RequestID: "req-abc-123",
		SourceIP:  "192.168.1.100",
		Timestamp: time.Date(2024, 12, 31, 12, 0, 0, 0, time.UTC).UnixMilli(),
		Sequencer: "0123456789ABCDEF",
	}

	event := BuildS3Event(payload, "us-west-2", "config-id-1")

	assert.Len(t, event.Records, 1)
	record := event.Records[0]

	assert.Equal(t, "2.1", record.EventVersion)
	assert.Equal(t, "zapfs:s3", record.EventSource)
	assert.Equal(t, "us-west-2", record.AWSRegion)
	assert.Equal(t, "s3:ObjectCreated:Put", record.EventName)

	assert.Equal(t, "owner123", record.UserIdentity.PrincipalID)
	assert.Equal(t, "192.168.1.100", record.RequestParameters.SourceIPAddress)
	assert.Equal(t, "req-abc-123", record.ResponseElements.RequestID)

	assert.Equal(t, "1.0", record.S3.SchemaVersion)
	assert.Equal(t, "config-id-1", record.S3.ConfigurationID)
	assert.Equal(t, "my-bucket", record.S3.Bucket.Name)
	assert.Equal(t, "owner123", record.S3.Bucket.OwnerIdentity.PrincipalID)
	assert.Equal(t, "arn:aws:s3:::my-bucket", record.S3.Bucket.ARN)

	assert.Equal(t, "path/to/object.txt", record.S3.Object.Key)
	assert.Equal(t, int64(1024), record.S3.Object.Size)
	assert.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", record.S3.Object.ETag)
	assert.Equal(t, "v1", record.S3.Object.VersionID)
	assert.Equal(t, "0123456789ABCDEF", record.S3.Object.Sequencer)
}
