//go:build enterprise

package accesslog

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestFormatAWSLogs(t *testing.T) {
	events := []AccessLogEvent{
		{
			EventTime:        time.Date(2025, 1, 15, 10, 30, 45, 0, time.UTC),
			RequestID:        "REQUESTID123",
			Bucket:           "mybucket",
			ObjectKey:        "myobject.txt",
			OwnerID:          "owner123",
			RequesterID:      "requester456",
			RemoteIP:         net.ParseIP("192.168.1.100"),
			Operation:        "REST.GET.OBJECT",
			HTTPMethod:       "GET",
			HTTPStatus:       200,
			BytesSent:        1024,
			ObjectSize:       2048,
			TotalTimeMs:      150,
			TurnAroundMs:     10,
			SignatureVersion: "SigV4",
			TLSVersion:       "TLSv1.2",
			AuthType:         "AuthHeader",
			UserAgent:        "aws-cli/2.0",
			Referer:          "-",
			HostHeader:       "mybucket.s3.amazonaws.com",
			RequestURI:       "GET /myobject.txt HTTP/1.1",
			ErrorCode:        "",
			VersionID:        "v1",
		},
	}

	output := FormatAWSLogs(events)
	lines := strings.Split(strings.TrimSpace(string(output)), "\n")

	assert.Len(t, lines, 1)

	// Verify key fields are present
	line := lines[0]
	assert.Contains(t, line, "owner123")
	assert.Contains(t, line, "mybucket")
	assert.Contains(t, line, "192.168.1.100")
	assert.Contains(t, line, "REQUESTID123")
	assert.Contains(t, line, "REST.GET.OBJECT")
	assert.Contains(t, line, "myobject.txt")
	assert.Contains(t, line, "200")
	assert.Contains(t, line, "SigV4")
}

func TestFormatAWSLogs_Empty(t *testing.T) {
	output := FormatAWSLogs(nil)
	assert.Empty(t, output)

	output = FormatAWSLogs([]AccessLogEvent{})
	assert.Empty(t, output)
}

func TestFormatAWSLogs_SpecialChars(t *testing.T) {
	events := []AccessLogEvent{
		{
			EventTime:   time.Now(),
			RequestID:   "REQ123",
			Bucket:      "test-bucket",
			ObjectKey:   "path/to/file with spaces.txt",
			UserAgent:   "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
			RequestURI:  `GET /path/to/file%20with%20spaces.txt HTTP/1.1`,
			HTTPStatus:  200,
			RemoteIP:    net.ParseIP("10.0.0.1"),
		},
	}

	output := FormatAWSLogs(events)
	line := string(output)

	// Keys with spaces should be URL-encoded
	assert.Contains(t, line, "path/to/file%20with%20spaces.txt")
	// User agent is inside quotes per AWS format
	assert.Contains(t, line, `"Mozilla/5.0 (Windows NT 10.0; Win64; x64)"`)
}

func TestOrDash(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{"", "-"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := orDash(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestEscapeQuotes(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"simple", "simple"},
		{`with"quote`, `with\"quote`},
		{"", "-"},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := escapeQuotes(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
