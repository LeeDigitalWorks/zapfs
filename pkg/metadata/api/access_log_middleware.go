// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"net"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
)

// AccessLogCollector is the interface for collecting access log events.
// This is implemented by enterprise/accesslog.Collector.
type AccessLogCollector interface {
	Record(event *AccessLogEvent)
}

// AccessLogEvent represents a single S3 access log entry.
// This mirrors enterprise/accesslog.AccessLogEvent for use in the API layer.
type AccessLogEvent struct {
	EventTime        time.Time
	RequestID        string
	Bucket           string
	ObjectKey        string
	OwnerID          string
	RequesterID      string
	RemoteIP         net.IP
	Operation        string
	HTTPMethod       string
	HTTPStatus       int
	BytesSent        uint64
	ObjectSize       uint64
	TotalTimeMs      uint32
	TurnAroundMs     uint32
	SignatureVersion string
	TLSVersion       string
	AuthType         string
	UserAgent        string
	Referer          string
	HostHeader       string
	RequestURI       string
	ErrorCode        string
	VersionID        string
}

// NopAccessLogCollector is a no-op collector for when access logging is disabled.
type NopAccessLogCollector struct{}

func (NopAccessLogCollector) Record(*AccessLogEvent) {}

// captureAccessLog builds and records an access log event after request completion.
func (s *MetadataServer) captureAccessLog(d *data.Data, startTime time.Time, statusCode int, bytesWritten int64) {
	if s.accessLogCollector == nil {
		return
	}

	// Check if this bucket has logging enabled (from cache)
	bucket, exists := s.bucketStore.GetBucket(d.S3Info.Bucket)
	if !exists || bucket.Logging == nil || bucket.Logging.LoggingEnabled.TargetBucket == "" {
		return
	}

	// Build the access log event
	event := s.buildAccessLogEvent(d, startTime, statusCode, bytesWritten)
	s.accessLogCollector.Record(event)
}

// buildAccessLogEvent constructs an AccessLogEvent from request data.
func (s *MetadataServer) buildAccessLogEvent(d *data.Data, startTime time.Time, statusCode int, bytesWritten int64) *AccessLogEvent {
	totalTime := time.Since(startTime)

	// Parse remote IP
	remoteIP := parseRemoteIP(d.Req.RemoteAddr)

	// Get TLS version if available
	tlsVersion := ""
	if d.Req.TLS != nil {
		tlsVersion = tlsVersionString(d.Req.TLS.Version)
	}

	// Determine auth type from signature version
	authType := "AuthHeader"
	if d.Req.URL.Query().Get("X-Amz-Signature") != "" {
		authType = "QueryString"
	}

	// Get owner and requester IDs
	ownerID := ""
	requesterID := ""
	if d.Identity != nil && d.Identity.Account != nil {
		ownerID = d.Identity.Account.ID
	}
	if d.S3Info != nil {
		requesterID = d.S3Info.AccessKey
	}

	// Get request ID from header (set by RequestIDFilter)
	requestID := d.Req.Header.Get("X-Amz-Request-Id")

	// Get version ID from query string
	versionID := d.Req.URL.Query().Get("versionId")

	// Determine signature version from headers
	signatureVersion := "SigV4"
	if d.Req.Header.Get("Authorization") != "" {
		if strings.HasPrefix(d.Req.Header.Get("Authorization"), "AWS4") {
			signatureVersion = "SigV4"
		} else if strings.HasPrefix(d.Req.Header.Get("Authorization"), "AWS ") {
			signatureVersion = "SigV2"
		}
	}

	return &AccessLogEvent{
		EventTime:        startTime,
		RequestID:        requestID,
		Bucket:           d.S3Info.Bucket,
		ObjectKey:        d.S3Info.Key,
		OwnerID:          ownerID,
		RequesterID:      requesterID,
		RemoteIP:         remoteIP,
		Operation:        formatOperation(d.S3Info.Action.String()),
		HTTPMethod:       d.Req.Method,
		HTTPStatus:       statusCode,
		BytesSent:        uint64(bytesWritten),
		ObjectSize:       0, // Populated by handler for GET requests
		TotalTimeMs:      uint32(totalTime.Milliseconds()),
		TurnAroundMs:     0, // Time to first byte, not tracked currently
		SignatureVersion: signatureVersion,
		TLSVersion:       tlsVersion,
		AuthType:         authType,
		UserAgent:        d.Req.UserAgent(),
		Referer:          d.Req.Referer(),
		HostHeader:       d.Req.Host,
		RequestURI:       d.Req.RequestURI,
		ErrorCode:        "", // Set by error response handler
		VersionID:        versionID,
	}
}

// parseRemoteIP extracts the IP from a remote address string.
func parseRemoteIP(remoteAddr string) net.IP {
	// Handle X-Forwarded-For or direct connection
	host, _, err := net.SplitHostPort(remoteAddr)
	if err != nil {
		// Try parsing as IP directly
		return net.ParseIP(remoteAddr)
	}
	return net.ParseIP(host)
}

// formatOperation converts action name to AWS format.
// e.g., "GetObject" -> "REST.GET.OBJECT"
func formatOperation(action string) string {
	// Map common actions to AWS format
	switch action {
	case "GetObject":
		return "REST.GET.OBJECT"
	case "PutObject":
		return "REST.PUT.OBJECT"
	case "DeleteObject":
		return "REST.DELETE.OBJECT"
	case "HeadObject":
		return "REST.HEAD.OBJECT"
	case "ListObjects":
		return "REST.GET.BUCKET"
	case "ListObjectsV2":
		return "REST.GET.BUCKET"
	case "CreateBucket":
		return "REST.PUT.BUCKET"
	case "DeleteBucket":
		return "REST.DELETE.BUCKET"
	case "HeadBucket":
		return "REST.HEAD.BUCKET"
	case "ListBuckets":
		return "REST.GET.SERVICE"
	case "CreateMultipartUpload":
		return "REST.POST.UPLOADS"
	case "UploadPart":
		return "REST.PUT.PART"
	case "CompleteMultipartUpload":
		return "REST.POST.UPLOAD"
	case "AbortMultipartUpload":
		return "REST.DELETE.UPLOAD"
	case "CopyObject":
		return "REST.COPY.OBJECT"
	default:
		// Convert CamelCase to REST.ACTION.RESOURCE format
		parts := splitCamelCase(action)
		if len(parts) >= 2 {
			method := strings.ToUpper(parts[0])
			resource := strings.ToUpper(strings.Join(parts[1:], "."))
			return "REST." + method + "." + resource
		}
		return "REST." + strings.ToUpper(action)
	}
}

// splitCamelCase splits a CamelCase string into parts.
func splitCamelCase(s string) []string {
	var parts []string
	var current strings.Builder

	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			if current.Len() > 0 {
				parts = append(parts, current.String())
				current.Reset()
			}
		}
		current.WriteRune(r)
	}
	if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

// tlsVersionString converts TLS version to string.
func tlsVersionString(version uint16) string {
	switch version {
	case 0x0301:
		return "TLSv1.0"
	case 0x0302:
		return "TLSv1.1"
	case 0x0303:
		return "TLSv1.2"
	case 0x0304:
		return "TLSv1.3"
	default:
		return ""
	}
}
