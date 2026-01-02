//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"net"
	"time"
)

// AccessLogEvent represents a single S3 access log entry.
// The fields are designed to match the AWS S3 access log format.
type AccessLogEvent struct {
	// Time of the request
	EventTime time.Time

	// Unique request identifier
	RequestID string

	// Bucket and object information
	Bucket    string
	ObjectKey string

	// Ownership and requester
	OwnerID     string // Bucket owner's canonical user ID
	RequesterID string // Requester's access key or "-" for anonymous

	// Network information
	RemoteIP net.IP

	// Request details
	Operation  string // e.g., REST.GET.OBJECT, REST.PUT.OBJECT
	HTTPMethod string
	HTTPStatus int
	RequestURI string

	// Size metrics
	BytesSent  uint64 // Response bytes sent
	ObjectSize uint64 // Size of the object (for GET requests)

	// Timing metrics (milliseconds)
	TotalTimeMs    uint32 // Total request time
	TurnAroundMs   uint32 // Time to first byte

	// Authentication details
	SignatureVersion string // SigV2, SigV4
	AuthType         string // AuthHeader, QueryString, Anonymous
	TLSVersion       string // e.g., TLSv1.2, TLSv1.3

	// HTTP headers
	UserAgent  string
	Referer    string
	HostHeader string

	// Error information
	ErrorCode string // S3 error code if request failed

	// Versioning
	VersionID string
}

// FormatOperation converts an S3 action to the AWS access log operation format.
// For example: s3:GetObject -> REST.GET.OBJECT
func FormatOperation(action, method string) string {
	// AWS format: REST.<HTTP_METHOD>.<OPERATION>
	// e.g., REST.GET.OBJECT, REST.PUT.OBJECT, REST.DELETE.OBJECT
	return "REST." + method + "." + action
}
