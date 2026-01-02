//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"bytes"
	"fmt"
	"net"
	"strings"
)

// FormatAWSLogs converts access log events to AWS S3 access log format.
//
// AWS S3 access log format (space-delimited):
// bucket_owner bucket [time] remote_ip requester request_id operation key
// "request_uri" http_status error_code bytes_sent object_size total_time
// turn_around_time "referrer" "user_agent" version_id host_id signature_version
// cipher_suite authentication_type host_header tls_version access_point_arn acl_required
//
// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/LogFormat.html
func FormatAWSLogs(events []AccessLogEvent) []byte {
	var buf bytes.Buffer

	for _, e := range events {
		// Format: owner bucket [time] ip requester request_id operation key "uri" status error bytes size time turnaround "referrer" "user_agent" version host_id sig_version cipher auth_type host tls access_point acl_required
		fmt.Fprintf(&buf, "%s %s [%s] %s %s %s %s %s \"%s\" %d %s %d %d %d %d \"%s\" \"%s\" %s %s %s %s %s %s %s %s %s\n",
			orDash(e.OwnerID),                                    // bucket_owner
			orDash(e.Bucket),                                     // bucket
			e.EventTime.Format("02/Jan/2006:15:04:05 -0700"),     // [time]
			formatIP(e.RemoteIP),                                 // remote_ip
			orDash(e.RequesterID),                                // requester
			orDash(e.RequestID),                                  // request_id
			orDash(e.Operation),                                  // operation
			formatKey(e.ObjectKey),                               // key
			escapeQuotes(e.RequestURI),                           // "request_uri"
			e.HTTPStatus,                                         // http_status
			orDash(e.ErrorCode),                                  // error_code
			e.BytesSent,                                          // bytes_sent
			e.ObjectSize,                                         // object_size
			e.TotalTimeMs,                                        // total_time
			e.TurnAroundMs,                                       // turn_around_time
			escapeQuotes(e.Referer),                              // "referrer"
			escapeQuotes(e.UserAgent),                            // "user_agent"
			orDash(e.VersionID),                                  // version_id
			"-",                                                  // host_id (internal)
			orDash(e.SignatureVersion),                           // signature_version
			"-",                                                  // cipher_suite
			orDash(e.AuthType),                                   // authentication_type
			orDash(e.HostHeader),                                 // host_header
			orDash(e.TLSVersion),                                 // tls_version
			"-",                                                  // access_point_arn
			"-",                                                  // acl_required
		)
	}

	return buf.Bytes()
}

// orDash returns the string or "-" if empty.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// formatIP formats an IP address or returns "-" if nil.
func formatIP(ip net.IP) string {
	if ip == nil || len(ip) == 0 {
		return "-"
	}
	return ip.String()
}

// formatKey URL-encodes the object key or returns "-" if empty.
func formatKey(key string) string {
	if key == "" {
		return "-"
	}
	// URL-encode special characters
	return strings.ReplaceAll(key, " ", "%20")
}

// escapeQuotes escapes double quotes in a string.
func escapeQuotes(s string) string {
	if s == "" {
		return "-"
	}
	return strings.ReplaceAll(s, "\"", "\\\"")
}
