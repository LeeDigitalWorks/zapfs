//go:build enterprise

// Copyright 2025 ZapInvest, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

// Package audit provides enterprise audit logging functionality.
// This package is only available in the enterprise edition of ZapFS.
//
// Audit logging captures:
// - All API requests and responses
// - Authentication events
// - Authorization decisions
// - Configuration changes
// - Administrative actions
//
// Logs can be exported to:
// - S3-compatible storage
// - Elasticsearch
// - Splunk
// - CloudWatch
// - File system
package audit

// TODO: Implement audit logging
// - AuditLogger interface
// - S3 exporter
// - Elasticsearch exporter
// - Query API for audit logs
// - Retention policies
