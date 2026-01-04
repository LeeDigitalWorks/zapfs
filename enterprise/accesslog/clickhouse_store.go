//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/rs/zerolog/log"
)

// ClickHouseStore implements Store for ClickHouse.
type ClickHouseStore struct {
	conn    driver.Conn
	cfg     Config
	metrics *Metrics
}

// NewClickHouseStore creates a new ClickHouse store.
func NewClickHouseStore(cfg Config) (*ClickHouseStore, error) {
	cfg.Validate()

	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse clickhouse DSN: %w", err)
	}

	opts.MaxOpenConns = cfg.MaxOpenConns
	opts.MaxIdleConns = cfg.MaxIdleConns
	opts.DialTimeout = cfg.DialTimeout
	opts.ReadTimeout = cfg.ReadTimeout

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), cfg.DialTimeout)
	defer cancel()

	if err := conn.Ping(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ping clickhouse: %w", err)
	}

	store := &ClickHouseStore{
		conn:    conn,
		cfg:     cfg,
		metrics: NewMetrics(),
	}

	// Ensure schema exists
	if err := store.EnsureSchema(ctx); err != nil {
		conn.Close()
		return nil, fmt.Errorf("ensure schema: %w", err)
	}

	log.Info().Str("dsn", maskDSN(cfg.DSN)).Msg("connected to ClickHouse")

	return store, nil
}

// EnsureSchema creates the database and table if they don't exist.
// Schema is loaded from embedded schema.sql file.
func (s *ClickHouseStore) EnsureSchema(ctx context.Context) error {
	// Execute each statement from schema.sql
	statements := splitSQLStatements(SchemaSQL())
	for i, stmt := range statements {
		if stmt == "" {
			continue
		}
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("execute statement %d: %w", i+1, err)
		}
	}
	return nil
}

// splitSQLStatements splits SQL by semicolons, respecting comments.
func splitSQLStatements(sql string) []string {
	var statements []string
	var current strings.Builder

	lines := strings.Split(sql, "\n")
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		// Skip comment-only lines
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		current.WriteString(line)
		current.WriteString("\n")

		// Check if line ends with semicolon (end of statement)
		if strings.HasSuffix(trimmed, ";") {
			stmt := strings.TrimSpace(current.String())
			stmt = strings.TrimSuffix(stmt, ";")
			if stmt != "" {
				statements = append(statements, stmt)
			}
			current.Reset()
		}
	}

	// Handle any remaining content
	if remaining := strings.TrimSpace(current.String()); remaining != "" {
		statements = append(statements, remaining)
	}

	return statements
}

// InsertEvents batch inserts access log events.
func (s *ClickHouseStore) InsertEvents(ctx context.Context, events []AccessLogEvent) error {
	if len(events) == 0 {
		return nil
	}

	start := time.Now()
	defer func() {
		s.metrics.InsertDuration.Observe(time.Since(start).Seconds())
	}()

	batch, err := s.conn.PrepareBatch(ctx, `
		INSERT INTO access_logs (
			event_time, request_id, bucket, object_key, owner_id, requester_id,
			remote_ip, operation, http_method, http_status, request_uri,
			bytes_sent, object_size, total_time_ms, turn_around_time_ms,
			signature_version, auth_type, tls_version,
			user_agent, referer, host_header, error_code, version_id
		)
	`)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, e := range events {
		remoteIP := e.RemoteIP
		if remoteIP == nil {
			remoteIP = net.IPv4zero
		}

		if err := batch.Append(
			e.EventTime,
			e.RequestID,
			e.Bucket,
			e.ObjectKey,
			e.OwnerID,
			e.RequesterID,
			remoteIP,
			e.Operation,
			e.HTTPMethod,
			uint16(e.HTTPStatus),
			e.RequestURI,
			e.BytesSent,
			e.ObjectSize,
			e.TotalTimeMs,
			e.TurnAroundMs,
			e.SignatureVersion,
			e.AuthType,
			e.TLSVersion,
			e.UserAgent,
			e.Referer,
			e.HostHeader,
			e.ErrorCode,
			e.VersionID,
		); err != nil {
			return fmt.Errorf("append event: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	return nil
}

// QueryLogs retrieves logs for a bucket within a time range.
func (s *ClickHouseStore) QueryLogs(ctx context.Context, bucket string, start, end time.Time, limit int) ([]AccessLogEvent, error) {
	queryStart := time.Now()
	defer func() {
		s.metrics.QueryDuration.Observe(time.Since(queryStart).Seconds())
	}()

	rows, err := s.conn.Query(ctx, `
		SELECT
			event_time, request_id, bucket, object_key, owner_id, requester_id,
			remote_ip, operation, http_method, http_status, request_uri,
			bytes_sent, object_size, total_time_ms, turn_around_time_ms,
			signature_version, auth_type, tls_version,
			user_agent, referer, host_header, error_code, version_id
		FROM access_logs
		WHERE bucket = ? AND event_time >= ? AND event_time < ?
		ORDER BY event_time DESC
		LIMIT ?
	`, bucket, start, end, limit)
	if err != nil {
		return nil, fmt.Errorf("query logs: %w", err)
	}
	defer rows.Close()

	return s.scanRows(rows)
}

// QueryLogsForExport retrieves logs for S3 delivery.
func (s *ClickHouseStore) QueryLogsForExport(ctx context.Context, bucket string, since time.Time, limit int) ([]AccessLogEvent, error) {
	queryStart := time.Now()
	defer func() {
		s.metrics.QueryDuration.Observe(time.Since(queryStart).Seconds())
	}()

	rows, err := s.conn.Query(ctx, `
		SELECT
			event_time, request_id, bucket, object_key, owner_id, requester_id,
			remote_ip, operation, http_method, http_status, request_uri,
			bytes_sent, object_size, total_time_ms, turn_around_time_ms,
			signature_version, auth_type, tls_version,
			user_agent, referer, host_header, error_code, version_id
		FROM access_logs
		WHERE bucket = ? AND event_time > ?
		ORDER BY event_time ASC
		LIMIT ?
	`, bucket, since, limit)
	if err != nil {
		return nil, fmt.Errorf("query logs for export: %w", err)
	}
	defer rows.Close()

	return s.scanRows(rows)
}

// GetStats returns aggregate statistics for a bucket.
func (s *ClickHouseStore) GetStats(ctx context.Context, bucket string, start, end time.Time) (*BucketLogStats, error) {
	queryStart := time.Now()
	defer func() {
		s.metrics.QueryDuration.Observe(time.Since(queryStart).Seconds())
	}()

	var stats BucketLogStats

	row := s.conn.QueryRow(ctx, `
		SELECT
			count() as total_requests,
			sum(bytes_sent) as total_bytes_sent,
			uniqExact(requester_id) as unique_requesters,
			countIf(error_code != '') as error_count,
			countIf(operation LIKE '%GET%') as get_requests,
			countIf(operation LIKE '%PUT%') as put_requests,
			countIf(operation LIKE '%DELETE%') as delete_requests,
			countIf(operation LIKE '%LIST%') as list_requests,
			countIf(operation NOT LIKE '%GET%' AND operation NOT LIKE '%PUT%' AND operation NOT LIKE '%DELETE%' AND operation NOT LIKE '%LIST%') as other_requests
		FROM access_logs
		WHERE bucket = ? AND event_time >= ? AND event_time < ?
	`, bucket, start, end)

	if err := row.Scan(
		&stats.TotalRequests,
		&stats.TotalBytesSent,
		&stats.UniqueRequesters,
		&stats.ErrorCount,
		&stats.GetRequests,
		&stats.PutRequests,
		&stats.DeleteRequests,
		&stats.ListRequests,
		&stats.OtherRequests,
	); err != nil {
		return nil, fmt.Errorf("scan stats: %w", err)
	}

	return &stats, nil
}

// Close releases resources.
func (s *ClickHouseStore) Close() error {
	return s.conn.Close()
}

// scanRows scans rows into AccessLogEvent slice.
func (s *ClickHouseStore) scanRows(rows driver.Rows) ([]AccessLogEvent, error) {
	var events []AccessLogEvent

	for rows.Next() {
		var e AccessLogEvent
		var remoteIP net.IP
		var httpStatus uint16

		if err := rows.Scan(
			&e.EventTime,
			&e.RequestID,
			&e.Bucket,
			&e.ObjectKey,
			&e.OwnerID,
			&e.RequesterID,
			&remoteIP,
			&e.Operation,
			&e.HTTPMethod,
			&httpStatus,
			&e.RequestURI,
			&e.BytesSent,
			&e.ObjectSize,
			&e.TotalTimeMs,
			&e.TurnAroundMs,
			&e.SignatureVersion,
			&e.AuthType,
			&e.TLSVersion,
			&e.UserAgent,
			&e.Referer,
			&e.HostHeader,
			&e.ErrorCode,
			&e.VersionID,
		); err != nil {
			return nil, fmt.Errorf("scan row: %w", err)
		}

		e.RemoteIP = remoteIP
		e.HTTPStatus = int(httpStatus)
		events = append(events, e)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}

	return events, nil
}

// maskDSN masks password in DSN for logging.
// Supports formats:
//   - clickhouse://user:password@host:port/db -> clickhouse://user:***@host:port/db
//   - tcp://host:port?username=user&password=secret&database=db -> tcp://host:port?username=user&password=***&database=db
func maskDSN(dsn string) string {
	// Pattern 1: URL-style password in userinfo (user:password@host)
	// Match pattern: ://user:password@ and replace password with ***
	// Note: Password may contain @ so we need to find the LAST @ before / or ?
	if idx := strings.Index(dsn, "://"); idx != -1 {
		afterScheme := dsn[idx+3:]

		// Find the host separator (@ that is followed by host:port, not another @)
		// The last @ before / or ? or end is the userinfo separator
		hostStart := -1
		for i := len(afterScheme) - 1; i >= 0; i-- {
			if afterScheme[i] == '@' {
				// Check if this could be start of host (followed by something like host:port)
				rest := afterScheme[i+1:]
				// If there's a path or query after this @, it's likely the userinfo separator
				if strings.ContainsAny(rest, "/:?") || !strings.Contains(rest, "@") {
					hostStart = i
					break
				}
			}
		}

		if hostStart != -1 {
			userInfo := afterScheme[:hostStart]
			if colonIdx := strings.Index(userInfo, ":"); colonIdx != -1 {
				// Has password in userinfo
				user := userInfo[:colonIdx]
				rest := afterScheme[hostStart:]
				return dsn[:idx+3] + user + ":***" + rest
			}
		}
	}

	// Pattern 2: Query parameter password (?password=secret or &password=secret)
	// Use regex-like manual replacement for password= parameter
	lowerDSN := strings.ToLower(dsn)
	for _, prefix := range []string{"password=", "passwd="} {
		if idx := strings.Index(lowerDSN, prefix); idx != -1 {
			// Find the actual case-sensitive position
			paramStart := idx + len(prefix)
			// Find end of parameter value (next & or end of string)
			paramEnd := strings.Index(dsn[paramStart:], "&")
			if paramEnd == -1 {
				paramEnd = len(dsn)
			} else {
				paramEnd = paramStart + paramEnd
			}
			// Replace the password value
			return dsn[:paramStart] + "***" + dsn[paramEnd:]
		}
	}

	return dsn
}
