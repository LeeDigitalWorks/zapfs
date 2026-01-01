package vitess

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/usage"
)

// UsageStore returns a usage.Store backed by Vitess.
func (v *Vitess) UsageStore() usage.Store {
	return &vitessUsageStore{db: v.db}
}

// vitessUsageStore implements usage.Store using Vitess.
type vitessUsageStore struct {
	db *sql.DB
}

// Verify interface implementation
var _ usage.Store = (*vitessUsageStore)(nil)

// ============================================================================
// Event Operations
// ============================================================================

func (s *vitessUsageStore) InsertEvents(ctx context.Context, events []usage.UsageEvent) error {
	if len(events) == 0 {
		return nil
	}

	// Use a transaction for batch insert
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO usage_events (event_time, owner_id, bucket, event_type, bytes_delta, count_delta, operation, direction, storage_class)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("prepare insert: %w", err)
	}
	defer stmt.Close()

	for _, e := range events {
		_, err := stmt.ExecContext(ctx,
			e.EventTime,
			e.OwnerID,
			e.Bucket,
			string(e.EventType),
			e.BytesDelta,
			e.CountDelta,
			e.Operation,
			nullString(string(e.Direction)),
			nullString(e.StorageClass),
		)
		if err != nil {
			return fmt.Errorf("insert event: %w", err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	return nil
}

func (s *vitessUsageStore) GetDistinctOwnerBuckets(ctx context.Context, start, end time.Time) ([]usage.OwnerBucketPair, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT owner_id, bucket
		FROM usage_events
		WHERE event_time >= ? AND event_time < ?
	`, start, end)
	if err != nil {
		return nil, fmt.Errorf("query distinct pairs: %w", err)
	}
	defer rows.Close()

	var pairs []usage.OwnerBucketPair
	for rows.Next() {
		var p usage.OwnerBucketPair
		if err := rows.Scan(&p.OwnerID, &p.Bucket); err != nil {
			return nil, fmt.Errorf("scan pair: %w", err)
		}
		pairs = append(pairs, p)
	}
	return pairs, rows.Err()
}

func (s *vitessUsageStore) AggregateEvents(ctx context.Context, ownerID, bucket string, start, end time.Time) (*usage.AggregatedSummary, error) {
	// Aggregate storage deltas
	var storageBytes, objectCount int64
	err := s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN event_type = 'storage_delta' THEN bytes_delta ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN event_type = 'object_delta' THEN count_delta ELSE 0 END), 0)
		FROM usage_events
		WHERE owner_id = ? AND bucket = ? AND event_time >= ? AND event_time < ?
	`, ownerID, bucket, start, end).Scan(&storageBytes, &objectCount)
	if err != nil {
		return nil, fmt.Errorf("aggregate storage: %w", err)
	}

	// Aggregate storage by class
	storageByClass := make(map[string]int64)
	rows, err := s.db.QueryContext(ctx, `
		SELECT storage_class, COALESCE(SUM(bytes_delta), 0)
		FROM usage_events
		WHERE owner_id = ? AND bucket = ? AND event_time >= ? AND event_time < ?
		  AND event_type = 'storage_delta' AND storage_class IS NOT NULL
		GROUP BY storage_class
	`, ownerID, bucket, start, end)
	if err != nil {
		return nil, fmt.Errorf("aggregate storage by class: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var class string
		var bytes int64
		if err := rows.Scan(&class, &bytes); err != nil {
			return nil, fmt.Errorf("scan storage class: %w", err)
		}
		storageByClass[class] = bytes
	}

	// Aggregate requests by operation
	requests := make(map[string]int)
	var requestsOther int
	rows, err = s.db.QueryContext(ctx, `
		SELECT operation, COUNT(*)
		FROM usage_events
		WHERE owner_id = ? AND bucket = ? AND event_time >= ? AND event_time < ?
		  AND event_type = 'request'
		GROUP BY operation
	`, ownerID, bucket, start, end)
	if err != nil {
		return nil, fmt.Errorf("aggregate requests: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var op sql.NullString
		var count int
		if err := rows.Scan(&op, &count); err != nil {
			return nil, fmt.Errorf("scan request: %w", err)
		}
		if op.Valid && op.String != "" {
			requests[op.String] = count
		} else {
			requestsOther += count
		}
	}

	// Aggregate bandwidth
	var ingressBytes, egressBytes int64
	err = s.db.QueryRowContext(ctx, `
		SELECT
			COALESCE(SUM(CASE WHEN direction = 'ingress' THEN bytes_delta ELSE 0 END), 0),
			COALESCE(SUM(CASE WHEN direction = 'egress' THEN bytes_delta ELSE 0 END), 0)
		FROM usage_events
		WHERE owner_id = ? AND bucket = ? AND event_time >= ? AND event_time < ?
		  AND event_type = 'bandwidth'
	`, ownerID, bucket, start, end).Scan(&ingressBytes, &egressBytes)
	if err != nil {
		return nil, fmt.Errorf("aggregate bandwidth: %w", err)
	}

	return &usage.AggregatedSummary{
		StorageBytes:     storageBytes,
		ObjectCount:      objectCount,
		StorageByClass:   storageByClass,
		Requests:         requests,
		RequestsOther:    requestsOther,
		BandwidthIngress: ingressBytes,
		BandwidthEgress:  egressBytes,
	}, nil
}

func (s *vitessUsageStore) DeleteEventsOlderThan(ctx context.Context, cutoff time.Time) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM usage_events WHERE event_time < ?
	`, cutoff)
	if err != nil {
		return 0, fmt.Errorf("delete old events: %w", err)
	}
	return result.RowsAffected()
}

// ============================================================================
// Daily Usage Operations
// ============================================================================

func (s *vitessUsageStore) GetDailyUsage(ctx context.Context, ownerID string, start, end time.Time) ([]usage.DailyUsage, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, usage_date, owner_id, bucket,
			storage_bytes, storage_bytes_std, storage_bytes_ia, storage_bytes_glacier,
			object_count,
			requests_get, requests_put, requests_delete, requests_list, requests_head, requests_copy, requests_other,
			bandwidth_ingress_bytes, bandwidth_egress_bytes,
			created_at, updated_at
		FROM usage_daily
		WHERE owner_id = ? AND usage_date >= ? AND usage_date <= ?
		ORDER BY usage_date, bucket
	`, ownerID, start, end)
	if err != nil {
		return nil, fmt.Errorf("query daily usage: %w", err)
	}
	defer rows.Close()

	return scanDailyUsageRows(rows)
}

func (s *vitessUsageStore) GetDailyUsageByBucket(ctx context.Context, ownerID, bucket string, start, end time.Time) ([]usage.DailyUsage, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, usage_date, owner_id, bucket,
			storage_bytes, storage_bytes_std, storage_bytes_ia, storage_bytes_glacier,
			object_count,
			requests_get, requests_put, requests_delete, requests_list, requests_head, requests_copy, requests_other,
			bandwidth_ingress_bytes, bandwidth_egress_bytes,
			created_at, updated_at
		FROM usage_daily
		WHERE owner_id = ? AND bucket = ? AND usage_date >= ? AND usage_date <= ?
		ORDER BY usage_date
	`, ownerID, bucket, start, end)
	if err != nil {
		return nil, fmt.Errorf("query daily usage by bucket: %w", err)
	}
	defer rows.Close()

	return scanDailyUsageRows(rows)
}

func (s *vitessUsageStore) UpsertDailyUsage(ctx context.Context, u *usage.DailyUsage) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO usage_daily (
			usage_date, owner_id, bucket,
			storage_bytes, storage_bytes_std, storage_bytes_ia, storage_bytes_glacier,
			object_count,
			requests_get, requests_put, requests_delete, requests_list, requests_head, requests_copy, requests_other,
			bandwidth_ingress_bytes, bandwidth_egress_bytes
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON DUPLICATE KEY UPDATE
			storage_bytes = VALUES(storage_bytes),
			storage_bytes_std = VALUES(storage_bytes_std),
			storage_bytes_ia = VALUES(storage_bytes_ia),
			storage_bytes_glacier = VALUES(storage_bytes_glacier),
			object_count = VALUES(object_count),
			requests_get = VALUES(requests_get),
			requests_put = VALUES(requests_put),
			requests_delete = VALUES(requests_delete),
			requests_list = VALUES(requests_list),
			requests_head = VALUES(requests_head),
			requests_copy = VALUES(requests_copy),
			requests_other = VALUES(requests_other),
			bandwidth_ingress_bytes = VALUES(bandwidth_ingress_bytes),
			bandwidth_egress_bytes = VALUES(bandwidth_egress_bytes)
	`,
		u.UsageDate, u.OwnerID, u.Bucket,
		u.StorageBytes, u.StorageBytesStandard, u.StorageBytesIA, u.StorageBytesGlacier,
		u.ObjectCount,
		u.RequestsGet, u.RequestsPut, u.RequestsDelete, u.RequestsList, u.RequestsHead, u.RequestsCopy, u.RequestsOther,
		u.BandwidthIngressBytes, u.BandwidthEgressBytes,
	)
	if err != nil {
		return fmt.Errorf("upsert daily usage: %w", err)
	}
	return nil
}

func (s *vitessUsageStore) GetLatestDailyUsage(ctx context.Context, ownerID string) ([]usage.DailyUsage, error) {
	// Get most recent record for each bucket
	rows, err := s.db.QueryContext(ctx, `
		SELECT d.id, d.usage_date, d.owner_id, d.bucket,
			d.storage_bytes, d.storage_bytes_std, d.storage_bytes_ia, d.storage_bytes_glacier,
			d.object_count,
			d.requests_get, d.requests_put, d.requests_delete, d.requests_list, d.requests_head, d.requests_copy, d.requests_other,
			d.bandwidth_ingress_bytes, d.bandwidth_egress_bytes,
			d.created_at, d.updated_at
		FROM usage_daily d
		INNER JOIN (
			SELECT bucket, MAX(usage_date) as max_date
			FROM usage_daily
			WHERE owner_id = ?
			GROUP BY bucket
		) latest ON d.bucket = latest.bucket AND d.usage_date = latest.max_date
		WHERE d.owner_id = ?
	`, ownerID, ownerID)
	if err != nil {
		return nil, fmt.Errorf("query latest daily usage: %w", err)
	}
	defer rows.Close()

	return scanDailyUsageRows(rows)
}

// ============================================================================
// Report Job Operations
// ============================================================================

func (s *vitessUsageStore) CreateReportJob(ctx context.Context, job *usage.ReportJob) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO usage_report_jobs (
			id, owner_id, period_start, period_end, include_daily,
			status, progress_pct, expires_at, created_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`,
		job.ID, job.OwnerID, job.PeriodStart, job.PeriodEnd, job.IncludeDaily,
		string(job.Status), job.ProgressPct, job.ExpiresAt, job.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("create report job: %w", err)
	}
	return nil
}

func (s *vitessUsageStore) GetReportJob(ctx context.Context, jobID string) (*usage.ReportJob, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, owner_id, period_start, period_end, include_daily,
			status, progress_pct, error_message, result_json,
			created_at, started_at, completed_at, expires_at
		FROM usage_report_jobs
		WHERE id = ?
	`, jobID)

	return scanReportJob(row)
}

func (s *vitessUsageStore) UpdateReportJob(ctx context.Context, job *usage.ReportJob) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE usage_report_jobs SET
			status = ?,
			progress_pct = ?,
			error_message = ?,
			result_json = ?,
			started_at = ?,
			completed_at = ?
		WHERE id = ?
	`,
		string(job.Status),
		job.ProgressPct,
		nullString(job.ErrorMessage),
		nullString(job.ResultJSON),
		job.StartedAt,
		job.CompletedAt,
		job.ID,
	)
	if err != nil {
		return fmt.Errorf("update report job: %w", err)
	}
	return nil
}

func (s *vitessUsageStore) ClaimPendingJob(ctx context.Context) (*usage.ReportJob, error) {
	// Use SELECT FOR UPDATE to atomically claim a job
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(ctx, `
		SELECT id, owner_id, period_start, period_end, include_daily,
			status, progress_pct, error_message, result_json,
			created_at, started_at, completed_at, expires_at
		FROM usage_report_jobs
		WHERE status = 'pending'
		ORDER BY created_at
		LIMIT 1
		FOR UPDATE
	`)

	job, err := scanReportJob(row)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	// Claim it
	now := time.Now()
	job.Status = usage.JobStatusProcessing
	job.StartedAt = &now

	_, err = tx.ExecContext(ctx, `
		UPDATE usage_report_jobs SET status = 'processing', started_at = ?
		WHERE id = ?
	`, now, job.ID)
	if err != nil {
		return nil, fmt.Errorf("claim job: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return nil, fmt.Errorf("commit: %w", err)
	}

	return job, nil
}

func (s *vitessUsageStore) ListReportJobs(ctx context.Context, ownerID string, limit int) ([]usage.ReportJob, error) {
	if limit <= 0 {
		limit = 10
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT id, owner_id, period_start, period_end, include_daily,
			status, progress_pct, error_message, result_json,
			created_at, started_at, completed_at, expires_at
		FROM usage_report_jobs
		WHERE owner_id = ?
		ORDER BY created_at DESC
		LIMIT ?
	`, ownerID, limit)
	if err != nil {
		return nil, fmt.Errorf("list report jobs: %w", err)
	}
	defer rows.Close()

	var jobs []usage.ReportJob
	for rows.Next() {
		job, err := scanReportJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, *job)
	}
	return jobs, rows.Err()
}

func (s *vitessUsageStore) DeleteExpiredReportJobs(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `
		DELETE FROM usage_report_jobs WHERE expires_at < ?
	`, time.Now())
	if err != nil {
		return 0, fmt.Errorf("delete expired jobs: %w", err)
	}
	return result.RowsAffected()
}

// ============================================================================
// Current Usage Operations
// ============================================================================

func (s *vitessUsageStore) GetCurrentStorageByOwner(ctx context.Context, ownerID string) ([]usage.BucketSnapshot, error) {
	// Get latest daily snapshot plus any pending deltas from events
	// For simplicity, just return latest daily for now
	rows, err := s.db.QueryContext(ctx, `
		SELECT d.bucket, d.storage_bytes, d.object_count
		FROM usage_daily d
		INNER JOIN (
			SELECT bucket, MAX(usage_date) as max_date
			FROM usage_daily
			WHERE owner_id = ?
			GROUP BY bucket
		) latest ON d.bucket = latest.bucket AND d.usage_date = latest.max_date
		WHERE d.owner_id = ?
	`, ownerID, ownerID)
	if err != nil {
		return nil, fmt.Errorf("get current storage: %w", err)
	}
	defer rows.Close()

	var snapshots []usage.BucketSnapshot
	for rows.Next() {
		var snap usage.BucketSnapshot
		if err := rows.Scan(&snap.Bucket, &snap.StorageBytes, &snap.ObjectCount); err != nil {
			return nil, fmt.Errorf("scan snapshot: %w", err)
		}
		snapshots = append(snapshots, snap)
	}
	return snapshots, rows.Err()
}

func (s *vitessUsageStore) GetMTDRequestCount(ctx context.Context, ownerID string) (int64, error) {
	// Get month-to-date request count
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	var count int64
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(requests_get + requests_put + requests_delete + requests_list + requests_head + requests_copy + requests_other), 0)
		FROM usage_daily
		WHERE owner_id = ? AND usage_date >= ?
	`, ownerID, monthStart).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("get MTD requests: %w", err)
	}
	return count, nil
}

func (s *vitessUsageStore) GetMTDBandwidthEgress(ctx context.Context, ownerID string) (int64, error) {
	now := time.Now().UTC()
	monthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)

	var bytes int64
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(SUM(bandwidth_egress_bytes), 0)
		FROM usage_daily
		WHERE owner_id = ? AND usage_date >= ?
	`, ownerID, monthStart).Scan(&bytes)
	if err != nil {
		return 0, fmt.Errorf("get MTD egress: %w", err)
	}
	return bytes, nil
}

// ============================================================================
// Helpers
// ============================================================================

func nullString(s string) sql.NullString {
	if s == "" {
		return sql.NullString{}
	}
	return sql.NullString{String: s, Valid: true}
}

func scanDailyUsageRows(rows *sql.Rows) ([]usage.DailyUsage, error) {
	var result []usage.DailyUsage
	for rows.Next() {
		var d usage.DailyUsage
		err := rows.Scan(
			&d.ID, &d.UsageDate, &d.OwnerID, &d.Bucket,
			&d.StorageBytes, &d.StorageBytesStandard, &d.StorageBytesIA, &d.StorageBytesGlacier,
			&d.ObjectCount,
			&d.RequestsGet, &d.RequestsPut, &d.RequestsDelete, &d.RequestsList, &d.RequestsHead, &d.RequestsCopy, &d.RequestsOther,
			&d.BandwidthIngressBytes, &d.BandwidthEgressBytes,
			&d.CreatedAt, &d.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan daily usage: %w", err)
		}
		result = append(result, d)
	}
	return result, rows.Err()
}

type reportScanner interface {
	Scan(dest ...any) error
}

func scanReportJob(s reportScanner) (*usage.ReportJob, error) {
	var job usage.ReportJob
	var status string
	var errorMsg, resultJSON sql.NullString
	var startedAt, completedAt sql.NullTime

	err := s.Scan(
		&job.ID, &job.OwnerID, &job.PeriodStart, &job.PeriodEnd, &job.IncludeDaily,
		&status, &job.ProgressPct, &errorMsg, &resultJSON,
		&job.CreatedAt, &startedAt, &completedAt, &job.ExpiresAt,
	)
	if err != nil {
		return nil, err
	}

	job.Status = usage.JobStatus(status)
	job.ErrorMessage = errorMsg.String
	job.ResultJSON = resultJSON.String
	if startedAt.Valid {
		job.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		job.CompletedAt = &completedAt.Time
	}

	return &job, nil
}
