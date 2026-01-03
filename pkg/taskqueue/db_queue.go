// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package taskqueue

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
)

const (
	// maxDeadlockRetries is the maximum number of retry attempts for deadlock errors
	maxDeadlockRetries = 3
	// baseDeadlockBackoff is the base backoff duration for deadlock retries
	baseDeadlockBackoff = 10 * time.Millisecond
)

// Driver identifies a database driver type for the task queue.
type Driver string

const (
	// DriverMySQL uses MySQL/MariaDB/Vitess with ? placeholders
	DriverMySQL Driver = "mysql"
	// DriverPostgres uses PostgreSQL/CockroachDB with $N placeholders
	DriverPostgres Driver = "postgres"
)

// DBQueue is a database-backed implementation of Queue.
// Supports MySQL/Vitess and PostgreSQL/CockroachDB for durable, distributed task storage.
// Supports multiple concurrent workers via FOR UPDATE SKIP LOCKED.
type DBQueue struct {
	db                *sql.DB
	tableName         string
	visibilityTimeout time.Duration // How long a task can be "running" before being reclaimed
	driver            Driver        // Database driver type for SQL dialect differences
}

// DBQueueConfig configures the database queue.
type DBQueueConfig struct {
	DB                *sql.DB
	Driver            Driver        // Database driver (mysql, postgres). Defaults to mysql.
	TableName         string        // Defaults to "tasks"
	VisibilityTimeout time.Duration // How long before a running task is considered abandoned (default: 5m)
}

// NewDBQueue creates a new database-backed queue.
func NewDBQueue(cfg DBQueueConfig) (*DBQueue, error) {
	if cfg.DB == nil {
		return nil, fmt.Errorf("database connection is required")
	}
	if cfg.TableName == "" {
		cfg.TableName = "tasks"
	}
	if cfg.VisibilityTimeout == 0 {
		cfg.VisibilityTimeout = 5 * time.Minute
	}
	if cfg.Driver == "" {
		cfg.Driver = DriverMySQL
	}

	q := &DBQueue{
		db:                cfg.DB,
		tableName:         cfg.TableName,
		visibilityTimeout: cfg.VisibilityTimeout,
		driver:            cfg.Driver,
	}

	return q, nil
}

// rebind converts MySQL-style ? placeholders to PostgreSQL-style $N placeholders
// if the driver is PostgreSQL. For MySQL, it returns the query unchanged.
func (q *DBQueue) rebind(query string) string {
	if q.driver != DriverPostgres {
		return query
	}

	// Count placeholders and replace them with $1, $2, etc.
	var result strings.Builder
	n := 1
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			result.WriteString(fmt.Sprintf("$%d", n))
			n++
		} else {
			result.WriteByte(query[i])
		}
	}
	return result.String()
}

func (q *DBQueue) Enqueue(ctx context.Context, task *Task) error {
	if task.ID == "" {
		task.ID = uuid.New().String()
	}
	if task.Status == "" {
		task.Status = StatusPending
	}
	if task.ScheduledAt.IsZero() {
		task.ScheduledAt = time.Now()
	}
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	task.UpdatedAt = time.Now()

	query := q.rebind(fmt.Sprintf(`
		INSERT INTO %s (id, type, status, priority, payload, scheduled_at,
			attempts, max_retries, created_at, updated_at, region)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, q.tableName))

	_, err := q.db.ExecContext(ctx, query,
		task.ID, task.Type, task.Status, task.Priority, task.Payload,
		task.ScheduledAt, task.Attempts, task.MaxRetries,
		task.CreatedAt, task.UpdatedAt, task.Region,
	)
	return err
}

// isDeadlockError checks if the error is a database deadlock error.
// Supports both MySQL (Error 1213) and PostgreSQL (40P01).
func isDeadlockError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	// MySQL error 1213: Deadlock found when trying to get lock
	if strings.Contains(errStr, "Error 1213") || strings.Contains(errStr, "Deadlock") {
		return true
	}
	// PostgreSQL error 40P01: deadlock_detected
	if strings.Contains(errStr, "40P01") || strings.Contains(errStr, "deadlock detected") {
		return true
	}
	return false
}

func (q *DBQueue) Dequeue(ctx context.Context, workerID string, taskTypes ...TaskType) (*Task, error) {
	// Retry with exponential backoff on deadlock errors
	var lastErr error
	for attempt := range maxDeadlockRetries {
		task, err := q.dequeueOnce(ctx, workerID, taskTypes...)
		if err == nil {
			return task, nil
		}
		if !isDeadlockError(err) {
			return nil, err
		}
		lastErr = err
		DeadlockRetries.Inc()

		// Exponential backoff with jitter: 10-20ms, 20-40ms, 40-80ms
		backoff := baseDeadlockBackoff * time.Duration(1<<attempt)
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(backoff + jitter):
		}
	}
	return nil, lastErr
}

func (q *DBQueue) dequeueOnce(ctx context.Context, workerID string, taskTypes ...TaskType) (*Task, error) {
	// Use SELECT ... FOR UPDATE SKIP LOCKED for concurrent workers
	// Supported in MySQL 8.0+, MariaDB 10.6+, Vitess, PostgreSQL 9.5+, CockroachDB

	tx, err := q.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now()
	staleThreshold := now.Add(-q.visibilityTimeout)

	// Build type filter
	typeFilter := ""
	args := []any{now, now, staleThreshold}
	if len(taskTypes) > 0 {
		typeFilter = " AND type IN (?"
		for i, t := range taskTypes {
			if i == 0 {
				args = append(args, string(t))
			} else {
				typeFilter += ",?"
				args = append(args, string(t))
			}
		}
		typeFilter += ")"
	}

	// Select highest priority, oldest first
	// Also reclaim tasks that have been running too long (worker crashed)
	selectQuery := q.rebind(fmt.Sprintf(`
		SELECT id, type, status, priority, payload, scheduled_at, started_at,
			completed_at, attempts, max_retries, retry_after, last_error,
			created_at, updated_at, heartbeat_at, region, worker_id
		FROM %s
		WHERE (
			(status = 'pending' AND scheduled_at <= ? AND (retry_after IS NULL OR retry_after <= ?))
			OR
			(status = 'running' AND heartbeat_at < ?)
		)
		%s
		ORDER BY priority DESC, scheduled_at ASC
		LIMIT 1
		FOR UPDATE SKIP LOCKED
	`, q.tableName, typeFilter))

	row := tx.QueryRowContext(ctx, selectQuery, args...)

	var task Task
	var startedAt, completedAt, retryAfter, heartbeatAt sql.NullTime
	var lastError, workerIDNull, region sql.NullString

	err = row.Scan(
		&task.ID, &task.Type, &task.Status, &task.Priority, &task.Payload,
		&task.ScheduledAt, &startedAt, &completedAt, &task.Attempts,
		&task.MaxRetries, &retryAfter, &lastError, &task.CreatedAt,
		&task.UpdatedAt, &heartbeatAt, &region, &workerIDNull,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}
	if retryAfter.Valid {
		task.RetryAfter = retryAfter.Time
	}
	if lastError.Valid {
		task.LastError = lastError.String
	}
	if region.Valid {
		task.Region = region.String
	}

	// Increment attempts if reclaiming a stale task
	attempts := task.Attempts
	if task.Status == StatusRunning {
		attempts++
	}

	// Mark as running with fresh heartbeat
	updateQuery := q.rebind(fmt.Sprintf(`
		UPDATE %s SET status = 'running', started_at = ?, heartbeat_at = ?,
			worker_id = ?, attempts = ?, updated_at = ?
		WHERE id = ?
	`, q.tableName))

	_, err = tx.ExecContext(ctx, updateQuery, now, now, workerID, attempts, now, task.ID)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	task.Status = StatusRunning
	task.StartedAt = &now
	task.WorkerID = workerID
	task.Attempts = attempts
	task.UpdatedAt = now

	return &task, nil
}

func (q *DBQueue) Complete(ctx context.Context, taskID string) error {
	now := time.Now()
	query := q.rebind(fmt.Sprintf(`
		UPDATE %s SET status = 'completed', completed_at = ?, updated_at = ?
		WHERE id = ?
	`, q.tableName))

	result, err := q.db.ExecContext(ctx, query, now, now, taskID)
	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrTaskNotFound
	}
	return nil
}

func (q *DBQueue) Fail(ctx context.Context, taskID string, taskErr error) error {
	// Get current task state
	task, err := q.Get(ctx, taskID)
	if err != nil {
		return err
	}

	task.Attempts++
	task.LastError = taskErr.Error()
	task.UpdatedAt = time.Now()

	if task.Attempts >= task.MaxRetries {
		task.Status = StatusDeadLetter
	} else {
		// Exponential backoff
		backoff := time.Duration(1<<task.Attempts) * time.Second
		task.RetryAfter = time.Now().Add(backoff)
		task.Status = StatusPending
		task.WorkerID = ""
	}

	query := q.rebind(fmt.Sprintf(`
		UPDATE %s SET status = ?, attempts = ?, last_error = ?,
			retry_after = ?, worker_id = ?, updated_at = ?
		WHERE id = ?
	`, q.tableName))

	var retryAfter *time.Time
	if !task.RetryAfter.IsZero() {
		retryAfter = &task.RetryAfter
	}

	_, err = q.db.ExecContext(ctx, query,
		task.Status, task.Attempts, task.LastError,
		retryAfter, task.WorkerID, task.UpdatedAt, taskID,
	)
	return err
}

func (q *DBQueue) Cancel(ctx context.Context, taskID string) error {
	now := time.Now()
	query := q.rebind(fmt.Sprintf(`
		UPDATE %s SET status = 'cancelled', updated_at = ?
		WHERE id = ?
	`, q.tableName))

	result, err := q.db.ExecContext(ctx, query, now, taskID)
	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrTaskNotFound
	}
	return nil
}

func (q *DBQueue) Get(ctx context.Context, taskID string) (*Task, error) {
	query := q.rebind(fmt.Sprintf(`
		SELECT id, type, status, priority, payload, scheduled_at, started_at,
			completed_at, attempts, max_retries, retry_after, last_error,
			created_at, updated_at, heartbeat_at, region, worker_id
		FROM %s WHERE id = ?
	`, q.tableName))

	row := q.db.QueryRowContext(ctx, query, taskID)

	var task Task
	var startedAt, completedAt, retryAfter, heartbeatAt sql.NullTime
	var lastError, workerID, region sql.NullString

	err := row.Scan(
		&task.ID, &task.Type, &task.Status, &task.Priority, &task.Payload,
		&task.ScheduledAt, &startedAt, &completedAt, &task.Attempts,
		&task.MaxRetries, &retryAfter, &lastError, &task.CreatedAt,
		&task.UpdatedAt, &heartbeatAt, &region, &workerID,
	)
	if err == sql.ErrNoRows {
		return nil, ErrTaskNotFound
	}
	if err != nil {
		return nil, err
	}

	if startedAt.Valid {
		task.StartedAt = &startedAt.Time
	}
	if completedAt.Valid {
		task.CompletedAt = &completedAt.Time
	}
	if retryAfter.Valid {
		task.RetryAfter = retryAfter.Time
	}
	if lastError.Valid {
		task.LastError = lastError.String
	}
	if region.Valid {
		task.Region = region.String
	}
	if workerID.Valid {
		task.WorkerID = workerID.String
	}

	return &task, nil
}

func (q *DBQueue) List(ctx context.Context, filter TaskFilter) ([]*Task, error) {
	query := fmt.Sprintf("SELECT id, type, status, priority, payload, scheduled_at, "+
		"started_at, completed_at, attempts, max_retries, retry_after, last_error, "+
		"created_at, updated_at, heartbeat_at, region, worker_id FROM %s WHERE 1=1", q.tableName)

	args := []any{}

	if filter.Type != "" {
		query += " AND type = ?"
		args = append(args, filter.Type)
	}
	if filter.Status != "" {
		query += " AND status = ?"
		args = append(args, filter.Status)
	}
	if filter.Region != "" {
		query += " AND region = ?"
		args = append(args, filter.Region)
	}

	query += " ORDER BY created_at DESC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", filter.Limit)
	}
	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET %d", filter.Offset)
	}

	rows, err := q.db.QueryContext(ctx, q.rebind(query), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tasks []*Task
	for rows.Next() {
		var task Task
		var startedAt, completedAt, retryAfter, heartbeatAt sql.NullTime
		var lastError, workerID, region sql.NullString

		err := rows.Scan(
			&task.ID, &task.Type, &task.Status, &task.Priority, &task.Payload,
			&task.ScheduledAt, &startedAt, &completedAt, &task.Attempts,
			&task.MaxRetries, &retryAfter, &lastError, &task.CreatedAt,
			&task.UpdatedAt, &heartbeatAt, &region, &workerID,
		)
		if err != nil {
			return nil, err
		}

		if startedAt.Valid {
			task.StartedAt = &startedAt.Time
		}
		if completedAt.Valid {
			task.CompletedAt = &completedAt.Time
		}
		if retryAfter.Valid {
			task.RetryAfter = retryAfter.Time
		}
		if lastError.Valid {
			task.LastError = lastError.String
		}
		if region.Valid {
			task.Region = region.String
		}
		if workerID.Valid {
			task.WorkerID = workerID.String
		}

		tasks = append(tasks, &task)
	}

	return tasks, rows.Err()
}

func (q *DBQueue) Stats(ctx context.Context) (*QueueStats, error) {
	stats := &QueueStats{
		ByType: make(map[TaskType]int64),
	}

	// Count by status
	statusQuery := fmt.Sprintf(`
		SELECT status, COUNT(*) FROM %s GROUP BY status
	`, q.tableName)

	rows, err := q.db.QueryContext(ctx, statusQuery)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var status string
		var count int64
		if err := rows.Scan(&status, &count); err != nil {
			return nil, err
		}
		switch TaskStatus(status) {
		case StatusPending:
			stats.Pending = count
		case StatusRunning:
			stats.Running = count
		case StatusCompleted:
			stats.Completed = count
		case StatusFailed:
			stats.Failed = count
		case StatusDeadLetter:
			stats.DeadLetter = count
		}
	}

	// Count by type
	typeQuery := fmt.Sprintf(`
		SELECT type, COUNT(*) FROM %s WHERE status = 'pending' GROUP BY type
	`, q.tableName)

	rows2, err := q.db.QueryContext(ctx, typeQuery)
	if err != nil {
		return nil, err
	}
	defer rows2.Close()

	for rows2.Next() {
		var taskType string
		var count int64
		if err := rows2.Scan(&taskType, &count); err != nil {
			return nil, err
		}
		stats.ByType[TaskType(taskType)] = count
	}

	// Oldest pending
	oldestQuery := fmt.Sprintf(`
		SELECT MIN(scheduled_at) FROM %s WHERE status = 'pending'
	`, q.tableName)

	var oldest sql.NullTime
	if err := q.db.QueryRowContext(ctx, oldestQuery).Scan(&oldest); err != nil && err != sql.ErrNoRows {
		return nil, err
	}
	if oldest.Valid {
		stats.OldestPending = &oldest.Time
	}

	return stats, nil
}

func (q *DBQueue) Cleanup(ctx context.Context, olderThan time.Duration) (int, error) {
	cutoff := time.Now().Add(-olderThan)

	query := q.rebind(fmt.Sprintf(`
		DELETE FROM %s
		WHERE status IN ('completed', 'cancelled')
		AND completed_at < ?
	`, q.tableName))

	result, err := q.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		return 0, err
	}

	rows, _ := result.RowsAffected()
	return int(rows), nil
}

// Heartbeat extends the visibility timeout for a running task.
// Workers should call this periodically to prevent the task from being reclaimed.
// Uses deadlock retry to ensure heartbeat succeeds even under contention.
func (q *DBQueue) Heartbeat(ctx context.Context, taskID string, workerID string) error {
	var lastErr error
	for attempt := range maxDeadlockRetries {
		err := q.heartbeatOnce(ctx, taskID, workerID)
		if err == nil {
			return nil
		}
		if !isDeadlockError(err) {
			return err
		}
		lastErr = err
		DeadlockRetries.Inc()

		// Exponential backoff with jitter
		backoff := baseDeadlockBackoff * time.Duration(1<<attempt)
		jitter := time.Duration(rand.Int63n(int64(backoff)))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff + jitter):
		}
	}
	return lastErr
}

func (q *DBQueue) heartbeatOnce(ctx context.Context, taskID string, workerID string) error {
	now := time.Now()
	query := q.rebind(fmt.Sprintf(`
		UPDATE %s SET heartbeat_at = ?, updated_at = ?
		WHERE id = ? AND worker_id = ? AND status = 'running'
	`, q.tableName))

	result, err := q.db.ExecContext(ctx, query, now, now, taskID, workerID)
	if err != nil {
		return err
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
		return ErrTaskNotFound
	}
	return nil
}

// ReclaimStale finds tasks that have been running too long without a heartbeat
// and marks them as pending for retry. Returns the number of tasks reclaimed.
func (q *DBQueue) ReclaimStale(ctx context.Context) (int, error) {
	staleThreshold := time.Now().Add(-q.visibilityTimeout)
	now := time.Now()

	query := q.rebind(fmt.Sprintf(`
		UPDATE %s
		SET status = 'pending',
			worker_id = NULL,
			attempts = attempts + 1,
			last_error = 'reclaimed: worker timeout',
			updated_at = ?
		WHERE status = 'running'
			AND heartbeat_at < ?
			AND attempts < max_retries
	`, q.tableName))

	result, err := q.db.ExecContext(ctx, query, now, staleThreshold)
	if err != nil {
		return 0, err
	}

	rows, _ := result.RowsAffected()

	// Mark exceeded retries as dead letter
	deadLetterQuery := q.rebind(fmt.Sprintf(`
		UPDATE %s
		SET status = 'dead_letter',
			worker_id = NULL,
			last_error = 'reclaimed: max retries exceeded',
			updated_at = ?
		WHERE status = 'running'
			AND heartbeat_at < ?
			AND attempts >= max_retries
	`, q.tableName))

	deadResult, err := q.db.ExecContext(ctx, deadLetterQuery, now, staleThreshold)
	if err != nil {
		return int(rows), err
	}

	deadRows, _ := deadResult.RowsAffected()
	return int(rows) + int(deadRows), nil
}

// VisibilityTimeout returns the configured visibility timeout.
func (q *DBQueue) VisibilityTimeout() time.Duration {
	return q.visibilityTimeout
}

func (q *DBQueue) Close() error {
	// DB connection is managed externally
	return nil
}
