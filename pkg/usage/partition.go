// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog/log"
)

// PartitionManager handles usage_events partition maintenance.
// It provides O(1) partition drops instead of row-by-row deletion,
// and manages creation of future partitions.
type PartitionManager struct {
	db *sql.DB
}

// NewPartitionManager creates a new partition manager.
func NewPartitionManager(db *sql.DB) *PartitionManager {
	return &PartitionManager{db: db}
}

// IsTablePartitioned checks if usage_events table has partitions.
func (pm *PartitionManager) IsTablePartitioned(ctx context.Context) (bool, error) {
	var count int
	err := pm.db.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'usage_events'
		  AND PARTITION_NAME IS NOT NULL
	`).Scan(&count)
	if err != nil {
		return false, fmt.Errorf("check partitions: %w", err)
	}
	return count > 1, nil
}

// PartitionInfo contains metadata about a partition.
type PartitionInfo struct {
	Name        string
	Description string // TO_DAYS value as string
	Rows        int64
}

// ListPartitions returns all partitions for usage_events.
func (pm *PartitionManager) ListPartitions(ctx context.Context) ([]PartitionInfo, error) {
	rows, err := pm.db.QueryContext(ctx, `
		SELECT PARTITION_NAME, PARTITION_DESCRIPTION, TABLE_ROWS
		FROM INFORMATION_SCHEMA.PARTITIONS
		WHERE TABLE_SCHEMA = DATABASE()
		  AND TABLE_NAME = 'usage_events'
		  AND PARTITION_NAME IS NOT NULL
		ORDER BY PARTITION_ORDINAL_POSITION
	`)
	if err != nil {
		return nil, fmt.Errorf("list partitions: %w", err)
	}
	defer rows.Close()

	var partitions []PartitionInfo
	for rows.Next() {
		var p PartitionInfo
		if err := rows.Scan(&p.Name, &p.Description, &p.Rows); err != nil {
			return nil, err
		}
		partitions = append(partitions, p)
	}
	return partitions, rows.Err()
}

// DropOldPartitions drops partitions containing data older than the retention period.
// Returns the number of partitions dropped.
// This is O(1) compared to row-by-row deletion.
func (pm *PartitionManager) DropOldPartitions(ctx context.Context, retentionDays int) (int, error) {
	cutoffDate := time.Now().UTC().AddDate(0, 0, -retentionDays)
	cutoffDays := toDays(cutoffDate)

	partitions, err := pm.ListPartitions(ctx)
	if err != nil {
		return 0, err
	}

	var dropped int
	for _, p := range partitions {
		// Skip the p_future partition
		if p.Name == "p_future" {
			continue
		}

		// Parse partition boundary (TO_DAYS value)
		partitionDays, err := strconv.Atoi(p.Description)
		if err != nil {
			log.Warn().Str("partition", p.Name).Str("desc", p.Description).Msg("invalid partition description")
			continue
		}

		// Drop if partition's upper bound is before cutoff
		if partitionDays <= cutoffDays {
			log.Info().
				Str("partition", p.Name).
				Int64("rows", p.Rows).
				Msg("dropping old partition")

			_, err := pm.db.ExecContext(ctx,
				fmt.Sprintf("ALTER TABLE usage_events DROP PARTITION %s", p.Name))
			if err != nil {
				return dropped, fmt.Errorf("drop partition %s: %w", p.Name, err)
			}
			dropped++
		}
	}

	return dropped, nil
}

// AddFuturePartitions ensures partitions exist for upcoming months.
// This prevents data from going into p_future which could grow unbounded.
func (pm *PartitionManager) AddFuturePartitions(ctx context.Context, monthsAhead int) error {
	now := time.Now().UTC()

	for i := 0; i < monthsAhead; i++ {
		// Calculate target month (start of month)
		targetMonth := time.Date(now.Year(), now.Month()+time.Month(i+1), 1, 0, 0, 0, 0, time.UTC)
		partitionName := fmt.Sprintf("p_%d_%02d", targetMonth.Year(), targetMonth.Month())

		// Calculate the boundary (start of next month)
		nextMonth := targetMonth.AddDate(0, 1, 0)

		// Check if partition already exists
		var count int
		err := pm.db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM INFORMATION_SCHEMA.PARTITIONS
			WHERE TABLE_SCHEMA = DATABASE()
			  AND TABLE_NAME = 'usage_events'
			  AND PARTITION_NAME = ?
		`, partitionName).Scan(&count)
		if err != nil {
			return fmt.Errorf("check partition %s: %w", partitionName, err)
		}
		if count > 0 {
			// Partition already exists
			continue
		}

		// REORGANIZE p_future to add the new partition
		log.Info().
			Str("partition", partitionName).
			Time("boundary", nextMonth).
			Msg("adding future partition")

		_, err = pm.db.ExecContext(ctx, fmt.Sprintf(`
			ALTER TABLE usage_events REORGANIZE PARTITION p_future INTO (
				PARTITION %s VALUES LESS THAN (TO_DAYS('%s')),
				PARTITION p_future VALUES LESS THAN MAXVALUE
			)
		`, partitionName, nextMonth.Format("2006-01-02")))

		if err != nil {
			// Check if it's a duplicate partition error (can happen in race)
			if strings.Contains(err.Error(), "Duplicate partition") ||
				strings.Contains(err.Error(), "already exists") {
				continue
			}
			return fmt.Errorf("add partition %s: %w", partitionName, err)
		}
	}

	return nil
}

// RunMaintenance performs full partition maintenance:
// 1. Drops old partitions beyond retention
// 2. Adds future partitions for upcoming months
func (pm *PartitionManager) RunMaintenance(ctx context.Context, retentionDays, monthsAhead int) error {
	// First check if table is partitioned
	partitioned, err := pm.IsTablePartitioned(ctx)
	if err != nil {
		return err
	}
	if !partitioned {
		log.Debug().Msg("usage_events not partitioned, skipping partition maintenance")
		return nil
	}

	// Drop old partitions
	dropped, err := pm.DropOldPartitions(ctx, retentionDays)
	if err != nil {
		return fmt.Errorf("drop old partitions: %w", err)
	}
	if dropped > 0 {
		log.Info().Int("dropped", dropped).Msg("dropped old partitions")
	}

	// Add future partitions
	if err := pm.AddFuturePartitions(ctx, monthsAhead); err != nil {
		return fmt.Errorf("add future partitions: %w", err)
	}

	return nil
}

// toDays calculates MySQL's TO_DAYS value for a date.
// TO_DAYS returns the number of days since year 0.
func toDays(t time.Time) int {
	// MySQL's TO_DAYS epoch is 0000-00-00
	// Days from 0000-01-01 to 1970-01-01 is approximately 719528
	const epochOffset = 719528
	return int(t.Unix()/86400) + epochOffset
}
