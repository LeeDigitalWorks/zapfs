package usage

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Aggregator handles daily aggregation of usage events and retention cleanup.
// It runs in both community and enterprise editions but only processes data
// when usage reporting is enabled.
type Aggregator struct {
	cfg   Config
	store Store

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewAggregator creates a new usage aggregator.
func NewAggregator(cfg Config, store Store) *Aggregator {
	cfg.Validate()
	return &Aggregator{
		cfg:   cfg,
		store: store,
	}
}

// Start begins the aggregation scheduler.
func (a *Aggregator) Start(ctx context.Context) {
	a.ctx, a.cancel = context.WithCancel(ctx)

	// Run compact on startup if configured
	if a.cfg.CompactOnStartup && IsUsageReportingEnabled() {
		log.Info().Msg("running usage retention cleanup on startup")
		if deleted, err := a.runRetention(ctx); err != nil {
			log.Error().Err(err).Msg("startup retention cleanup failed")
		} else if deleted > 0 {
			log.Info().Int64("deleted", deleted).Msg("startup retention cleanup completed")
		}
	}

	a.wg.Add(1)
	go a.scheduleLoop()

	log.Info().
		Int("aggregation_hour", a.cfg.AggregationTime).
		Int("retention_days", a.cfg.RetentionDays).
		Msg("usage aggregator started")
}

// Stop gracefully shuts down the aggregator.
func (a *Aggregator) Stop() {
	if a.cancel != nil {
		a.cancel()
	}
	a.wg.Wait()
	log.Info().Msg("usage aggregator stopped")
}

// RunNow triggers an immediate aggregation for the previous day.
// Useful for testing or manual catch-up.
func (a *Aggregator) RunNow(ctx context.Context) error {
	if !IsUsageReportingEnabled() {
		return nil
	}

	yesterday := time.Now().UTC().AddDate(0, 0, -1)
	return a.aggregateDay(ctx, yesterday)
}

// scheduleLoop runs the daily aggregation at the configured hour.
func (a *Aggregator) scheduleLoop() {
	defer a.wg.Done()

	for {
		// Calculate time until next aggregation
		now := time.Now().UTC()
		next := time.Date(now.Year(), now.Month(), now.Day(), a.cfg.AggregationTime, 15, 0, 0, time.UTC)
		if now.After(next) {
			// Already past today's time, schedule for tomorrow
			next = next.AddDate(0, 0, 1)
		}

		timer := time.NewTimer(time.Until(next))

		select {
		case <-a.ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			a.runDailyTasks()
		}
	}
}

// runDailyTasks performs aggregation and retention cleanup.
func (a *Aggregator) runDailyTasks() {
	if !IsUsageReportingEnabled() {
		log.Debug().Msg("usage reporting not enabled, skipping aggregation")
		return
	}

	ctx, cancel := context.WithTimeout(a.ctx, 30*time.Minute)
	defer cancel()

	// Aggregate yesterday's data
	yesterday := time.Now().UTC().AddDate(0, 0, -1)
	if err := a.aggregateDay(ctx, yesterday); err != nil {
		log.Error().Err(err).Time("date", yesterday).Msg("daily aggregation failed")
	} else {
		log.Info().Time("date", yesterday).Msg("daily aggregation completed")
	}

	// Run retention cleanup
	if deleted, err := a.runRetention(ctx); err != nil {
		log.Error().Err(err).Msg("retention cleanup failed")
	} else if deleted > 0 {
		log.Info().Int64("deleted", deleted).Msg("retention cleanup completed")
	}

	// Clean up expired report jobs
	if deleted, err := a.store.DeleteExpiredReportJobs(ctx); err != nil {
		log.Error().Err(err).Msg("report job cleanup failed")
	} else if deleted > 0 {
		log.Info().Int64("deleted", deleted).Msg("expired report jobs cleaned up")
	}
}

// aggregateDay aggregates events for a specific day into usage_daily records.
func (a *Aggregator) aggregateDay(ctx context.Context, date time.Time) error {
	// Get the day boundaries in UTC
	dayStart := time.Date(date.Year(), date.Month(), date.Day(), 0, 0, 0, 0, time.UTC)
	dayEnd := dayStart.AddDate(0, 0, 1)

	log.Debug().
		Time("start", dayStart).
		Time("end", dayEnd).
		Msg("aggregating usage events")

	// Get all owner/bucket pairs with events in this time range
	pairs, err := a.store.GetDistinctOwnerBuckets(ctx, dayStart, dayEnd)
	if err != nil {
		return err
	}

	if len(pairs) == 0 {
		log.Debug().Time("date", date).Msg("no events to aggregate")
		return nil
	}

	log.Debug().Int("pairs", len(pairs)).Msg("found owner/bucket pairs to aggregate")

	// Get the previous day's storage values for each bucket
	// We need this to calculate end-of-day storage
	prevDayStart := dayStart.AddDate(0, 0, -1)

	for _, pair := range pairs {
		if err := a.aggregatePair(ctx, pair, dayStart, dayEnd, prevDayStart); err != nil {
			log.Error().
				Err(err).
				Str("owner", pair.OwnerID).
				Str("bucket", pair.Bucket).
				Msg("failed to aggregate pair")
			// Continue with other pairs
		}
	}

	return nil
}

// aggregatePair aggregates events for a single owner/bucket pair.
func (a *Aggregator) aggregatePair(ctx context.Context, pair OwnerBucketPair, dayStart, dayEnd, prevDayStart time.Time) error {
	// Get aggregated event data
	summary, err := a.store.AggregateEvents(ctx, pair.OwnerID, pair.Bucket, dayStart, dayEnd)
	if err != nil {
		return err
	}

	// Get previous day's storage to calculate end-of-day value
	prevUsage, err := a.store.GetDailyUsageByBucket(ctx, pair.OwnerID, pair.Bucket, prevDayStart, prevDayStart)
	if err != nil {
		return err
	}

	var prevStorage, prevObjects int64
	if len(prevUsage) > 0 {
		prevStorage = prevUsage[0].StorageBytes
		prevObjects = prevUsage[0].ObjectCount
	}

	// Calculate end-of-day storage (previous day + today's delta)
	endStorage := prevStorage + summary.StorageBytes
	endObjects := prevObjects + summary.ObjectCount

	// Ensure non-negative
	if endStorage < 0 {
		endStorage = 0
	}
	if endObjects < 0 {
		endObjects = 0
	}

	// Build daily usage record
	daily := &DailyUsage{
		UsageDate:    dayStart,
		OwnerID:      pair.OwnerID,
		Bucket:       pair.Bucket,
		StorageBytes: endStorage,
		ObjectCount:  endObjects,

		// Storage by class
		StorageBytesStandard: summary.StorageByClass["STANDARD"],
		StorageBytesIA:       summary.StorageByClass["STANDARD_IA"] + summary.StorageByClass["ONEZONE_IA"],
		StorageBytesGlacier:  summary.StorageByClass["GLACIER"] + summary.StorageByClass["DEEP_ARCHIVE"],

		// Request counts
		RequestsGet:    summary.Requests["GetObject"] + summary.Requests["HeadObject"],
		RequestsPut:    summary.Requests["PutObject"] + summary.Requests["CopyObject"],
		RequestsDelete: summary.Requests["DeleteObject"] + summary.Requests["DeleteObjects"],
		RequestsList:   summary.Requests["ListObjects"] + summary.Requests["ListObjectsV2"] + summary.Requests["ListBuckets"],
		RequestsHead:   summary.Requests["HeadBucket"],
		RequestsCopy:   summary.Requests["CopyObject"],
		RequestsOther:  summary.RequestsOther,

		// Bandwidth
		BandwidthIngressBytes: summary.BandwidthIngress,
		BandwidthEgressBytes:  summary.BandwidthEgress,
	}

	// Upsert (idempotent)
	if err := a.store.UpsertDailyUsage(ctx, daily); err != nil {
		return err
	}

	log.Debug().
		Str("owner", pair.OwnerID).
		Str("bucket", pair.Bucket).
		Int64("storage", endStorage).
		Int64("objects", endObjects).
		Msg("aggregated daily usage")

	return nil
}

// runRetention deletes events older than the retention period.
func (a *Aggregator) runRetention(ctx context.Context) (int64, error) {
	cutoff := time.Now().UTC().Add(-a.cfg.RetentionDuration())
	return a.store.DeleteEventsOlderThan(ctx, cutoff)
}
