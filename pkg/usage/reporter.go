// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package usage

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// Reporter processes pending report jobs in the background.
// It claims jobs atomically, generates reports, and updates job status.
type Reporter struct {
	cfg   Config
	store Store

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// NewReporter creates a new report processor.
func NewReporter(cfg Config, store Store) *Reporter {
	cfg.Validate()
	return &Reporter{
		cfg:   cfg,
		store: store,
	}
}

// Start begins processing report jobs.
func (r *Reporter) Start(ctx context.Context) {
	r.ctx, r.cancel = context.WithCancel(ctx)

	r.wg.Add(1)
	go r.processLoop()

	log.Info().Msg("usage reporter started")
}

// Stop gracefully shuts down the reporter.
func (r *Reporter) Stop() {
	if r.cancel != nil {
		r.cancel()
	}
	r.wg.Wait()
	log.Info().Msg("usage reporter stopped")
}

// processLoop continuously processes pending report jobs.
func (r *Reporter) processLoop() {
	defer r.wg.Done()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if !IsUsageReportingEnabled() {
				continue
			}
			r.processNextJob()
		}
	}
}

// processNextJob claims and processes one pending job.
func (r *Reporter) processNextJob() {
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Minute)
	defer cancel()

	// Atomically claim a pending job
	job, err := r.store.ClaimPendingJob(ctx)
	if err != nil {
		log.Error().Err(err).Msg("failed to claim report job")
		return
	}
	if job == nil {
		// No pending jobs
		return
	}

	log.Info().
		Str("job_id", job.ID).
		Str("owner", job.OwnerID).
		Time("period_start", job.PeriodStart).
		Time("period_end", job.PeriodEnd).
		Msg("processing report job")

	// Generate the report
	report, err := r.generateReport(ctx, job)
	if err != nil {
		log.Error().Err(err).Str("job_id", job.ID).Msg("failed to generate report")
		r.failJob(ctx, job, err.Error())
		return
	}

	// Serialize report to JSON
	resultJSON, err := json.Marshal(report)
	if err != nil {
		log.Error().Err(err).Str("job_id", job.ID).Msg("failed to marshal report")
		r.failJob(ctx, job, "failed to serialize report")
		return
	}

	// Mark job as completed
	now := time.Now()
	job.Status = JobStatusCompleted
	job.ProgressPct = 100
	job.ResultJSON = string(resultJSON)
	job.CompletedAt = &now

	if err := r.store.UpdateReportJob(ctx, job); err != nil {
		log.Error().Err(err).Str("job_id", job.ID).Msg("failed to update completed job")
		return
	}

	log.Info().
		Str("job_id", job.ID).
		Str("owner", job.OwnerID).
		Int("buckets", len(report.Buckets)).
		Msg("report job completed")
}

// failJob marks a job as failed with the given error message.
func (r *Reporter) failJob(ctx context.Context, job *ReportJob, errMsg string) {
	now := time.Now()
	job.Status = JobStatusFailed
	job.ErrorMessage = errMsg
	job.CompletedAt = &now

	if err := r.store.UpdateReportJob(ctx, job); err != nil {
		log.Error().Err(err).Str("job_id", job.ID).Msg("failed to update failed job")
	}
}

// generateReport builds a usage report for the given job.
func (r *Reporter) generateReport(ctx context.Context, job *ReportJob) (*UsageReport, error) {
	// Update progress: starting
	job.ProgressPct = 10
	r.store.UpdateReportJob(ctx, job)

	// Get daily usage data for the period
	dailyData, err := r.store.GetDailyUsage(ctx, job.OwnerID, job.PeriodStart, job.PeriodEnd)
	if err != nil {
		return nil, err
	}

	// Update progress: data retrieved
	job.ProgressPct = 30
	r.store.UpdateReportJob(ctx, job)

	// Group by bucket
	bucketMap := make(map[string]*BucketUsage)
	for _, d := range dailyData {
		bu, ok := bucketMap[d.Bucket]
		if !ok {
			bu = &BucketUsage{
				Bucket:         d.Bucket,
				StorageByClass: make(map[string]int64),
			}
			bucketMap[d.Bucket] = bu
		}

		// Accumulate totals
		bu.StorageBytesAvg += d.StorageBytes
		if d.StorageBytes > bu.StorageBytesMax {
			bu.StorageBytesMax = d.StorageBytes
		}
		bu.ObjectCountAvg += d.ObjectCount
		bu.Requests.Get += int64(d.RequestsGet)
		bu.Requests.Put += int64(d.RequestsPut)
		bu.Requests.Delete += int64(d.RequestsDelete)
		bu.Requests.List += int64(d.RequestsList)
		bu.Requests.Head += int64(d.RequestsHead)
		bu.Requests.Copy += int64(d.RequestsCopy)
		bu.Requests.Other += int64(d.RequestsOther)
		bu.BandwidthIngress += d.BandwidthIngressBytes
		bu.BandwidthEgress += d.BandwidthEgressBytes

		// Storage by class
		if d.StorageBytesStandard > 0 {
			bu.StorageByClass["STANDARD"] += d.StorageBytesStandard
		}
		if d.StorageBytesIA > 0 {
			bu.StorageByClass["STANDARD_IA"] += d.StorageBytesIA
		}
		if d.StorageBytesGlacier > 0 {
			bu.StorageByClass["GLACIER"] += d.StorageBytesGlacier
		}

		// Include daily breakdown if requested
		if job.IncludeDaily {
			bu.Daily = append(bu.Daily, DailySnapshot{
				Date:             d.UsageDate.Format("2006-01-02"),
				StorageBytes:     d.StorageBytes,
				ObjectCount:      d.ObjectCount,
				BandwidthIngress: d.BandwidthIngressBytes,
				BandwidthEgress:  d.BandwidthEgressBytes,
				Requests: RequestCounts{
					Get:    int64(d.RequestsGet),
					Put:    int64(d.RequestsPut),
					Delete: int64(d.RequestsDelete),
					List:   int64(d.RequestsList),
					Head:   int64(d.RequestsHead),
					Copy:   int64(d.RequestsCopy),
					Other:  int64(d.RequestsOther),
					Total:  int64(d.RequestsGet + d.RequestsPut + d.RequestsDelete + d.RequestsList + d.RequestsHead + d.RequestsCopy + d.RequestsOther),
				},
			})
		}
	}

	// Update progress: aggregating
	job.ProgressPct = 60
	r.store.UpdateReportJob(ctx, job)

	// Calculate averages and totals
	numDays := int64(job.PeriodEnd.Sub(job.PeriodStart).Hours()/24) + 1
	if numDays < 1 {
		numDays = 1
	}

	var summary UsageSummary
	summary.StorageByClass = make(map[string]int64)
	buckets := make([]BucketUsage, 0, len(bucketMap))

	for _, bu := range bucketMap {
		// Calculate averages
		bu.StorageBytesAvg /= numDays
		bu.ObjectCountAvg /= numDays
		bu.Requests.Total = bu.Requests.Get + bu.Requests.Put + bu.Requests.Delete +
			bu.Requests.List + bu.Requests.Head + bu.Requests.Copy + bu.Requests.Other

		// Add to summary
		summary.StorageBytesAvg += bu.StorageBytesAvg
		if bu.StorageBytesMax > summary.StorageBytesMax {
			summary.StorageBytesMax = bu.StorageBytesMax
		}
		summary.ObjectCountAvg += bu.ObjectCountAvg
		summary.Requests.Get += bu.Requests.Get
		summary.Requests.Put += bu.Requests.Put
		summary.Requests.Delete += bu.Requests.Delete
		summary.Requests.List += bu.Requests.List
		summary.Requests.Head += bu.Requests.Head
		summary.Requests.Copy += bu.Requests.Copy
		summary.Requests.Other += bu.Requests.Other
		summary.BandwidthIngress += bu.BandwidthIngress
		summary.BandwidthEgress += bu.BandwidthEgress

		for class, bytes := range bu.StorageByClass {
			summary.StorageByClass[class] += bytes
		}

		buckets = append(buckets, *bu)
	}
	summary.Requests.Total = summary.Requests.Get + summary.Requests.Put + summary.Requests.Delete +
		summary.Requests.List + summary.Requests.Head + summary.Requests.Copy + summary.Requests.Other

	// Update progress: finalizing
	job.ProgressPct = 90
	r.store.UpdateReportJob(ctx, job)

	return &UsageReport{
		ReportID:    job.ID,
		OwnerID:     job.OwnerID,
		PeriodStart: job.PeriodStart,
		PeriodEnd:   job.PeriodEnd,
		GeneratedAt: time.Now(),
		Summary:     summary,
		Buckets:     buckets,
	}, nil
}
