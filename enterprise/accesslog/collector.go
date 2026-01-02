//go:build enterprise

// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE.enterprise file.

package accesslog

import (
	"context"
	"sync"
	"time"

	"github.com/rs/zerolog/log"
)

// collector implements the Collector interface with event buffering and batching.
type collector struct {
	store   Store
	cfg     Config
	buffer  chan *AccessLogEvent
	done    chan struct{}
	wg      sync.WaitGroup
	metrics *Metrics
}

// NewCollector creates a new access log collector with event buffering.
func NewCollector(cfg Config, store Store) Collector {
	cfg.Validate()
	return &collector{
		store:   store,
		cfg:     cfg,
		buffer:  make(chan *AccessLogEvent, cfg.BufferSize),
		done:    make(chan struct{}),
		metrics: NewMetrics(),
	}
}

// Record captures an access log event asynchronously.
func (c *collector) Record(event *AccessLogEvent) {
	select {
	case c.buffer <- event:
		c.metrics.EventsBuffered.Inc()
	default:
		// Buffer full, drop event and record metric
		c.metrics.EventsDropped.Inc()
		log.Warn().Msg("access log buffer full, event dropped")
	}
}

// Start begins background event flushing.
func (c *collector) Start(ctx context.Context) {
	c.wg.Add(1)
	go c.flushLoop(ctx)
	log.Info().
		Int("batch_size", c.cfg.BatchSize).
		Dur("flush_interval", c.cfg.FlushInterval).
		Msg("access log collector started")
}

// Stop gracefully shuts down, flushing any pending events.
func (c *collector) Stop() {
	close(c.done)
	c.wg.Wait()
	log.Info().Msg("access log collector stopped")
}

// Flush immediately flushes pending events to the store.
func (c *collector) Flush(ctx context.Context) error {
	batch := c.drain()
	if len(batch) == 0 {
		return nil
	}
	return c.flush(ctx, batch)
}

// flushLoop runs the background event flushing.
func (c *collector) flushLoop(ctx context.Context) {
	defer c.wg.Done()

	ticker := time.NewTicker(c.cfg.FlushInterval)
	defer ticker.Stop()

	batch := make([]AccessLogEvent, 0, c.cfg.BatchSize)

	for {
		select {
		case <-ctx.Done():
			// Context cancelled, flush remaining events
			batch = append(batch, c.drain()...)
			if len(batch) > 0 {
				c.flush(context.Background(), batch)
			}
			return

		case <-c.done:
			// Shutdown requested, flush remaining events
			batch = append(batch, c.drain()...)
			if len(batch) > 0 {
				c.flush(context.Background(), batch)
			}
			return

		case event := <-c.buffer:
			batch = append(batch, *event)
			if len(batch) >= c.cfg.BatchSize {
				c.flush(ctx, batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				c.flush(ctx, batch)
				batch = batch[:0]
			}
		}
	}
}

// drain empties the buffer and returns all pending events.
func (c *collector) drain() []AccessLogEvent {
	var events []AccessLogEvent
	for {
		select {
		case event := <-c.buffer:
			events = append(events, *event)
		default:
			return events
		}
	}
}

// flush writes a batch of events to the store.
func (c *collector) flush(ctx context.Context, batch []AccessLogEvent) error {
	if len(batch) == 0 {
		return nil
	}

	start := time.Now()
	err := c.store.InsertEvents(ctx, batch)
	duration := time.Since(start)

	if err != nil {
		c.metrics.FlushErrors.Inc()
		log.Error().Err(err).Int("count", len(batch)).Msg("failed to flush access logs")
		return err
	}

	c.metrics.EventsFlushed.Add(float64(len(batch)))
	c.metrics.FlushDuration.Observe(duration.Seconds())

	log.Debug().
		Int("count", len(batch)).
		Dur("duration", duration).
		Msg("flushed access logs")

	return nil
}
