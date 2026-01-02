// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package events

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/debug"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// EventsEmittedTotal tracks total events emitted by event type
	EventsEmittedTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "events",
		Name:      "emitted_total",
		Help:      "Total number of S3 events emitted",
	}, []string{"event_type"}) // event_type: "s3:ObjectCreated:Put", etc.

	// EventsDroppedTotal tracks events dropped (emitter disabled)
	EventsDroppedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "events",
		Name:      "dropped_total",
		Help:      "Total number of S3 events dropped (emitter disabled)",
	})

	// EventsErrorsTotal tracks event emission errors
	EventsErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "events",
		Name:      "errors_total",
		Help:      "Total number of event emission errors",
	}, []string{"error_type"}) // error_type: "marshal", "enqueue"

	// EventsDeliveredTotal tracks events delivered by publisher type (Enterprise)
	EventsDeliveredTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "events",
		Name:      "delivered_total",
		Help:      "Total number of S3 events delivered to publishers",
	}, []string{"publisher"}) // publisher: "redis", "kafka", "webhook"

	// EventsDeliveryErrorsTotal tracks delivery errors by publisher (Enterprise)
	EventsDeliveryErrorsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "events",
		Name:      "delivery_errors_total",
		Help:      "Total number of event delivery errors",
	}, []string{"publisher"}) // publisher: "redis", "kafka", "webhook"

	// EventsDeliveryDuration tracks event delivery latency by publisher (Enterprise)
	EventsDeliveryDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: "zapfs",
		Subsystem: "events",
		Name:      "delivery_duration_seconds",
		Help:      "Time spent delivering events to publishers",
		Buckets:   []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5},
	}, []string{"publisher"}) // publisher: "redis", "kafka", "webhook"

	// EventsQueueDepth tracks current event queue depth
	EventsQueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "events",
		Name:      "queue_depth",
		Help:      "Current number of events pending delivery",
	})
)

func init() {
	debug.Registry().MustRegister(
		EventsEmittedTotal,
		EventsDroppedTotal,
		EventsErrorsTotal,
		EventsDeliveredTotal,
		EventsDeliveryErrorsTotal,
		EventsDeliveryDuration,
		EventsQueueDepth,
	)
}
