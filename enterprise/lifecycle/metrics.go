// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE file.

//go:build enterprise

package lifecycle

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	// TransitionsTotal counts the total number of storage class transitions
	TransitionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "lifecycle",
			Name:      "transitions_total",
			Help:      "Total number of storage class transitions",
		},
		[]string{"storage_class", "status"},
	)

	// TransitionBytesTotal counts the total bytes transitioned to each storage class
	TransitionBytesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "lifecycle",
			Name:      "transition_bytes_total",
			Help:      "Total bytes transitioned to each storage class",
		},
		[]string{"storage_class"},
	)

	// TransitionDuration tracks the duration of transition operations
	TransitionDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "zapfs",
			Subsystem: "lifecycle",
			Name:      "transition_duration_seconds",
			Help:      "Duration of storage class transition operations",
			Buckets:   prometheus.ExponentialBuckets(0.1, 2, 12), // 0.1s to ~400s
		},
		[]string{"storage_class"},
	)

	// TierBackendErrors counts errors when interacting with tier backends
	TierBackendErrors = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "lifecycle",
			Name:      "tier_backend_errors_total",
			Help:      "Total number of tier backend errors",
		},
		[]string{"storage_class", "operation"},
	)

	// PromotionsTotal counts the total number of intelligent tiering promotions
	PromotionsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "lifecycle",
			Name:      "promotions_total",
			Help:      "Total number of intelligent tiering promotions back to hot storage",
		},
		[]string{"status"},
	)

	// PromotionBytesTotal counts the total bytes promoted back to hot storage
	PromotionBytesTotal = promauto.NewCounter(
		prometheus.CounterOpts{
			Namespace: "zapfs",
			Subsystem: "lifecycle",
			Name:      "promotion_bytes_total",
			Help:      "Total bytes promoted back to hot storage",
		},
	)
)
