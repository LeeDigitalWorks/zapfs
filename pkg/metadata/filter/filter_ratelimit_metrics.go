package filter

import (
	"github.com/LeeDigitalWorks/zapfs/pkg/debug"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	// RateLimitRequestsTotal tracks total requests checked by rate limiter
	RateLimitRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "ratelimit",
		Name:      "requests_total",
		Help:      "Total number of requests checked by rate limiter",
	}, []string{"operation", "result"}) // operation: read/write/list, result: allowed/rejected

	// RateLimitRejectionsTotal tracks rejected requests by scope
	RateLimitRejectionsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "ratelimit",
		Name:      "rejections_total",
		Help:      "Total number of rate-limited requests",
	}, []string{"scope", "operation"}) // scope: global/bucket/user/ip

	// RateLimitBandwidthBytes tracks bandwidth consumed
	RateLimitBandwidthBytes = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zapfs",
		Subsystem: "ratelimit",
		Name:      "bandwidth_bytes_total",
		Help:      "Total bytes processed through rate limiter",
	}, []string{"direction"}) // direction: read/write

	// RateLimitActiveLimiters tracks number of active per-key limiters
	RateLimitActiveLimiters = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "ratelimit",
		Name:      "active_limiters",
		Help:      "Number of active rate limiters by type",
	}, []string{"type"}) // type: bucket/user/ip

	// RateLimitTokensRemaining tracks remaining tokens in global limiters
	RateLimitTokensRemaining = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "zapfs",
		Subsystem: "ratelimit",
		Name:      "tokens_remaining",
		Help:      "Remaining tokens in global rate limiters",
	}, []string{"limiter"}) // limiter: read_rps/write_rps/list_rps/read_bw/write_bw
)

func init() {
	debug.Registry().MustRegister(
		RateLimitRequestsTotal,
		RateLimitRejectionsTotal,
		RateLimitBandwidthBytes,
		RateLimitActiveLimiters,
		RateLimitTokensRemaining,
	)
}
