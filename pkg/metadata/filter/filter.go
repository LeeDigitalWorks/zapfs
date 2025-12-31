package filter

import (
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"

	"github.com/prometheus/client_golang/prometheus"
)

type Response interface {
	IsEnd() bool
}

type Next struct{}

func (n Next) IsEnd() bool {
	return false
}

type End struct{}

func (e End) IsEnd() bool {
	return true
}

type Filter interface {
	Run(d *data.Data) (Response, error)
	Type() string
}

type Chain struct {
	filters                []Filter
	metricErrorCount       *prometheus.CounterVec
	metricRunDuration      prometheus.Histogram
	metricRequestCount     *prometheus.CounterVec
	metricContextCancelled *prometheus.CounterVec
}

func NewChain() *Chain {
	return &Chain{
		filters: make([]Filter, 0),
		metricErrorCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "filter_error_count",
			Help: "Number of errors encountered in filters",
		}, []string{"filter"}),
		metricRunDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "filter_run_duration_seconds",
			Help:    "Duration of filter runs in seconds",
			Buckets: prometheus.DefBuckets,
		}),
		metricRequestCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "filter_request_count",
			Help: "Number of requests processed by filters",
		}, []string{"filter"}),
		metricContextCancelled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "filter_context_cancelled_count",
			Help: "Number of times filter context was cancelled",
		}, []string{"filter", "error"}),
	}
}

func (c *Chain) AddFilter(f Filter) {
	c.filters = append(c.filters, f)
}

func (c *Chain) Run(d *data.Data) (string, error) {
	for _, filter := range c.filters {
		t := time.Now()
		resp, err := filter.Run(d)
		duration := time.Since(t).Seconds()
		c.metricRunDuration.Observe(duration)
		c.metricRequestCount.WithLabelValues(filter.Type()).Inc()

		if d.Ctx.Err() != nil {
			c.metricContextCancelled.WithLabelValues(filter.Type(), d.Ctx.Err().Error()).Inc()
			return filter.Type(), d.Ctx.Err()
		}

		if err != nil {
			c.metricErrorCount.WithLabelValues(filter.Type()).Inc()
			return filter.Type(), err
		}
		if resp.IsEnd() {
			return filter.Type(), nil
		}
	}
	return "", nil
}
