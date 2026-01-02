// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package filter

import (
	"strconv"
	"sync/atomic"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"

	"github.com/google/uuid"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	FilterTypeRequestID = "RequestIDFilter"
)

type RequestIDFilter struct {
	counter          atomic.Uint64
	prefix           string
	metricErrorCount *prometheus.CounterVec
}

func NewRequestIDFilter() *RequestIDFilter {
	return &RequestIDFilter{
		prefix: uuid.New().String()[0:8],
		metricErrorCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "request_id_filter_error_count",
			Help: "Number of errors encountered in RequestIDFilter",
		}, []string{"filter"}),
	}
}

func (f *RequestIDFilter) Run(d *data.Data) (Response, error) {
	if d.Ctx.Err() != nil {
		return nil, d.Ctx.Err()
	}

	// Set the request id
	newRequestID := f.generateRequestID()
	d.Req.Header.Set(s3consts.XAmzRequestID, newRequestID)

	return Next{}, nil
}

func (f *RequestIDFilter) generateRequestID() string {
	return f.prefix + strconv.FormatUint(f.counter.Add(1), 10)
}

func (f *RequestIDFilter) Type() string {
	return FilterTypeRequestID
}
