// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"context"
	"errors"
	"net/http"
	"runtime/debug"
	"strconv"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

type Handler func(*data.Data, http.ResponseWriter)

func (s *MetadataServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	wrappedWriter := &wrappedResponseRecorder{
		ResponseWriter: w,
		statusCode:     0,
	}

	// Pass the request through the filter chain
	d := data.NewData(r.Context(), r)

	// Defer metrics and access logging (runs after panic recovery)
	defer func() {
		// If the request was cancelled by the client, avoid logging a 500 error
		if wrappedWriter.statusCode == http.StatusInternalServerError && errors.Is(r.Context().Err(), context.Canceled) {
			wrappedWriter.statusCode = 0
		}
		s.metricsRequest.WithLabelValues(d.S3Info.Action.String(), strconv.FormatInt(int64(wrappedWriter.statusCode), 10)).Inc()
		s.metricsRequestDuration.WithLabelValues(d.S3Info.Action.String(), strconv.FormatInt(int64(wrappedWriter.statusCode), 10)).Observe(time.Since(start).Seconds())

		// Capture access log for buckets with logging enabled (enterprise: FeatureAuditLog)
		s.captureAccessLog(d, start, wrappedWriter.statusCode, wrappedWriter.bytesWritten)
	}()

	// Recover from panics to prevent server crash and return proper S3 error
	defer func() {
		if rec := recover(); rec != nil {
			// Capture stack trace
			stack := debug.Stack()

			// Log the panic with stack trace
			logger.Error().
				Interface("panic", rec).
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("action", d.S3Info.Action.String()).
				Str("request_id", r.Header.Get("X-Amz-Request-Id")).
				Bytes("stack", stack).
				Msg("panic recovered in request handler")

			// Return S3 internal error if response not already written
			if wrappedWriter.statusCode == 0 {
				writeXMLErrorResponse(wrappedWriter, d, s3err.ErrInternalError)
			}
		}
	}()

	_, err := s.chain.Run(d)
	if err != nil {
		// TODO: Check if response should be HTML or XML based on bucket website
		var httpErr s3err.ErrorCode
		if errors.As(err, &httpErr) {
			writeXMLErrorResponse(wrappedWriter, d, httpErr)
		} else {
			writeXMLErrorResponse(wrappedWriter, d, s3err.ErrInternalError)
		}
		return
	}

	handler, exists := s.handlers[d.S3Info.Action]
	if !exists {
		writeXMLErrorResponse(wrappedWriter, d, s3err.ErrNotImplemented)
		return
	}

	handler(d, wrappedWriter)
}

// handleObjectError converts service layer errors to HTTP responses.
// It handles domain errors from the object service and maps them to S3 error codes.
func (s *MetadataServer) handleObjectError(w http.ResponseWriter, d *data.Data, err error) {
	if err == nil {
		return
	}

	// Check for object package error type (interface check to avoid import cycle)
	type objectError interface {
		ToS3Error() s3err.ErrorCode
	}

	if objErr, ok := err.(objectError); ok {
		errCode := objErr.ToS3Error()
		// Special case for HEAD requests - use status code instead of XML
		if d.Req.Method == http.MethodHead {
			switch errCode {
			case s3err.ErrNoSuchKey:
				w.WriteHeader(http.StatusNotFound)
			case s3err.ErrNotModified:
				w.WriteHeader(http.StatusNotModified)
			default:
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		}
		writeXMLErrorResponse(w, d, errCode)
		return
	}

	// Generic error - log and return internal error
	logger.Error().Err(err).Msg("service layer error")
	if d.Req.Method == http.MethodHead {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	writeXMLErrorResponse(w, d, s3err.ErrInternalError)
}
