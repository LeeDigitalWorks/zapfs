package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/config"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// GetBucketCorsHandler returns the CORS configuration for a bucket.
// GET /{bucket}?cors
func (s *MetadataServer) GetBucketCorsHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	cors, err := s.svc.Config().GetBucketCORS(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to get bucket CORS")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	xml.NewEncoder(w).Encode(cors)
}

// PutBucketCorsHandler sets the CORS configuration for a bucket.
// PUT /{bucket}?cors
func (s *MetadataServer) PutBucketCorsHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	// Read CORS config from body
	body, err := io.ReadAll(d.Req.Body)
	if err != nil || len(body) == 0 {
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Parse CORS configuration
	var cors s3types.CORSConfiguration
	if err := xml.Unmarshal(body, &cors); err != nil {
		logger.Warn().Err(err).Str("bucket", d.S3Info.Bucket).Msg("invalid CORS XML")
		writeXMLErrorResponse(w, d, s3err.ErrMalformedXML)
		return
	}

	// Call service (validation is done in service)
	if err := s.svc.Config().SetBucketCORS(d.Ctx, d.S3Info.Bucket, &cors); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to set bucket CORS")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)

	logger.Info().Str("bucket", d.S3Info.Bucket).Int("rules", len(cors.Rules)).Msg("bucket CORS updated")
}

// DeleteBucketCorsHandler removes the CORS configuration for a bucket.
// DELETE /{bucket}?cors
func (s *MetadataServer) DeleteBucketCorsHandler(d *data.Data, w http.ResponseWriter) {
	if s.svc == nil {
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	if err := s.svc.Config().DeleteBucketCORS(d.Ctx, d.S3Info.Bucket); err != nil {
		var cfgErr *config.Error
		if errors.As(err, &cfgErr) {
			writeXMLErrorResponse(w, d, cfgErr.ToS3Error())
			return
		}
		logger.Error().Err(err).Str("bucket", d.S3Info.Bucket).Msg("failed to delete bucket CORS")
		writeXMLErrorResponse(w, d, s3err.ErrInternalError)
		return
	}

	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusNoContent)

	logger.Info().Str("bucket", d.S3Info.Bucket).Msg("bucket CORS deleted")
}

// OptionsPreflightHandler handles CORS preflight OPTIONS requests.
// OPTIONS /{bucket}/{key}
//
// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html
func (s *MetadataServer) OptionsPreflightHandler(d *data.Data, w http.ResponseWriter) {
	origin := d.Req.Header.Get("Origin")
	requestMethod := d.Req.Header.Get("Access-Control-Request-Method")
	requestHeaders := d.Req.Header.Get("Access-Control-Request-Headers")

	// Origin header is required for preflight
	if origin == "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Request method header is required for preflight
	if requestMethod == "" {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Get CORS configuration for bucket
	if s.svc == nil {
		w.WriteHeader(http.StatusForbidden)
		return
	}

	cors, err := s.svc.Config().GetBucketCORS(d.Ctx, d.S3Info.Bucket)
	if err != nil {
		// No CORS config means no preflight allowed
		w.WriteHeader(http.StatusForbidden)
		return
	}

	// Find matching CORS rule
	for _, rule := range cors.Rules {
		if !matchCORSOrigin(rule.AllowedOrigins, origin) {
			continue
		}
		if !matchCORSMethod(rule.AllowedMethods, requestMethod) {
			continue
		}
		if !matchCORSHeaders(rule.AllowedHeaders, requestHeaders) {
			continue
		}

		// Match found - set CORS response headers
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", joinStrings(rule.AllowedMethods))

		// Return the requested headers if they're allowed
		if requestHeaders != "" && len(rule.AllowedHeaders) > 0 {
			// If rule allows all headers (*), echo back the requested headers
			if hasWildcard(rule.AllowedHeaders) {
				w.Header().Set("Access-Control-Allow-Headers", requestHeaders)
			} else {
				w.Header().Set("Access-Control-Allow-Headers", joinStrings(rule.AllowedHeaders))
			}
		}

		if len(rule.ExposeHeaders) > 0 {
			w.Header().Set("Access-Control-Expose-Headers", joinStrings(rule.ExposeHeaders))
		}

		if rule.MaxAgeSeconds > 0 {
			w.Header().Set("Access-Control-Max-Age", itoa(rule.MaxAgeSeconds))
		}

		// Vary header for caching
		w.Header().Set("Vary", "Origin, Access-Control-Request-Method, Access-Control-Request-Headers")
		w.WriteHeader(http.StatusOK)
		return
	}

	// No matching rule
	w.WriteHeader(http.StatusForbidden)
}

// matchCORSOrigin checks if origin matches any allowed origin pattern.
// Supports exact match, "*" wildcard, and wildcard patterns like "*.example.com" or "http://*.example.com".
func matchCORSOrigin(allowed []string, origin string) bool {
	for _, pattern := range allowed {
		if pattern == "*" {
			return true
		}
		if pattern == origin {
			return true
		}
		// Handle wildcard patterns with "*" anywhere in the pattern
		if idx := strings.Index(pattern, "*"); idx >= 0 {
			prefix := pattern[:idx]   // e.g., "http://" or ""
			suffix := pattern[idx+1:] // e.g., ".example.com"
			if strings.HasPrefix(origin, prefix) && strings.HasSuffix(origin, suffix) {
				// Ensure there's something in the wildcard part
				if len(origin) > len(prefix)+len(suffix) {
					return true
				}
			}
		}
	}
	return false
}

// matchCORSMethod checks if method is in allowed methods
func matchCORSMethod(allowed []string, method string) bool {
	for _, m := range allowed {
		if m == method {
			return true
		}
	}
	return false
}

// matchCORSHeaders checks if all requested headers are allowed.
// Returns true if requestHeaders is empty or all headers are allowed.
// AllowedHeaders can contain "*" to allow all headers.
func matchCORSHeaders(allowedHeaders []string, requestHeaders string) bool {
	// No headers requested - always allowed
	if requestHeaders == "" {
		return true
	}

	// No headers configured in rule - deny if headers were requested
	if len(allowedHeaders) == 0 {
		return false
	}

	// Check if rule allows all headers
	if hasWildcard(allowedHeaders) {
		return true
	}

	// Parse requested headers (comma-separated, case-insensitive)
	requested := parseHeaderList(requestHeaders)

	// Check each requested header against allowed list
	for _, reqHeader := range requested {
		if !headerAllowed(allowedHeaders, reqHeader) {
			return false
		}
	}
	return true
}

// hasWildcard checks if the list contains "*"
func hasWildcard(list []string) bool {
	for _, v := range list {
		if v == "*" {
			return true
		}
	}
	return false
}

// parseHeaderList parses a comma-separated header list into individual headers
func parseHeaderList(headerList string) []string {
	var result []string
	for _, h := range strings.Split(headerList, ",") {
		trimmed := strings.TrimSpace(h)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// headerAllowed checks if a header is in the allowed list (case-insensitive)
func headerAllowed(allowed []string, header string) bool {
	headerLower := strings.ToLower(header)
	for _, a := range allowed {
		if strings.ToLower(a) == headerLower {
			return true
		}
	}
	return false
}

// joinStrings joins strings with comma separator
func joinStrings(s []string) string {
	return strings.Join(s, ", ")
}

// itoa converts int to string
func itoa(i int) string {
	return strconv.Itoa(i)
}
