package api

import (
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"strconv"

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
func (s *MetadataServer) OptionsPreflightHandler(d *data.Data, w http.ResponseWriter) {
	// TODO: Full CORS preflight implementation
	// Implementation steps:
	// 1. Get Origin header from request
	// 2. Get Access-Control-Request-Method header
	// 3. Get Access-Control-Request-Headers header (optional)
	// 4. Load bucket CORS configuration
	// 5. Find matching CORS rule by origin and method
	// 6. If match found, set response headers:
	//    - Access-Control-Allow-Origin
	//    - Access-Control-Allow-Methods
	//    - Access-Control-Allow-Headers
	//    - Access-Control-Expose-Headers
	//    - Access-Control-Max-Age
	//    - Access-Control-Allow-Credentials (if configured)
	// 7. If no match, return 403 Forbidden
	// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/cors.html

	origin := d.Req.Header.Get("Origin")
	requestMethod := d.Req.Header.Get("Access-Control-Request-Method")

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
		if matchCORSOrigin(rule.AllowedOrigins, origin) && matchCORSMethod(rule.AllowedMethods, requestMethod) {
			// Set CORS response headers
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Methods", joinStrings(rule.AllowedMethods))

			if len(rule.AllowedHeaders) > 0 {
				w.Header().Set("Access-Control-Allow-Headers", joinStrings(rule.AllowedHeaders))
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
	}

	// No matching rule
	w.WriteHeader(http.StatusForbidden)
}

// matchCORSOrigin checks if origin matches any allowed origin pattern
func matchCORSOrigin(allowed []string, origin string) bool {
	for _, a := range allowed {
		if a == "*" || a == origin {
			return true
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

// joinStrings joins strings with comma separator
func joinStrings(s []string) string {
	result := ""
	for i, v := range s {
		if i > 0 {
			result += ", "
		}
		result += v
	}
	return result
}

// itoa converts int to string
func itoa(i int) string {
	return strconv.Itoa(i)
}
