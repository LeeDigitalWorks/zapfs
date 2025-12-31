package api

import (
	"net/http"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/data"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3consts"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3err"
)

// ============================================================================
// Bucket Request Payment (AWS-Specific - Requester Pays)
// ============================================================================

// GetBucketRequestPaymentHandler returns requester pays configuration.
// GET /{bucket}?requestPayment
//
// ZapFS does not support requester pays - always returns BucketOwner.
func (s *MetadataServer) GetBucketRequestPaymentHandler(d *data.Data, w http.ResponseWriter) {
	// Requester Pays is not supported - always return BucketOwner
	// This is compliant with AWS S3 which defaults to BucketOwner
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><RequestPaymentConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Payer>BucketOwner</Payer></RequestPaymentConfiguration>`))
}

// PutBucketRequestPaymentHandler sets requester pays configuration.
// PUT /{bucket}?requestPayment
//
// ZapFS does not support requester pays - accepts but ignores.
func (s *MetadataServer) PutBucketRequestPaymentHandler(d *data.Data, w http.ResponseWriter) {
	// Accept but ignore - requester pays not supported
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}

// ============================================================================
// Bucket Transfer Acceleration (AWS-Specific)
// ============================================================================

// GetBucketAccelerateConfigurationHandler returns transfer acceleration config.
// GET /{bucket}?accelerate
//
// ZapFS does not support transfer acceleration - returns empty/suspended.
func (s *MetadataServer) GetBucketAccelerateConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// Transfer acceleration not supported - return empty config (suspended)
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><AccelerateConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></AccelerateConfiguration>`))
}

// PutBucketAccelerateConfigurationHandler sets transfer acceleration.
// PUT /{bucket}?accelerate
//
// ZapFS does not support transfer acceleration.
func (s *MetadataServer) PutBucketAccelerateConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// Transfer acceleration not supported
	writeXMLErrorResponse(w, d, s3err.ErrNotImplemented)
}

// ============================================================================
// Bucket Event Notifications (AWS-Specific - Lambda/SNS/SQS)
// ============================================================================

// GetBucketNotificationConfigurationHandler returns notification configuration.
// GET /{bucket}?notification
//
// ZapFS does not support event notifications - returns empty config.
func (s *MetadataServer) GetBucketNotificationConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// TODO: Implement event notifications (future feature)
	// Implementation steps:
	// 1. Load notification configuration from bucket metadata
	// 2. Return TopicConfigurations, QueueConfigurations, LambdaFunctionConfigurations
	// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetBucketNotificationConfiguration.html

	// Return empty config - notifications not configured
	w.Header().Set("Content-Type", "application/xml")
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><NotificationConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"></NotificationConfiguration>`))
}

// PutBucketNotificationConfigurationHandler sets notification configuration.
// PUT /{bucket}?notification
//
// ZapFS does not support event notifications - accepts but ignores.
func (s *MetadataServer) PutBucketNotificationConfigurationHandler(d *data.Data, w http.ResponseWriter) {
	// TODO: Implement event notifications (future feature)
	// Implementation steps:
	// 1. Parse NotificationConfiguration XML
	// 2. Validate topic ARNs, queue ARNs, lambda ARNs
	// 3. Store configuration in bucket metadata
	// 4. Set up event publishing
	// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_PutBucketNotificationConfiguration.html

	// Accept but ignore - notifications not implemented
	w.Header().Set(s3consts.XAmzRequestID, d.Req.Header.Get(s3consts.XAmzRequestID))
	w.WriteHeader(http.StatusOK)
}
