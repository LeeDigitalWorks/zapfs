package s3err

import (
	"net/http"
	"strings"
)

// APIError represents an S3 API error with its code, description, and HTTP status.
// Based on: https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
type APIError struct {
	Code           string
	Description    string
	HTTPStatusCode int
}

// Error represents the XML error response returned to S3 clients.
type Error struct {
	XMLName   string `xml:"Error"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
	Resource  string `xml:"Resource"`
	RequestID string `xml:"RequestId"`
	HTTPCode  int    `xml:"-"`
}

func (e Error) Error() string {
	var b strings.Builder
	b.WriteString(e.Code)
	b.WriteString(": ")
	if e.Resource != "" {
		b.WriteString(e.Resource)
		b.WriteString(": ")
	}
	b.WriteString(e.Message)
	return b.String()
}

// ErrorCode is an enumeration of S3 error codes.
// See full list at: https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
type ErrorCode int

const (
	ErrNone ErrorCode = iota

	// =========================================================================
	// Access & Authentication Errors
	// =========================================================================
	ErrAccessDenied
	ErrAccountProblem
	ErrAllAccessDisabled
	ErrInvalidAccessKeyID
	ErrInvalidSecurity
	ErrNotSignedUp
	ErrSignatureDoesNotMatch
	ErrRequestTimeTooSkewed
	ErrExpiredToken
	ErrInvalidToken

	// Authorization header/query errors
	ErrAuthHeaderEmpty
	ErrAuthorizationHeaderMalformed
	ErrMissingSecurityElement
	ErrMissingSecurityHeader
	ErrMissingCredTag
	ErrCredMalformed
	ErrMissingSignHeadersTag
	ErrMissingSignTag
	ErrUnsignedHeaders
	ErrInvalidQueryParams
	ErrInvalidQuerySignatureAlgo
	ErrSignatureVersionNotSupported

	// Date/time errors
	ErrMissingDateHeader
	ErrMalformedDate
	ErrMalformedPresignedDate
	ErrMalformedCredentialDate

	// Presigned URL errors
	ErrExpiredPresignRequest
	ErrMalformedExpires
	ErrNegativeExpires
	ErrMaximumExpires
	ErrRequestNotReadyYet

	// =========================================================================
	// Bucket Errors
	// =========================================================================
	ErrNoSuchBucket
	ErrBucketAlreadyExists
	ErrBucketAlreadyOwnedByYou
	ErrBucketNotEmpty
	ErrInvalidBucketName
	ErrInvalidBucketState
	ErrTooManyBuckets
	ErrOperationAborted

	// Bucket configuration errors
	ErrNoSuchBucketPolicy
	ErrNoSuchCORSConfiguration
	ErrCORSMissingAllowedOrigin
	ErrCORSMissingAllowedMethod
	ErrCORSInvalidMethod
	ErrNoSuchLifecycleConfiguration
	ErrNoSuchBucketEncryptionConfiguration
	ErrNoSuchWebsiteConfiguration
	ErrInvalidWebsiteConfiguration
	ErrNoSuchTagSet
	ErrOwnershipControlsNotFoundError
	ErrReplicationConfigurationNotFoundError
	ErrNoSuchConfiguration                      // For analytics, metrics, inventory, intelligent tiering
	ErrNoSuchPublicAccessBlockConfiguration

	// Object Lock
	ErrNoSuchObjectLockConfiguration
	ErrNoSuchObjectLegalHold
	ErrInvalidRetentionPeriod
	ErrObjectLockConfigurationNotFoundError

	// Access Control
	ErrAccessControlListNotSupported
	ErrMalformedACLError
	ErrAmbiguousGrantByEmailAddress
	ErrUnresolvableGrantByEmailAddress
	ErrCrossLocationLoggingProhibited
	ErrInvalidTargetBucketForLogging

	// =========================================================================
	// Object Errors
	// =========================================================================
	ErrNoSuchKey
	ErrNoSuchVersion
	ErrInvalidVersionID
	ErrInvalidObjectState
	ErrKeyTooLong
	ErrMetadataTooLarge

	// =========================================================================
	// Multipart Upload Errors
	// =========================================================================
	ErrNoSuchUpload
	ErrInvalidPart
	ErrInvalidPartOrder
	ErrTooManyParts
	ErrEntityTooSmall
	ErrEntityTooLarge
	ErrIncompleteBody

	// =========================================================================
	// Request Validation Errors
	// =========================================================================
	ErrInvalidRequest
	ErrInvalidArgument
	ErrInvalidRange
	ErrInvalidDigest
	ErrBadDigest
	ErrMalformedXML
	ErrMalformedPolicy
	ErrInvalidPolicyDocument
	ErrInvalidTag
	ErrInvalidURI
	ErrInvalidLocationConstraint
	ErrInvalidPayer
	ErrInvalidStorageClass
	ErrMissingContentLength
	ErrMissingRequestBodyError
	ErrUnexpectedContent
	ErrRequestTimeout
	ErrRequestHeaderSectionTooLarge

	// Copy operation errors
	ErrInvalidCopyDest
	ErrInvalidCopySource
	ErrCopySourceTooLarge

	// POST/Form errors
	ErrMalformedPOSTRequest
	ErrPOSTFileRequired
	ErrPostPolicyConditionInvalidFormat
	ErrMaxPostPreDataLengthExceededError
	ErrMissingFields
	ErrIncorrectNumberOfFilesInPostRequest
	ErrRequestIsNotMultiPartContent

	// Checksum errors
	ErrChecksumMismatch
	ErrMissingChecksum
	ErrContentSHA256Mismatch

	// =========================================================================
	// SSE-C (Server-Side Encryption with Customer Keys) Errors
	// =========================================================================
	ErrInvalidEncryptionAlgorithm
	ErrInvalidEncryptionKey
	ErrSSECustomerKeyMD5Mismatch
	ErrSSECustomerKeyMissing
	ErrSSECustomerKeyNotNeeded
	ErrSSEEncryptionTypeMismatch

	// =========================================================================
	// SSE-KMS Errors
	// =========================================================================
	ErrKMSKeyNotFound
	ErrKMSAccessDenied
	ErrKMSDisabled
	ErrKMSInvalidCiphertext

	// =========================================================================
	// Service / Throttling Errors
	// =========================================================================
	ErrInternalError
	ErrNotImplemented
	ErrServiceUnavailable
	ErrSlowDown
	ErrTooManyRequests
	ErrRequestBytesExceed

	// =========================================================================
	// Response / Redirect Errors
	// =========================================================================
	ErrMethodNotAllowed
	ErrPreconditionFailed
	ErrNotModified
	ErrPermanentRedirect
	ErrTemporaryRedirect

	// =========================================================================
	// Custom / Extended Errors (non-AWS)
	// =========================================================================
	ErrExistingObjectIsDirectory
	ErrExistingObjectIsFile
	ErrMaxSizeExceeded
	ErrAuthNotSetup
	ErrInvalidMaxKeys
	ErrInvalidMaxUploads
	ErrInvalidMaxParts
	ErrInvalidMaxDeleteObjects
	ErrInvalidPartNumberMarker
	ErrInvalidUnorderedWithDelimiter
)

// errorCodeResponse maps error codes to their AWS API error definitions.
// Based on: https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html#ErrorCodeList
var errorCodeResponse = map[ErrorCode]APIError{
	// =========================================================================
	// Access & Authentication Errors
	// =========================================================================
	ErrAccessDenied: {
		Code:           "AccessDenied",
		Description:    "Access Denied.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrAccountProblem: {
		Code:           "AccountProblem",
		Description:    "There is a problem with your AWS account that prevents the operation from completing successfully.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrAllAccessDisabled: {
		Code:           "AllAccessDisabled",
		Description:    "All access to this Amazon S3 resource has been disabled.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrInvalidAccessKeyID: {
		Code:           "InvalidAccessKeyId",
		Description:    "The AWS access key ID you provided does not exist in our records.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrInvalidSecurity: {
		Code:           "InvalidSecurity",
		Description:    "The provided security credentials are not valid.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrNotSignedUp: {
		Code:           "NotSignedUp",
		Description:    "Your account is not signed up for the Amazon S3 service.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrSignatureDoesNotMatch: {
		Code:           "SignatureDoesNotMatch",
		Description:    "The request signature we calculated does not match the signature you provided. Check your key and signing method.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrRequestTimeTooSkewed: {
		Code:           "RequestTimeTooSkewed",
		Description:    "The difference between the request time and the server's time is too large.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrExpiredToken: {
		Code:           "ExpiredToken",
		Description:    "The provided token has expired.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidToken: {
		Code:           "InvalidToken",
		Description:    "The provided token is malformed or otherwise invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// Authorization header/query errors
	ErrAuthHeaderEmpty: {
		Code:           "InvalidArgument",
		Description:    "Authorization header is invalid -- one and only one ' ' (space) required.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAuthorizationHeaderMalformed: {
		Code:           "AuthorizationHeaderMalformed",
		Description:    "The authorization header is malformed; the region is wrong.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSecurityElement: {
		Code:           "MissingSecurityElement",
		Description:    "The SOAP 1.1 request is missing a security element.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSecurityHeader: {
		Code:           "MissingSecurityHeader",
		Description:    "Your request is missing a required header.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingCredTag: {
		Code:           "InvalidRequest",
		Description:    "Missing Credential field for this request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrCredMalformed: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; the Credential is mal-formed; expecting \"<YOUR-AKID>/YYYYMMDD/REGION/SERVICE/aws4_request\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSignHeadersTag: {
		Code:           "InvalidArgument",
		Description:    "Signature header missing SignedHeaders field.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingSignTag: {
		Code:           "AccessDenied",
		Description:    "Signature header missing Signature field.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnsignedHeaders: {
		Code:           "AccessDenied",
		Description:    "There were headers present in the request which were not signed.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQueryParams: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Query-string authentication version 4 requires the X-Amz-Algorithm, X-Amz-Credential, X-Amz-Signature, X-Amz-Date, X-Amz-SignedHeaders, and X-Amz-Expires parameters.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidQuerySignatureAlgo: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Algorithm only supports \"AWS4-HMAC-SHA256\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSignatureVersionNotSupported: {
		Code:           "InvalidRequest",
		Description:    "The authorization mechanism you have provided is not supported. Please use AWS4-HMAC-SHA256.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// Date/time errors
	ErrMissingDateHeader: {
		Code:           "AccessDenied",
		Description:    "AWS authentication requires a valid Date or x-amz-date header.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedDate: {
		Code:           "MalformedDate",
		Description:    "Invalid date format header, expected to be in ISO8601, RFC1123 or RFC1123Z time format.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedPresignedDate: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Date must be in the ISO8601 Long Format \"yyyyMMdd'T'HHmmss'Z'\".",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedCredentialDate: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "Error parsing the X-Amz-Credential parameter; invalid date format.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// Presigned URL errors
	ErrExpiredPresignRequest: {
		Code:           "AccessDenied",
		Description:    "Request has expired.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrMalformedExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires should be a number.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNegativeExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires must be non-negative.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMaximumExpires: {
		Code:           "AuthorizationQueryParametersError",
		Description:    "X-Amz-Expires must be less than a week (in seconds); that is, the given X-Amz-Expires must be less than 604800 seconds.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRequestNotReadyYet: {
		Code:           "AccessDenied",
		Description:    "Request is not valid yet.",
		HTTPStatusCode: http.StatusForbidden,
	},

	// =========================================================================
	// Bucket Errors
	// =========================================================================
	ErrNoSuchBucket: {
		Code:           "NoSuchBucket",
		Description:    "The specified bucket does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrBucketAlreadyExists: {
		Code:           "BucketAlreadyExists",
		Description:    "The requested bucket name is not available. The bucket namespace is shared by all users of the system. Please select a different name and try again.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrBucketAlreadyOwnedByYou: {
		Code:           "BucketAlreadyOwnedByYou",
		Description:    "Your previous request to create the named bucket succeeded and you already own it.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrBucketNotEmpty: {
		Code:           "BucketNotEmpty",
		Description:    "The bucket you tried to delete is not empty.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrInvalidBucketName: {
		Code:           "InvalidBucketName",
		Description:    "The specified bucket is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidBucketState: {
		Code:           "InvalidBucketState",
		Description:    "The request is not valid with the current state of the bucket.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrTooManyBuckets: {
		Code:           "TooManyBuckets",
		Description:    "You have attempted to create more buckets than allowed.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrOperationAborted: {
		Code:           "OperationAborted",
		Description:    "A conflicting conditional operation is currently in progress against this resource. Try again.",
		HTTPStatusCode: http.StatusConflict,
	},

	// Bucket configuration errors
	ErrNoSuchBucketPolicy: {
		Code:           "NoSuchBucketPolicy",
		Description:    "The bucket policy does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchCORSConfiguration: {
		Code:           "NoSuchCORSConfiguration",
		Description:    "The CORS configuration does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrCORSMissingAllowedOrigin: {
		Code:           "MalformedXML",
		Description:    "CORS configuration must have at least one allowed origin.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrCORSMissingAllowedMethod: {
		Code:           "MalformedXML",
		Description:    "CORS configuration must have at least one allowed method.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrCORSInvalidMethod: {
		Code:           "InvalidRequest",
		Description:    "CORS configuration contains an invalid HTTP method.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchLifecycleConfiguration: {
		Code:           "NoSuchLifecycleConfiguration",
		Description:    "The lifecycle configuration does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchBucketEncryptionConfiguration: {
		Code:           "ServerSideEncryptionConfigurationNotFoundError",
		Description:    "The server side encryption configuration was not found.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchWebsiteConfiguration: {
		Code:           "NoSuchWebsiteConfiguration",
		Description:    "The specified bucket does not have a website configuration.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidWebsiteConfiguration: {
		Code:           "InvalidArgument",
		Description:    "The website configuration is not valid. It must contain an index document or redirect configuration.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrNoSuchTagSet: {
		Code:           "NoSuchTagSet",
		Description:    "The TagSet does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrOwnershipControlsNotFoundError: {
		Code:           "OwnershipControlsNotFoundError",
		Description:    "The bucket ownership controls were not found.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrReplicationConfigurationNotFoundError: {
		Code:           "ReplicationConfigurationNotFoundError",
		Description:    "The replication configuration was not found.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchConfiguration: {
		Code:           "NoSuchConfiguration",
		Description:    "The specified configuration does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchPublicAccessBlockConfiguration: {
		Code:           "NoSuchPublicAccessBlockConfiguration",
		Description:    "The public access block configuration was not found.",
		HTTPStatusCode: http.StatusNotFound,
	},

	// Object Lock
	ErrNoSuchObjectLockConfiguration: {
		Code:           "ObjectLockConfigurationNotFoundError",
		Description:    "Object Lock configuration does not exist for this bucket.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchObjectLegalHold: {
		Code:           "NoSuchObjectLegalHold",
		Description:    "The specified object does not have a legal hold configuration.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidRetentionPeriod: {
		Code:           "InvalidRetentionPeriod",
		Description:    "The retention period specified is invalid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrObjectLockConfigurationNotFoundError: {
		Code:           "ObjectLockConfigurationNotFoundError",
		Description:    "Object Lock configuration does not exist for this bucket.",
		HTTPStatusCode: http.StatusNotFound,
	},

	// Access Control
	ErrAccessControlListNotSupported: {
		Code:           "AccessControlListNotSupported",
		Description:    "The bucket does not allow ACLs.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedACLError: {
		Code:           "MalformedACLError",
		Description:    "The XML you provided was not well-formed or did not validate against our published schema.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAmbiguousGrantByEmailAddress: {
		Code:           "AmbiguousGrantByEmailAddress",
		Description:    "The email address you provided is associated with more than one account.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnresolvableGrantByEmailAddress: {
		Code:           "UnresolvableGrantByEmailAddress",
		Description:    "The email address you provided does not match any account on record.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrCrossLocationLoggingProhibited: {
		Code:           "CrossLocationLoggingProhibited",
		Description:    "Cross-location logging not allowed. Buckets in one geographic location cannot log information to a bucket in another location.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrInvalidTargetBucketForLogging: {
		Code:           "InvalidTargetBucketForLogging",
		Description:    "The target bucket for logging does not exist, is not owned by you, or does not have the appropriate grants for the log-delivery group.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// =========================================================================
	// Object Errors
	// =========================================================================
	ErrNoSuchKey: {
		Code:           "NoSuchKey",
		Description:    "The specified key does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrNoSuchVersion: {
		Code:           "NoSuchVersion",
		Description:    "The specified version does not exist.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidVersionID: {
		Code:           "InvalidArgument",
		Description:    "Invalid version id specified.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidObjectState: {
		Code:           "InvalidObjectState",
		Description:    "The operation is not valid for the current state of the object.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrKeyTooLong: {
		Code:           "KeyTooLongError",
		Description:    "Your key is too long.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMetadataTooLarge: {
		Code:           "MetadataTooLarge",
		Description:    "Your metadata headers exceed the maximum allowed metadata size.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// =========================================================================
	// Multipart Upload Errors
	// =========================================================================
	ErrNoSuchUpload: {
		Code:           "NoSuchUpload",
		Description:    "The specified multipart upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed.",
		HTTPStatusCode: http.StatusNotFound,
	},
	ErrInvalidPart: {
		Code:           "InvalidPart",
		Description:    "One or more of the specified parts could not be found. The part may not have been uploaded, or the specified entity tag may not match the part's entity tag.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartOrder: {
		Code:           "InvalidPartOrder",
		Description:    "The list of parts was not in ascending order. Parts must be ordered by part number.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrTooManyParts: {
		Code:           "TooManyParts",
		Description:    "You have attempted to upload more parts than Amazon S3 accepts.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEntityTooSmall: {
		Code:           "EntityTooSmall",
		Description:    "Your proposed upload is smaller than the minimum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrEntityTooLarge: {
		Code:           "EntityTooLarge",
		Description:    "Your proposed upload exceeds the maximum allowed object size.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIncompleteBody: {
		Code:           "IncompleteBody",
		Description:    "You did not provide the number of bytes specified by the Content-Length HTTP header.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// =========================================================================
	// Request Validation Errors
	// =========================================================================
	ErrInvalidRequest: {
		Code:           "InvalidRequest",
		Description:    "Invalid Request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidArgument: {
		Code:           "InvalidArgument",
		Description:    "Invalid Argument.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidRange: {
		Code:           "InvalidRange",
		Description:    "The requested range is not satisfiable.",
		HTTPStatusCode: http.StatusRequestedRangeNotSatisfiable,
	},
	ErrInvalidDigest: {
		Code:           "InvalidDigest",
		Description:    "The Content-MD5 you specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrBadDigest: {
		Code:           "BadDigest",
		Description:    "The Content-MD5 you specified did not match what we received.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedXML: {
		Code:           "MalformedXML",
		Description:    "The XML you provided was not well-formed or did not validate against our published schema.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMalformedPolicy: {
		Code:           "MalformedPolicy",
		Description:    "Policy has invalid resource.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPolicyDocument: {
		Code:           "InvalidPolicyDocument",
		Description:    "The content of the form does not meet the conditions specified in the policy document.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidTag: {
		Code:           "InvalidTag",
		Description:    "The tag provided was not a valid tag. This error can occur if the tag did not pass input validation.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidURI: {
		Code:           "InvalidURI",
		Description:    "Couldn't parse the specified URI.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidLocationConstraint: {
		Code:           "InvalidLocationConstraint",
		Description:    "The specified location constraint is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPayer: {
		Code:           "InvalidPayer",
		Description:    "All access to this object has been disabled.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrInvalidStorageClass: {
		Code:           "InvalidStorageClass",
		Description:    "The storage class you specified is not valid.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingContentLength: {
		Code:           "MissingContentLength",
		Description:    "You must provide the Content-Length HTTP header.",
		HTTPStatusCode: http.StatusLengthRequired,
	},
	ErrMissingRequestBodyError: {
		Code:           "MissingRequestBodyError",
		Description:    "Request body is empty.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrUnexpectedContent: {
		Code:           "UnexpectedContent",
		Description:    "This request does not support content.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRequestTimeout: {
		Code:           "RequestTimeout",
		Description:    "Your socket connection to the server was not read from or written to within the timeout period.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRequestHeaderSectionTooLarge: {
		Code:           "RequestHeaderSectionTooLarge",
		Description:    "Your request header exceeds the maximum allowed request header size.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// Copy operation errors
	ErrInvalidCopyDest: {
		Code:           "InvalidRequest",
		Description:    "This copy request is illegal because it is trying to copy an object to itself without changing the object's metadata, storage class, website redirect location or encryption attributes.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidCopySource: {
		Code:           "InvalidArgument",
		Description:    "Copy Source must mention the source bucket and key: sourcebucket/sourcekey.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrCopySourceTooLarge: {
		Code:           "InvalidRequest",
		Description:    "The specified copy source is larger than the maximum allowable size for a copy source.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// POST/Form errors
	ErrMalformedPOSTRequest: {
		Code:           "MalformedPOSTRequest",
		Description:    "The body of your POST request is not well-formed multipart/form-data.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPOSTFileRequired: {
		Code:           "InvalidArgument",
		Description:    "POST requires exactly one file upload per request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrPostPolicyConditionInvalidFormat: {
		Code:           "PostPolicyInvalidKeyName",
		Description:    "Invalid according to Policy: Policy Condition failed.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrMaxPostPreDataLengthExceededError: {
		Code:           "MaxPostPreDataLengthExceededError",
		Description:    "Your POST request fields preceding the upload file were too large.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingFields: {
		Code:           "MissingFields",
		Description:    "Missing fields in request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrIncorrectNumberOfFilesInPostRequest: {
		Code:           "IncorrectNumberOfFilesInPostRequest",
		Description:    "POST requires exactly one file upload per request.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrRequestIsNotMultiPartContent: {
		Code:           "RequestIsNotMultiPartContent",
		Description:    "Bucket POST must be of the enclosure-type multipart/form-data.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// Checksum errors
	ErrChecksumMismatch: {
		Code:           "BadDigest",
		Description:    "The checksum value you specified did not match what we computed.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrMissingChecksum: {
		Code:           "MissingContentMD5",
		Description:    "Missing required header for this request: Content-MD5.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrContentSHA256Mismatch: {
		Code:           "XAmzContentSHA256Mismatch",
		Description:    "The provided 'x-amz-content-sha256' header does not match what was computed.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// =========================================================================
	// SSE-C Errors
	// =========================================================================
	ErrInvalidEncryptionAlgorithm: {
		Code:           "InvalidEncryptionAlgorithmError",
		Description:    "The encryption request you specified is not valid. The valid value is AES256.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidEncryptionKey: {
		Code:           "InvalidArgument",
		Description:    "The provided encryption key is not valid. Encryption key must be 256-bit base64-encoded.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSECustomerKeyMD5Mismatch: {
		Code:           "InvalidArgument",
		Description:    "The calculated MD5 hash of the key did not match the hash that was provided.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSECustomerKeyMissing: {
		Code:           "InvalidRequest",
		Description:    "The object was stored using a form of Server Side Encryption. The correct parameters must be provided to retrieve the object.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSECustomerKeyNotNeeded: {
		Code:           "InvalidRequest",
		Description:    "The object was not stored using Server Side Encryption. Specifying encryption key headers is not permitted.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrSSEEncryptionTypeMismatch: {
		Code:           "InvalidRequest",
		Description:    "The encryption method specified in the request does not match the encryption method used to encrypt the object.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// =========================================================================
	// SSE-KMS Errors
	// =========================================================================
	ErrKMSKeyNotFound: {
		Code:           "KMS.NotFoundException",
		Description:    "The specified KMS key does not exist.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrKMSAccessDenied: {
		Code:           "AccessDenied",
		Description:    "Access denied to the specified KMS key.",
		HTTPStatusCode: http.StatusForbidden,
	},
	ErrKMSDisabled: {
		Code:           "KMS.DisabledException",
		Description:    "The specified KMS key is disabled.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrKMSInvalidCiphertext: {
		Code:           "InvalidCiphertextException",
		Description:    "The provided ciphertext is invalid or corrupted.",
		HTTPStatusCode: http.StatusBadRequest,
	},

	// =========================================================================
	// Service / Throttling Errors
	// =========================================================================
	ErrInternalError: {
		Code:           "InternalError",
		Description:    "We encountered an internal error. Please try again.",
		HTTPStatusCode: http.StatusInternalServerError,
	},
	ErrNotImplemented: {
		Code:           "NotImplemented",
		Description:    "A header you provided implies functionality that is not implemented.",
		HTTPStatusCode: http.StatusNotImplemented,
	},
	ErrServiceUnavailable: {
		Code:           "ServiceUnavailable",
		Description:    "Service is unable to handle request.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrSlowDown: {
		Code:           "SlowDown",
		Description:    "Please reduce your request rate.",
		HTTPStatusCode: http.StatusServiceUnavailable,
	},
	ErrTooManyRequests: {
		Code:           "TooManyRequests",
		Description:    "Please reduce your request rate.",
		HTTPStatusCode: http.StatusTooManyRequests,
	},
	ErrRequestBytesExceed: {
		Code:           "RequestThrottled",
		Description:    "Request bytes exceed limitations.",
		HTTPStatusCode: http.StatusTooManyRequests,
	},

	// =========================================================================
	// Response / Redirect Errors
	// =========================================================================
	ErrMethodNotAllowed: {
		Code:           "MethodNotAllowed",
		Description:    "The specified method is not allowed against this resource.",
		HTTPStatusCode: http.StatusMethodNotAllowed,
	},
	ErrPreconditionFailed: {
		Code:           "PreconditionFailed",
		Description:    "At least one of the pre-conditions you specified did not hold.",
		HTTPStatusCode: http.StatusPreconditionFailed,
	},
	ErrNotModified: {
		Code:           "NotModified",
		Description:    "Not Modified.",
		HTTPStatusCode: http.StatusNotModified,
	},
	ErrPermanentRedirect: {
		Code:           "PermanentRedirect",
		Description:    "The bucket you are attempting to access must be addressed using the specified endpoint.",
		HTTPStatusCode: http.StatusMovedPermanently,
	},
	ErrTemporaryRedirect: {
		Code:           "TemporaryRedirect",
		Description:    "You are being redirected to the bucket while DNS updates.",
		HTTPStatusCode: http.StatusTemporaryRedirect,
	},

	// =========================================================================
	// Custom / Extended Errors
	// =========================================================================
	ErrExistingObjectIsDirectory: {
		Code:           "ExistingObjectIsDirectory",
		Description:    "Existing Object is a directory.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrExistingObjectIsFile: {
		Code:           "ExistingObjectIsFile",
		Description:    "Existing Object is a file.",
		HTTPStatusCode: http.StatusConflict,
	},
	ErrMaxSizeExceeded: {
		Code:           "MaxMessageLengthExceeded",
		Description:    "Your request was too big.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrAuthNotSetup: {
		Code:           "InvalidRequest",
		Description:    "Signed request requires setting up S3 authentication.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxKeys: {
		Code:           "InvalidArgument",
		Description:    "Argument maxKeys must be an integer between 0 and 2147483647.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxUploads: {
		Code:           "InvalidArgument",
		Description:    "Argument max-uploads must be an integer between 0 and 2147483647.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxParts: {
		Code:           "InvalidArgument",
		Description:    "Argument max-parts must be an integer between 0 and 2147483647.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidMaxDeleteObjects: {
		Code:           "InvalidArgument",
		Description:    "The Objects argument can contain a list of up to 1000 keys.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidPartNumberMarker: {
		Code:           "InvalidArgument",
		Description:    "Argument partNumberMarker must be an integer.",
		HTTPStatusCode: http.StatusBadRequest,
	},
	ErrInvalidUnorderedWithDelimiter: {
		Code:           "InvalidArgument",
		Description:    "Unordered listing cannot be used with delimiter.",
		HTTPStatusCode: http.StatusBadRequest,
	},
}

// =========================================================================
// ErrorCode Methods
// =========================================================================

// APIError returns the full APIError struct for this error code.
func (e ErrorCode) APIError() APIError {
	if err, ok := errorCodeResponse[e]; ok {
		return err
	}
	return APIError{
		Code:           "InternalError",
		Description:    "We encountered an internal error. Please try again.",
		HTTPStatusCode: http.StatusInternalServerError,
	}
}

// Code returns the S3 error code string.
func (e ErrorCode) Code() string {
	return e.APIError().Code
}

// Description returns the error description.
func (e ErrorCode) Description() string {
	return e.APIError().Description
}

// Error implements the error interface.
func (e ErrorCode) Error() string {
	return e.Description()
}

// HTTPStatusCode returns the HTTP status code for this error.
func (e ErrorCode) HTTPStatusCode() int {
	return e.APIError().HTTPStatusCode
}

// ToErrorResponse creates an Error response suitable for XML serialization.
func (e ErrorCode) ToErrorResponse(resource string) Error {
	api := e.APIError()
	return Error{
		Code:      api.Code,
		Message:   api.Description,
		Resource:  resource,
		RequestID: "", // Will be filled in by response handler
		HTTPCode:  api.HTTPStatusCode,
	}
}

// ToErrorResponseWithMessage creates an Error response with a custom message.
func (e ErrorCode) ToErrorResponseWithMessage(resource, message string) Error {
	api := e.APIError()
	return Error{
		Code:      api.Code,
		Message:   message,
		Resource:  resource,
		RequestID: "",
		HTTPCode:  api.HTTPStatusCode,
	}
}
