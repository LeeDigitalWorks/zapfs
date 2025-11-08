package signature

import (
	"mime"
	"net/http"
	"strings"

	"zapfs/pkg/metadata/data"
	"zapfs/pkg/s3api/s3consts"
)

const (
	AuthHeaderV4 = "AWS4-HMAC-SHA256"
	AuthHeaderV2 = "AWS"

	Iso8601BasicFormat = "20060102T150405Z"
	Iso8601DateFormat  = "20060102"

	UnsignedPayload         = "UNSIGNED-PAYLOAD"
	StreamingPayload        = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD"
	StreamingPayloadTrailer = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"

	// Precomputed SHA256 hash of an empty payload
	HashedEmptyPayload = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
)

type AuthType int

const (
	AuthTypeNone AuthType = iota
	AuthTypeAnonymous
	AuthTypeV2
	AuthTypeV4
	AuthTypePresignedV2
	AuthTypePresignedV4
	AuthTypePostPolicy
	AuthTypeStreamingSigned
	AuthTypeStreamingSignedTrailer
	AuthTypeStreamingUnsignedTrailer
)

func (a AuthType) String() string {
	switch a {
	case AuthTypeNone:
		return "none"
	case AuthTypeAnonymous:
		return "anonymous"
	case AuthTypeV2:
		return "v2"
	case AuthTypeV4:
		return "v4"
	case AuthTypePresignedV2:
		return "presigned_v2"
	case AuthTypePresignedV4:
		return "presigned_v4"
	case AuthTypePostPolicy:
		return "post_policy"
	case AuthTypeStreamingSigned:
		return "streaming_signed"
	case AuthTypeStreamingSignedTrailer:
		return "streaming_signed_trailer"
	case AuthTypeStreamingUnsignedTrailer:
		return "streaming_unsigned_trailer"
	default:
		return "unknown"
	}
}

func isRequestSignStreamingSigned(d *data.Data) bool {
	return d.Req.Header.Get(s3consts.XAmzContentSHA256) == StreamingPayload && d.Req.Method == http.MethodPut
}

func isRequestSignStreamingSignedTrailer(d *data.Data) bool {
	return d.Req.Header.Get(s3consts.XAmzContentSHA256) == StreamingPayloadTrailer && d.Req.Method == http.MethodPut
}

func isRequestSignStreamingUnsignedTrailer(d *data.Data) bool {
	return d.Req.Header.Get(s3consts.XAmzContentSHA256) == UnsignedPayload
}

func isRequestAnonymous(d *data.Data) bool {
	authHeader := d.Req.Header.Get("Authorization")
	return authHeader == ""
}

func isRequestSignatureV4(d *data.Data) bool {
	authHeader := d.Req.Header.Get("Authorization")
	return strings.HasPrefix(authHeader, AuthHeaderV4+" ")
}

func isRequestSignatureV2(d *data.Data) bool {
	authHeader := d.Req.Header.Get("Authorization")
	return strings.HasPrefix(authHeader, AuthHeaderV2+" ")
}

func isRequestPresignedV4(d *data.Data) bool {
	query := d.Req.URL.Query()
	_, hasAlgorithm := query[s3consts.XAmzAlgorithm]
	_, hasCredential := query[s3consts.XAmzCredential]
	_, hasSignature := query[s3consts.XAmzSignature]
	return hasAlgorithm && hasCredential && hasSignature
}

func isRequestPresignedV2(d *data.Data) bool {
	query := d.Req.URL.Query()
	_, hasAccessKeyID := query["AWSAccessKeyId"]
	_, hasSignature := query["Signature"]
	return hasAccessKeyID && hasSignature
}

func isRequestPostPolicy(d *data.Data) bool {
	if d.Req.Method != http.MethodPost {
		return false
	}

	contentType, _, err := mime.ParseMediaType(d.Req.Header.Get("Content-Type"))
	if err != nil {
		return false
	}

	return contentType == "multipart/form-data"
}

func GetAuthType(d *data.Data) AuthType {
	switch {
	case isRequestSignStreamingSigned(d):
		return AuthTypeStreamingSigned
	case isRequestSignStreamingSignedTrailer(d):
		return AuthTypeStreamingSignedTrailer
	case isRequestSignStreamingUnsignedTrailer(d):
		return AuthTypeStreamingUnsignedTrailer
	case isRequestAnonymous(d):
		return AuthTypeAnonymous
	case isRequestSignatureV4(d):
		return AuthTypeV4
	case isRequestSignatureV2(d):
		return AuthTypeV2
	case isRequestPresignedV4(d):
		return AuthTypePresignedV4
	case isRequestPresignedV2(d):
		return AuthTypePresignedV2
	case isRequestPostPolicy(d):
		return AuthTypePostPolicy
	}
	return AuthTypeNone
}
