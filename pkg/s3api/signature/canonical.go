package signature

import (
	"net/url"
	"strings"
)

// encodeCanonicalURI encodes a path for AWS Signature canonical URI.
// Each path segment is URL-encoded separately, preserving slashes as path separators.
// This matches how AWS SDKs encode paths for signature calculation.
// Used by both Signature V2 and V4.
func encodeCanonicalURI(path string) string {
	if path == "" || path == "/" {
		return "/"
	}

	// Remove leading slash for processing, we'll add it back
	path = strings.TrimPrefix(path, "/")

	// Split by / and encode each segment
	segments := strings.Split(path, "/")
	encoded := make([]string, len(segments))
	for i, segment := range segments {
		// Encode each segment separately (this handles spaces, special chars, etc.)
		encoded[i] = url.PathEscape(segment)
	}

	// Join with / and add leading /
	return "/" + strings.Join(encoded, "/")
}
