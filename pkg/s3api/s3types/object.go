package s3types

import "encoding/xml"

type Object struct {
	Name              string
	Size              int64
	ContentType       string
	ETag              string
	ChecksumAlgorithm ChecksumAlgorithm
	ChecksumValue     string
	StorageClass      StorageClass

	// TODO: Add specific backend metadata fields
}

// PostObjectResponse is the XML response for successful POST object uploads
// when success_action_status is 200 or 201
type PostObjectResponse struct {
	XMLName  xml.Name `xml:"PostResponse"`
	Location string   `xml:"Location,omitempty"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}
