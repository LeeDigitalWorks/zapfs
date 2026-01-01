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

// GetObjectAttributesResponse is the XML response for GetObjectAttributes API
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObjectAttributes.html
type GetObjectAttributesResponse struct {
	XMLName      xml.Name                   `xml:"GetObjectAttributesResponse"`
	Xmlns        string                     `xml:"xmlns,attr,omitempty"`
	ETag         string                     `xml:"ETag,omitempty"`
	Checksum     *ObjectAttributesChecksum  `xml:"Checksum,omitempty"`
	ObjectParts  *ObjectAttributesParts     `xml:"ObjectParts,omitempty"`
	StorageClass string                     `xml:"StorageClass,omitempty"`
	ObjectSize   *int64                     `xml:"ObjectSize,omitempty"`
	LastModified string                     `xml:"LastModified,omitempty"`
}

// ObjectAttributesChecksum contains checksum information for an object
type ObjectAttributesChecksum struct {
	ChecksumCRC32   string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C  string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1    string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256  string `xml:"ChecksumSHA256,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
}

// ObjectAttributesParts contains multipart upload part information
type ObjectAttributesParts struct {
	TotalPartsCount int                       `xml:"TotalPartsCount,omitempty"`
	PartNumberMarker int                      `xml:"PartNumberMarker,omitempty"`
	NextPartNumberMarker int                  `xml:"NextPartNumberMarker,omitempty"`
	MaxParts        int                       `xml:"MaxParts,omitempty"`
	IsTruncated     bool                      `xml:"IsTruncated,omitempty"`
	Parts           []ObjectAttributesPart    `xml:"Part,omitempty"`
}

// ObjectAttributesPart contains information about a single part
type ObjectAttributesPart struct {
	PartNumber     int    `xml:"PartNumber,omitempty"`
	Size           int64  `xml:"Size,omitempty"`
	ChecksumCRC32  string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumSHA1   string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256 string `xml:"ChecksumSHA256,omitempty"`
}
