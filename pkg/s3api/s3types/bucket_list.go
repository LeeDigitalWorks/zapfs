package s3types

import "encoding/xml"

// ListAllMyBucketsResult is the XML response for ListBuckets
type ListAllMyBucketsResult struct {
	XMLName           xml.Name    `xml:"ListAllMyBucketsResult"`
	Xmlns             string      `xml:"xmlns,attr"`
	Owner             BucketOwner `xml:"Owner"`
	Buckets           BucketList  `xml:"Buckets"`
	ContinuationToken string      `xml:"ContinuationToken,omitempty"` // For paginated responses
	Prefix            string      `xml:"Prefix,omitempty"`            // Echo back prefix if provided
}

// BucketOwner represents owner info in bucket responses
type BucketOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// BucketList is a list of buckets
type BucketList struct {
	Buckets []BucketInfo `xml:"Bucket"`
}

// BucketInfo represents a bucket in list responses
type BucketInfo struct {
	Name         string `xml:"Name"`
	CreationDate string `xml:"CreationDate"`           // ISO 8601 format
	Region       string `xml:"BucketRegion,omitempty"` // Region (included in paginated responses)
}

// LocationConstraint is the XML response for GetBucketLocation
type LocationConstraint struct {
	XMLName  xml.Name `xml:"LocationConstraint"`
	Xmlns    string   `xml:"xmlns,attr,omitempty"`
	Location string   `xml:",chardata"`
}
