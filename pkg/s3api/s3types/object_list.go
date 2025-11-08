package s3types

import "encoding/xml"

// ListObjectsResult represents the XML response for ListObjects (v1)
type ListObjectsResult struct {
	XMLName        xml.Name        `xml:"ListBucketResult"`
	Xmlns          string          `xml:"xmlns,attr"`
	Name           string          `xml:"Name"`
	Prefix         string          `xml:"Prefix"`
	Marker         string          `xml:"Marker,omitempty"`
	Delimiter      string          `xml:"Delimiter,omitempty"`
	MaxKeys        int             `xml:"MaxKeys"`
	IsTruncated    bool            `xml:"IsTruncated"`
	NextMarker     string          `xml:"NextMarker,omitempty"`
	Contents       []ObjectContent `xml:"Contents"`
	CommonPrefixes []CommonPrefix  `xml:"CommonPrefixes,omitempty"`
}

// ObjectContent represents an object in list responses (v1)
type ObjectContent struct {
	Key          string `xml:"Key"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         uint64 `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
}

// ListObjectsV2Result is the XML response for ListObjectsV2
type ListObjectsV2Result struct {
	XMLName               xml.Name          `xml:"ListBucketResult"`
	Xmlns                 string            `xml:"xmlns,attr"`
	Name                  string            `xml:"Name"`
	Prefix                string            `xml:"Prefix"`
	Delimiter             string            `xml:"Delimiter,omitempty"`
	MaxKeys               int               `xml:"MaxKeys"`
	KeyCount              int               `xml:"KeyCount"`
	IsTruncated           bool              `xml:"IsTruncated"`
	Contents              []ListObjectEntry `xml:"Contents"`
	CommonPrefixes        []CommonPrefix    `xml:"CommonPrefixes,omitempty"`
	ContinuationToken     string            `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string            `xml:"NextContinuationToken,omitempty"`
	StartAfter            string            `xml:"StartAfter,omitempty"`
	EncodingType          string            `xml:"EncodingType,omitempty"`
}

// ListObjectEntry represents an object in list responses (v2)
type ListObjectEntry struct {
	Key          string       `xml:"Key"`
	LastModified string       `xml:"LastModified"`
	ETag         string       `xml:"ETag"`
	Size         int64        `xml:"Size"`
	StorageClass string       `xml:"StorageClass"`
	Owner        *ObjectOwner `xml:"Owner,omitempty"`
}

// ObjectOwner represents an object owner in list responses
type ObjectOwner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// CommonPrefix represents a common prefix in list responses (for delimiter)
// Note: Also used by multipart uploads list
type CommonPrefix struct {
	Prefix string `xml:"Prefix"`
}
