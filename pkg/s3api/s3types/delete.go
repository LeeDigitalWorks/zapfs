package s3types

import "encoding/xml"

// DeleteObjectsRequest is the XML request for DeleteObjects (multi-object delete)
type DeleteObjectsRequest struct {
	XMLName xml.Name            `xml:"Delete"`
	Quiet   bool                `xml:"Quiet"`
	Objects []DeleteObjectEntry `xml:"Object"`
}

// DeleteObjectEntry represents an object to delete
type DeleteObjectEntry struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
}

// DeleteObjectsResult is the XML response for DeleteObjects
type DeleteObjectsResult struct {
	XMLName xml.Name        `xml:"DeleteResult"`
	Xmlns   string          `xml:"xmlns,attr"`
	Deleted []DeletedObject `xml:"Deleted"`
	Error   []DeleteError   `xml:"Error,omitempty"`
}

// DeletedObject represents a successfully deleted object
type DeletedObject struct {
	Key                   string `xml:"Key"`
	VersionID             string `xml:"VersionId,omitempty"`
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
}

// DeleteError represents a deletion error
type DeleteError struct {
	Key       string `xml:"Key"`
	VersionID string `xml:"VersionId,omitempty"`
	Code      string `xml:"Code"`
	Message   string `xml:"Message"`
}
