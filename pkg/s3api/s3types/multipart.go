// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import "encoding/xml"

// InitiateMultipartUploadResult is the response for CreateMultipartUpload
type InitiateMultipartUploadResult struct {
	XMLName  xml.Name `xml:"InitiateMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr,omitempty"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	UploadID string   `xml:"UploadId"`
}

// CompleteMultipartUploadRequest is the request body for CompleteMultipartUpload
type CompleteMultipartUploadRequest struct {
	XMLName xml.Name       `xml:"CompleteMultipartUpload"`
	Parts   []CompletePart `xml:"Part"`
}

// CompletePart represents a part in the complete request
type CompletePart struct {
	PartNumber int    `xml:"PartNumber"`
	ETag       string `xml:"ETag"`
}

// CompleteMultipartUploadResult is the response for CompleteMultipartUpload
type CompleteMultipartUploadResult struct {
	XMLName  xml.Name `xml:"CompleteMultipartUploadResult"`
	Xmlns    string   `xml:"xmlns,attr,omitempty"`
	Location string   `xml:"Location"`
	Bucket   string   `xml:"Bucket"`
	Key      string   `xml:"Key"`
	ETag     string   `xml:"ETag"`
}

// ListPartsResult is the response for ListParts
type ListPartsResult struct {
	XMLName              xml.Name   `xml:"ListPartsResult"`
	Xmlns                string     `xml:"xmlns,attr,omitempty"`
	Bucket               string     `xml:"Bucket"`
	Key                  string     `xml:"Key"`
	UploadID             string     `xml:"UploadId"`
	Initiator            *Initiator `xml:"Initiator,omitempty"`
	Owner                *Owner     `xml:"Owner,omitempty"`
	StorageClass         string     `xml:"StorageClass"`
	PartNumberMarker     int        `xml:"PartNumberMarker"`
	NextPartNumberMarker int        `xml:"NextPartNumberMarker"`
	MaxParts             int        `xml:"MaxParts"`
	IsTruncated          bool       `xml:"IsTruncated"`
	Parts                []PartInfo `xml:"Part"`
}

// PartInfo represents a part in list responses
type PartInfo struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
}

// Initiator represents the initiator of an upload
type Initiator struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName"`
}

// ListMultipartUploadsResult is the response for ListMultipartUploads
type ListMultipartUploadsResult struct {
	XMLName            xml.Name          `xml:"ListMultipartUploadsResult"`
	Xmlns              string            `xml:"xmlns,attr,omitempty"`
	Bucket             string            `xml:"Bucket"`
	KeyMarker          string            `xml:"KeyMarker"`
	UploadIDMarker     string            `xml:"UploadIdMarker"`
	NextKeyMarker      string            `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string            `xml:"NextUploadIdMarker,omitempty"`
	Delimiter          string            `xml:"Delimiter,omitempty"`
	Prefix             string            `xml:"Prefix,omitempty"`
	MaxUploads         int               `xml:"MaxUploads"`
	IsTruncated        bool              `xml:"IsTruncated"`
	Uploads            []MultipartUpload `xml:"Upload"`
	CommonPrefixes     []CommonPrefix    `xml:"CommonPrefixes,omitempty"`
}

// MultipartUpload represents an in-progress multipart upload
type MultipartUpload struct {
	Key          string     `xml:"Key"`
	UploadID     string     `xml:"UploadId"`
	Initiator    *Initiator `xml:"Initiator,omitempty"`
	Owner        *Owner     `xml:"Owner,omitempty"`
	StorageClass string     `xml:"StorageClass"`
	Initiated    string     `xml:"Initiated"`
}

// Note: CommonPrefix is defined in object_list.go and shared with multipart uploads
