// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import "encoding/xml"

// Versioning represents the versioning state of a bucket
type Versioning string

const (
	VersioningEnabled   Versioning = "Enabled"
	VersioningSuspended Versioning = "Suspended"
	VersioningDisabled  Versioning = "" // Not set = disabled
)

// VersioningConfiguration is the XML request/response for bucket versioning
type VersioningConfiguration struct {
	XMLName   xml.Name `xml:"VersioningConfiguration"`
	Xmlns     string   `xml:"xmlns,attr,omitempty"`
	Status    string   `xml:"Status,omitempty"`    // Enabled or Suspended
	MFADelete string   `xml:"MfaDelete,omitempty"` // Disabled or Enabled (requires MFA)
}

// ListVersionsResult is the XML response for ListObjectVersions
type ListVersionsResult struct {
	XMLName             xml.Name        `xml:"ListVersionsResult"`
	Xmlns               string          `xml:"xmlns,attr,omitempty"`
	Name                string          `xml:"Name"`
	Prefix              string          `xml:"Prefix,omitempty"`
	KeyMarker           string          `xml:"KeyMarker,omitempty"`
	VersionIDMarker     string          `xml:"VersionIdMarker,omitempty"`
	NextKeyMarker       string          `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string          `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int             `xml:"MaxKeys"`
	Delimiter           string          `xml:"Delimiter,omitempty"`
	IsTruncated         bool            `xml:"IsTruncated"`
	Versions            []ObjectVersion `xml:"Version"`
	DeleteMarkers       []DeleteMarker  `xml:"DeleteMarker,omitempty"`
	CommonPrefixes      []CommonPrefix  `xml:"CommonPrefixes,omitempty"`
}

// ObjectVersion represents a version of an object
type ObjectVersion struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	StorageClass string `xml:"StorageClass"`
	Owner        *Owner `xml:"Owner,omitempty"`
}

// DeleteMarker represents a delete marker version
type DeleteMarker struct {
	Key          string `xml:"Key"`
	VersionID    string `xml:"VersionId"`
	IsLatest     bool   `xml:"IsLatest"`
	LastModified string `xml:"LastModified"`
	Owner        *Owner `xml:"Owner,omitempty"`
}

// Note: Owner is defined in acl.go
