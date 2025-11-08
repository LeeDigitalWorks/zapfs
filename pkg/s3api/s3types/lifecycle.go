package s3types

import (
	"encoding/xml"
	"time"
)

type Lifecycle struct {
	XMLName xml.Name        `xml:"LifecycleConfiguration"`
	Rules   []LifecycleRule `xml:"Rule"`
}

type LifecycleRule struct {
	XMLName                        xml.Name                                 `xml:"Rule"`
	ID                             *string                                  `xml:"ID,omitempty"`
	Status                         LifecycleStatus                          `xml:"Status"`
	AbortIncompleteMultipartUpload *LifecycleAbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
	Expiration                     *LifecycleExpiration                     `xml:"LifecycleExpiration,omitempty"`
	Filter                         *LifecycleFilter                         `xml:"Filter,omitempty"`
	NoncurrentVersionExpiration    *LifecycleNoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentVersionTransition    *LifecycleNoncurrentVersionTransition    `xml:"NoncurrentVersionTransition,omitempty"`
	Transitions                    []*LifecycleTransition                   `xml:"Transition,omitempty"`
}

type LifecycleStatus string

const (
	LifecycleStatusEnabled  LifecycleStatus = "Enabled"
	LifecycleStatusDisabled LifecycleStatus = "Disabled"
)

type LifecycleAbortIncompleteMultipartUpload struct {
	XMLName             xml.Name `xml:"AbortIncompleteMultipartUpload"`
	DaysAfterInitiation *int64   `xml:"DaysAfterInitiation,omitempty"`
}

type LifecycleExpiration struct {
	XMLName                   xml.Name   `xml:"LifecycleExpiration"`
	Date                      *time.Time `xml:"Date,omitempty"`
	Days                      *int64     `xml:"Days,omitempty"`
	ExpiredObjectDeleteMarker *bool      `xml:"ExpiredObjectDeleteMarker,omitempty"`
}

type LifecycleFilter struct {
	XMLName               xml.Name                  `xml:"Filter"`
	And                   *LifecycleRuleAndOperator `xml:"And,omitempty"`
	ObjectSizeGreaterThan *int64                    `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    *int64                    `xml:"ObjectSizeLessThan,omitempty"`
	Prefix                *string                   `xml:"Prefix,omitempty"`
	Tag                   *Tag                      `xml:"Tag,omitempty"`
}

type LifecycleRuleAndOperator struct {
	XMLName               xml.Name `xml:"And"`
	ObjectSizeGreaterThan *int64   `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    *int64   `xml:"ObjectSizeLessThan,omitempty"`
	Prefix                *string  `xml:"Prefix,omitempty"`
	Tags                  []*Tag   `xml:"Tag,omitempty"`
}

type LifecycleNoncurrentVersionExpiration struct {
	XMLName                 xml.Name `xml:"NoncurrentVersionExpiration"`
	NewerNoncurrentVersions *int64   `xml:"NewerNoncurrentVersions,omitempty"`
	Days                    *int64   `xml:"Days,omitempty"`
}

type LifecycleNoncurrentVersionTransition struct {
	XMLName                 xml.Name `xml:"NoncurrentVersionTransition"`
	NewerNoncurrentVersions *int64   `xml:"NewerNoncurrentVersions,omitempty"`
	Days                    *int64   `xml:"Days,omitempty"`
	StorageClass            *string  `xml:"StorageClass,omitempty"`
}

type LifecycleTransition struct {
	XMLName      xml.Name   `xml:"Transition"`
	Date         *time.Time `xml:"Date,omitempty"`
	Days         *int64     `xml:"Days,omitempty"`
	StorageClass *string    `xml:"StorageClass,omitempty"`
}
