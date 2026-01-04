// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import "encoding/xml"

// IntelligentTieringConfiguration represents an S3 Intelligent-Tiering configuration.
// See: https://docs.aws.amazon.com/AmazonS3/latest/API/API_IntelligentTieringConfiguration.html
type IntelligentTieringConfiguration struct {
	XMLName xml.Name `xml:"IntelligentTieringConfiguration"`

	// ID is the unique identifier for the configuration.
	ID string `xml:"Id"`

	// Filter limits the scope of the configuration to objects matching the filter.
	Filter *IntelligentTieringFilter `xml:"Filter,omitempty"`

	// Status indicates whether the configuration is enabled.
	Status string `xml:"Status"` // "Enabled" or "Disabled"

	// Tierings specifies the access tier transitions.
	Tierings []Tiering `xml:"Tiering"`
}

// IntelligentTieringFilter specifies which objects to include.
type IntelligentTieringFilter struct {
	// Prefix limits to objects with this prefix.
	Prefix string `xml:"Prefix,omitempty"`

	// Tag limits to objects with this tag.
	Tag *Tag `xml:"Tag,omitempty"`

	// And combines multiple filter conditions.
	And *IntelligentTieringAndOperator `xml:"And,omitempty"`
}

// IntelligentTieringAndOperator combines filter conditions.
type IntelligentTieringAndOperator struct {
	Prefix string `xml:"Prefix,omitempty"`
	Tags   []Tag  `xml:"Tag,omitempty"`
}

// Tiering specifies when to transition objects to a specific access tier.
type Tiering struct {
	// AccessTier is the target tier: ARCHIVE_ACCESS or DEEP_ARCHIVE_ACCESS.
	AccessTier string `xml:"AccessTier"`

	// Days is the number of days without access before transitioning.
	Days int `xml:"Days"`
}

// IntelligentTieringAccessTier constants
const (
	AccessTierArchive     = "ARCHIVE_ACCESS"
	AccessTierDeepArchive = "DEEP_ARCHIVE_ACCESS"
)

// IntelligentTieringStatus constants
const (
	IntelligentTieringEnabled  = "Enabled"
	IntelligentTieringDisabled = "Disabled"
)

// ListBucketIntelligentTieringConfigurationsResult is the response for listing configurations.
type ListBucketIntelligentTieringConfigurationsResult struct {
	XMLName xml.Name `xml:"ListBucketIntelligentTieringConfigurationsResult"`
	XMLNS   string   `xml:"xmlns,attr"`

	// Configurations is the list of configurations.
	Configurations []IntelligentTieringConfiguration `xml:"IntelligentTieringConfiguration,omitempty"`

	// IsTruncated indicates if there are more configurations.
	IsTruncated bool `xml:"IsTruncated"`

	// ContinuationToken for pagination.
	ContinuationToken string `xml:"ContinuationToken,omitempty"`

	// NextContinuationToken for fetching the next page.
	NextContinuationToken string `xml:"NextContinuationToken,omitempty"`
}

// Validate validates the intelligent tiering configuration.
func (c *IntelligentTieringConfiguration) Validate() error {
	if c.ID == "" {
		return &ValidationError{Field: "Id", Message: "Id is required"}
	}
	if len(c.ID) > 64 {
		return &ValidationError{Field: "Id", Message: "Id must be at most 64 characters"}
	}

	if c.Status != IntelligentTieringEnabled && c.Status != IntelligentTieringDisabled {
		return &ValidationError{Field: "Status", Message: "Status must be Enabled or Disabled"}
	}

	if len(c.Tierings) == 0 {
		return &ValidationError{Field: "Tiering", Message: "At least one Tiering is required"}
	}

	for _, tiering := range c.Tierings {
		if tiering.AccessTier != AccessTierArchive && tiering.AccessTier != AccessTierDeepArchive {
			return &ValidationError{Field: "AccessTier", Message: "AccessTier must be ARCHIVE_ACCESS or DEEP_ARCHIVE_ACCESS"}
		}
		if tiering.Days < 90 && tiering.AccessTier == AccessTierArchive {
			return &ValidationError{Field: "Days", Message: "ARCHIVE_ACCESS requires at least 90 days"}
		}
		if tiering.Days < 180 && tiering.AccessTier == AccessTierDeepArchive {
			return &ValidationError{Field: "Days", Message: "DEEP_ARCHIVE_ACCESS requires at least 180 days"}
		}
	}

	return nil
}

// ValidationError represents a validation error.
type ValidationError struct {
	Field   string
	Message string
}

func (e *ValidationError) Error() string {
	return e.Field + ": " + e.Message
}
