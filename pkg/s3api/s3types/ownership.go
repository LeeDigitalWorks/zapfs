// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import "encoding/xml"

// OwnershipControls defines object ownership settings.
type OwnershipControls struct {
	XMLName xml.Name                `xml:"OwnershipControls" json:"-"`
	Rules   []OwnershipControlsRule `xml:"Rule" json:"rules"`
}

// OwnershipControlsRule defines an ownership rule.
type OwnershipControlsRule struct {
	ObjectOwnership string `xml:"ObjectOwnership" json:"object_ownership"` // BucketOwnerPreferred, ObjectWriter, BucketOwnerEnforced
}

// ObjectOwnership values
const (
	ObjectOwnershipBucketOwnerPreferred = "BucketOwnerPreferred"
	ObjectOwnershipObjectWriter         = "ObjectWriter"
	ObjectOwnershipBucketOwnerEnforced  = "BucketOwnerEnforced"
)

// PublicAccessBlockConfig blocks public access.
type PublicAccessBlockConfig struct {
	XMLName               xml.Name `xml:"PublicAccessBlockConfiguration" json:"-"`
	BlockPublicAcls       bool     `xml:"BlockPublicAcls" json:"block_public_acls"`
	IgnorePublicAcls      bool     `xml:"IgnorePublicAcls" json:"ignore_public_acls"`
	BlockPublicPolicy     bool     `xml:"BlockPublicPolicy" json:"block_public_policy"`
	RestrictPublicBuckets bool     `xml:"RestrictPublicBuckets" json:"restrict_public_buckets"`
}

// AccelerateConfiguration defines transfer acceleration.
type AccelerateConfiguration struct {
	XMLName xml.Name `xml:"AccelerateConfiguration" json:"-"`
	Status  string   `xml:"Status" json:"status"` // Enabled or Suspended
}

// RequestPaymentConfig defines who pays for requests.
type RequestPaymentConfig struct {
	XMLName xml.Name `xml:"RequestPaymentConfiguration" json:"-"`
	Payer   string   `xml:"Payer" json:"payer"` // BucketOwner or Requester
}

// Payer values
const (
	PayerBucketOwner = "BucketOwner"
	PayerRequester   = "Requester"
)
