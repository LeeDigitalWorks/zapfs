package s3types

import "encoding/xml"

// ServerSideEncryptionConfig defines default bucket encryption.
type ServerSideEncryptionConfig struct {
	XMLName xml.Name                   `xml:"ServerSideEncryptionConfiguration" json:"-"`
	Rules   []ServerSideEncryptionRule `xml:"Rule" json:"rules"`
}

// ServerSideEncryptionRule defines an encryption rule.
type ServerSideEncryptionRule struct {
	ApplyServerSideEncryptionByDefault *EncryptionByDefault `xml:"ApplyServerSideEncryptionByDefault" json:"apply_default"`
	BucketKeyEnabled                   bool                 `xml:"BucketKeyEnabled,omitempty" json:"bucket_key_enabled,omitempty"`
}

// EncryptionByDefault specifies the default encryption settings.
type EncryptionByDefault struct {
	SSEAlgorithm   string `xml:"SSEAlgorithm" json:"sse_algorithm"` // AES256 or aws:kms
	KMSMasterKeyID string `xml:"KMSMasterKeyID,omitempty" json:"kms_key_id,omitempty"`
}
