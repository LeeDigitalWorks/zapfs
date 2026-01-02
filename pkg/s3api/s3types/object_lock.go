// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import "encoding/xml"

// ObjectLockConfiguration defines WORM (Write Once Read Many) settings.
type ObjectLockConfiguration struct {
	XMLName           xml.Name        `xml:"ObjectLockConfiguration" json:"-"`
	ObjectLockEnabled string          `xml:"ObjectLockEnabled" json:"object_lock_enabled"` // "Enabled"
	Rule              *ObjectLockRule `xml:"Rule,omitempty" json:"rule,omitempty"`
}

// ObjectLockRule defines default retention settings.
type ObjectLockRule struct {
	DefaultRetention *DefaultRetention `xml:"DefaultRetention" json:"default_retention"`
}

// DefaultRetention defines default retention period.
type DefaultRetention struct {
	Mode  string `xml:"Mode" json:"mode"` // GOVERNANCE or COMPLIANCE
	Days  int    `xml:"Days,omitempty" json:"days,omitempty"`
	Years int    `xml:"Years,omitempty" json:"years,omitempty"`
}

// ObjectLockRetention defines per-object retention.
type ObjectLockRetention struct {
	XMLName         xml.Name `xml:"Retention" json:"-"`
	Mode            string   `xml:"Mode" json:"mode"`
	RetainUntilDate string   `xml:"RetainUntilDate" json:"retain_until_date"`
}

// ObjectLockLegalHold defines legal hold status.
type ObjectLockLegalHold struct {
	XMLName xml.Name `xml:"LegalHold" json:"-"`
	Status  string   `xml:"Status" json:"status"` // ON or OFF
}
