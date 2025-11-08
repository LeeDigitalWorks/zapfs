package s3types

import "encoding/xml"

// TagSet holds a set of tags for buckets or objects.
type TagSet struct {
	XMLName xml.Name `xml:"Tagging" json:"-"`
	Tags    []Tag    `xml:"TagSet>Tag" json:"tags"`
}

// Tag is a key-value pair for tagging resources.
// Note: This is the canonical Tag type used across the codebase.
// lifecycle.go has its own Tag type for historical reasons.
type Tag struct {
	XMLName xml.Name `xml:"Tag"`
	Key     string   `xml:"Key" json:"key"`
	Value   string   `xml:"Value" json:"value"`
}
