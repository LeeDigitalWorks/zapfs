package s3types

import "encoding/xml"

// CORSConfiguration defines cross-origin resource sharing rules.
type CORSConfiguration struct {
	XMLName xml.Name   `xml:"CORSConfiguration" json:"-"`
	Rules   []CORSRule `xml:"CORSRule" json:"cors_rules"`
}

// CORSRule defines a single CORS rule.
type CORSRule struct {
	ID             string   `xml:"ID,omitempty" json:"id,omitempty"`
	AllowedHeaders []string `xml:"AllowedHeader" json:"allowed_headers,omitempty"`
	AllowedMethods []string `xml:"AllowedMethod" json:"allowed_methods"`
	AllowedOrigins []string `xml:"AllowedOrigin" json:"allowed_origins"`
	ExposeHeaders  []string `xml:"ExposeHeader,omitempty" json:"expose_headers,omitempty"`
	MaxAgeSeconds  int      `xml:"MaxAgeSeconds,omitempty" json:"max_age_seconds,omitempty"`
}
