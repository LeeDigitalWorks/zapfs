package s3types

import "encoding/xml"

// WebsiteConfiguration defines static website hosting.
type WebsiteConfiguration struct {
	XMLName               xml.Name               `xml:"WebsiteConfiguration" json:"-"`
	IndexDocument         *IndexDocument         `xml:"IndexDocument,omitempty" json:"index_document,omitempty"`
	ErrorDocument         *ErrorDocument         `xml:"ErrorDocument,omitempty" json:"error_document,omitempty"`
	RedirectAllRequestsTo *RedirectAllRequestsTo `xml:"RedirectAllRequestsTo,omitempty" json:"redirect_all,omitempty"`
	RoutingRules          *RoutingRules          `xml:"RoutingRules,omitempty" json:"routing_rules,omitempty"`
}

// IndexDocument specifies the index document for website.
type IndexDocument struct {
	Suffix string `xml:"Suffix" json:"suffix"`
}

// ErrorDocument specifies the error document for website.
type ErrorDocument struct {
	Key string `xml:"Key" json:"key"`
}

// RedirectAllRequestsTo redirects all requests to another host.
type RedirectAllRequestsTo struct {
	HostName string `xml:"HostName" json:"host_name"`
	Protocol string `xml:"Protocol,omitempty" json:"protocol,omitempty"`
}

// RoutingRules is a list of routing rules.
type RoutingRules struct {
	Rules []RoutingRule `xml:"RoutingRule" json:"rules"`
}

// RoutingRule defines a website routing rule.
type RoutingRule struct {
	Condition *RoutingCondition `xml:"Condition,omitempty" json:"condition,omitempty"`
	Redirect  *RoutingRedirect  `xml:"Redirect" json:"redirect"`
}

// RoutingCondition defines when to apply a routing rule.
type RoutingCondition struct {
	HttpErrorCodeReturnedEquals string `xml:"HttpErrorCodeReturnedEquals,omitempty" json:"http_error_code,omitempty"`
	KeyPrefixEquals             string `xml:"KeyPrefixEquals,omitempty" json:"key_prefix,omitempty"`
}

// RoutingRedirect defines where to redirect.
type RoutingRedirect struct {
	HostName             string `xml:"HostName,omitempty" json:"host_name,omitempty"`
	HttpRedirectCode     string `xml:"HttpRedirectCode,omitempty" json:"http_redirect_code,omitempty"`
	Protocol             string `xml:"Protocol,omitempty" json:"protocol,omitempty"`
	ReplaceKeyPrefixWith string `xml:"ReplaceKeyPrefixWith,omitempty" json:"replace_key_prefix_with,omitempty"`
	ReplaceKeyWith       string `xml:"ReplaceKeyWith,omitempty" json:"replace_key_with,omitempty"`
}
