// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3types

import "encoding/xml"

// NotificationConfiguration defines event notification settings.
type NotificationConfiguration struct {
	XMLName                      xml.Name                      `xml:"NotificationConfiguration" json:"-"`
	TopicConfigurations          []TopicConfiguration          `xml:"TopicConfiguration,omitempty" json:"topic_configurations,omitempty"`
	QueueConfigurations          []QueueConfiguration          `xml:"QueueConfiguration,omitempty" json:"queue_configurations,omitempty"`
	LambdaFunctionConfigurations []LambdaFunctionConfiguration `xml:"CloudFunctionConfiguration,omitempty" json:"lambda_configurations,omitempty"`
}

// TopicConfiguration defines SNS topic notification.
type TopicConfiguration struct {
	ID       string              `xml:"Id,omitempty" json:"id,omitempty"`
	TopicArn string              `xml:"Topic" json:"topic_arn"`
	Events   []string            `xml:"Event" json:"events"`
	Filter   *NotificationFilter `xml:"Filter,omitempty" json:"filter,omitempty"`
}

// QueueConfiguration defines SQS queue notification.
type QueueConfiguration struct {
	ID       string              `xml:"Id,omitempty" json:"id,omitempty"`
	QueueArn string              `xml:"Queue" json:"queue_arn"`
	Events   []string            `xml:"Event" json:"events"`
	Filter   *NotificationFilter `xml:"Filter,omitempty" json:"filter,omitempty"`
}

// LambdaFunctionConfiguration defines Lambda notification.
type LambdaFunctionConfiguration struct {
	ID                string              `xml:"Id,omitempty" json:"id,omitempty"`
	LambdaFunctionArn string              `xml:"CloudFunction" json:"lambda_arn"`
	Events            []string            `xml:"Event" json:"events"`
	Filter            *NotificationFilter `xml:"Filter,omitempty" json:"filter,omitempty"`
}

// NotificationFilter filters which objects trigger notifications.
type NotificationFilter struct {
	Key *NotificationFilterKey `xml:"S3Key,omitempty" json:"key,omitempty"`
}

// NotificationFilterKey filters by object key.
type NotificationFilterKey struct {
	FilterRules []NotificationFilterRule `xml:"FilterRule" json:"filter_rules"`
}

// NotificationFilterRule is a key filter rule for notifications.
type NotificationFilterRule struct {
	Name  string `xml:"Name" json:"name"` // prefix or suffix
	Value string `xml:"Value" json:"value"`
}
