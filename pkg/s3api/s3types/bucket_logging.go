package s3types

import "encoding/xml"

type BucketLoggingStatus struct {
	XMLName        xml.Name    `xml:"BucketLoggingStatus"`
	LoggingEnabled LoggingRule `xml:"LoggingEnabled"`
}

type LoggingRule struct {
	TargetBucket string `xml:"TargetBucket"`
	TargetPrefix string `xml:"TargetPrefix"`
}
