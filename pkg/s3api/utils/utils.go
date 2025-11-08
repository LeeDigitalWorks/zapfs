package utils

import (
	"errors"
	"net"
	"strings"
)

func ValidateBucketName(bucketName string) error {
	if strings.TrimSpace(bucketName) == "" {
		return errors.New("bucket name cannot be empty")
	}
	if len(bucketName) < 3 || len(bucketName) > 63 {
		return errors.New("bucket name length must be between 3 and 63 characters")
	}
	if net.ParseIP(bucketName) != nil {
		return errors.New("bucket name cannot be formatted as an IP address")
	}
	if strings.Contains(bucketName, "..") {
		return errors.New("bucket name contains invalid characters")
	}
	if bucketName[0] == '.' || bucketName[len(bucketName)-1] == '.' {
		return errors.New("bucket name cannot start or end with a period")
	}
	if bucketName[0] == '-' || bucketName[len(bucketName)-1] == '-' {
		return errors.New("bucket name cannot start or end with a hyphen")
	}
	if strings.HasPrefix(bucketName, "xn--") {
		return errors.New("bucket name cannot start with 'xn--'")
	}
	if strings.HasSuffix(bucketName, "-s3alias") {
		return errors.New("bucket name cannot end with '-s3alias'")
	}
	if strings.HasPrefix(bucketName, "sthree-") {
		return errors.New("bucket name cannot start with 'sthree-'")
	}
	if strings.HasPrefix(bucketName, "amzn-s3-demo-") {
		return errors.New("bucket name cannot start with 'amzn-s3-demo-'")
	}
	if strings.HasSuffix(bucketName, "--ol-s3") {
		return errors.New("bucket name cannot end with '--ol-s3'")
	}
	if strings.HasSuffix(bucketName, ".mrap") {
		return errors.New("bucket name cannot end with '.mrap'")
	}
	if strings.HasSuffix(bucketName, "--x-s3") {
		return errors.New("bucket name cannot end with '--x-s3'")
	}
	if strings.HasSuffix(bucketName, "--table-s3") {
		return errors.New("bucket name cannot end with '--table-s3'")
	}
	for _, char := range bucketName {
		if (char >= 'a' && char <= 'z') || (char >= '0' && char <= '9') || char == '-' || char == '.' {
			continue
		}
		return errors.New("bucket name contains invalid characters")
	}
	return nil
}
