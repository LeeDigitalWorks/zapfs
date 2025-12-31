package utils_test

import (
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/utils"
)

func TestUtis_ValidateBucketName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		bucketName string
		wantErr    bool
	}{
		{"ValidBucket", "my-valid-bucket", false},
		{"EmptyBucket", "", true},
		{"TooShortBucket", "ab", true},
		{"TooLongBucket", "a-very-long-bucket-name-that-exceeds-the-maximum-length-of-sixty-three-characters", true},
		{"IPAddressBucket", "192.1.1.0", true},
		{"ConsecutiveDots", "my..bucket", true},
		{"StartsWithDot", ".mybucket", true},
		{"EndsWithDot", "mybucket.", true},
		{"StartsWithHyphen", "-mybucket", true},
		{"EndsWithHyphen", "mybucket-", true},
		{"StartsWithXn", "xn--mybucket", true},
		{"EndsWithS3Alias", "mybucket-s3alias", true},
		{"StartsWithSthree", "sthree-mybucket", true},
		{"StartsWithAmznS3Demo", "amzn-s3-demo-mybucket", true},
		{"ContainsSpace", "my bucket", true},
		{"InvalidCharacter", "my_bucket!", true},
		{"ValidWithDots", "my.bucket.name", false},
		{"EndsWithOlS3", "mybucket--ol-s3", true},
		{"EndsWithMrap", "mybucket.mrap", true},
		{"EndsWithXS3", "mybucket--x-s3", true},
		{"EndsWithTableS3", "mybucket--table-s3", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			err := utils.ValidateBucketName(tt.bucketName)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateBucketName(%q) error = %v, wantErr %v", tt.bucketName, err, tt.wantErr)
			}
		})
	}
}
