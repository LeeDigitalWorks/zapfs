package s3types_test

import (
	"encoding/xml"
	"testing"

	"zapfs/pkg/s3api/s3types"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
)

var ignoreXMLName = cmp.FilterPath(func(p cmp.Path) bool {
	if sf, ok := p.Last().(cmp.StructField); ok {
		return sf.Name() == "XMLName"
	}
	return false
}, cmp.Ignore())

func TestLifecycle_ParseXML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		xml     string
		want    s3types.Lifecycle
		wantErr bool
	}{
		{
			name: "simple rule with prefix filter and expiration days",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>expire-old-logs</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix>logs/</Prefix>
					</Filter>
					<LifecycleExpiration>
						<Days>90</Days>
					</LifecycleExpiration>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("expire-old-logs"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("logs/"),
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(90),
						},
					},
				},
			},
		},
		{
			name: "rule with transition to different storage class",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>transition-archive</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix>archive/</Prefix>
					</Filter>
					<Transition>
						<Days>30</Days>
						<StorageClass>GLACIER</StorageClass>
					</Transition>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("transition-archive"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("archive/"),
						},
						Transitions: []*s3types.LifecycleTransition{
							{
								Days:         aws.Int64(30),
								StorageClass: aws.String("GLACIER"),
							},
						},
					},
				},
			},
		},
		{
			name: "rule with multiple transitions",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>multi-tier-transition</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix>data/</Prefix>
					</Filter>
					<Transition>
						<Days>30</Days>
						<StorageClass>STANDARD_IA</StorageClass>
					</Transition>
					<Transition>
						<Days>90</Days>
						<StorageClass>GLACIER</StorageClass>
					</Transition>
					<Transition>
						<Days>365</Days>
						<StorageClass>DEEP_ARCHIVE</StorageClass>
					</Transition>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("multi-tier-transition"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("data/"),
						},
						Transitions: []*s3types.LifecycleTransition{
							{Days: aws.Int64(30), StorageClass: aws.String("STANDARD_IA")},
							{Days: aws.Int64(90), StorageClass: aws.String("GLACIER")},
							{Days: aws.Int64(365), StorageClass: aws.String("DEEP_ARCHIVE")},
						},
					},
				},
			},
		},
		{
			name: "rule with tag filter",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>expire-temp-files</ID>
					<Status>Enabled</Status>
					<Filter>
						<Tag>
							<Key>type</Key>
							<Value>temporary</Value>
						</Tag>
					</Filter>
					<LifecycleExpiration>
						<Days>7</Days>
					</LifecycleExpiration>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("expire-temp-files"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Tag: &s3types.Tag{
								Key:   "type",
								Value: "temporary",
							},
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(7),
						},
					},
				},
			},
		},
		{
			name: "rule with And filter combining prefix and tags",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>complex-filter</ID>
					<Status>Enabled</Status>
					<Filter>
						<And>
							<Prefix>documents/</Prefix>
							<Tag>
								<Key>archive</Key>
								<Value>true</Value>
							</Tag>
							<Tag>
								<Key>department</Key>
								<Value>legal</Value>
							</Tag>
						</And>
					</Filter>
					<LifecycleExpiration>
						<Days>2555</Days>
					</LifecycleExpiration>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("complex-filter"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							And: &s3types.LifecycleRuleAndOperator{
								Prefix: aws.String("documents/"),
								Tags: []*s3types.Tag{
									{Key: "archive", Value: "true"},
									{Key: "department", Value: "legal"},
								},
							},
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(2555),
						},
					},
				},
			},
		},
		{
			name: "rule with object size filters",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>large-object-filter</ID>
					<Status>Enabled</Status>
					<Filter>
						<And>
							<Prefix>uploads/</Prefix>
							<ObjectSizeGreaterThan>1048576</ObjectSizeGreaterThan>
							<ObjectSizeLessThan>10485760</ObjectSizeLessThan>
						</And>
					</Filter>
					<LifecycleExpiration>
						<Days>30</Days>
					</LifecycleExpiration>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("large-object-filter"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							And: &s3types.LifecycleRuleAndOperator{
								Prefix:                aws.String("uploads/"),
								ObjectSizeGreaterThan: aws.Int64(1048576),
								ObjectSizeLessThan:    aws.Int64(10485760),
							},
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(30),
						},
					},
				},
			},
		},
		{
			name: "rule with abort incomplete multipart upload",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>cleanup-multipart</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix></Prefix>
					</Filter>
					<AbortIncompleteMultipartUpload>
						<DaysAfterInitiation>7</DaysAfterInitiation>
					</AbortIncompleteMultipartUpload>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("cleanup-multipart"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String(""),
						},
						AbortIncompleteMultipartUpload: &s3types.LifecycleAbortIncompleteMultipartUpload{
							DaysAfterInitiation: aws.Int64(7),
						},
					},
				},
			},
		},
		{
			name: "rule with noncurrent version expiration",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>expire-old-versions</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix>versioned/</Prefix>
					</Filter>
					<NoncurrentVersionExpiration>
						<Days>90</Days>
						<NewerNoncurrentVersions>5</NewerNoncurrentVersions>
					</NoncurrentVersionExpiration>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("expire-old-versions"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("versioned/"),
						},
						NoncurrentVersionExpiration: &s3types.LifecycleNoncurrentVersionExpiration{
							Days:                    aws.Int64(90),
							NewerNoncurrentVersions: aws.Int64(5),
						},
					},
				},
			},
		},
		{
			name: "rule with noncurrent version transition",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>transition-old-versions</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix>versioned/</Prefix>
					</Filter>
					<NoncurrentVersionTransition>
						<Days>30</Days>
						<StorageClass>GLACIER</StorageClass>
						<NewerNoncurrentVersions>3</NewerNoncurrentVersions>
					</NoncurrentVersionTransition>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("transition-old-versions"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("versioned/"),
						},
						NoncurrentVersionTransition: &s3types.LifecycleNoncurrentVersionTransition{
							Days:                    aws.Int64(30),
							StorageClass:            aws.String("GLACIER"),
							NewerNoncurrentVersions: aws.Int64(3),
						},
					},
				},
			},
		},
		{
			name: "rule with expired object delete marker",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>delete-markers</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix></Prefix>
					</Filter>
					<LifecycleExpiration>
						<ExpiredObjectDeleteMarker>true</ExpiredObjectDeleteMarker>
					</LifecycleExpiration>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("delete-markers"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String(""),
						},
						Expiration: &s3types.LifecycleExpiration{
							ExpiredObjectDeleteMarker: aws.Bool(true),
						},
					},
				},
			},
		},
		{
			name: "disabled rule",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>disabled-rule</ID>
					<Status>Disabled</Status>
					<Filter>
						<Prefix>test/</Prefix>
					</Filter>
					<LifecycleExpiration>
						<Days>1</Days>
					</LifecycleExpiration>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("disabled-rule"),
						Status: s3types.LifecycleStatusDisabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("test/"),
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(1),
						},
					},
				},
			},
		},
		{
			name: "multiple rules",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>rule-1</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix>logs/</Prefix>
					</Filter>
					<LifecycleExpiration>
						<Days>30</Days>
					</LifecycleExpiration>
				</Rule>
				<Rule>
					<ID>rule-2</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix>archive/</Prefix>
					</Filter>
					<Transition>
						<Days>90</Days>
						<StorageClass>GLACIER</StorageClass>
					</Transition>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("rule-1"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("logs/"),
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(30),
						},
					},
					{
						ID:     aws.String("rule-2"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("archive/"),
						},
						Transitions: []*s3types.LifecycleTransition{
							{Days: aws.Int64(90), StorageClass: aws.String("GLACIER")},
						},
					},
				},
			},
		},
		{
			name: "comprehensive rule with all actions",
			xml: `<LifecycleConfiguration>
				<Rule>
					<ID>comprehensive-rule</ID>
					<Status>Enabled</Status>
					<Filter>
						<Prefix>full-lifecycle/</Prefix>
					</Filter>
					<AbortIncompleteMultipartUpload>
						<DaysAfterInitiation>7</DaysAfterInitiation>
					</AbortIncompleteMultipartUpload>
					<Transition>
						<Days>30</Days>
						<StorageClass>STANDARD_IA</StorageClass>
					</Transition>
					<Transition>
						<Days>90</Days>
						<StorageClass>GLACIER</StorageClass>
					</Transition>
					<LifecycleExpiration>
						<Days>365</Days>
					</LifecycleExpiration>
					<NoncurrentVersionTransition>
						<Days>30</Days>
						<StorageClass>GLACIER</StorageClass>
					</NoncurrentVersionTransition>
					<NoncurrentVersionExpiration>
						<Days>90</Days>
					</NoncurrentVersionExpiration>
				</Rule>
			</LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("comprehensive-rule"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("full-lifecycle/"),
						},
						AbortIncompleteMultipartUpload: &s3types.LifecycleAbortIncompleteMultipartUpload{
							DaysAfterInitiation: aws.Int64(7),
						},
						Transitions: []*s3types.LifecycleTransition{
							{Days: aws.Int64(30), StorageClass: aws.String("STANDARD_IA")},
							{Days: aws.Int64(90), StorageClass: aws.String("GLACIER")},
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(365),
						},
						NoncurrentVersionTransition: &s3types.LifecycleNoncurrentVersionTransition{
							Days:         aws.Int64(30),
							StorageClass: aws.String("GLACIER"),
						},
						NoncurrentVersionExpiration: &s3types.LifecycleNoncurrentVersionExpiration{
							Days: aws.Int64(90),
						},
					},
				},
			},
		},
		{
			name: "empty lifecycle configuration",
			xml:  `<LifecycleConfiguration></LifecycleConfiguration>`,
			want: s3types.Lifecycle{
				Rules: nil,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var got s3types.Lifecycle
			err := xml.Unmarshal([]byte(tt.xml), &got)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			require.Empty(t, cmp.Diff(tt.want, got, ignoreXMLName))
		})
	}
}

func TestLifecycle_MarshalXML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lifecycle s3types.Lifecycle
		wantErr   bool
	}{
		{
			name: "simple rule with expiration",
			lifecycle: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("expire-old-logs"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("logs/"),
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(90),
						},
					},
				},
			},
		},
		{
			name: "rule with transition",
			lifecycle: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("archive-data"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("archive/"),
						},
						Transitions: []*s3types.LifecycleTransition{
							{Days: aws.Int64(30), StorageClass: aws.String("GLACIER")},
						},
					},
				},
			},
		},
		{
			name: "rule with tag filter",
			lifecycle: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("temp-files"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							Tag: &s3types.Tag{
								Key:   "temporary",
								Value: "true",
							},
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(7),
						},
					},
				},
			},
		},
		{
			name: "rule with And filter",
			lifecycle: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("complex-filter"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{
							And: &s3types.LifecycleRuleAndOperator{
								Prefix: aws.String("docs/"),
								Tags: []*s3types.Tag{
									{Key: "archive", Value: "true"},
								},
							},
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(365),
						},
					},
				},
			},
		},
		{
			name: "disabled rule",
			lifecycle: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("disabled-rule"),
						Status: s3types.LifecycleStatusDisabled,
						Filter: &s3types.LifecycleFilter{
							Prefix: aws.String("test/"),
						},
						Expiration: &s3types.LifecycleExpiration{
							Days: aws.Int64(1),
						},
					},
				},
			},
		},
		{
			name: "multiple rules",
			lifecycle: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:         aws.String("rule-1"),
						Status:     s3types.LifecycleStatusEnabled,
						Filter:     &s3types.LifecycleFilter{Prefix: aws.String("a/")},
						Expiration: &s3types.LifecycleExpiration{Days: aws.Int64(10)},
					},
					{
						ID:         aws.String("rule-2"),
						Status:     s3types.LifecycleStatusEnabled,
						Filter:     &s3types.LifecycleFilter{Prefix: aws.String("b/")},
						Expiration: &s3types.LifecycleExpiration{Days: aws.Int64(20)},
					},
				},
			},
		},
		{
			name: "rule with abort incomplete multipart upload",
			lifecycle: s3types.Lifecycle{
				Rules: []s3types.LifecycleRule{
					{
						ID:     aws.String("cleanup-multipart"),
						Status: s3types.LifecycleStatusEnabled,
						Filter: &s3types.LifecycleFilter{Prefix: aws.String("")},
						AbortIncompleteMultipartUpload: &s3types.LifecycleAbortIncompleteMultipartUpload{
							DaysAfterInitiation: aws.Int64(7),
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Marshal to XML
			xmlBytes, err := xml.Marshal(&tt.lifecycle)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Verify roundtrip: unmarshal the generated XML and compare with original
			var roundtrip s3types.Lifecycle
			err = xml.Unmarshal(xmlBytes, &roundtrip)
			require.NoError(t, err, "roundtrip unmarshal failed for XML: %s", string(xmlBytes))
			require.Empty(t, cmp.Diff(tt.lifecycle, roundtrip, ignoreXMLName), "roundtrip mismatch")
		})
	}
}
