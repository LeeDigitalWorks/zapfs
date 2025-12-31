package config_test

import (
	"context"
	"errors"
	"testing"

	configmocks "github.com/LeeDigitalWorks/zapfs/mocks/config"
	dbmocks "github.com/LeeDigitalWorks/zapfs/mocks/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/service/config"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNewService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupConfig func(t *testing.T) config.Config
		wantErr     bool
		errContains string
	}{
		{
			name: "missing DB returns error",
			setupConfig: func(t *testing.T) config.Config {
				return config.Config{
					DB:          nil,
					BucketStore: configmocks.NewMockBucketStore(t),
				}
			},
			wantErr:     true,
			errContains: "DB is required",
		},
		{
			name: "missing BucketStore returns error",
			setupConfig: func(t *testing.T) config.Config {
				return config.Config{
					DB:          dbmocks.NewMockDB(t),
					BucketStore: nil,
				}
			},
			wantErr:     true,
			errContains: "BucketStore is required",
		},
		{
			name: "valid config succeeds",
			setupConfig: func(t *testing.T) config.Config {
				return config.Config{
					DB:          dbmocks.NewMockDB(t),
					BucketStore: configmocks.NewMockBucketStore(t),
				}
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := tc.setupConfig(t)
			svc, err := config.NewService(cfg)

			if tc.wantErr {
				assert.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, svc)
		})
	}
}

// ============================================================================
// Bucket Tagging Tests
// ============================================================================

func TestGetBucketTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		setupMocks  func(*dbmocks.MockDB, *configmocks.MockBucketStore)
		wantErr     bool
		wantErrCode config.ErrorCode
		checkResult func(*testing.T, *s3types.TagSet)
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("nonexistent").Return(s3types.Bucket{}, false)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchBucket,
		},
		{
			name:   "no tagging configured",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
				mockDB.EXPECT().GetBucketTagging(mock.Anything, "test-bucket").Return(nil, db.ErrTaggingNotFound)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchTagSet,
		},
		{
			name:   "successful get",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
				mockDB.EXPECT().GetBucketTagging(mock.Anything, "test-bucket").Return(&s3types.TagSet{
					Tags: []s3types.Tag{{Key: "env", Value: "prod"}},
				}, nil)
			},
			wantErr: false,
			checkResult: func(t *testing.T, tagSet *s3types.TagSet) {
				assert.Len(t, tagSet.Tags, 1)
				assert.Equal(t, "env", tagSet.Tags[0].Key)
				assert.Equal(t, "prod", tagSet.Tags[0].Value)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockBS := configmocks.NewMockBucketStore(t)

			tc.setupMocks(mockDB, mockBS)

			svc, err := config.NewService(config.Config{
				DB:          mockDB,
				BucketStore: mockBS,
			})
			require.NoError(t, err)

			result, err := svc.GetBucketTagging(context.Background(), tc.bucket)

			if tc.wantErr {
				assert.Error(t, err)
				var cfgErr *config.Error
				assert.True(t, errors.As(err, &cfgErr))
				assert.Equal(t, tc.wantErrCode, cfgErr.Code)
				return
			}

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

func TestSetBucketTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		tags        *s3types.TagSet
		setupMocks  func(*dbmocks.MockDB, *configmocks.MockBucketStore)
		wantErr     bool
		wantErrCode config.ErrorCode
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent",
			tags:   &s3types.TagSet{Tags: []s3types.Tag{{Key: "env", Value: "prod"}}},
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("nonexistent").Return(s3types.Bucket{}, false)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchBucket,
		},
		{
			name:   "invalid tag - empty key",
			bucket: "test-bucket",
			tags:   &s3types.TagSet{Tags: []s3types.Tag{{Key: "", Value: "value"}}},
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeInvalidTag,
		},
		{
			name:   "too many tags",
			bucket: "test-bucket",
			tags: &s3types.TagSet{Tags: func() []s3types.Tag {
				tags := make([]s3types.Tag, 51)
				for i := range tags {
					tags[i] = s3types.Tag{Key: "key" + string(rune('a'+i%26)), Value: "val"}
				}
				return tags
			}()},
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeInvalidTag,
		},
		{
			name:   "successful set",
			bucket: "test-bucket",
			tags:   &s3types.TagSet{Tags: []s3types.Tag{{Key: "env", Value: "prod"}}},
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
				mockDB.EXPECT().SetBucketTagging(mock.Anything, "test-bucket", mock.Anything).Return(nil)
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockBS := configmocks.NewMockBucketStore(t)

			tc.setupMocks(mockDB, mockBS)

			svc, err := config.NewService(config.Config{
				DB:          mockDB,
				BucketStore: mockBS,
			})
			require.NoError(t, err)

			err = svc.SetBucketTagging(context.Background(), tc.bucket, tc.tags)

			if tc.wantErr {
				assert.Error(t, err)
				var cfgErr *config.Error
				assert.True(t, errors.As(err, &cfgErr))
				assert.Equal(t, tc.wantErrCode, cfgErr.Code)
				return
			}

			assert.NoError(t, err)
		})
	}
}

func TestDeleteBucketTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		setupMocks  func(*dbmocks.MockDB, *configmocks.MockBucketStore)
		wantErr     bool
		wantErrCode config.ErrorCode
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("nonexistent").Return(s3types.Bucket{}, false)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchBucket,
		},
		{
			name:   "successful delete",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
				mockDB.EXPECT().DeleteBucketTagging(mock.Anything, "test-bucket").Return(nil)
			},
			wantErr: false,
		},
		{
			name:   "delete non-existent tagging is ok",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
				mockDB.EXPECT().DeleteBucketTagging(mock.Anything, "test-bucket").Return(db.ErrTaggingNotFound)
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockBS := configmocks.NewMockBucketStore(t)

			tc.setupMocks(mockDB, mockBS)

			svc, err := config.NewService(config.Config{
				DB:          mockDB,
				BucketStore: mockBS,
			})
			require.NoError(t, err)

			err = svc.DeleteBucketTagging(context.Background(), tc.bucket)

			if tc.wantErr {
				assert.Error(t, err)
				var cfgErr *config.Error
				assert.True(t, errors.As(err, &cfgErr))
				assert.Equal(t, tc.wantErrCode, cfgErr.Code)
				return
			}

			assert.NoError(t, err)
		})
	}
}

// ============================================================================
// Bucket ACL Tests
// ============================================================================

func TestGetBucketACL(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		setupMocks  func(*dbmocks.MockDB, *configmocks.MockBucketStore)
		wantErr     bool
		wantErrCode config.ErrorCode
		checkResult func(*testing.T, *s3types.AccessControlList)
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("nonexistent").Return(s3types.Bucket{}, false)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchBucket,
		},
		{
			name:   "successful get with ACL",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{
					Name:    "test-bucket",
					OwnerID: "owner-123",
					ACL: &s3types.AccessControlList{
						Owner: s3types.Owner{ID: "owner-123"},
						Grants: []s3types.Grant{
							{Permission: s3types.PermissionFullControl, Grantee: s3types.Grantee{ID: "owner-123", Type: s3types.GranteeTypeCanonicalUser}},
						},
					},
				}, true)
			},
			wantErr: false,
			checkResult: func(t *testing.T, acl *s3types.AccessControlList) {
				assert.Equal(t, "owner-123", acl.Owner.ID)
				assert.Len(t, acl.Grants, 1)
			},
		},
		{
			name:   "successful get with default ACL",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{
					Name:    "test-bucket",
					OwnerID: "owner-123",
					ACL:     nil, // No ACL set
				}, true)
			},
			wantErr: false,
			checkResult: func(t *testing.T, acl *s3types.AccessControlList) {
				assert.Equal(t, "owner-123", acl.Owner.ID)
				assert.Len(t, acl.Grants, 1)
				assert.Equal(t, s3types.PermissionFullControl, acl.Grants[0].Permission)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockBS := configmocks.NewMockBucketStore(t)

			tc.setupMocks(mockDB, mockBS)

			svc, err := config.NewService(config.Config{
				DB:          mockDB,
				BucketStore: mockBS,
			})
			require.NoError(t, err)

			result, err := svc.GetBucketACL(context.Background(), tc.bucket)

			if tc.wantErr {
				assert.Error(t, err)
				var cfgErr *config.Error
				assert.True(t, errors.As(err, &cfgErr))
				assert.Equal(t, tc.wantErrCode, cfgErr.Code)
				return
			}

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

// ============================================================================
// Bucket Policy Tests
// ============================================================================

func TestGetBucketPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		setupMocks  func(*dbmocks.MockDB, *configmocks.MockBucketStore)
		wantErr     bool
		wantErrCode config.ErrorCode
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				// GetBucketPolicy is called first, returns false
				mockBS.EXPECT().GetBucketPolicy(mock.Anything, "nonexistent").Return(nil, false)
				// Then GetBucket is called to check if bucket exists
				mockBS.EXPECT().GetBucket("nonexistent").Return(s3types.Bucket{}, false)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchBucket,
		},
		{
			name:   "no policy configured",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				// GetBucketPolicy is called first, returns false
				mockBS.EXPECT().GetBucketPolicy(mock.Anything, "test-bucket").Return(nil, false)
				// Then GetBucket is called to check if bucket exists
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchBucketPolicy,
		},
		{
			name:   "successful get",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				// GetBucketPolicy returns the policy, GetBucket is not called
				mockBS.EXPECT().GetBucketPolicy(mock.Anything, "test-bucket").Return(&s3types.BucketPolicy{Version: "2012-10-17"}, true)
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockBS := configmocks.NewMockBucketStore(t)

			tc.setupMocks(mockDB, mockBS)

			svc, err := config.NewService(config.Config{
				DB:          mockDB,
				BucketStore: mockBS,
			})
			require.NoError(t, err)

			_, err = svc.GetBucketPolicy(context.Background(), tc.bucket)

			if tc.wantErr {
				assert.Error(t, err)
				var cfgErr *config.Error
				assert.True(t, errors.As(err, &cfgErr))
				assert.Equal(t, tc.wantErrCode, cfgErr.Code)
				return
			}

			assert.NoError(t, err)
		})
	}
}

// ============================================================================
// Bucket Versioning Tests
// ============================================================================

func TestGetBucketVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		setupMocks  func(*dbmocks.MockDB, *configmocks.MockBucketStore)
		wantErr     bool
		wantErrCode config.ErrorCode
		checkResult func(*testing.T, *s3types.VersioningConfiguration)
	}{
		{
			name:   "bucket not found",
			bucket: "nonexistent",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("nonexistent").Return(s3types.Bucket{}, false)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchBucket,
		},
		{
			name:   "versioning enabled",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{
					Name:       "test-bucket",
					Versioning: s3types.VersioningEnabled,
				}, true)
			},
			wantErr: false,
			checkResult: func(t *testing.T, cfg *s3types.VersioningConfiguration) {
				assert.Equal(t, "Enabled", cfg.Status)
			},
		},
		{
			name:   "versioning suspended",
			bucket: "test-bucket",
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{
					Name:       "test-bucket",
					Versioning: s3types.VersioningSuspended,
				}, true)
			},
			wantErr: false,
			checkResult: func(t *testing.T, cfg *s3types.VersioningConfiguration) {
				assert.Equal(t, "Suspended", cfg.Status)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockBS := configmocks.NewMockBucketStore(t)

			tc.setupMocks(mockDB, mockBS)

			svc, err := config.NewService(config.Config{
				DB:          mockDB,
				BucketStore: mockBS,
			})
			require.NoError(t, err)

			result, err := svc.GetBucketVersioning(context.Background(), tc.bucket)

			if tc.wantErr {
				assert.Error(t, err)
				var cfgErr *config.Error
				assert.True(t, errors.As(err, &cfgErr))
				assert.Equal(t, tc.wantErrCode, cfgErr.Code)
				return
			}

			assert.NoError(t, err)
			if tc.checkResult != nil {
				tc.checkResult(t, result)
			}
		})
	}
}

func TestSetBucketVersioning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		bucket      string
		versioning  *s3types.VersioningConfiguration
		setupMocks  func(*dbmocks.MockDB, *configmocks.MockBucketStore)
		wantErr     bool
		wantErrCode config.ErrorCode
	}{
		{
			name:       "bucket not found",
			bucket:     "nonexistent",
			versioning: &s3types.VersioningConfiguration{Status: "Enabled"},
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("nonexistent").Return(s3types.Bucket{}, false)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeNoSuchBucket,
		},
		{
			name:       "invalid status",
			bucket:     "test-bucket",
			versioning: &s3types.VersioningConfiguration{Status: "Invalid"},
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
			},
			wantErr:     true,
			wantErrCode: config.ErrCodeMalformedXML,
		},
		{
			name:       "successful enable",
			bucket:     "test-bucket",
			versioning: &s3types.VersioningConfiguration{Status: "Enabled"},
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{Name: "test-bucket"}, true)
				mockDB.EXPECT().UpdateBucketVersioning(mock.Anything, "test-bucket", "Enabled").Return(nil)
				mockBS.EXPECT().SetBucket("test-bucket", mock.MatchedBy(func(b s3types.Bucket) bool {
					return b.Versioning == s3types.VersioningEnabled
				})).Return()
			},
			wantErr: false,
		},
		{
			name:       "successful suspend",
			bucket:     "test-bucket",
			versioning: &s3types.VersioningConfiguration{Status: "Suspended"},
			setupMocks: func(mockDB *dbmocks.MockDB, mockBS *configmocks.MockBucketStore) {
				mockBS.EXPECT().GetBucket("test-bucket").Return(s3types.Bucket{
					Name:       "test-bucket",
					Versioning: s3types.VersioningEnabled,
				}, true)
				mockDB.EXPECT().UpdateBucketVersioning(mock.Anything, "test-bucket", "Suspended").Return(nil)
				mockBS.EXPECT().SetBucket("test-bucket", mock.MatchedBy(func(b s3types.Bucket) bool {
					return b.Versioning == s3types.VersioningSuspended
				})).Return()
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			mockDB := dbmocks.NewMockDB(t)
			mockBS := configmocks.NewMockBucketStore(t)

			tc.setupMocks(mockDB, mockBS)

			svc, err := config.NewService(config.Config{
				DB:          mockDB,
				BucketStore: mockBS,
			})
			require.NoError(t, err)

			err = svc.SetBucketVersioning(context.Background(), tc.bucket, tc.versioning)

			if tc.wantErr {
				assert.Error(t, err)
				var cfgErr *config.Error
				assert.True(t, errors.As(err, &cfgErr))
				assert.Equal(t, tc.wantErrCode, cfgErr.Code)
				return
			}

			assert.NoError(t, err)
		})
	}
}

// ============================================================================
// Error Mapping Tests
// ============================================================================

func TestErrorToS3Error(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		errCode  config.ErrorCode
		expected string
	}{
		{"no such bucket", config.ErrCodeNoSuchBucket, "NoSuchBucket"},
		{"no such tag set", config.ErrCodeNoSuchTagSet, "NoSuchTagSet"},
		{"no such bucket policy", config.ErrCodeNoSuchBucketPolicy, "NoSuchBucketPolicy"},
		{"internal error", config.ErrCodeInternalError, "InternalError"},
		{"malformed xml", config.ErrCodeMalformedXML, "MalformedXML"},
		{"invalid tag", config.ErrCodeInvalidTag, "InvalidTag"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := &config.Error{Code: tc.errCode, Message: "test"}
			s3Err := err.ToS3Error()
			assert.Equal(t, tc.expected, s3Err.Code())
		})
	}
}
