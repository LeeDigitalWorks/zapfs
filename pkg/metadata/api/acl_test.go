// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"bytes"
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetBucketAclHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		createBucket bool
		setupACL     bool
		expectedCode int
		verifyOwner  string
	}{
		{
			name:         "bucket not found",
			bucket:       "nonexistent-bucket",
			createBucket: false,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "bucket with ACL",
			bucket:       "test-bucket",
			createBucket: true,
			setupACL:     true,
			expectedCode: http.StatusOK,
			verifyOwner:  "test-owner",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			if tc.createBucket {
				err := srv.db.CreateBucket(ctx, &types.BucketInfo{
					Name:    tc.bucket,
					OwnerID: "test-owner",
				})
				require.NoError(t, err)

				// Add bucket to cache (handlers check cache first)
				srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
					Name:    tc.bucket,
					OwnerID: "test-owner",
				})
			}

			if tc.setupACL {
				acl := &s3types.AccessControlList{
					Owner: s3types.Owner{
						ID:          "test-owner",
						DisplayName: "Test User",
					},
					Grants: []s3types.Grant{
						{
							Grantee: s3types.Grantee{
								ID:          "test-owner",
								DisplayName: "Test User",
								Type:        "CanonicalUser",
							},
							Permission: "FULL_CONTROL",
						},
					},
				}
				err := srv.db.SetBucketACL(ctx, tc.bucket, acl)
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, "", "test-owner")
			w := httptest.NewRecorder()

			srv.GetBucketAclHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.expectedCode == http.StatusOK {
				if w.Body.Len() > 0 {
					var result s3types.AccessControlPolicy
					err := xml.Unmarshal(w.Body.Bytes(), &result)
					require.NoError(t, err)
					assert.Equal(t, tc.verifyOwner, result.Owner.ID)
					assert.Len(t, result.AccessControlList.Grants, 1)
				}
			}
		})
	}
}

func TestPutBucketAclHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		ownerID      string
		expectedCode int
		verifyStored bool
	}{
		{
			name:         "successfully set bucket ACL",
			bucket:       "test-bucket",
			ownerID:      "test-owner",
			expectedCode: http.StatusOK,
			verifyStored: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: tc.ownerID,
			})
			require.NoError(t, err)

			// Add bucket to cache (handlers check cache first)
			srv.bucketStore.SetBucket(tc.bucket, s3types.Bucket{
				Name:    tc.bucket,
				OwnerID: tc.ownerID,
			})

			// Create ACL request body
			aclPolicy := s3types.AccessControlPolicy{
				Owner: s3types.Owner{
					ID:          tc.ownerID,
					DisplayName: "Test User",
				},
				AccessControlList: s3types.AccessControlListXML{
					Grants: []s3types.GrantXML{
						{
							Grantee: s3types.GranteeXML{
								ID:          tc.ownerID,
								DisplayName: "Test User",
								XsiType:     "CanonicalUser",
							},
							Permission: "FULL_CONTROL",
						},
					},
				},
			}
			body, _ := xml.Marshal(aclPolicy)

			d := createTestData(tc.bucket, "", tc.ownerID)
			d.Req = httptest.NewRequest("PUT", "/"+tc.bucket+"?acl", bytes.NewReader(body))
			w := httptest.NewRecorder()

			srv.PutBucketAclHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.verifyStored {
				storedACL, err := srv.db.GetBucketACL(ctx, tc.bucket)
				require.NoError(t, err)
				assert.Equal(t, tc.ownerID, storedACL.Owner.ID)
			}
		})
	}
}

func TestGetObjectAclHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		key          string
		createObject bool
		setupACL     bool
		expectedCode int
		verifyOwner  string
	}{
		{
			name:         "object not found",
			bucket:       "test-bucket",
			key:          "nonexistent.txt",
			createObject: false,
			expectedCode: http.StatusNotFound,
		},
		{
			name:         "object with ACL",
			bucket:       "test-bucket",
			key:          "test.txt",
			createObject: true,
			setupACL:     true,
			expectedCode: http.StatusOK,
			verifyOwner:  "test-owner",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: "test-owner",
			})
			require.NoError(t, err)

			if tc.createObject {
				err = srv.db.PutObject(ctx, &types.ObjectRef{
					Bucket: tc.bucket,
					Key:    tc.key,
					Size:   100,
					ETag:   "abc123",
				})
				require.NoError(t, err)
			}

			if tc.setupACL {
				acl := &s3types.AccessControlList{
					Owner: s3types.Owner{
						ID:          "test-owner",
						DisplayName: "Test User",
					},
					Grants: []s3types.Grant{
						{
							Grantee: s3types.Grantee{
								ID:          "test-owner",
								DisplayName: "Test User",
								Type:        "CanonicalUser",
							},
							Permission: "FULL_CONTROL",
						},
					},
				}
				err = srv.db.SetObjectACL(ctx, tc.bucket, tc.key, acl)
				require.NoError(t, err)
			}

			d := createTestData(tc.bucket, tc.key, "test-owner")
			w := httptest.NewRecorder()

			srv.GetObjectAclHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.expectedCode == http.StatusOK {
				var result s3types.AccessControlPolicy
				err := xml.Unmarshal(w.Body.Bytes(), &result)
				require.NoError(t, err)
				assert.Equal(t, tc.verifyOwner, result.Owner.ID)
			}
		})
	}
}

func TestPutObjectAclHandler(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		bucket       string
		key          string
		ownerID      string
		expectedCode int
		verifyStored bool
	}{
		{
			name:         "successfully set object ACL",
			bucket:       "test-bucket",
			key:          "test.txt",
			ownerID:      "test-owner",
			expectedCode: http.StatusOK,
			verifyStored: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			srv := newTestServer(t)
			ctx := context.Background()

			// Create bucket
			err := srv.db.CreateBucket(ctx, &types.BucketInfo{
				Name:    tc.bucket,
				OwnerID: tc.ownerID,
			})
			require.NoError(t, err)

			// Create object
			err = srv.db.PutObject(ctx, &types.ObjectRef{
				Bucket: tc.bucket,
				Key:    tc.key,
				Size:   100,
				ETag:   "abc123",
			})
			require.NoError(t, err)

			// Create ACL request body
			aclPolicy := s3types.AccessControlPolicy{
				Owner: s3types.Owner{
					ID:          tc.ownerID,
					DisplayName: "Test User",
				},
				AccessControlList: s3types.AccessControlListXML{
					Grants: []s3types.GrantXML{
						{
							Grantee: s3types.GranteeXML{
								ID:          tc.ownerID,
								DisplayName: "Test User",
								XsiType:     "CanonicalUser",
							},
							Permission: "FULL_CONTROL",
						},
					},
				},
			}
			body, _ := xml.Marshal(aclPolicy)

			d := createTestData(tc.bucket, tc.key, tc.ownerID)
			d.Req = httptest.NewRequest("PUT", "/"+tc.bucket+"/"+tc.key+"?acl", bytes.NewReader(body))
			w := httptest.NewRecorder()

			srv.PutObjectAclHandler(d, w)

			assert.Equal(t, tc.expectedCode, w.Code)

			if tc.verifyStored {
				storedACL, err := srv.db.GetObjectACL(ctx, tc.bucket, tc.key)
				require.NoError(t, err)
				assert.Equal(t, tc.ownerID, storedACL.Owner.ID)
			}
		})
	}
}
