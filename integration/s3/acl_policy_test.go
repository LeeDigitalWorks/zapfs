//go:build integration

// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package s3

import (
	"context"
	"io"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/integration/testutil"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBucketACL(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("get default bucket acl", func(t *testing.T) {
		bucket := uniqueBucket("test-acl-default")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		resp, err := rawClient.GetBucketAcl(ctx, &s3.GetBucketAclInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		assert.NotNil(t, resp.Owner, "should have owner")
		assert.NotEmpty(t, resp.Owner.ID, "owner should have ID")

		// Default ACL should have FULL_CONTROL for owner
		require.NotEmpty(t, resp.Grants, "should have at least one grant")
		var hasFullControl bool
		for _, grant := range resp.Grants {
			if grant.Permission == s3types.PermissionFullControl {
				hasFullControl = true
				break
			}
		}
		assert.True(t, hasFullControl, "owner should have FULL_CONTROL")
	})

	t.Run("put bucket acl - canned", func(t *testing.T) {
		bucket := uniqueBucket("test-acl-canned")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set public-read ACL
		_, err := rawClient.PutBucketAcl(ctx, &s3.PutBucketAclInput{
			Bucket: aws.String(bucket),
			ACL:    s3types.BucketCannedACLPublicRead,
		})
		require.NoError(t, err)

		// Verify the ACL
		resp, err := rawClient.GetBucketAcl(ctx, &s3.GetBucketAclInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		// Should have grants for public read
		var hasPublicRead bool
		for _, grant := range resp.Grants {
			if grant.Grantee != nil && grant.Grantee.URI != nil {
				if *grant.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" {
					if grant.Permission == s3types.PermissionRead {
						hasPublicRead = true
					}
				}
			}
		}
		assert.True(t, hasPublicRead, "should have public read grant")
	})
}

func TestObjectACL(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("get default object acl", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-acl-default")
		key := uniqueKey("test-object")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, []byte("test content"))
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		resp, err := rawClient.GetObjectAcl(ctx, &s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		assert.NotNil(t, resp.Owner, "should have owner")

		// Default ACL should have FULL_CONTROL for owner
		require.NotEmpty(t, resp.Grants, "should have at least one grant")
		var hasFullControl bool
		for _, grant := range resp.Grants {
			if grant.Permission == s3types.PermissionFullControl {
				hasFullControl = true
				break
			}
		}
		assert.True(t, hasFullControl, "owner should have FULL_CONTROL")
	})

	t.Run("put object acl - canned", func(t *testing.T) {
		bucket := uniqueBucket("test-obj-acl-canned")
		key := uniqueKey("test-object")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, []byte("test content"))
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set public-read ACL
		_, err := rawClient.PutObjectAcl(ctx, &s3.PutObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ACL:    s3types.ObjectCannedACLPublicRead,
		})
		require.NoError(t, err)

		// Verify the ACL
		resp, err := rawClient.GetObjectAcl(ctx, &s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Should have grants for public read
		var hasPublicRead bool
		for _, grant := range resp.Grants {
			if grant.Grantee != nil && grant.Grantee.URI != nil {
				if *grant.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" {
					if grant.Permission == s3types.PermissionRead {
						hasPublicRead = true
					}
				}
			}
		}
		assert.True(t, hasPublicRead, "should have public read grant")
	})
}

func TestBucketPolicy(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("put and get bucket policy", func(t *testing.T) {
		bucket := uniqueBucket("test-policy")
		key := uniqueKey("test-object")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, []byte("test content"))
		defer func() {
			client.DeleteObject(bucket, key)
			// Delete policy before deleting bucket
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set a simple policy
		policy := `{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Sid": "AllowPublicRead",
					"Effect": "Allow",
					"Principal": "*",
					"Action": "s3:GetObject",
					"Resource": "arn:aws:s3:::` + bucket + `/*"
				}
			]
		}`

		_, err := rawClient.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucket),
			Policy: aws.String(policy),
		})
		require.NoError(t, err)

		// Get the policy
		resp, err := rawClient.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		assert.NotEmpty(t, resp.Policy, "should have policy")
		assert.Contains(t, *resp.Policy, "AllowPublicRead", "policy should contain our statement")
	})

	t.Run("delete bucket policy", func(t *testing.T) {
		bucket := uniqueBucket("test-policy-delete")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set a policy
		policy := `{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Sid": "TestPolicy",
					"Effect": "Allow",
					"Principal": "*",
					"Action": "s3:ListBucket",
					"Resource": "arn:aws:s3:::` + bucket + `"
				}
			]
		}`

		_, err := rawClient.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucket),
			Policy: aws.String(policy),
		})
		require.NoError(t, err)

		// Delete the policy
		_, err = rawClient.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		// Verify policy is gone
		_, err = rawClient.GetBucketPolicy(ctx, &s3.GetBucketPolicyInput{
			Bucket: aws.String(bucket),
		})
		assert.Error(t, err, "should get error when policy doesn't exist")
	})

	t.Run("policy enforcement - public read", func(t *testing.T) {
		bucket := uniqueBucket("test-policy-enforce")
		key := uniqueKey("public-object")
		data := []byte("publicly readable content")
		client.CreateBucket(bucket)
		client.PutObject(bucket, key, data)
		defer func() {
			client.DeleteObject(bucket, key)
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketPolicy(ctx, &s3.DeleteBucketPolicyInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set a policy allowing public read
		policy := `{
			"Version": "2012-10-17",
			"Statement": [
				{
					"Sid": "PublicRead",
					"Effect": "Allow",
					"Principal": "*",
					"Action": "s3:GetObject",
					"Resource": "arn:aws:s3:::` + bucket + `/*"
				}
			]
		}`

		_, err := rawClient.PutBucketPolicy(ctx, &s3.PutBucketPolicyInput{
			Bucket: aws.String(bucket),
			Policy: aws.String(policy),
		})
		require.NoError(t, err)

		// Read the object using our client (should work)
		getResp, err := rawClient.GetObject(ctx, &s3.GetObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)
		defer getResp.Body.Close()

		body, err := io.ReadAll(getResp.Body)
		require.NoError(t, err)
		assert.Equal(t, data, body)
	})
}

func TestPublicAccessBlock(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("put and get public access block", func(t *testing.T) {
		bucket := uniqueBucket("test-pab")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeletePublicAccessBlock(ctx, &s3.DeletePublicAccessBlockInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set public access block
		_, err := rawClient.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
			Bucket: aws.String(bucket),
			PublicAccessBlockConfiguration: &s3types.PublicAccessBlockConfiguration{
				BlockPublicAcls:       aws.Bool(true),
				IgnorePublicAcls:      aws.Bool(true),
				BlockPublicPolicy:     aws.Bool(true),
				RestrictPublicBuckets: aws.Bool(true),
			},
		})
		require.NoError(t, err)

		// Get public access block
		resp, err := rawClient.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		assert.True(t, *resp.PublicAccessBlockConfiguration.BlockPublicAcls)
		assert.True(t, *resp.PublicAccessBlockConfiguration.IgnorePublicAcls)
		assert.True(t, *resp.PublicAccessBlockConfiguration.BlockPublicPolicy)
		assert.True(t, *resp.PublicAccessBlockConfiguration.RestrictPublicBuckets)
	})

	t.Run("delete public access block", func(t *testing.T) {
		bucket := uniqueBucket("test-pab-delete")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set public access block
		_, err := rawClient.PutPublicAccessBlock(ctx, &s3.PutPublicAccessBlockInput{
			Bucket: aws.String(bucket),
			PublicAccessBlockConfiguration: &s3types.PublicAccessBlockConfiguration{
				BlockPublicAcls: aws.Bool(true),
			},
		})
		require.NoError(t, err)

		// Delete it
		_, err = rawClient.DeletePublicAccessBlock(ctx, &s3.DeletePublicAccessBlockInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		// Should no longer exist
		_, err = rawClient.GetPublicAccessBlock(ctx, &s3.GetPublicAccessBlockInput{
			Bucket: aws.String(bucket),
		})
		assert.Error(t, err, "should get error when public access block doesn't exist")
	})
}

func TestOwnershipControls(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("put and get ownership controls", func(t *testing.T) {
		bucket := uniqueBucket("test-ownership")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set ownership controls
		_, err := rawClient.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
			OwnershipControls: &s3types.OwnershipControls{
				Rules: []s3types.OwnershipControlsRule{
					{
						ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced,
					},
				},
			},
		})
		require.NoError(t, err)

		// Get ownership controls
		resp, err := rawClient.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		require.Len(t, resp.OwnershipControls.Rules, 1)
		assert.Equal(t, s3types.ObjectOwnershipBucketOwnerEnforced, resp.OwnershipControls.Rules[0].ObjectOwnership)
	})

	t.Run("delete ownership controls", func(t *testing.T) {
		bucket := uniqueBucket("test-ownership-delete")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set ownership controls
		_, err := rawClient.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
			OwnershipControls: &s3types.OwnershipControls{
				Rules: []s3types.OwnershipControlsRule{
					{
						ObjectOwnership: s3types.ObjectOwnershipObjectWriter,
					},
				},
			},
		})
		require.NoError(t, err)

		// Delete ownership controls
		_, err = rawClient.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)

		// Should no longer exist
		_, err = rawClient.GetBucketOwnershipControls(ctx, &s3.GetBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
		})
		assert.Error(t, err, "should get error when ownership controls don't exist")
	})

	t.Run("BucketOwnerEnforced blocks PutBucketAcl", func(t *testing.T) {
		bucket := uniqueBucket("test-owner-enforced-bucket-acl")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set BucketOwnerEnforced ownership controls
		_, err := rawClient.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
			OwnershipControls: &s3types.OwnershipControls{
				Rules: []s3types.OwnershipControlsRule{
					{
						ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced,
					},
				},
			},
		})
		require.NoError(t, err)

		// Try to set bucket ACL - should fail with AccessControlListNotSupported
		_, err = rawClient.PutBucketAcl(ctx, &s3.PutBucketAclInput{
			Bucket: aws.String(bucket),
			ACL:    s3types.BucketCannedACLPublicRead,
		})
		require.Error(t, err, "PutBucketAcl should fail when BucketOwnerEnforced is set")
		assert.Contains(t, err.Error(), "AccessControlListNotSupported")
	})

	t.Run("BucketOwnerEnforced blocks PutObjectAcl", func(t *testing.T) {
		bucket := uniqueBucket("test-owner-enforced-obj-acl")
		key := uniqueKey("test-object")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// First create an object before setting ownership controls
		client.PutObject(bucket, key, []byte("test content"))

		// Set BucketOwnerEnforced ownership controls
		_, err := rawClient.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
			OwnershipControls: &s3types.OwnershipControls{
				Rules: []s3types.OwnershipControlsRule{
					{
						ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced,
					},
				},
			},
		})
		require.NoError(t, err)

		// Try to set object ACL - should fail with AccessControlListNotSupported
		_, err = rawClient.PutObjectAcl(ctx, &s3.PutObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ACL:    s3types.ObjectCannedACLPublicRead,
		})
		require.Error(t, err, "PutObjectAcl should fail when BucketOwnerEnforced is set")
		assert.Contains(t, err.Error(), "AccessControlListNotSupported")
	})

	t.Run("BucketOwnerEnforced allows bucket-owner-full-control on PutObject", func(t *testing.T) {
		bucket := uniqueBucket("test-owner-enforced-put")
		key := uniqueKey("test-object")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set BucketOwnerEnforced ownership controls
		_, err := rawClient.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
			OwnershipControls: &s3types.OwnershipControls{
				Rules: []s3types.OwnershipControlsRule{
					{
						ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced,
					},
				},
			},
		})
		require.NoError(t, err)

		// PutObject with bucket-owner-full-control should succeed
		_, err = rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   nil,
			ACL:    s3types.ObjectCannedACLBucketOwnerFullControl,
		})
		require.NoError(t, err, "PutObject with bucket-owner-full-control should succeed")

		// PutObject without ACL should also succeed
		key2 := uniqueKey("test-object-no-acl")
		defer client.DeleteObject(bucket, key2)
		_, err = rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key2),
			Body:   nil,
		})
		require.NoError(t, err, "PutObject without ACL should succeed")
	})

	t.Run("BucketOwnerEnforced rejects other ACLs on PutObject", func(t *testing.T) {
		bucket := uniqueBucket("test-owner-enforced-reject")
		key := uniqueKey("test-object")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set BucketOwnerEnforced ownership controls
		_, err := rawClient.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
			OwnershipControls: &s3types.OwnershipControls{
				Rules: []s3types.OwnershipControlsRule{
					{
						ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced,
					},
				},
			},
		})
		require.NoError(t, err)

		// PutObject with public-read ACL should fail
		_, err = rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   nil,
			ACL:    s3types.ObjectCannedACLPublicRead,
		})
		require.Error(t, err, "PutObject with public-read ACL should fail when BucketOwnerEnforced is set")
		assert.Contains(t, err.Error(), "AccessControlListNotSupported")
	})
}

func TestPutObjectWithACL(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("PutObject with canned ACL public-read", func(t *testing.T) {
		bucket := uniqueBucket("test-put-obj-acl")
		key := uniqueKey("public-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// PutObject with public-read ACL
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   nil,
			ACL:    s3types.ObjectCannedACLPublicRead,
		})
		require.NoError(t, err)

		// Verify the ACL was set
		resp, err := rawClient.GetObjectAcl(ctx, &s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Should have grants for public read
		var hasPublicRead bool
		for _, grant := range resp.Grants {
			if grant.Grantee != nil && grant.Grantee.URI != nil {
				if *grant.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" {
					if grant.Permission == s3types.PermissionRead {
						hasPublicRead = true
					}
				}
			}
		}
		assert.True(t, hasPublicRead, "object should have public read grant")
	})

	t.Run("PutObject with canned ACL private", func(t *testing.T) {
		bucket := uniqueBucket("test-put-obj-acl-private")
		key := uniqueKey("private-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// PutObject with private ACL
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			Body:   nil,
			ACL:    s3types.ObjectCannedACLPrivate,
		})
		require.NoError(t, err)

		// Verify the ACL - should only have owner FULL_CONTROL
		resp, err := rawClient.GetObjectAcl(ctx, &s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Should have only FULL_CONTROL for owner
		require.NotEmpty(t, resp.Grants)
		for _, grant := range resp.Grants {
			// Should not have any public grants
			if grant.Grantee != nil && grant.Grantee.URI != nil {
				assert.NotEqual(t, "http://acs.amazonaws.com/groups/global/AllUsers", *grant.Grantee.URI,
					"private ACL should not have public grants")
			}
		}
	})

	t.Run("PutObject with x-amz-grant-read header", func(t *testing.T) {
		bucket := uniqueBucket("test-put-obj-grant-read")
		key := uniqueKey("grant-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// PutObject with grant-read header for AllUsers group
		_, err := rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:    aws.String(bucket),
			Key:       aws.String(key),
			Body:      nil,
			GrantRead: aws.String("uri=\"http://acs.amazonaws.com/groups/global/AllUsers\""),
		})
		require.NoError(t, err)

		// Verify the ACL was set
		resp, err := rawClient.GetObjectAcl(ctx, &s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Should have grants for AllUsers read
		var hasAllUsersRead bool
		for _, grant := range resp.Grants {
			if grant.Grantee != nil && grant.Grantee.URI != nil {
				if *grant.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" {
					if grant.Permission == s3types.PermissionRead {
						hasAllUsersRead = true
					}
				}
			}
		}
		assert.True(t, hasAllUsersRead, "object should have AllUsers read grant")
	})

	t.Run("PutObject with x-amz-grant-full-control header", func(t *testing.T) {
		bucket := uniqueBucket("test-put-obj-grant-fc")
		key := uniqueKey("grant-fc-object")
		client.CreateBucket(bucket)
		defer func() {
			client.DeleteObject(bucket, key)
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Get the bucket owner ID first
		aclResp, err := rawClient.GetBucketAcl(ctx, &s3.GetBucketAclInput{
			Bucket: aws.String(bucket),
		})
		require.NoError(t, err)
		ownerID := *aclResp.Owner.ID

		// PutObject with grant-full-control header for a canonical user
		_, err = rawClient.PutObject(ctx, &s3.PutObjectInput{
			Bucket:           aws.String(bucket),
			Key:              aws.String(key),
			Body:             nil,
			GrantFullControl: aws.String("id=\"" + ownerID + "\""),
		})
		require.NoError(t, err)

		// Verify the ACL was set
		resp, err := rawClient.GetObjectAcl(ctx, &s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Should have FULL_CONTROL grant
		var hasFullControl bool
		for _, grant := range resp.Grants {
			if grant.Permission == s3types.PermissionFullControl {
				hasFullControl = true
				break
			}
		}
		assert.True(t, hasFullControl, "object should have FULL_CONTROL grant")
	})
}

func TestCreateMultipartUploadWithACL(t *testing.T) {
	t.Parallel()

	client := newS3Client(t)
	rawClient := client.Client

	t.Run("CreateMultipartUpload with canned ACL", func(t *testing.T) {
		bucket := uniqueBucket("test-mpu-acl")
		key := uniqueKey("mpu-object")
		client.CreateBucket(bucket)
		defer client.DeleteBucket(bucket)

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Create multipart upload with public-read ACL
		createResp, err := rawClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ACL:    s3types.ObjectCannedACLPublicRead,
		})
		require.NoError(t, err)
		uploadID := createResp.UploadId

		defer func() {
			// Abort the upload in case test fails before complete
			rawClient.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket:   aws.String(bucket),
				Key:      aws.String(key),
				UploadId: uploadID,
			})
			client.DeleteObject(bucket, key)
		}()

		// Upload a part
		partResp, err := rawClient.UploadPart(ctx, &s3.UploadPartInput{
			Bucket:     aws.String(bucket),
			Key:        aws.String(key),
			UploadId:   uploadID,
			PartNumber: aws.Int32(1),
			Body:       nil,
		})
		require.NoError(t, err)

		// Complete the multipart upload
		_, err = rawClient.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: uploadID,
			MultipartUpload: &s3types.CompletedMultipartUpload{
				Parts: []s3types.CompletedPart{
					{
						PartNumber: aws.Int32(1),
						ETag:       partResp.ETag,
					},
				},
			},
		})
		require.NoError(t, err)

		// Verify the ACL was applied to the completed object
		aclResp, err := rawClient.GetObjectAcl(ctx, &s3.GetObjectAclInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
		})
		require.NoError(t, err)

		// Should have grants for public read
		var hasPublicRead bool
		for _, grant := range aclResp.Grants {
			if grant.Grantee != nil && grant.Grantee.URI != nil {
				if *grant.Grantee.URI == "http://acs.amazonaws.com/groups/global/AllUsers" {
					if grant.Permission == s3types.PermissionRead {
						hasPublicRead = true
					}
				}
			}
		}
		assert.True(t, hasPublicRead, "completed multipart object should have public read grant")
	})

	t.Run("CreateMultipartUpload with BucketOwnerEnforced rejects ACL", func(t *testing.T) {
		bucket := uniqueBucket("test-mpu-owner-enforced")
		key := uniqueKey("mpu-object")
		client.CreateBucket(bucket)
		defer func() {
			ctx, cancel := testutil.WithTimeout(context.Background())
			defer cancel()
			rawClient.DeleteBucketOwnershipControls(ctx, &s3.DeleteBucketOwnershipControlsInput{
				Bucket: aws.String(bucket),
			})
			client.DeleteBucket(bucket)
		}()

		ctx, cancel := testutil.WithTimeout(context.Background())
		defer cancel()

		// Set BucketOwnerEnforced ownership controls
		_, err := rawClient.PutBucketOwnershipControls(ctx, &s3.PutBucketOwnershipControlsInput{
			Bucket: aws.String(bucket),
			OwnershipControls: &s3types.OwnershipControls{
				Rules: []s3types.OwnershipControlsRule{
					{
						ObjectOwnership: s3types.ObjectOwnershipBucketOwnerEnforced,
					},
				},
			},
		})
		require.NoError(t, err)

		// CreateMultipartUpload with public-read ACL should fail
		_, err = rawClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ACL:    s3types.ObjectCannedACLPublicRead,
		})
		require.Error(t, err, "CreateMultipartUpload with public-read ACL should fail when BucketOwnerEnforced")
		assert.Contains(t, err.Error(), "AccessControlListNotSupported")

		// CreateMultipartUpload with bucket-owner-full-control should succeed
		createResp, err := rawClient.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(key),
			ACL:    s3types.ObjectCannedACLBucketOwnerFullControl,
		})
		require.NoError(t, err, "CreateMultipartUpload with bucket-owner-full-control should succeed")

		// Cleanup - abort the upload
		rawClient.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
			Bucket:   aws.String(bucket),
			Key:      aws.String(key),
			UploadId: createResp.UploadId,
		})
	})
}
