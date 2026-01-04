// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package postgres

// Note: All config operations are inherited from the embedded *sql.Store:
// - ACL: GetBucketACL, SetBucketACL, GetObjectACL, SetObjectACL
// - Policy: GetBucketPolicy, SetBucketPolicy, DeleteBucketPolicy
// - CORS: GetBucketCORS, SetBucketCORS, DeleteBucketCORS
// - Website: GetBucketWebsite, SetBucketWebsite, DeleteBucketWebsite
// - Tagging: GetBucketTagging, SetBucketTagging, DeleteBucketTagging,
//            GetObjectTagging, SetObjectTagging, DeleteObjectTagging
// - Encryption: GetBucketEncryption, SetBucketEncryption, DeleteBucketEncryption
// - Lifecycle: GetBucketLifecycle, SetBucketLifecycle, DeleteBucketLifecycle
// - Replication: GetReplicationConfiguration, SetReplicationConfiguration, DeleteReplicationConfiguration
// - Intelligent Tiering: GetIntelligentTieringConfiguration, PutIntelligentTieringConfiguration,
//                        DeleteIntelligentTieringConfiguration, ListIntelligentTieringConfigurations
