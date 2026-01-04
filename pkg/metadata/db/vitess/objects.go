// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package vitess

import (
	"context"

	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

// ============================================================================
// Object Operations
// ============================================================================

// PutObject creates or updates an object within a transaction.
// This method uses SELECT FOR UPDATE to serialize concurrent writes to the
// same bucket/key, preventing races where two concurrent calls both set
// is_latest=1.
func (v *Vitess) PutObject(ctx context.Context, obj *types.ObjectRef) error {
	return v.WithTx(ctx, func(tx db.TxStore) error {
		return tx.PutObject(ctx, obj)
	})
}

// Note: Other object operations (GetObject, GetObjectByID, DeleteObject,
// ListObjects, ListObjectsV2, ListDeletedObjects, MarkObjectDeleted,
// UpdateObjectTransition, UpdateRestoreStatus, UpdateRestoreExpiry,
// CompleteRestore, ResetRestoreStatus, UpdateLastAccessedAt,
// GetColdIntelligentTieringObjects, GetExpiredRestores) are inherited
// from the embedded *sql.Store.
