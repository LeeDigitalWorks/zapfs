// Copyright 2025 ZapFS, Inc. All rights reserved.
// Use of this source code is governed by the ZapFS Enterprise License
// that can be found in the LICENSE file.

//go:build !enterprise

package lifecycle

import (
	"context"
	"errors"
	"os"

	"github.com/LeeDigitalWorks/zapfs/pkg/storage/backend"
	"github.com/LeeDigitalWorks/zapfs/pkg/taskqueue"
	"github.com/LeeDigitalWorks/zapfs/pkg/types"
)

var (
	// ErrEnterpriseRequired indicates that the enterprise edition is required
	ErrEnterpriseRequired = errors.New("storage transitions require enterprise edition")

	// ErrLicenseRequired indicates that a valid enterprise license is required
	ErrLicenseRequired = ErrEnterpriseRequired

	// ErrNoBackendManager indicates the backend manager is not configured
	ErrNoBackendManager = ErrEnterpriseRequired

	// ErrNoProfiles indicates the profiles are not configured
	ErrNoProfiles = ErrEnterpriseRequired

	// ErrProfileNotFound indicates the target storage class profile was not found
	ErrProfileNotFound = ErrEnterpriseRequired

	// ErrObjectModified indicates the object was modified since evaluation
	ErrObjectModified = ErrEnterpriseRequired

	// ErrAlreadyTransitioned indicates the object is already at the target storage class
	ErrAlreadyTransitioned = ErrEnterpriseRequired
)

// TransitionDeps contains dependencies for transition execution (stub).
// In community edition, transitions are not supported.
type TransitionDeps struct {
	DB             interface{}
	FileClient     interface{}
	BackendManager *backend.Manager
	Profiles       *types.ProfileSet
	Pools          *types.PoolSet
}

// ExecuteTransition returns an error in community edition
func ExecuteTransition(_ context.Context, _ *TransitionDeps, _ taskqueue.LifecyclePayload) error {
	return ErrEnterpriseRequired
}

// Metrics stubs - no-op in community edition
type noopCounter struct{}
type noopHistogram struct{}

func (noopCounter) Inc()                {}
func (noopCounter) Add(_ float64)       {}
func (noopHistogram) Observe(_ float64) {}

type noopCounterVec struct{}
type noopHistogramVec struct{}

func (noopCounterVec) WithLabelValues(_ ...string) noopCounter     { return noopCounter{} }
func (noopHistogramVec) WithLabelValues(_ ...string) noopHistogram { return noopHistogram{} }

var (
	TransitionsTotal     = noopCounterVec{}
	TransitionBytesTotal = noopCounterVec{}
	TransitionDuration   = noopHistogramVec{}
	TierBackendErrors    = noopCounterVec{}
)

// init checks if lifecycle transitions are configured and warns user
func init() {
	if os.Getenv("ZAPFS_TIER_CONFIG") != "" {
		// Will be logged at startup
		_ = "storage tier configuration ignored in community edition"
	}
}
