// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package api

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/db"
	"github.com/LeeDigitalWorks/zapfs/pkg/s3api/s3types"
)

// FederationAdminHandler handles federation admin HTTP endpoints.
// These endpoints are for managing S3 federation (passthrough/migration).
type FederationAdminHandler struct {
	db db.DB
	// TODO: Add Manager client for gRPC calls when available for:
	// - Bucket mode changes (BucketMode is stored in Manager/Raft)
	// - External S3 bucket validation (HeadBucket, etc.)
}

// NewFederationAdminHandler creates a new federation admin handler.
func NewFederationAdminHandler(database db.DB) *FederationAdminHandler {
	return &FederationAdminHandler{
		db: database,
	}
}

// RegisterRoutes registers federation admin routes on the provided mux.
// All routes are under /admin/federation/
func (h *FederationAdminHandler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/admin/federation/buckets", h.handleBuckets)
	mux.HandleFunc("/admin/federation/buckets/", h.handleBucket)
}

// handleBuckets handles requests to /admin/federation/buckets
func (h *FederationAdminHandler) handleBuckets(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.listFederatedBuckets(w, r)
	case http.MethodPost:
		h.registerBucket(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleBucket handles requests to /admin/federation/buckets/{bucket}
func (h *FederationAdminHandler) handleBucket(w http.ResponseWriter, r *http.Request) {
	// Parse bucket name and sub-path from URL
	// /admin/federation/buckets/{bucket}
	// /admin/federation/buckets/{bucket}/mode
	// /admin/federation/buckets/{bucket}/pause
	// /admin/federation/buckets/{bucket}/resume
	// /admin/federation/buckets/{bucket}/dual-write
	// /admin/federation/buckets/{bucket}/credentials
	path := strings.TrimPrefix(r.URL.Path, "/admin/federation/buckets/")
	parts := strings.SplitN(path, "/", 2)
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, "bucket name required", http.StatusBadRequest)
		return
	}

	bucket := parts[0]
	subPath := ""
	if len(parts) > 1 {
		subPath = parts[1]
	}

	switch subPath {
	case "":
		// /admin/federation/buckets/{bucket}
		switch r.Method {
		case http.MethodGet:
			h.getBucketStatus(w, r, bucket)
		case http.MethodDelete:
			h.unregisterBucket(w, r, bucket)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "mode":
		if r.Method == http.MethodPut {
			h.setBucketMode(w, r, bucket)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "pause":
		if r.Method == http.MethodPost {
			h.pauseMigration(w, r, bucket)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "resume":
		if r.Method == http.MethodPost {
			h.resumeMigration(w, r, bucket)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "dual-write":
		if r.Method == http.MethodPut {
			h.setDualWrite(w, r, bucket)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	case "credentials":
		if r.Method == http.MethodPut {
			h.updateCredentials(w, r, bucket)
		} else {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

// RegisterBucketRequest is the request body for POST /admin/federation/buckets
type RegisterBucketRequest struct {
	LocalBucket          string           `json:"local_bucket"`
	Mode                 string           `json:"mode"` // "passthrough" or "migrating"
	External             ExternalS3Config `json:"external"`
	StartActiveMigration bool             `json:"start_active_migration,omitempty"`
}

// ExternalS3Config holds external S3 connection details.
type ExternalS3Config struct {
	Endpoint        string `json:"endpoint"`
	Region          string `json:"region"`
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
	Bucket          string `json:"bucket"`
	PathStyle       bool   `json:"path_style,omitempty"`
}

// RegisterBucketResponse is the response for POST /admin/federation/buckets
type RegisterBucketResponse struct {
	Success             bool   `json:"success"`
	Error               string `json:"error,omitempty"`
	ExternalObjectCount int64  `json:"external_object_count,omitempty"`
}

// BucketStatusResponse is the response for GET /admin/federation/buckets/{bucket}
type BucketStatusResponse struct {
	Bucket            string           `json:"bucket"`
	Mode              string           `json:"mode"` // "local", "passthrough", "migrating"
	DualWriteEnabled  bool             `json:"dual_write_enabled"`
	MigrationPaused   bool             `json:"migration_paused"`
	ObjectsDiscovered int64            `json:"objects_discovered"`
	ObjectsSynced     int64            `json:"objects_synced"`
	BytesSynced       int64            `json:"bytes_synced"`
	ProgressPercent   float64          `json:"progress_percent"`
	External          ExternalS3Config `json:"external"` // credentials redacted
}

// SetModeRequest is the request body for PUT /admin/federation/buckets/{bucket}/mode
type SetModeRequest struct {
	Mode                     string `json:"mode"` // "local", "passthrough", "migrating"
	DeleteExternalOnComplete bool   `json:"delete_external_on_complete,omitempty"`
}

// SetDualWriteRequest is the request body for PUT /admin/federation/buckets/{bucket}/dual-write
type SetDualWriteRequest struct {
	Enabled bool `json:"enabled"`
}

// UpdateCredentialsRequest is the request for PUT /admin/federation/buckets/{bucket}/credentials
type UpdateCredentialsRequest struct {
	AccessKeyID     string `json:"access_key_id"`
	SecretAccessKey string `json:"secret_access_key"`
}

// listFederatedBuckets lists all buckets with federation configs.
// GET /admin/federation/buckets
func (h *FederationAdminHandler) listFederatedBuckets(w http.ResponseWriter, r *http.Request) {
	configs, err := h.db.ListFederatedBuckets(r.Context())
	if err != nil {
		logger.Error().Err(err).Msg("failed to list federation configs")
		writeJSONError(w, "failed to list federation configs", http.StatusInternalServerError)
		return
	}

	// Build response with redacted credentials
	var response []BucketStatusResponse
	for _, cfg := range configs {
		response = append(response, BucketStatusResponse{
			Bucket:            cfg.Bucket,
			Mode:              getModeFromConfig(cfg),
			DualWriteEnabled:  cfg.DualWriteEnabled,
			MigrationPaused:   cfg.MigrationPaused,
			ObjectsDiscovered: cfg.ObjectsDiscovered,
			ObjectsSynced:     cfg.ObjectsSynced,
			BytesSynced:       cfg.BytesSynced,
			ProgressPercent:   cfg.ProgressPercent(),
			External: ExternalS3Config{
				Endpoint:        cfg.Endpoint,
				Region:          cfg.Region,
				Bucket:          cfg.ExternalBucket,
				PathStyle:       cfg.PathStyle,
				AccessKeyID:     redactCredential(cfg.AccessKeyID),
				SecretAccessKey: "***",
			},
		})
	}

	writeJSON(w, response, http.StatusOK)
}

// registerBucket registers a new federated bucket.
// POST /admin/federation/buckets
func (h *FederationAdminHandler) registerBucket(w http.ResponseWriter, r *http.Request) {
	var req RegisterBucketRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	// Validate required fields
	if req.LocalBucket == "" {
		writeJSONError(w, "local_bucket is required", http.StatusBadRequest)
		return
	}
	if req.Mode != "passthrough" && req.Mode != "migrating" {
		writeJSONError(w, "mode must be 'passthrough' or 'migrating'", http.StatusBadRequest)
		return
	}
	if req.External.Endpoint == "" || req.External.Bucket == "" {
		writeJSONError(w, "external.endpoint and external.bucket are required", http.StatusBadRequest)
		return
	}
	if req.External.AccessKeyID == "" || req.External.SecretAccessKey == "" {
		writeJSONError(w, "external.access_key_id and external.secret_access_key are required", http.StatusBadRequest)
		return
	}

	now := time.Now().UnixNano()

	// Create federation config
	// Note: BucketMode is stored in the bucket itself (via Manager/Raft), not in FederationConfig
	// The mode in the request is used to determine if we should start migration
	cfg := &s3types.FederationConfig{
		Bucket:          req.LocalBucket,
		Endpoint:        req.External.Endpoint,
		Region:          req.External.Region,
		AccessKeyID:     req.External.AccessKeyID,
		SecretAccessKey: req.External.SecretAccessKey,
		ExternalBucket:  req.External.Bucket,
		PathStyle:       req.External.PathStyle,
		CreatedAt:       now,
		UpdatedAt:       now,
	}

	// If migrating mode, set migration start time
	if req.Mode == "migrating" && req.StartActiveMigration {
		cfg.MigrationStartedAt = now
	}

	if err := h.db.SetFederationConfig(r.Context(), cfg); err != nil {
		logger.Error().Err(err).Str("bucket", req.LocalBucket).Msg("failed to create federation config")
		writeJSONError(w, "failed to register bucket: "+err.Error(), http.StatusInternalServerError)
		return
	}

	logger.Info().
		Str("bucket", req.LocalBucket).
		Str("mode", req.Mode).
		Str("external_bucket", req.External.Bucket).
		Str("endpoint", req.External.Endpoint).
		Msg("federation bucket registered")

	writeJSON(w, RegisterBucketResponse{Success: true}, http.StatusCreated)
}

// getBucketStatus gets the federation status for a bucket.
// GET /admin/federation/buckets/{bucket}
func (h *FederationAdminHandler) getBucketStatus(w http.ResponseWriter, r *http.Request, bucket string) {
	cfg, err := h.db.GetFederationConfig(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, db.ErrFederationNotFound) {
			writeJSONError(w, "bucket not federated", http.StatusNotFound)
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to get federation config")
		writeJSONError(w, "failed to get federation status", http.StatusInternalServerError)
		return
	}

	response := BucketStatusResponse{
		Bucket:            cfg.Bucket,
		Mode:              getModeFromConfig(cfg),
		DualWriteEnabled:  cfg.DualWriteEnabled,
		MigrationPaused:   cfg.MigrationPaused,
		ObjectsDiscovered: cfg.ObjectsDiscovered,
		ObjectsSynced:     cfg.ObjectsSynced,
		BytesSynced:       cfg.BytesSynced,
		ProgressPercent:   cfg.ProgressPercent(),
		External: ExternalS3Config{
			Endpoint:        cfg.Endpoint,
			Region:          cfg.Region,
			Bucket:          cfg.ExternalBucket,
			PathStyle:       cfg.PathStyle,
			AccessKeyID:     redactCredential(cfg.AccessKeyID),
			SecretAccessKey: "***",
		},
	}

	writeJSON(w, response, http.StatusOK)
}

// unregisterBucket removes federation config for a bucket.
// DELETE /admin/federation/buckets/{bucket}
func (h *FederationAdminHandler) unregisterBucket(w http.ResponseWriter, r *http.Request, bucket string) {
	// Query params for optional cleanup
	deleteExternal := r.URL.Query().Get("delete_external") == "true"
	deleteLocalData := r.URL.Query().Get("delete_local_data") == "true"

	if deleteExternal || deleteLocalData {
		// TODO: Implement cleanup when Manager gRPC is available
		logger.Warn().
			Str("bucket", bucket).
			Bool("delete_external", deleteExternal).
			Bool("delete_local_data", deleteLocalData).
			Msg("cleanup options not yet implemented")
	}

	if err := h.db.DeleteFederationConfig(r.Context(), bucket); err != nil {
		if errors.Is(err, db.ErrFederationNotFound) {
			writeJSONError(w, "bucket not federated", http.StatusNotFound)
			return
		}
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to delete federation config")
		writeJSONError(w, "failed to unregister bucket", http.StatusInternalServerError)
		return
	}

	logger.Info().Str("bucket", bucket).Msg("federation bucket unregistered")
	w.WriteHeader(http.StatusNoContent)
}

// setBucketMode changes the mode of a federated bucket.
// PUT /admin/federation/buckets/{bucket}/mode
// Note: This updates the bucket mode which is managed via Manager/Raft.
// For now, we track the mode transition via migration state in FederationConfig.
func (h *FederationAdminHandler) setBucketMode(w http.ResponseWriter, r *http.Request, bucket string) {
	var req SetModeRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.Mode != "local" && req.Mode != "passthrough" && req.Mode != "migrating" {
		writeJSONError(w, "mode must be 'local', 'passthrough', or 'migrating'", http.StatusBadRequest)
		return
	}

	cfg, err := h.db.GetFederationConfig(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, db.ErrFederationNotFound) {
			writeJSONError(w, "bucket not federated", http.StatusNotFound)
			return
		}
		writeJSONError(w, "failed to get federation config", http.StatusInternalServerError)
		return
	}

	// Update migration state based on mode transition
	now := time.Now().UnixNano()
	switch req.Mode {
	case "migrating":
		if cfg.MigrationStartedAt == 0 {
			cfg.MigrationStartedAt = now
		}
		cfg.MigrationPaused = false
	case "local":
		// Migration complete - could clean up external if requested
		// TODO: Call Manager gRPC to set BucketMode to None
		logger.Info().Str("bucket", bucket).Msg("transitioning to local mode (migration complete)")
	case "passthrough":
		// Reset migration state
		cfg.MigrationStartedAt = 0
		cfg.ObjectsDiscovered = 0
		cfg.ObjectsSynced = 0
		cfg.BytesSynced = 0
	}
	cfg.UpdatedAt = now

	if err := h.db.SetFederationConfig(r.Context(), cfg); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to update bucket mode")
		writeJSONError(w, "failed to update bucket mode", http.StatusInternalServerError)
		return
	}

	logger.Info().Str("bucket", bucket).Str("mode", req.Mode).Msg("federation bucket mode updated")
	writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

// pauseMigration pauses migration for a bucket.
// POST /admin/federation/buckets/{bucket}/pause
func (h *FederationAdminHandler) pauseMigration(w http.ResponseWriter, r *http.Request, bucket string) {
	cfg, err := h.db.GetFederationConfig(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, db.ErrFederationNotFound) {
			writeJSONError(w, "bucket not federated", http.StatusNotFound)
			return
		}
		writeJSONError(w, "failed to get federation config", http.StatusInternalServerError)
		return
	}

	cfg.MigrationPaused = true
	cfg.UpdatedAt = time.Now().UnixNano()

	if err := h.db.SetFederationConfig(r.Context(), cfg); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to pause migration")
		writeJSONError(w, "failed to pause migration", http.StatusInternalServerError)
		return
	}

	logger.Info().Str("bucket", bucket).Msg("federation migration paused")
	writeJSON(w, map[string]string{"status": "paused"}, http.StatusOK)
}

// resumeMigration resumes migration for a bucket.
// POST /admin/federation/buckets/{bucket}/resume
func (h *FederationAdminHandler) resumeMigration(w http.ResponseWriter, r *http.Request, bucket string) {
	cfg, err := h.db.GetFederationConfig(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, db.ErrFederationNotFound) {
			writeJSONError(w, "bucket not federated", http.StatusNotFound)
			return
		}
		writeJSONError(w, "failed to get federation config", http.StatusInternalServerError)
		return
	}

	cfg.MigrationPaused = false
	cfg.UpdatedAt = time.Now().UnixNano()

	if err := h.db.SetFederationConfig(r.Context(), cfg); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to resume migration")
		writeJSONError(w, "failed to resume migration", http.StatusInternalServerError)
		return
	}

	logger.Info().Str("bucket", bucket).Msg("federation migration resumed")
	writeJSON(w, map[string]string{"status": "resumed"}, http.StatusOK)
}

// setDualWrite enables or disables dual-write for a bucket.
// PUT /admin/federation/buckets/{bucket}/dual-write
func (h *FederationAdminHandler) setDualWrite(w http.ResponseWriter, r *http.Request, bucket string) {
	var req SetDualWriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	cfg, err := h.db.GetFederationConfig(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, db.ErrFederationNotFound) {
			writeJSONError(w, "bucket not federated", http.StatusNotFound)
			return
		}
		writeJSONError(w, "failed to get federation config", http.StatusInternalServerError)
		return
	}

	cfg.DualWriteEnabled = req.Enabled
	cfg.UpdatedAt = time.Now().UnixNano()

	if err := h.db.SetFederationConfig(r.Context(), cfg); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to update dual-write setting")
		writeJSONError(w, "failed to update dual-write setting", http.StatusInternalServerError)
		return
	}

	logger.Info().Str("bucket", bucket).Bool("enabled", req.Enabled).Msg("federation dual-write updated")
	writeJSON(w, map[string]any{"status": "ok", "dual_write_enabled": req.Enabled}, http.StatusOK)
}

// updateCredentials updates the external S3 credentials for a bucket.
// PUT /admin/federation/buckets/{bucket}/credentials
func (h *FederationAdminHandler) updateCredentials(w http.ResponseWriter, r *http.Request, bucket string) {
	var req UpdateCredentialsRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, "invalid request body", http.StatusBadRequest)
		return
	}

	if req.AccessKeyID == "" || req.SecretAccessKey == "" {
		writeJSONError(w, "access_key_id and secret_access_key are required", http.StatusBadRequest)
		return
	}

	cfg, err := h.db.GetFederationConfig(r.Context(), bucket)
	if err != nil {
		if errors.Is(err, db.ErrFederationNotFound) {
			writeJSONError(w, "bucket not federated", http.StatusNotFound)
			return
		}
		writeJSONError(w, "failed to get federation config", http.StatusInternalServerError)
		return
	}

	cfg.AccessKeyID = req.AccessKeyID
	cfg.SecretAccessKey = req.SecretAccessKey
	cfg.UpdatedAt = time.Now().UnixNano()

	if err := h.db.SetFederationConfig(r.Context(), cfg); err != nil {
		logger.Error().Err(err).Str("bucket", bucket).Msg("failed to update credentials")
		writeJSONError(w, "failed to update credentials", http.StatusInternalServerError)
		return
	}

	logger.Info().Str("bucket", bucket).Msg("federation credentials updated")
	writeJSON(w, map[string]string{"status": "ok"}, http.StatusOK)
}

// Helper functions

// getModeFromConfig determines the mode string based on federation config state.
// Since BucketMode is stored in the bucket (via Manager/Raft), we infer the mode
// from the federation config state.
func getModeFromConfig(cfg *s3types.FederationConfig) string {
	if cfg.MigrationStartedAt > 0 {
		return "migrating"
	}
	// If we have a federation config, we're in passthrough mode
	return "passthrough"
}

func redactCredential(cred string) string {
	if len(cred) <= 8 {
		return "***"
	}
	return cred[:4] + "***" + cred[len(cred)-4:]
}

func writeJSON(w http.ResponseWriter, data any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeJSONError(w http.ResponseWriter, message string, status int) {
	writeJSON(w, map[string]string{"error": message}, status)
}
