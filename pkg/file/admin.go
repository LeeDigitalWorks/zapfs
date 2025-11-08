package file

import (
	"encoding/json"
	"net/http"
	"strings"

	"zapfs/pkg/debug"
	"zapfs/pkg/types"

	"github.com/google/uuid"
)

// RegisterAdminHandlers registers admin HTTP endpoints on the debug mux.
// These endpoints expose internal state for debugging and testing:
//   - GET /admin/chunks/{chunkID} - Get chunk info (RefCount, ZeroRefSince, etc.)
//   - GET /admin/index/stats - Overall index statistics
//   - GET /admin/ec-groups/{groupID} - Get EC group info
//   - POST /admin/gc/run - Force GC scan (query param: backend=<id>)
//
// Must be called before debug.GetMux() is invoked.
func (fs *FileServer) RegisterAdminHandlers() {
	debug.RegisterHandlerFunc("/admin/chunks/", fs.handleGetChunk)
	debug.RegisterHandlerFunc("/admin/index/stats", fs.handleIndexStats)
	debug.RegisterHandlerFunc("/admin/ec-groups/", fs.handleGetECGroup)
	debug.RegisterHandlerFunc("/admin/gc/run", fs.handleForceGC)
}

// handleGetChunk returns chunk metadata by ID
// GET /admin/chunks/{chunkID}
func (fs *FileServer) handleGetChunk(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract chunk ID from path: /admin/chunks/{chunkID}
	chunkID := strings.TrimPrefix(r.URL.Path, "/admin/chunks/")
	if chunkID == "" {
		http.Error(w, "chunk ID required", http.StatusBadRequest)
		return
	}

	chunk, err := fs.store.GetChunkInfo(types.ChunkID(chunkID))
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(chunk)
}

// handleIndexStats returns statistics about the chunk index
// GET /admin/index/stats
func (fs *FileServer) handleIndexStats(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	stats, err := fs.store.GetIndexStats()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(stats)
}

// handleGetECGroup returns EC group metadata by ID
// GET /admin/ec-groups/{groupID}
func (fs *FileServer) handleGetECGroup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract group ID from path: /admin/ec-groups/{groupID}
	groupIDStr := strings.TrimPrefix(r.URL.Path, "/admin/ec-groups/")
	if groupIDStr == "" {
		http.Error(w, "EC group ID required", http.StatusBadRequest)
		return
	}

	groupID, err := uuid.Parse(groupIDStr)
	if err != nil {
		http.Error(w, "invalid UUID: "+err.Error(), http.StatusBadRequest)
		return
	}

	group, err := fs.store.GetECGroup(groupID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(group)
}

// ForceGCResponse is returned by the force GC endpoint
type ForceGCResponse struct {
	Status     string `json:"status"`
	BackendID  string `json:"backend_id,omitempty"`
	WorkersRun int    `json:"workers_run"`
}

// handleForceGC triggers an immediate GC run
// POST /admin/gc/run?backend={backendID}
func (fs *FileServer) handleForceGC(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	backendID := r.URL.Query().Get("backend")
	workersRun := fs.store.ForceGC(backendID)

	resp := ForceGCResponse{
		Status:     "ok",
		BackendID:  backendID,
		WorkersRun: workersRun,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
