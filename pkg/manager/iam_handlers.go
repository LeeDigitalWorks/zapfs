package manager

import (
	"context"
	"embed"
	"encoding/json"
	"io/fs"
	"net/http"
	"os"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/iam_pb"
)

//go:embed static/*
var staticFiles embed.FS

// CredentialNotifier is called when credentials are created/updated/deleted
// to notify streaming subscribers (metadata services)
type CredentialNotifier interface {
	NotifyCredentialChange(eventType iam_pb.CredentialEvent_EventType, identity *iam.Identity, cred *iam.Credential)
}

// IAMAdminConfig holds configuration for the IAM admin handler
type IAMAdminConfig struct {
	// EnableAuth enables basic authentication for the admin UI/API
	EnableAuth bool
	// AdminUsername is the username for basic auth (default: admin)
	AdminUsername string
	// AdminPassword is the password for basic auth (required if EnableAuth is true)
	AdminPassword string
}

// IAMAdminHandler provides HTTP endpoints for IAM administration.
// The admin UI is served at /v1/iam/ui/ and the API at /v1/iam/*.
//
// Security considerations:
// - The admin port (8060) should not be exposed publicly
// - Use network policies to restrict access to internal IPs
// - Optionally enable basic auth with IAMAdminConfig
type IAMAdminHandler struct {
	iamService *iam.Service
	notifier   CredentialNotifier
	config     IAMAdminConfig
	mux        *http.ServeMux
}

// NewIAMAdminHandler creates a new IAM admin handler
func NewIAMAdminHandler(iamService *iam.Service) *IAMAdminHandler {
	h := &IAMAdminHandler{
		iamService: iamService,
		mux:        http.NewServeMux(),
		config: IAMAdminConfig{
			EnableAuth:    os.Getenv("ZAPFS_ADMIN_AUTH") == "true",
			AdminUsername: getEnvOrDefault("ZAPFS_ADMIN_USER", "admin"),
			AdminPassword: os.Getenv("ZAPFS_ADMIN_PASSWORD"),
		},
	}
	h.registerRoutes()
	return h
}

// NewIAMAdminHandlerWithConfig creates a new IAM admin handler with custom config
func NewIAMAdminHandlerWithConfig(iamService *iam.Service, config IAMAdminConfig) *IAMAdminHandler {
	h := &IAMAdminHandler{
		iamService: iamService,
		mux:        http.NewServeMux(),
		config:     config,
	}
	h.registerRoutes()
	return h
}

// SetNotifier sets the credential notifier for streaming updates
func (h *IAMAdminHandler) SetNotifier(n CredentialNotifier) {
	h.notifier = n
}

func (h *IAMAdminHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// Apply basic auth if enabled
	if h.config.EnableAuth && h.config.AdminPassword != "" {
		user, pass, ok := r.BasicAuth()
		if !ok || user != h.config.AdminUsername || pass != h.config.AdminPassword {
			w.Header().Set("WWW-Authenticate", `Basic realm="ZapFS Admin"`)
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
	}

	h.mux.ServeHTTP(w, r)
}

func (h *IAMAdminHandler) registerRoutes() {
	// Admin UI (static files)
	staticFS, err := fs.Sub(staticFiles, "static")
	if err != nil {
		logger.Error().Err(err).Msg("failed to create static file system")
	} else {
		h.mux.Handle("GET /v1/iam/ui/", http.StripPrefix("/v1/iam/ui/", http.FileServer(http.FS(staticFS))))
	}

	// Users
	h.mux.HandleFunc("POST /v1/iam/users", h.createUser)
	h.mux.HandleFunc("GET /v1/iam/users", h.listUsers)
	h.mux.HandleFunc("GET /v1/iam/users/{name}", h.getUser)
	h.mux.HandleFunc("DELETE /v1/iam/users/{name}", h.deleteUser)

	// Access Keys
	h.mux.HandleFunc("POST /v1/iam/users/{name}/keys", h.createAccessKey)
	h.mux.HandleFunc("GET /v1/iam/users/{name}/keys", h.listAccessKeys)
	h.mux.HandleFunc("DELETE /v1/iam/users/{name}/keys/{keyId}", h.deleteAccessKey)

	// Groups
	h.mux.HandleFunc("POST /v1/iam/groups", h.createGroup)
	h.mux.HandleFunc("GET /v1/iam/groups", h.listGroups)
	h.mux.HandleFunc("DELETE /v1/iam/groups/{name}", h.deleteGroup)
	h.mux.HandleFunc("POST /v1/iam/groups/{name}/members", h.addUserToGroup)
	h.mux.HandleFunc("DELETE /v1/iam/groups/{name}/members/{user}", h.removeUserFromGroup)

	// Status
	h.mux.HandleFunc("GET /v1/iam/status", h.getStatus)
}

func getEnvOrDefault(key, defaultValue string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultValue
}

// === Request/Response types ===

type createUserRequest struct {
	Name        string   `json:"name"`
	DisplayName string   `json:"display_name"`
	Email       string   `json:"email"`
	Policies    []string `json:"policies,omitempty"`
	Groups      []string `json:"groups,omitempty"`
}

type createUserResponse struct {
	Name      string `json:"name"`
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Message   string `json:"message"`
}

type userResponse struct {
	Name        string   `json:"name"`
	DisplayName string   `json:"display_name"`
	Email       string   `json:"email"`
	AccountID   string   `json:"account_id"`
	Disabled    bool     `json:"disabled"`
	AccessKeys  []string `json:"access_keys"`
}

type listUsersResponse struct {
	Users []string `json:"users"`
}

type createAccessKeyResponse struct {
	AccessKey string `json:"access_key"`
	SecretKey string `json:"secret_key"`
	Message   string `json:"message"`
}

type listAccessKeysResponse struct {
	AccessKeys []accessKeyInfo `json:"access_keys"`
}

type accessKeyInfo struct {
	AccessKey   string `json:"access_key"`
	Status      string `json:"status"`
	CreatedAt   string `json:"created_at"`
	Description string `json:"description,omitempty"`
}

type createGroupRequest struct {
	Name     string   `json:"name"`
	Policies []string `json:"policies,omitempty"`
}

type addUserToGroupRequest struct {
	Username string `json:"username"`
}

type statusResponse struct {
	CacheSize  int  `json:"cache_size"`
	STSEnabled bool `json:"sts_enabled"`
	KMSEnabled bool `json:"kms_enabled"`
	UserCount  int  `json:"user_count"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// === Handlers ===

func (h *IAMAdminHandler) createUser(w http.ResponseWriter, r *http.Request) {
	var req createUserRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.Name == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}

	// Generate credentials
	accessKey := iam.GenerateAccessKey()
	secretKey := iam.GenerateSecretKey()

	identity := &iam.Identity{
		Name: req.Name,
		Account: &iam.Account{
			ID:           iam.GenerateAccessKey()[:16], // Use as account ID
			DisplayName:  req.DisplayName,
			EmailAddress: req.Email,
		},
		Credentials: []*iam.Credential{{
			AccessKey:   accessKey,
			SecretKey:   secretKey,
			Status:      "Active",
			CreatedAt:   time.Now(),
			Description: "Created via admin API",
		}},
	}

	if identity.Account.DisplayName == "" {
		identity.Account.DisplayName = req.Name
	}

	ctx := r.Context()
	if err := h.iamService.CreateUser(ctx, identity); err != nil {
		if err == iam.ErrUserAlreadyExists {
			h.writeError(w, http.StatusConflict, "user_exists", "user already exists")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Attach policies if specified
	for _, policyName := range req.Policies {
		// For now, attach predefined policies
		switch policyName {
		case "FullAccess":
			h.iamService.AttachUserPolicy(req.Name, iam.FullAccessPolicy())
		case "ReadOnly":
			h.iamService.AttachUserPolicy(req.Name, iam.ReadOnlyPolicy())
		}
	}

	// Add to groups
	for _, groupName := range req.Groups {
		h.iamService.AddUserToGroup(req.Name, groupName)
	}

	logger.Info().Str("user", req.Name).Msg("IAM user created via admin API")

	// Notify subscribers (metadata services) of new credential
	if h.notifier != nil {
		h.notifier.NotifyCredentialChange(iam_pb.CredentialEvent_EVENT_TYPE_CREATED, identity, identity.Credentials[0])
	}

	h.writeJSON(w, http.StatusCreated, createUserResponse{
		Name:      req.Name,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Message:   "User created successfully. Save the secret key - it will not be shown again.",
	})
}

func (h *IAMAdminHandler) listUsers(w http.ResponseWriter, r *http.Request) {
	users, err := h.iamService.ListUsers(r.Context())
	if err != nil {
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	h.writeJSON(w, http.StatusOK, listUsersResponse{Users: users})
}

func (h *IAMAdminHandler) getUser(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "user name required")
		return
	}

	identity, err := h.iamService.GetUser(r.Context(), name)
	if err != nil {
		if err == iam.ErrUserNotFound {
			h.writeError(w, http.StatusNotFound, "not_found", "user not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Extract access keys (without secrets)
	var accessKeys []string
	for _, cred := range identity.Credentials {
		accessKeys = append(accessKeys, cred.AccessKey)
	}

	resp := userResponse{
		Name:       identity.Name,
		Disabled:   identity.Disabled,
		AccessKeys: accessKeys,
	}

	if identity.Account != nil {
		resp.DisplayName = identity.Account.DisplayName
		resp.Email = identity.Account.EmailAddress
		resp.AccountID = identity.Account.ID
	}

	h.writeJSON(w, http.StatusOK, resp)
}

func (h *IAMAdminHandler) deleteUser(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "user name required")
		return
	}

	// Get user first for notification
	identity, _ := h.iamService.GetUser(r.Context(), name)

	if err := h.iamService.DeleteUser(r.Context(), name); err != nil {
		if err == iam.ErrUserNotFound {
			h.writeError(w, http.StatusNotFound, "not_found", "user not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Notify subscribers of deletion
	if h.notifier != nil && identity != nil {
		for _, cred := range identity.Credentials {
			h.notifier.NotifyCredentialChange(iam_pb.CredentialEvent_EVENT_TYPE_DELETED, identity, cred)
		}
	}

	logger.Info().Str("user", name).Msg("IAM user deleted via admin API")
	w.WriteHeader(http.StatusNoContent)
}

func (h *IAMAdminHandler) createAccessKey(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "user name required")
		return
	}

	cred, err := h.iamService.CreateAccessKey(r.Context(), name)
	if err != nil {
		if err == iam.ErrUserNotFound {
			h.writeError(w, http.StatusNotFound, "not_found", "user not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	logger.Info().Str("user", name).Str("access_key", cred.AccessKey).Msg("Access key created via admin API")

	// Notify subscribers of new credential
	if h.notifier != nil {
		identity, _ := h.iamService.GetUser(r.Context(), name)
		if identity != nil {
			h.notifier.NotifyCredentialChange(iam_pb.CredentialEvent_EVENT_TYPE_CREATED, identity, cred)
		}
	}

	h.writeJSON(w, http.StatusCreated, createAccessKeyResponse{
		AccessKey: cred.AccessKey,
		SecretKey: cred.SecretKey,
		Message:   "Access key created. Save the secret key - it will not be shown again.",
	})
}

func (h *IAMAdminHandler) listAccessKeys(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "user name required")
		return
	}

	identity, err := h.iamService.GetUser(r.Context(), name)
	if err != nil {
		if err == iam.ErrUserNotFound {
			h.writeError(w, http.StatusNotFound, "not_found", "user not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	var keys []accessKeyInfo
	for _, cred := range identity.Credentials {
		keys = append(keys, accessKeyInfo{
			AccessKey:   cred.AccessKey,
			Status:      cred.Status,
			CreatedAt:   cred.CreatedAt.Format(time.RFC3339),
			Description: cred.Description,
		})
	}

	h.writeJSON(w, http.StatusOK, listAccessKeysResponse{AccessKeys: keys})
}

func (h *IAMAdminHandler) deleteAccessKey(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	keyID := r.PathValue("keyId")

	if name == "" || keyID == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "user name and key ID required")
		return
	}

	// Get identity before deletion for notification
	identity, _ := h.iamService.GetUser(r.Context(), name)

	if err := h.iamService.DeleteAccessKey(r.Context(), name, keyID); err != nil {
		if err == iam.ErrUserNotFound {
			h.writeError(w, http.StatusNotFound, "not_found", "user not found")
			return
		}
		if err == iam.ErrAccessKeyNotFound {
			h.writeError(w, http.StatusNotFound, "not_found", "access key not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	// Notify subscribers of deletion
	if h.notifier != nil && identity != nil {
		deletedCred := &iam.Credential{AccessKey: keyID}
		h.notifier.NotifyCredentialChange(iam_pb.CredentialEvent_EVENT_TYPE_DELETED, identity, deletedCred)
	}

	logger.Info().Str("user", name).Str("access_key", keyID).Msg("Access key deleted via admin API")
	w.WriteHeader(http.StatusNoContent)
}

func (h *IAMAdminHandler) createGroup(w http.ResponseWriter, r *http.Request) {
	var req createGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.Name == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}

	// Create group in policy store
	if mps, ok := h.iamService.PolicyStore().(*iam.MemoryPolicyStore); ok {
		mps.CreateGroup(req.Name)

		// Attach policies
		for _, policyName := range req.Policies {
			switch policyName {
			case "FullAccess":
				mps.AttachGroupPolicy(req.Name, iam.FullAccessPolicy())
			case "ReadOnly":
				mps.AttachGroupPolicy(req.Name, iam.ReadOnlyPolicy())
			}
		}
	}

	logger.Info().Str("group", req.Name).Msg("IAM group created via admin API")

	h.writeJSON(w, http.StatusCreated, map[string]string{
		"name":    req.Name,
		"message": "Group created successfully",
	})
}

func (h *IAMAdminHandler) listGroups(w http.ResponseWriter, r *http.Request) {
	var groups []string
	if mps, ok := h.iamService.PolicyStore().(*iam.MemoryPolicyStore); ok {
		groups = mps.ListGroups()
	}

	h.writeJSON(w, http.StatusOK, map[string][]string{"groups": groups})
}

func (h *IAMAdminHandler) deleteGroup(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	if name == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "group name required")
		return
	}

	if mps, ok := h.iamService.PolicyStore().(*iam.MemoryPolicyStore); ok {
		mps.DeleteGroup(name)
	}

	logger.Info().Str("group", name).Msg("IAM group deleted via admin API")
	w.WriteHeader(http.StatusNoContent)
}

func (h *IAMAdminHandler) addUserToGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("name")
	if groupName == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "group name required")
		return
	}

	var req addUserToGroupRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}

	if req.Username == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "username is required")
		return
	}

	// Verify user exists
	if _, err := h.iamService.GetUser(r.Context(), req.Username); err != nil {
		if err == iam.ErrUserNotFound {
			h.writeError(w, http.StatusNotFound, "not_found", "user not found")
			return
		}
		h.writeError(w, http.StatusInternalServerError, "internal_error", err.Error())
		return
	}

	h.iamService.AddUserToGroup(req.Username, groupName)

	logger.Info().Str("user", req.Username).Str("group", groupName).Msg("User added to group via admin API")

	h.writeJSON(w, http.StatusOK, map[string]string{
		"message": "User added to group",
	})
}

func (h *IAMAdminHandler) removeUserFromGroup(w http.ResponseWriter, r *http.Request) {
	groupName := r.PathValue("name")
	username := r.PathValue("user")

	if groupName == "" || username == "" {
		h.writeError(w, http.StatusBadRequest, "invalid_request", "group name and username required")
		return
	}

	h.iamService.RemoveUserFromGroup(username, groupName)

	logger.Info().Str("user", username).Str("group", groupName).Msg("User removed from group via admin API")
	w.WriteHeader(http.StatusNoContent)
}

func (h *IAMAdminHandler) getStatus(w http.ResponseWriter, r *http.Request) {
	users, _ := h.iamService.ListUsers(context.Background())

	resp := statusResponse{
		CacheSize:  h.iamService.Manager().CacheSize(),
		STSEnabled: h.iamService.STS() != nil,
		KMSEnabled: h.iamService.KMS() != nil,
		UserCount:  len(users),
	}

	h.writeJSON(w, http.StatusOK, resp)
}

// === Helpers ===

func (h *IAMAdminHandler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func (h *IAMAdminHandler) writeError(w http.ResponseWriter, status int, errType, message string) {
	h.writeJSON(w, status, errorResponse{
		Error:   errType,
		Message: message,
	})
}
