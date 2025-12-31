package file

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/LeeDigitalWorks/zapfs/pkg/types"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// mockFileStore implements the methods needed by FileServer handlers
type mockFileStore struct {
	mock.Mock
}

func (m *mockFileStore) PutObject(ctx context.Context, obj *types.ObjectRef, reader io.Reader) error {
	args := m.Called(ctx, obj, reader)
	return args.Error(0)
}

func (m *mockFileStore) DeleteObject(ctx context.Context, id uuid.UUID) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *mockFileStore) GetObjectData(ctx context.Context, id uuid.UUID) (io.ReadCloser, error) {
	args := m.Called(ctx, id)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(io.ReadCloser), args.Error(1)
}

func (m *mockFileStore) Close() error {
	args := m.Called()
	return args.Error(0)
}

// testFileServer creates a FileServer with a mock store for testing
type testFileServer struct {
	*mockFileStore
}

func (t *testFileServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPost:
		t.PostHandler(w, r)
	case http.MethodDelete:
		t.DeleteHandler(w, r)
	case http.MethodGet:
		t.GetHandler(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (t *testFileServer) PostHandler(w http.ResponseWriter, r *http.Request) {
	objectID, err := parseURLPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	obj := &types.ObjectRef{ID: objectID}
	err = t.PutObject(r.Context(), obj, r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func (t *testFileServer) DeleteHandler(w http.ResponseWriter, r *http.Request) {
	objectID, err := parseURLPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = t.DeleteObject(r.Context(), objectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func (t *testFileServer) GetHandler(w http.ResponseWriter, r *http.Request) {
	objectID, err := parseURLPath(r.URL.Path)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	reader, err := t.GetObjectData(r.Context(), objectID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer reader.Close()

	w.WriteHeader(http.StatusOK)
	io.Copy(w, reader)
}

func newTestFileServer(t *testing.T) *testFileServer {
	m := &mockFileStore{}
	t.Cleanup(func() {
		m.AssertExpectations(t)
	})
	return &testFileServer{mockFileStore: m}
}

// ============================================================================
// parseURLPath Tests
// ============================================================================

func TestParseURLPath_ValidUUID(t *testing.T) {
	t.Parallel()

	testID := uuid.New()
	path := "/" + testID.String()

	result, err := parseURLPath(path)
	require.NoError(t, err)
	assert.Equal(t, testID, result)
}

func TestParseURLPath_ValidUUID_NoLeadingSlash(t *testing.T) {
	t.Parallel()

	testID := uuid.New()
	path := testID.String()

	result, err := parseURLPath(path)
	require.NoError(t, err)
	assert.Equal(t, testID, result)
}

func TestParseURLPath_ValidUUID_TrailingSlash(t *testing.T) {
	t.Parallel()

	testID := uuid.New()
	path := "/" + testID.String() + "/"

	result, err := parseURLPath(path)
	require.NoError(t, err)
	assert.Equal(t, testID, result)
}

func TestParseURLPath_InvalidUUID(t *testing.T) {
	t.Parallel()

	path := "/not-a-uuid"

	_, err := parseURLPath(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid object ID")
}

func TestParseURLPath_EmptyPath(t *testing.T) {
	t.Parallel()

	path := "/"

	_, err := parseURLPath(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid object ID")
}

func TestParseURLPath_TooManySegments(t *testing.T) {
	t.Parallel()

	testID := uuid.New()
	path := "/extra/" + testID.String()

	_, err := parseURLPath(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid path")
}

func TestParseURLPath_PathTraversal(t *testing.T) {
	t.Parallel()

	// Path traversal attempts should fail UUID parsing
	paths := []string{
		"/../etc/passwd",
		"/..%2F..%2Fetc/passwd",
		"/..",
	}

	for _, path := range paths {
		_, err := parseURLPath(path)
		require.Error(t, err, "path %s should fail", path)
	}
}

// ============================================================================
// ServeHTTP Method Routing Tests
// ============================================================================

func TestServeHTTP_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)

	methods := []string{
		http.MethodPut,
		http.MethodPatch,
		http.MethodHead,
		http.MethodOptions,
	}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/"+uuid.New().String(), nil)
			rec := httptest.NewRecorder()

			fs.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
			assert.Contains(t, rec.Body.String(), "method not allowed")
		})
	}
}

// ============================================================================
// POST Handler Tests
// ============================================================================

func TestPostHandler_Success(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()
	testData := []byte("test object data")

	fs.On("PutObject", mock.Anything, mock.MatchedBy(func(obj *types.ObjectRef) bool {
		return obj.ID == testID
	}), mock.Anything).Return(nil)

	req := httptest.NewRequest(http.MethodPost, "/"+testID.String(), bytes.NewReader(testData))
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestPostHandler_InvalidPath(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)

	req := httptest.NewRequest(http.MethodPost, "/invalid-uuid", bytes.NewReader([]byte("data")))
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid object ID")
}

func TestPostHandler_StoreError(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	fs.On("PutObject", mock.Anything, mock.Anything, mock.Anything).
		Return(errors.New("storage error"))

	req := httptest.NewRequest(http.MethodPost, "/"+testID.String(), bytes.NewReader([]byte("data")))
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "storage error")
}

func TestPostHandler_EmptyBody(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	fs.On("PutObject", mock.Anything, mock.MatchedBy(func(obj *types.ObjectRef) bool {
		return obj.ID == testID
	}), mock.Anything).Return(nil)

	req := httptest.NewRequest(http.MethodPost, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
}

// ============================================================================
// DELETE Handler Tests
// ============================================================================

func TestDeleteHandler_Success(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	fs.On("DeleteObject", mock.Anything, testID).Return(nil)

	req := httptest.NewRequest(http.MethodDelete, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusNoContent, rec.Code)
}

func TestDeleteHandler_InvalidPath(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)

	req := httptest.NewRequest(http.MethodDelete, "/not-a-uuid", nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid object ID")
}

func TestDeleteHandler_StoreError(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	fs.On("DeleteObject", mock.Anything, testID).
		Return(errors.New("delete failed"))

	req := httptest.NewRequest(http.MethodDelete, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "delete failed")
}

func TestDeleteHandler_NotFound(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	fs.On("DeleteObject", mock.Anything, testID).
		Return(errors.New("object not found"))

	req := httptest.NewRequest(http.MethodDelete, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "object not found")
}

// ============================================================================
// GET Handler Tests
// ============================================================================

func TestGetHandler_Success(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()
	testData := []byte("test object content")

	fs.On("GetObjectData", mock.Anything, testID).
		Return(io.NopCloser(bytes.NewReader(testData)), nil)

	req := httptest.NewRequest(http.MethodGet, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, testData, rec.Body.Bytes())
}

func TestGetHandler_InvalidPath(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)

	req := httptest.NewRequest(http.MethodGet, "/invalid", nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid object ID")
}

func TestGetHandler_NotFound(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	fs.On("GetObjectData", mock.Anything, testID).
		Return(nil, errors.New("object not found"))

	req := httptest.NewRequest(http.MethodGet, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "object not found")
}

func TestGetHandler_StoreError(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	fs.On("GetObjectData", mock.Anything, testID).
		Return(nil, errors.New("internal storage error"))

	req := httptest.NewRequest(http.MethodGet, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
	assert.Contains(t, rec.Body.String(), "internal storage error")
}

func TestGetHandler_EmptyObject(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	// Empty object
	fs.On("GetObjectData", mock.Anything, testID).
		Return(io.NopCloser(bytes.NewReader([]byte{})), nil)

	req := httptest.NewRequest(http.MethodGet, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Body.Bytes())
}

func TestGetHandler_LargeObject(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	// 1MB of data
	testData := make([]byte, 1024*1024)
	for i := range testData {
		testData[i] = byte(i % 256)
	}

	fs.On("GetObjectData", mock.Anything, testID).
		Return(io.NopCloser(bytes.NewReader(testData)), nil)

	req := httptest.NewRequest(http.MethodGet, "/"+testID.String(), nil)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, len(testData), rec.Body.Len())
	assert.Equal(t, testData, rec.Body.Bytes())
}

// ============================================================================
// Context Propagation Tests
// ============================================================================

func TestPostHandler_ContextPropagation(t *testing.T) {
	t.Parallel()

	fs := newTestFileServer(t)
	testID := uuid.New()

	type contextKey string
	testKey := contextKey("test-key")
	testValue := "test-value"

	var capturedCtx context.Context
	fs.On("PutObject", mock.Anything, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			capturedCtx = args.Get(0).(context.Context)
		}).
		Return(nil)

	ctx := context.WithValue(context.Background(), testKey, testValue)
	req := httptest.NewRequest(http.MethodPost, "/"+testID.String(), bytes.NewReader([]byte("data")))
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	fs.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusCreated, rec.Code)
	require.NotNil(t, capturedCtx)
	assert.Equal(t, testValue, capturedCtx.Value(testKey))
}

// ============================================================================
// Edge Cases
// ============================================================================

func TestHandler_SpecialCharactersInPath(t *testing.T) {
	t.Parallel()

	// UUID with special characters shouldn't be valid
	// Note: We avoid raw control characters like \x00 because httptest.NewRequest
	// panics on invalid URL characters. Use URL-encoded versions instead.
	paths := []string{
		"/%00",
		"/uuid%00injection",
	}

	fs := newTestFileServer(t)

	for _, path := range paths {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()

		fs.ServeHTTP(rec, req)

		// Should fail with bad request
		assert.Equal(t, http.StatusBadRequest, rec.Code, "path: %s", path)
	}
}
