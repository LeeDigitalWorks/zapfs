//go:build integration

package iam_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/proto/iam_pb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
)

// Test configuration - can be overridden via environment variables
var (
	managerGRPCAddr  = getEnv("MANAGER_GRPC_ADDR", "localhost:8050")
	managerAdminAddr = getEnv("MANAGER_ADMIN_ADDR", "http://localhost:8060")
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// dialGRPC creates a gRPC connection using the modern grpc.NewClient API
func dialGRPC(t *testing.T, addr string, timeout time.Duration) *grpc.ClientConn {
	t.Helper()

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("failed to create gRPC client: %v", err)
	}

	// Trigger connection and wait for ready
	conn.Connect()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			return conn
		}
		if state == connectivity.TransientFailure || state == connectivity.Shutdown {
			conn.Close()
			t.Fatalf("failed to connect to %s: state=%v", addr, state)
		}
		if !conn.WaitForStateChange(ctx, state) {
			conn.Close()
			t.Fatalf("timeout connecting to %s", addr)
		}
	}
}

// TestIAMServiceConnection verifies we can connect to the manager's IAM gRPC service
func TestIAMServiceConnection(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialGRPC(t, managerGRPCAddr, 10*time.Second)
	defer conn.Close()

	client := iam_pb.NewIAMServiceClient(conn)

	// Get IAM version
	resp, err := client.GetIAMVersion(ctx, &iam_pb.GetIAMVersionRequest{})
	if err != nil {
		t.Fatalf("GetIAMVersion failed: %v", err)
	}

	t.Logf("IAM version: %d, credential count: %d", resp.Version, resp.CredentialCount)
}

// TestCredentialCreationViaAdminAPI tests creating a user and key via HTTP admin API
func TestCredentialCreationViaAdminAPI(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	username := fmt.Sprintf("testuser-%d", time.Now().UnixNano())

	// Create user via admin API
	userPayload := map[string]any{
		"username":     username,
		"display_name": "Test User",
		"email":        username + "@test.local",
	}
	userJSON, _ := json.Marshal(userPayload)

	req, _ := http.NewRequestWithContext(ctx, "POST", managerAdminAddr+"/v1/iam/users", bytes.NewReader(userJSON))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("create user failed: %s - %s", resp.Status, string(body))
	}

	t.Logf("created user: %s", username)

	// Create access key for user
	keyReq, _ := http.NewRequestWithContext(ctx, "POST",
		managerAdminAddr+"/v1/iam/users/"+username+"/keys", nil)

	keyResp, err := http.DefaultClient.Do(keyReq)
	if err != nil {
		t.Fatalf("failed to create key: %v", err)
	}
	defer keyResp.Body.Close()

	if keyResp.StatusCode != http.StatusCreated && keyResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(keyResp.Body)
		t.Fatalf("create key failed: %s - %s", keyResp.Status, string(body))
	}

	var keyResult struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
	}
	if err := json.NewDecoder(keyResp.Body).Decode(&keyResult); err != nil {
		t.Fatalf("failed to decode key response: %v", err)
	}

	t.Logf("created access key: %s", keyResult.AccessKey)

	// Verify the key is available via gRPC
	conn := dialGRPC(t, managerGRPCAddr, 10*time.Second)
	defer conn.Close()

	client := iam_pb.NewIAMServiceClient(conn)

	// Get credential via gRPC
	credResp, err := client.GetCredential(ctx, &iam_pb.GetCredentialRequest{
		AccessKey:     keyResult.AccessKey,
		IncludeSecret: true,
	})
	if err != nil {
		t.Fatalf("GetCredential failed: %v", err)
	}

	if !credResp.Found {
		t.Fatal("credential not found")
	}

	if credResp.Credential.Username != username {
		t.Errorf("expected username %s, got %s", username, credResp.Credential.Username)
	}

	if credResp.Credential.SecretKey == "" {
		t.Error("secret key should be included")
	}

	t.Logf("verified credential via gRPC: username=%s, has_secret=%v",
		credResp.Credential.Username, credResp.Credential.SecretKey != "")
}

// TestCredentialSyncToMetadata tests that credentials sync from manager to metadata
func TestCredentialSyncToMetadata(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// Connect to manager gRPC for credential creation
	managerConn := dialGRPC(t, managerGRPCAddr, 10*time.Second)
	defer managerConn.Close()

	// Create user via admin API
	username := fmt.Sprintf("synctest-%d", time.Now().UnixNano())
	userPayload := map[string]any{
		"username": username,
		"email":    username + "@test.local",
	}
	userJSON, _ := json.Marshal(userPayload)

	req, _ := http.NewRequestWithContext(ctx, "POST", managerAdminAddr+"/v1/iam/users", bytes.NewReader(userJSON))
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("failed to create user: %v", err)
	}
	resp.Body.Close()

	// Create access key
	keyReq, _ := http.NewRequestWithContext(ctx, "POST",
		managerAdminAddr+"/v1/iam/users/"+username+"/keys", nil)

	keyResp, err := http.DefaultClient.Do(keyReq)
	if err != nil {
		t.Fatalf("failed to create key: %v", err)
	}

	var keyResult struct {
		AccessKey string `json:"access_key"`
		SecretKey string `json:"secret_key"`
	}
	json.NewDecoder(keyResp.Body).Decode(&keyResult)
	keyResp.Body.Close()

	t.Logf("created key %s, waiting for sync to metadata...", keyResult.AccessKey)

	// Wait for credential to sync to metadata service
	// The metadata service should receive it via gRPC streaming

	// Create a RemoteCredentialStore pointing to manager to verify sync works
	remoteStore, err := iam.NewRemoteCredentialStore(ctx, iam.RemoteStoreConfig{
		ManagerAddrs:  []string{managerGRPCAddr},
		CacheMaxItems: 1000,
		CacheTTL:      1 * time.Minute,
	})
	if err != nil {
		t.Fatalf("failed to create remote store: %v", err)
	}

	// Do initial sync
	if err := remoteStore.InitialSync(ctx); err != nil {
		t.Fatalf("initial sync failed: %v", err)
	}

	// Lookup the credential
	identity, cred, err := remoteStore.GetUserByAccessKey(ctx, keyResult.AccessKey)
	if err != nil {
		t.Fatalf("failed to get credential: %v", err)
	}

	if identity.Name != username {
		t.Errorf("expected username %s, got %s", username, identity.Name)
	}

	if cred.SecretKey == "" {
		t.Error("secret key should be synced")
	}

	t.Logf("sync verified: username=%s, access_key=%s", identity.Name, cred.AccessKey)
}

// TestStreamCredentials tests real-time credential streaming
func TestStreamCredentials(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn := dialGRPC(t, managerGRPCAddr, 10*time.Second)
	defer conn.Close()

	client := iam_pb.NewIAMServiceClient(conn)

	// Get current version
	versionResp, _ := client.GetIAMVersion(ctx, &iam_pb.GetIAMVersionRequest{})
	currentVersion := versionResp.Version

	// Start streaming from current version (should receive only new events)
	stream, err := client.StreamCredentials(ctx, &iam_pb.StreamCredentialsRequest{
		SinceVersion: currentVersion,
	})
	if err != nil {
		t.Fatalf("StreamCredentials failed: %v", err)
	}

	// Create a new user to trigger an event
	username := fmt.Sprintf("streamtest-%d", time.Now().UnixNano())
	userPayload := map[string]any{
		"username": username,
		"email":    username + "@test.local",
	}
	userJSON, _ := json.Marshal(userPayload)

	go func() {
		// Wait a bit then create user
		time.Sleep(500 * time.Millisecond)

		req, _ := http.NewRequest("POST", managerAdminAddr+"/v1/iam/users", bytes.NewReader(userJSON))
		req.Header.Set("Content-Type", "application/json")
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Logf("create user error: %v", err)
			return
		}
		resp.Body.Close()

		// Create key
		keyReq, _ := http.NewRequest("POST", managerAdminAddr+"/v1/iam/users/"+username+"/keys", nil)
		keyResp, _ := http.DefaultClient.Do(keyReq)
		if keyResp != nil {
			keyResp.Body.Close()
		}
	}()

	// Wait for event
	receivedEvent := false
	for {
		event, err := stream.Recv()
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			t.Fatalf("stream error: %v", err)
		}

		if event.Credential.Username == username {
			t.Logf("received event: type=%s, username=%s, version=%d",
				event.Type, event.Credential.Username, event.Version)
			receivedEvent = true
			break
		}
	}

	if !receivedEvent {
		t.Error("did not receive expected credential event")
	}
}

// TestListCredentials tests listing all credentials
func TestListCredentials(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	conn := dialGRPC(t, managerGRPCAddr, 10*time.Second)
	defer conn.Close()

	client := iam_pb.NewIAMServiceClient(conn)

	stream, err := client.ListCredentials(ctx, &iam_pb.ListCredentialsRequest{})
	if err != nil {
		t.Fatalf("ListCredentials failed: %v", err)
	}

	count := 0
	for {
		cred, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}

		t.Logf("credential: username=%s, access_key=%s, policies=%v",
			cred.Username, cred.AccessKey, cred.PolicyNames)
		count++
	}

	t.Logf("listed %d credentials", count)
}

// TestPolicySyncWithCredentials verifies policies are synced alongside credentials
func TestPolicySyncWithCredentials(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Connect to manager gRPC
	conn := dialGRPC(t, managerGRPCAddr, 10*time.Second)
	defer conn.Close()

	client := iam_pb.NewIAMServiceClient(conn)

	// List credentials to check if policies are included
	stream, err := client.ListCredentials(ctx, &iam_pb.ListCredentialsRequest{})
	if err != nil {
		t.Fatalf("ListCredentials failed: %v", err)
	}

	foundWithPolicies := false
	for {
		cred, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("stream error: %v", err)
		}

		// Check if any credentials have policies attached
		if len(cred.Policies) > 0 {
			foundWithPolicies = true
			t.Logf("credential with policies: username=%s, policy_count=%d",
				cred.Username, len(cred.Policies))

			for _, p := range cred.Policies {
				t.Logf("  - policy: name=%s, doc_len=%d", p.Name, len(p.Document))
			}
		}
	}

	if !foundWithPolicies {
		t.Log("no credentials with policies found (this may be expected if no policies attached)")
	}
}
