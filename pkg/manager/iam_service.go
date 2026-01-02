// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/iam"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/iam_pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ===== IAM SERVICE gRPC IMPLEMENTATION =====
// These methods implement iam_pb.IAMServiceServer for credential sync to metadata services.

// GetCredential retrieves a credential by access key
func (ms *ManagerServer) GetCredential(ctx context.Context, req *iam_pb.GetCredentialRequest) (*iam_pb.GetCredentialResponse, error) {
	identity, cred, found := ms.iamService.LookupByAccessKey(ctx, req.AccessKey)
	if !found {
		return &iam_pb.GetCredentialResponse{
			Found:   false,
			Version: ms.iamVersion.Load(),
		}, nil
	}

	return &iam_pb.GetCredentialResponse{
		Credential: ms.identityToProto(identity, cred, req.IncludeSecret),
		Found:      true,
		Version:    ms.iamVersion.Load(),
	}, nil
}

// StreamCredentials streams credential changes to metadata services in real-time
func (ms *ManagerServer) StreamCredentials(req *iam_pb.StreamCredentialsRequest, stream iam_pb.IAMService_StreamCredentialsServer) error {
	subID := ms.iamNextSubID.Add(1)
	eventCh := make(chan *iam_pb.CredentialEvent, 100)

	ms.iamSubsMu.Lock()
	ms.iamSubs[subID] = eventCh
	ms.iamSubsMu.Unlock()

	defer func() {
		ms.iamSubsMu.Lock()
		delete(ms.iamSubs, subID)
		ms.iamSubsMu.Unlock()
		close(eventCh)
	}()

	logger.Info().Uint64("subscriber_id", subID).Uint64("since_version", req.SinceVersion).Msg("IAM stream subscriber connected")

	// Send catch-up events if client is behind (sinceVersion is a timestamp in nanos)
	if req.SinceVersion > 0 && req.SinceVersion < ms.iamVersion.Load() {
		if err := ms.sendCatchUpEvents(stream, req.SinceVersion); err != nil {
			return err
		}
	} else if req.SinceVersion == 0 {
		// Initial sync - send all credentials
		if err := ms.sendAllCredentials(stream); err != nil {
			return err
		}
	}

	// Stream events
	for {
		select {
		case event := <-eventCh:
			if err := stream.Send(event); err != nil {
				logger.Warn().Err(err).Uint64("subscriber_id", subID).Msg("failed to send IAM event")
				return err
			}
		case <-stream.Context().Done():
			logger.Info().Uint64("subscriber_id", subID).Msg("IAM stream subscriber disconnected")
			return nil
		}
	}
}

// sendCatchUpEvents sends credentials updated since the given timestamp
func (ms *ManagerServer) sendCatchUpEvents(stream iam_pb.IAMService_StreamCredentialsServer, sinceVersion uint64) error {
	sinceTime := time.Unix(0, int64(sinceVersion))

	users, err := ms.iamService.ListUsers(stream.Context())
	if err != nil {
		return err
	}

	for _, username := range users {
		identity, err := ms.iamService.GetUser(stream.Context(), username)
		if err != nil {
			continue
		}

		for _, cred := range identity.Credentials {
			// Only send credentials created/updated after sinceVersion
			if cred.CreatedAt.After(sinceTime) {
				event := &iam_pb.CredentialEvent{
					Type:       iam_pb.CredentialEvent_EVENT_TYPE_CREATED,
					Credential: ms.identityToProto(identity, cred, true),
					Version:    ms.iamVersion.Load(),
					Timestamp:  timestamppb.Now(),
				}
				if err := stream.Send(event); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// sendAllCredentials sends all credentials (for initial sync)
func (ms *ManagerServer) sendAllCredentials(stream iam_pb.IAMService_StreamCredentialsServer) error {
	users, err := ms.iamService.ListUsers(stream.Context())
	if err != nil {
		return err
	}

	for _, username := range users {
		identity, err := ms.iamService.GetUser(stream.Context(), username)
		if err != nil {
			continue
		}

		for _, cred := range identity.Credentials {
			event := &iam_pb.CredentialEvent{
				Type:       iam_pb.CredentialEvent_EVENT_TYPE_CREATED,
				Credential: ms.identityToProto(identity, cred, true),
				Version:    ms.iamVersion.Load(),
				Timestamp:  timestamppb.Now(),
			}
			if err := stream.Send(event); err != nil {
				return err
			}
		}
	}

	return nil
}

// ListCredentials returns all credentials for initial sync
func (ms *ManagerServer) ListCredentials(req *iam_pb.ListCredentialsRequest, stream iam_pb.IAMService_ListCredentialsServer) error {
	users, err := ms.iamService.ListUsers(stream.Context())
	if err != nil {
		return err
	}

	for _, username := range users {
		identity, err := ms.iamService.GetUser(stream.Context(), username)
		if err != nil {
			continue
		}

		for _, cred := range identity.Credentials {
			if req.StatusFilter == "" || cred.Status == req.StatusFilter {
				if err := stream.Send(ms.identityToProto(identity, cred, true)); err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// GetIAMVersion returns the current IAM data version
func (ms *ManagerServer) GetIAMVersion(ctx context.Context, req *iam_pb.GetIAMVersionRequest) (*iam_pb.GetIAMVersionResponse, error) {
	users, _ := ms.iamService.ListUsers(ctx)

	var credCount uint64
	for _, username := range users {
		if identity, err := ms.iamService.GetUser(ctx, username); err == nil {
			credCount += uint64(len(identity.Credentials))
		}
	}

	return &iam_pb.GetIAMVersionResponse{
		Version:         ms.iamVersion.Load(),
		CredentialCount: credCount,
		LastUpdated:     timestamppb.New(time.Unix(0, int64(ms.iamVersion.Load()))),
	}, nil
}

// NotifyCredentialChange broadcasts a credential change to all subscribers.
// Called by IAM admin handlers when credentials are created/updated/deleted.
func (ms *ManagerServer) NotifyCredentialChange(eventType iam_pb.CredentialEvent_EventType, identity *iam.Identity, cred *iam.Credential) {
	// Update version to current timestamp (nanoseconds for ordering)
	newVersion := uint64(time.Now().UnixNano())
	ms.iamVersion.Store(newVersion)

	event := &iam_pb.CredentialEvent{
		Type:       eventType,
		Credential: ms.identityToProto(identity, cred, true),
		Version:    newVersion,
		Timestamp:  timestamppb.Now(),
	}

	ms.iamSubsMu.RLock()
	defer ms.iamSubsMu.RUnlock()

	for subID, ch := range ms.iamSubs {
		select {
		case ch <- event:
			// Event sent
		default:
			IAMEventsDropped.Inc()
			logger.Warn().Uint64("subscriber_id", subID).Msg("IAM subscriber channel full, dropping event")
		}
	}
}

// identityToProto converts domain types to proto message
func (ms *ManagerServer) identityToProto(identity *iam.Identity, cred *iam.Credential, includeSecret bool) *iam_pb.Credential {
	pb := &iam_pb.Credential{
		AccessKey: cred.AccessKey,
		Username:  identity.Name,
		Disabled:  identity.Disabled,
		Status:    cred.Status,
		CreatedAt: timestamppb.New(cred.CreatedAt),
	}

	if identity.Account != nil {
		pb.AccountId = identity.Account.ID
		pb.DisplayName = identity.Account.DisplayName
		pb.Email = identity.Account.EmailAddress
	}

	if cred.ExpiresAt != nil {
		pb.ExpiresAt = timestamppb.New(*cred.ExpiresAt)
	}

	// Include decrypted secret for trusted metadata services
	if includeSecret {
		if cred.SecretKey == "" && len(cred.EncryptedSecret) > 0 {
			if decrypted, err := iam.DecryptCredentialSecret(cred.EncryptedSecret); err == nil {
				pb.SecretKey = decrypted
			}
		} else {
			pb.SecretKey = cred.SecretKey
		}
	}

	// Include policy and group information for authorization
	pb.PolicyNames, pb.GroupNames, pb.Policies = ms.getUserPoliciesAndGroups(identity.Name)

	return pb
}

// getUserPoliciesAndGroups retrieves policy names, group names, and full policy documents for a user
func (ms *ManagerServer) getUserPoliciesAndGroups(username string) ([]string, []string, []*iam_pb.Policy) {
	ctx := context.Background()

	// Get user's groups
	groups, err := ms.iamService.GroupStore().GetUserGroups(ctx, username)
	if err != nil {
		logger.Warn().Err(err).Str("username", username).Msg("failed to get user groups")
		groups = nil
	}

	// Collect all policy names (user policies + group policies)
	var policyNames []string
	var policies []*iam_pb.Policy

	// Get user's direct policies
	userPolicies, err := ms.iamService.PolicyStore().GetUserPolicies(ctx, username)
	if err != nil {
		logger.Warn().Err(err).Str("username", username).Msg("failed to get user policies")
	} else {
		for _, p := range userPolicies {
			policyNames = append(policyNames, p.ID)
			policies = append(policies, policyToProto(p))
		}
	}

	// Get policies from groups
	for _, groupName := range groups {
		groupPolicies, err := ms.iamService.PolicyStore().GetGroupPolicies(ctx, groupName)
		if err != nil {
			logger.Warn().Err(err).Str("group", groupName).Msg("failed to get group policies")
			continue
		}
		for _, p := range groupPolicies {
			policyNames = append(policyNames, p.ID)
			policies = append(policies, policyToProto(p))
		}
	}

	return policyNames, groups, policies
}

// policyToProto converts an IAM policy to proto format
func policyToProto(p *iam.Policy) *iam_pb.Policy {
	if p == nil {
		return nil
	}

	// Serialize policy document to JSON
	docJSON, err := p.ToJSON()
	if err != nil {
		logger.Warn().Err(err).Str("policy_id", p.ID).Msg("failed to serialize policy")
		docJSON = "{}"
	}

	pb := &iam_pb.Policy{
		Name:     p.ID,
		Document: docJSON,
	}

	return pb
}
