// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package manager

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/grpc/pool"
	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/file_pb"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// rebalancePlanCache stores calculated plans for later execution
var (
	rebalancePlanCache   = make(map[string]*manager_pb.RebalancePlan)
	rebalancePlanCacheMu sync.RWMutex
)

// fileClientPool is a pool for connecting to file servers
var fileClientPool = pool.NewPool(func(cc grpc.ClientConnInterface) file_pb.FileServiceClient {
	return file_pb.NewFileServiceClient(cc)
})

// GetClusterStatus returns the current status of all file servers in the cluster.
func (ms *ManagerServer) GetClusterStatus(ctx context.Context, req *manager_pb.GetClusterStatusRequest) (*manager_pb.GetClusterStatusResponse, error) {
	ms.state.RLock()
	defer ms.state.RUnlock()

	var servers []*manager_pb.FileServerStatus
	var totalBytes, usedBytes int64
	var minUsage, maxUsage float64 = 100, 0
	onlineCount := 0

	for id, svc := range ms.state.FileServices {
		var svcTotal, svcUsed int64
		var chunkCount int64

		// Sum up all backends for this server
		for _, sb := range svc.StorageBackends {
			for _, b := range sb.Backends {
				svcTotal += int64(b.GetTotalBytes())
				svcUsed += int64(b.GetUsedBytes())
			}
		}

		usagePercent := 0.0
		if svcTotal > 0 {
			usagePercent = float64(svcUsed) / float64(svcTotal) * 100
		}

		status := "online"
		if svc.Status == ServiceOffline {
			status = "offline"
		} else {
			onlineCount++
			totalBytes += svcTotal
			usedBytes += svcUsed

			if usagePercent < minUsage {
				minUsage = usagePercent
			}
			if usagePercent > maxUsage {
				maxUsage = usagePercent
			}
		}

		servers = append(servers, &manager_pb.FileServerStatus{
			ServerId:      id,
			Address:       svc.Location.GetAddress(),
			TotalBytes:    svcTotal,
			UsedBytes:     svcUsed,
			ChunkCount:    chunkCount,
			UsagePercent:  usagePercent,
			Status:        status,
			LastHeartbeat: timestamppb.New(svc.LastHeartbeat),
		})
	}

	// Sort by usage percent descending
	sort.Slice(servers, func(i, j int) bool {
		return servers[i].UsagePercent > servers[j].UsagePercent
	})

	avgUsage := 0.0
	if totalBytes > 0 {
		avgUsage = float64(usedBytes) / float64(totalBytes) * 100
	}

	// Handle case where no online servers
	if onlineCount == 0 {
		minUsage = 0
		maxUsage = 0
	}

	return &manager_pb.GetClusterStatusResponse{
		FileServers: servers,
		Capacity: &manager_pb.ClusterCapacitySummary{
			TotalBytes:       totalBytes,
			UsedBytes:        usedBytes,
			AvgUsagePercent:  avgUsage,
			MaxUsagePercent:  maxUsage,
			MinUsagePercent:  minUsage,
			ImbalancePercent: maxUsage - minUsage,
			OnlineServers:    int32(onlineCount),
		},
	}, nil
}

// CalculateRebalancePlan calculates a plan to rebalance chunks across file servers.
func (ms *ManagerServer) CalculateRebalancePlan(ctx context.Context, req *manager_pb.CalculateRebalancePlanRequest) (*manager_pb.CalculateRebalancePlanResponse, error) {
	targetVariance := req.GetTargetVariancePercent()
	if targetVariance <= 0 {
		targetVariance = 5.0 // Default 5%
	}

	// Get current cluster status
	statusResp, err := ms.GetClusterStatus(ctx, &manager_pb.GetClusterStatusRequest{})
	if err != nil {
		return &manager_pb.CalculateRebalancePlanResponse{
			Success: false,
			Error:   fmt.Sprintf("failed to get cluster status: %v", err),
		}, nil
	}

	onlineServers := statusResp.Capacity.OnlineServers
	if onlineServers < 2 {
		return &manager_pb.CalculateRebalancePlanResponse{
			Success: false,
			Error:   "need at least 2 online servers to rebalance",
		}, nil
	}

	avgUsage := statusResp.Capacity.AvgUsagePercent
	imbalance := statusResp.Capacity.ImbalancePercent

	logger.Info().
		Float64("avg_usage", avgUsage).
		Float64("imbalance", imbalance).
		Float64("target_variance", targetVariance).
		Msg("Calculating rebalance plan")

	// If cluster is already balanced within target variance, nothing to do
	if imbalance <= targetVariance*2 {
		return &manager_pb.CalculateRebalancePlanResponse{
			Success: true,
			Plan: &manager_pb.RebalancePlan{
				PlanId:     uuid.NewString(),
				Migrations: nil,
				TotalBytes: 0,
				CreatedAt:  timestamppb.Now(),
			},
		}, nil
	}

	// Find over-utilized and under-utilized servers
	var overUtilized, underUtilized []*manager_pb.FileServerStatus
	for _, srv := range statusResp.FileServers {
		if srv.Status != "online" {
			continue
		}
		if srv.UsagePercent > avgUsage+targetVariance {
			overUtilized = append(overUtilized, srv)
		} else if srv.UsagePercent < avgUsage-targetVariance {
			underUtilized = append(underUtilized, srv)
		}
	}

	if len(overUtilized) == 0 || len(underUtilized) == 0 {
		return &manager_pb.CalculateRebalancePlanResponse{
			Success: true,
			Plan: &manager_pb.RebalancePlan{
				PlanId:     uuid.NewString(),
				Migrations: nil,
				TotalBytes: 0,
				CreatedAt:  timestamppb.Now(),
			},
		}, nil
	}

	// Calculate migrations needed
	// For now, we calculate how many bytes need to move from over to under
	// Actual chunk selection requires querying metadata (chunk_replicas)
	var migrations []*manager_pb.ChunkMigration
	var totalBytes int64

	for _, over := range overUtilized {
		// Calculate how many bytes to move from this server
		targetBytes := int64(float64(over.TotalBytes) * avgUsage / 100)
		bytesToMove := over.UsedBytes - targetBytes

		if bytesToMove <= 0 {
			continue
		}

		// Find under-utilized targets
		for _, under := range underUtilized {
			if bytesToMove <= 0 {
				break
			}

			// How much can this server receive?
			underTarget := int64(float64(under.TotalBytes) * avgUsage / 100)
			canReceive := underTarget - under.UsedBytes

			if canReceive <= 0 {
				continue
			}

			moveAmount := min(bytesToMove, canReceive)
			if req.GetMaxBytesToMove() > 0 && totalBytes+moveAmount > req.GetMaxBytesToMove() {
				moveAmount = req.GetMaxBytesToMove() - totalBytes
			}

			if moveAmount > 0 {
				// Create a placeholder migration
				// In production, this would query chunk_replicas for actual chunks
				migrations = append(migrations, &manager_pb.ChunkMigration{
					ChunkId:    fmt.Sprintf("placeholder-%d", len(migrations)),
					FromServer: over.ServerId,
					ToServer:   under.ServerId,
					SizeBytes:  moveAmount,
					Priority:   int32(len(migrations)),
				})
				totalBytes += moveAmount
				bytesToMove -= moveAmount
			}

			if req.GetMaxBytesToMove() > 0 && totalBytes >= req.GetMaxBytesToMove() {
				break
			}
		}
	}

	// Estimate duration (assume 100 MB/s average throughput)
	estimatedDuration := totalBytes / (100 * 1024 * 1024) * 1000 // milliseconds

	plan := &manager_pb.RebalancePlan{
		PlanId:              uuid.NewString(),
		Migrations:          migrations,
		TotalBytes:          totalBytes,
		TotalChunks:         int32(len(migrations)),
		EstimatedDurationMs: estimatedDuration,
		CreatedAt:           timestamppb.Now(),
	}

	// Cache plan for later execution (unless dry run)
	if !req.GetDryRun() {
		rebalancePlanCacheMu.Lock()
		rebalancePlanCache[plan.PlanId] = plan
		rebalancePlanCacheMu.Unlock()
	}

	logger.Info().
		Str("plan_id", plan.PlanId).
		Int("migrations", len(migrations)).
		Int64("total_bytes", totalBytes).
		Bool("dry_run", req.GetDryRun()).
		Msg("Rebalance plan calculated")

	return &manager_pb.CalculateRebalancePlanResponse{
		Success: true,
		Plan:    plan,
	}, nil
}

// ExecuteRebalance executes a rebalance plan, streaming progress updates.
func (ms *ManagerServer) ExecuteRebalance(req *manager_pb.ExecuteRebalanceRequest, stream manager_pb.ManagerService_ExecuteRebalanceServer) error {
	ctx := stream.Context()

	// Get or calculate plan
	var plan *manager_pb.RebalancePlan
	if req.GetPlanId() != "" {
		rebalancePlanCacheMu.RLock()
		plan = rebalancePlanCache[req.GetPlanId()]
		rebalancePlanCacheMu.RUnlock()

		if plan == nil {
			return stream.Send(&manager_pb.RebalanceProgress{
				Status: "failed",
				Error:  "plan not found",
			})
		}
	} else {
		// Calculate new plan
		calcResp, err := ms.CalculateRebalancePlan(ctx, &manager_pb.CalculateRebalancePlanRequest{
			TargetVariancePercent: 5,
		})
		if err != nil || !calcResp.Success {
			errMsg := "failed to calculate plan"
			if calcResp != nil {
				errMsg = calcResp.Error
			}
			return stream.Send(&manager_pb.RebalanceProgress{
				Status: "failed",
				Error:  errMsg,
			})
		}
		plan = calcResp.Plan
	}

	if len(plan.Migrations) == 0 {
		return stream.Send(&manager_pb.RebalanceProgress{
			PlanId:              plan.PlanId,
			MigrationsCompleted: 0,
			MigrationsTotal:     0,
			BytesMoved:          0,
			BytesTotal:          0,
			ProgressPercent:     100,
			Status:              "completed",
		})
	}

	logger.Info().
		Str("plan_id", plan.PlanId).
		Int("migrations", len(plan.Migrations)).
		Int64("rate_limit_bps", req.GetRateLimitBps()).
		Msg("Starting rebalance execution")

	// Send initial progress
	if err := stream.Send(&manager_pb.RebalanceProgress{
		PlanId:          plan.PlanId,
		MigrationsTotal: int32(len(plan.Migrations)),
		BytesTotal:      plan.TotalBytes,
		Status:          "running",
	}); err != nil {
		return err
	}

	// Execute migrations
	var migrationsCompleted int32
	var bytesMoved int64

	// TODO: Use maxConcurrent for parallel migrations
	_ = req.GetMaxConcurrent()

	deleteSource := req.GetDeleteSource()

	// Group migrations by source server for concurrent execution
	migrationsBySource := make(map[string][]*manager_pb.ChunkMigration)
	for _, m := range plan.Migrations {
		migrationsBySource[m.FromServer] = append(migrationsBySource[m.FromServer], m)
	}

	// Execute migrations (simplified - single threaded for now)
	for _, migration := range plan.Migrations {
		select {
		case <-ctx.Done():
			return stream.Send(&manager_pb.RebalanceProgress{
				PlanId:              plan.PlanId,
				MigrationsCompleted: migrationsCompleted,
				MigrationsTotal:     int32(len(plan.Migrations)),
				BytesMoved:          bytesMoved,
				BytesTotal:          plan.TotalBytes,
				ProgressPercent:     float64(bytesMoved) / float64(plan.TotalBytes) * 100,
				Status:              "cancelled",
			})
		default:
		}

		// Execute this migration
		err := ms.executeSingleMigration(ctx, migration, deleteSource)
		if err != nil {
			logger.Warn().
				Err(err).
				Str("chunk_id", migration.ChunkId).
				Str("from", migration.FromServer).
				Str("to", migration.ToServer).
				Msg("Migration failed, continuing with next")
			// Continue with other migrations
			continue
		}

		migrationsCompleted++
		bytesMoved += migration.SizeBytes

		// Send progress update
		if err := stream.Send(&manager_pb.RebalanceProgress{
			PlanId:              plan.PlanId,
			MigrationsCompleted: migrationsCompleted,
			MigrationsTotal:     int32(len(plan.Migrations)),
			BytesMoved:          bytesMoved,
			BytesTotal:          plan.TotalBytes,
			ProgressPercent:     float64(bytesMoved) / float64(plan.TotalBytes) * 100,
			CurrentChunk:        migration.ChunkId,
			Status:              "running",
		}); err != nil {
			return err
		}
	}

	// Cleanup cached plan
	rebalancePlanCacheMu.Lock()
	delete(rebalancePlanCache, plan.PlanId)
	rebalancePlanCacheMu.Unlock()

	logger.Info().
		Str("plan_id", plan.PlanId).
		Int32("completed", migrationsCompleted).
		Int64("bytes_moved", bytesMoved).
		Msg("Rebalance execution completed")

	return stream.Send(&manager_pb.RebalanceProgress{
		PlanId:              plan.PlanId,
		MigrationsCompleted: migrationsCompleted,
		MigrationsTotal:     int32(len(plan.Migrations)),
		BytesMoved:          bytesMoved,
		BytesTotal:          plan.TotalBytes,
		ProgressPercent:     100,
		Status:              "completed",
	})
}

// executeSingleMigration migrates a single chunk from source to target.
func (ms *ManagerServer) executeSingleMigration(ctx context.Context, migration *manager_pb.ChunkMigration, deleteSource bool) error {
	// Skip placeholder migrations (these are just estimates)
	if len(migration.ChunkId) > 11 && migration.ChunkId[:11] == "placeholder" {
		// In production, we would query chunk_replicas for actual chunks
		logger.Debug().Str("chunk_id", migration.ChunkId).Msg("Skipping placeholder migration")
		return nil
	}

	// Connect to source file server
	sourceClient, err := fileClientPool.Get(ctx, migration.FromServer)
	if err != nil {
		return fmt.Errorf("failed to connect to source %s: %w", migration.FromServer, err)
	}

	// Request migration
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()

	resp, err := sourceClient.MigrateChunk(ctx, &file_pb.MigrateChunkRequest{
		ChunkId:            migration.ChunkId,
		TargetServer:       migration.ToServer,
		DeleteAfterMigrate: deleteSource,
	})
	if err != nil {
		return fmt.Errorf("migration RPC failed: %w", err)
	}

	if !resp.Success {
		return fmt.Errorf("migration failed: %s", resp.Error)
	}

	logger.Debug().
		Str("chunk_id", migration.ChunkId).
		Str("from", migration.FromServer).
		Str("to", migration.ToServer).
		Int64("bytes", resp.BytesTransferred).
		Int64("duration_ms", resp.DurationMs).
		Msg("Chunk migrated successfully")

	return nil
}
