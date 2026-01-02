// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/pkg/metadata/client"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/dustin/go-humanize"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var clusterCmd = &cobra.Command{
	Use:   "cluster",
	Short: "Cluster management commands",
	Long:  `Commands for managing the ZapFS cluster, including status and rebalancing.`,
}

var clusterStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show cluster status",
	Long:  `Display the status of all file servers in the cluster, including disk usage.`,
	Run:   runClusterStatus,
}

var clusterRebalanceCmd = &cobra.Command{
	Use:   "rebalance",
	Short: "Rebalance data across file servers",
	Long: `Rebalance chunks across file servers to achieve even disk usage.

By default, calculates and displays a rebalance plan. Use --execute to actually perform the rebalance.`,
	Run: runClusterRebalance,
}

func init() {
	rootCmd.AddCommand(clusterCmd)
	clusterCmd.AddCommand(clusterStatusCmd)
	clusterCmd.AddCommand(clusterRebalanceCmd)

	// Status flags
	clusterStatusCmd.Flags().String("manager", "", "Manager service address (host:port)")

	// Rebalance flags
	clusterRebalanceCmd.Flags().String("manager", "", "Manager service address (host:port)")
	clusterRebalanceCmd.Flags().Bool("execute", false, "Actually execute the rebalance (default: dry-run)")
	clusterRebalanceCmd.Flags().Float64("variance", 5.0, "Target usage variance percent (servers within this % of average won't be touched)")
	clusterRebalanceCmd.Flags().String("max-bytes", "", "Maximum bytes to move (e.g., '10GB', '500MB')")
	clusterRebalanceCmd.Flags().String("rate-limit", "", "Rate limit for migrations (e.g., '100MB/s')")
	clusterRebalanceCmd.Flags().Int("concurrent", 1, "Max concurrent migrations per server")
	clusterRebalanceCmd.Flags().Bool("delete-source", true, "Delete source chunk after successful migration")
}

func runClusterStatus(cmd *cobra.Command, args []string) {
	managerAddr := viper.GetString("manager")
	if managerAddr == "" {
		managerAddr, _ = cmd.Flags().GetString("manager")
	}
	if managerAddr == "" {
		logger.Fatal().Msg("--manager address is required")
	}

	// Connect to manager
	managerClient := client.NewManagerClientPool(client.ManagerClientPoolConfig{
		SeedAddrs:      []string{managerAddr},
		DialTimeout:    5 * time.Second,
		RequestTimeout: 30 * time.Second,
	})
	defer managerClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Get cluster status
	resp, err := managerClient.GetClusterStatus(ctx, &manager_pb.GetClusterStatusRequest{})
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to get cluster status")
	}

	// Print file server table
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "FILE SERVER\tADDRESS\tTOTAL\tUSED\tUSAGE\tSTATUS\tLAST HEARTBEAT")
	fmt.Fprintln(w, "-----------\t-------\t-----\t----\t-----\t------\t--------------")

	for _, srv := range resp.FileServers {
		lastHB := "never"
		if srv.LastHeartbeat != nil {
			lastHB = humanize.Time(srv.LastHeartbeat.AsTime())
		}

		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%.1f%%\t%s\t%s\n",
			srv.ServerId,
			srv.Address,
			humanize.Bytes(uint64(srv.TotalBytes)),
			humanize.Bytes(uint64(srv.UsedBytes)),
			srv.UsagePercent,
			srv.Status,
			lastHB,
		)
	}
	w.Flush()

	// Print summary
	fmt.Println()
	cap := resp.Capacity
	fmt.Printf("Cluster Summary:\n")
	fmt.Printf("  Online Servers:   %d\n", cap.OnlineServers)
	fmt.Printf("  Total Capacity:   %s\n", humanize.Bytes(uint64(cap.TotalBytes)))
	fmt.Printf("  Used:             %s (%.1f%%)\n", humanize.Bytes(uint64(cap.UsedBytes)), cap.AvgUsagePercent)
	fmt.Printf("  Usage Range:      %.1f%% - %.1f%%\n", cap.MinUsagePercent, cap.MaxUsagePercent)
	fmt.Printf("  Imbalance:        %.1f%%\n", cap.ImbalancePercent)
}

func runClusterRebalance(cmd *cobra.Command, args []string) {
	managerAddr := viper.GetString("manager")
	if managerAddr == "" {
		managerAddr, _ = cmd.Flags().GetString("manager")
	}
	if managerAddr == "" {
		logger.Fatal().Msg("--manager address is required")
	}

	execute, _ := cmd.Flags().GetBool("execute")
	variance, _ := cmd.Flags().GetFloat64("variance")
	maxBytesStr, _ := cmd.Flags().GetString("max-bytes")
	rateLimitStr, _ := cmd.Flags().GetString("rate-limit")
	concurrent, _ := cmd.Flags().GetInt("concurrent")
	deleteSource, _ := cmd.Flags().GetBool("delete-source")

	var maxBytes int64
	if maxBytesStr != "" {
		parsed, err := humanize.ParseBytes(maxBytesStr)
		if err != nil {
			logger.Fatal().Err(err).Str("value", maxBytesStr).Msg("Invalid --max-bytes value")
		}
		maxBytes = int64(parsed)
	}

	var rateLimit int64
	if rateLimitStr != "" {
		// Parse rate like "100MB/s" or "100MB"
		parsed, err := humanize.ParseBytes(rateLimitStr)
		if err != nil {
			logger.Fatal().Err(err).Str("value", rateLimitStr).Msg("Invalid --rate-limit value")
		}
		rateLimit = int64(parsed)
	}

	// Connect to manager
	managerClient := client.NewManagerClientPool(client.ManagerClientPoolConfig{
		SeedAddrs:      []string{managerAddr},
		DialTimeout:    5 * time.Second,
		RequestTimeout: 5 * time.Minute,
	})
	defer managerClient.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
	defer cancel()

	if !execute {
		// Dry run - just calculate plan
		fmt.Println("Calculating rebalance plan (dry-run)...")
		fmt.Println()

		resp, err := managerClient.CalculateRebalancePlan(ctx, &manager_pb.CalculateRebalancePlanRequest{
			TargetVariancePercent: variance,
			MaxBytesToMove:        maxBytes,
			DryRun:                true,
		})
		if err != nil {
			logger.Fatal().Err(err).Msg("Failed to calculate rebalance plan")
		}

		if !resp.Success {
			logger.Fatal().Str("error", resp.Error).Msg("Failed to calculate plan")
		}

		plan := resp.Plan
		if len(plan.Migrations) == 0 {
			fmt.Println("Cluster is already balanced within target variance.")
			return
		}

		// Group by source/target for display
		transfers := make(map[string]int64)
		for _, m := range plan.Migrations {
			key := fmt.Sprintf("%s -> %s", m.FromServer, m.ToServer)
			transfers[key] += m.SizeBytes
		}

		fmt.Println("REBALANCE PLAN:")
		fmt.Println()
		w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(w, "FROM\tTO\tBYTES")
		fmt.Fprintln(w, "----\t--\t-----")
		for transfer, bytes := range transfers {
			fmt.Fprintf(w, "%s\t%s\n", transfer, humanize.Bytes(uint64(bytes)))
		}
		w.Flush()

		fmt.Println()
		fmt.Printf("Total: %s (%d migrations)\n", humanize.Bytes(uint64(plan.TotalBytes)), plan.TotalChunks)
		fmt.Printf("Estimated duration: %s\n", time.Duration(plan.EstimatedDurationMs)*time.Millisecond)
		fmt.Println()
		fmt.Println("Run with --execute to perform this rebalance.")
		return
	}

	// Execute rebalance
	fmt.Println("Executing rebalance...")
	fmt.Println()

	stream, err := managerClient.ExecuteRebalance(ctx, &manager_pb.ExecuteRebalanceRequest{
		RateLimitBps:  rateLimit,
		MaxConcurrent: int32(concurrent),
		DeleteSource:  deleteSource,
	})
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to start rebalance")
	}

	// Stream progress
	for {
		progress, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			logger.Fatal().Err(err).Msg("Error receiving progress")
		}

		switch progress.Status {
		case "running":
			fmt.Printf("\rProgress: %d/%d migrations (%.1f%%) - %s/%s moved",
				progress.MigrationsCompleted,
				progress.MigrationsTotal,
				progress.ProgressPercent,
				humanize.Bytes(uint64(progress.BytesMoved)),
				humanize.Bytes(uint64(progress.BytesTotal)),
			)
		case "completed":
			fmt.Println()
			fmt.Println()
			fmt.Printf("Rebalance completed: %d migrations, %s moved\n",
				progress.MigrationsCompleted,
				humanize.Bytes(uint64(progress.BytesMoved)),
			)
		case "failed":
			fmt.Println()
			logger.Fatal().Str("error", progress.Error).Msg("Rebalance failed")
		case "cancelled":
			fmt.Println()
			fmt.Println("Rebalance cancelled")
			return
		}
	}
}
