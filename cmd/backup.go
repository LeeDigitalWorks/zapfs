// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/LeeDigitalWorks/zapfs/proto"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/dustin/go-humanize"
	"github.com/hashicorp/raft"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var backupCmd = &cobra.Command{
	Use:   "backup",
	Short: "Backup and restore operations",
	Long: `Backup and restore operations for ZapFS manager state.

Backup exports the Raft FSM state (buckets, IAM, service registry) to a JSON file.
Restore initializes a new Raft cluster from a backup file.

This is an enterprise feature requiring the FeatureBackup license.`,
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Help()
	},
}

var backupCreateCmd = &cobra.Command{
	Use:   "create",
	Short: "Create a backup of manager state",
	Long: `Create a backup of the manager's Raft FSM state.

The backup includes:
- Service registry (file servers, metadata servers)
- Collections (buckets)
- Topology and collections versions
- Placement policy
- Region configuration

Example:
  zapfs backup create --manager-addr localhost:8050 --output /backups/manager.json`,
	Run: runBackupCreate,
}

var backupStatusCmd = &cobra.Command{
	Use:   "status",
	Short: "Show backup status information",
	Long: `Show the current state of the manager that would be backed up.

Displays:
- Whether this node is the Raft leader
- Topology version
- Collections version
- Counts of registered services and collections`,
	Run: runBackupStatus,
}

var backupRestoreCmd = &cobra.Command{
	Use:   "restore",
	Short: "Restore manager state from a backup",
	Long: `Restore manager state from a backup file.

This command initializes a new Raft cluster with state from the backup.
Use this for disaster recovery when all manager nodes have lost their data.

WARNING: This will overwrite any existing Raft data in the target directory.

Example:
  zapfs backup restore --input /backups/manager.json --data-dir /var/lib/zapfs/raft`,
	Run: runBackupRestore,
}

var backupListCmd = &cobra.Command{
	Use:   "list",
	Short: "List available backups",
	Long: `List all available backups from the manager's backup scheduler.

Shows backup ID, creation time, size, and metadata versions.`,
	Run: runBackupList,
}

var backupDeleteCmd = &cobra.Command{
	Use:   "delete <backup-id>",
	Short: "Delete a backup",
	Long: `Delete a backup by ID.

Example:
  zapfs backup delete backup-1735849200`,
	Args: cobra.ExactArgs(1),
	Run:  runBackupDelete,
}

func init() {
	rootCmd.AddCommand(backupCmd)
	backupCmd.AddCommand(backupCreateCmd)
	backupCmd.AddCommand(backupStatusCmd)
	backupCmd.AddCommand(backupRestoreCmd)
	backupCmd.AddCommand(backupListCmd)
	backupCmd.AddCommand(backupDeleteCmd)

	// Create backup flags
	backupCreateCmd.Flags().String("manager-addr", "localhost:8050", "Manager gRPC address")
	backupCreateCmd.Flags().StringP("output", "o", "", "Output file path (required)")
	backupCreateCmd.Flags().String("description", "", "Optional description for this backup")
	backupCreateCmd.MarkFlagRequired("output")

	// Status flags
	backupStatusCmd.Flags().String("manager-addr", "localhost:8050", "Manager gRPC address")

	// Restore flags
	backupRestoreCmd.Flags().StringP("input", "i", "", "Input backup file path (required)")
	backupRestoreCmd.Flags().String("data-dir", "", "Raft data directory (required)")
	backupRestoreCmd.MarkFlagRequired("input")
	backupRestoreCmd.MarkFlagRequired("data-dir")

	// List flags
	backupListCmd.Flags().String("manager-addr", "localhost:8050", "Manager gRPC address")

	// Delete flags
	backupDeleteCmd.Flags().String("manager-addr", "localhost:8050", "Manager gRPC address")
}

func runBackupCreate(cmd *cobra.Command, args []string) {
	addr, _ := cmd.Flags().GetString("manager-addr")
	output, _ := cmd.Flags().GetString("output")
	description, _ := cmd.Flags().GetString("description")

	// Create gRPC client
	client, err := proto.NewManagerClient(addr, false, 1,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to connect to manager at %s: %v\n", addr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	fmt.Printf("Creating backup from manager at %s...\n", addr)

	resp, err := client.CreateBackup(ctx, &manager_pb.CreateBackupRequest{
		Description: description,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Backup failed: %v\n", err)
		os.Exit(1)
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Error: Backup failed: %s\n", resp.Error)
		os.Exit(1)
	}

	// Ensure output directory exists
	outputDir := filepath.Dir(output)
	if outputDir != "" && outputDir != "." {
		if err := os.MkdirAll(outputDir, 0755); err != nil {
			fmt.Fprintf(os.Stderr, "Error: Failed to create output directory: %v\n", err)
			os.Exit(1)
		}
	}

	// Write to file with restricted permissions (backup contains sensitive data)
	if err := os.WriteFile(output, resp.SnapshotData, 0600); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to write backup file: %v\n", err)
		os.Exit(1)
	}

	// Parse snapshot to show summary
	var snapshot map[string]interface{}
	if err := json.Unmarshal(resp.SnapshotData, &snapshot); err == nil {
		fmt.Printf("\nBackup created successfully!\n")
		fmt.Printf("  Backup ID:    %s\n", resp.BackupId)
		fmt.Printf("  Output:       %s\n", output)
		fmt.Printf("  Size:         %s\n", humanize.Bytes(uint64(len(resp.SnapshotData))))

		if fileServices, ok := snapshot["file_services"].(map[string]interface{}); ok {
			fmt.Printf("  File servers: %d\n", len(fileServices))
		}
		if metadataServices, ok := snapshot["metadata_services"].(map[string]interface{}); ok {
			fmt.Printf("  Metadata servers: %d\n", len(metadataServices))
		}
		if collections, ok := snapshot["collections"].(map[string]interface{}); ok {
			fmt.Printf("  Collections:  %d\n", len(collections))
		}
	} else {
		fmt.Printf("Backup saved to %s (ID: %s)\n", output, resp.BackupId)
	}
}

func runBackupStatus(cmd *cobra.Command, args []string) {
	addr, _ := cmd.Flags().GetString("manager-addr")

	// Create gRPC client
	client, err := proto.NewManagerClient(addr, false, 1,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to connect to manager at %s: %v\n", addr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.GetBackupStatus(ctx, &manager_pb.GetBackupStatusRequest{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to get backup status: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Manager Backup Status (%s)\n", addr)
	fmt.Printf("================================\n")
	fmt.Printf("  Is Leader:           %v\n", resp.IsLeader)
	fmt.Printf("  Topology Version:    %d\n", resp.TopologyVersion)
	fmt.Printf("  Collections Version: %d\n", resp.CollectionsVersion)
	fmt.Printf("  File Services:       %d\n", resp.FileServicesCount)
	fmt.Printf("  Metadata Services:   %d\n", resp.MetadataServicesCount)
	fmt.Printf("  Collections:         %d\n", resp.CollectionsCount)

	if !resp.IsLeader {
		fmt.Printf("\nNote: This node is not the leader. Backups should be taken from the leader.\n")
	}
}

func runBackupRestore(cmd *cobra.Command, args []string) {
	input, _ := cmd.Flags().GetString("input")
	dataDir, _ := cmd.Flags().GetString("data-dir")

	// Read backup file
	data, err := os.ReadFile(input)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to read backup file: %v\n", err)
		os.Exit(1)
	}

	// Validate JSON structure
	var snapshot map[string]interface{}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Invalid backup format: %v\n", err)
		os.Exit(1)
	}

	// Show backup info
	fmt.Printf("Backup file: %s\n", input)
	fmt.Printf("Size:        %s\n", humanize.Bytes(uint64(len(data))))

	if fileServices, ok := snapshot["file_services"].(map[string]interface{}); ok {
		fmt.Printf("File servers: %d\n", len(fileServices))
	}
	if metadataServices, ok := snapshot["metadata_services"].(map[string]interface{}); ok {
		fmt.Printf("Metadata servers: %d\n", len(metadataServices))
	}
	if collections, ok := snapshot["collections"].(map[string]interface{}); ok {
		fmt.Printf("Collections:  %d\n", len(collections))
	}

	// Create data directory
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create data directory: %v\n", err)
		os.Exit(1)
	}

	// Check if directory is empty (safety check)
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to read data directory: %v\n", err)
		os.Exit(1)
	}
	if len(entries) > 0 {
		fmt.Fprintf(os.Stderr, "Error: Data directory is not empty: %s\n", dataDir)
		fmt.Fprintf(os.Stderr, "Please remove existing data or choose an empty directory.\n")
		os.Exit(1)
	}

	// Create a Raft snapshot store and inject the snapshot
	snapshotStore, err := raft.NewFileSnapshotStore(dataDir, 3, os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create snapshot store: %v\n", err)
		os.Exit(1)
	}

	// Create a synthetic snapshot from the backup data
	// The backup is the FSM state serialized as JSON
	sink, err := snapshotStore.Create(
		raft.SnapshotVersionMax,
		1,        // Last applied index (doesn't matter for restore)
		1,        // Last applied term
		raft.Configuration{}, // Empty config, will be bootstrapped
		0,                    // Config index
		nil,                  // Transport (not needed for file sink)
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to create snapshot sink: %v\n", err)
		os.Exit(1)
	}

	// Write the backup data to the snapshot
	if _, err := io.Copy(sink, bytes.NewReader(data)); err != nil {
		sink.Cancel()
		fmt.Fprintf(os.Stderr, "Error: Failed to write snapshot: %v\n", err)
		os.Exit(1)
	}

	if err := sink.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to finalize snapshot: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("\nRestore complete!\n")
	fmt.Printf("Snapshot written to: %s\n", dataDir)
	fmt.Printf("\nNext steps:\n")
	fmt.Printf("1. Start the manager with: zapfs manager --data-dir %s\n", dataDir)
	fmt.Printf("2. The manager will restore state from the snapshot on startup\n")
	fmt.Printf("3. Other manager nodes can join via: zapfs manager --join <this-node-addr>\n")
}

func runBackupList(cmd *cobra.Command, args []string) {
	addr, _ := cmd.Flags().GetString("manager-addr")

	// Create gRPC client
	client, err := proto.NewManagerClient(addr, false, 1,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to connect to manager at %s: %v\n", addr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.ListBackups(ctx, &manager_pb.ListBackupsRequest{})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to list backups: %v\n", err)
		os.Exit(1)
	}

	if len(resp.Backups) == 0 {
		fmt.Println("No backups found.")
		fmt.Println("\nNote: Backups are only tracked when backup scheduling is enabled.")
		return
	}

	fmt.Printf("Available Backups (%d total)\n", len(resp.Backups))
	fmt.Printf("================================\n")

	for _, b := range resp.Backups {
		createdAt := time.Unix(b.CreatedAt, 0)
		scheduledStr := ""
		if b.Scheduled {
			scheduledStr = " [scheduled]"
		}

		fmt.Printf("\n%s%s\n", b.BackupId, scheduledStr)
		fmt.Printf("  Created:      %s\n", createdAt.Format("2006-01-02 15:04:05"))
		fmt.Printf("  Size:         %s\n", humanize.Bytes(uint64(b.SizeBytes)))
		fmt.Printf("  File:         %s\n", b.Filename)
		fmt.Printf("  Topology:     v%d\n", b.TopologyVersion)
		fmt.Printf("  Collections:  v%d (%d buckets)\n", b.CollectionsVersion, b.CollectionsCount)
		fmt.Printf("  Services:     %d file, %d metadata\n", b.FileServicesCount, b.MetadataServicesCount)
		if b.Description != "" {
			fmt.Printf("  Description:  %s\n", b.Description)
		}
	}
}

func runBackupDelete(cmd *cobra.Command, args []string) {
	addr, _ := cmd.Flags().GetString("manager-addr")
	backupID := args[0]

	// Create gRPC client
	client, err := proto.NewManagerClient(addr, false, 1,
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to connect to manager at %s: %v\n", addr, err)
		os.Exit(1)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.DeleteBackup(ctx, &manager_pb.DeleteBackupRequest{
		BackupId: backupID,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: Failed to delete backup: %v\n", err)
		os.Exit(1)
	}

	if !resp.Success {
		fmt.Fprintf(os.Stderr, "Error: %s\n", resp.Error)
		os.Exit(1)
	}

	fmt.Printf("Backup %s deleted successfully.\n", backupID)
}
