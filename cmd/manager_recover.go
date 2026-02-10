// Copyright 2025 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/LeeDigitalWorks/zapfs/pkg/logger"
	"github.com/LeeDigitalWorks/zapfs/proto/manager_pb"

	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var managerRecoverCmd = &cobra.Command{
	Use:   "manager-recover",
	Short: "Recover manager state from metadata services",
	Long: `Recover manager collection state from metadata service databases.

This command is used for disaster recovery when Raft data is lost but
metadata databases are still intact. It queries metadata services for
their bucket lists and rebuilds the manager's collection registry.

IMPORTANT: This should only be used after Raft data loss. Running this
on a healthy cluster will skip all existing collections (safe but unnecessary).

Examples:
  # Recover using auto-discovered metadata services
  zapfs manager-recover --manager-addr=localhost:8050

  # Recover from specific metadata services
  zapfs manager-recover --manager-addr=localhost:8050 \
    --metadata-addrs=meta1:8083,meta2:8083

  # Dry run to see what would be recovered
  zapfs manager-recover --manager-addr=localhost:8050 --dry-run
`,
	Run: runManagerRecover,
}

func init() {
	rootCmd.AddCommand(managerRecoverCmd)

	f := managerRecoverCmd.Flags()
	f.String("manager-addr", "localhost:8050", "Manager gRPC address (must be leader)")
	f.StringSlice("metadata-addrs", nil, "Metadata service addresses (comma-separated)")
	f.Bool("dry-run", false, "Show what would be recovered without making changes")
	f.Bool("yes", false, "Skip confirmation prompt")
	f.Duration("timeout", 5*time.Minute, "Operation timeout")
	f.String("admin-token", "", "Admin token for authenticating to the manager (must match manager's --admin_token)")

}

func runManagerRecover(cmd *cobra.Command, args []string) {
	managerAddr, _ := cmd.Flags().GetString("manager-addr")
	metadataAddrs, _ := cmd.Flags().GetStringSlice("metadata-addrs")
	dryRun, _ := cmd.Flags().GetBool("dry-run")
	skipConfirm, _ := cmd.Flags().GetBool("yes")
	timeout, _ := cmd.Flags().GetDuration("timeout")
	adminToken, _ := cmd.Flags().GetString("admin-token")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	// Connect to manager
	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if adminToken != "" {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(&tokenCredential{token: adminToken}))
	}

	conn, err := grpc.NewClient(managerAddr, dialOpts...)
	if err != nil {
		logger.Error().Err(err).Str("addr", managerAddr).Msg("failed to create gRPC client")
		os.Exit(1)
	}
	defer conn.Close()

	client := manager_pb.NewManagerServiceClient(conn)

	// Show what we're about to do
	fmt.Println("Manager Recovery Mode")
	fmt.Println("=====================")
	fmt.Printf("Manager:   %s\n", managerAddr)
	if len(metadataAddrs) > 0 {
		fmt.Printf("Metadata:  %s\n", strings.Join(metadataAddrs, ", "))
	} else {
		fmt.Println("Metadata:  (auto-discover from registered services)")
	}
	fmt.Printf("Dry run:   %v\n", dryRun)
	fmt.Println()

	if dryRun {
		fmt.Println("DRY RUN MODE - No changes will be made")
		fmt.Println()
	}

	// Confirmation prompt
	if !skipConfirm && !dryRun {
		fmt.Print("This will recover collections from metadata services. Continue? [y/N]: ")
		var response string
		fmt.Scanln(&response)
		if strings.ToLower(response) != "y" && strings.ToLower(response) != "yes" {
			fmt.Println("Aborted.")
			os.Exit(0)
		}
	}

	// Execute recovery
	resp, err := client.RecoverCollections(ctx, &manager_pb.RecoverCollectionsRequest{
		MetadataAddresses: metadataAddrs,
		DryRun:            dryRun,
	})
	if err != nil {
		logger.Error().Err(err).Msg("recovery failed")
		os.Exit(1)
	}

	// Print results
	fmt.Println()
	fmt.Println("Results")
	fmt.Println("-------")
	fmt.Printf("Recovered: %d\n", resp.GetCollectionsRecovered())
	fmt.Printf("Skipped:   %d (already existed)\n", resp.GetCollectionsSkipped())
	fmt.Printf("Failed:    %d\n", resp.GetCollectionsFailed())
	fmt.Println()
	fmt.Printf("Message: %s\n", resp.GetMessage())

	if len(resp.GetRecoveredNames()) > 0 && len(resp.GetRecoveredNames()) <= 20 {
		fmt.Println()
		fmt.Println("Recovered collections:")
		for _, name := range resp.GetRecoveredNames() {
			fmt.Printf("  + %s\n", name)
		}
	}

	if len(resp.GetFailedNames()) > 0 {
		fmt.Println()
		fmt.Println("Failed collections:")
		for _, name := range resp.GetFailedNames() {
			fmt.Printf("  ! %s\n", name)
		}
	}

	if !resp.GetSuccess() {
		os.Exit(1)
	}
}
