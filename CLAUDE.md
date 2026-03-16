# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ZapFS is a distributed object storage system with an S3-compatible API, written in Go. The system consists of three main services communicating via gRPC:

- **Manager**: Raft-based control plane for cluster coordination, IAM, service registry, and placement decisions
- **Metadata**: S3-compatible API gateway handling HTTP requests, authentication, and object/bucket metadata (MySQL/Vitess)
- **File**: Chunk storage servers with content-hash deduplication, erasure coding, and RefCount-based GC

## Build Commands

```bash
# Build (single binary — enterprise features gated by license key at runtime)
make build                    # Build ./zapfs (includes enterprise code, gated by license)
go build -o zapfs .           # Community-only build (no enterprise code compiled in)

# Test
make test                     # Unit tests
make test-race                # Unit tests with race detector
make test-cover               # Generate coverage report
go test -v ./path/to/pkg/...  # Run single package tests
go test -v -run TestName ./path/to/pkg/...  # Run single test

# Integration tests (requires Docker Compose)
make docker-up                # Start production compose
make docker-down              # Stop production compose
make integration              # Run all integration tests
make integration-s3           # S3 API tests only
make integration-all          # Full integration suite (starts minimal cluster automatically)
make integration-resiliency   # Resiliency-specific tests

# Development environment (hot reload)
make docker-dev               # Start dev environment with hot reload
make docker-dev-d             # Start dev environment detached
make docker-dev-down          # Stop dev environment
make docker-dev-logs          # View dev logs
make docker-dev-rebuild       # Rebuild dev images (clears caches)

# Minimal cluster (for benchmarks & quick tests)
make minimal-up               # Start minimal cluster (1 manager, 2 file, 1 metadata, 1 mysql)
make minimal-down             # Stop minimal cluster
make minimal-test             # Run resiliency tests on minimal cluster
make benchmark                # Run performance benchmarks on minimal cluster

# Code generation
make mocks                    # Generate mocks (mockery v3)
make mocks-clean              # Clean and regenerate mocks
make proto                    # Compile .proto files (cd proto && make protoc)

# Quality
make lint                     # Run golangci-lint
make fmt                      # Format with goimports
make vet                      # Run go vet
make staticcheck              # Run staticcheck (excludes proto/*_pb/)
make hooks-install            # Install pre-commit hook (gofmt, go vet, staticcheck, build)
```

**Note:** Ignore staticcheck warnings in `proto/*_pb/` (auto-generated) and `checkLicense` functions (enterprise feature guards).

## Architecture

```
S3 Clients → Metadata (8082 HTTP, 8083 gRPC) → Manager Cluster (8050 gRPC, Raft)
                                            → File Servers (8081 gRPC)
```

**Key packages:**
- `pkg/metadata/api/` - S3 HTTP handlers (object.go, bucket.go, multipart.go)
- `pkg/metadata/db/` - Database abstraction with Vitess (MySQL) and in-memory implementations
- `pkg/metadata/service/` - Business logic layer (object/, bucket/, multipart/, config/)
- `pkg/manager/` - Raft consensus, service registry, placement (grpc_server.go, raft_fsm.go)
- `pkg/file/` - Chunk storage, replication, GC (grpc_file.go, handler.go)
- `pkg/iam/` - Credential and policy stores (policy_evaluator.go, *_store.go)
- `pkg/storage/` - Storage backends (backend/local.go, backend/s3.go), indexing, GC
- `pkg/s3api/` - S3 compatibility (signature/, s3types/, s3err/, s3consts/)
- `proto/` - gRPC definitions (.proto files, generated code in *_pb/ directories)
- `enterprise/` - Commercial features (license/, audit/, kms/, ldap/, taskqueue/)

## Testing Patterns

- Unit tests use testify (`assert`, `require`, `mock`)
- Mocks generated via mockery v3, configured in `.mockery.yaml`, output to `mocks/`
- Integration tests require `-tags=integration` build tag
- Integration tests are in `integration/{s3,file,manager,metadata,iam}/`

## Proto Generation

Proto files are in `proto/`. To regenerate after changes:
```bash
cd proto && make protoc
```

Generated code goes to `proto/*_pb/` directories (common_pb, manager_pb, metadata_pb, file_pb, iam_pb, usage_pb).

## Environment Variables

| Variable | Description |
|----------|-------------|
| `ZAPFS_IAM_MASTER_KEY` | Base64-encoded 32-byte key for secret encryption |
| `LOG_LEVEL` | debug, info, warn, error |
| `DB_DSN` | Database connection string (e.g., `zapfs:zapfs@tcp(localhost:3306)/zapfs`) |
| `ZAPFS_LICENSE_KEY` | Enterprise license key or path |

Supported database drivers: `vitess` (default), `mysql`, `postgres`, `cockroachdb`.

Example configuration files for all services are in `cmd/config/` (manager.toml, metadata.toml, file.toml, iam.toml, security.toml, profiles.json, pools.json, tiers.toml).

## Development Credentials

Default S3 credentials for local development:
- Access Key: `AKIAIOSFODNN7EXAMPLE`
- Secret Key: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`

## Code Changes

For significant code changes, write a design plan in `workspace/plans/` before implementation:

1. **Create a design doc** - Document the problem, proposed solution, alternatives considered, and implementation steps
2. **Consider performance** - Analyze memory allocations, hot paths, and concurrency patterns. Profile before and after for performance-sensitive code
3. **No goroutine leaks** - Use `goleak` in tests for packages that spawn goroutines. Ensure proper cleanup with `defer`, context cancellation, and channel closing
4. **Test coverage** - Write unit tests for new code. Add integration tests for cross-service functionality. Run `make test-race` to catch data races
5. **Track TODOs** - Document feature gaps and future work in `workspace/todo.md`
6. **Metrics** - Add metrics for the new feature and use prometheus to expose them.
7. **License checks** - If the code is enterprise-only, add license checks to the code.
8. **Documentation** - Update the documentation to reflect the changes.
9. **License Header** - Add the license header to the code.
