# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

ZapFS is a distributed object storage system with an S3-compatible API, written in Go. The system consists of three main services communicating via gRPC:

- **Manager**: Raft-based control plane for cluster coordination, IAM, service registry, and placement decisions
- **Metadata**: S3-compatible API gateway handling HTTP requests, authentication, and object/bucket metadata (MySQL/Vitess)
- **File**: Chunk storage servers with content-hash deduplication, erasure coding, and RefCount-based GC

## Build Commands

```bash
# Build
make build                    # Community edition (./zapfs)
make build-enterprise         # Enterprise edition with -tags enterprise

# Test
make test                     # Unit tests
make test-race                # Unit tests with race detector
make test-cover               # Generate coverage report
go test -v ./path/to/pkg/...  # Run single package tests
go test -v -run TestName ./path/to/pkg/...  # Run single test

# Integration tests (requires Docker Compose)
make docker-up                # Start services
make integration              # Run all integration tests
make integration-s3           # S3 API tests only
make docker-down              # Stop services

# Code generation
make mocks                    # Generate mocks (mockery v3)
make proto                    # Compile .proto files (cd proto && make protoc)

# Quality
make lint                     # Run golangci-lint
make fmt                      # Format with goimports
staticcheck ./...             # Run staticcheck (catches unused code, deprecated APIs, style issues)
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
| `DB_DSN` | MySQL connection string (e.g., `zapfs:zapfs@tcp(localhost:3306)/zapfs`) |
| `ZAPFS_LICENSE_KEY` | Enterprise license key or path |

## Development Credentials

Default S3 credentials for local development:
- Access Key: `AKIAIOSFODNN7EXAMPLE`
- Secret Key: `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY`

## Large Feature Development

For significant features or architectural changes, write a design plan in `workspace/plans/` before implementation:

1. **Create a design doc** - Document the problem, proposed solution, alternatives considered, and implementation steps
2. **Consider performance** - Analyze memory allocations, hot paths, and concurrency patterns. Profile before and after for performance-sensitive code
3. **No goroutine leaks** - Use `goleak` in tests for packages that spawn goroutines. Ensure proper cleanup with `defer`, context cancellation, and channel closing
4. **Test coverage** - Write unit tests for new code. Add integration tests for cross-service functionality. Run `make test-race` to catch data races
5. **Track TODOs** - Document feature gaps and future work in `workspace/todo.md`
6. **Metrics** - Add metrics for the new feature and use prometheus to expose them.
