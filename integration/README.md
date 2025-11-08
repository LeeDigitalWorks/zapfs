# Integration Tests

This directory contains integration tests for all ZapFS services. Integration tests verify that services work together correctly in a deployed environment.

## Test Structure

- **`file/`** - File server integration tests
- **`manager/`** - Manager service integration tests
- **`metadata/`** - Metadata service integration tests (includes database tests)
- **`s3/`** - S3 API integration tests
- **`iam/`** - IAM service integration tests
- **`testutil/`** - Shared test utilities and helpers

## Prerequisites

- Go 1.21+ with `integration` build tag support
- Docker and Docker Compose (for Docker Compose testing)
- kubectl and access to a Kubernetes cluster (for Kubernetes testing)
- AWS SDK credentials configured (for S3 tests)

## Running Integration Tests

Integration tests require the `integration` build tag. The easiest way to run them is using the Makefile commands.

### Available Makefile Commands

The project includes convenient Makefile targets for running integration tests:

```bash
# Docker Compose management
make docker-up          # Start all services
make docker-down        # Stop all services
make docker-logs        # View service logs
make docker-rebuild     # Rebuild and restart services

# Integration test suites
make integration        # Run all integration tests (file, s3, metadata, manager)
make integration-s3      # Run S3 API tests only
make integration-file   # Run file server tests only
make integration-metadata # Run metadata service tests only
make integration-manager # Run manager service tests only
```

**Note:** All Makefile integration test commands automatically include `-race -cover -v` flags for race detection, coverage reporting, and verbose output.

### Quick Start (Docker Compose)

The easiest way to run integration tests is against a local Docker Compose environment:

```bash
# 1. Start all services
make docker-up

# 2. Wait for services to be ready (check health endpoints)
make docker-logs  # Or: cd docker && docker compose ps

# 3. Run all integration tests
make integration

# Or run specific test suites:
make integration-s3          # S3 API tests
make integration-file        # File server tests
make integration-metadata    # Metadata service tests
make integration-manager     # Manager service tests

# 4. Cleanup when done
make docker-down
```

### Manual Commands (Alternative)

If you prefer to run commands manually:

```bash
# Start services
cd docker
docker compose up -d

# Wait for services to be ready
docker compose ps

# Run tests from project root
cd ..
go test -tags=integration -v ./integration/...

# Cleanup
cd docker
docker compose down -v
```

**Note:** The Makefile commands automatically include `-race -cover -v` flags for better test coverage and race detection.

## Environment Configurations

### Docker Compose Environment

Integration tests are configured by default to work with the Docker Compose setup in `docker/docker-compose.yml`. The default addresses match the exposed ports:

**Default Service Addresses:**
- File Server 1: `localhost:8081` (gRPC)
- File Server 2: `localhost:8091` (gRPC)
- Metadata Server: `localhost:8083` (gRPC) / `localhost:8082` (S3 API HTTP)
- Manager Server 1: `localhost:8050` (gRPC)
- Manager Server 2: `localhost:8052` (gRPC) - *Note: Not exposed in docker-compose*
- Manager Server 3: `localhost:8054` (gRPC) - *Note: Not exposed in docker-compose*
- MySQL: `localhost:3306`

**Environment Variables for Docker Compose:**

```bash
# File servers
export FILE_SERVER_1_ADDR="localhost:8081"
export FILE_SERVER_2_ADDR="localhost:8091"
export FILE_SERVER_2_REPL_ADDR="file-2:8091"  # Internal Docker network address

# Metadata server
export METADATA_SERVER_ADDR="localhost:8083"

# Manager cluster
export MANAGER_SERVER_1_ADDR="localhost:8050"
export MANAGER_SERVER_2_ADDR="localhost:8052"  # Not exposed, use service name
export MANAGER_SERVER_3_ADDR="localhost:8054"  # Not exposed, use service name

# S3 API
export S3_ENDPOINT="http://localhost:8082"
export S3_REGION="us-east-1"
export S3_ACCESS_KEY_ID="AKIAADMINKEY00001"
export S3_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE1"

# Database (for metadata tests)
export DB_DSN="zapfs:zapfs@tcp(localhost:3306)/zapfs"
```

**Running Tests Against Docker Compose:**

Using Makefile (recommended):
```bash
# Start services
make docker-up

# Wait for services to be healthy
make docker-logs  # Monitor logs, or check: cd docker && docker compose ps

# Run all integration tests
make integration

# Or run specific test suites
make integration-s3
make integration-file
make integration-manager
make integration-metadata

# Cleanup
make docker-down
```

Using manual commands:
```bash
# Start services
cd docker
docker compose up -d

# Wait for all services to be healthy
docker compose ps

# Run tests (default addresses work automatically)
cd ..
go test -tags=integration -v ./integration/s3
go test -tags=integration -v ./integration/file
go test -tags=integration -v ./integration/manager
go test -tags=integration -v ./integration/metadata

# Or run all tests
go test -tags=integration -v ./integration/...

# Cleanup
cd docker
docker compose down -v
```

### Kubernetes Environment

To run integration tests against a Kubernetes deployment, you need to:

1. **Deploy ZapFS to Kubernetes** (using your deployment manifests)
2. **Expose services** via NodePort, LoadBalancer, or Ingress
3. **Configure environment variables** to point to Kubernetes service addresses

**Environment Variables for Kubernetes:**

```bash
# Get service addresses (example using kubectl port-forward or LoadBalancer IPs)
# File servers - use NodePort or LoadBalancer IPs
export FILE_SERVER_1_ADDR="<file-server-1-service>:8081"
export FILE_SERVER_2_ADDR="<file-server-2-service>:8091"
export FILE_SERVER_2_REPL_ADDR="<file-server-2-service>:8091"  # Internal service name

# Metadata server
export METADATA_SERVER_ADDR="<metadata-service>:8083"

# Manager cluster
export MANAGER_SERVER_1_ADDR="<manager-1-service>:8050"
export MANAGER_SERVER_2_ADDR="<manager-2-service>:8050"
export MANAGER_SERVER_3_ADDR="<manager-3-service>:8050"

# S3 API - use Ingress or LoadBalancer endpoint
export S3_ENDPOINT="http://<s3-ingress-or-lb>"
export S3_REGION="us-east-1"
export S3_ACCESS_KEY_ID="AKIAADMINKEY00001"
export S3_SECRET_ACCESS_KEY="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE1"

# Database - use service name or external endpoint
export DB_DSN="zapfs:zapfs@tcp(<mysql-service>:3306)/zapfs"
```

**Using kubectl port-forward (for local testing):**

```bash
# Port forward services to localhost
kubectl port-forward svc/file-server-1 8081:8081 &
kubectl port-forward svc/file-server-2 8091:8091 &
kubectl port-forward svc/metadata 8083:8083 &
kubectl port-forward svc/metadata 8082:8082 &  # S3 API
kubectl port-forward svc/manager-1 8050:8050 &
kubectl port-forward svc/mysql 3306:3306 &

# Now use Docker Compose addresses (localhost)
go test -tags=integration -v ./integration/...

# Cleanup port forwards
pkill -f "kubectl port-forward"
```

**Using Service IPs/Endpoints directly:**

```bash
# Get service endpoints
kubectl get svc -n zapfs

# Set environment variables to service IPs or DNS names
export FILE_SERVER_1_ADDR="file-server-1.zapfs.svc.cluster.local:8081"
export METADATA_SERVER_ADDR="metadata.zapfs.svc.cluster.local:8083"
# ... etc

# Run tests
go test -tags=integration -v ./integration/...
```

## Test Categories

### S3 API Tests (`integration/s3/`)

Tests the full S3 API implementation including:
- Bucket CRUD operations
- Object CRUD operations
- SSE-C encryption
- List operations with pagination
- Concurrent operations

**Run S3 tests:**
```bash
make integration-s3
# Or manually:
go test -tags=integration -v ./integration/s3
```

### File Server Tests (`integration/file/`)

Tests file server functionality:
- Basic file operations
- Replication
- Edge cases
- Concurrent operations
- **GC & Chunk Storage:**
  - Admin endpoints (index stats, chunk info, force GC)
  - Chunk-based storage with content-hash deduplication
  - RefCount tracking and decrement
  - Concurrency stress tests (20 concurrent uploads, mixed operations)

**Run file server tests:**
```bash
make integration-file
# Or manually:
go test -tags=integration -v ./integration/file
```

### Manager Tests (`integration/manager/`)

Tests manager service:
- Cluster operations
- Leader election
- Raft consensus

**Run manager tests:**
```bash
make integration-manager
# Or manually:
go test -tags=integration -v ./integration/manager
```

### Metadata Tests (`integration/metadata/`)

Tests metadata service:
- Database interface (in-memory and MySQL/Vitess)
- Encryption field storage
- Schema validation

**Run metadata tests:**
```bash
# In-memory database (default)
make integration-metadata
# Or manually:
go test -tags=integration -v ./integration/metadata

# MySQL/Vitess database
export DB_DSN="zapfs:zapfs@tcp(localhost:3306)/zapfs"
make integration-metadata
# Or manually:
go test -tags=integration -v ./integration/metadata -run TestDatabaseInterface
```

### IAM Tests (`integration/iam/`)

Tests IAM service:
- gRPC connections
- Credential creation via Admin API

**Run IAM tests:**
```bash
# Note: IAM tests are not included in 'make integration' yet
go test -tags=integration -v ./integration/iam
```

## Troubleshooting

### Services Not Ready

If tests fail with connection errors, verify services are healthy:

**Docker Compose:**
```bash
# Using Makefile
make docker-logs              # View all logs
cd docker && docker compose ps  # Check service status
cd docker && docker compose logs <service-name>  # View specific service logs

# Or manually
cd docker
docker compose ps
docker compose logs <service-name>
```

**Kubernetes:**
```bash
kubectl get pods -n zapfs
kubectl logs <pod-name> -n zapfs
kubectl get svc -n zapfs
```

### Port Conflicts

If you get "address already in use" errors:
- Check if services are already running: `docker compose ps` or `kubectl get pods`
- Stop conflicting services or change port mappings

### Database Connection Issues

For metadata database tests:
- Ensure MySQL is running and healthy
- Verify `DB_DSN` environment variable is correct
- Check network connectivity to database

### S3 API Connection Issues

For S3 tests:
- Verify `S3_ENDPOINT` points to the metadata server's HTTP port (8082)
- Check that credentials are set correctly
- Ensure the metadata service is running and healthy

## Continuous Integration

### GitHub Actions / CI Pipeline

Example CI configuration:

```yaml
# Start Docker Compose services
- name: Start services
  run: make docker-up

# Wait for services to be ready
- name: Wait for services
  run: |
    timeout 120 bash -c 'until cd docker && docker compose ps | grep -q "healthy"; do sleep 2; done'

# Run integration tests
- name: Run integration tests
  run: make integration

# Cleanup
- name: Cleanup
  if: always()
  run: make docker-down
```

Or using Makefile commands directly:
```yaml
- name: Start services and run tests
  run: |
    make docker-up
    sleep 30  # Wait for services
    make integration
    make docker-down
```

## Best Practices

1. **Always use the `integration` build tag** - Tests won't run without it
2. **Clean up test resources** - Tests should clean up buckets/objects they create
3. **Use unique names** - Use `testutil.UniqueID()` for bucket/object names
4. **Check service health** - Wait for services to be ready before running tests
5. **Isolate test environments** - Use separate Docker Compose projects or Kubernetes namespaces for parallel test runs

## Additional Resources

- [Metadata Integration Tests README](./metadata/README.md) - Detailed database testing instructions
- [Docker Compose Configuration](../docker/docker-compose.yml) - Service configuration
- [Test Utilities](./testutil/) - Shared test helpers and utilities

