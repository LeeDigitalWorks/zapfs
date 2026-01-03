# ZapFS

[![Go Version](https://img.shields.io/github/go-mod/go-version/LeeDigitalWorks/zapfs)](https://go.dev/)
[![Build Status](https://img.shields.io/github/actions/workflow/status/LeeDigitalWorks/zapfs/ci.yml?branch=main)](https://github.com/LeeDigitalWorks/zapfs/actions)
[![Coverage](https://img.shields.io/codecov/c/github/LeeDigitalWorks/zapfs)](https://codecov.io/gh/LeeDigitalWorks/zapfs)
[![Go Report Card](https://goreportcard.com/badge/github.com/LeeDigitalWorks/zapfs)](https://goreportcard.com/report/github.com/LeeDigitalWorks/zapfs)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![GitHub Stars](https://img.shields.io/github/stars/LeeDigitalWorks/zapfs?style=social)](https://github.com/LeeDigitalWorks/zapfs/stargazers)

A distributed object storage system with an S3-compatible API, written in Go.

## Architecture

```mermaid
flowchart TB
    subgraph Clients["S3 Clients"]
        C1["AWS SDK"]
        C2["s3cmd"]
        C3["mc"]
    end

    subgraph MetadataLayer["Metadata Service (:8082 HTTP, :8083 gRPC)"]
        direction TB
        API["S3 API Gateway<br/>• AWS Signature (SigV4/V2/Presigned/POST)<br/>• Bucket & object operations<br/>• Multipart, versioning, ACLs"]

        subgraph InternalClients["Internal Clients"]
            MC["Manager Client<br/>• IAM sync<br/>• Bucket registration<br/>• Placement"]
            FC["File Client<br/>• Chunk read/write<br/>• Streaming uploads<br/>• Replication"]
        end

        API --> MC & FC
    end

    subgraph ControlPlane["Manager Cluster (:8050 gRPC, :8051 Raft)"]
        MG1["Manager 1<br/>(Leader)"]
        MG2["Manager 2"]
        MG3["Manager 3"]

        MG1 <-->|Raft| MG2 & MG3
    end

    subgraph DataPlane["File Servers (:8081 gRPC)"]
        F1["File Server 1"]
        F2["File Server 2"]
        F3["File Server N"]
    end

    subgraph Storage["Storage Layer"]
        FS["Local/S3 Storage<br/>• Content-hash dedup<br/>• Erasure coding (RS)<br/>• RefCount GC<br/>• Replication (2+ copies)"]
    end

    subgraph Database["MySQL / Vitess"]
        DB1[("buckets, objects<br/>multipart_uploads<br/>ACLs, policies")]
        DB2[("usage_events<br/>usage_daily")]
        DB3[("tasks<br/>(task queue)")]
    end

    Clients -->|HTTP| API
    MC -->|gRPC| ControlPlane
    FC -->|gRPC| DataPlane
    DataPlane -->|gRPC| ControlPlane
    F1 & F2 & F3 --> FS
    MetadataLayer -->|SQL| Database
```

### Components

| Component | Description |
|-----------|-------------|
| **Manager** | Raft-based control plane for cluster coordination, IAM, and placement |
| **Metadata** | S3-compatible API gateway with authentication and request routing |
| **File** | Chunk storage servers with content-hash deduplication, erasure coding, and RefCount-based GC |

## Quick Start

### Docker Compose (Development)

```bash
cd docker
docker compose up -d

# Test with AWS CLI
aws --endpoint-url=http://localhost:8082 s3 mb s3://test-bucket
aws --endpoint-url=http://localhost:8082 s3 cp README.md s3://test-bucket/
aws --endpoint-url=http://localhost:8082 s3 ls s3://test-bucket/
```

### Kubernetes (Helm)

```bash
cd k8s/zapfs
helm dependency update
helm install zapfs . -n zapfs --create-namespace
```

## Configuration

### IAM Credentials

Default development credentials (change in production):

| Access Key | Secret Key |
|------------|------------|
| `AKIAIOSFODNN7EXAMPLE` | `wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` |

Configure via `cmd/config/iam.toml` or the Manager Admin API:

```bash
# Create a new user
curl -X POST http://localhost:8060/v1/iam/users \
  -H "Content-Type: application/json" \
  -d '{"username": "myuser"}'

# Create access key
curl -X POST http://localhost:8060/v1/iam/users/myuser/access-keys
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `ZAPFS_IAM_MASTER_KEY` | Base64-encoded 32-byte key for encrypting secrets at rest | (dev key) |
| `LOG_LEVEL` | Logging level (debug, info, warn, error) | info |

## Development

### Building

```bash
# Build binary
go build -o zapfs .

# Run tests
go test ./...

# Generate mocks
make mocks
```

### Running Locally

```bash
# Start manager (bootstrap node)
./zapfs manager --bootstrap --node_id=manager-1 --raft_dir=/tmp/raft

# Start metadata service
./zapfs metadata --manager_addr=localhost:8050 --db_driver=sqlite

# Start file server
./zapfs file --node_id=file-1
```

## Performance

ZapFS is designed for high-throughput object storage. Benchmarks use [MinIO WARP](https://github.com/minio/warp).

### Quick Benchmark

```bash
# Start benchmark environment
docker compose -f docker/docker-compose.benchmark.yml up -d

# Run quick benchmark (1 min)
./scripts/benchmark.sh --profile quick

# Run standard benchmark (5 min) with output
./scripts/benchmark.sh --profile standard --output ./results
```

### Benchmark Profiles

| Profile | Duration | Best For |
|---------|----------|----------|
| `quick` | 1 min | Sanity checks |
| `standard` | 5 min | General performance |
| `large` | 5 min | Large object workloads |
| `small` | 5 min | High-IOPS small files |
| `mixed` | 10 min | Realistic workloads |

See [docs/benchmarks/](docs/benchmarks/) for detailed methodology, environment setup, and recorded results.

## Acknowledgments

ZapFS draws inspiration from several excellent open-source projects:

| Project | Inspiration | License |
|---------|-------------|---------|
| [SeaweedFS](https://github.com/seaweedfs/seaweedfs) | S3 API handlers, signature verification | Apache 2.0 |
| [MinIO](https://github.com/minio/minio) | S3 error codes, IAM policy evaluation, erasure coding concepts | AGPL 3.0 |
| [Ceph](https://github.com/ceph/ceph) | Distributed storage architecture, placement algorithms | LGPL 2.1 |
| [HashiCorp Raft](https://github.com/hashicorp/raft) | Consensus implementation for manager cluster | MPL 2.0 |
| [Vitess](https://github.com/vitessio/vitess) | MySQL-compatible distributed database (used for metadata) | Apache 2.0 |

Some code patterns and S3 compatibility implementations were adapted from SeaweedFS under Apache 2.0 license.

## License

ZapFS is open-core software with dual licensing:

| Component | License | Description |
|-----------|---------|-------------|
| Core (`pkg/`, `cmd/`, etc.) | [Apache 2.0](LICENSE) | Open source, free forever |
| Enterprise (`enterprise/`, `*_enterprise.go`) | [Commercial](LICENSE.enterprise) | Requires license key |

### Community Edition (Default)

The default build includes all core S3 functionality:

| Category | Features |
|----------|----------|
| **Bucket Operations** | Create, Delete, List, Head, Location |
| **Object Operations** | Put, Get, Delete, Copy, Head, List (v1/v2) |
| **Multipart Upload** | Create, Upload Part, Complete, Abort, List Parts |
| **Versioning** | Enable/Suspend, List Versions, Delete Markers |
| **Access Control** | Bucket/Object ACLs, Bucket Policies |
| **Configuration** | CORS, Website Hosting, Tagging, Encryption |
| **Authentication** | AWS SigV4, SigV2, Presigned URLs, POST Policy |

```bash
go build -o zapfs .
```

### Enterprise Edition

Enterprise features require the `enterprise` build tag and a valid license key.

**Get your license at [zapfs.io](https://zapfs.io)** — Start with a 14-day free trial.

```bash
go build -tags enterprise -o zapfs-enterprise .
```

To activate your license, place the license key file at `/etc/zapfs/license.key` or set the `ZAPFS_LICENSE_KEY` environment variable:

```bash
# Option 1: File-based
sudo mkdir -p /etc/zapfs
sudo cp ~/Downloads/zapfs-license.key /etc/zapfs/license.key

# Option 2: Environment variable
export ZAPFS_LICENSE_KEY=$(cat ~/Downloads/zapfs-license.key)
```

### Enterprise Features

| Feature | License Required | Description |
|---------|------------------|-------------|
| Usage Reporting | FeatureAdvancedMetrics | Storage, bandwidth, and request tracking |
| Lifecycle Rules | FeatureLifecycle | Object expiration and transitions |
| Object Lock (WORM) | FeatureObjectLock | Retention and legal hold |
| SSE-KMS Encryption | FeatureKMS | KMS-managed encryption keys |
| Bucket Access Logging | FeatureAccessLog | S3 access logs to target bucket |
| Cross-Region Replication | FeatureMultiRegion | Replicate to remote regions |
| LDAP/AD Integration | FeatureLDAP | Directory service authentication |
| OIDC/SSO Integration | FeatureOIDC | OpenID Connect single sign-on |
| Event Notifications | FeatureEvents | S3 event notifications (SNS/SQS/Lambda) |
| Backup & DR | FeatureBackup | Raft state backup and restore |

See [LICENSE](LICENSE) and [LICENSE.enterprise](LICENSE.enterprise) for details.

## Enterprise Support

For enterprise licensing, support, and custom deployments:

- **Portal**: [zapfs.io](https://zapfs.io) — Manage licenses and subscriptions
- **Documentation**: [zapfs.io/docs](https://zapfs.io/docs) — Setup guides and API reference
