# ZapFS

A distributed object storage system with an S3-compatible API, written in Go.

## Architecture

```
                            ┌──────────────────────────────────┐
                            │  S3 Clients (AWS SDK, s3cmd...)  │
                            └──────────────────────────────────┘
                                            │
                                           HTTP
                                            ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                            Metadata Service                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │ S3 API Gateway                                                         │  │
│  │  • AWS Signature authentication (SigV4/V2/Presigned/POST)              │  │
│  │  • Bucket & object operations, multipart uploads, versioning           │  │
│  │  • ACLs, policies, CORS, encryption, tagging                           │  │
│  └────────────────────────────────────────────────────────────────────────┘  │
│                     │                                   │                    │
│                    gRPC                                gRPC                  │
│                     ▼                                   ▼                    │
│  ┌───────────────────────────┐         ┌───────────────────────────────┐     │
│  │  Manager Client           │         │  File Client                  │     │
│  │  • IAM credential sync    │         │  • Chunk read/write           │     │
│  │  • Bucket registration    │         │  • Streaming uploads          │     │
│  │  • Placement decisions    │         │  • Replication coordination   │     │
│  └───────────────────────────┘         └───────────────────────────────┘     │
└──────────────────────────────────────────────────────────────────────────────┘
           │                                                  │
          gRPC                                               gRPC
           ▼                                                  ▼
┌───────────────────────────────┐       ┌───────────────────────────────┐
│   Manager Cluster (Raft)      │←─────-│        File Servers           │
│                               │ gRPC  │                               │
│  • Raft consensus (3+ nodes)  │       │  • Chunk storage (local/S3)   │
│  • IAM authority              │       │  • Content-hash deduplication │
│  • Service registry           │       │  • Erasure coding (RS)        │
│  • Collection management      │       │  • RefCount-based GC          │
│  • Leader forwarding          │       │  • Replication (2+ copies)    │
└───────────────────────────────┘       └───────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│                              MySQL / Vitess                                  │
│  ┌─────────────────────────┐  ┌─────────────────────┐  ┌──────────────────┐  │
│  │ buckets, objects        │  │ usage_events        │  │ tasks            │  │
│  │ multipart_uploads       │  │ (partitioned/month) │  │ (task queue)     │  │
│  │ ACLs, policies, configs │  │ usage_daily         │  │                  │  │
│  └─────────────────────────┘  └─────────────────────┘  └──────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
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
