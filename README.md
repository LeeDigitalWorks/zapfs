# ZapFS

A distributed object storage system with an S3-compatible API, written in Go. Not ready for production.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              S3 Clients (AWS SDK, s3cmd, etc.)              │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Metadata Service (S3 API)                         │
│  • S3-compatible HTTP API           • AWS SigV4/V2 Authentication           │
│  • Request filtering & routing      • Bucket/Object metadata (MySQL)        │
│  • IAM credential sync from Manager                                         │
└─────────────────────────────────────────────────────────────────────────────┘
                           │                              │
                           ▼                              ▼
┌──────────────────────────────────────┐  ┌──────────────────────────────────┐
│       Manager Cluster (Raft)         │  │         File Servers             │
│  • Raft consensus (3+ nodes)         │  │  • Chunk storage                 │
│  • IAM authority (users, keys)       │  │  • Content-hash deduplication    │
│  • Service registry                  │  │  • Erasure coding                │
│  • Placement decisions               │  │  • RefCount-based GC             │
│  • Collection (bucket) management    │  │                                  │
└──────────────────────────────────────┘  └──────────────────────────────────┘
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
| Enterprise (`enterprise/`) | [Commercial](LICENSE.enterprise) | Requires license key |

The core functionality is fully open source and free to use. Enterprise features (audit logging, LDAP/SSO, external KMS, multi-region replication) require a commercial license.

See [LICENSE](LICENSE) and [LICENSE.enterprise](LICENSE.enterprise) for details.
