# ZapFS Performance Benchmarks

Performance testing for ZapFS S3-compatible API using [MinIO WARP](https://github.com/minio/warp).

## Overview

These benchmarks measure:
- **Throughput (MiB/s)**: Data transfer rate for PUT/GET operations
- **Operations per second**: Request handling capacity
- **Latency percentiles**: Response time distribution (p50, p99)

## Prerequisites

- Go 1.21+ (for WARP installation)
- Docker and Docker Compose
- ~10GB free disk space for test data

## Quick Start

```bash
# Start the benchmark environment (single-node)
docker compose -f docker/docker-compose.benchmark.yml up -d

# Wait for services to be healthy
docker compose -f docker/docker-compose.benchmark.yml ps

# Run a quick 1-minute benchmark
./scripts/benchmark.sh --profile quick

# Run standard 5-minute benchmark with saved results
./scripts/benchmark.sh --profile standard --output ./docs/benchmarks/results
```

## Test Profiles

| Profile | Duration | Concurrency | Object Size | Use Case |
|---------|----------|-------------|-------------|----------|
| `quick` | 1 min | 32 | 1 MiB | Quick sanity check |
| `standard` | 5 min | 64 | Mixed (1KB-10MB) | General performance testing |
| `large` | 5 min | 32 | 100 MiB | Large object throughput |
| `small` | 5 min | 128 | 4 KiB | High-IOPS small file workloads |
| `mixed` | 10 min | 64 | Random | Realistic mixed workload |
| `multipart` | 5 min | 16 | 500 MiB | Multipart upload performance |
| `stress` | 30 min | 128 | Continuous | Extended stress testing |

## Running Benchmarks

### Basic Usage

```bash
# Default quick profile
./scripts/benchmark.sh

# Specific profile
./scripts/benchmark.sh --profile standard

# Custom endpoint
./scripts/benchmark.sh --host localhost:8082

# Save results to directory
./scripts/benchmark.sh --profile standard --output ./results

# With TLS
./scripts/benchmark.sh --host s3.example.com:443 --tls
```

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `S3_HOST` | S3 endpoint address | `localhost:9000` |
| `AWS_ACCESS_KEY_ID` | Access key | `minioadmin` |
| `AWS_SECRET_ACCESS_KEY` | Secret key | `minioadmin` |
| `DURATION` | Test duration | `60s` |
| `CONCURRENT` | Concurrent operations | `32` |

### Command Line Options

```
-h, --host HOST        S3 endpoint (default: localhost:9000)
-a, --access-key KEY   Access key
-s, --secret-key KEY   Secret key
-d, --duration DUR     Test duration (e.g., 60s, 5m)
-c, --concurrent N     Concurrent operations
-p, --profile NAME     Test profile
-o, --output DIR       Save results to directory
-t, --tls              Use TLS/HTTPS
    --help             Show help message
```

## Understanding Results

WARP outputs detailed statistics for each operation type:

### Throughput
- **MiB/s**: Megabytes per second transferred
- Higher is better for data-intensive workloads

### Operations per Second
- **Ops/s**: Number of completed operations per second
- Critical for small object workloads

### Latency Percentiles
- **p50 (median)**: 50% of requests complete within this time
- **p99**: 99% of requests complete within this time
- **Max**: Worst-case latency observed

### Example Output

```
Operation: PUT
* Average: 156.32 MiB/s, 156.32 obj/s
* Throughput, split into 299 x 1s:
  - Fastest: 189.5 MiB/s, 189.50 obj/s
  - 50% Median: 157.2 MiB/s, 157.20 obj/s
  - Slowest: 98.4 MiB/s, 98.40 obj/s
* TTFB:
  - 50%: 12.3ms
  - 99%: 89.2ms
```

## Benchmark Environment

### Docker Compose Setup

The `docker/docker-compose.benchmark.yml` provides a minimal single-node environment:
- 1 Manager node (Raft leader)
- 1 Metadata service (S3 API)
- 1 File server (chunk storage)
- 1 MySQL instance

### Hardware Recommendations

For reproducible results:
- Dedicated machine (no competing workloads)
- SSD storage for consistent I/O
- Sufficient RAM (8GB+ recommended)
- Network isolation (localhost testing preferred)

## Recording Results

Results should be recorded in [RESULTS.md](RESULTS.md) with:
1. Test environment details (hardware, version, date)
2. Profile used and any custom parameters
3. Key metrics for each operation type
4. Observations and analysis

### Saving Raw Output

```bash
# Save WARP output to timestamped file
./scripts/benchmark.sh --profile standard --output ./docs/benchmarks/results

# Results are saved with timestamp prefix
ls ./docs/benchmarks/results/
# 2024-01-15-standard-put.csv
# 2024-01-15-standard-get.csv
# ...
```

## Comparing Results

When comparing benchmark results:
1. Use the same hardware and configuration
2. Run multiple iterations (3-5 recommended)
3. Report median values, not best-case
4. Note any environmental differences
5. Use the same WARP version

## Troubleshooting

### Services not starting
```bash
# Check service health
docker compose -f docker/docker-compose.benchmark.yml ps

# View logs
docker compose -f docker/docker-compose.benchmark.yml logs metadata
```

### Low throughput
- Ensure no other processes competing for I/O
- Check disk space (`df -h`)
- Verify network isn't the bottleneck

### Connection errors
- Wait for services to fully initialize (health checks)
- Verify endpoint and credentials
- Check firewall rules
