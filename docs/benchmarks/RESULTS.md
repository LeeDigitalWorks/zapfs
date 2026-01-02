# Benchmark Results

## Test Environment

| Property | Value |
|----------|-------|
| **Hardware** | [CPU model, cores, RAM, storage type] |
| **OS** | [Operating system and version] |
| **ZapFS Version** | [Git commit or release version] |
| **Test Date** | YYYY-MM-DD |
| **Configuration** | Single node |
| **WARP Version** | [WARP version used] |

## Results Summary

### PUT Operations

| Profile | Throughput | Ops/sec | p50 Latency | p99 Latency |
|---------|------------|---------|-------------|-------------|
| quick (1 MiB) | - MiB/s | - | - ms | - ms |
| standard (mixed) | - MiB/s | - | - ms | - ms |
| large (100 MiB) | - MiB/s | - | - ms | - ms |
| small (4 KiB) | - MiB/s | - | - ms | - ms |

### GET Operations

| Profile | Throughput | Ops/sec | p50 Latency | p99 Latency |
|---------|------------|---------|-------------|-------------|
| quick (1 MiB) | - MiB/s | - | - ms | - ms |
| standard (mixed) | - MiB/s | - | - ms | - ms |
| large (100 MiB) | - MiB/s | - | - ms | - ms |
| small (4 KiB) | - MiB/s | - | - ms | - ms |

### DELETE Operations

| Profile | Ops/sec | p50 Latency | p99 Latency |
|---------|---------|-------------|-------------|
| quick | - | - ms | - ms |
| standard | - | - ms | - ms |

### LIST Operations

| Profile | Ops/sec | p50 Latency | p99 Latency |
|---------|---------|-------------|-------------|
| quick | - | - ms | - ms |
| standard | - | - ms | - ms |

### Mixed Workload (70% GET, 30% PUT)

| Profile | Throughput | Ops/sec | p50 Latency | p99 Latency |
|---------|------------|---------|-------------|-------------|
| mixed | - MiB/s | - | - ms | - ms |

### Multipart Upload

| Profile | Throughput | Ops/sec | p50 Latency | p99 Latency |
|---------|------------|---------|-------------|-------------|
| multipart (500 MiB) | - MiB/s | - | - ms | - ms |

## Analysis

### Key Findings

- [Summary of performance characteristics]
- [Notable strengths or weaknesses]

### Bottlenecks Identified

- [Any performance bottlenecks observed]
- [Resource constraints hit during testing]

### Comparison Notes

- [Comparison with previous versions if applicable]
- [Comparison with other systems if available]

## Recommendations

- [Tuning recommendations based on results]
- [Optimal use cases for this configuration]

---

## How to Reproduce

```bash
# Start benchmark environment
docker compose -f docker/docker-compose.benchmark.yml up -d

# Wait for healthy status
docker compose -f docker/docker-compose.benchmark.yml ps

# Run benchmarks
./scripts/benchmark.sh --profile quick --output ./docs/benchmarks/results
./scripts/benchmark.sh --profile standard --output ./docs/benchmarks/results
./scripts/benchmark.sh --profile large --output ./docs/benchmarks/results
./scripts/benchmark.sh --profile small --output ./docs/benchmarks/results

# Stop environment
docker compose -f docker/docker-compose.benchmark.yml down
```
