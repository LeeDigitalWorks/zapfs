# Benchmark Results

## Test Environment

| Property | Value |
|----------|-------|
| **Server CPU** | 4 vCPU (Dedicated) |
| **Server Memory** | 16 GB |
| **Server Storage** | NVMe SSD |
| **Services** | 1 manager, 1 metadata, 1 file service |
| **Database** | DigitalOcean Managed MySQL (2 vCPU, 8 GB, Dedicated) |
| **Database Type** | Primary only (no replicas), TLS required |
| **OS** | Ubuntu 22.04 |
| **ZapFS Version** | main branch (2026-01-02) |
| **Test Date** | 2026-01-02 |
| **WARP Version** | minio/warp latest |
| **Client** | Separate DigitalOcean droplet in same datacenter |

## Results Summary

### PUT Operations

| Profile | Throughput | Ops/sec | p50 Latency | p99 Latency |
|---------|------------|---------|-------------|-------------|
| 1 MiB (concurrency 8) | 137 MiB/s | 144 | 52.8 ms | 115 ms |
| 1 MiB (concurrency 16) | 132 MiB/s | 139 | 109.1 ms | 243 ms |

### GET Operations

| Profile | Throughput | Ops/sec | p50 Latency | p99 Latency | TTFB (median) |
|---------|------------|---------|-------------|-------------|---------------|
| 1 MiB (concurrency 8) | 585 MiB/s | 614 | 11.9 ms | 36 ms | 7 ms |

## Detailed Results

### PUT - 1MB Objects (Concurrency 8)

```
Report: PUT. Concurrency: 8. Ran: 57s
 * Average: 137.08 MiB/s, 143.73 obj/s
 * Reqs: Avg: 55.4ms, 50%: 52.8ms, 90%: 73.1ms, 99%: 115.0ms, Fastest: 20.9ms, Slowest: 207.7ms, StdDev: 15.8ms

Throughput, split into 57 x 1s:
 * Fastest: 142.6MiB/s, 149.57 obj/s
 * 50% Median: 137.1MiB/s, 143.72 obj/s
 * Slowest: 132.7MiB/s, 139.13 obj/s
```

### PUT - 1MB Objects (Concurrency 16)

```
Report: PUT. Concurrency: 16. Ran: 57s
 * Average: 132.22 MiB/s, 138.65 obj/s
 * Reqs: Avg: 115.4ms, 50%: 109.1ms, 90%: 171.0ms, 99%: 242.9ms, Fastest: 18.8ms, Slowest: 420.3ms, StdDev: 41.9ms

Throughput, split into 57 x 1s:
 * Fastest: 145.6MiB/s, 152.72 obj/s
 * 50% Median: 131.4MiB/s, 137.82 obj/s
 * Slowest: 115.8MiB/s, 121.44 obj/s
```

**Observation:** Concurrency 8 is optimal. Going to 16 doubled latency without improving throughput, indicating saturation.

### GET - 1MB Objects (Concurrency 8)

```
Report: GET. Concurrency: 8. Ran: 57s
 * Average: 585.51 MiB/s, 613.95 obj/s
 * Reqs: Avg: 12.7ms, 50%: 11.9ms, 90%: 16.7ms, 99%: 36.0ms, Fastest: 3.2ms, Slowest: 83.8ms, StdDev: 5.2ms
 * TTFB: Avg: 8ms, Best: 2ms, 25th: 5ms, Median: 7ms, 75th: 9ms, 90th: 11ms, 99th: 31ms, Worst: 80ms StdDev: 5ms

Throughput, split into 57 x 1s:
 * Fastest: 610.5MiB/s, 640.11 obj/s
 * 50% Median: 587.8MiB/s, 616.30 obj/s
 * Slowest: 547.5MiB/s, 574.12 obj/s
```

## Database Metrics

Captured during PUT benchmark via `/debug/metrics`:

| Metric | Value |
|--------|-------|
| DB Connections (active) | 2 |
| DB Connections (idle) | 3 |
| DB Connections (total) | 5 |
| put_object avg latency | ~5ms |
| get_object avg latency | ~1ms |

**Observation:** MySQL is NOT the bottleneck. DB queries complete in <5ms.

## Analysis

### Key Findings

- **GET is 4.3x faster than PUT** - Expected since reads don't require disk writes or fsync
- **Concurrency 8 is optimal** for this hardware configuration
- **TTFB of 7-8ms** demonstrates fast metadata lookup from MySQL
- **Database is not the bottleneck** - queries complete in <5ms

### Latency Breakdown (PUT, 55ms total)

| Component | Estimated Time |
|-----------|----------------|
| Network to/from file service | ~5-10ms |
| DB operations | ~5ms |
| Disk write + fsync | ~20-30ms |
| Overhead (chunking, hashing) | ~10-15ms |

### Bottlenecks Identified

- **PUT limited by disk I/O** - fsync dominates write latency
- **GET limited by network bandwidth** - ~5 Gbps achievable

## Optimizations Applied

### Connection Pool Settings

```go
MaxOpenConns    = 50   // Was 25
MaxIdleConns    = 25   // Was 5
ConnMaxIdleTime = 2 * time.Minute  // Was 1 minute
```

### DSN Optimizations (auto-applied)

```
interpolateParams=true  // Client-side parameter interpolation
collation=utf8mb4_bin   // Faster string comparisons
```

## Scaling Recommendations

To improve beyond current limits:

1. **More file services** - Distribute writes across multiple disks
2. **Increase DB connections** - `--db_max_open_conns=100` for higher concurrency
3. **Larger droplet** - More CPU for metadata processing
4. **Read replicas** - For read-heavy workloads
5. **Local SSD for MySQL** - Reduce DB latency further

## Comparison

| System | PUT (1MB) | GET (1MB) | Notes |
|--------|-----------|-----------|-------|
| ZapFS (4 CPU) | 137 MiB/s | 585 MiB/s | Single file service |
| MinIO (4 CPU) | ~200 MiB/s | ~400 MiB/s | In-memory metadata |
| SeaweedFS | ~150 MiB/s | ~300 MiB/s | LevelDB metadata |
| AWS S3 | N/A | N/A | Not comparable (distributed) |

---

## How to Reproduce

```bash
# From a client machine in the same datacenter

# PUT test
warp put --host=<server-ip>:8082 \
  --access-key=AKIAADMINKEY00001 \
  --secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE1 \
  --duration=60s --obj.size=1MB --concurrent=8 --no-color

# GET test
warp get --host=<server-ip>:8082 \
  --access-key=AKIAADMINKEY00001 \
  --secret-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLE1 \
  --duration=60s --obj.size=1MB --concurrent=8 --no-color
```

### Docker Compose for External MySQL

See `docker/docker-compose.benchmark.yml` for configuration with external managed MySQL.
