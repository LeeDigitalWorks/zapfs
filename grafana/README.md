# ZapFS Grafana Dashboards

Pre-built Grafana dashboards for monitoring ZapFS clusters.

## Dashboards

| Dashboard | UID | Description |
|-----------|-----|-------------|
| [Cluster Overview](overview.json) | `zapfs-overview` | High-level cluster health, request rates, storage usage |
| [Manager](manager.json) | `zapfs-manager` | Raft consensus, leader election, FSM performance |
| [Metadata](metadata.json) | `zapfs-metadata` | S3 API metrics, database performance, events, GC |
| [File Server](fileserver.json) | `zapfs-fileserver` | Chunk storage, deduplication, storage utilization |

## Installation

### Option 1: Import via Grafana UI

1. Open Grafana and go to **Dashboards > Import**
2. Upload the JSON file or paste its contents
3. Select your Prometheus data source
4. Click **Import**

### Option 2: Provisioning

Add to your Grafana provisioning configuration:

```yaml
# /etc/grafana/provisioning/dashboards/zapfs.yaml
apiVersion: 1
providers:
  - name: 'ZapFS'
    orgId: 1
    folder: 'ZapFS'
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    options:
      path: /path/to/zapfs/grafana
```

### Option 3: Docker Compose

```yaml
services:
  grafana:
    image: grafana/grafana:latest
    volumes:
      - ./grafana:/var/lib/grafana/dashboards/zapfs:ro
      - ./grafana-provisioning:/etc/grafana/provisioning:ro
```

## Prerequisites

- Prometheus data source configured in Grafana
- ZapFS services exposing metrics on `/debug/metrics` endpoint

## Metrics Endpoint

ZapFS exposes Prometheus metrics on the debug HTTP server:

```bash
# Manager (default port 8051)
curl http://localhost:8051/debug/metrics

# Metadata (default port 8084)
curl http://localhost:8084/debug/metrics

# File Server (default port 8080)
curl http://localhost:8080/debug/metrics
```

## Prometheus Scrape Configuration

```yaml
scrape_configs:
  - job_name: 'zapfs-manager'
    static_configs:
      - targets: ['manager1:8051', 'manager2:8051', 'manager3:8051']
    metrics_path: /debug/metrics

  - job_name: 'zapfs-metadata'
    static_configs:
      - targets: ['metadata1:8084', 'metadata2:8084']
    metrics_path: /debug/metrics

  - job_name: 'zapfs-fileserver'
    static_configs:
      - targets: ['fileserver1:8080', 'fileserver2:8080']
    metrics_path: /debug/metrics
```

## Key Metrics

### Manager Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `zapfs_raft_state` | Gauge | Raft state (0=Follower, 1=Candidate, 2=Leader, 3=Shutdown) |
| `zapfs_raft_is_leader` | Gauge | 1 if this node is the leader |
| `zapfs_raft_term` | Gauge | Current Raft term |
| `zapfs_raft_peers` | Gauge | Number of peers in the cluster |
| `zapfs_raft_leader_changes_total` | Counter | Total leader elections |
| `zapfs_raft_fsm_apply_duration_seconds` | Histogram | FSM apply latency |
| `zapfs_manager_collections_total` | Gauge | Total buckets in manager |
| `zapfs_manager_data_loss_detected` | Gauge | 1 if data loss detected |

### Metadata Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `zapfs_db_query_duration_seconds` | Histogram | Database query latency |
| `zapfs_db_queries_total` | Counter | Total database queries (by operation, status) |
| `zapfs_db_connections_active` | Gauge | Active database connections |
| `zapfs_db_connections_idle` | Gauge | Idle database connections |
| `zapfs_events_emitted_total` | Counter | S3 events emitted (by type) |
| `zapfs_events_queue_depth` | Gauge | Events pending delivery |
| `zapfs_gc_runs_total` | Counter | GC runs |
| `zapfs_gc_chunks_deleted_total` | Counter | Chunks deleted by GC |
| `zapfs_gc_bytes_reclaimed_total` | Counter | Bytes reclaimed by GC |

### File Server Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `zapfs_storage_chunks_total` | Gauge | Total chunks stored |
| `zapfs_storage_chunks_bytes_total` | Gauge | Total bytes stored |
| `zapfs_storage_chunks_zero_ref_total` | Gauge | Chunks pending GC |
| `zapfs_storage_chunk_operations_total` | Counter | Chunk operations (create, delete, deduplicate) |
| `zapfs_storage_chunk_dedupe_hits_total` | Counter | Successful deduplication hits |

## Alerting

Example alert rules for Prometheus Alertmanager:

```yaml
groups:
  - name: zapfs
    rules:
      - alert: ZapFSNoLeader
        expr: max(zapfs_raft_is_leader) == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "No Raft leader elected"
          description: "ZapFS cluster has no leader for more than 1 minute"

      - alert: ZapFSDataLoss
        expr: zapfs_manager_data_loss_detected == 1
        for: 0m
        labels:
          severity: critical
        annotations:
          summary: "Potential data loss detected"
          description: "Manager detected inconsistency with metadata services"

      - alert: ZapFSHighErrorRate
        expr: |
          sum(rate(zapfs_db_queries_total{status="error"}[5m])) /
          sum(rate(zapfs_db_queries_total[5m])) > 0.05
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High error rate"
          description: "More than 5% of requests are failing"

      - alert: ZapFSHighLatency
        expr: |
          histogram_quantile(0.99, sum(rate(zapfs_db_query_duration_seconds_bucket[5m])) by (le)) > 1
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High p99 latency"
          description: "p99 latency is above 1 second"
```

## Customization

All dashboards use template variables:
- `$datasource` - Prometheus data source
- `$instance` - Filter by specific instance(s)

Modify thresholds and time ranges as needed for your environment.
