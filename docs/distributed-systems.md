# ZapFS Distributed Systems Design

This document describes the distributed systems principles, consistency guarantees, failure handling, and recovery mechanisms in ZapFS.

## Architecture Overview

ZapFS is a distributed object storage system with three main services:

```mermaid
flowchart TB
    subgraph Clients["S3 Clients"]
        C1[Client 1]
        C2[Client 2]
        C3[Client N]
    end

    subgraph MetadataLayer["Metadata Layer"]
        M1[Metadata 1<br/>:8082 HTTP<br/>:8083 gRPC]
        M2[Metadata 2]
        M3[Metadata N]
        DB[(MySQL/Vitess<br/>Object Metadata)]
    end

    subgraph ControlPlane["Control Plane (Raft Cluster)"]
        MG1[Manager 1<br/>Leader<br/>:8050 gRPC<br/>:8051 Raft]
        MG2[Manager 2<br/>Follower]
        MG3[Manager 3<br/>Follower]
    end

    subgraph DataPlane["Data Plane"]
        F1[File Server 1<br/>:8081 gRPC]
        F2[File Server 2]
        F3[File Server N]
        D1[(Local Storage<br/>Chunks)]
        D2[(Local Storage)]
        D3[(Local Storage)]
    end

    C1 & C2 & C3 -->|S3 API| M1 & M2 & M3
    M1 & M2 & M3 -->|Metadata| DB
    M1 & M2 & M3 -->|Topology/Placement| MG1
    M1 & M2 & M3 -->|Chunk Read/Write| F1 & F2 & F3
    MG1 <-->|Raft Consensus| MG2 & MG3
    F1 --- D1
    F2 --- D2
    F3 --- D3
```

### Service Roles

| Service | Role | Stateful? | Consistency Model |
|---------|------|-----------|-------------------|
| **Manager** | Control plane: service registry, IAM, placement, collections | Yes (Raft) | Strong (linearizable writes) |
| **Metadata** | S3 API gateway, object metadata, routing | Stateless* | Eventual (caches topology) |
| **File** | Chunk storage, content-hash deduplication | Yes (local) | Eventual (async replication) |

*Metadata services are stateless but rely on MySQL/Vitess for durable metadata.

---

## Consistency Guarantees

### Control Plane: Strong Consistency via Raft

The Manager cluster uses [HashiCorp Raft](https://github.com/hashicorp/raft) for strong consistency:

```mermaid
sequenceDiagram
    participant C as Client
    participant L as Leader
    participant F1 as Follower 1
    participant F2 as Follower 2

    C->>L: CreateBucket("my-bucket")
    L->>L: Append to log
    L->>F1: AppendEntries
    L->>F2: AppendEntries
    F1-->>L: ACK
    F2-->>L: ACK
    L->>L: Commit (majority)
    L->>L: Apply to FSM
    L-->>C: Success

    Note over L,F2: State replicated to majority<br/>before client response
```

**Replicated State (via Raft):**
- Service registry (file servers, metadata servers)
- Collections (buckets) and their metadata
- IAM users, credentials, and policies
- Placement policy and replication factors
- Topology version counters

**Guarantees:**
| Property | Guarantee |
|----------|-----------|
| **Writes** | Linearizable - committed to majority before ACK |
| **Reads on Leader** | Linearizable - always current |
| **Reads on Followers** | Potentially stale (use for non-critical reads) |
| **Durability** | BoltDB persistence + 2 snapshots retained |

### Data Plane: Eventual Consistency with Replication

Object data follows an eventually consistent model with configurable replication:

```mermaid
sequenceDiagram
    participant C as S3 Client
    participant M as Metadata
    participant MG as Manager
    participant F1 as File Server 1
    participant F2 as File Server 2

    C->>M: PUT /bucket/key
    M->>MG: GetReplicationTargets(size, n=2)
    MG-->>M: [F1, F2]

    par Parallel Write
        M->>F1: StreamWrite(chunks)
        M->>F2: StreamWrite(chunks)
    end

    F1-->>M: ACK + chunk IDs
    F2-->>M: ACK + chunk IDs

    M->>M: Store metadata + chunk refs
    M-->>C: 200 OK

    Note over F1,F2: Data replicated to N servers<br/>before client response
```

**Replication Model:**
- Configurable replication factor per storage profile (default: 2)
- Synchronous writes to all replicas (no write quorum - all-or-nothing)
- Chunk-level content-hash deduplication
- Reference counting for safe garbage collection

---

## Failure Tolerance

### Manager Failures

```mermaid
flowchart LR
    subgraph Healthy["Healthy Cluster (3 nodes)"]
        L[Leader]
        F1[Follower 1]
        F2[Follower 2]
    end

    subgraph FailedLeader["Leader Failure"]
        LX[Leader ❌]
        F1a[Follower 1]
        F2a[Follower 2]
    end

    subgraph Recovery["After Election (~1.5s)"]
        LXr[Old Leader ❌]
        NL[New Leader ✓]
        F2r[Follower]
    end

    Healthy --> FailedLeader
    FailedLeader -->|Election Timeout| Recovery
```

| Failure Scenario | Tolerance | Recovery Time |
|------------------|-----------|---------------|
| 1 of 3 managers down | ✅ Survives | Instant (if follower) |
| Leader failure | ✅ Survives | ~1.5s (election timeout) |
| 2 of 3 managers down | ❌ Read-only | Manual recovery |
| All managers down | ❌ Unavailable | Restart from snapshots |

**Leader Election:**
- Heartbeat timeout: ~150ms
- Election timeout: ~1.5s (randomized to prevent split-brain)
- Non-leader requests are forwarded to leader automatically

### File Server Failures

```mermaid
flowchart TB
    subgraph Normal["Normal Operation"]
        direction LR
        O1[Object A] --> C1[Chunk 1]
        C1 --> R1[Replica: F1]
        C1 --> R2[Replica: F2]
    end

    subgraph Failure["File Server 1 Fails"]
        direction LR
        O2[Object A] --> C2[Chunk 1]
        C2 --> R1X[F1 ❌]
        C2 --> R2a[F2 ✓]
    end

    subgraph Recovery["After Reconciliation"]
        direction LR
        O3[Object A] --> C3[Chunk 1]
        C3 --> R2b[F2 ✓]
        C3 --> R3[F3 ✓ new replica]
    end

    Normal --> Failure
    Failure -->|Reconciliation| Recovery
```

| Failure Scenario | Impact | Recovery |
|------------------|--------|----------|
| 1 file server down | Reads succeed (other replicas) | Automatic re-replication |
| Write during failure | May fail if < replication factor available | Retry with available servers |
| All replicas of chunk lost | **Data loss** | Restore from backup |

### Metadata Server Failures

Metadata servers are stateless - any instance can serve requests:

| Failure Scenario | Impact | Recovery |
|------------------|--------|----------|
| 1 metadata server down | Load balancer routes around | Instant |
| All metadata servers down | S3 API unavailable | Start new instances |
| Database (MySQL) failure | Complete outage | Database HA/failover |

---

## Drift Detection and Reconciliation

ZapFS uses anti-entropy reconciliation to detect and repair data drift between expected state (metadata database) and actual state (file servers).

### Reconciliation Flow

```mermaid
sequenceDiagram
    participant FS as File Server
    participant MG as Manager
    participant MD as Metadata DB

    Note over FS: Periodic reconciliation<br/>(configurable interval)

    FS->>MG: GetExpectedChunks(server_id)
    MG->>MD: StreamChunksForServer(server_id)
    MD-->>MG: Stream chunk IDs
    MG-->>FS: Stream expected chunks

    FS->>FS: Compare local vs expected

    alt Orphan Chunks Found
        FS->>FS: Check grace period
        FS->>FS: Delete orphans older than grace period
    end

    alt Missing Chunks Found
        FS->>MG: ReportReconciliation(missing=[...])
        MG->>MG: Log + trigger re-replication
    end
```

### Drift Scenarios

| Scenario | Detection | Resolution |
|----------|-----------|------------|
| **Orphan chunks** (local but not in metadata) | Reconciliation scan | Delete after grace period (default: 1 hour) |
| **Missing chunks** (in metadata but not local) | Reconciliation scan | Re-replicate from another replica |
| **Stale topology cache** | Version mismatch | Topology push subscription |
| **Split-brain during partition** | Raft prevents writes | Majority side continues; minority stops |

### Grace Periods

Grace periods prevent premature deletion of valid data:

| Component | Grace Period | Purpose |
|-----------|--------------|---------|
| Orphan chunks (reconciliation) | 1 hour (configurable) | Protect in-flight uploads |
| GC (RefCount=0 chunks) | 5 minutes | Protect race conditions |
| ZeroRefSince timestamp | N/A | Track when refcount hit zero |

---

## Garbage Collection

ZapFS uses reference counting with delayed garbage collection:

```mermaid
flowchart TB
    subgraph Upload["Object Upload"]
        U1[PUT Object] --> U2[Store chunks]
        U2 --> U3[RefCount++]
    end

    subgraph Delete["Object Delete"]
        D1[DELETE Object] --> D2[Remove metadata]
        D2 --> D3[RefCount-- async]
        D3 --> D4{RefCount == 0?}
        D4 -->|Yes| D5[Set ZeroRefSince = now]
        D4 -->|No| D6[Keep chunk]
    end

    subgraph GC["GC Worker (periodic)"]
        G1[Scan chunks] --> G2{RefCount == 0?}
        G2 -->|Yes| G3{ZeroRefSince + grace < now?}
        G3 -->|Yes| G4[Delete chunk]
        G3 -->|No| G5[Skip - too new]
        G2 -->|No| G5
    end
```

### GC Safety Properties

1. **Reference Counting**: Chunks are only deleted when RefCount reaches 0
2. **Grace Period**: 5-minute delay after RefCount=0 before deletion
3. **ZeroRefSince Tracking**: Timestamp when chunk became unreferenced
4. **Jittered Scheduling**: GC runs are spread across cluster (15% jitter)
5. **Deduplication Safe**: Same chunk shared by multiple objects (RefCount > 1)

---

## Recovery Procedures

### Manager Cluster Recovery

```mermaid
flowchart TD
    A[Manager Down] --> B{Still have majority?}
    B -->|Yes| C[Automatic failover]
    B -->|No| D[Manual intervention required]

    C --> E[New leader elected ~1.5s]
    E --> F[Service resumes]

    D --> G[Identify surviving nodes]
    G --> H{Have any node with data?}
    H -->|Yes| I[Bootstrap from surviving node]
    H -->|No| J[Restore from backup]

    I --> K[Rejoin other nodes]
    J --> K
    K --> F
```

**Steps for full cluster recovery:**
1. Start one manager with existing data directory (has raft.db + snapshots)
2. The node will bootstrap as single-member cluster
3. Add other managers via `--join <leader-grpc-addr>`
4. Verify cluster health via Raft metrics

### File Server Recovery

| Scenario | Procedure |
|----------|-----------|
| Server restart | Automatic rejoin via manager registration |
| Disk failure | Replace disk, rejoin, reconciliation restores chunks |
| Complete node loss | Add new node, reconciliation triggers re-replication |

### Data Recovery from Missing Chunks

When reconciliation detects missing chunks:
1. Manager logs warning with missing chunk IDs
2. Query `chunk_replicas` table for other servers with the chunk
3. Initiate chunk copy from healthy replica
4. Update chunk registry with new replica location

---

## Observability

### Key Metrics

| Metric | Description |
|--------|-------------|
| `zapfs_raft_state` | Current Raft state (0=Follower, 1=Candidate, 2=Leader) |
| `zapfs_raft_term` | Current Raft term |
| `zapfs_raft_commit_index` | Committed log index |
| `zapfs_raft_leader_changes_total` | Leadership transitions |
| `zapfs_gc_chunks_deleted_total` | Chunks deleted by GC |
| `zapfs_gc_bytes_reclaimed_total` | Storage reclaimed by GC |

### Health Checks

- **Manager**: Ready when leader is elected (`IsClusterReady()`)
- **Metadata**: Ready when connected to database and manager
- **File**: Ready when storage backends initialized

---

## CAP Theorem Trade-offs

ZapFS makes different trade-offs for different components:

```mermaid
graph TD
    subgraph CAP["CAP Theorem"]
        C[Consistency]
        A[Availability]
        P[Partition Tolerance]
    end

    subgraph Components["ZapFS Components"]
        M[Manager: CP]
        MD[Metadata: AP*]
        F[File: AP]
    end

    C --> M
    P --> M

    A --> MD
    P --> MD

    A --> F
    P --> F
```

| Component | CAP Choice | Explanation |
|-----------|------------|-------------|
| **Manager** | CP | Strong consistency via Raft; unavailable without majority |
| **Metadata** | AP* | Highly available; relies on MySQL for consistency |
| **File** | AP | Available for reads with any replica; eventual consistency |

*Metadata consistency depends on MySQL/Vitess configuration (can be CP with synchronous replication).

---

## Summary

| Aspect | Approach |
|--------|----------|
| **Control plane consistency** | Strong (Raft with majority quorum) |
| **Data replication** | Synchronous to N replicas on write |
| **Read consistency** | Eventual (any replica can serve) |
| **Failure detection** | Service registry heartbeats + Raft timeouts |
| **Drift detection** | Periodic reconciliation scans |
| **Recovery** | Automatic re-replication + manual backup restore |
| **Garbage collection** | Reference counting with grace periods |
