# ZapFS v2: Rust storage and transactional KV metadata

Status: proposed architecture, not implemented. Prepared 2026-11-07 against Go baseline `e9fa201b3298fca3aa42f0496996ccf00b9f9efc`.

The companion `architecture.html` is a self-contained, interactive presentation of this proposal. It separates the existing system, incremental milestones, and a future distributed design. Neither a diagram nor a decision in this document is evidence that a guarantee has been implemented.

## Current direction after design discussion

The user accepts a fresh implementation. Start directly with milestone B: one Rust binary, embedded transactional KV, and a local storage directory. Milestone A is an optional compatibility experiment, not a prerequisite. The distributed diagram is a future design exploration, not the onboarding model. A first runnable Rust slice now exists; README.md is the authoritative implemented-feature matrix. MPU and dynamic credential administration remain designed but unimplemented. Preserve the Go baseline before replacing source; no destructive reset or remote branch change has been performed.

Normal PUT storage is one immutable file per object generation, streamed with bounded buffers. Multipart objects use immutable part files plus a committed ordered manifest, avoiding an extra full-object copy at completion. No global deduplication, manager process, cluster membership, Raft setup, external SQL or FoundationDB service is required. The existing Go LevelDB adapter is historical context, not the selected Rust metadata engine.

## 1. Problem and intended outcome

ZapFS already demonstrates a distributed S3-compatible object store. Its breadth makes a Rust side project difficult to finish: multiple services, external SQL, many S3 features, and enterprise functionality all expand the correctness surface.

Build a smaller system with explicit durability, bounded memory, and a metadata model that matches point lookups and ordered listings. Use an established embedded KV engine rather than implementing a database. Preserve the Go version for comparison. A later distributed milestone must explain metadata consensus and data replication separately.

Success means a runnable vertical slice, reproducible failure tests, and measured tradeoffs that can be defended in an interview. Rust is a learning and resource-control choice; faster throughput is a hypothesis.

Initial requested operations: CreateBucket, DeleteBucket, PutObject, GetObject (including bounded range reads), DeleteObject, ListObjectsV2, and the full basic multipart lifecycle described in section 15. HeadObject, HeadBucket and ListBuckets are now accepted initial operations. Authenticate the exposed API; local test credentials do not imply production IAM support. Reject unsupported S3 operations explicitly. Defer versioning, global deduplication, erasure coding, tiering, events, enterprise features, multi-region, and online resharding. Recognized unsupported S3 operations return HTTP 501 with the S3 NotImplemented error contract; ordinary authentication, validation, absence and precondition errors retain their specific responses.

## 2. What exists today

The Go baseline has three roles: manager (Raft, membership and placement), metadata gateway (S3 and SQL-backed metadata), and file servers (chunk storage).

Two different indexes matter:

| State | Existing implementation | Proposed direction |
|---|---|---|
| File-local chunk/object indexes | LevelDB, with Go gob values | Embedded KV with explicitly versioned portable values |
| Bucket/object namespace, listings, chunk references and replica registry | SQL-backed metadata adapters | Transactional ordered KV owned by the metadata authority |
| Cluster coordination | Go manager/Raft | Absent from first Rust build; optional compatibility only |

Evidence: [local index](https://github.com/LeeDigitalWorks/zapfs/blob/e9fa201b3298fca3aa42f0496996ccf00b9f9efc/pkg/storage/index/leveldb.go), [object transactions](https://github.com/LeeDigitalWorks/zapfs/blob/e9fa201b3298fca3aa42f0496996ccf00b9f9efc/pkg/metadata/db/postgres/objects.go), [chunk registry transactions](https://github.com/LeeDigitalWorks/zapfs/blob/e9fa201b3298fca3aa42f0496996ccf00b9f9efc/pkg/metadata/db/sql/tx_chunks.go), [file protocol](https://github.com/LeeDigitalWorks/zapfs/blob/e9fa201b3298fca3aa42f0496996ccf00b9f9efc/proto/file.proto).

Removing SQL means taking ownership of transactions, secondary indexes and recovery. It is not a driver substitution. SQL remains a valid alternative when richer metadata queries or existing operational expertise dominate.

## 3. Repository strategy

Recommendation, not yet applied remotely:

1. Tag baseline commit as `go-v1-baseline`; optionally retain an `archive/go-v1` branch if old-line fixes will continue. A tag is the better immutable reference by convention; protect it if permanence matters.
2. Develop the rewrite on `rust-v2`, with small PR branches. Keep `main` on the working Go version until the Rust acceptance gate passes.
3. Merge the accepted rewrite into `main`; retain the baseline reference. Do not archive the whole GitHub repository, which would make it read-only.

Implementation now uses local branch `rust-v2`. The Go source tree was removed from this branch after preserving `archive/go-v1` and baseline references. The redesign is published on rust-v2; the remote default branch remains main. A shallow clone minimizes downloaded history; obtain full history later only when needed.

## 4. Milestones and ownership

### A. Optional: Rust file server behind the existing Go system

Skip this milestone for the fresh-design path. If compatibility is later useful, implement the supported subset of `FileService` and the manager registration/status behavior needed to join the existing stack. Keep the existing SQL metadata authority and Go gateway. Document unsupported RPCs and metadata flags; return explicit unsupported errors rather than silently ignoring them. Validate against the existing Go client and integration fixtures.

Do not promise direct reading of gob-encoded local indexes. Use fresh synthetic data first; any real data migration needs an explicit export/import tool. Keep protocol compatibility and disk-format compatibility as separate questions.

### B. SQL-free single-node Rust system

One binary, separate modules: authenticated API, metadata transactions, storage engine, and maintenance. One process owns the embedded database and local blobs. Transport remains outside core logic. The namespace and object manifests are authoritative in the KV database; blobs are authoritative bytes. Local lookup caches are derived and rebuildable from those records only.

Use a transactional ordered KV engine. Provisional first candidate: redb, to evaluate a pure-Rust dependency and a simple embedded deployment. Benchmark it before accepting it. RocksDB is the alternative for workloads that justify its native dependency and tuning complexity. Neither engine supplies distributed consensus by itself. Verify crash durability, transaction limits, writer concurrency, backup behavior, and format compatibility for the selected release before implementation.

The [redb project](https://github.com/cberner/redb) describes its embedded Rust design; the [RocksDB overview](https://github.com/meta/rocksdb/wiki/RocksDB-Overview) describes its native storage engine, point lookups, range scans, and configurable durability. These are candidates, not performance conclusions.

### C. Optional distributed Rust system, after B is proven

The following consensus-group design is one option, not a committed requirement. Evaluate FoundationDB as the authoritative distributed metadata service before building our own replicated metadata service. FoundationDB and custom metadata consensus are alternative ownership models: do not put both in charge of the same namespace. Application data placement, blob receipts and repair remain ZapFS responsibilities with either choice.


Stateless gateways route namespace operations to one metadata consensus group. Three metadata members each maintain their own local KV state; consensus commands determine mutations. The embedded KV is a state-machine store, never a database directory shared among nodes. Select and validate an established Raft implementation in a separate ADR. Persist consensus log/hard state and snapshots according to that implementation's contract.

Start with one metadata group, not speculative per-bucket sharding. Membership and placement epochs live in the same authority. Reads require a leader/quorum freshness barrier; merely reading from the apparent leader is insufficient. Fence old leaders and old placement epochs. Losing two of three metadata members stops authoritative reads/writes. An explicitly separate stale-read mode could be considered later.

Three data nodes across independent failure domains store immutable blob generations. Proposed initial replicated policy: replication factor 3, require 3 durable receipts before publishing a new manifest. This sacrifices write availability when a target is down but gives a simple durability contract. Reads try a checksum-verified healthy copy. Loss of all copies remains data loss; replication is not backup. A later quorum policy needs a separate design and evidence.

Do not reuse the old Go manager as an independent authority for the same namespace in this final architecture. Milestone A and C have different authority boundaries.

## 5. Metadata schema and queries

All examples are logical keys; define a binary codec and property-test its ordering before choosing persisted bytes.

| Logical record | Purpose / ordering |
|---|---|
| `bucket / bucket_id` | Bucket configuration; separate name-to-ID mapping updated in the same transaction |
| `object / bucket_id / object_key` | Current immutable-generation manifest, size, checksum, generation |
| `upload / upload_id` | Durable staging intent, target generation, deadline, outcome |
| `blob / generation / ordinal` | Chunk size, checksum and durable locations |
| `retire / generation` | Obsolete generation awaiting reclamation |
| `request / request_id` | Idempotency outcome, with a documented retention window |

Encode namespace components unambiguously. Preserve the bytewise lexical order of object keys with an order-preserving escaped encoding, including terminators; a naive length prefix does not preserve that order. Use fixed-width big-endian ordered integers. Version record values and the schema. Define maximum key, manifest, chunk-count and transaction sizes.

Point GET reads one manifest. LIST scans the bucket/prefix interval, exclusive of a continuation key, with a strict page limit and byte budget. Compute the exclusive prefix upper bound correctly, including the all-0xff case. Tokens include bucket, prefix, last key and token version; validate integrity and query binding. Each page uses a consistent read transaction. Across pages, concurrent mutations may change results; do not claim a cross-page snapshot without retaining one explicitly. Implement and test delimiter/common-prefix listing as part of the accepted ListObjectsV2 contract.

Atomic namespace replacement and retirement records belong to one transaction owner. Helpers receive that transaction; they must not silently open independent transactions. Large objects write bounded staging records, then atomically publish a small manifest root. Unpublished staging records are never listed as committed objects.

## 6. Write, read and delete contracts

### Single-node PUT

1. Authenticate and validate limits. Allocate an immutable object generation and durable upload intent. Acquire bounded upload admission; stream into bounded chunk buffers.
2. Write each chunk to a unique temporary file, verify length/checksum, sync bytes, atomically publish into its generation path, and persist required directory changes. Platform-specific durability behavior must be tested. Never overwrite an existing live blob in place.
3. In a durable KV transaction, verify the upload state and overwrite precondition; publish the manifest root, mark the old generation retired, and record the request outcome. Serialize conflicting updates to the same key. Large staging sets are bounded and validated before publication.
4. Acknowledge only after durable metadata commit. A timeout after commit is an unknown client outcome; resolve through the retained request record. S3 clients without an idempotency token must not be promised exactly-once retries.

An ordinary PUT uses one full-generation blob file; streaming buffers do not require an internally chunked on-disk format. MPU uses a multi-blob ordered manifest from the first release. Data and KV are not a single filesystem transaction. The ordering deliberately permits orphaned bytes after a crash but forbids committed metadata pointing at unsynced bytes under the stated durability assumptions. A metadata commit I/O error enters recovery/unavailable state until its outcome is resolved; it does not trigger speculative blob deletion.

In milestone C, step 2 requires durable receipts from all three selected data nodes, bound to upload ID, generation and placement epoch. Step 3 is a consensus commit, not a local KV write. Publication verifies an active intent and accepted placement. Failed publication leaves collectible staged data. This is ordered publication with reconciliation, not a claim of a cross-service ACID transaction.

### GET and range GET

Read the current manifest and acquire a generation pin coordinated with reclamation before opening its chunks. Retired data cannot be removed while pinned. Stream with bounded buffers; verify integrity and surface corruption distinctly from absence. Distributed reads use expiring, bounded read leases tracked by the metadata authority; renewal and fencing must be implemented before distributed reclamation is enabled. A node unable to validate reclamation authority retains data.

### DELETE and garbage collection

DELETE atomically removes the current mapping and writes a retirement record. It returns after that logical delete is durable; physical space reclamation is asynchronous. First build uses generation-owned chunks, not shared cross-object deduplication, so deleting one generation cannot delete another object's data.

Reclamation checks durable intent state, retired-generation state, read pins/leases and generation identity. Upload expiry and manifest commit serialize through the same metadata authority: an expired/aborted upload cannot later publish. A grace period alone is not a safety mechanism. Distributed deletion uses authorized generation-specific tasks; completion is idempotent and retryable. Uncertain metadata, partitions, or ambiguous leases pause reclamation rather than guessing.

Global content deduplication is a later feature requiring transactional references, generation fencing and concurrent-reuse tests. SHA-256 checksums are still useful without shared blob ownership.

## 7. Failure model and recovery

| Failure boundary | Required behavior |
|---|---|
| Crash during temporary write | Intent remains uncommitted; bounded recovery removes abandoned temporary bytes |
| Blob durable, manifest not committed | Existing object stays visible; resolve intent before cleanup |
| Commit durable, response lost | New generation visible; request outcome resolves retry ambiguity where supported |
| Two writers replace the same key | Defined serialization / conditional conflict; one current manifest, every displaced generation retired |
| Delete overlaps an active reader | Reader pin protects that generation; new GET sees absence after delete commit |
| Disk full / sync failure | Fail before acknowledgement; preserve old committed object and reserve recovery headroom |
| Bit corruption / missing blob | Integrity error; distributed milestone repairs from a verified replica |
| Metadata quorum lost | Distributed milestone stops authoritative operations and GC; resume after quorum recovery |
| Data-node partition | New writes fail under all-three policy; committed reads succeed only where a verified replica is reachable |

Startup checks database recovery and reconciles upload/retirement records before readiness. Reconciliation is checkpointed and bounded; corruption is not automatically repaired by silently discarding records. A restart test is weaker evidence than a power-loss test, so label results accordingly.

Backup requires a consistent metadata snapshot plus all referenced immutable blobs, protected against GC for the backup lifetime. Restore into an isolated directory and verify checksums and listings. Distributed snapshots alone do not back up object bytes. Define a backup retention policy before claiming recoverability from total-cluster loss.

## 8. Resource budget and evidence

Initial local budget: one checkout, one Cargo target directory, no vendored dependencies, no container stack for milestone B. Cap synthetic input at 1–2 GiB; count replicas, temporary generations, database growth and logs against a 10–20 GiB working budget. This is a target, not a measured Rust build size. Avoid eager creation of 65,536 directory leaves; create needed directories lazily. Keep benchmarks and data paths explicit and ignored by Git.

Admission control bounds active uploads, chunk bytes, blocking I/O jobs, queues, retries and deadlines. Avoid blocking the async executor with synchronous KV/disk calls. Record peak RSS, allocation pressure, p50/p95/p99, throughput, bytes written, sync time, metadata commit time, queue delay and repair/GC backlog. Use a fixed unique synthetic corpus so deduplication and repeated cache hits do not inflate results. Report warm/cold cache conditions separately.

Compare the same supported features, durability policy, hardware, concurrency and dataset against Go. Historical repository benchmark numbers are not a current baseline or proof that Rust will win. Multi-node behavior on one laptop tests protocols, not independent failure domains or production scale.

## 9. Acceptance gates and decision records

Before calling B complete: byte-exact round trips and cross-chunk ranges; ordered prefix pagination including arbitrary UTF-8 and boundary bytes; concurrent overwrite/delete/read schedules; crash injection at every publication boundary; lost-response replay; ENOSPC/sync failures; canceled uploads; corruption detection; backup/restore; bounded memory under stalled clients; supported S3 subset tested with a real client. Before C: leader changes, stale-leader fencing, partitions, repair, expired upload attempts, lease expiry/renewal, and GC races with deterministic barriers.

Keep short ADRs for: KV engine; generation-owned chunks versus deduplication; acknowledgement/durability policy; metadata consensus; key encoding/list semantics; on-disk migration. Each records alternatives, costs, evidence, and a condition that would reopen the decision. Outstanding design work: concrete engine and Raft release selection, manifest limits, chunk size, lease timing/fencing, metadata snapshot/install protocol and backup tooling.

## 10. Interview explanation

Two-minute narrative, using future tense until implemented:

“The original ZapFS separated S3 metadata, coordination and chunk storage, but running it required multiple services and SQL. I am narrowing the Rust version to make durability and resource use explicit. The local chunk index was already KV-backed; the bigger change is replacing SQL metadata transactions with an ordered transactional KV model. I publish immutable bytes before atomically publishing their manifest, accepting recoverable orphaned bytes rather than risking acknowledged metadata that references incomplete data. I defer shared deduplication to simplify ownership and deletion. Once the single-node implementation survives fault injection, the distributed design will replicate metadata through consensus and require durable data receipts before namespace publication.”

Then show one overwrite failure timeline, the LIST key layout, and measured results. Explain why SQL was reasonable originally; explain what KV makes simpler and which responsibilities it moves into the application. Discuss the single metadata group's throughput ceiling and the all-three data policy's availability cost. Avoid broad CAP labels as a substitute for per-operation guarantees. Do not claim production scale, fault tolerance or a Rust speedup before the relevant tests and measurements exist.

## 11. Proposed repository presentation cleanup

About description: `An experimental S3-compatible object store exploring durable storage, distributed systems, and a Rust redesign.`

Website: clear the defunct `zapfs.io` field. Once the architecture is merged, the field can point to its GitHub-rendered design document; do not publish a link to an unpushed local document. Separately replace stale README portal/trial/support links with an accurate historical-status notice. Keep existing license texts and attribution intact.

The About description above was applied and the website field cleared through the signed-in Chrome session on 2026-11-07. The separate in-app browser and CLI had been signed out; Chrome was already authenticated. README portal/trial/support links have not yet been changed.


## 12. Why embedded KV first; when FoundationDB fits

LevelDB is the existing Go local engine. It offers ordered keys and atomic write batches, but not the general distributed transactional metadata authority the new design needs. The first Rust engine candidate remains redb, selected provisionally for embedded ACID transactions and a Rust implementation. Choose based on correctness and representative workloads, not a claim that SQL or another KV is inherently slow.

FoundationDB is an ordered, distributed transactional KV database. It is a serious candidate when shared highly available metadata is a product requirement. It would replace the custom namespace-consensus service in the future diagram; application-specific records and invariants still need a design. Its service deployment, client libraries and cluster administration add an external dependency to onboarding. It does not atomically commit files written to independent ZapFS blob disks.

Keep metadata transactions short. The documented FoundationDB limits include 100,000-byte values, 10,000,000 bytes of affected data per transaction, and transactions limited to about five seconds. Stream uploads outside any metadata transaction; commit a small manifest pointer afterward. Rust client binding selection and native client packaging require a separate evaluation.

Sources: [FoundationDB overview](https://apple.github.io/foundationdb/), [limitations](https://apple.github.io/foundationdb/known-limitations.html), [LevelDB features and limits](https://github.com/google/leveldb), [redb](https://github.com/cberner/redb).

## 13. The index tells us where the bytes are

Two maps separate user names from physical storage without creating two services:

- Namespace: `(bucket_id, object_key) -> {generation_id, length, checksum, layout_version}`.
- Generation manifest: `generation_id -> {layout: single_file, volume_id, blob_id, length, checksum}`.

The storage module resolves an internal volume ID and opaque blob ID to a controlled relative file path. Never concatenate a user-supplied object key into a filesystem path. For a normal PUT the file starts at offset zero, so range reads need only a seek/positional read plus the requested length. An MPU manifest contains ordered logical ranges and immutable part-file IDs; reads map the requested range to the intersecting parts. Do not invent per-object placement services or pack offsets yet.

Multipart manifests already list logical ranges and part locations. A future erasure-coded layout can additionally list stripe IDs, shard positions and durable replica locations. The local data-node index then resolves shard/blob IDs to local paths (or pack-file offsets if packing is later justified). A metadata owner knows expected locations; actual presence and integrity are verified by the storage nodes and reconciliation. The manifest is authoritative; a cache of it is not a second authority.

No deduplication means identical uploads own independent generations and bytes. Checksums detect corruption without determining ownership. Overwrite publishes a fresh generation; deletion retires only that object's old generation. This removes the global shared-chunk reference-count problem from the first version.

## 14. Striping, replication and recovery roadmap

Striping splits data across devices or nodes; it does not itself supply redundancy. Pure striping can improve parallelism but loses the object if a required fragment is lost. Replication stores complete copies. Erasure coding adds parity fragments so a bounded number of missing fragments can be reconstructed.

An illustrative Reed–Solomon `4+2` stripe has four data shards and two parity shards. Any four valid shards can reconstruct its data, assuming the selected code has that property. It uses approximately 1.5x raw payload capacity before padding, metadata and temporary repair space; three replicas use approximately 3x. Six independent placement domains are needed to make a two-node-loss claim with one shard per node. Six processes on one laptop do not provide six independent hardware failure domains. See [Ceph's erasure-code explanation](https://docs.ceph.com/en/latest/rados/operations/erasure-code/).

Recommended delivery order:

1. Single-node crash recovery: temporary files, durable publication, orphan reconciliation, checksum verification and restore from backup. This does not recover bytes after total disk loss.
2. Multi-node replication: explicit copy locations, acknowledged durability policy, scrubbing, repair queue and throttled replacement copies. Membership changes and stale repair/delete commands need epochs and generation fencing.
3. Optional erasure coding for sufficiently large objects: bounded stripe buffers, versioned codec and stripe size, shard checksums, placement rules, partial final stripes, range reads, degraded reads and repair.

For the first EC implementation, publish a new generation only after all `k+m` shards are durable and its manifest commits. Choose degraded-write behavior later; do not acknowledge a fully protected object when parity is absent. Repair reads at least k verified shards, reconstructs the missing shard into a new temporary location, verifies and syncs it, and conditionally updates the manifest only if the generation/placement epoch is still current. Rate-limit repair and reserve free space. Preserve read leases or pins while deleting obsolete copies. Metadata itself needs replication/backup; parity over object bytes does not reconstruct a lost namespace.

Do not build in-place partial-stripe updates initially. Full-object overwrite creates a new immutable generation, avoiding the parity write-hole problem. Begin with the manifest contract and failure tests, not a promise to support every layout. Erasure coding, replication, and backup address different failure modes.


## 15. Initial S3 operation contract

This matrix defines operations, not just HTTP verbs: dispatch must distinguish query parameters, resource type and relevant headers. For example, PUT of an object, PUT of an MPU part, and a copy request must not accidentally share a success path.

| Group | Accepted initial operations |
|---|---|
| Bucket namespace | CreateBucket, DeleteBucket |
| Object data | PutObject, GetObject, DeleteObject |
| Listing | ListObjectsV2, including prefix, delimiter/common prefixes, start-after and opaque continuation tokens |
| Multipart | CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts, ListMultipartUploads |
| Accepted discovery/metadata operations | HeadObject, HeadBucket, ListBuckets |

ListObjectsV2 is the wire/API name; s3:ListBucket is the corresponding IAM action. Legacy ListObjects v1 is initially unsupported. CopyObject, UploadPartCopy, ACL/tagging/versioning/lifecycle/configuration APIs, browser POST uploads and other unimplemented operations return 501 NotImplemented. An unsupported header requesting an unimplemented feature is rejected, never silently ignored. An arbitrary unknown method or malformed request may require another S3/HTTP error rather than being treated as a supported operation failure.

Return S3 XML error bodies with request IDs where the method permits a body; honor HEAD's no-body contract if HEAD is implemented. A missing key stays NoSuchKey, an authentication failure stays its auth error, a non-empty bucket stays BucketNotEmpty, invalid MPU parts/order stay the relevant MPU errors, and an invalid range stays InvalidRange. A blanket catch-all 501 must not hide these failures. Determine routing/auth/error precedence through a compatibility matrix and client tests.

The accepted API is a general-purpose, non-versioned S3 subset. Do not accidentally import directory-bucket rules. DeleteBucket serializes with new writes, uses an empty-namespace check, and fences open/in-flight uploads by immutable bucket identity so deleted/recreated names cannot receive late publication. Document whether open MPU sessions are invalidated and cleaned up on successful deletion; do not claim AWS requires them to block general-purpose bucket deletion.

Sources: [ListObjectsV2](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html), [S3 errors](https://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html), [DeleteBucket](https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteBucket.html).

## 16. Multipart storage and transaction boundaries

An MPU is a durable workflow. The upload record binds an unpredictable upload ID to principal, immutable bucket ID and object key. UploadPart writes a fresh immutable part generation; part-number replacement atomically swaps its metadata and retires the displaced bytes. Every final part update rechecks that the upload is still open, including after network/disk I/O. An abort or completion race cannot resurrect it.

CompleteMultipartUpload validates the submitted ordered list of part numbers and ETags against current immutable part generations. Freeze a completion attempt with a unique fencing token. Prepare bounded manifest pages while rechecking that token; do not require one transaction proportional to all part rows. Publish a small manifest root, transition the upload outcome, and retire the old object atomically. Reads and listings expose only published manifests. Concurrent part replacement/abort must either serialize before the freeze or fail with a documented operation-specific conflict; after a crash a retry/recovery worker can take over using a fresh token. A stale worker must fail its commit even if a timeout caused takeover.

Selected part bytes become owned by the completed generation. Unselected parts, replaced parts, aborted uploads, failed attempts and displaced objects become explicit retirement work. No global reference counting is needed: distinguish upload-owned, prepared, committed-generation-owned and cleanup-owned data. Never delete a selected part merely because its staging row is removed. Completion and cleanup are separate steps with durable recovery state.

GetObject streams across ordered part ranges; a range request touches only overlapping files. Do not concatenate the entire object into a second file on completion. Internal object layout can change later without changing the S3 API. S3 upload part boundaries are client-chosen and are not automatically future erasure-coding stripe boundaries.

Validate part numbers/counts, lengths, order, ETags, permitted checksums and XML bounds. AWS currently documents 10,000 parts, part numbers 1–10,000, and 5 MiB–5 GiB parts except that the final part has no minimum. Use bounded fixtures and a separately documented local quota rather than allocating the maximum data volume. Preserve client-visible ETags through internal relocation; keep internal integrity checks distinct from S3 ETags/checksum fields. Do not assume a multipart ETag is the MD5 of the complete object.

Avoid sending success headers before the result is known where practical. If completion streams an early 200, the response body must still encode a later S3 error correctly; a 200 status alone does not prove MPU completion. Track durable outcomes for lost responses and define retry behavior against the S3 API, not a custom exactly-once promise.

Sources: [CompleteMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_CompleteMultipartUpload.html), [AbortMultipartUpload](https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html), [MPU limits](https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html).

## 17. Admin, control and data boundaries without onboarding overhead

Treat key creation/revocation as an admin/security boundary outside the S3 object API. It is not KMS: access credentials and data-encryption keys have different lifecycles. An admin module/API owns credential lifecycle, bucket grants and later node admission. The data API verifies SigV4 and authorizes each request against the effective policy. It must not synchronously call an external credential-creation service for every chunk. Bound any credential cache staleness, especially revocation, and define fail-closed behavior. SigV4 verification needs access to secret-derived signing material; a password-style one-way hash alone is insufficient.

Single-node packaging can offer one command with an authenticated, separately bound admin listener plus the S3 listener, or an admin CLI operating through that boundary. One process initially owns the KV store; a CLI does not open the same database independently while the server is running. Logical separation does not require separate daemons on day one. Distributed packaging can later run an admin/control service, stateless API gateways and storage nodes independently. The metadata/index authority remains part of the foreground object path; control-plane separation does not remove its consistency requirements.

Transferable design principles to implement independently: explicit operation dispatch; metadata names separated from physical locations; bounded resumable workflows with attempt fencing; immutable publication followed by asynchronous cleanup; and logical control/data separation with a simple initial deployment. No proprietary source, production topology, credentials, company-specific schemas or internal benchmark results belong in this side-project repository. This document records general requirements and an independent design; it contains no imported implementation.
