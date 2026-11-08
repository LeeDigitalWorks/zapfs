# ZapFS — Rust redesign

An experimental, single-node S3 server built in Rust. This branch is a fresh implementation; it does not run the original Go services or read their on-disk format. The Go baseline is preserved on `main` and `archive/go-v1`; the redesign lives on `rust-v2`.

## Run

Requires Rust 1.97 or newer. From this checkout:

```sh
export ZAPFS_ACCESS_KEY_ID=local-dev
export ZAPFS_SECRET_ACCESS_KEY='<choose-a-local-secret>'
cargo run -p zapfs-rs -- --data-dir ./data
```

S3 listens at `http://127.0.0.1:8082`; the separate admin listener is `http://127.0.0.1:8085`. The first slice accepts loopback binds only. Credentials are mandatory; there are no built-in credentials. All authenticated requests currently act as one local owner. This is a development implementation, not a production deployment.

Example with curl (path-style S3, default region `us-east-1`):

```sh
curl --aws-sigv4 'aws:amz:us-east-1:s3' --user "$ZAPFS_ACCESS_KEY_ID:$ZAPFS_SECRET_ACCESS_KEY" -X PUT http://127.0.0.1:8082/example-bucket
curl --aws-sigv4 'aws:amz:us-east-1:s3' --user "$ZAPFS_ACCESS_KEY_ID:$ZAPFS_SECRET_ACCESS_KEY" --data-binary @README.md http://127.0.0.1:8082/example-bucket/readme
curl --aws-sigv4 'aws:amz:us-east-1:s3' --user "$ZAPFS_ACCESS_KEY_ID:$ZAPFS_SECRET_ACCESS_KEY" http://127.0.0.1:8082/example-bucket/readme
```

The curl command-line example exposes its credential argument to local process inspection; use an appropriate credential/config mechanism outside a disposable local demo. No request secrets or headers are intentionally logged. Avoid enabling dependency debug logging with real credentials.

## Implemented first slice

- CreateBucket, DeleteBucket, HeadBucket and ListBuckets.
- PutObject, GetObject, HeadObject and DeleteObject.
- Streamed PUT/GET, single byte ranges, Content-Type and user metadata.
- ListObjectsV2: prefix, start-after, max-keys, delimiter/common prefixes, and authenticated query-bound continuation tokens. Each page reads one KV snapshot; pages are not a retained multi-page snapshot.
- Content-MD5 and ordinary SigV4 payload-hash validation before metadata publication.
- Correct HEAD body omission; specific missing-key, nonempty-bucket, digest, and range errors. Unimplemented S3 operations use the adapter's 501 NotImplemented response.
- Separate process readiness (`GET /health`) and Prometheus request counter (`GET /metrics`) on the admin listener. Credential creation/revocation endpoints are not implemented yet.

The accepted roadmap also includes CreateMultipartUpload, UploadPart, CompleteMultipartUpload, AbortMultipartUpload, ListParts and ListMultipartUploads. They currently return 501; no success is simulated. The complete MPU ownership/recovery design is in the architecture document.

## Storage and limits

`data/metadata.redb` is the transactional metadata authority. `data/blobs/` contains opaque, generation-owned files; user object names are never used as file paths. Data and its directory entry are synced before the manifest commits. Failed metadata commits make the store unavailable until recovery; candidate data is retained when the commit outcome is ambiguous. Open file readers remain valid through overwrites on Unix. Old generations are reclaimed after successful publication/deletion, with orphan reconciliation on exclusive startup.

The default object limit is 1 GiB (`--max-object-bytes`). Streams use 64 KiB buffers; synchronous storage runs on blocking workers. Connections are bounded (default 32), headers are bounded, and each connection has a 60-second total deadline. HTTP keepalive is disabled in this first slice. LIST pages are bounded to 1,000 entries. ListBuckets and startup reconciliation currently use memory proportional to the number of buckets/live objects respectively. Metadata publication serializes through one process mutex.

Still unsupported: MPU; dynamic credentials and grants; multi-tenancy; versioning; ACLs; conditional requests; URL-encoded list output; optional S3 checksum algorithms; SSE; compression; deduplication; replication; erasure coding; TLS; and virtual-hosted addressing. Feature-bearing headers/queries are explicitly rejected. Some SDKs enable optional checksum features automatically; compatibility must be tested with those client settings. The protocol test uses an independent SigV4 client.

Startup validates referenced file types and lengths, not a full checksum scrub. Restart tests do not prove power-loss durability. Fault-injected sync/commit errors, full corruption detection, graceful in-flight draining, and real AWS SDK compatibility remain follow-up gates. Schema v1 is explicit; migration tools do not exist yet.

## Validation

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build
python3 rust/tests/protocol.py
```

The Python test has no third-party dependencies and uses temporary data plus synthetic credentials. Build artifacts stay in one ignored root `target/`; debug symbols and incremental compilation are disabled to limit disk growth. No database daemon, container stack, vendored dependencies, or benchmark corpus is needed.

## Design

- [Architecture and decisions](docs/rust-v2-architecture.md)
- [Interactive explorer](docs/architecture.html) — open locally in a browser; GitHub does not execute it inline.

The existing Apache-2.0 license is retained. This is an independent implementation; no proprietary source is included.
