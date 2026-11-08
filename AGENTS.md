# ZapFS Rust workflow

Follow the parent projects/AGENTS.md. This is the Rust redesign branch; the Go implementation is preserved on archive/go-v1. Never import proprietary code or read Go data directories as Rust data.

- Cargo workspace root: this directory; package: rust/; persistent format: versioned redb + generation-owned files.
- Transport and S3 errors belong in rust/src/api.rs; durability/metadata/files in rust/src/store.rs; runtime wiring in rust/src/main.rs.
- Explicitly reject unsupported features. Authentication must always be configured. The default listener is loopback-only and bootstrap credentials are one local owner.
- Never acknowledge publication before durable bytes and metadata. Preserve candidate bytes after ambiguous commits. Keep reader/open-file safety and bucket recreation fencing intact.
- Bound streams, queues, pages and transactions. Keep blocking disk/KV calls off async executor threads.
- Run cargo fmt --all -- --check, cargo clippy --workspace --all-targets -- -D warnings, cargo test --workspace, cargo build and python3 rust/tests/protocol.py for changes to the first slice.
- Keep implemented behavior and accepted roadmap distinct in README.md and docs/rust-v2-architecture.md. MPU/admin credential lifecycle are not implemented yet.
- Keep one ignored target/ and explicit data paths. Do not vendor dependencies or generate large fixtures.
- Existing Apache-2.0 license and attribution are retained. New source files use the existing project SPDX header convention.
