// Copyright 2026 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::sync::{Arc, mpsc};
use std::time::Duration;

use zapfs_rs::store::{Error, ListEntry, Store};

fn put(store: &Store, key: &str, body: &[u8]) {
    store
        .put_object(
            "test-bucket",
            key,
            body,
            Some("text/plain".into()),
            BTreeMap::new(),
        )
        .unwrap();
}

#[test]
fn bucket_validation_and_namespace_lifecycle() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), 1024).unwrap();
    for name in [
        "",
        "ab",
        "UPPER",
        "a..b",
        "a.-b",
        "a-.b",
        "192.168.1.1",
        "xn--name",
        "foo--x-s3",
    ] {
        assert!(
            matches!(store.create_bucket(name), Err(Error::InvalidBucketName)),
            "{name}"
        );
    }
    let bucket = store.create_bucket("test-bucket").unwrap();
    assert_eq!(store.head_bucket("test-bucket").unwrap(), bucket);
    assert!(matches!(
        store.create_bucket("test-bucket"),
        Err(Error::BucketAlreadyOwnedByYou)
    ));
    assert_eq!(store.list_buckets().unwrap(), vec![bucket]);
    put(&store, "key", b"value");
    assert!(matches!(
        store.delete_bucket("test-bucket"),
        Err(Error::BucketNotEmpty)
    ));
    store.delete_object("test-bucket", "key").unwrap();
    store.delete_object("test-bucket", "key").unwrap();
    store.delete_bucket("test-bucket").unwrap();
    assert!(matches!(
        store.head_bucket("test-bucket"),
        Err(Error::NoSuchBucket)
    ));
}

#[test]
fn immutable_generation_survives_overwrite_delete_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), 1024).unwrap();
    store.create_bucket("test-bucket").unwrap();
    put(&store, "../escape/雪", b"old bytes");
    let (old_info, mut old_file) = store.get_object("test-bucket", "../escape/雪").unwrap();
    assert_eq!(old_info.etag, "125270c450105b4a49e9421ef42e0b53");
    put(&store, "../escape/雪", b"replacement");
    let mut original = String::new();
    old_file.read_to_string(&mut original).unwrap();
    assert_eq!(original, "old bytes");
    drop(old_file);
    drop(store);
    let store = Store::open(directory.path(), 1024).unwrap();
    let (info, mut file) = store.get_object("test-bucket", "../escape/雪").unwrap();
    assert_eq!(info.size, 11);
    let mut contents = String::new();
    file.read_to_string(&mut contents).unwrap();
    assert_eq!(contents, "replacement");
    store.delete_object("test-bucket", "../escape/雪").unwrap();
    assert!(matches!(
        store.head_object("test-bucket", "../escape/雪"),
        Err(Error::NoSuchKey)
    ));
    assert_eq!(
        std::fs::read_dir(directory.path().join("blobs"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn prefix_listing_is_ordered_exclusive_and_isolated() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), 1024).unwrap();
    store.create_bucket("test-bucket").unwrap();
    store.create_bucket("other-bucket").unwrap();
    for key in ["z", "a/雪", "a/é", "a/aa", "a/a", "a/", "b", "a/😀"] {
        put(&store, key, b"x");
    }
    store
        .put_object(
            "other-bucket",
            "a/hidden",
            b"x".as_slice(),
            None,
            BTreeMap::new(),
        )
        .unwrap();
    let page = store.list_objects("test-bucket", "a/", None, 3).unwrap();
    assert_eq!(
        page.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        ["a/", "a/a", "a/aa"]
    );
    let next = store
        .list_objects("test-bucket", "a/", Some("a/aa"), 10)
        .unwrap();
    assert_eq!(
        next.iter().map(|o| o.key.as_str()).collect::<Vec<_>>(),
        ["a/é", "a/雪", "a/😀"]
    );
    assert!(
        store
            .list_objects("test-bucket", "a/", Some("z"), 10)
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .list_objects("test-bucket", "", None, 0)
            .unwrap()
            .is_empty()
    );
    let inclusive = store
        .list_objects_from("test-bucket", "a/", Some("a/aa"), true, 1)
        .unwrap();
    assert_eq!(inclusive[0].key, "a/aa");
}

#[test]
fn oversized_or_failed_upload_preserves_committed_object_and_cleans_staging() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), 4).unwrap();
    store.create_bucket("test-bucket").unwrap();
    put(&store, "key", b"safe");
    assert!(matches!(
        store.put_object(
            "test-bucket",
            "key",
            io::repeat(b'x').take(5),
            None,
            BTreeMap::new()
        ),
        Err(Error::EntityTooLarge)
    ));
    struct FailedReader;
    impl Read for FailedReader {
        fn read(&mut self, _: &mut [u8]) -> io::Result<usize> {
            Err(io::Error::other("simulated body failure"))
        }
    }
    assert!(matches!(
        store.put_object("test-bucket", "key", FailedReader, None, BTreeMap::new()),
        Err(Error::BodyRead(_))
    ));
    struct InvalidAtEof {
        inner: io::Cursor<&'static [u8]>,
    }
    impl Read for InvalidAtEof {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let length = self.inner.read(buffer)?;
            if length == 0 {
                return Err(io::Error::other("digest mismatch at EOF"));
            }
            Ok(length)
        }
    }
    let invalid = InvalidAtEof {
        inner: io::Cursor::new(b"evil"),
    };
    assert!(matches!(
        store.put_object("test-bucket", "key", invalid, None, BTreeMap::new()),
        Err(Error::BodyRead(_))
    ));
    let (_, mut file) = store.get_object("test-bucket", "key").unwrap();
    let mut body = Vec::new();
    file.read_to_end(&mut body).unwrap();
    assert_eq!(body, b"safe");
    assert_eq!(
        std::fs::read_dir(directory.path().join("blobs"))
            .unwrap()
            .count(),
        1
    );
}

#[test]
fn delimiter_listing_seeks_groups_and_resumes_at_exact_successor() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path().join("nested/data"), 1024).unwrap();
    store.create_bucket("test-bucket").unwrap();
    for key in ["a/1", "a/2", "a/3", "a0", "b", "c/1"] {
        put(&store, key, b"x");
    }
    let first = store
        .list_page("test-bucket", "", None, false, Some("/"), 1)
        .unwrap();
    assert_eq!(first.entries, [ListEntry::CommonPrefix("a/".into())]);
    assert!(first.truncated);
    assert_eq!(first.next, Some(("a0".into(), true)));
    let second = store
        .list_page("test-bucket", "", Some("a0"), true, Some("/"), 2)
        .unwrap();
    let names = second
        .entries
        .iter()
        .map(|entry| match entry {
            ListEntry::Object(info) => info.key.as_str(),
            ListEntry::CommonPrefix(prefix) => prefix.as_str(),
        })
        .collect::<Vec<_>>();
    assert_eq!(names, ["a0", "b"]);
    assert!(second.truncated);
    assert_eq!(second.next, Some(("b".into(), false)));
    let last = store
        .list_page("test-bucket", "", Some("b"), false, Some("/"), 2)
        .unwrap();
    assert_eq!(last.entries, [ListEntry::CommonPrefix("c/".into())]);
    assert!(!last.truncated);
    assert_eq!(last.next, None);
    let filtered = store
        .list_page("test-bucket", "", Some("a/1"), false, Some("/"), 1)
        .unwrap();
    assert!(matches!(&filtered.entries[0], ListEntry::Object(info) if info.key == "a0"));
}

#[test]
fn delimiter_successor_handles_unicode_scalar_boundaries_and_zero_limit() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), 1024).unwrap();
    store.create_bucket("test-bucket").unwrap();
    for key in [
        "a\u{10ffff}x",
        "a\u{10ffff}y",
        "b",
        "x\u{d7ff}z",
        "x\u{e000}",
    ] {
        put(&store, key, b"x");
    }
    let page = store
        .list_page("test-bucket", "a", None, true, Some("\u{10ffff}"), 1)
        .unwrap();
    assert_eq!(
        page.entries,
        [ListEntry::CommonPrefix("a\u{10ffff}".into())]
    );
    assert!(!page.truncated); // 'b' is outside this prefix.
    let page = store
        .list_page("test-bucket", "", None, true, Some("\u{10ffff}"), 1)
        .unwrap();
    assert_eq!(page.next, Some(("b".into(), true)));
    let page = store
        .list_page("test-bucket", "x", None, true, Some("\u{d7ff}"), 1)
        .unwrap();
    assert_eq!(page.next, Some(("x\u{e000}".into(), true)));
    let empty = store
        .list_page("test-bucket", "", None, true, Some("/"), 0)
        .unwrap();
    assert!(empty.entries.is_empty());
    assert!(!empty.truncated);
    assert!(empty.next.is_none());
}

#[test]
fn delete_recreate_fences_an_inflight_upload() {
    let directory = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(directory.path(), 1024).unwrap());
    store.create_bucket("test-bucket").unwrap();
    let (entered, waiting) = mpsc::channel();
    let (release, released) = mpsc::channel();
    struct PausedReader {
        entered: mpsc::Sender<()>,
        release: mpsc::Receiver<()>,
        done: bool,
    }
    impl Read for PausedReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            if self.done {
                return Ok(0);
            }
            self.entered.send(()).map_err(io::Error::other)?;
            self.release
                .recv_timeout(Duration::from_secs(5))
                .map_err(io::Error::other)?;
            self.done = true;
            buf[0] = b'x';
            Ok(1)
        }
    }
    let writer_store = Arc::clone(&store);
    let reader = PausedReader {
        entered,
        release: released,
        done: false,
    };
    let writer = std::thread::spawn(move || {
        writer_store.put_object("test-bucket", "late", reader, None, BTreeMap::new())
    });
    waiting.recv_timeout(Duration::from_secs(5)).unwrap();
    store.delete_bucket("test-bucket").unwrap();
    store.create_bucket("test-bucket").unwrap();
    release.send(()).unwrap();
    assert!(matches!(writer.join().unwrap(), Err(Error::NoSuchBucket)));
    assert!(
        store
            .list_objects("test-bucket", "", None, 10)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        std::fs::read_dir(directory.path().join("blobs"))
            .unwrap()
            .count(),
        0
    );
}

#[test]
fn startup_reclaims_only_owned_orphans_and_refuses_missing_live_bytes() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), 1024).unwrap();
    store.create_bucket("test-bucket").unwrap();
    put(&store, "key", b"safe");
    drop(store);
    let blobs = directory.path().join("blobs");
    let orphan = blobs.join(uuid::Uuid::new_v4().to_string());
    let temporary = blobs.join(format!("{}.tmp", uuid::Uuid::new_v4()));
    std::fs::write(&orphan, b"orphan").unwrap();
    std::fs::write(&temporary, b"partial").unwrap();
    std::fs::write(blobs.join("unowned.txt"), b"keep").unwrap();
    let store = Store::open(directory.path(), 1024).unwrap();
    assert!(!orphan.exists());
    assert!(!temporary.exists());
    assert!(blobs.join("unowned.txt").exists());
    drop(store);
    for entry in std::fs::read_dir(&blobs).unwrap() {
        let entry = entry.unwrap();
        if entry.file_name() != "unowned.txt" {
            std::fs::remove_file(entry.path()).unwrap();
        }
    }
    assert!(matches!(
        Store::open(directory.path(), 1024),
        Err(Error::Internal(_))
    ));
}

#[test]
fn a_second_store_cannot_reconcile_an_active_upload_directory() {
    let directory = tempfile::tempdir().unwrap();
    let _store = Store::open(directory.path(), 1024).unwrap();
    let pending = directory
        .path()
        .join("blobs")
        .join(format!("{}.tmp", uuid::Uuid::new_v4()));
    std::fs::write(&pending, b"active upload").unwrap();
    assert!(matches!(
        Store::open(directory.path(), 1024),
        Err(Error::Internal(_))
    ));
    assert!(pending.exists());
}

#[test]
fn missing_metadata_never_turns_live_bytes_into_deletable_orphans() {
    let directory = tempfile::tempdir().unwrap();
    let blobs = directory.path().join("blobs");
    std::fs::create_dir_all(&blobs).unwrap();
    let blob = blobs.join(uuid::Uuid::new_v4().to_string());
    std::fs::write(&blob, b"must preserve").unwrap();
    assert!(matches!(
        Store::open(directory.path(), 1024),
        Err(Error::Internal(_))
    ));
    assert_eq!(std::fs::read(&blob).unwrap(), b"must preserve");
    assert!(!directory.path().join("metadata.redb").exists());
}

#[test]
fn missing_versioned_table_is_not_silently_recreated() {
    let directory = tempfile::tempdir().unwrap();
    let store = Store::open(directory.path(), 1024).unwrap();
    store.create_bucket("test-bucket").unwrap();
    put(&store, "key", b"must preserve");
    drop(store);
    let blob = std::fs::read_dir(directory.path().join("blobs"))
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .path();
    let db = redb::Database::open(directory.path().join("metadata.redb")).unwrap();
    let tx = db.begin_write().unwrap();
    tx.delete_table(redb::TableDefinition::<&[u8], &[u8]>::new("objects-v1"))
        .unwrap();
    tx.commit().unwrap();
    drop(db);
    assert!(matches!(
        Store::open(directory.path(), 1024),
        Err(Error::Internal(_))
    ));
    assert_eq!(std::fs::read(blob).unwrap(), b"must preserve");
}
