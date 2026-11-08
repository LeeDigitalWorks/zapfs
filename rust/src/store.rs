// Copyright 2026 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

//! Single-process metadata and immutable blob storage.
//!
//! Blob bytes and their directory entry are synced before the redb publication
//! transaction commits. A failed metadata commit has an ambiguous outcome: the
//! store stops serving until reopened and never deletes the candidate blob.
//! Successful overwrites/deletes reclaim old files after publication. GET opens
//! its file while holding the same lock, so an already-open reader remains valid
//! when its old name is unlinked (or unlink is deferred on platforms that forbid
//! it). Unreferenced UUID-named files are reconciled on the next exclusive open.

use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use md5::{Digest, Md5};
use redb::{Database, ReadableTable, TableDefinition};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

const BUCKETS: TableDefinition<&str, &[u8]> = TableDefinition::new("buckets-v1");
const OBJECTS: TableDefinition<&[u8], &[u8]> = TableDefinition::new("objects-v1");
const SYSTEM: TableDefinition<&str, u64> = TableDefinition::new("system");
const FORMAT_VERSION: u64 = 1;
const BUFFER_SIZE: usize = 64 * 1024;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("bucket does not exist")]
    NoSuchBucket,
    #[error("object does not exist")]
    NoSuchKey,
    #[error("bucket already exists")]
    BucketAlreadyOwnedByYou,
    #[error("bucket is not empty")]
    BucketNotEmpty,
    #[error("invalid bucket name")]
    InvalidBucketName,
    #[error("object key must be between 1 and 1024 UTF-8 bytes")]
    InvalidObjectKey,
    #[error("object exceeds the configured size limit")]
    EntityTooLarge,
    #[error("upload body read failed: {0}")]
    BodyRead(#[source] std::io::Error),
    #[error("storage error: {0}")]
    Internal(String),
}

fn internal(error: impl std::fmt::Display) -> Error {
    Error::Internal(error.to_string())
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Bucket {
    pub name: String,
    pub created_at: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    /// Unquoted hexadecimal MD5 for an ordinary PUT, not a content identity.
    pub etag: String,
    pub modified_at: u64,
    pub content_type: Option<String>,
    pub metadata: BTreeMap<String, String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ListEntry {
    Object(ObjectInfo),
    CommonPrefix(String),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ListPage {
    pub entries: Vec<ListEntry>,
    pub truncated: bool,
    /// Resume after an object exclusively, or at a group's successor inclusively.
    pub next: Option<(String, bool)>,
}

#[derive(Serialize, Deserialize)]
struct BucketRecord {
    id: Uuid,
    bucket: Bucket,
}

#[derive(Serialize, Deserialize)]
struct ObjectRecord {
    blob: Uuid,
    info: ObjectInfo,
}

pub struct Store {
    db: Database,
    blobs: PathBuf,
    publication: Mutex<()>,
    unavailable: AtomicBool,
    max_object_bytes: u64,
}

impl Store {
    pub fn open(path: impl AsRef<Path>, max_object_bytes: u64) -> Result<Self> {
        let path = path.as_ref();
        create_directory_durably(path)?;
        let metadata_path = path.join("metadata.redb");
        let new_database = !metadata_path.try_exists().map_err(internal)?;
        let blobs = path.join("blobs");
        if new_database
            && blobs.try_exists().map_err(internal)?
            && fs::read_dir(&blobs)
                .map_err(internal)?
                .next()
                .transpose()
                .map_err(internal)?
                .is_some()
        {
            return Err(internal(
                "blob directory has data but metadata is missing; restore metadata before opening",
            ));
        }
        // redb holds the database's exclusive process lock before reconciliation.
        let db = Database::create(metadata_path).map_err(internal)?;
        fs::create_dir_all(&blobs).map_err(internal)?;
        sync_directory(path)?;
        let tx = db.begin_write().map_err(internal)?;
        {
            let mut system = tx.open_table(SYSTEM).map_err(internal)?;
            let version = system
                .get("format-version")
                .map_err(internal)?
                .map(|v| v.value());
            match version {
                Some(FORMAT_VERSION) => {
                    // Missing authoritative tables are corruption, not an empty
                    // store. Refuse recovery before looking for orphan files.
                    let read = db.begin_read().map_err(internal)?;
                    read.open_table(BUCKETS).map_err(internal)?;
                    read.open_table(OBJECTS).map_err(internal)?;
                }
                Some(version) => {
                    return Err(internal(format!("unsupported format version {version}")));
                }
                None if new_database => {
                    system
                        .insert("format-version", FORMAT_VERSION)
                        .map_err(internal)?;
                }
                None => {
                    return Err(internal(
                        "existing database has no supported format version",
                    ));
                }
            }
            tx.open_table(BUCKETS).map_err(internal)?;
            tx.open_table(OBJECTS).map_err(internal)?;
        }
        tx.commit().map_err(internal)?;
        let store = Self {
            db,
            blobs,
            publication: Mutex::new(()),
            unavailable: AtomicBool::new(false),
            max_object_bytes,
        };
        store.reconcile()?;
        Ok(store)
    }

    fn ensure_available(&self) -> Result<()> {
        if self.unavailable.load(Ordering::Acquire) {
            return Err(internal(
                "metadata commit outcome unknown; reopen storage to recover",
            ));
        }
        Ok(())
    }

    fn commit(&self, tx: redb::WriteTransaction) -> Result<()> {
        tx.commit().map_err(|error| {
            self.unavailable.store(true, Ordering::Release);
            internal(error)
        })
    }

    fn bucket_record(&self, name: &str) -> Result<BucketRecord> {
        let tx = self.db.begin_read().map_err(internal)?;
        let buckets = tx.open_table(BUCKETS).map_err(internal)?;
        let record = buckets
            .get(name)
            .map_err(internal)?
            .ok_or(Error::NoSuchBucket)?;
        serde_json::from_slice(record.value()).map_err(internal)
    }

    pub fn create_bucket(&self, name: &str) -> Result<Bucket> {
        validate_bucket(name)?;
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        let bucket = Bucket {
            name: name.to_owned(),
            created_at: now()?,
        };
        let record = BucketRecord {
            id: Uuid::new_v4(),
            bucket: bucket.clone(),
        };
        let encoded = serde_json::to_vec(&record).map_err(internal)?;
        let tx = self.db.begin_write().map_err(internal)?;
        {
            let mut buckets = tx.open_table(BUCKETS).map_err(internal)?;
            if buckets.get(name).map_err(internal)?.is_some() {
                return Err(Error::BucketAlreadyOwnedByYou);
            }
            buckets.insert(name, encoded.as_slice()).map_err(internal)?;
        }
        self.commit(tx)?;
        Ok(bucket)
    }

    pub fn head_bucket(&self, name: &str) -> Result<Bucket> {
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        Ok(self.bucket_record(name)?.bucket)
    }

    pub fn list_buckets(&self) -> Result<Vec<Bucket>> {
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        let tx = self.db.begin_read().map_err(internal)?;
        let buckets = tx.open_table(BUCKETS).map_err(internal)?;
        buckets
            .iter()
            .map_err(internal)?
            .map(|entry| {
                let (_, value) = entry.map_err(internal)?;
                let record: BucketRecord =
                    serde_json::from_slice(value.value()).map_err(internal)?;
                Ok(record.bucket)
            })
            .collect()
    }

    pub fn delete_bucket(&self, name: &str) -> Result<()> {
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        let record = self.bucket_record(name)?;
        let tx = self.db.begin_write().map_err(internal)?;
        {
            let objects = tx.open_table(OBJECTS).map_err(internal)?;
            let prefix = record.id.as_bytes().as_slice();
            let mut range = objects.range(prefix..).map_err(internal)?;
            if let Some(entry) = range.next() {
                let (key, _) = entry.map_err(internal)?;
                if key.value().starts_with(prefix) {
                    return Err(Error::BucketNotEmpty);
                }
            }
        }
        {
            let mut buckets = tx.open_table(BUCKETS).map_err(internal)?;
            buckets.remove(name).map_err(internal)?;
        }
        self.commit(tx)
    }

    pub fn put_object(
        &self,
        bucket: &str,
        key: &str,
        mut reader: impl Read,
        content_type: Option<String>,
        metadata: BTreeMap<String, String>,
    ) -> Result<ObjectInfo> {
        validate_key(key)?;
        let bucket_id = {
            let _guard = self.publication.lock().map_err(internal)?;
            self.ensure_available()?;
            self.bucket_record(bucket)?.id
        };
        let blob = Uuid::new_v4();
        let temporary = self.blobs.join(format!("{blob}.tmp"));
        let mut pending = PendingFile {
            path: Some(temporary.clone()),
        };
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .map_err(internal)?;
        let mut buffer = [0_u8; BUFFER_SIZE];
        let mut digest = Md5::new();
        let mut size = 0_u64;
        loop {
            let length = reader.read(&mut buffer).map_err(Error::BodyRead)?;
            if length == 0 {
                break;
            }
            size = size
                .checked_add(length as u64)
                .ok_or(Error::EntityTooLarge)?;
            if size > self.max_object_bytes {
                return Err(Error::EntityTooLarge);
            }
            file.write_all(&buffer[..length]).map_err(internal)?;
            digest.update(&buffer[..length]);
        }
        file.sync_all().map_err(internal)?;
        drop(file);
        let info = ObjectInfo {
            key: key.to_owned(),
            size,
            etag: hex::encode(digest.finalize()),
            modified_at: now()?,
            content_type,
            metadata,
        };
        let record = ObjectRecord {
            blob,
            info: info.clone(),
        };
        let encoded = serde_json::to_vec(&record).map_err(internal)?;
        let object_key = object_key(bucket_id, key);
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        // A concurrent delete/recreate must not publish an old upload into the new bucket.
        if self.bucket_record(bucket)?.id != bucket_id {
            return Err(Error::NoSuchBucket);
        }
        let published = self.blobs.join(blob.to_string());
        fs::rename(&temporary, &published).map_err(internal)?;
        // From here an error may leave an orphan. Startup reconciliation is its owner.
        pending.path = None;
        sync_directory(&self.blobs)?;
        let tx = self.db.begin_write().map_err(internal)?;
        let previous = {
            let mut objects = tx.open_table(OBJECTS).map_err(internal)?;
            let previous = objects
                .get(object_key.as_slice())
                .map_err(internal)?
                .map(|v| serde_json::from_slice::<ObjectRecord>(v.value()).map_err(internal))
                .transpose()?;
            objects
                .insert(object_key.as_slice(), encoded.as_slice())
                .map_err(internal)?;
            previous
        };
        self.commit(tx)?;
        if let Some(previous) = previous {
            self.reclaim(previous.blob);
        }
        Ok(info)
    }

    pub fn get_object(&self, bucket: &str, key: &str) -> Result<(ObjectInfo, File)> {
        validate_key(key)?;
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        let record = self.object_record(bucket, key)?;
        let file = File::open(self.blobs.join(record.blob.to_string())).map_err(internal)?;
        if file.metadata().map_err(internal)?.len() != record.info.size {
            return Err(internal("committed blob length mismatch"));
        }
        Ok((record.info, file))
    }

    pub fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo> {
        validate_key(key)?;
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        Ok(self.object_record(bucket, key)?.info)
    }

    fn object_record(&self, bucket: &str, key: &str) -> Result<ObjectRecord> {
        let bucket = self.bucket_record(bucket)?;
        let key = object_key(bucket.id, key);
        let tx = self.db.begin_read().map_err(internal)?;
        let objects = tx.open_table(OBJECTS).map_err(internal)?;
        let value = objects
            .get(key.as_slice())
            .map_err(internal)?
            .ok_or(Error::NoSuchKey)?;
        serde_json::from_slice(value.value()).map_err(internal)
    }

    pub fn delete_object(&self, bucket: &str, key: &str) -> Result<()> {
        validate_key(key)?;
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        let bucket = self.bucket_record(bucket)?;
        let key = object_key(bucket.id, key);
        let tx = self.db.begin_write().map_err(internal)?;
        let previous = {
            let mut objects = tx.open_table(OBJECTS).map_err(internal)?;
            objects
                .remove(key.as_slice())
                .map_err(internal)?
                .map(|v| serde_json::from_slice::<ObjectRecord>(v.value()).map_err(internal))
                .transpose()?
        };
        self.commit(tx)?;
        if let Some(previous) = previous {
            self.reclaim(previous.blob);
        }
        Ok(())
    }

    /// Bytewise UTF-8 lexical order; start_after is exclusive. Only a bounded
    /// range starting at the requested prefix/cursor is read, not the whole bucket.
    pub fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ObjectInfo>> {
        self.list_objects_from(bucket, prefix, start_after, false, limit)
    }

    /// Range-scan variant with an inclusive cursor for efficiently skipping all
    /// keys in a delimiter group using the group's lexical successor.
    pub fn list_objects_from(
        &self,
        bucket: &str,
        prefix: &str,
        start_after: Option<&str>,
        inclusive: bool,
        limit: usize,
    ) -> Result<Vec<ObjectInfo>> {
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        let bucket = self.bucket_record(bucket)?;
        if limit == 0 {
            return Ok(Vec::new());
        }
        let start = start_after
            .filter(|after| *after > prefix)
            .unwrap_or(prefix);
        let begin = object_key(bucket.id, start);
        let id = bucket.id.as_bytes();
        let tx = self.db.begin_read().map_err(internal)?;
        let objects = tx.open_table(OBJECTS).map_err(internal)?;
        let mut out = Vec::with_capacity(limit.min(1000));
        for entry in objects.range(begin.as_slice()..).map_err(internal)? {
            let (key, value) = entry.map_err(internal)?;
            let bytes = key.value();
            if !bytes.starts_with(id) {
                break;
            }
            let name = std::str::from_utf8(&bytes[16..]).map_err(internal)?;
            if !name.starts_with(prefix) {
                break;
            }
            if start_after.is_some_and(|after| {
                if inclusive {
                    name < after
                } else {
                    name <= after
                }
            }) {
                continue;
            }
            let record: ObjectRecord = serde_json::from_slice(value.value()).map_err(internal)?;
            out.push(record.info);
            if out.len() >= limit {
                break;
            }
        }
        Ok(out)
    }

    /// One snapshot for an entire S3 listing page. Delimiter groups count as one
    /// entry and are skipped by a B-tree seek, not by visiting every member.
    #[allow(clippy::too_many_arguments)] // Mirrors one bounded S3 listing query.
    pub fn list_page(
        &self,
        bucket: &str,
        prefix: &str,
        lower: Option<&str>,
        inclusive: bool,
        delimiter: Option<&str>,
        limit: usize,
    ) -> Result<ListPage> {
        let _guard = self.publication.lock().map_err(internal)?;
        self.ensure_available()?;
        let bucket = self.bucket_record(bucket)?;
        let limit = limit.min(1000);
        let mut result = ListPage {
            entries: Vec::with_capacity(limit),
            truncated: false,
            next: None,
        };
        if limit == 0 {
            return Ok(result);
        }
        let delimiter = delimiter.filter(|value| !value.is_empty());
        let tx = self.db.begin_read().map_err(internal)?;
        let objects = tx.open_table(OBJECTS).map_err(internal)?;
        let mut cursor = lower
            .filter(|value| *value >= prefix)
            .unwrap_or(prefix)
            .to_owned();
        let mut cursor_inclusive = if lower.is_some_and(|value| value >= prefix) {
            inclusive
        } else {
            true
        };
        let mut after_last = None;
        loop {
            let begin = object_key(bucket.id, &cursor);
            let bound = if cursor_inclusive {
                std::ops::Bound::Included(begin.as_slice())
            } else {
                std::ops::Bound::Excluded(begin.as_slice())
            };
            let mut range = objects
                .range::<&[u8]>((bound, std::ops::Bound::Unbounded))
                .map_err(internal)?;
            let Some(entry) = range.next() else {
                break;
            };
            let (key, value) = entry.map_err(internal)?;
            let bytes = key.value();
            if !bytes.starts_with(bucket.id.as_bytes()) {
                break;
            }
            let name = std::str::from_utf8(&bytes[16..]).map_err(internal)?;
            if !name.starts_with(prefix) {
                break;
            }
            let common = delimiter.and_then(|delimiter| {
                name[prefix.len()..]
                    .find(delimiter)
                    .map(|offset| name[..prefix.len() + offset + delimiter.len()].to_owned())
            });
            let (entry, next) = if let Some(common) = common {
                let next = lexical_successor(&common).map(|successor| (successor, true));
                // S3 filters CommonPrefixes against StartAfter, not merely the
                // object's name that happened to reveal the group.
                if lower.is_some_and(|lower| {
                    if inclusive {
                        common.as_str() < lower
                    } else {
                        common.as_str() <= lower
                    }
                }) {
                    if let Some((next, next_inclusive)) = next {
                        cursor = next;
                        cursor_inclusive = next_inclusive;
                        continue;
                    }
                    break;
                }
                (ListEntry::CommonPrefix(common), next)
            } else {
                let record: ObjectRecord =
                    serde_json::from_slice(value.value()).map_err(internal)?;
                (
                    ListEntry::Object(record.info),
                    Some((name.to_owned(), false)),
                )
            };
            if result.entries.len() == limit {
                result.truncated = true;
                result.next = after_last;
                return Ok(result);
            }
            result.entries.push(entry);
            after_last = next.clone();
            if let Some((next, next_inclusive)) = next {
                cursor = next;
                cursor_inclusive = next_inclusive;
            } else {
                break;
            }
        }
        Ok(result)
    }

    fn reclaim(&self, blob: Uuid) {
        // Failed unlink remains an orphan and is retried at the next open. Never
        // turn a committed operation into a retryable failure due to cleanup.
        if fs::remove_file(self.blobs.join(blob.to_string())).is_ok() {
            let _ = sync_directory(&self.blobs);
        }
    }

    fn reconcile(&self) -> Result<()> {
        let tx = self.db.begin_read().map_err(internal)?;
        let objects = tx.open_table(OBJECTS).map_err(internal)?;
        let mut live = HashSet::new();
        // Validate every authoritative record before removing any orphan.
        for entry in objects.iter().map_err(internal)? {
            let (_, value) = entry.map_err(internal)?;
            let record: ObjectRecord = serde_json::from_slice(value.value()).map_err(internal)?;
            let path = self.blobs.join(record.blob.to_string());
            let metadata = fs::symlink_metadata(&path).map_err(internal)?;
            if !metadata.is_file() || metadata.len() != record.info.size {
                return Err(internal("missing, invalid or truncated committed blob"));
            }
            live.insert(record.blob);
        }
        for entry in fs::read_dir(&self.blobs).map_err(internal)? {
            let entry = entry.map_err(internal)?;
            if !entry.file_type().map_err(internal)?.is_file() {
                continue;
            }
            let name = entry.file_name();
            let Some(name) = name.to_str() else {
                continue;
            };
            let (candidate, temporary) = name
                .strip_suffix(".tmp")
                .map_or((name, false), |id| (id, true));
            let Ok(id) = Uuid::parse_str(candidate) else {
                continue;
            };
            // Only our canonical generated filenames belong to recovery.
            if candidate != id.to_string() {
                continue;
            }
            if temporary || !live.contains(&id) {
                fs::remove_file(entry.path()).map_err(internal)?;
            }
        }
        sync_directory(&self.blobs)
    }
}

struct PendingFile {
    path: Option<PathBuf>,
}
impl Drop for PendingFile {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

fn object_key(bucket: Uuid, key: &str) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16 + key.len());
    bytes.extend_from_slice(bucket.as_bytes());
    bytes.extend_from_slice(key.as_bytes());
    bytes
}

fn now() -> Result<u64> {
    Ok(SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(internal)?
        .as_secs())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(internal)
}

fn create_directory_durably(path: &Path) -> Result<()> {
    let mut missing = Vec::new();
    let mut current = path;
    loop {
        match fs::metadata(current) {
            Ok(metadata) if metadata.is_dir() => break,
            Ok(_) => return Err(internal("storage path is not a directory")),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                missing.push(current.to_path_buf())
            }
            Err(error) => return Err(internal(error)),
        }
        current = current
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
    }
    fs::create_dir_all(path).map_err(internal)?;
    for directory in missing.iter().rev() {
        sync_directory(directory)?;
        let parent = directory
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(parent)?;
    }
    Ok(())
}

// UTF-8 byte order follows Unicode scalar order. Carry across the maximum
// scalar and skip the surrogate range, preserving a valid String cursor.
fn lexical_successor(prefix: &str) -> Option<String> {
    let mut value = prefix.to_owned();
    while let Some(last) = value.pop() {
        let mut next = u32::from(last) + 1;
        if next == 0xd800 {
            next = 0xe000;
        }
        if let Some(next) = char::from_u32(next) {
            value.push(next);
            return Some(value);
        }
    }
    None
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty() || key.len() > 1024 {
        return Err(Error::InvalidObjectKey);
    }
    Ok(())
}

fn validate_bucket(name: &str) -> Result<()> {
    if !(3..=63).contains(&name.len())
        || name.parse::<std::net::Ipv4Addr>().is_ok()
        || name.starts_with("xn--")
        || name.starts_with("sthree-")
        || ["-s3alias", "--ol-s3", ".mrap", "--x-s3", "--table-s3"]
            .iter()
            .any(|suffix| name.ends_with(suffix))
        || !name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-' || byte == b'.'
        })
        || name.split('.').any(|label| {
            label.is_empty()
                || !label.as_bytes()[0].is_ascii_alphanumeric()
                || !label.as_bytes()[label.len() - 1].is_ascii_alphanumeric()
        })
    {
        return Err(Error::InvalidBucketName);
    }
    Ok(())
}
