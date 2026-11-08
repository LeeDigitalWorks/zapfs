// Copyright 2026 ZapFS Authors
// SPDX-License-Identifier: Apache-2.0

//! S3 protocol boundary. Unimplemented S3 trait methods retain s3s' 501 contract.
use crate::store::{Error, ListEntry, ObjectInfo, Store};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use futures::TryStreamExt;
use hmac::{Hmac, Mac};
use md5::{Digest, Md5};
use s3s::{S3, S3Error, S3Request, S3Response, S3Result, dto::*, s3_error};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    io::Read,
    sync::Arc,
    time::{Duration, UNIX_EPOCH},
};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio_util::io::{ReaderStream, StreamReader, SyncIoBridge};

#[derive(Clone)]
pub struct Api {
    store: Arc<Store>,
    cursor_key: Arc<Vec<u8>>,
}

impl Api {
    pub fn new(store: Arc<Store>, secret: &[u8]) -> Self {
        let mut h = sha2::Sha256::new();
        h.update(b"zapfs-cursor-v1");
        h.update(secret);
        Self {
            store,
            cursor_key: Arc::new(h.finalize().to_vec()),
        }
    }

    async fn blocking<T: Send + 'static>(
        &self,
        f: impl FnOnce(Arc<Store>) -> crate::store::Result<T> + Send + 'static,
    ) -> S3Result<T> {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || f(store))
            .await
            .map_err(|_| s3_error!(InternalError))?
            .map_err(map_error)
    }
}

fn map_error(e: Error) -> S3Error {
    match e {
        Error::NoSuchBucket => s3_error!(NoSuchBucket),
        Error::NoSuchKey => s3_error!(NoSuchKey),
        Error::BucketAlreadyOwnedByYou => s3_error!(BucketAlreadyOwnedByYou),
        Error::BucketNotEmpty => s3_error!(BucketNotEmpty),
        Error::InvalidBucketName => s3_error!(InvalidBucketName),
        Error::InvalidObjectKey => s3_error!(InvalidArgument, "Invalid object key"),
        Error::EntityTooLarge => s3_error!(EntityTooLarge),
        Error::BodyRead(e) => {
            match e
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<BodyValidationError>())
            {
                Some(BodyValidationError::Length) => s3_error!(IncompleteBody),
                Some(BodyValidationError::Md5) => s3_error!(BadDigest),
                Some(BodyValidationError::Sha256) => {
                    let mut e =
                        S3Error::new(s3s::S3ErrorCode::Custom("XAmzContentSHA256Mismatch".into()));
                    e.set_status_code(http::StatusCode::BAD_REQUEST);
                    e
                }
                None => s3_error!(IncompleteBody),
            }
        }
        Error::Internal(message) => {
            tracing::error!(%message, "storage operation failed");
            s3_error!(InternalError)
        }
    }
}

fn timestamp(seconds: u64) -> Timestamp {
    (UNIX_EPOCH + Duration::from_secs(seconds)).into()
}

/// Reject feature-bearing headers until their semantics are implemented.
fn check_headers(headers: &http::HeaderMap) -> S3Result<()> {
    for name in headers.keys() {
        let n = name.as_str();
        if n.starts_with("x-amz-meta-") {
            continue;
        }
        if n.starts_with("x-amz-")
            && !matches!(
                n,
                "x-amz-date"
                    | "x-amz-content-sha256"
                    | "x-amz-decoded-content-length"
                    | "x-amz-user-agent"
            )
        {
            return Err(s3_error!(NotImplemented, "Unsupported feature header: {n}"));
        }
        if matches!(
            n,
            "if-match"
                | "if-none-match"
                | "if-modified-since"
                | "if-unmodified-since"
                | "cache-control"
                | "content-disposition"
                | "content-encoding"
                | "content-language"
                | "expires"
        ) {
            return Err(s3_error!(NotImplemented, "Unsupported feature header: {n}"));
        }
    }
    Ok(())
}

fn check_query(req: &http::Uri, allowed: &[&str]) -> S3Result<()> {
    for pair in req
        .query()
        .unwrap_or_default()
        .split('&')
        .filter(|s| !s.is_empty())
    {
        let name = pair.split('=').next().unwrap_or_default();
        if name.starts_with("X-Amz-") {
            continue;
        }
        if !allowed.contains(&name) {
            return Err(s3_error!(NotImplemented, "Unsupported query parameter"));
        }
    }
    Ok(())
}

#[derive(Serialize, Deserialize)]
struct Cursor {
    version: u8,
    bucket: String,
    prefix: String,
    delimiter: Option<String>,
    after: String,
    inclusive: bool,
}

fn decode_cursor(
    token: &str,
    bucket: &str,
    prefix: &str,
    delimiter: Option<&str>,
    secret: &[u8],
) -> S3Result<Cursor> {
    if token.len() > 32768 {
        return Err(s3_error!(InvalidArgument, "Invalid continuation token"));
    }
    let invalid = || s3_error!(InvalidArgument, "Invalid continuation token");
    let bytes = URL_SAFE_NO_PAD.decode(token).map_err(|_| invalid())?;
    if bytes.len() < 32 {
        return Err(invalid());
    }
    let (payload, signature) = bytes.split_at(bytes.len() - 32);
    let mut mac = Hmac::<sha2::Sha256>::new_from_slice(secret).map_err(|_| invalid())?;
    mac.update(payload);
    mac.verify_slice(signature).map_err(|_| invalid())?;
    let c: Cursor = serde_json::from_slice(payload).map_err(|_| invalid())?;
    if c.version != 1
        || c.bucket != bucket
        || c.prefix != prefix
        || c.delimiter.as_deref() != delimiter
    {
        return Err(invalid());
    }
    Ok(c)
}

fn encode_cursor(c: &Cursor, secret: &[u8]) -> S3Result<String> {
    let mut payload = serde_json::to_vec(c).map_err(|_| s3_error!(InternalError))?;
    let mut mac =
        Hmac::<sha2::Sha256>::new_from_slice(secret).map_err(|_| s3_error!(InternalError))?;
    mac.update(&payload);
    payload.extend_from_slice(&mac.finalize().into_bytes());
    Ok(URL_SAFE_NO_PAD.encode(payload))
}

/// The EOF check runs before Store can publish metadata, so truncated bodies or
/// bad Content-MD5 cannot overwrite an existing generation.
#[derive(Debug, thiserror::Error)]
enum BodyValidationError {
    #[error("Content-Length mismatch")]
    Length,
    #[error("Content-MD5 mismatch")]
    Md5,
    #[error("Payload hash mismatch")]
    Sha256,
}

struct CheckedReader<R> {
    inner: R,
    expected: Option<u64>,
    expected_md5: Option<Vec<u8>>,
    read: u64,
    md5: Md5,
}

impl<R: Read> Read for CheckedReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.read += n as u64;
        self.md5.update(&buf[..n]);
        if n == 0 && !buf.is_empty() {
            if self.expected.is_some_and(|len| len != self.read) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    BodyValidationError::Length,
                ));
            }
            if self.expected_md5.as_ref().is_some_and(|expected| {
                expected.as_slice() != self.md5.clone().finalize().as_slice()
            }) {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    BodyValidationError::Md5,
                ));
            }
        }
        Ok(n)
    }
}

#[async_trait::async_trait]
impl S3 for Api {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        check_headers(&req.headers)?;
        check_query(&req.uri, &[])?;
        if req.input.create_bucket_configuration.is_some() {
            return Err(s3_error!(
                NotImplemented,
                "Only the default local region is implemented"
            ));
        }
        let bucket = req.input.bucket;
        let location = format!("/{bucket}");
        self.blocking(move |s| s.create_bucket(&bucket)).await?;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(location),
            ..Default::default()
        }))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        check_headers(&req.headers)?;
        check_query(&req.uri, &[])?;
        self.blocking(move |s| s.head_bucket(&req.input.bucket))
            .await?;
        Ok(S3Response::new(HeadBucketOutput {
            bucket_region: Some("us-east-1".into()),
            ..Default::default()
        }))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        check_headers(&req.headers)?;
        check_query(&req.uri, &[])?;
        self.blocking(move |s| s.delete_bucket(&req.input.bucket))
            .await?;
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        check_headers(&req.headers)?;
        check_query(&req.uri, &[])?;
        let buckets = self
            .blocking(|s| s.list_buckets())
            .await?
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(timestamp(b.created_at)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        }))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        check_headers(&req.headers)?;
        check_query(&req.uri, &[])?;
        // Plain SigV4 payload hashes and Content-MD5 need validation before commit.
        // Streaming signed payload verification is delegated to s3s.
        let expected_sha256 = req
            .headers
            .get("x-amz-content-sha256")
            .and_then(|v| v.to_str().ok())
            .filter(|v| v.len() == 64)
            .map(str::to_owned);
        let input = req.input;
        let expected = input
            .content_length
            .map(u64::try_from)
            .transpose()
            .map_err(|_| s3_error!(InvalidArgument))?;
        let md5 = input
            .content_md5
            .map(|s| base64::engine::general_purpose::STANDARD.decode(s))
            .transpose()
            .map_err(|_| s3_error!(InvalidDigest))?;
        if md5.as_ref().is_some_and(|v| v.len() != 16) {
            return Err(s3_error!(InvalidDigest));
        }
        let body = input
            .body
            .unwrap_or_else(|| StreamingBlob::from_bytes(bytes::Bytes::new()));
        let stream = body.map_err(std::io::Error::other);
        let reader = SyncIoBridge::new(StreamReader::new(stream));
        let metadata: BTreeMap<_, _> = input.metadata.unwrap_or_default().into_iter().collect();
        let info = self
            .blocking(move |s| {
                let checked = CheckedReader {
                    inner: reader,
                    expected,
                    expected_md5: md5,
                    read: 0,
                    md5: Md5::new(),
                };
                let checked = PayloadReader::new(checked, expected_sha256);
                s.put_object(
                    &input.bucket,
                    &input.key,
                    checked,
                    input.content_type,
                    metadata,
                )
            })
            .await?;
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(info.etag)),
            ..Default::default()
        }))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        check_headers(&req.headers)?;
        check_query(&req.uri, &[])?;
        let input = req.input;
        let (info, file) = self
            .blocking(move |s| s.get_object(&input.bucket, &input.key))
            .await?;
        let (start, end) = resolve_range(input.range, info.size)?;
        let mut file = tokio::fs::File::from_std(file);
        file.seek(std::io::SeekFrom::Start(start))
            .await
            .map_err(|_| s3_error!(InternalError))?;
        let output = GetObjectOutput {
            body: Some(StreamingBlob::wrap(ReaderStream::with_capacity(
                file.take(end - start),
                64 * 1024,
            ))),
            content_length: Some((end - start) as i64),
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(timestamp(info.modified_at)),
            metadata: Some(info.metadata.into_iter().collect()),
            accept_ranges: Some("bytes".into()),
            content_range: input
                .range
                .map(|_| format!("bytes {start}-{}/{size}", end - 1, size = info.size)),
            ..Default::default()
        };
        Ok(S3Response::with_status(
            output,
            if input.range.is_some() {
                http::StatusCode::PARTIAL_CONTENT
            } else {
                http::StatusCode::OK
            },
        ))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        check_headers(&req.headers)?;
        check_query(&req.uri, &[])?;
        let input = req.input;
        let info = self
            .blocking(move |s| s.head_object(&input.bucket, &input.key))
            .await?;
        let (start, end) = resolve_range(input.range, info.size)?;
        Ok(S3Response::new(HeadObjectOutput {
            content_length: Some((end - start) as i64),
            content_type: info.content_type,
            e_tag: Some(ETag::Strong(info.etag)),
            last_modified: Some(timestamp(info.modified_at)),
            metadata: Some(info.metadata.into_iter().collect()),
            accept_ranges: Some("bytes".into()),
            ..Default::default()
        }))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        check_headers(&req.headers)?;
        check_query(&req.uri, &[])?;
        self.blocking(move |s| s.delete_object(&req.input.bucket, &req.input.key))
            .await?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        check_headers(&req.headers)?;
        check_query(
            &req.uri,
            &[
                "list-type",
                "prefix",
                "start-after",
                "max-keys",
                "continuation-token",
                "delimiter",
            ],
        )?;
        let input = req.input;
        let max = input.max_keys.unwrap_or(1000);
        if !(0..=1000).contains(&max) {
            return Err(s3_error!(
                InvalidArgument,
                "max-keys must be between 0 and 1000"
            ));
        }
        let prefix = input.prefix.clone().unwrap_or_default();
        if prefix.len() > 1024 {
            return Err(s3_error!(InvalidArgument, "Prefix exceeds key size limit"));
        }
        let delimiter = input.delimiter.clone().filter(|s| !s.is_empty());
        let cursor = match &input.continuation_token {
            Some(token) => decode_cursor(
                token,
                &input.bucket,
                &prefix,
                delimiter.as_deref(),
                &self.cursor_key,
            )?,
            None => Cursor {
                version: 1,
                bucket: input.bucket.clone(),
                prefix: prefix.clone(),
                delimiter: delimiter.clone(),
                after: input.start_after.clone().unwrap_or_default(),
                inclusive: false,
            },
        };
        let (bucket, scan_prefix, scan_delimiter) =
            (input.bucket.clone(), prefix.clone(), delimiter.clone());
        let page = self
            .blocking(move |s| {
                s.list_page(
                    &bucket,
                    &scan_prefix,
                    Some(&cursor.after),
                    cursor.inclusive,
                    scan_delimiter.as_deref(),
                    max as usize,
                )
            })
            .await?;
        let count = page.entries.len() as i32;
        let mut contents = Vec::new();
        let mut common_prefixes = Vec::new();
        for entry in page.entries {
            match entry {
                ListEntry::Object(o) => contents.push(object_dto(o)),
                ListEntry::CommonPrefix(p) => {
                    common_prefixes.push(CommonPrefix { prefix: Some(p) })
                }
            }
        }
        let next = page
            .next
            .map(|(after, inclusive)| {
                encode_cursor(
                    &Cursor {
                        version: 1,
                        bucket: input.bucket.clone(),
                        prefix: prefix.clone(),
                        delimiter: delimiter.clone(),
                        after,
                        inclusive,
                    },
                    &self.cursor_key,
                )
            })
            .transpose()?;
        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: Some(prefix),
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            delimiter,
            max_keys: Some(max),
            key_count: Some(count),
            is_truncated: Some(page.truncated),
            next_continuation_token: next,
            continuation_token: input.continuation_token,
            start_after: input.start_after,
            ..Default::default()
        }))
    }
}

fn resolve_range(range: Option<Range>, size: u64) -> S3Result<(u64, u64)> {
    match range {
        Some(_) if size == 0 => Err(s3_error!(InvalidRange)),
        Some(r) => {
            let r = r.check(size)?;
            Ok((r.start, r.end))
        }
        None => Ok((0, size)),
    }
}

fn object_dto(o: ObjectInfo) -> Object {
    Object {
        key: Some(o.key),
        size: Some(o.size as i64),
        e_tag: Some(ETag::Strong(o.etag)),
        last_modified: Some(timestamp(o.modified_at)),
        ..Default::default()
    }
}

struct PayloadReader<R> {
    inner: R,
    expected: Option<String>,
    sha256: sha2::Sha256,
}
impl<R> PayloadReader<R> {
    fn new(inner: R, expected: Option<String>) -> Self {
        Self {
            inner,
            expected,
            sha256: sha2::Sha256::new(),
        }
    }
}
impl<R: Read> Read for PayloadReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let n = self.inner.read(buf)?;
        self.sha256.update(&buf[..n]);
        if n == 0
            && !buf.is_empty()
            && self
                .expected
                .as_ref()
                .is_some_and(|e| *e != hex::encode(self.sha256.clone().finalize()))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                BodyValidationError::Sha256,
            ));
        }
        Ok(n)
    }
}
