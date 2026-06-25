//! Metrics-recording wrapper around `ObjectStore`.

use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

use super::metrics;

fn classify_error(err: &Error) -> &'static str {
    match err {
        Error::NotFound { .. } => "not_found",
        Error::PermissionDenied { .. } => "permission",
        Error::Unauthenticated { .. } => "permission",
        Error::AlreadyExists { .. } => "already_exists",
        Error::Precondition { .. } | Error::NotModified { .. } => "precondition",
        Error::NotSupported { .. } | Error::NotImplemented => "not_supported",
        Error::InvalidPath { .. } => "invalid_path",
        Error::JoinError { .. } => "other",
        Error::Generic { source, .. } => {
            let msg = source.to_string().to_ascii_lowercase();
            if msg.contains("timed out") || msg.contains("timeout") {
                "timeout"
            } else if msg.contains("connect") || msg.contains("dns") || msg.contains("network") {
                "network"
            } else {
                "other"
            }
        }
        _ => "other",
    }
}

fn record<T>(operation: &'static str, started: Instant, result: &Result<T>) {
    let duration = started.elapsed().as_secs_f64();
    let status = if result.is_ok() { "success" } else { "error" };
    metrics::record_object_store_operation(operation, status, duration);
    if let Err(e) = result {
        metrics::record_object_store_error(operation, classify_error(e));
        record_health(false);
    } else {
        record_health(true);
    }
}

/// Update the consecutive-failure gauge that drives `/readyz` and zombie-mode
/// detection. Without this every list/delete/head/copy/rename failure is
/// invisible to the health probe — the gauge is otherwise only updated by
/// `partition_store.rs` produce/fetch hot paths.
fn record_health(success: bool) {
    let still_healthy = metrics::track_object_store_health(success);
    if !success && !still_healthy {
        tracing::error!(
            consecutive_failures = metrics::object_store_consecutive_failures(),
            "PARTIAL NETWORK PARTITION DETECTED: object store unreachable from \
             list/delete/head/copy/rename path."
        );
    }
}

/// Wraps an `ObjectStore` to record operation latency, byte counts, and errors.
pub struct MetricsObjectStore {
    inner: Arc<dyn ObjectStore>,
}

impl MetricsObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>) -> Self {
        Self { inner }
    }
}

impl std::fmt::Debug for MetricsObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MetricsObjectStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl std::fmt::Display for MetricsObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "MetricsObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for MetricsObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        let started = Instant::now();
        let bytes = payload.content_length() as u64;
        let result = self.inner.put(location, payload).await;
        record("put", started, &result);
        if result.is_ok() {
            metrics::record_object_store_bytes("write", bytes);
        }
        result
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        let started = Instant::now();
        let bytes = payload.content_length() as u64;
        let result = self.inner.put_opts(location, payload, opts).await;
        record("put", started, &result);
        if result.is_ok() {
            metrics::record_object_store_bytes("write", bytes);
        }
        result
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        let started = Instant::now();
        let result = self.inner.put_multipart(location).await;
        record("put", started, &result);
        result
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        let started = Instant::now();
        let result = self.inner.put_multipart_opts(location, opts).await;
        record("put", started, &result);
        result
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        self.get_opts(location, GetOptions::default()).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        let started = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        record("get", started, &result);
        result.map(wrap_get_result)
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        let started = Instant::now();
        let result = self.inner.get_range(location, range).await;
        record("get", started, &result);
        if let Ok(bytes) = &result {
            metrics::record_object_store_bytes("read", bytes.len() as u64);
        }
        result
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        let started = Instant::now();
        let result = self.inner.get_ranges(location, ranges).await;
        record("get", started, &result);
        if let Ok(parts) = &result {
            let total: u64 = parts.iter().map(|b| b.len() as u64).sum();
            metrics::record_object_store_bytes("read", total);
        }
        result
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        let started = Instant::now();
        let result = self.inner.head(location).await;
        record("head", started, &result);
        result
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.delete(location).await;
        record("delete", started, &result);
        result
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        let upstream = self.inner.delete_stream(locations);
        upstream
            .inspect(|item| match item {
                Ok(_) => {
                    metrics::record_object_store_operation("delete", "success", 0.0);
                    record_health(true);
                }
                Err(e) => {
                    metrics::record_object_store_operation("delete", "error", 0.0);
                    metrics::record_object_store_error("delete", classify_error(e));
                    record_health(false);
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let upstream = self.inner.list(prefix);
        upstream
            .inspect(|item| {
                if let Err(e) = item {
                    metrics::record_object_store_error("list", classify_error(e));
                    record_health(false);
                } else {
                    record_health(true);
                }
            })
            .boxed()
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let upstream = self.inner.list_with_offset(prefix, offset);
        upstream
            .inspect(|item| {
                if let Err(e) = item {
                    metrics::record_object_store_error("list", classify_error(e));
                    record_health(false);
                } else {
                    record_health(true);
                }
            })
            .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        let started = Instant::now();
        let result = self.inner.list_with_delimiter(prefix).await;
        record("list", started, &result);
        result
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.copy(from, to).await;
        record("copy", started, &result);
        result
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.rename(from, to).await;
        record("rename", started, &result);
        result
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.copy_if_not_exists(from, to).await;
        record("copy", started, &result);
        result
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.rename_if_not_exists(from, to).await;
        record("rename", started, &result);
        result
    }
}

fn wrap_get_result(result: GetResult) -> GetResult {
    let GetResult {
        payload,
        meta,
        range,
        attributes,
    } = result;
    let payload = match payload {
        GetResultPayload::Stream(stream) => GetResultPayload::Stream(
            stream
                .inspect(|chunk| match chunk {
                    Ok(bytes) => {
                        metrics::record_object_store_bytes("read", bytes.len() as u64);
                    }
                    Err(e) => {
                        // A mid-stream failure (e.g. the S3 connection
                        // dropping after `get_opts` returned OK) would
                        // otherwise leave the health gauge unchanged.
                        metrics::record_object_store_error("get", classify_error(e));
                        record_health(false);
                    }
                })
                .boxed(),
        ),
        other => other,
    };
    GetResult {
        payload,
        meta,
        range,
        attributes,
    }
}
