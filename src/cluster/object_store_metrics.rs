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
    CopyOptions, Error, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult, RenameOptions,
    Result,
};

use super::metrics;

fn classify_error(err: &Error) -> &'static str {
    match err {
        Error::NotFound { .. } => "not_found",
        Error::PermissionDenied { .. } => "permission",
        Error::Unauthenticated { .. } => "permission",
        Error::AlreadyExists { .. } => "already_exists",
        Error::Precondition { .. } | Error::NotModified { .. } => "precondition",
        Error::NotSupported { .. } | Error::NotImplemented { .. } => "not_supported",
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

/// Does this error mean the object store is unreachable?
///
/// [`record_health`] feeds the consecutive-failure gauge behind `/readyz`
/// and zombie-mode entry, so only genuine connectivity failures belong
/// there. Every error below is a *reply* — the store was reached and
/// answered — and SlateDB produces them constantly in normal operation:
///
/// * `NotFound` — the WAL/manifest pollers probe for not-yet-written
///   objects (`wal/…0001.sst`, `gc/manifest.boundary`) on every tick.
/// * `NotImplemented` / `NotSupported` — SlateDB attaches a ULID
///   attribute to conditional puts so a timeout-after-write is
///   verifiable; `LocalFileSystem` rejects attributes outright, and
///   SlateDB then retries the put without one (see its
///   `retrying_object_store.rs`). The rejected first attempt is expected.
/// * `Precondition` / `AlreadyExists` / `NotModified` — normal outcomes
///   of SlateDB's compare-and-swap manifest updates.
///
/// Counting these drove the gauge past the unhealthy threshold within
/// seconds of opening an idle partition, spamming "PARTIAL NETWORK
/// PARTITION DETECTED" and bouncing the broker through zombie mode.
/// Unknown (`#[non_exhaustive]`) variants stay unhealthy — fail closed on
/// anything not classified here.
fn indicates_unreachable(err: &Error) -> bool {
    !matches!(
        err,
        Error::NotFound { .. }
            | Error::AlreadyExists { .. }
            | Error::Precondition { .. }
            | Error::NotModified { .. }
            | Error::NotSupported { .. }
            | Error::NotImplemented { .. }
            | Error::InvalidPath { .. }
            | Error::UnknownConfigurationKey { .. }
    )
}

fn record<T>(operation: &'static str, started: Instant, result: &Result<T>) {
    let duration = started.elapsed().as_secs_f64();
    let status = if result.is_ok() { "success" } else { "error" };
    metrics::record_object_store_operation(operation, status, duration);
    if let Err(e) = result {
        // Still counted per error kind: a flood of NotFound is worth
        // seeing on a dashboard even though it says nothing about
        // reachability.
        metrics::record_object_store_error(operation, classify_error(e));
        if indicates_unreachable(e) {
            record_health(false);
        }
    } else {
        record_health(true);
    }
}

/// Error bookkeeping for a failed item from a streaming operation
/// (`list`, `delete_stream`, a `GetResult` body), which report per item
/// rather than per call. Same reachability gate as [`record`].
fn record_stream_error(operation: &'static str, err: &Error) {
    metrics::record_object_store_error(operation, classify_error(err));
    if indicates_unreachable(err) {
        record_health(false);
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

// NOTE: object_store 0.14 moved the convenience methods (`put`, `get`,
// `get_range`, `head`, `delete`, `copy`, `rename`, and the
// `*_if_not_exists` pair) out of `ObjectStore` and into the
// blanket-implemented `ObjectStoreExt`, which routes every one of them
// through the `*_opts` methods below. Instrumenting the core methods
// therefore covers the convenience calls too — and can't miss one the
// way the old per-method interception could.
#[async_trait]
impl ObjectStore for MetricsObjectStore {
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

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        // `ObjectStoreExt::head` is a `get_opts` with `head: true`, so keep
        // reporting it under its own operation label rather than folding
        // metadata probes into the read path's latency histogram.
        let operation = if options.head { "head" } else { "get" };
        let started = Instant::now();
        let result = self.inner.get_opts(location, options).await;
        record(operation, started, &result);
        result.map(wrap_get_result)
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        // Delegated rather than left to the default impl so the inner
        // store's range-coalescing still applies.
        let started = Instant::now();
        let result = self.inner.get_ranges(location, ranges).await;
        record("get", started, &result);
        if let Ok(parts) = &result {
            let total: u64 = parts.iter().map(|b| b.len() as u64).sum();
            metrics::record_object_store_bytes("read", total);
        }
        result
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, Result<Path>>,
    ) -> BoxStream<'static, Result<Path>> {
        let upstream = self.inner.delete_stream(locations);
        upstream
            .inspect(|item| match item {
                Ok(_) => {
                    metrics::record_object_store_operation("delete", "success", 0.0);
                    record_health(true);
                }
                Err(e) => {
                    metrics::record_object_store_operation("delete", "error", 0.0);
                    record_stream_error("delete", e);
                }
            })
            .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        let upstream = self.inner.list(prefix);
        upstream
            .inspect(|item| {
                if let Err(e) = item {
                    record_stream_error("list", e);
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
                    record_stream_error("list", e);
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

    async fn copy_opts(&self, from: &Path, to: &Path, options: CopyOptions) -> Result<()> {
        let started = Instant::now();
        let result = self.inner.copy_opts(from, to, options).await;
        record("copy", started, &result);
        result
    }

    async fn rename_opts(&self, from: &Path, to: &Path, options: RenameOptions) -> Result<()> {
        // Overridden so renames keep their own label: the default impl is a
        // `copy_opts` + `delete`, which would report them as two unrelated
        // operations.
        let started = Instant::now();
        let result = self.inner.rename_opts(from, to, options).await;
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
        extensions,
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
                        record_stream_error("get", e);
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
        extensions,
    }
}
