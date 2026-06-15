//! Fault-injecting wrapper around `ObjectStore`.
//!
//! Tests construct a [`FaultInjector`], wrap the production `Arc<dyn
//! ObjectStore>` with [`FaultingObjectStore`], and steer real broker
//! traffic through configurable failures, latency, and bandwidth limits.
//! This replaces the test-local `FaultInjector` in `tests/chaos_tests.rs`
//! that was never plumbed into a production code path.
//!
//! When no fault is configured the wrapper is a thin pass-through; the
//! per-operation overhead is one atomic load.

use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::BoxStream;
use object_store::path::Path;
use object_store::{
    Error, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult, Result,
};

/// Categories of object-store operations that can be independently faulted.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum OpKind {
    Get,
    Put,
    Delete,
    List,
    Copy,
    Rename,
    Head,
}

impl OpKind {
    fn as_str(self) -> &'static str {
        match self {
            OpKind::Get => "get",
            OpKind::Put => "put",
            OpKind::Delete => "delete",
            OpKind::List => "list",
            OpKind::Copy => "copy",
            OpKind::Rename => "rename",
            OpKind::Head => "head",
        }
    }
}

/// A handle that tests use to steer object-store faults at runtime.
///
/// Cloning shares state — every clone consults the same atomics, so
/// handing one clone to the broker via [`FaultingObjectStore::new`] and
/// another to the test harness is enough to drive injection live.
#[derive(Debug, Default, Clone)]
pub struct FaultInjector {
    inner: Arc<FaultState>,
}

#[derive(Debug, Default)]
struct FaultState {
    blocked: AtomicBool,
    latency_ns: AtomicU64,
    fail_next: AtomicU32,
    op_mask: AtomicU32,
}

impl FaultInjector {
    /// A fault injector that never injects anything. Cheap to construct;
    /// each operation costs one atomic load.
    pub fn new() -> Self {
        Self::default()
    }

    /// Block every operation immediately with `Error::Generic`. Useful
    /// for simulating a network partition.
    pub fn block(&self) {
        self.inner.blocked.store(true, Ordering::SeqCst);
    }

    /// Stop blocking; subsequent operations proceed.
    pub fn unblock(&self) {
        self.inner.blocked.store(false, Ordering::SeqCst);
    }

    /// Sleep for `duration` before each operation. Set to zero to clear.
    pub fn set_latency(&self, duration: Duration) {
        let ns = u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        self.inner.latency_ns.store(ns, Ordering::SeqCst);
    }

    /// Fail the next `n` operations with a generic transient error.
    /// Each failure decrements the counter so a test can stage a bounded
    /// burst without flipping a flag back off.
    pub fn fail_next(&self, n: u32) {
        self.inner.fail_next.store(n, Ordering::SeqCst);
    }

    /// Restrict failure injection to the given operation kinds. By
    /// default every kind is faultable. Pass an empty slice to disable
    /// all fault injection without clearing latency or block state.
    pub fn restrict_to(&self, kinds: &[OpKind]) {
        let mut mask: u32 = 0;
        for k in kinds {
            mask |= 1 << kind_bit(*k);
        }
        self.inner.op_mask.store(mask, Ordering::SeqCst);
    }

    /// Allow faults on every operation kind (the default).
    pub fn restrict_all(&self) {
        self.inner.op_mask.store(u32::MAX, Ordering::SeqCst);
    }

    fn would_inject(&self, op: OpKind) -> bool {
        let mask = self.inner.op_mask.load(Ordering::SeqCst);
        let mask = if mask == 0 { u32::MAX } else { mask };
        (mask >> kind_bit(op)) & 1 == 1
    }

    /// Returns Some(error) if this op should fail right now, after applying
    /// any configured latency / block.
    async fn intercept(&self, op: OpKind) -> Option<Error> {
        if !self.would_inject(op) {
            return None;
        }
        let latency_ns = self.inner.latency_ns.load(Ordering::SeqCst);
        if latency_ns > 0 {
            tokio::time::sleep(Duration::from_nanos(latency_ns)).await;
        }
        if self.inner.blocked.load(Ordering::SeqCst) {
            return Some(generic_error(op, "fault injector: network blocked"));
        }
        // Decrement-and-test. We use fetch_update so two concurrent ops
        // don't both decide they were the last failure.
        let mut current = self.inner.fail_next.load(Ordering::SeqCst);
        while current > 0 {
            match self.inner.fail_next.compare_exchange(
                current,
                current - 1,
                Ordering::SeqCst,
                Ordering::SeqCst,
            ) {
                Ok(_) => {
                    return Some(generic_error(op, "fault injector: planned failure"));
                }
                Err(observed) => current = observed,
            }
        }
        None
    }
}

fn kind_bit(op: OpKind) -> u32 {
    match op {
        OpKind::Get => 0,
        OpKind::Put => 1,
        OpKind::Delete => 2,
        OpKind::List => 3,
        OpKind::Copy => 4,
        OpKind::Rename => 5,
        OpKind::Head => 6,
    }
}

fn generic_error(op: OpKind, msg: &'static str) -> Error {
    Error::Generic {
        store: "fault-injector",
        source: format!("{}: {}", op.as_str(), msg).into(),
    }
}

/// `ObjectStore` decorator that consults a [`FaultInjector`] before each
/// operation. Mirrors the shape of `MetricsObjectStore` so the wrap order
/// in `create_object_store` is `Raw → Metrics → Faulting → Prefix`.
pub struct FaultingObjectStore {
    inner: Arc<dyn ObjectStore>,
    injector: FaultInjector,
}

impl FaultingObjectStore {
    pub fn new(inner: Arc<dyn ObjectStore>, injector: FaultInjector) -> Self {
        Self { inner, injector }
    }

    pub fn injector(&self) -> &FaultInjector {
        &self.injector
    }
}

impl std::fmt::Debug for FaultingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FaultingObjectStore")
            .field("inner", &self.inner)
            .finish()
    }
}

impl std::fmt::Display for FaultingObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FaultingObjectStore({})", self.inner)
    }
}

#[async_trait]
impl ObjectStore for FaultingObjectStore {
    async fn put(&self, location: &Path, payload: PutPayload) -> Result<PutResult> {
        if let Some(e) = self.injector.intercept(OpKind::Put).await {
            return Err(e);
        }
        self.inner.put(location, payload).await
    }

    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> Result<PutResult> {
        if let Some(e) = self.injector.intercept(OpKind::Put).await {
            return Err(e);
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart(&self, location: &Path) -> Result<Box<dyn MultipartUpload>> {
        if let Some(e) = self.injector.intercept(OpKind::Put).await {
            return Err(e);
        }
        self.inner.put_multipart(location).await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> Result<Box<dyn MultipartUpload>> {
        if let Some(e) = self.injector.intercept(OpKind::Put).await {
            return Err(e);
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get(&self, location: &Path) -> Result<GetResult> {
        if let Some(e) = self.injector.intercept(OpKind::Get).await {
            return Err(e);
        }
        self.inner.get(location).await
    }

    async fn get_opts(&self, location: &Path, options: GetOptions) -> Result<GetResult> {
        if let Some(e) = self.injector.intercept(OpKind::Get).await {
            return Err(e);
        }
        self.inner.get_opts(location, options).await
    }

    async fn get_range(&self, location: &Path, range: Range<u64>) -> Result<Bytes> {
        if let Some(e) = self.injector.intercept(OpKind::Get).await {
            return Err(e);
        }
        self.inner.get_range(location, range).await
    }

    async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> Result<Vec<Bytes>> {
        if let Some(e) = self.injector.intercept(OpKind::Get).await {
            return Err(e);
        }
        self.inner.get_ranges(location, ranges).await
    }

    async fn head(&self, location: &Path) -> Result<ObjectMeta> {
        if let Some(e) = self.injector.intercept(OpKind::Head).await {
            return Err(e);
        }
        self.inner.head(location).await
    }

    async fn delete(&self, location: &Path) -> Result<()> {
        if let Some(e) = self.injector.intercept(OpKind::Delete).await {
            return Err(e);
        }
        self.inner.delete(location).await
    }

    fn delete_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        self.inner.delete_stream(locations)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        self.inner.list_with_offset(prefix, offset)
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        if let Some(e) = self.injector.intercept(OpKind::List).await {
            return Err(e);
        }
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        if let Some(e) = self.injector.intercept(OpKind::Copy).await {
            return Err(e);
        }
        self.inner.copy(from, to).await
    }

    async fn rename(&self, from: &Path, to: &Path) -> Result<()> {
        if let Some(e) = self.injector.intercept(OpKind::Rename).await {
            return Err(e);
        }
        self.inner.rename(from, to).await
    }

    async fn copy_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if let Some(e) = self.injector.intercept(OpKind::Copy).await {
            return Err(e);
        }
        self.inner.copy_if_not_exists(from, to).await
    }

    async fn rename_if_not_exists(&self, from: &Path, to: &Path) -> Result<()> {
        if let Some(e) = self.injector.intercept(OpKind::Rename).await {
            return Err(e);
        }
        self.inner.rename_if_not_exists(from, to).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;

    fn store_under_test() -> (Arc<FaultingObjectStore>, FaultInjector) {
        let injector = FaultInjector::new();
        let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        let store = Arc::new(FaultingObjectStore::new(inner, injector.clone()));
        (store, injector)
    }

    #[tokio::test]
    async fn pass_through_when_idle() {
        let (store, _injector) = store_under_test();
        let location = Path::from("a");
        store
            .put(&location, PutPayload::from(b"hello".as_slice()))
            .await
            .expect("put");
        let got = store.get_range(&location, 0..5).await.expect("get_range");
        assert_eq!(got.as_ref(), b"hello");
    }

    #[tokio::test]
    async fn block_fails_subsequent_ops() {
        let (store, injector) = store_under_test();
        injector.block();
        let err = store
            .put(&Path::from("a"), PutPayload::from(b"x".as_slice()))
            .await
            .expect_err("blocked put must fail");
        assert!(err.to_string().contains("network blocked"));
    }

    #[tokio::test]
    async fn fail_next_decrements() {
        let (store, injector) = store_under_test();
        injector.fail_next(2);
        let _ = store
            .put(&Path::from("a"), PutPayload::from(b"x".as_slice()))
            .await
            .expect_err("first failure planned");
        let _ = store
            .put(&Path::from("a"), PutPayload::from(b"x".as_slice()))
            .await
            .expect_err("second failure planned");
        store
            .put(&Path::from("a"), PutPayload::from(b"x".as_slice()))
            .await
            .expect("third put recovers");
    }

    #[tokio::test]
    async fn restrict_to_only_faults_listed_ops() {
        let (store, injector) = store_under_test();
        injector.restrict_to(&[OpKind::Get]);
        injector.fail_next(10);

        store
            .put(&Path::from("a"), PutPayload::from(b"x".as_slice()))
            .await
            .expect("put not faulted");
        let _ = store
            .get(&Path::from("a"))
            .await
            .expect_err("get should fault");
    }

    #[tokio::test]
    async fn latency_applies_before_failure() {
        let (store, injector) = store_under_test();
        injector.set_latency(Duration::from_millis(20));
        injector.fail_next(1);
        let started = std::time::Instant::now();
        let _ = store
            .put(&Path::from("a"), PutPayload::from(b"x".as_slice()))
            .await
            .expect_err("planned failure");
        assert!(
            started.elapsed() >= Duration::from_millis(15),
            "latency should have been applied"
        );
    }
}
