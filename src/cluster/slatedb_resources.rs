//! Broker-wide SlateDB resources shared across every per-partition `Db`.
//!
//! Each partition that's acquired today opens its own SlateDB instance
//! (see `partition_store.rs`). Without intervention SlateDB falls back to
//! a default per-DB block cache and spawns its background compactor onto
//! the ambient runtime. At N partitions on one broker that's N × cache
//! footprint and N independent compactor pools — the largest scaling
//! hazard the project flagged.
//!
//! [`SharedSlateDbResources`] holds the two long-lived broker-wide
//! handles SlateDB exposes for sharing:
//!
//! - **`Arc<dyn DbCache>`** — built once via `MokaCache::new_with_opts`,
//!   passed to every `Db::builder` via `with_memory_cache`. SlateDB
//!   wraps the Arc in a `DbCacheWrapper` per-Db that adds scope tracking
//!   while all DBs share the same underlying bounded cache.
//!
//! - **`tokio::runtime::Handle`** for compaction — every per-Db compactor
//!   spawns onto one shared multi-thread runtime, capping total
//!   compaction parallelism via `slatedb_compaction_workers`. A
//!   **dedicated** runtime (not `control_runtime`) so compaction CPU
//!   bursts never head-of-line block raft heartbeats.
//!
//! Compactor instance sharing isn't possible in slatedb 0.10 — only the
//! runtime. A custom `CompactionSchedulerSupplier` for cross-DB
//! prioritization is deferred (see `implementation_plan.md`).

use std::sync::Arc;

use slatedb::db_cache::DbCache;
use slatedb::db_cache::moka::{MokaCache, MokaCacheOptions};
use tokio::runtime::{Handle, Runtime};

use super::ClusterConfig;

/// `Arc<Runtime>` wrapper that calls `shutdown_background` on drop.
///
/// Tokio panics when a `Runtime` is dropped from within an async
/// context ("Cannot drop a runtime in a context where blocking is not
/// allowed"). `PartitionManager` is constructed and dropped from
/// inside async code — including every `#[tokio::test]` — so we cannot
/// drop the bare `Runtime` directly. `shutdown_background` returns
/// immediately without waiting for in-flight tasks; combined with
/// `PartitionManager::stop` having already closed every store before
/// the manager drops, no compaction tasks should be in flight when
/// this runs anyway.
pub struct OwnedCompactionRuntime {
    // `Option` so `Drop` can `take()` ownership of the inner `Runtime`
    // and consume it via `shutdown_background()` (which takes `self`).
    inner: Option<Runtime>,
}

impl OwnedCompactionRuntime {
    fn new(rt: Runtime) -> Self {
        Self { inner: Some(rt) }
    }

    /// Borrow the runtime handle for spawning compaction tasks.
    pub fn handle(&self) -> Handle {
        self.inner
            .as_ref()
            .expect("OwnedCompactionRuntime accessed after take")
            .handle()
            .clone()
    }
}

impl Drop for OwnedCompactionRuntime {
    fn drop(&mut self) {
        if let Some(rt) = self.inner.take() {
            // `shutdown_background` returns immediately; in-flight
            // tasks are *not* awaited. The store-close path in
            // `PartitionManager::stop` is what awaits compaction
            // tasks; this drop is the cleanup for memory and OS
            // threads after that.
            rt.shutdown_background();
        }
    }
}

/// One-broker pool of long-lived SlateDB resources injected into every
/// per-partition `Db::builder`.
///
/// [`Self::cache`] is `None` when the broker is configured with
/// `slatedb_block_cache_bytes == 0`, in which case each `Db` falls back
/// to SlateDB's default per-DB cache (today's behaviour). The compaction
/// handle is always present — its workers come from a dedicated runtime
/// (preferred) or, when `slatedb_compaction_workers == 0`, the ambient
/// request runtime as an escape hatch.
#[derive(Clone)]
pub struct SharedSlateDbResources {
    /// Block cache shared across every `Db` opened on this broker.
    /// `None` disables the shared cache; per-DB defaults take over.
    pub cache: Option<Arc<dyn DbCache>>,
    /// Runtime onto which every per-`Db` compactor task spawns.
    pub compaction_handle: Handle,
}

impl SharedSlateDbResources {
    /// Build broker-wide resources from `config`.
    ///
    /// Returns the resources plus an `Option<Arc<Runtime>>` that the
    /// caller MUST hold until after every `Db` is closed. Dropping the
    /// runtime while in-flight compactions are running aborts those
    /// tasks mid-flush — the close-before-shutdown contract documented
    /// in `partition_manager.rs:2296` extends to the dedicated
    /// compaction runtime too.
    ///
    /// When `slatedb_compaction_workers == 0` no dedicated runtime is
    /// built; the returned `Handle` is `Handle::current()` (the ambient
    /// runtime) and the `Option<Arc<Runtime>>` is `None`. This is the
    /// escape hatch — the request runtime now hosts compaction CPU
    /// bursts, with the failover risk noted in the field's docstring.
    pub fn from_config(
        config: &ClusterConfig,
    ) -> std::io::Result<(Self, Option<Arc<OwnedCompactionRuntime>>)> {
        let cache: Option<Arc<dyn DbCache>> = if config.slatedb_block_cache_bytes == 0 {
            None
        } else {
            // `MokaCache`'s weigher returns `CachedEntry::size()` so
            // `max_capacity` is interpreted as a byte budget, not an
            // entry-count budget. Casting via `u64` is safe because
            // `slatedb_block_cache_bytes: usize` already validates the
            // shape; `0` is the explicit disable above.
            let opts = MokaCacheOptions {
                max_capacity: config.slatedb_block_cache_bytes as u64,
                // Leave TTL/TTI unset — we want size-driven eviction
                // only. SlateDB's per-DB scope wrapper handles the
                // lifetime layer; the underlying cache is purely a
                // bounded-byte LRU.
                time_to_live: None,
                time_to_idle: None,
            };
            Some(Arc::new(MokaCache::new_with_opts(opts)))
        };

        let (compaction_handle, owned) = if config.slatedb_compaction_workers == 0 {
            // Escape hatch: no dedicated runtime, every compactor
            // spawns onto whatever runtime called us. Useful for tests
            // and pathological-low-resource hosts; surfaces the
            // failover risk in the field's docstring.
            (Handle::current(), None)
        } else {
            let runtime = build_compaction_runtime(
                config.slatedb_compaction_workers,
                &config.slatedb_compaction_thread_name_prefix,
            )?;
            let owned = OwnedCompactionRuntime::new(runtime);
            let handle = owned.handle();
            (handle, Some(Arc::new(owned)))
        };

        Ok((
            Self {
                cache,
                compaction_handle,
            },
            owned,
        ))
    }

    /// Lightweight resources for tests: 4 MiB cache, 1 worker thread.
    /// Avoids spinning a 4-thread runtime per test fixture.
    ///
    /// Returns the resources plus the owned runtime; caller holds the
    /// `Arc` past test drop ordering (otherwise tokio panics on
    /// runtime-drop-inside-async-context).
    #[cfg(test)]
    pub fn for_tests() -> (Self, Arc<OwnedCompactionRuntime>) {
        let opts = MokaCacheOptions {
            max_capacity: 4 * 1024 * 1024,
            time_to_live: None,
            time_to_idle: None,
        };
        let cache: Arc<dyn DbCache> = Arc::new(MokaCache::new_with_opts(opts));
        let runtime = build_compaction_runtime(1, "slatedb-compact-test")
            .expect("test compaction runtime must build");
        let owned = Arc::new(OwnedCompactionRuntime::new(runtime));
        let handle = owned.handle();
        (
            Self {
                cache: Some(cache),
                compaction_handle: handle,
            },
            owned,
        )
    }

    /// Non-functional placeholder used inside in-process unit tests that
    /// exercise the wiring shape but never actually open a `Db` (so the
    /// cache trait object's identity is irrelevant). The compaction
    /// handle is `Handle::current()`, so this MUST only be called from
    /// inside a tokio runtime context.
    #[cfg(test)]
    pub fn for_unit_tests() -> Self {
        Self {
            cache: None,
            compaction_handle: Handle::current(),
        }
    }
}

/// Build the dedicated compaction runtime. Lifted to a free helper so
/// production and test code share one source of truth for the worker /
/// thread name configuration.
fn build_compaction_runtime(
    worker_threads: usize,
    thread_name_prefix: &str,
) -> std::io::Result<Runtime> {
    // Multi-thread runtime so per-`Db` compactor tasks can run in
    // parallel up to `worker_threads`. `enable_all` so the runtime
    // can drive object-store I/O (compaction reads/writes SSTs
    // through the same async I/O primitives as the rest of the
    // system).
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .thread_name(thread_name_prefix)
        .enable_all()
        .build()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_zero_cache_disables_shared_cache() {
        // An operator can A/B against the legacy per-DB-cache behaviour
        // by setting `slatedb_block_cache_bytes = 0`. Pin that the
        // resources struct returns `None` for the cache in that case so
        // the per-DB default kicks in at the builder.
        let mut cfg = ClusterConfig::default();
        cfg.slatedb_block_cache_bytes = 0;
        // Avoid building the dedicated runtime here so we don't have
        // to enter a tokio context just to build resources.
        cfg.slatedb_compaction_workers = 0;
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (res, owned) = rt
            .block_on(async { SharedSlateDbResources::from_config(&cfg) })
            .unwrap();
        assert!(res.cache.is_none(), "0-byte cache must yield None");
        assert!(
            owned.is_none(),
            "0-worker compaction must not allocate a dedicated runtime"
        );
    }

    #[test]
    fn config_default_builds_cache_and_dedicated_runtime() {
        let cfg = ClusterConfig::default();
        // Sanity-check the default — if a future commit zeroes the
        // default cache we want a test to flag it.
        assert!(cfg.slatedb_block_cache_bytes > 0);
        assert!(cfg.slatedb_compaction_workers > 0);

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (res, owned) = rt
            .block_on(async { SharedSlateDbResources::from_config(&cfg) })
            .unwrap();
        assert!(res.cache.is_some(), "default config must build a cache");
        assert!(
            owned.is_some(),
            "default config must build a dedicated compaction runtime"
        );
        assert_eq!(
            res.compaction_handle.metrics().num_workers(),
            cfg.slatedb_compaction_workers,
            "dedicated runtime worker count must match config"
        );
    }

    #[test]
    fn for_tests_helper_yields_shared_cache_and_one_worker() {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (res, owned) = rt.block_on(async { SharedSlateDbResources::for_tests() });
        assert!(res.cache.is_some());
        assert_eq!(res.compaction_handle.metrics().num_workers(), 1);
        // Hold runtime past test drop ordering; explicit so reviewer
        // sees the lifetime contract.
        drop(owned);
    }
}
