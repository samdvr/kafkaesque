//! Ownership context, sweep waiting, and store-builder tuning.

use dashmap::DashMap;
use object_store::ObjectStore;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::debug;

use super::PartitionStateMap;
use super::jitter::with_jitter;
use crate::cluster::PartitionKey;
use crate::cluster::traits::ClusterCoordinator;
use crate::cluster::zombie_mode::ZombieModeState;

/// How often the ownership loop re-checks whether its hash-ring assignment
/// moved while it is waiting out the steady-state interval.
///
/// Matches the reconciler's 200ms-class cadence. The check is a local
/// control-SM read plus a ring computation (no RPC, no object-store I/O), so
/// the cost is a read-lock and O(topics × partitions) hashing per poll.
const ASSIGNMENT_POLL_INTERVAL: Duration = Duration::from_millis(250);

/// Why [`wait_for_ownership_sweep`] returned.
pub(super) enum OwnershipWake {
    /// The steady-state jittered interval elapsed.
    Interval,
    /// This broker's assigned-partition set changed — sweep now.
    AssignmentChanged,
    /// Shutdown was signalled; the loop must exit.
    Shutdown,
}

/// Wait until it is time for the next ownership sweep.
///
/// Returns as soon as **either** the jittered steady-state interval elapses
/// or this broker's assigned-partition set changes.
///
/// The second condition is what makes a freshly created topic usable
/// promptly. `CreateTopic` reaches every broker's control state machine in
/// one Raft round trip, so each broker can compute its new hash-ring
/// assignment almost immediately — but before this, acquisition only
/// happened on the next blind sweep, up to `ownership_check_interval` (5s by
/// default, ±15% jitter) later. In the meantime every partition of the new
/// topic that this broker owned reported `LeaderNotAvailable` in metadata,
/// and a consumer subscribed across all partitions could not make progress
/// on any of them. A 10-partition auto-created topic routinely showed 6 of
/// 10 partitions leaderless seconds after a successful produce.
///
/// Waking on assignment change also shortens rebalance after a broker joins
/// or is fenced, since that moves the ring too.
///
/// Acquisition is idempotent and lease-guarded, so an extra sweep is safe;
/// the `last_assigned` comparison keeps steady state at zero extra sweeps.
pub(super) async fn wait_for_ownership_sweep<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    interval: Duration,
    last_assigned: Option<&HashSet<(String, i32)>>,
    shutdown_rx: &mut broadcast::Receiver<()>,
) -> OwnershipWake {
    // Jittered deadline prevents a thundering herd of synchronized sweeps.
    let deadline = tokio::time::Instant::now() + with_jitter(interval);

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return OwnershipWake::Interval;
        }
        let nap = ASSIGNMENT_POLL_INTERVAL.min(deadline - now);
        tokio::select! {
            _ = tokio::time::sleep(nap) => {}
            _ = shutdown_rx.recv() => return OwnershipWake::Shutdown,
        }

        // Only meaningful once a sweep has established a baseline. Before
        // that, fall through to the interval so the first sweep still runs
        // on the initial-jitter schedule.
        let Some(previous) = last_assigned else {
            continue;
        };
        match ctx.coordinator.get_assigned_partitions().await {
            Ok(assigned) => {
                let current: HashSet<(String, i32)> = assigned.into_iter().collect();
                if current != *previous {
                    return OwnershipWake::AssignmentChanged;
                }
            }
            // A failed read is not a reason to sweep early; the interval
            // still fires and the sweep logs its own failure there.
            Err(e) => {
                debug!(error = %e, "Assignment-change poll failed; waiting for interval");
            }
        }
    }
}

pub(super) struct OwnershipContext<C: ClusterCoordinator> {
    pub(super) coordinator: Arc<C>,
    pub(super) partition_states: PartitionStateMap,
    pub(super) object_store: Arc<dyn ObjectStore>,
    pub(super) base_path: String,
    pub(super) lease_secs: u64,
    pub(super) broker_id: i32,
    pub(super) max_fetch_response_size: usize,
    pub(super) producer_state_cache_ttl_secs: u64,
    pub(super) zombie_state: Arc<ZombieModeState>,
    pub(super) fail_on_recovery_gap: bool,
    pub(super) lease_cache: Arc<DashMap<PartitionKey, Instant>>,
    pub(super) min_lease_ttl_for_write_secs: u64,
    // SlateDB / index tuning. These were previously configured on
    // `ClusterConfig` but never reached the stores — operators believed
    // they had set memory limits that were silently ignored.
    pub(super) batch_index_max_size: usize,
    pub(super) slatedb_max_unflushed_bytes: usize,
    pub(super) slatedb_l0_sst_size_bytes: usize,
    pub(super) slatedb_flush_interval_ms: u64,
    /// Broker-wide shared block cache + dedicated compaction runtime
    /// handle. Threaded into every `Db::builder` via `apply_store_tuning`
    /// so all per-partition `Db` instances share one cache and one
    /// bounded compaction runtime.
    pub(super) slatedb_resources: crate::cluster::slatedb_resources::SharedSlateDbResources,
    /// Maximum partitions this broker may own at once. `0` means unbounded.
    /// Enforced in `acquire_partition_core` before opening a SlateDB instance.
    pub(super) max_owned_partitions_per_broker: usize,
    /// Per-key serialization for `acquire_partition_core`.
    pub(super) acquire_locks: Arc<DashMap<PartitionKey, Arc<tokio::sync::Mutex<()>>>>,
    /// Topic-name interning cache shared with the parent `PartitionManager`.
    /// Looked up via `partition_key` so a hot topic resolves to a refcount
    /// bump on the existing `Arc<str>` instead of a fresh heap allocation
    /// per produce/fetch.
    pub(super) topic_name_cache: Arc<DashMap<String, Arc<str>>>,
    /// Optimistic reservation counter for the per-broker owned-partition cap.
    /// Concurrent acquires for *different* partitions used to race past the
    /// cap check (each saw `owned < cap` and proceeded), letting the broker
    /// briefly exceed `max_owned_partitions_per_broker` by the concurrency
    /// factor. Each acquire now `fetch_add`s a slot before the cap check
    /// and the post-increment value is compared against the cap, so the
    /// first request to push the count over the limit is the one rejected.
    /// Decremented unconditionally on every exit from the cap-checked
    /// region (success or failure) via a scope guard.
    pub(super) pending_acquires: Arc<std::sync::atomic::AtomicUsize>,
}

impl<C: ClusterCoordinator> OwnershipContext<C> {
    pub(super) fn partition_key(&self, topic: &str, partition: i32) -> PartitionKey {
        if let Some(cached) = self.topic_name_cache.get(topic) {
            return (cached.clone(), partition);
        }
        let arc: Arc<str> = Arc::from(topic);
        self.topic_name_cache.insert(topic.to_string(), arc.clone());
        (arc, partition)
    }
}

/// Apply the SlateDB / batch-index tuning from `ClusterConfig` to a store
/// builder. Centralized so every open path (acquire, zombie reopen) gets the
/// same effective configuration.
pub(super) fn apply_store_tuning<C: ClusterCoordinator>(
    builder: crate::cluster::partition_store::PartitionStoreBuilder,
    ctx: &OwnershipContext<C>,
) -> crate::cluster::partition_store::PartitionStoreBuilder {
    let mut builder = builder
        .max_fetch_response_size(ctx.max_fetch_response_size)
        .producer_state_cache_ttl_secs(ctx.producer_state_cache_ttl_secs)
        .fail_on_recovery_gap(ctx.fail_on_recovery_gap)
        .min_lease_ttl_for_write_secs(ctx.min_lease_ttl_for_write_secs)
        .batch_index_max_size(ctx.batch_index_max_size)
        .slatedb_max_unflushed_bytes(ctx.slatedb_max_unflushed_bytes)
        .slatedb_l0_sst_size_bytes(ctx.slatedb_l0_sst_size_bytes)
        .slatedb_flush_interval_ms(ctx.slatedb_flush_interval_ms)
        .slatedb_compaction_handle(ctx.slatedb_resources.compaction_handle.clone());
    // The cache is `Option<Arc<dyn DbCache>>` — `None` when the
    // operator zeroes `slatedb_block_cache_bytes` to A/B against the
    // legacy per-DB-cache behaviour. Only set the builder field when
    // present so `None` truly falls through to SlateDB's per-DB
    // default rather than overriding it with a useless wrapper.
    if let Some(cache) = ctx.slatedb_resources.cache.clone() {
        builder = builder.slatedb_block_cache(cache);
    }
    builder
}

