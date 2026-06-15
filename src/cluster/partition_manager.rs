//! Partition manager for handling ownership and lifecycle of partitions.
//!
//! Responsibilities:
//! - Track which partitions this broker owns
//! - Acquire/release partition ownership via coordinator
//! - Open/close SlateDB instances for partitions
//! - Periodic lease renewal
//! - Handle rebalancing events

use dashmap::DashMap;
use object_store::ObjectStore;
use std::collections::HashSet;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::{RwLock, broadcast};
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

use super::PartitionKey;
use super::config::ClusterConfig;
use super::error::{SlateDBError, SlateDBResult};
use super::partition_state::PartitionState;
use super::partition_store::PartitionStore;
use super::rebalance_coordinator::{
    BackgroundTaskHandles, CoordinatorExecutorAdapter, RebalanceCoordinator,
    RebalanceCoordinatorConfig,
};
use super::traits::ClusterCoordinator;
use super::zombie_mode::ZombieModeState;

/// Type alias for partition state map.
///
/// Uses `DashMap` for lock-free concurrent access.
/// This eliminates lock contention when many partitions are accessed concurrently.
/// Each partition can be accessed independently without blocking other partitions.
type PartitionStateMap = Arc<DashMap<PartitionKey, PartitionState>>;

/// Add jitter to a duration to prevent thundering herd.
///
/// Adds +/- 15% pseudo-random jitter to the base interval. This prevents all brokers
/// from sending heartbeats or renewing leases at exactly the same time after
/// a coordinator reconnection or cluster restart.
///
/// Uses `fastrand` crate for thread-local PRNG, which provides:
/// - Better entropy than system time nanoseconds
/// - Fast, non-blocking operation
/// - Automatically seeded per-thread from system entropy
///
/// # Example
/// A 10 second interval becomes anywhere from 8.5 to 11.5 seconds.
fn with_jitter(base: Duration) -> Duration {
    // Generate random factor in range [0.85, 1.15] using fastrand
    // fastrand::f64() returns a value in [0.0, 1.0)
    let jitter_factor = 0.85 + fastrand::f64() * 0.30;

    Duration::from_secs_f64(base.as_secs_f64() * jitter_factor)
}

/// One-shot uniform offset in `[0, base)` used as a per-task initial sleep.
///
/// `with_jitter` only varies a tick by ±15%, which still synchronizes the
/// *first* tick of every loop spawned in the same instant (e.g. all lease
/// renewal loops after a rebalance). This helper spreads first ticks
/// uniformly across a full interval, eliminating the resulting hot-spot on
/// the Raft leader.
fn initial_jitter(base: Duration) -> Duration {
    let millis = base.as_millis().min(u64::MAX as u128) as u64;
    if millis == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(fastrand::u64(0..millis))
}

/// Convert a "remaining TTL in seconds" lease window into an absolute
/// wall-clock expiry (epoch millis), suitable for publishing into the
/// partition store's `lease_expiry_ms`. Saturates the addition so a
/// malicious or buggy `remaining_secs` near `u64::MAX` cannot wrap
/// negative.
fn wall_clock_expiry_ms(remaining_secs: u64) -> i64 {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let remaining_ms: i64 = remaining_secs.saturating_mul(1000).min(i64::MAX as u64) as i64;
    now_ms.saturating_add(remaining_ms)
}

/// Manages partition ownership and SlateDB instances for this broker.
pub struct PartitionManager<C: ClusterCoordinator> {
    /// Broker identity.
    broker_id: i32,

    /// Generic coordinator for cluster-wide state.
    coordinator: Arc<C>,

    /// Object store for SlateDB.
    object_store: Arc<dyn ObjectStore>,

    /// Base path in object store.
    base_path: String,

    /// Partition states with explicit state machine tracking.
    partition_states: PartitionStateMap,

    /// Configuration.
    config: ClusterConfig,

    /// Background task handles.
    task_handles: RwLock<Vec<JoinHandle<()>>>,

    /// Shutdown signal sender.
    shutdown_tx: broadcast::Sender<()>,

    /// Zombie mode state for split-brain prevention.
    /// Wraps the atomic flag and entry timestamp with a type-safe API.
    zombie_state: Arc<ZombieModeState>,

    /// Lease expiration cache for fast-path writes.
    ///
    /// Maps (topic, partition) to the Instant when the cached lease expires.
    /// This allows `get_for_write` to skip Raft verification when the cached
    /// lease has sufficient remaining TTL.
    ///
    /// Cache entries are updated:
    /// - On successful `verify_and_extend_lease` calls
    /// - By the background lease renewal loop
    ///
    /// Cache entries are invalidated:
    /// - When ownership is lost
    /// - When entering zombie mode
    lease_cache: Arc<DashMap<PartitionKey, Instant>>,

    /// Cached `Arc<str>` topic names to avoid per-lookup allocations.
    topic_name_cache: Arc<DashMap<String, Arc<str>>>,

    /// Per-partition acquire serialization. Without this, two concurrent
    /// callers can both pass the existence check, build separate
    /// `PartitionStore` instances, and let the second `partition_states.insert`
    /// silently replace the first — leaking the orphaned SlateDB handle.
    /// Holding a per-key mutex across the whole acquire flow forces the
    /// second caller to wait, recheck ownership, and no-op.
    acquire_locks: Arc<DashMap<PartitionKey, Arc<tokio::sync::Mutex<()>>>>,

    /// Rebalance coordinator for fast failover and auto-balancing.
    ///
    /// This is responsible for:
    /// - Fast broker failure detection (500ms heartbeats)
    /// - Automatic partition redistribution on failure
    /// - Load-based auto-balancing
    rebalance_coordinator: Option<Arc<RebalanceCoordinator>>,

    /// Handles for rebalance coordinator background tasks.
    rebalance_task_handles: RwLock<Option<BackgroundTaskHandles>>,

    /// Runtime handle for spawning control plane tasks.
    control_runtime: Handle,
}

impl<C: ClusterCoordinator + 'static> PartitionManager<C> {
    /// Create a new partition manager.
    pub fn new(
        coordinator: Arc<C>,
        object_store: Arc<dyn ObjectStore>,
        config: ClusterConfig,
        control_runtime: Handle,
    ) -> Self {
        // Create shutdown channel with capacity for all background tasks
        let (shutdown_tx, _) = broadcast::channel(4);

        // Create rebalance coordinator if fast failover or auto-balancing is enabled
        let rebalance_coordinator = if config.fast_failover_enabled || config.auto_balancer_enabled
        {
            use super::auto_balancer::AutoBalancerConfig;
            use super::failure_detector::FailureDetectorConfig;
            use super::load_metrics::LoadMetricsConfig;

            // The failure detector is fed by *applied Raft broker heartbeats*,
            // which brokers send every `config.heartbeat_interval` (with up to
            // ±15% sender-side jitter) — there is no separate fast-heartbeat
            // sender running at `fast_heartbeat_interval_ms`. Telling the
            // detector to expect 500ms heartbeats while they actually arrive
            // every 5s would declare every healthy broker failed between two
            // consecutive heartbeats and trigger continuous spurious
            // failovers. Clamp the detector's expectation to the real cadence
            // and scale the jitter tolerance to absorb sender jitter plus
            // Raft replication/apply latency.
            let configured_interval = Duration::from_millis(config.fast_heartbeat_interval_ms);
            let actual_interval = config.heartbeat_interval;
            let detector_interval = configured_interval.max(actual_interval);
            if configured_interval < actual_interval {
                warn!(
                    broker_id = config.broker_id,
                    fast_heartbeat_interval_ms = configured_interval.as_millis() as u64,
                    heartbeat_interval_ms = actual_interval.as_millis() as u64,
                    "fast_heartbeat_interval_ms is shorter than the interval at which broker \
                     heartbeats are actually sent; clamping the failure detector to the real \
                     heartbeat cadence to avoid spurious failure detection. Lower \
                     HEARTBEAT_INTERVAL_SECS to detect failures faster."
                );
            }

            let rebalance_config = RebalanceCoordinatorConfig {
                failure_detector: FailureDetectorConfig {
                    heartbeat_interval: detector_interval,
                    failure_threshold: config.failure_threshold,
                    suspicion_threshold: config.failure_suspicion_threshold,
                    check_interval: (detector_interval / 2).max(Duration::from_millis(100)),
                    // Broker heartbeats carry up to ±15% jitter and go through
                    // Raft consensus before reaching the detector hook, so the
                    // default 50ms tolerance is far too tight at multi-second
                    // intervals. A quarter interval comfortably covers both.
                    jitter_tolerance: detector_interval / 4,
                    ..Default::default()
                },
                load_metrics: LoadMetricsConfig::default(),
                auto_balancer: AutoBalancerConfig {
                    enabled: config.auto_balancer_enabled,
                    evaluation_interval_secs: config.auto_balancer_evaluation_interval_secs,
                    deviation_threshold: config.auto_balancer_deviation_threshold,
                    max_partitions_per_cycle: config.auto_balancer_max_partitions_per_cycle,
                    cooldown_per_partition_secs: config.auto_balancer_cooldown_secs,
                    throughput_weight: config.auto_balancer_throughput_weight,
                    ..Default::default()
                },
                fast_failover_enabled: config.fast_failover_enabled,
                transfer_lease_duration_ms: config.lease_duration.as_millis() as u64,
                ..Default::default()
            };

            Some(Arc::new(RebalanceCoordinator::new(
                rebalance_config,
                config.broker_id,
                control_runtime.clone(),
            )))
        } else {
            None
        };

        Self {
            broker_id: config.broker_id,
            coordinator,
            object_store,
            base_path: config.object_store_path.clone(),
            partition_states: Arc::new(DashMap::new()),
            config,
            task_handles: RwLock::new(Vec::new()),
            shutdown_tx,
            zombie_state: Arc::new(ZombieModeState::new()),
            lease_cache: Arc::new(DashMap::new()),
            topic_name_cache: Arc::new(DashMap::new()),
            acquire_locks: Arc::new(DashMap::new()),
            rebalance_coordinator,
            rebalance_task_handles: RwLock::new(None),
            control_runtime,
        }
    }

    /// Check if broker is in zombie mode (cannot coordinate with cluster).
    pub fn is_zombie(&self) -> bool {
        self.zombie_state.is_active()
    }

    /// Get the count of partitions currently owned by this broker.
    ///
    /// This returns the number of partitions in the Owned state.
    pub fn owned_partition_count(&self) -> usize {
        self.partition_states
            .iter()
            .filter(|entry| entry.value().is_owned())
            .count()
    }

    /// Get the zombie mode state for sharing with other components.
    pub fn zombie_state(&self) -> Arc<ZombieModeState> {
        self.zombie_state.clone()
    }

    pub fn config(&self) -> &ClusterConfig {
        &self.config
    }

    fn partition_key(&self, topic: &str, partition: i32) -> PartitionKey {
        if let Some(cached) = self.topic_name_cache.get(topic) {
            return (cached.clone(), partition);
        }
        let arc: Arc<str> = Arc::from(topic);
        self.topic_name_cache.insert(topic.to_string(), arc.clone());
        (arc, partition)
    }

    pub fn coordinator(&self) -> &Arc<C> {
        &self.coordinator
    }

    /// The local rebalance coordinator, if fast failover or auto-balancing
    /// is enabled. Used by the cluster handler to wire broker heartbeats
    /// from the Raft state machine into the failure detector.
    pub fn rebalance_coordinator(&self) -> Option<&Arc<RebalanceCoordinator>> {
        self.rebalance_coordinator.as_ref()
    }

    /// Get the cached lease expiry time for a partition (test-only).
    ///
    /// Returns the `Instant` when the cached lease expires, or `None` if not cached.
    /// This is used to verify that lease renewal properly updates the cache.
    #[cfg(test)]
    pub fn get_cached_lease_expiry(&self, topic: &str, partition: i32) -> Option<Instant> {
        self.lease_cache
            .get(&(Arc::from(topic), partition))
            .map(|entry| *entry.value())
    }

    /// Manually trigger lease renewal for a specific partition (test-only).
    ///
    /// This is used to test that lease renewal properly updates the cache without
    /// needing to wait for the background renewal loop.
    #[cfg(test)]
    pub async fn renew_lease_for_test(&self, topic: &str, partition: i32) -> bool {
        let lease_secs = self.config.lease_duration.as_secs();
        match self
            .coordinator
            .renew_partition_lease(topic, partition, lease_secs)
            .await
        {
            Ok(true) => {
                // Update lease cache with new expiry time (same as renewal loop)
                let new_expiry = Instant::now() + Duration::from_secs(lease_secs);
                self.lease_cache
                    .insert((Arc::from(topic), partition), new_expiry);
                true
            }
            _ => false,
        }
    }

    /// Create an OwnershipContext for internal operations.
    fn ownership_context(&self) -> OwnershipContext<C> {
        OwnershipContext {
            coordinator: self.coordinator.clone(),
            partition_states: self.partition_states.clone(),
            object_store: self.object_store.clone(),
            base_path: self.base_path.clone(),
            lease_secs: self.config.lease_duration.as_secs(),
            broker_id: self.broker_id,
            max_fetch_response_size: self.config.max_fetch_response_size,
            producer_state_cache_ttl_secs: self.config.producer_state_cache_ttl_secs,
            zombie_state: self.zombie_state.clone(),
            fail_on_recovery_gap: self.config.fail_on_recovery_gap,
            lease_cache: self.lease_cache.clone(),
            min_lease_ttl_for_write_secs: self.config.min_lease_ttl_for_write_secs,
            batch_index_max_size: self.config.batch_index_max_size,
            slatedb_max_unflushed_bytes: self.config.slatedb_max_unflushed_bytes,
            slatedb_l0_sst_size_bytes: self.config.slatedb_l0_sst_size_bytes,
            slatedb_flush_interval_ms: self.config.slatedb_flush_interval_ms,
            max_owned_partitions_per_broker: self.config.max_owned_partitions_per_broker,
            acquire_locks: self.acquire_locks.clone(),
            topic_name_cache: self.topic_name_cache.clone(),
        }
    }

    /// Exit zombie mode safely by verifying all partition leases.
    pub async fn exit_zombie_mode_safely(&self) -> SlateDBResult<()> {
        if !self.zombie_state.is_active() {
            return Ok(());
        }

        let ctx = self.ownership_context();

        match try_exit_zombie_mode(&ctx).await {
            Ok(true) => Ok(()),
            Ok(false) => Err(SlateDBError::Storage(
                "Failed to exit zombie mode - some verifications failed".to_string(),
            )),
            Err(e) => Err(e),
        }
    }

    /// Start background tasks for ownership management.
    pub async fn start(&self) {
        let heartbeat_handle = self.start_heartbeat_loop();
        let lease_handle = self.start_lease_renewal_loop();
        let ownership_handle = self.start_ownership_loop();
        let session_handle = self.start_session_timeout_loop();

        let mut handles = self.task_handles.write().await;
        handles.push(heartbeat_handle);
        handles.push(lease_handle);
        handles.push(ownership_handle);
        handles.push(session_handle);

        // Time-based log retention (only when enabled; see ClusterConfig docs)
        if self.config.log_retention_ms > 0 {
            handles.push(self.start_retention_loop());
        }

        // Start rebalance coordinator background tasks if enabled
        if let Some(ref rebalance_coord) = self.rebalance_coordinator {
            let executor = Arc::new(CoordinatorExecutorAdapter::new(self.coordinator.clone()));
            let task_handles = rebalance_coord.start_background_tasks(executor);
            *self.rebalance_task_handles.write().await = Some(task_handles);
            info!(
                broker_id = self.broker_id,
                fast_failover = self.config.fast_failover_enabled,
                auto_balancer = self.config.auto_balancer_enabled,
                "Rebalance coordinator started"
            );
        }

        info!(broker_id = self.broker_id, "Partition manager started");
    }

    /// Start the broker heartbeat loop.
    fn start_heartbeat_loop(&self) -> JoinHandle<()> {
        let coordinator = self.coordinator.clone();
        let interval = self.config.heartbeat_interval;
        let broker_id = self.broker_id;
        let zombie_state = self.zombie_state.clone();
        let partition_states = self.partition_states.clone();
        let lease_cache = self.lease_cache.clone();
        let max_consecutive_failures = self.config.max_consecutive_heartbeat_failures;
        let batch_index_max_size = self.config.batch_index_max_size;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        self.control_runtime.spawn(async move {
            let mut consecutive_failures: u32 = 0;

            // Spread first ticks uniformly across `interval` so concurrent
            // heartbeat loops don't synchronize after rebalance.
            tokio::select! {
                _ = tokio::time::sleep(initial_jitter(interval)) => {},
                _ = shutdown_rx.recv() => {
                    info!(broker_id, "Heartbeat loop received shutdown signal");
                    return;
                }
            }

            loop {
                // Use jittered sleep instead of fixed interval to prevent thundering herd
                tokio::select! {
                    _ = tokio::time::sleep(with_jitter(interval)) => {},
                    _ = shutdown_rx.recv() => {
                        info!(broker_id, "Heartbeat loop received shutdown signal");
                        break;
                    }
                }

                // Refresh the estimated owned-partition memory gauge from the
                // live owned set. Done here (single periodic source) rather
                // than at every acquire/release site so it cannot drift.
                let owned_count = partition_states
                    .iter()
                    .filter(|e| e.value().is_owned())
                    .count();
                super::metrics::record_estimated_partition_memory(owned_count, batch_index_max_size);

                match coordinator.heartbeat().await {
                    Ok(_) => {
                        if consecutive_failures > 0 {
                            info!(
                                broker_id,
                                previous_failures = consecutive_failures,
                                "Heartbeat recovered after failures"
                            );
                            if zombie_state.is_active() {
                                info!(
                                    broker_id,
                                    "Heartbeat recovered - will verify partition leases before exiting zombie mode"
                                );
                            }
                        }
                        consecutive_failures = 0;
                    }
                    Err(e) => {
                        consecutive_failures += 1;
                        if consecutive_failures >= max_consecutive_failures {
                            error!(
                                broker_id,
                                consecutive_failures,
                                error = %e,
                                "CRITICAL: Heartbeat failed {} times - entering zombie mode",
                                consecutive_failures
                            );
                            if zombie_state.enter() {
                                error!(
                                    broker_id,
                                    "ENTERING ZOMBIE MODE - releasing all partitions"
                                );

                                // Clear entire lease cache when entering zombie mode
                                lease_cache.clear();

                                // Snapshot partitions AND their open stores in one
                                // pass so we can close them after dropping the
                                // map. Just removing entries leaves SlateDB
                                // memtables, S3 connections, and background
                                // compaction tasks alive on the dead writer.
                                let mut owned: Vec<(PartitionKey, Option<Arc<PartitionStore>>)> =
                                    Vec::new();
                                for entry in partition_states.iter() {
                                    if entry.value().is_owned() {
                                        owned.push((entry.key().clone(), entry.value().store()));
                                    }
                                }
                                for (key, _) in &owned {
                                    partition_states.remove(key);
                                }
                                for (key, store_opt) in owned {
                                    let (topic, partition) = key;
                                    warn!(
                                        topic = &*topic,
                                        partition, "Releasing partition due to zombie mode"
                                    );
                                    // Flush + close the local store BEFORE telling the
                                    // coordinator we no longer own the partition. Once
                                    // the coordinator releases ownership, the next
                                    // owner can begin writing immediately, so any
                                    // unflushed `acks=0` writes still in our memtable
                                    // would be silently lost. If close/flush fails,
                                    // skip the release so the lease lingers (until
                                    // expiry or operator intervention) rather than
                                    // handing ownership over with data still pinned
                                    // here.
                                    let close_ok = match &store_opt {
                                        Some(store) => match store.close().await {
                                            Ok(()) => true,
                                            Err(e) => {
                                                error!(
                                                    topic = &*topic,
                                                    partition,
                                                    error = %e,
                                                    "Failed to close partition store on zombie entry; skipping release"
                                                );
                                                false
                                            }
                                        },
                                        None => true,
                                    };
                                    if close_ok
                                        && let Err(e) =
                                            coordinator.release_partition(&topic, partition).await
                                    {
                                        error!(topic = &*topic, partition, error = %e, "Failed to release partition during zombie mode");
                                    }
                                }
                                super::metrics::record_coordinator_failure("zombie_mode");
                            }
                            super::metrics::record_coordinator_failure("heartbeat");
                        } else {
                            warn!(
                                broker_id,
                                consecutive_failures,
                                error = %e,
                                "Failed to send heartbeat (attempt {}/{})",
                                consecutive_failures,
                                max_consecutive_failures
                            );
                        }
                    }
                }
            }
        })
    }

    /// Start the time-based log retention loop.
    ///
    /// Periodically walks the partitions this broker owns and deletes record
    /// batches older than `log_retention_ms`, advancing each partition's log
    /// start offset. Only owned partitions are processed — the owner is the
    /// single writer, so retention deletes can't race another broker's
    /// appends. Without this loop object-store usage grows without bound
    /// (there is no other delete path for record data).
    fn start_retention_loop(&self) -> JoinHandle<()> {
        let partition_states = self.partition_states.clone();
        let retention_ms = self.config.log_retention_ms;
        let interval = Duration::from_secs(self.config.log_retention_check_interval_secs);
        let broker_id = self.broker_id;
        let zombie_state = self.zombie_state.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Producer-state retention: a generous multiple of the in-memory
        // cache TTL so an active producer that briefly idles past TTL but
        // returns within the retention horizon still has its on-disk state
        // available. Without this sweep, every distinct `producer_id` ever
        // seen leaves a permanent SlateDB key — see partition_store
        // `prune_producer_states` for the full argument.
        let producer_state_retention_ms: i64 = self
            .config
            .producer_state_cache_ttl_secs
            .saturating_mul(8)
            .saturating_mul(1000)
            .min(i64::MAX as u64) as i64;

        self.control_runtime.spawn(async move {
            info!(
                broker_id,
                retention_ms,
                check_interval_secs = interval.as_secs(),
                "Log retention loop started"
            );
            // Spread first tick uniformly to avoid synchronized retention scans.
            tokio::select! {
                _ = tokio::time::sleep(initial_jitter(interval)) => {},
                _ = shutdown_rx.recv() => {
                    info!(broker_id, "Retention loop received shutdown signal");
                    return;
                }
            }
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(with_jitter(interval)) => {},
                    _ = shutdown_rx.recv() => {
                        info!(broker_id, "Retention loop received shutdown signal");
                        break;
                    }
                }

                // Skip the pass entirely in zombie mode: ownership is in
                // doubt, so we must not delete anything.
                if zombie_state.is_active() {
                    continue;
                }

                // Snapshot owned stores first so we don't hold DashMap
                // guards across awaits.
                let stores: Vec<(PartitionKey, Arc<PartitionStore>)> = partition_states
                    .iter()
                    .filter_map(|e| {
                        if e.value().is_owned() {
                            e.value().store().map(|s| (e.key().clone(), s))
                        } else {
                            None
                        }
                    })
                    .collect();

                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as i64)
                    .unwrap_or(0);

                let mut total_deleted: u64 = 0;
                let mut total_producer_states_pruned: u64 = 0;
                for ((topic, partition), store) in stores {
                    match store.apply_retention(retention_ms, now_ms).await {
                        Ok(deleted) => total_deleted += deleted,
                        Err(e) => {
                            // Non-fatal: retention re-runs next cycle. Fenced
                            // errors mean we lost the partition mid-pass; the
                            // ownership loop will clean up.
                            warn!(
                                topic = &*topic,
                                partition,
                                error = %e,
                                "Retention pass failed for partition"
                            );
                        }
                    }

                    match store
                        .prune_producer_states(producer_state_retention_ms, now_ms)
                        .await
                    {
                        Ok(pruned) => total_producer_states_pruned += pruned,
                        Err(e) => {
                            warn!(
                                topic = &*topic,
                                partition,
                                error = %e,
                                "Producer-state prune failed for partition"
                            );
                        }
                    }
                }

                if total_deleted > 0 {
                    info!(
                        broker_id,
                        deleted_batches = total_deleted,
                        "Retention pass deleted expired batches"
                    );
                }
                if total_producer_states_pruned > 0 {
                    info!(
                        broker_id,
                        deleted_producer_states = total_producer_states_pruned,
                        "Retention pass pruned stale producer-state keys"
                    );
                }
            }
        })
    }

    /// Start the lease renewal loop.
    fn start_lease_renewal_loop(&self) -> JoinHandle<()> {
        let coordinator = self.coordinator.clone();
        let partition_states = self.partition_states.clone();
        let lease_cache = self.lease_cache.clone();
        let interval = self.config.lease_renewal_interval;
        let lease_secs = self.config.lease_duration.as_secs();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        self.control_runtime.spawn(async move {
            // Spread first lease renewal uniformly across `interval` so 1k+
            // partitions acquired in a rebalance don't all renew at once.
            tokio::select! {
                _ = tokio::time::sleep(initial_jitter(interval)) => {},
                _ = shutdown_rx.recv() => {
                    info!("Lease renewal loop received shutdown signal");
                    return;
                }
            }
            loop {
                // Use jittered sleep instead of fixed interval to prevent thundering herd
                tokio::select! {
                    _ = tokio::time::sleep(with_jitter(interval)) => {},
                    _ = shutdown_rx.recv() => {
                        info!("Lease renewal loop received shutdown signal");
                        break;
                    }
                }
                // Only renew leases for actively owned partitions (not draining)
                let partitions: Vec<_> = partition_states
                    .iter()
                    .filter_map(|entry| {
                        if entry.value().is_actively_owned() {
                            Some(entry.key().clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                if partitions.is_empty() {
                    continue;
                }

                // Issue ONE Raft proposal carrying every renewal in this
                // cycle, instead of N proposals. The default trait impl
                // falls back to per-partition calls for coordinators that
                // don't override; the Raft impl batches into a single
                // entry so the leader pays one commit / one fsync per
                // cycle.
                let renewal_results = match coordinator
                    .renew_partition_leases_batch(partitions, lease_secs)
                    .await
                {
                    Ok(r) => r,
                    Err(e) => {
                        error!(error = %e, "Batch lease renewal failed (will retry)");
                        super::metrics::record_lease_operation("renew", "error");
                        super::metrics::record_coordinator_failure("batch_lease_renewal_failure");
                        continue;
                    }
                };

                for (topic, partition, renewed) in renewal_results {
                    if renewed {
                        debug!(topic = &*topic, partition, "Renewed lease");
                        super::metrics::record_lease_operation("renew", "success");
                        // Update lease cache with new expiry time
                        let new_expiry = Instant::now() + Duration::from_secs(lease_secs);
                        lease_cache.insert((Arc::clone(&topic), partition), new_expiry);
                        // Push the new wall-clock expiry to the
                        // partition store so its in-flight produces
                        // re-validate against the renewed window
                        // rather than the older, soon-to-expire one
                        // captured at the last cache hit.
                        if let Some(state) = partition_states.get(&(Arc::clone(&topic), partition))
                            && let Some(store) = state.store()
                        {
                            store.set_lease_expiry_ms(wall_clock_expiry_ms(lease_secs));
                        }
                    } else {
                        // Lease was lost - another broker owns this partition now.
                        // Immediately update partition state to prevent writes.
                        warn!(
                            topic = &*topic,
                            partition,
                            "Lost partition lease - immediately transitioning to Releasing state"
                        );
                        // Invalidate lease cache for this partition
                        lease_cache.remove(&(Arc::clone(&topic), partition));
                        // Remove from partition_states to reject further operations,
                        // and close the underlying SlateDB handle so its memtables,
                        // S3 connections, and background tasks are released.
                        // Without this the new owner relies
                        // on SlateDB-internal fencing while we leak resources.
                        if let Some((key, prev_state)) =
                            partition_states.remove(&(Arc::clone(&topic), partition))
                        {
                            if let Some(store) = prev_state.store()
                                && let Err(e) = store.close().await
                            {
                                warn!(
                                    topic = &*key.0,
                                    partition = key.1,
                                    error = %e,
                                    "Failed to close partition store after lease loss"
                                );
                            }
                            info!(
                                topic = &*key.0,
                                partition = key.1,
                                "Partition removed from active state after lease loss"
                            );
                        }
                        super::metrics::record_lease_operation("renew", "failure");
                    }
                }
            }
        })
    }

    /// Start the ownership check loop.
    fn start_ownership_loop(&self) -> JoinHandle<()> {
        let ctx = self.ownership_context();
        let interval = self.config.ownership_check_interval;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        self.control_runtime.spawn(async move {
            // Spread first ownership-check uniformly across `interval` to
            // avoid synchronized object-store probes after rebalance.
            tokio::select! {
                _ = tokio::time::sleep(initial_jitter(interval)) => {},
                _ = shutdown_rx.recv() => {
                    info!(ctx.broker_id, "Ownership loop received shutdown signal");
                    return;
                }
            }
            loop {
                // Use jittered sleep instead of fixed interval to prevent thundering herd
                tokio::select! {
                    _ = tokio::time::sleep(with_jitter(interval)) => {},
                    _ = shutdown_rx.recv() => {
                        info!(ctx.broker_id, "Ownership loop received shutdown signal");
                        break;
                    }
                }

                // Check object store health for partial network partition detection
                // If object store is unhealthy (too many consecutive failures), log warning
                // and consider entering zombie mode to prevent serving stale data
                if !super::metrics::is_object_store_healthy() {
                    let failures = super::metrics::object_store_consecutive_failures();
                    error!(
                        ctx.broker_id,
                        consecutive_failures = failures,
                        "PARTIAL NETWORK PARTITION: Object store unhealthy. \
                         Broker can reach Raft but not object store. \
                         Consider releasing partitions to prevent serving stale data."
                    );

                    // If we're not already in zombie mode and object store is severely degraded,
                    // enter zombie mode to prevent serving requests
                    if !ctx.zombie_state.is_active() && failures >= 15 {
                        warn!(
                            ctx.broker_id,
                            "Entering zombie mode due to object store unavailability"
                        );
                        if ctx.zombie_state.enter() {
                            // Clear lease cache since we can't trust our state
                            ctx.lease_cache.clear();
                            super::metrics::record_coordinator_failure("object_store_partition");
                        }
                    }
                }

                if ctx.zombie_state.is_active() {
                    match try_exit_zombie_mode(&ctx).await {
                        Ok(true) => info!(ctx.broker_id, "Successfully exited zombie mode"),
                        Ok(false) => {
                            debug!(
                                ctx.broker_id,
                                "Still in zombie mode, skipping ownership check"
                            );
                            continue;
                        }
                        Err(e) => {
                            warn!(ctx.broker_id, error = %e, "Failed to verify leases for zombie mode exit");
                            continue;
                        }
                    }
                }
                let assigned = match ctx.coordinator.get_assigned_partitions().await {
                    Ok(a) => a,
                    Err(e) => {
                        warn!(error = %e, "Failed to get assigned partitions");
                        continue;
                    }
                };
                let assigned_set: HashSet<_> = assigned.into_iter().collect();

                // Collect partition states by category
                let mut actively_owned: HashSet<(String, i32)> = HashSet::new();
                let mut owned_topics: HashSet<String> = HashSet::new();

                for entry in ctx.partition_states.iter() {
                    let (topic_arc, partition) = entry.key();
                    let topic = topic_arc.to_string();
                    let state = entry.value();

                    if state.is_actively_owned() {
                        owned_topics.insert(topic.clone());
                        actively_owned.insert((topic, *partition));
                    }
                }

                // Check for deleted topics
                for topic in &owned_topics {
                    match ctx.coordinator.topic_exists(topic).await {
                        Ok(false) => {
                            info!(topic = %topic, "Topic deleted - releasing all partitions");
                            let topic_partitions: Vec<_> = actively_owned
                                .iter()
                                .filter(|(t, _)| t == topic)
                                .cloned()
                                .collect();
                            for (t, p) in topic_partitions {
                                release_partition_for_deleted_topic(&ctx, &t, p).await;
                            }
                        }
                        Ok(true) => {}
                        Err(e) => {
                            warn!(topic = %topic, error = %e, "Failed to check if topic exists")
                        }
                    }
                }

                // Acquire new partitions
                let to_acquire: Vec<_> = assigned_set.difference(&actively_owned).cloned().collect();
                if !to_acquire.is_empty() {
                    // Rate limit partition acquisitions to prevent thundering herd
                    //
                    // After a coordinator failure or cluster restart, all brokers try to
                    // re-acquire their partitions simultaneously, creating a storm of requests
                    // that can overwhelm the coordinator.
                    //
                    // We use chunked acquisition with jittered delays:
                    // - Process partitions in chunks of MAX_CONCURRENT_ACQUISITIONS
                    // - Add small random delay between chunks to spread load
                    //
                    // This doesn't significantly slow down normal operation (single partition
                    // acquisition), but prevents the O(N) simultaneous requests during recovery.
                    const MAX_CONCURRENT_ACQUISITIONS: usize = 5;
                    const INTER_CHUNK_DELAY_MS: u64 = 50;

                    for (chunk_idx, chunk) in
                        to_acquire.chunks(MAX_CONCURRENT_ACQUISITIONS).enumerate()
                    {
                        let acquire_futures: Vec<_> = chunk
                            .iter()
                            .map(|(topic, partition)| acquire_partition(&ctx, topic, *partition))
                            .collect();
                        futures::future::join_all(acquire_futures).await;

                        // Jittered delay between chunks to prevent thundering
                        // herd. Skip after the LAST chunk: the old code slept
                        // unconditionally as long as `to_acquire` was longer
                        // than one chunk, adding 50ms × N/5 of pure wall-clock
                        // latency on coordinator failover (a 1k-partition
                        // re-acquisition paid 10–20s of mandatory sleep).
                        let is_last_chunk = (chunk_idx + 1) * MAX_CONCURRENT_ACQUISITIONS
                            >= to_acquire.len();
                        if !is_last_chunk {
                            let jitter_ms = fastrand::u64(0..INTER_CHUNK_DELAY_MS);
                            tokio::time::sleep(Duration::from_millis(
                                INTER_CHUNK_DELAY_MS + jitter_ms,
                            ))
                            .await;
                        }
                    }
                }

                // Release actively-owned partitions that are no longer assigned.
                // The old "drain" state machine kept the store open until the
                // lease expired, but `start_draining_partition` always closed
                // SlateDB immediately to keep the compactor from racing the new
                // owner — so there was never a real drain window. We do the
                // same close-and-release here.
                let to_release: Vec<_> =
                    actively_owned.difference(&assigned_set).cloned().collect();
                if !to_release.is_empty() {
                    for (topic, partition) in to_release {
                        release_reassigned_partition(&ctx, &topic, partition).await;
                    }
                }

                // Verify ownership of partitions we should still own (in parallel)
                let to_verify: Vec<_> = actively_owned
                    .intersection(&assigned_set)
                    .cloned()
                    .collect();
                if !to_verify.is_empty() {
                    let verify_futures: Vec<_> = to_verify
                        .iter()
                        .map(|(topic, partition)| verify_ownership(&ctx, topic, *partition))
                        .collect();
                    futures::future::join_all(verify_futures).await;
                }
            }
        })
    }

    /// Start the session timeout enforcement loop.
    fn start_session_timeout_loop(&self) -> JoinHandle<()> {
        let coordinator = self.coordinator.clone();
        let interval = self.config.session_timeout_check_interval;
        let default_session_timeout_ms = self.config.default_session_timeout_ms;
        let broker_id = self.broker_id;
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        self.control_runtime.spawn(async move {
            // Spread first session-timeout sweep uniformly across `interval`.
            tokio::select! {
                _ = tokio::time::sleep(initial_jitter(interval)) => {},
                _ = shutdown_rx.recv() => {
                    info!(broker_id, "Session timeout loop received shutdown signal");
                    return;
                }
            }
            loop {
                // Use jittered sleep instead of fixed interval to prevent thundering herd
                tokio::select! {
                    _ = tokio::time::sleep(with_jitter(interval)) => {},
                    _ = shutdown_rx.recv() => {
                        info!(broker_id, "Session timeout loop received shutdown signal");
                        break;
                    }
                }
                match coordinator
                    .evict_expired_members(default_session_timeout_ms)
                    .await
                {
                    Ok(evicted) if !evicted.is_empty() => {
                        info!(
                            broker_id,
                            evicted_count = evicted.len(),
                            "Evicted expired consumer group members"
                        );
                    }
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "Failed to check for expired members"),
                }
            }
        })
    }

    /// Get a partition store if we own it (internal use only).
    async fn get_store(&self, topic: &str, partition: i32) -> Option<Arc<PartitionStore>> {
        let key = self.partition_key(topic, partition);
        self.partition_states.get(&key).and_then(|e| e.store())
    }

    /// Check if we own a partition.
    pub async fn owns_partition(&self, topic: &str, partition: i32) -> bool {
        let key = self.partition_key(topic, partition);
        self.partition_states
            .get(&key)
            .map(|e| e.is_owned())
            .unwrap_or(false)
    }

    /// Try to acquire and open a partition.
    pub async fn acquire_partition(&self, topic: &str, partition: i32) -> SlateDBResult<bool> {
        use tracing::Instrument;
        if self.owns_partition(topic, partition).await {
            return Ok(true);
        }
        let span = crate::cluster::observability::partition_ownership_span(
            topic,
            partition,
            self.broker_id,
            crate::cluster::observability::OwnershipOperation::Acquire,
        );
        let ctx = self.ownership_context();
        let result = acquire_partition_core(&ctx, topic, partition)
            .instrument(span.clone())
            .await;
        match &result {
            Ok(_) => {
                span.record("otel.status_code", "OK");
            }
            Err(_) => {
                span.record("otel.status_code", "ERROR");
            }
        }
        result
    }

    /// Release a partition.
    pub async fn release_partition(&self, topic: &str, partition: i32) -> SlateDBResult<()> {
        use tracing::Instrument;
        let span = crate::cluster::observability::partition_ownership_span(
            topic,
            partition,
            self.broker_id,
            crate::cluster::observability::OwnershipOperation::Release,
        );
        self.release_partition_inner(topic, partition)
            .instrument(span)
            .await
    }

    async fn release_partition_inner(&self, topic: &str, partition: i32) -> SlateDBResult<()> {
        let key = self.partition_key(topic, partition);

        // Invalidate lease cache for this partition
        self.lease_cache.remove(&key);

        // Race-guard against a concurrent caller releasing the same partition:
        // `DashMap::remove` returns `None` for a missing key, so a second
        // racer that lost the remove sees `None` and skips the Raft command
        // entirely. Without this check, both racers would propose a release
        // that the coordinator must dedupe.
        let Some((_, state)) = self.partition_states.remove(&key) else {
            return Ok(());
        };

        let mut close_ok = true;
        if let Some(store) = state.store() {
            store.clear_load_metrics();
            // Close the store (close() takes &self so this always works)
            if let Err(e) = store.close().await {
                error!(topic, partition, error = %e, "Failed to close partition store");
                close_ok = false;
            } else {
                debug!(topic, partition, "Partition store closed successfully");
            }
        }
        // If close() failed, any acks=0 data still in SlateDB's flush window
        // is at risk. Releasing the lease now hands ownership to a new broker
        // that would race the unflushed write — accepted-but-unflushed bytes
        // would be lost. Hold the lease and let it expire naturally so the
        // new owner only takes over after our flush window has elapsed.
        if !close_ok {
            super::metrics::record_lease_operation("release", "skipped_close_failure");
            return Err(SlateDBError::Config(format!(
                "skipped lease release for {}/{} after close() failure; lease will expire \
                 naturally to avoid racing unflushed writes",
                topic, partition
            )));
        }
        match self.coordinator.release_partition(topic, partition).await {
            Ok(_) => {
                super::metrics::record_lease_operation("release", "success");
                super::metrics::OWNED_PARTITIONS
                    .with_label_values(&[topic])
                    .dec();
                info!(topic, partition, "Released partition");
                Ok(())
            }
            Err(e) => {
                super::metrics::record_lease_operation("release", "error");
                Err(e)
            }
        }
    }

    /// Ensure a topic/partition exists, creating and acquiring if necessary.
    pub async fn ensure_partition(
        &self,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<Arc<PartitionStore>> {
        // Reject negative partition indices at the boundary. Without this a
        // bug or malicious caller can create a SlateDB instance under
        // `topic-X/partition--1/`, which downstream code never garbage
        // collects and fencing assumes does not exist.
        if partition < 0 {
            return Err(SlateDBError::Config(format!(
                "Invalid partition index {} for topic {}: must be non-negative",
                partition, topic
            )));
        }
        if self.is_zombie() {
            return Err(SlateDBError::Config(
                "Broker in zombie mode - cannot acquire partitions".to_string(),
            ));
        }
        if let Some(store) = self.get_store(topic, partition).await {
            return Ok(store);
        }

        // Check partition limit before auto-creating
        let max_partitions = self.config.max_partitions_per_topic;
        if max_partitions > 0 && partition >= max_partitions {
            warn!(
                topic,
                partition, max_partitions, "Partition index exceeds max_partitions_per_topic limit"
            );
            return Err(SlateDBError::Config(format!(
                "Partition {} exceeds max_partitions_per_topic limit of {}",
                partition, max_partitions
            )));
        }

        let current_partitions = self
            .coordinator
            .get_partition_count(topic)
            .await?
            .unwrap_or(0);

        if current_partitions == 0 {
            // Topic is new. Create with default number of partitions.
            // Check topic count limit before creating new topic.
            let max_topics = self.config.max_auto_created_topics;
            if max_topics > 0 {
                let existing_topics = self.coordinator.get_topics().await?.len() as u32;
                if existing_topics >= max_topics {
                    warn!(
                        topic,
                        existing_topics,
                        max_topics,
                        "Cannot auto-create topic: max_auto_created_topics limit reached"
                    );
                    return Err(SlateDBError::Config(format!(
                        "Cannot auto-create topic: max_auto_created_topics limit of {} reached ({} existing)",
                        max_topics, existing_topics
                    )));
                }
            }

            self.coordinator
                .register_topic(topic, self.config.default_num_partitions)
                .await?;

            // After creating the topic, we need to check if the requested partition is valid.
            if partition >= self.config.default_num_partitions {
                warn!(
                    topic,
                    partition,
                    default_partitions = self.config.default_num_partitions,
                    "Requested partition index exceeds default partition count for new topic"
                );
                return Err(SlateDBError::InvalidPartition {
                    topic: topic.to_string(),
                    partition,
                });
            }
        } else if partition >= current_partitions {
            // Topic exists, expand it to include the requested partition.
            self.coordinator
                .register_topic(topic, partition + 1)
                .await?;
        }
        if self.acquire_partition(topic, partition).await? {
            self.get_store(topic, partition)
                .await
                .ok_or_else(|| SlateDBError::PartitionNotFound {
                    topic: topic.to_string(),
                    partition,
                })
        } else {
            Err(SlateDBError::NotOwned {
                topic: topic.to_string(),
                partition,
            })
        }
    }

    /// Get a partition for reading (no ownership verification).
    ///
    /// Use this for fetch/read operations where eventual consistency is acceptable.
    /// The partition must already be owned by this broker.
    ///
    /// # Errors
    /// Returns `NotOwned` if the partition is not currently owned.
    pub async fn get_for_read(
        &self,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<Arc<PartitionStore>> {
        // Reject reads in zombie mode for the same reason produce does:
        // we've lost cluster coordination, so any data we serve may already
        // belong to another owner who has advanced the log. Without this
        // check fetch would return stale data while produce was already
        // rejecting.
        if self.is_zombie() {
            return Err(SlateDBError::NotOwned {
                topic: topic.to_string(),
                partition,
            });
        }
        self.get_store(topic, partition)
            .await
            .ok_or_else(|| SlateDBError::NotOwned {
                topic: topic.to_string(),
                partition,
            })
    }

    /// Get a partition store for writing with fresh ownership verification.
    ///
    /// Use this for produce/write operations where strong consistency is required.
    /// This verifies ownership with coordinator, extends the lease, and validates that
    /// the remaining TTL is sufficient to safely complete the write operation.
    ///
    /// # Performance Optimization: Lease Caching
    ///
    /// To avoid expensive Raft calls on every produce, this method uses a lease cache.
    /// If the cached lease has sufficient remaining TTL (>= LEASE_CACHE_REFRESH_THRESHOLD_SECS),
    /// we skip the Raft verification and use the cached value. This removes Raft overhead
    /// from ~67% of produce requests while maintaining safety.
    ///
    /// # TOCTOU Prevention
    ///
    /// This method prevents the time-of-check to time-of-use (TOCTOU) race condition
    /// between lease verification and write completion by:
    ///
    /// 1. Checking cached lease (fast path) or verifying with coordinator (slow path)
    /// 2. Validating that remaining TTL >= MIN_LEASE_TTL_FOR_WRITE_SECS (15s)
    /// 3. Only then returning the store for writing
    ///
    /// If the remaining lease time is too short, the write is rejected proactively
    /// with `LeaseTooShort` error, preventing scenarios where the lease could expire
    /// during the write operation.
    ///
    /// # Errors
    /// - Returns `NotOwned` if in zombie mode
    /// - Returns `NotOwned` if the partition is not owned
    /// - Returns `NotOwned` if ownership verification fails (another broker took it)
    /// - Returns `LeaseTooShort` if remaining lease TTL is below safety threshold
    pub async fn get_for_write(
        &self,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<Arc<PartitionStore>> {
        use crate::constants::LEASE_CACHE_REFRESH_THRESHOLD_SECS;

        if self.is_zombie() {
            return Err(SlateDBError::NotOwned {
                topic: topic.to_string(),
                partition,
            });
        }

        // No additional draining-state gate is needed; partitions being
        // released are removed from `partition_states` synchronously by
        // `release_reassigned_partition`, so any subsequent get_for_write()
        // simply falls through to `get_store` which checks ownership.

        let store =
            self.get_store(topic, partition)
                .await
                .ok_or_else(|| SlateDBError::NotOwned {
                    topic: topic.to_string(),
                    partition,
                })?;

        // Fast path: Check lease cache first to avoid Raft overhead
        let cache_key = self.partition_key(topic, partition);
        if let Some(cached_expiry) = self.lease_cache.get(&cache_key) {
            let now = Instant::now();
            if *cached_expiry > now {
                let remaining_secs = cached_expiry.duration_since(now).as_secs();
                // If sufficient TTL remaining, use cached lease
                if remaining_secs >= LEASE_CACHE_REFRESH_THRESHOLD_SECS {
                    // Validate lease is sufficient for write (MIN_LEASE_TTL_FOR_WRITE_SECS = 15s)
                    store.validate_lease_for_write(remaining_secs)?;
                    // Publish the wall-clock expiry to the store so the
                    // partition's `append_batch_inner` can re-validate
                    // against decay between admission and write. We use
                    // wall clock here (not the cached Instant) because the
                    // store reads `SystemTime::now`; on a single host the
                    // two clocks track within microseconds, well below the
                    // 15s safety floor.
                    let expiry_ms = wall_clock_expiry_ms(remaining_secs);
                    store.set_lease_expiry_ms(expiry_ms);
                    super::metrics::record_lease_cache("hit");
                    return Ok(store);
                }
            }
        }

        // Slow path: Verify ownership and extend lease via Raft
        super::metrics::record_lease_cache("miss");
        let lease_secs = self.config.lease_duration.as_secs();
        let remaining_ttl = match self
            .coordinator
            .verify_and_extend_lease(topic, partition, lease_secs)
            .await
        {
            Ok(ttl) => ttl,
            Err(e) => {
                // Distinguish a definitive ownership-loss signal (NotOwned —
                // the Raft state machine says we are no longer the owner) from
                // a transient Raft hiccup (timeout, leader hand-off, network
                // blip). Closing the store on every error tears down the
                // SlateDB instance and forces a full recovery scan on the next
                // write — for a partition with millions of records that's a
                // multi-second pause, and during a leader hand-off it cascades
                // across every partition. Only explicit NotOwned ejects.
                let is_ownership_loss = matches!(&e, SlateDBError::NotOwned { .. });
                if !is_ownership_loss {
                    warn!(topic, partition, error = %e, "Transient Raft error during lease verification - propagating without ejecting partition");
                    return Err(e);
                }
                warn!(topic, partition, error = %e, "Ownership verification failed during write - releasing partition");
                // Close the store too: removing the entry without close()
                // leaks SlateDB memtables, object-store connections, and
                // background tasks every time ownership is lost mid-write.
                if let Some(store) = self
                    .partition_states
                    .remove(&cache_key)
                    .and_then(|(_, s)| s.store())
                {
                    store.clear_load_metrics();
                    if let Err(close_err) = store.close().await {
                        warn!(topic, partition, error = %close_err, "Failed to close partition store after ownership loss");
                    }
                }
                self.lease_cache.remove(&cache_key);
                return Err(SlateDBError::NotOwned {
                    topic: topic.to_string(),
                    partition,
                });
            }
        };

        // Update lease cache with new expiry
        let new_expiry = Instant::now() + Duration::from_secs(remaining_ttl);
        self.lease_cache.insert(cache_key, new_expiry);

        // CRITICAL: Validate that remaining TTL is sufficient for a safe write
        // This prevents TOCTOU races where lease expires during write
        store.validate_lease_for_write(remaining_ttl)?;

        // Publish the absolute wall-clock expiry to the store so its
        // append-time re-check has something to compare against on the
        // next produce.
        let expiry_ms = wall_clock_expiry_ms(remaining_ttl);
        store.set_lease_expiry_ms(expiry_ms);

        Ok(store)
    }

    /// Get all owned partitions.
    pub async fn list_owned(&self) -> Vec<(String, i32)> {
        self.partition_states
            .iter()
            .filter_map(|e| {
                if e.value().is_owned() {
                    let (topic, partition) = e.key();
                    Some((topic.to_string(), *partition))
                } else {
                    None
                }
            })
            .collect()
    }

    /// Delete all data for a partition from object storage.
    pub async fn delete_partition_data(&self, topic: &str, partition: i32) -> SlateDBResult<()> {
        use futures::{StreamExt, TryStreamExt};
        use object_store::path::Path as ObjectPath;
        if self.owns_partition(topic, partition).await {
            self.release_partition(topic, partition).await?;
        }
        // The object_store is already configured with `base_path` as its
        // prefix (see PartitionStore::open / ObjectStoreBuilder), so we list
        // and delete using the same relative prefix that the store opens at.
        // Prepending `base_path` here would double the prefix and silently
        // delete nothing — leaving the SST/WAL objects in S3 forever.
        let path_prefix = format!("topic-{}/partition-{}", topic, partition);
        let prefix = ObjectPath::from(path_prefix.as_str());
        info!(topic, partition, path = %path_prefix, base_path = %self.base_path, "Deleting partition data from object store");
        // Hand the listing stream straight to `delete_stream`. On S3 / GCS
        // this issues batched DeleteObjects calls; serial per-object delete
        // turned 10k SSTs × ~30 ms each into a 5-minute topic delete.
        let locations = self
            .object_store
            .list(Some(&prefix))
            .map_ok(|m| m.location)
            .map_err(|e| object_store::Error::Generic {
                store: "list",
                source: Box::new(e),
            })
            .boxed();
        let mut stream = self.object_store.delete_stream(locations);
        let mut object_count: usize = 0;
        let mut failures: usize = 0;
        while let Some(item) = stream.next().await {
            match item {
                Ok(_) => object_count += 1,
                Err(e) => {
                    failures += 1;
                    warn!(topic, partition, error = %e, "Failed to delete object");
                }
            }
        }
        info!(
            topic,
            partition,
            object_count,
            failed = failures,
            "Deleted partition data"
        );
        Ok(())
    }

    /// Shutdown the partition manager, releasing all partitions.
    pub async fn shutdown(&self) -> SlateDBResult<()> {
        use std::time::Duration;
        use tokio::time::timeout;
        info!(
            broker_id = self.broker_id,
            "Shutting down partition manager"
        );

        // Clear lease cache immediately on shutdown to prevent stale entries
        // from being used if the PartitionManager instance is somehow reused or if
        // shutdown is interrupted. This provides defense-in-depth.
        self.lease_cache.clear();

        // Stop rebalance coordinator if enabled
        if let Some(ref rebalance_coord) = self.rebalance_coordinator {
            rebalance_coord.stop();
            if let Some(handles) = self.rebalance_task_handles.write().await.take() {
                handles.abort_all();
                debug!(broker_id = self.broker_id, "Stopped rebalance coordinator");
            }
        }

        let _ = self.shutdown_tx.send(());
        debug!(
            broker_id = self.broker_id,
            "Sent shutdown signal to background tasks"
        );
        let handles: Vec<_> = { self.task_handles.write().await.drain(..).collect() };
        for (idx, handle) in handles.into_iter().enumerate() {
            match timeout(Duration::from_secs(5), handle).await {
                Ok(Ok(())) => debug!(task_index = idx, "Background task completed gracefully"),
                Ok(Err(e)) => {
                    if e.is_cancelled() {
                        debug!(task_index = idx, "Background task was cancelled");
                    } else {
                        warn!(task_index = idx, error = %e, "Background task completed with error");
                    }
                }
                Err(_) => warn!(
                    task_index = idx,
                    "Background task did not complete within timeout"
                ),
            }
        }
        let partitions: Vec<_> = self.list_owned().await;
        if !partitions.is_empty() {
            info!(
                broker_id = self.broker_id,
                partition_count = partitions.len(),
                "Releasing partitions during shutdown"
            );

            // Durability barrier: flush every owned store BEFORE the
            // release/unregister dance. The per-partition release path also
            // calls close() (which flushes), but it's gated by a 5s timeout
            // and the resulting error is discarded. Doing an explicit
            // parallel flush first guarantees that any write the broker
            // acked has hit the durable WAL even if a subsequent release
            // step fails or times out — closing the documented gap between
            // `await_durable: false` produce writes and SlateDB's periodic
            // (~100ms) flush window.
            let flush_keys: Vec<PartitionKey> = partitions
                .iter()
                .map(|(topic, partition)| (Arc::from(topic.as_str()), *partition))
                .collect();
            let flush_futures = flush_keys.into_iter().filter_map(|key| {
                let store = self
                    .partition_states
                    .get(&key)
                    .and_then(|e| e.value().store())?;
                let topic = key.0.clone();
                let partition = key.1;
                Some(async move {
                    match timeout(Duration::from_secs(5), store.flush()).await {
                        Ok(Ok(())) => {
                            debug!(topic = %topic, partition, "Flushed partition before release")
                        }
                        Ok(Err(e)) => warn!(
                            topic = %topic,
                            partition,
                            error = %e,
                            "Pre-release flush failed; data after the last SlateDB periodic flush may be lost on hard kill"
                        ),
                        Err(_) => warn!(
                            topic = %topic,
                            partition,
                            "Pre-release flush timed out"
                        ),
                    }
                })
            });
            futures::future::join_all(flush_futures).await;

            let release_futures: Vec<_> = partitions.into_iter().map(|(topic, partition)| {
                let coordinator = self.coordinator.clone();
                let partition_states = self.partition_states.clone();
                async move {
                    let key: PartitionKey = (Arc::from(topic.as_str()), partition);
                    match timeout(Duration::from_secs(5), async {
                        if let Some(store) = partition_states.remove(&key).and_then(|(_, s)| s.store()) {
                            // close() takes &self so this always works
                            let _ = store.close().await;
                        }
                        coordinator.release_partition(&topic, partition).await
                    }).await {
                        Ok(Ok(_)) => debug!(topic = %topic, partition, "Released partition during shutdown"),
                        Ok(Err(e)) => warn!(topic = %topic, partition, error = %e, "Error releasing partition during shutdown"),
                        Err(_) => warn!(topic = %topic, partition, "Timeout releasing partition during shutdown"),
                    }
                }
            }).collect();
            futures::future::join_all(release_futures).await;
        }
        match timeout(Duration::from_secs(5), self.coordinator.unregister_broker()).await {
            Ok(Ok(_)) => info!(broker_id = self.broker_id, "Unregistered broker"),
            Ok(Err(e)) => {
                warn!(broker_id = self.broker_id, error = %e, "Error unregistering broker")
            }
            Err(_) => warn!(broker_id = self.broker_id, "Timeout unregistering broker"),
        }
        info!(
            broker_id = self.broker_id,
            "Partition manager shutdown complete"
        );
        Ok(())
    }
}

struct OwnershipContext<C: ClusterCoordinator> {
    coordinator: Arc<C>,
    partition_states: PartitionStateMap,
    object_store: Arc<dyn ObjectStore>,
    base_path: String,
    lease_secs: u64,
    broker_id: i32,
    max_fetch_response_size: usize,
    producer_state_cache_ttl_secs: u64,
    zombie_state: Arc<ZombieModeState>,
    fail_on_recovery_gap: bool,
    lease_cache: Arc<DashMap<PartitionKey, Instant>>,
    min_lease_ttl_for_write_secs: u64,
    // SlateDB / index tuning. These were previously configured on
    // `ClusterConfig` but never reached the stores — operators believed
    // they had set memory limits that were silently ignored.
    batch_index_max_size: usize,
    slatedb_max_unflushed_bytes: usize,
    slatedb_l0_sst_size_bytes: usize,
    slatedb_flush_interval_ms: u64,
    /// Maximum partitions this broker may own at once. `0` means unbounded.
    /// Enforced in `acquire_partition_core` before opening a SlateDB instance.
    max_owned_partitions_per_broker: usize,
    /// Per-key serialization for `acquire_partition_core`.
    acquire_locks: Arc<DashMap<PartitionKey, Arc<tokio::sync::Mutex<()>>>>,
    /// Topic-name interning cache shared with the parent `PartitionManager`.
    /// Looked up via `partition_key` so a hot topic resolves to a refcount
    /// bump on the existing `Arc<str>` instead of a fresh heap allocation
    /// per produce/fetch.
    topic_name_cache: Arc<DashMap<String, Arc<str>>>,
}

impl<C: ClusterCoordinator> OwnershipContext<C> {
    fn partition_key(&self, topic: &str, partition: i32) -> PartitionKey {
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
fn apply_store_tuning<C: ClusterCoordinator>(
    builder: crate::cluster::partition_store::PartitionStoreBuilder,
    ctx: &OwnershipContext<C>,
) -> crate::cluster::partition_store::PartitionStoreBuilder {
    builder
        .max_fetch_response_size(ctx.max_fetch_response_size)
        .producer_state_cache_ttl_secs(ctx.producer_state_cache_ttl_secs)
        .fail_on_recovery_gap(ctx.fail_on_recovery_gap)
        .min_lease_ttl_for_write_secs(ctx.min_lease_ttl_for_write_secs)
        .batch_index_max_size(ctx.batch_index_max_size)
        .slatedb_max_unflushed_bytes(ctx.slatedb_max_unflushed_bytes)
        .slatedb_l0_sst_size_bytes(ctx.slatedb_l0_sst_size_bytes)
        .slatedb_flush_interval_ms(ctx.slatedb_flush_interval_ms)
}

/// Helper macro to check if zombie mode was re-entered during verification.
/// Returns early with Ok(false) if re-entry detected.
macro_rules! check_zombie_reentry {
    ($ctx:expr, $entered_at_start:expr, $operation:expr) => {
        if $ctx.zombie_state.entered_at() != $entered_at_start {
            warn!(
                "Zombie mode re-entered during verification (heartbeat failed again) - aborting {} and staying in zombie mode (broker_id={}, operation={})",
                $operation,
                $ctx.broker_id,
                $operation
            );
            return Ok(false);
        }
    };
}

async fn try_exit_zombie_mode<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
) -> SlateDBResult<bool> {
    if !ctx.zombie_state.is_active() {
        return Ok(true);
    }

    // CRITICAL: Capture the zombie mode entry timestamp at the start of verification.
    // If the heartbeat loop enters zombie mode again during our verification,
    // the timestamp will change and we MUST NOT exit zombie mode.
    //
    // We now check this timestamp after EACH async operation, not just at the end.
    // This prevents the race condition where:
    //   1. We start verification (zombie_mode = true, entered_at = T1)
    //   2. We verify partition A (OK)
    //   3. Heartbeat fails again, re-enters zombie mode (entered_at = T2)
    //   4. We continue verifying partitions B, C, D with STALE assumptions
    //   5. We try to exit - BAD: partition A's verification is now invalid
    let entered_at_start = ctx.zombie_state.entered_at();

    info!(
        ctx.broker_id,
        "Attempting to exit zombie mode - verifying partition leases and SlateDB fencing"
    );

    let owned_partitions: Vec<_> = ctx
        .partition_states
        .iter()
        .filter_map(|e| {
            if e.value().is_owned() {
                Some(e.key().clone())
            } else {
                None
            }
        })
        .collect();

    let mut lost_partitions = Vec::new();
    let mut need_reopen = Vec::new();

    for (topic, partition) in &owned_partitions {
        // Check for re-entry before each partition verification
        check_zombie_reentry!(ctx, entered_at_start, "partition verification");

        // Step 1: Verify coordinator ownership
        match ctx
            .coordinator
            .owns_partition_for_read(topic, *partition)
            .await
        {
            Ok(true) => {
                debug!(
                    topic = &**topic,
                    partition, "Verified partition ownership in coordinator"
                );

                // Check for re-entry after coordinator call
                check_zombie_reentry!(ctx, entered_at_start, "ownership verification");

                // Step 2: Verify SlateDB handle is not fenced
                // During zombie mode, another broker may have acquired the partition,
                // opened SlateDB (getting a new fencing token), and fenced our handle.
                // We need to verify our SlateDB handle is still valid.
                if let Some(state) = ctx.partition_states.get(&(Arc::clone(topic), *partition))
                    && let Some(store) = state.store()
                {
                    // Try to read HWM to verify SlateDB access
                    // This will fail if we've been fenced
                    match store.high_watermark_check().await {
                        Ok(_) => {
                            debug!(
                                topic = &**topic,
                                partition, "Verified SlateDB handle is not fenced"
                            );
                            // Check for re-entry after SlateDB check
                            check_zombie_reentry!(ctx, entered_at_start, "SlateDB verification");
                        }
                        Err(e) => {
                            if e.is_fenced() {
                                warn!(
                                    topic = &**topic,
                                    partition,
                                    "SlateDB handle fenced - will close and re-acquire partition"
                                );
                                // Mark for re-opening with fresh SlateDB handle
                                need_reopen.push((Arc::clone(topic), *partition));
                            } else {
                                error!(
                                    topic = &**topic, partition, error = %e,
                                    "Failed to verify SlateDB handle - staying in zombie mode"
                                );
                                return Err(e);
                            }
                        }
                    }
                }
            }
            Ok(false) => {
                warn!(
                    topic = &**topic,
                    partition, "Lost partition ownership during zombie mode"
                );
                lost_partitions.push((Arc::clone(topic), *partition));
            }
            Err(e) => {
                error!(topic = &**topic, partition, error = %e, "Failed to verify partition ownership - staying in zombie mode");
                return Err(e);
            }
        }
    }

    // Check for re-entry before cleanup phase
    check_zombie_reentry!(ctx, entered_at_start, "cleanup phase");

    // Close and remove lost partitions
    for (topic, partition) in lost_partitions {
        if let Some(store) = ctx
            .partition_states
            .remove(&(Arc::clone(&topic), partition))
            .and_then(|(_, s)| s.store())
        {
            // close() takes &self so this always works
            let _ = store.close().await;
        }
        super::metrics::OWNED_PARTITIONS
            .with_label_values(&[&topic])
            .dec();
    }

    // Check for re-entry before re-opening phase
    check_zombie_reentry!(ctx, entered_at_start, "re-open phase");

    // Re-open partitions that have fenced SlateDB handles
    for (topic, partition) in need_reopen {
        // Check for re-entry before each re-open operation
        check_zombie_reentry!(ctx, entered_at_start, "partition re-open");

        info!(
            topic = &*topic,
            partition, "Re-opening partition with fresh SlateDB handle after zombie mode"
        );

        // Close existing store
        if let Some(store) = ctx
            .partition_states
            .remove(&(Arc::clone(&topic), partition))
            .and_then(|(_, s)| s.store())
        {
            // close() takes &self so this always works
            let _ = store.close().await;
        }

        // Check for re-entry after close
        check_zombie_reentry!(ctx, entered_at_start, "store close");

        // CRITICAL: Verify we still own the partition AND extend the lease before re-opening SlateDB.
        // This addresses the race condition where:
        // 1. We entered zombie mode
        // 2. Our lease expired during zombie mode
        // 3. Another broker acquired the partition
        // 4. We try to re-open, thinking we still own it
        //
        // Using verify_and_extend_lease ensures:
        // - We still own the partition (fresh check, no cache)
        // - The lease is extended to ensure it won't expire during SlateDB open
        match ctx
            .coordinator
            .verify_and_extend_lease(&topic, partition, ctx.lease_secs)
            .await
        {
            Ok(remaining_ttl) => {
                debug!(
                    topic = &*topic,
                    partition,
                    remaining_ttl,
                    "Verified lease ownership and extended before re-opening SlateDB"
                );
                // Check for re-entry after lease extension
                check_zombie_reentry!(ctx, entered_at_start, "lease extension");
            }
            Err(e) => {
                warn!(
                    topic = &*topic, partition, error = %e,
                    "Lost partition ownership during zombie mode - cannot re-open"
                );
                super::metrics::OWNED_PARTITIONS
                    .with_label_values(&[&topic])
                    .dec();
                continue;
            }
        }

        // Re-acquire to bump the leader epoch. The fresh epoch both fences
        // any stale handle (including our own pre-zombie one) and is REQUIRED
        // for the reopen below: opening without `.leader_epoch(...)` would
        // disable per-write epoch validation entirely, leaving SlateDB
        // single-writer fencing as the only guard on a broker that was just
        // suspected dead — exactly when epoch fencing matters most.
        let reacquired_epoch = match ctx
            .coordinator
            .acquire_partition_with_epoch(&topic, partition, ctx.lease_secs)
            .await
        {
            Ok(Some(epoch)) => {
                check_zombie_reentry!(ctx, entered_at_start, "epoch re-acquisition");
                epoch
            }
            Ok(None) => {
                warn!(
                    topic = &*topic,
                    partition,
                    "Lost partition to another broker during zombie recovery - cannot re-open"
                );
                super::metrics::OWNED_PARTITIONS
                    .with_label_values(&[&topic])
                    .dec();
                continue;
            }
            Err(e) => {
                warn!(
                    topic = &*topic, partition, error = %e,
                    "Failed to re-acquire epoch during zombie recovery - cannot re-open"
                );
                super::metrics::OWNED_PARTITIONS
                    .with_label_values(&[&topic])
                    .dec();
                continue;
            }
        };

        // Open fresh SlateDB instance - lease is verified and extended,
        // epoch validation armed with the freshly bumped epoch.
        match apply_store_tuning(PartitionStore::builder(), ctx)
            .object_store(ctx.object_store.clone())
            .base_path(&ctx.base_path)
            .topic(&topic)
            .partition(partition)
            .zombie_mode(ctx.zombie_state.clone())
            .leader_epoch(reacquired_epoch)
            .build()
            .await
        {
            Ok(store) => {
                // Check for re-entry after SlateDB open
                if ctx.zombie_state.entered_at() != entered_at_start {
                    warn!(
                        ctx.broker_id,
                        topic = &*topic,
                        partition,
                        "Zombie mode re-entered after opening SlateDB - closing store and aborting"
                    );
                    // Close the store we just opened since our verification is now invalid
                    let _ = store.close().await;
                    return Ok(false);
                }

                ctx.partition_states.insert(
                    (Arc::clone(&topic), partition),
                    PartitionState::acquired(Arc::new(store)),
                );
                info!(
                    topic = &*topic,
                    partition, "Successfully re-opened partition with fresh SlateDB handle"
                );
            }
            Err(e) => {
                error!(topic = &*topic, partition, error = %e, "Failed to re-open partition - releasing ownership");
                let _ = ctx.coordinator.release_partition(&topic, partition).await;
                super::metrics::OWNED_PARTITIONS
                    .with_label_values(&[&topic])
                    .dec();
            }
        }
    }

    // CRITICAL: Use try_exit which atomically checks:
    // 1. We're still in zombie mode (someone else might have exited it)
    // 2. The zombie mode entry timestamp hasn't changed (heartbeat didn't fail again)
    //
    // This prevents the race where heartbeat fails again during our verification,
    // causing us to exit zombie mode while the broker is actually unhealthy.
    if ctx.zombie_state.try_exit(entered_at_start, "recovered") {
        info!(
            ctx.broker_id,
            "EXITED ZOMBIE MODE - all partition handles verified"
        );
        Ok(true)
    } else {
        // Either zombie mode was re-entered or another thread already exited
        if !ctx.zombie_state.is_active() {
            info!(ctx.broker_id, "Another thread already exited zombie mode");
            Ok(true)
        } else {
            warn!(
                ctx.broker_id,
                "Zombie mode was re-entered during verification (heartbeat failed again) - not exiting"
            );
            Ok(false)
        }
    }
}

async fn acquire_partition_core<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) -> SlateDBResult<bool> {
    // A broker that has lost coordination must not acquire fresh
    // partitions: opening a SlateDB instance and bumping the leader
    // epoch from a stale node is the split-brain scenario zombie mode
    // exists to prevent. Reject before taking any locks.
    if ctx.zombie_state.is_active() {
        return Ok(false);
    }

    let key = ctx.partition_key(topic, partition);

    // Per-key serialization. Two concurrent acquires for the same partition
    // would otherwise both pass the existence check, build separate
    // `PartitionStore` instances, and let the second insert silently replace
    // the first — leaking the orphaned SlateDB handle (and any in-flight
    // writes still holding it). The mutex is keyed by partition, so different
    // partitions still acquire concurrently.
    let lock = ctx
        .acquire_locks
        .entry(key.clone())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _guard = lock.lock().await;

    // Recheck under the lock: the winning concurrent caller may have already
    // acquired and inserted while we were queued.
    if let Some(state) = ctx.partition_states.get(&key)
        && state.is_owned()
    {
        return Ok(true);
    }

    // Enforce the per-broker owned-partition cap before opening another
    // SlateDB instance. Each owned partition holds a live LSM engine
    // (memtable, WAL, block cache, background tasks), so an unbounded owned
    // set is the most likely OOM vector at high partition density (audit
    // P1-2). Rejecting here turns that into a bounded, observable limit: the
    // partition simply isn't acquired and stays available to another broker.
    // `0` disables the cap. This check is correct rather than racy because we
    // hold the per-key acquire lock and count the live owned set directly;
    // different partitions may still race, so the effective ceiling can
    // briefly exceed the cap by the number of concurrent acquires, which is
    // bounded and self-correcting.
    if ctx.max_owned_partitions_per_broker > 0 {
        let owned = ctx
            .partition_states
            .iter()
            .filter(|e| e.value().is_owned())
            .count();
        if owned >= ctx.max_owned_partitions_per_broker {
            warn!(
                ctx.broker_id,
                topic,
                partition,
                owned,
                max_owned = ctx.max_owned_partitions_per_broker,
                "Rejecting partition acquisition: broker is at its max_owned_partitions_per_broker cap"
            );
            super::metrics::record_partition_acquire_rejected("max_owned");
            super::metrics::record_lease_operation("acquire", "rejected_max_owned");
            return Ok(false);
        }
    }

    // Use acquire_partition_with_epoch to get the leader epoch for TOCTOU prevention
    let leader_epoch = match ctx
        .coordinator
        .acquire_partition_with_epoch(topic, partition, ctx.lease_secs)
        .await?
    {
        Some(epoch) => epoch,
        None => {
            super::metrics::record_lease_operation("acquire", "failure");
            return Ok(false);
        }
    };

    let build_result = apply_store_tuning(PartitionStore::builder(), ctx)
        .object_store(ctx.object_store.clone())
        .base_path(&ctx.base_path)
        .topic(topic)
        .partition(partition)
        .zombie_mode(ctx.zombie_state.clone())
        .leader_epoch(leader_epoch) // Pass epoch for TOCTOU prevention
        .build()
        .await;

    match build_result {
        Ok(store) => {
            ctx.partition_states
                .insert(key, PartitionState::acquired(Arc::new(store)));
            super::metrics::record_lease_operation("acquire", "success");
            super::metrics::OWNED_PARTITIONS
                .with_label_values(&[topic])
                .inc();
            info!(
                ctx.broker_id,
                topic, partition, leader_epoch, "Acquired partition with epoch"
            );
            Ok(true)
        }
        Err(e) => {
            if e.is_fenced() {
                warn!(
                    topic,
                    partition, "Fenced during open - another broker owns this partition"
                );
                super::metrics::record_lease_operation("acquire", "fenced");
                ctx.coordinator
                    .invalidate_ownership_cache(topic, partition)
                    .await;
            } else {
                error!(topic, partition, error = %e, "Failed to open partition store");
                super::metrics::record_lease_operation("acquire", "error");
                if let Err(release_err) = ctx.coordinator.release_partition(topic, partition).await
                {
                    warn!(topic, partition, error = %release_err, "Failed to release partition after open error");
                }
            }
            Err(e)
        }
    }
}

async fn acquire_partition<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    match acquire_partition_core(ctx, topic, partition).await {
        Ok(true) => {}
        Ok(false) => debug!(topic, partition, "Partition owned by another broker"),
        Err(_) => {}
    }
}

async fn verify_ownership<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    let key = ctx.partition_key(topic, partition);
    match ctx
        .coordinator
        .owns_partition_for_read(topic, partition)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            warn!(topic, partition, "Lost partition ownership unexpectedly");
            // Close the store on this loss path too — see release_partition.
            if let Some(store) = ctx
                .partition_states
                .remove(&key)
                .and_then(|(_, s)| s.store())
            {
                store.clear_load_metrics();
                if let Err(e) = store.close().await {
                    warn!(topic, partition, error = %e, "Failed to close partition store after ownership loss");
                }
            }
            ctx.lease_cache.remove(&key);
            super::metrics::OWNED_PARTITIONS
                .with_label_values(&[topic])
                .dec();
        }
        Err(e) => warn!(topic, partition, error = %e, "Failed to verify partition ownership"),
    }
}

/// Release a partition that has been reassigned away from this broker.
///
/// SlateDB is closed immediately to keep the background compactor from
/// panicking when the new owner opens and fences us. The coordinator
/// release is best-effort — if it fails, the lease will expire naturally.
async fn release_reassigned_partition<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    let key = ctx.partition_key(topic, partition);

    if let Some((_, state)) = ctx.partition_states.remove(&key)
        && let Some(store) = state.store()
    {
        info!(
            ctx.broker_id,
            topic, partition, "Releasing reassigned partition - closing SlateDB"
        );

        store.clear_load_metrics();
        if let Err(e) = store.close().await {
            if e.is_fenced() {
                debug!(
                    topic,
                    partition, "Fenced during close (expected after reassignment)"
                );
            } else {
                warn!(topic, partition, error = %e, "Error closing store during reassignment");
            }
        }

        match ctx.coordinator.release_partition(topic, partition).await {
            Ok(()) => {
                info!(
                    ctx.broker_id,
                    topic, partition, "Released partition through coordinator"
                );
            }
            Err(e) => {
                warn!(
                    ctx.broker_id,
                    topic,
                    partition,
                    error = %e,
                    "Failed to release partition through coordinator - lease will expire naturally"
                );
            }
        }

        ctx.lease_cache.remove(&key);

        super::metrics::record_lease_operation("release", "reassigned");
        super::metrics::OWNED_PARTITIONS
            .with_label_values(&[topic])
            .dec();
    }
}

async fn release_partition_for_deleted_topic<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    info!(
        ctx.broker_id,
        topic, partition, "Releasing partition for deleted topic"
    );
    let key = ctx.partition_key(topic, partition);
    if let Some(store) = ctx
        .partition_states
        .remove(&key)
        .and_then(|(_, s)| s.store())
    {
        store.clear_load_metrics();
        // close() takes &self so this always works
        let _ = store.close().await;
    }
    // Drop the cluster lease too. Skipping the coordinator release leaves the
    // owner record in the Raft state machine until natural lease expiry; with
    // the default 60s lease the partition is unrecoverable for that window
    // even though the topic is gone, and the metric label below claims a
    // release happened.
    ctx.lease_cache.remove(&key);
    if let Err(e) = ctx.coordinator.release_partition(topic, partition).await {
        warn!(
            ctx.broker_id,
            topic,
            partition,
            error = %e,
            "Failed to release coordinator lease for deleted topic - lease will expire naturally"
        );
    }
    super::metrics::record_lease_operation("release", "topic_deleted");
    super::metrics::OWNED_PARTITIONS
        .with_label_values(&[topic])
        .dec();
}

// ============================================================================
// Partition Manager Tests
// ============================================================================

#[cfg(test)]
#[path = "partition_manager_tests.rs"]
mod tests;
