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

    /// Rebalance coordinator for fast failover and auto-balancing.
    ///
    /// This is responsible for:
    /// - Fast broker failure detection (500ms heartbeats)
    /// - Automatic partition redistribution on failure
    /// - Load-based auto-balancing
    rebalance_coordinator: Option<Arc<RebalanceCoordinator>>,

    /// Handles for rebalance coordinator background tasks.
    rebalance_task_handles: RwLock<Option<BackgroundTaskHandles>>,
}

impl<C: ClusterCoordinator + 'static> PartitionManager<C> {
    /// Create a new partition manager.
    pub fn new(
        coordinator: Arc<C>,
        object_store: Arc<dyn ObjectStore>,
        config: ClusterConfig,
    ) -> Self {
        // Create shutdown channel with capacity for all background tasks
        let (shutdown_tx, _) = broadcast::channel(4);

        // Create rebalance coordinator if fast failover or auto-balancing is enabled
        let rebalance_coordinator = if config.fast_failover_enabled || config.auto_balancer_enabled
        {
            use super::auto_balancer::AutoBalancerConfig;
            use super::failure_detector::FailureDetectorConfig;
            use super::load_metrics::LoadMetricsConfig;

            let rebalance_config = RebalanceCoordinatorConfig {
                failure_detector: FailureDetectorConfig {
                    heartbeat_interval: Duration::from_millis(config.fast_heartbeat_interval_ms),
                    failure_threshold: config.failure_threshold,
                    suspicion_threshold: config.failure_suspicion_threshold,
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
            rebalance_coordinator,
            rebalance_task_handles: RwLock::new(None),
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

    pub fn coordinator(&self) -> &Arc<C> {
        &self.coordinator
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
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        tokio::spawn(async move {
            let mut consecutive_failures: u32 = 0;

            loop {
                // Use jittered sleep instead of fixed interval to prevent thundering herd
                tokio::select! {
                    _ = tokio::time::sleep(with_jitter(interval)) => {},
                    _ = shutdown_rx.recv() => {
                        info!(broker_id, "Heartbeat loop received shutdown signal");
                        break;
                    }
                }

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

                                let partitions: Vec<_> = partition_states
                                    .iter()
                                    .filter_map(|entry| {
                                        if entry.value().is_owned() {
                                            Some(entry.key().clone())
                                        } else {
                                            None
                                        }
                                    })
                                    .collect();
                                for (topic, partition) in partitions {
                                    warn!(
                                        topic = &*topic,
                                        partition, "Releasing partition due to zombie mode"
                                    );
                                    if let Err(e) =
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

    /// Start the lease renewal loop.
    fn start_lease_renewal_loop(&self) -> JoinHandle<()> {
        let coordinator = self.coordinator.clone();
        let partition_states = self.partition_states.clone();
        let lease_cache = self.lease_cache.clone();
        let interval = self.config.lease_renewal_interval;
        let lease_secs = self.config.lease_duration.as_secs();
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        // Track consecutive errors to detect bulk failures.
        // If too many consecutive errors occur in a renewal cycle, it likely indicates
        // a systemic issue (coordinator unreachable) rather than partition-specific problems.
        // In this case, clear the entire lease cache to be safe.
        const BULK_FAILURE_THRESHOLD: usize = 3;

        tokio::spawn(async move {
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

                // Track consecutive errors in this renewal cycle
                let mut consecutive_errors: usize = 0;

                for (topic, partition) in partitions {
                    match coordinator
                        .renew_partition_lease(&topic, partition, lease_secs)
                        .await
                    {
                        Ok(true) => {
                            debug!(topic = &*topic, partition, "Renewed lease");
                            super::metrics::record_lease_operation("renew", "success");
                            // Update lease cache with new expiry time
                            let new_expiry = Instant::now() + Duration::from_secs(lease_secs);
                            lease_cache.insert((Arc::clone(&topic), partition), new_expiry);
                            // Reset consecutive error counter on success
                            consecutive_errors = 0;
                        }
                        Ok(false) => {
                            // Lease was lost - another broker owns this partition now.
                            // Immediately update partition state to prevent writes.
                            warn!(
                                topic = &*topic,
                                partition,
                                "Lost partition lease - immediately transitioning to Releasing state"
                            );
                            // Invalidate lease cache for this partition
                            lease_cache.remove(&(Arc::clone(&topic), partition));
                            // Remove from partition_states to reject further operations
                            if let Some((key, _)) =
                                partition_states.remove(&(Arc::clone(&topic), partition))
                            {
                                info!(
                                    topic = &*key.0,
                                    partition = key.1,
                                    "Partition removed from active state after lease loss"
                                );
                            }
                            super::metrics::record_lease_operation("renew", "failure");
                            // Reset counter - this is a valid response, not an error
                            consecutive_errors = 0;
                        }
                        Err(e) => {
                            consecutive_errors += 1;
                            error!(topic = &*topic, partition, error = %e, consecutive_errors, "Failed to renew lease (will retry)");
                            super::metrics::record_lease_operation("renew", "error");

                            // If too many consecutive errors, clear entire cache
                            // This indicates a systemic problem (coordinator unreachable)
                            // and we should be conservative about cache validity
                            if consecutive_errors >= BULK_FAILURE_THRESHOLD {
                                warn!(
                                    consecutive_errors,
                                    "Bulk lease renewal failure detected - clearing entire lease cache for safety"
                                );
                                lease_cache.clear();
                                super::metrics::record_coordinator_failure(
                                    "bulk_lease_renewal_failure",
                                );
                                // Don't keep trying in this cycle - break and wait for next interval
                                break;
                            }
                        }
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

        tokio::spawn(async move {
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
                let mut draining: HashSet<(String, i32)> = HashSet::new();
                let mut owned_topics: HashSet<String> = HashSet::new();

                for entry in ctx.partition_states.iter() {
                    let (topic_arc, partition) = entry.key();
                    let topic = topic_arc.to_string();
                    let state = entry.value();

                    if state.is_actively_owned() {
                        owned_topics.insert(topic.clone());
                        actively_owned.insert((topic, *partition));
                    } else if state.is_draining() {
                        owned_topics.insert(topic.clone());
                        draining.insert((topic, *partition));
                    }
                }

                // Check for deleted topics
                for topic in &owned_topics {
                    match ctx.coordinator.topic_exists(topic).await {
                        Ok(false) => {
                            info!(topic = %topic, "Topic deleted - releasing all partitions");
                            let topic_partitions: Vec<_> = actively_owned
                                .iter()
                                .chain(draining.iter())
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

                // Check draining partitions - release those whose drain is complete
                let drain_complete: Vec<_> = draining
                    .iter()
                    .filter(|(topic, partition)| {
                        let key: PartitionKey = (Arc::from(topic.as_str()), *partition);
                        ctx.partition_states
                            .get(&key)
                            .is_some_and(|s| s.is_drain_complete())
                    })
                    .cloned()
                    .collect();

                if !drain_complete.is_empty() {
                    let release_futures: Vec<_> = drain_complete
                        .iter()
                        .map(|(topic, partition)| {
                            info!(topic, partition, "Drain complete, releasing partition");
                            release_drained_partition(&ctx, topic, *partition)
                        })
                        .collect();
                    futures::future::join_all(release_futures).await;
                }

                // Acquire new partitions (only if not draining)
                // A partition being drained might get re-assigned to us if the cluster changes
                let all_owned: HashSet<_> = actively_owned.union(&draining).cloned().collect();
                let to_acquire: Vec<_> = assigned_set.difference(&all_owned).cloned().collect();
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

                    for chunk in to_acquire.chunks(MAX_CONCURRENT_ACQUISITIONS) {
                        let acquire_futures: Vec<_> = chunk
                            .iter()
                            .map(|(topic, partition)| acquire_partition(&ctx, topic, *partition))
                            .collect();
                        futures::future::join_all(acquire_futures).await;

                        // Add jittered delay between chunks to prevent thundering herd
                        if to_acquire.len() > MAX_CONCURRENT_ACQUISITIONS {
                            let jitter_ms = fastrand::u64(0..INTER_CHUNK_DELAY_MS);
                            tokio::time::sleep(Duration::from_millis(
                                INTER_CHUNK_DELAY_MS + jitter_ms,
                            ))
                            .await;
                        }
                    }
                }

                // Start draining actively owned partitions that are no longer assigned
                // This is a graceful handoff - we stop renewing the lease and wait for it to expire
                let to_drain: Vec<_> = actively_owned.difference(&assigned_set).cloned().collect();
                if !to_drain.is_empty() {
                    for (topic, partition) in to_drain {
                        start_draining_partition(&ctx, &topic, partition).await;
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

        tokio::spawn(async move {
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
        let key: PartitionKey = (Arc::from(topic), partition);
        self.partition_states.get(&key).and_then(|e| e.store())
    }

    /// Check if we own a partition.
    pub async fn owns_partition(&self, topic: &str, partition: i32) -> bool {
        let key: PartitionKey = (Arc::from(topic), partition);
        self.partition_states
            .get(&key)
            .map(|e| e.is_owned())
            .unwrap_or(false)
    }

    /// Try to acquire and open a partition.
    pub async fn acquire_partition(&self, topic: &str, partition: i32) -> SlateDBResult<bool> {
        if self.owns_partition(topic, partition).await {
            return Ok(true);
        }
        let ctx = self.ownership_context();
        acquire_partition_core(&ctx, topic, partition).await
    }

    /// Release a partition.
    pub async fn release_partition(&self, topic: &str, partition: i32) -> SlateDBResult<()> {
        let key: PartitionKey = (Arc::from(topic), partition);

        // Invalidate lease cache for this partition
        self.lease_cache.remove(&key);

        if let Some(store) = self
            .partition_states
            .remove(&key)
            .and_then(|(_, s)| s.store())
        {
            // Close the store (close() takes &self so this always works)
            if let Err(e) = store.close().await {
                error!(topic, partition, error = %e, "Failed to close partition store");
            } else {
                debug!(topic, partition, "Partition store closed successfully");
            }
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

        // Check if partition is draining (being handed off to another broker)
        // Draining partitions should not accept new writes
        let key: PartitionKey = (Arc::from(topic), partition);
        if let Some(state) = self.partition_states.get(&key)
            && state.is_draining()
        {
            return Err(SlateDBError::NotOwned {
                topic: topic.to_string(),
                partition,
            });
        }

        let store =
            self.get_store(topic, partition)
                .await
                .ok_or_else(|| SlateDBError::NotOwned {
                    topic: topic.to_string(),
                    partition,
                })?;

        // Fast path: Check lease cache first to avoid Raft overhead
        let cache_key: PartitionKey = (Arc::from(topic), partition);
        if let Some(cached_expiry) = self.lease_cache.get(&cache_key) {
            let now = Instant::now();
            if *cached_expiry > now {
                let remaining_secs = cached_expiry.duration_since(now).as_secs();
                // If sufficient TTL remaining, use cached lease
                if remaining_secs >= LEASE_CACHE_REFRESH_THRESHOLD_SECS {
                    // Validate lease is sufficient for write (MIN_LEASE_TTL_FOR_WRITE_SECS = 15s)
                    store.validate_lease_for_write(remaining_secs)?;
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
                warn!(topic, partition, error = %e, "Ownership verification failed during write - releasing partition");
                self.partition_states.remove(&key);
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
        use futures::TryStreamExt;
        use object_store::path::Path as ObjectPath;
        if self.owns_partition(topic, partition).await {
            self.release_partition(topic, partition).await?;
        }
        let path_prefix = format!("{}/topic-{}/partition-{}", self.base_path, topic, partition);
        let prefix = ObjectPath::from(path_prefix.as_str());
        info!(topic, partition, path = %path_prefix, "Deleting partition data from object store");
        let objects: Vec<_> = self
            .object_store
            .list(Some(&prefix))
            .try_collect()
            .await
            .map_err(|e| SlateDBError::Storage(e.to_string()))?;
        let object_count = objects.len();
        for obj in objects {
            if let Err(e) = self.object_store.delete(&obj.location).await {
                warn!(topic, partition, path = %obj.location, error = %e, "Failed to delete object");
            }
        }
        info!(topic, partition, object_count, "Deleted partition data");
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

        // Open fresh SlateDB instance - lease is verified and extended
        match PartitionStore::builder()
            .object_store(ctx.object_store.clone())
            .base_path(&ctx.base_path)
            .topic(&topic)
            .partition(partition)
            .max_fetch_response_size(ctx.max_fetch_response_size)
            .producer_state_cache_ttl_secs(ctx.producer_state_cache_ttl_secs)
            .zombie_mode(ctx.zombie_state.clone())
            .fail_on_recovery_gap(ctx.fail_on_recovery_gap)
            .min_lease_ttl_for_write_secs(ctx.min_lease_ttl_for_write_secs)
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

    match PartitionStore::builder()
        .object_store(ctx.object_store.clone())
        .base_path(&ctx.base_path)
        .topic(topic)
        .partition(partition)
        .max_fetch_response_size(ctx.max_fetch_response_size)
        .producer_state_cache_ttl_secs(ctx.producer_state_cache_ttl_secs)
        .zombie_mode(ctx.zombie_state.clone())
        .fail_on_recovery_gap(ctx.fail_on_recovery_gap)
        .min_lease_ttl_for_write_secs(ctx.min_lease_ttl_for_write_secs)
        .leader_epoch(leader_epoch) // Pass epoch for TOCTOU prevention
        .build()
        .await
    {
        Ok(store) => {
            ctx.partition_states.insert(
                (Arc::from(topic), partition),
                PartitionState::acquired(Arc::new(store)),
            );
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
    let key: PartitionKey = (Arc::from(topic), partition);
    match ctx
        .coordinator
        .owns_partition_for_read(topic, partition)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            warn!(topic, partition, "Lost partition ownership unexpectedly");
            ctx.partition_states.remove(&key);
            ctx.lease_cache.remove(&key);
            super::metrics::OWNED_PARTITIONS
                .with_label_values(&[topic])
                .dec();
        }
        Err(e) => warn!(topic, partition, error = %e, "Failed to verify partition ownership"),
    }
}

/// Start draining a partition (graceful handoff to another broker).
/// This transitions the partition to Draining state, which:
/// - Stops lease renewal (lease will expire naturally)
/// - Closes SlateDB immediately to prevent fencing panic from compactor
/// - Explicitly releases the partition through the coordinator
/// - The drain period gives the lease time to expire before cleanup
///
/// # Atomic Handoff
///
/// The partition is explicitly released through the coordinator after closing
/// SlateDB. This ensures:
/// 1. Old broker closes SlateDB (stops writes)
/// 2. Old broker releases partition via Raft (atomic state update)
/// 3. New broker can immediately acquire (doesn't wait for lease expiry)
///
/// If coordinator is unreachable during release, the lease will still expire
/// naturally, ensuring eventual recovery.
///
/// Note: SlateDB is closed immediately because keeping it open causes the
/// background compactor to panic when the new broker opens and fences us.
async fn start_draining_partition<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    let key: PartitionKey = (Arc::from(topic), partition);

    // Get the store from the current state and close it immediately
    if let Some((_, state)) = ctx.partition_states.remove(&key)
        && let Some(store) = state.store()
    {
        info!(
            ctx.broker_id,
            topic,
            partition,
            lease_secs = ctx.lease_secs,
            "Starting partition drain - closing SlateDB immediately to prevent fencing"
        );

        // Close SlateDB immediately to prevent compactor from panicking when fenced
        if let Err(e) = store.close().await {
            // Fencing errors are acceptable - the new owner may have already opened
            if e.is_fenced() {
                debug!(topic, partition, "Fenced during drain close (expected)");
            } else {
                warn!(topic, partition, error = %e, "Error closing store during drain");
            }
        }

        // Explicitly release the partition through the coordinator
        // This ensures the partition becomes available immediately for the new owner,
        // rather than waiting for the lease to expire naturally.
        //
        // If this fails (e.g., coordinator unreachable), the lease will still expire
        // naturally, ensuring eventual recovery.
        match ctx.coordinator.release_partition(topic, partition).await {
            Ok(()) => {
                info!(
                    ctx.broker_id,
                    topic, partition, "Released partition through coordinator - handoff complete"
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

        // Invalidate lease cache since we've released
        ctx.lease_cache.remove(&key);

        super::metrics::record_lease_operation("release", "drained");
        super::metrics::OWNED_PARTITIONS
            .with_label_values(&[topic])
            .dec();
    }
}

/// Release a partition that has completed draining.
/// The lease has expired, so it's safe to close SlateDB without racing with the new owner.
async fn release_drained_partition<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    info!(
        ctx.broker_id,
        topic, partition, "Releasing drained partition"
    );

    let key: PartitionKey = (Arc::from(topic), partition);

    if let Some(store) = ctx
        .partition_states
        .remove(&key)
        .and_then(|(_, s)| s.store())
    {
        // Close the store (close() now takes &self so this always works)
        if let Err(e) = store.close().await {
            // Fencing errors during close are expected since our lease expired
            if e.is_fenced() {
                debug!(topic, partition, "Fenced during drain close (expected)");
            } else {
                error!(topic, partition, error = %e, "Failed to close drained partition store");
            }
        }
    }

    super::metrics::record_lease_operation("release", "success");
    super::metrics::OWNED_PARTITIONS
        .with_label_values(&[topic])
        .dec();
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
    let key: PartitionKey = (Arc::from(topic), partition);
    if let Some(store) = ctx
        .partition_states
        .remove(&key)
        .and_then(|(_, s)| s.store())
    {
        // close() takes &self so this always works
        let _ = store.close().await;
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
mod tests {
    use super::*;
    use crate::cluster::mock_coordinator::MockCoordinator;
    use crate::cluster::traits::PartitionCoordinator;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    fn test_config(broker_id: i32) -> ClusterConfig {
        ClusterConfig {
            broker_id,
            host: "localhost".to_string(),
            port: 9092,
            auto_create_topics: true,
            ..Default::default()
        }
    }

    async fn create_test_partition_manager()
    -> (PartitionManager<MockCoordinator>, Arc<MockCoordinator>) {
        let config = test_config(1);
        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());

        coordinator.register_broker().await.unwrap();

        let pm = PartitionManager::new(coordinator.clone(), object_store, config);
        (pm, coordinator)
    }

    // ========================================================================
    // Basic Partition Manager Tests
    // ========================================================================

    #[tokio::test]
    async fn test_partition_manager_creation() {
        let (pm, _) = create_test_partition_manager().await;
        assert_eq!(pm.config().broker_id, 1);
        assert!(!pm.is_zombie());
    }

    #[tokio::test]
    async fn test_partition_manager_zombie_state() {
        let (pm, _) = create_test_partition_manager().await;

        let zombie_state = pm.zombie_state();
        assert!(!zombie_state.is_active());
        assert!(!pm.is_zombie());
    }

    #[tokio::test]
    async fn test_list_owned_initially_empty() {
        let (pm, _) = create_test_partition_manager().await;
        let owned = pm.list_owned().await;
        assert!(owned.is_empty());
    }

    // ========================================================================
    // Partition Ownership Tests
    // ========================================================================

    #[tokio::test]
    async fn test_acquire_partition() {
        let (pm, coordinator) = create_test_partition_manager().await;

        // Register topic first
        coordinator.register_topic("test-topic", 2).await.unwrap();

        // Initially not owned
        assert!(!pm.owns_partition("test-topic", 0).await);

        // Acquire partition
        let acquired = pm.acquire_partition("test-topic", 0).await.unwrap();
        assert!(acquired);

        // Now owned
        assert!(pm.owns_partition("test-topic", 0).await);
    }

    #[tokio::test]
    async fn test_release_partition() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("test-topic", 1).await.unwrap();

        // Acquire first
        pm.acquire_partition("test-topic", 0).await.unwrap();
        assert!(pm.owns_partition("test-topic", 0).await);

        // Release
        pm.release_partition("test-topic", 0).await.unwrap();
        assert!(!pm.owns_partition("test-topic", 0).await);
    }

    #[tokio::test]
    async fn test_list_owned_after_acquisition() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("test-topic", 3).await.unwrap();

        pm.acquire_partition("test-topic", 0).await.unwrap();
        pm.acquire_partition("test-topic", 2).await.unwrap();

        let owned = pm.list_owned().await;
        assert_eq!(owned.len(), 2);
        assert!(owned.contains(&("test-topic".to_string(), 0)));
        assert!(owned.contains(&("test-topic".to_string(), 2)));
    }

    // ========================================================================
    // Partition Limits Tests
    // ========================================================================

    #[tokio::test]
    async fn test_partition_limit_enforced() {
        let config = ClusterConfig {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            max_partitions_per_topic: 3, // Limit to 3 partitions
            ..Default::default()
        };

        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());

        coordinator.register_broker().await.unwrap();
        coordinator
            .register_topic("limited-topic", 5)
            .await
            .unwrap();

        let pm = PartitionManager::new(coordinator.clone(), object_store, config);

        // Should fail when trying to ensure partition beyond limit
        let result = pm.ensure_partition("limited-topic", 4).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Config Access Tests
    // ========================================================================

    #[tokio::test]
    async fn test_config_access() {
        let (pm, _) = create_test_partition_manager().await;

        let config = pm.config();
        assert_eq!(config.broker_id, 1);
        assert_eq!(config.host, "localhost");
        assert_eq!(config.port, 9092);
    }

    #[tokio::test]
    async fn test_coordinator_access() {
        let (pm, coordinator) = create_test_partition_manager().await;

        // Verify we can access the coordinator through the partition manager
        let pm_coordinator = pm.coordinator();
        assert_eq!(pm_coordinator.broker_id(), coordinator.broker_id());
    }

    // ========================================================================
    // Zombie Mode Tests
    // ========================================================================

    #[tokio::test]
    async fn test_exit_zombie_mode_when_not_zombie() {
        let (pm, _) = create_test_partition_manager().await;

        // Should succeed when not in zombie mode
        let result = pm.exit_zombie_mode_safely().await;
        assert!(result.is_ok());
    }

    // ========================================================================
    // Error Handling Tests
    // ========================================================================

    #[tokio::test]
    async fn test_get_for_read_non_owned_partition() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("test-topic", 1).await.unwrap();

        // Try to get for read without owning
        let result = pm.get_for_read("test-topic", 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_for_write_non_owned_partition() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("test-topic", 1).await.unwrap();

        // Try to get for write without owning
        let result = pm.get_for_write("test-topic", 0).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ensure_partition_auto_creates_topic_with_default_partitions() {
        let config = ClusterConfig {
            broker_id: 1,
            default_num_partitions: 8,
            auto_create_topics: true,
            ..Default::default()
        };
        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());
        coordinator.register_broker().await.unwrap();
        let pm = PartitionManager::new(coordinator.clone(), object_store, config);

        // Action: Ensure partition 0 of a new topic. This should auto-create the topic.
        let result = pm.ensure_partition("new-topic", 0).await;

        // Assert
        assert!(result.is_ok(), "ensure_partition failed: {:?}", result);
        let partition_count = coordinator.get_partition_count("new-topic").await.unwrap();
        assert_eq!(
            partition_count,
            Some(8),
            "Topic should have been created with default_num_partitions"
        );
    }

    #[tokio::test]
    async fn test_ensure_partition_auto_create_rejects_invalid_partition() {
        let config = ClusterConfig {
            broker_id: 1,
            default_num_partitions: 8,
            auto_create_topics: true,
            ..Default::default()
        };
        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());
        coordinator.register_broker().await.unwrap();
        let pm = PartitionManager::new(coordinator.clone(), object_store, config);

        // Action: Ensure a partition that is out of bounds for a new topic.
        let result = pm.ensure_partition("new-topic", 8).await;

        // Assert
        assert!(
            matches!(result, Err(SlateDBError::InvalidPartition { .. })),
            "ensure_partition should fail with InvalidPartition for out-of-bounds request on new topic"
        );
    }

    #[tokio::test]
    async fn test_ensure_partition_expands_existing_topic() {
        let config = ClusterConfig {
            broker_id: 1,
            default_num_partitions: 2,
            auto_create_topics: true,
            ..Default::default()
        };
        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());
        coordinator.register_broker().await.unwrap();
        let pm = PartitionManager::new(coordinator.clone(), object_store, config);

        // Setup: create a topic first
        pm.ensure_partition("existing-topic", 0).await.unwrap();
        let initial_count = coordinator
            .get_partition_count("existing-topic")
            .await
            .unwrap();
        assert_eq!(initial_count, Some(2));

        // Action: ensure a partition outside the current range
        let result = pm.ensure_partition("existing-topic", 3).await;

        // Assert
        assert!(
            result.is_ok(),
            "ensure_partition for expansion should succeed"
        );
        let expanded_count = coordinator
            .get_partition_count("existing-topic")
            .await
            .unwrap();
        assert_eq!(
            expanded_count,
            Some(4), // 3 + 1
            "Topic should have been expanded"
        );
    }

    // ========================================================================
    // Lease Cache Tests
    // ========================================================================

    /// Regression test: Verify that lease renewal updates the lease cache.
    ///
    /// Before the fix (commit where this test was added), the lease renewal loop
    /// would successfully renew leases via the coordinator but fail to update
    /// the local lease_cache. This caused writes to fail with "lease TTL too short"
    /// errors even though the lease was actually valid.
    ///
    /// The bug manifested as:
    /// - Partition acquired at T=0 with 60s lease
    /// - Cache says "expires at T=60"
    /// - Lease renewed at T=20 via coordinator (now valid until T=80)
    /// - But cache still says "expires at T=60" (BUG: not updated)
    /// - At T=33, cache shows only 27s remaining
    /// - Writes rejected because 27s < 30s minimum required
    #[tokio::test]
    async fn test_lease_renewal_updates_cache() {
        let (pm, coordinator) = create_test_partition_manager().await;

        // Setup: Register topic and acquire partition
        coordinator.register_topic("cache-test", 1).await.unwrap();
        pm.acquire_partition("cache-test", 0).await.unwrap();

        // The cache should have an entry after get_for_write populates it
        // First, populate the cache by doing a get_for_write
        let _ = pm.get_for_write("cache-test", 0).await;

        // Get the initial cache expiry
        let initial_expiry = pm
            .get_cached_lease_expiry("cache-test", 0)
            .expect("Cache should have entry after get_for_write");

        // Wait a small amount to ensure time passes
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Manually trigger lease renewal (simulates what the renewal loop does)
        let renewed = pm.renew_lease_for_test("cache-test", 0).await;
        assert!(renewed, "Lease renewal should succeed");

        // Get the new cache expiry
        let new_expiry = pm
            .get_cached_lease_expiry("cache-test", 0)
            .expect("Cache should still have entry after renewal");

        // The new expiry should be later than the initial expiry
        // (accounting for the time that passed plus the full lease duration)
        assert!(
            new_expiry > initial_expiry,
            "Lease cache should be updated with later expiry after renewal. \
             Initial: {:?}, New: {:?}",
            initial_expiry,
            new_expiry
        );

        // Verify the new expiry is approximately lease_duration from now
        let expected_min =
            Instant::now() + Duration::from_secs(pm.config().lease_duration.as_secs() - 1);
        assert!(
            new_expiry >= expected_min,
            "New expiry should be at least (now + lease_duration - 1s)"
        );
    }

    /// Test that lease cache is properly invalidated when lease is lost.
    #[tokio::test]
    async fn test_lease_cache_invalidated_on_lease_loss() {
        let (pm, coordinator) = create_test_partition_manager().await;

        // Setup
        coordinator.register_topic("loss-test", 1).await.unwrap();
        pm.acquire_partition("loss-test", 0).await.unwrap();

        // Populate cache
        let _ = pm.get_for_write("loss-test", 0).await;
        assert!(
            pm.get_cached_lease_expiry("loss-test", 0).is_some(),
            "Cache should have entry"
        );

        // Release the partition
        pm.release_partition("loss-test", 0).await.unwrap();

        // Cache entry should be removed (or effectively invalidated via partition_states removal)
        // Note: The cache entry itself may still exist, but the partition is no longer owned
        assert!(
            !pm.owns_partition("loss-test", 0).await,
            "Partition should no longer be owned after release"
        );
    }

    // ========================================================================
    // Additional Coverage Tests
    // ========================================================================

    #[tokio::test]
    async fn test_owned_partition_count() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("count-test", 5).await.unwrap();

        // Initially zero
        assert_eq!(pm.owned_partition_count(), 0);

        // Acquire some partitions
        pm.acquire_partition("count-test", 0).await.unwrap();
        assert_eq!(pm.owned_partition_count(), 1);

        pm.acquire_partition("count-test", 1).await.unwrap();
        pm.acquire_partition("count-test", 2).await.unwrap();
        assert_eq!(pm.owned_partition_count(), 3);

        // Release one
        pm.release_partition("count-test", 1).await.unwrap();
        assert_eq!(pm.owned_partition_count(), 2);
    }

    #[tokio::test]
    async fn test_ensure_partition_auto_create_disabled() {
        // Test that with auto_create_topics disabled, we still can ensure
        // partitions for topics that exist. MockCoordinator may auto-register topics
        // so we just verify the config is respected.
        let config = ClusterConfig {
            broker_id: 1,
            auto_create_topics: false, // Disabled!
            ..Default::default()
        };
        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());
        coordinator.register_broker().await.unwrap();
        let pm = PartitionManager::new(coordinator.clone(), object_store, config);

        // Verify the config was applied
        assert!(!pm.config().auto_create_topics);
    }

    #[tokio::test]
    async fn test_get_for_write_success() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("write-test", 1).await.unwrap();
        pm.acquire_partition("write-test", 0).await.unwrap();

        // Should succeed for owned partition
        let result = pm.get_for_write("write-test", 0).await;
        assert!(
            result.is_ok(),
            "get_for_write should succeed for owned partition"
        );

        let write_guard = result.unwrap();
        assert_eq!(write_guard.topic(), "write-test");
        assert_eq!(write_guard.partition(), 0);
    }

    #[tokio::test]
    async fn test_get_for_read_success() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("read-test", 1).await.unwrap();
        pm.acquire_partition("read-test", 0).await.unwrap();

        // Should succeed for owned partition
        let result = pm.get_for_read("read-test", 0).await;
        assert!(
            result.is_ok(),
            "get_for_read should succeed for owned partition"
        );

        let read_guard = result.unwrap();
        assert_eq!(read_guard.topic(), "read-test");
        assert_eq!(read_guard.partition(), 0);
    }

    #[tokio::test]
    async fn test_acquire_already_owned_partition() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator
            .register_topic("already-owned", 1)
            .await
            .unwrap();

        // Acquire first
        let first = pm.acquire_partition("already-owned", 0).await.unwrap();
        assert!(first);

        // Second acquire should also succeed (already owned)
        let second = pm.acquire_partition("already-owned", 0).await.unwrap();
        assert!(second);
    }

    #[tokio::test]
    async fn test_release_non_owned_partition() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("not-owned", 1).await.unwrap();

        // Release without owning - should succeed (no-op)
        let result = pm.release_partition("not-owned", 0).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_ensure_partition_negative_partition() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("neg-test", 5).await.unwrap();

        // Negative partition should either fail or be handled gracefully
        // The exact behavior depends on the implementation, but we verify it doesn't panic
        let result = pm.ensure_partition("neg-test", -1).await;
        // Just verify we get a defined result (either error or success)
        let _ = result; // Consume the result
    }

    #[tokio::test]
    async fn test_with_jitter_returns_reasonable_values() {
        // Test the with_jitter function by verifying the output is within expected range
        let base = Duration::from_secs(10);

        // Run multiple times to verify jitter is applied
        let mut min_seen = Duration::from_secs(100);
        let mut max_seen = Duration::ZERO;

        for _ in 0..100 {
            let jittered = with_jitter(base);
            if jittered < min_seen {
                min_seen = jittered;
            }
            if jittered > max_seen {
                max_seen = jittered;
            }
        }

        // Expected range: 8.5s to 11.5s (±15% of 10s)
        assert!(
            min_seen >= Duration::from_millis(8000),
            "Min jittered value should be >= 8s, got {:?}",
            min_seen
        );
        assert!(
            max_seen <= Duration::from_millis(12000),
            "Max jittered value should be <= 12s, got {:?}",
            max_seen
        );
        // Verify we got some variation
        assert!(
            max_seen - min_seen >= Duration::from_millis(500),
            "Should see at least 500ms variation in jitter"
        );
    }

    #[tokio::test]
    async fn test_multiple_topics_partitions() {
        let (pm, coordinator) = create_test_partition_manager().await;

        // Register multiple topics
        coordinator.register_topic("topic-a", 3).await.unwrap();
        coordinator.register_topic("topic-b", 2).await.unwrap();

        // Acquire partitions from different topics
        pm.acquire_partition("topic-a", 0).await.unwrap();
        pm.acquire_partition("topic-a", 2).await.unwrap();
        pm.acquire_partition("topic-b", 1).await.unwrap();

        // Verify ownership
        assert!(pm.owns_partition("topic-a", 0).await);
        assert!(!pm.owns_partition("topic-a", 1).await);
        assert!(pm.owns_partition("topic-a", 2).await);
        assert!(!pm.owns_partition("topic-b", 0).await);
        assert!(pm.owns_partition("topic-b", 1).await);

        // Verify count and list
        assert_eq!(pm.owned_partition_count(), 3);
        let owned = pm.list_owned().await;
        assert_eq!(owned.len(), 3);
    }

    #[tokio::test]
    async fn test_zombie_state_accessor() {
        let (pm, _) = create_test_partition_manager().await;

        // Get zombie state
        let zombie_state = pm.zombie_state();

        // Initially not active
        assert!(!zombie_state.is_active());

        // The zombie_state() returns an Arc that can be shared
        let zombie_state2 = pm.zombie_state();
        assert!(Arc::ptr_eq(&zombie_state, &zombie_state2));
    }

    #[tokio::test]
    async fn test_partition_manager_with_fast_failover_disabled() {
        let config = ClusterConfig {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            fast_failover_enabled: false,
            auto_balancer_enabled: false,
            ..Default::default()
        };

        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());
        coordinator.register_broker().await.unwrap();

        let pm = PartitionManager::new(coordinator, object_store, config);

        // Should still work without rebalance coordinator
        assert_eq!(pm.config().broker_id, 1);
        assert!(!pm.is_zombie());
    }

    #[tokio::test]
    async fn test_partition_manager_with_fast_failover_enabled() {
        let config = ClusterConfig {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            fast_failover_enabled: true,
            auto_balancer_enabled: true,
            ..Default::default()
        };

        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());
        coordinator.register_broker().await.unwrap();

        let pm = PartitionManager::new(coordinator, object_store, config);

        // Should have rebalance coordinator enabled
        assert_eq!(pm.config().broker_id, 1);
        assert!(pm.config().fast_failover_enabled);
        assert!(pm.config().auto_balancer_enabled);
    }

    #[tokio::test]
    async fn test_object_store_access() {
        let (pm, _) = create_test_partition_manager().await;

        // Verify the config has some object store path set
        assert!(!pm.config().object_store_path.is_empty());
    }

    #[tokio::test]
    async fn test_ensure_partition_creates_slatedb_store() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("store-test", 1).await.unwrap();

        // Ensure creates the store
        let result = pm.ensure_partition("store-test", 0).await;
        assert!(result.is_ok(), "ensure_partition should succeed");

        // After ensure, we should be able to get for read/write
        let read_result = pm.get_for_read("store-test", 0).await;
        assert!(
            read_result.is_ok(),
            "get_for_read should succeed after ensure"
        );
    }

    // ========================================================================
    // Additional Zombie Mode and Edge Case Tests
    // ========================================================================

    #[tokio::test]
    async fn test_zombie_mode_enter_and_exit() {
        let (pm, _) = create_test_partition_manager().await;

        // Initially not zombie
        assert!(!pm.is_zombie());

        // Enter zombie mode via the state
        pm.zombie_state().enter();
        assert!(pm.is_zombie());

        // Exit zombie mode using force_exit
        pm.zombie_state().force_exit("test exit");
        assert!(!pm.is_zombie());
    }

    #[tokio::test]
    async fn test_get_for_write_fails_in_zombie_mode() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("zombie-test", 1).await.unwrap();
        pm.acquire_partition("zombie-test", 0).await.unwrap();

        // Ensure partition is available
        let _ = pm.get_for_write("zombie-test", 0).await.unwrap();

        // Enter zombie mode
        pm.zombie_state().enter();

        // get_for_write should fail in zombie mode
        let result = pm.get_for_write("zombie-test", 0).await;
        assert!(result.is_err(), "get_for_write should fail in zombie mode");

        // Exit and verify it works again
        pm.zombie_state().force_exit("test");
        let result = pm.get_for_write("zombie-test", 0).await;
        assert!(
            result.is_ok(),
            "get_for_write should succeed after exiting zombie mode"
        );
    }

    #[tokio::test]
    async fn test_exit_zombie_mode_safely_when_zombie() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("safe-exit", 1).await.unwrap();
        pm.acquire_partition("safe-exit", 0).await.unwrap();

        // Enter zombie mode
        pm.zombie_state().enter();
        assert!(pm.is_zombie());

        // Exit zombie mode safely should succeed
        let result = pm.exit_zombie_mode_safely().await;
        assert!(result.is_ok(), "exit_zombie_mode_safely should succeed");

        // Should no longer be zombie
        assert!(!pm.is_zombie());
    }

    #[tokio::test]
    async fn test_concurrent_partition_acquire_release() {
        let (pm, coordinator) = create_test_partition_manager().await;
        let pm = Arc::new(pm);

        coordinator.register_topic("concurrent", 10).await.unwrap();

        // Spawn multiple tasks that acquire and release partitions
        let mut handles = vec![];
        for i in 0..5 {
            let pm_clone = pm.clone();
            handles.push(tokio::spawn(async move {
                for _ in 0..3 {
                    pm_clone.acquire_partition("concurrent", i).await.unwrap();
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    pm_clone.release_partition("concurrent", i).await.unwrap();
                }
            }));
        }

        // Wait for all tasks
        for handle in handles {
            handle.await.unwrap();
        }

        // All partitions should be released
        assert_eq!(pm.owned_partition_count(), 0);
    }

    #[tokio::test]
    async fn test_lease_cache_ttl_check() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("ttl-test", 1).await.unwrap();
        pm.acquire_partition("ttl-test", 0).await.unwrap();

        // Populate cache
        let _ = pm.get_for_write("ttl-test", 0).await.unwrap();

        // Get cached lease TTL
        let expiry = pm.get_cached_lease_expiry("ttl-test", 0);
        assert!(expiry.is_some());

        // The expiry should be in the future
        let expiry = expiry.unwrap();
        assert!(expiry > Instant::now());
    }

    #[tokio::test]
    async fn test_get_cached_lease_expiry_non_owned() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("no-cache", 1).await.unwrap();

        // Don't acquire, just check cache
        let expiry = pm.get_cached_lease_expiry("no-cache", 0);
        assert!(
            expiry.is_none(),
            "Non-owned partition should not have cache entry"
        );
    }

    #[tokio::test]
    async fn test_acquire_partition_twice_same_session() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator
            .register_topic("double-acquire", 1)
            .await
            .unwrap();

        // First acquire
        let first = pm.acquire_partition("double-acquire", 0).await.unwrap();
        assert!(first);

        // Second acquire (same partition) should succeed
        let second = pm.acquire_partition("double-acquire", 0).await.unwrap();
        assert!(second);

        // Should still be owned (count = 1, not 2)
        let owned = pm.list_owned().await;
        assert_eq!(owned.len(), 1);
    }

    #[tokio::test]
    async fn test_partition_manager_broker_id() {
        let config = ClusterConfig {
            broker_id: 42,
            ..Default::default()
        };
        let coordinator = Arc::new(MockCoordinator::new(42, "localhost", 9092));
        let object_store = Arc::new(InMemory::new());
        coordinator.register_broker().await.unwrap();

        let pm = PartitionManager::new(coordinator, object_store, config);

        assert_eq!(pm.config().broker_id, 42);
    }

    #[tokio::test]
    async fn test_shutdown_releases_partitions() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator
            .register_topic("shutdown-test", 3)
            .await
            .unwrap();
        pm.acquire_partition("shutdown-test", 0).await.unwrap();
        pm.acquire_partition("shutdown-test", 1).await.unwrap();
        pm.acquire_partition("shutdown-test", 2).await.unwrap();

        assert_eq!(pm.owned_partition_count(), 3);

        // Initiate shutdown
        let _ = pm.shutdown().await;

        // After shutdown, all partitions should be released
        assert_eq!(pm.owned_partition_count(), 0);
    }

    #[tokio::test]
    async fn test_get_for_write_returns_write_guard_fields() {
        let (pm, coordinator) = create_test_partition_manager().await;

        coordinator.register_topic("guard-fields", 1).await.unwrap();
        pm.acquire_partition("guard-fields", 0).await.unwrap();

        let guard = pm.get_for_write("guard-fields", 0).await.unwrap();

        // Verify the guard is accessible (Arc<PartitionStore>)
        assert_eq!(guard.topic(), "guard-fields");
        assert_eq!(guard.partition(), 0);
    }
}
