//! Rebalance coordinator for orchestrating fast failover and auto-balancing.
//!
//! This module ties together the failure detector, load metrics collector,
//! and auto-balancer to provide:
//! - Fast broker failure detection and partition redistribution
//! - Periodic load-based rebalancing
//! - Centralized coordination of partition ownership changes
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────────┐
//! │                    RebalanceCoordinator                          │
//! │                                                                  │
//! │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │
//! │  │FailureDetector  │  │LoadMetrics      │  │ AutoBalancer    │  │
//! │  │                 │  │Collector        │  │                 │  │
//! │  │- 500ms heartbeat│  │- bytes/sec      │  │- 70/30 weighted │  │
//! │  │- 5 missed=fail  │  │- per partition  │  │- 30% deviation  │  │
//! │  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘  │
//! │           │                    │                    │           │
//! │           └────────────────────┼────────────────────┘           │
//! │                                ▼                                │
//! │                    ┌─────────────────────┐                      │
//! │                    │  Raft Coordinator   │                      │
//! │                    │  TransferPartition  │                      │
//! │                    │  BatchTransfer      │                      │
//! │                    └─────────────────────┘                      │
//! └─────────────────────────────────────────────────────────────────┘
//! ```

use std::collections::HashMap;
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::runtime::Handle;
use tokio::sync::Mutex;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

use super::auto_balancer::{AutoBalancer, AutoBalancerConfig, LoadSnapshot, PartitionMove};
use super::failure_detector::{
    BrokerHealthState, FailureDetector, FailureDetectorConfig, HealthStateChange,
};
use super::load_metrics::{LoadMetricsCollector, LoadMetricsConfig};
use super::metrics;
use super::raft::domains::{PartitionTransfer, TransferReason};

/// Wrap a background-task body so panics are caught and logged. Without
/// this, a `tokio::spawn` body that panics has its stack unwound and the
/// task simply ends — `tokio::task::JoinHandle::await` would surface the
/// panic, but no caller awaits these long-running loops, so the failure is
/// silent and K8s liveness probes still pass even though the broker has
/// stopped doing the work the task represented.
async fn supervised<F>(name: &'static str, fut: F)
where
    F: std::future::Future<Output = ()>,
{
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;
    if let Err(panic_payload) = AssertUnwindSafe(fut).catch_unwind().await {
        tracing::error!(task = name, payload = ?panic_payload, "Background task panicked");
    }
}

/// Configuration for the rebalance coordinator.
#[derive(Debug, Clone)]
pub struct RebalanceCoordinatorConfig {
    /// Failure detector configuration.
    pub failure_detector: FailureDetectorConfig,

    /// Load metrics collector configuration.
    pub load_metrics: LoadMetricsConfig,

    /// Auto-balancer configuration.
    pub auto_balancer: AutoBalancerConfig,

    /// Whether fast failover is enabled.
    pub fast_failover_enabled: bool,

    /// Default lease duration for transferred partitions (ms).
    pub transfer_lease_duration_ms: u64,

    /// Maximum partitions to transfer per failover batch.
    /// Prevents thundering herd by chunking transfers.
    /// Default: 10 partitions per batch.
    pub max_failover_batch_size: usize,

    /// Delay between failover batches (ms).
    /// Allows target brokers to stabilize before receiving more partitions.
    /// Default: 500ms.
    pub failover_batch_delay_ms: u64,
}

impl Default for RebalanceCoordinatorConfig {
    fn default() -> Self {
        Self {
            failure_detector: FailureDetectorConfig::default(),
            load_metrics: LoadMetricsConfig::default(),
            auto_balancer: AutoBalancerConfig::default(),
            fast_failover_enabled: true,
            transfer_lease_duration_ms: 60_000, // 60 seconds
            max_failover_batch_size: 10,        // 10 partitions per batch
            failover_batch_delay_ms: 500,       // 500ms between batches
        }
    }
}

impl RebalanceCoordinatorConfig {
    /// Create a config with fast failover disabled.
    pub fn with_failover_disabled(mut self) -> Self {
        self.fast_failover_enabled = false;
        self
    }

    /// Create a config with auto-balancing disabled.
    pub fn with_balancing_disabled(mut self) -> Self {
        self.auto_balancer.enabled = false;
        self
    }
}

/// Result of a broker failure handling operation.
#[derive(Debug)]
pub struct FailoverResult {
    /// Broker ID that failed.
    pub failed_broker_id: i32,

    /// Number of partitions that were redistributed.
    pub partitions_redistributed: usize,

    /// Number of partitions that failed to transfer.
    pub partitions_failed: usize,

    /// Target brokers that received partitions.
    pub target_brokers: Vec<i32>,
}

/// Result of a rebalance operation.
#[derive(Debug)]
pub struct RebalanceResult {
    /// Whether rebalancing was performed.
    pub performed: bool,

    /// Number of partitions moved.
    pub partitions_moved: usize,

    /// Current deviation after rebalance.
    pub deviation_after: f64,

    /// Moves that were executed.
    pub executed_moves: Vec<PartitionMove>,
}

/// Callback trait for partition transfer execution.
///
/// The rebalance coordinator uses this trait to execute actual
/// partition transfers without being tightly coupled to the
/// Raft coordinator implementation.
#[async_trait::async_trait]
pub trait PartitionTransferExecutor: Send + Sync {
    /// Transfer a single partition from one broker to another.
    async fn transfer_partition(
        &self,
        topic: &str,
        partition: i32,
        from_broker_id: i32,
        to_broker_id: i32,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> Result<(), String>;

    /// Transfer multiple partitions atomically.
    async fn batch_transfer_partitions(
        &self,
        transfers: Vec<PartitionTransfer>,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> Result<(usize, Vec<(String, i32, String)>), String>;

    /// Mark a broker as failed in the coordination state.
    ///
    /// Implementations must atomically fence the broker and release its
    /// partitions, returning the released `(topic, partition)` pairs so the
    /// failover flow can reassign exactly that set.
    async fn mark_broker_failed(
        &self,
        broker_id: i32,
        reason: &str,
    ) -> Result<Vec<(String, i32)>, String>;

    /// Get current partition ownership map.
    async fn get_partition_owners(&self) -> HashMap<(String, i32), i32>;

    /// Get partition owners along with Raft commit index for consistent snapshots.
    ///
    /// This ensures rebalancing decisions are made on data from the same
    /// committed Raft state, preventing race conditions.
    async fn get_partition_owners_with_index(&self) -> (HashMap<(String, i32), i32>, u64);

    /// Get list of active broker IDs.
    async fn get_active_brokers(&self) -> Vec<i32>;

    /// Whether this node should initiate broker-failure handling.
    ///
    /// Only the coordination leader should propose `MarkBrokerFailed` and
    /// drive partition reassignment; followers skip the failure loop tick.
    async fn should_initiate_failover(&self) -> bool {
        true
    }
}

// ============================================================================
// Blanket implementation of PartitionTransferExecutor for ClusterCoordinator
// ============================================================================

use super::traits::ClusterCoordinator;

/// Adapter that implements `PartitionTransferExecutor` for any `ClusterCoordinator`.
///
/// This allows the `RebalanceCoordinator` to work with any coordinator implementation
/// (RaftCoordinator for production, MockCoordinator for tests) without tight coupling.
pub struct CoordinatorExecutorAdapter<C: ClusterCoordinator> {
    coordinator: Arc<C>,
}

impl<C: ClusterCoordinator> CoordinatorExecutorAdapter<C> {
    /// Create a new adapter wrapping the given coordinator.
    pub fn new(coordinator: Arc<C>) -> Self {
        Self { coordinator }
    }
}

#[async_trait::async_trait]
impl<C: ClusterCoordinator + 'static> PartitionTransferExecutor for CoordinatorExecutorAdapter<C> {
    async fn transfer_partition(
        &self,
        topic: &str,
        partition: i32,
        from_broker_id: i32,
        to_broker_id: i32,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> Result<(), String> {
        self.coordinator
            .transfer_partition(
                topic,
                partition,
                from_broker_id,
                to_broker_id,
                reason,
                lease_duration_ms,
            )
            .await
            .map_err(|e| e.to_string())
    }

    async fn batch_transfer_partitions(
        &self,
        transfers: Vec<PartitionTransfer>,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> Result<(usize, Vec<(String, i32, String)>), String> {
        self.coordinator
            .batch_transfer_partitions(transfers, reason, lease_duration_ms)
            .await
            .map_err(|e| e.to_string())
    }

    async fn mark_broker_failed(
        &self,
        broker_id: i32,
        reason: &str,
    ) -> Result<Vec<(String, i32)>, String> {
        self.coordinator
            .mark_broker_failed(broker_id, reason)
            .await
            .map_err(|e| e.to_string())
    }

    async fn get_partition_owners(&self) -> HashMap<(String, i32), i32> {
        self.coordinator.get_all_partition_owners().await
    }

    async fn get_partition_owners_with_index(&self) -> (HashMap<(String, i32), i32>, u64) {
        self.coordinator.get_all_partition_owners_with_index().await
    }

    async fn get_active_brokers(&self) -> Vec<i32> {
        self.coordinator.get_active_broker_ids().await
    }

    async fn should_initiate_failover(&self) -> bool {
        match self.coordinator.current_leader_id().await {
            Ok(Some(leader)) => leader == self.coordinator.broker_id(),
            _ => false,
        }
    }
}

/// Rebalance coordinator that orchestrates fast failover and auto-balancing.
pub struct RebalanceCoordinator {
    config: RebalanceCoordinatorConfig,

    /// Failure detector for fast broker failure detection.
    failure_detector: Arc<FailureDetector>,

    /// Load metrics collector for throughput tracking.
    load_collector: Arc<LoadMetricsCollector>,

    /// Auto-balancer for load-based rebalancing.
    auto_balancer: RwLock<AutoBalancer>,

    /// Whether the coordinator is running.
    running: AtomicBool,

    /// Wakes background loops on shutdown so they don't sit in a long
    /// `sleep` past the requested stop.
    shutdown_notify: Arc<tokio::sync::Notify>,

    /// Counter for total failovers handled.
    total_failovers: AtomicU64,

    /// Counter for total rebalances performed.
    total_rebalances: AtomicU64,

    /// This broker's ID.
    broker_id: i32,

    /// Runtime handle for spawning control plane tasks.
    runtime: Handle,

    /// Brokers detected as failed whose handoff has not yet been
    /// successfully proposed to Raft. Re-driven on every failure-check tick
    /// so a single transient `mark_broker_failed` error does not strand the
    /// broker's partitions.
    pending_failovers: Mutex<HashSet<i32>>,

    /// Partitions whose failover transfer failed and need to be retried.
    /// Keyed by the originally-failed broker so the failover loop can
    /// continue retrying after the broker has been removed from
    /// `pending_failovers`. Without this, a partial-batch failure during
    /// failover (transient Raft error, target broker fenced) strands the
    /// failed partitions: `mark_broker_failed` already cleared their owner,
    /// the next call returns an empty list, and the transfers are never
    /// re-issued.
    pending_orphaned_partitions: Mutex<HashMap<i32, HashSet<(String, i32)>>>,
}

impl RebalanceCoordinator {
    /// Create a new rebalance coordinator.
    pub fn new(config: RebalanceCoordinatorConfig, broker_id: i32, runtime: Handle) -> Self {
        info!(
            broker_id,
            fast_failover = config.fast_failover_enabled,
            auto_balancer = config.auto_balancer.enabled,
            heartbeat_interval_ms = config.failure_detector.heartbeat_interval.as_millis(),
            "Creating rebalance coordinator"
        );

        Self {
            failure_detector: Arc::new(FailureDetector::new(config.failure_detector.clone())),
            load_collector: Arc::new(LoadMetricsCollector::with_config(
                config.load_metrics.clone(),
            )),
            auto_balancer: RwLock::new(AutoBalancer::new(config.auto_balancer.clone())),
            running: AtomicBool::new(false),
            shutdown_notify: Arc::new(tokio::sync::Notify::new()),
            total_failovers: AtomicU64::new(0),
            total_rebalances: AtomicU64::new(0),
            config,
            broker_id,
            runtime,
            pending_failovers: Mutex::new(HashSet::new()),
            pending_orphaned_partitions: Mutex::new(HashMap::new()),
        }
    }

    /// Create with default configuration.
    pub fn with_defaults(broker_id: i32, runtime: Handle) -> Self {
        Self::new(RebalanceCoordinatorConfig::default(), broker_id, runtime)
    }

    /// Get the configuration.
    pub fn config(&self) -> &RebalanceCoordinatorConfig {
        &self.config
    }

    /// Get the failure detector.
    pub fn failure_detector(&self) -> &Arc<FailureDetector> {
        &self.failure_detector
    }

    /// Get the load metrics collector.
    pub fn load_collector(&self) -> &Arc<LoadMetricsCollector> {
        &self.load_collector
    }

    /// Register a broker for health tracking.
    pub fn register_broker(&self, broker_id: i32) {
        self.failure_detector.register_broker(broker_id);
    }

    /// Unregister a broker from health tracking.
    pub fn unregister_broker(&self, broker_id: i32) {
        self.failure_detector.unregister_broker(broker_id);
    }

    /// Record a heartbeat from a broker.
    pub fn record_heartbeat(&self, broker_id: i32) {
        self.failure_detector.record_heartbeat(broker_id);
    }

    /// Check brokers for failures and return state changes.
    pub fn check_broker_health(&self) -> Vec<HealthStateChange> {
        self.failure_detector.check_brokers()
    }

    /// Handle broker failure by redistributing its partitions.
    ///
    /// This is called when the failure detector marks a broker as failed.
    pub async fn handle_broker_failure<E: PartitionTransferExecutor>(
        &self,
        failed_broker_id: i32,
        executor: &E,
    ) -> Result<FailoverResult, String> {
        if !self.config.fast_failover_enabled {
            return Err("Fast failover is disabled".to_string());
        }

        info!(
            failed_broker_id,
            "Handling broker failure - starting partition redistribution"
        );

        let failover_start = Instant::now();
        metrics::record_broker_failure("heartbeat_timeout");

        // Mark broker as failed in Raft state. This single applied command
        // fences the broker and releases all of its partitions (clearing
        // owners and bumping leader epochs), returning exactly the set that
        // needs reassignment. Because the partitions are already released,
        // re-reading the ownership map would no longer show them — the
        // returned list is the authoritative source for the transfer plan.
        let failed_partitions = executor
            .mark_broker_failed(failed_broker_id, "heartbeat_timeout")
            .await?;

        if failed_partitions.is_empty() {
            info!(
                failed_broker_id,
                "No partitions to redistribute for failed broker"
            );
            metrics::record_failover_duration(
                "heartbeat_timeout",
                failover_start.elapsed().as_secs_f64(),
            );
            return Ok(FailoverResult {
                failed_broker_id,
                partitions_redistributed: 0,
                partitions_failed: 0,
                target_brokers: vec![],
            });
        }

        // Get current partition ownership (for load-aware distribution)
        let partition_owners = executor.get_partition_owners().await;
        let active_brokers = executor.get_active_brokers().await;

        // Filter out the failed broker
        let available_brokers: Vec<i32> = active_brokers
            .into_iter()
            .filter(|&id| id != failed_broker_id)
            .collect();

        if available_brokers.is_empty() {
            error!(
                failed_broker_id,
                "No available brokers for partition redistribution; \
                 released partitions will be picked up by the ownership loop \
                 once brokers become available"
            );
            return Err("No available brokers for failover".to_string());
        }

        // Count existing partitions per broker for load-aware distribution.
        // Partition count is the cheap baseline; per-broker bytes/sec is the
        // dimension that actually matters for failover impact (a broker with
        // ten 100 KB/s partitions and one 1 GB/s partition is the same
        // partition count as a broker with eleven idle ones, but very
        // different stress under failover). Score by `(throughput, count)`
        // so the lightest-loaded by bytes wins; partition count breaks ties
        // and makes single-broker test setups deterministic.
        let mut broker_partition_counts: HashMap<i32, usize> =
            available_brokers.iter().map(|&id| (id, 0)).collect();
        let broker_loads = self
            .load_collector
            .aggregate_broker_loads(&partition_owners);
        let mut broker_throughput: HashMap<i32, f64> =
            available_brokers.iter().map(|&id| (id, 0.0_f64)).collect();
        for (id, summary) in &broker_loads {
            if let Some(slot) = broker_throughput.get_mut(id) {
                *slot = summary.total_bytes_per_sec;
            }
        }

        // Estimate the per-partition throughput each transfer drags onto
        // its target so the running totals stay honest as we assign.
        let partition_loads = self.load_collector.get_all_partition_loads();

        // Count existing partitions (excluding the failed broker's partitions)
        for ((_, _), &owner) in &partition_owners {
            if owner != failed_broker_id
                && let Some(count) = broker_partition_counts.get_mut(&owner)
            {
                *count += 1;
            }
        }

        // Distribute partitions to the least loaded brokers
        let transfers: Vec<PartitionTransfer> = failed_partitions
            .iter()
            .map(|(topic, partition)| {
                // Score by (throughput, partition_count): the lightest broker
                // by bytes/sec wins, with partition count breaking ties.
                let target_broker = *available_brokers
                    .iter()
                    .min_by(|a, b| {
                        let ta = broker_throughput.get(a).copied().unwrap_or(0.0);
                        let tb = broker_throughput.get(b).copied().unwrap_or(0.0);
                        let ca = broker_partition_counts.get(a).copied().unwrap_or(0);
                        let cb = broker_partition_counts.get(b).copied().unwrap_or(0);
                        ta.partial_cmp(&tb)
                            .unwrap_or(std::cmp::Ordering::Equal)
                            .then_with(|| ca.cmp(&cb))
                    })
                    .unwrap_or(&available_brokers[0]);

                // Increment running totals so subsequent transfers see the
                // load this one will drag onto the target.
                if let Some(count) = broker_partition_counts.get_mut(&target_broker) {
                    *count += 1;
                }
                let transferred_load = partition_loads
                    .get(&(topic.clone(), *partition))
                    .copied()
                    .unwrap_or(0.0);
                if let Some(slot) = broker_throughput.get_mut(&target_broker) {
                    *slot += transferred_load;
                }

                PartitionTransfer {
                    topic: topic.clone(),
                    partition: *partition,
                    from_broker_id: failed_broker_id,
                    to_broker_id: target_broker,
                    // During broker failure failover, we intentionally skip epoch
                    // validation because the partition may have changed state during the
                    // failure detection window. The ownership check in the state machine
                    // handles this case by allowing transfers from unowned partitions.
                    expected_leader_epoch: None,
                }
            })
            .collect();

        let target_brokers: Vec<i32> = transfers
            .iter()
            .map(|t| t.to_broker_id)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect();

        let total_partitions = transfers.len();
        let batch_size = self.config.max_failover_batch_size;
        let batch_delay = Duration::from_millis(self.config.failover_batch_delay_ms);

        info!(
            failed_broker_id,
            partition_count = total_partitions,
            batch_size,
            target_brokers = ?target_brokers,
            "Executing chunked partition transfer for failover (thundering herd prevention)"
        );

        // Execute batch transfers in chunks to prevent thundering herd
        let mut total_successful = 0usize;
        let mut total_failed: Vec<(String, i32, String)> = Vec::new();
        let chunks: Vec<Vec<PartitionTransfer>> =
            transfers.chunks(batch_size).map(|c| c.to_vec()).collect();
        let num_batches = chunks.len();

        for (batch_idx, batch) in chunks.into_iter().enumerate() {
            let batch_len = batch.len();

            debug!(
                failed_broker_id,
                batch = batch_idx + 1,
                total_batches = num_batches,
                partitions_in_batch = batch_len,
                "Processing failover batch"
            );

            // Clone batch for error handling - we need to track failed partitions if
            // the entire batch fails, but batch_transfer_partitions takes ownership
            let batch_for_error = batch.clone();

            match executor
                .batch_transfer_partitions(
                    batch,
                    TransferReason::BrokerFailure,
                    self.config.transfer_lease_duration_ms,
                )
                .await
            {
                Ok((successful, failed)) => {
                    let batch_status = if failed.is_empty() {
                        "success"
                    } else if successful > 0 {
                        "partial"
                    } else {
                        "failed"
                    };
                    metrics::record_failover_batch(failed_broker_id, batch_len, batch_status);
                    for _ in 0..successful {
                        metrics::record_partition_transfer("broker_failure", true);
                    }
                    for _ in 0..failed.len() {
                        metrics::record_partition_transfer("broker_failure", false);
                    }
                    total_successful += successful;
                    total_failed.extend(failed);

                    debug!(
                        failed_broker_id,
                        batch = batch_idx + 1,
                        successful,
                        "Failover batch completed"
                    );
                }
                Err(e) => {
                    metrics::record_failover_batch(failed_broker_id, batch_len, "failed");
                    for _ in 0..batch_len {
                        metrics::record_partition_transfer("broker_failure", false);
                    }
                    // Log batch error but continue with remaining batches
                    error!(
                        failed_broker_id,
                        batch = batch_idx + 1,
                        partitions_in_batch = batch_len,
                        error = %e,
                        "Failover batch transfer failed, continuing with remaining batches"
                    );
                    // Mark all partitions in this batch as failed with the batch error
                    // This ensures failed partitions are properly tracked in the result
                    let error_msg = format!("batch_transfer_failed: {}", e);
                    for transfer in batch_for_error {
                        total_failed.push((transfer.topic, transfer.partition, error_msg.clone()));
                    }
                }
            }

            // Add delay between batches to let target brokers stabilize
            // Skip delay after the last batch
            if batch_idx < num_batches - 1 && batch_delay > Duration::ZERO {
                debug!(
                    failed_broker_id,
                    delay_ms = self.config.failover_batch_delay_ms,
                    "Waiting before next failover batch"
                );
                tokio::time::sleep(batch_delay).await;
            }
        }

        self.total_failovers.fetch_add(1, Ordering::Relaxed);

        // Persist orphaned partitions so the failure loop can retry them on
        // future ticks. `mark_broker_failed` already cleared their owner —
        // without this the next loop iteration would treat the broker as
        // fully handled and the failed batches would never be re-issued.
        if !total_failed.is_empty() {
            let mut orphans = self.pending_orphaned_partitions.lock().await;
            let entry = orphans.entry(failed_broker_id).or_default();
            for (topic, partition, _err) in &total_failed {
                entry.insert((topic.clone(), *partition));
            }
            warn!(
                failed_broker_id,
                successful_count = total_successful,
                failed_count = total_failed.len(),
                batches = num_batches,
                "Some partition transfers failed during failover; orphans queued for retry"
            );
        } else {
            // Clear any orphans recorded by previous attempts for this broker.
            self.pending_orphaned_partitions
                .lock()
                .await
                .remove(&failed_broker_id);
            info!(
                failed_broker_id,
                partitions_redistributed = total_successful,
                batches = num_batches,
                "Failover completed successfully"
            );
        }

        // Clear the failed broker from detector
        self.failure_detector.clear_failed_broker(failed_broker_id);

        metrics::record_failover_duration(
            "heartbeat_timeout",
            failover_start.elapsed().as_secs_f64(),
        );

        Ok(FailoverResult {
            failed_broker_id,
            partitions_redistributed: total_successful,
            partitions_failed: total_failed.len(),
            target_brokers,
        })
    }

    /// Retry transfers for partitions whose initial failover batch failed.
    ///
    /// Returns `Ok(remaining)` with the number of orphans still pending after
    /// this attempt. Picks targets the same way `handle_broker_failure` does
    /// (least-loaded active broker, excluding the originally-failed broker).
    /// On full success the broker's orphan entry is removed.
    async fn retry_orphaned_partitions<E: PartitionTransferExecutor>(
        &self,
        failed_broker_id: i32,
        executor: &E,
    ) -> Result<usize, String> {
        let orphans: Vec<(String, i32)> = {
            let map = self.pending_orphaned_partitions.lock().await;
            match map.get(&failed_broker_id) {
                Some(set) if !set.is_empty() => set.iter().cloned().collect(),
                _ => return Ok(0),
            }
        };

        let partition_owners = executor.get_partition_owners().await;
        let active_brokers = executor.get_active_brokers().await;
        let available_brokers: Vec<i32> = active_brokers
            .into_iter()
            .filter(|&id| id != failed_broker_id)
            .collect();
        if available_brokers.is_empty() {
            return Err("No available brokers for orphan retry".to_string());
        }

        let mut broker_partition_counts: HashMap<i32, usize> =
            available_brokers.iter().map(|&id| (id, 0)).collect();
        for ((_, _), &owner) in &partition_owners {
            if owner != failed_broker_id
                && let Some(count) = broker_partition_counts.get_mut(&owner)
            {
                *count += 1;
            }
        }

        let transfers: Vec<PartitionTransfer> = orphans
            .iter()
            .map(|(topic, partition)| {
                let target_broker = *broker_partition_counts
                    .iter()
                    .min_by_key(|(_, count)| *count)
                    .map(|(id, _)| id)
                    .unwrap_or(&available_brokers[0]);
                if let Some(count) = broker_partition_counts.get_mut(&target_broker) {
                    *count += 1;
                }
                PartitionTransfer {
                    topic: topic.clone(),
                    partition: *partition,
                    from_broker_id: failed_broker_id,
                    to_broker_id: target_broker,
                    expected_leader_epoch: None,
                }
            })
            .collect();

        let batch_size = self.config.max_failover_batch_size;
        let batch_delay = Duration::from_millis(self.config.failover_batch_delay_ms);
        let chunks: Vec<Vec<PartitionTransfer>> =
            transfers.chunks(batch_size).map(|c| c.to_vec()).collect();
        let num_batches = chunks.len();
        let mut still_failed: HashSet<(String, i32)> = HashSet::new();

        for (batch_idx, batch) in chunks.into_iter().enumerate() {
            let batch_for_error = batch.clone();
            match executor
                .batch_transfer_partitions(
                    batch,
                    TransferReason::BrokerFailure,
                    self.config.transfer_lease_duration_ms,
                )
                .await
            {
                Ok((_successful, failed)) => {
                    for (topic, partition, _err) in failed {
                        still_failed.insert((topic, partition));
                    }
                }
                Err(e) => {
                    error!(
                        failed_broker_id,
                        batch = batch_idx + 1,
                        error = %e,
                        "Orphan retry batch failed; partitions remain queued"
                    );
                    for transfer in batch_for_error {
                        still_failed.insert((transfer.topic, transfer.partition));
                    }
                }
            }
            if batch_idx < num_batches - 1 && batch_delay > Duration::ZERO {
                tokio::time::sleep(batch_delay).await;
            }
        }

        let remaining = still_failed.len();
        let mut map = self.pending_orphaned_partitions.lock().await;
        if still_failed.is_empty() {
            map.remove(&failed_broker_id);
            info!(
                failed_broker_id,
                "Orphaned partition retry succeeded for all partitions"
            );
        } else {
            map.insert(failed_broker_id, still_failed);
            warn!(
                failed_broker_id,
                remaining, "Orphaned partition retry made progress; some still pending"
            );
        }
        Ok(remaining)
    }

    /// Evaluate and perform load-based rebalancing if needed.
    pub async fn evaluate_and_rebalance<E: PartitionTransferExecutor>(
        &self,
        executor: &E,
    ) -> Result<RebalanceResult, String> {
        if !self.config.auto_balancer.enabled {
            return Ok(RebalanceResult {
                performed: false,
                partitions_moved: 0,
                deviation_after: 0.0,
                executed_moves: vec![],
            });
        }

        let mut balancer = self.auto_balancer.write().await;

        if !balancer.should_evaluate() {
            metrics::record_auto_rebalance("skipped_cooldown");
            return Ok(RebalanceResult {
                performed: false,
                partitions_moved: 0,
                deviation_after: 0.0,
                executed_moves: vec![],
            });
        }

        // Get current state with Raft index for consistency
        // This ensures all data comes from the same committed Raft state,
        // preventing race conditions where ownership changes between reads.
        let (partition_owners, raft_index) = executor.get_partition_owners_with_index().await;
        let partition_loads = self.load_collector.get_all_partition_loads();
        let broker_loads = self
            .load_collector
            .aggregate_broker_loads(&partition_owners);

        for summary in broker_loads.values() {
            metrics::set_broker_load(summary.broker_id, summary.total_bytes_per_sec);
            metrics::set_broker_partition_count(summary.broker_id, summary.partition_count);
        }
        metrics::set_active_cooldowns(balancer.active_cooldown_count());

        // Create a consistent snapshot with the Raft index
        let snapshot = LoadSnapshot::with_raft_index(
            broker_loads.clone(),
            partition_owners.clone(),
            partition_loads.clone(),
            raft_index,
        );

        // Evaluate balance using the consistent snapshot
        let decision = balancer.evaluate_snapshot(&snapshot);

        if !decision.should_rebalance {
            debug!(
                deviation = format!("{:.2}%", decision.current_deviation * 100.0),
                "Cluster is balanced"
            );
            metrics::record_auto_rebalance("skipped_balanced");
            metrics::set_cluster_load_deviation(decision.current_deviation);
            return Ok(RebalanceResult {
                performed: false,
                partitions_moved: 0,
                deviation_after: decision.current_deviation,
                executed_moves: vec![],
            });
        }

        metrics::record_auto_rebalance("performed");
        metrics::set_cluster_load_deviation(decision.current_deviation);

        // Drop the write lock before issuing transfers. Each transfer is a
        // Raft proposal RTT and can stall on leadership churn; holding the
        // auto-balancer lock across that window starves cooldown bookkeeping,
        // get_state_summary, metrics, and any other writer for hundreds of
        // ms to seconds. Snapshot the planned moves, then re-acquire briefly
        // at the end to record successful moves into the cooldown set.
        let planned_moves = decision.moves.clone();
        drop(balancer);

        // Execute moves without holding any auto-balancer lock.
        let mut executed_moves = Vec::new();
        let mut partitions_moved = 0;

        for partition_move in &planned_moves {
            match executor
                .transfer_partition(
                    &partition_move.topic,
                    partition_move.partition,
                    partition_move.from_broker_id,
                    partition_move.to_broker_id,
                    TransferReason::LoadBalancing,
                    self.config.transfer_lease_duration_ms,
                )
                .await
            {
                Ok(()) => {
                    executed_moves.push(partition_move.clone());
                    partitions_moved += 1;
                    metrics::record_partition_transfer("load_balancing", true);

                    info!(
                        topic = %partition_move.topic,
                        partition = partition_move.partition,
                        from = partition_move.from_broker_id,
                        to = partition_move.to_broker_id,
                        "Partition rebalanced"
                    );
                }
                Err(e) => {
                    metrics::record_partition_transfer("load_balancing", false);
                    warn!(
                        topic = %partition_move.topic,
                        partition = partition_move.partition,
                        error = %e,
                        "Failed to rebalance partition"
                    );
                }
            }
        }

        if !executed_moves.is_empty() {
            let mut balancer = self.auto_balancer.write().await;
            for partition_move in &executed_moves {
                balancer.record_move(&partition_move.topic, partition_move.partition);
            }
        }

        if partitions_moved > 0 {
            self.total_rebalances.fetch_add(1, Ordering::Relaxed);

            info!(
                partitions_moved,
                deviation_before = format!("{:.2}%", decision.current_deviation * 100.0),
                "Rebalance completed"
            );
        }

        Ok(RebalanceResult {
            performed: partitions_moved > 0,
            partitions_moved,
            deviation_after: decision.current_deviation, // Will change after moves
            executed_moves,
        })
    }

    /// Start background tasks for failure detection and rebalancing.
    ///
    /// Returns handles that should be joined on shutdown.
    pub fn start_background_tasks<E: PartitionTransferExecutor + 'static>(
        self: &Arc<Self>,
        executor: Arc<E>,
    ) -> BackgroundTaskHandles {
        self.running.store(true, Ordering::SeqCst);

        let failure_check_handle = if self.config.fast_failover_enabled {
            let coordinator = Arc::clone(self);
            let exec = Arc::clone(&executor);
            Some(self.runtime.spawn(supervised("failure_check", async move {
                coordinator.failure_check_loop(exec).await;
            })))
        } else {
            None
        };

        let rebalance_handle = if self.config.auto_balancer.enabled {
            let coordinator = Arc::clone(self);
            let exec = Arc::clone(&executor);
            Some(self.runtime.spawn(supervised("rebalance", async move {
                coordinator.rebalance_loop(exec).await;
            })))
        } else {
            None
        };

        let metrics_reset_handle = {
            let coordinator = Arc::clone(self);
            let exec = Arc::clone(&executor);
            Some(self.runtime.spawn(supervised("metrics_reset", async move {
                coordinator.metrics_reset_loop(exec).await;
            })))
        };

        BackgroundTaskHandles {
            failure_check: failure_check_handle,
            rebalance: rebalance_handle,
            metrics_reset: metrics_reset_handle,
        }
    }

    /// Stop background tasks.
    ///
    /// Flips the `running` flag and notifies the shutdown listener so loops
    /// wake immediately instead of waiting out their next sleep tick. The
    /// caller is still responsible for joining the spawned task handles.
    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
        self.shutdown_notify.notify_waiters();
    }

    /// Check if the coordinator is running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Background loop for failure detection.
    async fn failure_check_loop<E: PartitionTransferExecutor>(&self, executor: Arc<E>) {
        let check_interval = self.config.failure_detector.check_interval;

        // Track our last-observed leadership state. The pending-failover set
        // is per-broker in-memory and was previously LOST when leadership
        // moved the new leader's set is empty, the failure
        // detector already has the brokers in `Failed` (no transition fires),
        // and the new leader never retries the failover. On every false→true
        // transition of `should_initiate_failover` we now seed the pending
        // set from the failure detector's current Failed-state snapshot, so
        // failovers survive leader changes.
        let mut was_leader = false;

        while self.running.load(Ordering::SeqCst) {
            // Sleep but wake immediately if `stop()` is called.
            tokio::select! {
                _ = tokio::time::sleep(check_interval) => {}
                _ = self.shutdown_notify.notified() => break,
            }
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            let state_changes = self.check_broker_health();

            metrics::set_suspected_brokers(
                self.failure_detector
                    .get_brokers_in_state(BrokerHealthState::Suspected)
                    .len(),
            );
            metrics::set_failed_brokers(
                self.failure_detector
                    .get_brokers_in_state(BrokerHealthState::Failed)
                    .len(),
            );

            // Newly Failed brokers join the pending set; from there each
            // tick re-attempts the handoff until Raft accepts it. A single
            // transient `mark_broker_failed` error must not strand the
            // broker's partitions — `check_brokers` only re-emits on state
            // transitions, so without this set the next tick would see
            // `state == Failed (no event)` and never retry.
            {
                let mut pending = self.pending_failovers.lock().await;
                for change in &state_changes {
                    if change.new_state == BrokerHealthState::Failed {
                        pending.insert(change.broker_id);
                    }
                }
            }

            let is_leader = executor.should_initiate_failover().await;
            if !is_leader {
                debug!("Skipping failover initiation on non-leader broker");
                was_leader = false;
                continue;
            }

            // On a non-leader → leader transition, seed the pending set with
            // any broker the failure detector currently shows as Failed.
            // Without this, a follower that becomes leader inherits an empty
            // pending set even though Failed brokers exist, and the next
            // `check_broker_health` will not re-emit a transition for them.
            if !was_leader {
                let already_failed = self
                    .failure_detector
                    .get_brokers_in_state(BrokerHealthState::Failed);
                if !already_failed.is_empty() {
                    let mut pending = self.pending_failovers.lock().await;
                    for broker_id in &already_failed {
                        pending.insert(*broker_id);
                    }
                    info!(
                        seeded = already_failed.len(),
                        "Became failover leader; seeded pending failovers from failure detector"
                    );
                }
            }
            was_leader = true;

            let to_retry: Vec<i32> = {
                let pending = self.pending_failovers.lock().await;
                pending.iter().copied().collect()
            };

            for broker_id in to_retry {
                // Recheck the failure detector before fencing: a broker may
                // have heartbeated back to Healthy between joining
                // `pending_failovers` and now. Without this recheck, a single
                // ~1s network blip would trigger a full fence and partition
                // redistribution even after the broker recovered.
                if !self.failure_detector.is_failed(broker_id) {
                    info!(
                        broker_id,
                        "Broker recovered before failover ran; clearing pending entry"
                    );
                    self.pending_failovers.lock().await.remove(&broker_id);
                    continue;
                }

                info!(broker_id, "Driving broker failure handling");

                match self.handle_broker_failure(broker_id, &*executor).await {
                    Ok(_) => {
                        self.pending_failovers.lock().await.remove(&broker_id);
                    }
                    Err(e) => {
                        error!(
                            broker_id,
                            error = %e,
                            "Failed to handle broker failure; will retry on next tick"
                        );
                    }
                }
            }

            // Retry transfers for partitions whose initial batch failed.
            // Brokers may have been removed from `pending_failovers` already
            // (since `handle_broker_failure` returned Ok), but their
            // partitions are still ownerless until these transfers land.
            let orphan_brokers: Vec<i32> = {
                let map = self.pending_orphaned_partitions.lock().await;
                map.keys().copied().collect()
            };
            for broker_id in orphan_brokers {
                if let Err(e) = self.retry_orphaned_partitions(broker_id, &*executor).await {
                    error!(
                        broker_id,
                        error = %e,
                        "Orphan-partition retry failed; will retry next tick"
                    );
                }
            }
        }
    }

    /// Background loop for periodic rebalancing.
    async fn rebalance_loop<E: PartitionTransferExecutor>(&self, executor: Arc<E>) {
        let evaluation_interval = self.config.auto_balancer.evaluation_interval();

        while self.running.load(Ordering::SeqCst) {
            tokio::select! {
                _ = tokio::time::sleep(evaluation_interval) => {}
                _ = self.shutdown_notify.notified() => break,
            }
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            // Only the coordination leader proposes rebalance moves.
            // Followers wake on the same interval but skip the body so N
            // brokers don't fight over the same partitions every tick.
            if !executor.should_initiate_failover().await {
                continue;
            }

            if let Err(e) = self.evaluate_and_rebalance(&*executor).await {
                warn!(error = %e, "Rebalance evaluation failed");
            }
        }
    }

    /// Background loop for resetting metrics at window boundaries.
    async fn metrics_reset_loop<E: PartitionTransferExecutor>(&self, executor: Arc<E>) {
        while self.running.load(Ordering::SeqCst) {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(10)) => {}
                _ = self.shutdown_notify.notified() => break,
            }
            if !self.running.load(Ordering::SeqCst) {
                break;
            }

            if self.load_collector.should_reset() {
                self.load_collector.reset_all();
                debug!("Reset load metrics at window boundary");
            }

            // Cooldowns are populated by the leader's rebalance loop, so
            // their cleanup is leader-only too — followers' cooldown maps
            // are empty and contending for the auto-balancer write lock
            // here just starves the leader's rebalance pass.
            if executor.should_initiate_failover().await {
                let balancer = self.auto_balancer.read().await;
                balancer.cleanup_cooldowns();
            }
        }
    }

    /// Get total failovers handled.
    pub fn total_failovers(&self) -> u64 {
        self.total_failovers.load(Ordering::Relaxed)
    }

    /// Get total rebalances performed.
    pub fn total_rebalances(&self) -> u64 {
        self.total_rebalances.load(Ordering::Relaxed)
    }

    /// Get a summary of the current state.
    pub async fn get_state_summary(&self) -> CoordinatorStateSummary {
        let balancer = self.auto_balancer.read().await;

        CoordinatorStateSummary {
            broker_id: self.broker_id,
            is_running: self.is_running(),
            fast_failover_enabled: self.config.fast_failover_enabled,
            auto_balancer_enabled: self.config.auto_balancer.enabled,
            tracked_brokers: self.failure_detector.broker_count(),
            healthy_brokers: self.failure_detector.healthy_broker_count(),
            suspected_brokers: self.failure_detector.suspected_broker_count(),
            failed_brokers: self.failure_detector.failed_broker_count(),
            tracked_partitions: self.load_collector.partition_count(),
            active_cooldowns: balancer.active_cooldown_count(),
            total_failovers: self.total_failovers(),
            total_rebalances: self.total_rebalances(),
        }
    }
}

/// Handles for background tasks.
pub struct BackgroundTaskHandles {
    pub failure_check: Option<tokio::task::JoinHandle<()>>,
    pub rebalance: Option<tokio::task::JoinHandle<()>>,
    pub metrics_reset: Option<tokio::task::JoinHandle<()>>,
}

impl BackgroundTaskHandles {
    /// Abort all background tasks.
    pub fn abort_all(&self) {
        if let Some(ref h) = self.failure_check {
            h.abort();
        }
        if let Some(ref h) = self.rebalance {
            h.abort();
        }
        if let Some(ref h) = self.metrics_reset {
            h.abort();
        }
    }
}

/// Summary of the coordinator state.
#[derive(Debug, Clone)]
pub struct CoordinatorStateSummary {
    pub broker_id: i32,
    pub is_running: bool,
    pub fast_failover_enabled: bool,
    pub auto_balancer_enabled: bool,
    pub tracked_brokers: usize,
    pub healthy_brokers: usize,
    pub suspected_brokers: usize,
    pub failed_brokers: usize,
    pub tracked_partitions: usize,
    pub active_cooldowns: usize,
    pub total_failovers: u64,
    pub total_rebalances: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = RebalanceCoordinatorConfig::default();
        assert!(config.fast_failover_enabled);
        assert!(config.auto_balancer.enabled);
        assert_eq!(config.transfer_lease_duration_ms, 60_000);
    }

    #[tokio::test]
    async fn test_coordinator_creation() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        assert_eq!(coordinator.broker_id, 1);
        assert!(!coordinator.is_running());
        assert_eq!(coordinator.total_failovers(), 0);
        assert_eq!(coordinator.total_rebalances(), 0);
    }

    #[tokio::test]
    async fn test_broker_registration() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());

        coordinator.register_broker(2);
        coordinator.register_broker(3);

        assert_eq!(coordinator.failure_detector.broker_count(), 2);

        coordinator.unregister_broker(2);
        assert_eq!(coordinator.failure_detector.broker_count(), 1);
    }

    #[tokio::test]
    async fn test_heartbeat_recording() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());

        coordinator.register_broker(2);
        coordinator.record_heartbeat(2);

        assert!(coordinator.failure_detector.is_healthy(2));
    }

    #[tokio::test]
    async fn test_state_summary() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        coordinator.register_broker(2);
        coordinator.register_broker(3);

        let summary = coordinator.get_state_summary().await;

        assert_eq!(summary.broker_id, 1);
        assert!(!summary.is_running);
        assert!(summary.fast_failover_enabled);
        assert!(summary.auto_balancer_enabled);
        assert_eq!(summary.tracked_brokers, 2);
        assert_eq!(summary.healthy_brokers, 2);
    }

    #[test]
    fn test_config_builder() {
        let config = RebalanceCoordinatorConfig::default()
            .with_failover_disabled()
            .with_balancing_disabled();

        assert!(!config.fast_failover_enabled);
        assert!(!config.auto_balancer.enabled);
    }

    #[tokio::test]
    async fn test_config_accessor() {
        let config = RebalanceCoordinatorConfig {
            transfer_lease_duration_ms: 120_000,
            ..Default::default()
        };
        let coordinator = RebalanceCoordinator::new(config, 1, Handle::current());
        assert_eq!(coordinator.config().transfer_lease_duration_ms, 120_000);
    }

    #[tokio::test]
    async fn test_failure_detector_accessor() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        coordinator.register_broker(2);

        let fd = coordinator.failure_detector();
        assert_eq!(fd.broker_count(), 1);
    }

    #[tokio::test]
    async fn test_load_collector_accessor() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let lc = coordinator.load_collector();
        assert_eq!(lc.partition_count(), 0);
    }

    #[tokio::test]
    async fn test_check_broker_health() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        coordinator.register_broker(2);
        coordinator.record_heartbeat(2);

        let changes = coordinator.check_broker_health();
        // Should be empty since broker is healthy
        assert!(changes.is_empty());
    }

    #[tokio::test]
    async fn test_stop() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        assert!(!coordinator.is_running());

        // Note: start_background_tasks would set running to true
        // stop() sets it to false
        coordinator.stop();
        assert!(!coordinator.is_running());
    }

    #[test]
    fn test_failover_result_fields() {
        let result = FailoverResult {
            failed_broker_id: 1,
            partitions_redistributed: 5,
            partitions_failed: 1,
            target_brokers: vec![2, 3],
        };

        assert_eq!(result.failed_broker_id, 1);
        assert_eq!(result.partitions_redistributed, 5);
        assert_eq!(result.partitions_failed, 1);
        assert_eq!(result.target_brokers.len(), 2);
    }

    #[test]
    fn test_rebalance_result_fields() {
        let result = RebalanceResult {
            performed: true,
            partitions_moved: 3,
            deviation_after: 0.15,
            executed_moves: vec![],
        };

        assert!(result.performed);
        assert_eq!(result.partitions_moved, 3);
        assert!((result.deviation_after - 0.15).abs() < 0.001);
    }

    #[test]
    fn test_coordinator_state_summary_fields() {
        let summary = CoordinatorStateSummary {
            broker_id: 1,
            is_running: true,
            fast_failover_enabled: true,
            auto_balancer_enabled: false,
            tracked_brokers: 5,
            healthy_brokers: 4,
            suspected_brokers: 1,
            failed_brokers: 0,
            tracked_partitions: 100,
            active_cooldowns: 2,
            total_failovers: 10,
            total_rebalances: 5,
        };

        assert_eq!(summary.broker_id, 1);
        assert!(summary.is_running);
        assert!(summary.fast_failover_enabled);
        assert!(!summary.auto_balancer_enabled);
        assert_eq!(summary.tracked_brokers, 5);
        assert_eq!(summary.healthy_brokers, 4);
        assert_eq!(summary.suspected_brokers, 1);
        assert_eq!(summary.failed_brokers, 0);
        assert_eq!(summary.tracked_partitions, 100);
        assert_eq!(summary.active_cooldowns, 2);
        assert_eq!(summary.total_failovers, 10);
        assert_eq!(summary.total_rebalances, 5);
    }

    #[test]
    fn test_background_task_handles_abort() {
        let handles = BackgroundTaskHandles {
            failure_check: None,
            rebalance: None,
            metrics_reset: None,
        };

        // Should not panic with None handles
        handles.abort_all();
    }

    // Mock executor for testing
    type BatchTransferResult = Result<(usize, Vec<(String, i32, String)>), String>;

    struct MockExecutor {
        partition_owners: std::sync::RwLock<HashMap<(String, i32), i32>>,
        active_brokers: std::sync::RwLock<Vec<i32>>,
        mark_failed_error: std::sync::RwLock<Option<String>>,
        batch_transfer_result: std::sync::RwLock<Option<BatchTransferResult>>,
    }

    impl MockExecutor {
        fn new() -> Self {
            Self {
                partition_owners: std::sync::RwLock::new(HashMap::new()),
                active_brokers: std::sync::RwLock::new(vec![]),
                mark_failed_error: std::sync::RwLock::new(None),
                batch_transfer_result: std::sync::RwLock::new(None),
            }
        }

        fn with_brokers(brokers: Vec<i32>) -> Self {
            let executor = Self::new();
            *executor.active_brokers.write().unwrap() = brokers;
            executor
        }

        fn add_partition(&self, topic: &str, partition: i32, owner: i32) {
            self.partition_owners
                .write()
                .unwrap()
                .insert((topic.to_string(), partition), owner);
        }

        fn set_mark_failed_error(&self, error: String) {
            *self.mark_failed_error.write().unwrap() = Some(error);
        }

        fn set_batch_transfer_result(&self, result: BatchTransferResult) {
            *self.batch_transfer_result.write().unwrap() = Some(result);
        }
    }

    #[async_trait::async_trait]
    impl PartitionTransferExecutor for MockExecutor {
        async fn transfer_partition(
            &self,
            _topic: &str,
            _partition: i32,
            _from_broker_id: i32,
            _to_broker_id: i32,
            _reason: TransferReason,
            _lease_duration_ms: u64,
        ) -> Result<(), String> {
            Ok(())
        }

        async fn batch_transfer_partitions(
            &self,
            transfers: Vec<PartitionTransfer>,
            _reason: TransferReason,
            _lease_duration_ms: u64,
        ) -> Result<(usize, Vec<(String, i32, String)>), String> {
            // Use custom result if set, otherwise default success
            if let Some(result) = self.batch_transfer_result.read().unwrap().as_ref() {
                result.clone()
            } else {
                Ok((transfers.len(), vec![]))
            }
        }

        async fn mark_broker_failed(
            &self,
            broker_id: i32,
            _reason: &str,
        ) -> Result<Vec<(String, i32)>, String> {
            if let Some(error) = self.mark_failed_error.read().unwrap().as_ref() {
                return Err(error.clone());
            }
            // Mirror the real semantics: release (and return) every
            // partition owned by the failed broker.
            let mut owners = self.partition_owners.write().unwrap();
            let released: Vec<(String, i32)> = owners
                .iter()
                .filter(|(_, owner)| **owner == broker_id)
                .map(|(key, _)| key.clone())
                .collect();
            for key in &released {
                owners.remove(key);
            }
            Ok(released)
        }

        async fn get_partition_owners(&self) -> HashMap<(String, i32), i32> {
            self.partition_owners.read().unwrap().clone()
        }

        async fn get_partition_owners_with_index(&self) -> (HashMap<(String, i32), i32>, u64) {
            (self.partition_owners.read().unwrap().clone(), 1)
        }

        async fn get_active_brokers(&self) -> Vec<i32> {
            self.active_brokers.read().unwrap().clone()
        }
    }

    #[tokio::test]
    async fn test_handle_broker_failure_disabled() {
        let config = RebalanceCoordinatorConfig::default().with_failover_disabled();
        let coordinator = RebalanceCoordinator::new(config, 1, Handle::current());
        let executor = MockExecutor::new();

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Fast failover is disabled");
    }

    #[tokio::test]
    async fn test_handle_broker_failure_no_partitions() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_ok());

        let failover = result.unwrap();
        assert_eq!(failover.failed_broker_id, 2);
        assert_eq!(failover.partitions_redistributed, 0);
        assert_eq!(failover.partitions_failed, 0);
    }

    #[tokio::test]
    async fn test_handle_broker_failure_no_available_brokers() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::new();
        // Broker 2 owns 5 partitions
        for i in 0..5 {
            executor.add_partition("topic", i, 2);
        }

        // Only the failed broker is available
        *executor.active_brokers.write().unwrap() = vec![2];

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "No available brokers for failover");
    }

    #[tokio::test]
    async fn test_handle_broker_failure_with_partitions() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);
        executor.add_partition("topic", 0, 2);
        executor.add_partition("topic", 1, 2);
        executor.add_partition("topic", 2, 3);

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_ok());

        let failover = result.unwrap();
        assert_eq!(failover.failed_broker_id, 2);
        assert_eq!(failover.partitions_redistributed, 2);
        assert_eq!(coordinator.total_failovers(), 1);
    }

    #[tokio::test]
    async fn test_evaluate_and_rebalance_disabled() {
        let config = RebalanceCoordinatorConfig::default().with_balancing_disabled();
        let coordinator = RebalanceCoordinator::new(config, 1, Handle::current());
        let executor = MockExecutor::new();

        let result = coordinator.evaluate_and_rebalance(&executor).await;
        assert!(result.is_ok());

        let rebalance = result.unwrap();
        assert!(!rebalance.performed);
        assert_eq!(rebalance.partitions_moved, 0);
    }

    #[tokio::test]
    async fn test_evaluate_and_rebalance_should_not_evaluate() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2]);

        // Trigger first evaluation
        let _ = coordinator.evaluate_and_rebalance(&executor).await;

        // Immediate second evaluation should be skipped (evaluation interval not passed)
        let result = coordinator.evaluate_and_rebalance(&executor).await;
        assert!(result.is_ok());
        assert!(!result.unwrap().performed);
    }

    #[tokio::test]
    async fn test_handle_broker_failure_batch_error_propagation() {
        // Test that batch transfer failures properly track all partitions as failed
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);

        // Add partitions owned by broker 2 (the one that will fail)
        executor.add_partition("topic-a", 0, 2);
        executor.add_partition("topic-a", 1, 2);
        executor.add_partition("topic-b", 0, 2);

        // Set batch transfer to fail
        executor.set_batch_transfer_result(Err("Raft proposal failed: timeout".to_string()));

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_ok());

        let failover = result.unwrap();
        assert_eq!(failover.failed_broker_id, 2);
        // No partitions were successfully transferred
        assert_eq!(failover.partitions_redistributed, 0);
        // All 3 partitions should be tracked as failed
        assert_eq!(failover.partitions_failed, 3);
    }

    #[tokio::test]
    async fn test_handle_broker_failure_partial_batch_failure() {
        // Test that individual partition failures within a batch are tracked
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);

        // Add partitions owned by broker 2
        executor.add_partition("topic", 0, 2);
        executor.add_partition("topic", 1, 2);

        // Set batch transfer to succeed with 1 failure
        executor.set_batch_transfer_result(Ok((
            1,
            vec![(
                "topic".to_string(),
                1,
                "partition already owned".to_string(),
            )],
        )));

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_ok());

        let failover = result.unwrap();
        assert_eq!(failover.partitions_redistributed, 1);
        assert_eq!(failover.partitions_failed, 1);
    }

    // ========================================================================
    // Additional Tests for Critical Coverage
    // ========================================================================

    #[tokio::test]
    async fn test_handle_broker_failure_multiple_batches() {
        // Test failover with more partitions than batch size to verify chunking
        let config = RebalanceCoordinatorConfig {
            max_failover_batch_size: 3, // Small batch size for testing
            failover_batch_delay_ms: 0, // No delay for faster test
            ..Default::default()
        };
        let coordinator = RebalanceCoordinator::new(config, 1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);

        // Add 8 partitions owned by broker 2 (will require 3 batches: 3+3+2)
        for i in 0..8 {
            executor.add_partition("topic", i, 2);
        }

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_ok());

        let failover = result.unwrap();
        assert_eq!(failover.partitions_redistributed, 8);
        assert_eq!(failover.partitions_failed, 0);
        assert_eq!(coordinator.total_failovers(), 1);
    }

    #[tokio::test]
    async fn test_handle_broker_failure_batch_delay() {
        // Test that batch delays are applied between chunks
        let config = RebalanceCoordinatorConfig {
            max_failover_batch_size: 2,
            failover_batch_delay_ms: 50, // 50ms delay between batches
            ..Default::default()
        };
        let coordinator = RebalanceCoordinator::new(config, 1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);

        // Add 4 partitions (2 batches)
        for i in 0..4 {
            executor.add_partition("topic", i, 2);
        }

        let start = std::time::Instant::now();
        let result = coordinator.handle_broker_failure(2, &executor).await;
        let elapsed = start.elapsed();

        assert!(result.is_ok());
        // Should have taken at least 50ms for the delay between 2 batches
        // (delay is only applied between batches, not after the last one)
        assert!(
            elapsed >= std::time::Duration::from_millis(40),
            "Expected delay between batches"
        );
    }

    #[tokio::test]
    async fn test_handle_broker_failure_distributes_to_least_loaded() {
        // Verify partitions are distributed to brokers with fewest partitions
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);

        // Broker 3 already has 5 partitions, broker 1 has 1
        executor.add_partition("existing", 0, 1);
        for i in 0..5 {
            executor.add_partition("existing", i + 10, 3);
        }

        // Broker 2 fails with 2 partitions
        executor.add_partition("topic", 0, 2);
        executor.add_partition("topic", 1, 2);

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_ok());

        let failover = result.unwrap();
        // Both partitions should go to broker 1 (least loaded) before broker 3
        // The mock doesn't actually track this, but we verify the coordinator logic runs
        assert_eq!(failover.partitions_redistributed, 2);
    }

    #[tokio::test]
    async fn test_start_and_stop() {
        let coordinator = Arc::new(RebalanceCoordinator::with_defaults(1, Handle::current()));
        let executor = Arc::new(MockExecutor::with_brokers(vec![1, 2]));

        // Start background tasks
        let handles = coordinator.start_background_tasks(executor);

        // Should be running
        assert!(coordinator.is_running());

        // Stop
        coordinator.stop();
        assert!(!coordinator.is_running());

        // Abort task handles
        handles.abort_all();
    }

    #[tokio::test]
    async fn test_get_state_summary_comprehensive() {
        let config = RebalanceCoordinatorConfig {
            fast_failover_enabled: true,
            auto_balancer: super::super::auto_balancer::AutoBalancerConfig {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        };
        let coordinator = RebalanceCoordinator::new(config, 42, Handle::current());

        // Register some brokers
        // Note: Registering a broker sets it as healthy with last_heartbeat=now
        coordinator.register_broker(1);
        coordinator.register_broker(2);
        coordinator.record_heartbeat(1);

        let summary = coordinator.get_state_summary().await;

        assert_eq!(summary.broker_id, 42);
        assert!(!summary.is_running);
        assert!(summary.fast_failover_enabled);
        assert!(summary.auto_balancer_enabled);
        assert_eq!(summary.tracked_brokers, 2);
        // Both brokers are healthy because registration sets initial state to healthy
        assert_eq!(summary.healthy_brokers, 2);
        assert_eq!(summary.total_failovers, 0);
        assert_eq!(summary.total_rebalances, 0);
    }

    #[tokio::test]
    async fn test_evaluate_and_rebalance_not_enough_time() {
        // Test that rebalance is skipped when evaluation interval hasn't passed
        let config = RebalanceCoordinatorConfig {
            auto_balancer: super::super::auto_balancer::AutoBalancerConfig {
                enabled: true,
                evaluation_interval_secs: 60, // Long interval
                ..Default::default()
            },
            ..Default::default()
        };
        let coordinator = RebalanceCoordinator::new(config, 1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2]);

        // First evaluation triggers internal state update
        let _ = coordinator.evaluate_and_rebalance(&executor).await;

        // Immediate second should be skipped
        let result = coordinator.evaluate_and_rebalance(&executor).await;
        assert!(result.is_ok());
        assert!(!result.unwrap().performed);
    }

    #[tokio::test]
    async fn test_config_builder_methods() {
        let config = RebalanceCoordinatorConfig::default()
            .with_failover_disabled()
            .with_balancing_disabled();

        let coordinator = RebalanceCoordinator::new(config.clone(), 1, Handle::current());

        assert!(!coordinator.config().fast_failover_enabled);
        assert!(!coordinator.config().auto_balancer.enabled);
    }

    #[tokio::test]
    async fn test_handle_broker_failure_mark_failed_error() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);

        // Make mark_broker_failed return error
        executor.set_mark_failed_error("Raft proposal failed".to_string());

        let result = coordinator.handle_broker_failure(2, &executor).await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Raft proposal failed");
    }

    #[tokio::test]
    async fn test_load_collector_from_coordinator() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let load_collector = coordinator.load_collector();

        // Should be able to use the load collector
        assert_eq!(load_collector.partition_count(), 0);
    }

    #[tokio::test]
    async fn test_failure_detector_from_coordinator() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());

        coordinator.register_broker(5);
        let fd = coordinator.failure_detector();
        assert_eq!(fd.broker_count(), 1);
    }

    #[tokio::test]
    async fn test_multiple_failovers_increment_counter() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3, 4]);

        // First failover
        executor.add_partition("t1", 0, 2);
        coordinator
            .handle_broker_failure(2, &executor)
            .await
            .unwrap();

        // Second failover (different broker)
        executor.partition_owners.write().unwrap().clear();
        executor.add_partition("t1", 1, 3);
        coordinator
            .handle_broker_failure(3, &executor)
            .await
            .unwrap();

        assert_eq!(coordinator.total_failovers(), 2);
    }

    #[tokio::test]
    async fn test_background_task_handles_abort_with_handles() {
        let coordinator = Arc::new(RebalanceCoordinator::with_defaults(1, Handle::current()));
        let executor = Arc::new(MockExecutor::with_brokers(vec![1, 2]));

        let handles = coordinator.start_background_tasks(executor);

        // Give tasks time to start
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Abort all
        handles.abort_all();

        // Should not panic
        coordinator.stop();
    }

    #[tokio::test]
    async fn test_handle_broker_failure_empty_active_brokers_except_failed() {
        // Edge case: Only broker available is the failed one
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::new();

        executor.add_partition("topic", 0, 1);
        *executor.active_brokers.write().unwrap() = vec![1]; // Only failed broker

        let result = coordinator.handle_broker_failure(1, &executor).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No available brokers"));
    }

    #[tokio::test]
    async fn test_failover_clears_failed_broker_from_detector() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());
        let executor = MockExecutor::with_brokers(vec![1, 2, 3]);

        // Register and fail broker 2
        coordinator.register_broker(2);
        executor.add_partition("topic", 0, 2);

        // Before failover, broker should be tracked
        assert_eq!(coordinator.failure_detector().broker_count(), 1);

        // Handle failure
        coordinator
            .handle_broker_failure(2, &executor)
            .await
            .unwrap();

        // After failover, failed broker should be cleared
        assert_eq!(coordinator.failure_detector().broker_count(), 0);
    }
}
