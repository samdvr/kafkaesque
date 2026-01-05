//! Raft-based coordinator implementing ClusterCoordinator traits.
//!
//! This module provides a coordinator backed by the Raft consensus layer,
//! implementing all the traits needed for cluster coordination.
//!
//! # Module Organization
//!
//! The coordinator is decomposed into focused modules:
//!
//! - [`partition`]: Partition ownership and topic metadata
//! - [`groups`]: Consumer group coordination
//! - [`producer`]: Producer ID allocation and epoch tracking
//! - [`transfer`]: Partition transfer coordination
//!
//! Each module implements a single trait for better separation of concerns.

mod groups;
mod partition;
mod producer;
mod state_accessor;
mod transfer;

use std::sync::Arc;
use std::time::Duration;

use moka::future::Cache;
use object_store::ObjectStore;
use tokio::runtime::Handle;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::commands::CoordinationCommand;
use super::config::RaftConfig;
use super::node::RaftNode;

use crate::cluster::PartitionKey;
use crate::cluster::coordinator::BrokerInfo;
use crate::cluster::error::{SlateDBError, SlateDBResult};

pub use state_accessor::StateAccessor;

/// Raft-based coordinator for cluster state.
pub struct RaftCoordinator {
    /// The underlying Raft node.
    node: Arc<RaftNode>,
    /// This broker's ID.
    broker_id: i32,
    /// Broker info for this node.
    broker_info: BrokerInfo,
    /// Cache of partition owners for fast local lookups.
    owner_cache: Cache<PartitionKey, i32>,
    /// Configuration.
    config: RaftConfig,
    /// Background task handles.
    task_handles: RwLock<Vec<tokio::task::JoinHandle<()>>>,
    /// Shutdown signal.
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    /// Atomic counter for generating unique transaction IDs.
    transaction_counter: std::sync::atomic::AtomicU64,
    /// Runtime handle for spawning control plane tasks.
    runtime: Handle,
}

impl RaftCoordinator {
    /// Create a new Raft coordinator.
    ///
    /// # Arguments
    /// * `config` - Raft configuration
    /// * `object_store` - Object store for durable snapshot persistence
    /// * `runtime` - Runtime handle for spawning control plane tasks
    pub async fn new(
        config: RaftConfig,
        object_store: Arc<dyn ObjectStore>,
        runtime: Handle,
    ) -> SlateDBResult<Self> {
        let node = RaftNode::new(config.clone(), object_store, runtime.clone()).await?;
        let node = Arc::new(node);

        let broker_info = BrokerInfo {
            broker_id: config.broker_id,
            host: config.host.clone(),
            port: config.port,
            registered_at: chrono::Utc::now().timestamp_millis(),
        };

        // Create ownership cache
        let owner_cache = Cache::builder()
            .time_to_live(Duration::from_secs(1))
            .max_capacity(100_000)
            .build();

        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);

        let coordinator = Self {
            node,
            broker_id: config.broker_id,
            broker_info,
            owner_cache,
            config,
            task_handles: RwLock::new(Vec::new()),
            shutdown_tx,
            transaction_counter: std::sync::atomic::AtomicU64::new(0),
            runtime,
        };

        Ok(coordinator)
    }

    /// Get a state accessor for reading/writing coordinator state.
    #[inline]
    pub fn state_accessor(&self) -> StateAccessor<'_> {
        StateAccessor::new(&self.node)
    }

    /// Get the underlying Raft node.
    pub fn node(&self) -> &Arc<RaftNode> {
        &self.node
    }

    /// Initialize the cluster (call on first node only).
    pub async fn initialize_cluster(&self) -> SlateDBResult<()> {
        self.node.initialize_cluster().await
    }

    /// Check if the cluster is already initialized.
    ///
    /// Returns true if the cluster has existing membership from a restored
    /// snapshot or previous initialization. This should be checked before
    /// calling `initialize_cluster()` to avoid re-initialization errors on restart.
    pub fn is_initialized(&self) -> bool {
        self.node.is_initialized()
    }

    /// Start background tasks.
    pub async fn start_background_tasks(&self) {
        let node = self.node.clone();
        let broker_id = self.broker_id;
        let shutdown_tx = self.shutdown_tx.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let heartbeat_interval = self.config.broker_heartbeat_interval;

        // Broker heartbeat task with jitter to prevent thundering herd
        let handle = self.runtime.spawn(async move {
            // Add initial jitter (0-50% of interval) to stagger startup
            let initial_jitter = jitter_duration(heartbeat_interval, 0.5);
            tokio::time::sleep(initial_jitter).await;

            let mut interval = tokio::time::interval(heartbeat_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Add per-tick jitter (0-10% of interval) to prevent sync
                        let tick_jitter = jitter_duration(heartbeat_interval, 0.1);
                        tokio::time::sleep(tick_jitter).await;

                        let now = current_time_ms();
                        let command = CoordinationCommand::BrokerDomain(
                            super::domains::BrokerCommand::Heartbeat {
                                broker_id,
                                timestamp_ms: now,
                                reported_local_timestamp_ms: now,
                            }
                        );
                        if let Err(e) = node.write(command).await {
                            warn!(error = %e, "Failed to send broker heartbeat");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
        self.task_handles.write().await.push(handle);

        // Lease expiration task with jitter
        let node = self.node.clone();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let check_interval = Duration::from_secs(5);

        let handle = self.runtime.spawn(async move {
            // Add initial jitter (0-100% of interval) to stagger across brokers
            let initial_jitter = jitter_duration(check_interval, 1.0);
            tokio::time::sleep(initial_jitter).await;

            let mut interval = tokio::time::interval(check_interval);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        // Add per-tick jitter (0-20% of interval)
                        let tick_jitter = jitter_duration(check_interval, 0.2);
                        tokio::time::sleep(tick_jitter).await;

                        let now = current_time_ms();
                        let command = CoordinationCommand::PartitionDomain(
                            super::domains::PartitionCommand::ExpireLeases { current_time_ms: now }
                        );
                        if let Err(e) = node.write(command).await {
                            warn!(error = %e, "Failed to expire leases");
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        break;
                    }
                }
            }
        });
        self.task_handles.write().await.push(handle);
    }

    /// Shutdown the coordinator.
    pub async fn shutdown(&self) -> SlateDBResult<()> {
        info!(broker_id = self.broker_id, "Shutting down Raft coordinator");

        // Signal shutdown to all background tasks
        let _ = self.shutdown_tx.send(());

        // Wait for background tasks to finish
        let mut handles = self.task_handles.write().await;
        for handle in handles.drain(..) {
            let _ = handle.await;
        }

        debug!(
            broker_id = self.broker_id,
            "Raft coordinator shutdown complete"
        );
        Ok(())
    }

    /// Join a Raft cluster.
    ///
    /// This initiates the cluster join process by contacting the leader
    /// and requesting to be added as a learner, then promoted to voter.
    pub async fn join_cluster(&self, leader_addr: &str) -> SlateDBResult<()> {
        use super::network::request_cluster_join;
        request_cluster_join(leader_addr, self.broker_id as u64, &self.config.raft_addr)
            .await
            .map_err(|e| SlateDBError::Storage(format!("Failed to join cluster: {}", e)))
    }

    /// Get the current Raft leader.
    pub async fn get_leader(&self) -> Option<u64> {
        self.node.current_leader().await
    }

    /// Check if this node is the Raft leader.
    pub async fn is_leader(&self) -> bool {
        self.node.is_leader().await
    }

    /// Get Raft metrics.
    pub fn metrics(&self) -> openraft::RaftMetrics<super::types::RaftNodeId, openraft::BasicNode> {
        self.node.metrics()
    }

    /// Wait for leader election to complete.
    pub async fn wait_for_leader(&self) -> SlateDBResult<()> {
        self.wait_for_leader_with_timeout(Duration::from_secs(30))
            .await
    }

    /// Wait for leader election with a custom timeout.
    pub async fn wait_for_leader_with_timeout(&self, timeout: Duration) -> SlateDBResult<()> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(100);

        loop {
            if self.node.current_leader().await.is_some() {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(SlateDBError::Storage(
                    "Timeout waiting for Raft leader election".to_string(),
                ));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

/// Generate a random jitter duration.
///
/// Returns a duration uniformly distributed between 0 and `base * factor`.
/// Uses fastrand for better entropy distribution.
fn jitter_duration(base: Duration, factor: f64) -> Duration {
    let max_jitter_ms = (base.as_millis() as f64 * factor) as u64;
    if max_jitter_ms == 0 {
        return Duration::ZERO;
    }
    let jitter_ms = fastrand::u64(0..=max_jitter_ms);
    Duration::from_millis(jitter_ms)
}

/// Get current time in milliseconds since UNIX epoch.
pub(crate) fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_jitter_is_bounded() {
        let base = Duration::from_secs(10);
        let factor = 0.5; // max 5 seconds

        let mut max_seen = Duration::ZERO;
        for _ in 0..100 {
            let jitter = jitter_duration(base, factor);
            assert!(jitter <= Duration::from_secs(5), "Jitter exceeded maximum");
            if jitter > max_seen {
                max_seen = jitter;
            }
        }

        // Should have some variation
        assert!(
            max_seen > Duration::ZERO,
            "Should see non-zero jitter values"
        );
    }

    #[test]
    fn test_current_time_ms_is_reasonable() {
        let now = current_time_ms();
        // Should be after 2020-01-01 (1577836800000 ms since epoch)
        assert!(now > 1577836800000);
        // Should be before 2100-01-01
        assert!(now < 4102444800000);
    }
}
