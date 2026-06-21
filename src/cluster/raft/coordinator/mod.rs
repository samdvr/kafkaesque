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
//!
//! # Multi-group routing
//!
//! Each method routes by key:
//!
//! - **Control** group: broker registry, ACLs, topic registry, producer-id
//!   allocation. `cluster.write_control(...)` / `cluster.control()`.
//! - **Shard** group (`hash(key) % metadata_shards`): per-partition leases,
//!   consumer groups, per-producer idempotency, transfers.
//!   `cluster.write_shard_for_topic(...)` / `cluster.shard_for_topic(...)`.

mod acl;
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

use super::cluster::RaftCluster;
use super::commands::{ControlCommand, ShardCommand};
use super::config::RaftConfig;
use super::domains::{BrokerCommand, PartitionStateCommand};
use super::reconciler;

use crate::cluster::PartitionKey;
use crate::cluster::coordinator::BrokerInfo;
use crate::cluster::error::{SlateDBError, SlateDBResult};

pub use state_accessor::StateAccessor;

/// Cached partition owner with the generation stamps that were live at insert
/// time. Both `key_gen` and `global_gen` must equal the *current* values for
/// the entry to be served — any per-key invalidation OR a bulk invalidate-all
/// renders the entry stale without having to evict it from moka synchronously.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct CachedOwnerEntry {
    broker_id: i32,
    /// Per-key tombstone the entry was inserted under.
    key_gen: u64,
    /// Global generation the entry was inserted under (bumped only by
    /// `owner_cache_invalidate_all`, which is rare — leadership change,
    /// snapshot install).
    global_gen: u64,
}

/// Snapshot of the (`key_gen`, `global_gen`) pair taken at the start of a
/// linearizable read, validated again before the cache insert. If the
/// generation has advanced between the snapshot and the insert, an
/// invalidation hook fired during the read window and the value we computed
/// is potentially stale; the insert is dropped to avoid landing a stale
/// entry as fresh.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct OwnerCacheReadStamp {
    key_gen: u64,
    global_gen: u64,
}

/// Raft-based coordinator for cluster state.
pub struct RaftCoordinator {
    /// The underlying multi-group Raft cluster (one control + N shards).
    cluster: Arc<RaftCluster>,
    /// This broker's ID.
    broker_id: i32,
    /// Broker info for this node.
    broker_info: BrokerInfo,
    /// Cache of partition owners for fast local lookups.
    owner_cache: Cache<PartitionKey, CachedOwnerEntry>,
    /// Per-key invalidation tombstones. Bumped synchronously by ownership-
    /// change hooks so concurrent readers reject the cached entry without
    /// waiting for moka's async eviction. Only entries for the changed key
    /// are invalidated (this used to be a global counter that blew the whole
    /// cache on every ownership change — a thundering herd through Raft on
    /// every lease renewal/release).
    owner_cache_tombstones: Arc<dashmap::DashMap<PartitionKey, u64>>,
    /// Bulk-invalidate stamp. Bumped only by `owner_cache_invalidate_all`
    /// (leadership change, snapshot install) — never on per-key paths.
    owner_cache_global_generation: std::sync::atomic::AtomicU64,
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
    /// Soft heartbeat state for consumer groups: per-(group, member) the
    /// last time we issued a Raft proposal. Used to coalesce frequent
    /// client heartbeats (every 3s by default) into one durable bump every
    /// `default_session_timeout_ms / HEARTBEAT_RAFT_DUTY_DIVISOR`. Without
    /// this, a 10k-consumer cluster issues ~3 333 commits/s on heartbeats
    /// alone — the dominant Raft load.
    heartbeat_propose_state: dashmap::DashMap<(String, String), std::time::Instant>,
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
        let cluster = RaftCluster::new(config.clone(), object_store, runtime.clone()).await?;

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
            cluster,
            broker_id: config.broker_id,
            broker_info,
            owner_cache,
            owner_cache_tombstones: Arc::new(dashmap::DashMap::new()),
            owner_cache_global_generation: std::sync::atomic::AtomicU64::new(0),
            config,
            task_handles: RwLock::new(Vec::new()),
            shutdown_tx,
            transaction_counter: std::sync::atomic::AtomicU64::new(0),
            runtime,
            heartbeat_propose_state: dashmap::DashMap::new(),
        };

        Ok(coordinator)
    }

    /// Get a state accessor for reading/writing coordinator state.
    #[inline]
    pub fn state_accessor(&self) -> StateAccessor<'_> {
        StateAccessor::new(&self.cluster)
    }

    /// Get the underlying multi-group Raft cluster.
    pub fn cluster(&self) -> &Arc<RaftCluster> {
        &self.cluster
    }

    /// Initialize the cluster (call on first node only). Initializes the
    /// control group AND every shard in one shot — each `Raft::initialize`
    /// is idempotent so a partial failure mid-fan-out is recoverable on
    /// retry.
    pub async fn initialize_cluster(&self) -> SlateDBResult<()> {
        self.cluster.initialize_cluster().await
    }

    /// Check if the cluster is already initialized.
    ///
    /// "Initialized" means the **control** group has membership. The shard
    /// groups follow control's bootstrap by way of [`Self::initialize_cluster`];
    /// if control has voters and shards don't (e.g. an interrupted bootstrap),
    /// `initialize_cluster` is the recovery path — it's idempotent on the
    /// already-initialized control group and finishes whichever shards are
    /// still missing membership.
    pub fn is_initialized(&self) -> bool {
        self.cluster.control().is_initialized()
    }

    /// Start background tasks.
    pub async fn start_background_tasks(&self) {
        let cluster = self.cluster.clone();
        let broker_id = self.broker_id;
        let shutdown_tx = self.shutdown_tx.clone();
        let mut shutdown_rx = shutdown_tx.subscribe();
        let heartbeat_interval = self.config.broker_heartbeat_interval;

        // Broker heartbeat task with jitter to prevent thundering herd. The
        // broker registry is cluster-wide state — heartbeats target the
        // control group.
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
                        let command = ControlCommand::Broker(BrokerCommand::Heartbeat {
                            broker_id,
                            timestamp_ms: now,
                            reported_local_timestamp_ms: now,
                        });
                        if let Err(e) = cluster.write_control(command).await {
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

        // Lease expiration sweep, fanned out one task per shard. Each task
        // independently checks `shard.is_leader(broker_id)` and proposes
        // `ShardCommand::Partition(ExpireLeases)` only if it is the local
        // shard's leader. Per-shard tasks (rather than one task that loops
        // through shards) so a stalled shard's metrics watch can't block
        // sibling shards' sweeps.
        let check_interval = Duration::from_secs(5);
        let clock_skew_tolerance_ms = self.config.clock_skew_tolerance_ms;
        let node_id = self.config.node_id;
        for shard_idx in 0..self.cluster.metadata_shards() {
            let cluster = self.cluster.clone();
            let mut shutdown_rx = self.shutdown_tx.subscribe();
            let handle = self.runtime.spawn(async move {
                // Initial jitter staggers the sweep start across shards too
                // — N shards all firing at the same wall clock would queue up
                // on the proposal semaphore.
                let initial_jitter = jitter_duration(check_interval, 1.0);
                tokio::time::sleep(initial_jitter).await;

                let mut interval = tokio::time::interval(check_interval);
                loop {
                    tokio::select! {
                        _ = interval.tick() => {
                            let tick_jitter = jitter_duration(check_interval, 0.2);
                            tokio::time::sleep(tick_jitter).await;

                            let Some(shard) = cluster.shard(shard_idx) else { return };
                            // Leader-only sweep, per shard. Non-leaders skip
                            // the tick so the sweep neither multiplies write
                            // load by the cluster size nor stalls on
                            // forwarding during partitions. Sweeps are
                            // expire-by-deadline and therefore idempotent;
                            // a duplicate proposal from a deposed leader is
                            // a cheap no-op at the FSM.
                            if !shard.is_leader(node_id) {
                                continue;
                            }

                            // Subtract skew tolerance from the expiry comparison.
                            let now = current_time_ms().saturating_sub(clock_skew_tolerance_ms);
                            let command = ShardCommand::Partition(
                                PartitionStateCommand::ExpireLeases { current_time_ms: now }
                            );
                            if let Err(e) = cluster.write_shard(shard_idx, command).await {
                                warn!(shard = shard_idx, error = %e, "Failed to expire leases");
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

        // Reconciler: propagates control-group decisions (topic registry,
        // broker liveness) into every shard's state machine. Runs on every
        // broker; only proposes for shards this broker leads. See
        // `reconciler` module docs for the full propagation set.
        {
            let cluster = self.cluster.clone();
            let node_id = self.config.node_id;
            let shutdown_rx = self.shutdown_tx.subscribe();
            let handle = self.runtime.spawn(async move {
                reconciler::run(
                    cluster,
                    node_id,
                    reconciler::DEFAULT_RECONCILE_INTERVAL,
                    shutdown_rx,
                )
                .await;
            });
            self.task_handles.write().await.push(handle);
        }

        // Metrics emission: tracks the **control** group's openraft state.
        // Per-shard metrics live on the shard's own `metrics()` watch; this
        // tier surfaces the controller-leader signal that operator alerts
        // ("election storm", "no controller") were originally written
        // against. Per-shard gauges land alongside step 10's full migration.
        let cluster = self.cluster.clone();
        let max_pending_proposals = self.config.max_pending_proposals;
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let handle = self.runtime.spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(1));
            // Track the previous (state, term) so we can record election
            // outcomes when the state transitions out of Candidate. Without
            // this the `raft_elections_total` counter is permanently zero —
            // operators have no signal for election storms.
            let mut prev_state: Option<openraft::ServerState> = None;
            let mut prev_term: u64 = 0;
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => break,
                    _ = interval.tick() => {
                        let metrics = cluster.control().raft().metrics().borrow().clone();
                        let state_val = match metrics.state {
                            openraft::ServerState::Follower => 0,
                            openraft::ServerState::Candidate => 1,
                            openraft::ServerState::Leader => 2,
                            openraft::ServerState::Learner => 0,
                            openraft::ServerState::Shutdown => -1,
                        };
                        crate::cluster::metrics::set_raft_state(state_val);
                        crate::cluster::metrics::set_raft_term(metrics.current_term as i64);
                        crate::cluster::metrics::set_raft_commit_index(
                            metrics.last_log_index.unwrap_or(0) as i64,
                        );
                        crate::cluster::metrics::set_raft_applied_index(
                            metrics.last_applied.as_ref().map(|l| l.index).unwrap_or(0) as i64,
                        );
                        crate::cluster::metrics::set_raft_log_entries(
                            metrics.last_log_index.unwrap_or(0) as i64,
                        );
                        // Pending proposals: control group only for now.
                        // Per-group gauges are a follow-up — control's
                        // pressure dominates because it's the only group
                        // that fans out from a single coordinator-driven
                        // task (broker heartbeat).
                        let available = cluster.control().proposal_semaphore().available_permits();
                        crate::cluster::metrics::set_raft_pending_proposals((max_pending_proposals.saturating_sub(available)) as i64);

                        // Record election outcomes on state transitions out
                        // of Candidate. We may miss elections that complete
                        // entirely between two 1s polls, but the dominant
                        // observability case (election storms during a
                        // partition) hits the same state for many seconds and
                        // is captured.
                        let cur_state = metrics.state;
                        let cur_term = metrics.current_term;
                        if let Some(prev) = prev_state
                            && prev == openraft::ServerState::Candidate
                            && cur_state != openraft::ServerState::Candidate
                        {
                            match cur_state {
                                openraft::ServerState::Leader => {
                                    crate::cluster::metrics::record_raft_election("won");
                                }
                                openraft::ServerState::Follower
                                | openraft::ServerState::Learner => {
                                    crate::cluster::metrics::record_raft_election("lost");
                                }
                                _ => {}
                            }
                        } else if cur_state == openraft::ServerState::Candidate
                            && cur_term > prev_term
                            && prev_state == Some(openraft::ServerState::Candidate)
                        {
                            // Still a candidate in a higher term → previous
                            // election timed out without electing anyone.
                            crate::cluster::metrics::record_raft_election("timeout");
                        }
                        prev_state = Some(cur_state);
                        prev_term = cur_term;
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

        // Shut down the underlying multi-group cluster (RPC server +
        // every group's openraft handle).
        if let Err(e) = self.cluster.shutdown().await {
            tracing::warn!(error = %e, "Raft cluster shutdown returned an error");
        }

        debug!(
            broker_id = self.broker_id,
            "Raft coordinator shutdown complete"
        );
        Ok(())
    }

    /// Join a Raft cluster.
    ///
    /// Two-phase fan-out across all `metadata_shards + 1` groups:
    ///
    /// 1. **Phase 1 (learner)** — for each group `(Control, Shard(0..N-1))`,
    ///    send `JoinCluster { group, node_id, raft_addr }` to `leader_addr`
    ///    under the **join** HMAC purpose. The receiver's mux dispatcher
    ///    routes to the addressed group's `Raft<_>` handle and adds this
    ///    broker as a learner. Each call is structurally idempotent — a
    ///    retry against a group that already has the learner is a no-op.
    /// 2. **Phase 2 (promote)** — for each group, send `PromoteMember
    ///    { node_id, group }` to `leader_addr` under the **cluster** HMAC
    ///    purpose. The cluster purpose is required to promote, so a
    ///    holder-of-join-token-only cannot self-promote on any group.
    ///
    /// `leader_addr` is the bootstrap contact: at single-broker bootstrap
    /// it is the leader of every group (because the control leader
    /// initialized them all). For richer multi-broker join, the receiver
    /// returns a typed `NotLeader { leader_hint }` and the joining broker
    /// retries against the hint — that retry loop lives one layer up.
    ///
    /// **Sidecar (planned)**: per the sharding plan, `{raft_log_dir}/node-
    /// {id}/join_state.bin` should record per-group phase progress so a
    /// half-joined broker resumes from the first incomplete group on
    /// restart. Not yet implemented — every RPC is idempotent so a fresh
    /// `join_cluster` call after restart re-runs the full fan-out as a
    /// no-op for groups already in the right state.
    pub async fn join_cluster(&self, leader_addr: &str) -> SlateDBResult<()> {
        use super::mux_client::{request_mux_join, request_mux_promote};
        use super::types::GroupId;

        let node_id = self.config.node_id;
        let raft_addr = self.config.raft_addr.clone();
        let auth_keys = self.config.auth_keys.clone();
        let tls = self.config.tls.clone();
        let metadata_shards = self.cluster.metadata_shards();

        // Build the full group list so the order is deterministic across
        // restarts (control first, shards 0..N-1 in order). Important when
        // the sidecar lands: a half-joined broker resumes scanning from
        // the first group not yet in phase 2.
        let mut groups: Vec<GroupId> = Vec::with_capacity(metadata_shards as usize + 1);
        groups.push(GroupId::Control);
        for i in 0..metadata_shards {
            groups.push(GroupId::Shard(i));
        }

        // Phase 1: add learner on every group.
        for group in &groups {
            request_mux_join(
                leader_addr,
                node_id,
                &raft_addr,
                *group,
                &auth_keys,
                tls.as_ref(),
            )
            .await
            .map_err(|e| {
                SlateDBError::Storage(format!(
                    "Failed to join cluster (phase 1, {:?}): {}",
                    group, e
                ))
            })?;
        }

        // Phase 2: promote learner to voter on every group.
        for group in &groups {
            request_mux_promote(leader_addr, node_id, *group, &auth_keys, tls.as_ref())
                .await
                .map_err(|e| {
                    SlateDBError::Storage(format!(
                        "Failed to join cluster (phase 2, {:?}): {}",
                        group, e
                    ))
                })?;
        }
        Ok(())
    }

    /// Get the current Raft leader (of the control group — the "controller"
    /// in Kafka terms).
    pub async fn get_leader(&self) -> Option<u64> {
        self.cluster.control().current_leader()
    }

    /// Check if this node is the Raft leader of the **control** group.
    /// Per-shard leadership is queried directly on `cluster.shard(id)`.
    pub async fn is_leader(&self) -> bool {
        self.cluster.control().is_leader(self.config.node_id)
    }

    /// Get Raft metrics for the **control** group. Per-shard metrics are
    /// accessed via `cluster.shard(id).raft().metrics()`.
    pub fn metrics(&self) -> openraft::RaftMetrics<super::types::RaftNodeId, openraft::BasicNode> {
        self.cluster.control().raft().metrics().borrow().clone()
    }

    /// Current Raft state code, encoded the same way as the
    /// `kafkaesque_raft_state` gauge: 0 = Follower (or Learner — both are
    /// healthy non-leader states), 1 = Candidate, 2 = Leader, -1 = Shutdown.
    /// Used by the readiness probe to query state directly instead of
    /// going through the global gauge (which lags by up to 1s).
    ///
    /// Reflects the **control** group's state — the controller-leader signal
    /// the readiness probe was originally wired to track.
    pub fn current_raft_state(&self) -> i64 {
        match self.cluster.control().raft().metrics().borrow().state {
            openraft::ServerState::Follower => 0,
            openraft::ServerState::Candidate => 1,
            openraft::ServerState::Leader => 2,
            openraft::ServerState::Learner => 0,
            openraft::ServerState::Shutdown => -1,
        }
    }

    /// Wait for leader election to complete (control group).
    pub async fn wait_for_leader(&self) -> SlateDBResult<()> {
        self.wait_for_leader_with_timeout(Duration::from_secs(30))
            .await
    }

    /// Wait for leader election with a custom timeout (control group).
    pub async fn wait_for_leader_with_timeout(&self, timeout: Duration) -> SlateDBResult<()> {
        let start = std::time::Instant::now();
        let poll_interval = Duration::from_millis(100);

        loop {
            if self.cluster.control().current_leader().is_some() {
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

    /// Synchronously bump the per-key tombstone for `key`. Called from
    /// ownership-change hooks; immediately makes any cached entry for this
    /// key (and only this key) appear stale to concurrent readers, even
    /// before moka's async eviction completes. Replaces the old global
    /// generation counter that invalidated every cached entry on every
    /// per-key change.
    pub(crate) fn bump_owner_cache_tombstone(&self, key: &PartitionKey) {
        let mut entry = self.owner_cache_tombstones.entry(key.clone()).or_insert(0);
        *entry = entry.saturating_add(1);
    }

    /// Bump the global cache generation. Reserved for bulk invalidation —
    /// called by `owner_cache_invalidate_all` on leader change / snapshot
    /// install / membership change, where the entire cache must be
    /// considered stale at once.
    pub(crate) fn bump_owner_cache_global_generation(&self) {
        use std::sync::atomic::Ordering;
        self.owner_cache_global_generation
            .fetch_add(1, Ordering::SeqCst);
    }

    /// Snapshot the current (per-key, global) generation pair before doing a
    /// linearizable read. Pass the result to `owner_cache_insert_if_fresh`
    /// after the read; if either generation has advanced in the meantime,
    /// the insert is skipped to avoid landing a stale value as fresh.
    pub(super) fn owner_cache_read_stamp(&self, key: &PartitionKey) -> OwnerCacheReadStamp {
        use std::sync::atomic::Ordering;
        OwnerCacheReadStamp {
            key_gen: self
                .owner_cache_tombstones
                .get(key)
                .map(|v| *v)
                .unwrap_or(0),
            global_gen: self.owner_cache_global_generation.load(Ordering::SeqCst),
        }
    }

    /// Insert only if neither tombstone has advanced since `stamp` was
    /// taken. Returns true if the insert landed.
    pub(super) async fn owner_cache_insert_if_fresh(
        &self,
        key: PartitionKey,
        broker_id: i32,
        stamp: OwnerCacheReadStamp,
    ) -> bool {
        let current = self.owner_cache_read_stamp(&key);
        if current != stamp {
            return false;
        }
        self.owner_cache
            .insert(
                key,
                CachedOwnerEntry {
                    broker_id,
                    key_gen: stamp.key_gen,
                    global_gen: stamp.global_gen,
                },
            )
            .await;
        true
    }

    pub(super) async fn owner_cache_get(&self, key: &PartitionKey) -> Option<i32> {
        let stamp = self.owner_cache_read_stamp(key);
        self.owner_cache.get(key).await.and_then(|entry| {
            if entry.key_gen == stamp.key_gen && entry.global_gen == stamp.global_gen {
                Some(entry.broker_id)
            } else {
                None
            }
        })
    }

    pub(super) async fn owner_cache_invalidate(&self, key: &PartitionKey) {
        // Per-key tombstone only — DOES NOT bump the global generation.
        // Previously this bumped the global counter and silently discarded
        // every cached entry on every per-key invalidation.
        self.bump_owner_cache_tombstone(key);
        self.owner_cache.invalidate(key).await;
    }

    pub(super) async fn owner_cache_invalidate_all(&self) {
        self.bump_owner_cache_global_generation();
        self.owner_cache.invalidate_all();
        self.owner_cache.run_pending_tasks().await;
    }
}

/// Direct readiness query for the health server. Avoids the up-to-1s lag of
/// the global `RAFT_STATE` gauge, and the dependency on the coordinator's
/// metric poller having published at least once before `/ready` is asked.
impl crate::server::health::RaftStateProvider for RaftCoordinator {
    fn current_state(&self) -> i64 {
        self.current_raft_state()
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
    // A clock set before 1970 (extremely rare, but possible from a misconfigured
    // VM or container) would otherwise panic here and kill the coordinator
    // background task. Treating that case as t=0 is benign — the timestamp is
    // used for monotonically-clamped lease/heartbeat clocks downstream.
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
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
