//! Multi-group Raft cluster — `RaftGroup<G>` and `RaftCluster`.
//!
//! The single-group [`super::node::RaftNode`] funnels every metadata write
//! through one log and one leader. The sharded layout introduces:
//!
//! - One **control** group ([`super::group::ControlGroupKind`]) carrying
//!   cluster-wide state (broker registry, ACLs, topic registry, producer-id
//!   allocation).
//! - N **shard** groups ([`super::group::ShardGroupKind`]) each carrying a
//!   slice of the hot per-entity state (per-partition leases, consumer
//!   groups, per-producer idempotency, transfers).
//!
//! Each group is a [`RaftGroup<G>`]: an `Arc<Raft<G::Cfg>>` plus the
//! application-level wrappers a single openraft node needs (state-machine
//! handle, leader cache, proposal backpressure semaphore). [`RaftCluster`]
//! owns the control group and the fixed-size shard fleet, and provides the
//! routing helpers that decide which shard owns a given topic / consumer
//! group / producer id.
//!
//! # Status
//!
//! [`RaftCluster::new`] is the runnable bootstrap path: it builds the
//! per-group on-disk stores, recovers the WAL + snapshot, wires the
//! multiplexed network factory and listener, and constructs the openraft
//! handles. The legacy [`super::node::RaftNode`] continues to back the
//! running broker until the coordinator is rerouted (sharding plan, step 6);
//! this module compiles and tests alongside it.

#![allow(dead_code)] // wired in subsequent migration steps

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use arc_swap::ArcSwap;
use object_store::ObjectStore;
use openraft::storage::Adaptor;
use openraft::{BasicNode, Raft};
use tokio::runtime::Handle;
use tokio::sync::{RwLock, Semaphore};
use tokio::time::timeout;
use tracing::info;

use super::commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};
use super::config::RaftConfig;
use super::group::{ControlGroupKind, GroupKind, ShardGroupKind};
use super::hash;
use super::mux_client::{MuxAddrBook, MuxFactoryShared, build_mux_factories, new_addr_book};
use super::mux_server::{MuxRaftHandles, MuxRaftRpcServer};
use super::storage::RaftStore;
use super::types::{ControlConfig, RaftNodeId, ShardConfig, ShardId};
use crate::cluster::error::{SlateDBError, SlateDBResult};

// ============================================================================
// ShardRouter — pure routing decisions.
// ============================================================================

/// Decides which shard owns a given key.
///
/// Pulled out of [`RaftCluster`] so the routing math is testable without
/// real Raft handles, and so other layers (admin tools, the reconciler)
/// can compute shard ids without holding a cluster reference.
///
/// `metadata_shards` is fixed at bootstrap and persisted in
/// `cluster_meta.bin`. Re-creating a `ShardRouter` with a different value
/// after the cluster has accepted writes would silently re-shuffle every
/// key — see the validation gate in `RaftConfig::metadata_shards` and the
/// `cluster_meta.bin` mismatch check (sharding plan, step 9).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardRouter {
    metadata_shards: u16,
}

impl ShardRouter {
    /// Build a router for a cluster with `metadata_shards` shards.
    ///
    /// `metadata_shards` is clamped to 1 if 0 is passed, mirroring the
    /// saturating modulo in [`hash::shard_for_key`]. The config layer
    /// rejects 0 at load time; this is a defence-in-depth against
    /// programmer error in test setups.
    pub fn new(metadata_shards: u16) -> Self {
        Self {
            metadata_shards: metadata_shards.max(1),
        }
    }

    /// How many shards this router routes across.
    #[inline]
    pub fn count(&self) -> u16 {
        self.metadata_shards
    }

    /// Shard owning `topic`. All partitions of the topic land here.
    #[inline]
    pub fn shard_for_topic(&self, topic: &str) -> ShardId {
        hash::shard_for_topic(topic, self.metadata_shards)
    }

    /// Shard owning the consumer group `group_id`.
    #[inline]
    pub fn shard_for_group(&self, group_id: &str) -> ShardId {
        hash::shard_for_group(group_id, self.metadata_shards)
    }

    /// Shard owning the producer with `producer_id`.
    #[inline]
    pub fn shard_for_producer(&self, producer_id: i64) -> ShardId {
        hash::shard_for_producer(producer_id, self.metadata_shards)
    }
}

// ============================================================================
// RaftGroup<G> — one openraft instance plus its application-level handles.
// ============================================================================

/// A single Raft group instance.
///
/// Holds everything a caller needs to drive *one* group end-to-end: the
/// `Raft` handle, the state-machine wrapper, the leader-cache slot, and a
/// proposal-backpressure semaphore. Generic over [`GroupKind`] so the
/// control and shard kinds share this shape (and the storage / dispatch
/// code already keyed on `GroupKind`).
///
/// The leader cache is updated by a periodic `metrics()` poll
/// (sharding plan: "per-group `leader_cache` updated by a 200ms metrics
/// poll"). The cache exists so routing decisions don't `await` on the
/// metrics watch every hot-path call. See [`Self::refresh_leader_cache`]
/// and [`Self::cached_leader`].
pub struct RaftGroup<G: GroupKind> {
    /// Underlying openraft node for this group.
    raft: Arc<Raft<G::Cfg>>,
    /// State-machine wrapper. Cloneable per [`GroupKind::Sm`]; we hold one
    /// canonical clone so the storage layer and the application share the
    /// same inner `Arc<RwLock<State>>`.
    state_machine: G::Sm,
    /// Construction parameter for the SM (`()` for control, `ShardId` for
    /// shards). Kept on the group so [`Self::dir_segment`] / metrics
    /// labels can identify which shard this is without re-deriving it.
    sm_init: G::SmInit,
    /// Last-known leader id, updated by a background poll on this group's
    /// `metrics()` watch. `None` means "no leader observed yet" or "the
    /// last poll saw a leaderless interval". Kept as
    /// `ArcSwap<Option<RaftNodeId>>` per the plan spec — lock-free reads
    /// matter because every forwarded write checks this first.
    leader_cache: ArcSwap<Option<RaftNodeId>>,
    /// Bounds the number of in-flight proposals against this group.
    /// One semaphore per group rather than one shared, because the shard
    /// fleet's whole point is to absorb proposals in parallel — a shared
    /// semaphore would serialise them again.
    proposal_semaphore: Arc<Semaphore>,
}

impl<G: GroupKind> RaftGroup<G> {
    /// Build from already-constructed parts.
    ///
    /// `RaftCluster::new` (follow-up commit) wires this together with the
    /// per-group storage and network factory. Tests that need a
    /// `RaftGroup` will land alongside the real bootstrap because the
    /// `Arc<Raft<G::Cfg>>` cannot be stubbed without a full openraft
    /// fixture.
    pub fn from_parts(
        raft: Arc<Raft<G::Cfg>>,
        state_machine: G::Sm,
        sm_init: G::SmInit,
        proposal_semaphore: Arc<Semaphore>,
    ) -> Self {
        Self {
            raft,
            state_machine,
            sm_init,
            leader_cache: ArcSwap::from_pointee(None),
            proposal_semaphore,
        }
    }

    /// The openraft handle. Callers send `client_write`, `metrics`, etc.
    /// through this.
    #[inline]
    pub fn raft(&self) -> &Arc<Raft<G::Cfg>> {
        &self.raft
    }

    /// The state-machine wrapper for read paths that bypass Raft (admin
    /// list operations, in-memory peeks at the version counter, etc.).
    #[inline]
    pub fn state_machine(&self) -> &G::Sm {
        &self.state_machine
    }

    /// The construction parameter the SM was built with (e.g. the
    /// `ShardId` for a shard group). Useful when an iterator over
    /// `RaftCluster::shards()` needs to know which shard it's looking at.
    #[inline]
    pub fn sm_init(&self) -> &G::SmInit {
        &self.sm_init
    }

    /// Per-group on-disk directory segment (e.g. `"control"`,
    /// `"shard-3"`). Stored alongside the SM init so callers don't have to
    /// re-derive it.
    pub fn dir_segment(&self) -> String {
        G::dir_segment(&self.sm_init)
    }

    /// Backpressure handle for proposal submission. `acquire().await` to
    /// gate a proposal; the permit is released when dropped after the
    /// `client_write` completes.
    #[inline]
    pub fn proposal_semaphore(&self) -> &Arc<Semaphore> {
        &self.proposal_semaphore
    }

    /// Read the cached leader id without touching the metrics watch.
    /// Lock-free. Safe to call on the hot path.
    pub fn cached_leader(&self) -> Option<RaftNodeId> {
        **self.leader_cache.load()
    }

    /// Update the cached leader id from the openraft metrics watch.
    /// Cheap (one watch borrow + one Arc swap); intended to be called by a
    /// periodic 200ms-tick task per the sharding plan.
    pub fn refresh_leader_cache(&self) {
        let leader = self.raft.metrics().borrow().current_leader;
        // Only allocate a new Arc on actual change. ArcSwap::store always
        // allocates, but the cost is irrelevant compared to the update
        // frequency (every 200ms per group).
        self.leader_cache.store(Arc::new(leader));
    }

    /// Synchronous current-leader read straight from the metrics watch.
    /// Use [`Self::cached_leader`] on hot paths; this is for places that
    /// would otherwise await the watch and want the freshest value.
    pub fn current_leader(&self) -> Option<RaftNodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// True if this node is the current leader of this group, per the
    /// metrics watch. Independent of the cached value.
    pub fn is_leader(&self, node_id: RaftNodeId) -> bool {
        self.current_leader() == Some(node_id)
    }
}

// ============================================================================
// RaftCluster — control + shard fleet, plus routing helpers.
// ============================================================================

/// Owns the cluster-wide control group and the fixed-size shard fleet.
///
/// One per broker process. `Arc<RaftCluster>` is the handle the coordinator
/// (sharding plan, step 6) uses to decide *which* group a given write
/// targets:
///
/// - Topic / partition writes → [`Self::shard_for_topic`]
/// - Consumer-group writes → [`Self::shard_for_group`]
/// - Per-producer writes → [`Self::shard_for_producer`]
/// - Cluster-wide writes (broker registry, topic registry, producer-id
///   allocation, ACLs) → [`Self::control`]
///
/// The shard fleet is a `Vec<Arc<RaftGroup<ShardGroupKind>>>` with
/// `len == metadata_shards` indexed by [`ShardId`]. Indices are stable
/// for the cluster lifetime; the vector is built once at bootstrap and
/// never resized.
pub struct RaftCluster {
    control: Arc<RaftGroup<ControlGroupKind>>,
    shards: Vec<Arc<RaftGroup<ShardGroupKind>>>,
    router: ShardRouter,
    node_id: RaftNodeId,
    /// `Some` when constructed via [`Self::new`]; `None` for the test-only
    /// [`Self::from_parts`] path. Background tasks, the address book, and the
    /// shutdown channel live here. Methods that need bootstrap state assert
    /// `Some` (the test path doesn't call them).
    bootstrap: Option<ClusterBootstrap>,
}

impl RaftCluster {
    /// Construct from already-built groups.
    ///
    /// The number of shards is taken from `shards.len()` and pinned in the
    /// `ShardRouter`. The caller is responsible for building each shard
    /// with the matching `ShardId` (i.e. `shards[i].sm_init() == &i`); a
    /// debug-mode assertion enforces this so a bootstrap bug doesn't
    /// silently route topic A's writes through the wrong shard.
    pub fn from_parts(
        control: Arc<RaftGroup<ControlGroupKind>>,
        shards: Vec<Arc<RaftGroup<ShardGroupKind>>>,
        node_id: RaftNodeId,
    ) -> Self {
        let metadata_shards = u16::try_from(shards.len())
            .expect("metadata_shards must fit in u16; config validates 1..=1024");
        // Defence-in-depth: every shard's SM init must match its index.
        // A misordering would route every key to the wrong shard. Cheap
        // to assert at construction; impossible to recover from later.
        debug_assert!(
            shards
                .iter()
                .enumerate()
                .all(|(i, g)| u16::try_from(i).map(|x| *g.sm_init() == x).unwrap_or(false)),
            "shard fleet order does not match ShardId indices"
        );
        Self {
            control,
            shards,
            router: ShardRouter::new(metadata_shards),
            node_id,
            bootstrap: None,
        }
    }

    /// The control group.
    #[inline]
    pub fn control(&self) -> &Arc<RaftGroup<ControlGroupKind>> {
        &self.control
    }

    /// All shard groups, indexed by [`ShardId`].
    #[inline]
    pub fn shards(&self) -> &[Arc<RaftGroup<ShardGroupKind>>] {
        &self.shards
    }

    /// Bounds-checked shard accessor. Returns `None` for ids `>= metadata_shards`.
    #[inline]
    pub fn shard(&self, id: ShardId) -> Option<&Arc<RaftGroup<ShardGroupKind>>> {
        self.shards.get(id as usize)
    }

    /// The shard owning every partition of `topic`.
    pub fn shard_for_topic(&self, topic: &str) -> &Arc<RaftGroup<ShardGroupKind>> {
        let id = self.router.shard_for_topic(topic);
        self.shard_unchecked(id)
    }

    /// The shard owning the consumer group `group_id`.
    pub fn shard_for_group(&self, group_id: &str) -> &Arc<RaftGroup<ShardGroupKind>> {
        let id = self.router.shard_for_group(group_id);
        self.shard_unchecked(id)
    }

    /// The shard owning `producer_id`.
    pub fn shard_for_producer(&self, producer_id: i64) -> &Arc<RaftGroup<ShardGroupKind>> {
        let id = self.router.shard_for_producer(producer_id);
        self.shard_unchecked(id)
    }

    /// Internal infallible lookup used after a router computation. The
    /// router is constructed with `metadata_shards == self.shards.len()`,
    /// so any id it produces is in range; if that ever changes we'd
    /// rather find out via a clear panic at the routing site than silently
    /// route to the wrong shard.
    fn shard_unchecked(&self, id: ShardId) -> &Arc<RaftGroup<ShardGroupKind>> {
        self.shards
            .get(id as usize)
            .expect("router produced shard id outside the fleet — bootstrap mismatch")
    }

    /// The router used to compute shard ids. Exposed for tooling that
    /// wants the raw decision (e.g. `kafkaesque admin describe-topic` to
    /// print which shard owns a topic without holding a group handle).
    #[inline]
    pub fn router(&self) -> &ShardRouter {
        &self.router
    }

    /// The local broker's node id (the same value passed when constructing
    /// every group's `Raft`).
    #[inline]
    pub fn node_id(&self) -> RaftNodeId {
        self.node_id
    }

    /// Number of shards in the fleet. Equivalent to `self.router().count()`.
    #[inline]
    pub fn metadata_shards(&self) -> u16 {
        self.router.count()
    }
}

// ============================================================================
// RaftCluster::new — runnable bootstrap of control + shard fleet.
// ============================================================================

/// Per-group Raft handle the network factory and the multiplexed RPC server
/// both reference. Distinct from [`RaftGroup<G>`] (which carries the SM, leader
/// cache, etc.) because the listener only needs the openraft handle.
type ControlRaft = Arc<Raft<ControlConfig>>;
type ShardRaft = Arc<Raft<ShardConfig>>;

/// Cluster-wide bootstrap state held alongside the public [`RaftCluster`].
///
/// Kept in its own struct so `RaftCluster::new` can hand back a single
/// `Arc<RaftCluster>` while internal background tasks (leader-cache poll,
/// shutdown) reach into the bootstrap state without going through the public
/// API.
struct ClusterBootstrap {
    /// Cluster-wide address book shared with every group's network factory.
    /// Pre-seeded from `config.cluster_members`; `add_node` on either group's
    /// factory writes here.
    addrs: MuxAddrBook,
    /// Multiplexed network shared state (auth keys, TLS, addrs).
    factory_shared: Arc<MuxFactoryShared>,
    /// Broadcast channel used to signal the RPC server + background pollers
    /// to stop on shutdown. Receivers are subscribed at spawn time.
    shutdown_tx: tokio::sync::broadcast::Sender<()>,
    /// Background-task handles, awaited on shutdown so a fresh process can
    /// re-bind the same port without lingering stragglers.
    background_handles: RwLock<Vec<tokio::task::JoinHandle<()>>>,
    /// Original config — kept on the cluster so background tasks can read
    /// `proposal_timeout`, `auth_keys`, `tls`, etc. without stashing copies.
    config: RaftConfig,
    /// Period of the leader-cache poll task. Default 200ms per the sharding
    /// plan; configurable here so tests can dial it down without exposing it
    /// on `RaftConfig`.
    leader_cache_poll_interval: Duration,
}

impl RaftCluster {
    /// Build the runnable cluster.
    ///
    /// Steps, in order:
    ///
    /// 1. Validate `config` and the `cluster_meta.bin` pinning (refuses to
    ///    start on a `metadata_shards` mismatch — sharding plan, step 9).
    /// 2. Build per-group `RaftStore<G>` instances (one control,
    ///    `config.metadata_shards` shards). Each store recovers its WAL and
    ///    loads its snapshot before openraft is constructed; corrupt WAL or
    ///    snapshot fails closed (same fail-closed contract as
    ///    [`super::node::RaftNode::new`]).
    /// 3. Build the multiplexed network factory shared across every group.
    ///    The address book is pre-seeded from `config.cluster_members` so
    ///    openraft's first replication attempt has a target to dial.
    /// 4. Hand each store + per-group factory to `Raft::new`.
    /// 5. Spawn one [`MuxRaftRpcServer`] for the whole cluster on the
    ///    control-plane runtime. Per-group leader-cache poll tasks tick
    ///    every 200ms.
    ///
    /// Returns an `Arc<Self>` because background tasks (leader-cache poll
    /// task, future reconciler) need to reach back into the cluster.
    pub async fn new(
        config: RaftConfig,
        object_store: Arc<dyn ObjectStore>,
        runtime: Handle,
    ) -> SlateDBResult<Arc<Self>> {
        // ---- (1) Config + cluster_meta.bin validation ------------------------
        if let Err(errors) = config.validate() {
            return Err(SlateDBError::Config(format!(
                "Invalid Raft config: {}",
                errors.join(", ")
            )));
        }
        if let Err(msg) = config.validate_or_init_cluster_meta() {
            return Err(SlateDBError::Config(msg));
        }

        let node_id = config.node_id;
        let node_root: PathBuf = PathBuf::from(format!("{}/node-{}", config.raft_log_dir, node_id));
        let snapshot_root_prefix =
            format!("{}/raft/snapshots/node-{}", config.snapshot_dir, node_id);

        // ---- (2) Per-group stores --------------------------------------------
        let openraft_config = Arc::new(config.to_openraft_config());

        let control_store = build_and_recover_store::<ControlGroupKind>(
            &object_store,
            &snapshot_root_prefix,
            &node_root,
            (),
            node_id,
        )
        .await?;
        // Pull the SM handle BEFORE moving the store into the Adaptor; the
        // store is consumed by `Raft::new`, so a clone of `Arc<RwLock<G::Sm>>`
        // here keeps the cluster's own SM read path live.
        let control_sm_handle = clone_sm::<ControlGroupKind>(&control_store.state_machine()).await;

        let mut shard_stores: Vec<RaftStore<ShardGroupKind>> =
            Vec::with_capacity(config.metadata_shards as usize);
        let mut shard_sms: Vec<<ShardGroupKind as GroupKind>::Sm> =
            Vec::with_capacity(config.metadata_shards as usize);
        for shard_id in 0..config.metadata_shards {
            let store = build_and_recover_store::<ShardGroupKind>(
                &object_store,
                &snapshot_root_prefix,
                &node_root,
                shard_id,
                node_id,
            )
            .await?;
            let sm_handle = clone_sm::<ShardGroupKind>(&store.state_machine()).await;
            shard_stores.push(store);
            shard_sms.push(sm_handle);
        }

        // ---- (3) Multiplexed network factory ---------------------------------
        let addrs = new_addr_book();
        // Pre-seed addresses so openraft's first replication attempt has a
        // target. Same shape as RaftNode::new: peers added later via
        // `add_node` overwrite an entry rather than allocating a new map.
        {
            let mut book = addrs.write().await;
            for (peer_id, addr) in &config.cluster_members {
                book.insert(*peer_id, addr.clone());
            }
        }
        let factory_shared = Arc::new(MuxFactoryShared::new(
            addrs.clone(),
            config.auth_keys.clone(),
            config.tls.clone(),
        ));
        let (control_factory, shard_factories) =
            build_mux_factories(factory_shared.clone(), config.metadata_shards);

        // ---- (4) Build openraft handles --------------------------------------
        let control_raft = build_raft_handle::<ControlGroupKind, _>(
            &openraft_config,
            node_id,
            control_store,
            control_factory,
        )
        .await?;
        let mut shard_rafts: Vec<ShardRaft> = Vec::with_capacity(config.metadata_shards as usize);
        // Drain `shard_stores` (move) and zip with the corresponding factory
        // so each shard's store ends up paired with the factory tagged with
        // its `shard_id` — a swap here would silently route writes to the
        // wrong shard.
        for (store, factory) in shard_stores.into_iter().zip(shard_factories.into_iter()) {
            let raft =
                build_raft_handle::<ShardGroupKind, _>(&openraft_config, node_id, store, factory)
                    .await?;
            shard_rafts.push(raft);
        }

        // ---- Wrap each group in a RaftGroup<G> -------------------------------
        let proposal_semaphore_control = Arc::new(Semaphore::new(config.max_pending_proposals));
        let control_group = Arc::new(RaftGroup::<ControlGroupKind>::from_parts(
            control_raft.clone(),
            control_sm_handle,
            (),
            proposal_semaphore_control,
        ));

        let mut shard_groups: Vec<Arc<RaftGroup<ShardGroupKind>>> =
            Vec::with_capacity(config.metadata_shards as usize);
        for (i, raft) in shard_rafts.iter().enumerate() {
            let semaphore = Arc::new(Semaphore::new(config.max_pending_proposals));
            let shard_id = u16::try_from(i).expect("metadata_shards <= u16::MAX by config");
            let sm = shard_sms[i].clone();
            shard_groups.push(Arc::new(RaftGroup::<ShardGroupKind>::from_parts(
                raft.clone(),
                sm,
                shard_id,
                semaphore,
            )));
        }

        // ---- (5) RPC server + background pollers -----------------------------
        let (shutdown_tx, _) = tokio::sync::broadcast::channel(1);
        let bootstrap = ClusterBootstrap {
            addrs,
            factory_shared,
            shutdown_tx: shutdown_tx.clone(),
            background_handles: RwLock::new(Vec::new()),
            config: config.clone(),
            leader_cache_poll_interval: Duration::from_millis(200),
        };

        let cluster = Arc::new(Self {
            control: control_group,
            shards: shard_groups,
            router: ShardRouter::new(config.metadata_shards),
            node_id,
            bootstrap: Some(bootstrap),
        });

        // Spawn the multiplexed RPC server. Holds the control + shard
        // handles directly so a frame addressed to shard `i` lands on the
        // exact `Arc<Raft<ShardConfig>>` the cluster also references for
        // local writes.
        let mux_handles = Arc::new(MuxRaftHandles {
            control: control_raft,
            shards: shard_rafts.into(),
        });
        let server = MuxRaftRpcServer::new(
            mux_handles,
            config.raft_addr.clone(),
            runtime.clone(),
            config.auth_keys.clone(),
            config.tls.clone(),
        );
        let server_shutdown = shutdown_tx.subscribe();
        let server_handle = runtime.spawn(async move {
            if let Err(e) = server.run(server_shutdown).await {
                tracing::error!(error = %e, "Mux Raft RPC server error");
            }
        });
        cluster.push_background_handle(server_handle).await;

        // Per-group leader-cache poll. One task per group polls its
        // `metrics()` watch and updates the cached leader id every
        // `leader_cache_poll_interval` (200ms by default). The cache is
        // ArcSwap so callers on the hot path do a lock-free load instead of
        // taking the watch each time.
        cluster.spawn_leader_cache_pollers(&runtime).await;

        info!(
            node_id,
            raft_addr = %config.raft_addr,
            metadata_shards = config.metadata_shards,
            "Multi-group Raft cluster started"
        );

        Ok(cluster)
    }

    fn bootstrap(&self) -> &ClusterBootstrap {
        // Always Some after `new()`; only set to None on the
        // `from_parts` test path where no bootstrap state exists.
        self.bootstrap
            .as_ref()
            .expect("RaftCluster::bootstrap accessed on a from_parts-only cluster")
    }

    async fn push_background_handle(&self, handle: tokio::task::JoinHandle<()>) {
        if let Some(b) = self.bootstrap.as_ref() {
            b.background_handles.write().await.push(handle);
        }
    }

    async fn spawn_leader_cache_pollers(self: &Arc<Self>, runtime: &Handle) {
        let interval = self.bootstrap().leader_cache_poll_interval;

        // Control group.
        {
            let cluster = self.clone();
            let mut shutdown_rx = self.bootstrap().shutdown_tx.subscribe();
            let handle = runtime.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => break,
                        _ = tick.tick() => {
                            cluster.control.refresh_leader_cache();
                        }
                    }
                }
            });
            self.push_background_handle(handle).await;
        }

        // Shard groups. One task per shard so a stalled shard's metrics
        // watch can't park the control group's cache update behind it.
        // Per-shard latency dominates this anyway (the watch borrow is
        // µs); spawning N+1 tasks is the right granularity.
        for shard in &self.shards {
            let shard = shard.clone();
            let mut shutdown_rx = self.bootstrap().shutdown_tx.subscribe();
            let handle = runtime.spawn(async move {
                let mut tick = tokio::time::interval(interval);
                loop {
                    tokio::select! {
                        _ = shutdown_rx.recv() => break,
                        _ = tick.tick() => {
                            shard.refresh_leader_cache();
                        }
                    }
                }
            });
            self.push_background_handle(handle).await;
        }
    }

    /// Initialize this cluster as the bootstrap-first node.
    ///
    /// Calls `Raft::initialize` on the control group then on every shard,
    /// each with this node as the only initial voter. Subsequent brokers
    /// join via the `JoinCluster` RPC (see [`super::network::request_cluster_join`]
    /// for the legacy single-group equivalent; the multi-group variant lands
    /// alongside the join sidecar in step 8 of the sharding plan).
    ///
    /// Already-initialized groups (e.g. on restart with a populated WAL)
    /// surface an openraft error with `is_initialized() == true`; the legacy
    /// path treats those as success and so does this one. Idempotency is
    /// asserted *structurally* (membership has voters) rather than by
    /// inspecting upstream error strings, so a crate bump can't silently
    /// break the contract.
    pub async fn initialize_cluster(&self) -> SlateDBResult<()> {
        let mut members: BTreeMap<RaftNodeId, BasicNode> = BTreeMap::new();
        members.insert(
            self.node_id,
            BasicNode {
                addr: self.bootstrap().config.raft_addr.clone(),
            },
        );

        if !self.control.is_initialized() {
            self.control
                .raft()
                .initialize(members.clone())
                .await
                .map_err(|e| SlateDBError::Storage(format!("init control: {}", e)))?;
        }
        for (i, shard) in self.shards.iter().enumerate() {
            if !shard.is_initialized() {
                shard
                    .raft()
                    .initialize(members.clone())
                    .await
                    .map_err(|e| SlateDBError::Storage(format!("init shard {}: {}", i, e)))?;
            }
        }
        info!(
            node_id = self.node_id,
            metadata_shards = self.shards.len(),
            "Initialized control + shard groups as single-node cluster",
        );
        Ok(())
    }

    /// Add a known peer's address to every group's network factory.
    ///
    /// Callers (e.g. join-cluster handler) push the new broker's
    /// `(node_id, raft_addr)` here so subsequent outbound RPCs from any
    /// group can dial the peer. This shares one address book across every
    /// factory by design — re-discovering a peer for shard 3 separately
    /// from control would be a footgun on cluster join.
    pub async fn add_node(&self, node_id: RaftNodeId, addr: String) {
        self.bootstrap().addrs.write().await.insert(node_id, addr);
    }

    /// Lookup the address for `node_id`, or `None` if unknown.
    pub async fn get_node_addr(&self, node_id: RaftNodeId) -> Option<String> {
        self.bootstrap().addrs.read().await.get(&node_id).cloned()
    }

    /// Propose a write to the control group.
    ///
    /// Same backpressure + propose semantics as [`super::node::RaftNode::write`]:
    /// per-group proposal semaphore bounds in-flight proposals, then
    /// `client_write` runs through openraft. Forwarded writes (when this
    /// node isn't the control leader) currently bubble up the openraft
    /// `ForwardToLeader` error verbatim — the dedicated forwarder that
    /// routes a `ClientWriteWithTerm` over the multiplexed port lands with
    /// step 8 (group-tagged forwarding); until then callers handle the
    /// fallback by retrying after a leader-cache refresh.
    pub async fn write_control(&self, command: ControlCommand) -> SlateDBResult<ControlResponse> {
        let permit = self
            .acquire_propose_permit(self.control.proposal_semaphore())
            .await?;
        let _permit = permit;
        match self.control.raft().client_write(command.clone()).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => {
                if is_forward_to_leader_error(&e) {
                    self.forward_control_write(command).await
                } else {
                    Err(SlateDBError::Storage(format!(
                        "Raft control write failed: {}",
                        e
                    )))
                }
            }
        }
    }

    /// Propose a write to shard `shard_id`.
    pub async fn write_shard(
        &self,
        shard_id: ShardId,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        let shard = self.shards.get(shard_id as usize).ok_or_else(|| {
            SlateDBError::Config(format!(
                "shard id {} out of range (metadata_shards = {})",
                shard_id,
                self.shards.len()
            ))
        })?;
        let permit = self
            .acquire_propose_permit(shard.proposal_semaphore())
            .await?;
        let _permit = permit;
        match shard.raft().client_write(command.clone()).await {
            Ok(resp) => Ok(resp.data),
            Err(e) => {
                if is_forward_to_leader_error(&e) {
                    self.forward_shard_write(shard_id, command).await
                } else {
                    Err(SlateDBError::Storage(format!(
                        "Raft shard {} write failed: {}",
                        shard_id, e
                    )))
                }
            }
        }
    }

    /// Forward a control-group client write to the current control leader
    /// over the multiplexed port.
    ///
    /// Reads the cached leader id (lock-free `ArcSwap`) and the leader's
    /// address from the cluster-wide address book; bails if either is
    /// unknown. The receiver's mux dispatcher applies the same hop-cap +
    /// term + is-leader checks as the local Raft instance, so a stale
    /// cached leader id surfaces as a typed error and the caller's retry
    /// loop refreshes from `metrics()` for the next attempt.
    async fn forward_control_write(
        &self,
        command: ControlCommand,
    ) -> SlateDBResult<ControlResponse> {
        let metrics = self.control.raft().metrics().borrow().clone();
        let current_term = metrics.current_term;
        let leader_id = self.control.cached_leader().or(metrics.current_leader);
        let Some(leader_id) = leader_id else {
            return Err(SlateDBError::Storage(
                "control: no leader to forward to".to_string(),
            ));
        };
        let Some(leader_addr) = self.get_node_addr(leader_id).await else {
            return Err(SlateDBError::Storage(format!(
                "control: leader id {} not in address book",
                leader_id
            )));
        };
        let cfg = &self.bootstrap().config;
        super::mux_client::forward_control_write_with_term(
            &leader_addr,
            command,
            current_term,
            0,
            &cfg.auth_keys,
            cfg.tls.as_ref(),
        )
        .await
        .map_err(|e| SlateDBError::Storage(format!("Failed to forward to control leader: {}", e)))
    }

    /// Forward a shard-group client write to the current leader of `shard_id`.
    /// Same shape as [`Self::forward_control_write`] but addressed at one
    /// shard's `Raft<ShardConfig>` handle on the receiver.
    async fn forward_shard_write(
        &self,
        shard_id: ShardId,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        let shard = self
            .shards
            .get(shard_id as usize)
            .expect("shard_id pre-validated by caller");
        let metrics = shard.raft().metrics().borrow().clone();
        let current_term = metrics.current_term;
        let leader_id = shard.cached_leader().or(metrics.current_leader);
        let Some(leader_id) = leader_id else {
            return Err(SlateDBError::Storage(format!(
                "shard {}: no leader to forward to",
                shard_id
            )));
        };
        let Some(leader_addr) = self.get_node_addr(leader_id).await else {
            return Err(SlateDBError::Storage(format!(
                "shard {}: leader id {} not in address book",
                shard_id, leader_id
            )));
        };
        let cfg = &self.bootstrap().config;
        super::mux_client::forward_shard_write_with_term(
            &leader_addr,
            shard_id,
            command,
            current_term,
            0,
            &cfg.auth_keys,
            cfg.tls.as_ref(),
        )
        .await
        .map_err(|e| {
            SlateDBError::Storage(format!(
                "Failed to forward to shard {} leader: {}",
                shard_id, e
            ))
        })
    }

    /// Convenience: propose to the shard owning `topic` (one of N shards).
    /// `hash(topic) % metadata_shards`.
    #[inline]
    pub async fn write_shard_for_topic(
        &self,
        topic: &str,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        let id = self.router.shard_for_topic(topic);
        self.write_shard(id, command).await
    }

    /// Convenience: propose to the shard owning consumer group `group_id`.
    /// `hash(group_id) % metadata_shards`.
    #[inline]
    pub async fn write_shard_for_group(
        &self,
        group_id: &str,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        let id = self.router.shard_for_group(group_id);
        self.write_shard(id, command).await
    }

    /// Convenience: propose to the shard owning `producer_id`.
    /// `hash(producer_id) % metadata_shards`.
    #[inline]
    pub async fn write_shard_for_producer(
        &self,
        producer_id: i64,
        command: ShardCommand,
    ) -> SlateDBResult<ShardResponse> {
        let id = self.router.shard_for_producer(producer_id);
        self.write_shard(id, command).await
    }

    /// Add `node_id` as a learner to **every** group.
    ///
    /// Brokers must be voters in every group for any of them to make
    /// progress with the new broker, so we add as learner everywhere first
    /// and let [`Self::change_membership_all_groups`] promote them in one
    /// pass. Each `add_learner` is idempotent at the openraft layer; a
    /// retry against a group that already has the learner is a no-op.
    pub async fn add_learner_all_groups(
        &self,
        node_id: RaftNodeId,
        addr: String,
    ) -> SlateDBResult<()> {
        // Make every group's network factory aware of the address before
        // we kick off the membership change — without this, openraft's
        // first replication attempt to the new node has no addr to dial.
        self.add_node(node_id, addr.clone()).await;

        let node = BasicNode { addr };
        self.control
            .raft()
            .add_learner(node_id, node.clone(), true)
            .await
            .map_err(|e| SlateDBError::Storage(format!("add_learner control: {}", e)))?;
        for (i, shard) in self.shards.iter().enumerate() {
            shard
                .raft()
                .add_learner(node_id, node.clone(), true)
                .await
                .map_err(|e| SlateDBError::Storage(format!("add_learner shard {}: {}", i, e)))?;
        }
        Ok(())
    }

    /// Promote every group's membership to the given voter set.
    ///
    /// Each group's `change_membership` is independent; if one fails, the
    /// caller's standard retry loop covers the partial-success case.
    pub async fn change_membership_all_groups<I>(&self, members: I) -> SlateDBResult<()>
    where
        I: IntoIterator<Item = RaftNodeId>,
    {
        let voters: std::collections::BTreeSet<RaftNodeId> = members.into_iter().collect();
        self.control
            .raft()
            .change_membership(voters.clone(), false)
            .await
            .map_err(|e| SlateDBError::Storage(format!("change_membership control: {}", e)))?;
        for (i, shard) in self.shards.iter().enumerate() {
            shard
                .raft()
                .change_membership(voters.clone(), false)
                .await
                .map_err(|e| {
                    SlateDBError::Storage(format!("change_membership shard {}: {}", i, e))
                })?;
        }
        Ok(())
    }

    async fn acquire_propose_permit<'a>(
        &self,
        semaphore: &'a Arc<Semaphore>,
    ) -> SlateDBResult<tokio::sync::SemaphorePermit<'a>> {
        let proposal_timeout = self.bootstrap().config.proposal_timeout;
        let max_pending = self.bootstrap().config.max_pending_proposals;
        let _start = Instant::now();
        match timeout(proposal_timeout, semaphore.acquire()).await {
            Ok(Ok(permit)) => {
                crate::cluster::metrics::record_raft_backpressure("acquired");
                Ok(permit)
            }
            Ok(Err(_)) => Err(SlateDBError::Raft(
                "Proposal semaphore closed unexpectedly".to_string(),
            )),
            Err(_) => {
                crate::cluster::metrics::record_raft_backpressure("timeout");
                crate::cluster::metrics::COORDINATOR_FAILURES
                    .with_label_values(&["proposal_backpressure_timeout"])
                    .inc();
                Err(SlateDBError::Raft(format!(
                    "Proposal backpressure timeout: too many pending proposals (max {})",
                    max_pending
                )))
            }
        }
    }

    /// Stop the cluster: signal background tasks, await them, then shut down
    /// every group's openraft handle. Mirrors [`super::node::RaftNode::shutdown`]
    /// fanned out across all groups.
    pub async fn shutdown(&self) -> SlateDBResult<()> {
        if let Some(b) = self.bootstrap.as_ref() {
            let _ = b.shutdown_tx.send(());
            let mut handles = b.background_handles.write().await;
            for handle in handles.drain(..) {
                let _ = handle.await;
            }
        }

        if let Err(e) = self.control.raft().shutdown().await {
            tracing::warn!(error = %e, "control group shutdown returned an error");
        }
        for (i, shard) in self.shards.iter().enumerate() {
            if let Err(e) = shard.raft().shutdown().await {
                tracing::warn!(shard = i, error = %e, "shard group shutdown returned an error");
            }
        }
        info!(node_id = self.node_id, "RaftCluster shut down");
        Ok(())
    }
}

// ============================================================================
// Bootstrap helpers — kept as free fns so the constructor reads top-down.
// ============================================================================

/// Detect openraft's "forward to leader" error by string match.
///
/// openraft's `ClientWriteError::ForwardToLeader` is a typed variant, but
/// it's wrapped in nested generic error enums whose path differs across
/// crate versions. The legacy [`super::node::RaftNode::write`] also relies
/// on string matching for the same reason — pinning the contract here
/// keeps both paths in lockstep so a request the legacy path forwards is
/// also forwarded by the multi-group path (and vice versa).
fn is_forward_to_leader_error<E: std::fmt::Display>(e: &E) -> bool {
    let s = e.to_string();
    s.contains("forward request to") || s.contains("ForwardToLeader")
}

/// Build a per-group `RaftStore<G>`, recover its WAL, and load any existing
/// snapshot. On any corruption, fails closed with the same error wording as
/// [`super::node::RaftNode::new`] — the operator restoring at 3am needs a
/// crash, not a silently amnesiac broker.
async fn build_and_recover_store<G: GroupKind>(
    object_store: &Arc<dyn ObjectStore>,
    snapshot_root_prefix: &str,
    node_root: &std::path::Path,
    init: G::SmInit,
    node_id: RaftNodeId,
) -> SlateDBResult<RaftStore<G>> {
    let dir_segment = G::dir_segment(&init);
    let log_dir = node_root.join(&dir_segment);
    let snapshot_prefix = format!("{}/{}", snapshot_root_prefix, dir_segment);

    let store = RaftStore::<G>::with_init_and_log_dir(
        object_store.clone(),
        &snapshot_prefix,
        log_dir.clone(),
        init,
    );

    if let Err(e) = store.recover_from_disk().await {
        tracing::error!(
            node_id,
            group = %dir_segment,
            error = %e,
            wal_dir = %log_dir.display(),
            "Failed to recover Raft WAL; refusing to start"
        );
        return Err(SlateDBError::Config(format!(
            "Failed to recover Raft WAL for node {} group {} from {}: {}. \
             Refusing to start to avoid consensus-safety violations.",
            node_id,
            dir_segment,
            log_dir.display(),
            e
        )));
    }

    match store.load_snapshot_from_store().await {
        Ok(true) => info!(node_id, group = %dir_segment, "Restored state from snapshot"),
        Ok(false) => info!(
            node_id,
            group = %dir_segment,
            "No existing snapshot found, starting fresh"
        ),
        Err(e) => {
            tracing::error!(
                node_id,
                group = %dir_segment,
                error = %e,
                "Failed to load Raft snapshot; refusing to start with empty state"
            );
            return Err(SlateDBError::Config(format!(
                "Failed to load Raft snapshot for node {} group {}: {}. \
                 Refusing to start to avoid metadata loss.",
                node_id, dir_segment, e
            )));
        }
    }

    Ok(store)
}

/// Wire a constructed store + factory into a fresh openraft handle via the
/// v1→v2 [`Adaptor`]. Mirrors the `Adaptor::new(store) → Raft::new(...)`
/// pattern used by [`super::node::RaftNode::new`]. The store is consumed
/// here — callers that need an SM read handle must clone it out before
/// calling.
async fn build_raft_handle<G, F>(
    openraft_config: &Arc<openraft::Config>,
    node_id: RaftNodeId,
    store: RaftStore<G>,
    factory: F,
) -> SlateDBResult<Arc<Raft<G::Cfg>>>
where
    G: GroupKind,
    F: openraft::network::RaftNetworkFactory<G::Cfg> + Clone + 'static,
{
    let (log_store, sm_store) = Adaptor::new(store);

    let raft = Raft::new(
        node_id,
        openraft_config.clone(),
        factory,
        log_store,
        sm_store,
    )
    .await
    .map_err(|e| SlateDBError::Config(format!("Failed to create Raft handle: {}", e)))?;
    Ok(Arc::new(raft))
}

/// Pull a clone of the inner `G::Sm` out of the Arc-RwLock the store
/// constructed at startup. The store keeps its own `Arc<RwLock<G::Sm>>`;
/// the cluster wants the cloneable handle so write paths don't have to
/// reach through the Arc on every call.
async fn clone_sm<G: GroupKind>(sm_lock: &Arc<RwLock<G::Sm>>) -> G::Sm {
    sm_lock.read().await.clone()
}

// ============================================================================
// RaftGroup<G> — small helpers used by the bootstrap path.
// ============================================================================

impl<G: GroupKind> RaftGroup<G> {
    /// Whether this group has membership (i.e. has been initialized or
    /// resumed from a populated snapshot).
    pub fn is_initialized(&self) -> bool {
        let metrics = self.raft.metrics();
        let m = metrics.borrow();
        m.membership_config
            .membership()
            .voter_ids()
            .next()
            .is_some()
    }
}

#[cfg(test)]
mod tests {
    //! Tests focus on [`ShardRouter`] because it's the only piece that can
    //! be exercised without a live openraft fixture.
    //!
    //! [`RaftGroup`] / [`RaftCluster`] indexing is mechanical — the router
    //! pins which shard a key maps to, and the cluster does an
    //! `Vec::get(id)` afterwards. The bootstrap path that builds real
    //! groups, plus its end-to-end routing test, lands with the network
    //! factory + `RaftCluster::new` follow-up.

    use super::*;

    #[test]
    fn router_clamps_zero_shards_to_one() {
        // Misconfiguration safety net — same contract as
        // `hash::shard_for_key`. Config layer rejects 0; this is a last
        // line of defence so we never panic with divide-by-zero.
        let r = ShardRouter::new(0);
        assert_eq!(r.count(), 1);
        assert_eq!(r.shard_for_topic("orders"), 0);
        assert_eq!(r.shard_for_group("g"), 0);
        assert_eq!(r.shard_for_producer(42), 0);
    }

    #[test]
    fn router_count_matches_constructor_input() {
        for n in [1_u16, 2, 8, 1024, u16::MAX] {
            assert_eq!(ShardRouter::new(n).count(), n);
        }
    }

    #[test]
    fn router_decisions_match_free_hash_helpers() {
        // The router is just a thin object wrapper around the free
        // helpers in `super::hash`. Pinning equivalence here means a
        // future refactor that reorders the wrapper can't silently change
        // routing — equivalent to `cluster_meta.bin`'s wire contract.
        let r = ShardRouter::new(8);
        assert_eq!(
            r.shard_for_topic("orders"),
            hash::shard_for_topic("orders", 8)
        );
        assert_eq!(r.shard_for_group("g42"), hash::shard_for_group("g42", 8));
        assert_eq!(
            r.shard_for_producer(0x0123_4567),
            hash::shard_for_producer(0x0123_4567, 8)
        );
    }

    #[test]
    fn router_topic_partitions_share_shard() {
        // The plan's "every partition of a topic lands on the same shard"
        // invariant — this is what lets the reconciler fan
        // InitPartition out without consulting the partition number.
        let r = ShardRouter::new(8);
        let target = r.shard_for_topic("orders");
        for partition in [0, 1, 7, 31, 1023] {
            let _ = partition; // partition number is intentionally unused
            assert_eq!(r.shard_for_topic("orders"), target);
        }
    }

    #[test]
    fn router_distribution_at_n_eight_is_balanced() {
        // 10k synthetic topics across 8 shards — every shard gets between
        // 10% and 15% of the load. A regression that biases distribution
        // would fail here (and silently saturate one shard in prod).
        let r = ShardRouter::new(8);
        let mut counts = [0_u32; 8];
        for i in 0..10_000 {
            counts[r.shard_for_topic(&format!("t-{i}")) as usize] += 1;
        }
        for (i, c) in counts.iter().enumerate() {
            assert!(
                (1000..=1500).contains(c),
                "shard {} got {} hits, expected 1000..=1500",
                i,
                c
            );
        }
    }

    #[test]
    fn router_is_copy_and_cheap_to_clone() {
        // ShardRouter shows up on every metadata write path; copying it
        // must be free. If a future change makes it non-Copy (e.g.
        // by holding an Arc), the routing layer will need to switch to
        // referencing it. Pin the contract.
        fn assert_copy<T: Copy>() {}
        assert_copy::<ShardRouter>();
    }

    // ========================================================================
    // RaftCluster::new bootstrap smoke test
    // ========================================================================
    //
    // The bootstrap path itself can only be exercised against a real openraft
    // fixture — there's no stub for storage + network factory + listener at
    // this layer. The test below stands up a 1-broker / 2-shard cluster on a
    // free port, initializes membership, waits for both the control and
    // shard-0 to elect a leader, drives one Noop write through each, and
    // shuts down. That's the smallest end-to-end exercise of:
    //
    //   - RaftCluster::new (per-group store recovery + Adaptor wiring)
    //   - MuxRaftRpcServer (TCP accept loop)
    //   - per-group leader-cache poll task
    //   - write_control / write_shard hot paths
    //   - shutdown fan-out
    //
    // Pinning these together means a regression in any one of them surfaces
    // as a test failure with a meaningful diagnostic rather than a silent
    // mistake at process start.

    use super::super::auth::RaftAuthKeys;
    use super::super::commands::{ControlCommand, ControlResponse, ShardCommand, ShardResponse};
    use super::super::config::RaftConfig as RealRaftConfig;
    use object_store::memory::InMemory;
    use std::net::TcpListener;
    use std::time::Duration;
    use tokio::runtime::Handle;

    fn next_test_port() -> u16 {
        let l = TcpListener::bind(("127.0.0.1", 0)).expect("bind ephemeral");
        l.local_addr().unwrap().port()
    }

    fn smoke_config(port: u16, root: &std::path::Path, shards: u16) -> RealRaftConfig {
        // Fast timers for tests — same shape as the existing failure-mode
        // test harness. `metadata_shards` parameterised so we can vary the
        // shard count without forking the helper.
        RealRaftConfig {
            node_id: 1,
            broker_id: 1,
            host: "127.0.0.1".to_string(),
            port: 9090,
            raft_addr: format!("127.0.0.1:{}", port),
            cluster_members: Vec::new(),
            raft_log_dir: root.join("log").to_string_lossy().into_owned(),
            snapshot_dir: root.join("snapshots").to_string_lossy().into_owned(),
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(150),
            election_timeout_max: Duration::from_millis(300),
            max_payload_entries: 100,
            snapshot_threshold: 100,
            is_voter: true,
            lease_duration: Duration::from_secs(5),
            lease_renewal_interval: Duration::from_secs(1),
            broker_heartbeat_interval: Duration::from_millis(200),
            broker_heartbeat_ttl: Duration::from_secs(1),
            default_session_timeout_ms: 5_000,
            session_timeout_check_interval: Duration::from_millis(500),
            auto_create_topics: true,
            max_partitions_per_topic: 100,
            max_pending_proposals: 100,
            proposal_timeout: Duration::from_secs(5),
            auth_keys: Arc::new(RaftAuthKeys::dev_unauthenticated()),
            tls: None,
            clock_skew_tolerance_ms: 5_000,
            metadata_shards: shards,
        }
    }

    async fn wait_for_leader(group: &Arc<RaftGroup<impl GroupKind>>, timeout: Duration) -> bool {
        let start = std::time::Instant::now();
        while start.elapsed() < timeout {
            if group.current_leader().is_some() {
                return true;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    #[tokio::test]
    async fn cluster_bootstraps_initializes_writes_and_shuts_down() {
        let tmp = tempfile::tempdir().expect("tempdir");
        let port = next_test_port();
        let config = smoke_config(port, tmp.path(), 2);
        let object_store = Arc::new(InMemory::new());

        let cluster = RaftCluster::new(config, object_store, Handle::current())
            .await
            .expect("RaftCluster::new");

        // Bootstrap the single-node membership on every group.
        cluster
            .initialize_cluster()
            .await
            .expect("initialize_cluster");

        // Wait for leadership on control + shard 0 — single voter so this
        // happens within one election timeout, not several seconds.
        let control = cluster.control().clone();
        assert!(
            wait_for_leader(&control, Duration::from_secs(5)).await,
            "control group never elected a leader"
        );
        let shard_0 = cluster.shard(0).expect("shard 0 exists").clone();
        assert!(
            wait_for_leader(&shard_0, Duration::from_secs(5)).await,
            "shard 0 never elected a leader"
        );

        // One Noop write to each group via the public write helpers — pins
        // both the propose-permit semaphore path and the per-group routing.
        let r = cluster
            .write_control(ControlCommand::Noop)
            .await
            .expect("write_control");
        assert!(matches!(r, ControlResponse::Ok));
        let r = cluster
            .write_shard(0, ShardCommand::Noop)
            .await
            .expect("write_shard 0");
        assert!(matches!(r, ShardResponse::Ok));

        // Out-of-range shard write is rejected before touching any Raft
        // handle — same Risk #2 boundary the dispatcher enforces.
        let err = cluster
            .write_shard(99, ShardCommand::Noop)
            .await
            .unwrap_err();
        assert!(format!("{}", err).contains("out of range"));

        cluster.shutdown().await.expect("shutdown");
    }

    #[tokio::test]
    async fn cluster_meta_mismatch_on_restart_refuses_to_start() {
        // Step 9 invariant — bootstrapping with metadata_shards=N then
        // restarting with M (M != N) refuses to start, so an operator can't
        // silently re-shard every key by editing config. Test pinned here
        // (rather than inside config.rs) because it requires the full
        // RaftCluster::new bootstrap to read the persisted file.
        let tmp = tempfile::tempdir().expect("tempdir");
        let port_a = next_test_port();
        let object_store = Arc::new(InMemory::new());

        // First boot writes cluster_meta.bin with metadata_shards=2.
        {
            let config = smoke_config(port_a, tmp.path(), 2);
            let cluster = RaftCluster::new(config, object_store.clone(), Handle::current())
                .await
                .expect("first RaftCluster::new");
            cluster.shutdown().await.expect("shutdown");
        }

        // Second boot with metadata_shards=4 must fail.
        let port_b = next_test_port();
        let mut config = smoke_config(port_b, tmp.path(), 4);
        config.node_id = 1; // same node id — same on-disk state
        let result = RaftCluster::new(config, object_store, Handle::current()).await;
        let err = match result {
            Ok(_) => panic!("second boot must refuse mismatched metadata_shards"),
            Err(e) => e,
        };
        let msg = format!("{}", err);
        assert!(
            msg.contains("metadata_shards"),
            "error must mention metadata_shards, got: {}",
            msg
        );
    }

    // ========================================================================
    // Reconciler end-to-end (step 7)
    // ========================================================================
    //
    // The reconciler unit tests live in `super::reconciler` but they can't
    // exercise the full propagation loop without a live cluster — that's
    // here. Each test builds a single-broker cluster, drives a control
    // commit, calls `reconciler::reconcile_once` once, and asserts the
    // shard state converged. Driving the reconciler directly (rather than
    // waiting for the 200ms background-task tick) keeps these tests fast
    // and deterministic.

    use super::super::domains::{
        BrokerCommand, BrokerStatus, PartitionStateResponse, TopicRegistryCommand,
    };
    use super::super::reconciler;
    use std::collections::HashMap;

    #[tokio::test]
    async fn reconciler_seeds_partitions_for_topic_created_on_control() {
        // The plan's two-phase CreateTopic: control records the topic, the
        // reconciler propagates `InitPartition` to the shard. Without the
        // reconciler, an `AcquirePartition` would auto-create the entry
        // (legacy fallback) — but that path leaves the partition without a
        // recorded `created_at_ms`. Pinning that the reconciler is what
        // does the seeding makes the topic→shard contract observable.
        let tmp = tempfile::tempdir().unwrap();
        let config = smoke_config(next_test_port(), tmp.path(), 4);
        let cluster = RaftCluster::new(config, Arc::new(InMemory::new()), Handle::current())
            .await
            .unwrap();
        cluster.initialize_cluster().await.unwrap();

        // Wait for every shard + control to elect leaders. The reconciler
        // gates on `is_leader`, so the test must run after election.
        assert!(wait_for_leader(&cluster.control().clone(), Duration::from_secs(5)).await);
        for i in 0..cluster.metadata_shards() {
            let s = cluster.shard(i).unwrap().clone();
            assert!(
                wait_for_leader(&s, Duration::from_secs(5)).await,
                "shard {} never elected a leader",
                i
            );
        }

        // CreateTopic on control. With `metadata_shards=4`, the topic
        // routes deterministically to one specific shard.
        let topic = "orders".to_string();
        let partition_count = 3i32;
        cluster
            .write_control(ControlCommand::TopicRegistry(
                TopicRegistryCommand::CreateTopic {
                    name: topic.clone(),
                    partitions: partition_count,
                    config: HashMap::new(),
                    timestamp_ms: 1_000,
                },
            ))
            .await
            .unwrap();

        // Pre-condition: the target shard has no partition entries yet —
        // CreateTopic on the new layout deliberately does NOT seed them.
        let target = cluster.router().shard_for_topic(&topic);
        {
            let sm = cluster.shard(target).unwrap().state_machine();
            let state = sm.state().await;
            for p in 0..partition_count {
                assert!(
                    !state
                        .partition_state
                        .partitions
                        .contains_key(&(Arc::from(topic.as_str()), p)),
                    "partition ({}, {}) seeded too early — CreateTopic must not seed",
                    topic,
                    p
                );
            }
        }

        // One reconciler pass — should propose `InitPartition` for every
        // partition, all routed to the same shard.
        reconciler::reconcile_once(&cluster, cluster.node_id()).await;

        // Post-condition: every partition is now present on the target
        // shard (and only the target shard).
        for shard_idx in 0..cluster.metadata_shards() {
            let sm = cluster.shard(shard_idx).unwrap().state_machine();
            let state = sm.state().await;
            for p in 0..partition_count {
                let key = (Arc::from(topic.as_str()), p);
                let present = state.partition_state.partitions.contains_key(&key);
                if shard_idx == target {
                    assert!(
                        present,
                        "shard {} (target) missing seeded partition ({}, {})",
                        shard_idx, topic, p
                    );
                } else {
                    assert!(
                        !present,
                        "shard {} (non-target) holds partition ({}, {}) — wrong shard",
                        shard_idx, topic, p
                    );
                }
            }
        }

        // Idempotency: a second reconcile pass must be a no-op (every
        // partition already present). The shard SM's `InitPartition`
        // returns `PartitionAlreadyExists` and nothing else changes.
        let version_before = {
            let sm = cluster.shard(target).unwrap().state_machine();
            sm.state().await.version
        };
        reconciler::reconcile_once(&cluster, cluster.node_id()).await;
        let version_after = {
            let sm = cluster.shard(target).unwrap().state_machine();
            sm.state().await.version
        };
        // Version on the shard SM advances only when the SM applies a
        // command. A converged tick should issue zero commands — pre-filter
        // catches the "already exists" case before proposing.
        assert_eq!(
            version_before, version_after,
            "second reconcile pass must be idempotent (zero shard SM applies)"
        );

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reconciler_propagates_broker_status_into_every_shard_shadow() {
        // Broker liveness is authored on control; the cross-domain fencing
        // gate on the shard reads its local liveness shadow. The
        // reconciler is the bridge — without this propagation a fenced
        // broker could keep acquiring leases on shards that haven't seen
        // the fence event.
        let tmp = tempfile::tempdir().unwrap();
        let config = smoke_config(next_test_port(), tmp.path(), 3);
        let cluster = RaftCluster::new(config, Arc::new(InMemory::new()), Handle::current())
            .await
            .unwrap();
        cluster.initialize_cluster().await.unwrap();

        assert!(wait_for_leader(&cluster.control().clone(), Duration::from_secs(5)).await);
        for i in 0..cluster.metadata_shards() {
            let s = cluster.shard(i).unwrap().clone();
            assert!(wait_for_leader(&s, Duration::from_secs(5)).await);
        }

        // Register a broker on control.
        cluster
            .write_control(ControlCommand::Broker(BrokerCommand::Register {
                broker_id: 42,
                host: "host-42".to_string(),
                port: 9092,
                timestamp_ms: 1_000,
            }))
            .await
            .unwrap();

        // Pre-condition: no shard has broker 42 in its shadow yet.
        for shard_idx in 0..cluster.metadata_shards() {
            let sm = cluster.shard(shard_idx).unwrap().state_machine();
            assert!(
                !sm.state().await.broker_liveness_shadow.contains_key(&42),
                "shard {} already has broker 42 before reconcile",
                shard_idx
            );
        }

        reconciler::reconcile_once(&cluster, cluster.node_id()).await;

        // Every shard now sees broker 42 as Active.
        for shard_idx in 0..cluster.metadata_shards() {
            let sm = cluster.shard(shard_idx).unwrap().state_machine();
            let state = sm.state().await;
            let rec = state
                .broker_liveness_shadow
                .get(&42)
                .unwrap_or_else(|| panic!("shard {} missing broker 42", shard_idx));
            assert_eq!(rec.status, BrokerStatus::Active);
        }

        // Now fence broker 42 on control and re-reconcile. The shadow on
        // every shard must flip to Fenced.
        cluster
            .write_control(ControlCommand::Broker(BrokerCommand::Fence {
                broker_id: 42,
                reason: "test".to_string(),
            }))
            .await
            .unwrap();
        reconciler::reconcile_once(&cluster, cluster.node_id()).await;

        for shard_idx in 0..cluster.metadata_shards() {
            let sm = cluster.shard(shard_idx).unwrap().state_machine();
            let state = sm.state().await;
            let rec = state.broker_liveness_shadow.get(&42).unwrap();
            assert_eq!(
                rec.status,
                BrokerStatus::Fenced,
                "shard {} did not flip broker 42 to Fenced",
                shard_idx
            );
        }

        cluster.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reconciler_purges_partitions_after_topic_deleted_on_control() {
        // Mirror of the seeding test but on the delete path — DeleteTopic
        // on control + reconcile must remove partition entries from the
        // owning shard. Without this, fenced topic state would linger
        // forever and a re-create with the same name would inherit stale
        // PartitionInfo (the same edge case the tombstone-TTL plan
        // addresses; basic purge is enough for now).
        let tmp = tempfile::tempdir().unwrap();
        let config = smoke_config(next_test_port(), tmp.path(), 2);
        let cluster = RaftCluster::new(config, Arc::new(InMemory::new()), Handle::current())
            .await
            .unwrap();
        cluster.initialize_cluster().await.unwrap();
        assert!(wait_for_leader(&cluster.control().clone(), Duration::from_secs(5)).await);
        for i in 0..cluster.metadata_shards() {
            let s = cluster.shard(i).unwrap().clone();
            assert!(wait_for_leader(&s, Duration::from_secs(5)).await);
        }

        let topic = "ephemeral".to_string();
        let partition_count = 2i32;
        // Create + reconcile to seed partitions.
        cluster
            .write_control(ControlCommand::TopicRegistry(
                TopicRegistryCommand::CreateTopic {
                    name: topic.clone(),
                    partitions: partition_count,
                    config: HashMap::new(),
                    timestamp_ms: 1_000,
                },
            ))
            .await
            .unwrap();
        reconciler::reconcile_once(&cluster, cluster.node_id()).await;
        let target = cluster.router().shard_for_topic(&topic);
        {
            let sm = cluster.shard(target).unwrap().state_machine();
            let state = sm.state().await;
            assert_eq!(
                state
                    .partition_state
                    .partitions
                    .keys()
                    .filter(|(t, _)| t.as_ref() == topic)
                    .count(),
                partition_count as usize,
                "seed didn't fully apply"
            );
        }

        // Delete on control.
        cluster
            .write_control(ControlCommand::TopicRegistry(
                TopicRegistryCommand::DeleteTopic {
                    name: topic.clone(),
                },
            ))
            .await
            .unwrap();

        // Pre-condition: partitions still on shard (delete hasn't reached
        // the shard yet — that's the reconciler's job).
        {
            let sm = cluster.shard(target).unwrap().state_machine();
            let state = sm.state().await;
            assert!(
                state
                    .partition_state
                    .partitions
                    .keys()
                    .any(|(t, _)| t.as_ref() == topic),
                "partitions purged before reconcile? unexpected"
            );
        }

        reconciler::reconcile_once(&cluster, cluster.node_id()).await;

        // Post-condition: zero partition entries remain for the topic.
        {
            let sm = cluster.shard(target).unwrap().state_machine();
            let state = sm.state().await;
            for p in 0..partition_count {
                let key = (Arc::from(topic.as_str()), p);
                assert!(
                    !state.partition_state.partitions.contains_key(&key),
                    "partition ({}, {}) still on shard after purge reconcile",
                    topic,
                    p
                );
            }
        }
        // Suppress unused-variant lint for the response type — pinned the
        // import so the test surface lists it next to the others.
        let _ = PartitionStateResponse::PartitionPurged {
            topic: String::new(),
            partition: 0,
        };
        let _: ShardCommand = ShardCommand::Noop;

        cluster.shutdown().await.unwrap();
    }

    // ========================================================================
    // Group-tagged forwarding (step 8)
    // ========================================================================
    //
    // The test below stands up a 2-broker cluster, identifies the non-leader
    // broker on the control group, and submits `write_control` against it.
    // For the write to succeed, three things must work end-to-end:
    //
    //   1. The local Raft instance returns a `ForwardToLeader`-shaped error
    //      (matched by `is_forward_to_leader_error`).
    //   2. `forward_control_write` looks up the cached leader id and dials
    //      it via `mux_client::forward_control_write_with_term`.
    //   3. The receiver's mux server routes the `ClientWriteWithTerm` to its
    //      local control Raft handle, which commits the proposal.
    //
    // Without forwarding, the non-leader's write would return a `Storage`
    // error and the test would fail at the `.unwrap()`.

    #[tokio::test]
    async fn write_on_non_leader_broker_is_forwarded_to_control_leader() {
        let object_store = Arc::new(InMemory::new());
        let runtime = Handle::current();

        // Build two brokers sharing one in-memory object store. Each gets
        // its own temp dir + raft port.
        let tmp_a = tempfile::tempdir().unwrap();
        let tmp_b = tempfile::tempdir().unwrap();
        let addr_a = format!("127.0.0.1:{}", next_test_port());
        let addr_b = format!("127.0.0.1:{}", next_test_port());

        let mut config_a = smoke_config(0, tmp_a.path(), 2);
        config_a.node_id = 1;
        config_a.broker_id = 1;
        config_a.raft_addr = addr_a.clone();
        config_a.cluster_members = vec![(2, addr_b.clone())];

        let mut config_b = smoke_config(0, tmp_b.path(), 2);
        config_b.node_id = 2;
        config_b.broker_id = 2;
        config_b.raft_addr = addr_b.clone();
        config_b.cluster_members = vec![(1, addr_a.clone())];

        let cluster_a = RaftCluster::new(config_a, object_store.clone(), runtime.clone())
            .await
            .unwrap();
        let cluster_b = RaftCluster::new(config_b, object_store.clone(), runtime.clone())
            .await
            .unwrap();

        // Bootstrap: A initializes single-voter membership, then adds B as
        // learner on every group, then promotes B to voter on every group.
        cluster_a.initialize_cluster().await.unwrap();
        assert!(wait_for_leader(&cluster_a.control().clone(), Duration::from_secs(5)).await);
        cluster_a
            .add_learner_all_groups(2, addr_b.clone())
            .await
            .unwrap();
        cluster_a
            .change_membership_all_groups([1u64, 2u64])
            .await
            .unwrap();

        // Wait for both brokers' control SMs to converge to the same
        // committed leader. Without this, a write on B might hit B's
        // election window and never reach the forward path.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let leader_id = loop {
            if std::time::Instant::now() > deadline {
                panic!("brokers never agreed on a control leader");
            }
            let a = cluster_a.control().current_leader();
            let b = cluster_b.control().current_leader();
            if let (Some(la), Some(lb)) = (a, b)
                && la == lb
            {
                break la;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        };

        // Pick the broker that's NOT the leader and submit a write through
        // it. The write must succeed — proving the forward path round-trip.
        let non_leader = if leader_id == 1 {
            &cluster_b
        } else {
            &cluster_a
        };
        let resp = non_leader
            .write_control(ControlCommand::Noop)
            .await
            .expect("write_control on non-leader must succeed via forwarding");
        assert!(matches!(resp, ControlResponse::Ok));

        // Same forward path on a shard. Pick whichever broker is not leader
        // of shard 0 — this exercises the per-shard forward helper, which
        // is parameterised independently from control's.
        let shard_leader = cluster_a
            .shard(0)
            .unwrap()
            .current_leader()
            .or_else(|| cluster_b.shard(0).unwrap().current_leader())
            .expect("shard 0 must have elected a leader by now");
        let non_shard_leader = if shard_leader == 1 {
            &cluster_b
        } else {
            &cluster_a
        };
        let resp = non_shard_leader
            .write_shard(0, ShardCommand::Noop)
            .await
            .expect("write_shard on non-leader must succeed via forwarding");
        assert!(matches!(resp, ShardResponse::Ok));

        cluster_a.shutdown().await.unwrap();
        cluster_b.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn join_cluster_via_mux_adds_learner_to_every_group() {
        // Pins the per-group `JoinCluster { group: ... }` fan-out: a fresh
        // broker calling `request_mux_join` against a single leader address
        // ends up as a member of control AND every shard. Without this
        // contract the joining broker would only see traffic for the one
        // group it joined.
        use super::super::auth::RaftAuthKeys as RealAuthKeys;
        use super::super::mux_client::{request_mux_join, request_mux_promote};
        use super::super::types::GroupId;

        // Configure real auth keys: the mux server enforces a structural
        // check that JoinCluster frames must travel under the **join**
        // purpose tag, which only fires when a join token is configured
        // (otherwise outbound frames are tagged Unauthenticated and
        // rejected). Same contract the legacy `request_cluster_join`
        // depends on.
        let auth_keys = Arc::new(RealAuthKeys::from_strings(
            Some("test-cluster-secret-padded-to-32-bytes!!".to_string()),
            Some("test-join-token-padded-to-32-bytes!!!!".to_string()),
        ));

        let object_store = Arc::new(InMemory::new());
        let runtime = Handle::current();

        // Bootstrap leader: single-voter cluster with 3 shards.
        let tmp_a = tempfile::tempdir().unwrap();
        let addr_a = format!("127.0.0.1:{}", next_test_port());
        let mut config_a = smoke_config(0, tmp_a.path(), 3);
        config_a.node_id = 1;
        config_a.broker_id = 1;
        config_a.raft_addr = addr_a.clone();
        config_a.auth_keys = auth_keys.clone();
        let cluster_a = RaftCluster::new(config_a.clone(), object_store.clone(), runtime.clone())
            .await
            .unwrap();
        cluster_a.initialize_cluster().await.unwrap();
        assert!(wait_for_leader(&cluster_a.control().clone(), Duration::from_secs(5)).await);
        for i in 0..cluster_a.metadata_shards() {
            let s = cluster_a.shard(i).unwrap().clone();
            assert!(wait_for_leader(&s, Duration::from_secs(5)).await);
        }

        // The joiner runs on a distinct port. We don't actually need to
        // build a second `RaftCluster` for this test — the fan-out
        // contract is purely about the receiver-side membership state.
        let joiner_id: RaftNodeId = 2;
        let joiner_addr = format!("127.0.0.1:{}", next_test_port());

        // Phase 1: add the joiner as learner on every group via per-group
        // JoinCluster frames.
        let mut groups: Vec<GroupId> = Vec::with_capacity(4);
        groups.push(GroupId::Control);
        for i in 0..cluster_a.metadata_shards() {
            groups.push(GroupId::Shard(i));
        }
        for g in &groups {
            request_mux_join(&addr_a, joiner_id, &joiner_addr, *g, &auth_keys, None)
                .await
                .unwrap_or_else(|e| panic!("phase 1 ({:?}) failed: {}", g, e));
        }

        // Verify membership: every group's voter or learner set now
        // contains the joiner.
        let control_has = cluster_a
            .control()
            .raft()
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .get_node(&joiner_id)
            .is_some();
        assert!(control_has, "control did not record the joiner");
        for i in 0..cluster_a.metadata_shards() {
            let has = cluster_a
                .shard(i)
                .unwrap()
                .raft()
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .get_node(&joiner_id)
                .is_some();
            assert!(has, "shard {} did not record the joiner as learner", i);
        }

        // Phase 1 is the fan-out invariant we're pinning here. Phase 2
        // (`PromoteMember`) goes through the same per-group dispatcher
        // path but additionally requires the learner to be caught up on
        // log replication before openraft commits the membership change.
        // Driving phase 2 from this test would need a second live
        // `RaftCluster` listening on `joiner_addr` to receive
        // `AppendEntries` — that crosses into "real cluster join"
        // territory which the linearizability harness already exercises
        // via the in-process `add_learner_all_groups` /
        // `change_membership_all_groups` path. The dispatcher unit tests
        // in `mux_server` and the routing tests in `mux` cover the
        // PromoteMember wire path itself.
        let _ = request_mux_promote;

        cluster_a.shutdown().await.unwrap();
    }

    // ========================================================================
    // Cross-shard topic create distribution (sharding plan, essential test #1)
    // ========================================================================
    //
    // The plan calls for: with N=8, create 64 topics × 32 partitions each;
    // every topic's partitions all land on `hash(topic) % 8`. This pins
    // the contract that "every partition of a topic lives on the same
    // shard" end-to-end through the reconciler — the cheaper unit test
    // `router_topic_partitions_share_shard` only checks the routing math,
    // not the actual per-shard SM after seeding. Scaled down here from
    // 64×32 to 8×3 so the test runs in a few seconds; the invariant is
    // identical.

    #[tokio::test]
    async fn cross_shard_topic_create_partitions_land_on_target_shard() {
        let tmp = tempfile::tempdir().unwrap();
        let config = smoke_config(next_test_port(), tmp.path(), 8);
        let cluster = RaftCluster::new(config, Arc::new(InMemory::new()), Handle::current())
            .await
            .unwrap();
        cluster.initialize_cluster().await.unwrap();
        assert!(wait_for_leader(&cluster.control().clone(), Duration::from_secs(5)).await);
        for i in 0..cluster.metadata_shards() {
            let s = cluster.shard(i).unwrap().clone();
            assert!(wait_for_leader(&s, Duration::from_secs(5)).await);
        }

        // Create 8 topics × 3 partitions. The plan calls for 64×32 — we
        // ship the smaller scale here for fast CI; the per-topic
        // distribution invariant doesn't depend on the count.
        let topic_count = 8usize;
        let partition_count = 3i32;
        let topics: Vec<String> = (0..topic_count).map(|i| format!("topic-{}", i)).collect();
        for topic in &topics {
            cluster
                .write_control(ControlCommand::TopicRegistry(
                    TopicRegistryCommand::CreateTopic {
                        name: topic.clone(),
                        partitions: partition_count,
                        config: HashMap::new(),
                        timestamp_ms: 1_000,
                    },
                ))
                .await
                .unwrap();
        }

        // One reconcile pass seeds every topic's partitions onto its
        // routed shard. (One pass suffices because the local broker is
        // leader of every shard in this single-broker setup.)
        reconciler::reconcile_once(&cluster, cluster.node_id()).await;

        // Per-topic invariant: for each topic, every partition is present
        // ONLY on `hash(topic) % N` and absent on every other shard.
        for topic in &topics {
            let target = cluster.router().shard_for_topic(topic);
            for shard_idx in 0..cluster.metadata_shards() {
                let sm = cluster.shard(shard_idx).unwrap().state_machine();
                let state = sm.state().await;
                for p in 0..partition_count {
                    let key = (Arc::from(topic.as_str()), p);
                    let present = state.partition_state.partitions.contains_key(&key);
                    if shard_idx == target {
                        assert!(
                            present,
                            "topic {} partition {} missing on target shard {}",
                            topic, p, shard_idx
                        );
                    } else {
                        assert!(
                            !present,
                            "topic {} partition {} leaked onto non-target shard {} \
                             (target was {})",
                            topic, p, shard_idx, target
                        );
                    }
                }
            }
        }

        // Coverage check: with 8 topics across 8 shards on a fair hash
        // we expect at least 4 distinct shards to hold a topic. If the
        // distribution collapses to 1 shard (regression in routing),
        // every assertion above passes vacuously — pin it here.
        let shards_with_topics: std::collections::HashSet<u16> = topics
            .iter()
            .map(|t| cluster.router().shard_for_topic(t))
            .collect();
        assert!(
            shards_with_topics.len() >= 4,
            "expected ≥4 distinct shards used, got {}",
            shards_with_topics.len()
        );

        cluster.shutdown().await.unwrap();
    }
}
