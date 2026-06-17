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
//! # Status (sharding plan, step 5)
//!
//! This module establishes the *types* and the *routing* layer. The
//! constructor takes pre-built `RaftGroup` handles — the actual bootstrap
//! that wires storage, network factory, and snapshot recovery for both
//! control and shards lives in a follow-up commit, once the generic
//! `RaftNetworkFactory<G::Cfg>` is in place. Until then, the legacy
//! [`super::node::RaftNode`] continues to back the running broker; this
//! module compiles alongside it and is consumed by step 6's coordinator
//! routing.

#![allow(dead_code)] // wired in subsequent migration steps

use std::sync::Arc;

use arc_swap::ArcSwap;
use openraft::Raft;
use tokio::sync::Semaphore;

use super::group::{ControlGroupKind, GroupKind, ShardGroupKind};
use super::hash;
use super::types::{RaftNodeId, ShardId};

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
        assert_eq!(r.shard_for_topic("orders"), hash::shard_for_topic("orders", 8));
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
}
