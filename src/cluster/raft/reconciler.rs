//! Reconciler — propagates **control** group decisions into the shard groups.
//!
//! The control group records cluster-wide state (topic registry, broker
//! registry, ACLs, producer-id allocator). Several pieces of that state need
//! to be reflected in *every shard's* state machine for the hot paths to
//! decide locally without a control round-trip:
//!
//! - **Topic seeding** (`InitPartition`): when a topic is created on
//!   control, the per-partition lease state must materialize on the shard
//!   that owns the topic (`hash(topic) % metadata_shards`). The shard SM
//!   needs a `PartitionInfo` entry so `AcquirePartition` and friends find
//!   something to mutate.
//! - **Topic purging** (`PurgePartition`): when a topic is deleted from
//!   control, its per-partition entries must be removed from the owning
//!   shard.
//! - **Broker liveness shadow** (`UpdateBrokerLivenessShadow`): broker
//!   `status` (Active / Fenced / etc.) is authored on control. Each shard
//!   keeps a *shadow* of it so the cross-domain fencing gate
//!   (`AcquirePartition` / `RenewLease` reject a non-Active broker) can run
//!   locally without consulting control on every hot-path call.
//!
//! # Where the reconciler runs
//!
//! Every broker runs an instance. The reconciler reads control state from
//! its **local** SM (which Raft replication keeps in sync) and proposes to
//! shards where this broker is the **local** shard leader. Followers do not
//! need to do anything special — they observe the writes via Raft
//! replication.
//!
//! Two competing brokers proposing the same change is fine: the shard SM's
//! `version > stored.version` guard (for liveness shadow) and the
//! "partition already exists" no-op (for `InitPartition`) make every
//! proposal idempotent.
//!
//! # Cadence
//!
//! 200ms tick by default — same cadence as the per-group leader-cache poll.
//! Tighter cadence makes "topic created → ready to lease" faster; the cost
//! is a small steady-state read against every group's SM state lock. Each
//! tick is bounded work: O(topics × partitions_per_topic) to compute the
//! seed diff, plus O(brokers) for the liveness diff.
//!
//! # What's *not* here yet
//!
//! - **Tombstone TTL**: the plan describes control marking a topic as
//!   `tombstoned_at_ms` on `DeleteTopic`, then GC'ing the tombstone after
//!   `tombstone_ttl` elapses (no per-shard ack — TTL is the proxy). This
//!   commit takes a simpler approach: when a topic disappears from
//!   control's registry, the reconciler purges the corresponding shard
//!   partitions on the next tick. The race "create-delete-create the same
//!   topic name in the tombstone window" is not yet handled — adding a
//!   `created_at_ms` field to `PartitionInfo` (or implementing the TTL
//!   tombstone) is the follow-up.
//! - **Producer-id forward init**: per the plan this is *caller-routed*,
//!   not reconciler-driven, and already lives in
//!   `coordinator/producer.rs::init_producer_id` (control allocates the id,
//!   then the same call routes `Producer(InitProducerId)` to the shard
//!   owning that id). This file does not need to do anything for it.

#![allow(dead_code)] // wired by RaftCoordinator::start_background_tasks

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::broadcast;
use tracing::{debug, warn};

use super::cluster::RaftCluster;
use super::commands::ShardCommand;
use super::domains::{BrokerStatus, PartitionStateCommand};
use super::types::{RaftNodeId, ShardId};

/// Default cadence: 200ms ticks, same as the per-group leader-cache poll.
/// Pulled out as a constant so the test path can dial it down without
/// touching `RaftConfig`.
pub const DEFAULT_RECONCILE_INTERVAL: Duration = Duration::from_millis(200);

/// Snapshot of a single tick's view of control state, taken once per tick
/// and reused across the per-shard sub-passes. Avoids re-locking the
/// control SM 3× per tick (init + purge + shadow).
struct ControlSnapshot {
    /// `(topic_name → (partition_count, created_at_ms))`. Computed from
    /// `control_state.topic_registry.topics`.
    topics: Vec<(Arc<str>, i32, u64)>,
    /// Set of topic names — for fast membership checks during the purge
    /// sweep.
    topic_set: HashSet<Arc<str>>,
    /// `(broker_id, status)` snapshot used to compute the liveness diff.
    /// Only Active / Fenced statuses are propagated; transient statuses
    /// (Draining, etc.) flow through naturally because we propagate
    /// whatever we observed.
    brokers: Vec<(i32, BrokerStatus)>,
    /// Control SM apply count at snapshot time. Used as the `version`
    /// field of `UpdateBrokerLivenessShadow`. Monotonic on the control
    /// log, so two reconcilers proposing the same broker-status pair from
    /// the same tick attach the same version (idempotent at the shard);
    /// two reconcilers reading at different ticks attach different
    /// versions and the later one wins.
    version: u64,
}

/// Drive the reconciler loop until `shutdown_rx` fires.
///
/// One of these is spawned per broker by [`super::coordinator::RaftCoordinator::start_background_tasks`].
/// `tick` controls the cadence (defaults to [`DEFAULT_RECONCILE_INTERVAL`]
/// from the coordinator — tests may pass a faster value).
pub async fn run(
    cluster: Arc<RaftCluster>,
    node_id: RaftNodeId,
    tick: Duration,
    mut shutdown_rx: broadcast::Receiver<()>,
) {
    let mut interval = tokio::time::interval(tick);
    // The first tick fires immediately; we want to wait one period before
    // the first sweep so a freshly-started broker has time to apply any
    // initial replication burst before issuing its first set of
    // proposals. (Same convention as the broker-heartbeat task.)
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval.tick().await;
    loop {
        tokio::select! {
            _ = shutdown_rx.recv() => break,
            _ = interval.tick() => {
                reconcile_once(&cluster, node_id).await;
            }
        }
    }
}

/// One reconcile pass. Reads control state once, then for each shard this
/// broker leads, fans out the three propagations (init / purge / shadow).
///
/// `pub(super)` so end-to-end tests in sibling modules (notably
/// `super::cluster`) can drive a single tick deterministically without
/// racing the 200ms background-task cadence.
pub(super) async fn reconcile_once(cluster: &Arc<RaftCluster>, node_id: RaftNodeId) {
    let snapshot = capture_control_snapshot(cluster).await;

    for shard_idx in 0..cluster.metadata_shards() {
        let Some(shard) = cluster.shard(shard_idx) else {
            continue;
        };
        // Leader-only fan-out. Every broker runs the loop, but only the
        // current shard leader proposes — followers observe via Raft.
        // Without this gate every broker would propose every command on
        // every tick, multiplying write load by the cluster size.
        if !shard.is_leader(node_id) {
            continue;
        }

        propagate_topic_init(cluster, shard_idx, &snapshot).await;
        propagate_topic_purge(cluster, shard_idx, &snapshot).await;
        propagate_broker_shadow(cluster, shard_idx, &snapshot).await;
    }
}

/// Read everything we need out of control's SM in one borrow so the rest of
/// the tick can run without the control read guard held.
///
/// `topic_registry.topics` and `broker_domain.brokers` are both
/// `HashMap<...>`. Cloning into owned `Vec`s here is cheap relative to a
/// 200ms tick and lets us drop the control read lock before walking shards.
async fn capture_control_snapshot(cluster: &Arc<RaftCluster>) -> ControlSnapshot {
    let sm = cluster.control().state_machine();
    let state = sm.state().await;

    let topics: Vec<(Arc<str>, i32, u64)> = state
        .topic_registry
        .topics
        .iter()
        .map(|(name, info)| (name.clone(), info.partition_count, info.created_at_ms))
        .collect();
    let topic_set: HashSet<Arc<str>> = topics.iter().map(|(n, _, _)| n.clone()).collect();
    let brokers: Vec<(i32, BrokerStatus)> = state
        .broker_domain
        .brokers
        .values()
        .map(|b| (b.broker_id, b.status.clone()))
        .collect();
    let version = state.version;
    drop(state);

    ControlSnapshot {
        topics,
        topic_set,
        brokers,
        version,
    }
}

/// Seed every (topic, partition) the snapshot says should exist on `shard_idx`
/// but the shard SM hasn't applied yet.
///
/// The shard SM's `InitPartition` arm is idempotent: if the partition entry
/// already exists, it returns `PartitionAlreadyExists` and the call is a
/// cheap no-op. We pre-filter against the local shard SM read anyway so a
/// fully-converged shard issues zero proposals per tick (every "missing
/// partition" check returns false).
async fn propagate_topic_init(
    cluster: &Arc<RaftCluster>,
    shard_idx: ShardId,
    snapshot: &ControlSnapshot,
) {
    // Pre-compute which partitions are already present on the shard.
    let existing = {
        let sm = cluster.shard(shard_idx).expect("shard exists").state_machine();
        let state = sm.state().await;
        state
            .partition_state
            .partitions
            .keys()
            .map(|(t, p)| (t.clone(), *p))
            .collect::<HashSet<(Arc<str>, i32)>>()
    };

    for (topic_name, partition_count, created_at_ms) in &snapshot.topics {
        // Only this shard owns this topic. The router pin guarantees every
        // partition of a topic lives on the same shard, so we never split a
        // topic across shards on seeding.
        if cluster.router().shard_for_topic(topic_name) != shard_idx {
            continue;
        }
        for partition in 0..*partition_count {
            let key = (topic_name.clone(), partition);
            if existing.contains(&key) {
                continue;
            }
            let cmd = ShardCommand::Partition(PartitionStateCommand::InitPartition {
                topic: topic_name.to_string(),
                partition,
                created_at_ms: *created_at_ms,
            });
            // Errors here are routine during a leader handoff — the
            // proposal raced an election, the next tick retries on
            // whichever broker now holds leadership. Log at debug so
            // routine reconvergence doesn't fill the log.
            if let Err(e) = cluster.write_shard(shard_idx, cmd).await {
                debug!(
                    shard = shard_idx,
                    topic = %topic_name,
                    partition,
                    error = %e,
                    "reconciler: InitPartition propose failed (will retry next tick)"
                );
            }
        }
    }
}

/// Purge per-partition state on `shard_idx` for any topic that no longer
/// exists in control's registry.
///
/// **Caveat — same-name re-create race**: if `CreateTopic("X") →
/// DeleteTopic("X") → CreateTopic("X")` happens within one reconciler tick,
/// the purge sweep observes the topic in control (the second create) and
/// does nothing, so stale `PartitionInfo` from the *first* create can
/// survive on the shard. Closing this race needs either a `created_at_ms`
/// fingerprint on `PartitionInfo` or an explicit tombstone TTL on control —
/// see the module docs.
async fn propagate_topic_purge(
    cluster: &Arc<RaftCluster>,
    shard_idx: ShardId,
    snapshot: &ControlSnapshot,
) {
    let to_purge: Vec<(String, i32)> = {
        let sm = cluster.shard(shard_idx).expect("shard exists").state_machine();
        let state = sm.state().await;
        state
            .partition_state
            .partitions
            .keys()
            .filter(|(topic, _)| !snapshot.topic_set.contains(topic))
            .map(|(t, p)| (t.to_string(), *p))
            .collect()
    };

    for (topic, partition) in to_purge {
        let cmd = ShardCommand::Partition(PartitionStateCommand::PurgePartition {
            topic: topic.clone(),
            partition,
        });
        if let Err(e) = cluster.write_shard(shard_idx, cmd).await {
            debug!(
                shard = shard_idx,
                topic,
                partition,
                error = %e,
                "reconciler: PurgePartition propose failed (will retry next tick)"
            );
        }
    }
}

/// Sync the shard's broker-liveness shadow with control's broker registry.
///
/// Pre-filters against the shard's existing shadow so a converged shard
/// issues zero proposals per tick. Without this filter the reconciler
/// would propose `UpdateBrokerLivenessShadow` for every broker on every
/// tick — N proposals/tick/shard for no observable change.
async fn propagate_broker_shadow(
    cluster: &Arc<RaftCluster>,
    shard_idx: ShardId,
    snapshot: &ControlSnapshot,
) {
    // Snapshot the existing shadow so the propose loop runs without the
    // shard SM read lock held.
    let shadow = {
        let sm = cluster.shard(shard_idx).expect("shard exists").state_machine();
        let state = sm.state().await;
        state.broker_liveness_shadow.clone()
    };

    for (broker_id, status) in &snapshot.brokers {
        let needs_update = match shadow.get(broker_id) {
            None => true,
            Some(rec) => &rec.status != status,
        };
        if !needs_update {
            continue;
        }
        let cmd = ShardCommand::UpdateBrokerLivenessShadow {
            broker_id: *broker_id,
            status: status.clone(),
            // `snapshot.version` is control's SM apply count at read time.
            // Two reconcilers reading the same tick attach the same
            // version (idempotent at the shard's stale-write filter); two
            // reconcilers reading different ticks attach different
            // versions and the later one wins.
            version: snapshot.version,
        };
        if let Err(e) = cluster.write_shard(shard_idx, cmd).await {
            // Same as the topic propagations: failures here are routine
            // during leader churn and will retry next tick.
            warn!(
                shard = shard_idx,
                broker = broker_id,
                error = %e,
                "reconciler: UpdateBrokerLivenessShadow propose failed"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    //! End-to-end behaviour is exercised in
    //! `tests/raft_reconciler_tests.rs` (which requires a real
    //! `RaftCluster`). The unit tests here cover only pieces that don't
    //! need a live cluster — currently the snapshot extraction is too
    //! tightly coupled to the SM types to test without one, so this
    //! module stays light. The integration suite is where the
    //! reconvergence guarantees are pinned.

    #[test]
    fn default_interval_is_200ms() {
        // The 200ms cadence is part of the sharding plan's contract.
        // Pinning it here means a refactor that bumps it has to come with
        // a deliberate decision about latency budget and write rate.
        assert_eq!(super::DEFAULT_RECONCILE_INTERVAL.as_millis(), 200);
    }
}
