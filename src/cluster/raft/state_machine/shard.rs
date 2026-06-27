//! Shard-group state machine.
//!
//! Each shard owns a slice of three hot-write domains, partitioned by hash:
//!
//! - **Per-partition lease state** ([`PartitionStateDomain`]) — keyed by
//!   `hash(topic) % N`. Hosts the lease grant/renew/release/expire path.
//! - **Consumer groups + offsets** ([`GroupDomainState`]) — keyed by
//!   `hash(group_id) % N`.
//! - **Per-producer idempotency state** ([`ProducerDomainState`]) — keyed by
//!   `hash(producer_id) % N`. Producer-id *allocation* lives on control.
//! - **Transfers** ([`TransferDomainState`]) — single-shard transfers only;
//!   cross-shard moves are out of scope.
//!
//! The shard SM also keeps a **broker-liveness shadow**: a local copy of the
//! control group's broker status, propagated by the reconciler. The shadow
//! lets the cross-domain fencing gate (`AcquirePartition` / `RenewLease` /
//! `RenewLeases`) decide locally without a control round-trip on the hot
//! path.

#![allow(dead_code)] // wired in subsequent migration steps

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::commands::{ShardCommand, ShardResponse};
use super::super::domains::{
    BrokerStatus, GroupDomainState, PartitionInfo, PartitionStateCommand, PartitionStateDomain,
    PartitionStateResponse, ProducerDomainState, TransferDomainState,
};
use super::super::types::ShardId;
use super::{OwnershipCacheInvalidation, OwnershipChangeHook};

/// Subset of `BrokerInfo` the shard SM needs for its fencing gate.
///
/// We replicate this from control rather than calling across because every
/// cross-group hop on the hot path is a latency tax. `version` is monotonic
/// and reconciler-issued: a stale `UpdateBrokerLivenessShadow` proposal
/// (e.g. retry of an older view) is filtered by `version <= stored.version`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BrokerLivenessRecord {
    pub status: BrokerStatus,
    pub version: u64,
}

/// Replicated state for a single shard group.
///
/// IMPORTANT: field order is part of the on-disk snapshot contract. New
/// fields go at the *tail* with `#[serde(default)]`.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ShardState {
    /// Total number of commands applied to this state machine.
    pub version: u64,

    /// Per-partition lease/ownership state for partitions whose
    /// `hash(topic) % N == shard_id`.
    #[serde(default)]
    pub partition_state: PartitionStateDomain,

    /// Consumer groups (members, offsets, assignments) for groups whose
    /// `hash(group_id) % N == shard_id`.
    #[serde(default)]
    pub group_domain: GroupDomainState,

    /// Per-producer idempotency state for producers whose
    /// `hash(producer_id) % N == shard_id`. Allocation of new producer ids
    /// happens on control; this domain only handles the per-id state that
    /// follows.
    #[serde(default)]
    pub producer_state: ProducerDomainState,

    /// Single-shard partition transfers.
    #[serde(default)]
    pub transfer_domain: TransferDomainState,

    /// Local copy of broker status, propagated from control by the
    /// reconciler. Used by the shard's cross-domain fencing gate so a fenced
    /// broker can't acquire a partition or renew a lease, without a control
    /// round-trip on the hot path.
    #[serde(default)]
    pub broker_liveness_shadow: HashMap<i32, BrokerLivenessRecord>,

    /// Replicated, monotonic lease clock for this shard.
    /// Independent of the control clock and of every other shard's clock.
    #[serde(default)]
    pub lease_clock_ms: u64,

    /// Identifier of the shard this state machine serves. Snapshotted so a
    /// restored SM can sanity-check its loaded `shard_id` against its
    /// configured one — guards against accidentally loading shard-3's
    /// snapshot into a shard-5 process.
    #[serde(default)]
    pub shard_id: ShardId,
}

/// State machine wrapper for a shard group.
#[derive(Clone)]
pub struct ShardStateMachine {
    state: Arc<RwLock<ShardState>>,
    /// Shard id this SM serves. Set at construction; on `try_restore`,
    /// validated against the snapshot's `shard_id`.
    shard_id: ShardId,
    /// Optional sink fired when an applied shard command changes ownership.
    /// Every replica installs this so read-path owner caches stay in sync
    /// with the SM, not just the broker that initiated a transfer.
    ownership_change_hook: Arc<std::sync::OnceLock<OwnershipChangeHook>>,
}

impl ShardStateMachine {
    pub fn new(shard_id: ShardId) -> Self {
        Self {
            state: Arc::new(RwLock::new(ShardState {
                shard_id,
                ..ShardState::default()
            })),
            shard_id,
            ownership_change_hook: Arc::new(std::sync::OnceLock::new()),
        }
    }

    pub fn shard_id(&self) -> ShardId {
        self.shard_id
    }

    pub fn set_ownership_change_hook(&self, hook: OwnershipChangeHook) {
        let _ = self.ownership_change_hook.set(hook);
    }

    fn fire_ownership_change_hook(&self, invalidation: OwnershipCacheInvalidation) {
        if let Some(hook) = self.ownership_change_hook.get() {
            hook(invalidation);
        }
    }

    pub async fn state(&self) -> tokio::sync::RwLockReadGuard<'_, ShardState> {
        self.state.read().await
    }

    pub fn state_arc(&self) -> Arc<RwLock<ShardState>> {
        self.state.clone()
    }

    /// Apply a shard command. The hook side effects (ownership-cache
    /// invalidation) fire AFTER the inner write guard is dropped — see
    /// [`legacy::CoordinationStateMachine::apply_command`] for the rationale
    /// (panicking under the lock would corrupt every concurrent reader).
    pub async fn apply_command(&self, command: ShardCommand) -> ShardResponse {
        let mut state = self.state.write().await;
        state.version += 1;

        match command {
            ShardCommand::Noop => ShardResponse::Ok,

            ShardCommand::Partition(cmd) => {
                // Cross-domain fencing gate: a broker that is not Active in
                // our local liveness shadow must not be able to acquire
                // partitions or renew leases. The shadow is reconciler-fed
                // from control; if it lags behind, we err on the side of
                // *blocking* (the broker reappears as Active on the next
                // tick).
                let requesting_broker = match &cmd {
                    PartitionStateCommand::AcquirePartition { broker_id, .. }
                    | PartitionStateCommand::RenewLease { broker_id, .. }
                    | PartitionStateCommand::RenewLeases { broker_id, .. } => Some(*broker_id),
                    _ => None,
                };
                if let Some(broker_id) = requesting_broker
                    && let Some(record) = state.broker_liveness_shadow.get(&broker_id)
                    && record.status != BrokerStatus::Active
                {
                    return ShardResponse::Partition(PartitionStateResponse::BrokerNotActive {
                        broker_id,
                    });
                }
                let state_mut = &mut *state;
                let response = state_mut
                    .partition_state
                    .apply(cmd.clone(), &mut state_mut.lease_clock_ms);
                let invalidation = ownership_invalidation_for_partition(&cmd, &response);
                drop(state);
                if let Some(invalidation) = invalidation {
                    self.fire_ownership_change_hook(invalidation);
                }
                ShardResponse::Partition(response)
            }

            ShardCommand::Group(cmd) => ShardResponse::Group(state.group_domain.apply(cmd)),

            ShardCommand::Producer(cmd) => ShardResponse::Producer(state.producer_state.apply(cmd)),

            ShardCommand::Transfer(cmd) => {
                let state_mut = &mut *state;
                // The transfer domain wants `&mut HashMap<i32, BrokerInfo>`
                // but we only carry a liveness shadow here. Build a thin
                // synthetic broker map from the shadow so transfers can run
                // their is_active check locally; the shadow has the only
                // field the transfer domain reads (status).
                let mut shadow_brokers: HashMap<i32, super::super::domains::BrokerInfo> = state_mut
                    .broker_liveness_shadow
                    .iter()
                    .map(|(id, rec)| {
                        (
                            *id,
                            super::super::domains::BrokerInfo {
                                broker_id: *id,
                                host: String::new(),
                                port: 0,
                                registered_at_ms: 0,
                                last_heartbeat_ms: 0,
                                status: rec.status.clone(),
                                reported_timestamp_ms: 0,
                            },
                        )
                    })
                    .collect();
                let response = state_mut.transfer_domain.apply_with_context(
                    cmd,
                    &mut shadow_brokers,
                    &mut state_mut.partition_state.partitions,
                    &mut state_mut.lease_clock_ms,
                );
                // We deliberately do NOT write back `shadow_brokers` — the
                // shard SM's broker view is authored only by the
                // reconciler. Any status change the transfer domain might
                // make is also made on control, which the reconciler will
                // then propagate back to us.
                let invalidation = ownership_invalidation_for_transfer(&response);
                drop(state);
                if let Some(invalidation) = invalidation {
                    self.fire_ownership_change_hook(invalidation);
                }
                ShardResponse::Transfer(response)
            }

            ShardCommand::UpdateBrokerLivenessShadow {
                broker_id,
                status,
                version,
            } => {
                // Stale-write filter: ignore reconciler retries that carry
                // an older view than what we already applied. Without this
                // a slow proposer could overwrite a fresher status.
                let entry = state.broker_liveness_shadow.entry(broker_id);
                use std::collections::hash_map::Entry;
                match entry {
                    Entry::Occupied(mut occ) => {
                        if version > occ.get().version {
                            occ.insert(BrokerLivenessRecord { status, version });
                        }
                    }
                    Entry::Vacant(vac) => {
                        vac.insert(BrokerLivenessRecord { status, version });
                    }
                }
                ShardResponse::Ok
            }
        }
    }

    /// Encode the current state for snapshotting.
    pub async fn snapshot(&self) -> Vec<u8> {
        let state = self.state.read().await;
        postcard::to_stdvec(&*state).expect("ShardState must serialize")
    }

    /// Decode snapshot bytes into a [`ShardState`] without mutating
    /// anything. **Greenfield**: there is no legacy fallback path.
    pub fn deserialize_state(snapshot: &[u8]) -> std::io::Result<ShardState> {
        postcard::from_bytes::<ShardState>(snapshot).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("shard snapshot deserialize failed: {}", e),
            )
        })
    }

    pub async fn replace_state(&self, new_state: ShardState) {
        *self.state.write().await = new_state;
    }

    /// Validate snapshot bytes BEFORE mutating the in-memory state. On
    /// corrupt input, OR on a `shard_id` mismatch, the in-memory state is
    /// left untouched and an error is returned.
    pub async fn try_restore(&self, snapshot: &[u8]) -> std::io::Result<()> {
        let restored = Self::deserialize_state(snapshot)?;
        if restored.shard_id != self.shard_id {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "shard snapshot shard_id mismatch: snapshot={}, expected={}",
                    restored.shard_id, self.shard_id
                ),
            ));
        }
        self.replace_state(restored).await;
        Ok(())
    }
}

fn ownership_invalidation_for_partition(
    cmd: &PartitionStateCommand,
    response: &PartitionStateResponse,
) -> Option<OwnershipCacheInvalidation> {
    match response {
        PartitionStateResponse::PartitionAcquired {
            topic, partition, ..
        }
        | PartitionStateResponse::PartitionReleased { topic, partition }
        | PartitionStateResponse::PartitionPurged { topic, partition } => {
            Some(OwnershipCacheInvalidation::Partition {
                topic: topic.clone(),
                partition: *partition,
            })
        }
        PartitionStateResponse::LeasesExpired { count } if *count > 0 => {
            Some(OwnershipCacheInvalidation::All)
        }
        // A failed acquire against an active owner means our cached view
        // of who owns this partition may be stale.
        PartitionStateResponse::PartitionOwnedByOther {
            topic, partition, ..
        } if matches!(cmd, PartitionStateCommand::AcquirePartition { .. }) => {
            Some(OwnershipCacheInvalidation::Partition {
                topic: topic.clone(),
                partition: *partition,
            })
        }
        _ => None,
    }
}

fn ownership_invalidation_for_transfer(
    response: &super::super::domains::TransferResponse,
) -> Option<OwnershipCacheInvalidation> {
    use super::super::domains::TransferResponse;

    match response {
        TransferResponse::PartitionTransferred {
            topic, partition, ..
        } => Some(OwnershipCacheInvalidation::Partition {
            topic: topic.clone(),
            partition: *partition,
        }),
        TransferResponse::BrokerMarkedFailed {
            partitions_affected,
            ..
        } if *partitions_affected > 0 => Some(OwnershipCacheInvalidation::All),
        TransferResponse::BatchTransferCompleted {
            successful_transfers,
            ..
        } if *successful_transfers > 0 => Some(OwnershipCacheInvalidation::All),
        _ => None,
    }
}

// `PartitionInfo` is referenced by the type system but otherwise unused
// inside the SM module right now; keeping the import grouped with the rest
// avoids touching this list when the reconciler wires up next.
#[allow(unused_imports)]
use PartitionInfo as _;

#[cfg(test)]
mod tests {
    use super::super::super::domains::PartitionStateCommand;
    use super::*;

    fn fresh(shard_id: ShardId) -> ShardStateMachine {
        ShardStateMachine::new(shard_id)
    }

    #[tokio::test]
    async fn noop_increments_version() {
        let sm = fresh(0);
        sm.apply_command(ShardCommand::Noop).await;
        sm.apply_command(ShardCommand::Noop).await;
        assert_eq!(sm.state().await.version, 2);
    }

    #[tokio::test]
    async fn init_then_acquire_partition_assigns_owner() {
        let sm = fresh(0);
        sm.apply_command(ShardCommand::Partition(
            PartitionStateCommand::InitPartition {
                topic: "t".into(),
                partition: 0,
                created_at_ms: 1,
            },
        ))
        .await;
        let resp = sm
            .apply_command(ShardCommand::Partition(
                PartitionStateCommand::AcquirePartition {
                    topic: "t".into(),
                    partition: 0,
                    broker_id: 1,
                    lease_duration_ms: 1000,
                    timestamp_ms: 100,
                },
            ))
            .await;
        assert!(matches!(
            resp,
            ShardResponse::Partition(PartitionStateResponse::PartitionAcquired { .. })
        ));
    }

    #[tokio::test]
    async fn fenced_broker_cannot_acquire_via_liveness_shadow() {
        let sm = fresh(0);
        // Reconciler-issued: broker 1 is fenced.
        sm.apply_command(ShardCommand::UpdateBrokerLivenessShadow {
            broker_id: 1,
            status: BrokerStatus::Fenced,
            version: 1,
        })
        .await;
        let resp = sm
            .apply_command(ShardCommand::Partition(
                PartitionStateCommand::AcquirePartition {
                    topic: "t".into(),
                    partition: 0,
                    broker_id: 1,
                    lease_duration_ms: 1000,
                    timestamp_ms: 1,
                },
            ))
            .await;
        assert!(matches!(
            resp,
            ShardResponse::Partition(PartitionStateResponse::BrokerNotActive { broker_id: 1 })
        ));
    }

    #[tokio::test]
    async fn liveness_shadow_ignores_stale_versions() {
        let sm = fresh(0);
        sm.apply_command(ShardCommand::UpdateBrokerLivenessShadow {
            broker_id: 1,
            status: BrokerStatus::Active,
            version: 5,
        })
        .await;
        // A retry of an older view must not flip the broker back to Fenced.
        sm.apply_command(ShardCommand::UpdateBrokerLivenessShadow {
            broker_id: 1,
            status: BrokerStatus::Fenced,
            version: 3,
        })
        .await;
        let state = sm.state().await;
        let r = state.broker_liveness_shadow.get(&1).unwrap();
        assert_eq!(r.status, BrokerStatus::Active);
        assert_eq!(r.version, 5);
    }

    #[tokio::test]
    async fn unknown_broker_is_treated_as_active_for_now() {
        // The reconciler has not yet reported anything about this broker —
        // the fence gate must NOT block. This mirrors the legacy SM's
        // behaviour where fencing was authoritatively decided in the same
        // group.
        let sm = fresh(0);
        let resp = sm
            .apply_command(ShardCommand::Partition(
                PartitionStateCommand::AcquirePartition {
                    topic: "t".into(),
                    partition: 0,
                    broker_id: 42,
                    lease_duration_ms: 1000,
                    timestamp_ms: 1,
                },
            ))
            .await;
        assert!(matches!(
            resp,
            ShardResponse::Partition(PartitionStateResponse::PartitionAcquired { .. })
        ));
    }

    #[tokio::test]
    async fn snapshot_roundtrip_preserves_state() {
        let sm = fresh(3);
        sm.apply_command(ShardCommand::Partition(
            PartitionStateCommand::InitPartition {
                topic: "snap".into(),
                partition: 1,
                created_at_ms: 1,
            },
        ))
        .await;
        sm.apply_command(ShardCommand::UpdateBrokerLivenessShadow {
            broker_id: 7,
            status: BrokerStatus::Active,
            version: 1,
        })
        .await;

        let bytes = sm.snapshot().await;
        let restored = fresh(3);
        restored.try_restore(&bytes).await.unwrap();
        let s = restored.state().await;
        assert_eq!(s.shard_id, 3);
        assert!(s.partition_state.get_partition("snap", 1).is_some());
        assert!(s.broker_liveness_shadow.contains_key(&7));
    }

    #[tokio::test]
    async fn restore_with_wrong_shard_id_errors_and_preserves_state() {
        let sm_3 = fresh(3);
        sm_3.apply_command(ShardCommand::Noop).await;
        let bytes = sm_3.snapshot().await;

        let sm_5 = fresh(5);
        sm_5.apply_command(ShardCommand::Noop).await;
        sm_5.apply_command(ShardCommand::Noop).await;
        let prev_version = sm_5.state().await.version;

        let err = sm_5.try_restore(&bytes).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        // Pre-existing state preserved.
        assert_eq!(sm_5.state().await.version, prev_version);
    }

    #[tokio::test]
    async fn restore_corrupt_bytes_returns_error() {
        let sm = fresh(0);
        let err = sm.try_restore(b"not a postcard frame").await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
