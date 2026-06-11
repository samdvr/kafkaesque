//! Raft state machine for cluster coordination.
//!
//! The state machine holds all coordination state and applies commands
//! to produce deterministic state transitions.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::commands::{CoordinationCommand, CoordinationResponse};
use super::domains::{
    AclDomainState, BrokerDomainState, GroupDomainState, PartitionDomainState, ProducerDomainState,
    TransferDomainState,
};

/// The complete coordination state managed by Raft.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CoordinationState {
    /// State version (incremented on each change).
    pub version: u64,

    /// Broker domain state (registration, heartbeat, fencing).
    #[serde(default)]
    pub broker_domain: BrokerDomainState,

    /// Partition domain state (topics, partitions, ownership).
    #[serde(default)]
    pub partition_domain: PartitionDomainState,

    /// Consumer group domain state (groups, members, offsets).
    #[serde(default)]
    pub group_domain: GroupDomainState,

    /// Producer domain state (ID allocation, idempotency).
    #[serde(default)]
    pub producer_domain: ProducerDomainState,

    /// Transfer domain state (partition transfers, failover).
    #[serde(default)]
    pub transfer_domain: TransferDomainState,

    /// ACL domain state.
    ///
    /// `#[serde(default)]` keeps existing snapshots loadable even though
    /// they predate this field — the fall-back is an empty rule set, which
    /// is exactly what older deployments had implicitly.
    #[serde(default)]
    pub acl_domain: AclDomainState,

    /// Replicated, monotonic lease clock for the partition domain, in ms
    /// since the UNIX epoch. Advanced to `max(clock, cmd.timestamp_ms)` on
    /// every timestamp-carrying partition command; all lease grant/renew/
    /// expiry comparisons use the clamped value so replicas stay
    /// deterministic and lease time never moves backward.
    ///
    /// IMPORTANT: this field must remain the LAST field of this struct.
    /// Snapshots are encoded with postcard (not self-describing); keeping
    /// new fields at the tail means (a) old snapshot bytes fail to decode
    /// here with a clean EOF, which `deserialize_state` handles via the
    /// legacy fallback, and (b) old binaries can still decode new snapshot
    /// bytes because postcard ignores trailing bytes.
    #[serde(default)]
    pub lease_clock_ms: u64,
}

/// The pre-`lease_clock_ms` snapshot layout, used as a decode fallback for
/// snapshots written before the lease clock existed. Field order and types
/// must mirror `CoordinationState` exactly (minus the trailing clock).
#[derive(Deserialize)]
struct LegacyCoordinationState {
    version: u64,
    #[serde(default)]
    broker_domain: BrokerDomainState,
    #[serde(default)]
    partition_domain: PartitionDomainState,
    #[serde(default)]
    group_domain: GroupDomainState,
    #[serde(default)]
    producer_domain: ProducerDomainState,
    #[serde(default)]
    transfer_domain: TransferDomainState,
    #[serde(default)]
    acl_domain: AclDomainState,
}

impl From<LegacyCoordinationState> for CoordinationState {
    fn from(legacy: LegacyCoordinationState) -> Self {
        Self {
            version: legacy.version,
            broker_domain: legacy.broker_domain,
            partition_domain: legacy.partition_domain,
            group_domain: legacy.group_domain,
            producer_domain: legacy.producer_domain,
            transfer_domain: legacy.transfer_domain,
            acl_domain: legacy.acl_domain,
            // Legacy snapshots predate the lease clock; it starts at 0 and
            // catches up monotonically from the next applied command.
            lease_clock_ms: 0,
        }
    }
}

/// Type alias for a heartbeat-applied callback. Invoked once per
/// `BrokerCommand::Heartbeat` after the state mutation lands. The
/// argument is the broker id that just heartbeated, so a local
/// failure detector can refresh its liveness map.
pub type HeartbeatHook = Arc<dyn Fn(i32) + Send + Sync>;

/// What to invalidate in the local partition-owner cache after an applied
/// command changes ownership. Fired on every replica so read-path routing
/// (`owns_partition_for_read`) cannot serve stale ownership after a
/// transfer or lease expiry that this broker did not initiate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnershipCacheInvalidation {
    Partition { topic: String, partition: i32 },
    All,
}

/// Callback fired after applied commands that change partition ownership.
pub type OwnershipChangeHook = Arc<dyn Fn(OwnershipCacheInvalidation) + Send + Sync>;

/// The state machine wrapper for coordination.
#[derive(Clone)]
pub struct CoordinationStateMachine {
    /// The current state.
    state: Arc<RwLock<CoordinationState>>,
    /// Optional sink fired on every applied broker heartbeat. Used by
    /// the partition manager to feed `RebalanceCoordinator::record_heartbeat`
    /// so fast failover sees liveness from all brokers, not just self.
    /// Set once at startup; read on every apply.
    heartbeat_hook: Arc<std::sync::OnceLock<HeartbeatHook>>,
    /// Optional sink fired when an applied command changes partition
    /// ownership. Used to invalidate the local owner cache on every
    /// replica, not only the broker that initiated the transfer.
    ownership_change_hook: Arc<std::sync::OnceLock<OwnershipChangeHook>>,
}

impl CoordinationStateMachine {
    /// Create a new state machine.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(CoordinationState::default())),
            heartbeat_hook: Arc::new(std::sync::OnceLock::new()),
            ownership_change_hook: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Install a callback that fires after each broker-heartbeat command
    /// is applied. Idempotent: the hook is set once; subsequent calls are
    /// silently ignored. Used to plumb broker liveness into the local
    /// `RebalanceCoordinator` for fast failover.
    pub fn set_heartbeat_hook(&self, hook: HeartbeatHook) {
        let _ = self.heartbeat_hook.set(hook);
    }

    /// Install a callback that fires after applied commands change partition
    /// ownership. Idempotent: set once at startup. Every replica should
    /// install this so read-path owner caches stay consistent with the SM.
    pub fn set_ownership_change_hook(&self, hook: OwnershipChangeHook) {
        let _ = self.ownership_change_hook.set(hook);
    }

    fn fire_ownership_change_hook(&self, invalidation: OwnershipCacheInvalidation) {
        if let Some(hook) = self.ownership_change_hook.get() {
            hook(invalidation);
        }
    }

    /// Get a read-only reference to the current state.
    pub async fn state(&self) -> tokio::sync::RwLockReadGuard<'_, CoordinationState> {
        self.state.read().await
    }

    /// Get the state Arc for cloning.
    pub fn state_arc(&self) -> Arc<RwLock<CoordinationState>> {
        self.state.clone()
    }

    /// Apply a command to the state machine.
    pub async fn apply_command(&self, command: CoordinationCommand) -> CoordinationResponse {
        let mut state = self.state.write().await;
        state.version += 1;

        match command {
            CoordinationCommand::Noop => CoordinationResponse::Ok,

            CoordinationCommand::BrokerDomain(cmd) => {
                // Capture the broker_id before `cmd` is moved into apply,
                // so the heartbeat hook below can pass it to the local
                // failure detector.
                let heartbeat_broker =
                    if let super::domains::BrokerCommand::Heartbeat { broker_id, .. } = &cmd {
                        Some(*broker_id)
                    } else {
                        None
                    };
                let response =
                    CoordinationResponse::BrokerDomainResponse(state.broker_domain.apply(cmd));
                if let Some(broker_id) = heartbeat_broker
                    && let Some(hook) = self.heartbeat_hook.get()
                {
                    hook(broker_id);
                }
                response
            }

            CoordinationCommand::PartitionDomain(cmd) => {
                // Cross-domain fencing gate: a broker that is not Active
                // (fenced after being marked failed, or shutting down) must
                // not be able to acquire partitions or renew leases. Without
                // this check a fenced-but-still-running broker could win back
                // ownership it just lost and resume writing (split-brain).
                let requesting_broker = match &cmd {
                    super::domains::PartitionCommand::AcquirePartition { broker_id, .. }
                    | super::domains::PartitionCommand::RenewLease { broker_id, .. } => {
                        Some(*broker_id)
                    }
                    _ => None,
                };
                if let Some(broker_id) = requesting_broker
                    && let Some(broker) = state.broker_domain.brokers.get(&broker_id)
                    && broker.status != super::domains::BrokerStatus::Active
                {
                    return CoordinationResponse::PartitionDomainResponse(
                        super::domains::PartitionResponse::BrokerNotActive { broker_id },
                    );
                }
                // Disjoint borrows of `partition_domain` and `lease_clock_ms`.
                let state = &mut *state;
                let response =
                    state.partition_domain.apply(cmd.clone(), &mut state.lease_clock_ms);
                if let Some(invalidation) = ownership_invalidation_for_partition(&cmd, &response) {
                    self.fire_ownership_change_hook(invalidation);
                }
                CoordinationResponse::PartitionDomainResponse(response)
            }

            CoordinationCommand::GroupDomain(cmd) => {
                CoordinationResponse::GroupDomainResponse(state.group_domain.apply(cmd))
            }

            CoordinationCommand::ProducerDomain(cmd) => {
                CoordinationResponse::ProducerDomainResponse(state.producer_domain.apply(cmd))
            }

            CoordinationCommand::TransferDomain(cmd) => {
                // Transfer domain requires cross-domain access: it reads and
                // *mutates* broker state (fencing on MarkBrokerFailed) and
                // partition state (ownership transfers / releases) in the
                // same applied command. The three borrows are of disjoint
                // fields, so no cloning is needed.
                let state = &mut *state;
                let response = state.transfer_domain.apply_with_context(
                    cmd.clone(),
                    &mut state.broker_domain.brokers,
                    &mut state.partition_domain.partitions,
                );
                if let Some(invalidation) = ownership_invalidation_for_transfer(&response) {
                    self.fire_ownership_change_hook(invalidation);
                }

                CoordinationResponse::TransferDomainResponse(response)
            }

            CoordinationCommand::AclDomain(cmd) => {
                CoordinationResponse::AclDomainResponse(state.acl_domain.apply(cmd))
            }
        }
    }

    /// Create a snapshot of the current state for persistence.
    pub async fn snapshot(&self) -> Vec<u8> {
        let state = self.state.read().await;
        postcard::to_stdvec(&*state).expect("Failed to serialize state")
    }

    /// Decode snapshot bytes into a [`CoordinationState`] without mutating
    /// anything.
    ///
    /// Tries the current layout first; on failure falls back to the legacy
    /// (pre-`lease_clock_ms`) layout. Returns an `InvalidData` error on
    /// corrupt input — it NEVER panics, so callers can treat a corrupt
    /// snapshot as an error (storage propagates a `StorageError` to
    /// openraft) instead of aborting the process.
    pub fn deserialize_state(snapshot: &[u8]) -> std::io::Result<CoordinationState> {
        match postcard::from_bytes::<CoordinationState>(snapshot) {
            Ok(state) => Ok(state),
            Err(current_err) => match postcard::from_bytes::<LegacyCoordinationState>(snapshot) {
                Ok(legacy) => {
                    tracing::info!(
                        "Loaded legacy (pre-lease-clock) snapshot layout; \
                         lease clock starts at 0 and catches up monotonically"
                    );
                    Ok(legacy.into())
                }
                Err(legacy_err) => Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "snapshot deserialize failed (current layout: {}; legacy layout: {})",
                        current_err, legacy_err
                    ),
                )),
            },
        }
    }

    /// Replace the entire state with an already-validated snapshot state.
    pub async fn replace_state(&self, new_state: CoordinationState) {
        *self.state.write().await = new_state;
    }

    /// Restore state from snapshot bytes, validating BEFORE mutating.
    ///
    /// On corrupt input the in-memory state is left untouched and an error
    /// is returned; the process is never aborted.
    pub async fn try_restore(&self, snapshot: &[u8]) -> std::io::Result<()> {
        let restored_state = Self::deserialize_state(snapshot)?;
        self.replace_state(restored_state).await;
        Ok(())
    }

    /// Restore state from a snapshot, ignoring corrupt input.
    ///
    /// Kept for callers that cannot handle an error return. On corrupt
    /// bytes this logs loudly and leaves the state machine unchanged
    /// (it used to `panic!`). New code should prefer [`Self::try_restore`].
    pub async fn restore(&self, snapshot: &[u8]) {
        if let Err(e) = self.try_restore(snapshot).await {
            tracing::error!(
                error = %e,
                "CORRUPTION: snapshot bytes failed to deserialize; \
                 state machine left unchanged"
            );
        }
    }
}

fn ownership_invalidation_for_partition(
    cmd: &super::domains::PartitionCommand,
    response: &super::domains::PartitionResponse,
) -> Option<OwnershipCacheInvalidation> {
    use super::domains::{PartitionCommand, PartitionResponse};

    match response {
        PartitionResponse::PartitionAcquired { topic, partition, .. }
        | PartitionResponse::PartitionReleased { topic, partition } => {
            Some(OwnershipCacheInvalidation::Partition {
                topic: topic.clone(),
                partition: *partition,
            })
        }
        PartitionResponse::LeasesExpired { count } if *count > 0 => {
            Some(OwnershipCacheInvalidation::All)
        }
        // A failed acquire against an active owner means our cached view
        // of who owns this partition may be stale.
        PartitionResponse::PartitionOwnedByOther { topic, partition, .. }
            if matches!(cmd, PartitionCommand::AcquirePartition { .. }) =>
        {
            Some(OwnershipCacheInvalidation::Partition {
                topic: topic.clone(),
                partition: *partition,
            })
        }
        _ => None,
    }
}

fn ownership_invalidation_for_transfer(
    response: &super::domains::TransferResponse,
) -> Option<OwnershipCacheInvalidation> {
    use super::domains::TransferResponse;

    match response {
        TransferResponse::PartitionTransferred { topic, partition, .. } => {
            Some(OwnershipCacheInvalidation::Partition {
                topic: topic.clone(),
                partition: *partition,
            })
        }
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

impl Default for CoordinationStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::super::domains::{PartitionCommand, PartitionResponse, TransferResponse};
    use super::*;

    #[test]
    fn ownership_invalidation_on_partition_acquire() {
        let inv = ownership_invalidation_for_partition(
            &PartitionCommand::AcquirePartition {
                topic: "t".to_string(),
                partition: 1,
                broker_id: 2,
                lease_duration_ms: 60_000,
                timestamp_ms: 1,
            },
            &PartitionResponse::PartitionAcquired {
                topic: "t".to_string(),
                partition: 1,
                leader_epoch: 1,
                lease_expires_at_ms: 61_000,
            },
        );
        assert_eq!(
            inv,
            Some(OwnershipCacheInvalidation::Partition {
                topic: "t".to_string(),
                partition: 1,
            })
        );
    }

    #[test]
    fn ownership_invalidation_on_lease_expiry_sweep() {
        let inv = ownership_invalidation_for_partition(
            &PartitionCommand::ExpireLeases {
                current_time_ms: 100,
            },
            &PartitionResponse::LeasesExpired { count: 3 },
        );
        assert_eq!(inv, Some(OwnershipCacheInvalidation::All));
    }

    #[test]
    fn ownership_invalidation_on_broker_failure() {
        let inv = ownership_invalidation_for_transfer(&TransferResponse::BrokerMarkedFailed {
            broker_id: 2,
            partitions_affected: 5,
            released_partitions: vec![("t".to_string(), 0)],
        });
        assert_eq!(inv, Some(OwnershipCacheInvalidation::All));
    }

    #[test]
    fn no_invalidation_on_noop_lease_sweep() {
        let inv = ownership_invalidation_for_partition(
            &PartitionCommand::ExpireLeases {
                current_time_ms: 100,
            },
            &PartitionResponse::LeasesExpired { count: 0 },
        );
        assert!(inv.is_none());
    }

    #[test]
    fn ownership_invalidation_on_transfer() {
        let inv = ownership_invalidation_for_transfer(&TransferResponse::PartitionTransferred {
            topic: "orders".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            new_epoch: 3,
            lease_expires_at_ms: 99_000,
        });
        assert_eq!(
            inv,
            Some(OwnershipCacheInvalidation::Partition {
                topic: "orders".to_string(),
                partition: 0,
            })
        );
    }

    #[test]
    fn ownership_invalidation_on_failed_acquire_against_active_owner() {
        let inv = ownership_invalidation_for_partition(
            &PartitionCommand::AcquirePartition {
                topic: "t".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 60_000,
                timestamp_ms: 1,
            },
            &PartitionResponse::PartitionOwnedByOther {
                topic: "t".to_string(),
                partition: 0,
                owner: 1,
            },
        );
        assert!(inv.is_some());
    }
}
