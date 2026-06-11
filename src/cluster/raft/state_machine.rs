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
}

/// Type alias for a heartbeat-applied callback. Invoked once per
/// `BrokerCommand::Heartbeat` after the state mutation lands. The
/// argument is the broker id that just heartbeated, so a local
/// failure detector can refresh its liveness map.
pub type HeartbeatHook = Arc<dyn Fn(i32) + Send + Sync>;

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
}

impl CoordinationStateMachine {
    /// Create a new state machine.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(CoordinationState::default())),
            heartbeat_hook: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// Install a callback that fires after each broker-heartbeat command
    /// is applied. Idempotent: the hook is set once; subsequent calls are
    /// silently ignored. Used to plumb broker liveness into the local
    /// `RebalanceCoordinator` for fast failover.
    pub fn set_heartbeat_hook(&self, hook: HeartbeatHook) {
        let _ = self.heartbeat_hook.set(hook);
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
                CoordinationResponse::PartitionDomainResponse(state.partition_domain.apply(cmd))
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
                    cmd,
                    &mut state.broker_domain.brokers,
                    &mut state.partition_domain.partitions,
                );

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

    /// Restore state from a snapshot.
    pub async fn restore(&self, snapshot: &[u8]) {
        let restored_state: CoordinationState =
            postcard::from_bytes(snapshot).expect("Failed to deserialize state");
        *self.state.write().await = restored_state;
    }
}

impl Default for CoordinationStateMachine {
    fn default() -> Self {
        Self::new()
    }
}
