//! Raft state machine for cluster coordination.
//!
//! The state machine holds all coordination state and applies commands
//! to produce deterministic state transitions.

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::commands::{CoordinationCommand, CoordinationResponse};
use super::domains::{
    BrokerDomainState, GroupDomainState, PartitionDomainState, ProducerDomainState,
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
}

/// The state machine wrapper for coordination.
#[derive(Clone)]
pub struct CoordinationStateMachine {
    /// The current state.
    state: Arc<RwLock<CoordinationState>>,
}

impl CoordinationStateMachine {
    /// Create a new state machine.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(CoordinationState::default())),
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
                CoordinationResponse::BrokerDomainResponse(state.broker_domain.apply(cmd))
            }

            CoordinationCommand::PartitionDomain(cmd) => {
                CoordinationResponse::PartitionDomainResponse(state.partition_domain.apply(cmd))
            }

            CoordinationCommand::GroupDomain(cmd) => {
                CoordinationResponse::GroupDomainResponse(state.group_domain.apply(cmd))
            }

            CoordinationCommand::ProducerDomain(cmd) => {
                CoordinationResponse::ProducerDomainResponse(state.producer_domain.apply(cmd))
            }

            CoordinationCommand::TransferDomain(cmd) => {
                // Transfer domain requires cross-domain access
                // Clone broker states to avoid borrow checker issues
                let broker_states = state.broker_domain.brokers.clone();

                let is_broker_active = |broker_id: i32| {
                    broker_states
                        .get(&broker_id)
                        .map(|b| b.status == super::domains::BrokerStatus::Active)
                        .unwrap_or(false)
                };

                let broker_status =
                    |broker_id: i32| broker_states.get(&broker_id).map(|b| b.status.clone());

                // Temporarily take ownership to work around borrow checker
                let mut transfer_domain = std::mem::take(&mut state.transfer_domain);
                let response = transfer_domain.apply_with_context(
                    cmd,
                    is_broker_active,
                    &mut state.partition_domain.partitions,
                    broker_status,
                );
                state.transfer_domain = transfer_domain;

                CoordinationResponse::TransferDomainResponse(response)
            }
        }
    }

    /// Create a snapshot of the current state for persistence.
    pub async fn snapshot(&self) -> Vec<u8> {
        let state = self.state.read().await;
        bincode::serialize(&*state).expect("Failed to serialize state")
    }

    /// Restore state from a snapshot.
    pub async fn restore(&self, snapshot: &[u8]) {
        let restored_state: CoordinationState =
            bincode::deserialize(snapshot).expect("Failed to deserialize state");
        *self.state.write().await = restored_state;
    }
}

impl Default for CoordinationStateMachine {
    fn default() -> Self {
        Self::new()
    }
}
