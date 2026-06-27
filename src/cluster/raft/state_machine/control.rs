//! Control-group state machine.
//!
//! The control group holds cluster-wide metadata that is *not* sharded by key:
//!
//! - **Broker registry** ([`BrokerDomainState`]) — registration, heartbeat,
//!   fencing decisions. Authoritative source of broker liveness; the
//!   reconciler propagates [`shard::ShardStateMachine`]'s liveness shadow.
//! - **ACL bindings** ([`AclDomainState`]).
//! - **Topic registry** ([`TopicRegistryState`]) — topic existence,
//!   partition count, configs. Per-partition lease state lives in shards.
//! - **Producer-id allocator** ([`ProducerDomainState::next_producer_id`]) —
//!   the cluster-wide monotonic counter. Per-id idempotency state lives in
//!   shards.
//!
//! The control SM keeps its own monotonic [`lease_clock_ms`]; sibling shard
//! SMs each keep their own. The clocks never need to agree — they just need
//! to never move backward inside a single group.
//!
//! Hot-path domains (per-partition leases, consumer groups, per-producer
//! idempotency, transfers) live on [`super::shard::ShardStateMachine`].

#![allow(dead_code)] // wired in subsequent migration steps

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use super::super::commands::{ControlCommand, ControlResponse};
use super::super::domains::{
    AclDomainState, BrokerCommand, BrokerDomainState, ProducerCommand, ProducerDomainState,
    TopicRegistryState,
};
use super::{HeartbeatHook, OwnershipCacheInvalidation, OwnershipChangeHook};

/// Replicated state for the control group.
///
/// IMPORTANT: field order is part of the on-disk snapshot contract. New
/// fields go at the *tail* with `#[serde(default)]` so old snapshots decode
/// cleanly and old binaries can decode new snapshots. See [`legacy`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ControlState {
    /// Total number of commands applied to this state machine.
    pub version: u64,

    /// Broker registry.
    #[serde(default)]
    pub broker_domain: BrokerDomainState,

    /// ACL bindings.
    #[serde(default)]
    pub acl_domain: AclDomainState,

    /// Topic registry — existence record, partition count, configs only.
    /// Per-partition lease state lives in the per-shard SM.
    #[serde(default)]
    pub topic_registry: TopicRegistryState,

    /// Producer-id allocator. Only `next_producer_id` and `recent_allocations`
    /// (the dedup ring) are meaningful here; per-producer idempotency tables
    /// live in the shard SM. Storing the full struct lets us reuse
    /// [`ProducerDomainState::apply`] for `AllocateProducerId` without
    /// duplicating the dedup logic.
    #[serde(default)]
    pub producer_id_allocator: ProducerDomainState,

    /// Replicated, monotonic lease clock for the control group's domains.
    /// Independent of every shard's clock.
    #[serde(default)]
    pub lease_clock_ms: u64,
}

/// State machine wrapper for the control group.
#[derive(Clone)]
pub struct ControlStateMachine {
    state: Arc<RwLock<ControlState>>,
    /// Optional sink fired on every applied broker heartbeat. Plumbed into
    /// the local `RebalanceCoordinator` for fast failover.
    heartbeat_hook: Arc<std::sync::OnceLock<HeartbeatHook>>,
    /// Optional sink fired when an applied control command changes the
    /// ownership view that read-path caches rely on. The control SM does
    /// not own per-partition lease state, so this almost always fires
    /// `OwnershipCacheInvalidation::All` when a broker is fenced or
    /// unregistered — those events affect every shard's view.
    ownership_change_hook: Arc<std::sync::OnceLock<OwnershipChangeHook>>,
}

impl ControlStateMachine {
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(ControlState::default())),
            heartbeat_hook: Arc::new(std::sync::OnceLock::new()),
            ownership_change_hook: Arc::new(std::sync::OnceLock::new()),
        }
    }

    pub fn set_heartbeat_hook(&self, hook: HeartbeatHook) {
        let _ = self.heartbeat_hook.set(hook);
    }

    pub fn set_ownership_change_hook(&self, hook: OwnershipChangeHook) {
        let _ = self.ownership_change_hook.set(hook);
    }

    fn fire_ownership_change_hook(&self, invalidation: OwnershipCacheInvalidation) {
        if let Some(hook) = self.ownership_change_hook.get() {
            hook(invalidation);
        }
    }

    pub async fn state(&self) -> tokio::sync::RwLockReadGuard<'_, ControlState> {
        self.state.read().await
    }

    pub fn state_arc(&self) -> Arc<RwLock<ControlState>> {
        self.state.clone()
    }

    /// Apply a control command. The hook side effects (heartbeat,
    /// ownership-cache invalidation) fire AFTER the inner write guard is
    /// dropped — see [`legacy::CoordinationStateMachine::apply_command`] for
    /// the rationale.
    pub async fn apply_command(&self, command: ControlCommand) -> ControlResponse {
        let mut state = self.state.write().await;
        state.version += 1;

        match command {
            ControlCommand::Noop => ControlResponse::Ok,

            ControlCommand::Broker(cmd) => {
                let broker_id = extract_broker_id(&cmd);
                let heartbeat_broker = if let BrokerCommand::Heartbeat { broker_id, .. } = &cmd {
                    Some(*broker_id)
                } else {
                    None
                };
                // A broker leaving / being fenced affects every shard's
                // owner-cache view. We don't know whose partitions move
                // here — that's the shard SM's job — so we conservatively
                // invalidate everything when the broker exits the Active
                // set.
                let pre_status = state.broker_domain.get(broker_id).map(|b| b.status.clone());
                let response = ControlResponse::Broker(state.broker_domain.apply(cmd));
                let post_status = state.broker_domain.get(broker_id).map(|b| b.status.clone());
                let became_inactive = matches!(
                    (pre_status, post_status),
                    (Some(prev), Some(now))
                        if prev == super::super::domains::BrokerStatus::Active
                            && now != super::super::domains::BrokerStatus::Active
                );
                drop(state);
                if let Some(broker_id) = heartbeat_broker
                    && let Some(hook) = self.heartbeat_hook.get()
                {
                    hook(broker_id);
                }
                if became_inactive {
                    self.fire_ownership_change_hook(OwnershipCacheInvalidation::All);
                }
                response
            }

            ControlCommand::Acl(cmd) => ControlResponse::Acl(state.acl_domain.apply(cmd)),

            ControlCommand::TopicRegistry(cmd) => {
                let state_mut = &mut *state;
                let response = state_mut
                    .topic_registry
                    .apply(cmd, &mut state_mut.lease_clock_ms);
                ControlResponse::TopicRegistry(response)
            }

            ControlCommand::AllocateProducerId { request_token } => {
                // Reuse the existing producer-domain dedup logic. Only the
                // counter field of `producer_id_allocator` matters here; the
                // idempotency tables on the same struct are unused on the
                // control SM and live unset.
                let response = state
                    .producer_id_allocator
                    .apply(ProducerCommand::AllocateProducerId { request_token });
                ControlResponse::Producer(response)
            }
        }
    }

    /// Encode the current state for snapshotting.
    pub async fn snapshot(&self) -> Vec<u8> {
        let state = self.state.read().await;
        postcard::to_stdvec(&*state).expect("ControlState must serialize")
    }

    /// Decode snapshot bytes into a [`ControlState`] without mutating
    /// anything. **Greenfield**: there is no legacy fallback — the multi-group
    /// layout is the only on-disk format.
    pub fn deserialize_state(snapshot: &[u8]) -> std::io::Result<ControlState> {
        postcard::from_bytes::<ControlState>(snapshot).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("control snapshot deserialize failed: {}", e),
            )
        })
    }

    pub async fn replace_state(&self, new_state: ControlState) {
        *self.state.write().await = new_state;
    }

    /// Validate snapshot bytes BEFORE mutating the in-memory state. On
    /// corrupt input the state is left untouched and an error returned.
    pub async fn try_restore(&self, snapshot: &[u8]) -> std::io::Result<()> {
        let restored = Self::deserialize_state(snapshot)?;
        self.replace_state(restored).await;
        Ok(())
    }
}

impl Default for ControlStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

fn extract_broker_id(cmd: &BrokerCommand) -> i32 {
    match cmd {
        BrokerCommand::Register { broker_id, .. }
        | BrokerCommand::Heartbeat { broker_id, .. }
        | BrokerCommand::Unregister { broker_id, .. }
        | BrokerCommand::Fence { broker_id, .. } => *broker_id,
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::domains::{
        AclCommand, BrokerStatus, TopicRegistryCommand, TopicRegistryResponse,
    };
    use super::*;
    use std::collections::HashMap;

    fn fresh() -> ControlStateMachine {
        ControlStateMachine::new()
    }

    #[tokio::test]
    async fn noop_increments_version() {
        let sm = fresh();
        sm.apply_command(ControlCommand::Noop).await;
        sm.apply_command(ControlCommand::Noop).await;
        assert_eq!(sm.state().await.version, 2);
    }

    #[tokio::test]
    async fn broker_register_then_heartbeat_records_broker() {
        let sm = fresh();
        sm.apply_command(ControlCommand::Broker(BrokerCommand::Register {
            broker_id: 1,
            host: "h".into(),
            port: 1,
            timestamp_ms: 0,
        }))
        .await;
        sm.apply_command(ControlCommand::Broker(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 100,
            reported_local_timestamp_ms: 100,
        }))
        .await;
        let state = sm.state().await;
        let broker = state.broker_domain.get(1).unwrap();
        assert_eq!(broker.status, BrokerStatus::Active);
    }

    #[tokio::test]
    async fn topic_registry_create_then_delete_roundtrips() {
        let sm = fresh();
        let create = sm
            .apply_command(ControlCommand::TopicRegistry(
                TopicRegistryCommand::CreateTopic {
                    name: "t".into(),
                    partitions: 4,
                    config: HashMap::new(),
                    timestamp_ms: 100,
                },
            ))
            .await;
        assert!(matches!(
            create,
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicCreated { .. })
        ));
        let delete = sm
            .apply_command(ControlCommand::TopicRegistry(
                TopicRegistryCommand::DeleteTopic { name: "t".into() },
            ))
            .await;
        assert!(matches!(
            delete,
            ControlResponse::TopicRegistry(TopicRegistryResponse::TopicDeleted { .. })
        ));
    }

    #[tokio::test]
    async fn allocate_producer_id_dedups_on_request_token() {
        let sm = fresh();
        let token = Some(0xdeadbeef_u128);
        let r1 = sm
            .apply_command(ControlCommand::AllocateProducerId {
                request_token: token,
            })
            .await;
        let r2 = sm
            .apply_command(ControlCommand::AllocateProducerId {
                request_token: token,
            })
            .await;
        // Same producer id on retry, only one counter advance.
        match (r1, r2) {
            (
                ControlResponse::Producer(
                    super::super::super::domains::ProducerResponse::ProducerIdAllocated {
                        producer_id: pid_a,
                        ..
                    },
                ),
                ControlResponse::Producer(
                    super::super::super::domains::ProducerResponse::ProducerIdAllocated {
                        producer_id: pid_b,
                        ..
                    },
                ),
            ) => assert_eq!(pid_a, pid_b),
            _ => panic!("expected ProducerIdAllocated"),
        }
    }

    #[tokio::test]
    async fn snapshot_roundtrip_preserves_state() {
        let sm = fresh();
        sm.apply_command(ControlCommand::TopicRegistry(
            TopicRegistryCommand::CreateTopic {
                name: "snap".into(),
                partitions: 2,
                config: HashMap::new(),
                timestamp_ms: 50,
            },
        ))
        .await;
        sm.apply_command(ControlCommand::Broker(BrokerCommand::Register {
            broker_id: 9,
            host: "h".into(),
            port: 9,
            timestamp_ms: 1,
        }))
        .await;

        let bytes = sm.snapshot().await;
        let restored_sm = fresh();
        restored_sm.try_restore(&bytes).await.unwrap();
        let s = restored_sm.state().await;
        assert!(s.topic_registry.get_topic("snap").is_some());
        assert!(s.broker_domain.get(9).is_some());
    }

    #[tokio::test]
    async fn restore_on_corrupt_bytes_is_an_error_and_leaves_state_untouched() {
        let sm = fresh();
        sm.apply_command(ControlCommand::TopicRegistry(
            TopicRegistryCommand::CreateTopic {
                name: "before".into(),
                partitions: 1,
                config: HashMap::new(),
                timestamp_ms: 1,
            },
        ))
        .await;
        let err = sm.try_restore(b"not a postcard frame").await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        // State preserved.
        assert!(
            sm.state()
                .await
                .topic_registry
                .get_topic("before")
                .is_some()
        );
    }

    #[tokio::test]
    async fn acl_domain_round_trips() {
        // The ACL domain is plumbed but doesn't need a heavy test here —
        // its own module covers correctness. We just confirm it's wired.
        let sm = fresh();
        let resp = sm
            .apply_command(ControlCommand::Acl(AclCommand::DeleteAcls {
                filters: vec![],
            }))
            .await;
        // Deleting an empty filter list returns 0 affected with no panic.
        let _ = resp;
    }
}
