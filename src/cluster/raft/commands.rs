//! Raft commands and responses for cluster coordination.
//!
//! Two parallel command/response enums, one per Raft group kind:
//!
//! - [`ControlCommand`] / [`ControlResponse`] — cluster-wide state.
//! - [`ShardCommand`] / [`ShardResponse`] — per-shard hot-path state,
//!   keyed by `hash(...) % metadata_shards`.

use serde::{Deserialize, Serialize};

use super::domains::{
    AclCommand, AclResponse, BrokerCommand, BrokerResponse, BrokerStatus, GroupCommand,
    GroupResponse, PartitionStateCommand, PartitionStateResponse, ProducerCommand,
    ProducerResponse, TopicRegistryCommand, TopicRegistryResponse, TransferCommand,
    TransferResponse,
};

/// Commands applied to the control group's state machine.
///
/// The control group holds cluster-wide state: broker registry, ACLs, the
/// topic registry, and the producer-id allocator. Per-partition lease state,
/// consumer groups, per-producer idempotency state, and transfers all live in
/// the shard groups (see [`ShardCommand`]).
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlCommand {
    /// No-op command (used for linearizable reads on control).
    Noop,

    /// Broker registry: registration, heartbeat, fencing.
    #[serde(rename = "control_broker_v1")]
    Broker(BrokerCommand),

    /// ACL bindings.
    #[serde(rename = "control_acl_v1")]
    Acl(AclCommand),

    /// Topic registry: existence record, partition count, configs. The
    /// reconciler propagates `InitPartition` / `PurgePartition` into the
    /// relevant shard group when topics are created/deleted here.
    #[serde(rename = "control_topic_registry_v1")]
    TopicRegistry(TopicRegistryCommand),

    /// Allocate a fresh producer ID (cluster-wide unique counter). The caller
    /// then routes a follow-up `ShardCommand::Producer(InitProducerId)` to the
    /// shard that owns this id.
    #[serde(rename = "control_allocate_producer_id_v1")]
    AllocateProducerId { request_token: Option<u128> },
}

/// Responses from applying [`ControlCommand`] to the control state machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ControlResponse {
    /// Generic success / Noop.
    Ok,

    #[serde(rename = "control_broker_response_v1")]
    Broker(BrokerResponse),

    #[serde(rename = "control_acl_response_v1")]
    Acl(AclResponse),

    #[serde(rename = "control_topic_registry_response_v1")]
    TopicRegistry(TopicRegistryResponse),

    /// Result of `AllocateProducerId`. Re-uses `ProducerResponse` so the
    /// `ProducerIdAllocated { producer_id, epoch }` shape is shared with the
    /// shard's per-producer response variants.
    #[serde(rename = "control_producer_response_v1")]
    Producer(ProducerResponse),
}

/// Commands applied to a shard group's state machine.
///
/// Each shard owns a slice of three domains, partitioned by hash:
/// - per-partition lease state, keyed by `hash(topic) % N`
/// - consumer groups + offsets, keyed by `hash(group_id) % N`
/// - per-producer idempotency state, keyed by `hash(producer_id) % N`
///
/// `UpdateBrokerLivenessShadow` is reconciler-issued: broker liveness is
/// authoritative in control and the reconciler propagates the current view
/// into every shard so the shard-level fencing gate can run locally.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShardCommand {
    /// No-op command (used for linearizable reads on a shard).
    Noop,

    /// Per-partition lease/ownership state, plus reconciler-issued
    /// `InitPartition` / `PurgePartition`.
    #[serde(rename = "shard_partition_v1")]
    Partition(PartitionStateCommand),

    /// Consumer group coordination.
    #[serde(rename = "shard_group_v1")]
    Group(GroupCommand),

    /// Per-producer state. `AllocateProducerId` lives in control; the rest of
    /// the producer command surface (e.g. `InitProducerId`) is dispatched
    /// here, routed by `hash(producer_id) % N`.
    #[serde(rename = "shard_producer_v1")]
    Producer(ProducerCommand),

    /// Fast-failover partition transfers (single-shard only).
    #[serde(rename = "shard_transfer_v1")]
    Transfer(TransferCommand),

    /// Reconciler-issued: update the local broker-liveness shadow so the
    /// shard's cross-domain fencing gate can decide locally.
    #[serde(rename = "shard_broker_liveness_shadow_v1")]
    UpdateBrokerLivenessShadow {
        broker_id: i32,
        status: BrokerStatus,
        version: u64,
    },
}

/// Responses from applying [`ShardCommand`] to a shard state machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ShardResponse {
    /// Generic success / Noop / shadow update.
    Ok,

    #[serde(rename = "shard_partition_response_v1")]
    Partition(PartitionStateResponse),

    #[serde(rename = "shard_group_response_v1")]
    Group(GroupResponse),

    #[serde(rename = "shard_producer_response_v1")]
    Producer(ProducerResponse),

    #[serde(rename = "shard_transfer_response_v1")]
    Transfer(TransferResponse),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn control_command_topic_registry_roundtrip() {
        let cmd = ControlCommand::TopicRegistry(TopicRegistryCommand::CreateTopic {
            name: "t".into(),
            partitions: 4,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });
        let bytes = postcard::to_stdvec(&cmd).unwrap();
        let back: ControlCommand = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, back);
    }

    #[test]
    fn control_command_allocate_producer_id_roundtrip() {
        let cmd = ControlCommand::AllocateProducerId {
            request_token: Some(42),
        };
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("control_allocate_producer_id_v1"));
        let back: ControlCommand = serde_json::from_str(&json).unwrap();
        assert_eq!(cmd, back);
    }

    #[test]
    fn shard_command_partition_state_roundtrip() {
        let cmd = ShardCommand::Partition(PartitionStateCommand::InitPartition {
            topic: "t".into(),
            partition: 3,
            created_at_ms: 100,
        });
        let bytes = postcard::to_stdvec(&cmd).unwrap();
        let back: ShardCommand = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, back);
    }

    #[test]
    fn shard_command_broker_liveness_shadow_roundtrip() {
        let cmd = ShardCommand::UpdateBrokerLivenessShadow {
            broker_id: 7,
            status: BrokerStatus::Active,
            version: 12,
        };
        let bytes = postcard::to_stdvec(&cmd).unwrap();
        let back: ShardCommand = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, back);
    }
}
