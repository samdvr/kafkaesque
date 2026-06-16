//! Raft commands and responses for cluster coordination.
//!
//! Commands are the inputs to the Raft state machine - they represent
//! operations that need to be replicated across the cluster.
//!
//! Responses are the outputs from applying commands to the state machine.

use serde::{Deserialize, Serialize};

use super::domains::{
    AclCommand, AclResponse, BrokerCommand, BrokerResponse, BrokerStatus, GroupCommand,
    GroupResponse, PartitionCommand, PartitionResponse, PartitionStateCommand,
    PartitionStateResponse, ProducerCommand, ProducerResponse, TopicRegistryCommand,
    TopicRegistryResponse, TransferCommand, TransferResponse,
};

/// Commands that can be applied to the coordination state machine.
///
/// These commands are replicated via Raft and applied deterministically
/// on all nodes to maintain consistent state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CoordinationCommand {
    /// No-op command (used for linearizable reads).
    Noop,

    /// Broker domain command (registration, heartbeat, fencing).
    #[serde(rename = "broker_v2")]
    BrokerDomain(BrokerCommand),

    /// Partition domain command (topics, partitions, ownership).
    #[serde(rename = "partition_v2")]
    PartitionDomain(PartitionCommand),

    /// Consumer group domain command (groups, members, rebalancing).
    #[serde(rename = "group_v2")]
    GroupDomain(GroupCommand),

    /// Producer domain command (ID allocation, idempotency).
    #[serde(rename = "producer_v2")]
    ProducerDomain(ProducerCommand),

    /// Transfer domain command (partition transfers, failover).
    #[serde(rename = "transfer_v2")]
    TransferDomain(TransferCommand),

    /// ACL domain command (CreateAcls / DeleteAcls).
    #[serde(rename = "acl_v1")]
    AclDomain(AclCommand),
}

/// Responses from applying commands to the state machine.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CoordinationResponse {
    /// Generic success.
    Ok,

    /// Broker domain response.
    #[serde(rename = "broker_response_v2")]
    BrokerDomainResponse(BrokerResponse),

    /// Partition domain response.
    #[serde(rename = "partition_response_v2")]
    PartitionDomainResponse(PartitionResponse),

    /// Consumer group domain response.
    #[serde(rename = "group_response_v2")]
    GroupDomainResponse(GroupResponse),

    /// Producer domain response.
    #[serde(rename = "producer_response_v2")]
    ProducerDomainResponse(ProducerResponse),

    /// Transfer domain response.
    #[serde(rename = "transfer_response_v2")]
    TransferDomainResponse(TransferResponse),

    /// ACL domain response.
    #[serde(rename = "acl_response_v1")]
    AclDomainResponse(AclResponse),
}

// ============================================================================
// Sharded metadata Raft: control + shard command/response enums
// ============================================================================
//
// These coexist with `CoordinationCommand` / `CoordinationResponse` while the
// rest of the system migrates onto the multi-group layout. Once the migration
// is complete (see implementation plan, step 10) the legacy enums above are
// deleted.

/// Commands applied to the control group's state machine.
///
/// The control group holds cluster-wide state: broker registry, ACLs, the
/// topic registry, and the producer-id allocator. Per-partition lease state,
/// consumer groups, per-producer idempotency state, and transfers all live in
/// the shard groups (see [`ShardCommand`]).
#[allow(dead_code)] // wired in subsequent migration steps
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
#[allow(dead_code)] // wired in subsequent migration steps
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
#[allow(dead_code)] // wired in subsequent migration steps
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
#[allow(dead_code)] // wired in subsequent migration steps
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

    // ========================================================================
    // CoordinationCommand Serialization Tests
    // ========================================================================

    #[test]
    fn test_noop_command_roundtrip() {
        let cmd = CoordinationCommand::Noop;
        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_broker_register_command_roundtrip() {
        let cmd = CoordinationCommand::BrokerDomain(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_broker_heartbeat_command_roundtrip() {
        let cmd = CoordinationCommand::BrokerDomain(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 1234567890,
            reported_local_timestamp_ms: 1234567890,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_broker_unregister_command_roundtrip() {
        let cmd = CoordinationCommand::BrokerDomain(BrokerCommand::Unregister { broker_id: 1 });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_partition_create_topic_command_roundtrip() {
        let mut config = HashMap::new();
        config.insert("retention.ms".to_string(), "86400000".to_string());

        let cmd = CoordinationCommand::PartitionDomain(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config,
            timestamp_ms: 1000,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_partition_delete_topic_command_roundtrip() {
        let cmd = CoordinationCommand::PartitionDomain(PartitionCommand::DeleteTopic {
            name: "test-topic".to_string(),
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_partition_acquire_command_roundtrip() {
        let cmd = CoordinationCommand::PartitionDomain(PartitionCommand::AcquirePartition {
            topic: "test-topic".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_group_join_command_roundtrip() {
        let cmd = CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "127.0.0.1".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![1, 2, 3])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: 1000,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_group_leave_command_roundtrip() {
        let cmd = CoordinationCommand::GroupDomain(GroupCommand::LeaveGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_group_commit_offset_command_roundtrip() {
        let cmd = CoordinationCommand::GroupDomain(GroupCommand::CommitOffset {
            group_id: "test-group".to_string(),
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            metadata: Some("consumer-metadata".to_string()),
            timestamp_ms: 1000,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_producer_allocate_id_command_roundtrip() {
        let cmd = CoordinationCommand::ProducerDomain(ProducerCommand::AllocateProducerId {
            request_token: None,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_producer_init_id_command_roundtrip() {
        let cmd = CoordinationCommand::ProducerDomain(ProducerCommand::InitProducerId {
            transactional_id: Some("my-txn".to_string()),
            producer_id: 1000,
            epoch: 0,
            timeout_ms: 60000,
            timestamp_ms: 1000,
            request_token: None,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_transfer_partition_command_roundtrip() {
        use super::super::domains::TransferReason;

        let cmd = CoordinationCommand::TransferDomain(TransferCommand::TransferPartition {
            topic: "test-topic".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            reason: TransferReason::LoadBalancing,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    // ========================================================================
    // CoordinationResponse Serialization Tests
    // ========================================================================

    #[test]
    fn test_ok_response_roundtrip() {
        let resp = CoordinationResponse::Ok;
        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: CoordinationResponse = serde_json::from_str(&serialized).unwrap();
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn test_broker_response_roundtrip() {
        let resp =
            CoordinationResponse::BrokerDomainResponse(BrokerResponse::Registered { broker_id: 1 });

        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: CoordinationResponse = serde_json::from_str(&serialized).unwrap();
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn test_partition_response_roundtrip() {
        let resp = CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicCreated {
            name: "test".to_string(),
            partitions: 3,
        });

        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: CoordinationResponse = serde_json::from_str(&serialized).unwrap();
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn test_group_response_roundtrip() {
        let resp = CoordinationResponse::GroupDomainResponse(GroupResponse::HeartbeatAck);

        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: CoordinationResponse = serde_json::from_str(&serialized).unwrap();
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn test_producer_response_roundtrip() {
        let resp =
            CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerIdAllocated {
                producer_id: 1000,
                epoch: 0,
            });

        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: CoordinationResponse = serde_json::from_str(&serialized).unwrap();
        assert_eq!(resp, deserialized);
    }

    #[test]
    fn test_transfer_response_roundtrip() {
        let resp =
            CoordinationResponse::TransferDomainResponse(TransferResponse::PartitionTransferred {
                topic: "test".to_string(),
                partition: 0,
                from_broker_id: 1,
                to_broker_id: 2,
                new_epoch: 1,
                lease_expires_at_ms: 60000,
            });

        let serialized = serde_json::to_string(&resp).unwrap();
        let deserialized: CoordinationResponse = serde_json::from_str(&serialized).unwrap();
        assert_eq!(resp, deserialized);
    }

    // ========================================================================
    // Binary Serialization Tests (Bincode)
    // ========================================================================

    #[test]
    fn test_noop_command_bincode_roundtrip() {
        let cmd = CoordinationCommand::Noop;
        let serialized = postcard::to_stdvec(&cmd).unwrap();
        let deserialized: CoordinationCommand = postcard::from_bytes(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_broker_command_bincode_roundtrip() {
        let cmd = CoordinationCommand::BrokerDomain(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        let serialized = postcard::to_stdvec(&cmd).unwrap();
        let deserialized: CoordinationCommand = postcard::from_bytes(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_group_command_bincode_roundtrip() {
        let cmd = CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "127.0.0.1".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![1, 2, 3, 4, 5])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: 1000,
        });

        let serialized = postcard::to_stdvec(&cmd).unwrap();
        let deserialized: CoordinationCommand = postcard::from_bytes(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    // ========================================================================
    // Serde Rename Attribute Tests
    // ========================================================================

    #[test]
    fn test_command_serde_rename_broker() {
        let cmd = CoordinationCommand::BrokerDomain(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        let json = serde_json::to_string(&cmd).unwrap();
        // Verify the renamed field is present in JSON
        assert!(json.contains("broker_v2"));
    }

    #[test]
    fn test_command_serde_rename_partition() {
        let cmd = CoordinationCommand::PartitionDomain(PartitionCommand::DeleteTopic {
            name: "test".to_string(),
        });

        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("partition_v2"));
    }

    #[test]
    fn test_command_serde_rename_group() {
        let cmd = CoordinationCommand::GroupDomain(GroupCommand::LeaveGroup {
            group_id: "g".to_string(),
            member_id: "m".to_string(),
        });

        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("group_v2"));
    }

    #[test]
    fn test_command_serde_rename_producer() {
        let cmd = CoordinationCommand::ProducerDomain(ProducerCommand::AllocateProducerId {
            request_token: None,
        });

        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("producer_v2"));
    }

    #[test]
    fn test_command_serde_rename_transfer() {
        use super::super::domains::TransferReason;

        let cmd = CoordinationCommand::TransferDomain(TransferCommand::TransferPartition {
            topic: "t".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            reason: TransferReason::LoadBalancing,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("transfer_v2"));
    }

    // ========================================================================
    // Edge Cases Tests
    // ========================================================================

    #[test]
    fn test_empty_strings_roundtrip() {
        let cmd = CoordinationCommand::PartitionDomain(PartitionCommand::CreateTopic {
            name: String::new(),
            partitions: 0,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_large_values_roundtrip() {
        let cmd = CoordinationCommand::GroupDomain(GroupCommand::CommitOffset {
            group_id: "test-group".to_string(),
            topic: "test-topic".to_string(),
            partition: i32::MAX,
            offset: i64::MAX,
            metadata: Some("a".repeat(10000)),
            timestamp_ms: u64::MAX,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    #[test]
    fn test_unicode_strings_roundtrip() {
        let cmd = CoordinationCommand::PartitionDomain(PartitionCommand::CreateTopic {
            name: "テスト-topic-🚀".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        let serialized = serde_json::to_string(&cmd).unwrap();
        let deserialized: CoordinationCommand = serde_json::from_str(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    // ========================================================================
    // ControlCommand / ShardCommand Serialization Tests
    // ========================================================================

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
            status: super::super::domains::BrokerStatus::Active,
            version: 12,
        };
        let bytes = postcard::to_stdvec(&cmd).unwrap();
        let back: ShardCommand = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, back);
    }
}
