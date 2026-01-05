//! Raft commands and responses for cluster coordination.
//!
//! Commands are the inputs to the Raft state machine - they represent
//! operations that need to be replicated across the cluster.
//!
//! Responses are the outputs from applying commands to the state machine.

use serde::{Deserialize, Serialize};

use super::domains::{
    BrokerCommand, BrokerResponse, GroupCommand, GroupResponse, PartitionCommand,
    PartitionResponse, ProducerCommand, ProducerResponse, TransferCommand, TransferResponse,
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
        let cmd = CoordinationCommand::ProducerDomain(ProducerCommand::AllocateProducerId);

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
        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: CoordinationCommand = bincode::deserialize(&serialized).unwrap();
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

        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: CoordinationCommand = bincode::deserialize(&serialized).unwrap();
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

        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: CoordinationCommand = bincode::deserialize(&serialized).unwrap();
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
        let cmd = CoordinationCommand::ProducerDomain(ProducerCommand::AllocateProducerId);

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
}
