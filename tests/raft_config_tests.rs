//! Tests for Raft configuration and command/response types.
//!
//! These tests verify RaftConfig validation, CoordinationCommand
//! serialization, and CoordinationResponse methods.

#![allow(clippy::field_reassign_with_default)]

use std::collections::HashMap;
use std::time::Duration;

use kafkaesque::cluster::raft::{
    BrokerCommand, BrokerResponse, CoordinationCommand, CoordinationResponse, GroupCommand,
    GroupResponse, MemberDescription, PartitionCommand, PartitionResponse, ProducerResponse,
};
use kafkaesque::cluster::{ClusterConfig, RaftConfig};

// ============================================================================
// RaftConfig Default Tests
// ============================================================================

#[test]
fn test_raft_config_default() {
    let config = RaftConfig::default();

    assert_eq!(config.node_id, 0);
    assert_eq!(config.broker_id, 0);
    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.port, 9092);
    assert_eq!(config.raft_addr, "127.0.0.1:9191");
    assert!(config.cluster_members.is_empty());
    assert_eq!(config.heartbeat_interval, Duration::from_millis(100));
    assert_eq!(config.election_timeout_min, Duration::from_millis(200));
    assert_eq!(config.election_timeout_max, Duration::from_millis(400));
    assert_eq!(config.max_payload_entries, 100);
    assert_eq!(config.snapshot_threshold, 1_000);
    assert!(config.is_voter);
    assert!(config.auto_create_topics);
    assert_eq!(config.max_pending_proposals, 1000);
}

#[test]
fn test_raft_config_debug() {
    let config = RaftConfig::default();
    let debug_str = format!("{:?}", config);
    assert!(debug_str.contains("RaftConfig"));
    assert!(debug_str.contains("node_id"));
}

#[test]
fn test_raft_config_clone() {
    let config = RaftConfig::default();
    let cloned = config.clone();
    assert_eq!(cloned.node_id, config.node_id);
    assert_eq!(cloned.port, config.port);
}

// ============================================================================
// RaftConfig Validation Tests
// ============================================================================

#[test]
fn test_raft_config_validate_default_succeeds() {
    let config = RaftConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_raft_config_validate_invalid_port_zero() {
    let mut config = RaftConfig::default();
    config.port = 0;

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("port")));
}

#[test]
fn test_raft_config_validate_invalid_port_too_high() {
    let mut config = RaftConfig::default();
    config.port = 70000;

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_raft_config_validate_election_timeout_min_gte_max() {
    let mut config = RaftConfig::default();
    config.election_timeout_min = Duration::from_millis(500);
    config.election_timeout_max = Duration::from_millis(400);

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("election_timeout_min")));
}

#[test]
fn test_raft_config_validate_heartbeat_gte_election_min() {
    let mut config = RaftConfig::default();
    config.heartbeat_interval = Duration::from_millis(300);
    config.election_timeout_min = Duration::from_millis(200);

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("heartbeat_interval")));
}

#[test]
fn test_raft_config_validate_lease_renewal_gte_duration() {
    let mut config = RaftConfig::default();
    config.lease_renewal_interval = Duration::from_secs(60);
    config.lease_duration = Duration::from_secs(30);

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("lease_renewal_interval")));
}

#[test]
fn test_raft_config_validate_multiple_errors() {
    let mut config = RaftConfig::default();
    config.port = 0;
    config.lease_renewal_interval = Duration::from_secs(60);
    config.lease_duration = Duration::from_secs(30);

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.len() >= 2);
}

// ============================================================================
// RaftConfig from_cluster_config Tests
// ============================================================================

#[test]
fn test_raft_config_from_cluster_config() {
    let cluster_config = ClusterConfig::default();
    let raft_config = RaftConfig::from_cluster_config(&cluster_config);

    assert_eq!(raft_config.broker_id, cluster_config.broker_id);
    assert_eq!(raft_config.node_id, cluster_config.broker_id as u64);
    assert_eq!(raft_config.host, cluster_config.host);
    assert_eq!(raft_config.port, cluster_config.port);
    assert_eq!(raft_config.lease_duration, cluster_config.lease_duration);
    assert_eq!(
        raft_config.auto_create_topics,
        cluster_config.auto_create_topics
    );
}

#[test]
fn test_raft_config_from_cluster_config_with_peers() {
    let mut cluster_config = ClusterConfig::default();
    cluster_config.raft_peers = Some("0=host1:9191,1=host2:9191,2=host3:9191".to_string());

    let raft_config = RaftConfig::from_cluster_config(&cluster_config);

    assert_eq!(raft_config.cluster_members.len(), 3);
    assert!(raft_config.cluster_members.iter().any(|(id, _)| *id == 0));
    assert!(raft_config.cluster_members.iter().any(|(id, _)| *id == 1));
    assert!(raft_config.cluster_members.iter().any(|(id, _)| *id == 2));
}

#[test]
fn test_raft_config_from_cluster_config_empty_peers() {
    let cluster_config = ClusterConfig::default();
    let raft_config = RaftConfig::from_cluster_config(&cluster_config);

    assert!(raft_config.cluster_members.is_empty());
}

#[test]
fn test_raft_config_from_cluster_config_malformed_peers() {
    let mut cluster_config = ClusterConfig::default();
    cluster_config.raft_peers = Some("invalid,0=host:9191,also-invalid".to_string());

    let raft_config = RaftConfig::from_cluster_config(&cluster_config);

    // Only valid entry should be parsed
    assert_eq!(raft_config.cluster_members.len(), 1);
}

// ============================================================================
// RaftConfig to_openraft_config Tests
// ============================================================================

#[test]
fn test_raft_config_to_openraft_config() {
    let config = RaftConfig::default();
    let openraft_config = config.to_openraft_config();

    assert_eq!(openraft_config.cluster_name, "kafkaesque-cluster");
    assert_eq!(openraft_config.heartbeat_interval, 100);
    assert_eq!(openraft_config.election_timeout_min, 200);
    assert_eq!(openraft_config.election_timeout_max, 400);
    assert_eq!(openraft_config.max_payload_entries, 100);
}

// ============================================================================
// CoordinationCommand Tests
// ============================================================================

#[test]
fn test_coordination_command_register_broker() {
    let cmd = CoordinationCommand::BrokerDomain(BrokerCommand::Register {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        timestamp_ms: 1000,
    });

    // Test serialization roundtrip
    let json = serde_json::to_string(&cmd).unwrap();
    let deserialized: CoordinationCommand = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, deserialized);
}

#[test]
fn test_coordination_command_create_topic() {
    let mut config = HashMap::new();
    config.insert("retention.ms".to_string(), "86400000".to_string());

    let cmd = CoordinationCommand::PartitionDomain(PartitionCommand::CreateTopic {
        name: "test-topic".to_string(),
        partitions: 3,
        config,
        timestamp_ms: 1000,
    });

    let json = serde_json::to_string(&cmd).unwrap();
    let deserialized: CoordinationCommand = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, deserialized);
}

#[test]
fn test_coordination_command_join_group() {
    let cmd = CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
        group_id: "test-group".to_string(),
        member_id: "member-1".to_string(),
        client_id: "client-1".to_string(),
        client_host: "localhost".to_string(),
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![1, 2, 3])],
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 30000,
        timestamp_ms: 1000,
    });

    let json = serde_json::to_string(&cmd).unwrap();
    let deserialized: CoordinationCommand = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, deserialized);
}

#[test]
fn test_coordination_command_noop() {
    let cmd = CoordinationCommand::Noop;
    let json = serde_json::to_string(&cmd).unwrap();
    let deserialized: CoordinationCommand = serde_json::from_str(&json).unwrap();
    assert_eq!(cmd, deserialized);
}

#[test]
fn test_coordination_command_debug() {
    let cmd = CoordinationCommand::Noop;
    let debug_str = format!("{:?}", cmd);
    assert!(debug_str.contains("Noop"));
}

#[test]
fn test_coordination_command_clone() {
    let cmd = CoordinationCommand::BrokerDomain(BrokerCommand::Heartbeat {
        broker_id: 1,
        timestamp_ms: 1000,
        reported_local_timestamp_ms: 1000,
    });
    let cloned = cmd.clone();
    assert_eq!(cmd, cloned);
}

// ============================================================================
// CoordinationResponse Tests
// ============================================================================

#[test]
fn test_coordination_response_broker_responses() {
    // Test broker domain responses serialize correctly
    let registered =
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::Registered { broker_id: 1 });
    let json = serde_json::to_string(&registered).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(registered, deserialized);

    let heartbeat_ack = CoordinationResponse::BrokerDomainResponse(BrokerResponse::HeartbeatAck);
    let json = serde_json::to_string(&heartbeat_ack).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(heartbeat_ack, deserialized);

    let not_found =
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::NotFound { broker_id: 1 });
    let json = serde_json::to_string(&not_found).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(not_found, deserialized);
}

#[test]
fn test_coordination_response_partition_responses() {
    // Test partition domain responses serialize correctly
    let created = CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicCreated {
        name: "t".to_string(),
        partitions: 3,
    });
    let json = serde_json::to_string(&created).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(created, deserialized);

    let acquired =
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired {
            topic: "t".to_string(),
            partition: 0,
            leader_epoch: 1,
            lease_expires_at_ms: 1000,
        });
    let json = serde_json::to_string(&acquired).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(acquired, deserialized);

    let owned_by_other =
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionOwnedByOther {
            topic: "t".to_string(),
            partition: 0,
            owner: 1,
        });
    let json = serde_json::to_string(&owned_by_other).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(owned_by_other, deserialized);
}

#[test]
fn test_coordination_response_group_responses() {
    // Test group domain responses serialize correctly
    let join_response =
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            generation: 1,
            leader_id: "member-1".to_string(),
            member_id: "member-1".to_string(),
            members: vec![MemberDescription {
                member_id: "member-1".to_string(),
                client_id: "client-1".to_string(),
                client_host: "localhost".to_string(),
                metadata: vec![1, 2, 3],
            }],
            protocol_name: "range".to_string(),
        });

    let json = serde_json::to_string(&join_response).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(join_response, deserialized);

    let sync_response =
        CoordinationResponse::GroupDomainResponse(GroupResponse::SyncGroupResponse {
            assignment: vec![1, 2, 3, 4],
        });
    let json = serde_json::to_string(&sync_response).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(sync_response, deserialized);

    let heartbeat_ack = CoordinationResponse::GroupDomainResponse(GroupResponse::HeartbeatAck);
    let json = serde_json::to_string(&heartbeat_ack).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(heartbeat_ack, deserialized);
}

#[test]
fn test_coordination_response_producer_responses() {
    // Test producer domain responses serialize correctly
    let allocated =
        CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerIdAllocated {
            producer_id: 1,
            epoch: 0,
        });
    let json = serde_json::to_string(&allocated).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(allocated, deserialized);
}

#[test]
fn test_coordination_response_ok() {
    let ok = CoordinationResponse::Ok;
    let json = serde_json::to_string(&ok).unwrap();
    let deserialized: CoordinationResponse = serde_json::from_str(&json).unwrap();
    assert_eq!(ok, deserialized);
}

#[test]
fn test_coordination_response_debug() {
    let response = CoordinationResponse::Ok;
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("Ok"));
}

// ============================================================================
// MemberDescription Tests
// ============================================================================

#[test]
fn test_member_description() {
    let member = MemberDescription {
        member_id: "member-1".to_string(),
        client_id: "client-1".to_string(),
        client_host: "192.168.1.100".to_string(),
        metadata: vec![0, 1, 2, 3],
    };

    // Test serialization
    let json = serde_json::to_string(&member).unwrap();
    let deserialized: MemberDescription = serde_json::from_str(&json).unwrap();
    assert_eq!(member, deserialized);

    // Test debug
    let debug_str = format!("{:?}", member);
    assert!(debug_str.contains("member-1"));
}

#[test]
fn test_member_description_clone() {
    let member = MemberDescription {
        member_id: "m".to_string(),
        client_id: "c".to_string(),
        client_host: "h".to_string(),
        metadata: vec![],
    };
    let cloned = member.clone();
    assert_eq!(member, cloned);
}
