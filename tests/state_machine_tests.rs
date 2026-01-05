//! Tests for the Raft state machine.
//!
//! These tests verify all command types and edge cases
//! in the coordination state machine.

use std::collections::HashMap;

use kafkaesque::cluster::raft::{
    BrokerCommand, BrokerResponse, CoordinationCommand, CoordinationResponse,
    CoordinationStateMachine, GroupCommand, GroupResponse, PartitionCommand, PartitionResponse,
    ProducerCommand, ProducerResponse,
};

// ============================================================================
// State Machine Creation Tests
// ============================================================================

#[tokio::test]
async fn test_state_machine_creation() {
    let sm = CoordinationStateMachine::new();
    let state = sm.state().await;
    assert_eq!(state.version, 0);
    assert!(state.broker_domain.brokers.is_empty());
    assert!(state.partition_domain.topics.is_empty());
    assert!(state.group_domain.groups.is_empty());
}

#[tokio::test]
async fn test_state_machine_default() {
    let sm = CoordinationStateMachine::default();
    let state = sm.state().await;
    assert_eq!(state.version, 0);
}

// ============================================================================
// Broker Management Tests
// ============================================================================

#[tokio::test]
async fn test_register_broker() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        }))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::Registered { broker_id: 1 })
    ));

    let state = sm.state().await;
    assert!(state.broker_domain.brokers.contains_key(&1));
    assert_eq!(
        state.broker_domain.brokers.get(&1).unwrap().host,
        "localhost"
    );
}

#[tokio::test]
async fn test_broker_heartbeat() {
    let sm = CoordinationStateMachine::new();

    // Register first
    sm.apply_command(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        timestamp_ms: 1000,
    }))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::BrokerDomain(
            BrokerCommand::Heartbeat {
                broker_id: 1,
                timestamp_ms: 2000,
                reported_local_timestamp_ms: 2000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::HeartbeatAck)
    ));
}

#[tokio::test]
async fn test_broker_heartbeat_not_found() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::BrokerDomain(
            BrokerCommand::Heartbeat {
                broker_id: 999,
                timestamp_ms: 1000,
                reported_local_timestamp_ms: 1000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::NotFound { broker_id: 999 })
    ));
}

#[tokio::test]
async fn test_unregister_broker() {
    let sm = CoordinationStateMachine::new();

    // Register
    sm.apply_command(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        timestamp_ms: 1000,
    }))
    .await;

    // Create topic and acquire partition
    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "test-topic".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        },
    ))
    .await;

    // Unregister
    let response = sm
        .apply_command(CoordinationCommand::BrokerDomain(
            BrokerCommand::Unregister { broker_id: 1 },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::Unregistered { broker_id: 1 })
    ));

    // Verify broker removed
    let state = sm.state().await;
    assert!(!state.broker_domain.brokers.contains_key(&1));
}

#[tokio::test]
async fn test_fence_broker() {
    let sm = CoordinationStateMachine::new();

    // Register
    sm.apply_command(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        timestamp_ms: 1000,
    }))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::BrokerDomain(BrokerCommand::Fence {
            broker_id: 1,
            reason: "test".to_string(),
        }))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::Fenced { broker_id: 1 })
    ));
}

#[tokio::test]
async fn test_fence_broker_not_found() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::BrokerDomain(BrokerCommand::Fence {
            broker_id: 999,
            reason: "test".to_string(),
        }))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::NotFound { broker_id: 999 })
    ));
}

// ============================================================================
// Topic Management Tests
// ============================================================================

#[tokio::test]
async fn test_create_topic() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::CreateTopic {
                name: "test-topic".to_string(),
                partitions: 3,
                config: HashMap::new(),
                timestamp_ms: 1000,
            },
        ))
        .await;

    match response {
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicCreated {
            name,
            partitions,
        }) => {
            assert_eq!(name, "test-topic");
            assert_eq!(partitions, 3);
        }
        other => panic!("Expected TopicCreated, got {:?}", other),
    }

    // Verify partitions were created
    let state = sm.state().await;
    for i in 0..3 {
        assert!(
            state
                .partition_domain
                .partitions
                .contains_key(&(std::sync::Arc::from("test-topic"), i))
        );
    }
}

#[tokio::test]
async fn test_create_topic_already_exists() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::CreateTopic {
                name: "test-topic".to_string(),
                partitions: 5,
                config: HashMap::new(),
                timestamp_ms: 1000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicAlreadyExists { topic }) if topic == "test-topic"
    ));
}

#[tokio::test]
async fn test_delete_topic() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::DeleteTopic {
                name: "test-topic".to_string(),
            },
        ))
        .await;

    match response {
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicDeleted { name }) => {
            assert_eq!(name, "test-topic");
        }
        other => panic!("Expected TopicDeleted, got {:?}", other),
    }

    // Verify topic and partitions removed
    let state = sm.state().await;
    assert!(!state.partition_domain.topics.contains_key("test-topic"));
    assert!(
        !state
            .partition_domain
            .partitions
            .contains_key(&(std::sync::Arc::from("test-topic"), 0))
    );
}

#[tokio::test]
async fn test_delete_topic_not_found() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::DeleteTopic {
                name: "nonexistent".to_string(),
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicNotFound { topic }) if topic == "nonexistent"
    ));
}

#[tokio::test]
async fn test_delete_topic_preserves_offsets() {
    // Note: In the domain-based architecture, topic deletion in PartitionDomain
    // does NOT automatically clean up offsets in GroupDomain. This is by design
    // to support topic recreation scenarios where consumers can resume from
    // their previous position.
    let sm = CoordinationStateMachine::new();

    // Create topic
    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    // Commit offset
    sm.apply_command(CoordinationCommand::GroupDomain(
        GroupCommand::CommitOffset {
            group_id: "test-group".to_string(),
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            metadata: None,
            timestamp_ms: 1000,
        },
    ))
    .await;

    // Delete topic
    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::DeleteTopic {
            name: "test-topic".to_string(),
        },
    ))
    .await;

    // Verify topic is deleted
    let state = sm.state().await;
    assert!(!state.partition_domain.topics.contains_key("test-topic"));

    // Offsets are preserved (separate domain, not cleaned up on topic deletion)
    assert!(state.group_domain.offsets.contains_key(&(
        "test-group".to_string(),
        std::sync::Arc::from("test-topic"),
        0
    )));
}

#[tokio::test]
async fn test_update_topic_config() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    let mut new_config = HashMap::new();
    new_config.insert("retention.ms".to_string(), "86400000".to_string());

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::UpdateTopicConfig {
                name: "test-topic".to_string(),
                config: new_config,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicConfigUpdated { .. })
    ));

    let state = sm.state().await;
    let topic = state.partition_domain.topics.get("test-topic").unwrap();
    assert_eq!(
        topic.config.get("retention.ms"),
        Some(&"86400000".to_string())
    );
}

#[tokio::test]
async fn test_update_topic_config_not_found() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::UpdateTopicConfig {
                name: "nonexistent".to_string(),
                config: HashMap::new(),
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicNotFound { topic }) if topic == "nonexistent"
    ));
}

// ============================================================================
// Partition Ownership Tests
// ============================================================================

#[tokio::test]
async fn test_acquire_partition() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "test-topic".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
        ))
        .await;

    match response {
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired {
            topic,
            partition,
            leader_epoch,
            ..
        }) => {
            assert_eq!(topic, "test-topic");
            assert_eq!(partition, 0);
            assert_eq!(leader_epoch, 1);
        }
        other => panic!("Expected PartitionAcquired, got {:?}", other),
    }
}

#[tokio::test]
async fn test_acquire_partition_auto_create() {
    let sm = CoordinationStateMachine::new();

    // Acquire without creating topic first
    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "auto-topic".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired { .. })
    ));
}

#[tokio::test]
async fn test_acquire_partition_owned_by_other() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    // Broker 1 acquires
    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "test-topic".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        },
    ))
    .await;

    // Broker 2 tries to acquire
    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "test-topic".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionOwnedByOther {
            owner: 1,
            ..
        })
    ));
}

#[tokio::test]
async fn test_renew_partition_lease() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "test-topic".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        },
    ))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::RenewLease {
                topic: "test-topic".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 60000,
                timestamp_ms: 2000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeaseRenewed { .. })
    ));
}

#[tokio::test]
async fn test_renew_partition_lease_not_owned() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::RenewLease {
                topic: "test-topic".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionNotOwned { .. })
    ));
}

#[tokio::test]
async fn test_release_partition() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "test-topic".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        },
    ))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::ReleasePartition {
                topic: "test-topic".to_string(),
                partition: 0,
                broker_id: 1,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionReleased { .. })
    ));

    // Verify partition is unowned
    let state = sm.state().await;
    let partition = state
        .partition_domain
        .partitions
        .get(&(std::sync::Arc::from("test-topic"), 0))
        .unwrap();
    assert!(partition.owner_broker_id.is_none());
}

#[tokio::test]
async fn test_expire_leases() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "test-topic".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 100, // Very short lease
            timestamp_ms: 1000,
        },
    ))
    .await;

    // Wait for lease to "expire" (use future timestamp)
    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::ExpireLeases {
                current_time_ms: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64
                    + 10000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeasesExpired {
            count: 1
        })
    ));
}

// ============================================================================
// Consumer Group Tests
// ============================================================================

#[tokio::test]
async fn test_join_group_new_member() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(), // New member
            client_id: "test-client".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![1, 2, 3])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            timestamp_ms: 1000,
        }))
        .await;

    match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            generation,
            member_id,
            ..
        }) => {
            assert_eq!(generation, 1);
            assert!(!member_id.is_empty());
        }
        other => panic!("Expected JoinGroupResponse, got {:?}", other),
    }
}

#[tokio::test]
async fn test_join_group_existing_member() {
    let sm = CoordinationStateMachine::new();

    // First join
    let first_response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(),
            client_id: "test-client".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            timestamp_ms: 1000,
        }))
        .await;

    let member_id = match first_response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            member_id,
            ..
        }) => member_id,
        _ => panic!("Expected JoinGroupResponse"),
    };

    // Second member joins
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(),
            client_id: "test-client-2".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            timestamp_ms: 2000,
        }))
        .await;

    match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            generation,
            leader_id,
            ..
        }) => {
            assert_eq!(generation, 2);
            assert_eq!(leader_id, member_id); // First member should be leader
        }
        other => panic!("Expected JoinGroupResponse, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sync_group() {
    let sm = CoordinationStateMachine::new();

    // Join first
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(),
            client_id: "test-client".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            timestamp_ms: 1000,
        }))
        .await;

    let (generation, member_id) = match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            generation,
            member_id,
            ..
        }) => (generation, member_id),
        _ => panic!("Expected JoinGroupResponse"),
    };

    // Sync with assignment
    let assignments = vec![(member_id.clone(), vec![1u8, 2u8, 3u8])];

    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::SyncGroup {
            group_id: "test-group".to_string(),
            member_id: member_id.clone(),
            generation,
            assignments,
        }))
        .await;

    match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::SyncGroupResponse {
            assignment,
        }) => {
            assert_eq!(assignment, vec![1, 2, 3]);
        }
        other => panic!("Expected SyncGroupResponse, got {:?}", other),
    }
}

#[tokio::test]
async fn test_sync_group_illegal_generation() {
    let sm = CoordinationStateMachine::new();

    // Join
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(),
            client_id: "test-client".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            timestamp_ms: 1000,
        }))
        .await;

    let member_id = match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            member_id,
            ..
        }) => member_id,
        _ => panic!("Expected JoinGroupResponse"),
    };

    // Sync with wrong generation
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::SyncGroup {
            group_id: "test-group".to_string(),
            member_id,
            generation: 999,
            assignments: vec![],
        }))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::IllegalGeneration { .. })
    ));
}

#[tokio::test]
async fn test_sync_group_unknown_member() {
    let sm = CoordinationStateMachine::new();

    // Join to create group
    sm.apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
        group_id: "test-group".to_string(),
        member_id: "".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 30000,
        timestamp_ms: 1000,
    }))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::SyncGroup {
            group_id: "test-group".to_string(),
            member_id: "unknown-member".to_string(),
            generation: 1,
            assignments: vec![],
        }))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::UnknownMember { .. })
    ));
}

#[tokio::test]
async fn test_member_heartbeat() {
    let sm = CoordinationStateMachine::new();

    // Join
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(),
            client_id: "test-client".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            timestamp_ms: 1000,
        }))
        .await;

    let (generation, member_id) = match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            generation,
            member_id,
            ..
        }) => (generation, member_id),
        _ => panic!("Expected JoinGroupResponse"),
    };

    // Heartbeat
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::Heartbeat {
            group_id: "test-group".to_string(),
            member_id,
            generation,
            timestamp_ms: 2000,
        }))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::HeartbeatAck)
    ));
}

#[tokio::test]
async fn test_leave_group() {
    let sm = CoordinationStateMachine::new();

    // Join
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(),
            client_id: "test-client".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            timestamp_ms: 1000,
        }))
        .await;

    let member_id = match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            member_id,
            ..
        }) => member_id,
        _ => panic!("Expected JoinGroupResponse"),
    };

    // Leave
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(GroupCommand::LeaveGroup {
            group_id: "test-group".to_string(),
            member_id,
        }))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::LeftGroup)
    ));
}

#[tokio::test]
async fn test_expire_members() {
    let sm = CoordinationStateMachine::new();

    // Join with short session timeout
    sm.apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
        group_id: "test-group".to_string(),
        member_id: "".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
        session_timeout_ms: 100,
        rebalance_timeout_ms: 100,
        timestamp_ms: 1000,
    }))
    .await;

    // Expire members (far in future)
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(
            GroupCommand::ExpireMembers {
                current_time_ms: 100000,
            },
        ))
        .await;

    match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::MembersExpired { expired }) => {
            assert!(!expired.is_empty());
        }
        other => panic!("Expected MembersExpired, got {:?}", other),
    }
}

#[tokio::test]
async fn test_delete_group() {
    let sm = CoordinationStateMachine::new();

    // Create group
    sm.apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
        group_id: "test-group".to_string(),
        member_id: "".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 30000,
        timestamp_ms: 1000,
    }))
    .await;

    // Delete
    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(
            GroupCommand::DeleteGroup {
                group_id: "test-group".to_string(),
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::GroupDeleted { .. })
    ));

    let state = sm.state().await;
    assert!(!state.group_domain.groups.contains_key("test-group"));
}

#[tokio::test]
async fn test_complete_rebalance() {
    let sm = CoordinationStateMachine::new();

    // Join
    sm.apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
        group_id: "test-group".to_string(),
        member_id: "".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 30000,
        timestamp_ms: 1000,
    }))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(
            GroupCommand::CompleteRebalance {
                group_id: "test-group".to_string(),
                generation: 1,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::RebalanceCompleted { .. })
    ));
}

#[tokio::test]
async fn test_trigger_rebalance() {
    let sm = CoordinationStateMachine::new();

    // Create group
    sm.apply_command(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
        group_id: "test-group".to_string(),
        member_id: "".to_string(),
        client_id: "test-client".to_string(),
        client_host: "localhost".to_string(),
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 30000,
        timestamp_ms: 1000,
    }))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(
            GroupCommand::TriggerRebalance {
                group_id: "test-group".to_string(),
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::RebalanceTriggered { .. })
    ));

    let state = sm.state().await;
    let group = state.group_domain.groups.get("test-group").unwrap();
    assert_eq!(group.generation, 2); // Incremented
}

// ============================================================================
// Consumer Offset Tests
// ============================================================================

#[tokio::test]
async fn test_commit_offset() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::GroupDomain(
            GroupCommand::CommitOffset {
                group_id: "test-group".to_string(),
                topic: "test-topic".to_string(),
                partition: 0,
                offset: 100,
                metadata: Some("metadata".to_string()),
                timestamp_ms: 1000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::OffsetCommitted)
    ));

    let state = sm.state().await;
    let key = (
        "test-group".to_string(),
        std::sync::Arc::from("test-topic"),
        0,
    );
    let offset = state.group_domain.offsets.get(&key).unwrap();
    assert_eq!(offset.offset, 100);
    assert_eq!(offset.metadata, Some("metadata".to_string()));
}

// ============================================================================
// Producer ID Tests
// ============================================================================

#[tokio::test]
async fn test_allocate_producer_id() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::ProducerDomain(
            ProducerCommand::AllocateProducerId,
        ))
        .await;

    match response {
        CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerIdAllocated {
            producer_id,
            epoch,
        }) => {
            assert_eq!(producer_id, 0);
            assert_eq!(epoch, 0);
        }
        other => panic!("Expected ProducerIdAllocated, got {:?}", other),
    }

    // Allocate another
    let response = sm
        .apply_command(CoordinationCommand::ProducerDomain(
            ProducerCommand::AllocateProducerId,
        ))
        .await;

    match response {
        CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerIdAllocated {
            producer_id,
            ..
        }) => {
            assert_eq!(producer_id, 1);
        }
        other => panic!("Expected ProducerIdAllocated, got {:?}", other),
    }
}

#[tokio::test]
async fn test_init_producer_id_non_transactional() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::ProducerDomain(
            ProducerCommand::InitProducerId {
                transactional_id: None,
                producer_id: -1,
                epoch: -1,
                timeout_ms: 60000,
                timestamp_ms: 1000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerIdAllocated { .. })
    ));
}

#[tokio::test]
async fn test_init_producer_id_transactional() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::ProducerDomain(
            ProducerCommand::InitProducerId {
                transactional_id: Some("txn-1".to_string()),
                producer_id: -1,
                epoch: -1,
                timeout_ms: 60000,
                timestamp_ms: 1000,
            },
        ))
        .await;

    let (pid, epoch) = match response {
        CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerIdAllocated {
            producer_id,
            epoch,
        }) => (producer_id, epoch),
        _ => panic!("Expected ProducerIdAllocated"),
    };
    assert_eq!(epoch, 0);

    // Init again with same transactional_id should bump epoch
    let response = sm
        .apply_command(CoordinationCommand::ProducerDomain(
            ProducerCommand::InitProducerId {
                transactional_id: Some("txn-1".to_string()),
                producer_id: pid,
                epoch,
                timeout_ms: 60000,
                timestamp_ms: 1000,
            },
        ))
        .await;

    match response {
        CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerIdAllocated {
            producer_id,
            epoch,
        }) => {
            assert_eq!(producer_id, pid);
            assert_eq!(epoch, 1); // Bumped
        }
        _ => panic!("Expected ProducerIdAllocated"),
    }
}

#[tokio::test]
async fn test_store_producer_state() {
    let sm = CoordinationStateMachine::new();

    let response = sm
        .apply_command(CoordinationCommand::ProducerDomain(
            ProducerCommand::StoreProducerState {
                topic: "test-topic".to_string(),
                partition: 0,
                producer_id: 1,
                last_sequence: 10,
                producer_epoch: 0,
                timestamp_ms: 1000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerStateStored)
    ));

    let state = sm.state().await;
    let key = (std::sync::Arc::from("test-topic"), 0, 1i64);
    let producer_state = state.producer_domain.producer_states.get(&key).unwrap();
    assert_eq!(producer_state.last_sequence, 10);
}

#[tokio::test]
async fn test_expire_producer_states() {
    let sm = CoordinationStateMachine::new();

    // Store producer state with old timestamp
    sm.apply_command(CoordinationCommand::ProducerDomain(
        ProducerCommand::StoreProducerState {
            topic: "test-topic".to_string(),
            partition: 0,
            producer_id: 1,
            last_sequence: 10,
            producer_epoch: 0,
            timestamp_ms: 1000, // Old timestamp
        },
    ))
    .await;

    // Store another producer state with newer timestamp
    sm.apply_command(CoordinationCommand::ProducerDomain(
        ProducerCommand::StoreProducerState {
            topic: "test-topic".to_string(),
            partition: 0,
            producer_id: 2,
            last_sequence: 20,
            producer_epoch: 0,
            timestamp_ms: 100_000, // Newer timestamp
        },
    ))
    .await;

    // Verify both exist
    {
        let state = sm.state().await;
        assert_eq!(state.producer_domain.producer_states.len(), 2);
    }

    // Expire with 100 second TTL from timestamp 200_000
    // Producer 1: 200_000 - 1000 = 199_000 > 100_000 = expired
    // Producer 2: 200_000 - 100_000 = 100_000 > 100_000 = NOT expired (not >)
    let response = sm
        .apply_command(CoordinationCommand::ProducerDomain(
            ProducerCommand::ExpireProducerStates {
                current_time_ms: 200_000,
                ttl_ms: 100_000,
            },
        ))
        .await;

    assert!(matches!(
        response,
        CoordinationResponse::ProducerDomainResponse(ProducerResponse::ProducerStatesExpired {
            count: 1
        })
    ));

    // Verify only producer 2 remains
    let state = sm.state().await;
    assert_eq!(state.producer_domain.producer_states.len(), 1);
    let key2 = (std::sync::Arc::from("test-topic"), 0, 2i64);
    assert!(state.producer_domain.producer_states.contains_key(&key2));
}

// ============================================================================
// Noop Test
// ============================================================================

#[tokio::test]
async fn test_noop() {
    let sm = CoordinationStateMachine::new();

    let response = sm.apply_command(CoordinationCommand::Noop).await;

    assert!(matches!(response, CoordinationResponse::Ok));
}

// ============================================================================
// Snapshot/Restore Tests
// ============================================================================

#[tokio::test]
async fn test_snapshot_and_restore() {
    let sm = CoordinationStateMachine::new();

    // Create some state
    sm.apply_command(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        timestamp_ms: 1000,
    }))
    .await;

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await;

    // Take snapshot (returns serialized bytes)
    let snapshot_bytes = sm.snapshot().await;

    // Create new state machine and restore
    let sm2 = CoordinationStateMachine::new();
    sm2.restore(&snapshot_bytes).await;

    let state = sm2.state().await;
    assert!(state.broker_domain.brokers.contains_key(&1));
    assert!(state.partition_domain.topics.contains_key("test-topic"));
}

#[tokio::test]
async fn test_state_arc() {
    let sm = CoordinationStateMachine::new();
    let _arc = sm.state_arc();
    // Just verify we can get the arc
}

// ============================================================================
// Version Increment Tests
// ============================================================================

#[tokio::test]
async fn test_version_increments() {
    let sm = CoordinationStateMachine::new();

    {
        let state = sm.state().await;
        assert_eq!(state.version, 0);
    }

    sm.apply_command(CoordinationCommand::Noop).await;

    {
        let state = sm.state().await;
        assert_eq!(state.version, 1);
    }

    sm.apply_command(CoordinationCommand::Noop).await;

    {
        let state = sm.state().await;
        assert_eq!(state.version, 2);
    }
}
