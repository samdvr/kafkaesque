//! Integration tests for Raft consensus layer.
//!
//! These tests verify single-node Raft operations including state machine,
//! topic management, partition ownership, and consumer groups.
//!
//! **Note:** These tests require network socket access (TCP listener on 127.0.0.1).
//!
//! To run these tests:
//! ```bash
//! cargo test --test raft_integration_tests
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::time::sleep;

use kafkaesque::cluster::PartitionCoordinator;
use kafkaesque::cluster::raft::{
    BrokerCommand, BrokerResponse, CoordinationCommand, CoordinationResponse, GroupCommand,
    GroupResponse, PartitionCommand, PartitionResponse, ProducerCommand, ProducerResponse,
    RaftConfig, RaftCoordinator, RaftNode,
};

// ============================================================================
// Test Utilities
// ============================================================================

/// Atomic port counter to ensure unique ports per test.
static PORT_COUNTER: AtomicU16 = AtomicU16::new(19000);

/// Get a unique port for a test node.
fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Create a test configuration for a Raft node.
fn test_config(node_id: u64, port: u16) -> RaftConfig {
    RaftConfig {
        node_id,
        broker_id: node_id as i32,
        host: "127.0.0.1".to_string(),
        port: 9092 + node_id as i32,
        raft_addr: format!("127.0.0.1:{}", port),
        cluster_members: vec![],
        raft_log_dir: format!("/tmp/kafkaesque-test-{}/log", node_id),
        snapshot_dir: format!("/tmp/kafkaesque-test-{}/snapshots", node_id),
        // Fast timeouts for testing
        heartbeat_interval: Duration::from_millis(50),
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        max_payload_entries: 100,
        snapshot_threshold: 100,
        is_voter: true,
        lease_duration: Duration::from_secs(10),
        lease_renewal_interval: Duration::from_secs(3),
        broker_heartbeat_interval: Duration::from_secs(1),
        broker_heartbeat_ttl: Duration::from_secs(5),
        default_session_timeout_ms: 10_000,
        session_timeout_check_interval: Duration::from_secs(1),
        auto_create_topics: true,
        max_partitions_per_topic: 100,
        max_pending_proposals: 100,
        proposal_timeout: Duration::from_secs(5),
    }
}

/// Create a shared in-memory object store for a test cluster.
fn create_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// Wait for a specific node to become leader.
async fn wait_for_node_to_be_leader(node: &RaftNode, timeout_duration: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout_duration {
        if node.is_leader().await {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

// ============================================================================
// Single Node Tests
// ============================================================================

#[tokio::test]
async fn test_single_node_cluster_initialization() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let node = RaftNode::new(config, store).await.unwrap();

    // Initialize as single-node cluster
    node.initialize_cluster().await.unwrap();

    // Wait for the node to become leader
    let is_leader = wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await;
    assert!(is_leader, "Single node should become leader");

    // Verify we can write
    let response = node
        .write(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        }))
        .await
        .unwrap();

    assert!(
        matches!(
            response,
            CoordinationResponse::BrokerDomainResponse(BrokerResponse::Registered { broker_id: 1 })
        ),
        "Expected BrokerRegistered, got {:?}",
        response
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_single_node_state_machine_operations() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let node = RaftNode::new(config, store).await.unwrap();
    node.initialize_cluster().await.unwrap();

    // Wait for leader
    assert!(wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await);

    // Create topic
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::CreateTopic {
                name: "test-topic".to_string(),
                partitions: 3,
                config: HashMap::new(),
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicCreated { .. })
    ));

    // Acquire partition
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "test-topic".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 60_000,
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired { .. })
    ));

    // Verify state
    let sm = node.state_machine();
    let state = sm.read().await;
    let inner = state.state().await;
    assert!(inner.partition_domain.topics.contains_key("test-topic"));
    assert_eq!(
        inner
            .partition_domain
            .topics
            .get("test-topic")
            .unwrap()
            .partition_count,
        3
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_single_node_linearizable_read() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let node = RaftNode::new(config, store).await.unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await);

    // Ensure linearizable should succeed on leader
    let result = node.ensure_linearizable().await;
    assert!(result.is_ok(), "Leader should provide linearizable reads");

    node.shutdown().await.unwrap();
}

// ============================================================================
// Edge Case Tests
// ============================================================================

#[tokio::test]
async fn test_duplicate_topic_creation() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let node = RaftNode::new(config, store).await.unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await);

    // Create topic first time
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::CreateTopic {
                name: "duplicate-test".to_string(),
                partitions: 3,
                config: HashMap::new(),
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicCreated { .. })
    ));

    // Try to create same topic again
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::CreateTopic {
                name: "duplicate-test".to_string(),
                partitions: 5, // Different partitions
                config: HashMap::new(),
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicAlreadyExists { .. })
    ));

    // Original topic should remain unchanged
    let sm = node.state_machine();
    let state = sm.read().await;
    let inner = state.state().await;
    assert_eq!(
        inner
            .partition_domain
            .topics
            .get("duplicate-test")
            .unwrap()
            .partition_count,
        3
    );

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_ownership_conflict() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let node = RaftNode::new(config, store).await.unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await);

    // Create topic
    node.write(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "ownership-test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await
    .unwrap();

    // Broker 1 acquires partition
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "ownership-test".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 60_000,
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired { .. })
    ));

    // Broker 2 tries to acquire same partition
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "ownership-test".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 60_000,
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionOwnedByOther {
            owner: 1,
            ..
        })
    ));

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_lease_expiration() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let node = RaftNode::new(config, store).await.unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await);

    // Create topic
    node.write(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "lease-test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await
    .unwrap();

    // Acquire with short lease
    let acquire_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    node.write(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "lease-test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 100, // Very short lease
            timestamp_ms: acquire_time,
        },
    ))
    .await
    .unwrap();

    // Wait for lease to expire
    sleep(Duration::from_millis(200)).await;

    // Run expire leases
    let expire_time = acquire_time + 500; // Well after lease expires
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::ExpireLeases {
                current_time_ms: expire_time,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeasesExpired {
            count: 1
        })
    ));

    // Now broker 2 should be able to acquire
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "lease-test".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 60_000,
                timestamp_ms: expire_time,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired { .. })
    ));

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_lifecycle() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let node = RaftNode::new(config, store).await.unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await);

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Member joins group
    let response = node
        .write(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(), // Empty for new member
            client_id: "test-client".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![1, 2, 3])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 30000,
            timestamp_ms: now,
        }))
        .await
        .unwrap();

    let (generation, member_id) = match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            generation,
            member_id,
            ..
        }) => (generation, member_id),
        other => panic!("Expected JoinGroupResponse, got {:?}", other),
    };

    assert_eq!(generation, 1);
    assert!(!member_id.is_empty());

    // Member heartbeat
    let response = node
        .write(CoordinationCommand::GroupDomain(GroupCommand::Heartbeat {
            group_id: "test-group".to_string(),
            member_id: member_id.clone(),
            generation: 1,
            timestamp_ms: now + 1000,
        }))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::HeartbeatAck)
    ));

    // Member leaves
    let response = node
        .write(CoordinationCommand::GroupDomain(GroupCommand::LeaveGroup {
            group_id: "test-group".to_string(),
            member_id: member_id.clone(),
        }))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::GroupDomainResponse(GroupResponse::LeftGroup)
    ));

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_producer_id_allocation() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let node = RaftNode::new(config, store).await.unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await);

    // Allocate multiple producer IDs
    let mut ids = vec![];
    for _ in 0..5 {
        let response = node
            .write(CoordinationCommand::ProducerDomain(
                ProducerCommand::AllocateProducerId,
            ))
            .await
            .unwrap();
        if let CoordinationResponse::ProducerDomainResponse(
            ProducerResponse::ProducerIdAllocated { producer_id, .. },
        ) = response
        {
            ids.push(producer_id);
        } else {
            panic!("Expected ProducerIdAllocated");
        }
    }

    // All IDs should be unique and sequential
    for i in 1..ids.len() {
        assert!(ids[i] > ids[i - 1], "Producer IDs should be increasing");
    }

    node.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_backpressure_under_load() {
    let port = next_port();
    let mut config = test_config(1, port);
    config.max_pending_proposals = 5; // Very low limit for testing
    config.proposal_timeout = Duration::from_millis(100);

    let store = create_object_store();
    let node = Arc::new(RaftNode::new(config, store).await.unwrap());
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_node_to_be_leader(&node, Duration::from_secs(5)).await);

    // Flood with many concurrent requests
    let mut handles = vec![];
    for i in 0..20 {
        let node_clone = node.clone();
        handles.push(tokio::spawn(async move {
            node_clone
                .write(CoordinationCommand::PartitionDomain(
                    PartitionCommand::CreateTopic {
                        name: format!("backpressure-topic-{}", i),
                        partitions: 1,
                        config: HashMap::new(),
                        timestamp_ms: 1000,
                    },
                ))
                .await
        }));
    }

    let mut success = 0;

    for handle in handles {
        if let Ok(Ok(_)) = handle.await {
            success += 1;
        }
    }

    // Some requests should succeed
    assert!(success > 0, "Some requests should succeed");

    node.shutdown().await.unwrap();
}

// ============================================================================
// RaftCoordinator Integration Tests
// ============================================================================

#[tokio::test]
async fn test_raft_coordinator_basic_operations() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        coordinator.is_leader().await,
        "Coordinator should be leader"
    );

    // Register broker
    coordinator.register_broker().await.unwrap();

    // Create topic
    coordinator
        .register_topic("coord-test-topic", 3)
        .await
        .unwrap();

    // Verify topic exists
    assert!(coordinator.topic_exists("coord-test-topic").await.unwrap());
    assert_eq!(
        coordinator
            .get_partition_count("coord-test-topic")
            .await
            .unwrap(),
        Some(3)
    );

    // Acquire partition
    let acquired = coordinator
        .acquire_partition("coord-test-topic", 0, 60)
        .await
        .unwrap();
    assert!(acquired);

    // Verify ownership
    assert!(
        coordinator
            .owns_partition_for_read("coord-test-topic", 0)
            .await
            .unwrap()
    );

    // Release partition
    coordinator
        .release_partition("coord-test-topic", 0)
        .await
        .unwrap();

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_raft_coordinator_with_background_tasks() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    // Start background tasks
    coordinator.start_background_tasks().await;
    coordinator.register_broker().await.unwrap();

    // Give background tasks time to run a few iterations
    sleep(Duration::from_secs(2)).await;

    // Coordinator should still be functional
    coordinator
        .register_topic("background-test-topic", 1)
        .await
        .unwrap();

    assert!(
        coordinator
            .topic_exists("background-test-topic")
            .await
            .unwrap()
    );

    coordinator.shutdown().await.unwrap();
}

// ============================================================================
// PartitionTransferCoordinator Integration Tests
// ============================================================================

use kafkaesque::cluster::PartitionTransferCoordinator;
use kafkaesque::cluster::raft::{PartitionTransfer, TransferReason};

#[tokio::test]
async fn test_partition_transfer_coordinator_get_active_brokers() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    // Register broker
    coordinator.register_broker().await.unwrap();

    // Get active brokers
    let active_brokers = coordinator.get_active_broker_ids().await;
    assert!(active_brokers.contains(&1), "Broker 1 should be active");

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_transfer_coordinator_get_all_owners() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Create topic and acquire partition
    coordinator.register_topic("owners-test", 2).await.unwrap();
    coordinator
        .acquire_partition("owners-test", 0, 60)
        .await
        .unwrap();
    coordinator
        .acquire_partition("owners-test", 1, 60)
        .await
        .unwrap();

    // Get all partition owners
    let owners = coordinator.get_all_partition_owners().await;
    assert_eq!(owners.get(&("owners-test".to_string(), 0)), Some(&1));
    assert_eq!(owners.get(&("owners-test".to_string(), 1)), Some(&1));

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_transfer_coordinator_get_owners_with_index() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("indexed-test", 1).await.unwrap();
    coordinator
        .acquire_partition("indexed-test", 0, 60)
        .await
        .unwrap();

    // Get all partition owners with Raft index
    let (owners, index) = coordinator.get_all_partition_owners_with_index().await;
    assert!(owners.contains_key(&("indexed-test".to_string(), 0)));
    assert!(index > 0, "Raft index should be > 0 after operations");

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_transfer_mark_broker_failed() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("fail-test", 1).await.unwrap();
    coordinator
        .acquire_partition("fail-test", 0, 60)
        .await
        .unwrap();

    // Mark a different broker as failed (broker 99 doesn't exist, but we're testing the path)
    let result = coordinator
        .mark_broker_failed(99, "Test failure detection")
        .await;
    assert!(result.is_ok());

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_transfer_single_transfer() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator
        .register_topic("transfer-test", 1)
        .await
        .unwrap();
    coordinator
        .acquire_partition("transfer-test", 0, 60)
        .await
        .unwrap();

    // Verify we own the partition before transfer
    let owners = coordinator.get_all_partition_owners().await;
    assert_eq!(owners.get(&("transfer-test".to_string(), 0)), Some(&1));

    // Transfer partition from broker 1 to broker 1 (same broker, but tests the path)
    // In a real scenario, this would be to a different broker
    let result = coordinator
        .transfer_partition(
            "transfer-test",
            0,
            1, // from broker 1
            1, // to broker 1 (self-transfer for testing)
            TransferReason::LoadBalancing,
            60000,
        )
        .await;

    // This should succeed since source and dest are the same
    assert!(result.is_ok());

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_transfer_batch_transfer() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("batch-test", 3).await.unwrap();

    // Acquire multiple partitions
    coordinator
        .acquire_partition("batch-test", 0, 60)
        .await
        .unwrap();
    coordinator
        .acquire_partition("batch-test", 1, 60)
        .await
        .unwrap();
    coordinator
        .acquire_partition("batch-test", 2, 60)
        .await
        .unwrap();

    // Create batch transfer (self-transfer for testing)
    let transfers = vec![
        PartitionTransfer {
            topic: "batch-test".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 1,
            expected_leader_epoch: None,
        },
        PartitionTransfer {
            topic: "batch-test".to_string(),
            partition: 1,
            from_broker_id: 1,
            to_broker_id: 1,
            expected_leader_epoch: None,
        },
    ];

    let result = coordinator
        .batch_transfer_partitions(transfers, TransferReason::LoadBalancing, 60000)
        .await;

    assert!(result.is_ok());
    let (success_count, failures) = result.unwrap();
    assert_eq!(success_count, 2);
    assert!(failures.is_empty());

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_transfer_nonexistent_partition() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Try to transfer a partition that doesn't exist
    let result = coordinator
        .transfer_partition(
            "nonexistent-topic",
            0,
            1,
            2,
            TransferReason::LoadBalancing,
            60000,
        )
        .await;

    // Should fail because partition doesn't exist
    assert!(result.is_err());

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_transfer_wrong_source_owner() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator
        .register_topic("wrong-owner-test", 1)
        .await
        .unwrap();
    coordinator
        .acquire_partition("wrong-owner-test", 0, 60)
        .await
        .unwrap();

    // Try to transfer from broker 99 (not the owner)
    let result = coordinator
        .transfer_partition(
            "wrong-owner-test",
            0,
            99, // Wrong source owner
            2,
            TransferReason::LoadBalancing,
            60000,
        )
        .await;

    // Should fail because broker 99 is not the owner
    assert!(result.is_err());

    coordinator.shutdown().await.unwrap();
}

// ============================================================================
// Additional PartitionCoordinator Tests
// ============================================================================

#[tokio::test]
async fn test_partition_coordinator_delete_topic() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Create topic
    coordinator
        .register_topic("delete-topic-test", 3)
        .await
        .unwrap();
    assert!(coordinator.topic_exists("delete-topic-test").await.unwrap());

    // Delete topic
    let deleted = coordinator.delete_topic("delete-topic-test").await.unwrap();
    assert!(deleted);

    // Verify topic no longer exists
    assert!(!coordinator.topic_exists("delete-topic-test").await.unwrap());

    // Try to delete non-existent topic
    let deleted_again = coordinator.delete_topic("delete-topic-test").await.unwrap();
    assert!(!deleted_again);

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_get_partition_owners() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("owner-test", 3).await.unwrap();

    // Acquire some partitions
    coordinator
        .acquire_partition("owner-test", 0, 60)
        .await
        .unwrap();
    coordinator
        .acquire_partition("owner-test", 2, 60)
        .await
        .unwrap();

    // Get partition owners
    let owners = coordinator
        .get_partition_owners("owner-test")
        .await
        .unwrap();
    assert_eq!(owners.len(), 3);
    assert_eq!(owners[0], (0, Some(1))); // Partition 0 owned by broker 1
    assert_eq!(owners[1], (1, None)); // Partition 1 not owned
    assert_eq!(owners[2], (2, Some(1))); // Partition 2 owned by broker 1

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_get_assigned_partitions() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator
        .register_topic("assigned-test", 5)
        .await
        .unwrap();

    // With only one broker, all partitions should be assigned to it
    let assigned = coordinator.get_assigned_partitions().await.unwrap();

    // All 5 partitions should be assigned to broker 1 (single broker)
    assert_eq!(assigned.len(), 5);
    for (topic, partition) in &assigned {
        assert_eq!(topic, "assigned-test");
        assert!(*partition >= 0 && *partition < 5);
    }

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_should_own_partition() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator
        .register_topic("should-own-test", 3)
        .await
        .unwrap();

    // With single broker, should_own_partition should return true for all partitions
    for p in 0..3 {
        let should_own = coordinator
            .should_own_partition("should-own-test", p)
            .await
            .unwrap();
        assert!(should_own, "Single broker should own all partitions");
    }

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_owns_partition_for_read() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("read-test", 2).await.unwrap();

    // Before acquiring, should not own for read
    let owns_before = coordinator
        .owns_partition_for_read("read-test", 0)
        .await
        .unwrap();
    assert!(!owns_before);

    // Acquire partition
    coordinator
        .acquire_partition("read-test", 0, 60)
        .await
        .unwrap();

    // After acquiring, should own for read
    let owns_after = coordinator
        .owns_partition_for_read("read-test", 0)
        .await
        .unwrap();
    assert!(owns_after);

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_owns_partition_for_write() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("write-test", 2).await.unwrap();

    // Before acquiring, should_partition_for_write should fail
    let write_result = coordinator
        .owns_partition_for_write("write-test", 0, 60)
        .await;
    assert!(write_result.is_err());

    // Acquire partition
    coordinator
        .acquire_partition("write-test", 0, 60)
        .await
        .unwrap();

    // After acquiring, should return TTL
    let ttl = coordinator
        .owns_partition_for_write("write-test", 0, 60)
        .await
        .unwrap();
    assert!(ttl > 0, "TTL should be positive");

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_cache_invalidation() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("cache-test", 2).await.unwrap();
    coordinator
        .acquire_partition("cache-test", 0, 60)
        .await
        .unwrap();

    // Populate cache by reading
    let _ = coordinator.owns_partition_for_read("cache-test", 0).await;

    // Invalidate single partition cache
    coordinator
        .invalidate_ownership_cache("cache-test", 0)
        .await;

    // Invalidate all cache
    coordinator.invalidate_all_ownership_cache().await;

    // Should still work after cache invalidation
    let owns = coordinator
        .owns_partition_for_read("cache-test", 0)
        .await
        .unwrap();
    assert!(owns);

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_get_topics() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Create multiple topics
    coordinator.register_topic("topic-alpha", 1).await.unwrap();
    coordinator.register_topic("topic-beta", 2).await.unwrap();
    coordinator.register_topic("topic-gamma", 3).await.unwrap();

    // Get all topics
    let topics = coordinator.get_topics().await.unwrap();
    assert!(topics.contains(&"topic-alpha".to_string()));
    assert!(topics.contains(&"topic-beta".to_string()));
    assert!(topics.contains(&"topic-gamma".to_string()));

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_heartbeat() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Send heartbeat
    let result = coordinator.heartbeat().await;
    assert!(result.is_ok());

    // Send another heartbeat
    let result2 = coordinator.heartbeat().await;
    assert!(result2.is_ok());

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_partition_coordinator_get_live_brokers() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Get live brokers
    let brokers = coordinator.get_live_brokers().await.unwrap();
    assert!(!brokers.is_empty());
    assert!(brokers.iter().any(|b| b.broker_id == 1));

    coordinator.shutdown().await.unwrap();
}

// ============================================================================
// ConsumerGroupCoordinator Integration Tests
// ============================================================================

use kafkaesque::cluster::ConsumerGroupCoordinator;

#[tokio::test]
async fn test_consumer_group_complete_rebalance() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Join group first
    let (generation, _member_id, _, _, _) = coordinator
        .join_group("rebalance-group", "", &[1, 2, 3], 30000)
        .await
        .unwrap();

    // Complete rebalance with correct generation should succeed
    let result = coordinator
        .complete_rebalance("rebalance-group", generation)
        .await
        .unwrap();
    assert!(result);

    // Complete rebalance with wrong generation should fail
    let result = coordinator
        .complete_rebalance("rebalance-group", generation + 100)
        .await
        .unwrap();
    assert!(!result);

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_get_generation() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Non-existent group should return 0
    let group_gen = coordinator.get_generation("no-such-group").await.unwrap();
    assert_eq!(group_gen, 0);

    // Join group
    let (generation, _, _, _, _) = coordinator
        .join_group("gen-test-group", "", &[1, 2, 3], 30000)
        .await
        .unwrap();

    // Get generation should match
    let group_gen = coordinator.get_generation("gen-test-group").await.unwrap();
    assert_eq!(group_gen, generation);

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_get_members() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Empty group should have no members
    let members = coordinator.get_group_members("empty-group").await.unwrap();
    assert!(members.is_empty());

    // Join group
    let (_, member_id, _, _, _) = coordinator
        .join_group("member-test-group", "", &[1, 2, 3], 30000)
        .await
        .unwrap();

    // Get members should include the joined member
    let members = coordinator
        .get_group_members("member-test-group")
        .await
        .unwrap();
    assert!(members.contains(&member_id));

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_get_leader() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Non-existent group should have no leader
    let leader = coordinator.get_group_leader("no-group").await.unwrap();
    assert!(leader.is_none());

    // Join group as first member (becomes leader)
    let (_, member_id, is_leader, _, _) = coordinator
        .join_group("leader-test-group", "", &[1, 2, 3], 30000)
        .await
        .unwrap();

    assert!(is_leader, "First member should be leader");

    // Get leader should return the member
    let leader = coordinator
        .get_group_leader("leader-test-group")
        .await
        .unwrap();
    assert_eq!(leader, Some(member_id));

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_validate_member_for_commit() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Join group
    let (generation, member_id, _, _, _) = coordinator
        .join_group("validate-group", "", &[1, 2, 3], 30000)
        .await
        .unwrap();

    // Validate with correct member and generation
    let result = coordinator
        .validate_member_for_commit("validate-group", &member_id, generation)
        .await
        .unwrap();
    assert!(matches!(
        result,
        kafkaesque::cluster::HeartbeatResult::Success
    ));

    // Validate with wrong member
    let result = coordinator
        .validate_member_for_commit("validate-group", "unknown-member", generation)
        .await
        .unwrap();
    assert!(matches!(
        result,
        kafkaesque::cluster::HeartbeatResult::UnknownMember
    ));

    // Validate with wrong generation
    let result = coordinator
        .validate_member_for_commit("validate-group", &member_id, generation + 100)
        .await
        .unwrap();
    assert!(matches!(
        result,
        kafkaesque::cluster::HeartbeatResult::IllegalGeneration
    ));

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_set_assignments_atomically() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Join group as leader
    let (generation, member_id, is_leader, _, _) = coordinator
        .join_group("assign-group", "", &[1, 2, 3], 30000)
        .await
        .unwrap();

    assert!(is_leader);

    // Set assignments
    let assignments = vec![(member_id.clone(), vec![1, 2, 3, 4])];
    let result = coordinator
        .set_assignments_atomically("assign-group", &member_id, generation, &assignments)
        .await
        .unwrap();
    assert!(result);

    // Get assignment should return what we set
    let assignment = coordinator
        .get_member_assignment("assign-group", &member_id)
        .await
        .unwrap();
    assert_eq!(assignment, vec![1, 2, 3, 4]);

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_evict_expired_members() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Just test that the function runs without error
    let expired = coordinator.evict_expired_members(30000).await.unwrap();
    // No members should be expired if none joined
    assert!(expired.is_empty());

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_get_all_groups() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Join multiple groups
    coordinator
        .join_group("all-groups-1", "", &[1], 30000)
        .await
        .unwrap();
    coordinator
        .join_group("all-groups-2", "", &[2], 30000)
        .await
        .unwrap();
    coordinator
        .join_group("all-groups-3", "", &[3], 30000)
        .await
        .unwrap();

    // Get all groups
    let groups = coordinator.get_all_groups().await.unwrap();
    assert!(groups.contains(&"all-groups-1".to_string()));
    assert!(groups.contains(&"all-groups-2".to_string()));
    assert!(groups.contains(&"all-groups-3".to_string()));

    coordinator.shutdown().await.unwrap();
}

#[tokio::test]
async fn test_consumer_group_commit_and_fetch_offset() {
    let port = next_port();
    let config = test_config(1, port);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store).await.unwrap();
    coordinator.initialize_cluster().await.unwrap();

    // Wait for leader
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }

    coordinator.register_broker().await.unwrap();

    // Commit offset
    coordinator
        .commit_offset("offset-group", "test-topic", 0, 100, Some("metadata"))
        .await
        .unwrap();

    // Fetch offset
    let (offset, metadata) = coordinator
        .fetch_offset("offset-group", "test-topic", 0)
        .await
        .unwrap();
    assert_eq!(offset, 100);
    assert_eq!(metadata, Some("metadata".to_string()));

    // Fetch non-existent offset
    let (offset, metadata) = coordinator
        .fetch_offset("offset-group", "other-topic", 0)
        .await
        .unwrap();
    assert_eq!(offset, -1);
    assert!(metadata.is_none());

    coordinator.shutdown().await.unwrap();
}
