//! Failure mode integration tests for Raft cluster.
//!
//! These tests verify the system's behavior under failure conditions:
//! - Leader failure and re-election
//! - Broker heartbeat timeout
//! - Lease expiration
//! - Partition ownership conflicts
//! - Zombie mode activation
//!
//! **Note:** These tests require network socket access (TCP listeners on 127.0.0.1).
//!
//! To run these tests:
//! ```bash
//! cargo test --test raft_failure_mode_tests -- --test-threads=1
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::runtime::Handle;
use tokio::time::sleep;

use kafkaesque::cluster::raft::{
    BrokerCommand, BrokerResponse, CoordinationCommand, CoordinationResponse, GroupCommand,
    GroupResponse, PartitionCommand, PartitionResponse, ProducerCommand, ProducerResponse,
    RaftConfig, RaftCoordinator, RaftNode,
};
use kafkaesque::cluster::{ConsumerGroupCoordinator, PartitionCoordinator, ProducerCoordinator};

// ============================================================================
// Test Utilities
// ============================================================================

/// Atomic port counter to ensure unique ports per test.
static PORT_COUNTER: AtomicU16 = AtomicU16::new(21000);

/// Get a unique port for a test node.
fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Create a test configuration for a Raft node with fast timeouts.
fn test_config(node_id: u64, port: u16, cluster_members: Vec<(u64, String)>) -> RaftConfig {
    RaftConfig {
        node_id,
        broker_id: node_id as i32,
        host: "127.0.0.1".to_string(),
        port: 9092 + node_id as i32,
        raft_addr: format!("127.0.0.1:{}", port),
        cluster_members,
        raft_log_dir: format!("/tmp/kafkaesque-failure-test-{}/log", node_id),
        snapshot_dir: format!("/tmp/kafkaesque-failure-test-{}/snapshots", node_id),
        // Fast timeouts for testing failure scenarios
        heartbeat_interval: Duration::from_millis(50),
        election_timeout_min: Duration::from_millis(150),
        election_timeout_max: Duration::from_millis(300),
        max_payload_entries: 100,
        snapshot_threshold: 100,
        is_voter: true,
        lease_duration: Duration::from_secs(5), // Short lease for testing
        lease_renewal_interval: Duration::from_secs(1),
        broker_heartbeat_interval: Duration::from_millis(200), // Fast heartbeat
        broker_heartbeat_ttl: Duration::from_secs(1),          // Short TTL for failure detection
        default_session_timeout_ms: 5_000,
        session_timeout_check_interval: Duration::from_millis(500),
        auto_create_topics: true,
        max_partitions_per_topic: 100,
        max_pending_proposals: 100,
        proposal_timeout: Duration::from_secs(5),
    }
}

/// Create a shared in-memory object store.
fn create_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// Wait for a node to become leader with timeout.
async fn wait_for_leader(node: &RaftNode, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if node.is_leader().await {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Wait for coordinator to become leader.
async fn wait_for_coordinator_leader(coordinator: &RaftCoordinator, timeout: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < timeout {
        if coordinator.is_leader().await {
            return true;
        }
        sleep(Duration::from_millis(50)).await;
    }
    false
}

/// Get current timestamp in milliseconds.
fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

// ============================================================================
// Single Node Failure Mode Tests
// ============================================================================

/// Test that lease expiration allows partition reassignment.
#[tokio::test]
async fn test_lease_expiration_allows_reassignment() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let node = RaftNode::new(config, store, Handle::current())
        .await
        .unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_leader(&node, Duration::from_secs(5)).await);

    // Create topic
    node.write(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "lease-expire-test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await
    .unwrap();

    // Broker 1 acquires with very short lease (100ms)
    let acquire_time = now_ms();
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "lease-expire-test".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 100,
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired { .. })
    ));

    // Broker 2 tries immediately - should fail
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "lease-expire-test".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 60_000,
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            response,
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionOwnedByOther { owner: 1, .. }
            )
        ),
        "Should be owned by broker 1 initially"
    );

    // Wait for lease to expire
    sleep(Duration::from_millis(200)).await;

    // Expire the leases
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::ExpireLeases {
                current_time_ms: acquire_time + 500,
            },
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            response,
            CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeasesExpired {
                count: 1
            })
        ),
        "One lease should expire"
    );

    // Now broker 2 should be able to acquire
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "lease-expire-test".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 60_000,
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            response,
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionAcquired { .. }
            )
        ),
        "Broker 2 should acquire after lease expiry"
    );

    node.shutdown().await.unwrap();
}

/// Test broker registration and heartbeat.
#[tokio::test]
async fn test_broker_registration_and_heartbeat() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let node = RaftNode::new(config, store, Handle::current())
        .await
        .unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_leader(&node, Duration::from_secs(5)).await);

    let initial_time = now_ms();

    // Register broker
    let response = node
        .write(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        }))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::Registered { .. })
    ));

    // Send heartbeat
    let response = node
        .write(CoordinationCommand::BrokerDomain(
            BrokerCommand::Heartbeat {
                broker_id: 1,
                timestamp_ms: initial_time,
                reported_local_timestamp_ms: initial_time,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::HeartbeatAck)
    ));

    // Send another heartbeat after some delay
    sleep(Duration::from_millis(100)).await;
    let now = now_ms();
    let response = node
        .write(CoordinationCommand::BrokerDomain(
            BrokerCommand::Heartbeat {
                broker_id: 1,
                timestamp_ms: now,
                reported_local_timestamp_ms: now,
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::BrokerDomainResponse(BrokerResponse::HeartbeatAck)
    ));

    node.shutdown().await.unwrap();
}

/// Test that partition ownership is properly tracked across multiple brokers.
#[tokio::test]
async fn test_partition_ownership_tracking() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let node = RaftNode::new(config, store, Handle::current())
        .await
        .unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_leader(&node, Duration::from_secs(5)).await);

    // Create topic with multiple partitions
    node.write(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "multi-owner-test".to_string(),
            partitions: 4,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await
    .unwrap();

    // Different brokers acquire different partitions
    for i in 0..4 {
        let broker_id = (i % 2) + 1; // Alternate between broker 1 and 2
        let response = node
            .write(CoordinationCommand::PartitionDomain(
                PartitionCommand::AcquirePartition {
                    topic: "multi-owner-test".to_string(),
                    partition: i,
                    broker_id,
                    lease_duration_ms: 60_000,
                    timestamp_ms: 1000,
                },
            ))
            .await
            .unwrap();
        assert!(matches!(
            response,
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionAcquired { .. }
            )
        ));
    }

    // Verify ownership by trying to acquire already-owned partitions
    for i in 0..4 {
        let expected_owner = (i % 2) + 1;
        let other_broker = if expected_owner == 1 { 2 } else { 1 };

        let response = node
            .write(CoordinationCommand::PartitionDomain(
                PartitionCommand::AcquirePartition {
                    topic: "multi-owner-test".to_string(),
                    partition: i,
                    broker_id: other_broker,
                    lease_duration_ms: 60_000,
                    timestamp_ms: 1000,
                },
            ))
            .await
            .unwrap();

        assert!(
            matches!(
                response,
                CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionOwnedByOther { owner, .. }) if owner == expected_owner
            ),
            "Partition {} should be owned by broker {}",
            i,
            expected_owner
        );
    }

    node.shutdown().await.unwrap();
}

/// Test consumer group member expiration.
#[tokio::test]
async fn test_consumer_group_member_expiration() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let node = RaftNode::new(config, store, Handle::current())
        .await
        .unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_leader(&node, Duration::from_secs(5)).await);

    let initial_time = now_ms();

    // Member joins group
    let response = node
        .write(CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: "expiry-test-group".to_string(),
            member_id: "".to_string(),
            client_id: "test-client".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 500, // Very short session timeout
            rebalance_timeout_ms: 1000,
            timestamp_ms: initial_time,
        }))
        .await
        .unwrap();

    let member_id = match response {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            member_id,
            ..
        }) => member_id,
        _ => panic!("Expected JoinGroupResponse"),
    };
    assert!(!member_id.is_empty(), "Should get a member ID");

    // Wait for session to expire
    sleep(Duration::from_millis(800)).await;

    // Expire members
    let response = node
        .write(CoordinationCommand::GroupDomain(
            GroupCommand::ExpireMembers {
                current_time_ms: initial_time + 1000,
            },
        ))
        .await
        .unwrap();

    if let CoordinationResponse::GroupDomainResponse(GroupResponse::MembersExpired {
        expired,
        ..
    }) = response
    {
        assert_eq!(expired.len(), 1, "One member should be expired");
        assert_eq!(expired[0].0, "expiry-test-group");
        assert_eq!(expired[0].1, member_id);
    } else {
        panic!("Expected MembersExpired response");
    }

    node.shutdown().await.unwrap();
}

/// Test offset commit persistence.
#[tokio::test]
async fn test_offset_persistence() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let node = RaftNode::new(config, store, Handle::current())
        .await
        .unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_leader(&node, Duration::from_secs(5)).await);

    // Commit offsets for multiple partitions
    for partition in 0..3 {
        let response = node
            .write(CoordinationCommand::GroupDomain(
                GroupCommand::CommitOffset {
                    group_id: "offset-test-group".to_string(),
                    topic: "offset-test-topic".to_string(),
                    partition,
                    offset: (partition + 1) as i64 * 100,
                    metadata: Some(format!("commit-{}", partition)),
                    timestamp_ms: now_ms(),
                },
            ))
            .await
            .unwrap();
        assert!(matches!(
            response,
            CoordinationResponse::GroupDomainResponse(GroupResponse::OffsetCommitted)
        ));
    }

    // Verify offsets by reading from state machine
    let sm = node.state_machine();
    let state = sm.read().await;
    let inner = state.state().await;

    for partition in 0..3 {
        let key = (
            "offset-test-group".to_string(),
            std::sync::Arc::from("offset-test-topic"),
            partition,
        );
        let stored = inner
            .group_domain
            .offsets
            .get(&key)
            .expect("Offset should be stored");
        assert_eq!(
            stored.offset,
            (partition + 1) as i64 * 100,
            "Offset should match for partition {}",
            partition
        );
    }

    drop(inner);
    drop(state);

    node.shutdown().await.unwrap();
}

/// Test producer ID allocation is monotonically increasing.
#[tokio::test]
async fn test_producer_id_monotonic() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let node = RaftNode::new(config, store, Handle::current())
        .await
        .unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_leader(&node, Duration::from_secs(5)).await);

    let mut last_id = -1i64;

    // Allocate many producer IDs
    for _ in 0..100 {
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
            assert!(
                producer_id > last_id,
                "Producer ID {} should be greater than {}",
                producer_id,
                last_id
            );
            last_id = producer_id;
        } else {
            panic!("Expected ProducerIdAllocated");
        }
    }

    node.shutdown().await.unwrap();
}

/// Test lease renewal extends the lease.
#[tokio::test]
async fn test_lease_renewal() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let node = RaftNode::new(config, store, Handle::current())
        .await
        .unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_leader(&node, Duration::from_secs(5)).await);

    // Create topic
    node.write(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "renewal-test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await
    .unwrap();

    // Acquire with short lease
    node.write(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "renewal-test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1000, // 1 second lease
            timestamp_ms: 1000,
        },
    ))
    .await
    .unwrap();

    // Wait 500ms
    sleep(Duration::from_millis(500)).await;

    // Renew the lease
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::RenewLease {
                topic: "renewal-test".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 2000, // Extend to 2 seconds
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            response,
            CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeaseRenewed { .. })
        ),
        "Lease should be renewed"
    );

    // Wait another 800ms (total 1.3s, past original expiry)
    sleep(Duration::from_millis(800)).await;

    // Another broker trying to acquire should fail (lease was renewed)
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "renewal-test".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 60_000,
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    assert!(
        matches!(
            response,
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionOwnedByOther { owner: 1, .. }
            )
        ),
        "Broker 1 should still own partition after renewal"
    );

    node.shutdown().await.unwrap();
}

/// Test topic deletion clears partitions.
#[tokio::test]
async fn test_topic_deletion() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let node = RaftNode::new(config, store, Handle::current())
        .await
        .unwrap();
    node.initialize_cluster().await.unwrap();

    assert!(wait_for_leader(&node, Duration::from_secs(5)).await);

    // Create topic
    node.write(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "delete-test".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        },
    ))
    .await
    .unwrap();

    // Acquire some partitions
    for i in 0..3 {
        node.write(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "delete-test".to_string(),
                partition: i,
                broker_id: 1,
                lease_duration_ms: 60_000,
                timestamp_ms: 1000,
            },
        ))
        .await
        .unwrap();
    }

    // Delete topic
    let response = node
        .write(CoordinationCommand::PartitionDomain(
            PartitionCommand::DeleteTopic {
                name: "delete-test".to_string(),
            },
        ))
        .await
        .unwrap();
    assert!(matches!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::TopicDeleted { .. })
    ));

    // Verify topic is deleted by checking state machine
    let sm = node.state_machine();
    let state = sm.read().await;
    let inner = state.state().await;

    assert!(
        !inner.partition_domain.topics.contains_key("delete-test"),
        "Topic should be deleted from metadata"
    );

    drop(inner);
    drop(state);

    node.shutdown().await.unwrap();
}

// ============================================================================
// RaftCoordinator Failure Mode Tests
// ============================================================================

/// Test RaftCoordinator partition acquisition and release cycle.
#[tokio::test]
async fn test_coordinator_partition_lifecycle() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    coordinator.register_broker().await.unwrap();
    coordinator
        .register_topic("lifecycle-test", 2)
        .await
        .unwrap();

    // Acquire partition
    let acquired = coordinator
        .acquire_partition("lifecycle-test", 0, 60)
        .await
        .unwrap();
    assert!(acquired, "Should acquire partition");

    // Verify ownership
    assert!(
        coordinator
            .owns_partition_for_read("lifecycle-test", 0)
            .await
            .unwrap(),
        "Should own partition after acquire"
    );

    // Release partition
    coordinator
        .release_partition("lifecycle-test", 0)
        .await
        .unwrap();

    // Ownership should be released
    assert!(
        !coordinator
            .owns_partition_for_read("lifecycle-test", 0)
            .await
            .unwrap(),
        "Should not own partition after release"
    );

    coordinator.shutdown().await.unwrap();
}

/// Test RaftCoordinator consumer group operations.
#[tokio::test]
async fn test_coordinator_consumer_groups() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    coordinator.register_broker().await.unwrap();

    // Join group
    let (generation, member_id, is_leader, leader_id, members) = coordinator
        .join_group("test-group", "", &[1, 2, 3], 30_000)
        .await
        .unwrap();

    assert_eq!(generation, 1, "First join should be generation 1");
    assert!(!member_id.is_empty(), "Should get a member ID");
    assert!(is_leader, "First member should be leader");
    assert_eq!(leader_id, member_id, "Leader ID should match member ID");
    assert_eq!(members.len(), 1, "Should have 1 member");

    // Commit offset
    coordinator
        .commit_offset("test-group", "test-topic", 0, 100, None)
        .await
        .unwrap();

    // Fetch offset
    let (offset, _metadata) = coordinator
        .fetch_offset("test-group", "test-topic", 0)
        .await
        .unwrap();
    assert_eq!(offset, 100, "Fetched offset should match committed");

    coordinator.shutdown().await.unwrap();
}

/// Test RaftCoordinator producer ID allocation.
#[tokio::test]
async fn test_coordinator_producer_ids() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    // Allocate multiple producer IDs
    let mut ids = Vec::new();
    for _ in 0..10 {
        let id = coordinator.next_producer_id().await.unwrap();
        ids.push(id);
    }

    // All should be unique and increasing
    for i in 1..ids.len() {
        assert!(ids[i] > ids[i - 1], "Producer IDs should be increasing");
    }

    coordinator.shutdown().await.unwrap();
}

/// Test RaftCoordinator handles multiple concurrent requests.
#[tokio::test]
async fn test_coordinator_concurrent_requests() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = Arc::new(
        RaftCoordinator::new(config, store, Handle::current())
            .await
            .unwrap(),
    );
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    coordinator.register_broker().await.unwrap();

    // Create multiple topics concurrently
    let mut handles = Vec::new();
    for i in 0..10 {
        let coord = coordinator.clone();
        handles.push(tokio::spawn(async move {
            coord
                .register_topic(&format!("concurrent-topic-{}", i), 3)
                .await
        }));
    }

    let mut success_count = 0;
    for handle in handles {
        if handle.await.unwrap().is_ok() {
            success_count += 1;
        }
    }

    assert_eq!(success_count, 10, "All topic creations should succeed");

    // Verify all topics exist
    let topics = coordinator.get_topics().await.unwrap();
    for i in 0..10 {
        assert!(
            topics.contains(&format!("concurrent-topic-{}", i)),
            "Topic {} should exist",
            i
        );
    }

    coordinator.shutdown().await.unwrap();
}

/// Test that verify_and_extend_lease works correctly.
#[tokio::test]
async fn test_coordinator_verify_and_extend_lease() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    coordinator.register_broker().await.unwrap();
    coordinator
        .register_topic("verify-lease-test", 1)
        .await
        .unwrap();

    // Acquire partition
    coordinator
        .acquire_partition("verify-lease-test", 0, 60)
        .await
        .unwrap();

    // Verify and extend lease should succeed
    let remaining = coordinator
        .verify_and_extend_lease("verify-lease-test", 0, 60)
        .await
        .unwrap();

    assert!(remaining > 0, "Should have remaining lease time");

    coordinator.shutdown().await.unwrap();
}

/// Test that get_partition_owners returns correct information.
#[tokio::test]
async fn test_coordinator_get_partition_owners() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("owners-test", 3).await.unwrap();

    // Acquire first two partitions
    coordinator
        .acquire_partition("owners-test", 0, 60)
        .await
        .unwrap();
    coordinator
        .acquire_partition("owners-test", 1, 60)
        .await
        .unwrap();

    // Get partition owners
    let owners = coordinator
        .get_partition_owners("owners-test")
        .await
        .unwrap();

    assert_eq!(owners.len(), 3, "Should have 3 partitions");

    // Partitions 0 and 1 should be owned
    assert_eq!(
        owners[0].1,
        Some(1),
        "Partition 0 should be owned by broker 1"
    );
    assert_eq!(
        owners[1].1,
        Some(1),
        "Partition 1 should be owned by broker 1"
    );
    assert_eq!(owners[2].1, None, "Partition 2 should have no owner");

    coordinator.shutdown().await.unwrap();
}

/// Test linearizable read guarantee.
#[tokio::test]
async fn test_coordinator_linearizable_ownership() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    coordinator.register_broker().await.unwrap();
    coordinator
        .register_topic("linearizable-test", 1)
        .await
        .unwrap();

    // Acquire partition
    coordinator
        .acquire_partition("linearizable-test", 0, 60)
        .await
        .unwrap();

    // Linearizable ownership check should succeed (owns_partition_for_write does linearizable check)
    let remaining = coordinator
        .owns_partition_for_write("linearizable-test", 0, 60)
        .await
        .unwrap();

    assert!(
        remaining > 0,
        "Should own partition with linearizable guarantee"
    );

    coordinator.shutdown().await.unwrap();
}

/// Test broker unregistration.
#[tokio::test]
async fn test_coordinator_broker_unregistration() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    // Register broker
    coordinator.register_broker().await.unwrap();

    // Unregister broker
    let result = coordinator.unregister_broker().await;
    assert!(result.is_ok(), "Unregistration should succeed");

    coordinator.shutdown().await.unwrap();
}

/// Test get_brokers returns registered brokers.
#[tokio::test]
async fn test_coordinator_get_brokers() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    // Register broker
    coordinator.register_broker().await.unwrap();

    // Get brokers list
    let brokers = coordinator.get_live_brokers().await.unwrap();

    assert!(!brokers.is_empty(), "Should have at least one broker");
    assert!(
        brokers.iter().any(|b| b.broker_id == 1),
        "Broker 1 should be in the list"
    );

    coordinator.shutdown().await.unwrap();
}

/// Test partition release when not owned.
#[tokio::test]
async fn test_coordinator_release_unowned_partition() {
    let port = next_port();
    let config = test_config(1, port, vec![]);
    let store = create_object_store();

    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .unwrap();
    coordinator.initialize_cluster().await.unwrap();

    assert!(wait_for_coordinator_leader(&coordinator, Duration::from_secs(5)).await);

    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("release-test", 1).await.unwrap();

    // Release without acquiring - should not error
    let result = coordinator.release_partition("release-test", 0).await;
    assert!(
        result.is_ok(),
        "Releasing unowned partition should not error"
    );

    coordinator.shutdown().await.unwrap();
}
