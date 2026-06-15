use super::*;

// ========================================================================
// Broker Registration Tests
// ========================================================================

#[tokio::test]
async fn test_broker_registration() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    assert!(coordinator.register_broker().await.is_ok());

    let brokers = coordinator.get_live_brokers().await.unwrap();
    assert_eq!(brokers.len(), 1);
    assert_eq!(brokers[0].broker_id, 1);
    assert_eq!(brokers[0].host, "localhost");
    assert_eq!(brokers[0].port, 9092);
}

#[tokio::test]
async fn test_heartbeat_requires_registration() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    // Heartbeat should fail without registration
    assert!(coordinator.heartbeat().await.is_err());

    // After registration, heartbeat should succeed
    coordinator.register_broker().await.unwrap();
    assert!(coordinator.heartbeat().await.is_ok());
}

#[tokio::test]
async fn test_unregister_broker() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    coordinator.register_broker().await.unwrap();
    assert_eq!(coordinator.get_live_brokers().await.unwrap().len(), 1);

    coordinator.unregister_broker().await.unwrap();
    assert_eq!(coordinator.get_live_brokers().await.unwrap().len(), 0);
}

// ========================================================================
// Partition Ownership Tests
// ========================================================================

#[tokio::test]
async fn test_acquire_partition() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 3).await.unwrap();

    // First acquisition should succeed
    let acquired = coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(acquired);

    // Second acquisition by same broker should fail (already owned)
    let acquired_again = coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(!acquired_again);
}

#[tokio::test]
async fn test_partition_ownership_check() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 3).await.unwrap();

    // Not owned initially
    assert!(
        !coordinator
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );

    // Acquire and check
    coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(
        coordinator
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
    assert!(
        coordinator
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_renew_partition_lease() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 1).await.unwrap();

    // Acquire first
    coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();

    // Renew should succeed
    let renewed = coordinator
        .renew_partition_lease("test-topic", 0, 120)
        .await
        .unwrap();
    assert!(renewed);

    // Renew non-owned partition should fail
    let renewed_other = coordinator
        .renew_partition_lease("test-topic", 1, 60)
        .await
        .unwrap();
    assert!(!renewed_other);
}

#[tokio::test]
async fn test_release_partition() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 1).await.unwrap();

    coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(
        coordinator
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );

    coordinator
        .release_partition("test-topic", 0)
        .await
        .unwrap();
    assert!(
        !coordinator
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_verify_and_extend_lease() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 1).await.unwrap();

    coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();

    // Verify and extend should succeed
    let remaining = coordinator
        .verify_and_extend_lease("test-topic", 0, 120)
        .await
        .unwrap();
    assert_eq!(remaining, 120);

    // Non-owned partition should return error
    let result = coordinator
        .verify_and_extend_lease("test-topic", 1, 60)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_partition_owner() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 1).await.unwrap();

    // No owner initially
    assert_eq!(
        coordinator
            .get_partition_owner("test-topic", 0)
            .await
            .unwrap(),
        None
    );

    // Acquire and check owner
    coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert_eq!(
        coordinator
            .get_partition_owner("test-topic", 0)
            .await
            .unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn test_expire_all_leases() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 2).await.unwrap();

    coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    coordinator
        .acquire_partition("test-topic", 1, 60)
        .await
        .unwrap();

    assert!(
        coordinator
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
    assert!(
        coordinator
            .owns_partition_for_read("test-topic", 1)
            .await
            .unwrap()
    );

    // Expire all leases
    coordinator.expire_all_leases().await;

    assert!(
        !coordinator
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
    assert!(
        !coordinator
            .owns_partition_for_read("test-topic", 1)
            .await
            .unwrap()
    );
}

// ========================================================================
// Topic Management Tests
// ========================================================================

#[tokio::test]
async fn test_register_topic() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    coordinator.register_topic("my-topic", 5).await.unwrap();

    assert!(coordinator.topic_exists("my-topic").await.unwrap());
    assert_eq!(
        coordinator.get_partition_count("my-topic").await.unwrap(),
        Some(5)
    );
}

#[tokio::test]
async fn test_get_topics() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    coordinator.register_topic("topic-a", 1).await.unwrap();
    coordinator.register_topic("topic-b", 2).await.unwrap();

    let topics = coordinator.get_topics().await.unwrap();
    assert_eq!(topics.len(), 2);
    assert!(topics.contains(&"topic-a".to_string()));
    assert!(topics.contains(&"topic-b".to_string()));
}

#[tokio::test]
async fn test_delete_topic() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    coordinator.register_topic("delete-me", 1).await.unwrap();
    assert!(coordinator.topic_exists("delete-me").await.unwrap());

    let deleted = coordinator.delete_topic("delete-me").await.unwrap();
    assert!(deleted);
    assert!(!coordinator.topic_exists("delete-me").await.unwrap());

    // Deleting non-existent topic returns false
    let deleted_again = coordinator.delete_topic("delete-me").await.unwrap();
    assert!(!deleted_again);
}

#[tokio::test]
async fn test_topic_count() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    assert_eq!(coordinator.topic_count().await, 0);

    coordinator.register_topic("t1", 1).await.unwrap();
    assert_eq!(coordinator.topic_count().await, 1);

    coordinator.register_topic("t2", 1).await.unwrap();
    assert_eq!(coordinator.topic_count().await, 2);
}

// ========================================================================
// Consumer Group Tests
// ========================================================================

#[tokio::test]
async fn test_join_group_first_member() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let (gen_id, member_id, is_leader, leader_id, members) = coordinator
        .join_group("my-group", "", &[], 30000)
        .await
        .unwrap();

    assert_eq!(gen_id, 1);
    assert!(!member_id.is_empty());
    assert!(is_leader);
    assert_eq!(leader_id, member_id);
    assert_eq!(members.len(), 1);
}

#[tokio::test]
async fn test_join_group_second_member() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let (_, first_member, _, _, _) = coordinator
        .join_group("my-group", "", &[], 30000)
        .await
        .unwrap();

    let (gen_id, second_member, is_leader, leader_id, members) = coordinator
        .join_group("my-group", "", &[], 30000)
        .await
        .unwrap();

    // Generation increments on each join
    assert_eq!(gen_id, 2);
    assert!(!is_leader);
    assert_eq!(leader_id, first_member);
    assert_eq!(members.len(), 2);
    assert!(members.contains(&second_member));
}

#[tokio::test]
async fn test_leave_group() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let (_, member_id, _, _, _) = coordinator
        .join_group("my-group", "", &[], 30000)
        .await
        .unwrap();

    assert!(
        coordinator
            .get_group_members("my-group")
            .await
            .unwrap()
            .contains(&member_id)
    );

    coordinator
        .remove_group_member("my-group", &member_id)
        .await
        .unwrap();

    assert!(
        !coordinator
            .get_group_members("my-group")
            .await
            .unwrap()
            .contains(&member_id)
    );
}

#[tokio::test]
async fn test_leader_election_on_leave() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    // First member is leader
    let (_, first_member, _, _, _) = coordinator
        .join_group("my-group", "member-1", &[], 30000)
        .await
        .unwrap();

    // Second member joins
    let (_, second_member, _, _, _) = coordinator
        .join_group("my-group", "member-2", &[], 30000)
        .await
        .unwrap();

    assert_eq!(
        coordinator.get_group_leader("my-group").await.unwrap(),
        Some(first_member.clone())
    );

    // Leader leaves
    coordinator
        .remove_group_member("my-group", &first_member)
        .await
        .unwrap();

    // Second member should become leader
    assert_eq!(
        coordinator.get_group_leader("my-group").await.unwrap(),
        Some(second_member)
    );
}

#[tokio::test]
async fn test_heartbeat_with_generation() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let (gen_id, member_id, _, _, _) = coordinator
        .join_group("my-group", "", &[], 30000)
        .await
        .unwrap();

    // Correct generation should succeed
    let result = coordinator
        .update_member_heartbeat_with_generation("my-group", &member_id, gen_id)
        .await
        .unwrap();
    assert_eq!(result, HeartbeatResult::Success);

    // Wrong generation should fail
    let result = coordinator
        .update_member_heartbeat_with_generation("my-group", &member_id, gen_id + 1)
        .await
        .unwrap();
    assert_eq!(result, HeartbeatResult::IllegalGeneration);

    // Unknown member should fail
    let result = coordinator
        .update_member_heartbeat_with_generation("my-group", "unknown-member", gen_id)
        .await
        .unwrap();
    assert_eq!(result, HeartbeatResult::UnknownMember);
}

#[tokio::test]
async fn test_set_assignments_atomically() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let (gen_id, leader_id, _, _, _) = coordinator
        .join_group("my-group", "", &[], 30000)
        .await
        .unwrap();

    let assignment = vec![0u8, 1, 2, 3];
    let assignments = vec![(leader_id.clone(), assignment.clone())];

    // Set assignments as leader
    let success = coordinator
        .set_assignments_atomically("my-group", &leader_id, gen_id, &assignments)
        .await
        .unwrap();
    assert!(success);

    // Verify assignment was stored
    let stored = coordinator
        .get_member_assignment("my-group", &leader_id)
        .await
        .unwrap();
    assert_eq!(stored, assignment);
}

#[tokio::test]
async fn test_set_assignments_wrong_leader() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let (gen_id, _, _, _, _) = coordinator
        .join_group("my-group", "", &[], 30000)
        .await
        .unwrap();

    // Try to set assignments as non-leader
    let success = coordinator
        .set_assignments_atomically("my-group", "wrong-leader", gen_id, &[])
        .await
        .unwrap();
    assert!(!success);
}

#[tokio::test]
async fn test_complete_rebalance() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let (gen_id, _, _, _, _) = coordinator
        .join_group("my-group", "", &[], 30000)
        .await
        .unwrap();

    // Complete with correct generation
    let success = coordinator
        .complete_rebalance("my-group", gen_id)
        .await
        .unwrap();
    assert!(success);

    // Complete with wrong generation should fail
    let success = coordinator
        .complete_rebalance("my-group", gen_id + 1)
        .await
        .unwrap();
    assert!(!success);
}

// ========================================================================
// Offset Management Tests
// ========================================================================

#[tokio::test]
async fn test_commit_and_fetch_offset() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    // Fetch non-existent offset
    let (offset, metadata) = coordinator
        .fetch_offset("my-group", "my-topic", 0)
        .await
        .unwrap();
    assert_eq!(offset, -1);
    assert!(metadata.is_none());

    // Commit offset
    coordinator
        .commit_offset("my-group", "my-topic", 0, 100, Some("test-metadata"))
        .await
        .unwrap();

    // Fetch committed offset
    let (offset, metadata) = coordinator
        .fetch_offset("my-group", "my-topic", 0)
        .await
        .unwrap();
    assert_eq!(offset, 100);
    assert_eq!(metadata, Some("test-metadata".to_string()));
}

#[tokio::test]
async fn test_offset_update() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    coordinator
        .commit_offset("my-group", "my-topic", 0, 50, None)
        .await
        .unwrap();

    let (offset, _) = coordinator
        .fetch_offset("my-group", "my-topic", 0)
        .await
        .unwrap();
    assert_eq!(offset, 50);

    // Update to higher offset
    coordinator
        .commit_offset("my-group", "my-topic", 0, 100, None)
        .await
        .unwrap();

    let (offset, _) = coordinator
        .fetch_offset("my-group", "my-topic", 0)
        .await
        .unwrap();
    assert_eq!(offset, 100);
}

#[tokio::test]
async fn test_offset_per_partition() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    coordinator
        .commit_offset("my-group", "my-topic", 0, 10, None)
        .await
        .unwrap();
    coordinator
        .commit_offset("my-group", "my-topic", 1, 20, None)
        .await
        .unwrap();
    coordinator
        .commit_offset("my-group", "my-topic", 2, 30, None)
        .await
        .unwrap();

    let (o0, _) = coordinator
        .fetch_offset("my-group", "my-topic", 0)
        .await
        .unwrap();
    let (o1, _) = coordinator
        .fetch_offset("my-group", "my-topic", 1)
        .await
        .unwrap();
    let (o2, _) = coordinator
        .fetch_offset("my-group", "my-topic", 2)
        .await
        .unwrap();

    assert_eq!(o0, 10);
    assert_eq!(o1, 20);
    assert_eq!(o2, 30);
}

// ========================================================================
// Producer ID Tests
// ========================================================================

#[tokio::test]
async fn test_next_producer_id() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let id1 = coordinator.next_producer_id().await.unwrap();
    let id2 = coordinator.next_producer_id().await.unwrap();
    let id3 = coordinator.next_producer_id().await.unwrap();

    assert_eq!(id2, id1 + 1);
    assert_eq!(id3, id2 + 1);
}

#[tokio::test]
async fn test_init_producer_id() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);

    let (id1, epoch1) = coordinator.init_producer_id(None, -1, -1).await.unwrap();
    let (id2, epoch2) = coordinator.init_producer_id(None, -1, -1).await.unwrap();

    assert!(id1 > 0);
    assert!(id2 > id1);
    assert_eq!(epoch1, 0);
    assert_eq!(epoch2, 0);
}

// ========================================================================
// Partition Assignment Tests
// ========================================================================

#[tokio::test]
async fn test_get_assigned_partitions() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 3).await.unwrap();

    // No assignments initially
    let assigned = coordinator.get_assigned_partitions().await.unwrap();
    assert!(assigned.is_empty());

    // Acquire some partitions
    coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    coordinator
        .acquire_partition("test-topic", 2, 60)
        .await
        .unwrap();

    let assigned = coordinator.get_assigned_partitions().await.unwrap();
    assert_eq!(assigned.len(), 2);
    assert!(assigned.contains(&("test-topic".to_string(), 0)));
    assert!(assigned.contains(&("test-topic".to_string(), 2)));
}

#[tokio::test]
async fn test_get_partition_owners() {
    let coordinator = MockCoordinator::new(1, "localhost", 9092);
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("test-topic", 3).await.unwrap();

    // Set some owners
    coordinator
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    // Manually set different owner for partition 1
    coordinator
        .set_partition_owner("test-topic", 1, 2, 60)
        .await;
    // Partition 2 has no owner

    let owners = coordinator
        .get_partition_owners("test-topic")
        .await
        .unwrap();
    assert_eq!(owners.len(), 3);

    assert_eq!(owners[0], (0, Some(1))); // Owned by broker 1
    assert_eq!(owners[1], (1, Some(2))); // Owned by broker 2
    assert_eq!(owners[2], (2, None)); // No owner
}

// ========================================================================
// Partition Failover Tests
// ========================================================================
//
// These tests simulate partition failover scenarios including broker failure,
// lease expiration, and ownership transfer between brokers.

#[tokio::test]
async fn test_partition_failover_on_lease_expiry() {
    // Simulate partition failover when a broker's lease expires
    let broker1 = MockCoordinator::new(1, "localhost", 9092);
    let broker2 = MockCoordinator::new(2, "localhost", 9093);

    // Setup: Share the same state between brokers
    let shared_topics = broker1.topics.clone();
    let shared_owners = broker1.partition_owners.clone();
    let shared_epochs = broker1.partition_epochs.clone();
    let shared_brokers = broker1.brokers.clone();

    // Recreate broker2 with shared state
    let broker2 = MockCoordinator {
        broker_id: 2,
        host: "localhost".to_string(),
        port: 9093,
        brokers: shared_brokers,
        topics: shared_topics,
        partition_owners: shared_owners,
        partition_epochs: shared_epochs,
        consumer_groups: broker2.consumer_groups.clone(),
        offsets: broker2.offsets.clone(),
        next_producer_id: broker2.next_producer_id.clone(),
        next_member_id: broker2.next_member_id.clone(),
        producer_states: broker2.producer_states.clone(),
        mock_raft_index: Arc::new(AtomicU64::new(0)),
    };

    // Broker1 registers and acquires partition
    broker1.register_broker().await.unwrap();
    broker1.register_topic("test-topic", 1).await.unwrap();
    let acquired = broker1
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(acquired, "Broker1 should acquire partition");
    assert!(
        broker1
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );

    // Broker2 tries to acquire - should fail (lease still valid)
    broker2.register_broker().await.unwrap();
    let acquired = broker2
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(
        !acquired,
        "Broker2 should not acquire partition while Broker1's lease is valid"
    );

    // Simulate Broker1 failure by expiring all leases
    broker1.expire_all_leases().await;

    // Now Broker2 should be able to acquire the partition
    let acquired = broker2
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(
        acquired,
        "Broker2 should acquire partition after Broker1's lease expired"
    );
    assert!(
        broker2
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
    assert!(
        !broker1
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap(),
        "Broker1 should no longer own partition"
    );
}

#[tokio::test]
async fn test_partition_ownership_fencing() {
    // Test that verify_and_extend_lease fails when ownership is lost
    let broker1 = MockCoordinator::new(1, "localhost", 9092);
    let broker2 = MockCoordinator::new(2, "localhost", 9093);

    // Share state
    let shared_topics = broker1.topics.clone();
    let shared_owners = broker1.partition_owners.clone();
    let shared_epochs = broker1.partition_epochs.clone();
    let shared_brokers = broker1.brokers.clone();

    let broker2 = MockCoordinator {
        broker_id: 2,
        host: "localhost".to_string(),
        port: 9093,
        brokers: shared_brokers,
        topics: shared_topics,
        partition_owners: shared_owners,
        partition_epochs: shared_epochs,
        consumer_groups: broker2.consumer_groups.clone(),
        offsets: broker2.offsets.clone(),
        next_producer_id: broker2.next_producer_id.clone(),
        next_member_id: broker2.next_member_id.clone(),
        producer_states: broker2.producer_states.clone(),
        mock_raft_index: Arc::new(AtomicU64::new(0)),
    };

    // Broker1 acquires partition
    broker1.register_broker().await.unwrap();
    broker2.register_broker().await.unwrap();
    broker1.register_topic("test-topic", 1).await.unwrap();
    broker1
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();

    // Broker1 can extend lease
    let result = broker1.verify_and_extend_lease("test-topic", 0, 60).await;
    assert!(result.is_ok(), "Broker1 should be able to extend lease");

    // Expire and transfer ownership to Broker2
    broker1.expire_all_leases().await;
    broker2
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();

    // Broker1's verify_and_extend should now fail (fenced)
    let result = broker1.verify_and_extend_lease("test-topic", 0, 60).await;
    assert!(result.is_err(), "Broker1 should be fenced out");
    match result {
        Err(SlateDBError::Fenced) => (), // Expected
        Err(e) => panic!("Expected Fenced error, got: {:?}", e),
        Ok(_) => panic!("Should not succeed after being fenced"),
    }
}

#[tokio::test]
async fn test_concurrent_partition_acquisition_race() {
    // Test race condition where two brokers try to acquire the same partition
    let broker1 = MockCoordinator::new(1, "localhost", 9092);
    let broker2 = MockCoordinator::new(2, "localhost", 9093);

    // Share state
    let shared_topics = broker1.topics.clone();
    let shared_owners = broker1.partition_owners.clone();
    let shared_epochs = broker1.partition_epochs.clone();
    let shared_brokers = broker1.brokers.clone();

    let broker2 = MockCoordinator {
        broker_id: 2,
        host: "localhost".to_string(),
        port: 9093,
        brokers: shared_brokers,
        topics: shared_topics,
        partition_owners: shared_owners,
        partition_epochs: shared_epochs,
        consumer_groups: broker2.consumer_groups.clone(),
        offsets: broker2.offsets.clone(),
        next_producer_id: broker2.next_producer_id.clone(),
        next_member_id: broker2.next_member_id.clone(),
        producer_states: broker2.producer_states.clone(),
        mock_raft_index: Arc::new(AtomicU64::new(0)),
    };

    broker1.register_broker().await.unwrap();
    broker2.register_broker().await.unwrap();
    broker1.register_topic("test-topic", 1).await.unwrap();

    // Both try to acquire simultaneously - only one should succeed
    let result1 = broker1
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    let result2 = broker2
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();

    // Exactly one should succeed
    assert!(
        result1 ^ result2,
        "Exactly one broker should acquire the partition: broker1={}, broker2={}",
        result1,
        result2
    );

    // Verify consistent ownership
    let owner = broker1.get_partition_owner("test-topic", 0).await.unwrap();
    if result1 {
        assert_eq!(owner, Some(1));
    } else {
        assert_eq!(owner, Some(2));
    }
}

#[tokio::test]
async fn test_partition_release_and_reacquisition() {
    // Test releasing a partition and having another broker acquire it
    let broker1 = MockCoordinator::new(1, "localhost", 9092);
    let broker2 = MockCoordinator::new(2, "localhost", 9093);

    // Share state
    let shared_topics = broker1.topics.clone();
    let shared_owners = broker1.partition_owners.clone();
    let shared_epochs = broker1.partition_epochs.clone();
    let shared_brokers = broker1.brokers.clone();

    let broker2 = MockCoordinator {
        broker_id: 2,
        host: "localhost".to_string(),
        port: 9093,
        brokers: shared_brokers,
        topics: shared_topics,
        partition_owners: shared_owners,
        partition_epochs: shared_epochs,
        consumer_groups: broker2.consumer_groups.clone(),
        offsets: broker2.offsets.clone(),
        next_producer_id: broker2.next_producer_id.clone(),
        next_member_id: broker2.next_member_id.clone(),
        producer_states: broker2.producer_states.clone(),
        mock_raft_index: Arc::new(AtomicU64::new(0)),
    };

    broker1.register_broker().await.unwrap();
    broker2.register_broker().await.unwrap();
    broker1.register_topic("test-topic", 1).await.unwrap();

    // Broker1 acquires partition
    broker1
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(
        broker1
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );

    // Broker1 gracefully releases partition
    broker1.release_partition("test-topic", 0).await.unwrap();
    assert!(
        !broker1
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );

    // Broker2 can now acquire
    let acquired = broker2
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(acquired, "Broker2 should acquire released partition");
    assert!(
        broker2
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_zombie_mode_simulation() {
    // Simulate zombie mode: broker loses all leases but doesn't know it yet
    let broker1 = MockCoordinator::new(1, "localhost", 9092);
    broker1.register_broker().await.unwrap();
    broker1.register_topic("test-topic", 3).await.unwrap();

    // Acquire multiple partitions
    broker1
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    broker1
        .acquire_partition("test-topic", 1, 60)
        .await
        .unwrap();
    broker1
        .acquire_partition("test-topic", 2, 60)
        .await
        .unwrap();

    // All owned
    assert!(
        broker1
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
    assert!(
        broker1
            .owns_partition_for_read("test-topic", 1)
            .await
            .unwrap()
    );
    assert!(
        broker1
            .owns_partition_for_read("test-topic", 2)
            .await
            .unwrap()
    );

    // Simulate coordinator failure - all leases expire
    broker1.expire_all_leases().await;

    // Now none are owned (zombie state)
    assert!(
        !broker1
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );
    assert!(
        !broker1
            .owns_partition_for_read("test-topic", 1)
            .await
            .unwrap()
    );
    assert!(
        !broker1
            .owns_partition_for_read("test-topic", 2)
            .await
            .unwrap()
    );

    // verify_and_extend should fail for all
    for partition in 0..3 {
        let result = broker1
            .verify_and_extend_lease("test-topic", partition, 60)
            .await;
        assert!(
            result.is_err(),
            "Should be fenced for partition {}",
            partition
        );
    }
}

#[tokio::test]
async fn test_multi_partition_failover() {
    // Test failover of multiple partitions across brokers
    let broker1 = MockCoordinator::new(1, "localhost", 9092);
    let broker2 = MockCoordinator::new(2, "localhost", 9093);
    let broker3 = MockCoordinator::new(3, "localhost", 9094);

    // Share state among all brokers
    let shared_topics = broker1.topics.clone();
    let shared_owners = broker1.partition_owners.clone();
    let shared_epochs = broker1.partition_epochs.clone();
    let shared_brokers = broker1.brokers.clone();

    let broker2 = MockCoordinator {
        broker_id: 2,
        host: "localhost".to_string(),
        port: 9093,
        brokers: shared_brokers.clone(),
        topics: shared_topics.clone(),
        partition_owners: shared_owners.clone(),
        partition_epochs: shared_epochs.clone(),
        consumer_groups: broker2.consumer_groups.clone(),
        offsets: broker2.offsets.clone(),
        next_producer_id: broker2.next_producer_id.clone(),
        next_member_id: broker2.next_member_id.clone(),
        producer_states: broker2.producer_states.clone(),
        mock_raft_index: Arc::new(AtomicU64::new(0)),
    };

    let broker3 = MockCoordinator {
        broker_id: 3,
        host: "localhost".to_string(),
        port: 9094,
        brokers: shared_brokers,
        topics: shared_topics,
        partition_owners: shared_owners,
        partition_epochs: shared_epochs,
        consumer_groups: broker3.consumer_groups.clone(),
        offsets: broker3.offsets.clone(),
        next_producer_id: broker3.next_producer_id.clone(),
        next_member_id: broker3.next_member_id.clone(),
        producer_states: broker3.producer_states.clone(),
        mock_raft_index: Arc::new(AtomicU64::new(0)),
    };

    // Register all brokers and create topic
    broker1.register_broker().await.unwrap();
    broker2.register_broker().await.unwrap();
    broker3.register_broker().await.unwrap();
    broker1.register_topic("test-topic", 6).await.unwrap();

    // Distribute partitions: Broker1 gets 0,1,2 - Broker2 gets 3,4,5
    broker1
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    broker1
        .acquire_partition("test-topic", 1, 60)
        .await
        .unwrap();
    broker1
        .acquire_partition("test-topic", 2, 60)
        .await
        .unwrap();
    broker2
        .acquire_partition("test-topic", 3, 60)
        .await
        .unwrap();
    broker2
        .acquire_partition("test-topic", 4, 60)
        .await
        .unwrap();
    broker2
        .acquire_partition("test-topic", 5, 60)
        .await
        .unwrap();

    // Verify distribution
    for p in 0..3 {
        assert_eq!(
            broker1.get_partition_owner("test-topic", p).await.unwrap(),
            Some(1)
        );
    }
    for p in 3..6 {
        assert_eq!(
            broker1.get_partition_owner("test-topic", p).await.unwrap(),
            Some(2)
        );
    }

    // Broker1 fails - expire only broker1's leases (not broker2's)
    broker1.expire_my_leases().await;

    // Broker3 takes over partitions 0,1,2
    broker3
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    broker3
        .acquire_partition("test-topic", 1, 60)
        .await
        .unwrap();
    broker3
        .acquire_partition("test-topic", 2, 60)
        .await
        .unwrap();

    // Verify new distribution
    for p in 0..3 {
        assert_eq!(
            broker1.get_partition_owner("test-topic", p).await.unwrap(),
            Some(3)
        );
    }
    for p in 3..6 {
        assert_eq!(
            broker1.get_partition_owner("test-topic", p).await.unwrap(),
            Some(2)
        );
    }
}

#[tokio::test]
async fn test_ownership_check_during_write() {
    // Simulate the pattern used in PartitionStore::append_batch
    let broker1 = MockCoordinator::new(1, "localhost", 9092);
    broker1.register_broker().await.unwrap();
    broker1.register_topic("test-topic", 1).await.unwrap();
    broker1
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();

    // Simulate write path: verify_and_extend_lease before write
    let result = broker1.verify_and_extend_lease("test-topic", 0, 60).await;
    assert!(
        result.is_ok(),
        "Should be able to extend lease before write"
    );

    // Simulate lease expiry during long write (race condition)
    broker1.expire_all_leases().await;

    // Post-write verification would fail (this is caught by SlateDB fencing in production)
    let result = broker1.verify_and_extend_lease("test-topic", 0, 60).await;
    assert!(
        result.is_err(),
        "Should fail after lease expired during write"
    );
}

#[tokio::test]
async fn test_lease_renewal_keeps_ownership() {
    // Test that regular lease renewal maintains ownership
    let broker1 = MockCoordinator::new(1, "localhost", 9092);
    let broker2 = MockCoordinator::new(2, "localhost", 9093);

    // Share state
    let shared_topics = broker1.topics.clone();
    let shared_owners = broker1.partition_owners.clone();
    let shared_epochs = broker1.partition_epochs.clone();
    let shared_brokers = broker1.brokers.clone();

    let broker2 = MockCoordinator {
        broker_id: 2,
        host: "localhost".to_string(),
        port: 9093,
        brokers: shared_brokers,
        topics: shared_topics,
        partition_owners: shared_owners,
        partition_epochs: shared_epochs,
        consumer_groups: broker2.consumer_groups.clone(),
        offsets: broker2.offsets.clone(),
        next_producer_id: broker2.next_producer_id.clone(),
        next_member_id: broker2.next_member_id.clone(),
        producer_states: broker2.producer_states.clone(),
        mock_raft_index: Arc::new(AtomicU64::new(0)),
    };

    broker1.register_broker().await.unwrap();
    broker2.register_broker().await.unwrap();
    broker1.register_topic("test-topic", 1).await.unwrap();

    // Broker1 acquires with short lease
    broker1
        .acquire_partition("test-topic", 0, 10)
        .await
        .unwrap();

    // Renew lease multiple times
    for _ in 0..5 {
        let renewed = broker1
            .renew_partition_lease("test-topic", 0, 10)
            .await
            .unwrap();
        assert!(renewed, "Renewal should succeed");
    }

    // Broker1 still owns partition
    assert!(
        broker1
            .owns_partition_for_read("test-topic", 0)
            .await
            .unwrap()
    );

    // Broker2 cannot acquire (lease is still valid)
    let acquired = broker2
        .acquire_partition("test-topic", 0, 60)
        .await
        .unwrap();
    assert!(
        !acquired,
        "Broker2 should not acquire while Broker1 keeps renewing"
    );
}
