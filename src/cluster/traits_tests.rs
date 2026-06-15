//! Inline tests previously embedded in `traits.rs`. Moved to a sibling
//! file so the implementation isn't gated on scrolling past the test
//! block. `super::*` re-exports private helpers the tests rely on.

#![allow(clippy::module_inception)]
#![allow(unused_imports)]

use super::*;

use super::*;
use crate::cluster::mock_coordinator::MockCoordinator;

// ========================================================================
// Trait Compilation Tests
// ========================================================================

#[test]
fn test_mock_coordinator_implements_partition_coordinator() {
    // This test verifies that MockCoordinator implements PartitionCoordinator
    fn assert_partition_coordinator<T: PartitionCoordinator>() {}
    assert_partition_coordinator::<MockCoordinator>();
}

#[test]
fn test_mock_coordinator_implements_consumer_group_coordinator() {
    // This test verifies that MockCoordinator implements ConsumerGroupCoordinator
    fn assert_consumer_group_coordinator<T: ConsumerGroupCoordinator>() {}
    assert_consumer_group_coordinator::<MockCoordinator>();
}

#[test]
fn test_mock_coordinator_implements_producer_coordinator() {
    // This test verifies that MockCoordinator implements ProducerCoordinator
    fn assert_producer_coordinator<T: ProducerCoordinator>() {}
    assert_producer_coordinator::<MockCoordinator>();
}

#[test]
fn test_mock_coordinator_implements_partition_transfer_coordinator() {
    // This test verifies that MockCoordinator implements PartitionTransferCoordinator
    fn assert_partition_transfer_coordinator<T: PartitionTransferCoordinator>() {}
    assert_partition_transfer_coordinator::<MockCoordinator>();
}

#[test]
fn test_mock_coordinator_implements_cluster_coordinator() {
    // This test verifies the blanket implementation of ClusterCoordinator
    fn assert_cluster_coordinator<T: ClusterCoordinator>() {}
    assert_cluster_coordinator::<MockCoordinator>();
}

// ========================================================================
// MockCoordinator Basic Tests
// ========================================================================

#[test]
fn test_mock_coordinator_new() {
    let mock = MockCoordinator::new(1, "localhost", 9092);
    assert_eq!(mock.broker_id, 1);
    assert_eq!(mock.host, "localhost");
    assert_eq!(mock.port, 9092);
}

#[test]
fn test_mock_coordinator_broker_id() {
    let mock = MockCoordinator::new(42, "host", 9092);
    // Using trait method
    assert_eq!(PartitionCoordinator::broker_id(&mock), 42);
}

// ========================================================================
// Default Implementation Tests
// ========================================================================

#[tokio::test]
async fn test_acquire_partition_with_epoch_default() {
    // Test the default implementation of acquire_partition_with_epoch
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register topic first
    mock.register_topic("test-topic", 3).await.unwrap();

    // Acquire should return Some(epoch) on success
    let result = mock.acquire_partition_with_epoch("test-topic", 0, 60).await;
    assert!(result.is_ok());
    // Either we get an epoch or None depending on implementation
    let epoch_opt = result.unwrap();
    // MockCoordinator should return Some(epoch) on successful acquire
    assert!(epoch_opt.is_some());
}

#[tokio::test]
async fn test_acquire_partition_with_epoch_already_owned() {
    // Test when partition is owned by another broker
    let mock1 = MockCoordinator::new(1, "localhost", 9092);
    let mut mock2 = mock1.clone();
    mock2.broker_id = 2;

    // Register topic
    mock1.register_topic("test-topic", 3).await.unwrap();

    // First broker acquires
    mock1.acquire_partition("test-topic", 0, 60).await.unwrap();

    // Second broker tries to acquire - should fail (return None)
    let result = mock2
        .acquire_partition_with_epoch("test-topic", 0, 60)
        .await;
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[tokio::test]
async fn test_owns_partition_for_write_success() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register topic and acquire partition
    mock.register_topic("test-topic", 3).await.unwrap();
    mock.acquire_partition("test-topic", 0, 60).await.unwrap();

    // owns_partition_for_write should succeed and return remaining TTL
    let result = mock.owns_partition_for_write("test-topic", 0, 60).await;
    assert!(result.is_ok());
    // TTL should be positive
    let ttl = result.unwrap();
    assert!(ttl > 0);
}

#[tokio::test]
async fn test_owns_partition_for_write_not_owner() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register topic but don't acquire
    mock.register_topic("test-topic", 3).await.unwrap();

    // owns_partition_for_write should fail since we don't own it
    let result = mock.owns_partition_for_write("test-topic", 0, 60).await;
    assert!(result.is_err());
}

// ========================================================================
// Trait Object Tests
// ========================================================================

#[test]
fn test_partition_coordinator_trait_object() {
    // Verify PartitionCoordinator can be used as a trait object
    let mock = MockCoordinator::new(1, "localhost", 9092);
    let _trait_obj: &dyn PartitionCoordinator = &mock;
}

#[test]
fn test_consumer_group_coordinator_trait_object() {
    // Verify ConsumerGroupCoordinator can be used as a trait object
    let mock = MockCoordinator::new(1, "localhost", 9092);
    let _trait_obj: &dyn ConsumerGroupCoordinator = &mock;
}

#[test]
fn test_producer_coordinator_trait_object() {
    // Verify ProducerCoordinator can be used as a trait object
    let mock = MockCoordinator::new(1, "localhost", 9092);
    let _trait_obj: &dyn ProducerCoordinator = &mock;
}

#[test]
fn test_partition_transfer_coordinator_trait_object() {
    // Verify PartitionTransferCoordinator can be used as a trait object
    let mock = MockCoordinator::new(1, "localhost", 9092);
    let _trait_obj: &dyn PartitionTransferCoordinator = &mock;
}

// ========================================================================
// PartitionCoordinator Method Tests
// ========================================================================

#[tokio::test]
async fn test_register_and_get_topics() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register multiple topics
    mock.register_topic("topic-a", 3).await.unwrap();
    mock.register_topic("topic-b", 5).await.unwrap();
    mock.register_topic("topic-c", 1).await.unwrap();

    // Get all topics
    let topics = mock.get_topics().await.unwrap();
    assert!(topics.contains(&"topic-a".to_string()));
    assert!(topics.contains(&"topic-b".to_string()));
    assert!(topics.contains(&"topic-c".to_string()));
}

#[tokio::test]
async fn test_topic_exists() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register a topic
    mock.register_topic("existing-topic", 3).await.unwrap();

    // Check existence
    assert!(mock.topic_exists("existing-topic").await.unwrap());
    assert!(!mock.topic_exists("nonexistent-topic").await.unwrap());
}

#[tokio::test]
async fn test_get_partition_count() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register topic with 10 partitions
    mock.register_topic("my-topic", 10).await.unwrap();

    // Get partition count
    let count = mock.get_partition_count("my-topic").await.unwrap();
    assert_eq!(count, Some(10));

    // Nonexistent topic should return None
    let count = mock.get_partition_count("no-topic").await.unwrap();
    assert!(count.is_none());
}

#[tokio::test]
async fn test_delete_topic() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register and delete
    mock.register_topic("delete-me", 1).await.unwrap();
    assert!(mock.topic_exists("delete-me").await.unwrap());

    let deleted = mock.delete_topic("delete-me").await.unwrap();
    assert!(deleted);
    assert!(!mock.topic_exists("delete-me").await.unwrap());

    // Deleting again should return false
    let deleted_again = mock.delete_topic("delete-me").await.unwrap();
    assert!(!deleted_again);
}

#[tokio::test]
async fn test_partition_ownership_lifecycle() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    mock.register_topic("test", 1).await.unwrap();

    // Acquire
    let acquired = mock.acquire_partition("test", 0, 60).await.unwrap();
    assert!(acquired);

    // Get owner
    let owner = mock.get_partition_owner("test", 0).await.unwrap();
    assert_eq!(owner, Some(1));

    // Renew
    let renewed = mock.renew_partition_lease("test", 0, 120).await.unwrap();
    assert!(renewed);

    // Release
    mock.release_partition("test", 0).await.unwrap();

    // Owner should be None after release
    let owner = mock.get_partition_owner("test", 0).await.unwrap();
    assert!(owner.is_none());
}

// ========================================================================
// ProducerCoordinator Method Tests
// ========================================================================

#[tokio::test]
async fn test_next_producer_id() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Get first producer ID
    let id1 = mock.next_producer_id().await.unwrap();
    // Get second producer ID
    let id2 = mock.next_producer_id().await.unwrap();

    // IDs should be different and increasing
    assert!(id2 > id1);
}

#[tokio::test]
async fn test_init_producer_id_new() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Initialize new producer (no transactional_id)
    let (pid, epoch) = mock.init_producer_id(None, -1, -1).await.unwrap();

    // Should get a valid producer ID and epoch 0
    assert!(pid >= 0);
    assert_eq!(epoch, 0);
}

#[tokio::test]
async fn test_init_producer_id_with_txn() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Initialize with transactional_id
    let (pid1, epoch1) = mock.init_producer_id(Some("my-txn"), -1, -1).await.unwrap();
    assert!(pid1 >= 0);
    assert_eq!(epoch1, 0);

    // MockCoordinator doesn't track transactional IDs, so each call returns a new ID
    // This is simplified behavior - real Kafka would return same ID with bumped epoch
    let (pid2, epoch2) = mock
        .init_producer_id(Some("my-txn"), pid1, epoch1)
        .await
        .unwrap();
    assert!(pid2 >= 0);
    assert_eq!(epoch2, 0); // Mock always returns epoch 0
}

// ========================================================================
// ConsumerGroupCoordinator Method Tests
// ========================================================================

#[tokio::test]
async fn test_consumer_group_join_and_leave() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Join group
    let (generation, member_id, is_leader, leader_id, members) = mock
        .join_group("test-group", "", &[1, 2, 3], 30000)
        .await
        .unwrap();

    // First member should be leader
    assert!(generation > 0);
    assert!(!member_id.is_empty());
    assert!(is_leader);
    assert_eq!(leader_id, member_id);
    assert_eq!(members.len(), 1);

    // Leave group
    mock.remove_group_member("test-group", &member_id)
        .await
        .unwrap();

    // Members should be empty
    let members = mock.get_group_members("test-group").await.unwrap();
    assert!(members.is_empty());
}

#[tokio::test]
async fn test_get_all_groups() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Join multiple groups
    mock.join_group("group-a", "", &[], 30000).await.unwrap();
    mock.join_group("group-b", "", &[], 30000).await.unwrap();

    // Get all groups
    let groups = mock.get_all_groups().await.unwrap();
    assert!(groups.contains(&"group-a".to_string()));
    assert!(groups.contains(&"group-b".to_string()));
}

// ========================================================================
// PartitionTransferCoordinator Method Tests
// ========================================================================

#[tokio::test]
async fn test_get_active_broker_ids() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register this broker
    mock.register_broker().await.unwrap();

    // Get active brokers
    let active = mock.get_active_broker_ids().await;
    assert!(active.contains(&1));
}

#[tokio::test]
async fn test_get_all_partition_owners() {
    let mock = MockCoordinator::new(1, "localhost", 9092);

    // Register topics and acquire partitions
    mock.register_topic("topic-a", 2).await.unwrap();
    mock.acquire_partition("topic-a", 0, 60).await.unwrap();
    mock.acquire_partition("topic-a", 1, 60).await.unwrap();

    // Get all owners
    let owners = mock.get_all_partition_owners().await;
    assert_eq!(owners.get(&("topic-a".to_string(), 0)), Some(&1));
    assert_eq!(owners.get(&("topic-a".to_string(), 1)), Some(&1));
}
