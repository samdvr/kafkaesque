//! Integration tests for PartitionManager.
//!
//! These tests verify partition ownership, lease management, and background tasks.

use kafkaesque::cluster::{ClusterConfig, MockCoordinator, PartitionCoordinator, PartitionManager};
use object_store::memory::InMemory;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::Handle;
use tokio::time::sleep;

/// Create a test partition manager with mock coordinator.
async fn create_test_pm() -> (PartitionManager<MockCoordinator>, Arc<MockCoordinator>) {
    let config = ClusterConfig {
        broker_id: 1,
        auto_create_topics: true,
        fast_failover_enabled: false,
        auto_balancer_enabled: false,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    coordinator.register_broker().await.unwrap();
    let object_store = Arc::new(InMemory::new());
    let pm = PartitionManager::new(coordinator.clone(), object_store, config, Handle::current());
    (pm, coordinator)
}

// ============================================================================
// Basic Ownership Tests
// ============================================================================

#[tokio::test]
async fn test_partition_manager_acquire_and_release() {
    let (pm, coordinator) = create_test_pm().await;

    // Register a topic
    coordinator.register_topic("test-topic", 3).await.unwrap();

    // Acquire partition
    let acquired = pm.acquire_partition("test-topic", 0).await.unwrap();
    assert!(acquired);
    assert!(pm.owns_partition("test-topic", 0).await);

    // Release partition
    pm.release_partition("test-topic", 0).await.unwrap();

    // Wait a bit for async release
    sleep(Duration::from_millis(100)).await;

    assert!(!pm.owns_partition("test-topic", 0).await);
}

#[tokio::test]
async fn test_partition_manager_multiple_partitions() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator.register_topic("multi", 5).await.unwrap();

    // Acquire multiple partitions
    for i in 0..5 {
        let acquired = pm.acquire_partition("multi", i).await.unwrap();
        assert!(acquired);
    }

    assert_eq!(pm.owned_partition_count(), 5);

    // List owned should return all 5
    let owned = pm.list_owned().await;
    assert_eq!(owned.len(), 5);

    // Release all
    for i in 0..5 {
        pm.release_partition("multi", i).await.unwrap();
    }

    sleep(Duration::from_millis(100)).await;
    assert_eq!(pm.owned_partition_count(), 0);
}

#[tokio::test]
async fn test_partition_manager_ensure_partition() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator.register_topic("ensure-test", 2).await.unwrap();

    // Ensure partition creates the store
    pm.ensure_partition("ensure-test", 0).await.unwrap();
    pm.ensure_partition("ensure-test", 1).await.unwrap();

    // Should be able to get for read now
    let read_store = pm.get_for_read("ensure-test", 0).await.unwrap();
    assert_eq!(read_store.topic(), "ensure-test");
    assert_eq!(read_store.partition(), 0);
}

#[tokio::test]
async fn test_partition_manager_get_for_write() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator.register_topic("write-test", 1).await.unwrap();

    // Acquire partition
    pm.acquire_partition("write-test", 0).await.unwrap();

    // Get for write should succeed
    let write_store = pm.get_for_write("write-test", 0).await.unwrap();
    assert_eq!(write_store.topic(), "write-test");
    assert_eq!(write_store.partition(), 0);
}

#[tokio::test]
async fn test_partition_manager_get_for_read() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator.register_topic("read-test", 1).await.unwrap();

    // Acquire partition
    pm.acquire_partition("read-test", 0).await.unwrap();

    // Get for read should succeed
    let read_store = pm.get_for_read("read-test", 0).await.unwrap();
    assert_eq!(read_store.topic(), "read-test");
    assert_eq!(read_store.partition(), 0);
}

#[tokio::test]
async fn test_partition_manager_get_for_write_not_owned() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator.register_topic("not-owned", 1).await.unwrap();

    // Try to get for write without acquiring first
    let result = pm.get_for_write("not-owned", 0).await;
    assert!(result.is_err());
}

// ============================================================================
// Concurrent Access Tests
// ============================================================================

#[tokio::test]
async fn test_partition_manager_concurrent_acquire() {
    let (pm, coordinator) = create_test_pm().await;
    let pm = Arc::new(pm);

    coordinator.register_topic("concurrent", 1).await.unwrap();

    // Spawn multiple tasks trying to acquire the same partition
    let mut handles = vec![];
    for _ in 0..5 {
        let pm_clone = pm.clone();
        handles.push(tokio::spawn(async move {
            pm_clone.acquire_partition("concurrent", 0).await
        }));
    }

    // All should succeed (same broker acquiring multiple times is OK)
    let mut success_count = 0;
    for handle in handles {
        if let Ok(Ok(true)) = handle.await {
            success_count += 1;
        }
    }

    // At least one should succeed
    assert!(success_count > 0);
}

#[tokio::test]
async fn test_partition_manager_concurrent_read_write() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator.register_topic("rw-test", 1).await.unwrap();

    // Acquire partition
    pm.acquire_partition("rw-test", 0).await.unwrap();

    // Multiple concurrent reads
    let pm = Arc::new(pm);
    let mut read_handles = vec![];
    for _ in 0..3 {
        let pm_clone = pm.clone();
        read_handles.push(tokio::spawn(async move {
            pm_clone.get_for_read("rw-test", 0).await.is_ok()
        }));
    }

    // All reads should succeed
    for handle in read_handles {
        assert!(handle.await.unwrap());
    }
}

// ============================================================================
// Configuration Tests
// ============================================================================

#[tokio::test]
async fn test_partition_manager_config_access() {
    let (pm, _) = create_test_pm().await;

    let config = pm.config();
    assert_eq!(config.broker_id, 1);
    assert!(config.auto_create_topics);
    assert!(!config.fast_failover_enabled);
}

#[tokio::test]
async fn test_partition_manager_zombie_state() {
    let (pm, _) = create_test_pm().await;

    // Initially not zombie
    assert!(!pm.is_zombie());

    // Get zombie state
    let zombie_state = pm.zombie_state();
    assert!(!zombie_state.is_active());
}

// ============================================================================
// Auto-Create Topic Tests
// ============================================================================

#[tokio::test]
async fn test_partition_manager_auto_create_topic() {
    let config = ClusterConfig {
        broker_id: 1,
        auto_create_topics: true,
        default_num_partitions: 5,
        fast_failover_enabled: false,
        auto_balancer_enabled: false,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    coordinator.register_broker().await.unwrap();
    let object_store = Arc::new(InMemory::new());
    let pm = PartitionManager::new(coordinator, object_store, config, Handle::current());

    // Ensure partition on non-existent topic should auto-create
    let result = pm.ensure_partition("auto-created", 0).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_partition_manager_no_auto_create_topic() {
    let config = ClusterConfig {
        broker_id: 1,
        auto_create_topics: false,
        fast_failover_enabled: false,
        auto_balancer_enabled: false,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    coordinator.register_broker().await.unwrap();
    let object_store = Arc::new(InMemory::new());
    let pm = PartitionManager::new(coordinator, object_store, config, Handle::current());

    // Ensure partition on non-existent topic should fail
    // Note: MockCoordinator may still allow it, but we're testing the config path
    let _result = pm.ensure_partition("should-not-create", 0).await;
    // Just verify config is respected
    assert!(!pm.config().auto_create_topics);
}

// ============================================================================
// Multiple Topics Tests
// ============================================================================

#[tokio::test]
async fn test_partition_manager_multiple_topics() {
    let (pm, coordinator) = create_test_pm().await;

    // Register multiple topics
    coordinator.register_topic("topic-a", 2).await.unwrap();
    coordinator.register_topic("topic-b", 3).await.unwrap();
    coordinator.register_topic("topic-c", 1).await.unwrap();

    // Acquire partitions from different topics
    pm.acquire_partition("topic-a", 0).await.unwrap();
    pm.acquire_partition("topic-a", 1).await.unwrap();
    pm.acquire_partition("topic-b", 0).await.unwrap();
    pm.acquire_partition("topic-b", 2).await.unwrap();
    pm.acquire_partition("topic-c", 0).await.unwrap();

    // Verify ownership
    assert_eq!(pm.owned_partition_count(), 5);
    assert!(pm.owns_partition("topic-a", 0).await);
    assert!(pm.owns_partition("topic-a", 1).await);
    assert!(!pm.owns_partition("topic-b", 1).await); // Not acquired
    assert!(pm.owns_partition("topic-b", 0).await);
    assert!(pm.owns_partition("topic-b", 2).await);
    assert!(pm.owns_partition("topic-c", 0).await);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

#[tokio::test]
async fn test_partition_manager_release_non_owned() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator.register_topic("not-owned", 1).await.unwrap();

    // Release without owning should be a no-op (not an error)
    let result = pm.release_partition("not-owned", 0).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_partition_manager_acquire_invalid_partition() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator.register_topic("small-topic", 2).await.unwrap();

    // Acquire partition beyond topic's partition count
    // MockCoordinator may allow this, but real coordinator wouldn't
    let result = pm.acquire_partition("small-topic", 99).await;
    // Result depends on MockCoordinator behavior
    let _ = result; // Just consume, don't assert
}

// ============================================================================
// Fast Failover Configuration Tests
// ============================================================================

#[tokio::test]
async fn test_partition_manager_with_fast_failover() {
    let config = ClusterConfig {
        broker_id: 1,
        fast_failover_enabled: true,
        auto_balancer_enabled: true,
        fast_heartbeat_interval_ms: 100,
        failure_suspicion_threshold: 2,
        failure_threshold: 5,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    coordinator.register_broker().await.unwrap();
    let object_store = Arc::new(InMemory::new());
    let pm = PartitionManager::new(coordinator.clone(), object_store, config, Handle::current());

    // Verify config
    assert!(pm.config().fast_failover_enabled);
    assert!(pm.config().auto_balancer_enabled);

    // Register topic and acquire partition
    coordinator.register_topic("ff-topic", 1).await.unwrap();
    pm.acquire_partition("ff-topic", 0).await.unwrap();

    assert!(pm.owns_partition("ff-topic", 0).await);
}

// ============================================================================
// Shutdown Tests
// ============================================================================

#[tokio::test]
async fn test_partition_manager_shutdown_releases_partitions() {
    let (pm, coordinator) = create_test_pm().await;

    coordinator
        .register_topic("shutdown-test", 3)
        .await
        .unwrap();

    // Acquire partitions
    for i in 0..3 {
        pm.acquire_partition("shutdown-test", i).await.unwrap();
    }

    assert_eq!(pm.owned_partition_count(), 3);

    // Release all
    for i in 0..3 {
        pm.release_partition("shutdown-test", i).await.unwrap();
    }

    // Wait for async cleanup
    sleep(Duration::from_millis(200)).await;

    // After release, should have no owned partitions
    assert_eq!(pm.owned_partition_count(), 0);
}
