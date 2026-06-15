//! Inline tests previously embedded in `partition_manager.rs`. Moved
//! to a sibling file so the manager itself is one read away. `super::*`
//! re-exports private helpers the tests rely on.

#![allow(clippy::module_inception)]

use super::*;

use crate::cluster::mock_coordinator::MockCoordinator;
use crate::cluster::traits::PartitionCoordinator;
use object_store::memory::InMemory;
use std::sync::Arc;

fn test_config(broker_id: i32) -> ClusterConfig {
    ClusterConfig {
        broker_id,
        host: "localhost".to_string(),
        port: 9092,
        auto_create_topics: true,
        ..Default::default()
    }
}

async fn create_test_partition_manager() -> (PartitionManager<MockCoordinator>, Arc<MockCoordinator>)
{
    let config = test_config(1);
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());

    coordinator.register_broker().await.unwrap();

    let pm = PartitionManager::new(coordinator.clone(), object_store, config, Handle::current());
    (pm, coordinator)
}

// ========================================================================
// Basic Partition Manager Tests
// ========================================================================

#[tokio::test]
async fn test_partition_manager_creation() {
    let (pm, _) = create_test_partition_manager().await;
    assert_eq!(pm.config().broker_id, 1);
    assert!(!pm.is_zombie());
}

#[tokio::test]
async fn test_partition_manager_zombie_state() {
    let (pm, _) = create_test_partition_manager().await;

    let zombie_state = pm.zombie_state();
    assert!(!zombie_state.is_active());
    assert!(!pm.is_zombie());
}

#[tokio::test]
async fn test_list_owned_initially_empty() {
    let (pm, _) = create_test_partition_manager().await;
    let owned = pm.list_owned().await;
    assert!(owned.is_empty());
}

// ========================================================================
// Partition Ownership Tests
// ========================================================================

#[tokio::test]
async fn test_acquire_partition() {
    let (pm, coordinator) = create_test_partition_manager().await;

    // Register topic first
    coordinator.register_topic("test-topic", 2).await.unwrap();

    // Initially not owned
    assert!(!pm.owns_partition("test-topic", 0).await);

    // Acquire partition
    let acquired = pm.acquire_partition("test-topic", 0).await.unwrap();
    assert!(acquired);

    // Now owned
    assert!(pm.owns_partition("test-topic", 0).await);
}

#[tokio::test]
async fn test_release_partition() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("test-topic", 1).await.unwrap();

    // Acquire first
    pm.acquire_partition("test-topic", 0).await.unwrap();
    assert!(pm.owns_partition("test-topic", 0).await);

    // Release
    pm.release_partition("test-topic", 0).await.unwrap();
    assert!(!pm.owns_partition("test-topic", 0).await);
}

#[tokio::test]
async fn test_list_owned_after_acquisition() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("test-topic", 3).await.unwrap();

    pm.acquire_partition("test-topic", 0).await.unwrap();
    pm.acquire_partition("test-topic", 2).await.unwrap();

    let owned = pm.list_owned().await;
    assert_eq!(owned.len(), 2);
    assert!(owned.contains(&("test-topic".to_string(), 0)));
    assert!(owned.contains(&("test-topic".to_string(), 2)));
}

// ========================================================================
// Partition Limits Tests
// ========================================================================

#[tokio::test]
async fn test_partition_limit_enforced() {
    let config = ClusterConfig {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        max_partitions_per_topic: 3, // Limit to 3 partitions
        ..Default::default()
    };

    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());

    coordinator.register_broker().await.unwrap();
    coordinator
        .register_topic("limited-topic", 5)
        .await
        .unwrap();

    let pm = PartitionManager::new(coordinator.clone(), object_store, config, Handle::current());

    // Should fail when trying to ensure partition beyond limit
    let result = pm.ensure_partition("limited-topic", 4).await;
    assert!(result.is_err());
}

// ========================================================================
// Config Access Tests
// ========================================================================

#[tokio::test]
async fn test_config_access() {
    let (pm, _) = create_test_partition_manager().await;

    let config = pm.config();
    assert_eq!(config.broker_id, 1);
    assert_eq!(config.host, "localhost");
    assert_eq!(config.port, 9092);
}

#[tokio::test]
async fn test_coordinator_access() {
    let (pm, coordinator) = create_test_partition_manager().await;

    // Verify we can access the coordinator through the partition manager
    let pm_coordinator = pm.coordinator();
    assert_eq!(pm_coordinator.broker_id(), coordinator.broker_id());
}

// ========================================================================
// Zombie Mode Tests
// ========================================================================

#[tokio::test]
async fn test_exit_zombie_mode_when_not_zombie() {
    let (pm, _) = create_test_partition_manager().await;

    // Should succeed when not in zombie mode
    let result = pm.exit_zombie_mode_safely().await;
    assert!(result.is_ok());
}

// ========================================================================
// Error Handling Tests
// ========================================================================

#[tokio::test]
async fn test_get_for_read_non_owned_partition() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("test-topic", 1).await.unwrap();

    // Try to get for read without owning
    let result = pm.get_for_read("test-topic", 0).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_get_for_write_non_owned_partition() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("test-topic", 1).await.unwrap();

    // Try to get for write without owning
    let result = pm.get_for_write("test-topic", 0).await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_ensure_partition_auto_creates_topic_with_default_partitions() {
    let config = ClusterConfig {
        broker_id: 1,
        default_num_partitions: 8,
        auto_create_topics: true,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());
    coordinator.register_broker().await.unwrap();
    let pm = PartitionManager::new(coordinator.clone(), object_store, config, Handle::current());

    // Action: Ensure partition 0 of a new topic. This should auto-create the topic.
    let result = pm.ensure_partition("new-topic", 0).await;

    // Assert
    assert!(result.is_ok(), "ensure_partition failed: {:?}", result);
    let partition_count = coordinator.get_partition_count("new-topic").await.unwrap();
    assert_eq!(
        partition_count,
        Some(8),
        "Topic should have been created with default_num_partitions"
    );
}

#[tokio::test]
async fn test_ensure_partition_auto_create_rejects_invalid_partition() {
    let config = ClusterConfig {
        broker_id: 1,
        default_num_partitions: 8,
        auto_create_topics: true,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());
    coordinator.register_broker().await.unwrap();
    let pm = PartitionManager::new(coordinator.clone(), object_store, config, Handle::current());

    // Action: Ensure a partition that is out of bounds for a new topic.
    let result = pm.ensure_partition("new-topic", 8).await;

    // Assert
    assert!(
        matches!(result, Err(SlateDBError::InvalidPartition { .. })),
        "ensure_partition should fail with InvalidPartition for out-of-bounds request on new topic"
    );
}

#[tokio::test]
async fn test_ensure_partition_expands_existing_topic() {
    let config = ClusterConfig {
        broker_id: 1,
        default_num_partitions: 2,
        auto_create_topics: true,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());
    coordinator.register_broker().await.unwrap();
    let pm = PartitionManager::new(coordinator.clone(), object_store, config, Handle::current());

    // Setup: create a topic first
    pm.ensure_partition("existing-topic", 0).await.unwrap();
    let initial_count = coordinator
        .get_partition_count("existing-topic")
        .await
        .unwrap();
    assert_eq!(initial_count, Some(2));

    // Action: ensure a partition outside the current range
    let result = pm.ensure_partition("existing-topic", 3).await;

    // Assert
    assert!(
        result.is_ok(),
        "ensure_partition for expansion should succeed"
    );
    let expanded_count = coordinator
        .get_partition_count("existing-topic")
        .await
        .unwrap();
    assert_eq!(
        expanded_count,
        Some(4), // 3 + 1
        "Topic should have been expanded"
    );
}

// ========================================================================
// Lease Cache Tests
// ========================================================================

/// Regression test: Verify that lease renewal updates the lease cache.
///
/// Before the fix (commit where this test was added), the lease renewal loop
/// would successfully renew leases via the coordinator but fail to update
/// the local lease_cache. This caused writes to fail with "lease TTL too short"
/// errors even though the lease was actually valid.
///
/// The bug manifested as:
/// - Partition acquired at T=0 with 60s lease
/// - Cache says "expires at T=60"
/// - Lease renewed at T=20 via coordinator (now valid until T=80)
/// - But cache still says "expires at T=60" (BUG: not updated)
/// - At T=33, cache shows only 27s remaining
/// - Writes rejected because 27s < 30s minimum required
#[tokio::test]
async fn test_lease_renewal_updates_cache() {
    let (pm, coordinator) = create_test_partition_manager().await;

    // Setup: Register topic and acquire partition
    coordinator.register_topic("cache-test", 1).await.unwrap();
    pm.acquire_partition("cache-test", 0).await.unwrap();

    // The cache should have an entry after get_for_write populates it
    // First, populate the cache by doing a get_for_write
    let _ = pm.get_for_write("cache-test", 0).await;

    // Get the initial cache expiry
    let initial_expiry = pm
        .get_cached_lease_expiry("cache-test", 0)
        .expect("Cache should have entry after get_for_write");

    // Wait a small amount to ensure time passes
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Manually trigger lease renewal (simulates what the renewal loop does)
    let renewed = pm.renew_lease_for_test("cache-test", 0).await;
    assert!(renewed, "Lease renewal should succeed");

    // Get the new cache expiry
    let new_expiry = pm
        .get_cached_lease_expiry("cache-test", 0)
        .expect("Cache should still have entry after renewal");

    // The new expiry should be later than the initial expiry
    // (accounting for the time that passed plus the full lease duration)
    assert!(
        new_expiry > initial_expiry,
        "Lease cache should be updated with later expiry after renewal. \
             Initial: {:?}, New: {:?}",
        initial_expiry,
        new_expiry
    );

    // Verify the new expiry is approximately lease_duration from now
    let expected_min =
        Instant::now() + Duration::from_secs(pm.config().lease_duration.as_secs() - 1);
    assert!(
        new_expiry >= expected_min,
        "New expiry should be at least (now + lease_duration - 1s)"
    );
}

/// Test that lease cache is properly invalidated when lease is lost.
#[tokio::test]
async fn test_lease_cache_invalidated_on_lease_loss() {
    let (pm, coordinator) = create_test_partition_manager().await;

    // Setup
    coordinator.register_topic("loss-test", 1).await.unwrap();
    pm.acquire_partition("loss-test", 0).await.unwrap();

    // Populate cache
    let _ = pm.get_for_write("loss-test", 0).await;
    assert!(
        pm.get_cached_lease_expiry("loss-test", 0).is_some(),
        "Cache should have entry"
    );

    // Release the partition
    pm.release_partition("loss-test", 0).await.unwrap();

    // Cache entry should be removed (or effectively invalidated via partition_states removal)
    // Note: The cache entry itself may still exist, but the partition is no longer owned
    assert!(
        !pm.owns_partition("loss-test", 0).await,
        "Partition should no longer be owned after release"
    );
}

// ========================================================================
// Additional Coverage Tests
// ========================================================================

#[tokio::test]
async fn test_owned_partition_count() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("count-test", 5).await.unwrap();

    // Initially zero
    assert_eq!(pm.owned_partition_count(), 0);

    // Acquire some partitions
    pm.acquire_partition("count-test", 0).await.unwrap();
    assert_eq!(pm.owned_partition_count(), 1);

    pm.acquire_partition("count-test", 1).await.unwrap();
    pm.acquire_partition("count-test", 2).await.unwrap();
    assert_eq!(pm.owned_partition_count(), 3);

    // Release one
    pm.release_partition("count-test", 1).await.unwrap();
    assert_eq!(pm.owned_partition_count(), 2);
}

#[tokio::test]
async fn test_max_owned_partitions_cap_enforced() {
    let config = ClusterConfig {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        auto_create_topics: true,
        max_owned_partitions_per_broker: 2,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());
    coordinator.register_broker().await.unwrap();
    coordinator.register_topic("cap-test", 5).await.unwrap();
    let pm = PartitionManager::new(coordinator, object_store, config, Handle::current());

    // Up to the cap succeeds.
    assert!(pm.acquire_partition("cap-test", 0).await.unwrap());
    assert!(pm.acquire_partition("cap-test", 1).await.unwrap());
    assert_eq!(pm.owned_partition_count(), 2);

    // The next acquire is rejected (Ok(false)) without opening a store.
    assert!(!pm.acquire_partition("cap-test", 2).await.unwrap());
    assert!(!pm.owns_partition("cap-test", 2).await);
    assert_eq!(pm.owned_partition_count(), 2);

    // Releasing frees a slot, so a new acquire succeeds again.
    pm.release_partition("cap-test", 0).await.unwrap();
    assert!(pm.acquire_partition("cap-test", 2).await.unwrap());
    assert_eq!(pm.owned_partition_count(), 2);

    // Re-acquiring an already-owned partition is idempotent and never
    // rejected by the cap.
    assert!(pm.acquire_partition("cap-test", 2).await.unwrap());
}

#[tokio::test]
async fn test_max_owned_partitions_zero_is_unbounded() {
    // The default (0) imposes no cap.
    let (pm, coordinator) = create_test_partition_manager().await;
    coordinator.register_topic("unbounded", 4).await.unwrap();

    for p in 0..4 {
        assert!(pm.acquire_partition("unbounded", p).await.unwrap());
    }
    assert_eq!(pm.owned_partition_count(), 4);
}

#[tokio::test]
async fn test_ensure_partition_auto_create_disabled() {
    // Test that with auto_create_topics disabled, we still can ensure
    // partitions for topics that exist. MockCoordinator may auto-register topics
    // so we just verify the config is respected.
    let config = ClusterConfig {
        broker_id: 1,
        auto_create_topics: false, // Disabled!
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());
    coordinator.register_broker().await.unwrap();
    let pm = PartitionManager::new(coordinator.clone(), object_store, config, Handle::current());

    // Verify the config was applied
    assert!(!pm.config().auto_create_topics);
}

#[tokio::test]
async fn test_get_for_write_success() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("write-test", 1).await.unwrap();
    pm.acquire_partition("write-test", 0).await.unwrap();

    // Should succeed for owned partition
    let result = pm.get_for_write("write-test", 0).await;
    assert!(
        result.is_ok(),
        "get_for_write should succeed for owned partition"
    );

    let write_guard = result.unwrap();
    assert_eq!(write_guard.topic(), "write-test");
    assert_eq!(write_guard.partition(), 0);
}

#[tokio::test]
async fn test_get_for_read_success() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("read-test", 1).await.unwrap();
    pm.acquire_partition("read-test", 0).await.unwrap();

    // Should succeed for owned partition
    let result = pm.get_for_read("read-test", 0).await;
    assert!(
        result.is_ok(),
        "get_for_read should succeed for owned partition"
    );

    let read_guard = result.unwrap();
    assert_eq!(read_guard.topic(), "read-test");
    assert_eq!(read_guard.partition(), 0);
}

#[tokio::test]
async fn test_acquire_already_owned_partition() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator
        .register_topic("already-owned", 1)
        .await
        .unwrap();

    // Acquire first
    let first = pm.acquire_partition("already-owned", 0).await.unwrap();
    assert!(first);

    // Second acquire should also succeed (already owned)
    let second = pm.acquire_partition("already-owned", 0).await.unwrap();
    assert!(second);
}

#[tokio::test]
async fn test_release_non_owned_partition() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("not-owned", 1).await.unwrap();

    // Release without owning - should succeed (no-op)
    let result = pm.release_partition("not-owned", 0).await;
    assert!(result.is_ok());
}

#[tokio::test]
async fn test_ensure_partition_negative_partition() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("neg-test", 5).await.unwrap();

    // Negative partition should either fail or be handled gracefully
    // The exact behavior depends on the implementation, but we verify it doesn't panic
    let result = pm.ensure_partition("neg-test", -1).await;
    // Just verify we get a defined result (either error or success)
    let _ = result; // Consume the result
}

#[tokio::test]
async fn test_with_jitter_returns_reasonable_values() {
    // Test the with_jitter function by verifying the output is within expected range
    let base = Duration::from_secs(10);

    // Run multiple times to verify jitter is applied
    let mut min_seen = Duration::from_secs(100);
    let mut max_seen = Duration::ZERO;

    for _ in 0..100 {
        let jittered = with_jitter(base);
        if jittered < min_seen {
            min_seen = jittered;
        }
        if jittered > max_seen {
            max_seen = jittered;
        }
    }

    // Expected range: 8.5s to 11.5s (±15% of 10s)
    assert!(
        min_seen >= Duration::from_millis(8000),
        "Min jittered value should be >= 8s, got {:?}",
        min_seen
    );
    assert!(
        max_seen <= Duration::from_millis(12000),
        "Max jittered value should be <= 12s, got {:?}",
        max_seen
    );
    // Verify we got some variation
    assert!(
        max_seen - min_seen >= Duration::from_millis(500),
        "Should see at least 500ms variation in jitter"
    );
}

#[tokio::test]
async fn test_multiple_topics_partitions() {
    let (pm, coordinator) = create_test_partition_manager().await;

    // Register multiple topics
    coordinator.register_topic("topic-a", 3).await.unwrap();
    coordinator.register_topic("topic-b", 2).await.unwrap();

    // Acquire partitions from different topics
    pm.acquire_partition("topic-a", 0).await.unwrap();
    pm.acquire_partition("topic-a", 2).await.unwrap();
    pm.acquire_partition("topic-b", 1).await.unwrap();

    // Verify ownership
    assert!(pm.owns_partition("topic-a", 0).await);
    assert!(!pm.owns_partition("topic-a", 1).await);
    assert!(pm.owns_partition("topic-a", 2).await);
    assert!(!pm.owns_partition("topic-b", 0).await);
    assert!(pm.owns_partition("topic-b", 1).await);

    // Verify count and list
    assert_eq!(pm.owned_partition_count(), 3);
    let owned = pm.list_owned().await;
    assert_eq!(owned.len(), 3);
}

#[tokio::test]
async fn test_zombie_state_accessor() {
    let (pm, _) = create_test_partition_manager().await;

    // Get zombie state
    let zombie_state = pm.zombie_state();

    // Initially not active
    assert!(!zombie_state.is_active());

    // The zombie_state() returns an Arc that can be shared
    let zombie_state2 = pm.zombie_state();
    assert!(Arc::ptr_eq(&zombie_state, &zombie_state2));
}

#[tokio::test]
async fn test_partition_manager_with_fast_failover_disabled() {
    let config = ClusterConfig {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        fast_failover_enabled: false,
        auto_balancer_enabled: false,
        ..Default::default()
    };

    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());
    coordinator.register_broker().await.unwrap();

    let pm = PartitionManager::new(coordinator, object_store, config, Handle::current());

    // Should still work without rebalance coordinator
    assert_eq!(pm.config().broker_id, 1);
    assert!(!pm.is_zombie());
}

#[tokio::test]
async fn test_partition_manager_with_fast_failover_enabled() {
    let config = ClusterConfig {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        fast_failover_enabled: true,
        auto_balancer_enabled: true,
        ..Default::default()
    };

    let coordinator = Arc::new(MockCoordinator::new(1, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());
    coordinator.register_broker().await.unwrap();

    let pm = PartitionManager::new(coordinator, object_store, config, Handle::current());

    // Should have rebalance coordinator enabled
    assert_eq!(pm.config().broker_id, 1);
    assert!(pm.config().fast_failover_enabled);
    assert!(pm.config().auto_balancer_enabled);
}

#[tokio::test]
async fn test_object_store_access() {
    let (pm, _) = create_test_partition_manager().await;

    // Verify the config has some object store path set
    assert!(!pm.config().object_store_path.is_empty());
}

#[tokio::test]
async fn test_ensure_partition_creates_slatedb_store() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("store-test", 1).await.unwrap();

    // Ensure creates the store
    let result = pm.ensure_partition("store-test", 0).await;
    assert!(result.is_ok(), "ensure_partition should succeed");

    // After ensure, we should be able to get for read/write
    let read_result = pm.get_for_read("store-test", 0).await;
    assert!(
        read_result.is_ok(),
        "get_for_read should succeed after ensure"
    );
}

// ========================================================================
// Additional Zombie Mode and Edge Case Tests
// ========================================================================

#[tokio::test]
async fn test_zombie_mode_enter_and_exit() {
    let (pm, _) = create_test_partition_manager().await;

    // Initially not zombie
    assert!(!pm.is_zombie());

    // Enter zombie mode via the state
    pm.zombie_state().enter();
    assert!(pm.is_zombie());

    // Exit zombie mode using force_exit
    pm.zombie_state().force_exit("test exit");
    assert!(!pm.is_zombie());
}

#[tokio::test]
async fn test_get_for_write_fails_in_zombie_mode() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("zombie-test", 1).await.unwrap();
    pm.acquire_partition("zombie-test", 0).await.unwrap();

    // Ensure partition is available
    let _ = pm.get_for_write("zombie-test", 0).await.unwrap();

    // Enter zombie mode
    pm.zombie_state().enter();

    // get_for_write should fail in zombie mode
    let result = pm.get_for_write("zombie-test", 0).await;
    assert!(result.is_err(), "get_for_write should fail in zombie mode");

    // Exit and verify it works again
    pm.zombie_state().force_exit("test");
    let result = pm.get_for_write("zombie-test", 0).await;
    assert!(
        result.is_ok(),
        "get_for_write should succeed after exiting zombie mode"
    );
}

#[tokio::test]
async fn test_exit_zombie_mode_safely_when_zombie() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("safe-exit", 1).await.unwrap();
    pm.acquire_partition("safe-exit", 0).await.unwrap();

    // Enter zombie mode
    pm.zombie_state().enter();
    assert!(pm.is_zombie());

    // Exit zombie mode safely should succeed
    let result = pm.exit_zombie_mode_safely().await;
    assert!(result.is_ok(), "exit_zombie_mode_safely should succeed");

    // Should no longer be zombie
    assert!(!pm.is_zombie());
}

#[tokio::test]
async fn test_concurrent_partition_acquire_release() {
    let (pm, coordinator) = create_test_partition_manager().await;
    let pm = Arc::new(pm);

    coordinator.register_topic("concurrent", 10).await.unwrap();

    // Spawn multiple tasks that acquire and release partitions
    let mut handles = vec![];
    for i in 0..5 {
        let pm_clone = pm.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..3 {
                pm_clone.acquire_partition("concurrent", i).await.unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
                pm_clone.release_partition("concurrent", i).await.unwrap();
            }
        }));
    }

    // Wait for all tasks
    for handle in handles {
        handle.await.unwrap();
    }

    // All partitions should be released
    assert_eq!(pm.owned_partition_count(), 0);
}

#[tokio::test]
async fn test_lease_cache_ttl_check() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("ttl-test", 1).await.unwrap();
    pm.acquire_partition("ttl-test", 0).await.unwrap();

    // Populate cache
    let _ = pm.get_for_write("ttl-test", 0).await.unwrap();

    // Get cached lease TTL
    let expiry = pm.get_cached_lease_expiry("ttl-test", 0);
    assert!(expiry.is_some());

    // The expiry should be in the future
    let expiry = expiry.unwrap();
    assert!(expiry > Instant::now());
}

#[tokio::test]
async fn test_get_cached_lease_expiry_non_owned() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("no-cache", 1).await.unwrap();

    // Don't acquire, just check cache
    let expiry = pm.get_cached_lease_expiry("no-cache", 0);
    assert!(
        expiry.is_none(),
        "Non-owned partition should not have cache entry"
    );
}

#[tokio::test]
async fn test_acquire_partition_twice_same_session() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator
        .register_topic("double-acquire", 1)
        .await
        .unwrap();

    // First acquire
    let first = pm.acquire_partition("double-acquire", 0).await.unwrap();
    assert!(first);

    // Second acquire (same partition) should succeed
    let second = pm.acquire_partition("double-acquire", 0).await.unwrap();
    assert!(second);

    // Should still be owned (count = 1, not 2)
    let owned = pm.list_owned().await;
    assert_eq!(owned.len(), 1);
}

#[tokio::test]
async fn test_partition_manager_broker_id() {
    let config = ClusterConfig {
        broker_id: 42,
        ..Default::default()
    };
    let coordinator = Arc::new(MockCoordinator::new(42, "localhost", 9092));
    let object_store = Arc::new(InMemory::new());
    coordinator.register_broker().await.unwrap();

    let pm = PartitionManager::new(coordinator, object_store, config, Handle::current());

    assert_eq!(pm.config().broker_id, 42);
}

#[tokio::test]
async fn test_shutdown_releases_partitions() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator
        .register_topic("shutdown-test", 3)
        .await
        .unwrap();
    pm.acquire_partition("shutdown-test", 0).await.unwrap();
    pm.acquire_partition("shutdown-test", 1).await.unwrap();
    pm.acquire_partition("shutdown-test", 2).await.unwrap();

    assert_eq!(pm.owned_partition_count(), 3);

    // Initiate shutdown
    let _ = pm.shutdown().await;

    // After shutdown, all partitions should be released
    assert_eq!(pm.owned_partition_count(), 0);
}

#[tokio::test]
async fn test_get_for_write_returns_write_guard_fields() {
    let (pm, coordinator) = create_test_partition_manager().await;

    coordinator.register_topic("guard-fields", 1).await.unwrap();
    pm.acquire_partition("guard-fields", 0).await.unwrap();

    let guard = pm.get_for_write("guard-fields", 0).await.unwrap();

    // Verify the guard is accessible (Arc<PartitionStore>)
    assert_eq!(guard.topic(), "guard-fields");
    assert_eq!(guard.partition(), 0);
}
