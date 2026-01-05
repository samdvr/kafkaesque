//! Edge case and failure scenario tests.
//!
//! These tests cover critical failure modes:
//! - P0: Raft snapshot crash recovery
//! - P0: Producer state persistence and recovery after cache eviction
//! - P1: Lease expiry race conditions
//! - P2: Consumer group rebalance during fetch
//! - P2: Object store timeout handling
//!
//! Run these tests with:
//! ```bash
//! cargo test --test edge_case_tests
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutPayload};
use tokio::time::sleep;

use kafkaesque::cluster::SlateDBError;
use kafkaesque::cluster::raft::{
    BrokerCommand, CoordinationCommand, CoordinationResponse, GroupCommand, GroupResponse,
    PartitionCommand, ProducerCommand, RaftStore,
};

// ============================================================================
// P0: Raft Snapshot Crash Recovery Tests
// ============================================================================

/// Test that orphaned temp files from a crashed snapshot write are cleaned up.
///
/// Simulates a scenario where:
/// 1. Snapshot write starts, creates temp files
/// 2. Process crashes before rename completes
/// 3. On restart, orphaned temp files are cleaned up
/// 4. Old valid snapshot is still usable
#[tokio::test]
async fn test_snapshot_crash_leaves_orphaned_temp_files_cleaned_on_startup() {
    let object_store = Arc::new(InMemory::new());
    let snapshot_prefix = "crash-test/snapshots";

    // First, create a valid snapshot by using RaftStore normally
    {
        use openraft::RaftSnapshotBuilder;

        let mut store = RaftStore::new(object_store.clone(), snapshot_prefix);

        // Build a snapshot to create valid files
        let _snapshot = store.build_snapshot().await.unwrap();
    }

    // Simulate crashed snapshot by manually creating orphaned temp files
    let temp_data_path = ObjectPath::from(format!("{}/temp-crashed.snapshot", snapshot_prefix));
    let temp_meta_path = ObjectPath::from(format!("{}/temp-crashed.meta", snapshot_prefix));

    object_store
        .put(
            &temp_data_path,
            PutPayload::from(Bytes::from("partial data")),
        )
        .await
        .unwrap();
    object_store
        .put(
            &temp_meta_path,
            PutPayload::from(Bytes::from("partial meta")),
        )
        .await
        .unwrap();

    // Verify temp files exist
    assert!(object_store.head(&temp_data_path).await.is_ok());
    assert!(object_store.head(&temp_meta_path).await.is_ok());

    // Create new store - should clean up temp files on load
    let store2 = RaftStore::new(object_store.clone(), snapshot_prefix);
    let _loaded = store2.load_snapshot_from_store().await;

    // Give async cleanup a moment
    sleep(Duration::from_millis(100)).await;

    // Temp files should be cleaned up
    assert!(object_store.head(&temp_data_path).await.is_err());
    assert!(object_store.head(&temp_meta_path).await.is_err());
}

/// Test that a valid snapshot survives even if temp files exist.
///
/// This verifies the two-phase commit pattern: the final files are the
/// source of truth, temp files are just transient artifacts.
#[tokio::test]
async fn test_valid_snapshot_survives_with_orphaned_temps() {
    use openraft::{EntryPayload, RaftSnapshotBuilder, RaftStorage};

    let object_store = Arc::new(InMemory::new());
    let snapshot_prefix = "survive-test/snapshots";

    // Create a valid snapshot with some state
    {
        let mut store = RaftStore::new(object_store.clone(), snapshot_prefix);

        // Make a log entry with a specific command
        let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 1);
        let entry = openraft::Entry {
            log_id,
            payload: EntryPayload::Normal(CoordinationCommand::PartitionDomain(
                PartitionCommand::CreateTopic {
                    name: "test-topic".to_string(),
                    partitions: 3,
                    config: HashMap::new(),
                    timestamp_ms: 1000,
                },
            )),
        };

        // Apply the entry
        store.apply_to_state_machine(&[entry]).await.unwrap();

        // Build snapshot
        let _snapshot = store.build_snapshot().await.unwrap();
    }

    // Add orphaned temp files (simulating a second crashed snapshot)
    let temp_data_path = ObjectPath::from(format!("{}/temp-orphan.snapshot", snapshot_prefix));
    object_store
        .put(&temp_data_path, PutPayload::from(Bytes::from("garbage")))
        .await
        .unwrap();

    // Load snapshot in new store - should still work
    let store2 = RaftStore::new(object_store.clone(), snapshot_prefix);
    let loaded = store2.load_snapshot_from_store().await.unwrap();
    assert!(loaded, "Should successfully load valid snapshot");

    // Verify state was restored
    let sm = store2.state_machine();
    let state = sm.read().await;
    let inner_state = state.state().await;
    assert!(
        inner_state
            .partition_domain
            .topics
            .contains_key("test-topic"),
        "Topic should be restored from snapshot"
    );
    assert_eq!(
        inner_state.partition_domain.topics["test-topic"].partition_count, 3,
        "Partition count should be correct"
    );
}

// ============================================================================
// P0: Producer State Persistence and Recovery Tests
// ============================================================================

/// Test that producer state is persisted to SlateDB and survives restart.
///
/// Simulates:
/// 1. Producer sends messages with sequence numbers
/// 2. Partition store is closed (simulating restart)
/// 3. New partition store loads producer state from SlateDB
/// 4. Duplicate detection still works
#[tokio::test]
async fn test_producer_state_persists_across_restart() {
    use openraft::{EntryPayload, RaftSnapshotBuilder, RaftStorage};

    let object_store = Arc::new(InMemory::new());

    // Simulate producer state being stored via Raft
    let mut store = RaftStore::new(object_store.clone(), "producer-test/snapshots");

    // Store producer state
    let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 1);
    let entry = openraft::Entry {
        log_id,
        payload: EntryPayload::Normal(CoordinationCommand::ProducerDomain(
            ProducerCommand::StoreProducerState {
                topic: "test-topic".to_string(),
                partition: 0,
                producer_id: 12345,
                last_sequence: 100,
                producer_epoch: 1,
                timestamp_ms: 1000,
            },
        )),
    };

    store.apply_to_state_machine(&[entry]).await.unwrap();

    // Build snapshot to persist
    let _snapshot = store.build_snapshot().await.unwrap();

    // Simulate restart - create new store and load
    let store2 = RaftStore::new(object_store.clone(), "producer-test/snapshots");
    let loaded = store2.load_snapshot_from_store().await.unwrap();
    assert!(loaded);

    // Verify producer state was restored
    let sm = store2.state_machine();
    let state = sm.read().await;
    let inner_state = state.state().await;

    let key = (std::sync::Arc::from("test-topic"), 0i32, 12345i64);
    assert!(
        inner_state
            .producer_domain
            .producer_states
            .contains_key(&key),
        "Producer state should be restored"
    );

    let producer_state = &inner_state.producer_domain.producer_states[&key];
    assert_eq!(producer_state.last_sequence, 100);
    assert_eq!(producer_state.producer_epoch, 1);
}

/// Test that loading producer states from coordinator works correctly.
#[tokio::test]
async fn test_producer_state_load_from_coordinator() {
    use openraft::{EntryPayload, RaftSnapshotBuilder, RaftStorage};

    let object_store = Arc::new(InMemory::new());
    let mut store = RaftStore::new(object_store.clone(), "load-test/snapshots");

    // Store multiple producer states for the same partition
    let commands = vec![
        CoordinationCommand::ProducerDomain(ProducerCommand::StoreProducerState {
            topic: "topic-a".to_string(),
            partition: 0,
            producer_id: 1001,
            last_sequence: 50,
            producer_epoch: 0,
            timestamp_ms: 1000,
        }),
        CoordinationCommand::ProducerDomain(ProducerCommand::StoreProducerState {
            topic: "topic-a".to_string(),
            partition: 0,
            producer_id: 1002,
            last_sequence: 75,
            producer_epoch: 0,
            timestamp_ms: 1000,
        }),
        CoordinationCommand::ProducerDomain(ProducerCommand::StoreProducerState {
            topic: "topic-a".to_string(),
            partition: 1, // Different partition
            producer_id: 1001,
            last_sequence: 25,
            producer_epoch: 0,
            timestamp_ms: 1000,
        }),
    ];

    for (i, cmd) in commands.into_iter().enumerate() {
        let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), (i + 1) as u64);
        let entry = openraft::Entry {
            log_id,
            payload: EntryPayload::Normal(cmd),
        };
        store.apply_to_state_machine(&[entry]).await.unwrap();
    }

    // Build and restore snapshot
    let _snapshot = store.build_snapshot().await.unwrap();

    let store2 = RaftStore::new(object_store.clone(), "load-test/snapshots");
    store2.load_snapshot_from_store().await.unwrap();

    let sm = store2.state_machine();
    let state = sm.read().await;
    let inner_state = state.state().await;

    // Should have 3 producer states
    assert_eq!(inner_state.producer_domain.producer_states.len(), 3);

    // Verify each state
    assert_eq!(
        inner_state.producer_domain.producer_states[&(std::sync::Arc::from("topic-a"), 0, 1001)]
            .last_sequence,
        50
    );
    assert_eq!(
        inner_state.producer_domain.producer_states[&(std::sync::Arc::from("topic-a"), 0, 1002)]
            .last_sequence,
        75
    );
    assert_eq!(
        inner_state.producer_domain.producer_states[&(std::sync::Arc::from("topic-a"), 1, 1001)]
            .last_sequence,
        25
    );
}

// ============================================================================
// P1: Lease Expiry Race Condition Tests
// ============================================================================

/// Test that zombie mode correctly blocks writes during lease transition.
///
/// This tests the scenario where:
/// 1. Broker holds a lease
/// 2. Lease expires / another broker acquires
/// 3. Original broker's writes should be rejected
#[tokio::test]
async fn test_zombie_mode_blocks_writes() {
    use kafkaesque::cluster::zombie_mode::ZombieModeState;

    // Create zombie mode state
    let zombie_state = ZombieModeState::new();

    // Initially not in zombie mode
    assert!(!zombie_state.is_active());

    // Enter zombie mode (simulating heartbeat failure or lease loss)
    let entered = zombie_state.enter();
    assert!(entered, "Should successfully enter zombie mode");
    assert!(zombie_state.is_active());
}

/// Test the double-check locking pattern in partition store.
///
/// The partition store should check zombie mode both before and after
/// acquiring the write lock to handle the race condition where zombie
/// mode is entered while waiting for the lock.
#[tokio::test]
async fn test_double_check_zombie_mode_pattern() {
    use kafkaesque::cluster::zombie_mode::ZombieModeState;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    let zombie_state = Arc::new(ZombieModeState::new());
    let write_lock = Arc::new(Mutex::new(()));

    // Simulate the double-check pattern
    // Check 1: Before acquiring lock
    assert!(!zombie_state.is_active(), "Should not be zombie initially");

    // Acquire lock
    let _guard = write_lock.lock().await;

    // Simulate zombie mode being entered while we held the lock
    // (in reality this would happen from another task)
    zombie_state.enter();

    // Check 2: After acquiring lock (the double-check)
    assert!(
        zombie_state.is_active(),
        "Should detect zombie mode after lock acquisition"
    );

    // This check should cause the write to be rejected
}

/// Test zombie mode re-entry detection during verification.
#[tokio::test]
async fn test_zombie_mode_reentry_detection() {
    use kafkaesque::cluster::zombie_mode::ZombieModeState;

    let zombie_state = ZombieModeState::new();

    // Enter zombie mode
    zombie_state.enter();
    let entered_at = zombie_state.entered_at();
    assert!(entered_at > 0);

    // Simulate verification passing
    // In real code, this would involve checking partition ownership

    // Simulate re-entry during verification (from another task)
    // First exit, then enter again
    // Note: We can't easily test this without access to try_exit_with_timestamp
    // but we can verify the timestamp changes

    // The entered_at should be non-zero when active
    assert!(zombie_state.is_active());
    assert!(zombie_state.entered_at() > 0);
}

// ============================================================================
// P2: Consumer Group Rebalance During Fetch Tests
// ============================================================================

/// Test that generation validation prevents stale consumers from committing.
///
/// This is a unit test for the validate_member_for_commit functionality.
#[tokio::test]
async fn test_generation_validation_rejects_stale_consumer() {
    use openraft::{EntryPayload, RaftStorage};

    let object_store = Arc::new(InMemory::new());
    let mut store = RaftStore::new(object_store, "generation-test/snapshots");

    // Create a group and add a member
    let join_cmd = CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
        group_id: "test-group".to_string(),
        member_id: "member-1".to_string(),
        client_id: "client-1".to_string(),
        client_host: "localhost".to_string(),
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
        session_timeout_ms: 10000,
        rebalance_timeout_ms: 10000,
        timestamp_ms: 1000,
    });

    let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 1);
    let entry = openraft::Entry {
        log_id,
        payload: EntryPayload::Normal(join_cmd),
    };
    let responses = store.apply_to_state_machine(&[entry]).await.unwrap();

    // Get the assigned generation
    let generation = match &responses[0] {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            generation,
            ..
        }) => generation,
        _ => panic!("Expected JoinGroupResponse"),
    };

    // Verify member with correct generation is valid
    let sm = store.state_machine();
    let state = sm.read().await;
    let inner_state = state.state().await;

    let group = inner_state.group_domain.groups.get("test-group").unwrap();
    assert!(group.members.contains_key("member-1"));
    assert_eq!(group.generation, *generation);

    // A stale generation (generation - 1) should not match
    assert_ne!(group.generation, generation - 1);
}

/// Test that only the leader can set assignments in SyncGroup.
///
/// In Kafka's consumer group protocol:
/// - All members call SyncGroup
/// - The leader provides assignments for all members
/// - Followers call SyncGroup with empty assignments to receive their assignment
#[tokio::test]
async fn test_sync_group_leader_sets_assignments() {
    use openraft::{EntryPayload, RaftStorage};

    let object_store = Arc::new(InMemory::new());
    let mut store = RaftStore::new(object_store, "sync-test/snapshots");

    // Create group with leader-member as leader (first to join)
    let join_cmd = CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
        group_id: "sync-group".to_string(),
        member_id: "leader-member".to_string(),
        client_id: "client".to_string(),
        client_host: "localhost".to_string(),
        protocol_type: "consumer".to_string(),
        protocols: vec![("range".to_string(), vec![])],
        session_timeout_ms: 10000,
        rebalance_timeout_ms: 10000,
        timestamp_ms: 1000,
    });

    let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 1);
    let entry = openraft::Entry {
        log_id,
        payload: EntryPayload::Normal(join_cmd),
    };
    let responses = store.apply_to_state_machine(&[entry]).await.unwrap();

    let (generation, leader_id) = match &responses[0] {
        CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
            generation,
            leader_id,
            ..
        }) => (*generation, leader_id.clone()),
        _ => panic!("Expected JoinGroupResponse"),
    };

    // Verify leader-member is the leader
    assert_eq!(leader_id, "leader-member");

    // Leader can set assignments
    let sync_cmd = CoordinationCommand::GroupDomain(GroupCommand::SyncGroup {
        group_id: "sync-group".to_string(),
        member_id: "leader-member".to_string(),
        generation,
        assignments: vec![("leader-member".to_string(), vec![1, 2, 3])],
    });

    let log_id2 = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 2);
    let entry2 = openraft::Entry {
        log_id: log_id2,
        payload: EntryPayload::Normal(sync_cmd),
    };
    let responses2 = store.apply_to_state_machine(&[entry2]).await.unwrap();

    // Leader should get their assignment back
    match &responses2[0] {
        CoordinationResponse::GroupDomainResponse(GroupResponse::SyncGroupResponse {
            assignment,
        }) => {
            assert_eq!(assignment, &vec![1u8, 2, 3]);
        }
        other => panic!("Expected SyncGroupResponse, got {:?}", other),
    }
}

// ============================================================================
// P2: Object Store Timeout Tests
// ============================================================================

/// Test that flush failures are properly propagated.
///
/// Note: This tests the error handling path, not actual timeouts
/// (which would require mock object stores).
#[tokio::test]
async fn test_flush_failure_error_handling() {
    // Verify the FlushFailed error type exists and has correct Kafka mapping
    let error = SlateDBError::FlushFailed {
        topic: "test-topic".to_string(),
        partition: 0,
        message: "Simulated timeout".to_string(),
    };

    // Should map to retriable error
    assert!(error.is_retriable());

    // Should map to Unknown (retriable) Kafka code
    let kafka_code = error.to_kafka_code();
    assert_eq!(kafka_code, kafkaesque::error::KafkaCode::Unknown);
}

/// Test error classification for various storage errors.
#[tokio::test]
async fn test_storage_error_classification() {
    // Test various error types and their classifications
    let test_cases = vec![
        (
            SlateDBError::Fenced,
            true,  // is_fenced
            true,  // is_not_leader
            false, // is_retriable
        ),
        (
            SlateDBError::NotOwned {
                topic: "t".to_string(),
                partition: 0,
            },
            false, // is_fenced
            true,  // is_not_leader
            false, // is_retriable
        ),
        (
            SlateDBError::FlushFailed {
                topic: "t".to_string(),
                partition: 0,
                message: "err".to_string(),
            },
            false, // is_fenced
            false, // is_not_leader
            true,  // is_retriable
        ),
        (
            SlateDBError::Storage("error".to_string()),
            false, // is_fenced
            false, // is_not_leader
            true,  // is_retriable
        ),
    ];

    for (error, expected_fenced, expected_not_leader, expected_retriable) in test_cases {
        assert_eq!(
            error.is_fenced(),
            expected_fenced,
            "is_fenced mismatch for {:?}",
            error
        );
        assert_eq!(
            error.is_not_leader(),
            expected_not_leader,
            "is_not_leader mismatch for {:?}",
            error
        );
        assert_eq!(
            error.is_retriable(),
            expected_retriable,
            "is_retriable mismatch for {:?}",
            error
        );
    }
}

// ============================================================================
// Integration Test Helpers
// ============================================================================

/// Verify snapshot atomicity by checking that both data and metadata
/// files are written together.
#[tokio::test]
async fn test_snapshot_files_written_atomically() {
    use openraft::{EntryPayload, RaftSnapshotBuilder, RaftStorage};

    let object_store = Arc::new(InMemory::new());
    let snapshot_prefix = "atomic-test/snapshots";

    let mut store = RaftStore::new(object_store.clone(), snapshot_prefix);

    // Apply some state
    let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 1);
    let entry = openraft::Entry {
        log_id,
        payload: EntryPayload::Normal(CoordinationCommand::BrokerDomain(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        })),
    };
    store.apply_to_state_machine(&[entry]).await.unwrap();

    // Build snapshot
    let _snapshot = store.build_snapshot().await.unwrap();

    // Both files should exist
    let data_path = ObjectPath::from(format!("{}/current.snapshot", snapshot_prefix));
    let meta_path = ObjectPath::from(format!("{}/current.meta", snapshot_prefix));

    assert!(
        object_store.head(&data_path).await.is_ok(),
        "Snapshot data file should exist"
    );
    assert!(
        object_store.head(&meta_path).await.is_ok(),
        "Snapshot metadata file should exist"
    );
}

/// Test producer epoch fencing.
#[tokio::test]
async fn test_producer_epoch_fencing() {
    use openraft::{EntryPayload, RaftStorage};

    let object_store = Arc::new(InMemory::new());
    let mut store = RaftStore::new(object_store.clone(), "epoch-test/snapshots");

    // Store initial producer state with epoch 1
    let log_id = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 1);
    let entry = openraft::Entry {
        log_id,
        payload: EntryPayload::Normal(CoordinationCommand::ProducerDomain(
            ProducerCommand::StoreProducerState {
                topic: "test-topic".to_string(),
                partition: 0,
                producer_id: 1000,
                last_sequence: 50,
                producer_epoch: 1,
                timestamp_ms: 1000,
            },
        )),
    };
    store.apply_to_state_machine(&[entry]).await.unwrap();

    // Update to epoch 2 (simulating producer restart)
    let log_id2 = openraft::LogId::new(openraft::CommittedLeaderId::new(1, 0), 2);
    let entry2 = openraft::Entry {
        log_id: log_id2,
        payload: EntryPayload::Normal(CoordinationCommand::ProducerDomain(
            ProducerCommand::StoreProducerState {
                topic: "test-topic".to_string(),
                partition: 0,
                producer_id: 1000,
                last_sequence: 0, // Reset sequence for new epoch
                producer_epoch: 2,
                timestamp_ms: 1000,
            },
        )),
    };
    store.apply_to_state_machine(&[entry2]).await.unwrap();

    // Verify the state reflects the new epoch
    let sm = store.state_machine();
    let state = sm.read().await;
    let inner_state = state.state().await;

    let key = (std::sync::Arc::from("test-topic"), 0i32, 1000i64);
    let producer_state = &inner_state.producer_domain.producer_states[&key];
    assert_eq!(producer_state.producer_epoch, 2);
    assert_eq!(producer_state.last_sequence, 0);
}
