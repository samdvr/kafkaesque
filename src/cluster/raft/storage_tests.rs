//! Inline tests previously embedded in `storage.rs`. Moved to a sibling
//! file so the implementation isn't gated on scrolling past the test
//! block. `super::*` re-exports private helpers the tests rely on.
//!
//! Exercise the control-group flavour of `RaftStore<G>` — the storage
//! invariants (WAL recovery, snapshot pointer scheme, conflict-truncate
//! safety) are generic over `GroupKind`, so testing them through the
//! control kind covers the shard kind too.

#![allow(clippy::module_inception)]
#![allow(unused_imports)]

use super::*;

use super::super::commands::{ControlCommand, ControlResponse};
use super::super::domains;
use super::super::group::ControlGroupKind;
use super::super::state_machine::control::ControlState;
use super::super::types::ControlConfig;
use super::*;
use object_store::memory::InMemory;
use openraft::{RaftLogReader, RaftStorage, Vote};

/// Create a test RaftStore<ControlGroupKind> with in-memory object store.
fn create_test_store() -> RaftStore<ControlGroupKind> {
    let object_store = Arc::new(InMemory::new());
    RaftStore::<ControlGroupKind>::new(object_store, "test/snapshots")
}

/// Helper to create a log ID for testing.
fn make_log_id(term: u64, node: u64, index: u64) -> LogId<RaftNodeId> {
    LogId::new(openraft::CommittedLeaderId::new(term, node), index)
}

/// Helper to create an entry for testing.
fn make_entry(
    term: u64,
    node: u64,
    index: u64,
    payload: EntryPayload<ControlConfig>,
) -> Entry<ControlConfig> {
    Entry {
        log_id: make_log_id(term, node, index),
        payload,
    }
}

#[tokio::test]
async fn test_new_store_creation() {
    let store = create_test_store();

    // Verify initial state
    assert!(store.vote.read().await.is_none());
    assert!(store.log.read().await.is_empty());
    assert!(store.last_purged_log_id.read().await.is_none());
    assert!(store.last_applied_log.read().await.is_none());
    assert!(store.cached_snapshot.read().await.is_none());
}

#[tokio::test]
async fn test_save_and_read_vote() {
    let mut store = create_test_store();

    // Initially no vote
    let vote = store.read_vote().await.unwrap();
    assert!(vote.is_none());

    // Save a vote
    let test_vote = Vote::new(1, 42);
    store.save_vote(&test_vote).await.unwrap();

    // Read it back
    let vote = store.read_vote().await.unwrap();
    assert!(vote.is_some());
    let vote = vote.unwrap();
    assert_eq!(vote.leader_id().voted_for(), Some(42));
}

#[tokio::test]
async fn test_append_to_log() {
    let mut store = create_test_store();

    // Create some log entries
    let entries = vec![
        make_entry(1, 0, 1, EntryPayload::Blank),
        make_entry(1, 0, 2, EntryPayload::Blank),
        make_entry(1, 0, 3, EntryPayload::Blank),
    ];

    // Append entries
    store.append_to_log(entries).await.unwrap();

    // Verify they were stored
    let log = store.log.read().await;
    assert_eq!(log.len(), 3);
    assert!(log.contains_key(&1));
    assert!(log.contains_key(&2));
    assert!(log.contains_key(&3));
}

#[tokio::test]
async fn test_get_log_state() {
    let mut store = create_test_store();

    // Initial state - empty log
    let state = store.get_log_state().await.unwrap();
    assert!(state.last_purged_log_id.is_none());
    assert!(state.last_log_id.is_none());

    // Add some entries
    let entries = vec![
        make_entry(1, 0, 1, EntryPayload::Blank),
        make_entry(1, 0, 2, EntryPayload::Blank),
    ];
    store.append_to_log(entries).await.unwrap();

    // Check state again
    let state = store.get_log_state().await.unwrap();
    assert!(state.last_purged_log_id.is_none());
    assert_eq!(state.last_log_id.unwrap().index, 2);
}

#[tokio::test]
async fn test_delete_conflict_logs() {
    let mut store = create_test_store();

    // Add entries
    let entries = vec![
        make_entry(1, 0, 1, EntryPayload::Blank),
        make_entry(1, 0, 2, EntryPayload::Blank),
        make_entry(1, 0, 3, EntryPayload::Blank),
        make_entry(1, 0, 4, EntryPayload::Blank),
    ];
    store.append_to_log(entries).await.unwrap();

    // Delete from index 3 onwards
    let log_id = make_log_id(1, 0, 3);
    store.delete_conflict_logs_since(log_id).await.unwrap();

    // Verify only entries 1 and 2 remain
    let log = store.log.read().await;
    assert_eq!(log.len(), 2);
    assert!(log.contains_key(&1));
    assert!(log.contains_key(&2));
    assert!(!log.contains_key(&3));
    assert!(!log.contains_key(&4));
}

#[tokio::test]
async fn test_purge_logs() {
    let mut store = create_test_store();

    // Add entries
    let entries = vec![
        make_entry(1, 0, 1, EntryPayload::Blank),
        make_entry(1, 0, 2, EntryPayload::Blank),
        make_entry(1, 0, 3, EntryPayload::Blank),
        make_entry(1, 0, 4, EntryPayload::Blank),
    ];
    store.append_to_log(entries).await.unwrap();

    // Purge up to index 2
    let log_id = make_log_id(1, 0, 2);
    store.purge_logs_upto(log_id).await.unwrap();

    // Verify entries 1 and 2 are gone
    let log = store.log.read().await;
    assert_eq!(log.len(), 2);
    assert!(!log.contains_key(&1));
    assert!(!log.contains_key(&2));
    assert!(log.contains_key(&3));
    assert!(log.contains_key(&4));

    // Verify last_purged_log_id is set
    let purged = store.last_purged_log_id.read().await;
    assert!(purged.is_some());
    assert_eq!(purged.unwrap().index, 2);
}

#[tokio::test]
async fn test_last_applied_state() {
    let mut store = create_test_store();

    // Initial state
    let (last_applied, _membership) = store.last_applied_state().await.unwrap();
    assert!(last_applied.is_none());
}

#[tokio::test]
async fn test_apply_blank_entries() {
    let mut store = create_test_store();

    // Create blank entries
    let entries = vec![
        make_entry(1, 0, 1, EntryPayload::Blank),
        make_entry(1, 0, 2, EntryPayload::Blank),
    ];

    // Apply entries
    let responses = store.apply_to_state_machine(&entries).await.unwrap();

    // Verify responses
    assert_eq!(responses.len(), 2);
    for response in responses {
        assert_eq!(response, ControlResponse::Ok);
    }

    // Verify last_applied_log is updated
    let last_applied = store.last_applied_log.read().await;
    assert!(last_applied.is_some());
    assert_eq!(last_applied.unwrap().index, 2);
}

#[tokio::test]
async fn test_apply_normal_entries() {
    let mut store = create_test_store();

    // Create entries with a command
    let command = ControlCommand::Broker(domains::BrokerCommand::Register {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        timestamp_ms: 1000,
    });

    let entries = vec![make_entry(1, 0, 1, EntryPayload::Normal(command))];

    // Apply entries
    let responses = store.apply_to_state_machine(&entries).await.unwrap();

    // Verify response
    assert_eq!(responses.len(), 1);
    match &responses[0] {
        ControlResponse::Broker(domains::BrokerResponse::Registered { broker_id }) => {
            assert_eq!(*broker_id, 1);
        }
        _ => panic!("Expected BrokerRegistered response"),
    }
}

#[tokio::test]
async fn test_get_log_reader() {
    let mut store = create_test_store();

    // Add some entries
    let entries = vec![make_entry(1, 0, 1, EntryPayload::Blank)];
    store.append_to_log(entries).await.unwrap();

    // Get log reader
    let reader = store.get_log_reader().await;

    // Verify reader shares state
    let log = reader.log.read().await;
    assert_eq!(log.len(), 1);
}

#[tokio::test]
async fn test_get_snapshot_builder() {
    let mut store = create_test_store();

    // Get snapshot builder
    let builder = store.get_snapshot_builder().await;

    // Verify builder shares state
    let cached = builder.cached_snapshot.read().await;
    assert!(cached.is_none());
}

#[tokio::test]
async fn test_begin_receiving_snapshot() {
    let mut store = create_test_store();

    // Begin receiving snapshot
    let cursor = store.begin_receiving_snapshot().await.unwrap();

    // Verify we got an empty cursor
    assert!(cursor.get_ref().is_empty());
}

#[tokio::test]
async fn test_get_current_snapshot_empty() {
    let mut store = create_test_store();

    // Initially no snapshot
    let snapshot = store.get_current_snapshot().await.unwrap();
    assert!(snapshot.is_none());
}

#[tokio::test]
async fn test_state_machine_accessor() {
    let store = create_test_store();

    // Get state machine
    let sm = store.state_machine();

    // Verify it's accessible
    let _state = sm.read().await;
}

#[tokio::test]
async fn test_load_snapshot_from_store_no_snapshot() {
    let store = create_test_store();

    // Try to load non-existent snapshot
    let loaded = store.load_snapshot_from_store().await.unwrap();
    assert!(!loaded);
}

#[tokio::test]
async fn test_try_get_log_entries_empty() {
    let mut store = create_test_store();

    // Try to get entries from empty log
    let entries = store.try_get_log_entries(0..10).await.unwrap();
    assert!(entries.is_empty());
}

#[tokio::test]
async fn test_try_get_log_entries_range() {
    let mut store = create_test_store();

    // Add entries
    let entries = vec![
        make_entry(1, 0, 1, EntryPayload::Blank),
        make_entry(1, 0, 2, EntryPayload::Blank),
        make_entry(1, 0, 3, EntryPayload::Blank),
        make_entry(1, 0, 4, EntryPayload::Blank),
    ];
    store.append_to_log(entries).await.unwrap();

    // Get a subset of entries
    let subset = store.try_get_log_entries(2..4).await.unwrap();
    assert_eq!(subset.len(), 2);
    assert_eq!(subset[0].log_id.index, 2);
    assert_eq!(subset[1].log_id.index, 3);
}

#[tokio::test]
async fn test_install_snapshot() {
    let mut store = create_test_store();

    // Create a test state
    let state = ControlState {
        version: 5,
        ..Default::default()
    };
    let data = postcard::to_stdvec(&state).unwrap();
    let cursor = Box::new(Cursor::new(data));

    // Create snapshot metadata
    let meta = SnapshotMeta {
        last_log_id: Some(make_log_id(1, 0, 10)),
        last_membership: StoredMembership::default(),
        snapshot_id: "test-snapshot-1".to_string(),
    };

    // Install snapshot
    store.install_snapshot(&meta, cursor).await.unwrap();

    // Verify state was restored
    let sm = store.sm.read().await;
    let sm_state = sm.state().await;
    assert_eq!(sm_state.version, 5);

    // Verify last_applied_log is updated
    let last_applied = store.last_applied_log.read().await;
    assert!(last_applied.is_some());
    assert_eq!(last_applied.unwrap().index, 10);

    // Verify snapshot is cached
    let cached = store.cached_snapshot.read().await;
    assert!(cached.is_some());
    assert_eq!(cached.as_ref().unwrap().meta.snapshot_id, "test-snapshot-1");
}

#[tokio::test]
async fn test_build_snapshot() {
    let mut store = create_test_store();

    // Apply some commands to build state
    let command = ControlCommand::TopicRegistry(domains::TopicRegistryCommand::CreateTopic {
        name: "test-topic".to_string(),
        partitions: 3,
        config: std::collections::HashMap::new(),
        timestamp_ms: 1000,
    });

    let entries = vec![make_entry(1, 0, 1, EntryPayload::Normal(command))];
    store.apply_to_state_machine(&entries).await.unwrap();

    // Build snapshot
    use openraft::RaftSnapshotBuilder;
    let snapshot = store.build_snapshot().await.unwrap();

    // Verify snapshot metadata
    assert!(snapshot.meta.snapshot_id.starts_with("snapshot-"));

    // Verify snapshot is cached
    let cached = store.cached_snapshot.read().await;
    assert!(cached.is_some());
}

#[tokio::test]
async fn test_snapshot_persistence() {
    let object_store = Arc::new(InMemory::new());

    // Create store and build snapshot
    {
        let mut store =
            RaftStore::<ControlGroupKind>::new(object_store.clone(), "persistence-test");

        // Apply some state
        let command = ControlCommand::Broker(domains::BrokerCommand::Register {
            broker_id: 42,
            host: "test-host".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });
        let entries = vec![make_entry(1, 0, 1, EntryPayload::Normal(command))];
        store.apply_to_state_machine(&entries).await.unwrap();

        // Build snapshot (persists to object store)
        use openraft::RaftSnapshotBuilder;
        let _snapshot = store.build_snapshot().await.unwrap();
    }

    // Create new store and load snapshot
    let store2 = RaftStore::<ControlGroupKind>::new(object_store.clone(), "persistence-test");
    let loaded = store2.load_snapshot_from_store().await.unwrap();
    assert!(loaded);

    // Verify state was restored
    let sm = store2.sm.read().await;
    let state = sm.state().await;
    assert!(state.broker_domain.brokers.contains_key(&42));
    assert_eq!(state.broker_domain.brokers[&42].host, "test-host");
}

#[tokio::test]
async fn test_snapshot_roundtrip() {
    let mut store = create_test_store();

    // Build some state
    let commands = vec![
        ControlCommand::Broker(domains::BrokerCommand::Register {
            broker_id: 1,
            host: "broker1".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        }),
        ControlCommand::TopicRegistry(domains::TopicRegistryCommand::CreateTopic {
            name: "topic1".to_string(),
            partitions: 2,
            config: std::collections::HashMap::new(),
            timestamp_ms: 1000,
        }),
    ];

    for (i, cmd) in commands.into_iter().enumerate() {
        let entries = vec![make_entry(1, 0, (i + 1) as u64, EntryPayload::Normal(cmd))];
        store.apply_to_state_machine(&entries).await.unwrap();
    }

    // Build snapshot
    use openraft::RaftSnapshotBuilder;
    let snapshot = store.build_snapshot().await.unwrap();

    // Get current snapshot
    let retrieved = store.get_current_snapshot().await.unwrap();
    assert!(retrieved.is_some());
    let retrieved = retrieved.unwrap();

    // Verify metadata matches
    assert_eq!(retrieved.meta.snapshot_id, snapshot.meta.snapshot_id);
}

#[tokio::test]
async fn test_legacy_snapshot_layout_still_loads() {
    // Simulate a deployment that wrote snapshots with the old
    // rename-based scheme: legacy metadata at current.meta, data at
    // current.snapshot.
    let object_store = Arc::new(InMemory::new());
    let prefix = "legacy-test/snapshots";

    let state = ControlState {
        version: 7,
        ..Default::default()
    };
    let data = postcard::to_stdvec(&state).unwrap();
    let legacy_meta = SnapshotMetadata {
        last_log_id: Some(make_log_id(1, 0, 9)),
        last_membership: StoredMembership::default(),
        snapshot_id: "legacy-snap".to_string(),
    };
    let meta_bytes = postcard::to_stdvec(&legacy_meta).unwrap();

    object_store
        .put(
            &ObjectPath::from(format!("{}/current.snapshot", prefix)),
            Bytes::from(data).into(),
        )
        .await
        .unwrap();
    object_store
        .put(
            &ObjectPath::from(format!("{}/current.meta", prefix)),
            Bytes::from(meta_bytes).into(),
        )
        .await
        .unwrap();

    let store = RaftStore::<ControlGroupKind>::new(object_store.clone(), prefix);
    let loaded = store.load_snapshot_from_store().await.unwrap();
    assert!(loaded, "legacy snapshot layout should load");

    let sm = store.sm.read().await;
    assert_eq!(sm.state().await.version, 7);
    drop(sm);
    assert_eq!(store.last_applied_log.read().await.unwrap().index, 9);
}

#[tokio::test]
async fn test_snapshot_falls_back_to_previous_generation() {
    let object_store = Arc::new(InMemory::new());
    let prefix = "fallback-test/snapshots";

    // Build two snapshot generations with distinguishable state.
    let mut store = RaftStore::<ControlGroupKind>::new(object_store.clone(), prefix);
    let cmd1 = ControlCommand::Broker(domains::BrokerCommand::Register {
        broker_id: 1,
        host: "gen1-host".to_string(),
        port: 9092,
        timestamp_ms: 1000,
    });
    store
        .apply_to_state_machine(&[make_entry(1, 0, 1, EntryPayload::Normal(cmd1))])
        .await
        .unwrap();
    use openraft::RaftSnapshotBuilder;
    store.build_snapshot().await.unwrap();

    let cmd2 = ControlCommand::Broker(domains::BrokerCommand::Register {
        broker_id: 2,
        host: "gen2-host".to_string(),
        port: 9093,
        timestamp_ms: 2000,
    });
    store
        .apply_to_state_machine(&[make_entry(1, 0, 2, EntryPayload::Normal(cmd2))])
        .await
        .unwrap();
    store.build_snapshot().await.unwrap();

    // Sabotage: delete the CURRENT generation's data object out-of-band.
    let current_object = store
        .snapshot_pointer
        .read()
        .await
        .as_ref()
        .unwrap()
        .current
        .data_object
        .clone();
    object_store
        .delete(&ObjectPath::from(format!("{}/{}", prefix, current_object)))
        .await
        .unwrap();

    // A fresh store must fall back to the previous generation instead
    // of failing (and must not panic).
    let store2 = RaftStore::<ControlGroupKind>::new(object_store.clone(), prefix);
    let loaded = store2.load_snapshot_from_store().await.unwrap();
    assert!(loaded, "should fall back to the previous generation");

    let sm = store2.sm.read().await;
    let state = sm.state().await;
    assert!(
        state.broker_domain.brokers.contains_key(&1),
        "previous generation state should be restored"
    );
    assert!(
        !state.broker_domain.brokers.contains_key(&2),
        "current (lost) generation state should not appear"
    );
    drop(state);
    drop(sm);
    // Applied index regressed to the previous snapshot's — the log /
    // leader snapshot will catch the node up.
    assert_eq!(store2.last_applied_log.read().await.unwrap().index, 1);
}

#[tokio::test]
async fn test_corrupt_snapshot_meta_errors_without_panic() {
    let object_store = Arc::new(InMemory::new());
    let prefix = "corrupt-meta-test/snapshots";

    object_store
        .put(
            &ObjectPath::from(format!("{}/current.meta", prefix)),
            Bytes::from_static(b"\xff\xfe definitely not postcard").into(),
        )
        .await
        .unwrap();

    let store = RaftStore::<ControlGroupKind>::new(object_store.clone(), prefix);
    let result = store.load_snapshot_from_store().await;
    assert!(result.is_err(), "corrupt metadata must error, not panic");
}

#[tokio::test]
async fn test_install_snapshot_rejects_corrupt_bytes_without_mutating() {
    let mut store = create_test_store();

    // Seed some state.
    let cmd = ControlCommand::Broker(domains::BrokerCommand::Register {
        broker_id: 1,
        host: "host1".to_string(),
        port: 9092,
        timestamp_ms: 1000,
    });
    store
        .apply_to_state_machine(&[make_entry(1, 0, 1, EntryPayload::Normal(cmd))])
        .await
        .unwrap();

    let meta = SnapshotMeta {
        last_log_id: Some(make_log_id(1, 0, 10)),
        last_membership: StoredMembership::default(),
        snapshot_id: "corrupt-install".to_string(),
    };
    let garbage = Box::new(Cursor::new(b"\x00\x01garbage that is not a state".to_vec()));

    let result = store.install_snapshot(&meta, garbage).await;
    assert!(result.is_err(), "corrupt snapshot must be rejected");

    // In-memory state untouched.
    let sm = store.sm.read().await;
    assert!(sm.state().await.broker_domain.brokers.contains_key(&1));
    drop(sm);
    assert_eq!(
        store.last_applied_log.read().await.unwrap().index,
        1,
        "last_applied must not advance on a failed install"
    );
    assert!(store.cached_snapshot.read().await.is_none());
}

#[tokio::test]
async fn test_vote_persistence() {
    let mut store = create_test_store();

    // Save multiple votes (simulating term changes)
    let vote1 = Vote::new(1, 1);
    store.save_vote(&vote1).await.unwrap();

    let vote2 = Vote::new(2, 2);
    store.save_vote(&vote2).await.unwrap();

    // Read back - should be the latest vote
    let vote = store.read_vote().await.unwrap().unwrap();
    assert_eq!(vote.leader_id().voted_for(), Some(2));
}

#[tokio::test]
async fn test_membership_entry_application() {
    let mut store = create_test_store();

    // Create a membership entry
    let nodes = std::collections::BTreeMap::from([
        (1u64, BasicNode::new("127.0.0.1:9093")),
        (2u64, BasicNode::new("127.0.0.1:9094")),
    ]);
    let membership =
        openraft::Membership::new(vec![std::collections::BTreeSet::from([1, 2])], nodes);

    let entries = vec![make_entry(1, 0, 1, EntryPayload::Membership(membership))];

    // Apply membership entry
    let responses = store.apply_to_state_machine(&entries).await.unwrap();

    // Verify response
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0], ControlResponse::Ok);

    // Verify membership is updated
    let (_, stored_membership) = store.last_applied_state().await.unwrap();
    // Membership should not be empty after applying a membership entry
    let node_count = stored_membership.nodes().count();
    assert!(node_count > 0);
}

#[tokio::test]
async fn test_apply_multiple_commands() {
    let mut store = create_test_store();

    // Create multiple commands
    let entries = vec![
        make_entry(
            1,
            0,
            1,
            EntryPayload::Normal(ControlCommand::Broker(domains::BrokerCommand::Register {
                broker_id: 1,
                host: "host1".to_string(),
                port: 9092,
                timestamp_ms: 1000,
            })),
        ),
        make_entry(
            1,
            0,
            2,
            EntryPayload::Normal(ControlCommand::Broker(domains::BrokerCommand::Register {
                broker_id: 2,
                host: "host2".to_string(),
                port: 9093,
                timestamp_ms: 1000,
            })),
        ),
        make_entry(
            1,
            0,
            3,
            EntryPayload::Normal(ControlCommand::TopicRegistry(
                domains::TopicRegistryCommand::CreateTopic {
                    name: "topic1".to_string(),
                    partitions: 3,
                    config: std::collections::HashMap::new(),
                    timestamp_ms: 1000,
                },
            )),
        ),
    ];

    // Apply all entries
    let responses = store.apply_to_state_machine(&entries).await.unwrap();

    // Verify all responses
    assert_eq!(responses.len(), 3);

    // Verify state
    let sm = store.sm.read().await;
    let state = sm.state().await;
    assert_eq!(state.broker_domain.brokers.len(), 2);
    assert_eq!(state.topic_registry.topics.len(), 1);
}

#[tokio::test]
async fn test_noop_command() {
    let mut store = create_test_store();

    // Apply a Noop command
    let entries = vec![make_entry(
        1,
        0,
        1,
        EntryPayload::Normal(ControlCommand::Noop),
    )];

    let responses = store.apply_to_state_machine(&entries).await.unwrap();

    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0], ControlResponse::Ok);
}

#[tokio::test]
async fn test_log_indexing() {
    let mut store = create_test_store();

    // Add entries with non-contiguous indexes (simulating real-world scenario)
    let entries = vec![
        make_entry(1, 0, 5, EntryPayload::Blank),
        make_entry(1, 0, 6, EntryPayload::Blank),
        make_entry(1, 0, 7, EntryPayload::Blank),
    ];
    store.append_to_log(entries).await.unwrap();

    // Verify indexing
    let log = store.log.read().await;
    assert_eq!(log.len(), 3);
    assert!(log.contains_key(&5));
    assert!(log.contains_key(&6));
    assert!(log.contains_key(&7));
    assert!(!log.contains_key(&1));
}

// ============================================================================
// Append-only `last_applied` marker log.
//
// This is crash-recovery-critical: the marker is what stops a restart from
// re-applying non-idempotent commands (producer-id allocation, leader-epoch
// bumps). The append path trades the old tmp-write+rename for one
// `sync_data`, so these tests pin the properties that made the rename safe —
// last-record-wins, torn tails ignored, bounded growth, and legacy upgrade.
// ============================================================================

/// Round-trip: several appends, last one wins.
#[test]
fn marker_log_scan_returns_last_record() {
    let mut buf = Vec::new();
    for index in 1..=5u64 {
        let id = make_log_id(1, 1, index);
        buf.extend_from_slice(&encode_last_applied_record(
            &postcard::to_stdvec(&id).unwrap(),
        ));
    }
    let recovered = scan_last_applied_log(&buf).expect("a complete record");
    assert_eq!(recovered.index, 5);
}

#[test]
fn marker_log_scan_empty_is_none() {
    assert!(scan_last_applied_log(&[]).is_none());
}

/// A crash mid-append leaves a partial record. Recovery must ignore it and
/// return the last *complete* one rather than deserializing garbage.
#[test]
fn marker_log_scan_ignores_torn_tail() {
    let mut buf = Vec::new();
    for index in 1..=3u64 {
        let id = make_log_id(1, 1, index);
        buf.extend_from_slice(&encode_last_applied_record(
            &postcard::to_stdvec(&id).unwrap(),
        ));
    }
    let complete_len = buf.len();

    // Append a record and truncate it partway through the payload.
    let id = make_log_id(1, 1, 4);
    let full = encode_last_applied_record(&postcard::to_stdvec(&id).unwrap());
    buf.extend_from_slice(&full[..LAST_APPLIED_RECORD_HEADER + 1]);

    let recovered = scan_last_applied_log(&buf).expect("earlier records survive");
    assert_eq!(recovered.index, 3, "must not observe the torn record");

    // Truncating inside the *header* is also a torn tail.
    let mut header_torn = buf[..complete_len].to_vec();
    header_torn.extend_from_slice(&full[..3]);
    assert_eq!(
        scan_last_applied_log(&header_torn).map(|id| id.index),
        Some(3)
    );
}

/// A flipped payload byte must fail the CRC, not silently deserialize.
#[test]
fn marker_log_scan_rejects_corrupt_payload() {
    let id = make_log_id(7, 2, 99);
    let mut buf = encode_last_applied_record(&postcard::to_stdvec(&id).unwrap());
    let last = buf.len() - 1;
    buf[last] ^= 0xFF;
    assert!(
        scan_last_applied_log(&buf).is_none(),
        "CRC mismatch must be rejected"
    );
}

/// A bogus length field must not panic or over-read.
#[test]
fn marker_log_scan_rejects_absurd_length() {
    let mut buf = u32::MAX.to_le_bytes().to_vec();
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(b"short");
    assert!(scan_last_applied_log(&buf).is_none());
}

/// The append path is durable and readable, and compaction collapses the log
/// to a single record without losing the marker.
#[test]
fn marker_log_append_then_compact_preserves_latest() {
    let dir = tempfile::tempdir().unwrap();
    let writer = std::sync::Mutex::new(None);
    let path = dir.path().join(LAST_APPLIED_LOG_FILE);

    // First write compacts (counter starts at 0 in the real caller), so drive
    // it explicitly here to establish the file.
    let first = postcard::to_stdvec(&make_log_id(1, 1, 1)).unwrap();
    write_last_applied_blocking(dir.path(), &writer, &first, true).unwrap();
    assert_eq!(
        scan_last_applied_log(&std::fs::read(&path).unwrap())
            .unwrap()
            .index,
        1
    );

    // Appends accumulate; last wins.
    for index in 2..=10u64 {
        let payload = postcard::to_stdvec(&make_log_id(1, 1, index)).unwrap();
        write_last_applied_blocking(dir.path(), &writer, &payload, false).unwrap();
    }
    let grown = std::fs::read(&path).unwrap();
    assert_eq!(scan_last_applied_log(&grown).unwrap().index, 10);

    // Compaction keeps the newest marker and shrinks the file.
    let payload = postcard::to_stdvec(&make_log_id(1, 1, 11)).unwrap();
    write_last_applied_blocking(dir.path(), &writer, &payload, true).unwrap();
    let compacted = std::fs::read(&path).unwrap();
    assert_eq!(scan_last_applied_log(&compacted).unwrap().index, 11);
    assert!(
        compacted.len() < grown.len(),
        "compaction should shrink the log ({} -> {} bytes)",
        grown.len(),
        compacted.len()
    );

    // No temp file left behind.
    assert!(!dir.path().join("last_applied.log.tmp").exists());
}

/// Appending after a compaction must continue from the compacted record
/// rather than clobbering it (the cached handle is dropped by compaction).
#[test]
fn marker_log_append_after_compaction_continues() {
    let dir = tempfile::tempdir().unwrap();
    let writer = std::sync::Mutex::new(None);

    for (index, compact) in [(1u64, true), (2, false), (3, true), (4, false)] {
        let payload = postcard::to_stdvec(&make_log_id(1, 1, index)).unwrap();
        write_last_applied_blocking(dir.path(), &writer, &payload, compact).unwrap();
    }

    let bytes = std::fs::read(dir.path().join(LAST_APPLIED_LOG_FILE)).unwrap();
    assert_eq!(scan_last_applied_log(&bytes).unwrap().index, 4);
}

/// The temp name must not collide with the legacy `last_applied.bin` marker,
/// which `Path::with_extension("tmp")` would have mapped to the same path.
#[test]
fn marker_log_temp_name_does_not_collide_with_legacy() {
    let dir = tempfile::tempdir().unwrap();
    let legacy = dir.path().join(LAST_APPLIED_LEGACY_FILE);
    let legacy_bytes = postcard::to_stdvec(&make_log_id(9, 9, 42)).unwrap();
    std::fs::write(&legacy, &legacy_bytes).unwrap();

    let writer = std::sync::Mutex::new(None);
    let payload = postcard::to_stdvec(&make_log_id(1, 1, 1)).unwrap();
    write_last_applied_blocking(dir.path(), &writer, &payload, true).unwrap();

    // The legacy blob is untouched — a shared temp path would have eaten it.
    assert_eq!(std::fs::read(&legacy).unwrap(), legacy_bytes);
}

/// Upgrading in place: the legacy marker is ahead of (or the only) marker, so
/// recovery must adopt it instead of replaying from the snapshot point.
#[tokio::test]
async fn recover_prefers_further_along_marker_across_formats() {
    // Legacy only.
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join(LAST_APPLIED_LEGACY_FILE),
        postcard::to_stdvec(&make_log_id(3, 1, 77)).unwrap(),
    )
    .unwrap();
    let store = RaftStore::<ControlGroupKind>::new_with_log_dir(
        Arc::new(InMemory::new()),
        "marker-upgrade/snapshots",
        dir.path().to_path_buf(),
    );
    store.recover_from_disk().await.unwrap();
    assert_eq!(
        store.last_applied_log.read().await.unwrap().index,
        77,
        "legacy marker must be recovered on first boot after upgrade"
    );

    // Both present: the higher index wins regardless of which file it's in.
    let dir2 = tempfile::tempdir().unwrap();
    std::fs::write(
        dir2.path().join(LAST_APPLIED_LEGACY_FILE),
        postcard::to_stdvec(&make_log_id(3, 1, 77)).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir2.path().join(LAST_APPLIED_LOG_FILE),
        encode_last_applied_record(&postcard::to_stdvec(&make_log_id(3, 1, 120)).unwrap()),
    )
    .unwrap();
    let store2 = RaftStore::<ControlGroupKind>::new_with_log_dir(
        Arc::new(InMemory::new()),
        "marker-upgrade2/snapshots",
        dir2.path().to_path_buf(),
    );
    store2.recover_from_disk().await.unwrap();
    assert_eq!(store2.last_applied_log.read().await.unwrap().index, 120);

    // Legacy ahead of a stale marker log (crash right after upgrade wrote
    // the legacy file but before the new log caught up).
    let dir3 = tempfile::tempdir().unwrap();
    std::fs::write(
        dir3.path().join(LAST_APPLIED_LEGACY_FILE),
        postcard::to_stdvec(&make_log_id(3, 1, 200)).unwrap(),
    )
    .unwrap();
    std::fs::write(
        dir3.path().join(LAST_APPLIED_LOG_FILE),
        encode_last_applied_record(&postcard::to_stdvec(&make_log_id(3, 1, 5)).unwrap()),
    )
    .unwrap();
    let store3 = RaftStore::<ControlGroupKind>::new_with_log_dir(
        Arc::new(InMemory::new()),
        "marker-upgrade3/snapshots",
        dir3.path().to_path_buf(),
    );
    store3.recover_from_disk().await.unwrap();
    assert_eq!(store3.last_applied_log.read().await.unwrap().index, 200);
}

/// End-to-end through the real `RaftStore` API: applying entries persists the
/// marker, and a fresh store over the same directory recovers it.
#[tokio::test]
async fn apply_persists_marker_recoverable_by_fresh_store() {
    let dir = tempfile::tempdir().unwrap();
    let mut store = RaftStore::<ControlGroupKind>::new_with_log_dir(
        Arc::new(InMemory::new()),
        "marker-e2e/snapshots",
        dir.path().to_path_buf(),
    );

    let entries: Vec<_> = (1..=3u64)
        .map(|i| make_entry(1, 1, i, EntryPayload::Blank))
        .collect();
    store.apply_to_state_machine(&entries).await.unwrap();
    assert_eq!(store.last_applied_log.read().await.unwrap().index, 3);

    let recovered = RaftStore::<ControlGroupKind>::new_with_log_dir(
        Arc::new(InMemory::new()),
        "marker-e2e-2/snapshots",
        dir.path().to_path_buf(),
    );
    recovered.recover_from_disk().await.unwrap();
    assert_eq!(
        recovered.last_applied_log.read().await.unwrap().index,
        3,
        "marker must survive a restart so the entries are not re-applied"
    );
}
