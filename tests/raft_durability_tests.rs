//! Raft durability and lease-clock crash-recovery tests.
//!
//! Covers the High-priority audit findings around the Raft storage layer:
//!
//! 1. WAL recovery must never resurrect purged or conflict-truncated log
//!    entries, even when the (best-effort) file deletes were interrupted by
//!    a crash. Recovery is authoritative on the durable metadata
//!    (`purged.bin` purge floor, `log_meta.bin` log bound), not the file
//!    listing.
//! 2. `install_snapshot` persists durably BEFORE swapping the in-memory
//!    state machine, so a crash mid-install can never regress state.
//! 3. Corrupt snapshot bytes are an error, never a panic/abort.
//! 4. Lease lifecycle is driven by a replicated monotonic clock with a
//!    symmetric skew-tolerance grace window, and the `ExpireLeases` sweep
//!    is idempotent (so duplicate sweeps from non-leaders/deposed leaders
//!    are cheap no-ops).
//!
//! Run with:
//! ```bash
//! cargo test --test raft_durability_tests
//! ```

use std::collections::HashMap;
use std::io::Cursor;
use std::path::Path as FsPath;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use futures::stream::BoxStream;
use object_store::memory::InMemory;
use object_store::path::Path as ObjectPath;
use object_store::{
    GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta, ObjectStore,
    PutMultipartOptions, PutOptions, PutPayload, PutResult,
};
use openraft::{
    BasicNode, CommittedLeaderId, Entry, EntryPayload, LogId, RaftLogReader, RaftStorage,
    SnapshotMeta, StoredMembership,
};

use kafkaesque::cluster::raft::{
    CoordinationCommand, CoordinationResponse, CoordinationStateMachine, PartitionCommand,
    PartitionResponse, RaftStore, TypeConfig,
};

/// Mirrors `LEASE_TAKEOVER_GRACE_MS` in
/// `src/cluster/raft/domains/partition.rs` (a deterministic compile-time
/// constant, intentionally not exported as config).
const LEASE_TAKEOVER_GRACE_MS: u64 = 5_000;

fn make_log_id(term: u64, node: u64, index: u64) -> LogId<u64> {
    LogId::new(CommittedLeaderId::new(term, node), index)
}

fn blank_entry(term: u64, index: u64) -> Entry<TypeConfig> {
    Entry {
        log_id: make_log_id(term, 0, index),
        payload: EntryPayload::Blank,
    }
}

/// Save a copy of every `log/*.bin` file so tests can "un-delete" them
/// later, simulating a crash between durable metadata write and the
/// best-effort file deletes.
fn snapshot_log_files(log_dir: &FsPath) -> HashMap<String, Vec<u8>> {
    let mut saved = HashMap::new();
    let log_subdir = log_dir.join("log");
    for entry in std::fs::read_dir(&log_subdir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("bin") {
            saved.insert(
                entry.file_name().to_string_lossy().to_string(),
                std::fs::read(&path).unwrap(),
            );
        }
    }
    saved
}

/// Re-create previously saved log files that are now missing, simulating
/// file deletes that never happened before the crash.
fn restore_log_files(log_dir: &FsPath, saved: &HashMap<String, Vec<u8>>, names: &[&str]) {
    let log_subdir = log_dir.join("log");
    for name in names {
        let path = log_subdir.join(name);
        std::fs::write(&path, &saved[*name]).unwrap();
    }
}

fn log_file_name(index: u64) -> String {
    format!("{:020}.bin", index)
}

// ============================================================================
// Finding 1a: purge floor is authoritative on recovery
// ============================================================================

/// Crash-between-purge-and-delete: the purge floor is durable but some log
/// files below it survived. They must NOT resurrect on restart (openraft
/// invariant: entries at or below the purge floor never reappear).
#[tokio::test]
async fn purged_entries_do_not_resurrect_when_file_deletes_fail() {
    let tmp = tempfile::tempdir().unwrap();
    let log_dir = tmp.path().to_path_buf();
    let object_store = Arc::new(InMemory::new());

    {
        let mut store = RaftStore::new_with_log_dir(
            object_store.clone(),
            "durability/purge-test",
            log_dir.clone(),
        );
        store
            .append_to_log((1..=10).map(|i| blank_entry(1, i)))
            .await
            .unwrap();

        let saved = snapshot_log_files(&log_dir);

        store.purge_logs_upto(make_log_id(1, 0, 5)).await.unwrap();

        // Simulate the crash: the purge floor hit disk, but the file
        // deletes "failed" — bring the purged files back.
        restore_log_files(
            &log_dir,
            &saved,
            &[
                &log_file_name(1),
                &log_file_name(2),
                &log_file_name(3),
                &log_file_name(4),
                &log_file_name(5),
            ],
        );
    }

    // "Restart": fresh store over the same directory.
    let mut store = RaftStore::new_with_log_dir(
        object_store.clone(),
        "durability/purge-test",
        log_dir.clone(),
    );
    store.recover_from_disk().await.unwrap();

    let state = store.get_log_state().await.unwrap();
    assert_eq!(
        state.last_purged_log_id,
        Some(make_log_id(1, 0, 5)),
        "purge floor must survive restart"
    );
    assert_eq!(state.last_log_id, Some(make_log_id(1, 0, 10)));

    let entries = store.try_get_log_entries(0..100).await.unwrap();
    let indices: Vec<u64> = entries.iter().map(|e| e.log_id.index).collect();
    assert_eq!(
        indices,
        vec![6, 7, 8, 9, 10],
        "purged entries must not resurrect"
    );

    // Recovery also removed the stale files from disk.
    for i in 1..=5u64 {
        assert!(
            !log_dir.join("log").join(log_file_name(i)).exists(),
            "stale purged log file {} should be deleted at startup",
            i
        );
    }
}

// ============================================================================
// Finding 1b: conflict truncation is authoritative on recovery
// ============================================================================

/// Crash-between-truncation-and-delete: the lowered log bound is durable but
/// the forked entries' files survived. They must NOT resurrect.
#[tokio::test]
async fn conflict_truncated_entries_do_not_resurrect_when_file_deletes_fail() {
    let tmp = tempfile::tempdir().unwrap();
    let log_dir = tmp.path().to_path_buf();
    let object_store = Arc::new(InMemory::new());

    {
        let mut store = RaftStore::new_with_log_dir(
            object_store.clone(),
            "durability/truncate-test",
            log_dir.clone(),
        );
        store
            .append_to_log((1..=10).map(|i| blank_entry(1, i)))
            .await
            .unwrap();

        let saved = snapshot_log_files(&log_dir);

        // Conflict truncation removes entries >= 6.
        store
            .delete_conflict_logs_since(make_log_id(1, 0, 6))
            .await
            .unwrap();

        // Simulate the crash: the truncation metadata hit disk, but the
        // forked entries' files were never deleted.
        restore_log_files(
            &log_dir,
            &saved,
            &[
                &log_file_name(6),
                &log_file_name(7),
                &log_file_name(8),
                &log_file_name(9),
                &log_file_name(10),
            ],
        );
    }

    let mut store = RaftStore::new_with_log_dir(
        object_store.clone(),
        "durability/truncate-test",
        log_dir.clone(),
    );
    store.recover_from_disk().await.unwrap();

    let state = store.get_log_state().await.unwrap();
    assert_eq!(
        state.last_log_id,
        Some(make_log_id(1, 0, 5)),
        "truncated entries must not resurrect"
    );

    let entries = store.try_get_log_entries(0..100).await.unwrap();
    let indices: Vec<u64> = entries.iter().map(|e| e.log_id.index).collect();
    assert_eq!(indices, vec![1, 2, 3, 4, 5]);

    for i in 6..=10u64 {
        assert!(
            !log_dir.join("log").join(log_file_name(i)).exists(),
            "stale forked log file {} should be deleted at startup",
            i
        );
    }
}

/// Truncate-then-append: new-term entries written over the truncation point
/// recover, while stale forked files above the new tail do not.
#[tokio::test]
async fn truncate_then_append_recovers_only_new_entries() {
    let tmp = tempfile::tempdir().unwrap();
    let log_dir = tmp.path().to_path_buf();
    let object_store = Arc::new(InMemory::new());

    {
        let mut store = RaftStore::new_with_log_dir(
            object_store.clone(),
            "durability/truncate-append-test",
            log_dir.clone(),
        );
        store
            .append_to_log((1..=10).map(|i| blank_entry(1, i)))
            .await
            .unwrap();

        let saved = snapshot_log_files(&log_dir);

        store
            .delete_conflict_logs_since(make_log_id(1, 0, 6))
            .await
            .unwrap();

        // New leader (term 2) replicates replacement entries 6..=7.
        store
            .append_to_log((6..=7).map(|i| blank_entry(2, i)))
            .await
            .unwrap();

        // Stale term-1 files for 8..=10 "survived" the truncation.
        restore_log_files(
            &log_dir,
            &saved,
            &[&log_file_name(8), &log_file_name(9), &log_file_name(10)],
        );
    }

    let mut store = RaftStore::new_with_log_dir(
        object_store.clone(),
        "durability/truncate-append-test",
        log_dir.clone(),
    );
    store.recover_from_disk().await.unwrap();

    let entries = store.try_get_log_entries(0..100).await.unwrap();
    let ids: Vec<LogId<u64>> = entries.iter().map(|e| e.log_id).collect();
    assert_eq!(
        ids,
        vec![
            make_log_id(1, 0, 1),
            make_log_id(1, 0, 2),
            make_log_id(1, 0, 3),
            make_log_id(1, 0, 4),
            make_log_id(1, 0, 5),
            make_log_id(2, 0, 6),
            make_log_id(2, 0, 7),
        ],
        "replacement entries recover with the new term; forked tail does not"
    );
}

/// A legacy log directory (no `log_meta.bin`) still recovers exactly as
/// before, and the new metadata is written on the next append.
#[tokio::test]
async fn legacy_log_dir_without_log_meta_still_recovers() {
    let tmp = tempfile::tempdir().unwrap();
    let log_dir = tmp.path().to_path_buf();
    let object_store = Arc::new(InMemory::new());

    {
        let mut store = RaftStore::new_with_log_dir(
            object_store.clone(),
            "durability/legacy-test",
            log_dir.clone(),
        );
        store
            .append_to_log((1..=5).map(|i| blank_entry(1, i)))
            .await
            .unwrap();
    }

    // Simulate a directory written by the old code: no log bound metadata.
    let log_meta = log_dir.join("log_meta.bin");
    assert!(log_meta.exists());
    std::fs::remove_file(&log_meta).unwrap();

    let mut store = RaftStore::new_with_log_dir(
        object_store.clone(),
        "durability/legacy-test",
        log_dir.clone(),
    );
    store.recover_from_disk().await.unwrap();

    let entries = store.try_get_log_entries(0..100).await.unwrap();
    assert_eq!(entries.len(), 5, "legacy layout must load all entries");

    // The next append re-creates the metadata going forward.
    store.append_to_log([blank_entry(1, 6)]).await.unwrap();
    assert!(
        log_meta.exists(),
        "log_meta.bin must be written going forward"
    );
}

// ============================================================================
// Finding 4: corrupt snapshot bytes must never panic
// ============================================================================

/// Garbage at the snapshot pointer location: storage opens with an error,
/// never a panic.
#[tokio::test]
async fn corrupt_snapshot_pointer_errors_without_panic() {
    let object_store = Arc::new(InMemory::new());
    let prefix = "durability/corrupt-pointer";

    object_store
        .put(
            &ObjectPath::from(format!("{}/current.meta", prefix)),
            PutPayload::from_static(b"\xde\xad\xbe\xef not a snapshot pointer"),
        )
        .await
        .unwrap();

    let store = RaftStore::new(object_store.clone(), prefix);
    let result = store.load_snapshot_from_store().await;
    assert!(
        result.is_err(),
        "corrupt snapshot metadata must produce an error, not a panic"
    );
}

/// Corrupt snapshot DATA (valid pointer, garbage payload): storage opens
/// with an error (no previous generation to fall back to), never a panic.
#[tokio::test]
async fn corrupt_snapshot_data_errors_without_panic() {
    use openraft::RaftSnapshotBuilder;

    let object_store = Arc::new(InMemory::new());
    let prefix = "durability/corrupt-data";

    // Build one valid snapshot so a pointer exists.
    {
        let mut store = RaftStore::new(object_store.clone(), prefix);
        store.build_snapshot().await.unwrap();
    }

    // Overwrite every generation data object with garbage.
    use futures::TryStreamExt;
    let objects: Vec<ObjectMeta> = object_store
        .list(Some(&ObjectPath::from(prefix)))
        .try_collect()
        .await
        .unwrap();
    let mut corrupted = 0;
    for meta in objects {
        if meta
            .location
            .filename()
            .is_some_and(|n| n.starts_with("gen-") && n.ends_with(".snapshot"))
        {
            object_store
                .put(
                    &meta.location,
                    PutPayload::from_static(b"\x00\x01\x02 corrupt state bytes"),
                )
                .await
                .unwrap();
            corrupted += 1;
        }
    }
    assert!(corrupted > 0, "expected at least one generation object");

    let store = RaftStore::new(object_store.clone(), prefix);
    let result = store.load_snapshot_from_store().await;
    assert!(
        result.is_err(),
        "corrupt snapshot data must produce an error, not a panic"
    );
}

/// The state machine's restore path validates before mutating and returns
/// an error on corrupt bytes instead of panicking.
#[tokio::test]
async fn state_machine_try_restore_rejects_corrupt_bytes() {
    let sm = CoordinationStateMachine::new();
    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::CreateTopic {
            name: "keep-me".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1_000,
        },
    ))
    .await;
    let version_before = sm.state().await.version;

    let result = sm.try_restore(b"\xff\xff definitely not a snapshot").await;
    assert!(result.is_err(), "corrupt bytes must be rejected");

    // State machine untouched.
    let state = sm.state().await;
    assert_eq!(state.version, version_before);
    assert!(state.partition_domain.topics.contains_key("keep-me"));
    drop(state);

    // The non-panicking legacy entry point also leaves state unchanged.
    sm.restore(b"\xff\xff still not a snapshot").await;
    assert!(
        sm.state()
            .await
            .partition_domain
            .topics
            .contains_key("keep-me"),
        "restore() must be a no-op on corrupt bytes"
    );

    // And a valid snapshot still round-trips.
    let bytes = sm.snapshot().await;
    let sm2 = CoordinationStateMachine::new();
    sm2.try_restore(&bytes).await.unwrap();
    assert!(
        sm2.state()
            .await
            .partition_domain
            .topics
            .contains_key("keep-me")
    );
}

// ============================================================================
// Finding 2: install_snapshot persists before swapping
// ============================================================================

/// An object store wrapper whose writes can be made to fail, simulating an
/// unreachable/failing store at the moment a snapshot must be persisted.
#[derive(Debug)]
struct FailingPutStore {
    inner: InMemory,
    fail_puts: AtomicBool,
}

impl FailingPutStore {
    fn new() -> Self {
        Self {
            inner: InMemory::new(),
            fail_puts: AtomicBool::new(false),
        }
    }

    fn set_fail_puts(&self, fail: bool) {
        self.fail_puts.store(fail, Ordering::SeqCst);
    }

    fn injected_error() -> object_store::Error {
        object_store::Error::Generic {
            store: "FailingPutStore",
            source: "injected put failure".into(),
        }
    }
}

impl std::fmt::Display for FailingPutStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "FailingPutStore({})", self.inner)
    }
}

#[async_trait::async_trait]
impl ObjectStore for FailingPutStore {
    async fn put_opts(
        &self,
        location: &ObjectPath,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        if self.fail_puts.load(Ordering::SeqCst) {
            return Err(Self::injected_error());
        }
        self.inner.put_opts(location, payload, opts).await
    }

    async fn put_multipart_opts(
        &self,
        location: &ObjectPath,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        if self.fail_puts.load(Ordering::SeqCst) {
            return Err(Self::injected_error());
        }
        self.inner.put_multipart_opts(location, opts).await
    }

    async fn get_opts(
        &self,
        location: &ObjectPath,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.inner.get_opts(location, options).await
    }

    async fn delete(&self, location: &ObjectPath) -> object_store::Result<()> {
        self.inner.delete(location).await
    }

    fn list(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        self.inner.list(prefix)
    }

    async fn list_with_delimiter(
        &self,
        prefix: Option<&ObjectPath>,
    ) -> object_store::Result<ListResult> {
        self.inner.list_with_delimiter(prefix).await
    }

    async fn copy(&self, from: &ObjectPath, to: &ObjectPath) -> object_store::Result<()> {
        self.inner.copy(from, to).await
    }

    async fn copy_if_not_exists(
        &self,
        from: &ObjectPath,
        to: &ObjectPath,
    ) -> object_store::Result<()> {
        self.inner.copy_if_not_exists(from, to).await
    }
}

/// If persisting the incoming snapshot fails, the in-memory state machine
/// and applied indices must be left untouched (persist-then-swap ordering).
/// A crash at that point therefore resumes from the previous state instead
/// of regressing.
#[tokio::test]
async fn install_snapshot_does_not_swap_state_when_persist_fails() {
    let failing_store = Arc::new(FailingPutStore::new());
    let mut store = RaftStore::new(failing_store.clone(), "durability/install-test");

    // Seed in-memory state via the normal apply path.
    let seed = Entry {
        log_id: make_log_id(1, 0, 1),
        payload: EntryPayload::Normal(CoordinationCommand::PartitionDomain(
            PartitionCommand::CreateTopic {
                name: "original".to_string(),
                partitions: 1,
                config: HashMap::new(),
                timestamp_ms: 1_000,
            },
        )),
    };
    store.apply_to_state_machine(&[seed]).await.unwrap();

    // Build valid snapshot bytes representing DIFFERENT state.
    let other_sm = CoordinationStateMachine::new();
    other_sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::CreateTopic {
                name: "incoming".to_string(),
                partitions: 1,
                config: HashMap::new(),
                timestamp_ms: 2_000,
            },
        ))
        .await;
    let incoming_bytes = other_sm.snapshot().await;

    let meta = SnapshotMeta::<u64, BasicNode> {
        last_log_id: Some(make_log_id(2, 0, 50)),
        last_membership: StoredMembership::default(),
        snapshot_id: "install-ordering".to_string(),
    };

    // Persist fails -> install must fail and leave everything untouched.
    failing_store.set_fail_puts(true);
    let result = store
        .install_snapshot(&meta, Box::new(Cursor::new(incoming_bytes.clone())))
        .await;
    assert!(result.is_err(), "install must fail when persist fails");

    {
        let sm = store.state_machine();
        let sm = sm.read().await;
        let state = sm.state().await;
        assert!(
            state.partition_domain.topics.contains_key("original"),
            "in-memory state must be untouched after failed install"
        );
        assert!(
            !state.partition_domain.topics.contains_key("incoming"),
            "incoming snapshot must not be applied when persist failed"
        );
    }
    let (last_applied, _) = store.last_applied_state().await.unwrap();
    assert_eq!(
        last_applied,
        Some(make_log_id(1, 0, 1)),
        "applied index must not advance on failed install"
    );

    // Once the store is healthy, the same install succeeds, persists, and
    // only then swaps.
    failing_store.set_fail_puts(false);
    store
        .install_snapshot(&meta, Box::new(Cursor::new(incoming_bytes)))
        .await
        .unwrap();

    {
        let sm = store.state_machine();
        let sm = sm.read().await;
        let state = sm.state().await;
        assert!(state.partition_domain.topics.contains_key("incoming"));
        assert!(!state.partition_domain.topics.contains_key("original"));
    }
    let (last_applied, _) = store.last_applied_state().await.unwrap();
    assert_eq!(last_applied, Some(make_log_id(2, 0, 50)));

    // And the durable copy is loadable by a fresh store (i.e. it really was
    // persisted, not just swapped in memory).
    let store2 = RaftStore::new(failing_store.clone(), "durability/install-test");
    let loaded = store2.load_snapshot_from_store().await.unwrap();
    assert!(loaded);
    let sm = store2.state_machine();
    let sm = sm.read().await;
    assert!(
        sm.state()
            .await
            .partition_domain
            .topics
            .contains_key("incoming")
    );
}

// ============================================================================
// Finding 5: lease clock + idempotent sweep (state-machine level)
// ============================================================================

/// Duplicate `ExpireLeases` sweeps (as proposed by multiple brokers or a
/// just-deposed leader) are cheap no-ops: expire-by-deadline is idempotent.
#[tokio::test]
async fn expire_leases_sweep_is_idempotent() {
    let sm = CoordinationStateMachine::new();

    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1_000,
            timestamp_ms: 100_000,
        },
    ))
    .await;

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::ExpireLeases {
                current_time_ms: 200_000,
            },
        ))
        .await;
    assert_eq!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeasesExpired {
            count: 1
        })
    );

    // Duplicate sweeps — same timestamp, earlier timestamp (backdated
    // proposer), and again the same — all no-ops.
    for ts in [200_000, 150_000, 200_000] {
        let response = sm
            .apply_command(CoordinationCommand::PartitionDomain(
                PartitionCommand::ExpireLeases {
                    current_time_ms: ts,
                },
            ))
            .await;
        assert_eq!(
            response,
            CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeasesExpired {
                count: 0
            }),
            "duplicate sweep at ts={} must be a no-op",
            ts
        );
    }
}

/// The replicated lease clock is monotonic: backdated proposals can neither
/// shrink leases nor mass-expire them, and takeover/renewal honor the
/// symmetric skew-tolerance grace window.
#[tokio::test]
async fn lease_clock_is_monotonic_and_grace_window_applies() {
    let sm = CoordinationStateMachine::new();

    // Broker 1 acquires at t=100_000 for 10s -> expiry 110_000.
    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "t".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 10_000,
                timestamp_ms: 100_000,
            },
        ))
        .await;
    let CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired {
        lease_expires_at_ms,
        ..
    }) = response
    else {
        panic!("expected PartitionAcquired, got {:?}", response);
    };
    assert_eq!(lease_expires_at_ms, 110_000);

    // Backdated renewal (proposer wall clock jumped backward): clamped to
    // the lease clock, so the expiry does NOT regress to 50k + 10k.
    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::RenewLease {
                topic: "t".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 10_000,
                timestamp_ms: 50_000,
            },
        ))
        .await;
    let CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeaseRenewed {
        lease_expires_at_ms,
        ..
    }) = response
    else {
        panic!("expected LeaseRenewed, got {:?}", response);
    };
    assert_eq!(
        lease_expires_at_ms, 110_000,
        "backdated renewal must not move the lease backward"
    );

    // Backdated sweep: clamped, expires nothing.
    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::ExpireLeases {
                current_time_ms: 1_000,
            },
        ))
        .await;
    assert_eq!(
        response,
        CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeasesExpired {
            count: 0
        }),
        "a backdated sweep must not expire live leases"
    );

    // Takeover inside the grace window (forward-skewed acquirer) is
    // refused; at expiry + grace it succeeds with an epoch bump.
    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "t".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 10_000,
                timestamp_ms: 110_000 + LEASE_TAKEOVER_GRACE_MS - 1,
            },
        ))
        .await;
    assert!(
        matches!(
            response,
            CoordinationResponse::PartitionDomainResponse(
                PartitionResponse::PartitionOwnedByOther { owner: 1, .. }
            )
        ),
        "takeover within the grace window must be refused, got {:?}",
        response
    );

    let response = sm
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::AcquirePartition {
                topic: "t".to_string(),
                partition: 0,
                broker_id: 2,
                lease_duration_ms: 10_000,
                timestamp_ms: 110_000 + LEASE_TAKEOVER_GRACE_MS,
            },
        ))
        .await;
    let CoordinationResponse::PartitionDomainResponse(PartitionResponse::PartitionAcquired {
        leader_epoch,
        ..
    }) = response
    else {
        panic!("expected PartitionAcquired, got {:?}", response);
    };
    assert_eq!(leader_epoch, 2, "takeover must bump the leader epoch");
}

/// The lease clock survives snapshot round-trips, so monotonicity holds
/// across restore.
#[tokio::test]
async fn lease_clock_survives_snapshot_roundtrip() {
    let sm = CoordinationStateMachine::new();
    sm.apply_command(CoordinationCommand::PartitionDomain(
        PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 10_000,
            timestamp_ms: 500_000,
        },
    ))
    .await;
    assert_eq!(sm.state().await.lease_clock_ms, 500_000);

    let bytes = sm.snapshot().await;
    let sm2 = CoordinationStateMachine::new();
    sm2.try_restore(&bytes).await.unwrap();
    assert_eq!(
        sm2.state().await.lease_clock_ms,
        500_000,
        "lease clock must be part of the replicated snapshot state"
    );

    // A backdated renewal after restore is still clamped.
    let response = sm2
        .apply_command(CoordinationCommand::PartitionDomain(
            PartitionCommand::RenewLease {
                topic: "t".to_string(),
                partition: 0,
                broker_id: 1,
                lease_duration_ms: 10_000,
                timestamp_ms: 1_000,
            },
        ))
        .await;
    let CoordinationResponse::PartitionDomainResponse(PartitionResponse::LeaseRenewed {
        lease_expires_at_ms,
        ..
    }) = response
    else {
        panic!("expected LeaseRenewed, got {:?}", response);
    };
    assert_eq!(lease_expires_at_ms, 510_000);
}
