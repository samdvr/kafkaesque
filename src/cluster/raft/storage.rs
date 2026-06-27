//! Raft storage implementation using openraft's RaftStorage trait (v1 API).
//!
//! This module provides combined log and state machine storage for Raft.
//! Snapshots are persisted to the object store for production durability.
//!
//! Generic over [`GroupKind`] so the same implementation backs the
//! control group and every shard group.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftStorage, Snapshot, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use super::group::GroupKind;
use super::types::RaftNodeId;

/// Shorthand for a group's response type — the openraft `R` associated
/// type on its `TypeConfig`. Used by trait-impl signatures so
/// `Vec<<G::Cfg as RaftTypeConfig>::R>` doesn't drown the rest of the
/// signature.
type GroupResponse<G> = <<G as GroupKind>::Cfg as openraft::RaftTypeConfig>::R;

/// Legacy snapshot metadata layout (pre-pointer scheme), stored alongside a
/// data object at `current.snapshot`. Still understood on read for
/// backward compatibility; never written anymore.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotMetadata {
    /// The last log ID included in this snapshot.
    last_log_id: Option<LogId<RaftNodeId>>,
    /// The membership configuration at the time of the snapshot.
    last_membership: StoredMembership<RaftNodeId, BasicNode>,
    /// Unique identifier for this snapshot.
    snapshot_id: String,
}

/// One snapshot generation referenced by the pointer object.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotPointerEntry {
    /// The last log ID included in this snapshot.
    last_log_id: Option<LogId<RaftNodeId>>,
    /// The membership configuration at the time of the snapshot.
    last_membership: StoredMembership<RaftNodeId, BasicNode>,
    /// Unique identifier for this snapshot.
    snapshot_id: String,
    /// Name of the immutable data object (relative to the snapshot prefix)
    /// holding this generation's serialized state machine.
    data_object: String,
}

/// The snapshot pointer object stored at `current.meta`.
///
/// # Why a pointer scheme instead of rename?
///
/// `object_store::rename` on S3 (and the copy+delete fallback) is NOT
/// atomic: a crash mid-copy could leave metadata pointing at missing or
/// mismatched data. Here every snapshot's data is written to a fresh,
/// immutable, uniquely-named generation object FIRST; only then is this
/// single small pointer object overwritten (a single PUT, which is atomic
/// on S3/GCS/Azure and the local filesystem implementation). A reader
/// therefore always observes a pointer whose `current.data_object` was
/// fully written before the pointer itself.
///
/// The previous generation is retained until the new pointer is durably
/// written (it is referenced by `previous` and only the generation *before
/// that* is deleted), so even if the current data object is lost or
/// corrupted out-of-band, readers can fall back one generation instead of
/// failing.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotPointer {
    /// The committed snapshot.
    current: SnapshotPointerEntry,
    /// The immediately preceding snapshot generation, kept as a fallback.
    previous: Option<SnapshotPointerEntry>,
}

/// Durable bounds for the on-disk Raft log, stored at `log_meta.bin` in the
/// log directory and written with atomic-rename + fsync.
///
/// `recover_from_disk` treats this file as authoritative: any `log/*.bin`
/// file with an index above `last_log_index` is a leftover from a
/// conflict-truncation whose file deletes did not complete before a crash,
/// and is skipped (and removed). A missing file means the legacy layout —
/// recovery behaves as before, and the file is written on the next append
/// or truncation.
#[derive(Clone, Copy, serde::Serialize, serde::Deserialize)]
struct LogMeta {
    /// Highest valid log index, or `None` when the log is empty.
    last_log_index: Option<u64>,
}

/// Persisted snapshot data (cached in memory).
#[derive(Clone)]
struct CachedSnapshot {
    /// Snapshot metadata.
    meta: SnapshotMeta<RaftNodeId, BasicNode>,
    /// Serialized snapshot data.
    data: Vec<u8>,
}

/// Combined log and state machine storage for Raft.
pub struct RaftStore<G: GroupKind> {
    /// Vote state (who we voted for in current term).
    vote: Arc<RwLock<Option<Vote<RaftNodeId>>>>,
    /// Log entries indexed by log index.
    log: Arc<RwLock<BTreeMap<u64, Entry<G::Cfg>>>>,
    /// Last purged log ID.
    last_purged_log_id: Arc<RwLock<Option<LogId<RaftNodeId>>>>,
    /// State machine.
    sm: Arc<RwLock<G::Sm>>,
    /// Last applied log.
    last_applied_log: Arc<RwLock<Option<LogId<RaftNodeId>>>>,
    /// Current membership.
    last_membership: Arc<RwLock<StoredMembership<RaftNodeId, BasicNode>>>,
    /// Cached snapshot (in-memory cache of what's in object store).
    cached_snapshot: Arc<RwLock<Option<CachedSnapshot>>>,
    /// In-memory copy of the last durably written snapshot pointer.
    /// Written under this lock so concurrent persists serialize on the
    /// commit-point PUT.
    snapshot_pointer: Arc<RwLock<Option<SnapshotPointer>>>,
    /// Object store for durable snapshot persistence.
    object_store: Arc<dyn ObjectStore>,
    /// Path prefix for snapshots in the object store.
    snapshot_path: ObjectPath,
    /// Optional on-disk persistence root for vote and log entries.
    ///
    /// When `Some`, every `save_vote`, `append_to_log`, `purge_logs_upto`, and
    /// `delete_conflict_logs_since` writes through to this directory and
    /// fsyncs before returning, so a crash followed by restart resumes with
    /// the same vote and log prefix. When `None`, the store is
    /// in-memory only — used by unit tests that don't need durability.
    log_dir: Option<PathBuf>,
    /// Coarse mutex serializing `apply_state_transition` calls end-to-end,
    /// including the `last_applied` fsync that runs after the FSM mutation
    /// guards drop. Without this, two concurrent applies could interleave
    /// `(mutate, drop guards, fsync)` so the older entry's fsync wins and
    /// recovery replays both — re-allocating producer ids and bumping
    /// epochs twice. The contention added by this mutex is low: every
    /// `apply` is gated on a small fsync, and openraft serializes
    /// `apply_to_state_machine` callbacks through its own pipeline.
    apply_lock: Arc<tokio::sync::Mutex<()>>,
    /// Single-flight mutex around the full `persist_snapshot` flow: data
    /// write, pointer commit, mirror refresh, and best-effort delete of
    /// the generation that fell off retention. Without this, two persisters
    /// could overlap and one's step-4 delete could race the other's
    /// in-flight commit, dropping a generation the new pointer still
    /// references and leaving the next reader with a dangling pointer.
    /// Snapshot persistence is rare (driven by openraft's snapshot
    /// scheduler), so the serialization cost is negligible.
    snapshot_persist_lock: Arc<tokio::sync::Mutex<()>>,
}

/// Atomic update to one or more `RaftStore` fields.
///
/// Each `with_*` call marks one field for update; un-touched fields
/// stay un-set and are not modified by the corresponding
/// `apply_state_transition` call. This is the single helper every
/// multi-field FSM/index/snapshot mutation goes through — see
/// `RaftStore::apply_state_transition` for cancellation semantics and
/// lock ordering.
#[must_use = "build with `.with_*` and call `store.apply_state_transition(t).await`"]
struct StateTransition<G: GroupKind> {
    sm_state: Option<G::State>,
    /// `Some(value)` if `last_applied_log` is being set to `value`.
    /// (Inner `Option` is the field type itself — `None` means "no
    /// logs applied yet", which is a meaningful state to install.)
    last_applied: Option<Option<LogId<RaftNodeId>>>,
    last_membership: Option<StoredMembership<RaftNodeId, BasicNode>>,
    /// `Some(value)` to set the cache to `value` (where `value` itself
    /// may be `None` to clear the cache).
    cached_snapshot: Option<Option<CachedSnapshot>>,
    snapshot_pointer: Option<Option<SnapshotPointer>>,
}

impl<G: GroupKind> Default for StateTransition<G> {
    fn default() -> Self {
        Self {
            sm_state: None,
            last_applied: None,
            last_membership: None,
            cached_snapshot: None,
            snapshot_pointer: None,
        }
    }
}

impl<G: GroupKind> StateTransition<G> {
    fn new() -> Self {
        Self::default()
    }

    fn with_sm_state(mut self, state: G::State) -> Self {
        self.sm_state = Some(state);
        self
    }

    fn with_last_applied(mut self, last_applied: Option<LogId<RaftNodeId>>) -> Self {
        self.last_applied = Some(last_applied);
        self
    }

    fn with_last_membership(mut self, m: StoredMembership<RaftNodeId, BasicNode>) -> Self {
        self.last_membership = Some(m);
        self
    }

    fn with_cached_snapshot(mut self, cached: Option<CachedSnapshot>) -> Self {
        self.cached_snapshot = Some(cached);
        self
    }

    fn with_snapshot_pointer(mut self, ptr: Option<SnapshotPointer>) -> Self {
        self.snapshot_pointer = Some(ptr);
        self
    }
}

impl<G: GroupKind> RaftStore<G> {
    /// Create a new store with object store backing for snapshots.
    ///
    /// In-memory variant — vote and log entries live only in RAM. Used by
    /// unit tests; production code should use `with_init_and_log_dir`.
    /// `init` is the `G::SmInit` (e.g. `()` for control, `ShardId` for
    /// shard) used to construct the per-group state machine.
    pub fn with_init(
        object_store: Arc<dyn ObjectStore>,
        snapshot_prefix: &str,
        init: G::SmInit,
    ) -> Self {
        Self {
            vote: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            last_purged_log_id: Arc::new(RwLock::new(None)),
            sm: Arc::new(RwLock::new(G::new_sm(init))),
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            cached_snapshot: Arc::new(RwLock::new(None)),
            snapshot_pointer: Arc::new(RwLock::new(None)),
            object_store,
            snapshot_path: ObjectPath::from(snapshot_prefix),
            log_dir: None,
            apply_lock: Arc::new(tokio::sync::Mutex::new(())),
            snapshot_persist_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }

    /// Create a new store backed by an on-disk WAL for vote and log entries.
    /// The WAL is created on first write; `recover_from_disk()`
    /// must be called before `RaftNode::new` proceeds in order to repopulate
    /// in-memory state from any existing WAL files.
    pub fn with_init_and_log_dir(
        object_store: Arc<dyn ObjectStore>,
        snapshot_prefix: &str,
        log_dir: PathBuf,
        init: G::SmInit,
    ) -> Self {
        Self {
            vote: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            last_purged_log_id: Arc::new(RwLock::new(None)),
            sm: Arc::new(RwLock::new(G::new_sm(init))),
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            cached_snapshot: Arc::new(RwLock::new(None)),
            snapshot_pointer: Arc::new(RwLock::new(None)),
            object_store,
            snapshot_path: ObjectPath::from(snapshot_prefix),
            log_dir: Some(log_dir),
            apply_lock: Arc::new(tokio::sync::Mutex::new(())),
            snapshot_persist_lock: Arc::new(tokio::sync::Mutex::new(())),
        }
    }
}

/// Convenience constructors for groups whose `SmInit` is `()` (legacy and
/// control). `RaftStore::new(...)` continues to work for these without
/// having to thread an explicit init value through every call site.
impl<G: GroupKind<SmInit = ()>> RaftStore<G> {
    /// In-memory variant — see [`Self::with_init`].
    pub fn new(object_store: Arc<dyn ObjectStore>, snapshot_prefix: &str) -> Self {
        Self::with_init(object_store, snapshot_prefix, ())
    }

    /// Disk-backed variant — see [`Self::with_init_and_log_dir`].
    pub fn new_with_log_dir(
        object_store: Arc<dyn ObjectStore>,
        snapshot_prefix: &str,
        log_dir: PathBuf,
    ) -> Self {
        Self::with_init_and_log_dir(object_store, snapshot_prefix, log_dir, ())
    }
}

impl<G: GroupKind> RaftStore<G> {
    /// Replay the on-disk WAL into in-memory state. Called once at startup
    /// before openraft is constructed. On a fresh node with no
    /// WAL files this is a no-op; on a restart it loads the durable vote,
    /// every persisted log entry, and the purged-up-to marker so openraft
    /// resumes with the same state it had at crash time.
    ///
    /// # Stale-file filtering (crash safety)
    ///
    /// File deletion during `purge_logs_upto` and
    /// `delete_conflict_logs_since` is best-effort, so a crash between the
    /// (durable, ordered-first) metadata write and the file deletes can
    /// leave stale `log/*.bin` files behind. Recovery is authoritative on
    /// the metadata, never the file listing:
    ///
    /// - entries with `index <= purged.bin` are skipped: purged entries can
    ///   never resurrect (openraft invariant: nothing below the purge floor
    ///   may reappear);
    /// - entries with `index > log_meta.last_log_index` are skipped:
    ///   conflict-truncated (forked) entries can never resurrect.
    ///
    /// Stale files are deleted (best-effort) during the scan. A missing
    /// `log_meta.bin` means the legacy on-disk layout: no upper-bound
    /// filter is applied (matching old behavior) and the file is created
    /// by the next append or truncation.
    ///
    /// Failures here are fatal — see `RaftNode::new` for the fail-closed
    /// wrapping. A corrupt WAL is treated the same way a corrupt snapshot
    /// is: better to crash loudly than to silently lose state.
    pub async fn recover_from_disk(&self) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        if !dir.exists() {
            return Ok(());
        }

        // Vote.
        let vote_path = dir.join("vote.bin");
        if vote_path.exists() {
            let bytes = std::fs::read(&vote_path)?;
            let vote: Vote<RaftNodeId> = postcard::from_bytes(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("vote.bin deserialize: {}", e),
                )
            })?;
            *self.vote.write().await = Some(vote);
        }

        // Purged marker — the durable purge floor. Persisted BEFORE log
        // files are deleted, so it is always >= the highest purged index
        // whose file delete may have failed.
        let purged_path = dir.join("purged.bin");
        let mut purged_index: Option<u64> = None;
        if purged_path.exists() {
            let bytes = std::fs::read(&purged_path)?;
            let purged: LogId<RaftNodeId> = postcard::from_bytes(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("purged.bin deserialize: {}", e),
                )
            })?;
            purged_index = Some(purged.index);
            *self.last_purged_log_id.write().await = Some(purged);
        }

        // Last-applied marker. Persisted after every FSM apply; on a
        // restart the snapshot brings the FSM forward to its
        // `last_applied_log`, then openraft replays log entries from
        // there. Without recovering this marker openraft would replay
        // every entry above the snapshot's last_applied (the snapshot
        // can lag the actual apply point by hundreds or thousands of
        // entries) — re-running non-idempotent FSM commands.
        let last_applied_path = dir.join("last_applied.bin");
        if last_applied_path.exists() {
            let bytes = std::fs::read(&last_applied_path)?;
            let applied: LogId<RaftNodeId> = postcard::from_bytes(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("last_applied.bin deserialize: {}", e),
                )
            })?;
            *self.last_applied_log.write().await = Some(applied);
        }

        // Log bound metadata — the durable truncation point. Persisted
        // BEFORE conflict-truncation file deletes. `None` = legacy layout
        // (no upper-bound filter); `Some(meta)` = authoritative bound.
        let log_meta_path = dir.join("log_meta.bin");
        let log_bound: Option<LogMeta> = if log_meta_path.exists() {
            let bytes = std::fs::read(&log_meta_path)?;
            let meta: LogMeta = postcard::from_bytes(&bytes).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("log_meta.bin deserialize: {}", e),
                )
            })?;
            Some(meta)
        } else {
            None
        };

        // Clean up any leftover atomic-write temp files (crash between
        // temp-file write and rename). Best-effort.
        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().and_then(|s| s.to_str()) == Some("tmp") {
                let _ = std::fs::remove_file(&path);
            }
        }

        // Log entries — every log/{index:020}.bin under the dir, filtered
        // against the purge floor and the truncation bound.
        let log_subdir = dir.join("log");
        if log_subdir.exists() {
            let mut log = self.log.write().await;
            for entry in std::fs::read_dir(&log_subdir)? {
                let entry = entry?;
                let path = entry.path();
                let ext = path.extension().and_then(|s| s.to_str());
                if ext == Some("tmp") {
                    let _ = std::fs::remove_file(&path);
                    continue;
                }
                if ext != Some("bin") {
                    continue;
                }
                // The filename encodes the index; it is what purge/truncate
                // deletes key on, so it is what recovery filters on.
                let Some(idx) = path
                    .file_stem()
                    .and_then(|s| s.to_str())
                    .and_then(|s| s.parse::<u64>().ok())
                else {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!(
                            "Raft log file has unparseable index in name {}; \
                             remove or rename before restart",
                            path.display()
                        ),
                    ));
                };

                let below_purge_floor = purged_index.is_some_and(|floor| idx <= floor);
                let above_truncation =
                    log_bound.is_some_and(|meta| meta.last_log_index.is_none_or(|last| idx > last));
                if below_purge_floor || above_truncation {
                    info!(
                        index = idx,
                        below_purge_floor,
                        above_truncation,
                        "Removing stale Raft log file left behind by an \
                         interrupted purge/truncate"
                    );
                    let _ = std::fs::remove_file(&path);
                    continue;
                }

                let bytes = std::fs::read(&path)?;
                let log_entry: Entry<G::Cfg> = postcard::from_bytes(&bytes).map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("log {}: deserialize: {}", path.display(), e),
                    )
                })?;
                log.insert(log_entry.log_id.index, log_entry);
            }
        }

        Ok(())
    }

    /// Apply a multi-field state-machine transition atomically.
    ///
    /// Build a `StateTransition` with `with_*` methods (each call marks
    /// a field for update; un-touched fields stay un-set), then await
    /// `self.apply_state_transition(t)`.
    ///
    /// The helper acquires every requested write lock up front, in a
    /// fixed order, then performs the assignments inside a single
    /// no-`.await` block.once the synchronous mutation block runs
    /// the transition is total — cancellation between guard
    /// acquisitions can only abort BEFORE any field has changed.
    ///
    /// Lock acquisition order — never deviate, since deadlock-freedom
    /// across the three call sites depends on it:
    ///   1. sm inner state (`sm.read().await.state_arc().write_owned()`)
    ///   2. last_applied_log
    ///   3. last_membership
    ///   4. cached_snapshot
    ///   5. snapshot_pointer
    async fn apply_state_transition(
        &self,
        t: StateTransition<G>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        // Serialize the entire (mutate + drop guards + fsync) sequence so
        // two concurrent applies cannot interleave their fsyncs. Without
        // this, ordering of `last_applied.bin` writes is not the same as
        // the order in which the FSM was mutated, and a crash can resume
        // from a stale `last_applied` while the FSM holds a newer state.
        let _serialize = self.apply_lock.lock().await;

        let StateTransition {
            sm_state,
            last_applied,
            last_membership,
            cached_snapshot,
            snapshot_pointer,
        } = t;

        // Acquire write guards FIRST, in fixed order. Each await is a
        // cancellation point, but cancellation here aborts BEFORE any
        // mutation has run.
        let sm_state_guard = if sm_state.is_some() {
            Some(G::sm_state_arc(&*self.sm.read().await).write_owned().await)
        } else {
            None
        };
        let last_applied_guard = if last_applied.is_some() {
            Some(self.last_applied_log.write().await)
        } else {
            None
        };
        let last_membership_guard = if last_membership.is_some() {
            Some(self.last_membership.write().await)
        } else {
            None
        };
        let cached_snapshot_guard = if cached_snapshot.is_some() {
            Some(self.cached_snapshot.write().await)
        } else {
            None
        };
        let snapshot_pointer_guard = if snapshot_pointer.is_some() {
            Some(self.snapshot_pointer.write().await)
        } else {
            None
        };

        // ============================================================
        // Synchronous mutation block — NO `.await` below this line.
        // ============================================================
        if let (Some(mut g), Some(v)) = (sm_state_guard, sm_state) {
            *g = v;
        }
        if let (Some(mut g), Some(v)) = (last_applied_guard, last_applied) {
            *g = v;
        }
        if let (Some(mut g), Some(v)) = (last_membership_guard, last_membership) {
            *g = v;
        }
        if let (Some(mut g), Some(v)) = (cached_snapshot_guard, cached_snapshot) {
            *g = v;
        }
        if let (Some(mut g), Some(v)) = (snapshot_pointer_guard, snapshot_pointer) {
            *g = v;
        }

        // Persist last_applied AFTER the in-memory mutation. A crash between
        // FSM mutation and this fsync replays at most the just-applied entry
        // on restart — without this fsync, restart replays every entry since
        // the last snapshot, which double-applies non-idempotent commands
        // (producer_id, leader_epoch, transactional epoch).
        //
        // Persistence failure here is fatal: it must propagate out as a
        // `StorageError` so openraft refuses to mark the entry committed.
        // Previously this was downgraded to a warn-and-continue ("the next
        // successful apply will re-establish it"), but that comment is
        // wrong for non-idempotent commands — if the process crashes
        // before another apply succeeds, `last_applied.bin` is stale and
        // recovery replays every entry above it, double-applying epoch
        // bumps and minting duplicate producer IDs.
        if let Some(applied) = last_applied.flatten()
            && let Err(e) = self.persist_last_applied(&applied).await
        {
            tracing::error!(
                error = %e,
                index = applied.index,
                "persist_last_applied failed; refusing to commit entry"
            );
            return Err(StorageError::from_io_error(
                openraft::ErrorSubject::Logs,
                openraft::ErrorVerb::Write,
                std::io::Error::other(format!("persist_last_applied failed: {}", e)),
            ));
        }
        Ok(())
    }

    /// Atomically write `bytes` to `path` then fsync the file and its parent
    /// directory. Used for vote, log entry, and purged-marker writes.
    fn atomic_write_fsync_blocking(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
        use std::io::Write;
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let tmp_path = path.with_extension("tmp");
        {
            let mut f = std::fs::File::create(&tmp_path)?;
            f.write_all(bytes)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp_path, path)?;
        if let Some(parent) = path.parent() {
            let dir = std::fs::File::open(parent)?;
            dir.sync_all()?;
        }
        Ok(())
    }

    /// Group-fsync variant: write+rename `items` in one shot, then fsync the
    /// shared parent directory exactly once at the end.
    ///
    /// `atomic_write_fsync_blocking` does N file fsyncs + N parent-dir fsyncs
    /// for a batch of N items. The parent-dir fsyncs dominate when the
    /// directory has many entries (each must walk the dir's inode block).
    /// For a Raft `append_to_log` batch of 100 entries this is ~200 fsyncs,
    /// pinning broker metadata throughput at ~1 batch / 2× fsync_latency.
    ///
    /// Group-fsync collapses that to N file fsyncs + 1 dir fsync, while
    /// preserving the same crash semantics: each individual file's data is
    /// fsynced before its rename, and the dir-fsync at the end makes every
    /// rename durable atomically. A crash mid-batch can leave a contiguous
    /// prefix of items renamed-and-durable; the caller (e.g. `append_to_log`)
    /// only raises the durable bound after this returns, so the suffix of
    /// items not yet renamed-and-durable is treated as un-acked on recovery.
    ///
    /// All `items` must share the same parent directory.
    fn atomic_write_fsync_batch_blocking(items: &[(PathBuf, Vec<u8>)]) -> std::io::Result<()> {
        use std::io::Write;
        if items.is_empty() {
            return Ok(());
        }
        let parent = items[0].0.parent().map(|p| p.to_path_buf());
        if let Some(ref p) = parent {
            std::fs::create_dir_all(p)?;
        }
        for (path, bytes) in items {
            // Defensive: a mixed-parent batch would silently skip the dir
            // fsync for items in the other directory. Reject up front.
            debug_assert_eq!(
                path.parent(),
                parent.as_deref(),
                "atomic_write_fsync_batch_blocking requires a shared parent dir"
            );
            let tmp_path = path.with_extension("tmp");
            {
                let mut f = std::fs::File::create(&tmp_path)?;
                f.write_all(bytes)?;
                f.sync_all()?;
            }
            std::fs::rename(&tmp_path, path)?;
        }
        if let Some(p) = parent {
            let dir = std::fs::File::open(&p)?;
            dir.sync_all()?;
        }
        Ok(())
    }

    /// Async wrapper that runs the blocking write+fsync on a blocking thread.
    /// Each fsync can stall 5–20 ms; running it on the async runtime would
    /// pin a tokio worker for the duration and block all other tasks
    /// scheduled on that worker.
    async fn atomic_write_fsync(path: PathBuf, bytes: Vec<u8>) -> std::io::Result<()> {
        tokio::task::spawn_blocking(move || Self::atomic_write_fsync_blocking(&path, &bytes))
            .await
            .map_err(|e| std::io::Error::other(format!("spawn_blocking join: {}", e)))?
    }

    /// Persist `vote` to disk if we have a log_dir configured. Errors propagate.
    async fn persist_vote(&self, vote: &Vote<RaftNodeId>) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        let bytes = postcard::to_stdvec(vote).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("vote serialize: {}", e),
            )
        })?;
        Self::atomic_write_fsync(dir.join("vote.bin"), bytes).await
    }

    /// Persist a log entry to disk if we have a log_dir configured.
    ///
    /// Single-entry path retained for non-batch callers (vote/snapshot
    /// installs that may persist one entry at a time). The hot
    /// `append_to_log` path uses `atomic_write_fsync_batch_blocking` to
    /// amortize the parent-dir fsync across the whole batch.
    #[allow(dead_code)]
    async fn persist_log_entry(&self, entry: &Entry<G::Cfg>) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        let bytes = postcard::to_stdvec(entry).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("log serialize: {}", e),
            )
        })?;
        let path = dir
            .join("log")
            .join(format!("{:020}.bin", entry.log_id.index));
        Self::atomic_write_fsync(path, bytes).await
    }

    /// Remove on-disk log entries with index <= `up_to_index`. Best-effort:
    /// missing files are not an error (the in-memory map is the source of
    /// truth).
    fn purge_log_files(&self, up_to_index: u64) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        let log_subdir = dir.join("log");
        if !log_subdir.exists() {
            return Ok(());
        }
        for entry in std::fs::read_dir(&log_subdir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(idx_str) = name.strip_suffix(".bin")
                && let Ok(idx) = idx_str.parse::<u64>()
                && idx <= up_to_index
            {
                let _ = std::fs::remove_file(entry.path());
            }
        }
        Ok(())
    }

    /// Persist the purged-up-to marker.
    async fn persist_purged(&self, log_id: &LogId<RaftNodeId>) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        let bytes = postcard::to_stdvec(log_id).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("purged serialize: {}", e),
            )
        })?;
        Self::atomic_write_fsync(dir.join("purged.bin"), bytes).await
    }

    /// Persist the durable log bound (`log_meta.bin`).
    ///
    /// Written AFTER appends (raising the bound only once the entries are
    /// durable) and BEFORE conflict-truncation file deletes (lowering the
    /// bound so a crash mid-delete cannot resurrect forked entries).
    async fn persist_log_meta(&self, last_log_index: Option<u64>) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        let meta = LogMeta { last_log_index };
        let bytes = postcard::to_stdvec(&meta).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("log_meta serialize: {}", e),
            )
        })?;
        Self::atomic_write_fsync(dir.join("log_meta.bin"), bytes).await
    }

    /// Persist the FSM `last_applied_log` marker.
    ///
    /// Written after every per-entry FSM apply. Without this, the only
    /// durable upper bound on what's been applied is whatever was in the
    /// most recent snapshot — so a restart between snapshots reapplies
    /// every entry whose log_id is >= the snapshot's last_applied. That
    /// double-applies non-idempotent commands (producer-id allocation,
    /// leader-epoch bumps, transactional epoch bumps) on every restart.
    async fn persist_last_applied(&self, log_id: &LogId<RaftNodeId>) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        let bytes = postcard::to_stdvec(log_id).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("last_applied serialize: {}", e),
            )
        })?;
        Self::atomic_write_fsync(dir.join("last_applied.bin"), bytes).await
    }

    /// Get the state machine for reading.
    pub fn state_machine(&self) -> Arc<RwLock<G::Sm>> {
        self.sm.clone()
    }

    /// Load the latest snapshot from object store on startup.
    ///
    /// This should be called after creating the store to restore state.
    /// Also cleans up orphaned temp files and unreferenced snapshot
    /// generations from crashed snapshot writes.
    ///
    /// # Returns
    /// - `Ok(true)` if a snapshot was successfully loaded
    /// - `Ok(false)` if no snapshot exists (clean start)
    /// - `Err(...)` if snapshot metadata exists but no loadable snapshot
    ///   generation remains (should fail startup)
    ///
    /// # Corruption handling
    /// The pointer object (`current.meta`) names the current generation's
    /// data object and, as a fallback, the previous generation's. If the
    /// current generation is missing or fails to deserialize, this falls
    /// back to the previous generation (logging loudly). Only when no
    /// referenced generation is loadable does this return an error — the
    /// caller fails closed rather than starting with silent amnesia.
    ///
    /// Legacy layouts (metadata at `current.meta` in the pre-pointer
    /// format, data at `current.snapshot`) are still read transparently.
    pub async fn load_snapshot_from_store(&self) -> Result<bool, StorageError<RaftNodeId>> {
        let snapshot_meta_path = ObjectPath::from(format!("{}/current.meta", self.snapshot_path));

        // Try to load the pointer object.
        let meta_bytes = match self.object_store.get(&snapshot_meta_path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => {
                    // Pointer exists but couldn't be read - this is corruption
                    error!(
                        error = %e,
                        path = %snapshot_meta_path,
                        "CORRUPTION: Snapshot metadata file exists but failed to read bytes"
                    );
                    return Err(StorageError::from_io_error(
                        openraft::ErrorSubject::Snapshot(None),
                        openraft::ErrorVerb::Read,
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Snapshot metadata corruption: {}", e),
                        ),
                    ));
                }
            },
            Err(object_store::Error::NotFound { .. }) => {
                // No snapshot exists - this is OK, clean start
                debug!("No existing snapshot found in object store (clean start)");
                self.cleanup_stale_snapshot_files(&std::collections::HashSet::new())
                    .await;
                return Ok(false);
            }
            Err(e) => {
                // Other error accessing object store - could be transient or corruption
                error!(error = %e, "Failed to access snapshot metadata from object store");
                return Err(StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    std::io::Error::other(e.to_string()),
                ));
            }
        };

        // Decode the pointer. New format first; legacy bytes fail with a
        // clean EOF (they lack the trailing fields) and fall through to the
        // legacy decode, which maps to a synthetic single-candidate pointer
        // whose data lives at `current.snapshot`.
        let candidates: Vec<SnapshotPointerEntry> = match postcard::from_bytes::<SnapshotPointer>(
            &meta_bytes,
        ) {
            Ok(pointer) => {
                let mut v = vec![pointer.current];
                v.extend(pointer.previous);
                v
            }
            Err(pointer_err) => match postcard::from_bytes::<SnapshotMetadata>(&meta_bytes) {
                Ok(legacy) => {
                    info!("Loading legacy (pre-pointer) snapshot layout");
                    vec![SnapshotPointerEntry {
                        last_log_id: legacy.last_log_id,
                        last_membership: legacy.last_membership,
                        snapshot_id: legacy.snapshot_id,
                        data_object: "current.snapshot".to_string(),
                    }]
                }
                Err(legacy_err) => {
                    error!(
                        pointer_error = %pointer_err,
                        legacy_error = %legacy_err,
                        path = %snapshot_meta_path,
                        bytes_len = meta_bytes.len(),
                        "CORRUPTION: Snapshot metadata file is corrupted (deserialization failed)"
                    );
                    return Err(StorageError::from_io_error(
                        openraft::ErrorSubject::Snapshot(None),
                        openraft::ErrorVerb::Read,
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!(
                                "Snapshot metadata deserialization failed (corruption): \
                                     pointer: {}; legacy: {}",
                                pointer_err, legacy_err
                            ),
                        ),
                    ));
                }
            },
        };

        // Try each referenced generation, newest first.
        let mut failures: Vec<String> = Vec::new();
        for (idx, entry) in candidates.iter().enumerate() {
            let data_bytes = match self.read_snapshot_generation(entry).await {
                Ok(bytes) => bytes,
                Err(reason) => {
                    error!(
                        snapshot_id = %entry.snapshot_id,
                        data_object = %entry.data_object,
                        reason = %reason,
                        "CORRUPTION: snapshot generation unusable"
                    );
                    failures.push(reason);
                    continue;
                }
            };

            // Validate BEFORE mutating the state machine.
            let state = match G::deserialize_state(&data_bytes) {
                Ok(state) => state,
                Err(e) => {
                    let reason = format!(
                        "snapshot {} data object {} failed to deserialize: {}",
                        entry.snapshot_id, entry.data_object, e
                    );
                    error!(
                        snapshot_id = %entry.snapshot_id,
                        data_object = %entry.data_object,
                        error = %e,
                        "CORRUPTION: snapshot data failed to deserialize"
                    );
                    failures.push(reason);
                    continue;
                }
            };

            if idx > 0 {
                error!(
                    snapshot_id = %entry.snapshot_id,
                    "FALLBACK: current snapshot generation was unusable; \
                     restored the previous generation instead. State will be \
                     caught up from the Raft log / leader snapshot."
                );
            }

            // Install: state machine, applied indices, caches, pointer.
            //
            // Routed through `apply_state_transition` so the five field
            // updates land in a single critical section — the previous
            // open-coded sequence had four `.write().await` calls
            // between mutations, any of which could be cancelled
            // mid-install and leave the store with the new state but
            // stale indices.
            let meta = SnapshotMeta {
                last_log_id: entry.last_log_id,
                last_membership: entry.last_membership.clone(),
                snapshot_id: entry.snapshot_id.clone(),
            };
            let referenced: std::collections::HashSet<String> =
                candidates.iter().map(|c| c.data_object.clone()).collect();
            let new_pointer = SnapshotPointer {
                current: entry.clone(),
                previous: candidates.get(idx + 1).cloned(),
            };
            self.apply_state_transition(
                StateTransition::new()
                    .with_sm_state(state)
                    .with_last_applied(entry.last_log_id)
                    .with_last_membership(entry.last_membership.clone())
                    .with_cached_snapshot(Some(CachedSnapshot {
                        meta: meta.clone(),
                        data: data_bytes,
                    }))
                    .with_snapshot_pointer(Some(new_pointer)),
            )
            .await?;

            // Best-effort cleanup of temp files and unreferenced
            // generations from crashed writes.
            self.cleanup_stale_snapshot_files(&referenced).await;

            info!(
                snapshot_id = %meta.snapshot_id,
                last_log_index = ?meta.last_log_id.map(|l| l.index),
                "Restored snapshot from object store"
            );

            return Ok(true);
        }

        // Metadata exists but no referenced generation is loadable.
        Err(StorageError::from_io_error(
            openraft::ErrorSubject::Snapshot(None),
            openraft::ErrorVerb::Read,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Snapshot metadata exists but no referenced snapshot \
                     generation is loadable: {}",
                    failures.join("; ")
                ),
            ),
        ))
    }

    /// Read one snapshot generation's data object, returning a description
    /// of the failure instead of an error type so the caller can fall back.
    async fn read_snapshot_generation(
        &self,
        entry: &SnapshotPointerEntry,
    ) -> Result<Vec<u8>, String> {
        let data_path = ObjectPath::from(format!("{}/{}", self.snapshot_path, entry.data_object));
        match self.object_store.get(&data_path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => Ok(bytes.to_vec()),
                Err(e) => Err(format!(
                    "snapshot {} data object {} read failed: {}",
                    entry.snapshot_id, entry.data_object, e
                )),
            },
            Err(object_store::Error::NotFound { .. }) => Err(format!(
                "snapshot {} data object {} is missing",
                entry.snapshot_id, entry.data_object
            )),
            Err(e) => Err(format!(
                "snapshot {} data object {} access failed: {}",
                entry.snapshot_id, entry.data_object, e
            )),
        }
    }

    /// Clean up stale snapshot objects: legacy `temp-*` files from the old
    /// rename-based scheme, and generation data objects no longer referenced
    /// by the pointer. Best-effort; never deletes `current.meta` or any
    /// object named in `referenced`.
    async fn cleanup_stale_snapshot_files(&self, referenced: &std::collections::HashSet<String>) {
        let prefix = self.snapshot_path.clone();
        let mut stale: Vec<ObjectPath> = Vec::new();

        use futures::StreamExt;
        let mut stream = self.object_store.list(Some(&prefix));
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    let Some(name) = meta.location.filename().map(str::to_string) else {
                        continue;
                    };
                    if name == "current.meta" || referenced.contains(&name) {
                        continue;
                    }
                    // `current.snapshot` is never deleted: it is either the
                    // legacy layout's data object or the post-commit legacy
                    // mirror; both are refreshed/superseded in place.
                    if name == "current.snapshot" {
                        continue;
                    }
                    let is_temp = name.starts_with("temp-")
                        && (name.ends_with(".snapshot") || name.ends_with(".meta"));
                    let is_unreferenced_generation =
                        name.starts_with("gen-") && name.ends_with(".snapshot");
                    if is_temp || is_unreferenced_generation {
                        stale.push(meta.location);
                    }
                }
                Err(e) => {
                    debug!(error = %e, "Error listing files during snapshot cleanup");
                    // Continue - best effort cleanup
                }
            }
        }

        for path in stale {
            match self.object_store.delete(&path).await {
                Ok(()) => {
                    info!(path = %path, "Cleaned up stale snapshot object");
                }
                Err(e) => {
                    debug!(error = %e, path = %path, "Failed to delete stale snapshot object (may already be gone)");
                }
            }
        }
    }

    /// Persist a snapshot to the object store using the generation/pointer
    /// scheme.
    ///
    /// # Crash ordering
    ///
    /// 1. Write the snapshot data to a fresh, immutable, uniquely named
    ///    generation object (`gen-<id>-<uuid>.snapshot`).
    /// 2. Overwrite the single small pointer object (`current.meta`) with a
    ///    pointer naming the new generation as `current` and the outgoing
    ///    generation as `previous`. This single PUT is the commit point —
    ///    object-store PUTs are atomic, so readers observe either the old
    ///    or the new pointer, never a torn state, and the data a pointer
    ///    names is always fully written before the pointer itself.
    /// 3. Only after the pointer is durable, refresh the legacy
    ///    `current.snapshot` mirror (for pre-pointer binaries/tooling) and
    ///    best-effort delete the generation that fell off the end (older
    ///    than `previous`).
    ///
    /// A crash after step 1 leaves an orphaned generation (cleaned up at
    /// next startup); a crash after step 2 leaves a stale mirror and/or the
    /// now-unreferenced oldest generation behind (also harmless — new
    /// readers only follow the pointer). At no point can metadata reference
    /// missing data.
    async fn persist_snapshot(
        &self,
        meta: &SnapshotMeta<RaftNodeId, BasicNode>,
        data: &[u8],
    ) -> Result<(), StorageError<RaftNodeId>> {
        // Single-flight: hold this guard for the WHOLE persist sequence
        // (data write -> pointer commit -> mirror -> evict-delete) so a
        // concurrent persister cannot race our retention delete and drop a
        // generation that the new pointer still references. Snapshot
        // persistence is rare so the serialization cost is fine.
        let _persist_guard = self.snapshot_persist_lock.lock().await;

        // Unique per write: snapshot_id alone can repeat (it is derived from
        // the last applied index), and the referenced data object must be
        // immutable for the pointer scheme to be crash-safe.
        let data_object = format!("gen-{}-{}.snapshot", meta.snapshot_id, uuid::Uuid::new_v4());
        let data_path = ObjectPath::from(format!("{}/{}", self.snapshot_path, data_object));

        // Step 1: write the (large, slow) data object WITHOUT holding the
        // pointer lock. The data path is unique per call, so concurrent
        // persists cannot collide on the object store; the pointer lock only
        // exists to serialize the small commit-point write below. Holding it
        // across the data PUT used to freeze every other code path that
        // touches `snapshot_pointer` for the duration of an S3 round trip.
        self.object_store
            .put(&data_path, Bytes::copy_from_slice(data).into())
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to write snapshot data object");
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    std::io::Error::other(e.to_string()),
                )
            })?;

        // Step 2: under the pointer lock, read the prior pointer, write the
        // new commit-point object, and update the in-memory pointer. The
        // lock is held only across this small PUT and the `*pointer_guard`
        // store, not the multi-second data upload above.
        let mut pointer_guard = self.snapshot_pointer.write().await;
        let previous = pointer_guard.as_ref().map(|p| p.current.clone());
        let evicted = pointer_guard.as_ref().and_then(|p| p.previous.clone());

        let new_pointer = SnapshotPointer {
            current: SnapshotPointerEntry {
                last_log_id: meta.last_log_id,
                last_membership: meta.last_membership.clone(),
                snapshot_id: meta.snapshot_id.clone(),
                data_object: data_object.clone(),
            },
            previous,
        };
        let pointer_bytes = postcard::to_stdvec(&new_pointer).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        let pointer_path = ObjectPath::from(format!("{}/current.meta", self.snapshot_path));
        if let Err(e) = self
            .object_store
            .put(&pointer_path, Bytes::from(pointer_bytes).into())
            .await
        {
            error!(error = %e, "Failed to write snapshot pointer (commit point)");
            drop(pointer_guard);
            // The new generation was never referenced; remove it.
            let _ = self.object_store.delete(&data_path).await;
            return Err(StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::other(e.to_string()),
            ));
        }
        *pointer_guard = Some(new_pointer.clone());
        drop(pointer_guard);

        // Step 3: AFTER the commit point, refresh the legacy mirror at
        // `current.snapshot`. New readers never consult it (the pointer
        // references the immutable generation object), so a crash before or
        // during this write is harmless; it exists so pre-pointer binaries
        // and tooling that expect `{prefix}/current.snapshot` keep working.
        // Best-effort by design.
        let legacy_mirror_path =
            ObjectPath::from(format!("{}/current.snapshot", self.snapshot_path));
        if let Err(e) = self
            .object_store
            .put(&legacy_mirror_path, Bytes::copy_from_slice(data).into())
            .await
        {
            debug!(error = %e, "Failed to refresh legacy snapshot mirror (non-fatal)");
        }

        // Step 4: best-effort delete of the generation that fell off the
        // retention window (we keep current + previous).
        if let Some(evicted) = evicted
            && evicted.data_object != data_object
            && Some(&evicted.data_object) != new_pointer.previous.as_ref().map(|p| &p.data_object)
            && evicted.data_object != "current.snapshot"
        {
            let evicted_path =
                ObjectPath::from(format!("{}/{}", self.snapshot_path, evicted.data_object));
            let _ = self.object_store.delete(&evicted_path).await;
        }

        info!(
            snapshot_id = %meta.snapshot_id,
            last_log_index = ?meta.last_log_id.map(|l| l.index),
            size_bytes = data.len(),
            data_object = %data_object,
            "Persisted snapshot to object store (generation/pointer commit)"
        );

        Ok(())
    }
}

impl<G: GroupKind> RaftStorage<G::Cfg> for RaftStore<G> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_reader(&mut self) -> Self::LogReader {
        Self {
            vote: self.vote.clone(),
            log: self.log.clone(),
            last_purged_log_id: self.last_purged_log_id.clone(),
            sm: self.sm.clone(),
            last_applied_log: self.last_applied_log.clone(),
            last_membership: self.last_membership.clone(),
            cached_snapshot: self.cached_snapshot.clone(),
            snapshot_pointer: self.snapshot_pointer.clone(),
            object_store: self.object_store.clone(),
            snapshot_path: self.snapshot_path.clone(),
            log_dir: self.log_dir.clone(),
            apply_lock: self.apply_lock.clone(),
            snapshot_persist_lock: self.snapshot_persist_lock.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        // Persist BEFORE updating in-memory state. We must never claim to
        // have voted without it being durable, or two leaders can be elected
        // for the same term across a crash.
        if let Err(e) = self.persist_vote(vote).await {
            return Err(StorageError::IO {
                source: StorageIOError::write_vote(&e),
            });
        }
        *self.vote.write().await = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<RaftNodeId>>, StorageError<RaftNodeId>> {
        Ok(*self.vote.read().await)
    }

    async fn get_log_state(
        &mut self,
    ) -> Result<openraft::storage::LogState<G::Cfg>, StorageError<RaftNodeId>> {
        let log = self.log.read().await;
        let last_purged = *self.last_purged_log_id.read().await;
        let last_log_id = log.values().last().map(|e| e.log_id);

        Ok(openraft::storage::LogState {
            last_purged_log_id: last_purged,
            last_log_id,
        })
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<RaftNodeId>>
    where
        I: IntoIterator<Item = Entry<G::Cfg>> + OptionalSend,
    {
        // Collect into a Send-able Vec so the iterator is not held across
        // the spawn_blocking await. Without this the future would not be
        // Send and openraft refuses to schedule it.
        let entries: Vec<Entry<G::Cfg>> = entries.into_iter().collect();
        let mut log = self.log.write().await;

        // Build a single batch of (path, bytes) for all entries in this
        // call so the persist hits the disk as one group-fsync rather than
        // N independent (file fsync + dir fsync + spawn_blocking) trips.
        // Without this, every entry pays ≥2 fsyncs and a fresh blocking
        // worker — bounding metadata throughput to ~1/(N × fsync_latency).
        if let Some(dir) = self.log_dir.clone() {
            let mut items: Vec<(PathBuf, Vec<u8>)> = Vec::with_capacity(entries.len());
            for entry in &entries {
                let bytes = postcard::to_stdvec(entry).map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(&std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        format!("log serialize: {}", e),
                    )),
                })?;
                let path = dir
                    .join("log")
                    .join(format!("{:020}.bin", entry.log_id.index));
                items.push((path, bytes));
            }
            tokio::task::spawn_blocking(move || Self::atomic_write_fsync_batch_blocking(&items))
                .await
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(&std::io::Error::other(format!(
                        "spawn_blocking join: {}",
                        e
                    ))),
                })?
                .map_err(|e| StorageError::IO {
                    source: StorageIOError::write_logs(&e),
                })?;
        }

        // Insert into the in-memory log AFTER the durable batch — entries
        // become visible to readers only after they are crash-safe.
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        // Raise the durable log bound AFTER the entries themselves are
        // durable. A crash in between simply drops the (un-acked) tail of
        // this batch on recovery; it can never drop acked entries because
        // the bound only moves forward here.
        let last_log_index = log.keys().next_back().copied();
        if let Err(e) = self.persist_log_meta(last_log_index).await {
            return Err(StorageError::IO {
                source: StorageIOError::write_logs(&e),
            });
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let purged = *self.last_purged_log_id.read().await;
        let mut log = self.log.write().await;
        let keys_to_remove: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();

        // Persist the truncation intent (lowered log bound) BEFORE mutating
        // anything. File deletes below are best-effort, so the durable
        // bound is what guarantees a crash mid-delete cannot resurrect the
        // forked entries on recovery. This must succeed or the truncation
        // must fail wholesale.
        let new_last_log_index = log
            .range(..log_id.index)
            .next_back()
            .map(|(k, _)| *k)
            .or(purged.map(|p| p.index));
        if let Err(e) = self.persist_log_meta(new_last_log_index).await {
            return Err(StorageError::IO {
                source: StorageIOError::write_logs(&e),
            });
        }

        for key in &keys_to_remove {
            log.remove(key);
        }
        // Mirror the deletion on disk. Failures here are tolerable — the
        // durable log bound written above makes recovery skip (and remove)
        // any file we fail to delete here.
        if let Some(dir) = self.log_dir.as_ref() {
            for key in &keys_to_remove {
                let path = dir.join("log").join(format!("{:020}.bin", key));
                let _ = std::fs::remove_file(&path);
            }
        }
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        // Persist the purge floor BEFORE deleting anything (in memory or on
        // disk). File deletes are best-effort; the durable floor is what
        // guarantees purged entries can never resurrect on recovery.
        if let Err(e) = self.persist_purged(&log_id).await {
            return Err(StorageError::IO {
                source: StorageIOError::write_logs(&e),
            });
        }
        *self.last_purged_log_id.write().await = Some(log_id);

        let mut log = self.log.write().await;
        let keys_to_remove: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys_to_remove {
            log.remove(&key);
        }
        // Best-effort on-disk purge; recovery filters against purged.bin.
        let _ = self.purge_log_files(log_id.index);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<RaftNodeId>>,
            StoredMembership<RaftNodeId, BasicNode>,
        ),
        StorageError<RaftNodeId>,
    > {
        let last_applied = *self.last_applied_log.read().await;
        let membership = self.last_membership.read().await.clone();
        Ok((last_applied, membership))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<G::Cfg>],
    ) -> Result<Vec<GroupResponse<G>>, StorageError<RaftNodeId>> {
        let mut responses = Vec::with_capacity(entries.len());

        for entry in entries {
            let response = match &entry.payload {
                EntryPayload::Blank => G::ok_response(),
                EntryPayload::Normal(command) => {
                    let sm = self.sm.read().await;
                    let response = G::apply(&*sm, command.clone()).await;
                    drop(sm);
                    response
                }
                EntryPayload::Membership(_) => G::ok_response(),
            };

            // Advance `last_applied_log` (and any membership change) ONCE
            // per entry, immediately after the FSM mutation. Several
            // commands are non-idempotent on replay (producer-id alloc,
            // leader_epoch bump on AcquirePartition / Transfer / fence,
            // transactional epoch bump). A batch-end advance leaves a
            // cancellation window in which all N entries have already
            // mutated SM but `last_applied` still points before entry 0
            // — on resume openraft re-applies every entry, double-bumping
            // epochs and minting duplicate producer IDs. Per-entry advance
            // shrinks that window to one entry: a cancellation between
            // FSM-write and `apply_state_transition` may still replay the
            // *current* entry, but no entry already accounted for in
            // `last_applied` is ever replayed.
            let mut t = StateTransition::new().with_last_applied(Some(entry.log_id));
            if let EntryPayload::Membership(membership) = &entry.payload {
                t = t.with_last_membership(StoredMembership::new(
                    Some(entry.log_id),
                    membership.clone(),
                ));
            }
            self.apply_state_transition(t).await?;

            responses.push(response);
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        Self {
            vote: self.vote.clone(),
            log: self.log.clone(),
            last_purged_log_id: self.last_purged_log_id.clone(),
            sm: self.sm.clone(),
            last_applied_log: self.last_applied_log.clone(),
            last_membership: self.last_membership.clone(),
            cached_snapshot: self.cached_snapshot.clone(),
            snapshot_pointer: self.snapshot_pointer.clone(),
            object_store: self.object_store.clone(),
            snapshot_path: self.snapshot_path.clone(),
            log_dir: self.log_dir.clone(),
            apply_lock: self.apply_lock.clone(),
            snapshot_persist_lock: self.snapshot_persist_lock.clone(),
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<RaftNodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<RaftNodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let data = snapshot.into_inner();

        // Step 1: validate BEFORE touching anything. Corrupt bytes (e.g.
        // damaged in transit) produce a StorageError instead of a panic or
        // a half-applied install.
        let new_state = G::deserialize_state(&data).map_err(|e| {
            error!(
                snapshot_id = %meta.snapshot_id,
                error = %e,
                "Received snapshot failed to deserialize; rejecting install"
            );
            crate::cluster::metrics::record_raft_snapshot("install", "error");
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(Some(meta.signature())),
                openraft::ErrorVerb::Write,
                e,
            )
        })?;

        // Step 2: persist durably FIRST. If this fails (or we crash here),
        // the in-memory state machine and applied indices are untouched, so
        // a restart resumes from the previous snapshot — state never
        // regresses relative to what openraft was told.
        if let Err(e) = self.persist_snapshot(meta, &data).await {
            crate::cluster::metrics::record_raft_snapshot("install", "error");
            return Err(e);
        }

        // Step 3: route the four-field update through the
        // single-critical-section helper. The helper acquires every
        // write guard up front (in a fixed order) and then mutates
        // synchronously with no `.await` in between, so cancellation
        // between guard acquisitions cannot leave the store with the
        // new SM state but stale `last_applied_log` (the bug the
        // earlier four-await sequence was prone to). See
        // `apply_state_transition` for full cancellation semantics.
        self.apply_state_transition(
            StateTransition::new()
                .with_sm_state(new_state)
                .with_last_applied(meta.last_log_id)
                .with_last_membership(StoredMembership::new(
                    meta.last_log_id,
                    meta.last_membership.membership().clone(),
                ))
                .with_cached_snapshot(Some(CachedSnapshot {
                    meta: meta.clone(),
                    data,
                })),
        )
        .await?;

        crate::cluster::metrics::record_raft_snapshot("install", "success");
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<G::Cfg>>, StorageError<RaftNodeId>> {
        let snapshot_guard = self.cached_snapshot.read().await;
        match &*snapshot_guard {
            Some(cached) => Ok(Some(Snapshot {
                meta: cached.meta.clone(),
                snapshot: Box::new(Cursor::new(cached.data.clone())),
            })),
            None => Ok(None),
        }
    }
}

impl<G: GroupKind> openraft::RaftSnapshotBuilder<G::Cfg> for RaftStore<G> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<G::Cfg>, StorageError<RaftNodeId>> {
        // Capture state and `last_applied_log` under a single read guard on
        // `last_applied_log` to fence concurrent applies — applies hold the
        // matching write guard across `apply_command` + index update, so the
        // captured snapshot bytes and the meta index agree on which entries
        // are reflected.
        let last_applied_guard = self.last_applied_log.read().await;
        let sm = self.sm.read().await;
        let data = G::snapshot(&*sm).await;
        let last_applied = *last_applied_guard;
        let membership = self.last_membership.read().await.clone();
        drop(sm);
        drop(last_applied_guard);

        let snapshot_id = format!("snapshot-{}", last_applied.map(|l| l.index).unwrap_or(0));

        let meta = SnapshotMeta {
            last_log_id: last_applied,
            last_membership: membership,
            snapshot_id,
        };

        // Persist to object store for durability
        if let Err(e) = self.persist_snapshot(&meta, &data).await {
            crate::cluster::metrics::record_raft_snapshot("create", "error");
            return Err(e);
        }

        // Update in-memory cache via the shared helper so any future
        // multi-field cache write inherits the same critical-section
        // discipline.
        self.apply_state_transition(StateTransition::new().with_cached_snapshot(Some(
            CachedSnapshot {
                meta: meta.clone(),
                data: data.clone(),
            },
        )))
        .await?;

        crate::cluster::metrics::record_raft_snapshot("create", "success");
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl<G: GroupKind> openraft::RaftLogReader<G::Cfg> for RaftStore<G> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<G::Cfg>>, StorageError<RaftNodeId>> {
        let log = self.log.read().await;
        let entries: Vec<_> = log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
