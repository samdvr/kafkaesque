//! Raft storage implementation using openraft's RaftStorage trait (v1 API).
//!
//! This module provides combined log and state machine storage for Raft.
//! Snapshots are persisted to the object store for production durability.

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

use super::commands::CoordinationResponse;
use super::state_machine::CoordinationStateMachine;
use super::types::{RaftNodeId, TypeConfig};

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
pub struct RaftStore {
    /// Vote state (who we voted for in current term).
    vote: Arc<RwLock<Option<Vote<RaftNodeId>>>>,
    /// Log entries indexed by log index.
    log: Arc<RwLock<BTreeMap<u64, Entry<TypeConfig>>>>,
    /// Last purged log ID.
    last_purged_log_id: Arc<RwLock<Option<LogId<RaftNodeId>>>>,
    /// State machine.
    sm: Arc<RwLock<CoordinationStateMachine>>,
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
}

impl RaftStore {
    /// Create a new store with object store backing for snapshots.
    ///
    /// This is the in-memory variant — vote and log entries live only in RAM.
    /// Used by unit tests; production code should use `new_with_log_dir`.
    pub fn new(object_store: Arc<dyn ObjectStore>, snapshot_prefix: &str) -> Self {
        Self {
            vote: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            last_purged_log_id: Arc::new(RwLock::new(None)),
            sm: Arc::new(RwLock::new(CoordinationStateMachine::new())),
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            cached_snapshot: Arc::new(RwLock::new(None)),
            snapshot_pointer: Arc::new(RwLock::new(None)),
            object_store,
            snapshot_path: ObjectPath::from(snapshot_prefix),
            log_dir: None,
        }
    }

    /// Create a new store backed by an on-disk WAL for vote and log entries.
    /// The WAL is created on first write; `recover_from_disk()`
    /// must be called before `RaftNode::new` proceeds in order to repopulate
    /// in-memory state from any existing WAL files.
    pub fn new_with_log_dir(
        object_store: Arc<dyn ObjectStore>,
        snapshot_prefix: &str,
        log_dir: PathBuf,
    ) -> Self {
        Self {
            vote: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            last_purged_log_id: Arc::new(RwLock::new(None)),
            sm: Arc::new(RwLock::new(CoordinationStateMachine::new())),
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            cached_snapshot: Arc::new(RwLock::new(None)),
            snapshot_pointer: Arc::new(RwLock::new(None)),
            object_store,
            snapshot_path: ObjectPath::from(snapshot_prefix),
            log_dir: Some(log_dir),
        }
    }

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
                    continue;
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
                let log_entry: Entry<TypeConfig> = postcard::from_bytes(&bytes).map_err(|e| {
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

    /// Atomically write `bytes` to `path` then fsync the file and its parent
    /// directory. Used for vote, log entry, and purged-marker writes.
    fn atomic_write_fsync(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
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

    /// Persist `vote` to disk if we have a log_dir configured. Errors propagate.
    fn persist_vote(&self, vote: &Vote<RaftNodeId>) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        let bytes = postcard::to_stdvec(vote).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("vote serialize: {}", e),
            )
        })?;
        Self::atomic_write_fsync(&dir.join("vote.bin"), &bytes)
    }

    /// Persist a log entry to disk if we have a log_dir configured.
    fn persist_log_entry(&self, entry: &Entry<TypeConfig>) -> std::io::Result<()> {
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
        Self::atomic_write_fsync(&path, &bytes)
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
    fn persist_purged(&self, log_id: &LogId<RaftNodeId>) -> std::io::Result<()> {
        let Some(dir) = self.log_dir.as_ref() else {
            return Ok(());
        };
        let bytes = postcard::to_stdvec(log_id).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("purged serialize: {}", e),
            )
        })?;
        Self::atomic_write_fsync(&dir.join("purged.bin"), &bytes)
    }

    /// Persist the durable log bound (`log_meta.bin`).
    ///
    /// Written AFTER appends (raising the bound only once the entries are
    /// durable) and BEFORE conflict-truncation file deletes (lowering the
    /// bound so a crash mid-delete cannot resurrect forked entries).
    fn persist_log_meta(&self, last_log_index: Option<u64>) -> std::io::Result<()> {
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
        Self::atomic_write_fsync(&dir.join("log_meta.bin"), &bytes)
    }

    /// Get the state machine for reading.
    pub fn state_machine(&self) -> Arc<RwLock<CoordinationStateMachine>> {
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
            let state = match CoordinationStateMachine::deserialize_state(&data_bytes) {
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
            self.sm.write().await.replace_state(state).await;
            *self.last_applied_log.write().await = entry.last_log_id;
            *self.last_membership.write().await = entry.last_membership.clone();

            let meta = SnapshotMeta {
                last_log_id: entry.last_log_id,
                last_membership: entry.last_membership.clone(),
                snapshot_id: entry.snapshot_id.clone(),
            };
            *self.cached_snapshot.write().await = Some(CachedSnapshot {
                meta: meta.clone(),
                data: data_bytes,
            });

            // Record the loaded pointer so the next persist keeps this
            // generation as the fallback. When we fell back to `previous`
            // there is no older generation left to reference.
            let referenced: std::collections::HashSet<String> =
                candidates.iter().map(|c| c.data_object.clone()).collect();
            *self.snapshot_pointer.write().await = Some(SnapshotPointer {
                current: entry.clone(),
                previous: candidates.get(idx + 1).cloned(),
            });

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
        // Unique per write: snapshot_id alone can repeat (it is derived from
        // the last applied index), and the referenced data object must be
        // immutable for the pointer scheme to be crash-safe.
        let data_object = format!("gen-{}-{}.snapshot", meta.snapshot_id, uuid::Uuid::new_v4());
        let data_path = ObjectPath::from(format!("{}/{}", self.snapshot_path, data_object));

        // Step 1: write the new generation's data object.
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

        // Step 2: commit by overwriting the pointer. Hold the pointer lock
        // across the PUT so concurrent persists serialize.
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

impl RaftStorage<TypeConfig> for RaftStore {
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
        }
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
        // Persist BEFORE updating in-memory state. We must never claim to
        // have voted without it being durable, or two leaders can be elected
        // for the same term across a crash.
        if let Err(e) = self.persist_vote(vote) {
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
    ) -> Result<openraft::storage::LogState<TypeConfig>, StorageError<RaftNodeId>> {
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
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let mut log = self.log.write().await;
        for entry in entries {
            // Persist BEFORE updating in-memory state, otherwise a crash
            // between map insert and disk write would lose entries.
            if let Err(e) = self.persist_log_entry(&entry) {
                return Err(StorageError::IO {
                    source: StorageIOError::write_logs(&e),
                });
            }
            log.insert(entry.log_id.index, entry);
        }
        // Raise the durable log bound AFTER the entries themselves are
        // durable. A crash in between simply drops the (un-acked) tail of
        // this batch on recovery; it can never drop acked entries because
        // the bound only moves forward here.
        let last_log_index = log.keys().next_back().copied();
        if let Err(e) = self.persist_log_meta(last_log_index) {
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
        if let Err(e) = self.persist_log_meta(new_last_log_index) {
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
        if let Err(e) = self.persist_purged(&log_id) {
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
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<CoordinationResponse>, StorageError<RaftNodeId>> {
        let mut responses = Vec::new();

        for entry in entries {
            // Hold the `last_applied_log` write guard across the apply so a
            // concurrent `build_snapshot` (which takes `last_applied_log` as
            // a read guard before serializing state) cannot witness state
            // that includes this apply but a `last_applied_log` index that
            // excludes it, or vice versa. The mutation runs FIRST, then the
            // index advances under the same guard — a cancellation or panic
            // before the index update simply re-applies on restart, which is
            // safe given Raft re-delivery semantics.
            let mut last_applied_guard = self.last_applied_log.write().await;

            match &entry.payload {
                EntryPayload::Blank => {
                    *last_applied_guard = Some(entry.log_id);
                    responses.push(CoordinationResponse::Ok);
                }
                EntryPayload::Normal(command) => {
                    let sm = self.sm.read().await;
                    let response = sm.apply_command(command.clone()).await;
                    *last_applied_guard = Some(entry.log_id);
                    drop(sm);
                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    *self.last_membership.write().await =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    *last_applied_guard = Some(entry.log_id);
                    responses.push(CoordinationResponse::Ok);
                }
            }
            drop(last_applied_guard);
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
        let new_state = CoordinationStateMachine::deserialize_state(&data).map_err(|e| {
            error!(
                snapshot_id = %meta.snapshot_id,
                error = %e,
                "Received snapshot failed to deserialize; rejecting install"
            );
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
        self.persist_snapshot(meta, &data).await?;

        // Step 3: only now swap the in-memory state machine and indices.
        self.sm.write().await.replace_state(new_state).await;
        *self.last_applied_log.write().await = meta.last_log_id;
        *self.last_membership.write().await =
            StoredMembership::new(meta.last_log_id, meta.last_membership.membership().clone());

        // Update in-memory cache
        *self.cached_snapshot.write().await = Some(CachedSnapshot {
            meta: meta.clone(),
            data,
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<RaftNodeId>> {
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

impl openraft::RaftSnapshotBuilder<TypeConfig> for RaftStore {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<RaftNodeId>> {
        // Capture state and `last_applied_log` under a single read guard on
        // `last_applied_log` to fence concurrent applies — applies hold the
        // matching write guard across `apply_command` + index update, so the
        // captured snapshot bytes and the meta index agree on which entries
        // are reflected.
        let last_applied_guard = self.last_applied_log.read().await;
        let sm = self.sm.read().await;
        let data = sm.snapshot().await;
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
        self.persist_snapshot(&meta, &data).await?;

        // Update in-memory cache
        *self.cached_snapshot.write().await = Some(CachedSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl openraft::RaftLogReader<TypeConfig> for RaftStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<RaftNodeId>> {
        let log = self.log.read().await;
        let entries: Vec<_> = log.range(range).map(|(_, e)| e.clone()).collect();
        Ok(entries)
    }
}

#[cfg(test)]
mod tests {
    use super::super::commands::CoordinationCommand;
    use super::super::domains;
    use super::super::state_machine::CoordinationState;
    use super::*;
    use object_store::memory::InMemory;
    use openraft::{RaftLogReader, RaftStorage, Vote};

    /// Create a test RaftStore with in-memory object store.
    fn create_test_store() -> RaftStore {
        let object_store = Arc::new(InMemory::new());
        RaftStore::new(object_store, "test/snapshots")
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
        payload: EntryPayload<TypeConfig>,
    ) -> Entry<TypeConfig> {
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
            assert_eq!(response, CoordinationResponse::Ok);
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
        let command = CoordinationCommand::BrokerDomain(domains::BrokerCommand::Register {
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
            CoordinationResponse::BrokerDomainResponse(domains::BrokerResponse::Registered {
                broker_id,
            }) => {
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
        let state = CoordinationState {
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
        let command =
            CoordinationCommand::PartitionDomain(domains::PartitionCommand::CreateTopic {
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
            let mut store = RaftStore::new(object_store.clone(), "persistence-test");

            // Apply some state
            let command = CoordinationCommand::BrokerDomain(domains::BrokerCommand::Register {
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
        let store2 = RaftStore::new(object_store.clone(), "persistence-test");
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
            CoordinationCommand::BrokerDomain(domains::BrokerCommand::Register {
                broker_id: 1,
                host: "broker1".to_string(),
                port: 9092,
                timestamp_ms: 1000,
            }),
            CoordinationCommand::PartitionDomain(domains::PartitionCommand::CreateTopic {
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

        let state = CoordinationState {
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

        let store = RaftStore::new(object_store.clone(), prefix);
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
        let mut store = RaftStore::new(object_store.clone(), prefix);
        let cmd1 = CoordinationCommand::BrokerDomain(domains::BrokerCommand::Register {
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

        let cmd2 = CoordinationCommand::BrokerDomain(domains::BrokerCommand::Register {
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
        let store2 = RaftStore::new(object_store.clone(), prefix);
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

        let store = RaftStore::new(object_store.clone(), prefix);
        let result = store.load_snapshot_from_store().await;
        assert!(result.is_err(), "corrupt metadata must error, not panic");
    }

    #[tokio::test]
    async fn test_install_snapshot_rejects_corrupt_bytes_without_mutating() {
        let mut store = create_test_store();

        // Seed some state.
        let cmd = CoordinationCommand::BrokerDomain(domains::BrokerCommand::Register {
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
        assert_eq!(responses[0], CoordinationResponse::Ok);

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
                EntryPayload::Normal(CoordinationCommand::BrokerDomain(
                    domains::BrokerCommand::Register {
                        broker_id: 1,
                        host: "host1".to_string(),
                        port: 9092,
                        timestamp_ms: 1000,
                    },
                )),
            ),
            make_entry(
                1,
                0,
                2,
                EntryPayload::Normal(CoordinationCommand::BrokerDomain(
                    domains::BrokerCommand::Register {
                        broker_id: 2,
                        host: "host2".to_string(),
                        port: 9093,
                        timestamp_ms: 1000,
                    },
                )),
            ),
            make_entry(
                1,
                0,
                3,
                EntryPayload::Normal(CoordinationCommand::PartitionDomain(
                    domains::PartitionCommand::CreateTopic {
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
        assert_eq!(state.partition_domain.topics.len(), 1);
    }

    #[tokio::test]
    async fn test_noop_command() {
        let mut store = create_test_store();

        // Apply a Noop command
        let entries = vec![make_entry(
            1,
            0,
            1,
            EntryPayload::Normal(CoordinationCommand::Noop),
        )];

        let responses = store.apply_to_state_machine(&entries).await.unwrap();

        assert_eq!(responses.len(), 1);
        assert_eq!(responses[0], CoordinationResponse::Ok);
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
}
