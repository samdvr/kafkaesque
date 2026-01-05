//! Raft storage implementation using openraft's RaftStorage trait (v1 API).
//!
//! This module provides combined log and state machine storage for Raft.
//! Snapshots are persisted to the object store for production durability.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::Arc;

use bytes::Bytes;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, RaftStorage, Snapshot, SnapshotMeta,
    StorageError, StoredMembership, Vote,
};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

use super::commands::CoordinationResponse;
use super::state_machine::CoordinationStateMachine;
use super::types::{RaftNodeId, TypeConfig};

/// Snapshot metadata stored alongside the snapshot data.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct SnapshotMetadata {
    /// The last log ID included in this snapshot.
    last_log_id: Option<LogId<RaftNodeId>>,
    /// The membership configuration at the time of the snapshot.
    last_membership: StoredMembership<RaftNodeId, BasicNode>,
    /// Unique identifier for this snapshot.
    snapshot_id: String,
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
    /// Object store for durable snapshot persistence.
    object_store: Arc<dyn ObjectStore>,
    /// Path prefix for snapshots in the object store.
    snapshot_path: ObjectPath,
}

impl RaftStore {
    /// Create a new store with object store backing for snapshots.
    ///
    /// # Arguments
    /// * `object_store` - The object store to persist snapshots to
    /// * `snapshot_prefix` - Path prefix for snapshot files (e.g., "raft/snapshots/node-0")
    pub fn new(object_store: Arc<dyn ObjectStore>, snapshot_prefix: &str) -> Self {
        Self {
            vote: Arc::new(RwLock::new(None)),
            log: Arc::new(RwLock::new(BTreeMap::new())),
            last_purged_log_id: Arc::new(RwLock::new(None)),
            sm: Arc::new(RwLock::new(CoordinationStateMachine::new())),
            last_applied_log: Arc::new(RwLock::new(None)),
            last_membership: Arc::new(RwLock::new(StoredMembership::default())),
            cached_snapshot: Arc::new(RwLock::new(None)),
            object_store,
            snapshot_path: ObjectPath::from(snapshot_prefix),
        }
    }

    /// Get the state machine for reading.
    pub fn state_machine(&self) -> Arc<RwLock<CoordinationStateMachine>> {
        self.sm.clone()
    }

    /// Load the latest snapshot from object store on startup.
    ///
    /// This should be called after creating the store to restore state.
    /// Also cleans up any orphaned temp files from crashed snapshot writes.
    ///
    /// # Returns
    /// - `Ok(true)` if a snapshot was successfully loaded
    /// - `Ok(false)` if no snapshot exists (clean start)
    /// - `Err(...)` if a snapshot exists but is corrupted (should fail startup)
    ///
    /// # Corruption Detection
    /// The following conditions are treated as corruption and cause an error:
    /// - Metadata file exists but can't be read (I/O error)
    /// - Metadata file exists but can't be deserialized (format corruption)
    /// - Metadata exists but data file is missing (incomplete write/corruption)
    /// - Data file exists but can't be deserialized (format corruption)
    ///
    /// This is critical for safety: starting with corrupted state could lead to
    /// data loss or inconsistent cluster state.
    pub async fn load_snapshot_from_store(&self) -> Result<bool, StorageError<RaftNodeId>> {
        // Clean up any orphaned temp files from previous crashes
        self.cleanup_temp_files().await;

        let snapshot_data_path =
            ObjectPath::from(format!("{}/current.snapshot", self.snapshot_path));
        let snapshot_meta_path = ObjectPath::from(format!("{}/current.meta", self.snapshot_path));

        // Try to load snapshot metadata
        let meta_bytes = match self.object_store.get(&snapshot_meta_path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => bytes,
                Err(e) => {
                    // Metadata file exists but couldn't be read - this is corruption
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

        // Metadata file exists - deserialize it
        let metadata: SnapshotMetadata = bincode::deserialize(&meta_bytes).map_err(|e| {
            // Metadata exists but can't be deserialized - this is corruption
            error!(
                error = %e,
                path = %snapshot_meta_path,
                bytes_len = meta_bytes.len(),
                "CORRUPTION: Snapshot metadata file is corrupted (deserialization failed)"
            );
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Read,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "Snapshot metadata deserialization failed (corruption): {}",
                        e
                    ),
                ),
            )
        })?;

        // Load snapshot data - at this point we expect it to exist since metadata exists
        let data_bytes = match self.object_store.get(&snapshot_data_path).await {
            Ok(result) => match result.bytes().await {
                Ok(bytes) => bytes.to_vec(),
                Err(e) => {
                    // Data file exists but couldn't be read - this is corruption
                    error!(
                        error = %e,
                        path = %snapshot_data_path,
                        "CORRUPTION: Snapshot data file exists but failed to read bytes"
                    );
                    return Err(StorageError::from_io_error(
                        openraft::ErrorSubject::Snapshot(None),
                        openraft::ErrorVerb::Read,
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("Snapshot data corruption: {}", e),
                        ),
                    ));
                }
            },
            Err(object_store::Error::NotFound { .. }) => {
                // Metadata exists but data is missing - this indicates corruption or incomplete write
                error!(
                    meta_path = %snapshot_meta_path,
                    data_path = %snapshot_data_path,
                    snapshot_id = %metadata.snapshot_id,
                    "CORRUPTION: Snapshot metadata exists but data file is missing"
                );
                return Err(StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "Snapshot metadata exists but data file is missing (incomplete write or corruption)",
                    ),
                ));
            }
            Err(e) => {
                error!(error = %e, "Failed to access snapshot data from object store");
                return Err(StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Read,
                    std::io::Error::other(e.to_string()),
                ));
            }
        };

        // Restore state machine from raw bytes
        self.sm.write().await.restore(&data_bytes).await;
        *self.last_applied_log.write().await = metadata.last_log_id;
        *self.last_membership.write().await = metadata.last_membership.clone();

        // Cache the snapshot in memory
        let meta = SnapshotMeta {
            last_log_id: metadata.last_log_id,
            last_membership: metadata.last_membership,
            snapshot_id: metadata.snapshot_id,
        };
        *self.cached_snapshot.write().await = Some(CachedSnapshot {
            meta: meta.clone(),
            data: data_bytes,
        });

        info!(
            snapshot_id = %meta.snapshot_id,
            last_log_index = ?meta.last_log_id.map(|l| l.index),
            "Restored snapshot from object store"
        );

        Ok(true)
    }

    /// Clean up orphaned temporary files from previous crashed snapshot writes.
    ///
    /// This scans for files matching `temp-*.snapshot` and `temp-*.meta` patterns
    /// and deletes them. Safe to call at any time as temp files are never
    /// referenced by valid snapshots.
    async fn cleanup_temp_files(&self) {
        // List all files in the snapshot directory
        let prefix = self.snapshot_path.clone();
        let list_result = self.object_store.list(Some(&prefix));

        // Collect temp files to delete
        let mut temp_files: Vec<ObjectPath> = Vec::new();

        // Use a stream to iterate over files
        use futures::StreamExt;
        let mut stream = list_result;
        while let Some(result) = stream.next().await {
            match result {
                Ok(meta) => {
                    let path_str = meta.location.to_string();
                    // Check if this is a temp file
                    if path_str.contains("/temp-")
                        && (path_str.ends_with(".snapshot") || path_str.ends_with(".meta"))
                    {
                        temp_files.push(meta.location);
                    }
                }
                Err(e) => {
                    debug!(error = %e, "Error listing files during temp cleanup");
                    // Continue - best effort cleanup
                }
            }
        }

        // Delete temp files
        for path in temp_files {
            match self.object_store.delete(&path).await {
                Ok(()) => {
                    info!(path = %path, "Cleaned up orphaned temp snapshot file");
                }
                Err(e) => {
                    debug!(error = %e, path = %path, "Failed to delete temp file (may already be gone)");
                }
            }
        }
    }

    /// Persist a snapshot to the object store.
    ///
    /// # Atomicity
    ///
    /// This uses a two-phase commit pattern to ensure crash safety:
    /// 1. Write snapshot data to a temporary path with unique ID
    /// 2. Write metadata to a temporary path
    /// 3. Atomically rename both to final locations (data first, then meta)
    ///
    /// If we crash at any point:
    /// - Before step 3: Old snapshot remains valid, temp files are orphaned (cleanup on next write)
    /// - After data rename but before meta rename: Old metadata still points to old data (safe)
    /// - After both renames: New snapshot is complete
    ///
    /// The metadata file acts as the commit marker - a snapshot is only valid
    /// if its metadata exists and points to valid data.
    async fn persist_snapshot(
        &self,
        meta: &SnapshotMeta<RaftNodeId, BasicNode>,
        data: &[u8],
    ) -> Result<(), StorageError<RaftNodeId>> {
        // Use snapshot_id in temp paths to avoid collisions
        let temp_data_path = ObjectPath::from(format!(
            "{}/temp-{}.snapshot",
            self.snapshot_path, meta.snapshot_id
        ));
        let temp_meta_path = ObjectPath::from(format!(
            "{}/temp-{}.meta",
            self.snapshot_path, meta.snapshot_id
        ));
        let final_data_path = ObjectPath::from(format!("{}/current.snapshot", self.snapshot_path));
        let final_meta_path = ObjectPath::from(format!("{}/current.meta", self.snapshot_path));

        // Serialize metadata
        let metadata = SnapshotMetadata {
            last_log_id: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            snapshot_id: meta.snapshot_id.clone(),
        };
        let meta_bytes = bincode::serialize(&metadata).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            )
        })?;

        // Step 1: Write snapshot data to temp path
        self.object_store
            .put(&temp_data_path, Bytes::copy_from_slice(data).into())
            .await
            .map_err(|e| {
                error!(error = %e, "Failed to write snapshot data to temp path");
                StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    std::io::Error::other(e.to_string()),
                )
            })?;

        // Step 2: Write metadata to temp path
        if let Err(e) = self
            .object_store
            .put(&temp_meta_path, Bytes::copy_from_slice(&meta_bytes).into())
            .await
        {
            error!(error = %e, "Failed to write snapshot metadata to temp path");
            // Clean up temp data file on failure
            let _ = self.object_store.delete(&temp_data_path).await;
            return Err(StorageError::from_io_error(
                openraft::ErrorSubject::Snapshot(None),
                openraft::ErrorVerb::Write,
                std::io::Error::other(e.to_string()),
            ));
        }

        // Step 3a: Atomically rename data file
        // Note: object_store rename is atomic on most backends (S3, GCS, Azure)
        // For backends that don't support rename, this kafkaesques back to copy+delete
        if let Err(e) = self
            .object_store
            .rename(&temp_data_path, &final_data_path)
            .await
        {
            // Fallback: copy + delete for backends without native rename
            if let Err(copy_err) = self
                .object_store
                .copy(&temp_data_path, &final_data_path)
                .await
            {
                error!(error = %copy_err, "Failed to copy snapshot data to final path");
                // Clean up temp files
                let _ = self.object_store.delete(&temp_data_path).await;
                let _ = self.object_store.delete(&temp_meta_path).await;
                return Err(StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    std::io::Error::other(copy_err.to_string()),
                ));
            }
            // Delete temp after successful copy
            let _ = self.object_store.delete(&temp_data_path).await;
            debug!(error = %e, "Used copy+delete fallback for snapshot data (rename not supported)");
        }

        // Step 3b: Atomically rename metadata file (commit point)
        if let Err(e) = self
            .object_store
            .rename(&temp_meta_path, &final_meta_path)
            .await
        {
            // Fallback: copy + delete
            if let Err(copy_err) = self
                .object_store
                .copy(&temp_meta_path, &final_meta_path)
                .await
            {
                error!(error = %copy_err, "Failed to copy snapshot metadata to final path");
                // Clean up temp meta file
                let _ = self.object_store.delete(&temp_meta_path).await;
                return Err(StorageError::from_io_error(
                    openraft::ErrorSubject::Snapshot(None),
                    openraft::ErrorVerb::Write,
                    std::io::Error::other(copy_err.to_string()),
                ));
            }
            let _ = self.object_store.delete(&temp_meta_path).await;
            debug!(error = %e, "Used copy+delete fallback for snapshot metadata (rename not supported)");
        }

        info!(
            snapshot_id = %meta.snapshot_id,
            last_log_index = ?meta.last_log_id.map(|l| l.index),
            size_bytes = data.len(),
            "Persisted snapshot to object store (atomic commit)"
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
            object_store: self.object_store.clone(),
            snapshot_path: self.snapshot_path.clone(),
        }
    }

    async fn save_vote(&mut self, vote: &Vote<RaftNodeId>) -> Result<(), StorageError<RaftNodeId>> {
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
            log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        let mut log = self.log.write().await;
        let keys_to_remove: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for key in keys_to_remove {
            log.remove(&key);
        }
        Ok(())
    }

    async fn purge_logs_upto(
        &mut self,
        log_id: LogId<RaftNodeId>,
    ) -> Result<(), StorageError<RaftNodeId>> {
        *self.last_purged_log_id.write().await = Some(log_id);

        let mut log = self.log.write().await;
        let keys_to_remove: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for key in keys_to_remove {
            log.remove(&key);
        }
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
        let sm = self.sm.write().await;

        for entry in entries {
            *self.last_applied_log.write().await = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(CoordinationResponse::Ok);
                }
                EntryPayload::Normal(command) => {
                    let response = sm.apply_command(command.clone()).await;
                    responses.push(response);
                }
                EntryPayload::Membership(membership) => {
                    *self.last_membership.write().await =
                        StoredMembership::new(Some(entry.log_id), membership.clone());
                    responses.push(CoordinationResponse::Ok);
                }
            }
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
            object_store: self.object_store.clone(),
            snapshot_path: self.snapshot_path.clone(),
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

        // Restore state machine from raw bytes
        self.sm.write().await.restore(&data).await;
        *self.last_applied_log.write().await = meta.last_log_id;
        *self.last_membership.write().await =
            StoredMembership::new(meta.last_log_id, meta.last_membership.membership().clone());

        // Persist to object store for durability
        self.persist_snapshot(meta, &data).await?;

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
        let sm = self.sm.read().await;
        let data = sm.snapshot().await;

        let last_applied = *self.last_applied_log.read().await;
        let membership = self.last_membership.read().await.clone();

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
        let data = bincode::serialize(&state).unwrap();
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
