//! Builder for creating [`PartitionStore`] instances.

use super::batch_index::{BatchIndex, DEFAULT_BATCH_INDEX_MAX_SIZE};
use super::producer_state::{ProducerState, DEFAULT_PRODUCER_STATE_CACHE_SIZE};
use super::PartitionStore;
use super::{
    DEFAULT_MAX_FETCH_RESPONSE_SIZE, DURABLE_WRITE_OPTIONS, FAST_WRITE_OPTIONS,
};
use super::super::error::{SlateDBError, SlateDBResult};
use super::super::keys::HIGH_WATERMARK_KEY;
use super::super::partition_recovery::{load_producer_states, recover_hwm_from_records};
use super::super::zombie_mode::ZombieModeState;
use moka::sync::Cache as MokaCache;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use slatedb::Db;
use slatedb::config::{PutOptions, Settings as SlateDbSettings};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{error, info, warn};

/// Builder for creating PartitionStore instances.
///
/// This provides a fluent API for constructing partition stores with
/// various configuration options.
pub struct PartitionStoreBuilder {
    object_store: Option<Arc<dyn ObjectStore>>,
    base_path: Option<String>,
    topic: Option<String>,
    partition: Option<i32>,
    max_fetch_response_size: usize,
    batch_index_max_size: usize,
    producer_state_cache_ttl_secs: u64,
    zombie_mode: Option<Arc<ZombieModeState>>,
    fail_on_recovery_gap: bool,
    min_lease_ttl_for_write_secs: u64,
    /// Leader epoch from Raft for epoch-based fencing.
    /// 0 means epoch validation is disabled.
    leader_epoch: i32,
    /// SlateDB max unflushed bytes before backpressure.
    slatedb_max_unflushed_bytes: usize,
    /// SlateDB L0 SST size in bytes.
    slatedb_l0_sst_size_bytes: usize,
    /// SlateDB flush interval in milliseconds.
    slatedb_flush_interval_ms: u64,
    /// Optional shared block cache. When `Some`, every `Db::builder` is
    /// chained with `.with_memory_cache(cache.clone())`, replacing
    /// SlateDB's per-DB default. When `None`, the per-DB default is
    /// used (today's behaviour).
    slatedb_block_cache: Option<Arc<dyn slatedb::db_cache::DbCache>>,
    /// Optional dedicated compaction runtime handle. When `Some`,
    /// every `Db::builder` is chained with
    /// `.with_compaction_runtime(handle.clone())` so the per-DB
    /// compactor task spawns onto the broker-wide bounded runtime
    /// rather than the ambient runtime.
    slatedb_compaction_handle: Option<tokio::runtime::Handle>,
}

impl Default for PartitionStoreBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionStoreBuilder {
    /// Create a new builder with default values.
    pub fn new() -> Self {
        Self {
            object_store: None,
            base_path: None,
            topic: None,
            partition: None,
            max_fetch_response_size: DEFAULT_MAX_FETCH_RESPONSE_SIZE,
            batch_index_max_size: DEFAULT_BATCH_INDEX_MAX_SIZE,
            producer_state_cache_ttl_secs: 900, // 15 minutes
            zombie_mode: None,
            fail_on_recovery_gap: true,
            min_lease_ttl_for_write_secs: crate::constants::DEFAULT_MIN_LEASE_TTL_FOR_WRITE_SECS,
            leader_epoch: 0, // 0 means epoch validation disabled (backwards compat)
            slatedb_max_unflushed_bytes: 256 * 1024 * 1024, // 256 MB default
            slatedb_l0_sst_size_bytes: 64 * 1024 * 1024, // 64 MB default
            slatedb_flush_interval_ms: 100, // 100ms default
            slatedb_block_cache: None,
            slatedb_compaction_handle: None,
        }
    }

    /// Set the object store.
    pub fn object_store(mut self, store: Arc<dyn ObjectStore>) -> Self {
        self.object_store = Some(store);
        self
    }

    /// Set the base path in the object store.
    pub fn base_path(mut self, path: &str) -> Self {
        self.base_path = Some(path.to_string());
        self
    }

    /// Set the topic name.
    pub fn topic(mut self, topic: &str) -> Self {
        self.topic = Some(topic.to_string());
        self
    }

    /// Set the partition index.
    pub fn partition(mut self, partition: i32) -> Self {
        self.partition = Some(partition);
        self
    }

    /// Set the maximum fetch response size.
    pub fn max_fetch_response_size(mut self, size: usize) -> Self {
        self.max_fetch_response_size = size;
        self
    }

    /// Set the batch index max size.
    pub fn batch_index_max_size(mut self, size: usize) -> Self {
        self.batch_index_max_size = size;
        self
    }

    /// Set the producer state cache TTL in seconds.
    pub fn producer_state_cache_ttl_secs(mut self, secs: u64) -> Self {
        self.producer_state_cache_ttl_secs = secs;
        self
    }

    /// Set the zombie mode state.
    pub fn zombie_mode(mut self, state: Arc<ZombieModeState>) -> Self {
        self.zombie_mode = Some(state);
        self
    }

    /// Set whether to fail on recovery gaps.
    ///
    /// When enabled, if the HWM recovery scan detects gaps in the offset sequence,
    /// the partition will fail to open rather than continuing with potentially
    /// incomplete data.
    pub fn fail_on_recovery_gap(mut self, fail: bool) -> Self {
        self.fail_on_recovery_gap = fail;
        self
    }

    /// Set the minimum lease TTL required for writes (in seconds).
    ///
    /// Writes are rejected if the remaining lease TTL is less than this value,
    /// preventing TOCTOU races where the lease could expire during a write.
    ///
    /// Default: 15 seconds. Minimum recommended: 5 seconds.
    pub fn min_lease_ttl_for_write_secs(mut self, secs: u64) -> Self {
        self.min_lease_ttl_for_write_secs = secs;
        self
    }

    /// Set the leader epoch for epoch-based fencing.
    ///
    /// This epoch is obtained from Raft when acquiring the partition. It is
    /// stored in SlateDB and validated before each write to prevent TOCTOU
    /// races where we might write to a partition we no longer own.
    ///
    /// If the stored epoch in SlateDB is higher than this value, the partition
    /// open will fail (another broker has acquired it).
    ///
    /// Default: 0 (legacy / mock-coordinator path). The open path treats 0 on
    /// a fresh partition as a request to self-claim a floor epoch of 1, so
    /// per-write fencing is *always* armed once the partition is open. (The
    /// previous behavior of `0 = fencing disabled` partitions opened pre-coordinator could write with no
    /// fencing whatsoever, leaving them exposed to TOCTOU regardless of the
    /// per-call check.)
    pub fn leader_epoch(mut self, epoch: i32) -> Self {
        self.leader_epoch = epoch;
        self
    }

    /// Set the maximum unflushed bytes before SlateDB applies backpressure.
    ///
    /// When unflushed data exceeds this limit, writes are paused until
    /// data is flushed to object storage. This prevents OOM conditions
    /// when object store latency spikes.
    ///
    /// Default: 256 MB
    pub fn slatedb_max_unflushed_bytes(mut self, bytes: usize) -> Self {
        self.slatedb_max_unflushed_bytes = bytes;
        self
    }

    /// Set the target size for SlateDB L0 SSTables.
    ///
    /// Memtables are flushed to L0 when they reach this size.
    /// Smaller values mean more frequent flushes.
    ///
    /// Default: 64 MB
    pub fn slatedb_l0_sst_size_bytes(mut self, bytes: usize) -> Self {
        self.slatedb_l0_sst_size_bytes = bytes;
        self
    }

    /// Set the SlateDB flush interval in milliseconds.
    ///
    /// How frequently SlateDB flushes the WAL to object storage.
    ///
    /// Default: 100ms
    pub fn slatedb_flush_interval_ms(mut self, ms: u64) -> Self {
        self.slatedb_flush_interval_ms = ms;
        self
    }

    /// Inject a broker-wide shared block cache.
    ///
    /// One `Arc<dyn DbCache>` is built once at broker startup (see
    /// `super::super::slatedb_resources::SharedSlateDbResources`) and threaded
    /// into every per-partition open via this setter. SlateDB wraps it
    /// in a per-DB scope wrapper internally so all DBs share one
    /// underlying cache while their entries remain distinguishable.
    ///
    /// Pass `Arc::clone(&cache)` for each partition — the inner trait
    /// object is unchanged across opens, so cache hits accumulate
    /// across the broker's lifetime rather than per-partition.
    pub fn slatedb_block_cache(mut self, cache: Arc<dyn slatedb::db_cache::DbCache>) -> Self {
        self.slatedb_block_cache = Some(cache);
        self
    }

    /// Inject a broker-wide dedicated compaction runtime handle.
    ///
    /// All per-`Db` compactor tasks spawn onto this runtime, capping
    /// total compaction parallelism via the runtime's worker count.
    /// Without this setter SlateDB spawns its compactor on the ambient
    /// runtime, which puts compaction CPU bursts on the same threads
    /// as raft heartbeats — a recipe for spurious failovers.
    pub fn slatedb_compaction_handle(mut self, handle: tokio::runtime::Handle) -> Self {
        self.slatedb_compaction_handle = Some(handle);
        self
    }

    /// Build the PartitionStore.
    pub async fn build(self) -> SlateDBResult<PartitionStore> {
        use super::super::keys::{
            CURRENT_FORMAT_VERSION, FORMAT_VERSION_KEY, LEADER_EPOCH_KEY, decode_leader_epoch,
            encode_leader_epoch,
        };

        let object_store = self
            .object_store
            .ok_or_else(|| SlateDBError::Config("object_store is required".to_string()))?;
        let base_path = self
            .base_path
            .ok_or_else(|| SlateDBError::Config("base_path is required".to_string()))?;
        let topic = self
            .topic
            .ok_or_else(|| SlateDBError::Config("topic is required".to_string()))?;
        let partition = self
            .partition
            .ok_or_else(|| SlateDBError::Config("partition is required".to_string()))?;

        // Note: We use a relative path here because the object store is already
        // configured with base_path as its prefix (e.g., LocalFileSystem::new_with_prefix).
        // Including base_path here would cause path doubling.
        let path = format!("topic-{}/partition-{}", topic, partition);

        info!(topic = %topic, partition, path = %path, base_path = %base_path, leader_epoch = self.leader_epoch, "Opening partition store via builder");

        // Clone for the blocking task
        let path_for_task = path.clone();
        let topic_for_task = topic.clone();
        let topic_for_epoch_error = topic.clone();
        let object_store_for_task = object_store;
        let fail_on_gap = self.fail_on_recovery_gap;
        let expected_epoch = self.leader_epoch;
        // Move shared resources into the open future. `Option<Arc<...>>`
        // is `None` when the broker is configured without the shared
        // pool (escape hatch for A/B testing); the `Db::builder` chain
        // below skips the corresponding setter so SlateDB falls back
        // to its per-DB defaults.
        let block_cache = self.slatedb_block_cache.clone();
        let compaction_handle = self.slatedb_compaction_handle.clone();

        // Prepare SlateDB settings with explicit memory limits for backpressure
        let slatedb_settings = SlateDbSettings {
            max_unflushed_bytes: self.slatedb_max_unflushed_bytes,
            l0_sst_size_bytes: self.slatedb_l0_sst_size_bytes,
            flush_interval: Some(Duration::from_millis(self.slatedb_flush_interval_ms)),
            ..SlateDbSettings::default()
        };
        info!(
            max_unflushed_bytes = self.slatedb_max_unflushed_bytes,
            l0_sst_size_bytes = self.slatedb_l0_sst_size_bytes,
            flush_interval_ms = self.slatedb_flush_interval_ms,
            block_cache_shared = block_cache.is_some(),
            compaction_runtime_shared = compaction_handle.is_some(),
            "SlateDB settings configured for backpressure"
        );

        // SlateDB's open path is async-only and tokio-friendly, so a
        // round-trip through a blocking pool would just waste a worker. Run
        // the open inline; the slow step (object-store metadata I/O) yields
        // back to the runtime.
        //
        // Cloned out of `slatedb_settings` before it is moved into
        // `with_settings`: routing compaction onto the shared runtime below
        // means building the `CompactorBuilder` ourselves, and that setter
        // supersedes `Settings::compactor_options` entirely.
        let compactor_options = slatedb_settings.compactor_options.clone();
        let open_future = async move {
            let object_path = ObjectPath::from(path_for_task.as_str());
            let mut db_builder =
                Db::builder(object_path.clone(), Arc::clone(&object_store_for_task))
                    .with_settings(slatedb_settings);
            // Conditional chaining: only set the cache / compaction
            // runtime when broker-wide pools were configured.
            // `None` paths fall through to SlateDB's per-DB defaults
            // (today's behaviour).
            if let Some(cache) = block_cache {
                db_builder = db_builder.with_db_cache(cache);
            }
            // `DbBuilder::with_compaction_runtime` is gone as of slatedb
            // 0.14; the runtime now belongs to the compactor's own builder.
            // Reconstruct what `build()` would have derived from the
            // settings (same path, same main object store, same compactor
            // options) and add the runtime — and only when compaction is
            // configured at all, so a `compactor_options: None` setting
            // still means "no compactor".
            if let (Some(handle), Some(options)) = (compaction_handle, compactor_options) {
                db_builder = db_builder.with_compactor_builder(
                    slatedb::CompactorBuilder::new(object_path, Arc::clone(&object_store_for_task))
                        .with_options(options)
                        .with_runtime(handle),
                );
            }
            let db = db_builder.build().await.map_err(SlateDBError::from)?;

            // ==================================================================
            // FORMAT VERSION: write on first open; reject newer-than-known
            // ==================================================================
            // Future migrations branch on this value. Writing it at the first
            // open of a fresh partition means we never need to forensically
            // guess "is this v0 or v1?" — a missing key uniquely identifies
            // pre-versioning partitions and is treated as v1 (the current).
            match db
                .get(FORMAT_VERSION_KEY)
                .await
                .map_err(SlateDBError::from)?
            {
                Some(bytes) if bytes.len() >= 4 => {
                    let stored = u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
                    if stored > CURRENT_FORMAT_VERSION {
                        error!(
                            stored_format_version = stored,
                            supported_format_version = CURRENT_FORMAT_VERSION,
                            "Partition was written by a newer broker; refusing to open"
                        );
                        return Err(SlateDBError::Config(format!(
                            "Partition format version {} is newer than supported {}",
                            stored, CURRENT_FORMAT_VERSION
                        )));
                    }
                }
                Some(_) | None => {
                    db.put_with_options(
                        FORMAT_VERSION_KEY,
                        &CURRENT_FORMAT_VERSION.to_be_bytes(),
                        &PutOptions::default(),
                        &DURABLE_WRITE_OPTIONS,
                    )
                    .await
                    .map_err(SlateDBError::from)?;
                }
            }

            // ==================================================================
            // EPOCH-BASED FENCING: Validate and store leader epoch
            // ==================================================================
            // This prevents TOCTOU races where we might write to a partition
            // that another broker has already acquired.
            //
            // INVARIANT: After this block, `final_epoch` is always >= 1, so
            // the per-write fencing check (`if self.leader_epoch > 0`) is
            // *always* armed. Previously, partitions opened with
            // `expected_epoch == 0` (legacy / mock-coordinator paths) and a
            // never-set `LEADER_EPOCH_KEY` would land at `final_epoch = 0`
            // and silently skip per-write epoch validation entirely; this
            // line raises that floor.
            let stored_epoch = match db.get(LEADER_EPOCH_KEY).await.map_err(SlateDBError::from)? {
                Some(bytes) => decode_leader_epoch(&bytes).unwrap_or(0),
                None => 0,
            };

            // If we have a non-zero expected epoch, validate it.
            // Gate is `!= 0` (not `> 0`) so a negative on-disk value still
            // fences instead of silently bypassing validation.
            if expected_epoch != 0 {
                if stored_epoch > expected_epoch {
                    // Another broker has already acquired this partition with
                    // a higher epoch. We must not proceed.
                    error!(
                        expected_epoch,
                        stored_epoch,
                        "EPOCH FENCING: Stored epoch is higher than expected - another broker owns this partition"
                    );
                    return Err(SlateDBError::EpochMismatch {
                        topic: topic_for_epoch_error.clone(),
                        partition,
                        expected_epoch,
                        stored_epoch,
                    });
                }

                // Store our epoch to claim ownership
                // This must be durable before we proceed with any writes
                db.put_with_options(
                    LEADER_EPOCH_KEY,
                    &encode_leader_epoch(expected_epoch),
                    &PutOptions::default(),
                    &DURABLE_WRITE_OPTIONS,
                )
                .await
                .map_err(SlateDBError::from)?;

                info!(
                    expected_epoch,
                    stored_epoch, "Epoch fencing: Stored new epoch to SlateDB"
                );
            }

            // Compute the working epoch and ensure the floor invariant.
            //
            // - With a coordinator-issued epoch, we use it.
            // - Without one (legacy or mock paths), we fall back to whatever
            //   was already stored. If neither is set we self-claim epoch 1,
            //   which is safe: SlateDB's single-writer fencing prevents two
            //   simultaneous opens from succeeding, so only one broker can
            //   actually persist the floor value, and any genuine future
            //   coordinator acquire is required to be > stored.
            let mut final_epoch = if expected_epoch > 0 {
                expected_epoch
            } else {
                stored_epoch
            };
            if final_epoch == 0 {
                final_epoch = 1;
                db.put_with_options(
                    LEADER_EPOCH_KEY,
                    &encode_leader_epoch(final_epoch),
                    &PutOptions::default(),
                    &DURABLE_WRITE_OPTIONS,
                )
                .await
                .map_err(SlateDBError::from)?;
                info!(
                    partition,
                    "Epoch fencing: self-claimed floor epoch 1 (no coordinator-issued epoch)"
                );
            }

            // Load persisted high watermark from DB
            let persisted_hwm = match db
                .get(HIGH_WATERMARK_KEY)
                .await
                .map_err(SlateDBError::from)?
            {
                Some(bytes) => {
                    if bytes.len() >= 8 {
                        // Use expect() with descriptive message instead of unwrap().
                        // The length check above guarantees we have 8 bytes.
                        i64::from_be_bytes(
                            bytes[..8]
                                .try_into()
                                .expect("slice of exactly 8 bytes should convert to [u8; 8]"),
                        )
                    } else {
                        0
                    }
                }
                None => 0,
            };

            // Load the persisted log start offset (0 when retention has
            // never deleted a prefix).
            let log_start_offset = match db
                .get(super::super::keys::LOG_START_OFFSET_KEY)
                .await
                .map_err(SlateDBError::from)?
            {
                Some(bytes) if bytes.len() >= 8 => i64::from_be_bytes(
                    bytes[..8]
                        .try_into()
                        .expect("slice of exactly 8 bytes should convert to [u8; 8]"),
                ),
                _ => 0,
            };

            // Scan for highest record to recover from crash. The scan is
            // bounded below by the LSO (records beneath it were deleted by
            // retention) and the checkpointed HWM (SlateDB's WAL ordering
            // means a persisted checkpoint implies every earlier batch
            // persisted too), so open latency tracks recent write volume,
            // not total log size.
            let scan_floor = log_start_offset.max(persisted_hwm);
            let recovery =
                recover_hwm_from_records(&db, persisted_hwm, fail_on_gap, scan_floor).await?;
            let recovered_hwm = recovery.high_watermark;

            // If we recovered a higher HWM, persist it
            if recovered_hwm > persisted_hwm {
                warn!(
                    persisted_hwm,
                    recovered_hwm, "Recovered higher HWM from record scan - persisting"
                );
                db.put_with_options(
                    HIGH_WATERMARK_KEY,
                    &recovered_hwm.to_be_bytes(),
                    &PutOptions::default(),
                    &FAST_WRITE_OPTIONS,
                )
                .await
                .map_err(SlateDBError::from)?;
            }

            // Load persisted producer states for idempotency
            let producer_states = load_producer_states(&db).await?;

            Ok((
                db,
                recovered_hwm,
                log_start_offset,
                recovery.batches,
                producer_states,
                final_epoch,
            ))
        };

        let (db, hwm, log_start_offset, recovered_batches, persisted_states, validated_epoch) =
            open_future.await?;

        if !persisted_states.is_empty() {
            info!(
                topic = %topic_for_task,
                partition,
                producer_count = persisted_states.len(),
                "Recovered producer states for idempotency via builder"
            );
        }
        // Track recovery count metric (even if zero, for observability)
        super::super::metrics::set_producer_state_recovery_count(
            &topic_for_task,
            partition,
            persisted_states.len() as i64,
        );

        info!(topic = %topic_for_task, partition, high_watermark = hwm, leader_epoch = validated_epoch, "Partition store opened via builder");

        // Build producer state cache with size-bounded eviction.
        //
        // Time-to-idle eviction is intentionally NOT configured here. With
        // it on, an idle-but-active producer's cache entry would be evicted
        // before the persisted-state prune horizon and the producer would
        // be re-treated as new on reconnect — silently accepting duplicate
        // sequences. The persisted prune is the only retention mechanism;
        // the in-memory cache exists to avoid repeated point-reads on hot
        // producers and is bounded by `max_capacity`. Operators worried
        // about per-partition memory should tune that capacity.
        let topic_for_eviction = topic.clone();
        let producer_states_cache = MokaCache::builder()
            .max_capacity(DEFAULT_PRODUCER_STATE_CACHE_SIZE)
            .eviction_listener(move |producer_id: Arc<i64>, state: ProducerState, cause| {
                // Log warning when producers with active sequences are evicted.
                // Even with size-only eviction this can happen on hot
                // partitions with many concurrent idempotent producers; the
                // persisted state still gates duplicate acceptance, but a
                // sustained warning rate is operator signal that the cache
                // is undersized relative to the producer fleet.
                if state.last_sequence > 0 {
                    warn!(
                        topic = %topic_for_eviction,
                        partition,
                        producer_id = *producer_id,
                        last_sequence = state.last_sequence,
                        producer_epoch = state.producer_epoch,
                        eviction_cause = ?cause,
                        "Producer state evicted from cache (size-bounded); persisted state still authoritative"
                    );
                    super::super::metrics::record_producer_state_eviction(&topic_for_eviction, partition, true);
                } else {
                    super::super::metrics::record_producer_state_eviction(&topic_for_eviction, partition, false);
                }
            })
            .build();

        // Populate the cache with persisted producer states, including the
        // persisted retry-dedup pair so an exact retry of the last acked
        // batch is re-acked with its original offset across restarts.
        for (producer_id, persisted) in persisted_states {
            producer_states_cache.insert(
                producer_id,
                ProducerState {
                    last_sequence: persisted.last_sequence,
                    producer_epoch: persisted.producer_epoch,
                    last_first_sequence: persisted.last_first_sequence,
                    last_base_offset: persisted.last_base_offset,
                },
            );
        }

        // Build batch index (BTreeMap behind RwLock; supports range queries).
        let batch_index = BatchIndex::new(self.batch_index_max_size);

        let store = PartitionStore {
            db,
            high_watermark: AtomicI64::new(hwm),
            next_offset: AtomicI64::new(hwm),
            log_start_offset: AtomicI64::new(log_start_offset),
            appends_since_checkpoint: std::sync::atomic::AtomicU64::new(1),
            write_lock: Mutex::new(()),
            topic,
            partition,
            batch_index,
            max_fetch_response_size: AtomicUsize::new(self.max_fetch_response_size),
            batch_index_max_size: self.batch_index_max_size,
            zombie_mode: self.zombie_mode,
            producer_states: producer_states_cache,
            min_lease_ttl_for_write_secs: self.min_lease_ttl_for_write_secs,
            load_collector: arc_swap::ArcSwapOption::from(None),
            leader_epoch: validated_epoch,
            close_once: tokio::sync::OnceCell::new(),
            opened_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
            // 0 sentinel: lease unknown until the partition manager
            // populates it on the first cache hit / renewal. The
            // append-time TOCTOU re-check treats 0 as "skip" so opening a
            // store doesn't reject the very first write before the manager
            // gets a chance to record the lease.
            lease_expiry_ms: AtomicI64::new(0),
            append_failed: AtomicBool::new(false),
            prom_counters: std::sync::OnceLock::new(),
        };

        // Warm the batch index cache from the recovery scan (one storage
        // pass instead of two).
        store.warm_batch_index_from(&recovered_batches);

        Ok(store)
    }
}

