//! Partition store wrapping SlateDB for a single partition.
//!
//! Each partition gets its own SlateDB instance, stored at a unique path
//! in the object store (e.g., `s3://bucket/topic-X/partition-Y/`).
//!
//! # Memory Scaling
//!
//! Each partition maintains an in-memory batch index for efficient offset lookup.
//! The memory usage per broker scales with the number of partitions owned:
//!
//! ```text
//! Memory = batch_index_max_size * 16 bytes * num_partitions
//!        = 10,000 * 16 * P
//!        = ~160 KB per partition
//!        = ~160 MB for 1,000 partitions
//! ```
//!
//! This is a per-broker limit, not global. To reduce memory usage:
//! - Decrease `batch_index_max_size` in `ClusterConfig`
//! - Distribute partitions across more brokers
//!
//! The batch index uses LRU eviction when the limit is reached, so older
//! entries are removed first. This may cause additional SlateDB lookups
//! for older offsets but bounds memory growth.
//!
//! A future enhancement could use a shared moka cache across all partitions
//! with a global memory budget.
//!
//! # Idempotency
//!
//! The producer state cache tracks (last_sequence, epoch) per producer_id to
//! detect and reject duplicate or out-of-order messages. This provides
//! exactly-once semantics for idempotent producers.

use bytes::{Bytes, BytesMut};
use moka::sync::Cache as MokaCache;
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use slatedb::Db;
use slatedb::WriteBatch;
use slatedb::config::{PutOptions, Settings as SlateDbSettings, WriteOptions};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;

use super::load_metrics::LoadMetricsCollector;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

/// Write options for high-throughput non-blocking writes.
/// Uses await_durable=false to avoid blocking on each WAL flush.
/// Durability is provided by SlateDB's periodic flush_interval (default 100ms).
/// For crash recovery tests, call flush() explicitly before simulated crash.
const FAST_WRITE_OPTIONS: WriteOptions = WriteOptions {
    await_durable: false,
};

use super::error::{SlateDBError, SlateDBResult};
use super::keys::{
    HIGH_WATERMARK_KEY, encode_producer_state_key, encode_producer_state_value, encode_record_key,
    parse_record_count, patch_base_offset,
};
use super::partition_recovery::{load_producer_states, recover_hwm_from_records};
use super::zombie_mode::ZombieModeState;
use crate::protocol::parse_producer_info;

/// Default maximum response size for fetch operations (1 MB).
/// This limits memory usage when collecting batches for a single fetch response.
/// Can be overridden via ClusterConfig.max_fetch_response_size.
const DEFAULT_MAX_FETCH_RESPONSE_SIZE: usize = 1024 * 1024;

/// Default maximum number of batch boundaries to cache per partition.
/// Keeps memory bounded while providing efficient offset lookup.
/// Can be overridden via ClusterConfig.batch_index_max_size.
const DEFAULT_BATCH_INDEX_MAX_SIZE: usize = 10_000;

/// Default maximum number of producers to track per partition.
/// Bounds memory usage for the producer state cache.
const DEFAULT_PRODUCER_STATE_CACHE_SIZE: u64 = 10_000;

/// Checkpoint the `_hwm` key every N appended batches.
///
/// The HWM is embedded in every batch value, so the standalone `_hwm` key is
/// purely an optimization: it bounds the recovery scan on open to the batches
/// appended since the last checkpoint instead of the whole log. Every 64th
/// append piggybacks the checkpoint on the batch's own atomic `WriteBatch`,
/// so it costs no extra storage round-trip.
const HWM_CHECKPOINT_INTERVAL_BATCHES: u64 = 64;

/// Initial back-scan window (in offsets) used by `find_batch_start` when the
/// batch index misses. The window doubles until a covering batch is found or
/// the log start offset is reached, so lookups are O(window) reads instead of
/// O(partition size) scans from offset 0.
const INITIAL_BATCH_BACKSCAN_WINDOW: i64 = 4096;

/// State tracked per producer for idempotency checks.
///
/// This tracks the last sequence number and epoch for each producer_id,
/// enabling detection of duplicate or out-of-order messages.
#[derive(Debug, Clone, Copy)]
pub struct ProducerState {
    /// Last successfully written sequence number for this producer.
    pub last_sequence: i32,
    /// Producer epoch for fencing zombie producers.
    pub producer_epoch: i16,
    /// First sequence number of the most recent successfully appended batch.
    /// Used together with `last_base_offset` to recognize a duplicate retry
    /// of *the same* batch and reply with success-and-original-offset, as
    /// Kafka's idempotent-producer contract requires.
    /// Persisted atomically with each batch so retry dedup survives restart;
    /// -1 when unknown (legacy persisted values).
    pub last_first_sequence: i32,
    /// Base offset assigned to the most recent successfully appended batch.
    /// Persisted with each batch; -1 when unknown.
    pub last_base_offset: i64,
}

/// Wrapper around SlateDB for a single Kafka partition.
pub struct PartitionStore {
    /// The SlateDB instance.
    db: Db,

    /// Cached high watermark (also persisted in DB).
    high_watermark: AtomicI64,

    /// Next offset to allocate for an in-flight write.
    ///
    /// Always >= `high_watermark`. The split exists for cancellation safety
    /// (audit R-2): writers `fetch_add` here BEFORE the SlateDB write so a
    /// cancelled future cannot lead the next caller to reuse the same
    /// `base_offset`. `high_watermark` (the reader-visible bound) only
    /// advances after a successful durable write, so consumers never see
    /// uncommitted offsets — the worst case from a cancelled write is a gap
    /// in the offset range, recoverable by the producer's retry.
    next_offset: AtomicI64,

    /// Cached log start offset (also persisted under `_lso`).
    ///
    /// Starts at 0 and only advances when retention deletes a log prefix.
    /// Kept in memory so `earliest_offset()` is a load instead of a storage
    /// scan — the fetch path consults it for every partition on every pass.
    log_start_offset: AtomicI64,

    /// Appends since the last `_hwm` checkpoint (drives the periodic
    /// checkpoint that bounds the recovery scan on open).
    appends_since_checkpoint: std::sync::atomic::AtomicU64,

    /// Write lock to ensure atomic append operations.
    write_lock: Mutex<()>,

    /// Topic name.
    topic: String,

    /// Partition index.
    partition: i32,

    /// Index of batch boundaries: base_offset -> record_count.
    /// Used to efficiently find batches containing a given offset.
    /// This is an optimization to avoid repeated SlateDB lookups during fetch.
    /// Uses moka concurrent cache for lock-free reads (performance optimization).
    batch_index: MokaCache<i64, i32>,

    /// Maximum response size for fetch operations (configurable).
    /// Limits memory usage when collecting batches for a single fetch response.
    max_fetch_response_size: AtomicUsize,

    /// Maximum batch index entries (configurable).
    /// Make batch index size configurable.
    batch_index_max_size: usize,

    /// Zombie mode flag shared with PartitionManager.
    /// When set, the broker has lost cluster coordination and writes should be rejected.
    /// This provides an additional safety check beyond SlateDB fencing.
    zombie_mode: Option<Arc<ZombieModeState>>,

    /// Producer state cache for idempotency checks.
    /// Maps producer_id -> ProducerState (last_sequence, epoch).
    /// Uses moka with TTL for automatic eviction of inactive producers.
    producer_states: MokaCache<i64, ProducerState>,

    /// Minimum remaining lease TTL (in seconds) required to allow writes.
    /// Writes are rejected if lease has less than this remaining to prevent
    /// TOCTOU races where the lease could expire during a write.
    min_lease_ttl_for_write_secs: u64,

    /// Load metrics collector for auto-balancing.
    /// Records bytes/messages produced and fetched for this partition.
    /// Shared across all partitions via Arc for aggregation.
    ///
    /// Hot read path: every successful append/fetch records bytes here, so
    /// the read must be allocation- and lock-free. `ArcSwapOption` gives a
    /// lock-free `load()` that returns a refcount-bumped guard; the guard
    /// dereferences to `Option<&Arc<...>>` so we never block on a read lock
    /// or hold a tokio guard across an `await`.
    load_collector: arc_swap::ArcSwapOption<LoadMetricsCollector>,

    /// Leader epoch for epoch-based fencing (TOCTOU prevention).
    ///
    /// This epoch is obtained from Raft when acquiring the partition and stored
    /// in SlateDB. Before each write, we verify the stored epoch matches our
    /// expected epoch. If another broker acquired the partition (incrementing
    /// the epoch), our writes will be rejected.
    ///
    /// Value of 0 indicates epoch validation is disabled (for backwards compat
    /// or mock coordinators that don't track epochs).
    leader_epoch: i32,

    /// Guards the underlying `Db::close()` call so concurrent `close()` callers
    /// (release_partition, zombie-entry, lease-loss, shutdown — all of which
    /// can race) don't double-close SlateDB and panic in compaction.
    ///
    /// `OnceCell` semantics: the first caller runs the close and sets the
    /// result; later callers see the cell is initialized and return immediately
    /// without re-entering SlateDB.
    close_once: tokio::sync::OnceCell<()>,
}

impl PartitionStore {
    /// Open or create a partition store.
    ///
    /// # SlateDB Workaround
    ///
    /// Uses `spawn_blocking` internally because SlateDB 0.3.x's `open` future
    /// contains `Rc` and is not Send-safe. This is a known limitation:
    /// - SlateDB uses `Rc<SsTableHandle>` in its iterator implementation
    /// - This makes the future returned by `Db::open_with_opts` non-Send
    /// - We work around this by running the open in a blocking context
    ///
    /// This workaround will be removed when SlateDB fixes this issue.
    /// Track: https://github.com/slatedb/slatedb/issues
    ///
    /// # Recovery
    ///
    /// On recovery, we scan for the highest record key to derive the true high watermark,
    /// ensuring we don't lose records that were written but whose HWM update was lost.
    pub async fn open(
        object_store: Arc<dyn ObjectStore>,
        base_path: &str,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<Self> {
        Self::open_with_config(
            object_store,
            base_path,
            topic,
            partition,
            DEFAULT_MAX_FETCH_RESPONSE_SIZE,
            true, // fail_on_recovery_gap: default to true (refuse to open on confirmed gap)
        )
        .await
    }

    /// Open or create a partition store with configurable fetch response size.
    ///
    /// # Arguments
    /// * `object_store` - Object store for data persistence
    /// * `base_path` - Base path in object store
    /// * `topic` - Topic name
    /// * `partition` - Partition index
    /// * `max_fetch_response_size` - Maximum bytes to return in a single fetch
    /// * `fail_on_recovery_gap` - If true, fail to open if offset gaps are detected
    ///
    /// # Topic Validation
    /// Topic validation should be done at handler layer. This method assumes
    /// the topic name has already been validated.
    ///
    /// # Note
    /// This method delegates to the builder pattern for implementation consolidation.
    /// For more configuration options, use `PartitionStore::builder()` directly.
    pub async fn open_with_config(
        object_store: Arc<dyn ObjectStore>,
        base_path: &str,
        topic: &str,
        partition: i32,
        max_fetch_response_size: usize,
        fail_on_recovery_gap: bool,
    ) -> SlateDBResult<Self> {
        Self::builder()
            .object_store(object_store)
            .base_path(base_path)
            .topic(topic)
            .partition(partition)
            .max_fetch_response_size(max_fetch_response_size)
            .fail_on_recovery_gap(fail_on_recovery_gap)
            .build()
            .await
    }

    /// Open or create a partition store with zombie mode detection.
    ///
    /// This is the preferred constructor for production use. It accepts a shared
    /// zombie mode flag from the PartitionManager, enabling the store to reject
    /// writes when the broker has lost cluster coordination.
    ///
    /// # Arguments
    /// * `object_store` - Object store for data persistence
    /// * `base_path` - Base path in object store
    /// * `topic` - Topic name
    /// * `partition` - Partition index
    /// * `max_fetch_response_size` - Maximum bytes to return in a single fetch
    /// * `zombie_mode` - Shared zombie mode state indicating broker coordination status
    /// * `fail_on_recovery_gap` - If true, fail to open if offset gaps are detected
    pub async fn open_with_zombie_flag(
        object_store: Arc<dyn ObjectStore>,
        base_path: &str,
        topic: &str,
        partition: i32,
        max_fetch_response_size: usize,
        zombie_mode: Arc<ZombieModeState>,
        fail_on_recovery_gap: bool,
    ) -> SlateDBResult<Self> {
        let mut store = Self::open_with_config(
            object_store,
            base_path,
            topic,
            partition,
            max_fetch_response_size,
            fail_on_recovery_gap,
        )
        .await?;
        store.zombie_mode = Some(zombie_mode);
        Ok(store)
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }

    pub fn high_watermark(&self) -> i64 {
        self.high_watermark.load(Ordering::SeqCst)
    }

    /// The log start offset: the lowest offset still present in the log.
    /// 0 until retention deletes a prefix.
    pub fn log_start_offset(&self) -> i64 {
        self.log_start_offset.load(Ordering::SeqCst)
    }

    /// Minimum remaining lease TTL (seconds) the store requires for new writes.
    pub fn min_lease_ttl_for_write_secs(&self) -> u64 {
        self.min_lease_ttl_for_write_secs
    }

    /// Set the load metrics collector for this partition.
    ///
    /// The collector is shared across all partitions via Arc, enabling
    /// aggregated load statistics for auto-balancing decisions.
    pub fn set_load_collector(&self, collector: Arc<LoadMetricsCollector>) {
        self.load_collector.store(Some(collector));
    }

    /// Drop this partition's metrics from the shared collector so the
    /// per-partition `DashMap` doesn't grow unboundedly as ownership
    /// churns. Called from `release_partition` before the store is closed.
    pub fn clear_load_metrics(&self) {
        if let Some(collector) = self.load_collector.swap(None) {
            collector.clear_partition(&self.topic, self.partition);
        }
    }

    /// Record a produce operation in the load metrics.
    fn record_produce_metrics(&self, bytes: u64, messages: u64) {
        if let Some(collector) = self.load_collector.load_full() {
            collector.record_produce(&self.topic, self.partition, bytes, messages);
        }
    }

    /// Record a fetch operation in the load metrics.
    fn record_fetch_metrics(&self, bytes: u64, messages: u64) {
        if let Some(collector) = self.load_collector.load_full() {
            collector.record_fetch(&self.topic, self.partition, bytes, messages);
        }
    }

    /// Append a record batch to this partition.
    ///
    /// Returns the base offset of the appended batch.
    /// Uses a mutex to ensure atomic offset allocation.
    ///
    /// HWM is now embedded in the batch value to ensure
    /// atomicity. If the batch write succeeds but we crash before updating HWM,
    /// recovery will find the HWM from the batch itself.
    ///
    /// # Errors
    ///
    /// Returns `SlateDBError::Fenced` if another writer has taken over this partition.
    /// The caller should release ownership and stop writing to this partition.
    ///
    /// Returns `SlateDBError::NotOwned` if the broker is in zombie mode (lost cluster
    /// coordination). This is a safety check to prevent writes during split-brain.
    ///
    /// Returns `SlateDBError::EpochMismatch` if the stored epoch in SlateDB doesn't
    /// match our expected epoch, indicating another broker has acquired this partition.
    ///
    /// # Throughput ceiling
    ///
    /// Per-partition produce throughput is serialized on [`write_lock`] and,
    /// for [`append_batch_durable`] / `acks>=1`, bounded by the SlateDB
    /// durable round-trip latency. Offset allocation, epoch verification, and
    /// the storage write all happen under the same lock — durability is not
    /// pipelined because the fencing model requires the epoch check to
    /// precede every append atomically.
    pub async fn append_batch(&self, records: &Bytes) -> SlateDBResult<i64> {
        self.append_batch_inner(records, false).await
    }

    /// Append a record batch and wait for SlateDB to confirm durability before
    /// returning. Required for `acks>=1` to honor Kafka's ack contract — the
    /// fast `append_batch` path can lose acknowledged data in the ~100ms WAL
    /// flush window if the broker dies between ack and flush.
    pub async fn append_batch_durable(&self, records: &Bytes) -> SlateDBResult<i64> {
        self.append_batch_inner(records, true).await
    }

    async fn append_batch_inner(&self, records: &Bytes, durable: bool) -> SlateDBResult<i64> {
        use super::keys::{LEADER_EPOCH_KEY, decode_leader_epoch};

        // Check zombie mode BEFORE acquiring write lock
        // This prevents writes when the broker has lost cluster coordination
        if let Some(ref zombie_state) = self.zombie_mode
            && zombie_state.is_active()
        {
            error!(
                topic = %self.topic,
                partition = self.partition,
                "Rejecting write: broker is in zombie mode (lost cluster coordination)"
            );
            return Err(SlateDBError::NotOwned {
                topic: self.topic.clone(),
                partition: self.partition,
            });
        }

        // ==================================================================
        // EPOCH-BASED FENCING: read epoch BEFORE acquiring the write lock.
        // ==================================================================
        // The previous shape held the per-partition write_lock across this
        // SlateDB get plus the actual durable write — every concurrent
        // producer to the partition serialized end-to-end on object-store
        // latency. Reading the epoch outside the lock lets concurrent
        // writers parallelize the epoch fetch; the actual write below
        // remains atomic via SlateDB's own single-writer fencing.
        //
        // SAFETY: An adversarial broker bumping the epoch between this read
        // and our write is detected by SlateDB at `write_with_options` time,
        // which fails with `is_fenced()`. The check below provides an
        // earlier reject for the common case (cleaner error code, avoids
        // building the WriteBatch) without being load-bearing for safety.
        let prefetched_epoch: Option<i32> = if self.leader_epoch > 0 {
            match self.db.get(LEADER_EPOCH_KEY).await {
                Ok(Some(bytes)) => Some(decode_leader_epoch(&bytes).unwrap_or(0)),
                Ok(None) => Some(0),
                Err(e) => {
                    let err = SlateDBError::from(e);
                    if err.is_fenced() {
                        error!(
                            topic = %self.topic,
                            partition = self.partition,
                            "FENCED during epoch check"
                        );
                        return Err(err);
                    }
                    // SAFETY: Do NOT fall back to cached epoch on storage errors.
                    // If we cannot verify the epoch, we must fail the write to prevent
                    // split-brain scenarios where:
                    // 1. Storage is temporarily unavailable (e.g., network partition)
                    // 2. Another broker acquires partition and writes new epoch
                    // 3. We fall back to cached (stale) epoch and write anyway
                    // 4. Both brokers write to same partition = data corruption
                    error!(
                        topic = %self.topic,
                        partition = self.partition,
                        error = %err,
                        "Failed to read epoch from storage - rejecting write for safety"
                    );
                    return Err(SlateDBError::Storage(format!(
                        "Cannot verify epoch for {}/{}: {}",
                        self.topic, self.partition, err
                    )));
                }
            }
        } else {
            None
        };

        // Acquire write lock to ensure atomic offset allocation
        let _guard = self.write_lock.lock().await;

        // Re-check zombie mode AFTER acquiring lock (double-check pattern)
        // A broker could enter zombie mode while we were waiting for the lock
        if let Some(ref zombie_state) = self.zombie_mode
            && zombie_state.is_active()
        {
            error!(
                topic = %self.topic,
                partition = self.partition,
                "Rejecting write: broker entered zombie mode while waiting for lock"
            );
            return Err(SlateDBError::NotOwned {
                topic: self.topic.clone(),
                partition: self.partition,
            });
        }

        // Pure in-memory compare against the prefetched epoch — no I/O held
        // under the lock. SlateDB's own fence enforcement catches any race
        // between this check and the actual write below.
        if let Some(stored_epoch) = prefetched_epoch
            && stored_epoch != self.leader_epoch
        {
            error!(
                topic = %self.topic,
                partition = self.partition,
                expected_epoch = self.leader_epoch,
                stored_epoch,
                "EPOCH MISMATCH: Another broker has acquired this partition"
            );
            super::metrics::record_epoch_mismatch(&self.topic, self.partition);
            return Err(SlateDBError::EpochMismatch {
                topic: self.topic.clone(),
                partition: self.partition,
                expected_epoch: self.leader_epoch,
                stored_epoch,
            });
        }

        // Reject invalid record counts BEFORE we reserve any offsets — a
        // bumped `next_offset` followed by an early-return would create a
        // permanent offset gap for a request that was always going to fail.
        let record_count = parse_record_count(records);
        if record_count <= 0 {
            error!(
                topic = %self.topic,
                partition = self.partition,
                record_count,
                "Rejecting batch with invalid record count"
            );
            return Err(SlateDBError::Config(format!(
                "Invalid record count: {}",
                record_count
            )));
        }

        // RESERVE the offset range for this batch BEFORE the SlateDB await.
        //
        // Cancellation safety (audit R-2): if we read the offset from
        // `high_watermark` and *then* awaited the SlateDB write, a cancelled
        // future would leave `high_watermark` unchanged and the next caller
        // would reuse the same `base_offset`. SlateDB's queued cancelled
        // write may still eventually persist at that offset — clobbered by
        // the second caller's batch at the same key.
        //
        // Bumping `next_offset` here makes that impossible: every caller
        // gets a unique offset range, even across cancellation. The reader-
        // visible `high_watermark` only advances on a successful durable
        // write below, so consumers never see uncommitted offsets — the
        // failure mode degrades to a gap in the offset range, which the
        // producer recovers from by retrying (idempotent producers de-dup
        // by sequence number; non-idempotent producers accept Kafka's
        // standard "may duplicate on error" contract).
        let base_offset = self.next_offset.fetch_add(record_count as i64, Ordering::SeqCst);

        // Parse producer info once; reused by both the idempotency check here
        // and the producer-state persistence below (previously parsed twice
        // per append).
        let idempotent_producer_info = parse_producer_info(records).filter(|i| i.is_idempotent());

        // Idempotency check: detects and rejects duplicate or out-of-order messages.
        // For *exact-replay* duplicates we return the original base_offset (success)
        // rather than DuplicateSequence so retries that the network ate
        // don't break the producer.
        //
        // On an in-memory cache miss we consult the persisted `p<producer_id>`
        // key before treating the producer as new. Without this, idle-producer
        // cache eviction (time_to_idle TTL) silently accepted duplicate
        // batches as new — an idempotence violation.
        let mut idempotent_dup_offset: Option<i64> = None;
        let cached_producer_state = match &idempotent_producer_info {
            Some(info) => match self.producer_states.get(&info.producer_id) {
                Some(state) => Some(state),
                None => self.load_persisted_producer_state(info.producer_id).await?,
            },
            None => None,
        };
        if let Some(producer_info) = idempotent_producer_info
            && let Some(state) = cached_producer_state
        {
            // Check for epoch fencing (zombie producer detection)
            if producer_info.producer_epoch < state.producer_epoch {
                warn!(
                    topic = %self.topic,
                    partition = self.partition,
                    producer_id = producer_info.producer_id,
                    batch_epoch = producer_info.producer_epoch,
                    current_epoch = state.producer_epoch,
                    "Rejecting batch from fenced producer (stale epoch)"
                );
                super::metrics::record_idempotency_rejection("fenced_epoch");
                // Roll back the reservation; no SlateDB write happened.
                self.next_offset.store(base_offset, Ordering::SeqCst);
                return Err(SlateDBError::FencedProducer {
                    producer_id: producer_info.producer_id,
                    expected_epoch: state.producer_epoch,
                    actual_epoch: producer_info.producer_epoch,
                });
            }

            // Higher epoch indicates new producer incarnation. Kafka's contract
            // is that the producer resets its sequence to 0 on epoch bump, so
            // we MUST require first_sequence == 0 here. Without
            // this gate, a higher-epoch batch with an arbitrary replayed
            // sequence would be accepted as fresh.
            if producer_info.producer_epoch > state.producer_epoch {
                if producer_info.first_sequence != 0 {
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        producer_id = producer_info.producer_id,
                        new_epoch = producer_info.producer_epoch,
                        first_sequence = producer_info.first_sequence,
                        "Rejecting batch on epoch bump: first_sequence must be 0"
                    );
                    super::metrics::record_idempotency_rejection("out_of_order");
                    self.next_offset.store(base_offset, Ordering::SeqCst);
                    return Err(SlateDBError::OutOfOrderSequence {
                        producer_id: producer_info.producer_id,
                        expected_sequence: 0,
                        received_sequence: producer_info.first_sequence,
                    });
                }
                debug!(
                    topic = %self.topic,
                    partition = self.partition,
                    producer_id = producer_info.producer_id,
                    old_epoch = state.producer_epoch,
                    new_epoch = producer_info.producer_epoch,
                    "Producer epoch increased, resetting sequence tracking"
                );
            } else {
                // Use checked_add to detect overflow
                let expected_seq = match state.last_sequence.checked_add(1) {
                    Some(seq) => seq,
                    None => {
                        // Sequence numbers exhausted - producer should get a new producer_id
                        warn!(
                            topic = %self.topic,
                            partition = self.partition,
                            producer_id = producer_info.producer_id,
                            last_sequence = state.last_sequence,
                            "Sequence number overflow detected"
                        );
                        self.next_offset.store(base_offset, Ordering::SeqCst);
                        return Err(SlateDBError::SequenceOverflow {
                            producer_id: producer_info.producer_id,
                            topic: self.topic.clone(),
                            partition: self.partition,
                        });
                    }
                };

                if producer_info.first_sequence <= state.last_sequence {
                    // Exact-replay of the most recent batch: return the cached
                    // base_offset as success rather than DuplicateSequence —
                    // matches Kafka's idempotent producer contract. Older or
                    // partial replays still fall through to the error below.
                    if state.last_first_sequence == producer_info.first_sequence
                        && state.last_base_offset >= 0
                    {
                        debug!(
                            topic = %self.topic,
                            partition = self.partition,
                            producer_id = producer_info.producer_id,
                            first_sequence = producer_info.first_sequence,
                            cached_offset = state.last_base_offset,
                            "Returning cached base_offset for duplicate retry"
                        );
                        super::metrics::record_idempotency_rejection("duplicate_idempotent_ok");
                        idempotent_dup_offset = Some(state.last_base_offset);
                    } else {
                        warn!(
                            topic = %self.topic,
                            partition = self.partition,
                            producer_id = producer_info.producer_id,
                            first_sequence = producer_info.first_sequence,
                            last_seen = state.last_sequence,
                            "Rejecting duplicate batch (no cached offset for retry)"
                        );
                        super::metrics::record_idempotency_rejection("duplicate");
                        // Roll back our reservation — no SlateDB write
                        // happened, no offset gap should remain.
                        self.next_offset.store(base_offset, Ordering::SeqCst);
                        return Err(SlateDBError::DuplicateSequence {
                            producer_id: producer_info.producer_id,
                            expected_sequence: expected_seq,
                            received_sequence: producer_info.first_sequence,
                        });
                    }
                }

                if idempotent_dup_offset.is_none() && producer_info.first_sequence != expected_seq {
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        producer_id = producer_info.producer_id,
                        first_sequence = producer_info.first_sequence,
                        expected = expected_seq,
                        "Rejecting out-of-order batch"
                    );
                    super::metrics::record_idempotency_rejection("out_of_order");
                    // Roll back the reservation — no SlateDB write happened.
                    self.next_offset.store(base_offset, Ordering::SeqCst);
                    return Err(SlateDBError::OutOfOrderSequence {
                        producer_id: producer_info.producer_id,
                        expected_sequence: expected_seq,
                        received_sequence: producer_info.first_sequence,
                    });
                }
            }
        }

        // If this turned out to be a duplicate retry of the most recent batch,
        // return the cached base_offset without writing again. The records are
        // already durable from the first append.
        //
        // Roll back our `next_offset` reservation: we're not writing, so the
        // offsets we reserved should be returned to the pool. Safe under the
        // write_lock because no other writer can have advanced past us.
        if let Some(cached_offset) = idempotent_dup_offset {
            self.next_offset.store(base_offset, Ordering::SeqCst);
            return Ok(cached_offset);
        }

        let new_hwm = (record_count as i64)
            .checked_add(base_offset)
            .ok_or_else(|| {
                SlateDBError::Config(format!(
                    "HWM overflow: base_offset={} + record_count={} would overflow i64",
                    base_offset, record_count
                ))
            })?;

        // Build value with metadata using pooled buffer to reduce allocations.
        // Format: [new_hwm: i64][record_batch: bytes]
        // This ensures HWM is stored atomically with the batch.
        //
        // PERFORMANCE: Using get_buffer/return_buffer pattern instead of with_batch_buffer
        // to avoid clone() across async boundary. The buffer is borrowed for the SlateDB
        // write and returned to the pool afterward, eliminating allocation overhead.
        let mut buffer = super::buffer_pool::get_buffer(8 + records.len());
        buffer.extend_from_slice(&new_hwm.to_be_bytes());
        buffer.extend_from_slice(records);

        // Patch base_offset in record batch (at offset 8, where the batch starts)
        patch_base_offset(&mut buffer[8..], base_offset);

        // For acks>=1 we must wait for SlateDB durability before acking the
        // producer; otherwise an OOM-kill or rolling restart inside the WAL
        // flush window silently loses already-acknowledged data.
        let write_options = if durable {
            WriteOptions {
                await_durable: true,
            }
        } else {
            FAST_WRITE_OPTIONS
        };
        let key = encode_record_key(base_offset);

        // Pull producer-state metadata up-front so we can atomically commit it
        // alongside the record batch. Persisting after the batch
        // write let the two diverge: a persist failure either rejected an
        // already-committed append or silently succeeded non-durably, both of
        // which break idempotency across restart.
        //
        // The persisted value includes the retry-dedup pair
        // (last_first_sequence / last_base_offset) so an exact retry of this
        // batch is re-acked with its original offset even across a restart.
        let pending_producer_state = idempotent_producer_info.map(|info| {
            let last_sequence = info.last_sequence().unwrap_or(info.first_sequence);
            let key = encode_producer_state_key(info.producer_id);
            let value = encode_producer_state_value(&super::keys::PersistedProducerState {
                last_sequence,
                producer_epoch: info.producer_epoch,
                last_first_sequence: info.first_sequence,
                last_base_offset: base_offset,
            });
            (info, last_sequence, key, value)
        });

        // Periodically checkpoint `_hwm` inside the same atomic WriteBatch.
        // The checkpoint bounds the recovery scan on the next open to batches
        // appended after it (SlateDB's WAL is ordered, so a persisted
        // checkpoint implies every earlier batch is persisted too).
        let checkpoint_hwm = self
            .appends_since_checkpoint
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(HWM_CHECKPOINT_INTERVAL_BATCHES);

        let mut batch = WriteBatch::new();
        batch.put(key.as_slice(), buffer.as_slice());
        if let Some((_, _, ps_key, ps_value)) = &pending_producer_state {
            batch.put(ps_key.as_slice(), ps_value.as_slice());
        }
        if checkpoint_hwm {
            batch.put(HIGH_WATERMARK_KEY, new_hwm.to_be_bytes());
        }
        let write_result = self.db.write_with_options(batch, &write_options).await;

        // Return buffer to pool BEFORE handling result (ensures buffer is always returned)
        super::buffer_pool::return_buffer(buffer);

        if let Err(e) = write_result {
            let err = SlateDBError::from(e);

            // Track object store health on failure
            // This helps detect partial network partitions where broker can reach
            // Raft (for coordination) but not the object store (for data I/O)
            let still_healthy = super::metrics::track_object_store_health(false);
            if !still_healthy {
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    consecutive_failures = super::metrics::object_store_consecutive_failures(),
                    "PARTIAL NETWORK PARTITION DETECTED: Object store unreachable. \
                     Broker may need to release partitions."
                );
            }

            if err.is_fenced() {
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    "FENCED: Another writer has taken over this partition. Releasing ownership."
                );
            } else {
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    error = %err,
                    "Failed to append batch"
                );
            }
            return Err(err);
        }

        // Track object store health on success
        super::metrics::track_object_store_health(true);

        // Update high watermark in memory
        // If we crash here, recovery will read HWM from the batch metadata
        self.high_watermark.store(new_hwm, Ordering::SeqCst);

        // PERFORMANCE: Removed separate HWM persistence from hot path.
        // The HWM is already embedded in the record batch value (first 8 bytes),
        // and recovery scans for the highest record key to derive the true HWM.
        // This eliminates one SlateDB put operation per write (~33% reduction).
        // The separate HWM key is only updated during periodic maintenance.

        // NOTE: No explicit flush() in production for performance.
        // FAST_WRITE_OPTIONS uses await_durable=false for throughput.
        // Durability is provided by SlateDB's periodic flush_interval (default 100ms).
        // Crash recovery tests should call flush() explicitly before simulated crash.

        // Add to batch index for efficient lookup during fetch
        self.add_to_batch_index(base_offset, record_count);

        // Update producer state cache after successful atomic write. The
        // producer-state key was already written in the same WriteBatch
        // above, so we don't need a separate persist call.
        if let Some((producer_info, last_sequence, _, _)) = pending_producer_state {
            // Update in-memory cache
            self.producer_states.insert(
                producer_info.producer_id,
                ProducerState {
                    last_sequence,
                    producer_epoch: producer_info.producer_epoch,
                    last_first_sequence: producer_info.first_sequence,
                    last_base_offset: base_offset,
                },
            );

            // Proactive sequence number monitoring
            // Track sequence numbers and alert when approaching exhaustion
            super::metrics::record_sequence_number(
                &self.topic,
                self.partition,
                producer_info.producer_id,
                last_sequence,
            );

            debug!(
                topic = %self.topic,
                partition = self.partition,
                producer_id = producer_info.producer_id,
                producer_epoch = producer_info.producer_epoch,
                last_sequence,
                "Updated and persisted producer state"
            );
        }

        debug!(
            topic = %self.topic,
            partition = self.partition,
            base_offset,
            record_count,
            new_hwm,
            "Appended batch"
        );

        // Record load metrics for auto-balancing
        self.record_produce_metrics(records.len() as u64, record_count as u64);

        Ok(base_offset)
    }

    /// Load a producer's persisted idempotency state on an in-memory cache
    /// miss, repopulating the cache.
    ///
    /// Returns `Ok(None)` for a genuinely unknown producer. Storage errors
    /// propagate — if we cannot verify whether a producer was seen before,
    /// accepting the batch could admit a duplicate, so the write must fail
    /// (same fail-closed posture as the epoch check).
    async fn load_persisted_producer_state(
        &self,
        producer_id: i64,
    ) -> SlateDBResult<Option<ProducerState>> {
        let key = encode_producer_state_key(producer_id);
        let bytes = match self.db.get(key.as_slice()).await {
            Ok(maybe) => maybe,
            Err(e) => {
                let err = SlateDBError::from(e);
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    producer_id,
                    error = %err,
                    "Failed to read persisted producer state - rejecting write for safety"
                );
                return Err(err);
            }
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let Some(persisted) = super::keys::decode_producer_state_value(&bytes) else {
            warn!(
                topic = %self.topic,
                partition = self.partition,
                producer_id,
                "Undecodable persisted producer state - treating producer as new"
            );
            return Ok(None);
        };
        let state = ProducerState {
            last_sequence: persisted.last_sequence,
            producer_epoch: persisted.producer_epoch,
            last_first_sequence: persisted.last_first_sequence,
            last_base_offset: persisted.last_base_offset,
        };
        debug!(
            topic = %self.topic,
            partition = self.partition,
            producer_id,
            last_sequence = state.last_sequence,
            "Restored producer state from storage after cache miss"
        );
        self.producer_states.insert(producer_id, state);
        Ok(Some(state))
    }

    /// Fetch records starting from the given offset, using the store-wide
    /// default byte budget. See [`Self::fetch_from_with_budget`].
    pub async fn fetch_from(&self, fetch_offset: i64) -> SlateDBResult<(i64, Option<Bytes>)> {
        let max_size = self.max_fetch_response_size.load(Ordering::Relaxed);
        self.fetch_from_with_budget(fetch_offset, max_size).await
    }

    /// Fetch records starting from the given offset.
    ///
    /// Returns (high_watermark, records).
    ///
    /// Now strips the HWM metadata (first 8 bytes) from each batch.
    ///
    /// This uses SlateDB range scan for efficient sequential access:
    /// 1. Find the batch containing or following the fetch offset
    /// 2. Use range scan to iterate through consecutive batches
    /// 3. Collect batches until the byte budget is reached
    ///
    /// `max_bytes` is the per-call byte budget — the smaller of the client's
    /// `partition_max_bytes`, the remaining request-level `max_bytes`, and
    /// the broker's `max_fetch_response_size`. Kafka's contract that the
    /// first batch is always returned whole (even if oversized) is preserved.
    pub async fn fetch_from_with_budget(
        &self,
        fetch_offset: i64,
        max_bytes: usize,
    ) -> SlateDBResult<(i64, Option<Bytes>)> {
        use super::keys::decode_record_offset;

        let high_watermark = self.high_watermark.load(Ordering::SeqCst);

        if fetch_offset >= high_watermark {
            return Ok((high_watermark, None)); // No new data
        }

        if fetch_offset < 0 {
            return Ok((high_watermark, None)); // Invalid offset
        }

        // Find the batch that contains or follows fetch_offset
        let start_offset = match self.find_batch_start(fetch_offset, high_watermark).await? {
            Some(offset) => offset,
            None => {
                // No batch found containing or after fetch_offset
                return Ok((high_watermark, None));
            }
        };

        // Never exceed the broker-wide cap regardless of the client's ask.
        let max_size = max_bytes.min(self.max_fetch_response_size.load(Ordering::Relaxed));

        // Estimate capacity based on max_size, capped at 256KB to avoid over-allocation
        // for very large max_size values. The 4KB minimum handles small fetches efficiently.
        // Previous 64KB cap caused unnecessary reallocations for larger fetch requests.
        let estimated_capacity = max_size.clamp(4096, 256 * 1024);
        let mut combined = BytesMut::with_capacity(estimated_capacity);

        let mut batch_count = 0u32;

        // Use range scan from start_offset to high_watermark for efficient sequential access
        let start_key = encode_record_key(start_offset);
        let end_key = encode_record_key(high_watermark);

        let mut iter = match self.db.scan(start_key.as_slice()..end_key.as_slice()).await {
            Ok(iter) => {
                // Track object store health on successful scan
                super::metrics::track_object_store_health(true);
                iter
            }
            Err(e) => {
                // Track object store health on scan failure
                let still_healthy = super::metrics::track_object_store_health(false);
                if !still_healthy {
                    error!(
                        topic = %self.topic,
                        partition = self.partition,
                        consecutive_failures = super::metrics::object_store_consecutive_failures(),
                        "PARTIAL NETWORK PARTITION DETECTED: Object store unreachable during fetch"
                    );
                }
                return Err(e.into());
            }
        };

        // Propagate scan errors instead of treating them as end-of-data: a
        // transient storage error mid-scan must not silently truncate the
        // response (consumers would interpret it as "caught up").
        while let Some(item) = iter.next().await.map_err(SlateDBError::from)? {
            // Verify this is a record key and decode offset
            let current_offset = match decode_record_offset(&item.key) {
                Some(offset) => offset,
                None => continue, // Skip non-record keys
            };

            // Strip HWM metadata (first 8 bytes)
            // Format: [new_hwm: i64][record_batch: bytes]
            let batch_data = if item.value.len() >= 8 {
                &item.value[8..] // Skip first 8 bytes (HWM metadata)
            } else {
                // Old format or corrupted - use as-is
                item.value.as_ref()
            };

            let record_count = parse_record_count(batch_data);
            if record_count <= 0 {
                // Corrupted batch — return an explicit error rather than
                // silently truncating the response. A partial response would
                // let the consumer advance past valid data and stall on the
                // next fetch with no typed error code.
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    offset = current_offset,
                    "Batch with invalid record count during fetch"
                );
                return Err(SlateDBError::Storage(format!(
                    "Corrupt record batch at offset {} in {}/{}",
                    current_offset, self.topic, self.partition
                )));
            }

            // Kafka's fetch contract: always return at least one complete
            // record batch even if it exceeds `max_bytes`, otherwise the
            // consumer will be stuck (it can't parse a torn batch). Only
            // batches *after* the first are gated by the size budget.
            if !combined.is_empty() && combined.len() + batch_data.len() > max_size {
                break;
            }

            // Add to batch index for future lookups
            self.add_to_batch_index(current_offset, record_count);

            combined.extend_from_slice(batch_data);
            batch_count += 1;

            // After we've included the first (possibly oversized) batch,
            // stop if we've already met or exceeded the byte budget.
            if combined.len() >= max_size {
                break;
            }
        }

        let records = if combined.is_empty() {
            None
        } else {
            debug!(
                topic = %self.topic,
                partition = self.partition,
                fetch_offset,
                start_offset,
                bytes = combined.len(),
                batch_count,
                "Fetched records"
            );
            Some(combined.freeze())
        };

        // Record load metrics for auto-balancing
        if let Some(ref r) = records {
            self.record_fetch_metrics(r.len() as u64, batch_count as u64);
        }

        Ok((high_watermark, records))
    }

    /// Find the batch that contains or follows the given offset.
    ///
    /// Strategy:
    /// 1. Check if fetch_offset is exactly a batch start in the cache (fast path)
    /// 2. Bounded back-scan: scan forward from a window before `fetch_offset`,
    ///    doubling the window down to the log start offset on a miss.
    ///
    /// The bounded back-scan replaces the old fallback that scanned from
    /// offset 0 — O(partition size) object-store reads per fetch for any
    /// lagging consumer with a cold index. A batch containing `fetch_offset`
    /// must start within one batch-length of it, so the first (small) window
    /// almost always suffices; the widening loop is only taken on gappy logs.
    ///
    /// Note: The batch index uses moka cache (hash-based) which doesn't support
    /// range queries. For range lookups, we fall back to SlateDB scan which is
    /// efficient due to SST organization.
    ///
    /// Returns None if no batch exists at or after fetch_offset.
    async fn find_batch_start(
        &self,
        fetch_offset: i64,
        high_watermark: i64,
    ) -> SlateDBResult<Option<i64>> {
        use super::keys::decode_record_offset;

        // Strategy 1: Check if fetch_offset is exactly a batch start (fast path)
        if self.batch_index.contains_key(&fetch_offset) {
            super::metrics::record_batch_index_hit();
            return Ok(Some(fetch_offset));
        }

        super::metrics::record_batch_index_miss();

        // Bounded back-scan with widening window.
        let log_start = self.log_start_offset.load(Ordering::SeqCst);
        let mut window = INITIAL_BATCH_BACKSCAN_WINDOW;

        loop {
            let scan_start = (fetch_offset - window).max(log_start);
            let start_key = encode_record_key(scan_start);
            let end_key = encode_record_key(high_watermark);

            let mut iter = self
                .db
                .scan(start_key.as_slice()..end_key.as_slice())
                .await?;

            // Track whether the window contained any batch starting at or
            // before fetch_offset. If not, a batch containing fetch_offset
            // could still start before the window — we must widen rather
            // than wrongly return the next-following batch.
            let mut saw_batch_at_or_before = false;

            while let Some(item) = iter.next().await.map_err(SlateDBError::from)? {
                if let Some(offset) = decode_record_offset(&item.key) {
                    // Strip HWM metadata if present
                    let batch_data = if item.value.len() >= 8 {
                        &item.value[8..]
                    } else {
                        item.value.as_ref()
                    };
                    let record_count = parse_record_count(batch_data);

                    // Add to cache for future lookups
                    self.add_to_batch_index(offset, record_count);

                    let batch_end = offset + record_count as i64;
                    if offset <= fetch_offset {
                        saw_batch_at_or_before = true;
                        if batch_end > fetch_offset {
                            // This batch contains fetch_offset
                            return Ok(Some(offset));
                        }
                    } else {
                        // First batch after fetch_offset. This is the right
                        // answer only if we can rule out an earlier batch
                        // containing fetch_offset — i.e. we saw at least one
                        // batch at/before it in this window, or the window
                        // already reaches the log start.
                        if saw_batch_at_or_before || scan_start == log_start {
                            return Ok(Some(offset));
                        }
                        break; // widen the window and retry
                    }
                }
            }

            if scan_start == log_start {
                // Whole remaining range scanned: no batch contains or
                // follows fetch_offset.
                return Ok(None);
            }

            // Saw only batches before the window edge (or none) without a
            // conclusion — widen and retry.
            window = window.saturating_mul(2);
        }
    }

    /// Add a batch entry to the index.
    /// Uses lock-free moka cache with automatic LRU eviction.
    fn add_to_batch_index(&self, base_offset: i64, record_count: i32) {
        self.batch_index.insert(base_offset, record_count);
        // No manual eviction needed - moka handles LRU automatically
    }

    /// Warm the batch index cache by pre-loading the most recent batches.
    ///
    /// This is called during partition open to avoid cold-start cache misses.
    /// The batch boundaries come from the recovery scan, so opening a
    /// partition makes one pass over the record keyspace instead of two
    /// (recovery + warm) — a 2x open/failover latency win on large logs.
    ///
    /// Tail reads are the hot pattern (`fetch.offset` follows `HWM`), so only
    /// the last `batch_index_max_size` batches are inserted.
    fn warm_batch_index_from(&self, batches: &[(i64, i32)]) {
        let window_cap = self.batch_index_max_size;
        if window_cap == 0 {
            return;
        }

        let skip = batches.len().saturating_sub(window_cap);
        let mut count: u32 = 0;
        for &(offset, record_count) in &batches[skip..] {
            self.add_to_batch_index(offset, record_count);
            count += 1;
        }

        if count > 0 {
            info!(
                topic = %self.topic,
                partition = self.partition,
                entries = count,
                "Warmed batch index cache from recovery scan"
            );
        }

        super::metrics::record_batch_index_warm_entries(count as i64);
    }

    /// Get the earliest offset in this partition (the log start offset).
    ///
    /// This is a cached atomic load — the LSO only changes when retention
    /// runs. The fetch path consults it for every partition on every pass,
    /// so it must not be a storage scan (it used to be one; combined with
    /// the long-poll wakeup it produced a thundering herd of object-store
    /// reads scaling with consumers x partitions).
    pub async fn earliest_offset(&self) -> SlateDBResult<i64> {
        Ok(self.log_start_offset.load(Ordering::SeqCst))
    }

    /// Find the earliest offset whose batch `max_timestamp` is at or after
    /// `target_timestamp_ms` (Kafka `ListOffsets` timestamp semantics, at
    /// batch granularity — the same granularity Kafka's sparse time index
    /// provides before its final linear scan).
    ///
    /// Returns `Ok(Some((offset, max_timestamp)))` for a hit so the caller can
    /// populate the `timestamp` field of `ListOffsets` v1+ responses (required
    /// by `KafkaConsumer.offsetsForTimes()`), or `Ok(None)` when no such batch
    /// exists (Kafka reports offset -1 in that case).
    pub async fn offset_for_timestamp(
        &self,
        target_timestamp_ms: i64,
    ) -> SlateDBResult<Option<(i64, i64)>> {
        use super::keys::{decode_record_offset, parse_batch_max_timestamp};

        let log_start = self.log_start_offset.load(Ordering::SeqCst);
        let high_watermark = self.high_watermark.load(Ordering::SeqCst);
        if log_start >= high_watermark {
            return Ok(None); // Empty log
        }

        let start_key = encode_record_key(log_start);
        let end_key = encode_record_key(high_watermark);

        let mut iter = self
            .db
            .scan(start_key.as_slice()..end_key.as_slice())
            .await?;

        while let Some(item) = iter.next().await.map_err(SlateDBError::from)? {
            let Some(offset) = decode_record_offset(&item.key) else {
                continue;
            };
            let batch_data = if item.value.len() >= 8 {
                &item.value[8..]
            } else {
                item.value.as_ref()
            };
            match parse_batch_max_timestamp(batch_data) {
                // -1 = producer set no timestamps; skip (cannot match a time query)
                Some(ts) if ts != -1 && ts >= target_timestamp_ms => {
                    return Ok(Some((offset, ts)));
                }
                _ => {}
            }
        }

        Ok(None)
    }

    /// Apply time-based retention: delete every batch whose `max_timestamp`
    /// is older than `now_ms - retention_ms`, advance the persisted log start
    /// offset, and evict deleted entries from the batch index.
    ///
    /// Crash-safety: the new `_lso` is written durably *before* the record
    /// keys below it are deleted. A crash mid-delete leaves orphaned batches
    /// below the LSO, which are invisible to fetches (offset range checks use
    /// the LSO) and are re-deleted on the next retention pass.
    ///
    /// Conservative rules:
    /// - Batches whose timestamp cannot be parsed (or is -1) are never
    ///   deleted, and deletion stops at the first non-expired batch so the
    ///   surviving log stays contiguous.
    ///
    /// Returns the number of deleted batches.
    pub async fn apply_retention(&self, retention_ms: i64, now_ms: i64) -> SlateDBResult<u64> {
        use super::keys::{LEADER_EPOCH_KEY, decode_leader_epoch, decode_record_offset, parse_batch_max_timestamp};

        if retention_ms <= 0 {
            return Ok(0); // Retention disabled
        }

        // Refuse retention writes the moment another broker has acquired
        // ownership: retention advances LSO with `await_durable=true`, so a
        // stale owner running past a hand-off can delete records the new
        // owner has already acked reads on.
        if let Some(ref zombie_state) = self.zombie_mode
            && zombie_state.is_active()
        {
            return Err(SlateDBError::NotOwned {
                topic: self.topic.clone(),
                partition: self.partition,
            });
        }

        if self.leader_epoch > 0 {
            let stored_epoch = match self.db.get(LEADER_EPOCH_KEY).await {
                Ok(Some(bytes)) => decode_leader_epoch(&bytes).unwrap_or(0),
                Ok(None) => 0,
                Err(e) => {
                    let err = SlateDBError::from(e);
                    if err.is_fenced() {
                        return Err(err);
                    }
                    return Err(SlateDBError::Storage(format!(
                        "Cannot verify epoch for retention on {}/{}: {}",
                        self.topic, self.partition, err
                    )));
                }
            };
            if stored_epoch != self.leader_epoch {
                super::metrics::record_epoch_mismatch(&self.topic, self.partition);
                return Err(SlateDBError::EpochMismatch {
                    topic: self.topic.clone(),
                    partition: self.partition,
                    expected_epoch: self.leader_epoch,
                    stored_epoch,
                });
            }
        }

        let cutoff_ms = now_ms.saturating_sub(retention_ms);

        let log_start = self.log_start_offset.load(Ordering::SeqCst);
        let high_watermark = self.high_watermark.load(Ordering::SeqCst);
        if log_start >= high_watermark {
            return Ok(0); // Empty log
        }

        // Pass 1: collect the contiguous prefix of expired batches.
        let start_key = encode_record_key(log_start);
        let end_key = encode_record_key(high_watermark);
        let mut expired: Vec<(i64, i32)> = Vec::new();
        let mut new_log_start = log_start;

        {
            let mut iter = self
                .db
                .scan(start_key.as_slice()..end_key.as_slice())
                .await?;
            while let Some(item) = iter.next().await.map_err(SlateDBError::from)? {
                let Some(offset) = decode_record_offset(&item.key) else {
                    continue;
                };
                let batch_data = if item.value.len() >= 8 {
                    &item.value[8..]
                } else {
                    item.value.as_ref()
                };
                let max_ts = parse_batch_max_timestamp(batch_data);
                let record_count = parse_record_count(batch_data);

                match max_ts {
                    Some(ts) if ts != -1 && ts < cutoff_ms && record_count > 0 => {
                        expired.push((offset, record_count));
                        new_log_start = offset + record_count as i64;
                    }
                    _ => break, // First non-expired (or unparseable) batch — stop.
                }
            }
        }

        if expired.is_empty() {
            return Ok(0);
        }

        // Persist the new LSO durably BEFORE deleting any data. See method
        // docs for the crash-ordering argument.
        self.db
            .put_with_options(
                super::keys::LOG_START_OFFSET_KEY,
                &new_log_start.to_be_bytes(),
                &PutOptions::default(),
                &WriteOptions {
                    await_durable: true,
                },
            )
            .await?;
        self.log_start_offset.store(new_log_start, Ordering::SeqCst);

        // Delete the expired record keys (batched; fast writes are fine —
        // resurrection after a crash is harmless because the LSO already
        // moved past them).
        let mut delete_batch = WriteBatch::new();
        for &(offset, _) in &expired {
            delete_batch.delete(encode_record_key(offset).as_slice());
            self.batch_index.invalidate(&offset);
        }
        self.db
            .write_with_options(delete_batch, &FAST_WRITE_OPTIONS)
            .await?;

        let deleted = expired.len() as u64;
        info!(
            topic = %self.topic,
            partition = self.partition,
            deleted_batches = deleted,
            old_log_start = log_start,
            new_log_start,
            retention_ms,
            "Applied time-based retention"
        );
        super::metrics::record_retention_deleted_batches(&self.topic, self.partition, deleted);

        Ok(deleted)
    }

    /// Flush pending writes to storage.
    pub async fn flush(&self) -> SlateDBResult<()> {
        self.db.flush().await?;
        Ok(())
    }

    /// Close the partition store.
    ///
    /// Flushes pending writes before closing to ensure durability.
    /// This method takes `&self` (not `self`) so it can be called even when
    /// there are multiple Arc references to the store.
    ///
    /// Concurrent callers (release_partition, zombie-entry, lease-loss
    /// loops, shutdown — all of which can race) are serialized by an
    /// internal `OnceCell`: the first caller flushes + closes SlateDB; later
    /// callers observe the cell is initialized and return immediately. This
    /// prevents the double-close panic in SlateDB compaction.
    pub async fn close(&self) -> SlateDBResult<()> {
        let topic = &self.topic;
        let partition = self.partition;
        // `get_or_try_init` runs the closure exactly once, even under
        // concurrent calls; later callers wait for the in-flight init and
        // then return its result. We deliberately store `()` on success and
        // surface errors to all racing callers (so a failed close isn't
        // silently masked from the rest of the system).
        self.close_once
            .get_or_try_init(|| async {
                info!(topic = %topic, partition, "Closing partition store");
                self.db.flush().await?;
                self.db.close().await?;
                Ok(())
            })
            .await
            .map(|_| ())
    }

    /// Validate that the remaining lease TTL is sufficient for a safe write.
    ///
    /// This prevents TOCTOU (time-of-check to time-of-use) race conditions by
    /// rejecting writes when the lease is close to expiring. The minimum TTL
    /// ensures there's enough time for:
    /// - The write to complete
    /// - The flush to complete
    /// - Any network latency
    ///
    /// Returns Ok(()) if the lease is valid, or LeaseTooShort error if not.
    pub fn validate_lease_for_write(&self, remaining_ttl_secs: u64) -> SlateDBResult<()> {
        // Record TTL histogram for near-miss detection
        super::metrics::record_lease_ttl_at_write(&self.topic, remaining_ttl_secs);

        if remaining_ttl_secs < self.min_lease_ttl_for_write_secs {
            warn!(
                topic = %self.topic,
                partition = self.partition,
                remaining_ttl_secs,
                required_secs = self.min_lease_ttl_for_write_secs,
                "Rejecting write: lease TTL too short"
            );
            super::metrics::record_lease_too_short(&self.topic, self.partition);
            return Err(SlateDBError::LeaseTooShort {
                topic: self.topic.clone(),
                partition: self.partition,
                remaining_secs: remaining_ttl_secs,
                required_secs: self.min_lease_ttl_for_write_secs,
            });
        }
        Ok(())
    }

    /// Check if the SlateDB handle is still valid (not fenced).
    ///
    /// This performs a lightweight read operation to verify we haven't been
    /// fenced by another writer. Used during zombie mode recovery.
    ///
    /// Returns Ok(high_watermark) if valid, or Fenced error if fenced.
    pub async fn high_watermark_check(&self) -> SlateDBResult<i64> {
        // Health check: verify we can still access the DB
        match self.db.get(HIGH_WATERMARK_KEY).await {
            Ok(_) => Ok(self.high_watermark.load(Ordering::SeqCst)),
            Err(e) => {
                let err = SlateDBError::from(e);
                if err.is_fenced() {
                    Err(err)
                } else {
                    // Non-fencing errors: return the cached HWM but log the issue
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        error = %err,
                        "Error during HWM check (non-fencing)"
                    );
                    // Return cached HWM on non-fencing errors
                    Ok(self.high_watermark.load(Ordering::SeqCst))
                }
            }
        }
    }
}

impl Debug for PartitionStore {
    fn fmt(&self, f: &mut Formatter<'_>) -> FmtResult {
        f.debug_struct("PartitionStore")
            .field("topic", &self.topic)
            .field("partition", &self.partition)
            .field("high_watermark", &self.high_watermark)
            .field("batch_index_max_size", &self.batch_index_max_size)
            .field("max_fetch_response_size", &self.max_fetch_response_size)
            .field("zombie_mode", &self.zombie_mode)
            .finish_non_exhaustive()
    }
}

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
    /// previous behavior of `0 = fencing disabled` was the source of audit
    /// finding C-3 — partitions opened pre-coordinator could write with no
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

    /// Build the PartitionStore.
    pub async fn build(self) -> SlateDBResult<PartitionStore> {
        use super::keys::{
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
            "SlateDB settings configured for backpressure"
        );

        // SlateDB 0.10's open path is already async-only and tokio-friendly, so
        // a round-trip through a blocking pool would just waste a worker. Run
        // the open inline; the slow step (object-store metadata I/O) yields
        // back to the runtime.
        let open_future = async move {
            let object_path = ObjectPath::from(path_for_task.as_str());
            let db = Db::builder(object_path, object_store_for_task)
                .with_settings(slatedb_settings)
                .build()
                .await
                .map_err(SlateDBError::from)?;

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
                        &WriteOptions {
                            await_durable: true,
                        },
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

            // If we have a non-zero expected epoch, validate it
            if expected_epoch > 0 {
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
                    &WriteOptions {
                        await_durable: true,
                    }, // MUST be durable
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
                    &WriteOptions {
                        await_durable: true,
                    },
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
                .get(super::keys::LOG_START_OFFSET_KEY)
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
        super::metrics::set_producer_state_recovery_count(
            &topic_for_task,
            partition,
            persisted_states.len() as i64,
        );

        info!(topic = %topic_for_task, partition, high_watermark = hwm, leader_epoch = validated_epoch, "Partition store opened via builder");

        // Build producer state cache with configured TTL and eviction warning
        // Clone topic for the eviction listener closure
        let topic_for_eviction = topic.clone();
        let producer_states_cache = MokaCache::builder()
            .max_capacity(DEFAULT_PRODUCER_STATE_CACHE_SIZE)
            .time_to_idle(Duration::from_secs(self.producer_state_cache_ttl_secs))
            .eviction_listener(move |producer_id: Arc<i64>, state: ProducerState, cause| {
                // Log warning when producers with active sequences are evicted.
                // This is important because:
                // 1. If the producer reconnects after eviction, its sequence tracking is lost
                // 2. A duplicate message could be accepted as new (idempotency gap)
                // The warning helps operators identify producers with idle periods > TTL
                if state.last_sequence > 0 {
                    warn!(
                        topic = %topic_for_eviction,
                        partition,
                        producer_id = *producer_id,
                        last_sequence = state.last_sequence,
                        producer_epoch = state.producer_epoch,
                        eviction_cause = ?cause,
                        ttl_secs = self.producer_state_cache_ttl_secs,
                        "Producer state evicted from cache - idempotency may be lost if producer reconnects"
                    );
                    super::metrics::record_producer_state_eviction(&topic_for_eviction, partition, true);
                } else {
                    super::metrics::record_producer_state_eviction(&topic_for_eviction, partition, false);
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

        // Build batch index cache (lock-free concurrent cache)
        let batch_index_cache = MokaCache::builder()
            .max_capacity(self.batch_index_max_size as u64)
            .build();

        let store = PartitionStore {
            db,
            high_watermark: AtomicI64::new(hwm),
            next_offset: AtomicI64::new(hwm),
            log_start_offset: AtomicI64::new(log_start_offset),
            appends_since_checkpoint: std::sync::atomic::AtomicU64::new(1),
            write_lock: Mutex::new(()),
            topic,
            partition,
            batch_index: batch_index_cache,
            max_fetch_response_size: AtomicUsize::new(self.max_fetch_response_size),
            batch_index_max_size: self.batch_index_max_size,
            zombie_mode: self.zombie_mode,
            producer_states: producer_states_cache,
            min_lease_ttl_for_write_secs: self.min_lease_ttl_for_write_secs,
            load_collector: arc_swap::ArcSwapOption::from(None),
            leader_epoch: validated_epoch,
            close_once: tokio::sync::OnceCell::new(),
        };

        // Warm the batch index cache from the recovery scan (one storage
        // pass instead of two).
        store.warm_batch_index_from(&recovered_batches);

        Ok(store)
    }
}

impl PartitionStore {
    /// Create a new builder for PartitionStore.
    pub fn builder() -> PartitionStoreBuilder {
        PartitionStoreBuilder::new()
    }
}
