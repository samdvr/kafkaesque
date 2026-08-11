//! Partition store wrapping SlateDB for a single partition.
//!
//! Each partition gets its own SlateDB instance, stored at a unique path
//! in the object store (e.g., `s3://bucket/topic-X/partition-Y/`).
//!
//! # Memory Scaling
//!
//! Broker memory scales with the number of partitions **owned per broker**.
//! There are two terms, and the second dominates:
//!
//! 1. **Batch index** (bounded, predictable). Each partition keeps an
//!    in-memory index for offset lookup:
//!
//!    ```text
//!    Memory = batch_index_max_size * 16 bytes * num_partitions
//!           = 10,000 * 16 * P
//!           = ~160 KB per partition
//!           = ~160 MB for 1,000 partitions
//!    ```
//!
//!    The batch index uses LRU eviction when the limit is reached, so older
//!    entries are removed first. This may cause additional SlateDB lookups
//!    for older offsets but bounds growth.
//!
//! 2. **The SlateDB instance itself** (larger, less predictable — the real
//!    OOM vector). Each partition owns a dedicated `slatedb::Db` with its own
//!    memtable, WAL write buffer, block cache, and background flush/compaction
//!    tasks. This per-instance overhead is **not** captured by the batch-index
//!    formula above and typically dominates it well before P reaches the low
//!    thousands. A broker that owns thousands of partitions holds thousands of
//!    live LSM engines.
//!
//! To bound and observe this:
//! - Set `max_owned_partitions_per_broker` in `ClusterConfig` to cap how many
//!   stores a broker will open (rejecting further acquisitions rather than
//!   OOM-killing). The `estimated_partition_memory_bytes` gauge and
//!   `partition_acquire_rejected_total{reason="max_owned"}` counter make the
//!   limit observable. Calibrate the cap against measured RSS per owned
//!   partition for your workload.
//! - Decrease `batch_index_max_size` to shrink the (smaller) index term.
//! - Distribute partitions across more brokers.
//!
//! A future enhancement could use a shared moka cache across all partitions,
//! and — if SlateDB allows — a shared block-cache budget across instances,
//! both governed by a single global memory budget.
//!
//! # Idempotency
//!
//! The producer state cache tracks (last_sequence, epoch) per producer_id to
//! detect and reject duplicate or out-of-order messages. This provides
//! exactly-once semantics for idempotent producers.

mod append;
mod batch_index;
mod builder;
mod fetch;
mod offset_reservation;
mod producer_state;

pub use builder::PartitionStoreBuilder;
pub use producer_state::ProducerState;

use moka::sync::Cache as MokaCache;
use object_store::ObjectStore;
use slatedb::Db;
use slatedb::WriteBatch;
use slatedb::config::{PutOptions, WriteOptions};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
use tokio::sync::Mutex;
use tracing::{info, warn};

use super::error::{SlateDBError, SlateDBResult};
use super::keys::{HIGH_WATERMARK_KEY, encode_record_key, parse_record_count_checked};
use super::load_metrics::LoadMetricsCollector;
use super::zombie_mode::ZombieModeState;

use batch_index::BatchIndex;

/// Write options for the `acks=0` fire-and-forget produce path.
///
/// `await_durable: false` lets SlateDB return as soon as the write is
/// queued, before the WAL flush hits the object store. The caller has
/// explicitly opted out of durability by sending `acks=0`, so up to the
/// SlateDB flush interval (~100 ms) of writes can be lost on a hard kill.
///
/// Every other path — `acks=1`, `acks=all`, idempotent-producer state,
/// HWM checkpoints, retention, snapshot install — uses
/// `DURABLE_WRITE_OPTIONS` so the producer's ack contract holds.
/// See the "Durability contract" section of `README.md`.
pub(super) const FAST_WRITE_OPTIONS: WriteOptions = WriteOptions {
    await_durable: false,
    // 0 = let SlateDB assign the sequence number internally. A non-zero
    // value must be strictly greater than the current max or the write is
    // rejected; we have no reason to drive it by hand.
    seqnum: 0,
};

/// Write options that block on SlateDB's WAL flush before returning.
///
/// Used for any write whose loss would violate Kafka's `acks=1`/`acks=all`
/// contract or the idempotent-producer guarantee (last-sequence cache).
pub(super) const DURABLE_WRITE_OPTIONS: WriteOptions = WriteOptions {
    await_durable: true,
    seqnum: 0,
};

/// Default maximum response size for fetch operations (1 MB).
/// This limits memory usage when collecting batches for a single fetch response.
/// Can be overridden via ClusterConfig.max_fetch_response_size.
pub(super) const DEFAULT_MAX_FETCH_RESPONSE_SIZE: usize = 1024 * 1024;

/// Checkpoint the `_hwm` key every N appended batches.
///
/// The HWM is embedded in every batch value, so the standalone `_hwm` key is
/// purely an optimization: it bounds the recovery scan on open to the batches
/// appended since the last checkpoint instead of the whole log. Every 64th
/// append piggybacks the checkpoint on the batch's own atomic `WriteBatch`,
/// so it costs no extra storage round-trip.
pub(super) const HWM_CHECKPOINT_INTERVAL_BATCHES: u64 = 64;

/// Initial back-scan window (in offsets) used by `find_batch_start` when the
/// batch index misses. The window doubles until a covering batch is found or
/// the log start offset is reached, so lookups are O(window) reads instead of
/// O(partition size) scans from offset 0.
pub(super) const INITIAL_BATCH_BACKSCAN_WINDOW: i64 = 4096;

/// Wrapper around SlateDB for a single Kafka partition.
pub struct PartitionStore {
    /// The SlateDB instance.
    db: Db,

    /// Cached high watermark (also persisted in DB).
    high_watermark: AtomicI64,

    /// Next offset to allocate for an in-flight write.
    ///
    /// Always >= `high_watermark`. The split exists for cancellation safety
    ///: writers `fetch_add` here BEFORE the SlateDB write so a
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
    /// Backed by a BTreeMap behind an `RwLock` so the fetch path can resolve
    /// mid-batch lookups via a range query in O(log n) instead of falling
    /// back to a windowed SlateDB scan.
    batch_index: BatchIndex,

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

    /// Wall-clock time (epoch millis) when this `PartitionStore` was opened.
    ///
    /// Used as the conservative "last activity" baseline for legacy producer
    /// state values (those persisted before the `last_used_at_ms` field
    /// existed). The retention sweep treats a legacy entry as "last used at
    /// `opened_at_ms`" so it becomes eligible for deletion only after a full
    /// retention window has elapsed since the broker upgrade.
    opened_at_ms: i64,

    /// Cached lease expiry as milliseconds since `UNIX_EPOCH`, or 0 if not
    /// set yet. Updated by the partition manager whenever a write is admitted
    /// (cache hit in `get_for_write`) or an explicit lease renewal lands.
    ///
    /// Read inside `append_batch_inner` so a write that was admitted with
    /// 55s remaining can refuse if the actual TTL has dropped below
    /// `min_lease_ttl_for_write_secs` while it waited in the write_lock
    /// queue. Without this re-check the only fence is SlateDB epoch
    /// mismatch, which fires only after a competing broker has already
    /// acquired the partition — by then the producer has already seen a
    /// confusing `NotOwned` instead of a clean `LeaseTooShort` redirect.
    lease_expiry_ms: AtomicI64,

    /// Pre-resolved Prometheus throughput counters for this partition.
    ///
    /// Resolved on first record rather than at open so the cardinality decision
    /// happens at the same moment it does today; held afterwards so the hot
    /// produce/fetch paths pay four atomic `inc_by`s instead of a tracked-set
    /// probe plus four label-set hashes. See `metrics::PartitionCounters`.
    prom_counters: std::sync::OnceLock<super::metrics::PartitionCounters>,

    /// Sticky bit tripped when a SlateDB write returned an error AFTER the
    /// offset reservation had been disarmed — i.e. when the in-memory
    /// `next_offset` advanced past `high_watermark` with no durable record
    /// at the gapped key range. Without this guard, a subsequent append on
    /// this same instance would write at the post-gap offset, producing the
    /// "records → gap → records" pattern that bricks recovery under the
    /// default `fail_on_recovery_gap=true` policy.
    ///
    /// Once set, every append short-circuits with `NotOwned` so the lease
    /// holder fences itself; the next acquirer reopens the partition and
    /// the recovery scan re-derives `next_offset` from durable state. Either
    /// the failed write was eventually committed by SlateDB's writer queue
    /// (recovery sees a contiguous range and the gap was illusory) or it
    /// wasn't (recovery sees only the pre-gap records and `next_offset`
    /// resets cleanly to `high_watermark`).
    append_failed: AtomicBool,
}

impl PartitionStore {
    /// Open or create a partition store.
    ///
    /// # SlateDB Workaround
    ///
    /// Uses `spawn_blocking` internally because SlateDB's `open` future is not
    /// Send-safe (its iterator implementation holds `Rc<SsTableHandle>`), so
    /// the future returned by `Db::open_with_opts` cannot cross an `.await` on
    /// the multi-threaded runtime. We run the open in a blocking context to
    /// work around it.
    ///
    /// Last verified against the pinned `slatedb` version in `Cargo.toml`
    /// (0.10.x). Re-check whether the `Rc`/`!Send` limitation still holds on
    /// the next SlateDB bump; if it has been fixed upstream, this
    /// `spawn_blocking` hop can be removed.
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

    /// The leader epoch this store was opened with. Used by the Fetch and
    /// Produce handlers to enforce KIP-320 fencing: a client carrying a
    /// stale `current_leader_epoch` after a failover must be rejected with
    /// `FencedLeaderEpoch` instead of silently reading from / writing to
    /// the new owner. Returns `0` when epoch validation is disabled (mock
    /// coordinators in tests).
    pub fn leader_epoch(&self) -> i32 {
        self.leader_epoch
    }

    #[cfg(test)]
    pub fn invalidate_producer_state_cache(&self, producer_id: i64) {
        self.producer_states.invalidate(&producer_id);
    }

    #[cfg(test)]
    pub async fn raw_db_get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).await.ok().flatten().map(|b| b.to_vec())
    }

    #[cfg(test)]
    pub async fn raw_db_count_in_range(&self, start: &[u8], end: &[u8]) -> usize {
        let mut iter = match self.db.scan(start..end).await {
            Ok(iter) => iter,
            Err(_) => return 0,
        };
        let mut count = 0usize;
        while let Ok(Some(_)) = iter.next().await {
            count += 1;
        }
        count
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

    /// Pre-resolved Prometheus counters for this partition.
    fn prom_counters(&self) -> &super::metrics::PartitionCounters {
        self.prom_counters
            .get_or_init(|| super::metrics::partition_counters(&self.topic, self.partition))
    }

    /// Add to this partition's Prometheus produce counters.
    pub fn record_produce_counters(&self, message_count: u64, bytes: u64) {
        self.prom_counters().add_produce(message_count, bytes);
    }

    /// Add to this partition's Prometheus fetch counters.
    pub fn record_fetch_counters(&self, message_count: u64, bytes: u64) {
        self.prom_counters().add_fetch(message_count, bytes);
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

    /// Add a batch entry to the index.
    fn add_to_batch_index(&self, base_offset: i64, record_count: i32) {
        self.batch_index.insert(base_offset, record_count);
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
    ///
    /// Bounded scan: an authenticated reader cannot turn this into a full-log
    /// scan DoS by pointing the timestamp at `i64::MIN`. We cap the number of
    /// batches inspected at `LIST_OFFSETS_TIMESTAMP_SCAN_CAP`; if the cap is
    /// reached without a hit we report "not found" rather than continuing
    /// indefinitely. The cap is chosen high enough to satisfy normal
    /// `offsetsForTimes()` queries (which target recent timestamps) and low
    /// enough that a worst-case pathological query bounds CPU and object-store
    /// reads. Replace this with a real sparse time index when implementing the
    /// `.timeindex` analogue.
    pub async fn offset_for_timestamp(
        &self,
        target_timestamp_ms: i64,
    ) -> SlateDBResult<Option<(i64, i64)>> {
        use super::keys::{decode_record_offset, parse_batch_max_timestamp};

        const LIST_OFFSETS_TIMESTAMP_SCAN_CAP: usize = 100_000;

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

        let mut scanned = 0usize;
        while let Some(item) = iter.next().await.map_err(SlateDBError::from)? {
            scanned += 1;
            if scanned > LIST_OFFSETS_TIMESTAMP_SCAN_CAP {
                tracing::warn!(
                    topic = %self.topic,
                    partition = self.partition,
                    target_timestamp_ms,
                    scanned,
                    "offset_for_timestamp scan cap reached; reporting not-found"
                );
                super::metrics::record_list_offsets_truncated(&self.topic);
                return Ok(None);
            }
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
        use super::keys::{
            LEADER_EPOCH_KEY, decode_leader_epoch, decode_record_offset, parse_batch_max_timestamp,
        };

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

        if self.leader_epoch != 0 {
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
                let record_count = match parse_record_count_checked(batch_data) {
                    Ok(n) => n,
                    Err(e) => {
                        // Retention stops at the first non-expired (or
                        // unparseable) batch — but without a metric, an
                        // operator has no signal that a corrupt batch is
                        // permanently anchoring storage growth on this
                        // partition. Emit one so it pages someone.
                        super::metrics::record_corrupt_batch(
                            &self.topic,
                            self.partition,
                            "retention_scan",
                        );
                        tracing::warn!(
                            topic = %self.topic,
                            partition = self.partition,
                            offset,
                            error = %e,
                            "Corrupt batch encountered during retention scan; retention will stop here"
                        );
                        0
                    }
                };

                match max_ts {
                    Some(ts) if ts != -1 && ts < cutoff_ms && record_count > 0 => {
                        expired.push((offset, record_count));
                        new_log_start = offset.checked_add(record_count as i64).unwrap_or(i64::MAX);
                    }
                    _ => break, // First non-expired (or unparseable) batch — stop.
                }
            }
        }

        if expired.is_empty() {
            return Ok(0);
        }

        // Take `write_lock` for the epoch re-verify + LSO write + delete
        // batch. Without this, retention's epoch check (above) and its
        // durable LSO write are not atomic — a concurrent ownership
        // hand-off can fence us between the two, and our LSO write would
        // either fail (good, slatedb's own fencing catches it) OR land
        // on stale ordering versus a concurrent appender that's in the
        // middle of `append_batch_inner`. Holding `write_lock` across the
        // mutation block makes the "epoch verify + LSO + delete" atomic
        // with respect to any appender, so a freshly-acked record cannot
        // be retroactively retention-deleted.
        let _retention_guard = self.write_lock.lock().await;

        // Re-verify the epoch under the lock so we don't write LSO from a
        // stale-epoch retention pass. (The durable write itself is also
        // fenced by slatedb when ownership has moved, but failing fast
        // here avoids issuing a doomed write.)
        if self.leader_epoch != 0 {
            let stored_epoch = match self.db.get(LEADER_EPOCH_KEY).await {
                Ok(Some(bytes)) => decode_leader_epoch(&bytes).unwrap_or(0),
                Ok(None) => 0,
                Err(e) => {
                    let err = SlateDBError::from(e);
                    if err.is_fenced() {
                        return Err(err);
                    }
                    return Err(SlateDBError::Storage(format!(
                        "Cannot re-verify epoch for retention on {}/{}: {}",
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

        // Persist the new LSO durably BEFORE deleting any data. See method
        // docs for the crash-ordering argument.
        self.db
            .put_with_options(
                super::keys::LOG_START_OFFSET_KEY,
                &new_log_start.to_be_bytes(),
                &PutOptions::default(),
                &DURABLE_WRITE_OPTIONS,
            )
            .await?;
        self.log_start_offset.store(new_log_start, Ordering::SeqCst);

        // Delete the expired record keys (batched; fast writes are fine —
        // resurrection after a crash is harmless because the LSO already
        // moved past them).
        let mut delete_batch = WriteBatch::new();
        for &(offset, _) in &expired {
            delete_batch.delete(encode_record_key(offset).as_slice());
            self.batch_index.invalidate(offset);
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

    /// Delete persisted producer-state keys for producers that have not
    /// appended a batch within `retention_ms`.
    ///
    /// Without this sweep, every fresh `producer_id` (e.g. a new client
    /// session minting a non-idempotent ID per connection) leaves a
    /// permanent `p<producer_id>` key in SlateDB. Long-lived clusters
    /// accumulate one key per session forever; partition open scans them
    /// all into a HashMap, so open latency grows linearly with churn.
    ///
    /// The persisted `last_used_at_ms` field is the authoritative recency
    /// signal — every append writes a fresh timestamp, so an active
    /// producer always has a recent on-disk value even if Moka's capacity
    /// LRU has evicted it from the cache. The retention sweep deletes a
    /// key only when its persisted timestamp is past the retention
    /// horizon (or, for legacy entries with `last_used_at_ms == -1`,
    /// when partition-open age exceeds the horizon). The in-memory cache
    /// check is a fast-path optimization — never a safety check.
    ///
    /// Crash-safety: deleting a producer-state key is safe at any time —
    /// the producer either reconnects with `first_sequence == 0` (treated
    /// as a new producer, correctly) or with a higher epoch (also treated
    /// as new).
    ///
    /// Skips during zombie mode: ownership is in doubt, so deletes that
    /// could conflict with the new owner's writes must not happen.
    pub async fn prune_producer_states(
        &self,
        retention_ms: i64,
        now_ms: i64,
    ) -> SlateDBResult<u64> {
        use super::keys::{
            PRODUCER_STATE_KEY_PREFIX, decode_producer_id, decode_producer_state_value,
        };

        if retention_ms <= 0 {
            return Ok(0);
        }

        if let Some(ref zombie_state) = self.zombie_mode
            && zombie_state.is_active()
        {
            return Err(SlateDBError::NotOwned {
                topic: self.topic.clone(),
                partition: self.partition,
            });
        }

        let cutoff_ms = now_ms.saturating_sub(retention_ms);
        let opened_at_ms = self.opened_at_ms;

        let start_key = [PRODUCER_STATE_KEY_PREFIX];
        let end_key = [PRODUCER_STATE_KEY_PREFIX + 1];
        let mut to_delete: Vec<(i64, [u8; 9])> = Vec::new();

        {
            let mut iter = self
                .db
                .scan(start_key.as_slice()..end_key.as_slice())
                .await
                .map_err(SlateDBError::from)?;
            while let Some(item) = iter.next().await.map_err(SlateDBError::from)? {
                let Some(producer_id) = decode_producer_id(&item.key) else {
                    continue;
                };
                let Some(persisted) = decode_producer_state_value(&item.value) else {
                    continue;
                };

                // Persisted timestamp is authoritative: every append stamps
                // last_used_at_ms, so an active producer always has a fresh
                // on-disk value even if Moka has evicted it from the
                // in-memory cache under capacity pressure (Moka's
                // max_capacity LRU fires above ~10k concurrent producers
                // and would otherwise let retention delete still-active
                // state — an idempotency violation).
                let effective_last_used = if persisted.last_used_at_ms >= 0 {
                    persisted.last_used_at_ms
                } else {
                    opened_at_ms
                };
                if effective_last_used > cutoff_ms {
                    continue;
                }

                // Defense in depth: even if the persisted timestamp claims
                // the entry is stale, an in-cache entry means the producer
                // is currently live in this process. Don't race a delete
                // against the next append from the same producer.
                if self.producer_states.contains_key(&producer_id) {
                    continue;
                }

                to_delete.push((
                    producer_id,
                    super::keys::encode_producer_state_key(producer_id),
                ));
            }
        }

        if to_delete.is_empty() {
            return Ok(0);
        }

        // Re-verify each candidate under `write_lock` and build the delete
        // batch while holding it. Without this, a concurrent appender for
        // producer P can race the prune:
        //   1. Prune scan sees P's persisted `last_used_at_ms < cutoff` and
        //      `producer_states.contains_key(&P) == false` (cache evicted).
        //   2. Appender for P acquires write_lock, computes a batch that
        //      atomically writes a fresh persisted producer-state value
        //      AND inserts P into the in-memory cache.
        //   3. Appender's WriteBatch commits.
        //   4. Prune (no lock) issues its delete batch including P's key.
        //   5. P's persisted state is gone; next batch from P is treated
        //      as a new producer, accepting duplicate sequences as new.
        //      That's an idempotency violation.
        // Holding `write_lock` across the re-verify + delete makes the
        // re-verify and the delete atomic with respect to any appender:
        // an appender either ran fully BEFORE we got the lock (its put
        // bumped `last_used_at_ms` above the cutoff and our point-read
        // sees that, so we skip) or runs fully AFTER (so our delete
        // applies first, then the appender writes a fresh entry that
        // we never had a chance to delete).
        let _prune_guard = self.write_lock.lock().await;

        let mut verified: Vec<(i64, [u8; 9])> = Vec::with_capacity(to_delete.len());
        for (producer_id, key) in to_delete {
            // Cache check first: an appender's `producer_states.insert`
            // happens within the same critical section as its WriteBatch
            // commit (see `append_batch_inner`), so if the producer is in
            // the cache now, an appender beat us — leave it alone.
            if self.producer_states.contains_key(&producer_id) {
                continue;
            }
            // Re-read the persisted timestamp under the lock. A point-read
            // failure here is treated as "leave it alone" — better to
            // re-prune later than to delete an entry whose state we
            // couldn't confirm.
            let bytes = match self.db.get(key.as_slice()).await {
                Ok(Some(b)) => b,
                Ok(None) => continue,
                Err(e) => {
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        producer_id,
                        error = %e,
                        "Producer-state re-verify failed; skipping prune for this entry"
                    );
                    continue;
                }
            };
            let Some(persisted) = decode_producer_state_value(&bytes) else {
                continue;
            };
            let effective_last_used = if persisted.last_used_at_ms >= 0 {
                persisted.last_used_at_ms
            } else {
                opened_at_ms
            };
            if effective_last_used > cutoff_ms {
                continue;
            }
            verified.push((producer_id, key));
        }

        if verified.is_empty() {
            return Ok(0);
        }

        let mut delete_batch = WriteBatch::new();
        for (_, key) in &verified {
            delete_batch.delete(key.as_slice());
        }

        if let Err(e) = self
            .db
            .write_with_options(delete_batch, &DURABLE_WRITE_OPTIONS)
            .await
        {
            let err = SlateDBError::from(e);
            if err.is_fenced() {
                warn!(
                    topic = %self.topic,
                    partition = self.partition,
                    "Fenced during producer-state prune; aborting sweep"
                );
            } else {
                warn!(
                    topic = %self.topic,
                    partition = self.partition,
                    error = %err,
                    "Failed to commit producer-state prune batch"
                );
            }
            return Err(err);
        }

        let deleted = verified.len() as u64;
        for (producer_id, _) in &verified {
            self.producer_states.invalidate(producer_id);
        }

        info!(
            topic = %self.topic,
            partition = self.partition,
            deleted_producer_states = deleted,
            retention_ms,
            "Pruned stale producer-state keys"
        );

        Ok(deleted)
    }

    /// Flush pending writes to storage.
    pub async fn flush(&self) -> SlateDBResult<()> {
        let start = std::time::Instant::now();
        let result = self.db.flush().await;
        super::metrics::record_slatedb_flush(&self.topic, start.elapsed().as_secs_f64());
        result?;
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

    /// Publish the absolute lease expiry (epoch millis) so subsequent
    /// `append_batch_inner` calls can re-check the *current* remaining TTL
    /// rather than trusting whatever the partition manager observed when it
    /// admitted the request.
    ///
    /// `Ordering::Release` here pairs with the `Acquire` load inside
    /// `append_batch_inner`: any concurrent reader that observes the new
    /// expiry also sees every store made before this call.
    pub fn set_lease_expiry_ms(&self, expiry_ms: i64) {
        self.lease_expiry_ms.store(expiry_ms, Ordering::Release);
    }

    /// Re-validate the lease at write time. Returns the remaining TTL on
    /// success; returns `LeaseTooShort` if the lease has decayed below the
    /// safe-write floor since `get_for_write` admitted the request.
    ///
    /// Returns `None` (skip) if the partition manager hasn't yet recorded
    /// an expiry — first writes after open fall back to the legacy admit-
    /// time check in the manager.
    fn revalidate_lease_at_write(&self) -> SlateDBResult<()> {
        let expiry_ms = self.lease_expiry_ms.load(Ordering::Acquire);
        if expiry_ms == 0 {
            // No expiry recorded yet (e.g. mock coordinator path, or first
            // write after store open before the manager hits its fast
            // path). Skip — admit-time check is the only safety here.
            return Ok(());
        }
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);
        let remaining_ms = expiry_ms.saturating_sub(now_ms);
        if remaining_ms <= 0 {
            warn!(
                topic = %self.topic,
                partition = self.partition,
                "Rejecting write: lease already expired by the time the write reached the partition store"
            );
            super::metrics::record_lease_too_short(&self.topic, self.partition);
            return Err(SlateDBError::LeaseTooShort {
                topic: self.topic.clone(),
                partition: self.partition,
                remaining_secs: 0,
                required_secs: self.min_lease_ttl_for_write_secs,
            });
        }
        let remaining_secs = (remaining_ms / 1000) as u64;
        self.validate_lease_for_write(remaining_secs)
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

impl PartitionStore {
    /// Create a new builder for PartitionStore.
    pub fn builder() -> PartitionStoreBuilder {
        PartitionStoreBuilder::new()
    }
}
