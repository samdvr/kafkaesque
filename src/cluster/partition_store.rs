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
use slatedb::config::{PutOptions, Settings as SlateDbSettings, WriteOptions};
use std::fmt::{Debug, Formatter, Result as FmtResult};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::RwLock;

use super::load_metrics::LoadMetricsCollector;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
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
}

/// Wrapper around SlateDB for a single Kafka partition.
pub struct PartitionStore {
    /// The SlateDB instance.
    db: Db,

    /// Cached high watermark (also persisted in DB).
    high_watermark: AtomicI64,

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
    load_collector: RwLock<Option<Arc<LoadMetricsCollector>>>,

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
            false, // fail_on_recovery_gap: default to false for backwards compatibility
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

    /// Persist a producer's state to SlateDB.
    ///
    /// This helps idempotency survive broker restarts. Called after
    /// successful batch write.
    ///
    /// # Performance Note
    ///
    /// This is best-effort for non-fencing errors. The producer state is already
    /// in the in-memory cache for the current session. If persistence fails:
    /// - Current session: idempotency still works via in-memory cache
    /// - After restart: may accept duplicates if producer reconnects with same ID
    ///
    /// This matches the durability semantics of the record batch itself, which
    /// uses await_durable=false for throughput.
    ///
    /// Retry logic for transient failures
    ///
    /// Non-fencing errors are now retried up to 3 times before giving up.
    /// This reduces the risk of idempotency loss from transient storage issues.
    ///
    /// # Errors
    ///
    /// Only fencing errors are propagated to fail the produce request.
    /// Other errors are retried, then logged and ignored (best-effort persistence).
    async fn persist_producer_state(
        &self,
        producer_id: i64,
        last_sequence: i32,
        producer_epoch: i16,
    ) -> SlateDBResult<()> {
        let key = encode_producer_state_key(producer_id);
        let value = encode_producer_state_value(last_sequence, producer_epoch);

        // Retry up to 3 times for transient failures
        const MAX_RETRIES: u32 = 3;
        let mut last_error: Option<SlateDBError> = None;

        for attempt in 0..MAX_RETRIES {
            match self
                .db
                .put_with_options(&key, &value, &PutOptions::default(), &FAST_WRITE_OPTIONS)
                .await
            {
                Ok(()) => return Ok(()),
                Err(e) => {
                    let err = SlateDBError::from(e);

                    // Fencing errors are critical - another writer has taken over
                    // Don't retry, fail immediately
                    if err.is_fenced() {
                        error!(
                            topic = %self.topic,
                            partition = self.partition,
                            producer_id,
                            "FENCED during producer state persistence"
                        );
                        return Err(err);
                    }

                    if attempt < MAX_RETRIES - 1 {
                        warn!(
                            topic = %self.topic,
                            partition = self.partition,
                            producer_id,
                            attempt = attempt + 1,
                            max_retries = MAX_RETRIES,
                            error = %err,
                            "Retrying producer state persistence"
                        );
                        // Brief delay before retry (exponential backoff)
                        tokio::time::sleep(tokio::time::Duration::from_millis(10 * (1 << attempt)))
                            .await;
                    }

                    last_error = Some(err);
                }
            }
        }

        // All retries exhausted
        if let Some(err) = last_error {
            warn!(
                topic = %self.topic,
                partition = self.partition,
                producer_id,
                last_sequence,
                producer_epoch,
                error = %err,
                "Failed to persist producer state after {} retries \
                 (idempotency works for current session but may fail after restart)",
                MAX_RETRIES
            );
            super::metrics::record_producer_state_persistence_failure(&self.topic, self.partition);
        }
        Ok(())
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

    /// Set the load metrics collector for this partition.
    ///
    /// The collector is shared across all partitions via Arc, enabling
    /// aggregated load statistics for auto-balancing decisions.
    pub async fn set_load_collector(&self, collector: Arc<LoadMetricsCollector>) {
        *self.load_collector.write().await = Some(collector);
    }

    /// Record a produce operation in the load metrics.
    async fn record_produce_metrics(&self, bytes: u64, messages: u64) {
        if let Some(ref collector) = *self.load_collector.read().await {
            collector.record_produce(&self.topic, self.partition, bytes, messages);
        }
    }

    /// Record a fetch operation in the load metrics.
    async fn record_fetch_metrics(&self, bytes: u64, messages: u64) {
        if let Some(ref collector) = *self.load_collector.read().await {
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
    pub async fn append_batch(&self, records: &Bytes) -> SlateDBResult<i64> {
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

        // ==================================================================
        // EPOCH-BASED FENCING: Verify epoch before write
        // ==================================================================
        // This is the critical safety check that prevents TOCTOU races.
        // If another broker has acquired this partition (with a higher epoch),
        // we must reject the write to prevent data corruption.
        //
        // This check happens UNDER the write lock to prevent races between
        // the check and the actual write.
        if self.leader_epoch > 0 {
            let stored_epoch = match self.db.get(LEADER_EPOCH_KEY).await {
                Ok(Some(bytes)) => decode_leader_epoch(&bytes).unwrap_or(0),
                Ok(None) => 0,
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
                    // Prevents transient storage errors from blocking writes
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        error = %err,
                        "Failed to read epoch from storage, using cached value"
                    );
                    self.leader_epoch
                }
            };

            if stored_epoch != self.leader_epoch {
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
        }

        let base_offset = self.high_watermark.load(Ordering::SeqCst);

        // Reject invalid record counts to prevent offset corruption
        let record_count = parse_record_count(records);
        if record_count <= 0 {
            error!(
                topic = %self.topic,
                partition = self.partition,
                record_count,
                "Rejecting batch with invalid record count"
            );
            return Err(SlateDBError::Config(format!(
                "Invalid record count {} in batch for {}/{}",
                record_count, self.topic, self.partition
            )));
        }

        // Idempotency check: detects and rejects duplicate or out-of-order messages
        if let Some(producer_info) = parse_producer_info(records)
            && producer_info.is_idempotent()
            && let Some(state) = self.producer_states.get(&producer_info.producer_id)
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
                return Err(SlateDBError::FencedProducer {
                    producer_id: producer_info.producer_id,
                    expected_epoch: state.producer_epoch,
                    actual_epoch: producer_info.producer_epoch,
                });
            }

            // Higher epoch indicates new producer incarnation - reset sequence tracking
            if producer_info.producer_epoch > state.producer_epoch {
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
                        return Err(SlateDBError::SequenceOverflow {
                            producer_id: producer_info.producer_id,
                            topic: self.topic.clone(),
                            partition: self.partition,
                        });
                    }
                };

                if producer_info.first_sequence <= state.last_sequence {
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        producer_id = producer_info.producer_id,
                        first_sequence = producer_info.first_sequence,
                        last_seen = state.last_sequence,
                        "Rejecting duplicate batch"
                    );
                    super::metrics::record_idempotency_rejection("duplicate");
                    return Err(SlateDBError::DuplicateSequence {
                        producer_id: producer_info.producer_id,
                        expected_sequence: expected_seq,
                        received_sequence: producer_info.first_sequence,
                    });
                }

                if producer_info.first_sequence != expected_seq {
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        producer_id = producer_info.producer_id,
                        first_sequence = producer_info.first_sequence,
                        expected = expected_seq,
                        "Rejecting out-of-order batch"
                    );
                    super::metrics::record_idempotency_rejection("out_of_order");
                    return Err(SlateDBError::OutOfOrderSequence {
                        producer_id: producer_info.producer_id,
                        expected_sequence: expected_seq,
                        received_sequence: producer_info.first_sequence,
                    });
                }
            }
        }

        let new_hwm = base_offset + record_count as i64;

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

        // Uses FAST_WRITE_OPTIONS (await_durable=false) for throughput.
        // Durability is provided by SlateDB's periodic flush_interval.
        let key = encode_record_key(base_offset);
        let write_result = self
            .db
            .put_with_options(&key, &buffer, &PutOptions::default(), &FAST_WRITE_OPTIONS)
            .await;

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

        // Update producer state after successful write
        // This must happen after the write is confirmed durable
        if let Some(producer_info) = parse_producer_info(records)
            && producer_info.is_idempotent()
        {
            // Calculate last sequence in this batch
            let last_sequence = producer_info
                .last_sequence()
                .unwrap_or(producer_info.first_sequence);

            // Update in-memory cache
            self.producer_states.insert(
                producer_info.producer_id,
                ProducerState {
                    last_sequence,
                    producer_epoch: producer_info.producer_epoch,
                },
            );

            // CRITICAL: Persist producer state to SlateDB for durability
            // This ensures idempotency survives broker restarts
            self.persist_producer_state(
                producer_info.producer_id,
                last_sequence,
                producer_info.producer_epoch,
            )
            .await?;

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
        self.record_produce_metrics(records.len() as u64, record_count as u64)
            .await;

        Ok(base_offset)
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
    /// 3. Collect batches until max response size is reached
    ///
    /// Replaced individual db.get() calls in loop with range scan.
    pub async fn fetch_from(&self, fetch_offset: i64) -> SlateDBResult<(i64, Option<Bytes>)> {
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

        let max_size = self.max_fetch_response_size.load(Ordering::Relaxed);

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

        while let Ok(Some(item)) = iter.next().await {
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
                // Corrupted batch, stop iteration
                warn!(
                    topic = %self.topic,
                    partition = self.partition,
                    offset = current_offset,
                    "Batch with invalid record count"
                );
                break;
            }

            // Check if adding this batch would exceed max_size
            if combined.len() + batch_data.len() > max_size {
                // If we have some data, return what we have
                if !combined.is_empty() {
                    break;
                }
                // If this single batch exceeds max_size, truncate it
                let remaining = max_size.saturating_sub(combined.len());
                if remaining > 0 {
                    combined.extend_from_slice(&batch_data[..remaining.min(batch_data.len())]);
                }
                break;
            }

            // Add to batch index for future lookups
            self.add_to_batch_index(current_offset, record_count);

            combined.extend_from_slice(batch_data);
            batch_count += 1;
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
            self.record_fetch_metrics(r.len() as u64, batch_count as u64)
                .await;
        }

        Ok((high_watermark, records))
    }

    /// Find the batch that contains or follows the given offset.
    ///
    /// Strategy:
    /// 1. Check if fetch_offset is exactly a batch start in the cache (fast path)
    /// 2. Try exact match in SlateDB
    /// 3. Use SlateDB range scan to find next batch
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

        // Strategy 2: Try exact match in SlateDB
        let key = encode_record_key(fetch_offset);
        if let Some(data) = self.db.get(&key).await? {
            // Add to index for future lookups
            let record_count = parse_record_count(&data);
            self.add_to_batch_index(fetch_offset, record_count);
            return Ok(Some(fetch_offset));
        }

        // Strategy 3: Use SlateDB range scan to find the batch containing or following fetch_offset
        // This is efficient because SlateDB organizes data by key prefix
        let start_key = encode_record_key(0); // Start from beginning to find batch containing offset
        let end_key = encode_record_key(high_watermark);

        let mut iter = self
            .db
            .scan(start_key.as_slice()..end_key.as_slice())
            .await?;

        // Scan through batches to find the one containing or following fetch_offset
        while let Ok(Some(item)) = iter.next().await {
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

                // Check if this batch contains or follows fetch_offset
                let batch_end = offset + record_count as i64;
                if offset <= fetch_offset && batch_end > fetch_offset {
                    // This batch contains fetch_offset
                    return Ok(Some(offset));
                } else if offset > fetch_offset {
                    // This batch comes after fetch_offset
                    return Ok(Some(offset));
                }
            }
        }

        // No batch found
        Ok(None)
    }

    /// Add a batch entry to the index.
    /// Uses lock-free moka cache with automatic LRU eviction.
    fn add_to_batch_index(&self, base_offset: i64, record_count: i32) {
        self.batch_index.insert(base_offset, record_count);
        // No manual eviction needed - moka handles LRU automatically
    }

    /// Warm the batch index cache by pre-loading recent batches.
    ///
    /// This is called during partition open to avoid cold-start cache misses.
    /// Pre-populates the cache with the most recent batches (up to batch_index_max_size).
    ///
    /// Performance: This adds ~10-50ms to partition open time but significantly
    /// reduces fetch latency for the first requests after partition acquisition.
    async fn warm_batch_index(&self) {
        use super::keys::{RECORD_KEY_PREFIX, decode_record_offset};

        let start = std::time::Instant::now();
        let mut count = 0u32;

        // Scan all record keys and populate the cache
        // SlateDB scan is efficient - reads SST files sequentially
        let start_key = [RECORD_KEY_PREFIX];
        let end_key = [RECORD_KEY_PREFIX + 1];

        match self.db.scan(start_key.as_slice()..end_key.as_slice()).await {
            Ok(mut iter) => {
                while let Ok(Some(item)) = iter.next().await {
                    if count >= self.batch_index_max_size as u32 {
                        break; // Cache is full
                    }

                    if let Some(offset) = decode_record_offset(&item.key) {
                        // Strip HWM metadata if present
                        let batch_data = if item.value.len() >= 8 {
                            &item.value[8..]
                        } else {
                            item.value.as_ref()
                        };
                        let record_count = parse_record_count(batch_data);
                        self.add_to_batch_index(offset, record_count);
                        count += 1;
                    }
                }
            }
            Err(e) => {
                // Non-fatal - cache warming is an optimization, not critical
                warn!(
                    topic = %self.topic,
                    partition = self.partition,
                    error = %e,
                    "Failed to warm batch index cache"
                );
            }
        }

        let elapsed = start.elapsed();
        if count > 0 {
            info!(
                topic = %self.topic,
                partition = self.partition,
                entries = count,
                elapsed_ms = elapsed.as_millis(),
                "Warmed batch index cache"
            );
        }

        // Record metrics
        super::metrics::record_batch_index_warm_entries(count as i64);
    }

    /// Get the earliest offset in this partition.
    ///
    /// Uses SlateDB range scan to find the first record key.
    /// This is important for log compaction scenarios where records
    /// at the beginning may have been deleted.
    pub async fn earliest_offset(&self) -> SlateDBResult<i64> {
        use super::keys::{RECORD_KEY_PREFIX, decode_record_offset};

        // Scan from the beginning of record keys to find the first one
        let start_key = [RECORD_KEY_PREFIX];
        let end_key = [RECORD_KEY_PREFIX + 1];

        let mut iter = self
            .db
            .scan(start_key.as_slice()..end_key.as_slice())
            .await?;

        // Get the first record
        if let Ok(Some(item)) = iter.next().await
            && let Some(offset) = decode_record_offset(&item.key)
        {
            return Ok(offset);
        }

        // No records found, earliest is 0
        Ok(0)
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
    pub async fn close(&self) -> SlateDBResult<()> {
        info!(topic = %self.topic, partition = self.partition, "Closing partition store");
        // Flush pending writes before closing (needed since we use await_durable=false)
        self.db.flush().await?;
        self.db.close().await?;
        Ok(())
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
            fail_on_recovery_gap: false,
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
    /// Default: 0 (epoch validation disabled for backwards compatibility).
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
        use super::keys::{LEADER_EPOCH_KEY, decode_leader_epoch, encode_leader_epoch};

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

        // Open SlateDB in a blocking context
        let (db, hwm, persisted_states, validated_epoch) = spawn_blocking(move || {
            let handle = tokio::runtime::Handle::current();
            handle.block_on(async move {
                let object_path = ObjectPath::from(path_for_task.as_str());
                // Use DbBuilder with explicit settings for memory limits and backpressure
                let db = Db::builder(object_path, object_store_for_task)
                    .with_settings(slatedb_settings)
                    .build()
                    .await?;

                // ==================================================================
                // EPOCH-BASED FENCING: Validate and store leader epoch
                // ==================================================================
                // This prevents TOCTOU races where we might write to a partition
                // that another broker has already acquired.
                let stored_epoch = match db.get(LEADER_EPOCH_KEY).await? {
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
                        return Err(slatedb::Error::invalid(format!(
                            "Epoch mismatch: expected {}, found {}",
                            expected_epoch, stored_epoch
                        )));
                    }

                    // Store our epoch to claim ownership
                    // This must be durable before we proceed with any writes
                    db.put_with_options(
                        LEADER_EPOCH_KEY,
                        &encode_leader_epoch(expected_epoch),
                        &PutOptions::default(),
                        &WriteOptions { await_durable: true }, // MUST be durable
                    )
                    .await?;

                    info!(
                        expected_epoch,
                        stored_epoch,
                        "Epoch fencing: Stored new epoch to SlateDB"
                    );
                }

                // Use the expected epoch if provided, otherwise use stored epoch
                let final_epoch = if expected_epoch > 0 {
                    expected_epoch
                } else {
                    stored_epoch
                };

                // Load persisted high watermark from DB
                let persisted_hwm = match db.get(HIGH_WATERMARK_KEY).await? {
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

                // Scan for highest record to recover from crash
                let recovered_hwm =
                    recover_hwm_from_records(&db, persisted_hwm, fail_on_gap)
                        .await?;

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
                    .await?;
                }

                // Load persisted producer states for idempotency
                let producer_states = load_producer_states(&db).await?;

                Ok::<_, slatedb::Error>((db, recovered_hwm, producer_states, final_epoch))
            })
        })
        .await
        .map_err(|e| SlateDBError::SlateDB(format!("Task join error: {}", e)))?
        .map_err(|e| {
            // Convert slatedb::Error to SlateDBError, checking for epoch mismatch
            let err_str = e.to_string();
            if err_str.contains("Epoch mismatch") {
                // Parse out the epochs from the error message
                SlateDBError::EpochMismatch {
                    topic: topic_for_epoch_error.clone(),
                    partition,
                    expected_epoch: self.leader_epoch,
                    stored_epoch: 0, // We don't have the exact stored value here
                }
            } else {
                SlateDBError::from(e)
            }
        })?;

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

        // Build producer state cache with configured TTL
        let producer_states_cache = MokaCache::builder()
            .max_capacity(DEFAULT_PRODUCER_STATE_CACHE_SIZE)
            .time_to_idle(Duration::from_secs(self.producer_state_cache_ttl_secs))
            .build();

        // Populate the cache with persisted producer states
        for (producer_id, (last_sequence, producer_epoch)) in persisted_states {
            producer_states_cache.insert(
                producer_id,
                ProducerState {
                    last_sequence,
                    producer_epoch,
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
            write_lock: Mutex::new(()),
            topic,
            partition,
            batch_index: batch_index_cache,
            max_fetch_response_size: AtomicUsize::new(self.max_fetch_response_size),
            batch_index_max_size: self.batch_index_max_size,
            zombie_mode: self.zombie_mode,
            producer_states: producer_states_cache,
            min_lease_ttl_for_write_secs: self.min_lease_ttl_for_write_secs,
            load_collector: RwLock::new(None),
            leader_epoch: validated_epoch,
        };

        // Warm the batch index cache (performance optimization)
        store.warm_batch_index().await;

        Ok(store)
    }
}

impl PartitionStore {
    /// Create a new builder for PartitionStore.
    pub fn builder() -> PartitionStoreBuilder {
        PartitionStoreBuilder::new()
    }
}
