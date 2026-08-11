//! Per-producer idempotency state tracked by [`super::PartitionStore`].

/// Default maximum number of producers to track per partition.
/// Bounds memory usage for the producer state cache.
pub(super) const DEFAULT_PRODUCER_STATE_CACHE_SIZE: u64 = 10_000;

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
