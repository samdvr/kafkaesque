//! Centralized protocol and configuration constants.
//!
//! This module consolidates all magic numbers and protocol constants used throughout
//! the Kafkaesque Kafka broker. Having them in one place makes it easier to:
//!
//! - Understand the protocol constraints
//! - Update values consistently
//! - Document the rationale for each constant
//!
//! # Categories
//!
//! - **Protocol Constants**: Kafka wire protocol sizes and limits
//! - **Network Constants**: Connection and message size limits
//! - **Storage Constants**: Partition and batch handling limits
//! - **Coordinator Constants**: Cluster coordination limits

// =============================================================================
// Protocol Constants (Kafka Wire Protocol)
// =============================================================================

/// Minimum size of a Kafka record batch header.
///
/// This is the fixed overhead for each record batch, consisting of:
/// - baseOffset (8 bytes)
/// - batchLength (4 bytes)
/// - partitionLeaderEpoch (4 bytes)
/// - magic (1 byte)
/// - crc (4 bytes)
/// - attributes (2 bytes)
/// - lastOffsetDelta (4 bytes)
/// - baseTimestamp (8 bytes)
/// - maxTimestamp (8 bytes)
/// - producerId (8 bytes)
/// - producerEpoch (2 bytes)
/// - baseSequence (4 bytes)
/// - recordCount (4 bytes)
///
/// Total: 61 bytes
pub const MIN_BATCH_HEADER_SIZE: usize = 61;

// -----------------------------------------------------------------------------
// RecordBatch Header Field Offsets
// -----------------------------------------------------------------------------
// These constants document the byte offsets for each field in the Kafka
// RecordBatch header, as defined in the protocol specification.

/// Offset of the base_offset field (8 bytes, i64).
pub const BATCH_BASE_OFFSET: usize = 0;

/// Offset of the CRC field (4 bytes, u32 big-endian).
/// The CRC is computed over bytes from BATCH_CRC_DATA_START to end of batch.
pub const BATCH_CRC_OFFSET: usize = 17;

/// Start of the CRC-covered region (attributes field).
/// CRC is computed over bytes [21..end] of the batch.
pub const BATCH_CRC_DATA_START: usize = 21;

/// Offset of the last_offset_delta field (4 bytes, i32 big-endian).
/// record_count = last_offset_delta + 1
pub const BATCH_LAST_OFFSET_DELTA_OFFSET: usize = 23;

/// Minimum bytes needed to parse last_offset_delta.
pub const BATCH_LAST_OFFSET_DELTA_END: usize = 27;

/// Offset of the producer_id field (8 bytes, i64 big-endian).
pub const BATCH_PRODUCER_ID_OFFSET: usize = 43;

/// End offset of producer_id field.
pub const BATCH_PRODUCER_ID_END: usize = 51;

/// Offset of the producer_epoch field (2 bytes, i16 big-endian).
pub const BATCH_PRODUCER_EPOCH_OFFSET: usize = 51;

/// End offset of producer_epoch field.
pub const BATCH_PRODUCER_EPOCH_END: usize = 53;

/// Offset of the first_sequence field (4 bytes, i32 big-endian).
pub const BATCH_FIRST_SEQUENCE_OFFSET: usize = 53;

/// End offset of first_sequence field.
pub const BATCH_FIRST_SEQUENCE_END: usize = 57;

/// Maximum allowed array size in Kafka protocol parsing.
///
/// This prevents memory exhaustion from malformed messages that claim
/// to have billions of elements. 100,000 is generous but bounded.
pub const MAX_PROTOCOL_ARRAY_SIZE: i32 = 100_000;

// =============================================================================
// Network Constants
// =============================================================================

/// Default maximum message size (100 MB).
///
/// This is the maximum size of a single Kafka request or response.
/// It prevents memory exhaustion from malicious or malformed messages.
/// Can be overridden via `ClusterConfig.max_message_size`.
///
/// Note: This should match Kafka's `message.max.bytes` default.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

/// Default maximum connections per IP address.
///
/// This prevents a single IP from exhausting server resources.
/// Can be overridden via server configuration.
pub const DEFAULT_MAX_CONNECTIONS_PER_IP: usize = 100;

/// Default maximum total connections across all clients.
///
/// This prevents resource exhaustion from distributed DoS attacks.
/// Set to 0 for unlimited (not recommended in production).
/// Can be overridden via server configuration.
pub const DEFAULT_MAX_TOTAL_CONNECTIONS: usize = 10_000;

// =============================================================================
// Storage Constants
// =============================================================================

/// Default maximum fetch response size (1 MB).
///
/// Limits how much data is returned in a single fetch response.
/// Can be overridden via `ClusterConfig.max_fetch_response_size`.
pub const DEFAULT_MAX_FETCH_RESPONSE_SIZE: usize = 1024 * 1024;

/// Default maximum entries in the per-partition batch index.
///
/// The batch index maps base offsets to record counts for efficient
/// offset lookup during fetch operations.
///
/// Memory usage: ~16 bytes per entry (i64 offset + i32 count).
/// Default 10,000 entries = ~160KB per partition.
pub const DEFAULT_BATCH_INDEX_MAX_SIZE: usize = 10_000;

/// Default maximum concurrent partition writes per produce request.
///
/// Limits parallelism to prevent overwhelming the storage layer.
pub const DEFAULT_MAX_CONCURRENT_PARTITION_WRITES: usize = 16;

/// Default maximum concurrent partition reads per fetch request.
///
/// Limits parallelism to prevent overwhelming the storage layer.
pub const DEFAULT_MAX_CONCURRENT_PARTITION_READS: usize = 16;

/// Default maximum entries in the producer state cache.
///
/// The producer state cache stores (last_sequence, producer_epoch) per producer_id
/// for idempotency checks. Uses LRU eviction when capacity is reached.
///
/// Memory usage: ~24 bytes per entry (i64 producer_id + i32 seq + i16 epoch + overhead).
/// Default 10,000 entries = ~240KB per partition.
pub const DEFAULT_PRODUCER_STATE_CACHE_SIZE: u64 = 10_000;

/// Default TTL for producer state cache entries (15 minutes).
///
/// Entries expire after this duration of inactivity. This allows cleanup of
/// ephemeral producers while keeping active producers in cache.
///
/// Increased from 5 minutes to 15 minutes to better handle:
/// - Producers with bursty traffic patterns
/// - Backpressure scenarios where producers are temporarily idle
/// - Network partitions that cause temporary disconnects
///
/// A producer that reconnects after TTL expiry may have its idempotency state
/// lost, potentially causing duplicate detection to fail. The longer TTL reduces
/// this risk while still allowing cleanup of truly abandoned producers.
pub const DEFAULT_PRODUCER_STATE_CACHE_TTL_SECS: u64 = 900;

/// Default timeout for SlateDB write operations (30 seconds).
///
/// Prevents slow object store writes from blocking indefinitely.
pub const DEFAULT_WRITE_TIMEOUT_SECS: u64 = 30;

/// Default timeout for SlateDB open operations (120 seconds).
///
/// Opening SlateDB involves WAL replay and may take time on slow object stores.
/// This prevents indefinite blocking during partition acquisition.
pub const DEFAULT_SLATEDB_OPEN_TIMEOUT_SECS: u64 = 120;

/// Default timeout for reading a complete request from a client (30 seconds).
///
/// This prevents slowloris-style attacks where clients send data very slowly
/// to tie up server resources. If a request isn't completely received within
/// this timeout, the connection is closed.
pub const DEFAULT_REQUEST_READ_TIMEOUT_SECS: u64 = 30;

/// Default timeout for processing a request (60 seconds).
///
/// This prevents runaway handlers from blocking resources indefinitely.
/// Long-running operations should be designed to complete within this timeout.
pub const DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS: u64 = 60;

// =============================================================================
// Lease and Heartbeat Constants
// =============================================================================

/// Default partition lease duration in seconds.
///
/// This is how long a broker holds exclusive ownership of a partition.
/// If the broker fails to renew the lease before expiration, another
/// broker can acquire the partition.
pub const DEFAULT_LEASE_DURATION_SECS: u64 = 60;

/// Default lease renewal interval in seconds.
///
/// Brokers renew their partition leases at this interval.
/// Should be significantly less than lease duration to handle
/// temporary network issues.
pub const DEFAULT_LEASE_RENEWAL_INTERVAL_SECS: u64 = 20;

/// Default broker heartbeat interval in seconds.
///
/// How often brokers send heartbeats to indicate they're alive.
pub const DEFAULT_HEARTBEAT_INTERVAL_SECS: u64 = 10;

/// Default ownership check interval in seconds.
///
/// How often the ownership loop checks for partition changes.
pub const DEFAULT_OWNERSHIP_CHECK_INTERVAL_SECS: u64 = 5;

/// Default session timeout check interval in seconds.
///
/// How often to check for expired consumer group members.
pub const DEFAULT_SESSION_TIMEOUT_CHECK_INTERVAL_SECS: u64 = 10;

/// Minimum remaining lease TTL (in seconds) to use cached lease.
///
/// When a cached lease has less than this remaining, we refresh via Raft.
/// This provides a safety buffer for the write to complete before lease expires.
/// Should be significantly less than MIN_LEASE_TTL_FOR_WRITE_SECS but large
/// enough to absorb network latency and write time.
///
/// With 25s: We refresh when lease has <25s remaining, giving ~35s of cache hits
/// per 60s lease. This removes Raft overhead from ~58% of produce requests while
/// maintaining safety margins for the increased MIN_LEASE_TTL_FOR_WRITE_SECS.
pub const LEASE_CACHE_REFRESH_THRESHOLD_SECS: u64 = 25;

/// Default minimum remaining lease TTL (in seconds) required to allow writes.
///
/// If the lease has less than this remaining TTL, writes are rejected to prevent
/// TOCTOU (time-of-check-to-time-of-use) races where:
/// 1. Broker checks lease (valid)
/// 2. Write takes longer than remaining lease (due to GC, CPU contention, slow I/O)
/// 3. Lease expires mid-write
/// 4. Another broker acquires partition
/// 5. Both brokers write to same partition (split-brain)
///
/// This value should be configured based on:
/// - Expected worst-case write latency to object store (variable, 50-500ms for S3)
/// - Network latency between brokers
/// - GC pauses (can be 50-100ms in JVM clients, longer in extreme cases)
/// - CPU steal in cloud VMs (can be 100ms-2s under contention)
/// - Combined worst case: 2-3+ seconds, so 30s provides ~10x safety margin
///
/// Default: 30 seconds (assumes writes complete within 30s worst-case)
pub const DEFAULT_MIN_LEASE_TTL_FOR_WRITE_SECS: u64 = 30;

/// Default number of partitions for auto-created topics.
///
/// Higher partition counts enable better write parallelism since each partition
/// 10 partitions provides a good balance for most workloads, enabling up to
/// 10x write parallelism while keeping resource usage reasonable.
pub const DEFAULT_NUM_PARTITIONS: i32 = 10;

// =============================================================================
// Coordinator Constants
// =============================================================================

/// Milliseconds per second, for time conversions.
///
/// Used when converting from Kafka's millisecond-based timeouts to
/// second-based TTLs.
pub const MS_PER_SECOND: i32 = 1000;

/// Number of virtual nodes per broker in consistent hashing.
///
/// Higher values provide better distribution but use more memory.
/// 150 virtual nodes provides good balance for typical cluster sizes.
/// With N brokers, the ring has N * 150 points.
pub const VIRTUAL_NODES_PER_BROKER: usize = 150;

/// Minimum consumer group TTL in seconds.
///
/// Groups cannot have a TTL shorter than this to prevent excessive
/// churn in the coordinator.
pub const MIN_GROUP_TTL_SECS: i32 = 60;

/// Maximum size of consumer group member metadata.
///
/// This includes subscription info, user data, etc.
/// 1 MB should be more than enough for any reasonable use case.
pub const MAX_MEMBER_METADATA_SIZE: usize = 1024 * 1024;

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_header_size_is_correct() {
        // Verify the batch header size matches the documented breakdown
        let expected = 8  // baseOffset
            + 4  // batchLength
            + 4  // partitionLeaderEpoch
            + 1  // magic
            + 4  // crc
            + 2  // attributes
            + 4  // lastOffsetDelta
            + 8  // baseTimestamp
            + 8  // maxTimestamp
            + 8  // producerId
            + 2  // producerEpoch
            + 4  // baseSequence
            + 4; // recordCount
        assert_eq!(MIN_BATCH_HEADER_SIZE, expected);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_message_size_is_reasonable() {
        // 100 MB is a reasonable max message size
        assert_eq!(DEFAULT_MAX_MESSAGE_SIZE, 100 * 1024 * 1024);
        assert!(DEFAULT_MAX_MESSAGE_SIZE > DEFAULT_MAX_FETCH_RESPONSE_SIZE);
    }

    #[test]
    fn test_fetch_response_size_is_reasonable() {
        // 1 MB default fetch response
        assert_eq!(DEFAULT_MAX_FETCH_RESPONSE_SIZE, 1024 * 1024);
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_array_size_limit_is_bounded() {
        // Should be large enough for practical use but bounded
        assert!(MAX_PROTOCOL_ARRAY_SIZE >= 1000);
        assert!(MAX_PROTOCOL_ARRAY_SIZE <= 1_000_000);
    }
}
