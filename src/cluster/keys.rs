//! Key encoding utilities for SlateDB storage.
//!
//! Keys are designed for efficient lexicographic ordering and prefix scanning.
//!
//! # Key Format Specification (v1)
//!
//! This module defines the binary key format used in SlateDB for storing partition data.
//!
//! ## Record Keys
//!
//! Format: `r<offset:8>` (9 bytes total)
//! - Prefix: `r` (0x72) - 1 byte
//! - Offset: Big-endian i64 - 8 bytes
//!
//! Big-endian encoding ensures lexicographic ordering matches numeric ordering.
//!
//! ## Metadata Keys
//!
//! Metadata keys start with underscore (`_`) to separate them from record keys:
//! - `_hwm` - High watermark (latest committed offset)
//! - `_epoch` - Leader epoch (fencing token)
//! - `_fmt` - On-disk format version (u32 big-endian)
//!
//! ## SlateDB Namespace
//!
//! Each partition has its own SlateDB instance at:
//! `{object_store_path}/{topic}/{partition}/`
//!
//! This means keys only need to be unique within a partition, not globally.

// Re-export common protocol utilities for convenience
pub use crate::protocol::{parse_record_count, patch_base_offset};

/// Prefix byte for record keys.
pub const RECORD_KEY_PREFIX: u8 = b'r';

/// Encode a record batch key.
///
/// Format: `r:<offset>` where offset is 8 bytes big-endian.
/// Within a partition's SlateDB, we only need the offset as the key.
pub fn encode_record_key(offset: i64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = RECORD_KEY_PREFIX;
    key[1..9].copy_from_slice(&offset.to_be_bytes());
    key
}

/// Decode offset from a record key.
pub fn decode_record_offset(key: &[u8]) -> Option<i64> {
    if key.len() >= 9 && key[0] == RECORD_KEY_PREFIX {
        let bytes: [u8; 8] = key[1..9].try_into().ok()?;
        Some(i64::from_be_bytes(bytes))
    } else {
        None
    }
}

/// Key for storing high watermark metadata.
///
/// Value format: 8-byte big-endian i64 representing the HWM offset.
pub const HIGH_WATERMARK_KEY: &[u8] = b"_hwm";

/// Key for storing the leader epoch (fencing token).
///
/// Value format: 4-byte big-endian i32 representing the leader epoch.
///
/// This key is used for epoch-based fencing to prevent TOCTOU races:
/// 1. When acquiring a partition, we write our epoch to this key
/// 2. Before each write, we verify the stored epoch matches our expected epoch
/// 3. If another broker acquired the partition, they would have written a higher epoch
///
/// This provides optimistic concurrency control at the storage layer.
pub const LEADER_EPOCH_KEY: &[u8] = b"_epoch";

/// Key for storing the on-disk format version.
///
/// Value format: 4-byte big-endian u32. Written on first partition open if
/// absent. Future migrations branch on this number; the current format is 1.
pub const FORMAT_VERSION_KEY: &[u8] = b"_fmt";

/// Key for storing the log start offset (LSO).
///
/// Value format: 8-byte big-endian i64.
///
/// The log start offset is the lowest offset still present in the log. It
/// starts at 0 and only moves forward when retention deletes a prefix of the
/// log. Fetches below this offset return `OffsetOutOfRange`, and
/// `ListOffsets(earliest)` reports it.
///
/// Written durably *before* the record keys below it are deleted, so a crash
/// mid-retention can never leave the LSO pointing below surviving data —
/// the worst case is an LSO above already-deleted records, which just means
/// retention re-runs are no-ops for that range.
pub const LOG_START_OFFSET_KEY: &[u8] = b"_lso";

/// Current on-disk format version. Bump only when the layout of records,
/// metadata keys, or value-frame encoding changes incompatibly.
pub const CURRENT_FORMAT_VERSION: u32 = 1;

/// Encode a leader epoch value.
///
/// Format: 4-byte big-endian i32.
pub fn encode_leader_epoch(epoch: i32) -> [u8; 4] {
    epoch.to_be_bytes()
}

/// Decode a leader epoch value.
///
/// Returns `Some(epoch)` if valid, `None` if the value is too short.
pub fn decode_leader_epoch(value: &[u8]) -> Option<i32> {
    if value.len() >= 4 {
        let bytes: [u8; 4] = value[..4].try_into().ok()?;
        Some(i32::from_be_bytes(bytes))
    } else {
        None
    }
}

// =============================================================================
// Producer State Keys (for idempotency persistence)
// =============================================================================

/// Prefix byte for producer state keys.
///
/// Producer state is persisted to ensure idempotency survives broker restarts.
/// Format: `p<producer_id:8>` (9 bytes total)
pub const PRODUCER_STATE_KEY_PREFIX: u8 = b'p';

/// Encode a producer state key.
///
/// Format: `p<producer_id:8>` where producer_id is 8 bytes big-endian.
/// The value stored is `[last_sequence:4][producer_epoch:2]` (6 bytes).
pub fn encode_producer_state_key(producer_id: i64) -> [u8; 9] {
    let mut key = [0u8; 9];
    key[0] = PRODUCER_STATE_KEY_PREFIX;
    key[1..9].copy_from_slice(&producer_id.to_be_bytes());
    key
}

/// Decode producer_id from a producer state key.
pub fn decode_producer_id(key: &[u8]) -> Option<i64> {
    if key.len() >= 9 && key[0] == PRODUCER_STATE_KEY_PREFIX {
        let bytes: [u8; 8] = key[1..9].try_into().ok()?;
        Some(i64::from_be_bytes(bytes))
    } else {
        None
    }
}

/// Producer state as persisted in SlateDB.
///
/// The retry-dedup pair (`last_first_sequence`, `last_base_offset`) is
/// persisted alongside the sequence tracking so that an exact network retry
/// of the most recent batch is re-acked with its original base offset even
/// across a broker restart or producer-state cache eviction — Kafka's
/// idempotent-producer contract. Legacy 6-byte values (pre-retry-dedup)
/// decode with the retry fields set to the "unknown" sentinel (-1).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PersistedProducerState {
    /// Last successfully written sequence number for this producer.
    pub last_sequence: i32,
    /// Producer epoch for fencing zombie producers.
    pub producer_epoch: i16,
    /// First sequence of the most recent successfully appended batch
    /// (-1 if unknown, e.g. decoded from a legacy value).
    pub last_first_sequence: i32,
    /// Base offset assigned to the most recent successfully appended batch
    /// (-1 if unknown).
    pub last_base_offset: i64,
}

/// Encode producer state value.
///
/// Format (v2): `[last_sequence:4][producer_epoch:2][last_first_sequence:4][last_base_offset:8]`
/// (18 bytes total). Decoders accept the legacy 6-byte prefix-only form.
pub fn encode_producer_state_value(state: &PersistedProducerState) -> [u8; 18] {
    let mut value = [0u8; 18];
    value[0..4].copy_from_slice(&state.last_sequence.to_be_bytes());
    value[4..6].copy_from_slice(&state.producer_epoch.to_be_bytes());
    value[6..10].copy_from_slice(&state.last_first_sequence.to_be_bytes());
    value[10..18].copy_from_slice(&state.last_base_offset.to_be_bytes());
    value
}

/// Decode producer state from a value.
///
/// Accepts both the current 18-byte format and the legacy 6-byte format
/// (in which case the retry-dedup fields are -1 / unknown).
pub fn decode_producer_state_value(value: &[u8]) -> Option<PersistedProducerState> {
    if value.len() < 6 {
        return None;
    }
    let last_sequence = i32::from_be_bytes(value[0..4].try_into().ok()?);
    let producer_epoch = i16::from_be_bytes(value[4..6].try_into().ok()?);
    let (last_first_sequence, last_base_offset) = if value.len() >= 18 {
        (
            i32::from_be_bytes(value[6..10].try_into().ok()?),
            i64::from_be_bytes(value[10..18].try_into().ok()?),
        )
    } else {
        (-1, -1)
    };
    Some(PersistedProducerState {
        last_sequence,
        producer_epoch,
        last_first_sequence,
        last_base_offset,
    })
}

// =============================================================================
// Record batch timestamp extraction (for time-based retention / ListOffsets)
// =============================================================================

/// Byte offset of `max_timestamp` (INT64) in a Kafka v2 record batch header.
///
/// Layout: base_offset(0..8) batch_length(8..12) partition_leader_epoch(12..16)
/// magic(16) crc(17..21) attributes(21..23) last_offset_delta(23..27)
/// base_timestamp(27..35) **max_timestamp(35..43)** ...
const BATCH_MAX_TIMESTAMP_OFFSET: usize = 35;

/// Extract `max_timestamp` (epoch millis) from a v2 record batch.
///
/// Returns `None` for batches too short to contain the field. A value of -1
/// means the producer did not set timestamps (CreateTime with no records or
/// pre-v2 formats) — callers should treat such batches as "no timestamp".
pub fn parse_batch_max_timestamp(batch: &[u8]) -> Option<i64> {
    let end = BATCH_MAX_TIMESTAMP_OFFSET + 8;
    if batch.len() < end {
        return None;
    }
    let bytes: [u8; 8] = batch[BATCH_MAX_TIMESTAMP_OFFSET..end].try_into().ok()?;
    Some(i64::from_be_bytes(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_key_encoding() {
        let offset = 12345i64;
        let key = encode_record_key(offset);

        assert_eq!(key[0], b'r');
        assert_eq!(decode_record_offset(&key), Some(offset));
    }

    #[test]
    fn test_record_key_ordering() {
        // Verify that keys are ordered correctly
        let key1 = encode_record_key(100);
        let key2 = encode_record_key(200);
        let key3 = encode_record_key(50);

        assert!(key1 < key2);
        assert!(key3 < key1);
    }

    #[test]
    fn test_record_key_edge_cases() {
        // Test with zero offset
        let key = encode_record_key(0);
        assert_eq!(decode_record_offset(&key), Some(0));

        // Test with negative offset
        let key = encode_record_key(-1);
        assert_eq!(decode_record_offset(&key), Some(-1));

        // Test with max offset
        let key = encode_record_key(i64::MAX);
        assert_eq!(decode_record_offset(&key), Some(i64::MAX));

        // Test with min offset
        let key = encode_record_key(i64::MIN);
        assert_eq!(decode_record_offset(&key), Some(i64::MIN));
    }

    #[test]
    fn test_decode_record_offset_invalid() {
        // Too short
        assert!(decode_record_offset(&[]).is_none());
        assert!(decode_record_offset(b"r").is_none());
        assert!(decode_record_offset(&[b'r', 0, 0, 0, 0, 0, 0, 0]).is_none()); // Only 8 bytes total

        // Wrong prefix
        assert!(decode_record_offset(&[b'x', 0, 0, 0, 0, 0, 0, 0, 0]).is_none());
        assert!(decode_record_offset(&[b'p', 0, 0, 0, 0, 0, 0, 0, 0]).is_none());
    }

    // ==========================================================================
    // Leader Epoch Key Tests
    // ==========================================================================

    #[test]
    fn test_leader_epoch_encoding() {
        let epoch = 42i32;
        let encoded = encode_leader_epoch(epoch);
        assert_eq!(decode_leader_epoch(&encoded), Some(epoch));
    }

    #[test]
    fn test_leader_epoch_edge_cases() {
        // Zero
        assert_eq!(decode_leader_epoch(&encode_leader_epoch(0)), Some(0));

        // Negative
        assert_eq!(decode_leader_epoch(&encode_leader_epoch(-1)), Some(-1));

        // Max
        assert_eq!(
            decode_leader_epoch(&encode_leader_epoch(i32::MAX)),
            Some(i32::MAX)
        );

        // Min
        assert_eq!(
            decode_leader_epoch(&encode_leader_epoch(i32::MIN)),
            Some(i32::MIN)
        );
    }

    #[test]
    fn test_decode_leader_epoch_invalid() {
        // Too short
        assert!(decode_leader_epoch(&[]).is_none());
        assert!(decode_leader_epoch(&[0]).is_none());
        assert!(decode_leader_epoch(&[0, 0]).is_none());
        assert!(decode_leader_epoch(&[0, 0, 0]).is_none());
    }

    #[test]
    fn test_decode_leader_epoch_extra_bytes() {
        // Extra bytes should be ignored
        let mut value = encode_leader_epoch(42).to_vec();
        value.extend_from_slice(&[0xFF, 0xFF]); // Add extra bytes
        assert_eq!(decode_leader_epoch(&value), Some(42));
    }

    // ==========================================================================
    // Producer State Key Tests
    // ==========================================================================

    #[test]
    fn test_producer_state_key_encoding() {
        let producer_id = 123456789i64;
        let key = encode_producer_state_key(producer_id);

        assert_eq!(key[0], b'p');
        assert_eq!(decode_producer_id(&key), Some(producer_id));
    }

    #[test]
    fn test_producer_state_key_ordering() {
        // Verify that keys are ordered correctly
        let key1 = encode_producer_state_key(100);
        let key2 = encode_producer_state_key(200);
        let key3 = encode_producer_state_key(50);

        assert!(key1 < key2);
        assert!(key3 < key1);
    }

    fn pstate(
        last_sequence: i32,
        producer_epoch: i16,
        last_first_sequence: i32,
        last_base_offset: i64,
    ) -> PersistedProducerState {
        PersistedProducerState {
            last_sequence,
            producer_epoch,
            last_first_sequence,
            last_base_offset,
        }
    }

    #[test]
    fn test_producer_state_value_encoding() {
        let state = pstate(42, 3, 40, 1234);
        let value = encode_producer_state_value(&state);
        assert_eq!(decode_producer_state_value(&value), Some(state));
    }

    #[test]
    fn test_producer_state_value_edge_cases() {
        // Test with max values
        let state = pstate(i32::MAX, i16::MAX, i32::MAX, i64::MAX);
        assert_eq!(
            decode_producer_state_value(&encode_producer_state_value(&state)),
            Some(state)
        );

        // Test with negative values
        let state = pstate(-1, -1, -1, -1);
        assert_eq!(
            decode_producer_state_value(&encode_producer_state_value(&state)),
            Some(state)
        );

        // Test with zero
        let state = pstate(0, 0, 0, 0);
        assert_eq!(
            decode_producer_state_value(&encode_producer_state_value(&state)),
            Some(state)
        );
    }

    #[test]
    fn test_producer_state_value_legacy_6_byte_decode() {
        // Legacy format: [last_sequence:4][producer_epoch:2] only.
        // Retry-dedup fields must decode as -1 (unknown).
        let mut legacy = Vec::new();
        legacy.extend_from_slice(&42i32.to_be_bytes());
        legacy.extend_from_slice(&3i16.to_be_bytes());
        assert_eq!(
            decode_producer_state_value(&legacy),
            Some(pstate(42, 3, -1, -1))
        );
    }

    #[test]
    fn test_parse_batch_max_timestamp() {
        // Build a minimal v2 batch header (61 bytes) with a known max_timestamp.
        let mut batch = vec![0u8; 61];
        let ts: i64 = 1_700_000_000_123;
        batch[35..43].copy_from_slice(&ts.to_be_bytes());
        assert_eq!(parse_batch_max_timestamp(&batch), Some(ts));

        // Too short to contain the field
        assert_eq!(parse_batch_max_timestamp(&batch[..42]), None);
        assert_eq!(parse_batch_max_timestamp(&[]), None);
    }

    #[test]
    fn test_decode_producer_state_invalid() {
        // Too short
        assert!(decode_producer_state_value(&[0, 1, 2]).is_none());
        assert!(decode_producer_state_value(&[]).is_none());

        // Empty key
        assert!(decode_producer_id(&[]).is_none());

        // Wrong prefix
        assert!(decode_producer_id(&[b'r', 0, 0, 0, 0, 0, 0, 0, 0]).is_none());
    }

    #[test]
    fn test_producer_state_key_edge_cases() {
        // Zero producer_id
        let key = encode_producer_state_key(0);
        assert_eq!(decode_producer_id(&key), Some(0));

        // Negative producer_id
        let key = encode_producer_state_key(-1);
        assert_eq!(decode_producer_id(&key), Some(-1));

        // Max producer_id
        let key = encode_producer_state_key(i64::MAX);
        assert_eq!(decode_producer_id(&key), Some(i64::MAX));
    }

    #[test]
    fn test_decode_producer_id_short_key() {
        // 8 bytes total (need 9)
        assert!(decode_producer_id(&[b'p', 0, 0, 0, 0, 0, 0, 0]).is_none());
    }

    #[test]
    fn test_decode_producer_state_value_extra_bytes() {
        // Extra bytes should be ignored
        let mut value = encode_producer_state_value(&pstate(100, 5, 98, 7)).to_vec();
        value.extend_from_slice(&[0xFF, 0xFF]); // Add extra bytes
        let state = decode_producer_state_value(&value).unwrap();
        assert_eq!(state.last_sequence, 100);
        assert_eq!(state.producer_epoch, 5);
        assert_eq!(state.last_first_sequence, 98);
        assert_eq!(state.last_base_offset, 7);
    }

    // ==========================================================================
    // Key Prefix Tests
    // ==========================================================================

    #[test]
    fn test_key_prefixes() {
        assert_eq!(RECORD_KEY_PREFIX, b'r');
        assert_eq!(PRODUCER_STATE_KEY_PREFIX, b'p');
        assert_eq!(HIGH_WATERMARK_KEY, b"_hwm");
        assert_eq!(LEADER_EPOCH_KEY, b"_epoch");
    }

    // ==========================================================================
    // Property tests — encode/decode roundtrips for the full key/value space.
    //
    // These complement the hand-written cases above by exhaustively
    // covering integer ranges (i64::MIN..=i64::MAX, i32::MIN..=i32::MAX)
    // that point examples can't hit. A failure here means the on-disk
    // format would silently corrupt under specific inputs — the worst
    // class of storage bug.
    // ==========================================================================

    use proptest::prelude::*;

    proptest! {
        #[test]
        fn record_key_roundtrip(offset in any::<i64>()) {
            let key = encode_record_key(offset);
            prop_assert_eq!(decode_record_offset(&key), Some(offset));
        }

        #[test]
        fn record_keys_sort_lexicographically(a in any::<i64>(), b in any::<i64>()) {
            // Big-endian + signed offset: lexicographic order over the byte
            // representation matches numeric order ONLY when both offsets
            // share a sign bit. Negative offsets are never written in
            // practice (HWM is non-negative), but the property still holds
            // for the non-negative subset.
            prop_assume!(a >= 0 && b >= 0);
            let ka = encode_record_key(a);
            let kb = encode_record_key(b);
            prop_assert_eq!(a.cmp(&b), ka.cmp(&kb));
        }

        #[test]
        fn leader_epoch_roundtrip(epoch in any::<i32>()) {
            let v = encode_leader_epoch(epoch);
            prop_assert_eq!(decode_leader_epoch(&v), Some(epoch));
        }

        #[test]
        fn leader_epoch_decode_extra_bytes_ok(epoch in any::<i32>(), extra in proptest::collection::vec(any::<u8>(), 0..16)) {
            let mut v = encode_leader_epoch(epoch).to_vec();
            v.extend_from_slice(&extra);
            prop_assert_eq!(decode_leader_epoch(&v), Some(epoch));
        }

        #[test]
        fn leader_epoch_decode_too_short_none(bytes in proptest::collection::vec(any::<u8>(), 0..4)) {
            prop_assert_eq!(decode_leader_epoch(&bytes), None);
        }

        #[test]
        fn producer_state_key_roundtrip(producer_id in any::<i64>()) {
            let key = encode_producer_state_key(producer_id);
            prop_assert_eq!(decode_producer_id(&key), Some(producer_id));
        }

        #[test]
        fn producer_state_value_roundtrip(
            seq in any::<i32>(),
            epoch in any::<i16>(),
            first_seq in any::<i32>(),
            base_offset in any::<i64>(),
        ) {
            let state = PersistedProducerState {
                last_sequence: seq,
                producer_epoch: epoch,
                last_first_sequence: first_seq,
                last_base_offset: base_offset,
            };
            let v = encode_producer_state_value(&state);
            prop_assert_eq!(decode_producer_state_value(&v), Some(state));
        }

        #[test]
        fn record_key_wrong_prefix_returns_none(prefix in any::<u8>(), offset in any::<i64>()) {
            prop_assume!(prefix != RECORD_KEY_PREFIX);
            let mut key = [0u8; 9];
            key[0] = prefix;
            key[1..9].copy_from_slice(&offset.to_be_bytes());
            prop_assert_eq!(decode_record_offset(&key), None);
        }

        // ----------------------------------------------------------------
        // P3-3: producer-state value decode robustness across mixed formats.
        //
        // `decode_producer_state_value` accepts both the current 18-byte
        // layout AND the legacy 6-byte prefix-only layout. A bad decode
        // here is silent data loss for idempotent producers, so the
        // boundary cases (lengths 0..=32, including 5 / 6 / 17 / 18) need
        // explicit coverage that point examples can't supply.
        // ----------------------------------------------------------------

        #[test]
        fn producer_state_decode_arbitrary_short_never_panics(
            bytes in proptest::collection::vec(any::<u8>(), 0..=32)
        ) {
            // Catch-all robustness: any input up to twice the v2 size must
            // return `Some`/`None` without panicking. Lengths 0..6 are
            // None; ≥6 are Some.
            let decoded = decode_producer_state_value(&bytes);
            match bytes.len() {
                0..=5 => prop_assert!(decoded.is_none()),
                _ => prop_assert!(decoded.is_some()),
            }
        }

        #[test]
        fn producer_state_decode_legacy_6_byte(
            seq in any::<i32>(),
            epoch in any::<i16>(),
        ) {
            // Legacy-format guarantee: a 6-byte value must decode with
            // both retry-dedup fields at the "unknown" sentinel (-1).
            let mut legacy = Vec::with_capacity(6);
            legacy.extend_from_slice(&seq.to_be_bytes());
            legacy.extend_from_slice(&epoch.to_be_bytes());
            let decoded = decode_producer_state_value(&legacy)
                .expect("6-byte value decodes");
            prop_assert_eq!(decoded.last_sequence, seq);
            prop_assert_eq!(decoded.producer_epoch, epoch);
            prop_assert_eq!(decoded.last_first_sequence, -1);
            prop_assert_eq!(decoded.last_base_offset, -1);
        }

        #[test]
        fn producer_state_decode_v2_prefix_matches_legacy(
            seq in any::<i32>(),
            epoch in any::<i16>(),
            first_seq in any::<i32>(),
            base_offset in any::<i64>(),
        ) {
            // For length ≥18, the (last_sequence, producer_epoch) prefix
            // must match what the legacy 6-byte decode would produce.
            // This is the contract that lets a broker upgraded mid-flight
            // read either format without confusion.
            let state = PersistedProducerState {
                last_sequence: seq,
                producer_epoch: epoch,
                last_first_sequence: first_seq,
                last_base_offset: base_offset,
            };
            let v2 = encode_producer_state_value(&state);
            let v2_decoded = decode_producer_state_value(&v2)
                .expect("v2 value decodes");
            let legacy_decoded = decode_producer_state_value(&v2[..6])
                .expect("legacy prefix decodes");
            prop_assert_eq!(v2_decoded.last_sequence, legacy_decoded.last_sequence);
            prop_assert_eq!(v2_decoded.producer_epoch, legacy_decoded.producer_epoch);
            // Legacy decode treats the v2 retry fields as unknown.
            prop_assert_eq!(legacy_decoded.last_first_sequence, -1);
            prop_assert_eq!(legacy_decoded.last_base_offset, -1);
        }

        #[test]
        fn producer_state_decode_partial_v2_returns_legacy_fields(
            seq in any::<i32>(),
            epoch in any::<i16>(),
            extra in proptest::collection::vec(any::<u8>(), 0..12),
        ) {
            // Lengths 6..=17 are "between" formats: the 6-byte prefix is
            // honored; trailing bytes < 12 are not enough for v2 retry
            // fields, so retry fields stay at -1.
            let mut v = Vec::with_capacity(6 + extra.len());
            v.extend_from_slice(&seq.to_be_bytes());
            v.extend_from_slice(&epoch.to_be_bytes());
            v.extend_from_slice(&extra);
            prop_assume!(v.len() < 18);
            let decoded = decode_producer_state_value(&v)
                .expect("≥6-byte value decodes");
            prop_assert_eq!(decoded.last_sequence, seq);
            prop_assert_eq!(decoded.producer_epoch, epoch);
            prop_assert_eq!(decoded.last_first_sequence, -1);
            prop_assert_eq!(decoded.last_base_offset, -1);
        }
    }
}
