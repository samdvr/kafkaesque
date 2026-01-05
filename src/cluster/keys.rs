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

/// Encode producer state value.
///
/// Format: `[last_sequence:4][producer_epoch:2]` (6 bytes total).
pub fn encode_producer_state_value(last_sequence: i32, producer_epoch: i16) -> [u8; 6] {
    let mut value = [0u8; 6];
    value[0..4].copy_from_slice(&last_sequence.to_be_bytes());
    value[4..6].copy_from_slice(&producer_epoch.to_be_bytes());
    value
}

/// Decode producer state from a value.
///
/// Returns (last_sequence, producer_epoch) if valid.
pub fn decode_producer_state_value(value: &[u8]) -> Option<(i32, i16)> {
    if value.len() >= 6 {
        let last_sequence = i32::from_be_bytes(value[0..4].try_into().ok()?);
        let producer_epoch = i16::from_be_bytes(value[4..6].try_into().ok()?);
        Some((last_sequence, producer_epoch))
    } else {
        None
    }
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

    #[test]
    fn test_producer_state_value_encoding() {
        let last_sequence = 42i32;
        let producer_epoch = 3i16;
        let value = encode_producer_state_value(last_sequence, producer_epoch);

        let (decoded_seq, decoded_epoch) = decode_producer_state_value(&value).unwrap();
        assert_eq!(decoded_seq, last_sequence);
        assert_eq!(decoded_epoch, producer_epoch);
    }

    #[test]
    fn test_producer_state_value_edge_cases() {
        // Test with max values
        let value = encode_producer_state_value(i32::MAX, i16::MAX);
        let (seq, epoch) = decode_producer_state_value(&value).unwrap();
        assert_eq!(seq, i32::MAX);
        assert_eq!(epoch, i16::MAX);

        // Test with negative values
        let value = encode_producer_state_value(-1, -1);
        let (seq, epoch) = decode_producer_state_value(&value).unwrap();
        assert_eq!(seq, -1);
        assert_eq!(epoch, -1);

        // Test with zero
        let value = encode_producer_state_value(0, 0);
        let (seq, epoch) = decode_producer_state_value(&value).unwrap();
        assert_eq!(seq, 0);
        assert_eq!(epoch, 0);
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
        let mut value = encode_producer_state_value(100, 5).to_vec();
        value.extend_from_slice(&[0xFF, 0xFF]); // Add extra bytes
        let (seq, epoch) = decode_producer_state_value(&value).unwrap();
        assert_eq!(seq, 100);
        assert_eq!(epoch, 5);
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
}
