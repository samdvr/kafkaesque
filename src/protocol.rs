//! Kafka protocol utilities for working with RecordBatch data.
//!
//! This module provides common utilities for parsing and manipulating
//! Kafka RecordBatch structures.
//!
//! # RecordBatch Header Layout
//!
//! The Kafka RecordBatch header (first 61 bytes) contains:
//! ```text
//! Offset  Size  Field
//! 0       8     base_offset
//! 8       4     batch_length
//! 12      4     partition_leader_epoch
//! 16      1     magic (2 for v2+)
//! 17      4     crc
//! 21      2     attributes
//! 23      4     last_offset_delta
//! 27      8     first_timestamp
//! 35      8     max_timestamp
//! 43      8     producer_id
//! 51      2     producer_epoch
//! 53      4     first_sequence
//! 57      4     records_count
//! ```

use crate::constants::{
    BATCH_BASE_OFFSET, BATCH_CRC_DATA_START, BATCH_CRC_OFFSET, BATCH_FIRST_SEQUENCE_END,
    BATCH_FIRST_SEQUENCE_OFFSET, BATCH_LAST_OFFSET_DELTA_END, BATCH_LAST_OFFSET_DELTA_OFFSET,
    BATCH_PRODUCER_EPOCH_END, BATCH_PRODUCER_EPOCH_OFFSET, BATCH_PRODUCER_ID_END,
    BATCH_PRODUCER_ID_OFFSET, MIN_BATCH_HEADER_SIZE,
};

// CRC-32C polynomial used by Kafka (Castagnoli)
// Using a simple implementation since we don't want to add dependencies
const CRC32C_TABLE: [u32; 256] = {
    let mut table = [0u32; 256];
    let mut i = 0;
    while i < 256 {
        let mut crc = i as u32;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0x82F63B78; // CRC-32C polynomial
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i] = crc;
        i += 1;
    }
    table
};

/// Compute CRC-32C checksum (Castagnoli polynomial).
///
/// Kafka uses CRC-32C for record batch integrity verification.
fn crc32c(data: &[u8]) -> u32 {
    let mut crc = !0u32;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
    }
    !crc
}

/// Result of CRC validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CrcValidationResult {
    /// CRC is valid.
    Valid,
    /// CRC is invalid.
    Invalid { expected: u32, actual: u32 },
    /// Batch is too small to contain CRC.
    TooSmall,
}

/// Validate CRC-32C checksum of a Kafka RecordBatch.
///
/// The CRC covers bytes from offset 21 (attributes) to the end of the batch.
/// The stored CRC is at bytes 17-20 (big-endian).
///
/// # Arguments
/// * `batch` - The raw bytes of a RecordBatch
///
/// # Returns
/// - `CrcValidationResult::Valid` if CRC matches
/// - `CrcValidationResult::Invalid` if CRC doesn't match (includes expected/actual values)
/// - `CrcValidationResult::TooSmall` if batch is too small to contain CRC
///
/// # Example
/// ```
/// use kafkaesque::protocol::{validate_batch_crc, CrcValidationResult};
///
/// let batch = vec![0u8; 10]; // Too small
/// assert_eq!(validate_batch_crc(&batch), CrcValidationResult::TooSmall);
/// ```
pub fn validate_batch_crc(batch: &[u8]) -> CrcValidationResult {
    // Need at least BATCH_CRC_DATA_START bytes to have the CRC field and start of data
    if batch.len() < BATCH_CRC_DATA_START {
        return CrcValidationResult::TooSmall;
    }

    // Extract stored CRC from bytes 17-20 (big-endian)
    let stored_crc = u32::from_be_bytes([
        batch[BATCH_CRC_OFFSET],
        batch[BATCH_CRC_OFFSET + 1],
        batch[BATCH_CRC_OFFSET + 2],
        batch[BATCH_CRC_OFFSET + 3],
    ]);

    // Compute CRC over bytes from BATCH_CRC_DATA_START to end (attributes through records)
    let computed_crc = crc32c(&batch[BATCH_CRC_DATA_START..]);

    if stored_crc == computed_crc {
        CrcValidationResult::Valid
    } else {
        CrcValidationResult::Invalid {
            expected: stored_crc,
            actual: computed_crc,
        }
    }
}

/// Parse record count from a Kafka RecordBatch.
///
/// The `last_offset_delta` field is at bytes 23-26 and indicates the highest
/// relative offset in the batch. Record count = last_offset_delta + 1.
///
/// # Arguments
/// * `batch` - The raw bytes of a RecordBatch
///
/// # Returns
/// The number of records in the batch, or 1 if the batch is too small to parse.
///
/// # Example
/// ```
/// use kafkaesque::protocol::parse_record_count;
///
/// // A minimal batch header (would need at least 27 bytes for real data)
/// let batch = vec![0u8; 27];
/// let count = parse_record_count(&batch);
/// assert_eq!(count, 1); // last_offset_delta=0 means 1 record
/// ```
pub fn parse_record_count(batch: &[u8]) -> i32 {
    if batch.len() >= BATCH_LAST_OFFSET_DELTA_END {
        let last_offset_delta = i32::from_be_bytes([
            batch[BATCH_LAST_OFFSET_DELTA_OFFSET],
            batch[BATCH_LAST_OFFSET_DELTA_OFFSET + 1],
            batch[BATCH_LAST_OFFSET_DELTA_OFFSET + 2],
            batch[BATCH_LAST_OFFSET_DELTA_OFFSET + 3],
        ]);
        last_offset_delta + 1
    } else {
        1 // Assume 1 record if we can't parse
    }
}

/// Patch the base offset in a RecordBatch header and recalculate CRC.
///
/// The first 8 bytes of a RecordBatch contain the base offset (big-endian i64).
/// Kafka producers send batches with base_offset=0, and the broker must patch
/// this to the actual offset where the batch will be stored.
///
/// **IMPORTANT**: After modifying the base offset, the CRC (bytes 17-20) is
/// recalculated to maintain batch integrity. Kafka clients validate CRC on
/// fetch and will reject batches with invalid CRCs.
///
/// # Arguments
/// * `batch` - Mutable slice of the RecordBatch bytes
/// * `base_offset` - The offset to write into the header
///
/// # Example
/// ```
/// use kafkaesque::protocol::{patch_base_offset, validate_batch_crc, CrcValidationResult};
///
/// let mut batch = vec![0u8; 100];
/// patch_base_offset(&mut batch, 12345);
/// assert_eq!(&batch[0..8], &12345i64.to_be_bytes());
/// // CRC is also updated if batch is large enough
/// ```
pub fn patch_base_offset(batch: &mut [u8], base_offset: i64) {
    const BASE_OFFSET_SIZE: usize = 8;
    if batch.len() >= BASE_OFFSET_SIZE {
        batch[BATCH_BASE_OFFSET..BATCH_BASE_OFFSET + BASE_OFFSET_SIZE]
            .copy_from_slice(&base_offset.to_be_bytes());
    }

    // Recalculate and update CRC after modifying base offset
    // CRC covers bytes from BATCH_CRC_DATA_START to end of batch
    // CRC is stored at BATCH_CRC_OFFSET (4 bytes, big-endian u32)
    if batch.len() >= BATCH_CRC_DATA_START {
        let new_crc = crc32c(&batch[BATCH_CRC_DATA_START..]);
        batch[BATCH_CRC_OFFSET..BATCH_CRC_OFFSET + 4].copy_from_slice(&new_crc.to_be_bytes());
    }
}

/// Producer information extracted from a RecordBatch header.
///
/// Used for idempotency checking to detect duplicate or out-of-order messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProducerBatchInfo {
    /// Producer ID (-1 if not set/idempotent mode disabled)
    pub producer_id: i64,
    /// Producer epoch for fencing
    pub producer_epoch: i16,
    /// First sequence number in this batch
    pub first_sequence: i32,
    /// Number of records in the batch
    pub record_count: i32,
}

impl ProducerBatchInfo {
    /// Check if this batch has idempotency enabled.
    ///
    /// A producer_id of -1 indicates idempotency is not enabled.
    pub fn is_idempotent(&self) -> bool {
        self.producer_id >= 0
    }

    /// Calculate the last sequence number in this batch.
    ///
    /// Returns None if sequence numbers would overflow.
    pub fn last_sequence(&self) -> Option<i32> {
        if self.first_sequence < 0 || self.record_count <= 0 {
            return None;
        }
        self.first_sequence.checked_add(self.record_count - 1)
    }
}

/// Parse producer information from a Kafka RecordBatch header.
///
/// Extracts producer_id, producer_epoch, and first_sequence for idempotency checking.
///
/// # Arguments
/// * `batch` - The raw bytes of a RecordBatch
///
/// # Returns
/// ProducerBatchInfo if the batch is large enough to parse, None otherwise.
///
/// # Layout (bytes 43-56):
/// - producer_id: i64 at bytes 43-50
/// - producer_epoch: i16 at bytes 51-52
/// - first_sequence: i32 at bytes 53-56
pub fn parse_producer_info(batch: &[u8]) -> Option<ProducerBatchInfo> {
    if batch.len() < MIN_BATCH_HEADER_SIZE {
        return None;
    }

    let producer_id = i64::from_be_bytes(
        batch[BATCH_PRODUCER_ID_OFFSET..BATCH_PRODUCER_ID_END]
            .try_into()
            .ok()?,
    );
    let producer_epoch = i16::from_be_bytes(
        batch[BATCH_PRODUCER_EPOCH_OFFSET..BATCH_PRODUCER_EPOCH_END]
            .try_into()
            .ok()?,
    );
    let first_sequence = i32::from_be_bytes(
        batch[BATCH_FIRST_SEQUENCE_OFFSET..BATCH_FIRST_SEQUENCE_END]
            .try_into()
            .ok()?,
    );
    let record_count = parse_record_count(batch);

    Some(ProducerBatchInfo {
        producer_id,
        producer_epoch,
        first_sequence,
        record_count,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    // Note: Tests use literal byte offsets (e.g., batch[23..27]) rather than constants
    // to verify that the constants are correctly defined. This provides independent
    // verification of the protocol layout.

    #[test]
    fn test_parse_record_count_valid() {
        // Create a batch with last_offset_delta = 4 (meaning 5 records)
        let mut batch = vec![0u8; 27];
        batch[23..27].copy_from_slice(&4i32.to_be_bytes());
        assert_eq!(parse_record_count(&batch), 5);
    }

    #[test]
    fn test_parse_record_count_too_small() {
        let batch = vec![0u8; 10];
        assert_eq!(parse_record_count(&batch), 1);
    }

    #[test]
    fn test_patch_base_offset() {
        let mut batch = vec![0u8; 100];
        patch_base_offset(&mut batch, 12345);
        let patched = i64::from_be_bytes(batch[0..8].try_into().unwrap());
        assert_eq!(patched, 12345);
    }

    #[test]
    fn test_patch_base_offset_too_small() {
        let mut batch = vec![0u8; 4];
        // Should not panic, just do nothing
        patch_base_offset(&mut batch, 12345);
        assert_eq!(&batch, &[0, 0, 0, 0]);
    }

    #[test]
    fn test_crc32c_known_values() {
        // Test against known CRC-32C values
        assert_eq!(crc32c(b""), 0x00000000);
        assert_eq!(crc32c(b"a"), 0xC1D04330);
        assert_eq!(crc32c(b"123456789"), 0xE3069283);
    }

    #[test]
    fn test_validate_batch_crc_too_small() {
        let batch = vec![0u8; 10];
        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::TooSmall);

        let batch = vec![0u8; 20];
        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::TooSmall);
    }

    #[test]
    fn test_validate_batch_crc_valid() {
        // Create a batch with correct CRC
        // Header: 21 bytes before data, CRC at 17-20
        let mut batch = vec![0u8; 61]; // MIN_BATCH_HEADER_SIZE

        // Set some data in the CRC-covered region (bytes 21+)
        batch[21] = 0x01; // attributes
        batch[22] = 0x02;

        // Compute CRC over bytes 21+
        let crc = crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());

        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
    }

    #[test]
    fn test_validate_batch_crc_invalid() {
        let mut batch = vec![0u8; 61];

        // Set some data
        batch[21] = 0x01;

        // Set wrong CRC
        batch[17..21].copy_from_slice(&[0xFF, 0xFF, 0xFF, 0xFF]);

        match validate_batch_crc(&batch) {
            CrcValidationResult::Invalid { expected, actual } => {
                assert_eq!(expected, 0xFFFFFFFF);
                assert_ne!(actual, expected);
            }
            other => panic!("Expected Invalid, got {:?}", other),
        }
    }

    #[test]
    fn test_patch_base_offset_updates_crc() {
        // Create a valid batch with correct CRC
        let mut batch = vec![0u8; 61];

        // Set initial base offset
        batch[0..8].copy_from_slice(&0i64.to_be_bytes());

        // Set some data in the CRC-covered region (bytes 21+)
        batch[21] = 0x01;
        batch[22] = 0x02;
        batch[30] = 0xAB;

        // Compute and set initial CRC
        let initial_crc = crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&initial_crc.to_be_bytes());

        // Verify CRC is valid before patching
        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);

        // Patch base offset
        patch_base_offset(&mut batch, 99999);

        // Verify base offset was patched
        let patched_offset = i64::from_be_bytes(batch[0..8].try_into().unwrap());
        assert_eq!(patched_offset, 99999);

        // Verify CRC is still valid after patching
        // Note: Base offset is NOT covered by CRC (only bytes 21+ are covered),
        // so CRC should remain valid. But we recalculate it anyway for safety.
        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
    }

    #[test]
    fn test_parse_producer_info_valid() {
        // Create a batch with producer info
        // MIN_BATCH_HEADER_SIZE is 61 bytes
        let mut batch = vec![0u8; 61];

        // producer_id at bytes 43-50 (i64)
        let producer_id: i64 = 12345;
        batch[43..51].copy_from_slice(&producer_id.to_be_bytes());

        // producer_epoch at bytes 51-52 (i16)
        let producer_epoch: i16 = 5;
        batch[51..53].copy_from_slice(&producer_epoch.to_be_bytes());

        // first_sequence at bytes 53-56 (i32)
        let first_sequence: i32 = 100;
        batch[53..57].copy_from_slice(&first_sequence.to_be_bytes());

        // last_offset_delta at bytes 23-26 (means record_count = last_offset_delta + 1)
        let record_count = 10;
        batch[23..27].copy_from_slice(&(record_count - 1i32).to_be_bytes());

        let info = parse_producer_info(&batch).unwrap();
        assert_eq!(info.producer_id, 12345);
        assert_eq!(info.producer_epoch, 5);
        assert_eq!(info.first_sequence, 100);
        assert_eq!(info.record_count, 10);
    }

    #[test]
    fn test_parse_producer_info_too_small() {
        let batch = vec![0u8; 50]; // Too small
        assert!(parse_producer_info(&batch).is_none());
    }

    #[test]
    fn test_producer_batch_info_is_idempotent() {
        let info = ProducerBatchInfo {
            producer_id: 12345,
            producer_epoch: 5,
            first_sequence: 0,
            record_count: 10,
        };
        assert!(info.is_idempotent());

        let non_idempotent = ProducerBatchInfo {
            producer_id: -1, // -1 means idempotency disabled
            producer_epoch: 0,
            first_sequence: 0,
            record_count: 10,
        };
        assert!(!non_idempotent.is_idempotent());
    }

    #[test]
    fn test_producer_batch_info_last_sequence() {
        let info = ProducerBatchInfo {
            producer_id: 1,
            producer_epoch: 0,
            first_sequence: 100,
            record_count: 10,
        };
        // last_sequence = first_sequence + record_count - 1 = 100 + 10 - 1 = 109
        assert_eq!(info.last_sequence(), Some(109));

        // Edge case: single record
        let single = ProducerBatchInfo {
            producer_id: 1,
            producer_epoch: 0,
            first_sequence: 0,
            record_count: 1,
        };
        assert_eq!(single.last_sequence(), Some(0));

        // Invalid: negative first_sequence
        let invalid = ProducerBatchInfo {
            producer_id: 1,
            producer_epoch: 0,
            first_sequence: -1,
            record_count: 10,
        };
        assert_eq!(invalid.last_sequence(), None);

        // Invalid: zero record_count
        let zero_count = ProducerBatchInfo {
            producer_id: 1,
            producer_epoch: 0,
            first_sequence: 0,
            record_count: 0,
        };
        assert_eq!(zero_count.last_sequence(), None);
    }

    // ========================================================================
    // CRC Validation Edge Case Tests
    // ========================================================================

    #[test]
    fn test_crc32c_empty_data() {
        assert_eq!(crc32c(b""), 0x00000000);
    }

    #[test]
    fn test_crc32c_single_byte() {
        // Known values for single bytes
        assert_eq!(crc32c(b"a"), 0xC1D04330);
        assert_eq!(crc32c(b"\x00"), 0x527D5351);
        assert_eq!(crc32c(b"\xFF"), 0xFF000000);
    }

    #[test]
    fn test_crc32c_standard_test_vector() {
        // IETF RFC 3720 test vector
        assert_eq!(crc32c(b"123456789"), 0xE3069283);
    }

    #[test]
    fn test_crc32c_all_zeros() {
        let zeros = vec![0u8; 100];
        let crc = crc32c(&zeros);
        // CRC should be deterministic
        assert_eq!(crc, crc32c(&zeros));
    }

    #[test]
    fn test_crc32c_all_ones() {
        let ones = vec![0xFFu8; 100];
        let crc = crc32c(&ones);
        assert_eq!(crc, crc32c(&ones));
    }

    #[test]
    fn test_validate_batch_crc_exact_21_bytes() {
        // Exactly 21 bytes - minimum size that can have CRC
        let mut batch = vec![0u8; 21];
        // CRC covers bytes 21+ which is empty in this case
        let crc = crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
    }

    #[test]
    fn test_validate_batch_crc_with_real_data() {
        // Create a realistic batch with various data
        let mut batch = vec![0u8; 100];

        // Fill with some realistic values
        batch[21] = 0x00; // attributes low byte
        batch[22] = 0x00; // attributes high byte
        batch[23..27].copy_from_slice(&5i32.to_be_bytes()); // last_offset_delta
        batch[43..51].copy_from_slice(&12345i64.to_be_bytes()); // producer_id
        batch[51..53].copy_from_slice(&1i16.to_be_bytes()); // producer_epoch
        batch[53..57].copy_from_slice(&0i32.to_be_bytes()); // first_sequence

        // Compute correct CRC
        let crc = crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());

        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
    }

    #[test]
    fn test_validate_batch_crc_single_bit_flip() {
        // Create valid batch
        let mut batch = vec![0u8; 61];
        batch[30] = 0xAB;
        let crc = crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());

        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);

        // Flip a single bit in the data
        batch[30] ^= 0x01;

        match validate_batch_crc(&batch) {
            CrcValidationResult::Invalid { expected, actual } => {
                assert_ne!(expected, actual);
            }
            other => panic!("Expected Invalid after bit flip, got {:?}", other),
        }
    }

    #[test]
    fn test_patch_base_offset_preserves_crc_validity() {
        // Create a valid batch
        let mut batch = vec![0u8; 100];
        batch[0..8].copy_from_slice(&0i64.to_be_bytes()); // initial base offset
        batch[21] = 0x01;
        batch[30] = 0xAB;
        batch[50] = 0xCD;

        // Set valid CRC
        let initial_crc = crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&initial_crc.to_be_bytes());
        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);

        // Patch to various offsets
        for offset in [0i64, 1, 100, 999999, i64::MAX] {
            patch_base_offset(&mut batch, offset);

            // Verify offset was patched
            let patched = i64::from_be_bytes(batch[0..8].try_into().unwrap());
            assert_eq!(patched, offset);

            // CRC should still be valid (base offset not in CRC range)
            assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
        }
    }

    #[test]
    fn test_parse_producer_info_boundary_values() {
        let mut batch = vec![0u8; 61];

        // Test with max values
        batch[43..51].copy_from_slice(&i64::MAX.to_be_bytes());
        batch[51..53].copy_from_slice(&i16::MAX.to_be_bytes());
        batch[53..57].copy_from_slice(&i32::MAX.to_be_bytes());
        batch[23..27].copy_from_slice(&0i32.to_be_bytes()); // 1 record

        let info = parse_producer_info(&batch).unwrap();
        assert_eq!(info.producer_id, i64::MAX);
        assert_eq!(info.producer_epoch, i16::MAX);
        assert_eq!(info.first_sequence, i32::MAX);
        assert_eq!(info.record_count, 1);
    }

    #[test]
    fn test_parse_producer_info_negative_producer_id() {
        let mut batch = vec![0u8; 61];

        // producer_id = -1 means idempotency disabled
        batch[43..51].copy_from_slice(&(-1i64).to_be_bytes());
        batch[51..53].copy_from_slice(&0i16.to_be_bytes());
        batch[53..57].copy_from_slice(&0i32.to_be_bytes());
        batch[23..27].copy_from_slice(&0i32.to_be_bytes());

        let info = parse_producer_info(&batch).unwrap();
        assert_eq!(info.producer_id, -1);
        assert!(!info.is_idempotent());
    }

    #[test]
    fn test_parse_record_count_edge_cases() {
        // Exactly 27 bytes (minimum for record count)
        let mut batch = vec![0u8; 27];
        batch[23..27].copy_from_slice(&99i32.to_be_bytes());
        assert_eq!(parse_record_count(&batch), 100); // last_offset_delta + 1

        // Large batch
        let mut large_batch = vec![0u8; 1000];
        large_batch[23..27].copy_from_slice(&999i32.to_be_bytes());
        assert_eq!(parse_record_count(&large_batch), 1000);

        // Zero last_offset_delta means 1 record
        let mut single = vec![0u8; 27];
        single[23..27].copy_from_slice(&0i32.to_be_bytes());
        assert_eq!(parse_record_count(&single), 1);
    }

    #[test]
    fn test_producer_batch_info_last_sequence_overflow() {
        // Test sequence number overflow handling
        let info = ProducerBatchInfo {
            producer_id: 1,
            producer_epoch: 0,
            first_sequence: i32::MAX - 5,
            record_count: 10, // Would overflow
        };
        // Should return None on overflow
        assert_eq!(info.last_sequence(), None);

        // Just at the boundary - should succeed
        let boundary = ProducerBatchInfo {
            producer_id: 1,
            producer_epoch: 0,
            first_sequence: i32::MAX - 9,
            record_count: 10,
        };
        assert_eq!(boundary.last_sequence(), Some(i32::MAX));
    }

    #[test]
    fn test_crc_validation_result_equality() {
        assert_eq!(CrcValidationResult::Valid, CrcValidationResult::Valid);
        assert_eq!(CrcValidationResult::TooSmall, CrcValidationResult::TooSmall);
        assert_eq!(
            CrcValidationResult::Invalid {
                expected: 123,
                actual: 456
            },
            CrcValidationResult::Invalid {
                expected: 123,
                actual: 456
            }
        );
        assert_ne!(
            CrcValidationResult::Invalid {
                expected: 123,
                actual: 456
            },
            CrcValidationResult::Invalid {
                expected: 123,
                actual: 789
            }
        );
    }

    #[test]
    fn test_crc_validation_result_debug() {
        let valid = CrcValidationResult::Valid;
        assert!(format!("{:?}", valid).contains("Valid"));

        let invalid = CrcValidationResult::Invalid {
            expected: 0xABCD,
            actual: 0x1234,
        };
        let debug = format!("{:?}", invalid);
        assert!(debug.contains("Invalid"));
        assert!(debug.contains("expected"));
        assert!(debug.contains("actual"));
    }
}
