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
    BATCH_FIRST_SEQUENCE_OFFSET, BATCH_LAST_OFFSET_DELTA_OFFSET, BATCH_LENGTH_END,
    BATCH_LENGTH_OFFSET, BATCH_LENGTH_PREFIX, BATCH_MAGIC_OFFSET, BATCH_MAGIC_V2,
    BATCH_PRODUCER_EPOCH_END, BATCH_PRODUCER_EPOCH_OFFSET, BATCH_PRODUCER_ID_END,
    BATCH_PRODUCER_ID_OFFSET, MIN_BATCH_HEADER_SIZE,
};

/// Byte offset of the explicit `records_count` (INT32) field in a v2
/// RecordBatch header. See the module-level layout table.
const BATCH_RECORDS_COUNT_OFFSET: usize = 57;
/// End (exclusive) of the `records_count` field; also the minimum size of a
/// well-formed v2 RecordBatch header.
const BATCH_RECORDS_COUNT_END: usize = 61;

/// Compute CRC-32C checksum (Castagnoli polynomial).
///
/// Kafka uses CRC-32C for record batch integrity verification.
///
/// Backed by the `crc32c` crate, which uses hardware CRC instructions
/// (SSE 4.2 / ARMv8 CRC) when available and a fast slicing-by-8 software
/// fallback otherwise. The byte-at-a-time table implementation this
/// replaced is kept in the test module as a reference oracle.
///
/// Public so integration tests and embedders can compute the checksum a
/// batch is expected to carry at bytes 17–20 (covering bytes 21+).
pub fn crc32c(data: &[u8]) -> u32 {
    ::crc32c::crc32c(data)
}

/// Result of CRC validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CrcValidationResult {
    /// CRC is valid.
    Valid,
    /// CRC is invalid.
    Invalid { expected: u32, actual: u32 },
    /// Batch is too small to contain CRC.
    TooSmall,
    /// The header's `batch_length` field claims a region that extends past
    /// the slice that was passed in (or is non-positive). This is a framing
    /// error, distinct from a CRC mismatch — surfacing it separately keeps
    /// "the slice you handed me does not contain a complete batch" from
    /// being mis-diagnosed as data corruption.
    FrameMismatch {
        /// Total bytes the header claims the batch contains.
        claimed_size: usize,
        /// Bytes actually available in the supplied slice.
        actual_size: usize,
    },
    /// The async wrapper offloaded validation to `spawn_blocking` and the
    /// task failed to complete (panic or blocking-pool saturation).
    /// Surfaced as a distinct variant so the caller can reject the request
    /// rather than silently falling back to inline CRC on the very runtime
    /// worker the offload was meant to spare.
    OffloadFailed,
    /// The batch declares a non-v2 magic byte. The v2 RecordBatch layout
    /// (records-count at offset 57-60, CRC over bytes 21+) is the only
    /// layout the parser knows; v0/v1 MessageSets fed through this validator
    /// would be checksummed against the wrong region and pass or fail at
    /// random. Rejecting up front turns silent acceptance into a typed
    /// `CorruptMessage` at the caller.
    UnsupportedMagic { magic: u8 },
}

/// Validate CRC-32C checksum of a Kafka RecordBatch.
///
/// The CRC covers bytes from offset 21 (attributes) to the end of the batch.
/// The bound of "end of the batch" is read from the header's `batch_length`
/// field (bytes 8-11): the total batch size is `12 + batch_length`. CRCing
/// the entire input slice instead would silently fail when callers pass a
/// buffer that contains the batch plus trailing bytes (e.g. a Produce body
/// with concatenated batches).
///
/// # Arguments
/// * `batch` - The raw bytes of a RecordBatch
///
/// # Returns
/// - `CrcValidationResult::Valid` if CRC matches
/// - `CrcValidationResult::Invalid` if CRC doesn't match (includes expected/actual values)
/// - `CrcValidationResult::TooSmall` if batch is too small to contain CRC
/// - `CrcValidationResult::FrameMismatch` if `batch_length` is bogus or the
///   slice is shorter than the claimed region
///
/// # Example
/// ```
/// use kafkaesque::protocol::{validate_batch_crc, CrcValidationResult};
///
/// let batch = vec![0u8; 10]; // Too small
/// assert_eq!(validate_batch_crc(&batch), CrcValidationResult::TooSmall);
/// ```
pub fn validate_batch_crc(batch: &[u8]) -> CrcValidationResult {
    // Need at least BATCH_LENGTH_END bytes to read batch_length, and at least
    // BATCH_CRC_DATA_START bytes for the CRC field plus first covered byte.
    if batch.len() < BATCH_CRC_DATA_START {
        return CrcValidationResult::TooSmall;
    }

    // Reject anything that isn't v2 BEFORE checksumming. The CRC region for
    // v0/v1 starts at byte 12, not 21, so feeding a legacy MessageSet through
    // here would produce a meaningless verdict.
    let magic = batch[BATCH_MAGIC_OFFSET];
    if magic != BATCH_MAGIC_V2 {
        return CrcValidationResult::UnsupportedMagic { magic };
    }

    let batch_length_raw = i32::from_be_bytes([
        batch[BATCH_LENGTH_OFFSET],
        batch[BATCH_LENGTH_OFFSET + 1],
        batch[BATCH_LENGTH_OFFSET + 2],
        batch[BATCH_LENGTH_END - 1],
    ]);
    if batch_length_raw <= 0 {
        return CrcValidationResult::FrameMismatch {
            claimed_size: BATCH_LENGTH_PREFIX,
            actual_size: batch.len(),
        };
    }
    let claimed_size = BATCH_LENGTH_PREFIX.saturating_add(batch_length_raw as usize);
    if claimed_size < BATCH_CRC_DATA_START || claimed_size > batch.len() {
        return CrcValidationResult::FrameMismatch {
            claimed_size,
            actual_size: batch.len(),
        };
    }

    // Extract stored CRC from bytes 17-20 (big-endian)
    let stored_crc = u32::from_be_bytes([
        batch[BATCH_CRC_OFFSET],
        batch[BATCH_CRC_OFFSET + 1],
        batch[BATCH_CRC_OFFSET + 2],
        batch[BATCH_CRC_OFFSET + 3],
    ]);

    // Compute CRC over the bounded region [BATCH_CRC_DATA_START..claimed_size]
    let computed_crc = crc32c(&batch[BATCH_CRC_DATA_START..claimed_size]);

    if stored_crc == computed_crc {
        CrcValidationResult::Valid
    } else {
        CrcValidationResult::Invalid {
            expected: stored_crc,
            actual: computed_crc,
        }
    }
}

/// Threshold above which CRC validation is moved to a blocking thread.
///
/// For small batches the spawn_blocking + channel overhead exceeds the cost
/// of computing CRC32C inline. 64 KiB is the empirical break-even — above
/// it the synchronous path measurably stalls the tokio worker on concurrent
/// large produces.
///
/// The async wrapper that uses this threshold lives in the umbrella
/// `kafkaesque` crate (`crate::server::validate_batch_crc_async`) so this
/// crate stays runtime-independent.
pub const CRC_OFFLOAD_THRESHOLD: usize = 64 * 1024;

/// Why a RecordBatch's record count could not be determined.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordCountError {
    /// The batch is shorter than the 61-byte v2 RecordBatch header, so the
    /// explicit `records_count` field (bytes 57-60) is not present.
    TooShort {
        /// Actual batch length in bytes.
        len: usize,
    },
    /// The explicit `records_count` field disagrees with
    /// `last_offset_delta + 1`. A well-formed v2 batch always satisfies
    /// `records_count == last_offset_delta + 1`; a mismatch indicates a
    /// corrupt or forged header.
    Mismatch {
        /// Value of the explicit `records_count` field (bytes 57-60).
        records_count: i32,
        /// Value of the `last_offset_delta` field (bytes 23-26).
        last_offset_delta: i32,
    },
    /// The explicit `records_count` field is zero or negative; a produced
    /// batch must contain at least one record.
    NonPositive {
        /// Value of the explicit `records_count` field.
        records_count: i32,
    },
}

impl std::fmt::Display for RecordCountError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordCountError::TooShort { len } => {
                write!(f, "batch too short for v2 header: {} < 61 bytes", len)
            }
            RecordCountError::Mismatch {
                records_count,
                last_offset_delta,
            } => write!(
                f,
                "records_count {} != last_offset_delta {} + 1",
                records_count, last_offset_delta
            ),
            RecordCountError::NonPositive { records_count } => {
                write!(f, "records_count {} is not >= 1", records_count)
            }
        }
    }
}

impl std::error::Error for RecordCountError {}

/// Parse and validate the record count of a Kafka v2 RecordBatch.
///
/// The v2 RecordBatch header carries an explicit `records_count` INT32 at
/// bytes 57-60. This reads that field directly (instead of deriving the
/// count from `last_offset_delta`, which a malformed batch can set
/// independently) and cross-checks it:
///
/// - the batch must be at least 61 bytes (a full v2 header),
/// - `records_count` must be >= 1,
/// - `records_count` must equal `last_offset_delta + 1`.
///
/// Returns the validated count, or a [`RecordCountError`] describing the
/// inconsistency. Callers on the produce path map errors to a corrupt-batch
/// rejection (`KafkaCode::CorruptMessage`-class produce error) rather than
/// accepting an attacker-controlled count.
pub fn parse_record_count_checked(batch: &[u8]) -> std::result::Result<i32, RecordCountError> {
    if batch.len() < BATCH_RECORDS_COUNT_END {
        return Err(RecordCountError::TooShort { len: batch.len() });
    }

    let records_count = i32::from_be_bytes([
        batch[BATCH_RECORDS_COUNT_OFFSET],
        batch[BATCH_RECORDS_COUNT_OFFSET + 1],
        batch[BATCH_RECORDS_COUNT_OFFSET + 2],
        batch[BATCH_RECORDS_COUNT_OFFSET + 3],
    ]);
    let last_offset_delta = i32::from_be_bytes([
        batch[BATCH_LAST_OFFSET_DELTA_OFFSET],
        batch[BATCH_LAST_OFFSET_DELTA_OFFSET + 1],
        batch[BATCH_LAST_OFFSET_DELTA_OFFSET + 2],
        batch[BATCH_LAST_OFFSET_DELTA_OFFSET + 3],
    ]);

    if records_count < 1 {
        return Err(RecordCountError::NonPositive { records_count });
    }
    // checked_add: last_offset_delta == i32::MAX would otherwise overflow.
    if last_offset_delta.checked_add(1) != Some(records_count) {
        return Err(RecordCountError::Mismatch {
            records_count,
            last_offset_delta,
        });
    }

    Ok(records_count)
}

/// Parse record count from a Kafka RecordBatch.
///
/// Reads the explicit `records_count` INT32 at bytes 57-60 of the v2
/// RecordBatch header and validates it via [`parse_record_count_checked`].
///
/// # Arguments
/// * `batch` - The raw bytes of a RecordBatch
///
/// # Returns
/// The validated number of records (always >= 1), or `0` if the batch is
/// malformed: shorter than the 61-byte v2 header, `records_count < 1`, or
/// `records_count != last_offset_delta + 1`.
///
/// # Deprecated
/// Collapses three distinct framing errors into the same `0`. Prefer
/// [`parse_record_count_checked`] so callers can act on the typed
/// [`RecordCountError`] (log, return a corrupt-batch error, etc.).
#[deprecated(note = "use parse_record_count_checked and handle RecordCountError explicitly")]
pub fn parse_record_count(batch: &[u8]) -> i32 {
    parse_record_count_checked(batch).unwrap_or(0)
}

/// Patch the base offset in a RecordBatch header.
///
/// The first 8 bytes of a RecordBatch contain the base offset (big-endian i64).
/// Kafka producers send batches with base_offset=0, and the broker must patch
/// this to the actual offset where the batch will be stored.
///
/// The CRC is intentionally **not** touched: the Kafka v2 batch CRC (stored
/// at bytes 17-20) covers bytes 21 to the end of the batch only — the base
/// offset (bytes 0-7), batch length, partition leader epoch, magic, and the
/// CRC field itself are all outside the checksummed range. Rewriting the
/// base offset therefore cannot invalidate a batch's CRC, and recomputing
/// it here would burn a full pass over the batch on every produce append
/// for no effect (the produce path validates the CRC *before* this point,
/// so the stored value is already correct).
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
/// ```
pub fn patch_base_offset(batch: &mut [u8], base_offset: i64) {
    const BASE_OFFSET_SIZE: usize = 8;
    if batch.len() >= BASE_OFFSET_SIZE {
        batch[BATCH_BASE_OFFSET..BATCH_BASE_OFFSET + BASE_OFFSET_SIZE]
            .copy_from_slice(&base_offset.to_be_bytes());
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
/// `record_count` is the validated count from [`parse_record_count_checked`];
/// it is `0` when the batch header is internally inconsistent, in which case
/// [`ProducerBatchInfo::last_sequence`] returns `None`.
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
    let record_count = parse_record_count_checked(batch).unwrap_or(0);

    Some(ProducerBatchInfo {
        producer_id,
        producer_epoch,
        first_sequence,
        record_count,
    })
}

#[cfg(test)]
#[allow(deprecated)]
mod tests {
    use super::*;
    // Note: Tests use literal byte offsets (e.g., batch[23..27]) rather than constants
    // to verify that the constants are correctly defined. This provides independent
    // verification of the protocol layout.

    /// Reference byte-at-a-time CRC-32C implementation (the one the
    /// production path used before switching to the hardware-accelerated
    /// `crc32c` crate). Kept as an oracle: `crc32c_matches_reference_table`
    /// asserts the new implementation produces identical values.
    const REFERENCE_CRC32C_TABLE: [u32; 256] = {
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

    fn reference_crc32c(data: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &byte in data {
            let index = ((crc ^ byte as u32) & 0xFF) as usize;
            crc = (crc >> 8) ^ REFERENCE_CRC32C_TABLE[index];
        }
        !crc
    }

    /// Build a minimal, internally consistent 61-byte v2 batch header with
    /// the given record count (last_offset_delta = count - 1).
    fn consistent_batch(record_count: i32) -> Vec<u8> {
        let mut batch = vec![0u8; 61];
        batch[16] = 2; // magic
        batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());
        batch[57..61].copy_from_slice(&record_count.to_be_bytes());
        batch
    }

    #[test]
    fn crc32c_matches_reference_table() {
        let inputs: [&[u8]; 6] = [
            b"",
            b"a",
            b"123456789",
            b"\x00\x00\x00\x00",
            b"The quick brown fox jumps over the lazy dog",
            &[0xFFu8; 300],
        ];
        for input in inputs {
            assert_eq!(
                crc32c(input),
                reference_crc32c(input),
                "crate-backed CRC-32C diverges from reference table for {:?}",
                input
            );
        }

        // A pseudo-random buffer larger than any internal block size.
        let mut data = vec![0u8; 8192];
        let mut state = 0x12345678u32;
        for byte in data.iter_mut() {
            state = state.wrapping_mul(1664525).wrapping_add(1013904223);
            *byte = (state >> 24) as u8;
        }
        assert_eq!(crc32c(&data), reference_crc32c(&data));
    }

    #[test]
    fn test_parse_record_count_valid() {
        // A consistent batch: last_offset_delta = 4, records_count = 5.
        let batch = consistent_batch(5);
        assert_eq!(parse_record_count(&batch), 5);
        assert_eq!(parse_record_count_checked(&batch), Ok(5));
    }

    #[test]
    fn test_parse_record_count_too_small() {
        // Shorter than the 61-byte v2 header: malformed, not "1 record".
        let batch = vec![0u8; 10];
        assert_eq!(parse_record_count(&batch), 0);
        assert_eq!(
            parse_record_count_checked(&batch),
            Err(RecordCountError::TooShort { len: 10 })
        );

        // 27 bytes is enough for last_offset_delta but NOT for the explicit
        // records_count field — still malformed.
        let mut batch = vec![0u8; 27];
        batch[23..27].copy_from_slice(&4i32.to_be_bytes());
        assert_eq!(parse_record_count(&batch), 0);
        assert_eq!(
            parse_record_count_checked(&batch),
            Err(RecordCountError::TooShort { len: 27 })
        );
    }

    #[test]
    fn test_parse_record_count_mismatch_rejected() {
        // records_count says 100 but last_offset_delta says 5 records.
        let mut batch = consistent_batch(5);
        batch[57..61].copy_from_slice(&100i32.to_be_bytes());
        assert_eq!(parse_record_count(&batch), 0);
        assert_eq!(
            parse_record_count_checked(&batch),
            Err(RecordCountError::Mismatch {
                records_count: 100,
                last_offset_delta: 4
            })
        );
    }

    #[test]
    fn test_parse_record_count_non_positive_rejected() {
        // records_count = 0 (and consistent delta = -1) is still rejected:
        // a produced batch must contain at least one record.
        let mut batch = vec![0u8; 61];
        batch[23..27].copy_from_slice(&(-1i32).to_be_bytes());
        batch[57..61].copy_from_slice(&0i32.to_be_bytes());
        assert_eq!(parse_record_count(&batch), 0);
        assert_eq!(
            parse_record_count_checked(&batch),
            Err(RecordCountError::NonPositive { records_count: 0 })
        );

        // Negative counts likewise.
        batch[57..61].copy_from_slice(&(-7i32).to_be_bytes());
        assert_eq!(
            parse_record_count_checked(&batch),
            Err(RecordCountError::NonPositive { records_count: -7 })
        );
    }

    #[test]
    fn test_parse_record_count_delta_overflow_rejected() {
        // last_offset_delta = i32::MAX would overflow `+ 1`; must be a
        // clean mismatch error, not a panic.
        let mut batch = vec![0u8; 61];
        batch[23..27].copy_from_slice(&i32::MAX.to_be_bytes());
        batch[57..61].copy_from_slice(&1i32.to_be_bytes());
        assert_eq!(
            parse_record_count_checked(&batch),
            Err(RecordCountError::Mismatch {
                records_count: 1,
                last_offset_delta: i32::MAX
            })
        );
        assert_eq!(parse_record_count(&batch), 0);
    }

    #[test]
    fn test_record_count_error_display() {
        assert!(
            RecordCountError::TooShort { len: 5 }
                .to_string()
                .contains("5 < 61")
        );
        assert!(
            RecordCountError::Mismatch {
                records_count: 2,
                last_offset_delta: 7
            }
            .to_string()
            .contains("2 != last_offset_delta 7"),
        );
        assert!(
            RecordCountError::NonPositive { records_count: -1 }
                .to_string()
                .contains("-1")
        );
    }

    /// Stamp `batch_length` (bytes 8..12) so the buffer parses as a complete
    /// v2 RecordBatch. `batch_length` counts bytes from byte 12 to the end,
    /// so the right value for a flat fixture buffer is `batch.len() - 12`.
    /// Also stamps the magic byte to v2 (the only layout the validator
    /// accepts) so callers don't have to remember to set it themselves.
    fn set_batch_length_to_buffer_size(batch: &mut [u8]) {
        let len = (batch.len() as i32).saturating_sub(BATCH_LENGTH_PREFIX as i32);
        batch[BATCH_LENGTH_OFFSET..BATCH_LENGTH_END].copy_from_slice(&len.to_be_bytes());
        if batch.len() > BATCH_MAGIC_OFFSET {
            batch[BATCH_MAGIC_OFFSET] = BATCH_MAGIC_V2;
        }
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
        set_batch_length_to_buffer_size(&mut batch);

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
        set_batch_length_to_buffer_size(&mut batch);

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
    fn test_patch_base_offset_keeps_crc_valid() {
        // Create a valid batch with correct CRC
        let mut batch = vec![0u8; 61];
        set_batch_length_to_buffer_size(&mut batch);

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

        // The base offset (bytes 0-7) is NOT covered by the v2 batch CRC
        // (which covers bytes 21+ only), so the stored CRC must remain
        // valid — and the stored CRC bytes must be untouched.
        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
        assert_eq!(&batch[17..21], &initial_crc.to_be_bytes());
    }

    #[test]
    fn test_patch_base_offset_does_not_rewrite_crc() {
        // patch_base_offset must NOT recompute/repair the CRC: a batch
        // arriving with a bogus CRC stays bogus after the offset is
        // patched. (The produce path validates CRC before patching, so
        // silently "blessing" corrupt batches here would mask corruption.)
        let mut batch = vec![0u8; 61];
        set_batch_length_to_buffer_size(&mut batch);
        batch[30] = 0xAB;
        batch[17..21].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        assert!(matches!(
            validate_batch_crc(&batch),
            CrcValidationResult::Invalid { .. }
        ));

        patch_base_offset(&mut batch, 42);

        // Offset patched, CRC bytes untouched and still invalid.
        assert_eq!(i64::from_be_bytes(batch[0..8].try_into().unwrap()), 42);
        assert_eq!(&batch[17..21], &[0xDE, 0xAD, 0xBE, 0xEF]);
        assert!(matches!(
            validate_batch_crc(&batch),
            CrcValidationResult::Invalid { .. }
        ));
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

        // last_offset_delta at bytes 23-26 and the explicit records_count at
        // bytes 57-60 must agree (record_count = last_offset_delta + 1)
        let record_count = 10;
        batch[23..27].copy_from_slice(&(record_count - 1i32).to_be_bytes());
        batch[57..61].copy_from_slice(&record_count.to_be_bytes());

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
        set_batch_length_to_buffer_size(&mut batch);
        // CRC covers bytes 21+ which is empty in this case
        let crc = crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
        assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
    }

    #[test]
    fn test_validate_batch_crc_with_real_data() {
        // Create a realistic batch with various data
        let mut batch = vec![0u8; 100];
        set_batch_length_to_buffer_size(&mut batch);

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
        set_batch_length_to_buffer_size(&mut batch);
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
    fn test_validate_batch_crc_respects_batch_length_with_trailing_bytes() {
        // A buffer that contains a complete batch followed by unrelated
        // trailing bytes must validate using the bytes the header says
        // belong to the batch — not the entire slice.
        let mut batch = vec![0u8; 61];
        set_batch_length_to_buffer_size(&mut batch);
        batch[30] = 0xAB;
        let crc = crc32c(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());

        // Append trailing junk that isn't part of this batch.
        let mut with_trailer = batch.clone();
        with_trailer.extend_from_slice(&[0xCA, 0xFE, 0xBA, 0xBE, 0xDE, 0xAD]);

        // The original-batch's length is preserved at bytes 8..12; the
        // trailing bytes are outside the CRC region and must not affect
        // validation.
        assert_eq!(
            validate_batch_crc(&with_trailer),
            CrcValidationResult::Valid,
            "Trailing bytes after the batch must not break CRC validation"
        );
    }

    #[test]
    fn test_validate_batch_crc_frame_mismatch_when_too_short() {
        // batch_length claims more bytes than the slice provides — must be
        // FrameMismatch, not Invalid (a corrupt-frame report distinct from
        // a CRC report).
        let mut batch = vec![0u8; 61];
        batch[BATCH_MAGIC_OFFSET] = BATCH_MAGIC_V2;
        // Claim 200 bytes after byte 12 (total 212), but only 61 are present.
        let oversized: i32 = 200;
        batch[8..12].copy_from_slice(&oversized.to_be_bytes());

        match validate_batch_crc(&batch) {
            CrcValidationResult::FrameMismatch {
                claimed_size,
                actual_size,
            } => {
                assert_eq!(claimed_size, 212);
                assert_eq!(actual_size, 61);
            }
            other => panic!("Expected FrameMismatch, got {:?}", other),
        }
    }

    #[test]
    fn test_validate_batch_crc_frame_mismatch_when_non_positive() {
        // batch_length <= 0 is a malformed header; reject as FrameMismatch.
        let mut batch = vec![0u8; 61];
        batch[BATCH_MAGIC_OFFSET] = BATCH_MAGIC_V2;
        batch[8..12].copy_from_slice(&(-5i32).to_be_bytes());
        assert!(matches!(
            validate_batch_crc(&batch),
            CrcValidationResult::FrameMismatch { .. }
        ));

        let mut zero = vec![0u8; 61];
        zero[BATCH_MAGIC_OFFSET] = BATCH_MAGIC_V2;
        zero[8..12].copy_from_slice(&0i32.to_be_bytes());
        assert!(matches!(
            validate_batch_crc(&zero),
            CrcValidationResult::FrameMismatch { .. }
        ));
    }

    #[test]
    fn test_patch_base_offset_preserves_crc_validity() {
        // Create a valid batch
        let mut batch = vec![0u8; 100];
        batch[0..8].copy_from_slice(&0i64.to_be_bytes()); // initial base offset
        set_batch_length_to_buffer_size(&mut batch);
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
        batch[57..61].copy_from_slice(&1i32.to_be_bytes()); // records_count = 1

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
        batch[57..61].copy_from_slice(&1i32.to_be_bytes());

        let info = parse_producer_info(&batch).unwrap();
        assert_eq!(info.producer_id, -1);
        assert!(!info.is_idempotent());
    }

    #[test]
    fn test_parse_record_count_edge_cases() {
        // Exactly 61 bytes (minimum for the explicit records_count field)
        let batch = consistent_batch(100);
        assert_eq!(parse_record_count(&batch), 100);

        // Large batch: records beyond the header don't affect the count
        let mut large_batch = vec![0u8; 1000];
        large_batch[23..27].copy_from_slice(&999i32.to_be_bytes());
        large_batch[57..61].copy_from_slice(&1000i32.to_be_bytes());
        assert_eq!(parse_record_count(&large_batch), 1000);

        // Zero last_offset_delta + records_count 1 means a single record
        let single = consistent_batch(1);
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
