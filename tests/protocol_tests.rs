//! Integration tests for the protocol module.
//!
//! These tests verify record batch parsing and manipulation functions.

use bytes::{BufMut, Bytes, BytesMut};
use kafkaesque::protocol::{
    CrcValidationResult, parse_record_count, patch_base_offset, validate_batch_crc,
};

// ============================================================================
// Record Batch Format Reference
// ============================================================================
//
// Kafka Record Batch v2 format (61-byte header):
//   0-7:   baseOffset (8 bytes)
//   8-11:  batchLength (4 bytes)
//   12-15: partitionLeaderEpoch (4 bytes)
//   16:    magic (1 byte, must be 2)
//   17-20: crc (4 bytes)
//   21-22: attributes (2 bytes)
//   23-26: lastOffsetDelta (4 bytes)
//   27-34: firstTimestamp (8 bytes)
//   35-42: maxTimestamp (8 bytes)
//   43-50: producerId (8 bytes)
//   51-52: producerEpoch (2 bytes)
//   53-56: baseSequence (4 bytes)
//   57-60: numRecords (4 bytes)
//   61+:   records...

fn create_minimal_record_batch(base_offset: i64, num_records: i32) -> BytesMut {
    let mut batch = BytesMut::with_capacity(100);

    // baseOffset (8 bytes)
    batch.put_i64(base_offset);
    // batchLength (4 bytes)
    batch.put_i32(49);
    // partitionLeaderEpoch (4 bytes)
    batch.put_i32(1);
    // magic (1 byte)
    batch.put_i8(2);
    // crc (4 bytes) - placeholder, will compute later
    batch.put_u32(0);
    // attributes (2 bytes)
    batch.put_i16(0);
    // lastOffsetDelta (4 bytes)
    batch.put_i32(num_records - 1);
    // firstTimestamp (8 bytes)
    batch.put_i64(1000);
    // maxTimestamp (8 bytes)
    batch.put_i64(2000);
    // producerId (8 bytes)
    batch.put_i64(-1);
    // producerEpoch (2 bytes)
    batch.put_i16(-1);
    // baseSequence (4 bytes)
    batch.put_i32(-1);
    // numRecords (4 bytes)
    batch.put_i32(num_records);

    batch
}

// ============================================================================
// parse_record_count Tests
// ============================================================================

#[test]
fn test_parse_record_count_single_record() {
    let batch = create_minimal_record_batch(0, 1);
    let bytes = Bytes::from(batch.to_vec());
    // parse_record_count uses last_offset_delta + 1
    // last_offset_delta is at bytes 23-26
    let count = parse_record_count(&bytes);
    assert_eq!(count, 1);
}

#[test]
fn test_parse_record_count_multiple_records() {
    let batch = create_minimal_record_batch(0, 10);
    let bytes = Bytes::from(batch.to_vec());
    let count = parse_record_count(&bytes);
    assert_eq!(count, 10);
}

#[test]
fn test_parse_record_count_large_count() {
    let batch = create_minimal_record_batch(0, 1000);
    let bytes = Bytes::from(batch.to_vec());
    let count = parse_record_count(&bytes);
    assert_eq!(count, 1000);
}

#[test]
fn test_parse_record_count_empty_bytes() {
    let bytes = Bytes::new();
    let count = parse_record_count(&bytes);
    assert_eq!(count, 1); // Default when too short
}

#[test]
fn test_parse_record_count_too_short() {
    // Less than 27 bytes (need at least through last_offset_delta)
    let bytes = Bytes::from(vec![0u8; 20]);
    let count = parse_record_count(&bytes);
    assert_eq!(count, 1); // Default when too short
}

#[test]
fn test_parse_record_count_exact_27_bytes() {
    let mut batch = vec![0u8; 27];
    // Set last_offset_delta at bytes 23-26
    batch[23..27].copy_from_slice(&4i32.to_be_bytes());
    let bytes = Bytes::from(batch);
    let count = parse_record_count(&bytes);
    assert_eq!(count, 5); // last_offset_delta + 1
}

// ============================================================================
// patch_base_offset Tests
// ============================================================================

#[test]
fn test_patch_base_offset_valid() {
    let mut batch = create_minimal_record_batch(0, 1);
    let new_offset = 100i64;

    patch_base_offset(&mut batch, new_offset);

    // Verify the first 8 bytes contain the new offset
    let patched_offset = i64::from_be_bytes([
        batch[0], batch[1], batch[2], batch[3], batch[4], batch[5], batch[6], batch[7],
    ]);
    assert_eq!(patched_offset, 100);
}

#[test]
fn test_patch_base_offset_large_value() {
    let mut batch = create_minimal_record_batch(0, 1);
    let new_offset = i64::MAX;

    patch_base_offset(&mut batch, new_offset);

    let patched_offset = i64::from_be_bytes([
        batch[0], batch[1], batch[2], batch[3], batch[4], batch[5], batch[6], batch[7],
    ]);
    assert_eq!(patched_offset, i64::MAX);
}

#[test]
fn test_patch_base_offset_zero() {
    let mut batch = create_minimal_record_batch(100, 1);
    patch_base_offset(&mut batch, 0);

    let patched_offset = i64::from_be_bytes([
        batch[0], batch[1], batch[2], batch[3], batch[4], batch[5], batch[6], batch[7],
    ]);
    assert_eq!(patched_offset, 0);
}

#[test]
fn test_patch_base_offset_too_short() {
    // Buffer too short to contain base offset (less than 8 bytes)
    let mut batch = BytesMut::from(&[0u8; 7][..]);
    // Should not panic, just return without modification
    patch_base_offset(&mut batch, 100);
    assert_eq!(batch.len(), 7);
}

#[test]
fn test_patch_base_offset_updates_crc() {
    // Create a valid batch
    let mut batch = create_minimal_record_batch(0, 1);

    // Patch the offset - this should also update the CRC
    patch_base_offset(&mut batch, 99999);

    // Verify base offset was patched
    let patched_offset = i64::from_be_bytes([
        batch[0], batch[1], batch[2], batch[3], batch[4], batch[5], batch[6], batch[7],
    ]);
    assert_eq!(patched_offset, 99999);

    // Verify CRC is valid after patching
    let result = validate_batch_crc(&batch);
    assert_eq!(result, CrcValidationResult::Valid);
}

// ============================================================================
// validate_batch_crc Tests
// ============================================================================

#[test]
fn test_validate_batch_crc_too_small() {
    // Less than 21 bytes (minimum for CRC validation)
    let bytes = Bytes::from(vec![0u8; 20]);
    let result = validate_batch_crc(&bytes);
    assert_eq!(result, CrcValidationResult::TooSmall);
}

#[test]
fn test_validate_batch_crc_valid_after_patch() {
    // Create batch and patch it (which updates CRC)
    let mut batch = create_minimal_record_batch(0, 1);
    patch_base_offset(&mut batch, 12345);

    let result = validate_batch_crc(&batch);
    assert_eq!(result, CrcValidationResult::Valid);
}

#[test]
fn test_validate_batch_crc_corrupted_data() {
    // Create a valid batch
    let mut batch = create_minimal_record_batch(0, 1);
    patch_base_offset(&mut batch, 0); // Sets valid CRC

    // Corrupt some data after the CRC (in the CRC-covered region)
    let data_start = 21;
    if batch.len() > data_start + 1 {
        batch[data_start] ^= 0xFF; // Flip bits
    }

    let result = validate_batch_crc(&batch);
    match result {
        CrcValidationResult::Invalid { .. } => {}
        _ => panic!("Expected Invalid result, got {:?}", result),
    }
}

#[test]
fn test_validate_batch_crc_single_bit_flip() {
    // Create valid batch
    let mut batch = create_minimal_record_batch(0, 5);
    patch_base_offset(&mut batch, 0);
    assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);

    // Flip a single bit in the data (after CRC field)
    batch[30] ^= 0x01;

    match validate_batch_crc(&batch) {
        CrcValidationResult::Invalid { expected, actual } => {
            assert_ne!(expected, actual);
        }
        other => panic!("Expected Invalid after bit flip, got {:?}", other),
    }
}

// ============================================================================
// CrcValidationResult Tests
// ============================================================================

#[test]
fn test_crc_validation_result_debug() {
    let valid = CrcValidationResult::Valid;
    let debug_str = format!("{:?}", valid);
    assert!(debug_str.contains("Valid"));

    let too_small = CrcValidationResult::TooSmall;
    let debug_str = format!("{:?}", too_small);
    assert!(debug_str.contains("TooSmall"));

    let invalid = CrcValidationResult::Invalid {
        expected: 0x12345678,
        actual: 0xDEADBEEF,
    };
    let debug_str = format!("{:?}", invalid);
    assert!(debug_str.contains("Invalid"));
}

#[test]
fn test_crc_validation_result_equality() {
    assert_eq!(CrcValidationResult::Valid, CrcValidationResult::Valid);
    assert_eq!(CrcValidationResult::TooSmall, CrcValidationResult::TooSmall);
    assert_eq!(
        CrcValidationResult::Invalid {
            expected: 1,
            actual: 2
        },
        CrcValidationResult::Invalid {
            expected: 1,
            actual: 2
        }
    );
    assert_ne!(
        CrcValidationResult::Invalid {
            expected: 1,
            actual: 2
        },
        CrcValidationResult::Invalid {
            expected: 1,
            actual: 3
        }
    );
}

#[test]
fn test_crc_validation_result_clone() {
    let result = CrcValidationResult::Invalid {
        expected: 0x1234,
        actual: 0x5678,
    };
    let cloned = result;
    assert_eq!(result, cloned);
}

#[test]
fn test_crc_validation_result_copy() {
    let result = CrcValidationResult::Valid;
    let copied = result;
    assert_eq!(result, copied);
}
