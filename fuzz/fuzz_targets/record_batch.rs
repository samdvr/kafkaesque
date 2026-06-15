#![no_main]
#![allow(deprecated)] // intentional: parse_record_count is the legacy wrapper this target verifies.

//! RecordBatch header validation + producer info extraction.
//!
//! Drives every batch-header function the broker calls on attacker bytes:
//!   - [`validate_batch_crc`]: CRC mismatch detection (production: corrupt-batch reject).
//!   - [`parse_record_count_checked`]: explicit count vs. last_offset_delta cross-check.
//!   - [`parse_producer_info`]: producer-id / epoch / first-sequence extraction.
//!   - [`parse_batch_max_timestamp`]: timestamp extraction for retention.
//!
//! Property: a single bit-flip inside the CRC-covered range MUST change the
//! validation outcome. This is the only roundtrip guarantee a v2 RecordBatch
//! header gives us, and missing it lets corruption past the gate.

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::keys::parse_batch_max_timestamp;
use kafkaesque::protocol::{
    CrcValidationResult, crc32c, parse_producer_info, parse_record_count,
    parse_record_count_checked, validate_batch_crc,
};

const MAX_INPUT: usize = 1024 * 1024;

/// Layout reference (matches `src/protocol.rs` doc comment):
///   0..8   base_offset
///   8..12  batch_length
///   12..16 partition_leader_epoch
///   16     magic
///   17..21 crc                  <-- stored
///   21..   CRC-covered region
const BATCH_LENGTH_OFFSET: usize = 8;
const BATCH_LENGTH_END: usize = 12;
/// Bytes counted by `batch_length`: total batch size = 12 + batch_length.
const BATCH_LENGTH_PREFIX: usize = 12;
const BATCH_CRC_OFFSET: usize = 17;
const BATCH_CRC_DATA_START: usize = 21;
const MIN_HEADER: usize = 61;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    // 1. Every header function must return cleanly on arbitrary input.
    let _ = validate_batch_crc(data);
    let _ = parse_record_count_checked(data);
    let _ = parse_record_count(data);
    let _ = parse_producer_info(data);
    let _ = parse_batch_max_timestamp(data);

    // 2. CRC-roundtrip property: rebuild a "valid" batch by writing the
    //    correct CRC into the header, then assert validation says Valid.
    if data.len() > BATCH_CRC_DATA_START {
        let mut buf = data.to_vec();
        // `validate_batch_crc` CRCs the bounded region [21..12+batch_length],
        // not the full buffer. Patch `batch_length` so the claimed region
        // covers the entire buffer — otherwise a fuzz input where the
        // header's batch_length is shorter than the slice produces a
        // spurious Invalid (we hashed bytes the validator didn't).
        let batch_length = (buf.len() - BATCH_LENGTH_PREFIX) as u32;
        buf[BATCH_LENGTH_OFFSET..BATCH_LENGTH_END]
            .copy_from_slice(&batch_length.to_be_bytes());
        let computed = crc32c(&buf[BATCH_CRC_DATA_START..]);
        buf[BATCH_CRC_OFFSET..BATCH_CRC_OFFSET + 4].copy_from_slice(&computed.to_be_bytes());
        match validate_batch_crc(&buf) {
            CrcValidationResult::Valid => {}
            CrcValidationResult::Invalid { expected, actual } => panic!(
                "patched-CRC batch validated as Invalid: expected={:#x} actual={:#x}",
                expected, actual,
            ),
            CrcValidationResult::TooSmall => {
                // Shouldn't happen at this length, but tolerate.
            }
            CrcValidationResult::FrameMismatch { .. } => {
                // batch_length in the header is attacker-controlled; a
                // mismatch against the slice we built is a framing error,
                // not a CRC corruption signal. Tolerate.
            }
            CrcValidationResult::OffloadFailed => {
                unreachable!("synchronous validate_batch_crc never offloads")
            }
            // Enum is non_exhaustive across the crate boundary.
            _ => {}
        }

        // 3. Bit-flip detection: if we flip a CRC-covered byte, the result
        //    MUST change to Invalid (or stay TooSmall, but we already passed
        //    the size gate above). The first byte after the CRC field is
        //    `attributes` — flipping it always changes the CRC region.
        if buf.len() > BATCH_CRC_DATA_START {
            buf[BATCH_CRC_DATA_START] ^= 0x80;
            match validate_batch_crc(&buf) {
                CrcValidationResult::Invalid { .. } => {}
                CrcValidationResult::Valid => {
                    panic!("bit-flip in CRC-covered region was not detected by validate_batch_crc")
                }
                CrcValidationResult::TooSmall => unreachable!("size gate already passed"),
                CrcValidationResult::FrameMismatch { .. } => {
                    // Same framing tolerance as above.
                }
                CrcValidationResult::OffloadFailed => {
                    unreachable!("synchronous validate_batch_crc never offloads")
                }
                // Enum is non_exhaustive across the crate boundary.
                _ => {}
            }
        }
    }

    // 4. parse_record_count vs parse_record_count_checked must agree on
    //    success and on failure (the `_unwrap_or(0)` wrapper).
    match parse_record_count_checked(data) {
        Ok(n) => assert_eq!(parse_record_count(data), n),
        Err(_) => assert_eq!(parse_record_count(data), 0),
    }

    // 5. ProducerBatchInfo invariants: if a batch is well-formed enough
    //    to extract producer info, last_sequence() must not panic.
    if data.len() >= MIN_HEADER
        && let Some(info) = parse_producer_info(data)
    {
        // last_sequence may saturate to None on overflow — that's fine.
        let _ = info.last_sequence();
        // is_idempotent is just a sign check; can't panic.
        let _ = info.is_idempotent();
    }
});
