//! Produce-path RecordBatch validation beyond CRC.
//!
//! `kafkaesque-protocol::validate_batch_crc` checks magic, framing, and
//! CRC-32C. That is necessary but not sufficient for refuse-don't-lie:
//! a CRC-valid batch can still declare transactional/control attributes
//! or a compressed payload the broker cannot decode. Accepting those
//! would store bytes that later consumers (or a future txn coordinator)
//! must interpret, while today's broker has no such coordinator and
//! never inspects the inner stream.
//!
//! This module is the produce gate:
//! 1. CRC / framing / magic (delegates to `validate_batch_crc`)
//! 2. Attribute bits — reject transactional (bit 4) and control (bit 5)
//! 3. Compression codec — reject undefined ids (>4)
//! 4. Compressed payload — attempt decompress; reject undecompressible
//! 5. Inner records — walk the (decompressed) records section and require
//!    the number of records to match the header `records_count`
//!
//! Fetch still returns stored bytes as opaque; producers that speak
//! gzip/snappy/lz4/zstd continue to interoperate because consumers
//! decompress client-side.

use std::io::Read;

use bytes::Bytes;

use crate::constants::{
    BATCH_CRC_DATA_START, BATCH_LENGTH_END, BATCH_LENGTH_OFFSET, BATCH_LENGTH_PREFIX,
    MIN_BATCH_HEADER_SIZE,
};
use crate::error::KafkaCode;
use crate::protocol::{CrcValidationResult, validate_batch_crc};

/// Explicit `records_count` INT32 at bytes 57–60 of the v2 header.
const BATCH_RECORDS_COUNT_OFFSET: usize = 57;
const BATCH_RECORDS_COUNT_END: usize = 61;
/// Attribute bit: batch is part of a transaction.
const ATTR_TRANSACTIONAL: u16 = 1 << 4;
/// Attribute bit: batch carries control records (commit/abort markers).
const ATTR_CONTROL: u16 = 1 << 5;
/// Low 3 bits of attributes = compression codec id.
const ATTR_CODEC_MASK: u16 = 0b111;
/// Highest Kafka-defined codec (zstd). Values 5–7 are undefined.
const MAX_COMPRESSION_CODEC: u8 = 4;

/// Xerial / snappy-java framing magic used by Apache Kafka's Snappy codec.
const SNAPPY_XERIAL_MAGIC: &[u8] = &[0x82, b'S', b'N', b'A', b'P', b'P', b'Y', 0];

/// Result of full produce-batch validation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ProduceBatchValidation {
    /// Batch may be appended.
    Valid,
    /// CRC / framing / magic failure from [`validate_batch_crc`].
    Crc(CrcValidationResult),
    /// Attributes bit 4 set — transactions are not supported.
    TransactionalBatch,
    /// Attributes bit 5 set — clients must not produce control batches.
    ControlBatch,
    /// Attributes codec id is not one of none/gzip/snappy/lz4/zstd.
    UnsupportedCompression { codec: u8 },
    /// Codec is known but the records section does not decompress.
    Undecompressible { codec: u8 },
    /// Header `records_count` does not match a walk of the inner records,
    /// or the records section is truncated / malformed.
    InvalidRecordLayout {
        declared: i32,
        observed: Option<i32>,
    },
}

impl ProduceBatchValidation {
    /// Map a rejection to the Kafka error code returned on Produce.
    pub fn to_kafka_code(&self) -> KafkaCode {
        match self {
            Self::Valid => KafkaCode::None,
            Self::Crc(_) => KafkaCode::CorruptMessage,
            // Match the request-level `transactional_id` reject so clients
            // see one typed signal for "this broker does not do transactions".
            Self::TransactionalBatch => KafkaCode::InvalidRequest,
            Self::ControlBatch => KafkaCode::InvalidRecord,
            Self::UnsupportedCompression { .. } => KafkaCode::UnsupportedCompressionType,
            Self::Undecompressible { .. } => KafkaCode::CorruptMessage,
            Self::InvalidRecordLayout { .. } => KafkaCode::InvalidRecord,
        }
    }

    pub fn is_valid(&self) -> bool {
        matches!(self, Self::Valid)
    }
}

/// Validate a RecordBatch for the produce path (CRC + attributes + codec).
pub fn validate_produce_batch(batch: &[u8]) -> ProduceBatchValidation {
    match validate_batch_crc(batch) {
        CrcValidationResult::Valid => {}
        other => return ProduceBatchValidation::Crc(other),
    }

    if batch.len() < MIN_BATCH_HEADER_SIZE {
        // CRC Valid with < 61 bytes is only possible for the exact-21
        // edge fixture; produce never ships that. Treat as framing.
        return ProduceBatchValidation::Crc(CrcValidationResult::TooSmall);
    }

    let attrs = u16::from_be_bytes([batch[BATCH_CRC_DATA_START], batch[BATCH_CRC_DATA_START + 1]]);
    if attrs & ATTR_TRANSACTIONAL != 0 {
        return ProduceBatchValidation::TransactionalBatch;
    }
    if attrs & ATTR_CONTROL != 0 {
        return ProduceBatchValidation::ControlBatch;
    }

    let codec = (attrs & ATTR_CODEC_MASK) as u8;
    if codec > MAX_COMPRESSION_CODEC {
        return ProduceBatchValidation::UnsupportedCompression { codec };
    }

    let claimed_size = batch_claimed_size(batch);
    if claimed_size < MIN_BATCH_HEADER_SIZE || claimed_size > batch.len() {
        return ProduceBatchValidation::Crc(CrcValidationResult::FrameMismatch {
            claimed_size,
            actual_size: batch.len(),
        });
    }

    let records = &batch[MIN_BATCH_HEADER_SIZE..claimed_size];
    let declared = records_count_header(batch);

    let inner = if codec == 0 {
        records.to_vec()
    } else {
        match decompress_records_bytes(codec, records) {
            Some(bytes) => bytes,
            None => return ProduceBatchValidation::Undecompressible { codec },
        }
    };

    match count_inner_records(&inner) {
        Some(observed) if declared >= 0 && observed == declared as usize => {
            ProduceBatchValidation::Valid
        }
        Some(observed) => ProduceBatchValidation::InvalidRecordLayout {
            declared,
            observed: Some(observed as i32),
        },
        None => ProduceBatchValidation::InvalidRecordLayout {
            declared,
            observed: None,
        },
    }
}

/// Async wrapper: offload large batches the same way CRC validation does.
pub async fn validate_produce_batch_async(batch: &Bytes) -> ProduceBatchValidation {
    use crate::protocol::CRC_OFFLOAD_THRESHOLD;
    if batch.len() < CRC_OFFLOAD_THRESHOLD {
        return validate_produce_batch(batch);
    }
    let owned = batch.clone();
    match tokio::task::spawn_blocking(move || validate_produce_batch(&owned)).await {
        Ok(result) => result,
        Err(_) => ProduceBatchValidation::Crc(CrcValidationResult::OffloadFailed),
    }
}

/// Attribute / compression checks without re-running CRC.
///
/// Used when `validate_record_crc` is disabled but refuse-don't-lie
/// attribute policy must still apply.
pub fn validate_produce_batch_attributes_only(batch: &[u8]) -> ProduceBatchValidation {
    if batch.len() < MIN_BATCH_HEADER_SIZE {
        return ProduceBatchValidation::Crc(CrcValidationResult::TooSmall);
    }
    let attrs = u16::from_be_bytes([batch[BATCH_CRC_DATA_START], batch[BATCH_CRC_DATA_START + 1]]);
    if attrs & ATTR_TRANSACTIONAL != 0 {
        return ProduceBatchValidation::TransactionalBatch;
    }
    if attrs & ATTR_CONTROL != 0 {
        return ProduceBatchValidation::ControlBatch;
    }
    let codec = (attrs & ATTR_CODEC_MASK) as u8;
    if codec > MAX_COMPRESSION_CODEC {
        return ProduceBatchValidation::UnsupportedCompression { codec };
    }

    let claimed_size = batch_claimed_size(batch);
    if claimed_size < MIN_BATCH_HEADER_SIZE || claimed_size > batch.len() {
        return ProduceBatchValidation::Crc(CrcValidationResult::FrameMismatch {
            claimed_size,
            actual_size: batch.len(),
        });
    }
    let records = &batch[MIN_BATCH_HEADER_SIZE..claimed_size];
    let declared = records_count_header(batch);
    let inner = if codec == 0 {
        records.to_vec()
    } else {
        match decompress_records_bytes(codec, records) {
            Some(bytes) => bytes,
            None => return ProduceBatchValidation::Undecompressible { codec },
        }
    };
    match count_inner_records(&inner) {
        Some(observed) if declared >= 0 && observed == declared as usize => {
            ProduceBatchValidation::Valid
        }
        Some(observed) => ProduceBatchValidation::InvalidRecordLayout {
            declared,
            observed: Some(observed as i32),
        },
        None => ProduceBatchValidation::InvalidRecordLayout {
            declared,
            observed: None,
        },
    }
}

fn records_count_header(batch: &[u8]) -> i32 {
    if batch.len() < BATCH_RECORDS_COUNT_END {
        return -1;
    }
    i32::from_be_bytes([
        batch[BATCH_RECORDS_COUNT_OFFSET],
        batch[BATCH_RECORDS_COUNT_OFFSET + 1],
        batch[BATCH_RECORDS_COUNT_OFFSET + 2],
        batch[BATCH_RECORDS_COUNT_OFFSET + 3],
    ])
}

fn batch_claimed_size(batch: &[u8]) -> usize {
    if batch.len() < BATCH_LENGTH_END {
        return 0;
    }
    let batch_length = i32::from_be_bytes([
        batch[BATCH_LENGTH_OFFSET],
        batch[BATCH_LENGTH_OFFSET + 1],
        batch[BATCH_LENGTH_OFFSET + 2],
        batch[BATCH_LENGTH_END - 1],
    ]);
    if batch_length <= 0 {
        return 0;
    }
    BATCH_LENGTH_PREFIX.saturating_add(batch_length as usize)
}

/// Walk v2 inner records. Returns `None` if the stream is truncated or a
/// record length is illegal. Returns `Some(n)` when exactly `n` records
/// were parsed and the buffer was fully consumed.
fn count_inner_records(records: &[u8]) -> Option<usize> {
    let mut offset = 0usize;
    let mut count = 0usize;
    while offset < records.len() {
        let (length, len_size) = read_zigzag_varint(&records[offset..])?;
        if length < 0 {
            return None;
        }
        offset += len_size;
        let body_len = length as usize;
        if offset + body_len > records.len() {
            return None;
        }
        // Body must at least hold attributes (1). Deeper field validation
        // is left to consumers; we only enforce framing + count.
        if body_len < 1 {
            return None;
        }
        offset += body_len;
        count += 1;
    }
    Some(count)
}

/// Protobuf / Kafka zigzag varint → signed i32. Returns (value, bytes_consumed).
fn read_zigzag_varint(input: &[u8]) -> Option<(i32, usize)> {
    let mut result: u32 = 0;
    let mut shift = 0u32;
    for (i, &b) in input.iter().enumerate() {
        if i >= 5 {
            return None; // would overflow i32 zigzag
        }
        result |= u32::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            let decoded = ((result >> 1) as i32) ^ -((result & 1) as i32);
            return Some((decoded, i + 1));
        }
        shift += 7;
    }
    None
}

fn decompress_records_bytes(codec: u8, records: &[u8]) -> Option<Vec<u8>> {
    match codec {
        1 => gzip_bytes(records),
        2 => snappy_bytes(records),
        3 => lz4_bytes(records),
        4 => zstd::stream::decode_all(records).ok(),
        _ => None,
    }
}

fn gzip_bytes(data: &[u8]) -> Option<Vec<u8>> {
    let mut dec = flate2::read::GzDecoder::new(data);
    let mut out = Vec::new();
    dec.read_to_end(&mut out).ok().map(|_| out)
}

fn snappy_bytes(data: &[u8]) -> Option<Vec<u8>> {
    if data.starts_with(SNAPPY_XERIAL_MAGIC) {
        return snappy_xerial_bytes(data);
    }
    let mut dec = snap::raw::Decoder::new();
    dec.decompress_vec(data).ok()
}

fn snappy_xerial_bytes(data: &[u8]) -> Option<Vec<u8>> {
    if data.len() < SNAPPY_XERIAL_MAGIC.len() {
        return None;
    }
    let mut offset = SNAPPY_XERIAL_MAGIC.len();
    let mut out = Vec::new();
    let mut dec = snap::raw::Decoder::new();
    let mut saw_block = false;
    while offset + 4 <= data.len() {
        let compressed_len =
            u32::from_be_bytes(data[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        if compressed_len == 0 || offset + compressed_len > data.len() {
            return None;
        }
        let block = dec
            .decompress_vec(&data[offset..offset + compressed_len])
            .ok()?;
        out.extend_from_slice(&block);
        offset += compressed_len;
        saw_block = true;
    }
    if saw_block && offset == data.len() {
        Some(out)
    } else {
        None
    }
}

fn lz4_bytes(data: &[u8]) -> Option<Vec<u8>> {
    {
        let mut decoder = match lz4::Decoder::new(data) {
            Ok(d) => d,
            Err(_) => return lz4::block::decompress(data, None).ok(),
        };
        let mut out = Vec::new();
        if decoder.read_to_end(&mut out).is_ok() {
            return Some(out);
        }
    }
    lz4::block::decompress(data, None).ok()
}

/// Build a minimal valid uncompressed v2 batch with `record_count` real
/// inner records. Useful for tests that previously stamped a header-only
/// fixture (which the inner-record walk now rejects).
pub fn build_minimal_valid_batch(record_count: i32) -> Vec<u8> {
    assert!(record_count >= 1);
    let mut records = Vec::new();
    for i in 0..record_count {
        records.extend_from_slice(&encode_minimal_record(i));
    }
    let mut batch = vec![0u8; MIN_BATCH_HEADER_SIZE];
    batch.extend_from_slice(&records);
    let len = (batch.len() as i32) - 12;
    batch[8..12].copy_from_slice(&len.to_be_bytes());
    batch[16] = 2; // magic v2
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes()); // last_offset_delta
    batch[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id
    batch[51..53].copy_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    batch[53..57].copy_from_slice(&(-1i32).to_be_bytes()); // base_sequence
    batch[57..61].copy_from_slice(&record_count.to_be_bytes());
    let crc = crate::protocol::crc32c(&batch[BATCH_CRC_DATA_START..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    batch
}

fn encode_zigzag_varint(value: i32) -> Vec<u8> {
    let mut n = ((value << 1) ^ (value >> 31)) as u32;
    let mut out = Vec::new();
    loop {
        if (n & !0x7f) == 0 {
            out.push(n as u8);
            break;
        }
        out.push(((n as u8) & 0x7f) | 0x80);
        n >>= 7;
    }
    out
}

fn encode_minimal_record(offset_delta: i32) -> Vec<u8> {
    // attributes(1) + ts_delta(0) + offset_delta + key(-1) + value(0) + headers(0)
    let mut body = Vec::new();
    body.push(0); // attributes
    body.extend_from_slice(&encode_zigzag_varint(0)); // timestampDelta
    body.extend_from_slice(&encode_zigzag_varint(offset_delta));
    body.extend_from_slice(&encode_zigzag_varint(-1)); // null key
    body.extend_from_slice(&encode_zigzag_varint(0)); // empty value
    body.extend_from_slice(&encode_zigzag_varint(0)); // headersCount
    let mut record = encode_zigzag_varint(body.len() as i32);
    record.extend_from_slice(&body);
    record
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::crc32c;

    fn stamp_crc(batch: &mut [u8]) {
        let crc = crc32c(&batch[BATCH_CRC_DATA_START..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
    }

    #[test]
    fn uncompressed_batch_is_valid() {
        let batch = build_minimal_valid_batch(1);
        assert_eq!(
            validate_produce_batch(&batch),
            ProduceBatchValidation::Valid
        );
    }

    #[test]
    fn header_only_batch_is_rejected() {
        let mut batch = vec![0u8; MIN_BATCH_HEADER_SIZE];
        let len = (batch.len() as i32) - 12;
        batch[8..12].copy_from_slice(&len.to_be_bytes());
        batch[16] = 2;
        batch[23..27].copy_from_slice(&0i32.to_be_bytes());
        batch[57..61].copy_from_slice(&1i32.to_be_bytes());
        stamp_crc(&mut batch);
        assert!(matches!(
            validate_produce_batch(&batch),
            ProduceBatchValidation::InvalidRecordLayout { declared: 1, .. }
        ));
    }

    #[test]
    fn transactional_bit_is_rejected() {
        let mut batch = build_minimal_valid_batch(1);
        let attrs = u16::from_be_bytes([batch[21], batch[22]]) | ATTR_TRANSACTIONAL;
        batch[21..23].copy_from_slice(&attrs.to_be_bytes());
        stamp_crc(&mut batch);
        assert_eq!(
            validate_produce_batch(&batch),
            ProduceBatchValidation::TransactionalBatch
        );
        assert_eq!(
            validate_produce_batch(&batch).to_kafka_code(),
            KafkaCode::InvalidRequest
        );
    }

    #[test]
    fn control_bit_is_rejected() {
        let mut batch = build_minimal_valid_batch(1);
        let attrs = u16::from_be_bytes([batch[21], batch[22]]) | ATTR_CONTROL;
        batch[21..23].copy_from_slice(&attrs.to_be_bytes());
        stamp_crc(&mut batch);
        assert_eq!(
            validate_produce_batch(&batch),
            ProduceBatchValidation::ControlBatch
        );
        assert_eq!(
            validate_produce_batch(&batch).to_kafka_code(),
            KafkaCode::InvalidRecord
        );
    }

    #[test]
    fn undefined_codec_is_rejected() {
        let mut batch = build_minimal_valid_batch(1);
        let attrs = (u16::from_be_bytes([batch[21], batch[22]]) & !ATTR_CODEC_MASK) | 7;
        batch[21..23].copy_from_slice(&attrs.to_be_bytes());
        stamp_crc(&mut batch);
        assert_eq!(
            validate_produce_batch(&batch),
            ProduceBatchValidation::UnsupportedCompression { codec: 7 }
        );
        assert_eq!(
            validate_produce_batch(&batch).to_kafka_code(),
            KafkaCode::UnsupportedCompressionType
        );
    }

    #[test]
    fn bogus_gzip_payload_is_rejected() {
        let mut batch = build_minimal_valid_batch(1);
        // Replace records with bogus gzip and set codec.
        batch.truncate(MIN_BATCH_HEADER_SIZE);
        batch[21..23].copy_from_slice(&1u16.to_be_bytes());
        batch.extend_from_slice(b"\x1f\x8b\x08not-gzip");
        let len = (batch.len() as i32) - 12;
        batch[8..12].copy_from_slice(&len.to_be_bytes());
        stamp_crc(&mut batch);
        assert_eq!(
            validate_produce_batch(&batch),
            ProduceBatchValidation::Undecompressible { codec: 1 }
        );
    }

    #[test]
    fn multi_record_batch_count_matches() {
        let batch = build_minimal_valid_batch(3);
        assert_eq!(
            validate_produce_batch(&batch),
            ProduceBatchValidation::Valid
        );
    }
}
