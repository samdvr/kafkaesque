//! Compression codec pass-through tests for v2 RecordBatch.
//!
//! Background. Kafka encodes the compression codec in the low 3 bits of the
//! `attributes` field of a v2 RecordBatch (offset 21–22):
//!
//! ```text
//!   bits 0..2  compression codec  (0=none, 1=gzip, 2=snappy, 3=lz4, 4=zstd)
//!   bit  3     timestamp type     (0=create, 1=log-append)
//!   bit  4     transactional
//!   bit  5     control batch
//!   bits 6+    reserved
//! ```
//!
//! Kafkaesque today validates compressed payloads on produce: CRC, then
//! codec id, then an attempted decompress. Fetch still returns stored
//! bytes as opaque — clients decompress on the consumer side. Real Kafka
//! clients (librdkafka, Java) compress on the producer and decompress on
//! the consumer, so this model stays interoperable while refusing
//! undecompressible / undefined-codec batches (refuse-don't-lie).
//!
//! What this file asserts:
//!
//! 1. Each of the four Kafka codecs (gzip / snappy / lz4 / zstd) plus the
//!    uncompressed case round-trips through `validate_batch_crc` cleanly
//!    when the producer-supplied CRC matches the producer-computed CRC.
//! 2. A bit-flip inside the compressed payload is caught by CRC, regardless
//!    of codec — i.e. the broker DOES detect transport corruption of
//!    compressed batches, even without decoding them.
//! 3. The `attributes` bits encode codec + flag bits independently — setting
//!    `log-append-time` alongside a codec preserves both (transactional /
//!    control bits are refused on the produce path; see batch_validation).
//! 4. `parse_record_count` reads the explicit header field correctly
//!    irrespective of how (or whether) the records section is compressed.
//! 5. `patch_base_offset` does not disturb the CRC envelope on a compressed
//!    batch — `base_offset` lives outside the CRC region (bytes 0–7).
//! 6. The `UnsupportedMagic` rejection takes precedence over compression
//!    bits — a v0/v1 batch with codec bits set is still rejected on the
//!    magic byte, not silently mis-CRCed.
//! 7. The async CRC offload path (≥ 64 KiB) returns the same verdict as the
//!    sync path for compressed payloads.
//! 8. Produce-path validation refuses undefined codecs and undecompressible
//!    payloads while still accepting well-formed compressed batches.

use bytes::Bytes;
use kafkaesque::constants::{
    BATCH_CRC_DATA_START, BATCH_CRC_OFFSET, BATCH_LENGTH_OFFSET, BATCH_MAGIC_OFFSET,
};
use kafkaesque::protocol::{
    CrcValidationResult, crc32c, parse_record_count_checked, patch_base_offset, validate_batch_crc,
    validate_batch_crc_async,
};
use std::io::Write;

// ---------------------------------------------------------------------------
// Codec table & encoders
// ---------------------------------------------------------------------------

/// Kafka v2 compression codec identifiers, packed into the low 3 bits of
/// `attributes`. Mirrors `org.apache.kafka.common.record.CompressionType` so
/// a wire trace from a real Kafka producer lines up byte-for-byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Codec {
    None = 0,
    Gzip = 1,
    Snappy = 2,
    Lz4 = 3,
    Zstd = 4,
}

impl Codec {
    fn name(self) -> &'static str {
        match self {
            Codec::None => "none",
            Codec::Gzip => "gzip",
            Codec::Snappy => "snappy",
            Codec::Lz4 => "lz4",
            Codec::Zstd => "zstd",
        }
    }

    /// All codecs Kafka clients commonly emit, used to drive the matrix
    /// tests below. `None` is included so the matrix exercises the
    /// "no compression" branch of the same builder/CRC code path.
    fn all() -> [Codec; 5] {
        [
            Codec::None,
            Codec::Gzip,
            Codec::Snappy,
            Codec::Lz4,
            Codec::Zstd,
        ]
    }
}

/// Compress `payload` with `codec`. The output is whatever the codec
/// produces; the broker treats it as opaque bytes. We deliberately don't
/// try to be byte-exact with librdkafka's framing here — the tests are
/// asserting CRC pass-through and attribute-bit handling, not codec
/// conformance.
fn encode(codec: Codec, payload: &[u8]) -> Vec<u8> {
    match codec {
        Codec::None => payload.to_vec(),
        Codec::Gzip => {
            let mut enc = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            enc.write_all(payload).expect("gzip write");
            enc.finish().expect("gzip finish")
        }
        Codec::Snappy => {
            // Raw block format. Kafka actually uses xerial-framed snappy
            // on the wire, but the broker never inspects the payload, so
            // raw bytes are sufficient for the CRC contract.
            let mut enc = snap::raw::Encoder::new();
            enc.compress_vec(payload).expect("snappy encode")
        }
        Codec::Lz4 => {
            let mut enc = lz4::EncoderBuilder::new()
                .build(Vec::new())
                .expect("lz4 builder");
            enc.write_all(payload).expect("lz4 write");
            let (out, res) = enc.finish();
            res.expect("lz4 finish");
            out
        }
        Codec::Zstd => zstd::stream::encode_all(payload, 0).expect("zstd encode"),
    }
}

// ---------------------------------------------------------------------------
// RecordBatch builder
// ---------------------------------------------------------------------------

/// Build a v2 RecordBatch wrapping `payload_compressed_bytes` and stamp
/// `attributes` per `codec`/`extra_flags`. Returns the on-the-wire bytes
/// with CRC and batch_length filled in correctly.
///
/// `record_count` is written into both `last_offset_delta` (count − 1) and
/// the explicit `records_count` field so `parse_record_count_checked`
/// agrees with `parse_record_count` — independent of what's actually in
/// the records section.
fn build_batch(
    codec: Codec,
    extra_attr_flags: u16,
    record_count: i32,
    payload_compressed_bytes: &[u8],
) -> Vec<u8> {
    assert!(record_count >= 1, "v2 batches must contain ≥1 record");

    let mut batch = Vec::with_capacity(61 + payload_compressed_bytes.len());

    // attributes: low 3 bits = codec, plus any extra flag bits the caller
    // wants to stamp (transactional, log-append-time, control).
    let attributes: u16 = (codec as u16) | extra_attr_flags;

    // 0..8   base_offset
    batch.extend_from_slice(&0i64.to_be_bytes());
    // 8..12  batch_length (placeholder, patched below)
    batch.extend_from_slice(&0i32.to_be_bytes());
    // 12..16 partition_leader_epoch
    batch.extend_from_slice(&0i32.to_be_bytes());
    // 16     magic = v2
    batch.push(2);
    // 17..21 crc (placeholder, patched below)
    batch.extend_from_slice(&0u32.to_be_bytes());
    // 21..23 attributes
    batch.extend_from_slice(&attributes.to_be_bytes());
    // 23..27 last_offset_delta = record_count - 1
    batch.extend_from_slice(&(record_count - 1).to_be_bytes());
    // 27..35 first_timestamp
    batch.extend_from_slice(&1_700_000_000_000i64.to_be_bytes());
    // 35..43 max_timestamp
    batch.extend_from_slice(&1_700_000_001_000i64.to_be_bytes());
    // 43..51 producer_id
    batch.extend_from_slice(&(-1i64).to_be_bytes());
    // 51..53 producer_epoch
    batch.extend_from_slice(&(-1i16).to_be_bytes());
    // 53..57 base_sequence
    batch.extend_from_slice(&(-1i32).to_be_bytes());
    // 57..61 records_count
    batch.extend_from_slice(&record_count.to_be_bytes());
    // 61..   compressed (or plain) records
    batch.extend_from_slice(payload_compressed_bytes);

    // Stamp batch_length (counts bytes from 12 to end).
    let batch_length = (batch.len() - BATCH_LENGTH_OFFSET - 4) as i32;
    batch[BATCH_LENGTH_OFFSET..BATCH_LENGTH_OFFSET + 4]
        .copy_from_slice(&batch_length.to_be_bytes());

    // Stamp CRC over [21..end].
    let crc = crc32c(&batch[BATCH_CRC_DATA_START..]);
    batch[BATCH_CRC_OFFSET..BATCH_CRC_OFFSET + 4].copy_from_slice(&crc.to_be_bytes());

    batch
}

/// Convenience: build a batch with `record_count=3` carrying a fixed
/// payload so each codec test gets a non-trivially-compressible input.
fn build_canonical_batch(codec: Codec) -> Vec<u8> {
    let payload =
        b"kafkaesque-compression-test-record-payload-padding-padding-padding-padding-padding";
    let compressed = encode(codec, payload);
    build_batch(codec, 0, 3, &compressed)
}

fn attributes_of(batch: &[u8]) -> u16 {
    u16::from_be_bytes([batch[21], batch[22]])
}

// ---------------------------------------------------------------------------
// 1. CRC accepts well-formed batches for every codec
// ---------------------------------------------------------------------------

#[test]
fn every_codec_passes_crc_validation() {
    for codec in Codec::all() {
        let batch = build_canonical_batch(codec);
        assert_eq!(
            validate_batch_crc(&batch),
            CrcValidationResult::Valid,
            "{} batch should validate cleanly",
            codec.name(),
        );
    }
}

#[test]
fn every_codec_reports_correct_record_count() {
    // The records_count field lives in the v2 header (bytes 57-60), OUTSIDE
    // the compressed records section, so it must be readable for every
    // codec without any decompression. Pin this so a future inner-decode
    // implementation doesn't accidentally start using the inner count.
    for codec in Codec::all() {
        let batch = build_canonical_batch(codec);
        assert_eq!(
            parse_record_count_checked(&Bytes::from(batch)),
            Ok(3),
            "{} batch: header record_count must be 3 regardless of compression",
            codec.name(),
        );
    }
}

// ---------------------------------------------------------------------------
// 2. CRC catches mid-flight corruption of compressed payload
// ---------------------------------------------------------------------------

#[test]
fn bit_flip_inside_compressed_payload_is_rejected_for_every_codec() {
    // The CRC envelope covers bytes 21+, i.e. it covers the compressed
    // payload. Even though the broker never decodes the payload, it should
    // still reject batches whose payload was corrupted in transit. This
    // pins the "transport corruption is caught regardless of codec"
    // invariant.
    for codec in Codec::all() {
        let mut batch = build_canonical_batch(codec);
        let payload_byte_idx = 61; // first byte after header
        assert!(
            payload_byte_idx < batch.len(),
            "{}: empty payload",
            codec.name()
        );

        let original = batch[payload_byte_idx];
        batch[payload_byte_idx] ^= 0x01; // single-bit flip
        // Skip the test if the flip happens to leave a byte unchanged for
        // some reason (it can't for XOR 0x01, but be explicit).
        assert_ne!(
            batch[payload_byte_idx],
            original,
            "{}: flip didn't change byte",
            codec.name()
        );

        match validate_batch_crc(&batch) {
            CrcValidationResult::Invalid { .. } => {} // expected
            other => panic!(
                "{}: expected Invalid CRC after payload bit-flip, got {:?}",
                codec.name(),
                other
            ),
        }
    }
}

#[test]
fn bit_flip_in_attributes_is_rejected_for_every_codec() {
    // Attributes is byte 21–22 — the very first byte of the CRC region.
    // Flipping the codec bits without recomputing CRC must be caught: an
    // attacker should not be able to silently downgrade a codec.
    for codec in Codec::all() {
        let mut batch = build_canonical_batch(codec);
        batch[21] ^= 0x01; // flip the LSB of attributes (= codec bit 0)
        match validate_batch_crc(&batch) {
            CrcValidationResult::Invalid { .. } => {}
            other => panic!(
                "{}: flipping codec bits without re-CRC must fail, got {:?}",
                codec.name(),
                other
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// 3. Attribute-bit encoding: codec + flag bits are independent
// ---------------------------------------------------------------------------

#[test]
fn codec_bits_occupy_low_three_bits_of_attributes() {
    for codec in Codec::all() {
        let batch = build_canonical_batch(codec);
        let attrs = attributes_of(&batch);
        assert_eq!(
            attrs & 0b111,
            codec as u16,
            "{}: low 3 bits of attributes must equal codec id ({})",
            codec.name(),
            codec as u16,
        );
        // No other bits should be set in our canonical builder.
        assert_eq!(
            attrs & !0b111,
            0,
            "{}: builder should not have set any flag bits, attrs={:016b}",
            codec.name(),
            attrs,
        );
    }
}

#[test]
fn codec_coexists_with_transactional_and_log_append_time_bits() {
    // Bit 4 = transactional, bit 3 = log-append timestamp type.
    // These must be readable alongside the codec bits without interference
    // at the CRC layer. Produce-path validation still refuses the
    // transactional bit (refuse-don't-lie).
    let extra = (1u16 << 3) | (1u16 << 4); // log-append-time + transactional
    for codec in Codec::all() {
        let payload = encode(codec, b"with-flags");
        let batch = build_batch(codec, extra, 1, &payload);
        let attrs = attributes_of(&batch);
        assert_eq!(attrs & 0b111, codec as u16, "{}: codec bits", codec.name());
        assert_eq!((attrs >> 3) & 1, 1, "{}: log-append-time bit", codec.name());
        assert_eq!((attrs >> 4) & 1, 1, "{}: transactional bit", codec.name());
        assert_eq!(
            validate_batch_crc(&batch),
            CrcValidationResult::Valid,
            "{}: CRC must still validate with extra flags",
            codec.name(),
        );
        assert_eq!(
            kafkaesque::batch::validate_produce_batch(&batch),
            kafkaesque::batch::ProduceBatchValidation::TransactionalBatch,
            "{}: produce path must refuse transactional bit",
            codec.name(),
        );
    }
}

// ---------------------------------------------------------------------------
// 4. base_offset patching is codec-independent
// ---------------------------------------------------------------------------

#[test]
fn patch_base_offset_preserves_crc_for_every_codec() {
    // base_offset lives at bytes 0..8 — strictly outside the CRC region
    // (which starts at byte 21). Patching it on a compressed batch must
    // leave the CRC verdict unchanged, just as it does for an
    // uncompressed batch.
    for codec in Codec::all() {
        let mut batch = build_canonical_batch(codec);
        assert_eq!(
            validate_batch_crc(&batch),
            CrcValidationResult::Valid,
            "{}: pre-patch sanity",
            codec.name(),
        );
        patch_base_offset(&mut batch, 999_999).unwrap();
        assert_eq!(
            validate_batch_crc(&batch),
            CrcValidationResult::Valid,
            "{}: CRC must still validate after base_offset patch",
            codec.name(),
        );
        let patched = i64::from_be_bytes(batch[0..8].try_into().unwrap());
        assert_eq!(patched, 999_999, "{}: base_offset patched", codec.name());
    }
}

// ---------------------------------------------------------------------------
// 5. UnsupportedMagic precedence — codec bits do not bypass the magic check
// ---------------------------------------------------------------------------

#[test]
fn legacy_magic_byte_rejected_even_with_codec_bits_set() {
    // A v0 or v1 MessageSet has a different layout — feeding it to the v2
    // CRC validator without the magic-byte gate would silently checksum the
    // wrong region and return a meaningless verdict. Setting compression
    // bits in attributes does not bypass that gate.
    for codec in Codec::all() {
        let mut batch = build_canonical_batch(codec);
        batch[BATCH_MAGIC_OFFSET] = 1; // v1 — not supported
        match validate_batch_crc(&batch) {
            CrcValidationResult::UnsupportedMagic { magic: 1 } => {}
            other => panic!(
                "{}: v1 magic with {} codec bits should be UnsupportedMagic, got {:?}",
                codec.name(),
                codec.name(),
                other
            ),
        }

        batch[BATCH_MAGIC_OFFSET] = 0; // v0
        match validate_batch_crc(&batch) {
            CrcValidationResult::UnsupportedMagic { magic: 0 } => {}
            other => panic!(
                "{}: v0 magic with {} codec bits should be UnsupportedMagic, got {:?}",
                codec.name(),
                codec.name(),
                other
            ),
        }
    }
}

// ---------------------------------------------------------------------------
// 6. Async CRC offload path agrees with the sync path
// ---------------------------------------------------------------------------

#[tokio::test]
async fn async_crc_path_agrees_for_small_compressed_batches() {
    // Below the 64 KiB offload threshold the async wrapper inlines the
    // sync path. Pin that the verdict matches.
    for codec in Codec::all() {
        let batch = Bytes::from(build_canonical_batch(codec));
        assert_eq!(
            validate_batch_crc_async(&batch).await,
            CrcValidationResult::Valid,
            "{}: small async path",
            codec.name(),
        );
    }
}

#[tokio::test]
async fn async_crc_path_agrees_for_large_compressed_batches() {
    // Force the spawn_blocking offload path (≥ 64 KiB) for at least one
    // codec. To keep the *encoded* payload above the threshold even after
    // compression, we feed each codec a high-entropy pseudo-random buffer
    // — gzip / zstd / lz4 / snappy can't meaningfully compress incompressible
    // bytes, so the encoded output stays close to input size.
    const INPUT_SIZE: usize = 256 * 1024; // well above the 64 KiB threshold
    let mut rng_state = 0x12345678u32;
    let mut high_entropy = vec![0u8; INPUT_SIZE];
    for byte in high_entropy.iter_mut() {
        rng_state = rng_state.wrapping_mul(1664525).wrapping_add(1013904223);
        *byte = (rng_state >> 24) as u8;
    }

    for codec in Codec::all() {
        let encoded = encode(codec, &high_entropy);
        let batch = Bytes::from(build_batch(codec, 0, 1, &encoded));
        assert!(
            batch.len() >= 64 * 1024,
            "{}: test fixture must exceed CRC offload threshold (got {})",
            codec.name(),
            batch.len(),
        );
        assert_eq!(
            validate_batch_crc_async(&batch).await,
            CrcValidationResult::Valid,
            "{}: large async path",
            codec.name(),
        );
    }
}

// ---------------------------------------------------------------------------
// 7. Compression refuse-don't-lie — codec + inner decompress at produce.
// ---------------------------------------------------------------------------

#[test]
fn malformed_compressed_payload_with_valid_crc_is_rejected_on_produce_path() {
    // CRC can still validate an opaque gzip-looking blob; the produce
    // validator must still refuse it once it tries to decompress.
    let bogus_gzip = b"\x1f\x8b\x08not-actually-a-gzip-stream-just-the-magic";
    let batch = build_batch(Codec::Gzip, 0, 1, bogus_gzip);
    assert_eq!(
        validate_batch_crc(&batch),
        CrcValidationResult::Valid,
        "CRC remains a transport check and passes for this fixture",
    );
    assert_eq!(
        kafkaesque::batch::validate_produce_batch(&batch),
        kafkaesque::batch::ProduceBatchValidation::Undecompressible { codec: 1 },
        "produce path must refuse undecompressible gzip payloads",
    );
}

#[test]
fn unrecognised_codec_value_is_rejected_on_produce_path() {
    // Attributes bits 0..2 = 7 is not a defined codec.
    let payload = b"opaque";
    let mut batch = build_batch(Codec::None, 0, 1, payload);
    let mut attrs = u16::from_be_bytes([batch[21], batch[22]]);
    attrs = (attrs & !0b111) | 7;
    batch[21..23].copy_from_slice(&attrs.to_be_bytes());
    let new_crc = crc32c(&batch[BATCH_CRC_DATA_START..]);
    batch[BATCH_CRC_OFFSET..BATCH_CRC_OFFSET + 4].copy_from_slice(&new_crc.to_be_bytes());

    assert_eq!(
        attributes_of(&batch) & 0b111,
        7,
        "fixture should have stamped undefined codec id 7"
    );
    assert_eq!(
        validate_batch_crc(&batch),
        CrcValidationResult::Valid,
        "CRC does not interpret codec bits",
    );
    assert_eq!(
        kafkaesque::batch::validate_produce_batch(&batch),
        kafkaesque::batch::ProduceBatchValidation::UnsupportedCompression { codec: 7 },
        "produce path must refuse undefined compression codecs",
    );
}

#[test]
fn well_formed_compressed_codecs_pass_produce_validation() {
    // Compress a real v2 records section (not arbitrary bytes) so the
    // post-decompress inner-record walk matches `records_count`.
    let valid = kafkaesque::batch::build_minimal_valid_batch(1);
    let records = &valid[61..];
    for codec in Codec::all() {
        let payload = encode(codec, records);
        let batch = build_batch(codec, 0, 1, &payload);
        assert_eq!(
            kafkaesque::batch::validate_produce_batch(&batch),
            kafkaesque::batch::ProduceBatchValidation::Valid,
            "{}: well-formed compressed batch must be accepted",
            codec.name(),
        );
    }
}
