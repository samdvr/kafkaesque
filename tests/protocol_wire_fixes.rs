//! Integration tests for the protocol-layer
//!
//! Covers, end to end through the public crate API:
//! 1. Advertised API version enforcement in `Request::parse`
//! 2. Per-API wire-format gates (OffsetCommit, JoinGroup, InitProducerId,
//!    Metadata, CreateTopics, ListOffsets)
//! 3. `parse_record_count` reading the explicit `records_count` field
//! 4. CRC-32C correctness and `patch_base_offset` no longer touching the CRC
//!
//! ```sh
//! cargo test --test protocol_wire_fixes
//! ```

#![allow(deprecated)]

use bytes::{BufMut, Bytes, BytesMut};
use kafkaesque::protocol::{
    CrcValidationResult, RecordCountError, crc32c, parse_record_count, parse_record_count_checked,
    patch_base_offset, validate_batch_crc,
};
use kafkaesque::server::request::{ApiKey, Request};
use kafkaesque::server::versions::{SUPPORTED_VERSIONS, is_version_supported};

// ============================================================================
// Helpers
// ============================================================================

/// Standard (non-flexible) request header bytes.
fn header(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(64);
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);
    match client_id {
        Some(id) => {
            buf.put_i16(id.len() as i16);
            buf.put_slice(id.as_bytes());
        }
        None => buf.put_i16(-1),
    }
    buf
}

fn put_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

/// A minimal, internally consistent 61-byte v2 RecordBatch with a valid CRC.
/// `record_count` is written to BOTH `last_offset_delta` (bytes 23-26, as
/// count - 1) and the explicit `records_count` field (bytes 57-60).
fn consistent_batch_with_crc(record_count: i32) -> Vec<u8> {
    let mut batch = vec![0u8; 61];
    // batch_length (bytes 8-11) is the count of bytes from byte 12 to the
    // end of the batch — for a 61-byte buffer that's 49.
    let batch_length: i32 = (batch.len() as i32) - 12;
    batch[8..12].copy_from_slice(&batch_length.to_be_bytes());
    batch[16] = 2; // magic = 2
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());
    batch[57..61].copy_from_slice(&record_count.to_be_bytes());
    let crc = crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    batch
}

// ============================================================================
// Finding 1: advertised API versions are enforced
// ============================================================================

#[test]
fn unsupported_version_yields_typed_variant_with_correlation_id() {
    // Produce is advertised v3..=v3 only.
    let mut buf = header(0, 0, 424_242, Some("old-producer"));
    buf.put_slice(&[0xAB; 16]); // garbage body that must never be parsed

    let request = Request::parse(Bytes::from(buf.to_vec())).expect("must not be an Err");
    match request {
        Request::UnsupportedVersion(h) => {
            assert_eq!(h.api_key, ApiKey::Produce);
            assert_eq!(h.api_version, 0);
            assert_eq!(h.correlation_id, 424_242);
            assert_eq!(h.client_id, Some("old-producer".to_string()));
        }
        other => panic!("expected UnsupportedVersion, got {:?}", other),
    }
}

#[test]
fn every_advertised_boundary_is_enforced() {
    // For every advertised API: min-1 and max+1 must be refused, while min
    // and max pass the version gate (is_version_supported reflects parse).
    for sv in SUPPORTED_VERSIONS {
        assert!(is_version_supported(sv.api_key, sv.min_version));
        assert!(is_version_supported(sv.api_key, sv.max_version));
        assert!(!is_version_supported(sv.api_key, sv.min_version - 1));
        assert!(!is_version_supported(sv.api_key, sv.max_version + 1));

        // Below-min on the wire (empty body — must not matter, the body is
        // never parsed for unsupported versions).
        let api_key_i16: i16 = sv.api_key.into();
        let buf = header(api_key_i16, sv.min_version - 1, 9, None);
        let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
        assert!(
            matches!(request, Request::UnsupportedVersion(_)),
            "{:?} v{} must be refused",
            sv.api_key,
            sv.min_version - 1
        );
    }
}

#[test]
fn supported_version_parses_normally() {
    // Metadata v1 (advertised) with a null topics array parses fully.
    let mut buf = header(3, 1, 1001, Some("client"));
    buf.put_i32(-1);

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::Metadata(h, body) => {
            assert_eq!(h.correlation_id, 1001);
            assert!(body.topics.is_none());
        }
        other => panic!("expected Metadata, got {:?}", other),
    }
}

#[test]
fn unknown_api_key_is_not_version_gated() {
    let mut buf = header(999, 17, 5, None);
    buf.put_slice(b"opaque");
    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    assert!(matches!(request, Request::Unknown(_, _)));
}

// ============================================================================
// Finding 2a/2b: OffsetCommit per-version layouts
// ============================================================================

#[test]
fn offset_commit_v0_has_no_group_generation_fields() {
    // v0 layout: group_id, [topics]. generation_id/member_id must NOT be
    // read (they were previously consumed unconditionally, eating the
    // topics array).
    let mut buf = header(8, 0, 90, None);
    put_string(&mut buf, "grp");
    buf.put_i32(1); // 1 topic
    put_string(&mut buf, "t");
    buf.put_i32(1); // 1 partition
    buf.put_i32(4); // partition_index
    buf.put_i64(77); // committed_offset
    buf.put_i16(-1); // null metadata

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::OffsetCommit(_, body) => {
            assert_eq!(body.group_id, "grp");
            assert_eq!(body.generation_id, -1);
            assert_eq!(body.member_id, "");
            assert_eq!(body.topics.len(), 1);
            assert_eq!(body.topics[0].partitions[0].partition_index, 4);
            assert_eq!(body.topics[0].partitions[0].committed_offset, 77);
        }
        other => panic!("expected OffsetCommit, got {:?}", other),
    }
}

#[test]
fn offset_commit_v2_consumes_retention_time_ms() {
    // v2 layout: group_id, generation_id, member_id, retention_time_ms,
    // [topics]. Without consuming retention_time_ms the topics array is
    // read 8 bytes early.
    let mut buf = header(8, 2, 91, None);
    put_string(&mut buf, "grp2");
    buf.put_i32(3); // generation_id
    put_string(&mut buf, "member");
    buf.put_i64(86_400_000); // retention_time_ms (v2..=v4 only)
    buf.put_i32(1); // 1 topic
    put_string(&mut buf, "topic");
    buf.put_i32(1); // 1 partition
    buf.put_i32(0);
    buf.put_i64(123);
    put_string(&mut buf, "m");

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::OffsetCommit(_, body) => {
            assert_eq!(body.group_id, "grp2");
            assert_eq!(body.generation_id, 3);
            assert_eq!(body.member_id, "member");
            assert_eq!(body.topics[0].name, "topic");
            assert_eq!(body.topics[0].partitions[0].committed_offset, 123);
            assert_eq!(
                body.topics[0].partitions[0].committed_metadata,
                Some("m".to_string())
            );
        }
        other => panic!("expected OffsetCommit, got {:?}", other),
    }
}

#[test]
fn offset_commit_v1_consumes_per_partition_commit_timestamp() {
    // v1 (only) carries commit_timestamp per partition. Two partitions
    // back-to-back only parse correctly if it is consumed.
    let mut buf = header(8, 1, 92, None);
    put_string(&mut buf, "g");
    buf.put_i32(1); // generation_id
    put_string(&mut buf, "m");
    buf.put_i32(1); // 1 topic
    put_string(&mut buf, "t");
    buf.put_i32(2); // 2 partitions
    for p in 0..2i32 {
        buf.put_i32(p);
        buf.put_i64(100 + p as i64);
        buf.put_i64(1_700_000_000_000); // commit_timestamp (v1 only)
        buf.put_i16(-1); // null metadata
    }

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::OffsetCommit(_, body) => {
            assert_eq!(body.topics[0].partitions.len(), 2);
            assert_eq!(body.topics[0].partitions[1].partition_index, 1);
            assert_eq!(body.topics[0].partitions[1].committed_offset, 101);
        }
        other => panic!("expected OffsetCommit, got {:?}", other),
    }
}

// ============================================================================
// Finding 2c: JoinGroup v2 (verified: request layout identical to v1)
// ============================================================================

#[test]
fn join_group_v2_layout_matches_v1_with_rebalance_timeout() {
    // that field arrived in v5 (KIP-345). v1+ adds rebalance_timeout_ms
    // (KIP-62); v2 changed only the response. Verify a v2 request parses.
    let mut buf = header(11, 2, 93, Some("consumer-1"));
    put_string(&mut buf, "group");
    buf.put_i32(10_000); // session_timeout_ms
    buf.put_i32(20_000); // rebalance_timeout_ms (v1+)
    put_string(&mut buf, "member");
    put_string(&mut buf, "consumer");
    buf.put_i32(1); // 1 protocol
    put_string(&mut buf, "range");
    buf.put_i32(2);
    buf.put_slice(&[0xCA, 0xFE]);

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::JoinGroup(h, body) => {
            assert_eq!(h.api_version, 2);
            assert_eq!(body.group_id, "group");
            assert_eq!(body.session_timeout_ms, 10_000);
            assert_eq!(body.rebalance_timeout_ms, 20_000);
            assert_eq!(body.member_id, "member");
            assert_eq!(body.protocols[0].metadata.as_ref(), &[0xCA, 0xFE]);
        }
        other => panic!("expected JoinGroup, got {:?}", other),
    }
}

#[test]
fn join_group_v0_defaults_rebalance_timeout_to_session_timeout() {
    let mut buf = header(11, 0, 94, None);
    put_string(&mut buf, "g0");
    buf.put_i32(7_000); // session_timeout_ms; v0 has NO rebalance_timeout_ms
    put_string(&mut buf, "");
    put_string(&mut buf, "consumer");
    buf.put_i32(0); // no protocols

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::JoinGroup(_, body) => {
            assert_eq!(body.session_timeout_ms, 7_000);
            assert_eq!(body.rebalance_timeout_ms, 7_000);
        }
        other => panic!("expected JoinGroup, got {:?}", other),
    }
}

// ============================================================================
// Finding 2d: InitProducerId flexible at v2 (both directions' request half)
// ============================================================================

#[test]
fn init_producer_id_v2_parses_flexible_header_and_body() {
    // v2 request = header v2 (classic client_id + tagged fields) + flexible
    // body (compact nullable string + tagged fields). Previously parsed as
    // classic, mis-framing everything after the header.
    let mut buf = BytesMut::new();
    buf.put_i16(22); // InitProducerId
    buf.put_i16(2); // v2 — first flexible version per spec
    buf.put_i32(95);
    buf.put_i16(8); // client_id (classic in header v2)
    buf.put_slice(b"producer");
    buf.put_u8(0); // header tagged fields
    buf.put_u8(4); // compact string "txn" (len 3 + 1)
    buf.put_slice(b"txn");
    buf.put_i32(60_000);
    buf.put_u8(0); // body tagged fields

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::InitProducerId(h, body) => {
            assert_eq!(h.api_version, 2);
            assert_eq!(h.correlation_id, 95);
            assert_eq!(h.client_id, Some("producer".to_string()));
            assert_eq!(body.transactional_id, Some("txn".to_string()));
            assert_eq!(body.transaction_timeout_ms, 60_000);
            // v2 has no producer_id/epoch fields (added v3, KIP-360)
            assert_eq!(body.producer_id, -1);
            assert_eq!(body.producer_epoch, -1);
        }
        other => panic!("expected InitProducerId, got {:?}", other),
    }
}

#[test]
fn init_producer_id_v0_parses_classic() {
    let mut buf = header(22, 0, 96, None);
    buf.put_i16(-1); // null transactional_id (classic)
    buf.put_i32(30_000);

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::InitProducerId(_, body) => {
            assert!(body.transactional_id.is_none());
            assert_eq!(body.transaction_timeout_ms, 30_000);
        }
        other => panic!("expected InitProducerId, got {:?}", other),
    }
}

#[test]
fn init_producer_id_v4_parses_producer_fields() {
    let mut buf = BytesMut::new();
    buf.put_i16(22);
    buf.put_i16(4);
    buf.put_i32(97);
    buf.put_i16(-1); // null client_id
    buf.put_u8(0); // header tagged fields
    buf.put_u8(0); // null transactional_id (compact)
    buf.put_i32(10_000);
    buf.put_i64(31_337); // producer_id (v3+)
    buf.put_i16(2); // producer_epoch (v3+)
    buf.put_u8(0); // body tagged fields

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::InitProducerId(_, body) => {
            assert_eq!(body.producer_id, 31_337);
            assert_eq!(body.producer_epoch, 2);
        }
        other => panic!("expected InitProducerId, got {:?}", other),
    }
}

// ============================================================================
// Finding 2e: CreateTopics v1 validate_only / ListOffsets v0 max_num_offsets
// ============================================================================

#[test]
fn create_topics_v1_consumes_validate_only() {
    let mut buf = header(19, 1, 98, None);
    buf.put_i32(1); // 1 topic
    put_string(&mut buf, "nt");
    buf.put_i32(3); // partitions
    buf.put_i16(1); // replication
    buf.put_i32(0); // replica assignments
    buf.put_i32(0); // configs
    buf.put_i32(15_000); // timeout_ms
    buf.put_u8(1); // validate_only (v1+) — must be consumed

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::CreateTopics(_, body) => {
            assert_eq!(body.topics[0].name, "nt");
            assert_eq!(body.timeout_ms, 15_000);
        }
        other => panic!("expected CreateTopics, got {:?}", other),
    }
}

#[test]
fn list_offsets_v0_multi_partition_alignment() {
    // v0 partitions carry max_num_offsets; the second partition only parses
    // correctly if the first one's field was consumed.
    let mut buf = header(2, 0, 99, None);
    buf.put_i32(-1); // replica_id
    buf.put_i32(1); // 1 topic
    put_string(&mut buf, "t");
    buf.put_i32(2); // 2 partitions
    buf.put_i32(0);
    buf.put_i64(-1);
    buf.put_i32(5); // max_num_offsets (v0 only)
    buf.put_i32(1);
    buf.put_i64(-2);
    buf.put_i32(1); // max_num_offsets (v0 only)

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::ListOffsets(_, body) => {
            assert_eq!(body.topics[0].partitions.len(), 2);
            assert_eq!(body.topics[0].partitions[1].partition_index, 1);
            assert_eq!(body.topics[0].partitions[1].timestamp, -2);
        }
        other => panic!("expected ListOffsets, got {:?}", other),
    }
}

// ============================================================================
// Finding 3: parse_record_count reads the explicit records_count field
// ============================================================================

#[test]
fn record_count_uses_explicit_field_and_cross_checks() {
    let batch = consistent_batch_with_crc(5);
    assert_eq!(parse_record_count(&batch), 5);
    assert_eq!(parse_record_count_checked(&batch), Ok(5));
}

#[test]
fn record_count_rejects_forged_last_offset_delta() {
    // An attacker setting last_offset_delta to claim a huge count while the
    // explicit records_count disagrees must be rejected, not believed.
    let mut batch = consistent_batch_with_crc(5);
    batch[23..27].copy_from_slice(&999_999i32.to_be_bytes());
    assert_eq!(parse_record_count(&batch), 0);
    assert_eq!(
        parse_record_count_checked(&batch),
        Err(RecordCountError::Mismatch {
            records_count: 5,
            last_offset_delta: 999_999,
        })
    );
}

#[test]
fn record_count_rejects_truncated_batches() {
    // Shorter than the 61-byte v2 header: previously defaulted to 1 record.
    for len in [0usize, 10, 27, 56, 60] {
        let batch = vec![0u8; len];
        assert_eq!(
            parse_record_count(&batch),
            0,
            "{}-byte batch must be invalid (0), not defaulted",
            len
        );
        assert!(matches!(
            parse_record_count_checked(&batch),
            Err(RecordCountError::TooShort { .. })
        ));
    }
}

#[test]
fn record_count_rejects_non_positive_counts() {
    let mut batch = vec![0u8; 61];
    batch[23..27].copy_from_slice(&(-1i32).to_be_bytes()); // delta consistent with 0
    batch[57..61].copy_from_slice(&0i32.to_be_bytes());
    assert_eq!(parse_record_count(&batch), 0);
    assert_eq!(
        parse_record_count_checked(&batch),
        Err(RecordCountError::NonPositive { records_count: 0 })
    );
}

// ============================================================================
// Finding 4: CRC-32C crate correctness + patch_base_offset leaves CRC alone
// ============================================================================

#[test]
fn crc32c_known_test_vectors() {
    // RFC 3720 / well-known CRC-32C (Castagnoli) test vector.
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
    // Empty input.
    assert_eq!(crc32c(b""), 0);
    // 32 zero bytes (another standard Castagnoli vector).
    assert_eq!(crc32c(&[0u8; 32]), 0x8A91_36AA);
    // 32 0xFF bytes.
    assert_eq!(crc32c(&[0xFFu8; 32]), 0x62A8_AB43);
}

#[test]
fn patching_base_offset_preserves_crc_validity() {
    // The v2 batch CRC covers bytes 21+ only; rewriting bytes 0-7 cannot
    // invalidate it, so patch_base_offset must not (and no longer does)
    // recompute anything.
    let mut batch = consistent_batch_with_crc(3);
    let crc_before = batch[17..21].to_vec();
    assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);

    patch_base_offset(&mut batch, 987_654_321).unwrap();

    assert_eq!(
        i64::from_be_bytes(batch[0..8].try_into().unwrap()),
        987_654_321
    );
    assert_eq!(
        &batch[17..21],
        &crc_before[..],
        "CRC bytes must be untouched"
    );
    assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
}

#[test]
fn patching_base_offset_does_not_bless_corrupt_batches() {
    // A batch arriving with a wrong CRC must STILL have a wrong CRC after
    // patching — the old recompute silently "repaired" corruption.
    let mut batch = consistent_batch_with_crc(3);
    batch[17..21].copy_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);
    assert!(matches!(
        validate_batch_crc(&batch),
        CrcValidationResult::Invalid { .. }
    ));

    patch_base_offset(&mut batch, 42).unwrap();

    assert_eq!(&batch[17..21], &[0xDE, 0xAD, 0xBE, 0xEF]);
    assert!(matches!(
        validate_batch_crc(&batch),
        CrcValidationResult::Invalid { .. }
    ));
}

// ============================================================================
// Low-priority protocol/wire fixes
// ============================================================================

#[test]
fn parse_kafka_string_decodes_topic_names_in_one_step() {
    let mut buf = header(3, 1, 200, Some("meta"));
    buf.put_i32(1);
    put_string(&mut buf, "my-topic");

    let request = Request::parse(Bytes::from(buf.to_vec())).unwrap();
    match request {
        Request::Metadata(_, body) => {
            assert_eq!(body.topics, Some(vec!["my-topic".to_string()]));
        }
        other => panic!("expected Metadata, got {:?}", other),
    }
}

#[test]
fn io_error_preserves_underlying_message() {
    let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "broken pipe on read");
    let err = kafkaesque::error::Error::from(io_err);
    let display = format!("{}", err);
    assert!(display.contains("broken pipe on read"));
}

#[test]
fn trailing_request_bytes_are_counted_not_silently_dropped() {
    // Trailing bytes after a successful body parse are tolerated (real
    // clients like librdkafka with `broker.version.fallback=2.0.0` ship
    // padding after a valid body, and the alternative — a generic 2-byte
    // error response — is malformed for the request's API shape and
    // breaks the client). The metric increment is the operator-visible
    // signal; the request still dispatches with the parsed fields.
    let mut buf = header(3, 1, 201, None);
    buf.put_i32(-1); // all topics
    buf.put_slice(b"leftover-padding");

    let request = Request::parse(Bytes::from(buf.to_vec()))
        .expect("trailing bytes must NOT block dispatch — they are logged + counted");
    match request {
        Request::Metadata(header, body) => {
            assert_eq!(header.api_version, 1);
            assert_eq!(header.correlation_id, 201);
            assert_eq!(body.topics, None, "list-all metadata request");
        }
        other => panic!("expected Metadata after tolerated trailing bytes, got {other:?}"),
    }
}

// ============================================================================
// Finding 6: ParsingError payload bounded
// ============================================================================

#[test]
fn parsing_error_carries_bounded_prefix() {
    // 2 MiB of mostly-garbage with a valid header and an unparseable body.
    let mut buf = header(3, 1, 1, None);
    buf.put_i32(50); // claims 50 topics
    buf.resize(2 * 1024 * 1024, 0xAA); // 0xAAAA is a negative string length
    let err = Request::parse(Bytes::from(buf.to_vec())).unwrap_err();
    match err {
        kafkaesque::error::Error::ParsingError(prefix) => {
            assert!(prefix.len() <= 256, "got {} bytes", prefix.len());
        }
        other => panic!("expected ParsingError, got {:?}", other),
    }
}
