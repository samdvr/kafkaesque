//! Property-based tests targeting the highest-value parsers on the broker
//! attack surface.
//!
//! `proptest!` is preferable to libFuzzer alone because (a) it provides
//! shrinking on stable Rust, surfacing minimal repros without nightly +
//! corpus, and (b) it runs in normal `cargo test`, so a regression in any
//! parser branch fails CI immediately rather than waiting on the nightly
//! fuzz job. Each block below mirrors a `fuzz/fuzz_targets/*.rs` file but
//! adds shrinking and tests stronger structural properties on inputs we
//! know the wire decoder must accept (positive-set generators, not just
//! arbitrary bytes).
//!
//! Coverage:
//!   - `parse_record_count_checked` (record batch header parser)
//!   - `validate_batch_crc`
//!   - `parse_produce_request` / `parse_fetch_request` (the two hottest
//!     request bodies)
//!   - `read_kafka_frame_for_fuzz` (connection frame reader; the broker's
//!     first contact with attacker bytes)
//!   - `find_version` / `is_version_supported` (version negotiation)
//!   - postcard codec for `CoordinationCommand` (Raft log entries +
//!     snapshots are stored in this format)

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use nombytes::NomBytes;
use proptest::prelude::*;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::runtime::Builder;

use kafkaesque::protocol::{
    CrcValidationResult, RecordCountError, parse_record_count_checked, validate_batch_crc,
};
use kafkaesque::server::read_kafka_frame_for_fuzz;
use kafkaesque::server::request::{
    ApiKey, parse_fetch_request, parse_produce_request, parse_sasl_authenticate_request,
};
use kafkaesque::server::versions::{find_version, is_version_supported};

const FRAME_CAP: usize = 256 * 1024;
const RAW_BATCH_LEN: usize = 200;

fn put_kafka_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

fn put_kafka_string_opt(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(v) => put_kafka_string(buf, v),
        None => buf.put_i16(-1),
    }
}

fn build_well_formed_batch(record_count: i32, last_offset_delta: i32) -> Vec<u8> {
    // Lay out a v2 RecordBatch header. Field offsets are taken from the
    // protocol crate's constants; CRC isn't computed here because the
    // record-count parser doesn't validate it.
    let mut batch = vec![0u8; RAW_BATCH_LEN];
    batch[0..8].copy_from_slice(&0i64.to_be_bytes()); // base_offset
    batch[8..12].copy_from_slice(&((RAW_BATCH_LEN - 12) as i32).to_be_bytes()); // batch_length
    batch[12..16].copy_from_slice(&0i32.to_be_bytes()); // partition_leader_epoch
    batch[16] = 2; // magic
    batch[17..21].copy_from_slice(&0u32.to_be_bytes()); // crc (unchecked here)
    batch[21..23].copy_from_slice(&0i16.to_be_bytes()); // attributes
    batch[23..27].copy_from_slice(&last_offset_delta.to_be_bytes());
    batch[27..35].copy_from_slice(&0i64.to_be_bytes()); // base_timestamp
    batch[35..43].copy_from_slice(&0i64.to_be_bytes()); // max_timestamp
    batch[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id
    batch[51..53].copy_from_slice(&0i16.to_be_bytes()); // producer_epoch
    batch[53..57].copy_from_slice(&0i32.to_be_bytes()); // base_sequence
    batch[57..61].copy_from_slice(&record_count.to_be_bytes());
    batch
}

fn crc32c_record_batch(batch: &[u8]) -> u32 {
    // Constant table generated at runtime — duplicates the production
    // table on purpose so a bug in the production table can't mask a
    // bug in the production CRC routine.
    let mut table = [0u32; 256];
    for (i, slot) in table.iter_mut().enumerate() {
        let mut crc = i as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0x82F63B78
            } else {
                crc >> 1
            };
        }
        *slot = crc;
    }
    let mut crc = !0u32;
    for &b in batch {
        crc = (crc >> 8) ^ table[((crc ^ b as u32) & 0xFF) as usize];
    }
    !crc
}

// ===========================================================================
// parse_record_count_checked
// ===========================================================================

proptest! {
    #[test]
    fn parse_record_count_checked_never_panics(data in proptest::collection::vec(any::<u8>(), 0..512)) {
        // Property: any byte string accepted as input must produce an Ok or
        // a typed RecordCountError, never panic. This mirrors the libFuzzer
        // target but adds shrinking.
        let _ = parse_record_count_checked(&data);
    }

    #[test]
    fn parse_record_count_checked_accepts_well_formed_batch(
        record_count in 1i32..1_000_000,
    ) {
        let batch = build_well_formed_batch(record_count, record_count - 1);
        let parsed = parse_record_count_checked(&batch).expect("well-formed batch must parse");
        prop_assert_eq!(parsed, record_count);
    }

    #[test]
    fn parse_record_count_checked_rejects_mismatch(
        record_count in 1i32..1_000_000,
        delta_offset in 1i32..1_000,
    ) {
        // last_offset_delta must equal record_count - 1; any delta-from-that
        // produces RecordCountError::Mismatch.
        let bogus_delta = (record_count - 1).wrapping_add(delta_offset);
        let batch = build_well_formed_batch(record_count, bogus_delta);
        let err = parse_record_count_checked(&batch).expect_err("mismatch must error");
        prop_assert!(
            matches!(err, RecordCountError::Mismatch { .. }),
            "expected Mismatch, got {err:?}"
        );
    }

    #[test]
    fn parse_record_count_checked_rejects_non_positive(record_count in i32::MIN..=0) {
        let batch = build_well_formed_batch(record_count, record_count.saturating_sub(1));
        let err = parse_record_count_checked(&batch).expect_err("non-positive must error");
        prop_assert!(matches!(err, RecordCountError::NonPositive { .. }), "expected NonPositive");
    }

    #[test]
    fn parse_record_count_checked_rejects_too_short(short_len in 0usize..61) {
        let batch = vec![0u8; short_len];
        let err = parse_record_count_checked(&batch).expect_err("short must error");
        prop_assert!(matches!(err, RecordCountError::TooShort { .. }), "expected TooShort");
    }
}

// ===========================================================================
// validate_batch_crc
// ===========================================================================

proptest! {
    #[test]
    fn validate_batch_crc_never_panics(data in proptest::collection::vec(any::<u8>(), 0..512)) {
        let _ = validate_batch_crc(&data);
    }

    #[test]
    fn validate_batch_crc_accepts_well_computed_crc(record_count in 1i32..1_000) {
        let mut batch = build_well_formed_batch(record_count, record_count - 1);
        let crc = crc32c_record_batch(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
        let result = validate_batch_crc(&batch);
        prop_assert_eq!(result, CrcValidationResult::Valid);
    }

    #[test]
    fn validate_batch_crc_rejects_corrupted_crc(
        record_count in 1i32..1_000,
        flip_byte in 17usize..21,
    ) {
        let mut batch = build_well_formed_batch(record_count, record_count - 1);
        let crc = crc32c_record_batch(&batch[21..]);
        batch[17..21].copy_from_slice(&crc.to_be_bytes());
        // Flip a single bit inside the stored CRC field — must reject.
        batch[flip_byte] ^= 0xFF;
        let result = validate_batch_crc(&batch);
        prop_assert!(
            matches!(result, CrcValidationResult::Invalid { .. }),
            "expected Invalid, got {result:?}"
        );
    }

    #[test]
    fn validate_batch_crc_rejects_short_input(len in 0usize..21) {
        let batch = vec![0u8; len];
        let result = validate_batch_crc(&batch);
        prop_assert!(
            matches!(result, CrcValidationResult::TooSmall | CrcValidationResult::FrameMismatch { .. }),
            "expected TooSmall/FrameMismatch on short input, got {result:?}",
        );
    }
}

// ===========================================================================
// parse_produce_request / parse_fetch_request
// ===========================================================================

fn build_produce_v3(
    transactional: Option<&str>,
    acks: i16,
    timeout_ms: i32,
    topics: &[&str],
) -> Bytes {
    let mut buf = BytesMut::new();
    put_kafka_string_opt(&mut buf, transactional);
    buf.put_i16(acks);
    buf.put_i32(timeout_ms);
    buf.put_i32(topics.len() as i32);
    for name in topics {
        put_kafka_string(&mut buf, name);
        buf.put_i32(1); // one partition each
        buf.put_i32(0); // partition_index
        buf.put_i32(-1); // null records
    }
    buf.freeze()
}

fn build_fetch_v4(replica_id: i32, max_wait_ms: i32, topics: &[&str]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i32(replica_id);
    buf.put_i32(max_wait_ms);
    buf.put_i32(0);
    buf.put_i32(1_048_576);
    buf.put_i8(0);
    buf.put_i32(topics.len() as i32);
    for name in topics {
        put_kafka_string(&mut buf, name);
        buf.put_i32(1);
        buf.put_i32(0);
        buf.put_i64(0);
        buf.put_i32(65_536);
    }
    buf.freeze()
}

proptest! {
    #[test]
    fn parse_produce_request_never_panics(data in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let _ = parse_produce_request(NomBytes::new(Bytes::from(data)), 3);
    }

    #[test]
    fn parse_produce_request_accepts_constructed_inputs(
        acks in -1i16..=1,
        timeout_ms in 0i32..60_000,
        topics in proptest::collection::vec("[a-z][a-z0-9-]{0,31}", 0..8),
    ) {
        let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        let bytes = build_produce_v3(None, acks, timeout_ms, &topic_refs);
        let (rest, req) =
            parse_produce_request(NomBytes::new(bytes), 3).expect("constructed input must parse");
        prop_assert!(rest.to_bytes().is_empty(), "constructed input must consume all bytes");
        prop_assert_eq!(req.acks, acks);
        prop_assert_eq!(req.timeout_ms, timeout_ms);
        prop_assert_eq!(req.topics.len(), topics.len());
    }

    #[test]
    fn parse_produce_request_rejects_truncated(prefix_len in 0usize..6) {
        let bytes = Bytes::from(vec![0u8; prefix_len]);
        let result = parse_produce_request(NomBytes::new(bytes), 3);
        prop_assert!(result.is_err(), "truncated input must error, not panic");
    }

    #[test]
    fn parse_fetch_request_never_panics(data in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let _ = parse_fetch_request(NomBytes::new(Bytes::from(data)), 4);
    }

    #[test]
    fn parse_fetch_request_accepts_constructed_inputs(
        replica_id in -1i32..1024,
        max_wait_ms in 0i32..30_000,
        topics in proptest::collection::vec("[a-z][a-z0-9-]{0,31}", 0..8),
    ) {
        let topic_refs: Vec<&str> = topics.iter().map(String::as_str).collect();
        let bytes = build_fetch_v4(replica_id, max_wait_ms, &topic_refs);
        let (rest, req) =
            parse_fetch_request(NomBytes::new(bytes), 4).expect("constructed input must parse");
        prop_assert!(rest.to_bytes().is_empty());
        prop_assert_eq!(req.replica_id, replica_id);
        prop_assert_eq!(req.max_wait_ms, max_wait_ms);
        prop_assert_eq!(req.topics.len(), topics.len());
    }

    #[test]
    fn parse_fetch_request_v0_does_not_consume_max_bytes_or_iso(
        topics in proptest::collection::vec("[a-z][a-z0-9-]{0,15}", 0..4),
    ) {
        // For v0..=2 the parser must skip the max_bytes and isolation_level
        // reads. A version-blind regression that always reads them would
        // either error (consume too much) or leave bytes unconsumed (consume
        // too little).
        let mut buf = BytesMut::new();
        buf.put_i32(-1);
        buf.put_i32(0);
        buf.put_i32(0);
        buf.put_i32(topics.len() as i32);
        for name in &topics {
            put_kafka_string(&mut buf, name);
            buf.put_i32(1);
            buf.put_i32(0);
            buf.put_i64(0);
            buf.put_i32(0);
        }
        let bytes = buf.freeze();
        let (rest, _req) =
            parse_fetch_request(NomBytes::new(bytes), 0).expect("v0 must parse");
        prop_assert!(rest.to_bytes().is_empty(), "v0 parser must consume exactly the v0 layout");
    }

    #[test]
    fn parse_sasl_authenticate_never_panics(data in proptest::collection::vec(any::<u8>(), 0..2048)) {
        let _ = parse_sasl_authenticate_request(NomBytes::new(Bytes::from(data)), 0);
    }
}

// ===========================================================================
// find_version / is_version_supported
// ===========================================================================

fn arb_api_key() -> impl Strategy<Value = ApiKey> {
    prop_oneof![
        Just(ApiKey::Produce),
        Just(ApiKey::Fetch),
        Just(ApiKey::ListOffsets),
        Just(ApiKey::Metadata),
        Just(ApiKey::OffsetCommit),
        Just(ApiKey::OffsetFetch),
        Just(ApiKey::FindCoordinator),
        Just(ApiKey::JoinGroup),
        Just(ApiKey::Heartbeat),
        Just(ApiKey::LeaveGroup),
        Just(ApiKey::SyncGroup),
        Just(ApiKey::DescribeGroups),
        Just(ApiKey::ListGroups),
        Just(ApiKey::SaslHandshake),
        Just(ApiKey::ApiVersions),
        Just(ApiKey::CreateTopics),
        Just(ApiKey::DeleteTopics),
        Just(ApiKey::SaslAuthenticate),
        Just(ApiKey::InitProducerId),
        Just(ApiKey::DeleteGroups),
        // Unknown keys must be handled by find_version returning None.
        (i16::MIN..i16::MAX).prop_map(ApiKey::Unknown),
    ]
}

proptest! {
    #[test]
    fn find_version_agrees_with_is_version_supported(
        api_key in arb_api_key(),
        version in -10i16..200i16,
    ) {
        let supported = is_version_supported(api_key, version);
        let info = find_version(api_key);
        let derived = info.map(|v| v.supports(version)).unwrap_or(false);
        prop_assert_eq!(
            supported, derived,
            "is_version_supported must equal find_version().supports() for ({:?}, {})",
            api_key, version,
        );
    }

    #[test]
    fn find_version_returns_none_for_unknown_keys(n in i16::MIN..i16::MAX) {
        // ApiKey::Unknown(n) is never in SUPPORTED_VERSIONS.
        let info = find_version(ApiKey::Unknown(n));
        prop_assert!(info.is_none());
    }
}

// ===========================================================================
// read_kafka_frame_for_fuzz
// ===========================================================================

struct ChunkedReader<'a> {
    data: &'a [u8],
    pos: usize,
    chunk_idx: usize,
    chunk_sizes: &'a [u8],
}

impl<'a> AsyncRead for ChunkedReader<'a> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        if self.pos >= self.data.len() {
            return Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "test EOF",
            )));
        }
        let chunk = if self.chunk_sizes.is_empty() {
            16
        } else {
            let idx = self.chunk_idx % self.chunk_sizes.len();
            self.chunk_idx = self.chunk_idx.wrapping_add(1);
            (self.chunk_sizes[idx] as usize % 32) + 1
        };
        let remaining = self.data.len() - self.pos;
        let to_read = chunk.min(remaining).min(buf.remaining());
        buf.put_slice(&self.data[self.pos..self.pos + to_read]);
        self.pos += to_read;
        Poll::Ready(Ok(()))
    }
}

proptest! {
    #[test]
    fn frame_reader_never_panics_or_overallocates(
        wire in proptest::collection::vec(any::<u8>(), 0..8192),
        chunks in proptest::collection::vec(any::<u8>(), 0..16),
    ) {
        let rt = Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("build runtime");

        rt.block_on(async {
            let mut reader = ChunkedReader { data: &wire, pos: 0, chunk_idx: 0, chunk_sizes: &chunks };
            // any error is fine; absence of panic is the property
            if let Ok(body) = read_kafka_frame_for_fuzz(&mut reader, FRAME_CAP).await {
                // On Ok the wire must have a valid 4-byte size prefix and
                // body length must equal the declared size.
                assert!(wire.len() >= 4);
                let declared = i32::from_be_bytes([wire[0], wire[1], wire[2], wire[3]]);
                assert!(declared >= 0);
                assert_eq!(body.len(), declared as usize);
                assert!(body.len() <= FRAME_CAP);
            }
        });
    }

    #[test]
    fn frame_reader_rejects_size_above_cap(extra in 1usize..1024) {
        // Construct a valid 4-byte prefix declaring (FRAME_CAP + extra), then
        // some body bytes. The reader must error before allocating.
        let declared = FRAME_CAP.saturating_add(extra) as i32;
        let mut wire = Vec::with_capacity(8);
        wire.extend_from_slice(&declared.to_be_bytes());
        wire.extend_from_slice(&[0u8; 4]);

        let rt = Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("build runtime");
        rt.block_on(async {
            let mut reader = ChunkedReader { data: &wire, pos: 0, chunk_idx: 0, chunk_sizes: &[] };
            let result = read_kafka_frame_for_fuzz(&mut reader, FRAME_CAP).await;
            assert!(result.is_err(), "size > cap must error pre-allocation");
        });
    }

    #[test]
    fn frame_reader_rejects_negative_size(declared in i32::MIN..0) {
        let mut wire = Vec::with_capacity(4);
        wire.extend_from_slice(&declared.to_be_bytes());

        let rt = Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("build runtime");
        rt.block_on(async {
            let mut reader = ChunkedReader { data: &wire, pos: 0, chunk_idx: 0, chunk_sizes: &[] };
            let result = read_kafka_frame_for_fuzz(&mut reader, FRAME_CAP).await;
            assert!(result.is_err(), "negative size must be rejected");
        });
    }
}

// ===========================================================================
// postcard codec for ControlCommand / ShardCommand (Raft log entries + snapshots)
// ===========================================================================

mod postcard_props {
    use super::*;
    use kafkaesque::cluster::raft::domains::PartitionStateCommand;
    use kafkaesque::cluster::raft::{BrokerCommand, ControlCommand, ShardCommand};

    proptest! {
        #[test]
        fn control_command_postcard_decode_never_panics(
            data in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            // Property: arbitrary bytes feeding postcard::from_bytes must
            // never panic. A panic here takes down the broker on restart
            // (decoding a corrupt log entry / snapshot pointer).
            let _ = postcard::from_bytes::<ControlCommand>(&data);
        }

        #[test]
        fn shard_command_postcard_decode_never_panics(
            data in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            let _ = postcard::from_bytes::<ShardCommand>(&data);
        }

        #[test]
        fn control_command_postcard_canonical_roundtrip(
            data in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            // If a byte string decodes, re-encoding it must produce a
            // string that re-decodes to the same value (canonicalisation).
            if let Ok(decoded) = postcard::from_bytes::<ControlCommand>(&data) {
                let reencoded = postcard::to_stdvec(&decoded)
                    .expect("decoded value must always re-encode");
                let again: ControlCommand =
                    postcard::from_bytes(&reencoded)
                        .expect("canonical form must decode");
                prop_assert_eq!(decoded, again);
            }
        }

        #[test]
        fn shard_command_postcard_canonical_roundtrip(
            data in proptest::collection::vec(any::<u8>(), 0..1024),
        ) {
            if let Ok(decoded) = postcard::from_bytes::<ShardCommand>(&data) {
                let reencoded = postcard::to_stdvec(&decoded)
                    .expect("decoded value must always re-encode");
                let again: ShardCommand =
                    postcard::from_bytes(&reencoded)
                        .expect("canonical form must decode");
                prop_assert_eq!(decoded, again);
            }
        }
    }

    // Smoke roundtrips on hand-built representative variants — the
    // proptests above hammer the decode path with random bytes; these
    // pin the canonical-encode shape of each variant we actually emit.
    #[test]
    fn control_command_register_broker_roundtrip() {
        let cmd = ControlCommand::Broker(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });
        let bytes = postcard::to_stdvec(&cmd).unwrap();
        let back: ControlCommand = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, back);
    }

    #[test]
    fn shard_command_init_partition_roundtrip() {
        let cmd = ShardCommand::Partition(PartitionStateCommand::InitPartition {
            topic: "t".to_string(),
            partition: 3,
            created_at_ms: 1000,
        });
        let bytes = postcard::to_stdvec(&cmd).unwrap();
        let back: ShardCommand = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(cmd, back);
    }
}
