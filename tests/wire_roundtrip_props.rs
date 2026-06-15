//! Roundtrip property tests for Kafka request parsers.
//!
//! For each of a representative set of request APIs, we generate an
//! `Arbitrary` instance, encode it to bytes via a hand-rolled encoder, then
//! parse it back through the production parser and assert structural
//! equality. The hand-rolled encoder is intentionally a separate
//! implementation from the parser — the test thus doubles as a
//! differential check against an in-tree reference (full cross-implementation
//! differential lives in `wire_differential.rs`).
//!
//! Coverage:
//!   - Heartbeat (v0–v1, no version-specific layout difference here)
//!   - LeaveGroup
//!   - FindCoordinator (v0 vs v1 — v1 adds key_type)
//!   - SaslHandshake / SaslAuthenticate
//!   - DescribeGroups, ListGroups (v0–v2), DeleteGroups
//!   - Metadata (v0 vs v1 — v1 allows null array)
//!   - ApiVersions (v0/v2 classic vs v3 flexible)
//!   - InitProducerId (v0–v1 classic; v2 flexible; v3+ adds producer_id/epoch)
//!
//! Skipped (deferred to follow-up):
//!   - Produce / Fetch — bodies embed record batches, which need a
//!     dedicated builder before roundtrip is meaningful.
//!   - JoinGroup / SyncGroup — large nested member-protocol structures.
//!   - OffsetCommit / OffsetFetch / ListOffsets / CreateTopics /
//!     DeleteTopics — same encoder pattern, omitted to keep this file
//!     reviewable; identical structure can be added incrementally.

use bytes::{BufMut, Bytes, BytesMut};
use nombytes::NomBytes;
use proptest::prelude::*;

use kafkaesque::server::request::{
    parse_api_versions_request, parse_delete_groups_request, parse_describe_groups_request,
    parse_find_coordinator_request, parse_heartbeat_request, parse_init_producer_id_request,
    parse_leave_group_request, parse_list_groups_request, parse_metadata_request,
    parse_sasl_authenticate_request, parse_sasl_handshake_request,
};

// ===========================================================================
// Wire-format encoder helpers — NOT a copy of `src/encode.rs`. Hand-rolled
// for the test crate so a parser/encoder bug in production code can't
// silently mask itself in a "roundtrip" that uses the buggy encoder on
// both sides.
// ===========================================================================

fn put_str(buf: &mut BytesMut, s: &str) {
    // STRING: i16 length (non-negative), then bytes.
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

fn put_nullable_str(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(v) => put_str(buf, v),
        None => buf.put_i16(-1),
    }
}

fn put_string_array(buf: &mut BytesMut, items: &[String]) {
    buf.put_i32(items.len() as i32);
    for s in items {
        put_str(buf, s);
    }
}

fn put_unsigned_varint(buf: &mut BytesMut, mut v: u32) {
    loop {
        let mut byte = (v & 0x7F) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        buf.put_u8(byte);
        if v == 0 {
            break;
        }
    }
}

fn put_compact_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        None => put_unsigned_varint(buf, 0),
        Some(v) => {
            put_unsigned_varint(buf, (v.len() + 1) as u32);
            buf.put_slice(v.as_bytes());
        }
    }
}

fn put_empty_tagged_fields(buf: &mut BytesMut) {
    buf.put_u8(0);
}

// ===========================================================================
// Generators — keep strings ASCII printable so UTF-8 validation succeeds and
// we test the post-validation parser, not just the UTF-8 check (which is
// already covered by `parser_primitives` fuzzing).
// ===========================================================================

prop_compose! {
    fn arb_kafka_string()(
        s in "[a-zA-Z0-9._-]{0,128}",
    ) -> String { s }
}

prop_compose! {
    fn arb_kafka_nullable_string()(
        v in proptest::option::of("[a-zA-Z0-9._-]{0,128}"),
    ) -> Option<String> { v }
}

// ===========================================================================
// Heartbeat
// ===========================================================================

proptest! {
    #[test]
    fn heartbeat_roundtrip(
        group_id in arb_kafka_string(),
        generation_id in any::<i32>(),
        member_id in arb_kafka_string(),
    ) {
        let mut buf = BytesMut::new();
        put_str(&mut buf, &group_id);
        buf.put_i32(generation_id);
        put_str(&mut buf, &member_id);

        let bytes: Bytes = buf.freeze();
        let (rest, parsed) = parse_heartbeat_request(NomBytes::new(bytes), 0)
            .expect("heartbeat must parse");
        prop_assert!(rest.into_bytes().is_empty(), "trailing bytes in heartbeat");
        prop_assert_eq!(parsed.group_id, group_id);
        prop_assert_eq!(parsed.generation_id, generation_id);
        prop_assert_eq!(parsed.member_id, member_id);
    }
}

// ===========================================================================
// LeaveGroup
// ===========================================================================

proptest! {
    #[test]
    fn leave_group_roundtrip(
        group_id in arb_kafka_string(),
        member_id in arb_kafka_string(),
    ) {
        let mut buf = BytesMut::new();
        put_str(&mut buf, &group_id);
        put_str(&mut buf, &member_id);

        let (rest, parsed) = parse_leave_group_request(NomBytes::new(buf.freeze()), 0)
            .expect("leave_group must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.group_id, group_id);
        prop_assert_eq!(parsed.member_id, member_id);
    }
}

// ===========================================================================
// FindCoordinator — v0 has only `key`; v1 adds `key_type`.
// ===========================================================================

proptest! {
    #[test]
    fn find_coordinator_v0_roundtrip(key in arb_kafka_string()) {
        let mut buf = BytesMut::new();
        put_str(&mut buf, &key);
        let (rest, parsed) = parse_find_coordinator_request(NomBytes::new(buf.freeze()), 0)
            .expect("find_coordinator v0 must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.key, key);
        prop_assert_eq!(parsed.key_type, 0);
    }

    #[test]
    fn find_coordinator_v1_roundtrip(
        key in arb_kafka_string(),
        key_type in any::<i8>(),
    ) {
        let mut buf = BytesMut::new();
        put_str(&mut buf, &key);
        buf.put_i8(key_type);
        let (rest, parsed) = parse_find_coordinator_request(NomBytes::new(buf.freeze()), 1)
            .expect("find_coordinator v1 must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.key, key);
        prop_assert_eq!(parsed.key_type, key_type);
    }
}

// ===========================================================================
// SASL — Handshake (a single STRING) and Authenticate (BYTES)
// ===========================================================================

proptest! {
    #[test]
    fn sasl_handshake_roundtrip(mechanism in arb_kafka_string()) {
        let mut buf = BytesMut::new();
        put_str(&mut buf, &mechanism);
        let (rest, parsed) = parse_sasl_handshake_request(NomBytes::new(buf.freeze()), 0)
            .expect("sasl_handshake must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.mechanism, mechanism);
    }

    #[test]
    fn sasl_authenticate_roundtrip(auth_bytes in proptest::collection::vec(any::<u8>(), 0..512)) {
        let mut buf = BytesMut::new();
        buf.put_i32(auth_bytes.len() as i32);
        buf.put_slice(&auth_bytes);
        let (rest, parsed) = parse_sasl_authenticate_request(NomBytes::new(buf.freeze()), 0)
            .expect("sasl_authenticate must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.auth_bytes.as_ref(), auth_bytes.as_slice());
    }
}

// ===========================================================================
// DescribeGroups / ListGroups (v0–v2 has no body) / DeleteGroups
// ===========================================================================

proptest! {
    #[test]
    fn describe_groups_roundtrip(
        group_ids in proptest::collection::vec(arb_kafka_string(), 0..16),
    ) {
        let mut buf = BytesMut::new();
        put_string_array(&mut buf, &group_ids);
        let (rest, parsed) = parse_describe_groups_request(NomBytes::new(buf.freeze()), 0)
            .expect("describe_groups must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.group_ids, group_ids);
    }

    #[test]
    fn list_groups_v0_v2_has_empty_body(version in 0i16..=2i16) {
        let buf = BytesMut::new();
        let (rest, parsed) = parse_list_groups_request(NomBytes::new(buf.freeze()), version)
            .expect("list_groups v0–v2 must parse with no body");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert!(parsed.states_filter.is_empty());
    }

    #[test]
    fn delete_groups_roundtrip(
        group_ids in proptest::collection::vec(arb_kafka_string(), 0..16),
    ) {
        let mut buf = BytesMut::new();
        put_string_array(&mut buf, &group_ids);
        let (rest, parsed) = parse_delete_groups_request(NomBytes::new(buf.freeze()), 0)
            .expect("delete_groups must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.group_ids, group_ids);
    }
}

// ===========================================================================
// Metadata — v0 forbids null-array, v1 allows it.
// ===========================================================================

proptest! {
    #[test]
    fn metadata_v0_roundtrip(
        topics in proptest::collection::vec(arb_kafka_string(), 0..8),
    ) {
        let mut buf = BytesMut::new();
        put_string_array(&mut buf, &topics);
        let (rest, parsed) = parse_metadata_request(NomBytes::new(buf.freeze()), 0)
            .expect("metadata v0 must parse");
        prop_assert!(rest.into_bytes().is_empty());
        // v0: a present (possibly empty) topics array survives as Some(_).
        let parsed_topics = parsed.topics.expect("v0 always carries Some(topics)");
        prop_assert_eq!(parsed_topics, topics);
    }

    #[test]
    fn metadata_v1_specific_topics_roundtrip(
        topics in proptest::collection::vec(arb_kafka_string(), 1..8),
    ) {
        let mut buf = BytesMut::new();
        put_string_array(&mut buf, &topics);
        let (rest, parsed) = parse_metadata_request(NomBytes::new(buf.freeze()), 1)
            .expect("metadata v1 specific must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.topics.unwrap(), topics);
    }

    #[test]
    fn metadata_v1_null_means_all_topics(_seed in any::<u8>()) {
        // -1 array means "all topics" in v1+.
        let mut buf = BytesMut::new();
        buf.put_i32(-1);
        let (rest, parsed) = parse_metadata_request(NomBytes::new(buf.freeze()), 1)
            .expect("metadata v1 null must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert!(parsed.topics.is_none());
    }
}

// ===========================================================================
// ApiVersions — v0/v1/v2 have empty bodies; v3 is flexible (compact strings
// + tagged fields). Header parsing for v3 lives in `parse_request_header`,
// so the body parser sees just the body.
// ===========================================================================

proptest! {
    #[test]
    fn api_versions_v0_v2_empty_body(version in 0i16..=2i16) {
        let buf = BytesMut::new();
        let (rest, parsed) = parse_api_versions_request(NomBytes::new(buf.freeze()), version)
            .expect("api_versions v0–v2 must parse with empty body");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert!(parsed.client_software_name.is_none());
        prop_assert!(parsed.client_software_version.is_none());
    }

    #[test]
    fn api_versions_v3_flexible_roundtrip(
        name in arb_kafka_nullable_string(),
        version in arb_kafka_nullable_string(),
    ) {
        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, name.as_deref());
        put_compact_nullable_string(&mut buf, version.as_deref());
        put_empty_tagged_fields(&mut buf);
        let (rest, parsed) = parse_api_versions_request(NomBytes::new(buf.freeze()), 3)
            .expect("api_versions v3 must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.client_software_name, name);
        prop_assert_eq!(parsed.client_software_version, version);
    }
}

// ===========================================================================
// InitProducerId — three layout regimes:
//   v0–v1: classic NULLABLE_STRING + INT32
//   v2: flexible (COMPACT_NULLABLE_STRING + INT32 + tagged fields)
//   v3–v4: same as v2 plus producer_id (INT64) and producer_epoch (INT16)
// ===========================================================================

proptest! {
    #[test]
    fn init_producer_id_classic_roundtrip(
        version in 0i16..=1i16,
        transactional_id in arb_kafka_nullable_string(),
        timeout_ms in any::<i32>(),
    ) {
        let mut buf = BytesMut::new();
        put_nullable_str(&mut buf, transactional_id.as_deref());
        buf.put_i32(timeout_ms);
        let (rest, parsed) = parse_init_producer_id_request(NomBytes::new(buf.freeze()), version)
            .expect("init_producer_id classic must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.transactional_id, transactional_id);
        prop_assert_eq!(parsed.transaction_timeout_ms, timeout_ms);
        // Classic versions don't carry these — parser fills with -1.
        prop_assert_eq!(parsed.producer_id, -1);
        prop_assert_eq!(parsed.producer_epoch, -1);
    }

    #[test]
    fn init_producer_id_v2_flexible_roundtrip(
        transactional_id in arb_kafka_nullable_string(),
        timeout_ms in any::<i32>(),
    ) {
        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, transactional_id.as_deref());
        buf.put_i32(timeout_ms);
        put_empty_tagged_fields(&mut buf);
        let (rest, parsed) = parse_init_producer_id_request(NomBytes::new(buf.freeze()), 2)
            .expect("init_producer_id v2 must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.transactional_id, transactional_id);
        prop_assert_eq!(parsed.transaction_timeout_ms, timeout_ms);
        prop_assert_eq!(parsed.producer_id, -1);
        prop_assert_eq!(parsed.producer_epoch, -1);
    }

    #[test]
    fn init_producer_id_v3_v4_flexible_roundtrip(
        version in 3i16..=4i16,
        transactional_id in arb_kafka_nullable_string(),
        timeout_ms in any::<i32>(),
        producer_id in any::<i64>(),
        producer_epoch in any::<i16>(),
    ) {
        let mut buf = BytesMut::new();
        put_compact_nullable_string(&mut buf, transactional_id.as_deref());
        buf.put_i32(timeout_ms);
        buf.put_i64(producer_id);
        buf.put_i16(producer_epoch);
        put_empty_tagged_fields(&mut buf);
        let (rest, parsed) = parse_init_producer_id_request(NomBytes::new(buf.freeze()), version)
            .expect("init_producer_id v3+ must parse");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.transactional_id, transactional_id);
        prop_assert_eq!(parsed.transaction_timeout_ms, timeout_ms);
        prop_assert_eq!(parsed.producer_id, producer_id);
        prop_assert_eq!(parsed.producer_epoch, producer_epoch);
    }
}

// ===========================================================================
// Cross-version behaviour: a v0 layout fed to a v1 parser must NOT silently
// succeed when the layouts differ. (For Metadata v0/v1 the body shape is
// the same — the only diff is null-array legality — so v0 bytes always
// parse as v1; this property pins that.)
// ===========================================================================

proptest! {
    #[test]
    fn metadata_v0_bytes_parse_under_v1(
        topics in proptest::collection::vec(arb_kafka_string(), 0..4),
    ) {
        let mut buf = BytesMut::new();
        put_string_array(&mut buf, &topics);
        let bytes = buf.freeze();
        let (_, p0) = parse_metadata_request(NomBytes::new(bytes.clone()), 0).unwrap();
        let (_, p1) = parse_metadata_request(NomBytes::new(bytes), 1).unwrap();
        prop_assert_eq!(p0.topics, p1.topics);
    }
}
