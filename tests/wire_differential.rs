//! P1-1: Differential parser test against the [`kafka-protocol`] crate.
//!
//! Generates random structured request payloads via `proptest`, encodes
//! them with the **independent** `kafka-protocol` implementation, and
//! parses the resulting bytes with kafkaesque's parsers. Mismatches are
//! wire-format drift between two independent implementations of the same
//! spec — the highest-confidence signal short of running against a real
//! Kafka broker.
//!
//! Why kafka-protocol vs. rdkafka: rdkafka is a librdkafka binding whose
//! protocol-level encoders are not exposed through the Rust API; only the
//! high-level Producer/Consumer drive the wire. `kafka-protocol` is a
//! pure-Rust encoder/decoder we can call directly on synthetic data.
//!
//! Coverage: a representative subset of APIs whose v0/v1 wire layout
//! kafkaesque advertises. APIs whose body shape diverges between
//! kafka-protocol's earliest version and our parser's latest version
//! (e.g. JoinGroup, OffsetCommit) are deliberately omitted — bridging
//! that divergence belongs in a follow-up rather than here, where a
//! silent layout mismatch would mask a real parser bug.

use bytes::BytesMut;
use kafka_protocol::messages::{
    DescribeGroupsRequest, FindCoordinatorRequest, HeartbeatRequest, LeaveGroupRequest,
    ListGroupsRequest, MetadataRequest, SaslHandshakeRequest,
};
use kafka_protocol::messages::{GroupId, TopicName};
use kafka_protocol::messages::metadata_request::MetadataRequestTopic;
use kafka_protocol::protocol::{Encodable, StrBytes};
use nombytes::NomBytes;
use proptest::prelude::*;

use kafkaesque::server::request::{
    parse_describe_groups_request, parse_find_coordinator_request, parse_heartbeat_request,
    parse_leave_group_request, parse_list_groups_request, parse_metadata_request,
    parse_sasl_handshake_request,
};

/// Encode a kafka-protocol request struct into the bytes layout kafkaesque
/// expects to see *after* the request header has been stripped. The
/// kafkaesque parsers receive only the body.
fn encode_body<T: Encodable>(req: &T, version: i16) -> bytes::Bytes {
    let mut out = BytesMut::new();
    req.encode(&mut out, version)
        .expect("kafka-protocol encode must succeed for well-formed input");
    out.freeze()
}

/// ASCII identifier strategy — matches the strings real Kafka clients send
/// and avoids fighting kafka-protocol's `StrBytes` UTF-8 validation.
fn arb_ident() -> impl Strategy<Value = String> {
    "[a-zA-Z0-9._-]{1,64}"
}

/// Wrap a Rust `&str` in kafka-protocol's `StrBytes` newtype.
fn sb(s: &str) -> StrBytes {
    StrBytes::from_string(s.to_string())
}

// ===========================================================================
// Heartbeat — v0 layout: STRING group_id, INT32 generation_id, STRING member_id.
// ===========================================================================

proptest! {
    #[test]
    fn diff_heartbeat_v0(
        group_id in arb_ident(),
        generation_id in any::<i32>(),
        member_id in arb_ident(),
    ) {
        let mut req = HeartbeatRequest::default();
        req.group_id = GroupId(sb(&group_id));
        req.generation_id = generation_id;
        req.member_id = sb(&member_id);

        let body = encode_body(&req, 0);
        let (rest, parsed) = parse_heartbeat_request(NomBytes::new(body), 0)
            .expect("kafkaesque must parse a kafka-protocol-encoded heartbeat v0");
        prop_assert!(rest.into_bytes().is_empty(), "trailing bytes after body");
        prop_assert_eq!(parsed.group_id, group_id);
        prop_assert_eq!(parsed.generation_id, generation_id);
        prop_assert_eq!(parsed.member_id, member_id);
    }
}

// ===========================================================================
// LeaveGroup v0 — group_id + member_id.
// ===========================================================================

proptest! {
    #[test]
    fn diff_leave_group_v0(
        group_id in arb_ident(),
        member_id in arb_ident(),
    ) {
        let mut req = LeaveGroupRequest::default();
        req.group_id = GroupId(sb(&group_id));
        req.member_id = sb(&member_id);

        let body = encode_body(&req, 0);
        let (rest, parsed) = parse_leave_group_request(NomBytes::new(body), 0)
            .expect("kafkaesque must parse leave_group v0");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.group_id, group_id);
        prop_assert_eq!(parsed.member_id, member_id);
    }
}

// ===========================================================================
// FindCoordinator — v0 has only `key`; v1 adds key_type.
// ===========================================================================

proptest! {
    #[test]
    fn diff_find_coordinator_v0(key in arb_ident()) {
        let mut req = FindCoordinatorRequest::default();
        req.key = sb(&key);

        let body = encode_body(&req, 0);
        let (rest, parsed) = parse_find_coordinator_request(NomBytes::new(body), 0)
            .expect("kafkaesque must parse find_coordinator v0");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.key, key);
        // v0 has no key_type on the wire — parser fills with 0.
        prop_assert_eq!(parsed.key_type, 0);
    }

    #[test]
    fn diff_find_coordinator_v1(
        key in arb_ident(),
        key_type in 0i8..=2i8,
    ) {
        let mut req = FindCoordinatorRequest::default();
        req.key = sb(&key);
        req.key_type = key_type;

        let body = encode_body(&req, 1);
        let (rest, parsed) = parse_find_coordinator_request(NomBytes::new(body), 1)
            .expect("kafkaesque must parse find_coordinator v1");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.key, key);
        prop_assert_eq!(parsed.key_type, key_type);
    }
}

// ===========================================================================
// SaslHandshake v0 — single STRING `mechanism`.
// ===========================================================================

proptest! {
    #[test]
    fn diff_sasl_handshake_v0(mechanism in arb_ident()) {
        let mut req = SaslHandshakeRequest::default();
        req.mechanism = sb(&mechanism);

        let body = encode_body(&req, 0);
        let (rest, parsed) = parse_sasl_handshake_request(NomBytes::new(body), 0)
            .expect("kafkaesque must parse sasl_handshake v0");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.mechanism, mechanism);
    }
}

// ===========================================================================
// Metadata v0 / v1 — array of topic names.
// ===========================================================================

proptest! {
    #[test]
    fn diff_metadata_v0(
        topics in proptest::collection::vec(arb_ident(), 1..6),
    ) {
        let mut req = MetadataRequest::default();
        req.topics = Some(
            topics
                .iter()
                .map(|t| {
                    let mut entry = MetadataRequestTopic::default();
                    entry.name = Some(TopicName(sb(t)));
                    entry
                })
                .collect(),
        );

        let body = encode_body(&req, 0);
        let (rest, parsed) = parse_metadata_request(NomBytes::new(body), 0)
            .expect("kafkaesque must parse metadata v0");
        prop_assert!(rest.into_bytes().is_empty());
        let parsed_topics = parsed.topics.expect("v0 carries Some(_)");
        prop_assert_eq!(parsed_topics, topics);
    }

    #[test]
    fn diff_metadata_v1(
        topics in proptest::collection::vec(arb_ident(), 1..6),
    ) {
        let mut req = MetadataRequest::default();
        req.topics = Some(
            topics
                .iter()
                .map(|t| {
                    let mut entry = MetadataRequestTopic::default();
                    entry.name = Some(TopicName(sb(t)));
                    entry
                })
                .collect(),
        );

        let body = encode_body(&req, 1);
        let (rest, parsed) = parse_metadata_request(NomBytes::new(body), 1)
            .expect("kafkaesque must parse metadata v1");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.topics.unwrap(), topics);
    }
}

// ===========================================================================
// DescribeGroups v0 — array of group IDs.
// ===========================================================================

proptest! {
    #[test]
    fn diff_describe_groups_v0(
        group_ids in proptest::collection::vec(arb_ident(), 0..6),
    ) {
        let mut req = DescribeGroupsRequest::default();
        req.groups = group_ids.iter().map(|g| GroupId(sb(g))).collect();

        let body = encode_body(&req, 0);
        let (rest, parsed) = parse_describe_groups_request(NomBytes::new(body), 0)
            .expect("kafkaesque must parse describe_groups v0");
        prop_assert!(rest.into_bytes().is_empty());
        prop_assert_eq!(parsed.group_ids, group_ids);
    }
}

// ===========================================================================
// ListGroups v0 — empty body.
// ===========================================================================

#[test]
fn diff_list_groups_v0_empty_body() {
    let req = ListGroupsRequest::default();
    let body = encode_body(&req, 0);
    let (rest, parsed) =
        parse_list_groups_request(NomBytes::new(body), 0).expect("list_groups v0 must parse");
    assert!(rest.into_bytes().is_empty());
    assert!(parsed.states_filter.is_empty());
}

// ===========================================================================
// Full-frame smoke — kafkaesque's `Request::parse` reads the request header
// itself, so we encode a full frame (header + body) by combining a
// hand-rolled header with a kafka-protocol body and confirm the dispatcher
// routes correctly. One representative API is enough; the per-API
// roundtrips above carry the body-level coverage.
// ===========================================================================

#[test]
fn diff_full_frame_dispatches_to_correct_api() {
    use bytes::BufMut;

    let mut frame = BytesMut::new();
    frame.put_i16(12); // Heartbeat api_key
    frame.put_i16(0); // version
    frame.put_i32(42); // correlation_id
    frame.put_i16(-1); // null client_id

    let mut hb = HeartbeatRequest::default();
    hb.group_id = GroupId(sb("g"));
    hb.generation_id = 5;
    hb.member_id = sb("m");
    hb.encode(&mut frame, 0).unwrap();

    let req = kafkaesque::server::request::Request::parse(frame.freeze())
        .expect("Request::parse must accept a kafka-protocol-encoded frame");
    match req {
        kafkaesque::server::request::Request::Heartbeat(header, body) => {
            assert_eq!(i16::from(header.api_key), 12);
            assert_eq!(header.api_version, 0);
            assert_eq!(header.correlation_id, 42);
            assert_eq!(body.group_id, "g");
            assert_eq!(body.generation_id, 5);
            assert_eq!(body.member_id, "m");
        }
        other => panic!("expected Heartbeat, got {other:?}"),
    }
}
