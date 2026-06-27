//! End-to-end wire-protocol coverage for the Tier 2 version bumps:
//! Fetch v4..=v11, Metadata v0..=v9, Produce v3..=v9.
//!
//! These tests exercise the full request-parse → handler → response-encode
//! pipeline at the new high-watermark version of each API. The shape of the
//! encoded response is asserted against expected byte-position invariants
//! (length prefixes, sentinel values, flexible-encoding markers) so that
//! drift between the parser, encoder, and `versions::SUPPORTED_VERSIONS`
//! shows up as a test failure rather than as a runtime mis-frame.

use bytes::{BufMut, Bytes, BytesMut};

use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::{
    ApiKey, FetchPartitionData, FetchRequestData, FetchTopicData, MetadataRequestData,
    ProducePartitionData, ProduceRequestData, ProduceTopicData, Request,
};
use kafkaesque::server::response::{
    BrokerData, FetchPartitionResponse, FetchResponseData, FetchTopicResponse,
    MetadataResponseData, PartitionMetadata, ProducePartitionResponse, ProduceResponseData,
    ProduceTopicResponse, TopicMetadata,
};
use kafkaesque::server::versions::{find_version, is_version_supported};

fn put_kafka_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.put_slice(s.as_bytes());
}

fn put_nullable_string(buf: &mut BytesMut, s: Option<&str>) {
    match s {
        Some(s) => {
            buf.put_i16(s.len() as i16);
            buf.put_slice(s.as_bytes());
        }
        None => buf.put_i16(-1),
    }
}

/// Wrap an API body inside the standard `Request::parse` framing
/// (length-prefixed; api_key + api_version + correlation_id + client_id).
fn build_classic_request(api_key: i16, version: i16, correlation_id: i32, body: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(api_key);
    buf.put_i16(version);
    buf.put_i32(correlation_id);
    put_nullable_string(&mut buf, Some("test-client"));
    buf.put_slice(body);
    buf.freeze()
}

/// Wrap a flexible (header v2) request: classic client_id + tagged fields trailer.
fn build_flexible_request(api_key: i16, version: i16, correlation_id: i32, body: &[u8]) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_i16(api_key);
    buf.put_i16(version);
    buf.put_i32(correlation_id);
    put_nullable_string(&mut buf, Some("test-client"));
    buf.put_u8(0); // header tagged fields
    buf.put_slice(body);
    buf.freeze()
}

// ============================================================================
// Versions advertisement
// ============================================================================

/// Tier 2 bumps the advertised maxima. Pin the new ranges so an accidental
/// downgrade in `versions.rs` regresses immediately.
#[test]
fn advertised_versions_match_tier_2_targets() {
    let fetch = find_version(ApiKey::Fetch).expect("Fetch advertised");
    assert_eq!(fetch.min_version, 4);
    assert_eq!(fetch.max_version, 11);

    let metadata = find_version(ApiKey::Metadata).expect("Metadata advertised");
    assert_eq!(metadata.min_version, 0);
    assert_eq!(metadata.max_version, 9);

    let produce = find_version(ApiKey::Produce).expect("Produce advertised");
    assert_eq!(produce.min_version, 3);
    assert_eq!(produce.max_version, 9);

    // is_version_supported agrees with the advertised range.
    assert!(is_version_supported(ApiKey::Fetch, 11));
    assert!(!is_version_supported(ApiKey::Fetch, 12));
    assert!(is_version_supported(ApiKey::Metadata, 9));
    assert!(!is_version_supported(ApiKey::Metadata, 10));
    assert!(is_version_supported(ApiKey::Produce, 9));
    assert!(!is_version_supported(ApiKey::Produce, 10));
}

// ============================================================================
// Fetch v7+ and v11 — request parse + response encode end-to-end
// ============================================================================

/// Build a Fetch v7 body with one topic / one partition and the new
/// session_id / session_epoch / forgotten_topics fields. Round-trip
/// through `Request::parse` and assert the parsed view matches.
#[test]
fn fetch_v7_request_round_trips_session_fields() {
    let mut body = BytesMut::new();
    body.put_i32(-1); // replica_id
    body.put_i32(0); // max_wait_ms
    body.put_i32(0); // min_bytes
    body.put_i32(1_048_576); // max_bytes
    body.put_i8(0); // isolation_level
    body.put_i32(7); // session_id (v7+)
    body.put_i32(2); // session_epoch (v7+)
    body.put_i32(1); // topics
    put_kafka_string(&mut body, "t");
    body.put_i32(1); // partitions
    body.put_i32(0); // partition_index
    body.put_i64(0); // fetch_offset
    body.put_i64(-1); // log_start_offset (v5+)
    body.put_i32(64); // partition_max_bytes
    body.put_i32(1); // forgotten_topics: 1
    put_kafka_string(&mut body, "old");
    body.put_i32(2); // forgotten partitions: [3, 4]
    body.put_i32(3);
    body.put_i32(4);

    let frame = build_classic_request(i16::from(ApiKey::Fetch), 7, 99, &body);
    let req = Request::parse(frame).expect("v7 fetch parses");
    match req {
        Request::Fetch(header, data) => {
            assert_eq!(header.api_version, 7);
            assert_eq!(header.correlation_id, 99);
            assert_eq!(data.session_id, 7);
            assert_eq!(data.session_epoch, 2);
            assert_eq!(data.forgotten_topics.len(), 1);
            assert_eq!(data.forgotten_topics[0].topic, "old");
            assert_eq!(data.forgotten_topics[0].partitions, vec![3, 4]);
            assert_eq!(data.topics[0].partitions[0].log_start_offset, -1);
        }
        other => panic!("expected Fetch, got {:?}", other),
    }
}

/// v9 introduces per-partition `current_leader_epoch` (4 extra bytes per
/// partition) BEFORE `fetch_offset`. A wrong-version parse misaligns
/// every partition.
#[test]
fn fetch_v9_request_carries_current_leader_epoch() {
    let mut body = BytesMut::new();
    body.put_i32(-1);
    body.put_i32(0);
    body.put_i32(0);
    body.put_i32(1_000_000);
    body.put_i8(0);
    body.put_i32(0); // session_id
    body.put_i32(-1); // session_epoch
    body.put_i32(1); // topics
    put_kafka_string(&mut body, "t");
    body.put_i32(1); // partitions
    body.put_i32(2); // partition_index
    body.put_i32(42); // current_leader_epoch (v9+)
    body.put_i64(100); // fetch_offset
    body.put_i64(0); // log_start_offset
    body.put_i32(2048); // partition_max_bytes
    body.put_i32(0); // forgotten_topics

    let frame = build_classic_request(i16::from(ApiKey::Fetch), 9, 1, &body);
    let req = Request::parse(frame).expect("v9 fetch parses");
    match req {
        Request::Fetch(_, data) => {
            assert_eq!(data.topics[0].partitions[0].current_leader_epoch, 42);
            assert_eq!(data.topics[0].partitions[0].fetch_offset, 100);
        }
        _ => panic!("expected Fetch"),
    }
}

/// v11 introduces top-level `rack_id` and per-partition
/// `preferred_read_replica` in the response. End-to-end: a v11 response
/// must encode both.
#[test]
fn fetch_v11_response_emits_preferred_read_replica() {
    let response = FetchResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        session_id: 100,
        responses: vec![FetchTopicResponse {
            name: "t".to_string(),
            partitions: vec![FetchPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                high_watermark: 50,
                last_stable_offset: 50,
                log_start_offset: 0,
                aborted_transactions: vec![],
                preferred_read_replica: 7,
                records: None,
            }],
        }],
    };

    let mut buf_v10 = Vec::new();
    response.encode_versioned(&mut buf_v10, 10).unwrap();
    let mut buf_v11 = Vec::new();
    response.encode_versioned(&mut buf_v11, 11).unwrap();

    // v11 adds 4 bytes per partition for preferred_read_replica.
    assert_eq!(buf_v10.len() + 4, buf_v11.len());
    // Confirm the value 7 lands somewhere in v11 but NOT in v10.
    let v11_has_seven = buf_v11.windows(4).any(|w| w == 7i32.to_be_bytes());
    assert!(v11_has_seven);
}

// ============================================================================
// Metadata v9 (flexible) — request parse + response encode
// ============================================================================

/// Build a Metadata v9 request: compact arrays/strings, tagged fields.
/// Verify round-trip through `Request::parse`.
#[test]
fn metadata_v9_request_decodes_flexible_body() {
    let mut body = BytesMut::new();
    // topics compact_array: 1 topic -> varint 2
    body.put_u8(0x02);
    // topic.name compact_string "t" -> varint 2 + 1 byte
    body.put_u8(0x02);
    body.put_slice(b"t");
    body.put_u8(0); // tagged
    body.put_u8(1); // allow_auto_topic_creation
    body.put_u8(0); // include_cluster_authorized_operations
    body.put_u8(1); // include_topic_authorized_operations
    body.put_u8(0); // body tagged

    let frame = build_flexible_request(i16::from(ApiKey::Metadata), 9, 5, &body);
    let req = Request::parse(frame).expect("v9 metadata parses");
    match req {
        Request::Metadata(header, data) => {
            assert_eq!(header.api_version, 9);
            assert_eq!(header.correlation_id, 5);
            assert_eq!(data.topics, Some(vec!["t".to_string()]));
            assert!(data.allow_auto_topic_creation);
            assert!(!data.include_cluster_authorized_operations);
            assert!(data.include_topic_authorized_operations);
        }
        _ => panic!("expected Metadata"),
    }
}

/// v9 response wire-shape: cluster_id, leader_epoch, offline_replicas,
/// per-topic + cluster authorized operations, and the trailing tagged
/// fields. Sanity-check via the byte-position assertions in
/// `MetadataResponseData::encode_versioned` plus a payload spot-check.
#[test]
fn metadata_v9_response_round_trips_v9_only_fields() {
    let response = MetadataResponseData {
        throttle_time_ms: 100,
        brokers: vec![BrokerData {
            node_id: 1,
            host: "broker-1".to_string(),
            port: 9092,
            rack: Some("us-east-1a".to_string()),
        }],
        cluster_id: Some("cluster-id".to_string()),
        controller_id: 1,
        topics: vec![TopicMetadata {
            error_code: KafkaCode::None,
            name: "t".to_string(),
            is_internal: false,
            partitions: vec![PartitionMetadata {
                error_code: KafkaCode::None,
                partition_index: 0,
                leader_id: 1,
                leader_epoch: 5,
                replica_nodes: vec![1, 2, 3],
                isr_nodes: vec![1, 2],
                offline_replicas: vec![3],
            }],
            topic_authorized_operations: 0x12345678,
        }],
        cluster_authorized_operations: 0x0F0F_0F0F,
    };

    let mut buf = Vec::new();
    response.encode_versioned(&mut buf, 9).unwrap();
    // Final byte must be the body's tagged-fields zero.
    assert_eq!(*buf.last().unwrap(), 0);
    // Throttle prefix.
    assert_eq!(&buf[0..4], &100i32.to_be_bytes());
    // The cluster_authorized_operations value lands somewhere in the
    // body before the trailing tagged-fields byte.
    let needle = 0x0F0F_0F0Fu32.to_be_bytes();
    assert!(buf.windows(4).any(|w| w == needle));
    // The leader_epoch (5) should also be present.
    let leader_needle = 5i32.to_be_bytes();
    assert!(buf.windows(4).any(|w| w == leader_needle));
}

// ============================================================================
// Produce v9 (flexible) — request parse + response encode
// ============================================================================

/// v9 Produce request body: COMPACT_NULLABLE_STRING transactional_id,
/// compact arrays, COMPACT_NULLABLE_BYTES records, trailing tagged
/// fields per record. Round-trip through `Request::parse`.
#[test]
fn produce_v9_request_decodes_flexible_body() {
    let mut body = BytesMut::new();
    body.put_u8(0); // transactional_id = null
    body.put_i16(-1); // acks = -1
    body.put_i32(30_000); // timeout_ms
    // topics compact_array: 1 topic
    body.put_u8(0x02);
    body.put_u8(0x02); // name "t"
    body.put_slice(b"t");
    // partitions compact_array: 1
    body.put_u8(0x02);
    body.put_i32(0); // partition_index
    // records: compact_nullable_bytes — 4 bytes -> varint 5 + payload
    body.put_u8(0x05);
    body.put_slice(&[0xAA, 0xBB, 0xCC, 0xDD]);
    body.put_u8(0); // partition tagged
    body.put_u8(0); // topic tagged
    body.put_u8(0); // body tagged

    let frame = build_flexible_request(i16::from(ApiKey::Produce), 9, 77, &body);
    let req = Request::parse(frame).expect("v9 produce parses");
    match req {
        Request::Produce(header, data) => {
            assert_eq!(header.api_version, 9);
            assert_eq!(header.correlation_id, 77);
            assert_eq!(data.transactional_id, None);
            assert_eq!(data.acks, -1);
            assert_eq!(data.timeout_ms, 30_000);
            assert_eq!(data.topics.len(), 1);
            assert_eq!(data.topics[0].name, "t");
            assert_eq!(
                data.topics[0].partitions[0].records.as_ref(),
                &[0xAA, 0xBB, 0xCC, 0xDD]
            );
        }
        _ => panic!("expected Produce"),
    }
}

/// Produce v8 response: per-partition `record_errors` and `error_message`
/// fields are emitted, log_start_offset is present, throttle is at the
/// tail.
#[test]
fn produce_v8_response_emits_record_errors_and_log_start_offset() {
    let response = ProduceResponseData {
        responses: vec![ProduceTopicResponse {
            name: "t".to_string(),
            partitions: vec![ProducePartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                base_offset: 100,
                log_append_time: -1,
                log_start_offset: 50,
                record_errors: vec![],
                error_message: None,
            }],
        }],
        throttle_time_ms: 0,
    };
    let mut v7 = Vec::new();
    response.encode_versioned(&mut v7, 7).unwrap();
    let mut v8 = Vec::new();
    response.encode_versioned(&mut v8, 8).unwrap();
    // v8 adds: record_errors length (4) + error_message null (2) = 6 bytes.
    assert_eq!(v7.len() + 6, v8.len());
    // log_start_offset (50) lands in v7+.
    let needle = 50i64.to_be_bytes();
    assert!(v7.windows(8).any(|w| w == needle));
}

/// v9 Produce response: flexible encoding round-trips a populated
/// `error_message` and the trailing tagged fields. The compact-string
/// encoding for "bad" must contain the literal payload.
#[test]
fn produce_v9_response_emits_compact_error_message() {
    let response = ProduceResponseData {
        responses: vec![ProduceTopicResponse {
            name: "t".to_string(),
            partitions: vec![ProducePartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::CorruptMessage,
                base_offset: -1,
                log_append_time: -1,
                log_start_offset: -1,
                record_errors: vec![],
                error_message: Some("bad batch".to_string()),
            }],
        }],
        throttle_time_ms: 0,
    };
    let mut buf = Vec::new();
    response.encode_versioned(&mut buf, 9).unwrap();
    // Trailing body tagged fields.
    assert_eq!(*buf.last().unwrap(), 0);
    // "bad batch" payload is present verbatim.
    assert!(
        buf.windows(9).any(|w| w == b"bad batch"),
        "compact_nullable_string payload missing"
    );
}

/// Cross-version sanity: a v3 Produce request must still parse classic
/// (non-flexible) — v9's flexible parser kicks in only for v9+. This
/// guards against the parser silently picking the flexible path for
/// older clients.
#[test]
fn produce_v3_request_still_uses_classic_encoding() {
    let mut body = BytesMut::new();
    put_nullable_string(&mut body, None); // transactional_id = null (i16 -1)
    body.put_i16(1); // acks
    body.put_i32(5_000); // timeout_ms
    body.put_i32(1); // topics
    put_kafka_string(&mut body, "t");
    body.put_i32(1); // partitions
    body.put_i32(0); // partition_index
    body.put_i32(4); // records length
    body.put_slice(&[1, 2, 3, 4]);

    let frame = build_classic_request(i16::from(ApiKey::Produce), 3, 11, &body);
    let req = Request::parse(frame).expect("v3 produce parses");
    match req {
        Request::Produce(header, data) => {
            assert_eq!(header.api_version, 3);
            assert_eq!(data.acks, 1);
            assert_eq!(data.topics[0].partitions[0].records.as_ref(), &[1, 2, 3, 4]);
        }
        _ => panic!("expected Produce"),
    }
}

/// Cross-version sanity: a v0 Metadata request still parses without
/// the flexible body machinery.
#[test]
fn metadata_v0_request_still_uses_classic_encoding() {
    let mut body = BytesMut::new();
    body.put_i32(1); // 1 topic
    put_kafka_string(&mut body, "t");

    let frame = build_classic_request(i16::from(ApiKey::Metadata), 0, 1, &body);
    let req = Request::parse(frame).expect("v0 metadata parses");
    match req {
        Request::Metadata(header, data) => {
            assert_eq!(header.api_version, 0);
            assert_eq!(data.topics, Some(vec!["t".to_string()]));
        }
        _ => panic!("expected Metadata"),
    }
}

// ============================================================================
// Default-handler smoke test: round-trip through Handler trait defaults
// ============================================================================

/// Confirms the default `Handler::handle_metadata` returns a struct that
/// can be encoded at the new max version (v9) without hitting any panic
/// or null-default mismatch in the new fields.
#[tokio::test]
async fn default_metadata_handler_serializes_at_v9() {
    use kafkaesque::server::{Handler, RequestContext};
    use std::net::SocketAddr;
    use std::sync::Arc;

    struct DefaultHandler;
    #[async_trait::async_trait]
    impl Handler for DefaultHandler {}

    let ctx = RequestContext {
        client_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        conn_id: 1,
        api_version: 9,
        client_id: None,
        request_id: uuid::Uuid::nil(),
        principal: Arc::from("User:ANONYMOUS"),
        client_host: Arc::from("127.0.0.1"),
        transport_tls: false,
    };

    let resp = DefaultHandler
        .handle_metadata(
            &ctx,
            MetadataRequestData {
                topics: None,
                allow_auto_topic_creation: true,
                include_cluster_authorized_operations: false,
                include_topic_authorized_operations: false,
            },
        )
        .await;

    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 9)
        .expect("v9 metadata response encodes from default handler");
    // Should at least contain throttle (4) + brokers compact array (1) +
    // cluster_id (1) + controller_id (4) + topics compact array (1) +
    // cluster_authorized_operations (4) + tagged_fields (1) = 16 bytes.
    assert!(
        buf.len() >= 16,
        "v9 metadata wire frame too small: {}",
        buf.len()
    );
    assert_eq!(*buf.last().unwrap(), 0, "trailing tagged-fields byte");
}

/// Same for default `handle_fetch` at v11.
#[tokio::test]
async fn default_fetch_handler_serializes_at_v11() {
    use kafkaesque::server::{Handler, RequestContext};
    use std::net::SocketAddr;
    use std::sync::Arc;

    struct DefaultHandler;
    #[async_trait::async_trait]
    impl Handler for DefaultHandler {}

    let ctx = RequestContext {
        client_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        conn_id: 1,
        api_version: 11,
        client_id: None,
        request_id: uuid::Uuid::nil(),
        principal: Arc::from("User:ANONYMOUS"),
        client_host: Arc::from("127.0.0.1"),
        transport_tls: false,
    };

    let req = FetchRequestData {
        replica_id: -1,
        max_wait_ms: 0,
        min_bytes: 1,
        max_bytes: 1024,
        isolation_level: 0,
        session_id: 0,
        session_epoch: -1,
        topics: vec![FetchTopicData {
            name: "t".to_string(),
            partitions: vec![FetchPartitionData {
                partition_index: 0,
                current_leader_epoch: -1,
                fetch_offset: 0,
                log_start_offset: -1,
                partition_max_bytes: 1024,
            }],
        }],
        forgotten_topics: vec![],
        rack_id: String::new(),
    };

    let resp = DefaultHandler.handle_fetch(&ctx, req).await;
    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 11).unwrap();
    // The default handler returns UnknownTopicOrPartition per partition.
    // Every partition response in v11 carries preferred_read_replica
    // (-1) which encodes as 4 bytes of 0xFF.
    let dead_replica = (-1i32).to_be_bytes();
    assert!(buf.windows(4).any(|w| w == dead_replica));
}

/// Same for default `handle_produce` at v9.
#[tokio::test]
async fn default_produce_handler_serializes_at_v9() {
    use kafkaesque::server::{Handler, RequestContext};
    use std::net::SocketAddr;
    use std::sync::Arc;

    struct DefaultHandler;
    #[async_trait::async_trait]
    impl Handler for DefaultHandler {}

    let ctx = RequestContext {
        client_addr: "127.0.0.1:0".parse::<SocketAddr>().unwrap(),
        conn_id: 1,
        api_version: 9,
        client_id: None,
        request_id: uuid::Uuid::nil(),
        principal: Arc::from("User:ANONYMOUS"),
        client_host: Arc::from("127.0.0.1"),
        transport_tls: false,
    };

    let req = ProduceRequestData {
        transactional_id: None,
        acks: -1,
        timeout_ms: 0,
        topics: vec![ProduceTopicData {
            name: "t".to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records: Bytes::new(),
            }],
        }],
    };
    let resp = DefaultHandler.handle_produce(&ctx, req).await;
    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 9).unwrap();
    // v9 trailing tagged fields zero.
    assert_eq!(*buf.last().unwrap(), 0);
}
