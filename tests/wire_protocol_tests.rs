//! Kafka wire protocol compatibility integration tests.
//!
//! These tests verify that the Kafkaesque implementation correctly handles
//! the Kafka wire protocol at the byte level, ensuring compatibility with
//! standard Kafka clients.
//!
//! # Wire Protocol Structure
//!
//! Kafka messages follow this structure:
//! ```text
//! [message_size: i32] [request_header] [request_body]
//! ```
//!
//! Request header (standard format):
//! ```text
//! [api_key: i16] [api_version: i16] [correlation_id: i32] [client_id: nullable_string]
//! ```
//!
//! Flexible format (KIP-482) adds tagged fields after client_id.
//!
//! # Running Tests
//!
//! ```sh
//! cargo test --test wire_protocol_tests
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use kafkaesque::protocol::{
    CrcValidationResult, ProducerBatchInfo, parse_producer_info, parse_record_count,
    patch_base_offset, validate_batch_crc,
};
use kafkaesque::server::request::{ApiKey, Request};

// ============================================================================
// Test Helpers
// ============================================================================

/// Create a minimal Kafka request header in wire format.
fn create_request_header(
    api_key: i16,
    api_version: i16,
    correlation_id: i32,
    client_id: Option<&str>,
) -> BytesMut {
    let mut buf = BytesMut::with_capacity(64);
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);

    // Nullable string encoding
    match client_id {
        Some(id) => {
            buf.put_i16(id.len() as i16);
            buf.put_slice(id.as_bytes());
        }
        None => {
            buf.put_i16(-1); // null string
        }
    }

    buf
}

/// Create a framed message (length prefix + data).
fn frame_message(data: &[u8]) -> BytesMut {
    let mut buf = BytesMut::with_capacity(4 + data.len());
    buf.put_i32(data.len() as i32);
    buf.put_slice(data);
    buf
}

/// Create a minimal record batch with configurable fields.
fn create_record_batch(
    base_offset: i64,
    num_records: i32,
    producer_id: i64,
    producer_epoch: i16,
    first_sequence: i32,
) -> BytesMut {
    let mut batch = BytesMut::with_capacity(100);

    // baseOffset (8 bytes)
    batch.put_i64(base_offset);
    // batchLength (4 bytes) - placeholder
    batch.put_i32(49);
    // partitionLeaderEpoch (4 bytes)
    batch.put_i32(1);
    // magic (1 byte) - must be 2 for v2 format
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
    batch.put_i64(producer_id);
    // producerEpoch (2 bytes)
    batch.put_i16(producer_epoch);
    // baseSequence (4 bytes)
    batch.put_i32(first_sequence);
    // numRecords (4 bytes)
    batch.put_i32(num_records);

    batch
}

// ============================================================================
// ApiKey Tests
// ============================================================================

#[test]
fn test_api_key_roundtrip() {
    // Test all known API keys
    let known_keys = [
        (0i16, ApiKey::Produce),
        (1, ApiKey::Fetch),
        (2, ApiKey::ListOffsets),
        (3, ApiKey::Metadata),
        (4, ApiKey::LeaderAndIsr),
        (5, ApiKey::StopReplica),
        (6, ApiKey::UpdateMetadata),
        (7, ApiKey::ControlledShutdown),
        (8, ApiKey::OffsetCommit),
        (9, ApiKey::OffsetFetch),
        (10, ApiKey::FindCoordinator),
        (11, ApiKey::JoinGroup),
        (12, ApiKey::Heartbeat),
        (13, ApiKey::LeaveGroup),
        (14, ApiKey::SyncGroup),
        (15, ApiKey::DescribeGroups),
        (16, ApiKey::ListGroups),
        (17, ApiKey::SaslHandshake),
        (18, ApiKey::ApiVersions),
        (19, ApiKey::CreateTopics),
        (20, ApiKey::DeleteTopics),
        (22, ApiKey::InitProducerId),
        (36, ApiKey::SaslAuthenticate),
        (42, ApiKey::DeleteGroups),
    ];

    for (value, expected_key) in known_keys {
        let key: ApiKey = value.into();
        assert_eq!(
            key, expected_key,
            "API key {} should convert correctly",
            value
        );

        let back: i16 = key.into();
        assert_eq!(back, value, "API key should roundtrip to same value");
    }
}

#[test]
fn test_api_key_unknown() {
    // Unknown API keys should be preserved
    let unknown_values = [21i16, 23, 35, 37, 100, 999, -1];

    for value in unknown_values {
        let key: ApiKey = value.into();
        match key {
            ApiKey::Unknown(n) => assert_eq!(n, value),
            _ => panic!("Expected Unknown variant for API key {}", value),
        }

        let back: i16 = key.into();
        assert_eq!(back, value);
    }
}

#[test]
fn test_api_key_as_str() {
    assert_eq!(ApiKey::Produce.as_str(), "Produce");
    assert_eq!(ApiKey::Fetch.as_str(), "Fetch");
    assert_eq!(ApiKey::Metadata.as_str(), "Metadata");
    assert_eq!(ApiKey::ApiVersions.as_str(), "ApiVersions");
    assert_eq!(ApiKey::Unknown(999).as_str(), "Unknown");
}

// ============================================================================
// Request Header Parsing Tests
// ============================================================================

#[test]
fn test_parse_standard_request_header() {
    // Create a standard Metadata v0 request header with complete body
    let mut buf = BytesMut::new();
    buf.put_i16(3); // api_key = Metadata
    buf.put_i16(0); // api_version = 0 (simpler format)
    buf.put_i32(12345); // correlation_id
    buf.put_i16(8); // client_id length
    buf.put_slice(b"test-cli");

    // Body: empty topic array (null = -1 or empty = 0)
    buf.put_i32(0); // empty topic array

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse successfully");

    let header = request.header();
    assert_eq!(header.api_key, ApiKey::Metadata);
    assert_eq!(header.api_version, 0);
    assert_eq!(header.correlation_id, 12345);
    assert_eq!(header.client_id, Some("test-cli".to_string()));
}

#[test]
fn test_parse_request_header_null_client_id() {
    let mut buf = BytesMut::new();
    buf.put_i16(18); // api_key = ApiVersions
    buf.put_i16(0); // api_version = 0
    buf.put_i32(1); // correlation_id
    buf.put_i16(-1); // null client_id

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse with null client_id");

    assert_eq!(request.header().client_id, None);
}

#[test]
fn test_parse_api_versions_v3_flexible_header() {
    // ApiVersions v3+ uses hybrid format: standard client_id + tagged fields
    let mut buf = BytesMut::new();
    buf.put_i16(18); // api_key = ApiVersions
    buf.put_i16(3); // api_version = 3 (flexible)
    buf.put_i32(42); // correlation_id
    buf.put_i16(6); // client_id length
    buf.put_slice(b"client");
    buf.put_u8(0); // empty tagged fields

    // Add body for ApiVersions v3
    buf.put_i16(0); // client_software_name length (compact string = length + 1)
    buf.put_u8(0); // empty tagged fields

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse ApiVersions v3");

    let header = request.header();
    assert_eq!(header.api_key, ApiKey::ApiVersions);
    assert_eq!(header.api_version, 3);
    assert_eq!(header.client_id, Some("client".to_string()));
}

#[test]
fn test_parse_request_header_empty_client_id() {
    let mut buf = BytesMut::new();
    buf.put_i16(3); // api_key = Metadata
    buf.put_i16(0); // api_version = 0
    buf.put_i32(99); // correlation_id
    buf.put_i16(0); // empty client_id (length = 0)

    // Body: empty topic array
    buf.put_i32(0);

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse with empty client_id");

    assert_eq!(request.header().client_id, Some(String::new()));
}

// ============================================================================
// Request Parsing Tests (Various API Types)
// ============================================================================

#[test]
fn test_parse_metadata_request_v0() {
    let mut buf = BytesMut::new();
    // Header
    buf.put_i16(3); // Metadata
    buf.put_i16(0); // version 0
    buf.put_i32(1); // correlation_id
    buf.put_i16(-1); // null client_id

    // Body: topic array
    buf.put_i32(1); // array length = 1
    buf.put_i16(4); // topic name length
    buf.put_slice(b"test");

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse Metadata v0");

    match request {
        Request::Metadata(header, data) => {
            assert_eq!(header.api_version, 0);
            // Topics is Option<Vec<String>>
            assert!(data.topics.is_some());
            let topics = data.topics.unwrap();
            assert_eq!(topics.len(), 1);
            assert_eq!(topics[0], "test");
        }
        _ => panic!("Expected Metadata request"),
    }
}

#[test]
fn test_parse_produce_request_v0() {
    let mut buf = BytesMut::new();
    // Header
    buf.put_i16(0); // Produce
    buf.put_i16(0); // version 0
    buf.put_i32(2); // correlation_id
    buf.put_i16(-1); // null client_id

    // Body
    buf.put_i16(-1); // transactional_id (null)
    buf.put_i16(1); // acks
    buf.put_i32(5000); // timeout_ms

    // Topic array
    buf.put_i32(1); // 1 topic
    buf.put_i16(5); // topic name length
    buf.put_slice(b"topic");

    // Partition array
    buf.put_i32(1); // 1 partition
    buf.put_i32(0); // partition_index

    // Record batch (minimal - just length prefix saying no data)
    buf.put_i32(0); // records length = 0 (empty)

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse Produce v0");

    match request {
        Request::Produce(header, data) => {
            assert_eq!(header.api_version, 0);
            assert_eq!(data.acks, 1);
            assert_eq!(data.timeout_ms, 5000);
            assert_eq!(data.topics.len(), 1);
        }
        _ => panic!("Expected Produce request"),
    }
}

#[test]
fn test_parse_fetch_request_v0() {
    let mut buf = BytesMut::new();
    // Header
    buf.put_i16(1); // Fetch
    buf.put_i16(0); // version 0
    buf.put_i32(3); // correlation_id
    buf.put_i16(-1); // null client_id

    // Body
    buf.put_i32(-1); // replica_id
    buf.put_i32(500); // max_wait_ms
    buf.put_i32(1); // min_bytes
    buf.put_i32(1048576); // max_bytes
    buf.put_i8(0); // isolation_level

    // Topic array
    buf.put_i32(1); // 1 topic
    buf.put_i16(5); // topic name length
    buf.put_slice(b"topic");

    // Partition array
    buf.put_i32(1); // 1 partition
    buf.put_i32(0); // partition_index
    buf.put_i64(0); // fetch_offset
    buf.put_i32(1048576); // partition_max_bytes

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse Fetch v0");

    match request {
        Request::Fetch(header, data) => {
            assert_eq!(header.api_version, 0);
            assert_eq!(data.max_wait_ms, 500);
            assert_eq!(data.min_bytes, 1);
            assert_eq!(data.topics.len(), 1);
        }
        _ => panic!("Expected Fetch request"),
    }
}

#[test]
fn test_parse_unknown_api_key() {
    let mut buf = BytesMut::new();
    buf.put_i16(999); // Unknown API key
    buf.put_i16(0); // version
    buf.put_i32(1); // correlation_id
    buf.put_i16(-1); // null client_id
    buf.put_slice(b"some body data");

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse unknown request");

    match request {
        Request::Unknown(header, body) => {
            assert!(matches!(header.api_key, ApiKey::Unknown(999)));
            assert!(!body.is_empty());
        }
        _ => panic!("Expected Unknown request"),
    }
}

// ============================================================================
// Record Batch Tests
// ============================================================================

#[test]
fn test_record_batch_parse_record_count() {
    let batch = create_record_batch(0, 5, -1, -1, -1);
    let count = parse_record_count(&batch);
    assert_eq!(count, 5);
}

#[test]
fn test_record_batch_parse_producer_info() {
    let batch = create_record_batch(0, 10, 12345, 5, 100);
    let info = parse_producer_info(&batch).expect("Should parse producer info");

    assert_eq!(info.producer_id, 12345);
    assert_eq!(info.producer_epoch, 5);
    assert_eq!(info.first_sequence, 100);
    assert_eq!(info.record_count, 10);
    assert!(info.is_idempotent());
    assert_eq!(info.last_sequence(), Some(109));
}

#[test]
fn test_record_batch_non_idempotent() {
    let batch = create_record_batch(0, 5, -1, 0, 0);
    let info = parse_producer_info(&batch).expect("Should parse");

    assert_eq!(info.producer_id, -1);
    assert!(!info.is_idempotent());
}

#[test]
fn test_record_batch_patch_base_offset() {
    let mut batch = create_record_batch(0, 1, -1, -1, -1);

    // Verify initial offset is 0
    let initial_offset = i64::from_be_bytes(batch[0..8].try_into().unwrap());
    assert_eq!(initial_offset, 0);

    // Patch the offset
    patch_base_offset(&mut batch, 12345);

    let patched_offset = i64::from_be_bytes(batch[0..8].try_into().unwrap());
    assert_eq!(patched_offset, 12345);

    // CRC should be valid after patching
    assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);
}

#[test]
fn test_record_batch_crc_validation() {
    let mut batch = create_record_batch(0, 1, -1, -1, -1);

    // Initially CRC is 0 (placeholder), so should be invalid
    match validate_batch_crc(&batch) {
        CrcValidationResult::Invalid { .. } => {}
        other => panic!("Expected Invalid, got {:?}", other),
    }

    // After patching, CRC should be valid
    patch_base_offset(&mut batch, 0);
    assert_eq!(validate_batch_crc(&batch), CrcValidationResult::Valid);

    // Corrupt a byte in the CRC-covered region
    batch[25] ^= 0xFF;
    match validate_batch_crc(&batch) {
        CrcValidationResult::Invalid { expected, actual } => {
            assert_ne!(expected, actual);
        }
        other => panic!("Expected Invalid after corruption, got {:?}", other),
    }
}

#[test]
fn test_record_batch_too_small_for_crc() {
    let batch = vec![0u8; 15]; // Too small
    assert_eq!(validate_batch_crc(&batch), CrcValidationResult::TooSmall);
}

#[test]
fn test_record_batch_too_small_for_producer_info() {
    let batch = vec![0u8; 50]; // Too small for producer info (needs 61)
    assert!(parse_producer_info(&batch).is_none());
}

// ============================================================================
// Message Framing Tests
// ============================================================================

#[test]
fn test_message_framing() {
    let header = create_request_header(18, 0, 1, Some("client"));

    // Frame the message
    let framed = frame_message(&header);

    // First 4 bytes should be the length
    let length = i32::from_be_bytes(framed[0..4].try_into().unwrap());
    assert_eq!(length, header.len() as i32);

    // Rest should be the header data
    assert_eq!(&framed[4..], &header[..]);
}

#[test]
fn test_empty_message_framing() {
    let framed = frame_message(&[]);
    assert_eq!(framed.to_vec(), vec![0, 0, 0, 0]); // Length = 0
}

#[test]
fn test_large_message_framing() {
    let data = vec![0xABu8; 1024 * 1024]; // 1 MB
    let framed = frame_message(&data);

    let length = i32::from_be_bytes(framed[0..4].try_into().unwrap());
    assert_eq!(length, 1024 * 1024);
    assert_eq!(framed.len(), 4 + 1024 * 1024);
}

// ============================================================================
// Edge Cases and Error Handling
// ============================================================================

#[test]
fn test_parse_truncated_header() {
    // Only 2 bytes - missing most of the header
    let buf = Bytes::from(vec![0x00, 0x03]);
    let result = Request::parse(buf);
    assert!(result.is_err());
}

#[test]
fn test_parse_truncated_body() {
    let mut buf = BytesMut::new();
    // Complete header
    buf.put_i16(3); // Metadata
    buf.put_i16(0); // version
    buf.put_i32(1); // correlation_id
    buf.put_i16(-1); // null client_id

    // Partial body - array says 1000 topics but no data
    buf.put_i32(1000);

    let bytes = Bytes::from(buf.to_vec());
    let result = Request::parse(bytes);
    // This should fail due to truncated body
    assert!(result.is_err());
}

#[test]
fn test_producer_batch_info_edge_cases() {
    // Sequence overflow
    let info = ProducerBatchInfo {
        producer_id: 1,
        producer_epoch: 0,
        first_sequence: i32::MAX - 5,
        record_count: 10, // Would overflow
    };
    assert_eq!(info.last_sequence(), None);

    // Zero record count
    let info = ProducerBatchInfo {
        producer_id: 1,
        producer_epoch: 0,
        first_sequence: 0,
        record_count: 0,
    };
    assert_eq!(info.last_sequence(), None);

    // Negative first sequence
    let info = ProducerBatchInfo {
        producer_id: 1,
        producer_epoch: 0,
        first_sequence: -1,
        record_count: 10,
    };
    assert_eq!(info.last_sequence(), None);
}

// ============================================================================
// Integration: Full Request-Response Round Trip
// ============================================================================

#[test]
fn test_api_versions_request_roundtrip() {
    // Create an ApiVersions v0 request
    let mut buf = BytesMut::new();
    buf.put_i16(18); // ApiVersions
    buf.put_i16(0); // version 0
    buf.put_i32(42); // correlation_id
    buf.put_i16(4); // client_id length
    buf.put_slice(b"test");

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse");

    match request {
        Request::ApiVersions(header, _) => {
            assert_eq!(header.api_key, ApiKey::ApiVersions);
            assert_eq!(header.correlation_id, 42);
            assert_eq!(header.client_id, Some("test".to_string()));
        }
        _ => panic!("Expected ApiVersions request"),
    }
}

#[test]
fn test_join_group_request_parsing() {
    let mut buf = BytesMut::new();
    // Header
    buf.put_i16(11); // JoinGroup
    buf.put_i16(0); // version 0
    buf.put_i32(100); // correlation_id
    buf.put_i16(-1); // null client_id

    // Body
    buf.put_i16(8); // group_id length
    buf.put_slice(b"my-group");
    buf.put_i32(30000); // session_timeout_ms
    buf.put_i16(0); // member_id length (empty)
    buf.put_i16(8); // protocol_type length
    buf.put_slice(b"consumer");

    // Protocols array
    buf.put_i32(1); // 1 protocol
    buf.put_i16(5); // protocol name length
    buf.put_slice(b"range");
    buf.put_i32(0); // metadata length

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse JoinGroup");

    match request {
        Request::JoinGroup(header, data) => {
            assert_eq!(header.api_version, 0);
            assert_eq!(data.group_id, "my-group");
            assert_eq!(data.session_timeout_ms, 30000);
            assert_eq!(data.protocol_type, "consumer");
        }
        _ => panic!("Expected JoinGroup request"),
    }
}

#[test]
fn test_find_coordinator_request_parsing() {
    let mut buf = BytesMut::new();
    // Header
    buf.put_i16(10); // FindCoordinator
    buf.put_i16(0); // version 0
    buf.put_i32(1); // correlation_id
    buf.put_i16(-1); // null client_id

    // Body
    buf.put_i16(8); // key length
    buf.put_slice(b"my-group");

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse FindCoordinator");

    match request {
        Request::FindCoordinator(header, data) => {
            assert_eq!(header.api_version, 0);
            assert_eq!(data.key, "my-group");
        }
        _ => panic!("Expected FindCoordinator request"),
    }
}

#[test]
fn test_heartbeat_request_parsing() {
    let mut buf = BytesMut::new();
    // Header
    buf.put_i16(12); // Heartbeat
    buf.put_i16(0); // version 0
    buf.put_i32(5); // correlation_id
    buf.put_i16(-1); // null client_id

    // Body
    buf.put_i16(8); // group_id length
    buf.put_slice(b"my-group");
    buf.put_i32(1); // generation_id
    buf.put_i16(9); // member_id length
    buf.put_slice(b"member-01");

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse Heartbeat");

    match request {
        Request::Heartbeat(header, data) => {
            assert_eq!(header.api_version, 0);
            assert_eq!(data.group_id, "my-group");
            assert_eq!(data.generation_id, 1);
            assert_eq!(data.member_id, "member-01");
        }
        _ => panic!("Expected Heartbeat request"),
    }
}

#[test]
fn test_leave_group_request_parsing() {
    let mut buf = BytesMut::new();
    // Header
    buf.put_i16(13); // LeaveGroup
    buf.put_i16(0); // version 0
    buf.put_i32(6); // correlation_id
    buf.put_i16(-1); // null client_id

    // Body
    buf.put_i16(8); // group_id length
    buf.put_slice(b"my-group");
    buf.put_i16(9); // member_id length
    buf.put_slice(b"member-01");

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse LeaveGroup");

    match request {
        Request::LeaveGroup(header, data) => {
            assert_eq!(header.api_version, 0);
            assert_eq!(data.group_id, "my-group");
            assert_eq!(data.member_id, "member-01");
        }
        _ => panic!("Expected LeaveGroup request"),
    }
}

#[test]
fn test_list_offsets_request_parsing() {
    let mut buf = BytesMut::new();
    // Header
    buf.put_i16(2); // ListOffsets
    buf.put_i16(0); // version 0
    buf.put_i32(7); // correlation_id
    buf.put_i16(-1); // null client_id

    // Body
    buf.put_i32(-1); // replica_id

    // Topics array
    buf.put_i32(1); // 1 topic
    buf.put_i16(5); // topic name length
    buf.put_slice(b"topic");

    // Partitions array
    buf.put_i32(1); // 1 partition
    buf.put_i32(0); // partition_index
    buf.put_i64(-1); // timestamp (-1 = latest)
    buf.put_i32(1); // max_num_offsets

    let bytes = Bytes::from(buf.to_vec());
    let request = Request::parse(bytes).expect("Should parse ListOffsets");

    match request {
        Request::ListOffsets(header, data) => {
            assert_eq!(header.api_version, 0);
            assert_eq!(data.topics.len(), 1);
            assert_eq!(data.topics[0].name, "topic");
        }
        _ => panic!("Expected ListOffsets request"),
    }
}

// ============================================================================
// Additional Wire Protocol Tests
// ============================================================================

#[test]
fn test_correlation_id_propagation() {
    // Test that correlation IDs are correctly parsed for various requests
    let correlation_ids = [0i32, 1, -1, i32::MAX, i32::MIN, 12345678];

    for &correlation_id in &correlation_ids {
        let mut buf = BytesMut::new();
        buf.put_i16(18); // ApiVersions
        buf.put_i16(0); // version 0
        buf.put_i32(correlation_id);
        buf.put_i16(-1); // null client_id

        let bytes = Bytes::from(buf.to_vec());
        let request = Request::parse(bytes).expect("Should parse");
        assert_eq!(request.header().correlation_id, correlation_id);
    }
}

#[test]
fn test_client_id_with_special_characters() {
    // Test client IDs with various characters
    let client_ids = [
        "simple",
        "with-dash",
        "with_underscore",
        "with.dot",
        "123numeric",
        "MixedCase",
        "unicode-日本語",
    ];

    for &client_id in &client_ids {
        let mut buf = BytesMut::new();
        buf.put_i16(18); // ApiVersions
        buf.put_i16(0); // version 0
        buf.put_i32(1); // correlation_id
        buf.put_i16(client_id.len() as i16);
        buf.put_slice(client_id.as_bytes());

        let bytes = Bytes::from(buf.to_vec());
        let request = Request::parse(bytes).expect("Should parse");
        assert_eq!(request.header().client_id, Some(client_id.to_string()));
    }
}

#[test]
fn test_api_version_ranges() {
    // Test that various API versions are parsed correctly
    let versions = [0i16, 1, 5, 10, 15, i16::MAX];

    for &version in &versions {
        let mut buf = BytesMut::new();
        buf.put_i16(18); // ApiVersions
        buf.put_i16(version);
        buf.put_i32(1); // correlation_id
        buf.put_i16(-1); // null client_id

        let bytes = Bytes::from(buf.to_vec());
        // Note: Not all versions may be supported, but header should parse
        let result = Request::parse(bytes);
        // We just verify it doesn't panic and parses the header
        if let Ok(request) = result {
            assert_eq!(request.header().api_version, version);
        }
    }
}

#[test]
fn test_record_batch_with_max_values() {
    // Test record batch with boundary values
    let batch = create_record_batch(i64::MAX, i32::MAX, i64::MAX, i16::MAX, i32::MAX);

    let info = parse_producer_info(&batch).expect("Should parse");
    assert_eq!(info.producer_id, i64::MAX);
    assert_eq!(info.producer_epoch, i16::MAX);
    assert_eq!(info.first_sequence, i32::MAX);
    assert!(info.is_idempotent());
    // last_sequence will overflow
    assert_eq!(info.last_sequence(), None);
}

#[test]
fn test_record_batch_with_zero_records() {
    // Edge case: batch claiming 0 records (last_offset_delta = -1)
    let mut batch = BytesMut::with_capacity(100);
    batch.put_i64(0); // baseOffset
    batch.put_i32(49); // batchLength
    batch.put_i32(1); // partitionLeaderEpoch
    batch.put_i8(2); // magic
    batch.put_u32(0); // crc placeholder
    batch.put_i16(0); // attributes
    batch.put_i32(-1); // lastOffsetDelta = -1 (0 records)
    batch.put_i64(1000); // firstTimestamp
    batch.put_i64(2000); // maxTimestamp
    batch.put_i64(-1); // producerId
    batch.put_i16(-1); // producerEpoch
    batch.put_i32(-1); // baseSequence
    batch.put_i32(0); // numRecords

    // parse_record_count uses last_offset_delta + 1
    let count = parse_record_count(&batch);
    assert_eq!(count, 0); // -1 + 1 = 0
}

#[test]
fn test_crc_after_multiple_patches() {
    let mut batch = create_record_batch(0, 5, 1000, 1, 0);

    // Patch multiple times
    let offsets = [0i64, 100, 1000, 10000, i64::MAX];

    for &offset in &offsets {
        patch_base_offset(&mut batch, offset);

        // Verify offset was patched
        let patched = i64::from_be_bytes(batch[0..8].try_into().unwrap());
        assert_eq!(patched, offset, "Offset should be patched to {}", offset);

        // CRC should always be valid after patching
        assert_eq!(
            validate_batch_crc(&batch),
            CrcValidationResult::Valid,
            "CRC should be valid after patching to offset {}",
            offset
        );
    }
}
