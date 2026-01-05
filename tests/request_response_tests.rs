//! Tests for request parsing and response encoding.
//!
//! These tests verify the serialization/deserialization of Kafka protocol
//! messages including Fetch, Produce, and Offset operations.

use bytes::Bytes;
use nombytes::NomBytes;

use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::{
    parse_fetch_request, parse_list_offsets_request, parse_offset_commit_request,
    parse_offset_fetch_request, parse_produce_request,
};
use kafkaesque::server::response::{
    FetchPartitionResponse, FetchResponseData, FetchTopicResponse, ListOffsetsPartitionResponse,
    ListOffsetsResponseData, ListOffsetsTopicResponse, OffsetCommitPartitionResponse,
    OffsetCommitResponseData, OffsetCommitTopicResponse, OffsetFetchPartitionResponse,
    OffsetFetchResponseData, OffsetFetchTopicResponse, ProducePartitionResponse,
    ProduceResponseData, ProduceTopicResponse,
};

// ============================================================================
// Helper Functions
// ============================================================================

/// Encode a string in Kafka format (length-prefixed).
fn encode_string(s: &str) -> Vec<u8> {
    let len = s.len() as i16;
    let mut result = Vec::new();
    result.extend_from_slice(&len.to_be_bytes());
    result.extend_from_slice(s.as_bytes());
    result
}

/// Encode a nullable string in Kafka format (-1 for null).
fn encode_nullable_string(s: Option<&str>) -> Vec<u8> {
    match s {
        Some(val) => encode_string(val),
        None => (-1i16).to_be_bytes().to_vec(),
    }
}

// ============================================================================
// Fetch Request Parsing Tests
// ============================================================================

#[test]
fn test_parse_fetch_request_empty() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    data.extend_from_slice(&5000i32.to_be_bytes()); // max_wait_ms
    data.extend_from_slice(&1i32.to_be_bytes()); // min_bytes
    data.extend_from_slice(&1048576i32.to_be_bytes()); // max_bytes
    data.extend_from_slice(&0i8.to_be_bytes()); // isolation_level
    data.extend_from_slice(&0i32.to_be_bytes()); // topics array (empty)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_fetch_request(input, 4).unwrap();

    assert_eq!(request.replica_id, -1);
    assert_eq!(request.max_wait_ms, 5000);
    assert_eq!(request.min_bytes, 1);
    assert_eq!(request.max_bytes, 1048576);
    assert_eq!(request.isolation_level, 0);
    assert!(request.topics.is_empty());
}

#[test]
fn test_parse_fetch_request_with_topic() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    data.extend_from_slice(&100i32.to_be_bytes()); // max_wait_ms
    data.extend_from_slice(&1i32.to_be_bytes()); // min_bytes
    data.extend_from_slice(&1024i32.to_be_bytes()); // max_bytes
    data.extend_from_slice(&1i8.to_be_bytes()); // isolation_level

    // Topics array (1 topic)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("test-topic"));

    // Partitions array (1 partition)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend_from_slice(&0i32.to_be_bytes()); // partition_index
    data.extend_from_slice(&100i64.to_be_bytes()); // fetch_offset
    data.extend_from_slice(&1024i32.to_be_bytes()); // partition_max_bytes

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_fetch_request(input, 4).unwrap();

    assert_eq!(request.isolation_level, 1);
    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].name, "test-topic");
    assert_eq!(request.topics[0].partitions.len(), 1);
    assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    assert_eq!(request.topics[0].partitions[0].fetch_offset, 100);
    assert_eq!(request.topics[0].partitions[0].partition_max_bytes, 1024);
}

#[test]
fn test_parse_fetch_request_multiple_partitions() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes());
    data.extend_from_slice(&100i32.to_be_bytes());
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend_from_slice(&1024i32.to_be_bytes());
    data.extend_from_slice(&0i8.to_be_bytes());

    // Topics array (1 topic)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("test-topic"));

    // Partitions array (3 partitions)
    data.extend_from_slice(&3i32.to_be_bytes());
    for i in 0i32..3 {
        data.extend_from_slice(&i.to_be_bytes());
        data.extend_from_slice(&((i * 100) as i64).to_be_bytes());
        data.extend_from_slice(&1024i32.to_be_bytes());
    }

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_fetch_request(input, 4).unwrap();

    assert_eq!(request.topics[0].partitions.len(), 3);
    for i in 0..3 {
        assert_eq!(request.topics[0].partitions[i].partition_index, i as i32);
        assert_eq!(
            request.topics[0].partitions[i].fetch_offset,
            (i * 100) as i64
        );
    }
}

// ============================================================================
// Produce Request Parsing Tests
// ============================================================================

#[test]
fn test_parse_produce_request_empty() {
    let mut data = Vec::new();
    data.extend(encode_nullable_string(None)); // transactional_id
    data.extend_from_slice(&(-1i16).to_be_bytes()); // acks
    data.extend_from_slice(&5000i32.to_be_bytes()); // timeout_ms
    data.extend_from_slice(&0i32.to_be_bytes()); // topics array (empty)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_produce_request(input, 3).unwrap();

    assert!(request.transactional_id.is_none());
    assert_eq!(request.acks, -1);
    assert_eq!(request.timeout_ms, 5000);
    assert!(request.topics.is_empty());
}

#[test]
fn test_parse_produce_request_with_transactional_id() {
    let mut data = Vec::new();
    data.extend(encode_nullable_string(Some("txn-1"))); // transactional_id
    data.extend_from_slice(&1i16.to_be_bytes()); // acks
    data.extend_from_slice(&3000i32.to_be_bytes()); // timeout_ms
    data.extend_from_slice(&0i32.to_be_bytes()); // topics array (empty)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_produce_request(input, 3).unwrap();

    assert_eq!(request.transactional_id, Some("txn-1".to_string()));
    assert_eq!(request.acks, 1);
    assert_eq!(request.timeout_ms, 3000);
}

#[test]
fn test_parse_produce_request_with_records() {
    let mut data = Vec::new();
    data.extend(encode_nullable_string(None));
    data.extend_from_slice(&1i16.to_be_bytes());
    data.extend_from_slice(&5000i32.to_be_bytes());

    // Topics array (1 topic)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("test-topic"));

    // Partitions array (1 partition)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend_from_slice(&0i32.to_be_bytes()); // partition_index

    let records = vec![1u8, 2, 3, 4, 5]; // sample record data
    data.extend_from_slice(&(records.len() as i32).to_be_bytes()); // record_set_size
    data.extend(&records);

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_produce_request(input, 3).unwrap();

    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].name, "test-topic");
    assert_eq!(request.topics[0].partitions.len(), 1);
    assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    assert_eq!(
        request.topics[0].partitions[0].records.as_ref(),
        &records[..]
    );
}

// ============================================================================
// List Offsets Request Parsing Tests
// ============================================================================

#[test]
fn test_parse_list_offsets_request_v0() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    // No isolation_level in v0
    data.extend_from_slice(&0i32.to_be_bytes()); // topics array (empty)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_list_offsets_request(input, 0).unwrap();

    assert_eq!(request.replica_id, -1);
    assert_eq!(request.isolation_level, 0); // default
    assert!(request.topics.is_empty());
}

#[test]
fn test_parse_list_offsets_request_v2() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes()); // replica_id
    data.extend_from_slice(&1i8.to_be_bytes()); // isolation_level (v2+)
    data.extend_from_slice(&0i32.to_be_bytes()); // topics array (empty)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_list_offsets_request(input, 2).unwrap();

    assert_eq!(request.replica_id, -1);
    assert_eq!(request.isolation_level, 1);
}

#[test]
fn test_parse_list_offsets_request_with_topic() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes());
    data.extend_from_slice(&0i8.to_be_bytes());

    // Topics array (1 topic)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("test-topic"));

    // Partitions array (1 partition)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend_from_slice(&0i32.to_be_bytes()); // partition_index
    data.extend_from_slice(&(-1i64).to_be_bytes()); // timestamp (-1 = latest)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_list_offsets_request(input, 2).unwrap();

    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].name, "test-topic");
    assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    assert_eq!(request.topics[0].partitions[0].timestamp, -1);
}

#[test]
fn test_parse_list_offsets_request_earliest() {
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes());
    data.extend_from_slice(&0i8.to_be_bytes());
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("test-topic"));
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend_from_slice(&0i32.to_be_bytes());
    data.extend_from_slice(&(-2i64).to_be_bytes()); // timestamp (-2 = earliest)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_list_offsets_request(input, 2).unwrap();

    assert_eq!(request.topics[0].partitions[0].timestamp, -2);
}

// ============================================================================
// Offset Commit Request Parsing Tests
// ============================================================================

#[test]
fn test_parse_offset_commit_request() {
    let mut data = Vec::new();
    data.extend(encode_string("test-group")); // group_id
    data.extend_from_slice(&1i32.to_be_bytes()); // generation_id
    data.extend(encode_string("member-1")); // member_id
    data.extend_from_slice(&0i32.to_be_bytes()); // topics array (empty)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_offset_commit_request(input, 1).unwrap();

    assert_eq!(request.group_id, "test-group");
    assert_eq!(request.generation_id, 1);
    assert_eq!(request.member_id, "member-1");
    assert!(request.topics.is_empty());
}

#[test]
fn test_parse_offset_commit_request_with_topics() {
    let mut data = Vec::new();
    data.extend(encode_string("test-group"));
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("member-1"));

    // Topics array (1 topic)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("test-topic"));

    // Partitions array (1 partition)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend_from_slice(&0i32.to_be_bytes()); // partition_index
    data.extend_from_slice(&100i64.to_be_bytes()); // committed_offset
    data.extend(encode_nullable_string(Some("metadata"))); // committed_metadata

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_offset_commit_request(input, 1).unwrap();

    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].name, "test-topic");
    assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    assert_eq!(request.topics[0].partitions[0].committed_offset, 100);
    assert_eq!(
        request.topics[0].partitions[0].committed_metadata,
        Some("metadata".to_string())
    );
}

#[test]
fn test_parse_offset_commit_request_null_metadata() {
    let mut data = Vec::new();
    data.extend(encode_string("test-group"));
    data.extend_from_slice(&(-1i32).to_be_bytes()); // anonymous
    data.extend(encode_string(""));

    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("test-topic"));
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend_from_slice(&0i32.to_be_bytes());
    data.extend_from_slice(&50i64.to_be_bytes());
    data.extend(encode_nullable_string(None)); // null metadata

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_offset_commit_request(input, 1).unwrap();

    assert_eq!(request.topics[0].partitions[0].committed_offset, 50);
    assert!(request.topics[0].partitions[0].committed_metadata.is_none());
}

// ============================================================================
// Offset Fetch Request Parsing Tests
// ============================================================================

#[test]
fn test_parse_offset_fetch_request_empty() {
    let mut data = Vec::new();
    data.extend(encode_string("test-group"));
    data.extend_from_slice(&0i32.to_be_bytes()); // topics array (empty)

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_offset_fetch_request(input, 1).unwrap();

    assert_eq!(request.group_id, "test-group");
    assert!(request.topics.is_empty());
}

#[test]
fn test_parse_offset_fetch_request_with_topics() {
    let mut data = Vec::new();
    data.extend(encode_string("test-group"));

    // Topics array (1 topic)
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend(encode_string("test-topic"));

    // Partition indexes array
    data.extend_from_slice(&3i32.to_be_bytes()); // 3 partitions
    data.extend_from_slice(&0i32.to_be_bytes());
    data.extend_from_slice(&1i32.to_be_bytes());
    data.extend_from_slice(&2i32.to_be_bytes());

    let input = NomBytes::from(&data[..]);
    let (_, request) = parse_offset_fetch_request(input, 1).unwrap();

    assert_eq!(request.topics.len(), 1);
    assert_eq!(request.topics[0].name, "test-topic");
    assert_eq!(request.topics[0].partition_indexes, vec![0, 1, 2]);
}

// ============================================================================
// Fetch Response Construction Tests
// ============================================================================

#[test]
fn test_fetch_partition_response_error() {
    let response = FetchPartitionResponse::error(0, KafkaCode::NotLeaderForPartition);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
    assert_eq!(response.high_watermark, -1);
    assert_eq!(response.last_stable_offset, -1);
    assert!(response.records.is_none());
}

#[test]
fn test_fetch_partition_response_success() {
    let records = Bytes::from(vec![1, 2, 3]);
    let response = FetchPartitionResponse::success(0, 100, Some(records.clone()));

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.high_watermark, 100);
    assert_eq!(response.last_stable_offset, 100);
    assert_eq!(response.records, Some(records));
}

#[test]
fn test_fetch_response_construction() {
    let response = FetchResponseData {
        throttle_time_ms: 0,
        responses: vec![FetchTopicResponse {
            name: "test".to_string(),
            partitions: vec![FetchPartitionResponse::success(0, 100, None)],
        }],
    };

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].name, "test");
    assert_eq!(response.responses[0].partitions.len(), 1);
}

// ============================================================================
// Produce Response Construction Tests
// ============================================================================

#[test]
fn test_produce_partition_response_error() {
    let response = ProducePartitionResponse::error(0, KafkaCode::UnknownTopicOrPartition);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::UnknownTopicOrPartition);
    assert_eq!(response.base_offset, -1);
    assert_eq!(response.log_append_time, -1);
}

#[test]
fn test_produce_partition_response_success() {
    let response = ProducePartitionResponse::success(0, 50);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.base_offset, 50);
    assert_eq!(response.log_append_time, -1);
}

#[test]
fn test_produce_response_construction() {
    let response = ProduceResponseData {
        responses: vec![ProduceTopicResponse {
            name: "test".to_string(),
            partitions: vec![ProducePartitionResponse::success(0, 100)],
        }],
        throttle_time_ms: 0,
    };

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].name, "test");
}

// ============================================================================
// List Offsets Response Construction Tests
// ============================================================================

#[test]
fn test_list_offsets_partition_response_error() {
    let response = ListOffsetsPartitionResponse::error(0, KafkaCode::NotLeaderForPartition);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
    assert_eq!(response.timestamp, -1);
    assert_eq!(response.offset, -1);
}

#[test]
fn test_list_offsets_partition_response_success() {
    let response = ListOffsetsPartitionResponse::success(0, 100);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.timestamp, -1);
    assert_eq!(response.offset, 100);
}

#[test]
fn test_list_offsets_response_construction() {
    let response = ListOffsetsResponseData {
        throttle_time_ms: 0,
        topics: vec![ListOffsetsTopicResponse {
            name: "test".to_string(),
            partitions: vec![ListOffsetsPartitionResponse::success(0, 100)],
        }],
    };

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name, "test");
}

// ============================================================================
// Offset Commit Response Construction Tests
// ============================================================================

#[test]
fn test_offset_commit_partition_response_new() {
    let response = OffsetCommitPartitionResponse::new(0, KafkaCode::IllegalGeneration);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::IllegalGeneration);
}

#[test]
fn test_offset_commit_partition_response_success() {
    let response = OffsetCommitPartitionResponse::success(0);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
}

#[test]
fn test_offset_commit_response_construction() {
    let response = OffsetCommitResponseData {
        throttle_time_ms: 0,
        topics: vec![OffsetCommitTopicResponse {
            name: "test".to_string(),
            partitions: vec![OffsetCommitPartitionResponse::success(0)],
        }],
    };

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), 1);
}

// ============================================================================
// Offset Fetch Response Construction Tests
// ============================================================================

#[test]
fn test_offset_fetch_partition_response_new() {
    let response = OffsetFetchPartitionResponse::new(0, 100, Some("metadata".to_string()));

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.committed_offset, 100);
    assert_eq!(response.metadata, Some("metadata".to_string()));
    assert_eq!(response.error_code, KafkaCode::None);
}

#[test]
fn test_offset_fetch_partition_response_unknown() {
    let response = OffsetFetchPartitionResponse::unknown(0);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.committed_offset, -1);
    assert!(response.metadata.is_none());
    assert_eq!(response.error_code, KafkaCode::None);
}

#[test]
fn test_offset_fetch_response_construction() {
    let response = OffsetFetchResponseData {
        throttle_time_ms: 0,
        topics: vec![OffsetFetchTopicResponse {
            name: "test".to_string(),
            partitions: vec![OffsetFetchPartitionResponse::new(0, 100, None)],
        }],
        error_code: KafkaCode::None,
    };

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.error_code, KafkaCode::None);
}

// ============================================================================
// Default Implementations Tests
// ============================================================================

#[test]
fn test_list_offsets_response_default() {
    let response = ListOffsetsResponseData::default();
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.topics.is_empty());
}

#[test]
fn test_list_offsets_partition_response_default() {
    let response = ListOffsetsPartitionResponse::default();
    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.timestamp, 0);
    assert_eq!(response.offset, 0);
}

#[test]
fn test_offset_commit_response_default() {
    let response = OffsetCommitResponseData::default();
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.topics.is_empty());
}

#[test]
fn test_offset_fetch_response_default() {
    let response = OffsetFetchResponseData::default();
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.topics.is_empty());
    assert_eq!(response.error_code, KafkaCode::None);
}

#[test]
fn test_produce_partition_response_default() {
    let response = ProducePartitionResponse::default();
    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.base_offset, 0);
    assert_eq!(response.log_append_time, 0);
}

#[test]
fn test_fetch_partition_response_default() {
    let response = FetchPartitionResponse::default();
    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.high_watermark, 0);
    assert!(response.records.is_none());
}
