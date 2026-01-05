//! Integration tests for offset response types.

use kafkaesque::error::KafkaCode;
use kafkaesque::server::response::{
    ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
    OffsetCommitPartitionResponse, OffsetCommitResponseData, OffsetCommitTopicResponse,
    OffsetFetchPartitionResponse, OffsetFetchResponseData, OffsetFetchTopicResponse,
};

// ============================================================================
// ListOffsetsResponseData Tests
// ============================================================================

#[test]
fn test_list_offsets_response_default() {
    let response = ListOffsetsResponseData::default();
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.topics.is_empty());
}

#[test]
fn test_list_offsets_response_with_topics() {
    let response = ListOffsetsResponseData {
        throttle_time_ms: 0,
        topics: vec![ListOffsetsTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![ListOffsetsPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                timestamp: -1,
                offset: 1000,
            }],
        }],
    };

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name, "test-topic");
}

#[test]
fn test_list_offsets_response_debug() {
    let response = ListOffsetsResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("ListOffsetsResponseData"));
}

#[test]
fn test_list_offsets_response_clone() {
    let response = ListOffsetsResponseData::default();
    let cloned = response.clone();
    assert_eq!(response.throttle_time_ms, cloned.throttle_time_ms);
}

// ============================================================================
// ListOffsetsTopicResponse Tests
// ============================================================================

#[test]
fn test_list_offsets_topic_response_with_data() {
    let topic = ListOffsetsTopicResponse {
        name: "my-topic".to_string(),
        partitions: vec![
            ListOffsetsPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                timestamp: 1609459200000,
                offset: 500,
            },
            ListOffsetsPartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::None,
                timestamp: 1609459200000,
                offset: 750,
            },
        ],
    };

    assert_eq!(topic.name, "my-topic");
    assert_eq!(topic.partitions.len(), 2);
}

#[test]
fn test_list_offsets_topic_response_debug() {
    let topic = ListOffsetsTopicResponse {
        name: "".to_string(),
        partitions: vec![],
    };
    let debug_str = format!("{:?}", topic);
    assert!(debug_str.contains("ListOffsetsTopicResponse"));
}

#[test]
fn test_list_offsets_topic_response_clone() {
    let topic = ListOffsetsTopicResponse {
        name: "t".to_string(),
        partitions: vec![],
    };
    let cloned = topic.clone();
    assert_eq!(topic.name, cloned.name);
}

// ============================================================================
// ListOffsetsPartitionResponse Tests
// ============================================================================

#[test]
fn test_list_offsets_partition_response_default() {
    let partition = ListOffsetsPartitionResponse::default();
    assert_eq!(partition.partition_index, 0);
    assert_eq!(partition.error_code, KafkaCode::None);
    assert_eq!(partition.offset, 0);
}

#[test]
fn test_list_offsets_partition_response_earliest() {
    let partition = ListOffsetsPartitionResponse {
        partition_index: 0,
        error_code: KafkaCode::None,
        timestamp: -2, // EARLIEST
        offset: 0,
    };

    assert_eq!(partition.timestamp, -2);
    assert_eq!(partition.offset, 0);
}

#[test]
fn test_list_offsets_partition_response_latest() {
    let partition = ListOffsetsPartitionResponse {
        partition_index: 0,
        error_code: KafkaCode::None,
        timestamp: -1, // LATEST
        offset: 99999,
    };

    assert_eq!(partition.timestamp, -1);
    assert_eq!(partition.offset, 99999);
}

#[test]
fn test_list_offsets_partition_response_error() {
    let partition = ListOffsetsPartitionResponse::error(5, KafkaCode::NotLeaderForPartition);

    assert_eq!(partition.partition_index, 5);
    assert_eq!(partition.error_code, KafkaCode::NotLeaderForPartition);
    assert_eq!(partition.offset, -1);
}

#[test]
fn test_list_offsets_partition_response_success() {
    let partition = ListOffsetsPartitionResponse::success(3, 12345);

    assert_eq!(partition.partition_index, 3);
    assert_eq!(partition.error_code, KafkaCode::None);
    assert_eq!(partition.offset, 12345);
}

#[test]
fn test_list_offsets_partition_response_debug() {
    let partition = ListOffsetsPartitionResponse::default();
    let debug_str = format!("{:?}", partition);
    assert!(debug_str.contains("ListOffsetsPartitionResponse"));
}

#[test]
fn test_list_offsets_partition_response_clone() {
    let partition = ListOffsetsPartitionResponse {
        partition_index: 1,
        error_code: KafkaCode::None,
        timestamp: 1000,
        offset: 500,
    };
    let cloned = partition.clone();
    assert_eq!(partition.partition_index, cloned.partition_index);
    assert_eq!(partition.offset, cloned.offset);
}

// ============================================================================
// OffsetCommitResponseData Tests
// ============================================================================

#[test]
fn test_offset_commit_response_default() {
    let response = OffsetCommitResponseData::default();
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.topics.is_empty());
}

#[test]
fn test_offset_commit_response_with_topics() {
    let response = OffsetCommitResponseData {
        throttle_time_ms: 0,
        topics: vec![OffsetCommitTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![OffsetCommitPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
            }],
        }],
    };

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions[0].error_code, KafkaCode::None);
}

#[test]
fn test_offset_commit_response_debug() {
    let response = OffsetCommitResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("OffsetCommitResponseData"));
}

#[test]
fn test_offset_commit_response_clone() {
    let response = OffsetCommitResponseData::default();
    let cloned = response.clone();
    assert_eq!(response.throttle_time_ms, cloned.throttle_time_ms);
}

// ============================================================================
// OffsetCommitTopicResponse Tests
// ============================================================================

#[test]
fn test_offset_commit_topic_response_with_partitions() {
    let topic = OffsetCommitTopicResponse {
        name: "committed-topic".to_string(),
        partitions: vec![
            OffsetCommitPartitionResponse::success(0),
            OffsetCommitPartitionResponse::success(1),
        ],
    };

    assert_eq!(topic.name, "committed-topic");
    assert_eq!(topic.partitions.len(), 2);
}

#[test]
fn test_offset_commit_topic_response_debug() {
    let topic = OffsetCommitTopicResponse {
        name: "".to_string(),
        partitions: vec![],
    };
    let debug_str = format!("{:?}", topic);
    assert!(debug_str.contains("OffsetCommitTopicResponse"));
}

#[test]
fn test_offset_commit_topic_response_clone() {
    let topic = OffsetCommitTopicResponse {
        name: "t".to_string(),
        partitions: vec![],
    };
    let cloned = topic.clone();
    assert_eq!(topic.name, cloned.name);
}

// ============================================================================
// OffsetCommitPartitionResponse Tests
// ============================================================================

#[test]
fn test_offset_commit_partition_response_new() {
    let partition = OffsetCommitPartitionResponse::new(5, KafkaCode::IllegalGeneration);

    assert_eq!(partition.partition_index, 5);
    assert_eq!(partition.error_code, KafkaCode::IllegalGeneration);
}

#[test]
fn test_offset_commit_partition_response_success() {
    let partition = OffsetCommitPartitionResponse::success(3);

    assert_eq!(partition.partition_index, 3);
    assert_eq!(partition.error_code, KafkaCode::None);
}

#[test]
fn test_offset_commit_partition_response_debug() {
    let partition = OffsetCommitPartitionResponse::success(0);
    let debug_str = format!("{:?}", partition);
    assert!(debug_str.contains("OffsetCommitPartitionResponse"));
}

#[test]
fn test_offset_commit_partition_response_clone() {
    let partition = OffsetCommitPartitionResponse {
        partition_index: 3,
        error_code: KafkaCode::None,
    };
    let cloned = partition.clone();
    assert_eq!(partition.partition_index, cloned.partition_index);
}

// ============================================================================
// OffsetFetchResponseData Tests
// ============================================================================

#[test]
fn test_offset_fetch_response_default() {
    let response = OffsetFetchResponseData::default();
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.topics.is_empty());
}

#[test]
fn test_offset_fetch_response_with_topics() {
    let response = OffsetFetchResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        topics: vec![OffsetFetchTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![OffsetFetchPartitionResponse::new(0, 1000, None)],
        }],
    };

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions[0].committed_offset, 1000);
}

#[test]
fn test_offset_fetch_response_debug() {
    let response = OffsetFetchResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("OffsetFetchResponseData"));
}

#[test]
fn test_offset_fetch_response_clone() {
    let response = OffsetFetchResponseData::default();
    let cloned = response.clone();
    assert_eq!(response.error_code, cloned.error_code);
}

// ============================================================================
// OffsetFetchTopicResponse Tests
// ============================================================================

#[test]
fn test_offset_fetch_topic_response_with_partitions() {
    let topic = OffsetFetchTopicResponse {
        name: "fetched-topic".to_string(),
        partitions: vec![OffsetFetchPartitionResponse::new(0, 500, None)],
    };

    assert_eq!(topic.name, "fetched-topic");
    assert_eq!(topic.partitions.len(), 1);
}

#[test]
fn test_offset_fetch_topic_response_debug() {
    let topic = OffsetFetchTopicResponse {
        name: "".to_string(),
        partitions: vec![],
    };
    let debug_str = format!("{:?}", topic);
    assert!(debug_str.contains("OffsetFetchTopicResponse"));
}

#[test]
fn test_offset_fetch_topic_response_clone() {
    let topic = OffsetFetchTopicResponse {
        name: "t".to_string(),
        partitions: vec![],
    };
    let cloned = topic.clone();
    assert_eq!(topic.name, cloned.name);
}

// ============================================================================
// OffsetFetchPartitionResponse Tests
// ============================================================================

#[test]
fn test_offset_fetch_partition_response_new() {
    let partition = OffsetFetchPartitionResponse::new(3, 12345, Some("metadata".to_string()));

    assert_eq!(partition.partition_index, 3);
    assert_eq!(partition.committed_offset, 12345);
    assert_eq!(partition.metadata, Some("metadata".to_string()));
    assert_eq!(partition.error_code, KafkaCode::None);
}

#[test]
fn test_offset_fetch_partition_response_unknown() {
    let partition = OffsetFetchPartitionResponse::unknown(0);

    assert_eq!(partition.partition_index, 0);
    assert_eq!(partition.committed_offset, -1);
    assert!(partition.metadata.is_none());
    assert_eq!(partition.error_code, KafkaCode::None);
}

#[test]
fn test_offset_fetch_partition_response_debug() {
    let partition = OffsetFetchPartitionResponse::unknown(0);
    let debug_str = format!("{:?}", partition);
    assert!(debug_str.contains("OffsetFetchPartitionResponse"));
}

#[test]
fn test_offset_fetch_partition_response_clone() {
    let partition = OffsetFetchPartitionResponse {
        partition_index: 1,
        committed_offset: 999,
        metadata: Some("meta".to_string()),
        error_code: KafkaCode::None,
    };
    let cloned = partition.clone();
    assert_eq!(partition.committed_offset, cloned.committed_offset);
    assert_eq!(partition.metadata, cloned.metadata);
}
