//! Integration tests for fetch and produce response types.

use bytes::Bytes;

use kafkaesque::error::KafkaCode;
use kafkaesque::server::response::{
    AbortedTransaction, FetchPartitionResponse, FetchResponseData, FetchTopicResponse,
    ProducePartitionResponse, ProduceResponseData, ProduceTopicResponse,
};

// ============================================================================
// FetchResponseData Tests
// ============================================================================

#[test]
fn test_fetch_response_with_topics() {
    let response = FetchResponseData {
        throttle_time_ms: 100,
        responses: vec![FetchTopicResponse {
            name: "my-topic".to_string(),
            partitions: vec![FetchPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                high_watermark: 1000,
                last_stable_offset: 1000,
                aborted_transactions: vec![],
                records: Some(Bytes::from(vec![1, 2, 3, 4])),
            }],
        }],
    };

    assert_eq!(response.throttle_time_ms, 100);
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].name, "my-topic");
    assert_eq!(response.responses[0].partitions.len(), 1);
}

#[test]
fn test_fetch_response_debug() {
    let response = FetchResponseData {
        throttle_time_ms: 0,
        responses: vec![],
    };
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("FetchResponseData"));
}

#[test]
fn test_fetch_response_clone() {
    let response = FetchResponseData {
        throttle_time_ms: 50,
        responses: vec![],
    };
    let cloned = response.clone();
    assert_eq!(response.throttle_time_ms, cloned.throttle_time_ms);
}

// ============================================================================
// FetchTopicResponse Tests
// ============================================================================

#[test]
fn test_fetch_topic_response_with_data() {
    let response = FetchTopicResponse {
        name: "test-topic".to_string(),
        partitions: vec![
            FetchPartitionResponse::success(0, 100, None),
            FetchPartitionResponse::success(1, 200, Some(Bytes::new())),
        ],
    };

    assert_eq!(response.name, "test-topic");
    assert_eq!(response.partitions.len(), 2);
}

#[test]
fn test_fetch_topic_response_debug() {
    let response = FetchTopicResponse {
        name: "".to_string(),
        partitions: vec![],
    };
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("FetchTopicResponse"));
}

#[test]
fn test_fetch_topic_response_clone() {
    let response = FetchTopicResponse {
        name: "t".to_string(),
        partitions: vec![],
    };
    let cloned = response.clone();
    assert_eq!(response.name, cloned.name);
}

// ============================================================================
// FetchPartitionResponse Tests
// ============================================================================

#[test]
fn test_fetch_partition_response_default() {
    let response = FetchPartitionResponse::default();
    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.high_watermark, 0);
    assert!(response.records.is_none());
}

#[test]
fn test_fetch_partition_response_error() {
    let response = FetchPartitionResponse::error(5, KafkaCode::NotLeaderForPartition);

    assert_eq!(response.partition_index, 5);
    assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
    assert_eq!(response.high_watermark, -1);
    assert!(response.records.is_none());
}

#[test]
fn test_fetch_partition_response_success() {
    let records = Bytes::from(vec![0u8; 100]);
    let response = FetchPartitionResponse::success(0, 1000, Some(records.clone()));

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.high_watermark, 1000);
    assert_eq!(response.last_stable_offset, 1000);
    assert!(response.records.is_some());
    assert_eq!(response.records.unwrap().len(), 100);
}

#[test]
fn test_fetch_partition_response_with_aborted() {
    let response = FetchPartitionResponse {
        partition_index: 0,
        error_code: KafkaCode::None,
        high_watermark: 100,
        last_stable_offset: 100,
        aborted_transactions: vec![
            AbortedTransaction {
                producer_id: 123,
                first_offset: 50,
            },
            AbortedTransaction {
                producer_id: 456,
                first_offset: 75,
            },
        ],
        records: None,
    };

    assert_eq!(response.aborted_transactions.len(), 2);
    assert_eq!(response.aborted_transactions[0].producer_id, 123);
    assert_eq!(response.aborted_transactions[1].first_offset, 75);
}

#[test]
fn test_fetch_partition_response_debug() {
    let response = FetchPartitionResponse::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("FetchPartitionResponse"));
}

#[test]
fn test_fetch_partition_response_clone() {
    let response = FetchPartitionResponse {
        partition_index: 3,
        error_code: KafkaCode::None,
        high_watermark: 500,
        last_stable_offset: 500,
        aborted_transactions: vec![],
        records: Some(Bytes::from(vec![1, 2, 3])),
    };
    let cloned = response.clone();
    assert_eq!(response.partition_index, cloned.partition_index);
    assert_eq!(response.records, cloned.records);
}

// ============================================================================
// AbortedTransaction Tests
// ============================================================================

#[test]
fn test_aborted_transaction_with_values() {
    let aborted = AbortedTransaction {
        producer_id: 999,
        first_offset: 12345,
    };
    assert_eq!(aborted.producer_id, 999);
    assert_eq!(aborted.first_offset, 12345);
}

#[test]
fn test_aborted_transaction_debug() {
    let aborted = AbortedTransaction {
        producer_id: 0,
        first_offset: 0,
    };
    let debug_str = format!("{:?}", aborted);
    assert!(debug_str.contains("AbortedTransaction"));
}

#[test]
fn test_aborted_transaction_clone() {
    let aborted = AbortedTransaction {
        producer_id: 100,
        first_offset: 200,
    };
    let cloned = aborted.clone();
    assert_eq!(aborted.producer_id, cloned.producer_id);
}

// ============================================================================
// ProduceResponseData Tests
// ============================================================================

#[test]
fn test_produce_response_empty() {
    let response = ProduceResponseData {
        responses: vec![],
        throttle_time_ms: 0,
    };
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.responses.is_empty());
}

#[test]
fn test_produce_response_with_topics() {
    let response = ProduceResponseData {
        throttle_time_ms: 0,
        responses: vec![ProduceTopicResponse {
            name: "my-topic".to_string(),
            partitions: vec![ProducePartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                base_offset: 100,
                log_append_time: -1,
            }],
        }],
    };

    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].name, "my-topic");
}

#[test]
fn test_produce_response_debug() {
    let response = ProduceResponseData {
        responses: vec![],
        throttle_time_ms: 0,
    };
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("ProduceResponseData"));
}

#[test]
fn test_produce_response_clone() {
    let response = ProduceResponseData {
        responses: vec![],
        throttle_time_ms: 100,
    };
    let cloned = response.clone();
    assert_eq!(response.throttle_time_ms, cloned.throttle_time_ms);
}

// ============================================================================
// ProduceTopicResponse Tests
// ============================================================================

#[test]
fn test_produce_topic_response_empty() {
    let response = ProduceTopicResponse {
        name: "".to_string(),
        partitions: vec![],
    };
    assert!(response.name.is_empty());
    assert!(response.partitions.is_empty());
}

#[test]
fn test_produce_topic_response_with_partitions() {
    let response = ProduceTopicResponse {
        name: "test-topic".to_string(),
        partitions: vec![
            ProducePartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                base_offset: 0,
                log_append_time: -1,
            },
            ProducePartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::None,
                base_offset: 100,
                log_append_time: -1,
            },
        ],
    };

    assert_eq!(response.name, "test-topic");
    assert_eq!(response.partitions.len(), 2);
}

#[test]
fn test_produce_topic_response_debug() {
    let response = ProduceTopicResponse {
        name: "".to_string(),
        partitions: vec![],
    };
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("ProduceTopicResponse"));
}

#[test]
fn test_produce_topic_response_clone() {
    let response = ProduceTopicResponse {
        name: "t".to_string(),
        partitions: vec![],
    };
    let cloned = response.clone();
    assert_eq!(response.name, cloned.name);
}

// ============================================================================
// ProducePartitionResponse Tests
// ============================================================================

#[test]
fn test_produce_partition_response_default() {
    let response = ProducePartitionResponse::default();
    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.base_offset, 0);
}

#[test]
fn test_produce_partition_response_success() {
    let response = ProducePartitionResponse::success(5, 12345);

    assert_eq!(response.partition_index, 5);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.base_offset, 12345);
    assert_eq!(response.log_append_time, -1);
}

#[test]
fn test_produce_partition_response_error() {
    let response = ProducePartitionResponse::error(0, KafkaCode::NotLeaderForPartition);

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
    assert_eq!(response.base_offset, -1);
}

#[test]
fn test_produce_partition_response_debug() {
    let response = ProducePartitionResponse::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("ProducePartitionResponse"));
}

#[test]
fn test_produce_partition_response_clone() {
    let response = ProducePartitionResponse {
        partition_index: 3,
        error_code: KafkaCode::None,
        base_offset: 999,
        log_append_time: -1,
    };
    let cloned = response.clone();
    assert_eq!(response.partition_index, cloned.partition_index);
    assert_eq!(response.base_offset, cloned.base_offset);
}
