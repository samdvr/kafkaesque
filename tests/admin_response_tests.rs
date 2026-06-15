//! Integration tests for admin response types.
//!
//! These tests verify admin API response construction.

use kafkaesque::error::KafkaCode;
use kafkaesque::server::response::{
    CreateTopicsResponseData, DeleteTopicsResponseData, InitProducerIdResponseData,
};

// ============================================================================
// CreateTopicsResponseData Tests
// ============================================================================

#[test]
fn test_create_topics_response_default() {
    let response = CreateTopicsResponseData::default();
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.topics.is_empty());
}

#[test]
fn test_create_topics_response_debug() {
    let response = CreateTopicsResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("CreateTopicsResponseData"));
}

#[test]
fn test_create_topics_response_clone() {
    let response = CreateTopicsResponseData::default();
    let cloned = response.clone();
    assert_eq!(response.throttle_time_ms, cloned.throttle_time_ms);
}

// ============================================================================
// DeleteTopicsResponseData Tests
// ============================================================================

#[test]
fn test_delete_topics_response_default() {
    let response = DeleteTopicsResponseData::default();
    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.responses.is_empty());
}

#[test]
fn test_delete_topics_response_debug() {
    let response = DeleteTopicsResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("DeleteTopicsResponseData"));
}

#[test]
fn test_delete_topics_response_clone() {
    let response = DeleteTopicsResponseData::default();
    let cloned = response.clone();
    assert_eq!(response.throttle_time_ms, cloned.throttle_time_ms);
}

// ============================================================================
// InitProducerIdResponseData Tests
// ============================================================================

#[test]
fn test_init_producer_id_with_values() {
    let response = InitProducerIdResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        producer_id: 12345,
        producer_epoch: 0,
    };

    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.producer_id, 12345);
    assert_eq!(response.producer_epoch, 0);
    assert_eq!(response.throttle_time_ms, 0);
}

#[test]
fn test_init_producer_id_with_error() {
    let response = InitProducerIdResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::NotCoordinatorForGroup,
        producer_id: -1,
        producer_epoch: -1,
    };

    assert_eq!(response.error_code, KafkaCode::NotCoordinatorForGroup);
    assert_eq!(response.producer_id, -1);
    assert_eq!(response.producer_epoch, -1);
}

#[test]
fn test_init_producer_id_default() {
    let response = InitProducerIdResponseData::default();

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, KafkaCode::None);
}

#[test]
fn test_init_producer_id_debug() {
    let response = InitProducerIdResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("InitProducerIdResponseData"));
}

#[test]
fn test_init_producer_id_clone() {
    let response = InitProducerIdResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        producer_id: 1,
        producer_epoch: 0,
    };
    let cloned = response.clone();
    assert_eq!(response.producer_id, cloned.producer_id);
}
