//! Tests for BrokerInfo and validation utilities.

use kafkaesque::cluster::{BrokerInfo, validate_group_id, validate_topic_name};

// ============================================================================
// BrokerInfo Tests
// ============================================================================

#[test]
fn test_broker_info_creation() {
    let broker = BrokerInfo {
        broker_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        registered_at: 1234567890,
    };
    assert_eq!(broker.broker_id, 1);
    assert_eq!(broker.host, "localhost");
    assert_eq!(broker.port, 9092);
    assert_eq!(broker.registered_at, 1234567890);
}

#[test]
fn test_broker_info_clone() {
    let broker = BrokerInfo {
        broker_id: 2,
        host: "broker2.example.com".to_string(),
        port: 9093,
        registered_at: 1234567891,
    };
    let cloned = broker.clone();
    assert_eq!(broker.broker_id, cloned.broker_id);
    assert_eq!(broker.host, cloned.host);
    assert_eq!(broker.port, cloned.port);
    assert_eq!(broker.registered_at, cloned.registered_at);
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_validate_topic_name_valid() {
    assert!(validate_topic_name("my-topic").is_ok());
    assert!(validate_topic_name("my_topic_123").is_ok());
    assert!(validate_topic_name("MyTopic").is_ok());
    assert!(validate_topic_name("topic.with.dots").is_ok());
}

#[test]
fn test_validate_topic_name_invalid() {
    assert!(validate_topic_name("").is_err());
    assert!(validate_topic_name("invalid/topic").is_err());
    assert!(validate_topic_name("invalid:topic").is_err());
    assert!(validate_topic_name("a".repeat(300).as_str()).is_err());
}

#[test]
fn test_validate_group_id_valid() {
    assert!(validate_group_id("my-group").is_ok());
    assert!(validate_group_id("group_123").is_ok());
    assert!(validate_group_id("MyGroup").is_ok());
}

#[test]
fn test_validate_group_id_invalid() {
    assert!(validate_group_id("").is_err());
    assert!(validate_group_id("invalid/group").is_err());
    assert!(validate_group_id("invalid:group").is_err());
}
