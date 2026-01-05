//! Tests for SlateDBError Kafka code mappings and helper methods.

use kafkaesque::cluster::SlateDBError;
use kafkaesque::error::KafkaCode;

// ============================================================================
// SlateDBError Kafka Code Mapping Tests
// ============================================================================

#[test]
fn test_slatedb_error_to_kafka_code_raft() {
    let err = SlateDBError::Raft("raft consensus failed".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::Unknown);
}

#[test]
fn test_slatedb_error_to_kafka_code_storage() {
    let err = SlateDBError::Storage("storage write failed".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::Unknown);
}

#[test]
fn test_slatedb_error_to_kafka_code_io() {
    let io_err = std::io::Error::other("disk failure");
    let err = SlateDBError::Io(io_err);
    assert_eq!(err.to_kafka_code(), KafkaCode::Unknown);
}

#[test]
fn test_slatedb_error_to_kafka_code_slatedb() {
    let err = SlateDBError::SlateDB("compaction failed".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::Unknown);
}

#[test]
fn test_slatedb_error_to_kafka_code_serde() {
    let json_err = serde_json::from_str::<String>("invalid").unwrap_err();
    let err = SlateDBError::Serde(json_err);
    assert_eq!(err.to_kafka_code(), KafkaCode::CorruptMessage);
}

#[test]
fn test_slatedb_error_to_kafka_code_protocol() {
    let err = SlateDBError::Protocol("bad request format".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::CorruptMessage);
}

#[test]
fn test_slatedb_error_to_kafka_code_config() {
    let err = SlateDBError::Config("invalid configuration".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::InvalidTopic);
}

#[test]
fn test_slatedb_error_to_kafka_code_fenced_producer() {
    let err = SlateDBError::FencedProducer {
        producer_id: 123,
        expected_epoch: 5,
        actual_epoch: 3,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::OutOfOrderSequenceNumber);
}

#[test]
fn test_slatedb_error_to_kafka_code_sequence_overflow() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 456,
        topic: "test-topic".to_string(),
        partition: 2,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::OutOfOrderSequenceNumber);
}

#[test]
fn test_slatedb_error_to_kafka_code_flush_failed() {
    let err = SlateDBError::FlushFailed {
        topic: "topic".to_string(),
        partition: 0,
        message: "flush timeout".to_string(),
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::Unknown);
}

#[test]
fn test_slatedb_error_to_kafka_code_lease_too_short() {
    let err = SlateDBError::LeaseTooShort {
        topic: "topic".to_string(),
        partition: 0,
        remaining_secs: 5,
        required_secs: 10,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::NotLeaderForPartition);
}

// ============================================================================
// SlateDBError is_not_leader Tests
// ============================================================================

#[test]
fn test_slatedb_error_is_not_leader_lease_too_short() {
    let err = SlateDBError::LeaseTooShort {
        topic: "topic".to_string(),
        partition: 0,
        remaining_secs: 5,
        required_secs: 10,
    };
    assert!(err.is_not_leader());
}

#[test]
fn test_slatedb_error_is_not_leader_fenced() {
    let err = SlateDBError::Fenced;
    assert!(err.is_not_leader());
}

#[test]
fn test_slatedb_error_is_not_leader_not_owned() {
    let err = SlateDBError::NotOwned {
        topic: "topic".to_string(),
        partition: 0,
    };
    assert!(err.is_not_leader());
}

#[test]
fn test_slatedb_error_is_not_leader_other_errors() {
    let err = SlateDBError::Raft("failed".to_string());
    assert!(!err.is_not_leader());

    let err = SlateDBError::Storage("failed".to_string());
    assert!(!err.is_not_leader());

    let err = SlateDBError::Config("failed".to_string());
    assert!(!err.is_not_leader());
}

// ============================================================================
// SlateDBError is_retriable Tests
// ============================================================================

#[test]
fn test_slatedb_error_is_retriable_raft() {
    let err = SlateDBError::Raft("raft timeout".to_string());
    assert!(err.is_retriable());
}

#[test]
fn test_slatedb_error_is_retriable_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::TimedOut, "timed out");
    let err = SlateDBError::Io(io_err);
    assert!(err.is_retriable());
}

#[test]
fn test_slatedb_error_is_retriable_flush_failed() {
    let err = SlateDBError::FlushFailed {
        topic: "topic".to_string(),
        partition: 0,
        message: "timeout".to_string(),
    };
    assert!(err.is_retriable());
}

#[test]
fn test_slatedb_error_is_retriable_false() {
    let err = SlateDBError::Fenced;
    assert!(!err.is_retriable());

    let err = SlateDBError::Config("bad config".to_string());
    assert!(!err.is_retriable());

    let err = SlateDBError::Protocol("bad protocol".to_string());
    assert!(!err.is_retriable());
}

// ============================================================================
// SlateDBError is_idempotency_error Tests
// ============================================================================

#[test]
fn test_slatedb_error_is_idempotency_error_fenced_producer() {
    let err = SlateDBError::FencedProducer {
        producer_id: 1,
        expected_epoch: 2,
        actual_epoch: 1,
    };
    assert!(err.is_idempotency_error());
}

#[test]
fn test_slatedb_error_is_idempotency_error_sequence_overflow() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 1,
        topic: "t".to_string(),
        partition: 0,
    };
    assert!(err.is_idempotency_error());
}

#[test]
fn test_slatedb_error_is_idempotency_error_false() {
    let err = SlateDBError::Fenced;
    assert!(!err.is_idempotency_error());

    let err = SlateDBError::Raft("failed".to_string());
    assert!(!err.is_idempotency_error());
}

// ============================================================================
// SlateDBError Display Tests
// ============================================================================

#[test]
fn test_slatedb_error_display_raft() {
    let err = SlateDBError::Raft("consensus failure".to_string());
    assert!(err.to_string().contains("Raft error"));
    assert!(err.to_string().contains("consensus failure"));
}

#[test]
fn test_slatedb_error_display_duplicate_sequence() {
    let err = SlateDBError::DuplicateSequence {
        producer_id: 100,
        expected_sequence: 5,
        received_sequence: 3,
    };
    let display = err.to_string();
    assert!(display.contains("Duplicate"));
    assert!(display.contains("100"));
    assert!(display.contains("5"));
    assert!(display.contains("3"));
}

#[test]
fn test_slatedb_error_display_out_of_order() {
    let err = SlateDBError::OutOfOrderSequence {
        producer_id: 200,
        expected_sequence: 10,
        received_sequence: 15,
    };
    let display = err.to_string();
    assert!(display.contains("Out-of-order"));
    assert!(display.contains("200"));
    assert!(display.contains("10"));
    assert!(display.contains("15"));
}

#[test]
fn test_slatedb_error_display_fenced_producer() {
    let err = SlateDBError::FencedProducer {
        producer_id: 300,
        expected_epoch: 5,
        actual_epoch: 3,
    };
    let display = err.to_string();
    assert!(display.contains("Fenced producer"));
    assert!(display.contains("300"));
    assert!(display.contains("5"));
    assert!(display.contains("3"));
}

#[test]
fn test_slatedb_error_display_sequence_overflow() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 400,
        topic: "my-topic".to_string(),
        partition: 7,
    };
    let display = err.to_string();
    assert!(display.contains("Sequence overflow"));
    assert!(display.contains("400"));
    assert!(display.contains("my-topic"));
    assert!(display.contains("7"));
}

#[test]
fn test_slatedb_error_display_lease_too_short() {
    let err = SlateDBError::LeaseTooShort {
        topic: "topic-x".to_string(),
        partition: 3,
        remaining_secs: 5,
        required_secs: 10,
    };
    let display = err.to_string();
    assert!(display.contains("Lease too short"));
    assert!(display.contains("topic-x"));
    assert!(display.contains("3"));
    assert!(display.contains("5"));
    assert!(display.contains("10"));
}

#[test]
fn test_slatedb_error_display_flush_failed() {
    let err = SlateDBError::FlushFailed {
        topic: "flush-topic".to_string(),
        partition: 1,
        message: "disk busy".to_string(),
    };
    let display = err.to_string();
    assert!(display.contains("Flush failed"));
    assert!(display.contains("flush-topic"));
    assert!(display.contains("1"));
    assert!(display.contains("disk busy"));
}

#[test]
fn test_slatedb_error_display_protocol() {
    let err = SlateDBError::Protocol("invalid message format".to_string());
    let display = err.to_string();
    assert!(display.contains("Protocol error"));
    assert!(display.contains("invalid message format"));
}

// ============================================================================
// SlateDBError Debug Tests
// ============================================================================

#[test]
fn test_error_debug_format() {
    let err = SlateDBError::DuplicateSequence {
        producer_id: 1,
        expected_sequence: 2,
        received_sequence: 3,
    };
    let debug = format!("{:?}", err);
    assert!(debug.contains("DuplicateSequence"));
    assert!(debug.contains("producer_id"));
}

#[test]
fn test_error_debug_format_fenced_producer() {
    let err = SlateDBError::FencedProducer {
        producer_id: 1,
        expected_epoch: 2,
        actual_epoch: 3,
    };
    let debug = format!("{:?}", err);
    assert!(debug.contains("FencedProducer"));
}

#[test]
fn test_error_debug_format_sequence_overflow() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 1,
        topic: "t".to_string(),
        partition: 0,
    };
    let debug = format!("{:?}", err);
    assert!(debug.contains("SequenceOverflow"));
}

#[test]
fn test_error_debug_format_lease_too_short() {
    let err = SlateDBError::LeaseTooShort {
        topic: "t".to_string(),
        partition: 0,
        remaining_secs: 5,
        required_secs: 10,
    };
    let debug = format!("{:?}", err);
    assert!(debug.contains("LeaseTooShort"));
}

#[test]
fn test_error_debug_format_flush_failed() {
    let err = SlateDBError::FlushFailed {
        topic: "t".to_string(),
        partition: 0,
        message: "failed".to_string(),
    };
    let debug = format!("{:?}", err);
    assert!(debug.contains("FlushFailed"));
}

#[test]
fn test_error_debug_format_protocol() {
    let err = SlateDBError::Protocol("bad".to_string());
    let debug = format!("{:?}", err);
    assert!(debug.contains("Protocol"));
}

#[test]
fn test_error_debug_format_raft() {
    let err = SlateDBError::Raft("raft error".to_string());
    let debug = format!("{:?}", err);
    assert!(debug.contains("Raft"));
}
