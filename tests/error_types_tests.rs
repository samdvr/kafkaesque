//! Integration tests for error types.
//!
//! These tests verify error creation, display, and Kafka code mapping.

use kafkaesque::cluster::{FencingDetectionMethod, SlateDBError, detect_fencing_from_message};
use kafkaesque::error::KafkaCode;

// ============================================================================
// SlateDBError Creation Tests
// ============================================================================

#[test]
fn test_slatedb_error_slatedb() {
    let err = SlateDBError::SlateDB("test error message".to_string());
    let display = format!("{}", err);
    assert!(display.contains("SlateDB error"));
    assert!(display.contains("test error message"));
}

#[test]
fn test_slatedb_error_raft() {
    let err = SlateDBError::Raft("raft consensus failed".to_string());
    let display = format!("{}", err);
    assert!(display.contains("Raft error"));
    assert!(display.contains("raft consensus failed"));
}

#[test]
fn test_slatedb_error_storage() {
    let err = SlateDBError::Storage("disk full".to_string());
    let display = format!("{}", err);
    assert!(display.contains("Storage error"));
    assert!(display.contains("disk full"));
}

#[test]
fn test_slatedb_error_fenced() {
    let err = SlateDBError::Fenced;
    let display = format!("{}", err);
    assert!(display.contains("fenced"));
}

#[test]
fn test_slatedb_error_not_owned() {
    let err = SlateDBError::NotOwned {
        topic: "my-topic".to_string(),
        partition: 3,
    };
    let display = format!("{}", err);
    assert!(display.contains("my-topic"));
    assert!(display.contains("3"));
    assert!(display.contains("not owned"));
}

#[test]
fn test_slatedb_error_partition_not_found() {
    let err = SlateDBError::PartitionNotFound {
        topic: "missing-topic".to_string(),
        partition: 0,
    };
    let display = format!("{}", err);
    assert!(display.contains("missing-topic"));
    assert!(display.contains("not found"));
}

#[test]
fn test_slatedb_error_config() {
    let err = SlateDBError::Config("invalid port".to_string());
    let display = format!("{}", err);
    assert!(display.contains("Configuration error"));
    assert!(display.contains("invalid port"));
}

#[test]
fn test_slatedb_error_duplicate_sequence() {
    let err = SlateDBError::DuplicateSequence {
        producer_id: 123,
        expected_sequence: 5,
        received_sequence: 3,
    };
    let display = format!("{}", err);
    assert!(display.contains("Duplicate"));
    assert!(display.contains("123"));
    assert!(display.contains("5"));
    assert!(display.contains("3"));
}

#[test]
fn test_slatedb_error_out_of_order_sequence() {
    let err = SlateDBError::OutOfOrderSequence {
        producer_id: 456,
        expected_sequence: 10,
        received_sequence: 15,
    };
    let display = format!("{}", err);
    assert!(display.contains("Out-of-order"));
    assert!(display.contains("456"));
}

#[test]
fn test_slatedb_error_fenced_producer() {
    let err = SlateDBError::FencedProducer {
        producer_id: 789,
        expected_epoch: 5,
        actual_epoch: 3,
    };
    let display = format!("{}", err);
    assert!(display.contains("Fenced producer"));
    assert!(display.contains("789"));
}

#[test]
fn test_slatedb_error_lease_too_short() {
    let err = SlateDBError::LeaseTooShort {
        topic: "test-topic".to_string(),
        partition: 0,
        remaining_secs: 5,
        required_secs: 10,
    };
    let display = format!("{}", err);
    assert!(display.contains("Lease too short"));
    assert!(display.contains("test-topic"));
}

#[test]
fn test_slatedb_error_sequence_overflow() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 100,
        topic: "overflow-topic".to_string(),
        partition: 2,
    };
    let display = format!("{}", err);
    assert!(display.contains("Sequence overflow"));
}

#[test]
fn test_slatedb_error_flush_failed() {
    let err = SlateDBError::FlushFailed {
        topic: "flush-topic".to_string(),
        partition: 1,
        message: "IO error".to_string(),
    };
    let display = format!("{}", err);
    assert!(display.contains("Flush failed"));
    assert!(display.contains("IO error"));
}

#[test]
fn test_slatedb_error_protocol() {
    let err = SlateDBError::Protocol("invalid request format".to_string());
    let display = format!("{}", err);
    assert!(display.contains("Protocol error"));
}

#[test]
fn test_slatedb_error_debug() {
    let err = SlateDBError::Fenced;
    let debug_str = format!("{:?}", err);
    assert!(debug_str.contains("Fenced"));
}

// ============================================================================
// SlateDBError to_kafka_code Tests
// ============================================================================

#[test]
fn test_to_kafka_code_fenced() {
    assert_eq!(
        SlateDBError::Fenced.to_kafka_code(),
        KafkaCode::NotLeaderForPartition
    );
}

#[test]
fn test_to_kafka_code_not_owned() {
    let err = SlateDBError::NotOwned {
        topic: "t".to_string(),
        partition: 0,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::NotLeaderForPartition);
}

#[test]
fn test_to_kafka_code_lease_too_short() {
    let err = SlateDBError::LeaseTooShort {
        topic: "t".to_string(),
        partition: 0,
        remaining_secs: 1,
        required_secs: 10,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::NotLeaderForPartition);
}

#[test]
fn test_to_kafka_code_partition_not_found() {
    let err = SlateDBError::PartitionNotFound {
        topic: "t".to_string(),
        partition: 0,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::UnknownTopicOrPartition);
}

#[test]
fn test_to_kafka_code_duplicate_sequence() {
    let err = SlateDBError::DuplicateSequence {
        producer_id: 1,
        expected_sequence: 5,
        received_sequence: 3,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::DuplicateSequenceNumber);
}

#[test]
fn test_to_kafka_code_out_of_order_sequence() {
    let err = SlateDBError::OutOfOrderSequence {
        producer_id: 1,
        expected_sequence: 5,
        received_sequence: 10,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::OutOfOrderSequenceNumber);
}

#[test]
fn test_to_kafka_code_fenced_producer() {
    let err = SlateDBError::FencedProducer {
        producer_id: 1,
        expected_epoch: 2,
        actual_epoch: 1,
    };
    // Fenced producer uses OutOfOrderSequenceNumber as closest match
    assert_eq!(err.to_kafka_code(), KafkaCode::OutOfOrderSequenceNumber);
}

#[test]
fn test_to_kafka_code_sequence_overflow() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 1,
        topic: "t".to_string(),
        partition: 0,
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::OutOfOrderSequenceNumber);
}

#[test]
fn test_to_kafka_code_config() {
    let err = SlateDBError::Config("bad config".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::InvalidTopic);
}

#[test]
fn test_to_kafka_code_protocol() {
    let err = SlateDBError::Protocol("corrupt".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::CorruptMessage);
}

#[test]
fn test_to_kafka_code_storage() {
    let err = SlateDBError::Storage("error".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::Unknown);
}

#[test]
fn test_to_kafka_code_raft() {
    let err = SlateDBError::Raft("error".to_string());
    assert_eq!(err.to_kafka_code(), KafkaCode::Unknown);
}

#[test]
fn test_to_kafka_code_flush_failed() {
    let err = SlateDBError::FlushFailed {
        topic: "t".to_string(),
        partition: 0,
        message: "io".to_string(),
    };
    assert_eq!(err.to_kafka_code(), KafkaCode::Unknown);
}

// ============================================================================
// SlateDBError Classification Methods
// ============================================================================

#[test]
fn test_is_not_leader() {
    assert!(SlateDBError::Fenced.is_not_leader());
    assert!(
        SlateDBError::NotOwned {
            topic: "t".to_string(),
            partition: 0
        }
        .is_not_leader()
    );
    assert!(
        SlateDBError::LeaseTooShort {
            topic: "t".to_string(),
            partition: 0,
            remaining_secs: 1,
            required_secs: 10
        }
        .is_not_leader()
    );

    assert!(!SlateDBError::Storage("error".to_string()).is_not_leader());
    assert!(!SlateDBError::Config("error".to_string()).is_not_leader());
}

#[test]
fn test_is_retriable() {
    assert!(SlateDBError::Raft("error".to_string()).is_retriable());
    assert!(SlateDBError::Storage("error".to_string()).is_retriable());
    assert!(SlateDBError::SlateDB("error".to_string()).is_retriable());
    assert!(
        SlateDBError::FlushFailed {
            topic: "t".to_string(),
            partition: 0,
            message: "io".to_string()
        }
        .is_retriable()
    );

    assert!(!SlateDBError::Fenced.is_retriable());
    assert!(!SlateDBError::Config("error".to_string()).is_retriable());
    assert!(
        !SlateDBError::DuplicateSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 1
        }
        .is_retriable()
    );
}

#[test]
fn test_is_idempotency_error() {
    assert!(
        SlateDBError::DuplicateSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 1
        }
        .is_idempotency_error()
    );
    assert!(
        SlateDBError::OutOfOrderSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 5
        }
        .is_idempotency_error()
    );
    assert!(
        SlateDBError::FencedProducer {
            producer_id: 1,
            expected_epoch: 2,
            actual_epoch: 1
        }
        .is_idempotency_error()
    );
    assert!(
        SlateDBError::SequenceOverflow {
            producer_id: 1,
            topic: "t".to_string(),
            partition: 0
        }
        .is_idempotency_error()
    );

    assert!(!SlateDBError::Fenced.is_idempotency_error());
    assert!(!SlateDBError::Storage("error".to_string()).is_idempotency_error());
}

// ============================================================================
// FencingDetectionMethod Tests
// ============================================================================

#[test]
fn test_fencing_detection_method_is_fenced() {
    assert!(FencingDetectionMethod::TypedErrorKind.is_fenced());
    assert!(FencingDetectionMethod::PatternMatch.is_fenced());
    assert!(FencingDetectionMethod::FailClosed.is_fenced());
    assert!(!FencingDetectionMethod::NotFencing.is_fenced());
}

#[test]
fn test_fencing_detection_method_as_metric_label() {
    assert_eq!(
        FencingDetectionMethod::TypedErrorKind.as_metric_label(),
        "typed"
    );
    assert_eq!(
        FencingDetectionMethod::PatternMatch.as_metric_label(),
        "pattern"
    );
    assert_eq!(
        FencingDetectionMethod::FailClosed.as_metric_label(),
        "fail_closed"
    );
    assert_eq!(
        FencingDetectionMethod::NotFencing.as_metric_label(),
        "not_fencing"
    );
}

#[test]
fn test_fencing_detection_method_debug() {
    let method = FencingDetectionMethod::TypedErrorKind;
    let debug_str = format!("{:?}", method);
    assert!(debug_str.contains("TypedErrorKind"));
}

#[test]
fn test_fencing_detection_method_clone() {
    let method = FencingDetectionMethod::PatternMatch;
    let cloned = method;
    assert_eq!(method, cloned);
}

#[test]
fn test_fencing_detection_method_copy() {
    let method = FencingDetectionMethod::FailClosed;
    let copied = method;
    assert_eq!(method, copied);
}

// ============================================================================
// detect_fencing_from_message Tests
// ============================================================================

#[test]
fn test_detect_fencing_fenced_patterns() {
    assert_eq!(
        detect_fencing_from_message("Writer was fenced out"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("writer id mismatch detected"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("Manifest conflict: version mismatch"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("conditional write failed for object"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("precondition failed: etag mismatch"),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_safe_patterns() {
    assert_eq!(
        detect_fencing_from_message("Connection timeout after 30s"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Object not found in bucket"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Disk full: no space left on device"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Network unreachable"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Permission denied: unauthorized access"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_unknown_patterns() {
    // Unknown patterns should trigger fail-closed
    assert_eq!(
        detect_fencing_from_message("Some completely random error XYZ123"),
        FencingDetectionMethod::FailClosed
    );
    assert_eq!(
        detect_fencing_from_message("Unexpected state: ABC"),
        FencingDetectionMethod::FailClosed
    );
    assert_eq!(
        detect_fencing_from_message(""),
        FencingDetectionMethod::FailClosed
    );
}

#[test]
fn test_detect_fencing_case_insensitive() {
    assert_eq!(
        detect_fencing_from_message("WRITER WAS FENCED OUT"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("CONNECTION TIMEOUT"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_s3_patterns() {
    assert_eq!(
        detect_fencing_from_message("ConditionalRequestFailed: The conditional request failed"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("ConflictingOperationInProgress: Another operation"),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_gcs_patterns() {
    assert_eq!(
        detect_fencing_from_message("ConditionNotMet: At least one of the pre-conditions"),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_azure_patterns() {
    assert_eq!(
        detect_fencing_from_message("LeaseIdMismatch: The lease ID specified did not match"),
        FencingDetectionMethod::PatternMatch
    );
}

// ============================================================================
// KafkaCode Tests
// ============================================================================

#[test]
fn test_kafka_code_values() {
    assert_eq!(KafkaCode::None as i16, 0);
    assert_eq!(KafkaCode::Unknown as i16, -1);
    assert_eq!(KafkaCode::NotLeaderForPartition as i16, 6);
    assert_eq!(KafkaCode::UnknownTopicOrPartition as i16, 3);
    assert_eq!(KafkaCode::CorruptMessage as i16, 2);
    assert_eq!(KafkaCode::InvalidTopic as i16, 17);
    assert_eq!(KafkaCode::DuplicateSequenceNumber as i16, 46);
    assert_eq!(KafkaCode::OutOfOrderSequenceNumber as i16, 45);
}

#[test]
fn test_kafka_code_debug() {
    let code = KafkaCode::NotLeaderForPartition;
    let debug_str = format!("{:?}", code);
    assert!(debug_str.contains("NotLeaderForPartition"));
}

#[test]
fn test_kafka_code_clone() {
    let code = KafkaCode::Unknown;
    let cloned = code;
    assert_eq!(code, cloned);
}

#[test]
fn test_kafka_code_copy() {
    let code = KafkaCode::None;
    let copied = code;
    assert_eq!(code, copied);
}
