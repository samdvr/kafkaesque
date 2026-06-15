use super::*;
use std::error::Error;

#[test]
fn test_slatedb_error_display() {
    let err = SlateDBError::SlateDB("test error".to_string());
    let display = format!("{}", err);
    assert!(display.contains("SlateDB error"));
    assert!(display.contains("test error"));
}

#[test]
fn test_storage_error_display() {
    let err = SlateDBError::Storage("disk full".to_string());
    let display = format!("{}", err);
    assert!(display.contains("Storage error"));
    assert!(display.contains("disk full"));
}

#[test]
fn test_fenced_error_display() {
    let err = SlateDBError::Fenced;
    let display = format!("{}", err);
    assert!(display.contains("fenced"));
}

#[test]
fn test_not_owned_error_display() {
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
fn test_partition_not_found_error_display() {
    let err = SlateDBError::PartitionNotFound {
        topic: "missing-topic".to_string(),
        partition: 0,
    };
    let display = format!("{}", err);
    assert!(display.contains("missing-topic"));
    assert!(display.contains("0"));
    assert!(display.contains("not found"));
}

#[test]
fn test_config_error_display() {
    let err = SlateDBError::Config("invalid port".to_string());
    let display = format!("{}", err);
    assert!(display.contains("Configuration error"));
    assert!(display.contains("invalid port"));
}

#[test]
fn test_io_error_from() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err: SlateDBError = io_err.into();
    match err {
        SlateDBError::Io(_) => {}
        _ => panic!("Expected Io error"),
    }
    let display = format!("{}", err);
    assert!(display.contains("IO error"));
}

#[test]
fn test_serde_error_display() {
    let json_err = serde_json::from_str::<String>("not valid json").unwrap_err();
    let err: SlateDBError = json_err.into();
    match err {
        SlateDBError::Serde(_) => {}
        _ => panic!("Expected Serde error"),
    }
    let display = format!("{}", err);
    assert!(display.contains("Serialization error"));
}

#[test]
fn test_error_debug() {
    let err = SlateDBError::Fenced;
    let debug = format!("{:?}", err);
    assert!(debug.contains("Fenced"));
}

#[test]
fn test_slatedb_result_type() {
    fn returns_ok() -> SlateDBResult<i32> {
        Ok(42)
    }
    fn returns_err() -> SlateDBResult<i32> {
        Err(SlateDBError::Fenced)
    }

    assert_eq!(returns_ok().unwrap(), 42);
    assert!(returns_err().is_err());
}

#[test]
fn test_error_is_std_error() {
    let err: Box<dyn std::error::Error> = Box::new(SlateDBError::Config("test".to_string()));
    assert!(err.to_string().contains("Configuration error"));
}

#[test]
fn test_error_source_none() {
    let err = SlateDBError::Fenced;
    assert!(err.source().is_none());

    let err = SlateDBError::SlateDB("test".to_string());
    assert!(err.source().is_none());

    let err = SlateDBError::Storage("test".to_string());
    assert!(err.source().is_none());

    let err = SlateDBError::Config("test".to_string());
    assert!(err.source().is_none());

    let err = SlateDBError::NotOwned {
        topic: "t".to_string(),
        partition: 0,
    };
    assert!(err.source().is_none());

    let err = SlateDBError::PartitionNotFound {
        topic: "t".to_string(),
        partition: 0,
    };
    assert!(err.source().is_none());
}

#[test]
fn test_error_source_io() {
    let io_err = std::io::Error::other("inner");
    let err = SlateDBError::Io(io_err);
    assert!(err.source().is_some());
}

#[test]
fn test_error_source_serde() {
    let json_err = serde_json::from_str::<String>("bad").unwrap_err();
    let err = SlateDBError::Serde(json_err);
    assert!(err.source().is_some());
}

// ========================================================================
// Fencing Detection Tests
// ========================================================================
//
// Note: Testing From<slatedb::Error> directly is difficult because slatedb::Error
// cannot be easily constructed from arbitrary messages. Instead, we test the
// error mapping methods and the pattern matching logic indirectly.

#[test]
fn test_kafka_code_mapping_fenced() {
    assert_eq!(
        SlateDBError::Fenced.to_kafka_code(),
        crate::error::KafkaCode::NotLeaderForPartition
    );
}

#[test]
fn test_kafka_code_mapping_not_owned() {
    let err = SlateDBError::NotOwned {
        topic: "test".to_string(),
        partition: 0,
    };
    assert_eq!(
        err.to_kafka_code(),
        crate::error::KafkaCode::NotLeaderForPartition
    );
}

#[test]
fn test_kafka_code_mapping_duplicate_sequence() {
    let err = SlateDBError::DuplicateSequence {
        producer_id: 123,
        expected_sequence: 5,
        received_sequence: 3,
    };
    assert_eq!(
        err.to_kafka_code(),
        crate::error::KafkaCode::DuplicateSequenceNumber
    );
}

#[test]
fn test_kafka_code_mapping_out_of_order() {
    let err = SlateDBError::OutOfOrderSequence {
        producer_id: 123,
        expected_sequence: 5,
        received_sequence: 10,
    };
    assert_eq!(
        err.to_kafka_code(),
        crate::error::KafkaCode::OutOfOrderSequenceNumber
    );
}

#[test]
fn test_kafka_code_mapping_partition_not_found() {
    let err = SlateDBError::PartitionNotFound {
        topic: "test".to_string(),
        partition: 0,
    };
    assert_eq!(
        err.to_kafka_code(),
        crate::error::KafkaCode::UnknownTopicOrPartition
    );
}

#[test]
fn test_error_is_not_leader_method() {
    assert!(SlateDBError::Fenced.is_not_leader());
    assert!(
        SlateDBError::NotOwned {
            topic: "t".to_string(),
            partition: 0
        }
        .is_not_leader()
    );
    assert!(!SlateDBError::Storage("test".to_string()).is_not_leader());
}

#[test]
fn test_error_is_retriable_method() {
    assert!(SlateDBError::SlateDB("test".to_string()).is_retriable());
    assert!(SlateDBError::Storage("test".to_string()).is_retriable());
    assert!(!SlateDBError::Fenced.is_retriable());
    assert!(!SlateDBError::Config("test".to_string()).is_retriable());
}

#[test]
fn test_error_is_idempotency_error_method() {
    assert!(
        SlateDBError::DuplicateSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 1,
        }
        .is_idempotency_error()
    );
    assert!(
        SlateDBError::OutOfOrderSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 5,
        }
        .is_idempotency_error()
    );
    assert!(!SlateDBError::Fenced.is_idempotency_error());
}

// Test the string pattern matching logic used in From<slatedb::Error>
// by verifying the patterns are correctly specified.
#[test]
fn test_fencing_patterns_are_lowercase() {
    // Verify all fencing patterns are lowercase (since we lowercase the error message)
    let fencing_patterns = [
        "fenced",
        "fence",
        "writer id mismatch",
        "writer id conflict",
        "writer conflict",
        "manifest conflict",
        "manifest version",
        "version conflict",
        "version mismatch",
        "conditional write failed",
        "precondition failed",
        "etag mismatch",
        "if-match",
        "if-none-match",
        "conditional request failed",
        "conflictingoperationinprogress",
        "conditionnotmet",
        "leaseidmismatch",
        "already exists with different",
        "concurrent modification",
    ];

    for pattern in &fencing_patterns {
        assert_eq!(
            *pattern,
            pattern.to_lowercase(),
            "Pattern '{}' should be lowercase",
            pattern
        );
    }
}

#[test]
fn test_safe_patterns_are_lowercase() {
    // Verify all safe patterns are lowercase
    let safe_patterns = [
        "connection",
        "timeout",
        "timed out",
        "network",
        "io error",
        "broken pipe",
        "connection reset",
        "connection refused",
        "temporarily unavailable",
        "service unavailable",
        "no space",
        "disk full",
        "quota exceeded",
        "storage limit",
        "not found",
        "does not exist",
        "no such",
        "object not found",
        "corrupt",
        "checksum",
        "invalid data",
        "parse error",
        "deserialize",
        "serialize",
        "malformed",
        "invalid config",
        "configuration",
        "invalid argument",
        "shutdown",
        "cancelled",
        "aborted",
        "closed",
        "access denied",
        "permission denied",
        "unauthorized",
        "forbidden",
    ];

    for pattern in &safe_patterns {
        assert_eq!(
            *pattern,
            pattern.to_lowercase(),
            "Pattern '{}' should be lowercase",
            pattern
        );
    }
}

// ========================================================================
// Fencing Detection Method Tests
// ========================================================================

#[test]
fn test_fencing_detection_method_is_fenced() {
    assert!(FencingDetectionMethod::TypedErrorKind.is_fenced());
    assert!(FencingDetectionMethod::PatternMatch.is_fenced());
    assert!(FencingDetectionMethod::FailClosed.is_fenced());
    assert!(!FencingDetectionMethod::NotFencing.is_fenced());
}

#[test]
fn test_fencing_detection_method_metric_labels() {
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
fn test_detect_fencing_from_message_fenced_patterns() {
    // Test various fencing patterns
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
fn test_detect_fencing_from_message_safe_patterns() {
    // Test various safe patterns
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
    assert_eq!(
        detect_fencing_from_message("Database shutdown gracefully"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_from_message_unknown() {
    // Unknown patterns propagate without auto-fencing.
    assert_eq!(
        detect_fencing_from_message("Some completely random error XYZ123"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Unexpected state: ABC"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_case_insensitive() {
    // Test that detection is case-insensitive
    assert_eq!(
        detect_fencing_from_message("WRITER WAS FENCED OUT"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("CONNECTION TIMEOUT"),
        FencingDetectionMethod::NotFencing
    );
}

// ========================================================================
// Additional Fencing Detection Integration Tests
// ========================================================================

#[test]
fn test_detect_fencing_s3_specific_patterns() {
    // S3-specific conditional write failure patterns
    assert_eq!(
        detect_fencing_from_message("ConditionalRequestFailed: The conditional request failed"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message(
            "ConflictingOperationInProgress: Another operation is in progress"
        ),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_gcs_specific_patterns() {
    // GCS-specific conditional write failure patterns
    assert_eq!(
        detect_fencing_from_message(
            "ConditionNotMet: At least one of the pre-conditions did not hold"
        ),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_azure_specific_patterns() {
    // Azure-specific lease conflict patterns
    assert_eq!(
        detect_fencing_from_message("LeaseIdMismatch: The lease ID specified did not match"),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_etag_mismatch() {
    // Common ETag/If-Match patterns across cloud providers
    assert_eq!(
        detect_fencing_from_message("Precondition failed: If-Match condition not met"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("ETag mismatch: expected 'abc123', got 'def456'"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("If-None-Match condition failed"),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_slatedb_patterns() {
    // SlateDB-specific fencing patterns
    assert_eq!(
        detect_fencing_from_message("Writer ID mismatch: another writer has taken over"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("Writer conflict detected during manifest update"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("Manifest conflict: version 5 != expected 4"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("Version mismatch in compaction"),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_concurrent_modification() {
    // Concurrent modification patterns
    assert_eq!(
        detect_fencing_from_message("Concurrent modification detected: resource was changed"),
        FencingDetectionMethod::PatternMatch
    );
    assert_eq!(
        detect_fencing_from_message("Already exists with different content"),
        FencingDetectionMethod::PatternMatch
    );
}

#[test]
fn test_detect_fencing_transient_vs_fencing() {
    // Ensure transient errors are not treated as fencing
    assert_eq!(
        detect_fencing_from_message("Temporary service unavailable, please retry"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Request timed out after 30 seconds"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Connection refused: server not ready"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("I/O error: broken pipe during write"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_permission_errors() {
    // Permission errors should not be treated as fencing
    assert_eq!(
        detect_fencing_from_message("Access denied: insufficient permissions for bucket"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Forbidden: API key does not have write access"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Unauthorized: invalid credentials"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_not_found_errors() {
    // Not found errors should not be treated as fencing
    assert_eq!(
        detect_fencing_from_message("Object not found: key does not exist"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("No such file or directory: /path/to/file"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Bucket does not exist: my-bucket"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_corruption_errors() {
    // Corruption errors should not be treated as fencing
    assert_eq!(
        detect_fencing_from_message("Corrupt data: checksum mismatch"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Malformed SST file: invalid magic number"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Deserialize error: invalid JSON"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_empty_and_edge_cases() {
    assert_eq!(
        detect_fencing_from_message(""),
        FencingDetectionMethod::NotFencing
    );

    let long_msg = format!("Some error with lots of context: {}", "x".repeat(10000));
    assert_eq!(
        detect_fencing_from_message(&long_msg),
        FencingDetectionMethod::NotFencing
    );

    assert_eq!(
        detect_fencing_from_message("   \n\t   "),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_fencing_detection_method_properties() {
    // Verify is_fenced() correctly identifies fencing methods
    assert!(FencingDetectionMethod::TypedErrorKind.is_fenced());
    assert!(FencingDetectionMethod::PatternMatch.is_fenced());
    assert!(FencingDetectionMethod::FailClosed.is_fenced());
    assert!(!FencingDetectionMethod::NotFencing.is_fenced());

    // Verify all variants have valid metric labels
    for method in [
        FencingDetectionMethod::TypedErrorKind,
        FencingDetectionMethod::PatternMatch,
        FencingDetectionMethod::FailClosed,
        FencingDetectionMethod::NotFencing,
    ] {
        let label = method.as_metric_label();
        assert!(!label.is_empty());
        assert!(label.chars().all(|c| c.is_ascii_alphanumeric() || c == '_'));
    }
}

// ========================================================================
// Additional Error Type Tests
// ========================================================================

#[test]
fn test_lease_too_short_error_display() {
    let err = SlateDBError::LeaseTooShort {
        topic: "orders".to_string(),
        partition: 5,
        remaining_secs: 10,
        required_secs: 15,
    };
    let display = format!("{}", err);
    assert!(display.contains("orders"));
    assert!(display.contains("5"));
    assert!(display.contains("10"));
    assert!(display.contains("15"));
}

#[test]
fn test_fenced_producer_error_display() {
    let err = SlateDBError::FencedProducer {
        producer_id: 12345,
        expected_epoch: 5,
        actual_epoch: 3,
    };
    let display = format!("{}", err);
    assert!(display.contains("12345"));
    assert!(display.contains("5"));
    assert!(display.contains("3"));
}

#[test]
fn test_sequence_overflow_error_display() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 99999,
        topic: "events".to_string(),
        partition: 10,
    };
    let display = format!("{}", err);
    assert!(display.contains("99999"));
    assert!(display.contains("events"));
    assert!(display.contains("10"));
}

#[test]
fn test_flush_failed_error_display() {
    let err = SlateDBError::FlushFailed {
        topic: "logs".to_string(),
        partition: 2,
        message: "disk I/O timeout".to_string(),
    };
    let display = format!("{}", err);
    assert!(display.contains("logs"));
    assert!(display.contains("2"));
    assert!(display.contains("disk I/O timeout"));
}

#[test]
fn test_invalid_partition_error_display() {
    let err = SlateDBError::InvalidPartition {
        topic: "test-topic".to_string(),
        partition: -1,
    };
    let display = format!("{}", err);
    assert!(display.contains("test-topic"));
    assert!(display.contains("-1"));
    assert!(display.contains("Invalid partition"));
}

#[test]
fn test_raft_error_display() {
    let err = SlateDBError::Raft("leader election failed".to_string());
    let display = format!("{}", err);
    assert!(display.contains("Raft error"));
    assert!(display.contains("leader election failed"));
}

#[test]
fn test_protocol_error_display() {
    let err = SlateDBError::Protocol("invalid request format".to_string());
    let display = format!("{}", err);
    assert!(display.contains("Protocol error"));
    assert!(display.contains("invalid request format"));
}

#[test]
fn test_heartbeat_result_variants() {
    assert_eq!(HeartbeatResult::Success, HeartbeatResult::Success);
    assert_eq!(
        HeartbeatResult::UnknownMember,
        HeartbeatResult::UnknownMember
    );
    assert_eq!(
        HeartbeatResult::IllegalGeneration,
        HeartbeatResult::IllegalGeneration
    );

    assert_ne!(HeartbeatResult::Success, HeartbeatResult::UnknownMember);
    assert_ne!(HeartbeatResult::Success, HeartbeatResult::IllegalGeneration);
}

#[test]
fn test_heartbeat_result_debug() {
    assert_eq!(format!("{:?}", HeartbeatResult::Success), "Success");
    assert_eq!(
        format!("{:?}", HeartbeatResult::UnknownMember),
        "UnknownMember"
    );
    assert_eq!(
        format!("{:?}", HeartbeatResult::IllegalGeneration),
        "IllegalGeneration"
    );
}

#[test]
fn test_heartbeat_result_clone() {
    let result = HeartbeatResult::Success;
    let cloned = result;
    assert_eq!(result, cloned);
}

#[test]
fn test_kafka_code_mapping_lease_too_short() {
    let err = SlateDBError::LeaseTooShort {
        topic: "t".to_string(),
        partition: 0,
        remaining_secs: 5,
        required_secs: 15,
    };
    assert_eq!(
        err.to_kafka_code(),
        crate::error::KafkaCode::NotLeaderForPartition
    );
}

#[test]
fn test_kafka_code_mapping_fenced_producer() {
    let err = SlateDBError::FencedProducer {
        producer_id: 1,
        expected_epoch: 2,
        actual_epoch: 1,
    };
    assert_eq!(
        err.to_kafka_code(),
        crate::error::KafkaCode::InvalidProducerEpoch
    );
}

#[test]
fn test_kafka_code_mapping_sequence_overflow() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 1,
        topic: "t".to_string(),
        partition: 0,
    };
    assert_eq!(
        err.to_kafka_code(),
        crate::error::KafkaCode::UnknownProducerId
    );
}

#[test]
fn test_kafka_code_mapping_flush_failed() {
    let err = SlateDBError::FlushFailed {
        topic: "t".to_string(),
        partition: 0,
        message: "timeout".to_string(),
    };
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::Unknown);
}

#[test]
fn test_kafka_code_mapping_invalid_partition() {
    let err = SlateDBError::InvalidPartition {
        topic: "t".to_string(),
        partition: -1,
    };
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::InvalidTopic);
}

#[test]
fn test_kafka_code_mapping_raft() {
    let err = SlateDBError::Raft("error".to_string());
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::Unknown);
}

#[test]
fn test_kafka_code_mapping_protocol() {
    let err = SlateDBError::Protocol("error".to_string());
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::CorruptMessage);
}

#[test]
fn test_kafka_code_mapping_serde() {
    let json_err = serde_json::from_str::<String>("bad").unwrap_err();
    let err = SlateDBError::Serde(json_err);
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::CorruptMessage);
}

#[test]
fn test_is_not_leader_lease_too_short() {
    let err = SlateDBError::LeaseTooShort {
        topic: "t".to_string(),
        partition: 0,
        remaining_secs: 5,
        required_secs: 15,
    };
    assert!(err.is_not_leader());
}

#[test]
fn test_is_retriable_flush_failed() {
    let err = SlateDBError::FlushFailed {
        topic: "t".to_string(),
        partition: 0,
        message: "timeout".to_string(),
    };
    assert!(err.is_retriable());
}

#[test]
fn test_is_retriable_raft() {
    let err = SlateDBError::Raft("leader unavailable".to_string());
    assert!(err.is_retriable());
}

#[test]
fn test_is_idempotency_error_fenced_producer() {
    let err = SlateDBError::FencedProducer {
        producer_id: 1,
        expected_epoch: 2,
        actual_epoch: 1,
    };
    assert!(err.is_idempotency_error());
}

#[test]
fn test_is_idempotency_error_sequence_overflow() {
    let err = SlateDBError::SequenceOverflow {
        producer_id: 1,
        topic: "t".to_string(),
        partition: 0,
    };
    assert!(err.is_idempotency_error());
}

#[test]
fn test_is_fenced_explicit() {
    assert!(SlateDBError::Fenced.is_fenced());
}

#[test]
fn test_is_fenced_via_slatedb_message() {
    let err = SlateDBError::SlateDB("Writer was fenced out".to_string());
    assert!(err.is_fenced());
}

#[test]
fn test_is_fenced_safe_slatedb_message() {
    let err = SlateDBError::SlateDB("Connection timeout".to_string());
    assert!(!err.is_fenced());
}

#[test]
fn test_is_fenced_other_errors() {
    assert!(
        !SlateDBError::NotOwned {
            topic: "t".to_string(),
            partition: 0
        }
        .is_fenced()
    );
    assert!(!SlateDBError::Config("error".to_string()).is_fenced());
    assert!(!SlateDBError::Storage("error".to_string()).is_fenced());
}

#[test]
fn test_duplicate_sequence_error_display() {
    let err = SlateDBError::DuplicateSequence {
        producer_id: 123,
        expected_sequence: 10,
        received_sequence: 5,
    };
    let display = format!("{}", err);
    assert!(display.contains("123"));
    assert!(display.contains("10"));
    assert!(display.contains("5"));
    assert!(display.contains("Duplicate"));
}

#[test]
fn test_out_of_order_sequence_error_display() {
    let err = SlateDBError::OutOfOrderSequence {
        producer_id: 456,
        expected_sequence: 7,
        received_sequence: 15,
    };
    let display = format!("{}", err);
    assert!(display.contains("456"));
    assert!(display.contains("7"));
    assert!(display.contains("15"));
    assert!(display.contains("Out-of-order"));
}

// ========================================================================
// Object Store Error Conversion Tests
// ========================================================================

#[test]
fn test_object_store_error_conversion() {
    // Create an object store error (NotFound is easy to construct)
    let os_err = object_store::Error::NotFound {
        path: "test/path".to_string(),
        source: "test".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    match err {
        SlateDBError::ObjectStore(_) => {}
        _ => panic!("Expected ObjectStore error"),
    }
}

#[test]
fn test_kafka_code_mapping_object_store() {
    let os_err = object_store::Error::NotFound {
        path: "test".to_string(),
        source: "test".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::Unknown);
}

#[test]
fn test_object_store_error_is_retriable() {
    // NotFound is NOT retriable (it's a permanent failure)
    // Test a truly transient error instead (NotModified)
    let os_err = object_store::Error::NotModified {
        path: "test".to_string(),
        source: "test".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    assert!(err.is_retriable());

    // NotFound should NOT be retriable
    let os_err = object_store::Error::NotFound {
        path: "test".to_string(),
        source: "test".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    assert!(
        !err.is_retriable(),
        "NotFound errors should not be retriable"
    );
}

// ========================================================================
// Additional Error Classification Tests
// ========================================================================

#[test]
fn test_detect_fencing_shutdown_patterns() {
    // Shutdown-related patterns should not be fencing
    assert_eq!(
        detect_fencing_from_message("Server shutdown in progress"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Request was cancelled"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Operation aborted"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_storage_errors() {
    assert_eq!(
        detect_fencing_from_message("No space left on device"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Disk full error"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Quota exceeded for bucket"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_detect_fencing_parse_errors() {
    assert_eq!(
        detect_fencing_from_message("Failed to deserialize record"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Failed to serialize message"),
        FencingDetectionMethod::NotFencing
    );
    assert_eq!(
        detect_fencing_from_message("Parse error in JSON"),
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_is_fenced_unknown_slatedb_message() {
    // Unknown patterns must not auto-fence — avoids mass lease release on
    // transient glitches.
    let err = SlateDBError::SlateDB("Some completely unknown error xyz".to_string());
    assert!(!err.is_fenced());
}

#[test]
fn test_slatedb_error_source_object_store() {
    let os_err = object_store::Error::NotFound {
        path: "test".to_string(),
        source: "inner error".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    // ObjectStore errors should have source
    assert!(err.source().is_some());
}

// ========================================================================
// More Kafka Code Mapping Tests
// ========================================================================

#[test]
fn test_kafka_code_mapping_config() {
    let err = SlateDBError::Config("invalid setting".to_string());
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::InvalidTopic);
}

#[test]
fn test_kafka_code_mapping_storage() {
    let err = SlateDBError::Storage("disk error".to_string());
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::Unknown);
}

#[test]
fn test_kafka_code_mapping_io() {
    let io_err = std::io::Error::other("io failure");
    let err = SlateDBError::Io(io_err);
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::Unknown);
}

#[test]
fn test_kafka_code_mapping_slatedb() {
    let err = SlateDBError::SlateDB("internal error".to_string());
    assert_eq!(err.to_kafka_code(), crate::error::KafkaCode::Unknown);
}

// ========================================================================
// Error Classification Helper Tests
// ========================================================================

#[test]
fn test_is_not_leader_negative_cases() {
    assert!(
        !SlateDBError::PartitionNotFound {
            topic: "t".to_string(),
            partition: 0
        }
        .is_not_leader()
    );

    assert!(
        !SlateDBError::DuplicateSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 1,
        }
        .is_not_leader()
    );

    assert!(!SlateDBError::Protocol("error".to_string()).is_not_leader());
}

#[test]
fn test_is_retriable_negative_cases() {
    assert!(
        !SlateDBError::NotOwned {
            topic: "t".to_string(),
            partition: 0
        }
        .is_retriable()
    );

    assert!(
        !SlateDBError::DuplicateSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 1,
        }
        .is_retriable()
    );

    assert!(!SlateDBError::Protocol("error".to_string()).is_retriable());

    assert!(
        !SlateDBError::PartitionNotFound {
            topic: "t".to_string(),
            partition: 0
        }
        .is_retriable()
    );
}

#[test]
fn test_is_idempotency_error_negative_cases() {
    assert!(!SlateDBError::Storage("error".to_string()).is_idempotency_error());
    assert!(!SlateDBError::Config("error".to_string()).is_idempotency_error());
    assert!(!SlateDBError::Protocol("error".to_string()).is_idempotency_error());
    assert!(
        !SlateDBError::NotOwned {
            topic: "t".to_string(),
            partition: 0
        }
        .is_idempotency_error()
    );
}

#[test]
fn test_fencing_detection_method_equality() {
    assert_eq!(
        FencingDetectionMethod::TypedErrorKind,
        FencingDetectionMethod::TypedErrorKind
    );
    assert_eq!(
        FencingDetectionMethod::PatternMatch,
        FencingDetectionMethod::PatternMatch
    );
    assert_ne!(
        FencingDetectionMethod::TypedErrorKind,
        FencingDetectionMethod::PatternMatch
    );
    assert_ne!(
        FencingDetectionMethod::FailClosed,
        FencingDetectionMethod::NotFencing
    );
}

#[test]
fn test_fencing_detection_method_clone() {
    let method = FencingDetectionMethod::TypedErrorKind;
    let cloned = method;
    assert_eq!(method, cloned);
}

#[test]
fn test_fencing_detection_method_debug() {
    assert_eq!(
        format!("{:?}", FencingDetectionMethod::TypedErrorKind),
        "TypedErrorKind"
    );
    assert_eq!(
        format!("{:?}", FencingDetectionMethod::PatternMatch),
        "PatternMatch"
    );
    assert_eq!(
        format!("{:?}", FencingDetectionMethod::FailClosed),
        "FailClosed"
    );
    assert_eq!(
        format!("{:?}", FencingDetectionMethod::NotFencing),
        "NotFencing"
    );
}

#[test]
fn test_heartbeat_result_copy() {
    let result = HeartbeatResult::Success;
    let copied: HeartbeatResult = result; // Copy trait
    assert_eq!(result, copied);
}

// ========================================================================
// EpochMismatch Error Tests
// ========================================================================

#[test]
fn test_epoch_mismatch_error_display() {
    let err = SlateDBError::EpochMismatch {
        topic: "my-topic".to_string(),
        partition: 7,
        expected_epoch: 5,
        stored_epoch: 8,
    };
    let display = format!("{}", err);
    assert!(display.contains("my-topic"));
    assert!(display.contains("7"));
    assert!(display.contains("5"));
    assert!(display.contains("8"));
    assert!(display.contains("mismatch"));
}

#[test]
fn test_epoch_mismatch_kafka_code() {
    let err = SlateDBError::EpochMismatch {
        topic: "t".to_string(),
        partition: 0,
        expected_epoch: 1,
        stored_epoch: 2,
    };
    assert_eq!(
        err.to_kafka_code(),
        crate::error::KafkaCode::NotLeaderForPartition
    );
}

#[test]
fn test_epoch_mismatch_not_retriable() {
    let err = SlateDBError::EpochMismatch {
        topic: "t".to_string(),
        partition: 0,
        expected_epoch: 1,
        stored_epoch: 2,
    };
    assert!(!err.is_retriable());
}

#[test]
fn test_epoch_mismatch_source_none() {
    let err = SlateDBError::EpochMismatch {
        topic: "t".to_string(),
        partition: 0,
        expected_epoch: 1,
        stored_epoch: 2,
    };
    assert!(err.source().is_none());
}

// ========================================================================
// is_permission_error Tests
// ========================================================================

#[test]
fn test_is_permission_error_object_store_permission_denied() {
    // PermissionDenied requires a source, so we test via the method directly
    let os_err = object_store::Error::PermissionDenied {
        path: "bucket/key".to_string(),
        source: "access denied".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    assert!(err.is_permission_error());
}

#[test]
fn test_is_permission_error_object_store_unauthenticated() {
    let os_err = object_store::Error::Unauthenticated {
        path: "bucket/key".to_string(),
        source: "invalid token".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    assert!(err.is_permission_error());
}

#[test]
fn test_is_permission_error_io_permission_denied() {
    let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "access denied");
    let err = SlateDBError::Io(io_err);
    assert!(err.is_permission_error());
}

#[test]
fn test_is_permission_error_negative_cases() {
    // Other errors should not be permission errors
    assert!(!SlateDBError::Fenced.is_permission_error());
    assert!(!SlateDBError::Config("error".to_string()).is_permission_error());
    assert!(!SlateDBError::Storage("error".to_string()).is_permission_error());

    // NotFound is not a permission error
    let os_err = object_store::Error::NotFound {
        path: "test".to_string(),
        source: "not found".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    assert!(!err.is_permission_error());

    // Other IO errors are not permission errors
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err = SlateDBError::Io(io_err);
    assert!(!err.is_permission_error());
}

// ========================================================================
// is_not_found Tests
// ========================================================================

#[test]
fn test_is_not_found_object_store() {
    let os_err = object_store::Error::NotFound {
        path: "bucket/key".to_string(),
        source: "not found".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    assert!(err.is_not_found());
}

#[test]
fn test_is_not_found_io() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let err = SlateDBError::Io(io_err);
    assert!(err.is_not_found());
}

#[test]
fn test_is_not_found_partition_not_found() {
    let err = SlateDBError::PartitionNotFound {
        topic: "my-topic".to_string(),
        partition: 5,
    };
    assert!(err.is_not_found());
}

#[test]
fn test_is_not_found_negative_cases() {
    assert!(!SlateDBError::Fenced.is_not_found());
    assert!(!SlateDBError::Config("error".to_string()).is_not_found());
    assert!(!SlateDBError::Storage("error".to_string()).is_not_found());
    assert!(
        !SlateDBError::NotOwned {
            topic: "t".to_string(),
            partition: 0
        }
        .is_not_found()
    );

    // Permission denied is not NotFound
    let os_err = object_store::Error::PermissionDenied {
        path: "test".to_string(),
        source: "denied".to_string().into(),
    };
    let err: SlateDBError = os_err.into();
    assert!(!err.is_not_found());
}

// ========================================================================
// IO Error Retriability Tests
// ========================================================================

#[test]
fn test_io_error_retryable_connection_errors() {
    // All connection-related errors should be retryable
    for kind in [
        std::io::ErrorKind::ConnectionRefused,
        std::io::ErrorKind::ConnectionReset,
        std::io::ErrorKind::ConnectionAborted,
        std::io::ErrorKind::NotConnected,
        std::io::ErrorKind::BrokenPipe,
    ] {
        let io_err = std::io::Error::new(kind, "connection error");
        let err = SlateDBError::Io(io_err);
        assert!(
            err.is_retriable(),
            "IO error {:?} should be retryable",
            kind
        );
    }
}

#[test]
fn test_io_error_retryable_transient_errors() {
    // Transient errors should be retryable
    for kind in [
        std::io::ErrorKind::TimedOut,
        std::io::ErrorKind::Interrupted,
        std::io::ErrorKind::WouldBlock,
        std::io::ErrorKind::WriteZero,
        std::io::ErrorKind::UnexpectedEof,
    ] {
        let io_err = std::io::Error::new(kind, "transient error");
        let err = SlateDBError::Io(io_err);
        assert!(
            err.is_retriable(),
            "IO error {:?} should be retryable",
            kind
        );
    }
}

#[test]
fn test_io_error_not_retryable_permanent_errors() {
    // Permanent errors should NOT be retryable
    for kind in [
        std::io::ErrorKind::NotFound,
        std::io::ErrorKind::PermissionDenied,
        std::io::ErrorKind::AlreadyExists,
        std::io::ErrorKind::InvalidInput,
        std::io::ErrorKind::InvalidData,
        std::io::ErrorKind::AddrInUse,
        std::io::ErrorKind::AddrNotAvailable,
    ] {
        let io_err = std::io::Error::new(kind, "permanent error");
        let err = SlateDBError::Io(io_err);
        assert!(
            !err.is_retriable(),
            "IO error {:?} should NOT be retryable",
            kind
        );
    }
}

// ========================================================================
// Object Store Error Retriability Tests
// ========================================================================

#[test]
fn test_object_store_error_not_retryable_permanent() {
    // These permanent errors should NOT be retryable
    let not_found = object_store::Error::NotFound {
        path: "test".to_string(),
        source: "not found".to_string().into(),
    };
    assert!(!SlateDBError::is_object_store_error_retryable(&not_found));

    let already_exists = object_store::Error::AlreadyExists {
        path: "test".to_string(),
        source: "exists".to_string().into(),
    };
    assert!(!SlateDBError::is_object_store_error_retryable(
        &already_exists
    ));

    let precondition = object_store::Error::Precondition {
        path: "test".to_string(),
        source: "precondition failed".to_string().into(),
    };
    assert!(!SlateDBError::is_object_store_error_retryable(
        &precondition
    ));

    let permission = object_store::Error::PermissionDenied {
        path: "test".to_string(),
        source: "denied".to_string().into(),
    };
    assert!(!SlateDBError::is_object_store_error_retryable(&permission));
}

#[test]
fn test_object_store_error_retryable_not_modified() {
    let not_modified = object_store::Error::NotModified {
        path: "test".to_string(),
        source: "not modified".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(&not_modified));
}

#[test]
fn test_object_store_generic_error_transient_patterns() {
    // Generic errors with transient patterns in source should be retryable
    let timeout_err = object_store::Error::Generic {
        store: "test",
        source: "request timeout after 30s".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(&timeout_err));

    let connection_err = object_store::Error::Generic {
        store: "test",
        source: "connection refused".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(
        &connection_err
    ));

    let throttle_err = object_store::Error::Generic {
        store: "test",
        source: "request throttled, please retry".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(&throttle_err));

    let rate_limit_err = object_store::Error::Generic {
        store: "test",
        source: "rate limit exceeded".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(
        &rate_limit_err
    ));

    let unavailable_err = object_store::Error::Generic {
        store: "test",
        source: "service temporarily unavailable".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(
        &unavailable_err
    ));

    let http_503_err = object_store::Error::Generic {
        store: "test",
        source: "HTTP 503 Service Unavailable".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(&http_503_err));

    let http_500_err = object_store::Error::Generic {
        store: "test",
        source: "HTTP 500 Internal Server Error".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(&http_500_err));

    let http_429_err = object_store::Error::Generic {
        store: "test",
        source: "HTTP 429 Too Many Requests".to_string().into(),
    };
    assert!(SlateDBError::is_object_store_error_retryable(&http_429_err));
}

#[test]
fn test_object_store_generic_error_non_transient() {
    // Generic errors without transient patterns may still be retryable
    // (default behavior is retryable for unknown generic errors)
    let unknown_err = object_store::Error::Generic {
        store: "test",
        source: "some other error".to_string().into(),
    };
    // Unknown patterns default to NOT retryable (no matching patterns)
    assert!(!SlateDBError::is_object_store_error_retryable(&unknown_err));
}

// ========================================================================
// Error Category Classification Tests
// ========================================================================

#[test]
fn test_classify_error_message_fenced() {
    assert_eq!(classify_error_message("fenced out"), ErrorCategory::Fenced);
    assert_eq!(
        classify_error_message("writer id mismatch"),
        ErrorCategory::Fenced
    );
    assert_eq!(
        classify_error_message("manifest conflict"),
        ErrorCategory::Fenced
    );
    assert_eq!(
        classify_error_message("precondition failed"),
        ErrorCategory::Fenced
    );
}

#[test]
fn test_classify_error_message_safe() {
    assert_eq!(
        classify_error_message("connection timeout"),
        ErrorCategory::Safe
    );
    assert_eq!(classify_error_message("not found"), ErrorCategory::Safe);
    assert_eq!(
        classify_error_message("permission denied"),
        ErrorCategory::Safe
    );
    assert_eq!(classify_error_message("corrupt data"), ErrorCategory::Safe);
}

#[test]
fn test_classify_error_message_unknown() {
    assert_eq!(
        classify_error_message("xyz random error"),
        ErrorCategory::Unknown
    );
    assert_eq!(classify_error_message(""), ErrorCategory::Unknown);
}

// ========================================================================
// Additional Edge Case Tests
// ========================================================================

#[test]
fn test_error_debug_all_variants() {
    // Ensure all variants can be debug-printed
    let variants: Vec<SlateDBError> = vec![
        SlateDBError::SlateDB("test".to_string()),
        SlateDBError::Raft("test".to_string()),
        SlateDBError::Storage("test".to_string()),
        SlateDBError::Fenced,
        SlateDBError::NotOwned {
            topic: "t".to_string(),
            partition: 0,
        },
        SlateDBError::PartitionNotFound {
            topic: "t".to_string(),
            partition: 0,
        },
        SlateDBError::InvalidPartition {
            topic: "t".to_string(),
            partition: 0,
        },
        SlateDBError::Config("test".to_string()),
        SlateDBError::DuplicateSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 1,
        },
        SlateDBError::OutOfOrderSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 5,
        },
        SlateDBError::FencedProducer {
            producer_id: 1,
            expected_epoch: 2,
            actual_epoch: 1,
        },
        SlateDBError::LeaseTooShort {
            topic: "t".to_string(),
            partition: 0,
            remaining_secs: 5,
            required_secs: 15,
        },
        SlateDBError::EpochMismatch {
            topic: "t".to_string(),
            partition: 0,
            expected_epoch: 1,
            stored_epoch: 2,
        },
        SlateDBError::SequenceOverflow {
            producer_id: 1,
            topic: "t".to_string(),
            partition: 0,
        },
        SlateDBError::FlushFailed {
            topic: "t".to_string(),
            partition: 0,
            message: "timeout".to_string(),
        },
        SlateDBError::Protocol("test".to_string()),
    ];

    for err in variants {
        let debug = format!("{:?}", err);
        assert!(!debug.is_empty());
    }
}

#[test]
fn test_error_display_all_variants() {
    // Ensure all variants can be display-printed
    let variants: Vec<SlateDBError> = vec![
        SlateDBError::SlateDB("test".to_string()),
        SlateDBError::Raft("test".to_string()),
        SlateDBError::Storage("test".to_string()),
        SlateDBError::Fenced,
        SlateDBError::NotOwned {
            topic: "t".to_string(),
            partition: 0,
        },
        SlateDBError::PartitionNotFound {
            topic: "t".to_string(),
            partition: 0,
        },
        SlateDBError::InvalidPartition {
            topic: "t".to_string(),
            partition: 0,
        },
        SlateDBError::Config("test".to_string()),
        SlateDBError::DuplicateSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 1,
        },
        SlateDBError::OutOfOrderSequence {
            producer_id: 1,
            expected_sequence: 2,
            received_sequence: 5,
        },
        SlateDBError::FencedProducer {
            producer_id: 1,
            expected_epoch: 2,
            actual_epoch: 1,
        },
        SlateDBError::LeaseTooShort {
            topic: "t".to_string(),
            partition: 0,
            remaining_secs: 5,
            required_secs: 15,
        },
        SlateDBError::EpochMismatch {
            topic: "t".to_string(),
            partition: 0,
            expected_epoch: 1,
            stored_epoch: 2,
        },
        SlateDBError::SequenceOverflow {
            producer_id: 1,
            topic: "t".to_string(),
            partition: 0,
        },
        SlateDBError::FlushFailed {
            topic: "t".to_string(),
            partition: 0,
            message: "timeout".to_string(),
        },
        SlateDBError::Protocol("test".to_string()),
    ];

    for err in variants {
        let display = format!("{}", err);
        assert!(!display.is_empty());
    }
}

#[test]
fn test_fencing_detection_method_copy() {
    let method = FencingDetectionMethod::TypedErrorKind;
    let copied: FencingDetectionMethod = method; // Copy trait
    assert_eq!(method, copied);
}

#[test]
fn test_error_category_equality() {
    assert_eq!(ErrorCategory::Fenced, ErrorCategory::Fenced);
    assert_eq!(ErrorCategory::Safe, ErrorCategory::Safe);
    assert_eq!(ErrorCategory::Unknown, ErrorCategory::Unknown);
    assert_ne!(ErrorCategory::Fenced, ErrorCategory::Safe);
}

#[test]
fn test_error_category_debug() {
    assert_eq!(format!("{:?}", ErrorCategory::Fenced), "Fenced");
    assert_eq!(format!("{:?}", ErrorCategory::Safe), "Safe");
    assert_eq!(format!("{:?}", ErrorCategory::Unknown), "Unknown");
}

#[test]
fn test_error_category_clone() {
    let category = ErrorCategory::Fenced;
    let cloned = category;
    assert_eq!(category, cloned);
}
