//! Tests for FencingDetectionMethod and fencing detection utilities.
//!
//! These tests verify:
//! 1. Fencing detection from error messages
//! 2. Circuit breaker behavior for fail-closed fencing detection
//! 3. Error type classification and is_fenced() methods
//! 4. Safe pattern recognition to prevent false positives

use kafkaesque::cluster::metrics;
use kafkaesque::cluster::{FencingDetectionMethod, SlateDBError, detect_fencing_from_message};
use serial_test::serial;

// ============================================================================
// FencingDetectionMethod Tests
// ============================================================================

#[test]
fn test_fencing_detection_method_debug() {
    let typed = FencingDetectionMethod::TypedErrorKind;
    let pattern = FencingDetectionMethod::PatternMatch;
    let fail = FencingDetectionMethod::FailClosed;
    let not = FencingDetectionMethod::NotFencing;

    assert!(format!("{:?}", typed).contains("TypedErrorKind"));
    assert!(format!("{:?}", pattern).contains("PatternMatch"));
    assert!(format!("{:?}", fail).contains("FailClosed"));
    assert!(format!("{:?}", not).contains("NotFencing"));
}

#[test]
fn test_fencing_detection_method_clone() {
    let original = FencingDetectionMethod::TypedErrorKind;
    let cloned = original;
    assert_eq!(original, cloned);
}

#[test]
fn test_fencing_detection_method_eq() {
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
}

// ============================================================================
// detect_fencing_from_message Tests
// ============================================================================

#[test]
fn test_detect_fencing_from_message_fencing_patterns() {
    // Test fencing patterns
    assert!(detect_fencing_from_message("writer fence detected").is_fenced());
    assert!(detect_fencing_from_message("FENCE ERROR").is_fenced());
    assert!(detect_fencing_from_message("Version conflict in manifest").is_fenced());
}

#[test]
fn test_detect_fencing_from_message_safe_patterns() {
    // Test safe patterns
    assert!(!detect_fencing_from_message("Connection timeout").is_fenced());
    assert!(!detect_fencing_from_message("Network error").is_fenced());
    assert!(!detect_fencing_from_message("Object not found").is_fenced());
    assert!(!detect_fencing_from_message("Permission denied").is_fenced());
}

#[test]
fn test_detect_fencing_mixed_case() {
    assert!(detect_fencing_from_message("WRITER FENCED").is_fenced());
    assert!(detect_fencing_from_message("Writer Fenced").is_fenced());
    assert!(detect_fencing_from_message("wRiTeR fEnCeD").is_fenced());
}

// ============================================================================
// SlateDBError is_fenced() Tests
// ============================================================================

#[test]
fn test_slatedb_error_fenced_is_fenced() {
    let err = SlateDBError::Fenced;
    assert!(
        err.is_fenced(),
        "SlateDBError::Fenced should return is_fenced() = true"
    );
}

#[test]
fn test_slatedb_error_not_owned_is_not_fenced() {
    let err = SlateDBError::NotOwned {
        topic: "test-topic".to_string(),
        partition: 0,
    };
    assert!(
        !err.is_fenced(),
        "SlateDBError::NotOwned should return is_fenced() = false"
    );
}

#[test]
fn test_slatedb_error_storage_is_not_fenced() {
    let err = SlateDBError::Storage("Some storage error".to_string());
    assert!(
        !err.is_fenced(),
        "SlateDBError::Storage should return is_fenced() = false"
    );
}

#[test]
fn test_slatedb_error_config_is_not_fenced() {
    let err = SlateDBError::Config("Invalid config".to_string());
    assert!(
        !err.is_fenced(),
        "SlateDBError::Config should return is_fenced() = false"
    );
}

#[test]
fn test_slatedb_error_flush_failed_is_not_fenced() {
    let err = SlateDBError::FlushFailed {
        topic: "test-topic".to_string(),
        partition: 0,
        message: "Flush failed".to_string(),
    };
    assert!(
        !err.is_fenced(),
        "SlateDBError::FlushFailed should return is_fenced() = false"
    );
}

#[test]
fn test_slatedb_error_duplicate_sequence_is_not_fenced() {
    let err = SlateDBError::DuplicateSequence {
        producer_id: 123,
        expected_sequence: 5,
        received_sequence: 4,
    };
    assert!(
        !err.is_fenced(),
        "SlateDBError::DuplicateSequence should return is_fenced() = false"
    );
}

#[test]
fn test_slatedb_error_out_of_order_sequence_is_not_fenced() {
    let err = SlateDBError::OutOfOrderSequence {
        producer_id: 123,
        expected_sequence: 5,
        received_sequence: 7,
    };
    assert!(
        !err.is_fenced(),
        "SlateDBError::OutOfOrderSequence should return is_fenced() = false"
    );
}

#[test]
fn test_slatedb_error_fenced_producer_is_not_broker_fenced() {
    // FencedProducer is about producer epoch fencing, not broker fencing
    let err = SlateDBError::FencedProducer {
        producer_id: 123,
        expected_epoch: 5,
        actual_epoch: 3,
    };
    assert!(
        !err.is_fenced(),
        "SlateDBError::FencedProducer should return is_fenced() = false (it's producer fencing, not broker fencing)"
    );
}

#[test]
fn test_slatedb_error_lease_too_short_is_not_fenced() {
    let err = SlateDBError::LeaseTooShort {
        topic: "test-topic".to_string(),
        partition: 0,
        remaining_secs: 5,
        required_secs: 15,
    };
    assert!(
        !err.is_fenced(),
        "SlateDBError::LeaseTooShort should return is_fenced() = false"
    );
}

// ============================================================================
// Circuit Breaker Tests
// ============================================================================
// Circuit breaker tests use global state and must run serially to avoid
// interference from parallel test execution.

#[test]
#[serial]
fn test_circuit_breaker_initial_state() {
    // Reset circuit breaker to known state
    metrics::reset_circuit_breaker();

    let (count, tripped) = metrics::get_circuit_breaker_state();
    assert_eq!(count, 0, "Initial circuit breaker count should be 0");
    assert!(!tripped, "Initial circuit breaker should not be tripped");
}

#[test]
#[serial]
fn test_circuit_breaker_not_tripped_initially() {
    metrics::reset_circuit_breaker();
    assert!(
        !metrics::fail_closed_circuit_breaker_tripped(),
        "Circuit breaker should not be tripped with zero fail-closed events"
    );
}

#[test]
#[serial]
fn test_circuit_breaker_increments_on_fail_closed() {
    metrics::reset_circuit_breaker();

    // Record one fail_closed event
    let _ = metrics::record_fencing_detection_with_circuit_breaker("fail_closed");

    let (count, _) = metrics::get_circuit_breaker_state();
    // Count should be 1 or higher (may be higher if other tests ran)
    assert!(
        count >= 1,
        "fail_closed should increment counter, got {}",
        count
    );

    // Cleanup
    metrics::reset_circuit_breaker();
}

#[test]
#[serial]
fn test_circuit_breaker_reset_clears_counter() {
    // Record some events first
    let _ = metrics::record_fencing_detection_with_circuit_breaker("fail_closed");

    // Then reset
    metrics::reset_circuit_breaker();

    let (count, _) = metrics::get_circuit_breaker_state();
    assert_eq!(count, 0, "Reset should clear counter");
}

#[test]
#[serial]
fn test_circuit_breaker_confirmed_fencing_resets() {
    metrics::reset_circuit_breaker();

    // Record a fail_closed event
    let _ = metrics::record_fencing_detection_with_circuit_breaker("fail_closed");

    // Record a confirmed fencing event (typed)
    metrics::record_fencing_detection_with_circuit_breaker("typed");

    let (count, _) = metrics::get_circuit_breaker_state();
    assert_eq!(count, 0, "Counter should reset after confirmed fencing");

    // Cleanup
    metrics::reset_circuit_breaker();
}

#[test]
#[serial]
fn test_circuit_breaker_not_affected_by_not_fencing() {
    metrics::reset_circuit_breaker();

    // Record some not_fencing events
    metrics::record_fencing_detection_with_circuit_breaker("not_fencing");
    metrics::record_fencing_detection_with_circuit_breaker("not_fencing");

    let (count, _) = metrics::get_circuit_breaker_state();
    assert_eq!(count, 0, "not_fencing events should not increment counter");

    // Cleanup
    metrics::reset_circuit_breaker();
}

// ============================================================================
// FencingDetectionMethod is_fenced() Tests
// ============================================================================

#[test]
fn test_fencing_detection_method_is_fenced() {
    assert!(
        FencingDetectionMethod::TypedErrorKind.is_fenced(),
        "TypedErrorKind should be fenced"
    );
    assert!(
        FencingDetectionMethod::PatternMatch.is_fenced(),
        "PatternMatch should be fenced"
    );
    assert!(
        FencingDetectionMethod::FailClosed.is_fenced(),
        "FailClosed should be fenced"
    );
    assert!(
        !FencingDetectionMethod::NotFencing.is_fenced(),
        "NotFencing should not be fenced"
    );
}

// ============================================================================
// Error Message Pattern Tests
// ============================================================================

#[test]
fn test_fencing_pattern_version_conflict() {
    // Version conflict indicates manifest conflict - definite fencing
    assert!(detect_fencing_from_message("version conflict").is_fenced());
    assert!(detect_fencing_from_message("Version Conflict in manifest").is_fenced());
}

#[test]
fn test_fencing_pattern_writer_fenced() {
    // Writer fenced is explicit fencing
    assert!(detect_fencing_from_message("writer fenced").is_fenced());
    assert!(detect_fencing_from_message("writer fence detected").is_fenced());
}

#[test]
fn test_fencing_pattern_stale_epoch() {
    // Stale epoch indicates another writer took over
    assert!(detect_fencing_from_message("stale epoch").is_fenced());
}

#[test]
fn test_safe_pattern_timeout() {
    // Timeouts are transient, not fencing
    assert!(!detect_fencing_from_message("connection timeout").is_fenced());
    assert!(!detect_fencing_from_message("request timeout").is_fenced());
    assert!(!detect_fencing_from_message("operation timed out").is_fenced());
}

#[test]
fn test_safe_pattern_network() {
    // Network errors are transient, not fencing
    assert!(!detect_fencing_from_message("network error").is_fenced());
    assert!(!detect_fencing_from_message("connection refused").is_fenced());
    assert!(!detect_fencing_from_message("connection reset").is_fenced());
}

#[test]
fn test_safe_pattern_throttle() {
    // Note: Rate limiting and throttling are NOT currently in SAFE_PATTERNS.
    // They would be treated as unknown errors and potentially fenced via fail_closed.
    // This test verifies the current behavior - if this needs to change,
    // add "rate limit", "throttle", "too many requests" to SAFE_PATTERNS.

    // These patterns ARE in SAFE_PATTERNS and should not fence
    assert!(!detect_fencing_from_message("service unavailable").is_fenced());
    assert!(!detect_fencing_from_message("temporarily unavailable").is_fenced());
}

#[test]
fn test_safe_pattern_not_found() {
    // Not found errors are data errors, not fencing
    assert!(!detect_fencing_from_message("object not found").is_fenced());
    assert!(!detect_fencing_from_message("key not found").is_fenced());
    assert!(!detect_fencing_from_message("404 not found").is_fenced());
}

#[test]
fn test_safe_pattern_permission() {
    // Permission errors are auth errors, not fencing
    assert!(!detect_fencing_from_message("permission denied").is_fenced());
    assert!(!detect_fencing_from_message("access denied").is_fenced());
    assert!(!detect_fencing_from_message("unauthorized").is_fenced());
}
