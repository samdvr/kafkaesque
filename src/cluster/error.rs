//! Error types for SlateDB storage backend.
//!
//! # Error Handling Patterns
//!
//! This crate uses two error handling patterns based on operation criticality:
//!
//! ## Fail-Fast (Propagate Errors)
//!
//! Used for operations where failure indicates a serious problem:
//! - Partition ownership acquisition
//! - Produce/write operations
//! - Consumer group membership operations
//! - Topic creation/deletion
//!
//! Example:
//! ```text
//! pub async fn get_topics(&self) -> SlateDBResult<Vec<String>> {
//!     let topics: Vec<String> = conn.smembers("kafkaesque:topics").await?;
//!     Ok(topics)
//! }
//! ```
//!
//! ## Best-Effort (Log and Continue)
//!
//! Used for operations where partial failure is acceptable:
//! - Fetching member assignments (client can retry)
//! - Getting all consumer groups (used for monitoring)
//! - Metric collection
//! - Background eviction scans
//!
//! Example:
//! ```text
//! async fn get_all_groups(&self) -> SlateDBResult<Vec<String>> {
//!     let groups: Vec<String> = match conn.smembers("kafkaesque:groups").await {
//!         Ok(g) => g,
//!         Err(e) => {
//!             warn!(error = %e, "Failed to get group list");
//!             Vec::new() // Return empty list, caller handles gracefully
//!         }
//!     };
//!     Ok(groups)
//! }
//! ```
//!
//! ## Guidelines
//!
//! - **Write path**: Always fail-fast to prevent data loss
//! - **Read path**: Can be best-effort if stale/empty data is acceptable
//! - **Coordination**: Fail-fast for ownership, best-effort for monitoring
//! - **Background tasks**: Best-effort with logging for observability
//!
//! # Fencing Detection
//!
//! Fencing errors indicate that another writer has taken over the partition.
//! These are critical for split-brain prevention. The detection hierarchy is:
//!
//! 1. **Typed detection** (highest confidence): `ErrorKind::Closed(CloseReason::Fenced)`
//! 2. **Pattern matching** (medium confidence): Known fencing error message patterns
//! 3. **Propagate** (unknown): Unrecognized errors are returned as storage errors
//!    (no auto-fencing — avoids mass partition release on transient glitches)
//!
//! See [`FencingDetection`] for detection results and [`detect_fencing`] for the detector.

use thiserror::Error;

/// Result type for SlateDB operations.
pub type SlateDBResult<T> = Result<T, SlateDBError>;

/// How a fencing error was detected.
///
/// This is used for observability and debugging to understand
/// the reliability of fencing detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FencingDetectionMethod {
    /// Detected via typed SlateDB error kind (highest confidence).
    TypedErrorKind,
    /// Detected via error message pattern matching (medium confidence).
    PatternMatch,
    /// Unknown error treated as fencing (fail-closed, lowest confidence).
    FailClosed,
    /// Not a fencing error.
    NotFencing,
}

impl FencingDetectionMethod {
    /// Returns true if this indicates a fencing error.
    pub fn is_fenced(&self) -> bool {
        !matches!(self, FencingDetectionMethod::NotFencing)
    }

    /// Returns a string label for metrics.
    pub fn as_metric_label(&self) -> &'static str {
        match self {
            FencingDetectionMethod::TypedErrorKind => "typed",
            FencingDetectionMethod::PatternMatch => "pattern",
            FencingDetectionMethod::FailClosed => "fail_closed",
            FencingDetectionMethod::NotFencing => "not_fencing",
        }
    }
}

/// Result of a heartbeat operation with generation verification.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatResult {
    /// Heartbeat was recorded successfully.
    Success,
    /// Member ID is unknown (not in the group).
    UnknownMember,
    /// Generation ID doesn't match current group generation.
    IllegalGeneration,
    /// Group is in the middle of a rebalance — caller must rejoin.
    RebalanceInProgress,
}

/// Errors that can occur in the SlateDB storage backend.
#[derive(Debug, Error)]
pub enum SlateDBError {
    /// Error from SlateDB operations.
    #[error("SlateDB error: {0}")]
    SlateDB(String),

    /// Error from Raft operations.
    #[error("Raft error: {0}")]
    Raft(String),

    /// Error from object store operations.
    #[error("Object store error: {0}")]
    ObjectStore(#[from] object_store::Error),

    /// Generic storage error (e.g., delete operations).
    #[error("Storage error: {0}")]
    Storage(String),

    /// This broker was fenced out by another writer.
    #[error("Writer fenced out")]
    Fenced,

    /// Partition is not owned by this broker.
    #[error("Partition {topic}/{partition} not owned by this broker")]
    NotOwned { topic: String, partition: i32 },

    /// Partition not found.
    #[error("Partition {topic}/{partition} not found")]
    PartitionNotFound { topic: String, partition: i32 },

    /// Invalid partition index.
    #[error("Invalid partition index {partition} for topic {topic}")]
    InvalidPartition { topic: String, partition: i32 },

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Duplicate sequence number - batch was already committed.
    #[error(
        "Duplicate sequence for producer {producer_id}: expected {expected_sequence}, got {received_sequence}"
    )]
    DuplicateSequence {
        producer_id: i64,
        expected_sequence: i32,
        received_sequence: i32,
    },

    /// Out-of-order sequence number - gap in sequence.
    #[error(
        "Out-of-order sequence for producer {producer_id}: expected {expected_sequence}, got {received_sequence}"
    )]
    OutOfOrderSequence {
        producer_id: i64,
        expected_sequence: i32,
        received_sequence: i32,
    },

    /// Fenced producer - producer epoch is stale.
    ///
    /// This error is returned when a producer with a lower epoch than the
    /// current tracked epoch attempts to write. This typically indicates
    /// a zombie producer that should no longer be writing.
    #[error("Fenced producer {producer_id}: expected epoch {expected_epoch}, got {actual_epoch}")]
    FencedProducer {
        producer_id: i64,
        expected_epoch: i16,
        actual_epoch: i16,
    },

    /// Lease TTL too short to safely complete write operation.
    ///
    /// This error is returned proactively when the remaining lease time is
    /// below the safety threshold, preventing the TOCTOU race condition
    /// between lease verification and write completion.
    #[error(
        "Lease too short for {topic}/{partition}: {remaining_secs}s remaining, need {required_secs}s"
    )]
    LeaseTooShort {
        topic: String,
        partition: i32,
        remaining_secs: u64,
        required_secs: u64,
    },

    /// Leader epoch mismatch - another broker has acquired this partition.
    ///
    /// This error is returned when the stored leader epoch in SlateDB doesn't
    /// match our expected epoch, indicating that another broker has taken over
    /// ownership of this partition. This prevents TOCTOU races where we might
    /// write to a partition we no longer own.
    ///
    /// This is a critical safety error - the write must be rejected and the
    /// partition ownership should be released.
    #[error(
        "Epoch mismatch for {topic}/{partition}: expected {expected_epoch}, found {stored_epoch}"
    )]
    EpochMismatch {
        topic: String,
        partition: i32,
        expected_epoch: i32,
        stored_epoch: i32,
    },

    /// Sequence number overflow - producer's sequence numbers exhausted.
    ///
    /// This error is returned when a producer's sequence number would overflow
    /// i32::MAX. The producer should obtain a new producer_id.
    #[error("Sequence overflow for producer {producer_id} on {topic}/{partition}")]
    SequenceOverflow {
        producer_id: i64,
        topic: String,
        partition: i32,
    },

    /// Flush operation failed - durability cannot be guaranteed.
    ///
    /// This error is returned when a write succeeded locally but the flush
    /// to durable storage failed. The client should retry.
    #[error("Flush failed for {topic}/{partition}: {message}")]
    FlushFailed {
        topic: String,
        partition: i32,
        message: String,
    },

    /// Serialization/deserialization error.
    #[error("Serialization error: {0}")]
    Serde(#[from] serde_json::Error),

    /// IO error.
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Protocol-level error (wraps crate::error::Error).
    #[error("Protocol error: {0}")]
    Protocol(String),

    /// A record batch's wire format is malformed (record-count mismatch,
    /// truncated header, invalid CRC at parse time, etc.). Distinct from
    /// `Config` so it maps to `KafkaCode::CorruptMessage` rather than
    /// `KafkaCode::InvalidTopic` — librdkafka and kafka-python evict a
    /// topic from their metadata cache on `InvalidTopic`, turning a
    /// transient malformed batch from one producer into a permanent
    /// "topic gone" error for every co-tenant client.
    #[error("Corrupt batch on {topic}/{partition}: {reason}")]
    CorruptBatch {
        topic: String,
        partition: i32,
        reason: String,
    },
}

// ============================================================================
// Unified Error-to-KafkaCode Mapping (R3 Refactoring)
// ============================================================================
//
// This provides centralized mapping from SlateDBError to Kafka error codes.
// All handlers should use this instead of ad-hoc error mapping.

impl SlateDBError {
    /// Convert this error to the appropriate Kafka error code.
    ///
    /// This provides unified error mapping across all handlers, ensuring
    /// consistent error responses to clients.
    ///
    /// # Mapping Rules
    ///
    /// | SlateDBError              | KafkaCode                        |
    /// |---------------------------|----------------------------------|
    /// | Fenced                    | NotLeaderForPartition            |
    /// | NotOwned                  | NotLeaderForPartition            |
    /// | LeaseTooShort             | NotLeaderForPartition            |
    /// | PartitionNotFound         | UnknownTopicOrPartition          |
    /// | DuplicateSequence         | DuplicateSequenceNumber          |
    /// | OutOfOrderSequence        | OutOfOrderSequenceNumber         |
    /// | FencedProducer            | InvalidProducerEpoch             |
    /// | SequenceOverflow          | UnknownProducerId                |
    /// | Config                    | InvalidTopic (closest match)     |
    /// | Raft/Storage/IO           | Unknown (retriable)              |
    /// | Protocol                  | CorruptMessage                   |
    ///
    pub fn to_kafka_code(&self) -> crate::error::KafkaCode {
        use crate::error::KafkaCode;

        match self {
            // Ownership/leadership errors - client should refresh metadata
            SlateDBError::Fenced => KafkaCode::NotLeaderForPartition,
            SlateDBError::NotOwned { .. } => KafkaCode::NotLeaderForPartition,
            SlateDBError::LeaseTooShort { .. } => KafkaCode::NotLeaderForPartition,
            SlateDBError::EpochMismatch { .. } => KafkaCode::NotLeaderForPartition,

            // Topic/partition not found
            SlateDBError::PartitionNotFound { .. } => KafkaCode::UnknownTopicOrPartition,
            SlateDBError::InvalidPartition { .. } => KafkaCode::InvalidTopic,

            // Idempotent producer errors
            SlateDBError::DuplicateSequence { .. } => KafkaCode::DuplicateSequenceNumber,
            SlateDBError::OutOfOrderSequence { .. } => KafkaCode::OutOfOrderSequenceNumber,
            // Fenced producer (stale epoch) — InvalidProducerEpoch (47) is the
            // canonical Kafka signal; mapping to OutOfOrderSequenceNumber caused
            // clients to retry with the same epoch until `delivery.timeout.ms`.
            SlateDBError::FencedProducer { .. } => KafkaCode::InvalidProducerEpoch,
            // Sequence overflow — producer should get a new producer_id; clients
            // re-init on UnknownProducerId (59).
            SlateDBError::SequenceOverflow { .. } => KafkaCode::UnknownProducerId,

            // Flush failure - client should retry (retriable infrastructure error)
            SlateDBError::FlushFailed { .. } => KafkaCode::Unknown,

            // Configuration errors - use InvalidTopic as closest match
            SlateDBError::Config(_) => KafkaCode::InvalidTopic,

            // Protocol/request errors
            SlateDBError::Protocol(_) => KafkaCode::CorruptMessage,
            SlateDBError::Serde(_) => KafkaCode::CorruptMessage,
            SlateDBError::CorruptBatch { .. } => KafkaCode::CorruptMessage,

            // Storage/infrastructure errors - generic but retriable
            SlateDBError::SlateDB(_) => KafkaCode::Unknown,
            SlateDBError::Raft(_) => KafkaCode::Unknown,
            SlateDBError::ObjectStore(_) => KafkaCode::Unknown,
            SlateDBError::Storage(_) => KafkaCode::Unknown,
            SlateDBError::Io(_) => KafkaCode::Unknown,
        }
    }

    /// Check if this error indicates the broker is not the leader for the partition.
    ///
    /// Useful for determining if clients should refresh metadata.
    /// This includes fencing, ownership loss, and lease expiration.
    #[inline]
    pub fn is_not_leader(&self) -> bool {
        matches!(
            self,
            SlateDBError::Fenced
                | SlateDBError::NotOwned { .. }
                | SlateDBError::LeaseTooShort { .. }
                | SlateDBError::EpochMismatch { .. }
        )
    }

    /// Check if this is specifically a fencing error.
    ///
    /// Fencing occurs when another writer takes over a partition,
    /// indicating the single-writer guarantee was violated.
    ///
    /// This method detects fencing via:
    /// 1. The explicit `Fenced` variant (highest confidence)
    /// 2. String pattern matching on `SlateDB` error messages (medium confidence)
    ///
    /// Note: The `From<slatedb::Error>` impl already converts most fencing errors
    /// to `SlateDBError::Fenced`, but this method also handles cases where
    /// `SlateDBError::SlateDB(msg)` is constructed directly or the circuit breaker
    /// prevented automatic fencing detection.
    #[inline]
    pub fn is_fenced(&self) -> bool {
        match self {
            SlateDBError::Fenced => true,
            SlateDBError::SlateDB(msg) => detect_fencing_from_message(msg).is_fenced(),
            _ => false,
        }
    }

    /// Check if this error is retriable (transient infrastructure issue).
    ///
    /// Properly distinguishes transient vs permanent errors.
    /// For object store errors, this examines the actual error kind to determine
    /// if the operation should be retried.
    #[inline]
    pub fn is_retriable(&self) -> bool {
        match self {
            // Raft and storage errors are generally transient
            SlateDBError::Raft(_) => true,
            SlateDBError::Storage(_) => true,
            SlateDBError::SlateDB(_) => true,
            SlateDBError::FlushFailed { .. } => true,

            // IO errors: check the kind
            SlateDBError::Io(e) => Self::is_io_error_retryable(e),

            // Object store errors need fine-grained classification
            SlateDBError::ObjectStore(e) => Self::is_object_store_error_retryable(e),

            // These are NOT retriable - fix the underlying issue
            SlateDBError::Config(_) => false,
            SlateDBError::Fenced => false,
            SlateDBError::NotOwned { .. } => false,
            SlateDBError::PartitionNotFound { .. } => false,
            SlateDBError::InvalidPartition { .. } => false,
            SlateDBError::DuplicateSequence { .. } => false,
            SlateDBError::OutOfOrderSequence { .. } => false,
            SlateDBError::FencedProducer { .. } => false,
            SlateDBError::SequenceOverflow { .. } => false,
            SlateDBError::LeaseTooShort { .. } => false,
            SlateDBError::EpochMismatch { .. } => false, // Another broker owns partition
            SlateDBError::Protocol(_) => false,
            SlateDBError::Serde(_) => false,
            SlateDBError::CorruptBatch { .. } => false,
        }
    }

    /// Classify object_store errors as retryable or permanent.
    ///
    /// This method examines the object_store error kind to determine if
    /// an operation should be retried. Transient errors (network issues,
    /// temporary unavailability) return true. Permanent errors (not found,
    /// permission denied, invalid data) return false.
    fn is_object_store_error_retryable(e: &object_store::Error) -> bool {
        use object_store::Error as ObjErr;

        match e {
            // Definitely NOT retryable - permanent failures
            ObjErr::NotFound { .. } => false,
            ObjErr::AlreadyExists { .. } => false,
            ObjErr::Precondition { .. } => false, // Conditional write failed
            ObjErr::NotSupported { .. } => false,
            ObjErr::InvalidPath { .. } => false,
            ObjErr::JoinError { .. } => false,
            ObjErr::NotImplemented => false,
            ObjErr::PermissionDenied { .. } => false,
            ObjErr::Unauthenticated { .. } => false,
            ObjErr::UnknownConfigurationKey { .. } => false,

            // Definitely retryable - transient failures
            ObjErr::NotModified { .. } => true,

            // Generic error - examine the message for hints
            ObjErr::Generic { source, .. } => {
                // Check for common transient error patterns in source
                let msg = source.to_string().to_lowercase();
                msg.contains("timeout")
                    || msg.contains("connection")
                    || msg.contains("throttl")
                    || msg.contains("rate limit")
                    || msg.contains("temporary")
                    || msg.contains("unavailable")
                    || msg.contains("retry")
                    || msg.contains("503")
                    || msg.contains("500")
                    || msg.contains("429")
            }

            // Unknown variants - default to retryable (safer for transient issues)
            #[allow(unreachable_patterns)]
            _ => true,
        }
    }

    /// Check if an IO error is retryable.
    fn is_io_error_retryable(e: &std::io::Error) -> bool {
        use std::io::ErrorKind;

        match e.kind() {
            // Definitely retryable
            ErrorKind::ConnectionRefused => true,
            ErrorKind::ConnectionReset => true,
            ErrorKind::ConnectionAborted => true,
            ErrorKind::NotConnected => true,
            ErrorKind::BrokenPipe => true,
            ErrorKind::TimedOut => true,
            ErrorKind::Interrupted => true,
            ErrorKind::WouldBlock => true,
            ErrorKind::WriteZero => true,
            ErrorKind::UnexpectedEof => true,

            // NOT retryable - permanent failures
            ErrorKind::NotFound => false,
            ErrorKind::PermissionDenied => false,
            ErrorKind::AlreadyExists => false,
            ErrorKind::InvalidInput => false,
            ErrorKind::InvalidData => false,
            ErrorKind::AddrInUse => false,
            ErrorKind::AddrNotAvailable => false,

            // Other errors - default to retryable
            _ => true,
        }
    }

    /// Check if this is a permission/authentication error.
    ///
    /// These errors indicate configuration issues that need manual intervention,
    /// not transient failures that will resolve with retries.
    pub fn is_permission_error(&self) -> bool {
        match self {
            SlateDBError::ObjectStore(e) => {
                matches!(
                    e,
                    object_store::Error::PermissionDenied { .. }
                        | object_store::Error::Unauthenticated { .. }
                )
            }
            SlateDBError::Io(e) => e.kind() == std::io::ErrorKind::PermissionDenied,
            _ => false,
        }
    }

    /// Check if this is a "not found" error.
    ///
    /// These errors indicate the resource doesn't exist (vs transient unavailability).
    pub fn is_not_found(&self) -> bool {
        match self {
            SlateDBError::ObjectStore(e) => matches!(e, object_store::Error::NotFound { .. }),
            SlateDBError::Io(e) => e.kind() == std::io::ErrorKind::NotFound,
            SlateDBError::PartitionNotFound { .. } => true,
            _ => false,
        }
    }

    /// Check if this is an idempotency-related error.
    #[inline]
    pub fn is_idempotency_error(&self) -> bool {
        matches!(
            self,
            SlateDBError::DuplicateSequence { .. }
                | SlateDBError::OutOfOrderSequence { .. }
                | SlateDBError::FencedProducer { .. }
                | SlateDBError::SequenceOverflow { .. }
        )
    }
}

/// Error category for SlateDB errors (used internally for classification)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ErrorCategory {
    /// Fencing error - another writer has taken over
    Fenced,
    /// Safe/recoverable error - transient or expected
    Safe,
    /// Unknown error — propagate without auto-fencing to avoid mass lease
    /// release on transient glitches.
    Unknown,
}

/// Classify an error message into a category based on string patterns.
fn classify_error_message(msg: &str) -> ErrorCategory {
    // Comprehensive fencing pattern detection
    const FENCING_PATTERNS: &[&str] = &[
        // Explicit fencing terms
        "fenced",
        "fence",
        // Writer ID conflicts (single-writer guarantee violation)
        "writer id mismatch",
        "writer id conflict",
        "writer conflict",
        // Manifest conflicts (concurrent write detection)
        "manifest conflict",
        "manifest version",
        "version conflict",
        "version mismatch",
        // Object store conflict patterns (conditional write failures)
        "conditional write failed",
        "precondition failed",
        "etag mismatch",
        "if-match",
        "if-none-match",
        // S3-specific conflict patterns
        "conditional request failed",
        "conflictingoperationinprogress",
        // GCS-specific conflict patterns
        "conditionnotmet",
        // Azure-specific conflict patterns
        "leaseidmismatch",
        // Generic conflict indicators that likely mean fencing
        "already exists with different",
        "concurrent modification",
        // Leader epoch / writer generation conflicts
        "stale epoch",
        "epoch mismatch",
    ];

    // Known non-fencing errors that are safe to propagate
    const SAFE_PATTERNS: &[&str] = &[
        // I/O and network errors (transient)
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
        // Storage capacity errors
        "no space",
        "disk full",
        "quota exceeded",
        "storage limit",
        // Not found errors (expected in normal operation)
        "not found",
        "does not exist",
        "no such",
        "object not found",
        // Corruption/data errors (different from fencing)
        "corrupt",
        "checksum",
        "invalid data",
        "parse error",
        "deserialize",
        "serialize",
        "malformed",
        // Configuration errors
        "invalid config",
        "configuration",
        "invalid argument",
        // Shutdown/closed errors
        "shutdown",
        "cancelled",
        "aborted",
        "closed",
        // Permission errors (not fencing - config issue)
        "access denied",
        "permission denied",
        "unauthorized",
        "forbidden",
    ];

    let msg_lower = msg.to_lowercase();

    if FENCING_PATTERNS.iter().any(|p| msg_lower.contains(p)) {
        ErrorCategory::Fenced
    } else if SAFE_PATTERNS.iter().any(|p| msg_lower.contains(p)) {
        ErrorCategory::Safe
    } else {
        ErrorCategory::Unknown
    }
}

/// Detect fencing from an error message using pattern matching.
///
/// This is a public utility function for testing and debugging fencing detection.
/// It returns the detection method used, which indicates confidence level.
///
/// # Arguments
/// * `error_msg` - The error message to analyze
///
/// # Returns
/// * `FencingDetectionMethod::PatternMatch` - Known fencing pattern detected
/// * `FencingDetectionMethod::NotFencing` - Known safe pattern detected
/// * `FencingDetectionMethod::NotFencing` - Unknown or safe pattern (propagate)
///
/// # Example
/// ```
/// use kafkaesque::cluster::{detect_fencing_from_message, FencingDetectionMethod};
///
/// let method = detect_fencing_from_message("Writer was fenced out");
/// assert_eq!(method, FencingDetectionMethod::PatternMatch);
///
/// let method = detect_fencing_from_message("Connection timeout");
/// assert_eq!(method, FencingDetectionMethod::NotFencing);
/// ```
pub fn detect_fencing_from_message(error_msg: &str) -> FencingDetectionMethod {
    match classify_error_message(error_msg) {
        ErrorCategory::Fenced => FencingDetectionMethod::PatternMatch,
        ErrorCategory::Safe => FencingDetectionMethod::NotFencing,
        ErrorCategory::Unknown => FencingDetectionMethod::NotFencing,
    }
}

/// Convert a cluster-layer [`SlateDBError`] into a wire-protocol
/// [`crate::error::Error`].
///
/// Lives here (rather than in the protocol crate) so the protocol
/// crate can stay free of any dependency on cluster types — that's a
/// key property of the workspace split: `kafkaesque-protocol` is
/// runtime-independent and reusable on its own.
impl From<SlateDBError> for crate::error::Error {
    fn from(e: SlateDBError) -> Self {
        match e {
            SlateDBError::Io(io_err) => crate::error::Error::IoError(io_err.into()),
            SlateDBError::Config(msg) => crate::error::Error::Config(msg),
            other => crate::error::Error::Config(other.to_string()),
        }
    }
}

impl From<slatedb::Error> for SlateDBError {
    fn from(e: slatedb::Error) -> Self {
        // Robust fencing detection with fail-closed behavior and circuit breaker
        //
        // CRITICAL: Split-brain prevention is paramount. We use a fail-closed approach:
        // - If we detect known fencing patterns -> return Fenced
        // - If we detect known non-fencing patterns -> return SlateDB error
        // - If error is UNKNOWN -> treat as potential fencing (fail closed)
        //   BUT: Use circuit breaker to prevent cascading unavailability
        //   from transient unknown errors.

        use slatedb::{CloseReason, ErrorKind};

        // Primary detection: Match on ErrorKind (stable API, highest confidence)
        let detection_method = match e.kind() {
            ErrorKind::Closed(CloseReason::Fenced) => FencingDetectionMethod::TypedErrorKind,
            _ => {
                // Secondary detection: String matching fallback
                detect_fencing_from_message(&e.to_string())
            }
        };

        // Record metric and check circuit breaker
        let should_fence = super::metrics::record_fencing_detection_with_circuit_breaker(
            detection_method.as_metric_label(),
        );

        match detection_method {
            FencingDetectionMethod::TypedErrorKind => {
                tracing::warn!(
                    error = %e,
                    detection_method = "typed",
                    "Detected fencing error from SlateDB (CloseReason::Fenced)"
                );
                super::metrics::record_coordinator_failure("fencing_detected");
                SlateDBError::Fenced
            }
            FencingDetectionMethod::PatternMatch => {
                tracing::warn!(
                    error = %e,
                    error_kind = ?e.kind(),
                    detection_method = "pattern",
                    "Detected fencing error from SlateDB via string matching"
                );
                super::metrics::record_coordinator_failure("fencing_detected");
                SlateDBError::Fenced
            }
            FencingDetectionMethod::FailClosed => {
                if should_fence {
                    // Circuit breaker hasn't tripped - fail closed as usual
                    tracing::error!(
                        error = %e,
                        error_kind = ?e.kind(),
                        detection_method = "fail_closed",
                        "UNKNOWN SlateDB error - treating as potential fencing (fail-closed). \
                         If this is a false positive, add the pattern to SAFE_PATTERNS."
                    );
                    super::metrics::record_coordinator_failure("unknown_error_fail_closed");
                    SlateDBError::Fenced
                } else {
                    // Circuit breaker tripped - don't auto-fence
                    tracing::error!(
                        error = %e,
                        error_kind = ?e.kind(),
                        detection_method = "fail_closed_circuit_breaker",
                        "UNKNOWN SlateDB error - circuit breaker TRIPPED, NOT treating as fencing. \
                         Too many consecutive unknown errors without confirmed fencing. \
                         Investigate the root cause of these errors."
                    );
                    super::metrics::record_coordinator_failure(
                        "fail_closed_circuit_breaker_tripped",
                    );
                    SlateDBError::SlateDB(e.to_string())
                }
            }
            FencingDetectionMethod::NotFencing => {
                tracing::debug!(
                    error = %e,
                    error_kind = ?e.kind(),
                    detection_method = "not_fencing",
                    "SlateDB error (non-fencing, safe)"
                );
                SlateDBError::SlateDB(e.to_string())
            }
        }
    }
}

#[cfg(test)]
#[path = "error_tests.rs"]
mod tests;
