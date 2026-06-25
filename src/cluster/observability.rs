//! Observability utilities for distributed tracing.
//!
//! This module provides instrumentation helpers for critical paths in the cluster.
//! When the `otel` feature is enabled, it uses OpenTelemetry for distributed tracing.
//! Otherwise, it uses regular tracing spans.
//!
//! # Critical Paths Instrumented
//!
//! - Partition ownership changes
//! - Consumer group rebalances
//! - Leader elections
//! - Raft proposals
//! - Produce/Fetch request handling
//!
//! # Usage
//!
//! ```rust,ignore
//! use kafkaesque::cluster::observability::{partition_acquire_span, OwnershipOperation};
//!
//! // Create a span for partition acquisition
//! let span = partition_acquire_span("orders", 5, 1, OwnershipOperation::Acquire);
//! async {
//!     // Your partition acquisition logic
//! }.instrument(span).await;
//! ```

use tracing::{Level, Span, span};

/// Operation type for ownership tracking.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnershipOperation {
    /// Acquiring a new partition.
    Acquire,
    /// Renewing an existing lease.
    Renew,
    /// Releasing partition ownership.
    Release,
    /// Transferring to another broker.
    Transfer,
}

impl std::fmt::Display for OwnershipOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Acquire => write!(f, "acquire"),
            Self::Renew => write!(f, "renew"),
            Self::Release => write!(f, "release"),
            Self::Transfer => write!(f, "transfer"),
        }
    }
}

/// Consumer group operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupOperation {
    /// Member joining a group.
    Join,
    /// Member syncing assignments.
    Sync,
    /// Member heartbeat.
    Heartbeat,
    /// Member leaving.
    Leave,
    /// Group rebalancing.
    Rebalance,
}

impl std::fmt::Display for GroupOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Join => write!(f, "join"),
            Self::Sync => write!(f, "sync"),
            Self::Heartbeat => write!(f, "heartbeat"),
            Self::Leave => write!(f, "leave"),
            Self::Rebalance => write!(f, "rebalance"),
        }
    }
}

/// Raft operation type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaftOperation {
    /// Proposing a command.
    Propose,
    /// Applying a command.
    Apply,
    /// Leader election.
    Election,
    /// Snapshot creation.
    Snapshot,
    /// Snapshot restoration.
    Restore,
}

impl std::fmt::Display for RaftOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Propose => write!(f, "propose"),
            Self::Apply => write!(f, "apply"),
            Self::Election => write!(f, "election"),
            Self::Snapshot => write!(f, "snapshot"),
            Self::Restore => write!(f, "restore"),
        }
    }
}

/// Create a span for partition ownership operations.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `broker_id` - The broker performing the operation
/// * `operation` - The type of ownership operation
pub fn partition_ownership_span(
    topic: &str,
    partition: i32,
    broker_id: i32,
    operation: OwnershipOperation,
) -> Span {
    span!(
        Level::INFO,
        "partition.ownership",
        topic = %topic,
        partition = %partition,
        broker_id = %broker_id,
        operation = %operation,
        otel.kind = "producer",
        otel.status_code = tracing::field::Empty,
    )
}

/// Create a span for consumer group operations.
///
/// # Arguments
/// * `group_id` - The consumer group ID
/// * `member_id` - The member ID (if applicable)
/// * `generation` - The group generation (if applicable)
/// * `operation` - The type of group operation
pub fn group_coordination_span(
    group_id: &str,
    member_id: Option<&str>,
    generation: Option<i32>,
    operation: GroupOperation,
) -> Span {
    span!(
        Level::INFO,
        "consumer_group.coordination",
        group_id = %group_id,
        member_id = member_id.unwrap_or("none"),
        generation = generation.unwrap_or(-1),
        operation = %operation,
        otel.kind = "producer",
        otel.status_code = tracing::field::Empty,
    )
}

/// Create a span for Raft operations.
///
/// # Arguments
/// * `node_id` - The Raft node ID
/// * `operation` - The type of Raft operation
/// * `command_type` - Optional command type for propose/apply operations
pub fn raft_operation_span(
    node_id: u64,
    operation: RaftOperation,
    command_type: Option<&str>,
) -> Span {
    span!(
        Level::INFO,
        "raft.operation",
        node_id = %node_id,
        operation = %operation,
        command_type = command_type.unwrap_or("none"),
        otel.kind = "internal",
        otel.status_code = tracing::field::Empty,
    )
}

/// Create a span for produce requests.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `record_count` - Number of records being produced
/// * `_client_id` - Reserved (ignored); client_id is per-request high-cardinality
///   and would balloon trace storage when sampling is disabled.
pub fn produce_request_span(
    topic: &str,
    partition: i32,
    record_count: usize,
    _client_id: Option<&str>,
) -> Span {
    span!(
        Level::DEBUG,
        "kafka.produce",
        topic = %topic,
        partition = %partition,
        record_count = %record_count,
        otel.kind = "server",
        otel.status_code = tracing::field::Empty,
    )
}

/// Create a span for fetch requests.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `fetch_offset` - The starting offset
/// * `_client_id` - Reserved (ignored); see `produce_request_span`.
pub fn fetch_request_span(
    topic: &str,
    partition: i32,
    fetch_offset: i64,
    _client_id: Option<&str>,
) -> Span {
    span!(
        Level::DEBUG,
        "kafka.fetch",
        topic = %topic,
        partition = %partition,
        fetch_offset = %fetch_offset,
        otel.kind = "server",
        otel.status_code = tracing::field::Empty,
    )
}

/// Create a span for failure detection events.
///
/// # Arguments
/// * `broker_id` - The broker being monitored
/// * `event` - The type of failure event (e.g., "timeout", "recovered")
pub fn failure_detection_span(broker_id: i32, event: &str) -> Span {
    span!(
        Level::WARN,
        "failure_detection",
        broker_id = %broker_id,
        event = %event,
        otel.kind = "internal",
    )
}

/// Create a span for partition transfer operations.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `from_broker` - The source broker ID
/// * `to_broker` - The destination broker ID
/// * `reason` - The reason for transfer
pub fn partition_transfer_span(
    topic: &str,
    partition: i32,
    from_broker: i32,
    to_broker: i32,
    reason: &str,
) -> Span {
    span!(
        Level::INFO,
        "partition.transfer",
        topic = %topic,
        partition = %partition,
        from_broker = %from_broker,
        to_broker = %to_broker,
        reason = %reason,
        otel.kind = "internal",
        otel.status_code = tracing::field::Empty,
    )
}

/// Record an error on the current span.
///
/// This sets the `otel.status_code` to "ERROR" and records the error message.
#[macro_export]
macro_rules! record_error {
    ($span:expr, $err:expr) => {
        $span.record("otel.status_code", "ERROR");
        tracing::error!(parent: &$span, error = %$err, "Operation failed");
    };
}

/// Record success on the current span.
///
/// This sets the `otel.status_code` to "OK".
#[macro_export]
macro_rules! record_success {
    ($span:expr) => {
        $span.record("otel.status_code", "OK");
    };
}

/// `tracing::error!` with per-call-site rate limiting.
///
/// During a sustained outage (e.g. S3 unreachable) the produce/fetch hot
/// paths can fire one error per request, which floods log shippers and
/// amplifies the incident itself. This macro logs the first
/// `MAX_PER_WINDOW` events in each fixed window, emits a single
/// throttling notice on the (MAX+1)th, then drops the rest until the
/// window rolls. The counter lives in a `static` so each call site
/// throttles independently — one noisy site never silences another.
///
/// Usage: drop-in replacement for `tracing::error!`.
#[macro_export]
macro_rules! error_throttled {
    ($($arg:tt)*) => {{
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNT: AtomicU64 = AtomicU64::new(0);
        static WINDOW_START_MS: AtomicU64 = AtomicU64::new(0);
        const WINDOW_MS: u64 = 60_000;
        const MAX_PER_WINDOW: u64 = 10;

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let window = WINDOW_START_MS.load(Ordering::Relaxed);
        if now_ms.saturating_sub(window) >= WINDOW_MS {
            WINDOW_START_MS.store(now_ms, Ordering::Relaxed);
            COUNT.store(0, Ordering::Relaxed);
        }
        let n = COUNT.fetch_add(1, Ordering::Relaxed);
        if n < MAX_PER_WINDOW {
            tracing::error!($($arg)*);
        } else if n == MAX_PER_WINDOW {
            tracing::warn!(
                throttled_after = MAX_PER_WINDOW,
                window_ms = WINDOW_MS,
                "Suppressing further error logs from this call site for the rest of the window"
            );
        }
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::Instrument;

    #[test]
    fn test_ownership_operation_display() {
        assert_eq!(OwnershipOperation::Acquire.to_string(), "acquire");
        assert_eq!(OwnershipOperation::Renew.to_string(), "renew");
        assert_eq!(OwnershipOperation::Release.to_string(), "release");
        assert_eq!(OwnershipOperation::Transfer.to_string(), "transfer");
    }

    #[test]
    fn test_group_operation_display() {
        assert_eq!(GroupOperation::Join.to_string(), "join");
        assert_eq!(GroupOperation::Sync.to_string(), "sync");
        assert_eq!(GroupOperation::Heartbeat.to_string(), "heartbeat");
        assert_eq!(GroupOperation::Leave.to_string(), "leave");
        assert_eq!(GroupOperation::Rebalance.to_string(), "rebalance");
    }

    #[test]
    fn test_raft_operation_display() {
        assert_eq!(RaftOperation::Propose.to_string(), "propose");
        assert_eq!(RaftOperation::Apply.to_string(), "apply");
        assert_eq!(RaftOperation::Election.to_string(), "election");
        assert_eq!(RaftOperation::Snapshot.to_string(), "snapshot");
        assert_eq!(RaftOperation::Restore.to_string(), "restore");
    }

    #[tokio::test]
    async fn test_partition_ownership_span() {
        let span = partition_ownership_span("test-topic", 0, 1, OwnershipOperation::Acquire);
        async {
            // Simulated work
        }
        .instrument(span)
        .await;
    }

    #[tokio::test]
    async fn test_group_coordination_span() {
        let span = group_coordination_span(
            "test-group",
            Some("member-1"),
            Some(1),
            GroupOperation::Join,
        );
        async {
            // Simulated work
        }
        .instrument(span)
        .await;
    }

    #[tokio::test]
    async fn test_raft_operation_span() {
        let span = raft_operation_span(1, RaftOperation::Propose, Some("RegisterBroker"));
        async {
            // Simulated work
        }
        .instrument(span)
        .await;
    }

    #[tokio::test]
    async fn test_produce_request_span() {
        let span = produce_request_span("orders", 0, 100, Some("producer-1"));
        async {
            // Simulated work
        }
        .instrument(span)
        .await;
    }

    #[tokio::test]
    async fn test_fetch_request_span() {
        let span = fetch_request_span("orders", 0, 0, Some("consumer-1"));
        async {
            // Simulated work
        }
        .instrument(span)
        .await;
    }

    #[tokio::test]
    async fn test_failure_detection_span() {
        let span = failure_detection_span(1, "timeout");
        async {
            // Simulated work
        }
        .instrument(span)
        .await;
    }

    #[tokio::test]
    async fn test_partition_transfer_span() {
        let span = partition_transfer_span("orders", 0, 1, 2, "load_balancing");
        async {
            // Simulated work
        }
        .instrument(span)
        .await;
    }
}
