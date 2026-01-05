//! Unified retry policies for consistent backoff behavior across the codebase.
//!
//! This module replaces ad-hoc retry implementations with standardized policies
//! using the `backon` crate.
//!
//! # Design Goals
//!
//! - **Consistency**: All retries use the same patterns
//! - **Jitter**: All policies include jitter to prevent thundering herd
//! - **Observability**: Retry attempts are tracked via metrics
//! - **Clarity**: Named policies make intent clear
//!
//! # Available Policies
//!
//! | Policy | Min Delay | Max Delay | Retries | Use Case |
//! |--------|-----------|-----------|---------|----------|
//! | `coordinator_policy` | 50ms | 5s | 10 | Raft coordination ops |
//! | `storage_policy` | 10ms | 500ms | 3 | SlateDB writes |
//! | `network_policy` | 100ms | 10s | 5 | Network/RPC calls |
//! | `fast_policy` | 5ms | 100ms | 3 | Hot path retries |
//!
//! # Example
//!
//! ```rust,no_run
//! use kafkaesque::cluster::retry;
//! use backon::Retryable;
//!
//! async fn example() -> Result<(), Box<dyn std::error::Error>> {
//!     // Using retry with a closure
//!     let result = (|| async {
//!         // your fallible operation
//!         Ok::<_, std::io::Error>(())
//!     })
//!     .retry(retry::coordinator_policy())
//!     .when(|e| e.kind() == std::io::ErrorKind::TimedOut)
//!     .await?;
//!
//!     Ok(())
//! }
//! ```

use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};

/// Policy for Raft coordination operations (register, heartbeat, acquire).
///
/// Characteristics:
/// - Moderate initial delay (50ms) to avoid overwhelming Raft leader
/// - Long max delay (5s) for leader election scenarios
/// - Many retries (10) for transient failures during leadership changes
/// - Includes jitter to prevent thundering herd
///
/// Use for:
/// - `register_broker()`
/// - `acquire_partition()`
/// - `renew_partition_lease()`
/// - Any Raft write operation
pub fn coordinator_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(50))
        .with_max_delay(Duration::from_secs(5))
        .with_max_times(10)
        .with_jitter()
}

/// Policy for storage (SlateDB) operations.
///
/// Characteristics:
/// - Short initial delay (10ms) for fast recovery from transient issues
/// - Short max delay (500ms) to fail fast if storage is truly unavailable
/// - Few retries (3) since storage errors are often persistent
/// - Includes jitter
///
/// Use for:
/// - `persist_producer_state()`
/// - `db.put()` / `db.get()` operations
/// - Batch writes to SlateDB
pub fn storage_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(10))
        .with_max_delay(Duration::from_millis(500))
        .with_max_times(3)
        .with_jitter()
}

/// Policy for network/RPC operations.
///
/// Characteristics:
/// - Moderate initial delay (100ms) for network settling
/// - Long max delay (10s) for slow network recovery
/// - Moderate retries (5) balancing availability and fail-fast
/// - Includes jitter
///
/// Use for:
/// - TCP connections to other brokers
/// - Raft RPC calls
/// - Forward requests to leader
pub fn network_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(10))
        .with_max_times(5)
        .with_jitter()
}

/// Policy for hot path retries (minimal delay).
///
/// Characteristics:
/// - Very short initial delay (5ms)
/// - Very short max delay (100ms)
/// - Few retries (3)
/// - Includes jitter
///
/// Use for:
/// - Cache refresh
/// - Lock acquisition
/// - Operations where latency matters
pub fn fast_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(5))
        .with_max_delay(Duration::from_millis(100))
        .with_max_times(3)
        .with_jitter()
}

/// Policy for leader election/forwarding scenarios.
///
/// Characteristics:
/// - Short initial delay (100ms) with linear backoff
/// - Longer max delay (3s) for election stabilization
/// - Many retries (30) to handle extended election periods
/// - Includes jitter
///
/// Use for:
/// - Requests that need forwarding to leader
/// - Operations during Raft leader election
pub fn leader_election_policy() -> ExponentialBuilder {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(3))
        .with_max_times(30)
        .with_jitter()
}

/// Execute an async operation with the coordinator retry policy.
///
/// This is a convenience wrapper for common retry patterns.
///
/// # Example
///
/// ```rust,ignore
/// use kafkaesque::cluster::retry;
///
/// let result = retry::with_coordinator_policy(
///     || async { coordinator.register_broker().await },
///     |e| e.is_retriable(),
/// ).await;
/// ```
pub async fn with_coordinator_policy<F, Fut, T, E, C>(operation: F, condition: C) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::error::Error,
    C: FnMut(&E) -> bool,
{
    operation.retry(coordinator_policy()).when(condition).await
}

/// Execute an async operation with the storage retry policy.
///
/// This is a convenience wrapper for SlateDB operations.
pub async fn with_storage_policy<F, Fut, T, E, C>(operation: F, condition: C) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::error::Error,
    C: FnMut(&E) -> bool,
{
    operation.retry(storage_policy()).when(condition).await
}

/// Execute an async operation with the network retry policy.
///
/// This is a convenience wrapper for RPC/network operations.
pub async fn with_network_policy<F, Fut, T, E, C>(operation: F, condition: C) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T, E>>,
    E: std::error::Error,
    C: FnMut(&E) -> bool,
{
    operation.retry(network_policy()).when(condition).await
}

/// Record a retry attempt for metrics.
///
/// Call this in your retry condition to track retry rates.
pub fn record_retry_attempt(policy_name: &str, attempt: u32) {
    super::metrics::RETRY_ATTEMPTS
        .with_label_values(&[policy_name, "attempt"])
        .inc();

    tracing::debug!(policy = policy_name, attempt, "Retry attempt");
}

/// Record a retry exhaustion (all retries failed).
pub fn record_retry_exhausted(policy_name: &str) {
    super::metrics::RETRY_ATTEMPTS
        .with_label_values(&[policy_name, "exhausted"])
        .inc();

    tracing::warn!(policy = policy_name, "Retry policy exhausted");
}

/// Record a retry success.
pub fn record_retry_success(policy_name: &str) {
    super::metrics::RETRY_ATTEMPTS
        .with_label_values(&[policy_name, "success"])
        .inc();
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    // ========================================================================
    // Policy Creation Tests
    // ========================================================================

    #[test]
    fn test_coordinator_policy_parameters() {
        let policy = coordinator_policy();
        // Verify policy is created without panic
        let _ = policy;
    }

    #[test]
    fn test_storage_policy_parameters() {
        let policy = storage_policy();
        let _ = policy;
    }

    #[test]
    fn test_network_policy_parameters() {
        let policy = network_policy();
        let _ = policy;
    }

    #[test]
    fn test_fast_policy_parameters() {
        let policy = fast_policy();
        let _ = policy;
    }

    #[test]
    fn test_leader_election_policy_parameters() {
        let policy = leader_election_policy();
        let _ = policy;
    }

    // ========================================================================
    // Retry Behavior Tests
    // ========================================================================

    #[tokio::test]
    async fn test_retry_succeeds_on_third_attempt() {
        let attempts = AtomicU32::new(0);

        let result = (|| async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < 2 {
                Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
            } else {
                Ok(42)
            }
        })
        .retry(fast_policy())
        .when(|_| true)
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_respects_condition() {
        let attempts = AtomicU32::new(0);

        let result: Result<i32, std::io::Error> = (|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "not found",
            ))
        })
        .retry(fast_policy())
        .when(|e| e.kind() == std::io::ErrorKind::TimedOut) // Won't retry NotFound
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // Only one attempt
    }

    #[tokio::test]
    async fn test_retry_exhausts_after_max_attempts() {
        let attempts = AtomicU32::new(0);

        let result: Result<i32, std::io::Error> = (|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
        })
        .retry(fast_policy()) // max_times = 3
        .when(|_| true)
        .await;

        assert!(result.is_err());
        assert_eq!(attempts.load(Ordering::SeqCst), 4); // Initial + 3 retries
    }

    // ========================================================================
    // Wrapper Function Tests
    // ========================================================================

    #[tokio::test]
    async fn test_with_coordinator_policy_wrapper() {
        let attempts = AtomicU32::new(0);

        let result = with_coordinator_policy(
            || {
                let attempts = &attempts;
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt < 1 {
                        Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
                    } else {
                        Ok(100)
                    }
                }
            },
            |_| true,
        )
        .await;

        assert_eq!(result.unwrap(), 100);
    }

    #[tokio::test]
    async fn test_with_storage_policy_wrapper_success() {
        let attempts = AtomicU32::new(0);

        let result = with_storage_policy(
            || {
                let attempts = &attempts;
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt < 1 {
                        Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
                    } else {
                        Ok("stored")
                    }
                }
            },
            |_| true,
        )
        .await;

        assert_eq!(result.unwrap(), "stored");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn test_with_storage_policy_wrapper_exhausted() {
        let attempts = AtomicU32::new(0);

        let result: Result<i32, std::io::Error> = with_storage_policy(
            || {
                let attempts = &attempts;
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
                }
            },
            |_| true,
        )
        .await;

        assert!(result.is_err());
        // storage_policy has max_times = 3
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }

    #[tokio::test]
    async fn test_with_network_policy_wrapper_success() {
        let attempts = AtomicU32::new(0);

        let result = with_network_policy(
            || {
                let attempts = &attempts;
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                    if attempt < 2 {
                        Err(std::io::Error::new(
                            std::io::ErrorKind::ConnectionRefused,
                            "connection refused",
                        ))
                    } else {
                        Ok(vec![1, 2, 3])
                    }
                }
            },
            |_| true,
        )
        .await;

        assert_eq!(result.unwrap(), vec![1, 2, 3]);
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_with_network_policy_wrapper_condition_false() {
        let attempts = AtomicU32::new(0);

        let result: Result<i32, std::io::Error> = with_network_policy(
            || {
                let attempts = &attempts;
                async move {
                    attempts.fetch_add(1, Ordering::SeqCst);
                    Err(std::io::Error::new(
                        std::io::ErrorKind::NotFound,
                        "not found",
                    ))
                }
            },
            |e| e.kind() != std::io::ErrorKind::NotFound, // Don't retry NotFound
        )
        .await;

        assert!(result.is_err());
        // Should only try once since condition is false
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    // ========================================================================
    // Metric Recording Tests
    // ========================================================================

    #[test]
    fn test_record_retry_attempt() {
        // Just verify the function doesn't panic
        record_retry_attempt("test_policy", 1);
        record_retry_attempt("test_policy", 2);
        record_retry_attempt("coordinator", 5);
    }

    #[test]
    fn test_record_retry_exhausted() {
        // Just verify the function doesn't panic
        record_retry_exhausted("test_policy");
        record_retry_exhausted("coordinator");
        record_retry_exhausted("storage");
    }

    #[test]
    fn test_record_retry_success() {
        // Just verify the function doesn't panic
        record_retry_success("test_policy");
        record_retry_success("coordinator");
        record_retry_success("network");
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[tokio::test]
    async fn test_retry_immediate_success() {
        let attempts = AtomicU32::new(0);

        let result = (|| async {
            attempts.fetch_add(1, Ordering::SeqCst);
            Ok::<_, std::io::Error>(42)
        })
        .retry(fast_policy())
        .when(|_| true)
        .await;

        assert_eq!(result.unwrap(), 42);
        assert_eq!(attempts.load(Ordering::SeqCst), 1); // Only one attempt needed
    }

    #[tokio::test]
    async fn test_retry_with_different_error_types() {
        let attempts = AtomicU32::new(0);

        let result = (|| async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            match attempt {
                0 => Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout")),
                1 => Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "refused",
                )),
                _ => Ok("success"),
            }
        })
        .retry(fast_policy())
        .when(|_| true) // Retry all errors
        .await;

        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempts.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_retry_with_selective_error_retry() {
        let attempts = AtomicU32::new(0);

        let result: Result<i32, std::io::Error> = (|| async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt == 0 {
                // First error is TimedOut (retriable)
                Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
            } else {
                // Second error is PermissionDenied (not retriable)
                Err(std::io::Error::new(
                    std::io::ErrorKind::PermissionDenied,
                    "denied",
                ))
            }
        })
        .retry(fast_policy())
        .when(|e| e.kind() == std::io::ErrorKind::TimedOut)
        .await;

        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().kind(),
            std::io::ErrorKind::PermissionDenied
        );
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    // ========================================================================
    // Policy Specific Tests
    // ========================================================================

    #[tokio::test]
    async fn test_coordinator_policy_many_retries() {
        let attempts = AtomicU32::new(0);

        let result = (|| async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < 5 {
                Err(std::io::Error::new(std::io::ErrorKind::TimedOut, "timeout"))
            } else {
                Ok("success")
            }
        })
        .retry(coordinator_policy()) // max_times = 10
        .when(|_| true)
        .await;

        assert_eq!(result.unwrap(), "success");
        assert_eq!(attempts.load(Ordering::SeqCst), 6);
    }

    #[tokio::test]
    async fn test_leader_election_policy_long_retry() {
        let attempts = AtomicU32::new(0);

        let result = (|| async {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            if attempt < 3 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "not connected",
                ))
            } else {
                Ok("leader found")
            }
        })
        .retry(leader_election_policy()) // max_times = 30
        .when(|_| true)
        .await;

        assert_eq!(result.unwrap(), "leader found");
        assert_eq!(attempts.load(Ordering::SeqCst), 4);
    }
}
