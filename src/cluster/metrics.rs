//! Prometheus metrics for the SlateDB storage backend.
//!
//! This module provides comprehensive metrics for monitoring the Kafkaesque Kafka server.
//! Metrics cover:
//! - Connection management (active, total, errors)
//! - Request processing (count, latency per API)
//! - Produce/Fetch throughput (messages, bytes per topic/partition)
//! - Partition ownership (leases, ownership changes)
//! - Cache performance (hits, misses for partition owner lookups)
//! - Consumer group operations (joins, syncs, heartbeats)
//!
//! # Safety
//!
//! All metrics are registered to a custom registry with the "kafkaesque" prefix to avoid
//! name collisions with other libraries using the default Prometheus registry.
//! Registration errors are handled gracefully - if a metric fails to register,
//! a fallback no-op metric is used instead of panicking.

use once_cell::sync::Lazy;
use std::borrow::Cow;

use prometheus::{
    Encoder, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec,
    Registry, TextEncoder, opts,
};
use tracing::warn;

/// Custom Prometheus registry for Kafkaesque metrics.
/// Using a custom registry prevents name collisions with other libraries.
pub static REGISTRY: Lazy<Registry> = Lazy::new(|| {
    Registry::new_custom(Some("kafkaesque".to_string()), None).unwrap_or_else(|_| Registry::new())
});

// =============================================================================
// Metric Declaration Macros
// =============================================================================
//
// These macros reduce boilerplate for declaring metrics. Each expands to a
// Lazy static with safe registration. Examples:
//
//   define_gauge!(MY_GAUGE, "my_metric", "Description");
//   define_gauge_vec!(MY_GAUGE, "my_metric", "Description", ["label1"]);
//   define_counter_vec!(MY_COUNTER, "my_metric", "Description", ["label1"]);
//   define_histogram_vec!(MY_HISTOGRAM, "my_metric", "Description", ["label"],
//       [0.001, 0.01, 0.1, 1.0]);

/// Declare an IntGauge metric.
macro_rules! define_gauge {
    ($name:ident, $metric_name:expr, $help:expr) => {
        #[doc = $help]
        pub static $name: Lazy<IntGauge> =
            Lazy::new(|| register_int_gauge_safe(&REGISTRY, $metric_name, $help));
    };
}

/// Declare an IntGaugeVec metric with labels.
macro_rules! define_gauge_vec {
    ($name:ident, $metric_name:expr, $help:expr, [$($label:expr),+ $(,)?]) => {
        #[doc = $help]
        pub static $name: Lazy<IntGaugeVec> = Lazy::new(|| {
            register_int_gauge_vec_safe(&REGISTRY, $metric_name, $help, &[$($label),+])
        });
    };
}

/// Declare an IntCounterVec metric with labels.
macro_rules! define_counter_vec {
    ($name:ident, $metric_name:expr, $help:expr, [$($label:expr),+ $(,)?]) => {
        #[doc = $help]
        pub static $name: Lazy<IntCounterVec> = Lazy::new(|| {
            register_int_counter_vec_safe(&REGISTRY, $metric_name, $help, &[$($label),+])
        });
    };
}

/// Declare an IntCounter metric (no labels).
macro_rules! define_counter {
    ($name:ident, $metric_name:expr, $help:expr) => {
        #[doc = $help]
        pub static $name: Lazy<IntCounter> =
            Lazy::new(|| register_int_counter_safe(&REGISTRY, $metric_name, $help));
    };
}

/// Declare a HistogramVec metric with labels and buckets.
macro_rules! define_histogram_vec {
    ($name:ident, $metric_name:expr, $help:expr, [$($label:expr),+ $(,)?], [$($bucket:expr),+ $(,)?]) => {
        #[doc = $help]
        pub static $name: Lazy<HistogramVec> = Lazy::new(|| {
            register_histogram_vec_safe(&REGISTRY, $metric_name, $help, &[$($label),+], vec![$($bucket),+])
        });
    };
}

// =============================================================================
// Connection metrics
// =============================================================================

define_gauge!(
    ACTIVE_CONNECTIONS,
    "active_connections",
    "Number of active TCP connections"
);
define_counter_vec!(
    TOTAL_CONNECTIONS,
    "total_connections",
    "Total number of connections accepted",
    ["status"]
);
define_counter_vec!(
    FRAME_REJECTED,
    "frame_rejected_total",
    "Inbound Kafka frames the reader refused before dispatch, by reason \
     (frame_too_large, inflight_budget_exhausted, body_read_timeout, malformed_size). \
     A sustained non-zero rate is an oversized-frame / slow-client / memory-pressure \
     (DoS) signal — alert on it. Clean peer disconnects are NOT counted here.",
    ["reason"]
);

// =============================================================================
// Request metrics
// =============================================================================

define_counter_vec!(
    REQUEST_COUNT,
    "requests_total",
    "Total number of Kafka API requests",
    ["api", "status", "error_code"]
);
define_histogram_vec!(
    REQUEST_DURATION,
    "request_duration_seconds",
    "Request processing duration in seconds",
    ["api", "status"],
    [
        // Sub-millisecond buckets — well-tuned p99 produce/fetch on a hot
        // partition is in the 100–800 µs range, so a histogram starting at
        // 1ms collapses every quick request into the first bucket and the
        // SRE loses all signal on tail-latency regressions.
        0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
        5.0, 10.0
    ]
);
define_counter_vec!(
    REQUEST_TRAILING_BYTES,
    "request_trailing_bytes_total",
    "Requests whose body parse left unconsumed trailing bytes (wire layout gap or client padding)",
    ["api"]
);

// =============================================================================
// Produce metrics
// =============================================================================

define_counter_vec!(
    MESSAGES_PRODUCED,
    "messages_produced_total",
    "Total number of messages produced",
    ["topic", "partition"]
);
define_counter_vec!(
    BYTES_PRODUCED,
    "bytes_produced_total",
    "Total bytes produced",
    ["topic", "partition"]
);

// =============================================================================
// Fetch metrics
// =============================================================================

define_counter_vec!(
    MESSAGES_FETCHED,
    "messages_fetched_total",
    "Total number of messages fetched",
    ["topic", "partition"]
);
define_counter_vec!(
    BYTES_FETCHED,
    "bytes_fetched_total",
    "Total bytes fetched",
    ["topic", "partition"]
);

// =============================================================================
// Partition metrics
// =============================================================================

define_gauge_vec!(
    OWNED_PARTITIONS,
    "owned_partitions",
    "Number of partitions owned by this broker",
    ["topic"]
);
define_gauge!(
    ESTIMATED_PARTITION_MEMORY_BYTES,
    "estimated_partition_memory_bytes",
    "Estimated broker memory held by owned partition stores (batch index + per-instance SlateDB overhead estimate). Order-of-magnitude planning aid, not precise accounting."
);
define_counter_vec!(
    PARTITION_ACQUIRE_REJECTED,
    "partition_acquire_rejected_total",
    "Partition acquisitions rejected before opening a SlateDB store",
    ["reason"]
);
define_gauge_vec!(
    PARTITION_HIGH_WATERMARK,
    "partition_high_watermark",
    "High watermark (latest offset) for each partition",
    ["topic", "partition"]
);
define_counter_vec!(
    LEASE_OPERATIONS,
    "lease_operations_total",
    "Total number of lease operations",
    ["operation", "status"]
);

// =============================================================================
// Partition Store Pool metrics
// =============================================================================

define_gauge!(
    PARTITION_STORE_POOL_SIZE,
    "partition_store_pool_size",
    "Current number of partition stores open in the pool"
);
define_counter!(
    PARTITION_STORE_POOL_HITS,
    "partition_store_pool_hits_total",
    "Total number of partition store pool cache hits"
);
define_counter!(
    PARTITION_STORE_POOL_MISSES,
    "partition_store_pool_misses_total",
    "Total number of partition store pool cache misses (required opening)"
);
define_counter!(
    PARTITION_STORE_POOL_EVICTIONS,
    "partition_store_pool_evictions_total",
    "Total number of partition stores evicted from the pool"
);

// =============================================================================
// Cache metrics
// =============================================================================

define_counter_vec!(
    CACHE_OPERATIONS,
    "cache_operations_total",
    "Total cache lookup operations",
    ["cache", "result"]
);

// =============================================================================
// Coordinator operations
// =============================================================================

define_histogram_vec!(
    COORDINATOR_DURATION,
    "coordinator_duration_seconds",
    "Coordinator operation duration in seconds",
    ["operation"],
    [0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0]
);

// =============================================================================
// Consumer group metrics
// =============================================================================

define_counter_vec!(
    GROUP_OPERATIONS,
    "group_operations_total",
    "Total consumer group operations",
    ["operation", "status"]
);
define_gauge!(
    ACTIVE_GROUPS,
    "active_groups",
    "Number of active consumer groups"
);

// =============================================================================
// Offset commit metrics
// =============================================================================

define_counter_vec!(
    OFFSET_COMMITS,
    "offset_commits_total",
    "Total offset commit operations",
    ["group", "topic", "status"]
);

/// Coordinator failures (critical errors like consecutive heartbeat failures)
pub static COORDINATOR_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "coordinator_failures_total",
        "Critical coordinator operation failures",
        &["operation"], // operation=heartbeat/lease_renewal/etc
    )
});

/// Fencing detection events by method
pub static FENCING_DETECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "fencing_detections_total",
        "Fencing error detections by detection method",
        &["method"], // method=typed/pattern/fail_closed/not_fencing
    )
});

/// Retry attempts by policy and outcome.
///
/// Labels:
/// - `policy`: coordinator, storage, network, fast, leader_election
/// - `outcome`: attempt, success, exhausted
pub static RETRY_ATTEMPTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "retry_attempts_total",
        "Retry attempts by policy and outcome",
        &["policy", "outcome"],
    )
});

/// Idempotency rejection events by reason
pub static IDEMPOTENCY_REJECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "idempotency_rejections_total",
        "Rejected batches due to idempotency checks",
        &["reason"], // reason=duplicate/out_of_order/fenced_epoch
    )
});

/// Epoch mismatch detections during writes.
///
/// This counter tracks when a write is rejected because the stored leader epoch
/// in SlateDB doesn't match our expected epoch. This indicates another broker
/// has acquired the partition, preventing TOCTOU race conditions.
pub static EPOCH_MISMATCH_DETECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "epoch_mismatch_total",
        "Writes rejected due to epoch mismatch (TOCTOU prevention)",
        &["topic", "partition"],
    )
});

/// Permanent offset gap created when an in-flight `append_batch_inner`
/// hits a SlateDB / object-store error after the offset reservation has
/// been disarmed. We disarm before handing the batch to SlateDB so a
/// cancellation-driven rollback cannot race a queued write and clobber
/// the same key. The trade-off is a permanent gap; this metric counts
/// records lost to gaps so operators can alarm on persistent rates and
/// decide whether to set `fail_on_recovery_gap=true`.
pub static PARTITION_OFFSET_GAPS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "partition_offset_gap_records_total",
        "Records lost to permanent offset gaps after transient SlateDB write errors",
        &["topic", "partition"],
    )
});

/// A `ListOffsets(timestamp)` query exhausted the bounded scan budget
/// without finding a matching batch. The handler answers with offset -1
/// (Kafka's "not found" sentinel) rather than continue scanning. Tracks
/// pathological queries (timestamp far below log_start) and operator
/// signal that a real time index is overdue.
pub static LIST_OFFSETS_TRUNCATED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "list_offsets_timestamp_truncated_total",
        "ListOffsets(timestamp) scans halted at the cap; reported offset -1",
        &["topic"],
    )
});

/// A record batch on disk failed parse. Emitted from fetch- and
/// retention-path scans where the loop would otherwise silently fall back
/// to `record_count = 0`. A persistent rate signals on-disk corruption,
/// SlateDB / object-store data damage, or a producer-side encoding bug;
/// retention stops at the first corrupt batch in a partition, so an
/// alert on `corrupt_batch_total > 0` should page someone.
pub static CORRUPT_BATCHES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "corrupt_batch_total",
        "Record batches whose v2 header failed to parse during scan",
        &["topic", "partition", "operation"],
    )
});

// --- Fencing Circuit Breaker ---
// Tracks consecutive fail-closed events to prevent cascading unavailability
// from unknown but benign errors.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

/// Consecutive fail-closed fencing events without confirmed fencing.
/// Reset when a confirmed fencing (TypedErrorKind) is observed.
static CONSECUTIVE_FAIL_CLOSED: AtomicU64 = AtomicU64::new(0);

/// Timestamp of last confirmed fencing event (for circuit breaker reset).
static LAST_CONFIRMED_FENCING_MILLIS: AtomicU64 = AtomicU64::new(0);

/// Circuit breaker configuration - initialized from ClusterConfig.
/// Uses OnceLock for one-time initialization at startup.
struct CircuitBreakerConfig {
    threshold: u64,
    base_reset_window_ms: u64,
    max_reset_window_ms: u64,
}

/// Default circuit breaker configuration (used if not initialized from config).
const DEFAULT_CIRCUIT_BREAKER_CONFIG: CircuitBreakerConfig = CircuitBreakerConfig {
    threshold: 5,
    base_reset_window_ms: 60_000, // 1 minute
    max_reset_window_ms: 300_000, // 5 minutes
};

/// Configurable circuit breaker settings.
static CIRCUIT_BREAKER_CONFIG: OnceLock<CircuitBreakerConfig> = OnceLock::new();

/// Initialize circuit breaker configuration from ClusterConfig.
///
/// This should be called once at startup. If not called, default values are used.
/// Subsequent calls are ignored (OnceLock semantics).
pub fn init_circuit_breaker_config(
    threshold: u64,
    base_reset_window_ms: u64,
    max_reset_window_ms: u64,
) {
    let _ = CIRCUIT_BREAKER_CONFIG.set(CircuitBreakerConfig {
        threshold,
        base_reset_window_ms,
        max_reset_window_ms,
    });
}

/// Get the circuit breaker threshold.
fn circuit_breaker_threshold() -> u64 {
    CIRCUIT_BREAKER_CONFIG
        .get()
        .map(|c| c.threshold)
        .unwrap_or(DEFAULT_CIRCUIT_BREAKER_CONFIG.threshold)
}

/// Get the base reset window in milliseconds.
fn circuit_breaker_base_reset_window_ms() -> u64 {
    CIRCUIT_BREAKER_CONFIG
        .get()
        .map(|c| c.base_reset_window_ms)
        .unwrap_or(DEFAULT_CIRCUIT_BREAKER_CONFIG.base_reset_window_ms)
}

/// Get the max reset window in milliseconds.
fn circuit_breaker_max_reset_window_ms() -> u64 {
    CIRCUIT_BREAKER_CONFIG
        .get()
        .map(|c| c.max_reset_window_ms)
        .unwrap_or(DEFAULT_CIRCUIT_BREAKER_CONFIG.max_reset_window_ms)
}

/// Tracks consecutive circuit breaker trips for exponential backoff.
static CONSECUTIVE_CIRCUIT_BREAKER_TRIPS: AtomicU64 = AtomicU64::new(0);

/// Circuit breaker state gauge (0 = normal, 1 = tripped).
///
/// **ALERT**: Set up alerts when this metric is 1. A tripped circuit breaker
/// indicates that unknown errors are being suppressed, which could mask real
/// fencing events. Investigate the root cause immediately.
pub static CIRCUIT_BREAKER_STATE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "fencing_circuit_breaker_tripped",
        "Circuit breaker state for fail-closed fencing (0=normal, 1=tripped). ALERT when 1.",
    )
});

/// Circuit breaker trip counter.
///
/// **ALERT**: Set up alerts when this counter increases. Each trip indicates
/// a period where unknown errors were not treated as fencing.
pub static CIRCUIT_BREAKER_TRIPS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "fencing_circuit_breaker_trips_total",
        "Total circuit breaker trip events. ALERT on any increase.",
        &["reason"], // reason = "consecutive_unknown_errors"
    )
});

// =============================================================================
// Distributed Systems Observability Metrics
// =============================================================================

/// Raft query latency histogram.
///
/// Tracks the latency of Raft read operations (queries), which are critical for
/// ownership checks and partition coordination. High latencies here indicate
/// Raft cluster health issues or network problems.
///
/// **ALERT**: Set up alerts when p99 latency exceeds 100ms, as this can cause
/// timeouts in ownership verification and increase lease renewal failures.
pub static RAFT_QUERY_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "raft_query_duration_seconds",
        "Raft query (read) latency in seconds. ALERT when p99 > 100ms.",
        &["operation"], // operation=get_owner/get_brokers/get_topics/etc
        vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ],
    )
});

/// SlateDB flush latency histogram.
///
/// Tracks how long it takes for SlateDB to flush data to the object store.
/// This is critical for understanding the durability delay when using
/// `await_durable=false`. High flush latencies mean longer data loss windows.
///
/// **ALERT**: Set up alerts when p99 latency exceeds 500ms, as this extends
/// the potential data loss window during crashes.
pub static SLATEDB_FLUSH_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "slatedb_flush_duration_seconds",
        "SlateDB flush to object store latency in seconds. ALERT when p99 > 500ms.",
        &["topic"],
        vec![0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0],
    )
});

/// Rebalance stale decisions counter.
///
/// Tracks when rebalance decisions are detected as stale due to concurrent
/// ownership changes. This monitors the effectiveness of the rebalance atomicity
/// fix (using Raft commit index for consistent snapshots).
///
/// **ALERT**: Set up alerts when this counter increases rapidly, as it indicates
/// high churn in partition ownership that may cause rebalance thrashing.
pub static REBALANCE_STALE_DECISIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "rebalance_stale_decisions_total",
        "Rebalance decisions detected as stale (ownership changed during evaluation). ALERT on rapid increase.",
        &["reason"], // reason=ownership_changed/raft_index_mismatch
    )
});

/// Failover batch size histogram.
///
/// Tracks the size of partition transfer batches during broker failover.
/// This monitors the thundering herd prevention mechanism - smaller batches
/// indicate rate limiting is working to prevent cascade failures.
///
/// **ALERT**: Set up alerts when batch sizes consistently hit the max limit,
/// which may indicate failover rate limiting needs tuning.
pub static FAILOVER_BATCH_SIZE: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "failover_batch_size",
        "Number of partitions transferred per failover batch. Monitor for thundering herd.",
        &["failed_broker"],
        vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0],
    )
});

/// Failover batches total counter.
///
/// Tracks the total number of failover batches processed.
pub static FAILOVER_BATCHES_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "failover_batches_total",
        "Total failover batches processed",
        &["status"], // status=success/partial/failed
    )
});

/// Check if the fail-closed circuit breaker has tripped.
///
/// Returns true if we should NOT auto-fence on unknown errors because
/// we've seen too many consecutive fail-closed events without any confirmed fencing.
///
/// This prevents scenarios where transient unknown errors cause cascading
/// partition unavailability due to over-aggressive fencing.
pub fn fail_closed_circuit_breaker_tripped() -> bool {
    let count = CONSECUTIVE_FAIL_CLOSED.load(Ordering::SeqCst);
    count >= circuit_breaker_threshold()
}

/// Record a fencing detection event and update circuit breaker state.
///
/// - Confirmed fencing (TypedErrorKind): Reset the fail-closed counter
/// - Fail-closed: Increment the counter
/// - Other: No change to circuit breaker state
pub fn record_fencing_detection_with_circuit_breaker(method: &str) -> bool {
    FENCING_DETECTIONS.with_label_values(&[method]).inc();

    match method {
        "typed" => {
            // Confirmed fencing: reset circuit breaker and backoff
            let prev_count = CONSECUTIVE_FAIL_CLOSED.swap(0, Ordering::SeqCst);
            if prev_count >= circuit_breaker_threshold() {
                // Circuit breaker was tripped, now reset
                CIRCUIT_BREAKER_STATE.set(0);
                // Reset trip counter on confirmed fencing
                CONSECUTIVE_CIRCUIT_BREAKER_TRIPS.store(0, Ordering::SeqCst);
                tracing::info!(
                    previous_fail_closed_count = prev_count,
                    "Circuit breaker RESET due to confirmed fencing (typed detection)"
                );
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            LAST_CONFIRMED_FENCING_MILLIS.store(now, Ordering::SeqCst);
            true // Should fence
        }
        "pattern" => {
            // Pattern match is fairly confident, also reset circuit breaker
            let prev_count = CONSECUTIVE_FAIL_CLOSED.swap(0, Ordering::SeqCst);
            if prev_count >= circuit_breaker_threshold() {
                // Circuit breaker was tripped, now reset
                CIRCUIT_BREAKER_STATE.set(0);
                // Reset trip counter on confirmed fencing
                CONSECUTIVE_CIRCUIT_BREAKER_TRIPS.store(0, Ordering::SeqCst);
                tracing::info!(
                    previous_fail_closed_count = prev_count,
                    "Circuit breaker RESET due to confirmed fencing (pattern detection)"
                );
            }
            true // Should fence
        }
        "fail_closed" => {
            // Unknown error - check circuit breaker before deciding
            let count = CONSECUTIVE_FAIL_CLOSED.fetch_add(1, Ordering::SeqCst) + 1;

            if count >= circuit_breaker_threshold() {
                // Circuit breaker tripped - check if we should reset based on time
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                let last_confirmed = LAST_CONFIRMED_FENCING_MILLIS.load(Ordering::SeqCst);

                // Exponential backoff for reset window
                // Each consecutive trip doubles the reset window (up to max)
                let trips = CONSECUTIVE_CIRCUIT_BREAKER_TRIPS.load(Ordering::SeqCst);
                let backoff_multiplier = 1u64 << trips.min(4); // 1, 2, 4, 8, 16 (capped)
                let reset_window = (circuit_breaker_base_reset_window_ms() * backoff_multiplier)
                    .min(circuit_breaker_max_reset_window_ms());

                if last_confirmed > 0 && now - last_confirmed < reset_window {
                    // Recent confirmed fencing exists - still fence
                    true
                } else {
                    // No recent confirmed fencing - don't auto-fence
                    // Record the trip if this is the first time hitting threshold
                    if count == circuit_breaker_threshold() {
                        CIRCUIT_BREAKER_STATE.set(1);
                        CIRCUIT_BREAKER_TRIPS
                            .with_label_values(&["consecutive_unknown_errors"])
                            .inc();
                        // Track consecutive trips for backoff
                        CONSECUTIVE_CIRCUIT_BREAKER_TRIPS.fetch_add(1, Ordering::SeqCst);
                    }
                    tracing::error!(
                        consecutive_fail_closed = count,
                        threshold = circuit_breaker_threshold(),
                        reset_window_ms = reset_window,
                        "CRITICAL: Fencing circuit breaker TRIPPED - NOT treating unknown error as fencing. \
                         Too many consecutive unknown errors without confirmed fencing. \
                         ACTION REQUIRED: Investigate unknown SlateDB errors immediately. \
                         This could mask real fencing events and cause split-brain!"
                    );
                    false // Don't fence - let the error propagate as SlateDB error
                }
            } else {
                true // Still under threshold - fence as usual
            }
        }
        _ => {
            // "not_fencing" - no change to circuit breaker
            false // Don't fence
        }
    }
}

/// Get current circuit breaker state for monitoring.
pub fn get_circuit_breaker_state() -> (u64, bool) {
    let count = CONSECUTIVE_FAIL_CLOSED.load(Ordering::SeqCst);
    let tripped = count >= circuit_breaker_threshold();
    (count, tripped)
}

/// Reset the circuit breaker (for testing or manual intervention).
pub fn reset_circuit_breaker() {
    CONSECUTIVE_FAIL_CLOSED.store(0, Ordering::SeqCst);
    CONSECUTIVE_CIRCUIT_BREAKER_TRIPS.store(0, Ordering::SeqCst);
    CIRCUIT_BREAKER_STATE.set(0);
}

// --- Additional metrics for comprehensive monitoring ---

/// Consumer group rebalance duration in seconds
pub static REBALANCE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "rebalance_duration_seconds",
        "Consumer group rebalance duration in seconds",
        &["group"],
        vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
    )
});

/// Partition acquisition latency in seconds
pub static PARTITION_ACQUISITION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "partition_acquisition_duration_seconds",
        "Time to acquire partition ownership",
        &["topic", "status"], // status=success/error/fenced
        vec![0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0],
    )
});

/// Number of partitions per topic
pub static TOPIC_PARTITION_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "topic_partition_count",
        "Number of partitions for each topic",
        &["topic"],
    )
});

/// Number of members in each consumer group
pub static GROUP_MEMBER_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "group_member_count",
        "Number of members in each consumer group",
        &["group"],
    )
});

/// Current consumer group generation ID
pub static GROUP_GENERATION: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "group_generation",
        "Current generation ID for each consumer group",
        &["group"],
    )
});

/// Number of pending rebalances
pub static PENDING_REBALANCES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "pending_rebalances",
        "Number of consumer groups currently rebalancing",
    )
});

/// SlateDB storage operation latency
pub static STORAGE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "storage_duration_seconds",
        "SlateDB storage operation duration in seconds",
        &["operation"], // operation=get/put/delete
        vec![0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0],
    )
});

/// High watermark recovery events
pub static HWM_RECOVERY_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "hwm_recovery_total",
        "High watermark recovery events after crash",
        &["topic", "partition", "result"], // result=recovered/no_change
    )
});

/// Lease cache hit/miss counter for get_for_write operations
///
/// Tracks whether lease verification was served from cache (hit) or required
/// a Raft call (miss). High hit rates indicate the lease caching optimization
/// is working effectively.
pub static LEASE_CACHE_OPERATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "lease_cache_total",
        "Lease cache operations (hit=cache used, miss=Raft call)",
        &["result"], // result=hit/miss
    )
});

// --- Request latency metrics for specific operations ---

/// Produce request latency per topic
///
/// Tracks the end-to-end latency of produce requests, from request receipt
/// to response send. Useful for identifying slow topics or hot partitions.
pub static PRODUCE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "produce_duration_seconds",
        "Produce request duration in seconds per topic",
        &["topic", "status"], // status=success/error
        vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ],
    )
});

/// Fetch request latency per topic
///
/// Tracks the end-to-end latency of fetch requests. Note that fetch requests
/// may include a wait time for new data, so high latencies are expected when
/// waiting at the high watermark.
pub static FETCH_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "fetch_duration_seconds",
        "Fetch request duration in seconds per topic",
        &["topic", "status"], // status=success/error/timeout
        vec![0.001, 0.01, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0],
    )
});

/// Produce records silently dropped on the acks=0 (fire-and-forget) path.
///
/// Incremented when backpressure, zombie mode, ACL denial, or other
/// fire-and-forget failures cause records to be dropped without a client
/// response.
pub static PRODUCE_DROPPED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "produce_dropped_total",
        "Produce records dropped on the acks=0 path without client notification",
        &["reason"],
    )
});

/// Counts label values that were dropped to an `_overflow` / `_other`
/// placeholder because they would have exceeded `MAX_METRIC_CARDINALITY`.
///
/// Without this, the cardinality cap silently rolls every distinct label past
/// the limit into the same bucket, hiding both the runaway client and the
/// fact that downstream metrics with that label are now missing data. Operators
/// should alert on a non-zero rate here.
pub static METRIC_LABELS_DROPPED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "metric_labels_dropped_total",
        "Label values rolled into the cardinality-overflow placeholder",
        &["dimension"],
    )
});

/// Per-principal authorization-denial counter.
///
/// Tracked separately from `produce_dropped_total{reason=acl_denied}` so an
/// operator can answer "which principal is being denied and on which API"
/// without grepping logs. Cardinality is bounded by the active principal
/// set; principals beyond the cap fall back to "_other" via
/// `bounded_principal_label`.
pub static ACL_DENIAL_TOTAL: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "acl_denial_total",
        "ACL denials counted per principal and API surface",
        &["principal", "api", "resource_type"],
    )
});

/// Consumer group operation latency by operation type
///
/// Tracks latency for specific consumer group operations:
/// - join: JoinGroup request processing
/// - sync: SyncGroup request processing
/// - heartbeat: Heartbeat request processing
/// - leave: LeaveGroup request processing
/// - offset_commit: OffsetCommit request processing
/// - offset_fetch: OffsetFetch request processing
pub static GROUP_OPERATION_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "group_operation_duration_seconds",
        "Consumer group operation duration in seconds",
        &["operation", "status"], // operation=join/sync/heartbeat/leave/offset_commit/offset_fetch
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
    )
});

// --- Zombie mode observability metrics ---

/// Zombie mode state (1 = in zombie mode, 0 = normal)
pub static ZOMBIE_MODE_STATE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "zombie_mode_active",
        "Whether broker is currently in zombie mode (1=zombie, 0=normal)",
    )
});

/// Zombie mode duration histogram (time spent in zombie mode per episode)
pub static ZOMBIE_MODE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "zombie_mode_duration_seconds",
        "Duration of zombie mode episodes in seconds",
        &["exit_reason"], // exit_reason=recovered/manual/shutdown
        vec![
            1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
        ],
    )
});

/// Zombie mode transition counter
pub static ZOMBIE_MODE_TRANSITIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "zombie_mode_transitions_total",
        "Total zombie mode transitions",
        &["direction"], // direction=enter/exit
    )
});

// --- Write guard metrics ---

/// Write guard hold duration histogram.
///
/// Tracks how long WriteGuard instances are held before being dropped.
/// Long hold times (>5s) may indicate slow writes or leaked guards.
pub static WRITE_GUARD_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "write_guard_duration_seconds",
        "Duration write guards are held in seconds",
        &["topic"],
        vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ],
    )
});

// --- Batch index cache metrics ---

/// Batch index cache operations (hits/misses)
pub static BATCH_INDEX_OPERATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "batch_index_operations_total",
        "Batch index cache lookup operations",
        &["result"], // result=hit/miss
    )
});

/// Batch index size per partition
pub static BATCH_INDEX_SIZE: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "batch_index_size",
        "Number of entries in batch index per partition",
        &["topic", "partition"],
    )
});

/// Batch index evictions
pub static BATCH_INDEX_EVICTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "batch_index_evictions_total",
        "Batch index entries evicted due to size limit",
        &["topic", "partition"],
    )
});

/// Batch index cache warming - entries loaded during partition open.
pub static BATCH_INDEX_WARM_ENTRIES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "batch_index_warm_entries_total",
        "Total entries loaded during batch index cache warming (performance optimization)",
    )
});

/// Record batches deleted by time-based retention.
pub static RETENTION_DELETED_BATCHES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "retention_deleted_batches_total",
        "Record batches deleted by time-based log retention",
        &["topic", "partition"],
    )
});

// ============================================================================
// Fast Failover and Auto-Balancing metrics
// ============================================================================

/// Failover duration histogram - tracks how long failover operations take.
pub static FAILOVER_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "failover_duration_seconds",
        "Duration of failover operations in seconds",
        &["reason"], // reason=heartbeat_timeout/broker_crash/manual
        vec![0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0],
    )
});

/// Counter for broker failures detected.
pub static BROKER_FAILURES_DETECTED: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "broker_failures_detected_total",
        "Total broker failures detected by the failure detector",
        &["reason"], // reason=heartbeat_timeout/lease_expired
    )
});

/// Counter for partition transfers.
pub static PARTITION_TRANSFERS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "partition_transfers_total",
        "Total partition transfers executed",
        &["reason", "status"], // reason=broker_failure/load_balancing/manual, status=success/failure
    )
});

/// Gauge for suspected brokers count.
pub static SUSPECTED_BROKERS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "suspected_brokers",
        "Number of brokers currently in suspected state (heartbeats missed but not yet failed)",
    )
});

/// Gauge for failed brokers count.
pub static FAILED_BROKERS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "failed_brokers",
        "Number of brokers currently in failed state",
    )
});

/// Gauge for broker load (bytes per second per broker).
pub static BROKER_LOAD: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "broker_load_bytes_per_sec",
        "Current load (bytes/sec) per broker",
        &["broker_id"],
    )
});

/// Gauge for broker partition count.
pub static BROKER_PARTITION_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "broker_partition_count",
        "Number of partitions owned by each broker",
        &["broker_id"],
    )
});

/// Gauge for cluster load deviation.
pub static CLUSTER_LOAD_DEVIATION: Lazy<prometheus::Gauge> = Lazy::new(|| {
    let gauge = prometheus::Gauge::new(
        "cluster_load_deviation",
        "Current load deviation across brokers (0.0 = perfectly balanced)",
    )
    .expect("metric name/help should be valid");
    let _ = REGISTRY.register(Box::new(gauge.clone()));
    gauge
});

/// Counter for auto-rebalance operations.
pub static AUTO_REBALANCE_OPERATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "auto_rebalance_operations_total",
        "Total auto-rebalance operations performed",
        &["status"], // status=performed/skipped_balanced/skipped_cooldown
    )
});

/// Histogram for partition throughput (bytes/sec).
pub static PARTITION_THROUGHPUT: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "partition_throughput_bytes_per_sec",
        "Partition throughput in bytes per second",
        &["topic", "direction"], // direction=in/out
        vec![
            1000.0,      // 1 KB/s
            10000.0,     // 10 KB/s
            100000.0,    // 100 KB/s
            1000000.0,   // 1 MB/s
            10000000.0,  // 10 MB/s
            100000000.0, // 100 MB/s
        ],
    )
});

/// Gauge for active cooldowns in auto-balancer.
pub static ACTIVE_COOLDOWNS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "auto_balancer_active_cooldowns",
        "Number of partitions currently in cooldown (recently moved)",
    )
});

// ============================================================================
// Fast Failover helper functions
// ============================================================================

/// Record a broker failure detection.
pub fn record_broker_failure(reason: &str) {
    BROKER_FAILURES_DETECTED.with_label_values(&[reason]).inc();
}

/// Record a partition transfer.
pub fn record_partition_transfer(reason: &str, success: bool) {
    let status = if success { "success" } else { "failure" };
    PARTITION_TRANSFERS
        .with_label_values(&[reason, status])
        .inc();
}

/// Update suspected brokers gauge.
pub fn set_suspected_brokers(count: usize) {
    SUSPECTED_BROKERS.set(count as i64);
}

/// Update failed brokers gauge.
pub fn set_failed_brokers(count: usize) {
    FAILED_BROKERS.set(count as i64);
}

/// Update broker load gauge.
pub fn set_broker_load(broker_id: i32, bytes_per_sec: f64) {
    BROKER_LOAD
        .with_label_values(&[&broker_id.to_string()])
        .set(bytes_per_sec as i64);
}

/// Update broker partition count gauge.
pub fn set_broker_partition_count(broker_id: i32, count: usize) {
    BROKER_PARTITION_COUNT
        .with_label_values(&[&broker_id.to_string()])
        .set(count as i64);
}

/// Update cluster load deviation gauge.
pub fn set_cluster_load_deviation(deviation: f64) {
    CLUSTER_LOAD_DEVIATION.set(deviation);
}

/// Record an auto-rebalance operation.
pub fn record_auto_rebalance(status: &str) {
    AUTO_REBALANCE_OPERATIONS.with_label_values(&[status]).inc();
}

/// Update active cooldowns gauge.
pub fn set_active_cooldowns(count: usize) {
    ACTIVE_COOLDOWNS.set(count as i64);
}

/// Record failover duration.
pub fn record_failover_duration(reason: &str, duration_secs: f64) {
    FAILOVER_DURATION
        .with_label_values(&[reason])
        .observe(duration_secs);
}

// =============================================================================
// Distributed Systems Metrics Helper Functions
// =============================================================================

/// Record a Raft query latency.
///
/// Call this after completing a Raft read/query operation to track latency.
/// This is critical for monitoring ownership check performance.
///
/// # Arguments
/// * `operation` - The operation type: "get_owner", "get_brokers", "get_topics",
///   "get_group_state", "fetch_offset", etc.
/// * `duration_secs` - The query duration in seconds
pub fn record_raft_query(operation: &str, duration_secs: f64) {
    RAFT_QUERY_DURATION
        .with_label_values(&[operation])
        .observe(duration_secs);
}

/// Record a SlateDB flush latency.
///
/// Call this after SlateDB completes a flush to the object store.
/// This tracks the durability delay for writes when using `await_durable=false`.
///
/// # Arguments
/// * `topic` - The topic being flushed (or "_all" for compaction)
/// * `duration_secs` - The flush duration in seconds
pub fn record_slatedb_flush(topic: &str, duration_secs: f64) {
    SLATEDB_FLUSH_DURATION
        .with_label_values(&[topic])
        .observe(duration_secs);
}

/// Record a stale rebalance decision.
///
/// Call this when a rebalance decision is detected as stale due to concurrent
/// ownership changes. This monitors the rebalance atomicity fix effectiveness.
///
/// # Arguments
/// * `reason` - The staleness reason: "ownership_changed", "raft_index_mismatch"
pub fn record_rebalance_stale_decision(reason: &str) {
    REBALANCE_STALE_DECISIONS.with_label_values(&[reason]).inc();
}

/// Record a failover batch operation.
///
/// Call this after processing a batch of partition transfers during failover.
/// This monitors the thundering herd prevention mechanism.
///
/// # Arguments
/// * `failed_broker` - The broker ID that failed
/// * `batch_size` - Number of partitions in this batch
/// * `status` - The batch result: "success", "partial", "failed"
pub fn record_failover_batch(failed_broker: i32, batch_size: usize, status: &str) {
    let broker_str = failed_broker.to_string();
    FAILOVER_BATCH_SIZE
        .with_label_values(&[&broker_str])
        .observe(batch_size as f64);
    FAILOVER_BATCHES_TOTAL.with_label_values(&[status]).inc();
}

// ============================================================================
// Safe metric registration helpers
// ============================================================================
//
// These functions register metrics to a custom registry and handle errors
// gracefully by returning fallback metrics instead of panicking.

/// Register an IntGauge safely, returning a fallback on error.
fn register_int_gauge_safe(registry: &Registry, name: &str, help: &str) -> IntGauge {
    let gauge = IntGauge::new(name, help).expect("metric name/help should be valid");
    match registry.register(Box::new(gauge.clone())) {
        Ok(()) => gauge,
        Err(e) => {
            warn!(name, error = %e, "Failed to register IntGauge metric, using unregistered fallback");
            // Return the gauge anyway - it just won't be in the registry
            gauge
        }
    }
}

/// Register an IntGaugeVec safely, returning a fallback on error.
fn register_int_gauge_vec_safe(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> IntGaugeVec {
    let gauge = IntGaugeVec::new(opts!(name, help), labels).expect("metric opts should be valid");
    match registry.register(Box::new(gauge.clone())) {
        Ok(()) => gauge,
        Err(e) => {
            warn!(name, error = %e, "Failed to register IntGaugeVec metric, using unregistered fallback");
            gauge
        }
    }
}

/// Register an IntCounterVec safely, returning a fallback on error.
fn register_int_counter_vec_safe(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> IntCounterVec {
    let counter =
        IntCounterVec::new(opts!(name, help), labels).expect("metric opts should be valid");
    match registry.register(Box::new(counter.clone())) {
        Ok(()) => counter,
        Err(e) => {
            warn!(name, error = %e, "Failed to register IntCounterVec metric, using unregistered fallback");
            counter
        }
    }
}

/// Register an IntCounter safely, returning a fallback on error.
fn register_int_counter_safe(registry: &Registry, name: &str, help: &str) -> IntCounter {
    let counter = IntCounter::new(name, help).expect("metric name/help should be valid");
    match registry.register(Box::new(counter.clone())) {
        Ok(()) => counter,
        Err(e) => {
            warn!(name, error = %e, "Failed to register IntCounter metric, using unregistered fallback");
            counter
        }
    }
}

/// Register a HistogramVec safely, returning a fallback on error.
fn register_histogram_vec_safe(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: Vec<f64>,
) -> HistogramVec {
    let histogram = HistogramVec::new(
        HistogramOpts::new(name, help).buckets(buckets.clone()),
        labels,
    )
    .expect("metric opts should be valid");
    match registry.register(Box::new(histogram.clone())) {
        Ok(()) => histogram,
        Err(e) => {
            warn!(name, error = %e, "Failed to register HistogramVec metric, using unregistered fallback");
            histogram
        }
    }
}

/// Initialize the metrics registry by registering all metrics.
///
/// This function is idempotent - it can be called multiple times safely.
/// Metrics are lazily initialized on first access via `lazy_static!`.
pub fn init_metrics() {
    // Force initialization of all metrics by accessing them
    let _ = &*ACTIVE_CONNECTIONS;
    let _ = &*TOTAL_CONNECTIONS;
    let _ = &*FRAME_REJECTED;
    let _ = &*REQUEST_COUNT;
    let _ = &*REQUEST_DURATION;
    let _ = &*MESSAGES_PRODUCED;
    let _ = &*BYTES_PRODUCED;
    let _ = &*MESSAGES_FETCHED;
    let _ = &*BYTES_FETCHED;
    let _ = &*OWNED_PARTITIONS;
    let _ = &*ESTIMATED_PARTITION_MEMORY_BYTES;
    let _ = &*PARTITION_ACQUIRE_REJECTED;
    let _ = &*PARTITION_HIGH_WATERMARK;
    let _ = &*LEASE_OPERATIONS;
    let _ = &*CACHE_OPERATIONS;
    let _ = &*COORDINATOR_DURATION;
    let _ = &*GROUP_OPERATIONS;
    let _ = &*ACTIVE_GROUPS;
    let _ = &*OFFSET_COMMITS;
    let _ = &*COORDINATOR_FAILURES;
    let _ = &*FENCING_DETECTIONS;
    let _ = &*CIRCUIT_BREAKER_STATE;
    let _ = &*CIRCUIT_BREAKER_TRIPS;
    let _ = &*REBALANCE_DURATION;
    let _ = &*PARTITION_ACQUISITION_DURATION;
    let _ = &*TOPIC_PARTITION_COUNT;
    let _ = &*GROUP_MEMBER_COUNT;
    let _ = &*GROUP_GENERATION;
    let _ = &*PENDING_REBALANCES;
    let _ = &*STORAGE_DURATION;
    let _ = &*HWM_RECOVERY_EVENTS;
    let _ = &*PRODUCE_DURATION;
    let _ = &*FETCH_DURATION;
    let _ = &*PRODUCE_DROPPED;
    let _ = &*GROUP_OPERATION_DURATION;
    let _ = &*ZOMBIE_MODE_STATE;
    let _ = &*ZOMBIE_MODE_DURATION;
    let _ = &*ZOMBIE_MODE_TRANSITIONS;
    let _ = &*BATCH_INDEX_OPERATIONS;
    let _ = &*BATCH_INDEX_SIZE;
    let _ = &*BATCH_INDEX_EVICTIONS;
    let _ = &*BATCH_INDEX_WARM_ENTRIES;
    let _ = &*PRODUCER_ID_COUNTER;
    let _ = &*PRODUCER_STATE_PERSISTENCE_FAILURES;
    let _ = &*PRODUCER_STATE_RECOVERY_COUNT;
    // Sequence number monitoring
    let _ = &*SEQUENCE_HIGH_WATERMARK;
    let _ = &*SEQUENCE_WARNINGS;
    // Raft metrics
    let _ = &*RAFT_STATE;
    let _ = &*RAFT_TERM;
    let _ = &*RAFT_COMMIT_INDEX;
    let _ = &*RAFT_APPLIED_INDEX;
    let _ = &*RAFT_ELECTIONS;
    let _ = &*RAFT_PROPOSAL_DURATION;
    let _ = &*RAFT_SNAPSHOTS;
    let _ = &*RAFT_LOG_ENTRIES;
    let _ = &*RAFT_PENDING_PROPOSALS;
    let _ = &*RAFT_BACKPRESSURE_EVENTS;
    // Broker info metrics
    let _ = &*BROKER_INFO;
    let _ = &*BROKER_UPTIME;
    // Request/response size metrics
    let _ = &*REQUEST_SIZE;
    let _ = &*RESPONSE_SIZE;
    // Object store metrics
    let _ = &*OBJECT_STORE_DURATION;
    let _ = &*OBJECT_STORE_BYTES;
    let _ = &*OBJECT_STORE_ERRORS;
    // Network metrics
    let _ = &*NETWORK_BYTES_RECEIVED;
    let _ = &*NETWORK_BYTES_SENT;
    // Clock skew metrics
    let _ = &*HEARTBEAT_RTT;
    let _ = &*CLOCK_SKEW_ESTIMATE;
    let _ = &*MAX_CLOCK_SKEW;
    let _ = &*CLOCK_SKEW_WARNINGS;
    // Distributed systems observability metrics
    let _ = &*RAFT_QUERY_DURATION;
    let _ = &*SLATEDB_FLUSH_DURATION;
    let _ = &*REBALANCE_STALE_DECISIONS;
    let _ = &*FAILOVER_BATCH_SIZE;
    let _ = &*FAILOVER_BATCHES_TOTAL;
}

/// Encode all metrics in Prometheus text format.
pub fn encode_metrics() -> Result<String, Box<dyn std::error::Error>> {
    let encoder = TextEncoder::new();
    let metric_families = REGISTRY.gather();
    let mut buffer = Vec::new();
    encoder.encode(&metric_families, &mut buffer)?;
    Ok(String::from_utf8(buffer)?)
}

/// Gather all metric families from the registry.
/// Used by the health server to expose metrics.
pub fn gather_metrics() -> Vec<prometheus::proto::MetricFamily> {
    REGISTRY.gather()
}

/// Record a Kafka API request.
///
/// `error_code` should be the wire-level Kafka error code as a short string
/// (e.g. "NONE", "NOT_LEADER_FOR_PARTITION"). Cardinality is bounded by the
/// API surface (~25) × Kafka error codes (~80), well below
/// MAX_METRIC_CARDINALITY for any reasonable workload.
pub fn record_request(api: &str, status: &str, duration_secs: f64) {
    record_request_with_code(api, status, "NONE", duration_secs);
}

/// Record a Kafka API request with a typed error code label.
pub fn record_request_with_code(api: &str, status: &str, error_code: &str, duration_secs: f64) {
    REQUEST_COUNT
        .with_label_values(&[api, status, error_code])
        .inc();
    REQUEST_DURATION
        .with_label_values(&[api, status])
        .observe(duration_secs);
}

/// Hidden accessor for integration tests asserting metric values.
#[doc(hidden)]
pub fn request_count_for_test(api: &str, status: &str, error_code: &str) -> u64 {
    REQUEST_COUNT
        .with_label_values(&[api, status, error_code])
        .get()
}

/// Record trailing bytes left after a successful request-body parse.
pub fn record_request_trailing_bytes(api: &str, trailing_bytes: usize) {
    REQUEST_TRAILING_BYTES
        .with_label_values(&[api])
        .inc_by(trailing_bytes as u64);
}

// ============================================================================
// Cardinality-aware metric recording
// ============================================================================

use std::sync::atomic::{AtomicBool, AtomicUsize};

/// Pre-formatted partition labels for the common range. Most clusters keep
/// partition counts under 1024; pre-formatting amortizes the per-record
/// `itoa::Buffer` work and keeps a stable `&'static str` so the metric
/// `with_label_values` call doesn't allocate. Outside this range we fall
/// back to `format_partition_label`.
static PARTITION_LABEL_CACHE: Lazy<&'static [String]> = Lazy::new(|| {
    let v: Vec<String> = (0..1024).map(|i: i32| i.to_string()).collect();
    Box::leak(v.into_boxed_slice())
});

fn cached_partition_label(partition: i32) -> Option<&'static str> {
    if (0..1024).contains(&partition) {
        Some(PARTITION_LABEL_CACHE[partition as usize].as_str())
    } else {
        None
    }
}

/// Global configuration for partition-level metrics.
/// Set via `configure_metrics()` at startup.
static PARTITION_METRICS_ENABLED: AtomicBool = AtomicBool::new(true);
static MAX_METRIC_CARDINALITY: AtomicUsize = AtomicUsize::new(10_000);

/// Tracks unique topic/partition combinations for cardinality limiting.
///
/// `DashSet` gives wait-free reads (sharded — concurrent threads rarely
/// contend on the same shard) and lock-free first-time inserts, so the
/// produce/fetch hot path no longer serializes through a single async
/// rwlock for every record's cardinality check.
/// Tracks unique (topic, partition) pairs already labelled. Stored as a
/// 64-bit hash so the steady-state probe — `record_produce`/`record_fetch` —
/// hits a single `DashSet::contains(&u64)` with zero allocation. The
/// previous shape (`DashSet<(String, i32)>`) forced a `topic.to_string()`
/// per call to build the lookup key, allocating on every produce and
/// fetch. The cap is a soft limit on cardinality growth, so the negligible
/// false-positive rate from a 64-bit hash collision is benign — at most
/// one fewer label is dropped to `_overflow`.
static TRACKED_PARTITIONS: Lazy<dashmap::DashSet<u64>> = Lazy::new(dashmap::DashSet::new);

/// Hash a (topic, partition) pair into the cardinality-tracking key space.
/// Collision odds at the 10k-entry cap are ~10⁻¹², so a stray collision
/// just means one fewer label gets dropped to `_overflow` — benign on a
/// soft cap.
fn tracked_partition_key(topic: &str, partition: i32) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    topic.hash(&mut h);
    partition.hash(&mut h);
    h.finish()
}

/// Tracks unique topic labels used on latency histograms.
static TRACKED_LATENCY_TOPICS: Lazy<dashmap::DashSet<String>> = Lazy::new(dashmap::DashSet::new);

/// Tracks unique principals seen on labelled metrics. Bounded so a hostile
/// or buggy client can't blow up cardinality by churning through random
/// `User:<uuid>` strings.
static TRACKED_PRINCIPALS: Lazy<dashmap::DashSet<String>> = Lazy::new(dashmap::DashSet::new);

/// Tracks unique consumer-group IDs seen on labelled metrics. Bounded so a
/// runaway group-ID generator (random group IDs, per-request groups) cannot
/// inflate Prometheus cardinality without limit.
static TRACKED_GROUPS: Lazy<dashmap::DashSet<String>> = Lazy::new(dashmap::DashSet::new);

/// Configure metrics cardinality settings.
///
/// Call this once at startup before recording any partition metrics.
///
/// # Arguments
/// * `enable_partition_metrics` - Whether to include partition labels
/// * `max_cardinality` - Maximum unique topic/partition combinations to track (0 = unlimited)
pub fn configure_metrics(enable_partition_metrics: bool, max_cardinality: usize) {
    PARTITION_METRICS_ENABLED.store(enable_partition_metrics, Ordering::SeqCst);
    MAX_METRIC_CARDINALITY.store(max_cardinality, Ordering::SeqCst);
}

/// Check if a topic/partition should be tracked or overflow.
///
/// Returns the partition label to use:
/// - The actual partition number if under cardinality limit
/// - "_overflow" if limit is reached and this is a new topic/partition
/// - "_all" if partition metrics are disabled
async fn get_partition_label(topic: &str, partition: i32) -> String {
    if !PARTITION_METRICS_ENABLED.load(Ordering::Relaxed) {
        return "_all".to_string();
    }

    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return format_partition_label(partition);
    }

    let key = tracked_partition_key(topic, partition);

    // Wait-free read on DashSet — different shards never contend.
    if TRACKED_PARTITIONS.contains(&key) {
        return format_partition_label(partition);
    }
    if TRACKED_PARTITIONS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED
            .with_label_values(&["partition"])
            .inc();
        return "_overflow".to_string();
    }
    // Insert is lock-free at the shard granularity. We may briefly exceed the
    // cap under concurrent inserts; that's acceptable — the cap is a
    // soft limit on cardinality growth, not a hard ceiling.
    TRACKED_PARTITIONS.insert(key);
    format_partition_label(partition)
}

fn format_partition_label(partition: i32) -> String {
    if let Some(cached) = cached_partition_label(partition) {
        return cached.to_string();
    }
    let mut buf = itoa::Buffer::new();
    buf.format(partition).to_string()
}

/// Bound topic labels on latency histograms to avoid unbounded Prometheus
/// cardinality when clients auto-create topics.
fn bounded_topic_label(topic: &str) -> String {
    if topic == "_multi" {
        return topic.to_string();
    }

    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return topic.to_string();
    }

    if TRACKED_LATENCY_TOPICS.contains(topic) {
        return topic.to_string();
    }
    if TRACKED_LATENCY_TOPICS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED.with_label_values(&["topic"]).inc();
        return "_overflow".to_string();
    }
    TRACKED_LATENCY_TOPICS.insert(topic.to_string());
    topic.to_string()
}

/// Bound principal labels on metrics that include the principal as a
/// dimension. Same overflow semantics as [`bounded_topic_label`].
pub(crate) fn bounded_principal_label(principal: &str) -> String {
    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return principal.to_string();
    }
    if TRACKED_PRINCIPALS.contains(principal) {
        return principal.to_string();
    }
    if TRACKED_PRINCIPALS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED
            .with_label_values(&["principal"])
            .inc();
        return "_other".to_string();
    }
    TRACKED_PRINCIPALS.insert(principal.to_string());
    principal.to_string()
}

/// Bound consumer-group labels on metrics that include `group` as a dimension.
/// Same overflow semantics as [`bounded_topic_label`].— without
/// this, every `record_offset_commit` / `record_rebalance_duration` call
/// became a cardinality bomb when a buggy client used random group IDs.
pub(crate) fn bounded_group_label(group: &str) -> String {
    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return group.to_string();
    }
    if TRACKED_GROUPS.contains(group) {
        return group.to_string();
    }
    if TRACKED_GROUPS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED.with_label_values(&["group"]).inc();
        return "_overflow".to_string();
    }
    TRACKED_GROUPS.insert(group.to_string());
    group.to_string()
}

/// Bound (topic, partition) labels for per-partition counters/gauges. Same
/// overflow semantics as [`bounded_topic_label`].
pub(crate) fn bounded_partition_label(topic: &str, partition: i32) -> (String, String) {
    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return (topic.to_string(), partition.to_string());
    }
    let key = tracked_partition_key(topic, partition);
    if TRACKED_PARTITIONS.contains(&key) {
        return (topic.to_string(), partition.to_string());
    }
    if TRACKED_PARTITIONS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED
            .with_label_values(&["partition"])
            .inc();
        return ("_overflow".to_string(), "_overflow".to_string());
    }
    TRACKED_PARTITIONS.insert(key);
    (topic.to_string(), partition.to_string())
}

/// Synchronous partition label for the produce/fetch hot path.
///
/// Cardinality limiting is identical to the async [`get_partition_label`], but
/// the result is a `Cow<'static, str>` that avoids a per-call `String`
/// allocation in the overwhelming common case: partition-metrics disabled
/// (`_all`), the cardinality-overflow placeholder (`_overflow`), or a small
/// partition index served as a `&'static str` from `PARTITION_LABEL_CACHE`.
/// Only partition indices outside that cache (>= 1024, uncommon) allocate.
fn get_partition_label_sync(topic: &str, partition: i32) -> Cow<'static, str> {
    if !PARTITION_METRICS_ENABLED.load(Ordering::Relaxed) {
        return Cow::Borrowed("_all");
    }

    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality != 0 {
        let key = tracked_partition_key(topic, partition);
        if !TRACKED_PARTITIONS.contains(&key) {
            if TRACKED_PARTITIONS.len() >= max_cardinality {
                METRIC_LABELS_DROPPED
                    .with_label_values(&["partition"])
                    .inc();
                return Cow::Borrowed("_overflow");
            }
            TRACKED_PARTITIONS.insert(key);
        }
    }

    match cached_partition_label(partition) {
        Some(label) => Cow::Borrowed(label),
        None => {
            let mut buf = itoa::Buffer::new();
            Cow::Owned(buf.format(partition).to_string())
        }
    }
}

/// Record produced messages with cardinality awareness.
pub fn record_produce(topic: &str, partition: i32, message_count: u64, bytes: u64) {
    let partition_str = get_partition_label_sync(topic, partition);
    MESSAGES_PRODUCED
        .with_label_values(&[topic, partition_str.as_ref()])
        .inc_by(message_count);
    BYTES_PRODUCED
        .with_label_values(&[topic, partition_str.as_ref()])
        .inc_by(bytes);
}

/// Record produced messages with async cardinality limiting.
///
/// Use this version when calling from async context for proper cardinality enforcement.
pub async fn record_produce_async(topic: &str, partition: i32, message_count: u64, bytes: u64) {
    let partition_str = get_partition_label(topic, partition).await;
    MESSAGES_PRODUCED
        .with_label_values(&[topic, &partition_str])
        .inc_by(message_count);
    BYTES_PRODUCED
        .with_label_values(&[topic, &partition_str])
        .inc_by(bytes);
}

/// Record fetched messages with cardinality awareness.
pub fn record_fetch(topic: &str, partition: i32, message_count: u64, bytes: u64) {
    let partition_str = get_partition_label_sync(topic, partition);
    MESSAGES_FETCHED
        .with_label_values(&[topic, partition_str.as_ref()])
        .inc_by(message_count);
    BYTES_FETCHED
        .with_label_values(&[topic, partition_str.as_ref()])
        .inc_by(bytes);
}

/// Record fetched messages with async cardinality limiting.
///
/// Use this version when calling from async context for proper cardinality enforcement.
pub async fn record_fetch_async(topic: &str, partition: i32, message_count: u64, bytes: u64) {
    let partition_str = get_partition_label(topic, partition).await;
    MESSAGES_FETCHED
        .with_label_values(&[topic, &partition_str])
        .inc_by(message_count);
    BYTES_FETCHED
        .with_label_values(&[topic, &partition_str])
        .inc_by(bytes);
}

/// Record a partition lease operation.
pub fn record_lease_operation(operation: &str, status: &str) {
    LEASE_OPERATIONS
        .with_label_values(&[operation, status])
        .inc();
}

/// Rough per-partition SlateDB instance overhead used *only* to compute the
/// `estimated_partition_memory_bytes` gauge: memtable + WAL write buffer +
/// per-instance block cache + background task stacks. This is an
/// order-of-magnitude planning aid, not precise accounting — calibrate against
/// real measured RSS per owned partition for your workload (see audit P1-2).
pub const SLATEDB_ESTIMATED_BYTES_PER_PARTITION: usize = 8 * 1024 * 1024;

/// Refresh the `estimated_partition_memory_bytes` gauge from the current owned
/// partition count. Cheap (single multiply + gauge set); call from the
/// periodic heartbeat loop rather than per acquire/release so it cannot drift
/// from the true owned set.
pub fn record_estimated_partition_memory(owned_partitions: usize, batch_index_max_size: usize) {
    let per_partition =
        batch_index_max_size.saturating_mul(16) + SLATEDB_ESTIMATED_BYTES_PER_PARTITION;
    let total = per_partition.saturating_mul(owned_partitions);
    ESTIMATED_PARTITION_MEMORY_BYTES.set(total as i64);
}

/// Record a partition acquisition rejected before any store was opened.
///
/// # Arguments
/// * `reason` - Why the acquire was rejected (e.g. "max_owned").
pub fn record_partition_acquire_rejected(reason: &str) {
    PARTITION_ACQUIRE_REJECTED
        .with_label_values(&[reason])
        .inc();
}

/// Record a lease cache hit or miss.
///
/// Call with "hit" when lease verification is served from cache,
/// or "miss" when a Raft call is required.
pub fn record_lease_cache(result: &str) {
    LEASE_CACHE_OPERATIONS.with_label_values(&[result]).inc();
}

/// Record an idempotency rejection.
///
/// # Arguments
/// * `reason` - The rejection reason ("duplicate", "out_of_order", "fenced_epoch")
pub fn record_idempotency_rejection(reason: &str) {
    IDEMPOTENCY_REJECTIONS.with_label_values(&[reason]).inc();
}

/// Record an epoch mismatch detection.
///
/// This is called when a write is rejected because the stored leader epoch
/// doesn't match our expected epoch, indicating another broker has acquired
/// the partition. This is a critical safety metric for TOCTOU prevention.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition index
pub fn record_epoch_mismatch(topic: &str, partition: i32) {
    let (t, p) = bounded_partition_label(topic, partition);
    EPOCH_MISMATCH_DETECTIONS.with_label_values(&[&t, &p]).inc();
}

/// Record a permanent offset gap of `record_count` records caused by a
/// transient SlateDB write error after the offset reservation was
/// disarmed. See `PARTITION_OFFSET_GAPS` for the design rationale.
pub fn record_partition_offset_gap(topic: &str, partition: i32, record_count: i32) {
    let (t, p) = bounded_partition_label(topic, partition);
    PARTITION_OFFSET_GAPS
        .with_label_values(&[&t, &p])
        .inc_by(record_count.max(0) as u64);
}

/// Record that a `ListOffsets(timestamp)` query was halted at the scan cap.
/// The cap protects against a full-log-scan DoS where an authenticated
/// reader points the timestamp at `i64::MIN`. A persistent rate here means
/// real workloads are scanning past the cap and a sparse time index is
/// overdue.
pub fn record_list_offsets_truncated(topic: &str) {
    let t = bounded_principal_label(topic);
    LIST_OFFSETS_TRUNCATED.with_label_values(&[&t]).inc();
}

/// Record a corrupt-batch parse failure from a partition scan. `operation`
/// is e.g. `"fetch"` or `"retention"` so dashboards can split which
/// scanner observed it.
pub fn record_corrupt_batch(topic: &str, partition: i32, operation: &str) {
    let (t, p) = bounded_partition_label(topic, partition);
    CORRUPT_BATCHES
        .with_label_values(&[t.as_str(), p.as_str(), operation])
        .inc();
}

/// Record a cache lookup operation.
///
/// # Arguments
/// * `cache` - The cache name ("owner" for partition owner cache, "metadata" for topic metadata)
/// * `hit` - True if the cache hit, false if it missed
pub fn record_cache_lookup(cache: &str, hit: bool) {
    let result = if hit { "hit" } else { "miss" };
    CACHE_OPERATIONS.with_label_values(&[cache, result]).inc();
}

/// Record a coordinator operation with timing.
///
/// # Arguments
/// * `operation` - The operation name (e.g., "get_owner", "set_owner", "join_group")
/// * `duration_secs` - The operation duration in seconds
pub fn record_coordinator_operation(operation: &str, duration_secs: f64) {
    COORDINATOR_DURATION
        .with_label_values(&[operation])
        .observe(duration_secs);
}

/// Record a consumer group operation.
///
/// # Arguments
/// * `operation` - The operation type ("join", "sync", "heartbeat", "leave")
/// * `status` - The result ("success" or "error")
pub fn record_group_operation(operation: &str, status: &str) {
    GROUP_OPERATIONS
        .with_label_values(&[operation, status])
        .inc();
}

// ============================================================================
// Request latency recording functions
// ============================================================================

/// Record a produce request latency.
///
/// Call this after completing a produce request to track end-to-end latency.
///
/// # Arguments
/// * `topic` - The topic name (use "_multi" if request spans multiple topics)
/// * `status` - The result ("success" or "error")
/// * `duration_secs` - The request duration in seconds
pub fn record_produce_latency(topic: &str, status: &str, duration_secs: f64) {
    let topic_label = bounded_topic_label(topic);
    PRODUCE_DURATION
        .with_label_values(&[topic_label.as_str(), status])
        .observe(duration_secs);
}

/// Record a fetch request latency.
///
/// Call this after completing a fetch request to track end-to-end latency.
/// Note: Fetch requests may wait for new data, so timeouts are expected.
///
/// # Arguments
/// * `topic` - The topic name (use "_multi" if request spans multiple topics)
/// * `status` - The result ("success", "error", or "timeout")
/// * `duration_secs` - The request duration in seconds
pub fn record_fetch_latency(topic: &str, status: &str, duration_secs: f64) {
    let topic_label = bounded_topic_label(topic);
    FETCH_DURATION
        .with_label_values(&[topic_label.as_str(), status])
        .observe(duration_secs);
}

/// Record produce records dropped on the acks=0 path.
///
/// # Arguments
/// * `reason` - Drop cause (`backpressure`, `zombie`, `not_leader`, `acl_denied`, `invalid_topic`, `crc_invalid`)
/// * `count` - Number of records (or partition writes) dropped
pub fn record_produce_dropped(reason: &str, count: u64) {
    if count > 0 {
        PRODUCE_DROPPED.with_label_values(&[reason]).inc_by(count);
    }
}

/// Record an inbound frame the reader refused before dispatch.
///
/// `reason` must be one of the stable, low-cardinality tags produced by
/// [`crate::error::FrameRejectReason::as_str`] so the label set stays bounded.
/// Clean peer disconnects are deliberately *not* recorded here — only frames
/// the broker actively rejected (oversize, inflight-budget exhaustion, slow
/// body, malformed size), which are the capacity / DoS signals operators alert
/// on.
pub fn record_frame_rejected(reason: &str) {
    FRAME_REJECTED.with_label_values(&[reason]).inc();
}

/// Record an ACL denial. Caller passes the principal, the API surface
/// (`Produce`, `Fetch`, etc) and the resource type (`Topic`, `Group`, `Cluster`).
pub fn record_acl_denial(principal: &str, api: &str, resource_type: &str) {
    let principal_label = bounded_principal_label(principal);
    ACL_DENIAL_TOTAL
        .with_label_values(&[principal_label.as_str(), api, resource_type])
        .inc();
}

/// Record a consumer group operation latency.
///
/// Call this after completing a consumer group operation to track latency.
/// This complements `record_group_operation()` which only tracks counts.
///
/// # Arguments
/// * `operation` - The operation type ("join", "sync", "heartbeat", "leave", "offset_commit", "offset_fetch")
/// * `status` - The result ("success" or "error")
/// * `duration_secs` - The operation duration in seconds
pub fn record_group_operation_latency(operation: &str, status: &str, duration_secs: f64) {
    GROUP_OPERATION_DURATION
        .with_label_values(&[operation, status])
        .observe(duration_secs);
}

/// Record both count and latency for a consumer group operation.
///
/// Convenience function that calls both `record_group_operation()` and
/// `record_group_operation_latency()` in one call.
///
/// # Arguments
/// * `operation` - The operation type ("join", "sync", "heartbeat", "leave")
/// * `status` - The result ("success" or "error")
/// * `duration_secs` - The operation duration in seconds
pub fn record_group_operation_with_latency(operation: &str, status: &str, duration_secs: f64) {
    record_group_operation(operation, status);
    record_group_operation_latency(operation, status, duration_secs);
}

/// Record an offset commit operation.
///
/// # Arguments
/// * `group` - The consumer group ID
/// * `topic` - The topic name
/// * `status` - The result ("success" or "error")
pub fn record_offset_commit(group: &str, topic: &str, status: &str) {
    let group_lbl = bounded_group_label(group);
    let topic_lbl = bounded_topic_label(topic);
    OFFSET_COMMITS
        .with_label_values(&[group_lbl.as_str(), topic_lbl.as_str(), status])
        .inc();
}

/// Update the active consumer groups gauge.
pub fn set_active_groups(count: i64) {
    ACTIVE_GROUPS.set(count);
}

/// Increment active consumer groups.
pub fn inc_active_groups() {
    ACTIVE_GROUPS.inc();
}

/// Decrement active consumer groups.
pub fn dec_active_groups() {
    ACTIVE_GROUPS.dec();
}

/// Record a critical coordinator failure.
///
/// This should be called when a coordinator operation (like heartbeat)
/// has failed multiple times consecutively, indicating a serious issue.
///
/// # Arguments
/// * `operation` - The operation that failed ("heartbeat", "lease_renewal", etc.)
pub fn record_coordinator_failure(operation: &str) {
    COORDINATOR_FAILURES.with_label_values(&[operation]).inc();
}

/// Record a coordinator recovery (e.g., exit from zombie mode).
///
/// Track recovery from failure states.
///
/// # Arguments
/// * `operation` - The operation that recovered ("zombie_mode", "heartbeat", etc.)
pub fn record_coordinator_recovery(operation: &str) {
    // Reuse the same counter with a "recovery" suffix to track recoveries
    // This pairs with record_coordinator_failure for complete observability
    COORDINATOR_FAILURES
        .with_label_values(&[&format!("{}_recovery", operation)])
        .inc();
}

/// Record a fencing detection event.
///
/// Track how fencing errors are detected, useful for monitoring detection reliability.
/// High counts of "fail_closed" may indicate missing patterns in SAFE_PATTERNS.
///
/// # Arguments
/// * `method` - Detection method: "typed", "pattern", "fail_closed", or "not_fencing"
///
/// # Deprecated
/// Use `record_fencing_detection_with_circuit_breaker` instead for proper circuit breaker handling.
pub fn record_fencing_detection(method: &str) {
    // Delegate to circuit breaker version, ignoring return value
    let _ = record_fencing_detection_with_circuit_breaker(method);
}

/// Record a consumer group rebalance duration.
///
/// # Arguments
/// * `group` - The consumer group ID
/// * `duration_secs` - The rebalance duration in seconds
pub fn record_rebalance_duration(group: &str, duration_secs: f64) {
    let group_lbl = bounded_group_label(group);
    REBALANCE_DURATION
        .with_label_values(&[group_lbl.as_str()])
        .observe(duration_secs);
}

/// Record partition acquisition latency.
///
/// # Arguments
/// * `topic` - The topic name
/// * `status` - The result ("success", "error", "fenced")
/// * `duration_secs` - The acquisition duration in seconds
pub fn record_partition_acquisition(topic: &str, status: &str, duration_secs: f64) {
    let topic_lbl = bounded_topic_label(topic);
    PARTITION_ACQUISITION_DURATION
        .with_label_values(&[topic_lbl.as_str(), status])
        .observe(duration_secs);
}

/// Set the partition count for a topic.
///
/// # Arguments
/// * `topic` - The topic name
/// * `count` - Number of partitions
pub fn set_topic_partition_count(topic: &str, count: i64) {
    let topic_lbl = bounded_topic_label(topic);
    TOPIC_PARTITION_COUNT
        .with_label_values(&[topic_lbl.as_str()])
        .set(count);
}

/// Soft cap on per-group label cardinality. Beyond this, the set helpers
/// drop new labels instead of letting the registry grow unboundedly under a
/// runaway group-ID generator. Pick a number large enough that real
/// deployments aren't truncated but small enough to limit blast radius.
const MAX_GROUP_LABEL_CARDINALITY: usize = 10_000;

fn group_label_under_cap<C: prometheus::core::Collector>(metric: &C, _group: &str) -> bool {
    let mut count = 0usize;
    for family in metric.collect() {
        count += family.get_metric().len();
        if count > MAX_GROUP_LABEL_CARDINALITY {
            return false;
        }
    }
    true
}

/// Set the member count for a consumer group.
///
/// # Arguments
/// * `group` - The consumer group ID
/// * `count` - Number of members
pub fn set_group_member_count(group: &str, count: i64) {
    if !group_label_under_cap(&*GROUP_MEMBER_COUNT, group) {
        return;
    }
    GROUP_MEMBER_COUNT.with_label_values(&[group]).set(count);
}

/// Set the generation ID for a consumer group.
///
/// # Arguments
/// * `group` - The consumer group ID
/// * `generation` - The current generation ID
pub fn set_group_generation(group: &str, generation: i64) {
    if !group_label_under_cap(&*GROUP_GENERATION, group) {
        return;
    }
    GROUP_GENERATION.with_label_values(&[group]).set(generation);
}

/// Remove per-group time series for the named group.
///
/// Called from the DeleteGroups path so the Prometheus registry doesn't
/// accumulate dead labels for short-lived consumer groups.
pub fn forget_group_metrics(group: &str) {
    let _ = GROUP_MEMBER_COUNT.remove_label_values(&[group]);
    let _ = GROUP_GENERATION.remove_label_values(&[group]);
}

/// Remove per-topic time series for the named topic.
///
/// Called from the DeleteTopics path so the Prometheus registry doesn't
/// accumulate dead labels for deleted topics.
pub fn forget_topic_metrics(topic: &str, partition_count: i32) {
    let topic_lbl = bounded_topic_label(topic);
    let _ = TOPIC_PARTITION_COUNT.remove_label_values(&[topic_lbl.as_str()]);
    let mut buf = itoa::Buffer::new();
    for partition in 0..partition_count {
        let partition_str = buf.format(partition).to_string();
        let _ = MESSAGES_PRODUCED.remove_label_values(&[topic_lbl.as_str(), &partition_str]);
        let _ = BYTES_PRODUCED.remove_label_values(&[topic_lbl.as_str(), &partition_str]);
        let _ = MESSAGES_FETCHED.remove_label_values(&[topic_lbl.as_str(), &partition_str]);
        let _ = BYTES_FETCHED.remove_label_values(&[topic_lbl.as_str(), &partition_str]);
        let _ = PARTITION_HIGH_WATERMARK.remove_label_values(&[topic_lbl.as_str(), &partition_str]);
    }
}

/// Increment pending rebalances count.
pub fn inc_pending_rebalances() {
    PENDING_REBALANCES.inc();
}

/// Decrement pending rebalances count.
pub fn dec_pending_rebalances() {
    PENDING_REBALANCES.dec();
}

/// Record a SlateDB storage operation.
///
/// # Arguments
/// * `operation` - The operation type ("get", "put", "delete")
/// * `duration_secs` - The operation duration in seconds
pub fn record_storage_operation(operation: &str, duration_secs: f64) {
    STORAGE_DURATION
        .with_label_values(&[operation])
        .observe(duration_secs);
}

/// Record a high watermark recovery event.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `recovered` - True if HWM was recovered (orphaned records found), false otherwise
pub fn record_hwm_recovery(topic: &str, partition: i32, recovered: bool) {
    let result = if recovered { "recovered" } else { "no_change" };
    let (t, p) = bounded_partition_label(topic, partition);
    HWM_RECOVERY_EVENTS
        .with_label_values(&[t.as_str(), p.as_str(), result])
        .inc();
}

/// Recovery gap counter.
/// Tracks when offset gaps are detected during HWM recovery.
pub static RECOVERY_GAP_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "recovery_gap_events_total",
        "Offset gap events detected during HWM recovery",
        &["severity"], // "gap_count", "gap_records"
    )
});

/// Record recovery gap detection.
///
/// This is called when HWM recovery detects offset gaps, indicating
/// potential data loss. This is a critical alert condition.
///
/// # Arguments
/// * `gap_count` - Number of gaps detected
/// * `total_gap_records` - Total estimated records in gaps
pub fn record_recovery_gap(gap_count: i64, total_gap_records: i64) {
    RECOVERY_GAP_EVENTS
        .with_label_values(&["gap_count"])
        .inc_by(gap_count as u64);
    RECOVERY_GAP_EVENTS
        .with_label_values(&["gap_records"])
        .inc_by(total_gap_records as u64);
}

/// Lease TTL histogram.
/// Tracks remaining lease TTL at the time of write validation.
/// This helps detect near-miss scenarios where writes barely succeed
/// before lease expiration.
pub static LEASE_TTL_AT_WRITE: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "lease_ttl_at_write_seconds",
        "Remaining lease TTL when write is validated",
        &["topic"],
        vec![5.0, 10.0, 15.0, 20.0, 30.0, 45.0, 60.0],
    )
});

/// Record lease TTL at write time.
///
/// This should be called during write validation to track the remaining
/// lease TTL. Helps identify near-miss scenarios and tune lease parameters.
///
/// # Arguments
/// * `topic` - The topic name
/// * `remaining_ttl_secs` - Remaining lease TTL in seconds
pub fn record_lease_ttl_at_write(topic: &str, remaining_ttl_secs: u64) {
    LEASE_TTL_AT_WRITE
        .with_label_values(&[topic])
        .observe(remaining_ttl_secs as f64);
}

// --- Zombie mode helper functions ---

/// Record entering zombie mode.
///
/// Call this when the broker enters zombie mode. This:
/// - Sets the zombie mode gauge to 1
/// - Increments the "enter" transition counter
pub fn enter_zombie_mode() {
    ZOMBIE_MODE_STATE.set(1);
    ZOMBIE_MODE_TRANSITIONS.with_label_values(&["enter"]).inc();
}

/// Record exiting zombie mode.
///
/// Call this when the broker exits zombie mode. This:
/// - Sets the zombie mode gauge to 0
/// - Increments the "exit" transition counter
/// - Records the duration of the zombie mode episode
///
/// # Arguments
/// * `duration_secs` - How long the broker was in zombie mode
/// * `exit_reason` - Why zombie mode was exited ("recovered", "manual", "shutdown")
pub fn exit_zombie_mode(duration_secs: f64, exit_reason: &str) {
    ZOMBIE_MODE_STATE.set(0);
    ZOMBIE_MODE_TRANSITIONS.with_label_values(&["exit"]).inc();
    ZOMBIE_MODE_DURATION
        .with_label_values(&[exit_reason])
        .observe(duration_secs);
}

// --- Batch index cache helper functions ---

/// Record a batch index cache hit.
pub fn record_batch_index_hit() {
    BATCH_INDEX_OPERATIONS.with_label_values(&["hit"]).inc();
}

/// Record a batch index cache miss.
pub fn record_batch_index_miss() {
    BATCH_INDEX_OPERATIONS.with_label_values(&["miss"]).inc();
}

/// Update the batch index size for a partition.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `size` - Current number of entries in the index
pub fn set_batch_index_size(topic: &str, partition: i32, size: i64) {
    let (t, p) = bounded_partition_label(topic, partition);
    BATCH_INDEX_SIZE.with_label_values(&[&t, &p]).set(size);
}

/// Record batch index evictions.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `count` - Number of entries evicted
pub fn record_batch_index_eviction(topic: &str, partition: i32, count: u64) {
    let (t, p) = bounded_partition_label(topic, partition);
    BATCH_INDEX_EVICTIONS
        .with_label_values(&[&t, &p])
        .inc_by(count);
}

/// Record batch index cache warming entries.
///
/// Called during partition open to track how many entries were pre-loaded
/// into the batch index cache.
pub fn record_batch_index_warm_entries(count: i64) {
    BATCH_INDEX_WARM_ENTRIES.add(count);
}

/// Record batches deleted by the retention task for a partition.
pub fn record_retention_deleted_batches(topic: &str, partition: i32, count: u64) {
    let (t, p) = bounded_partition_label(topic, partition);
    RETENTION_DELETED_BATCHES
        .with_label_values(&[&t, &p])
        .inc_by(count);
}

// --- SASL Authentication metrics ---

/// SASL authentication operations
pub static SASL_AUTH_OPERATIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "sasl_auth_total",
        "Total SASL authentication attempts",
        &["mechanism", "status"], // mechanism=PLAIN/SCRAM-SHA-256/etc, status=success/failure
    )
});

/// Record a SASL authentication attempt.
///
/// # Arguments
/// * `mechanism` - The SASL mechanism used (e.g., "PLAIN", "SCRAM-SHA-256")
/// * `status` - Either "success" or "failure"
pub fn record_sasl_auth(mechanism: &str, status: &str) {
    SASL_AUTH_OPERATIONS
        .with_label_values(&[mechanism, status])
        .inc();
}

// --- Lease TTL metrics ---

/// Lease TTL too short rejections
pub static LEASE_TOO_SHORT: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "lease_too_short_total",
        "Total writes rejected due to insufficient lease TTL",
        &["topic", "partition"],
    )
});

/// Record a write rejection due to lease TTL being too short.
///
/// This tracks the TOCTOU prevention mechanism - when a write is rejected
/// because the remaining lease time is below the safety threshold.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
pub fn record_lease_too_short(topic: &str, partition: i32) {
    let (topic_lbl, partition_lbl) = bounded_partition_label(topic, partition);
    LEASE_TOO_SHORT
        .with_label_values(&[topic_lbl.as_str(), partition_lbl.as_str()])
        .inc();
}

// --- Consumer Lag metrics ---

/// Consumer group lag per topic/partition.
/// This tracks the difference between high watermark and committed offset.
pub static CONSUMER_LAG: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "consumer_lag",
        "Consumer group lag (high watermark - committed offset)",
        &["group", "topic", "partition"],
    )
});

/// Consumer group committed offset per topic/partition.
pub static CONSUMER_COMMITTED_OFFSET: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "consumer_committed_offset",
        "Last committed offset for consumer group",
        &["group", "topic", "partition"],
    )
});

// --- Producer ID metrics ---

/// Current producer ID counter value.
/// Tracks the last allocated producer ID to monitor capacity utilization.
/// Producer IDs are 64-bit signed integers, so capacity warnings trigger at
/// 50%, 75%, and 90% of i64::MAX.
pub static PRODUCER_ID_COUNTER: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "producer_id_counter",
        "Current producer ID counter value (tracks last allocated producer ID)",
    )
});

// --- Producer State Persistence metrics ---

/// Producer state persistence failures.
/// Tracks failures when persisting producer idempotency state to SlateDB.
/// Non-zero values indicate potential idempotency issues after restart.
pub static PRODUCER_STATE_PERSISTENCE_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "producer_state_persistence_failures_total",
        "Failed attempts to persist producer idempotency state",
        &["topic", "partition"],
    )
});

/// Producer state recovery count.
/// Tracks how many producer states were recovered from SlateDB on partition open.
/// Useful for understanding idempotency state cardinality and recovery health.
pub static PRODUCER_STATE_RECOVERY_COUNT: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "producer_state_recovery_count",
        "Number of producer states recovered from storage on partition open",
        &["topic", "partition"],
    )
});

/// Producer state cache evictions.
/// Tracks when producer idempotency states are evicted from the in-memory cache.
/// Evictions with has_sequence=true are concerning as they indicate potential
/// idempotency gaps if the producer reconnects.
///
/// **ALERT**: Monitor evictions where has_sequence=true - these could lead to
/// duplicate messages being accepted if producers reconnect after idle period.
pub static PRODUCER_STATE_EVICTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "producer_state_evictions_total",
        "Producer state cache evictions (has_sequence=true indicates idempotency risk)",
        &["topic", "partition", "has_sequence"],
    )
});

/// Record a producer state eviction from the cache.
///
/// This is called when a producer's idempotency state is evicted from the
/// in-memory cache due to TTL expiry or capacity limits.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `has_sequence` - Whether the producer had sequence > 0 (active producer)
pub fn record_producer_state_eviction(topic: &str, partition: i32, has_sequence: bool) {
    PRODUCER_STATE_EVICTIONS
        .with_label_values(&[
            topic,
            &partition.to_string(),
            if has_sequence { "true" } else { "false" },
        ])
        .inc();
}

/// Record consumer lag for a consumer group.
///
/// This tracks the difference between the high watermark (latest available offset)
/// and the committed offset for a consumer group. Essential for monitoring
/// consumer health and detecting stuck consumers.
///
/// # Arguments
/// * `group` - The consumer group ID
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `committed_offset` - The last committed offset
/// * `high_watermark` - The current high watermark (latest offset)
pub fn record_consumer_lag(
    group: &str,
    topic: &str,
    partition: i32,
    committed_offset: i64,
    high_watermark: i64,
) {
    if !group_label_under_cap(&*CONSUMER_LAG, group) {
        return;
    }
    let topic_label = bounded_topic_label(topic);
    let partition_str = partition.to_string();

    // Calculate lag (can be negative if committed offset is ahead of HWM due to race)
    let lag = (high_watermark - committed_offset).max(0);

    CONSUMER_LAG
        .with_label_values(&[group, topic_label.as_str(), &partition_str])
        .set(lag);

    CONSUMER_COMMITTED_OFFSET
        .with_label_values(&[group, topic_label.as_str(), &partition_str])
        .set(committed_offset);
}

/// Set consumer lag directly (useful when only lag is known).
///
/// # Arguments
/// * `group` - The consumer group ID
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `lag` - The calculated lag value
pub fn set_consumer_lag(group: &str, topic: &str, partition: i32, lag: i64) {
    if !group_label_under_cap(&*CONSUMER_LAG, group) {
        return;
    }
    let topic_label = bounded_topic_label(topic);
    CONSUMER_LAG
        .with_label_values(&[group, topic_label.as_str(), &partition.to_string()])
        .set(lag.max(0));
}

// --- Producer ID helper functions ---

/// Set the current producer ID counter value.
///
/// This tracks the last allocated producer ID to monitor capacity utilization.
/// Should be called after each producer ID allocation.
///
/// # Arguments
/// * `value` - The current producer ID counter value
pub fn set_producer_id_counter(value: i64) {
    PRODUCER_ID_COUNTER.set(value);
}

// --- Producer State Persistence helper functions ---

/// Record a producer state persistence failure.
///
/// Call this when persisting producer idempotency state to SlateDB fails.
/// This is a critical metric - non-zero values mean idempotency may not
/// survive restarts for affected producers.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
pub fn record_producer_state_persistence_failure(topic: &str, partition: i32) {
    let (topic_lbl, partition_lbl) = bounded_partition_label(topic, partition);
    PRODUCER_STATE_PERSISTENCE_FAILURES
        .with_label_values(&[topic_lbl.as_str(), partition_lbl.as_str()])
        .inc();
}

/// Set the number of producer states recovered on partition open.
///
/// Call this after loading producer states from SlateDB during partition
/// initialization. Helps monitor idempotency state cardinality.
///
/// # Arguments
/// * `topic` - The topic name
/// * `partition` - The partition ID
/// * `count` - Number of producer states recovered
pub fn set_producer_state_recovery_count(topic: &str, partition: i32, count: i64) {
    let (topic_lbl, partition_lbl) = bounded_partition_label(topic, partition);
    PRODUCER_STATE_RECOVERY_COUNT
        .with_label_values(&[topic_lbl.as_str(), partition_lbl.as_str()])
        .set(count);
}

// --- Proactive Sequence Number Monitoring ---

/// Maximum sequence number observed per producer.
///
/// Tracks the highest sequence number for idempotent producers. i32::MAX is the
/// limit, so alerts should trigger at 80% (1,717,986,918) and 90% (1,932,735,283).
///
/// **ALERT**: Set up alerts at these thresholds:
/// - Warning at 80%: sequence_high_watermark > 1717986918
/// - Critical at 90%: sequence_high_watermark > 1932735283
pub static SEQUENCE_HIGH_WATERMARK: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "producer_sequence_high_watermark",
        "Highest sequence number per producer (ALERT at 80% of i32::MAX)",
        &["topic", "partition", "producer_id"],
    )
});

/// Counter for sequence number warnings.
///
/// Incremented when a producer's sequence number exceeds warning thresholds.
/// Non-zero values indicate producers approaching sequence exhaustion.
pub static SEQUENCE_WARNINGS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "producer_sequence_warnings_total",
        "Sequence number threshold warnings",
        &["threshold"], // threshold = "80_pct" / "90_pct"
    )
});

/// Sequence number threshold constants.
/// 80% of i32::MAX = 0.8 * 2,147,483,647 = 1,717,986,918
const SEQUENCE_WARNING_80_PCT: i32 = 1_717_986_918;
/// 90% of i32::MAX = 0.9 * 2,147,483,647 = 1,932,735,283
const SEQUENCE_WARNING_90_PCT: i32 = 1_932_735_283;

/// Record sequence number and check for approaching exhaustion.
///
/// Call this after each successful produce with idempotent producer.
/// Returns true if sequence is approaching exhaustion (>80% of i32::MAX).
///
/// # Arguments
/// * `topic` - Topic name
/// * `partition` - Partition ID
/// * `producer_id` - Producer ID
/// * `sequence` - Current sequence number
///
/// # Returns
/// `true` if sequence exceeds 80% threshold (caller should warn client)
pub fn record_sequence_number(
    topic: &str,
    partition: i32,
    producer_id: i64,
    sequence: i32,
) -> bool {
    if !group_label_under_cap(&*SEQUENCE_HIGH_WATERMARK, &producer_id.to_string()) {
        return sequence > SEQUENCE_WARNING_80_PCT;
    }
    let topic_label = bounded_topic_label(topic);
    let partition_str = partition.to_string();
    let producer_str = producer_id.to_string();

    SEQUENCE_HIGH_WATERMARK
        .with_label_values(&[topic_label.as_str(), &partition_str, &producer_str])
        .set(sequence as i64);

    // Check thresholds
    if sequence >= SEQUENCE_WARNING_90_PCT {
        SEQUENCE_WARNINGS.with_label_values(&["90_pct"]).inc();
        tracing::error!(
            topic,
            partition,
            producer_id,
            sequence,
            max = i32::MAX,
            remaining = i32::MAX - sequence,
            "CRITICAL: Producer sequence number at 90% capacity! \
             Producer should get a new producer_id soon."
        );
        return true;
    } else if sequence >= SEQUENCE_WARNING_80_PCT {
        SEQUENCE_WARNINGS.with_label_values(&["80_pct"]).inc();
        tracing::warn!(
            topic,
            partition,
            producer_id,
            sequence,
            max = i32::MAX,
            remaining = i32::MAX - sequence,
            "Producer sequence number at 80% capacity - approaching exhaustion"
        );
        return true;
    }

    false
}

// =============================================================================
// Raft Consensus Metrics
// =============================================================================

/// Raft node state (-2=uninitialized, -1=shutdown, 0=follower, 1=candidate, 2=leader).
///
/// Seeded to [`RAFT_STATE_UNINITIALIZED`] so a broker that hasn't yet polled
/// openraft is **not** treated as a healthy follower by `/ready`. The first
/// metrics tick from the Raft coordinator overwrites this value.
pub static RAFT_STATE: Lazy<IntGauge> = Lazy::new(|| {
    let g = register_int_gauge_safe(
        &REGISTRY,
        "raft_state",
        "Current Raft node state (-2=uninitialized, -1=shutdown, 0=follower, 1=candidate, 2=leader)",
    );
    g.set(RAFT_STATE_UNINITIALIZED);
    g
});

/// Raft term counter.
pub static RAFT_TERM: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge_safe(&REGISTRY, "raft_term", "Current Raft term number"));

/// Raft commit index.
pub static RAFT_COMMIT_INDEX: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "raft_commit_index",
        "Current Raft commit index (last committed log entry)",
    )
});

/// Raft applied index.
pub static RAFT_APPLIED_INDEX: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "raft_applied_index",
        "Current Raft applied index (last applied to state machine)",
    )
});

/// Raft leader elections total.
pub static RAFT_ELECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "raft_elections_total",
        "Total Raft leader elections",
        &["result"], // result=won/lost/timeout
    )
});

/// Raft proposal latency histogram.
pub static RAFT_PROPOSAL_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "raft_proposal_duration_seconds",
        "Raft proposal (write) latency in seconds",
        &["status"], // status=success/error
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
    )
});

/// Raft snapshot operations.
pub static RAFT_SNAPSHOTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "raft_snapshots_total",
        "Total Raft snapshot operations",
        &["operation", "status"], // operation=create/install, status=success/error
    )
});

/// Raft log entries.
pub static RAFT_LOG_ENTRIES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "raft_log_entries",
        "Current number of entries in Raft log",
    )
});

/// Raft pending proposals for backpressure monitoring.
pub static RAFT_PENDING_PROPOSALS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "raft_pending_proposals",
        "Current number of pending Raft proposals",
    )
});

/// Raft proposal backpressure events.
pub static RAFT_BACKPRESSURE_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "raft_backpressure_events_total",
        "Total Raft proposal backpressure events",
        &["result"], // result=acquired/timeout/waiting
    )
});

/// Maximum observed clock skew per broker in milliseconds.
///
/// Clock skew is calculated as the difference between the leader's timestamp
/// and the broker's reported local timestamp when processing heartbeats.
/// High clock skew (>1s) can cause issues with lease management and
/// broker failure detection.
pub static CLOCK_SKEW_MS: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "clock_skew_ms",
        "Maximum observed clock skew per broker in milliseconds",
        &["broker_id"],
    )
});

/// Record clock skew for a broker.
///
/// This should be called after processing heartbeats to track clock drift
/// between brokers in the cluster.
pub fn record_clock_skew(broker_id: i32, skew_ms: i64) {
    CLOCK_SKEW_MS
        .with_label_values(&[&broker_id.to_string()])
        .set(skew_ms);
}

/// Set the current Raft state.
///
/// # Arguments
/// * `state` - The state: 0=follower, 1=candidate, 2=leader, -1=shutdown,
///   or [`RAFT_STATE_UNINITIALIZED`] before the Raft coordinator's first
///   metrics poll.
pub fn set_raft_state(state: i64) {
    RAFT_STATE.set(state);
}

/// Sentinel for "Raft has not reported a state yet". The Prometheus gauge
/// defaults to 0, which collides with the Follower state — so we publish
/// this distinct value at coordinator startup before the first poll lands,
/// and `/ready` treats it as "not ready" instead of "follower / OK".
pub const RAFT_STATE_UNINITIALIZED: i64 = -2;

/// Whether this broker's Raft node is currently ready to serve traffic.
///
/// Returns `true` for Follower (0) and Leader (2) — both states imply we
/// are participating in a quorum that has a leader. Candidate (1),
/// Shutdown (-1), and the pre-init sentinel (-2) all return `false`, so
/// Kubernetes will keep the pod out of rotation until the cluster reaches
/// consensus.
///
/// Learners are reported by the coordinator as Follower, so this returns
/// true for a learner; that matches openraft's "learner is a healthy
/// non-voting replica" model.
pub fn is_raft_ready() -> bool {
    matches!(RAFT_STATE.get(), 0 | 2)
}

/// Set the current Raft term.
pub fn set_raft_term(term: i64) {
    RAFT_TERM.set(term);
}

/// Set the current Raft commit index.
pub fn set_raft_commit_index(index: i64) {
    RAFT_COMMIT_INDEX.set(index);
}

/// Set the current Raft applied index.
pub fn set_raft_applied_index(index: i64) {
    RAFT_APPLIED_INDEX.set(index);
}

/// Record a Raft election event.
///
/// # Arguments
/// * `result` - The election result: "won", "lost", or "timeout"
pub fn record_raft_election(result: &str) {
    RAFT_ELECTIONS.with_label_values(&[result]).inc();
}

/// Record a Raft proposal latency.
///
/// # Arguments
/// * `status` - The proposal result: "success" or "error"
/// * `duration_secs` - The proposal duration in seconds
pub fn record_raft_proposal(status: &str, duration_secs: f64) {
    RAFT_PROPOSAL_DURATION
        .with_label_values(&[status])
        .observe(duration_secs);
}

/// Record a Raft snapshot operation.
///
/// # Arguments
/// * `operation` - The operation: "create" or "install"
/// * `status` - The result: "success" or "error"
pub fn record_raft_snapshot(operation: &str, status: &str) {
    RAFT_SNAPSHOTS.with_label_values(&[operation, status]).inc();
}

/// Set the current Raft log entry count.
pub fn set_raft_log_entries(count: i64) {
    RAFT_LOG_ENTRIES.set(count);
}

/// Set the current number of pending Raft proposals.
///
/// This should be called periodically to track backpressure state.
pub fn set_raft_pending_proposals(count: i64) {
    RAFT_PENDING_PROPOSALS.set(count);
}

/// Record a Raft backpressure event.
///
/// # Arguments
/// * `result` - The result: "acquired" (slot acquired immediately),
///   "waiting" (had to wait for slot), or "timeout" (failed to acquire)
pub fn record_raft_backpressure(result: &str) {
    RAFT_BACKPRESSURE_EVENTS.with_label_values(&[result]).inc();
}

// =============================================================================
// Broker Info Metrics
// =============================================================================

/// Broker info metric with static labels for identification.
/// This metric always has value 1 and provides broker metadata via labels.
pub static BROKER_INFO: Lazy<IntGaugeVec> = Lazy::new(|| {
    register_int_gauge_vec_safe(
        &REGISTRY,
        "broker_info",
        "Broker information (always 1, labels provide metadata)",
        &["broker_id", "host", "version"],
    )
});

/// Broker uptime in seconds (set once at startup, read from process start).
pub static BROKER_UPTIME: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "broker_uptime_seconds",
        "Broker uptime in seconds since start",
    )
});

/// Broker start timestamp (Unix epoch seconds).
static BROKER_START_TIME: Lazy<std::sync::atomic::AtomicI64> =
    Lazy::new(|| std::sync::atomic::AtomicI64::new(0));

/// Initialize broker info metrics.
///
/// Call this once at broker startup to set static broker information.
///
/// # Arguments
/// * `broker_id` - The broker ID
/// * `host` - The broker host address
/// * `version` - The broker version string
pub fn init_broker_info(broker_id: i32, host: &str, version: &str) {
    let broker_str = broker_id.to_string();
    BROKER_INFO
        .with_label_values(&[broker_str.as_str(), host, version])
        .set(1);

    // Record start time for uptime calculation
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0);
    BROKER_START_TIME.store(now, Ordering::SeqCst);
}

/// Update the broker uptime metric.
///
/// Call this periodically (e.g., every minute) to update the uptime gauge.
pub fn update_broker_uptime() {
    let start = BROKER_START_TIME.load(Ordering::SeqCst);
    if start > 0 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        BROKER_UPTIME.set(now - start);
    }
}

// =============================================================================
// Request/Response Size Metrics
// =============================================================================

/// Request size histogram by API type.
pub static REQUEST_SIZE: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "request_size_bytes",
        "Request payload size in bytes by API",
        &["api"],
        vec![
            100.0,
            500.0,
            1_000.0,
            5_000.0,
            10_000.0,
            50_000.0,
            100_000.0,
            500_000.0,
            1_000_000.0,
            10_000_000.0,
        ],
    )
});

/// Response size histogram by API type.
pub static RESPONSE_SIZE: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "response_size_bytes",
        "Response payload size in bytes by API",
        &["api"],
        vec![
            100.0,
            500.0,
            1_000.0,
            5_000.0,
            10_000.0,
            50_000.0,
            100_000.0,
            500_000.0,
            1_000_000.0,
            10_000_000.0,
        ],
    )
});

/// Record request size.
///
/// # Arguments
/// * `api` - The Kafka API name (e.g., "Produce", "Fetch")
/// * `size_bytes` - The request payload size in bytes
pub fn record_request_size(api: &str, size_bytes: usize) {
    REQUEST_SIZE
        .with_label_values(&[api])
        .observe(size_bytes as f64);
}

/// Record response size.
///
/// # Arguments
/// * `api` - The Kafka API name (e.g., "Produce", "Fetch")
/// * `size_bytes` - The response payload size in bytes
pub fn record_response_size(api: &str, size_bytes: usize) {
    RESPONSE_SIZE
        .with_label_values(&[api])
        .observe(size_bytes as f64);
}

/// Record both request size and response size for an API call.
///
/// Convenience function for recording both metrics together.
pub fn record_request_response_sizes(api: &str, request_bytes: usize, response_bytes: usize) {
    record_request_size(api, request_bytes);
    record_response_size(api, response_bytes);
}

// =============================================================================
// Object Store Metrics
// =============================================================================

/// Object store operation latency by operation type.
pub static OBJECT_STORE_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "object_store_duration_seconds",
        "Object store operation latency in seconds",
        &["operation", "status"], // operation=get/put/delete/list, status=success/error
        vec![
            0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
        ],
    )
});

/// Object store bytes transferred.
pub static OBJECT_STORE_BYTES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "object_store_bytes_total",
        "Total bytes transferred to/from object store",
        &["direction"], // direction=read/write
    )
});

/// Object store operation errors by error type.
pub static OBJECT_STORE_ERRORS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "object_store_errors_total",
        "Total object store errors by type",
        &["operation", "error_type"], // error_type=timeout/not_found/permission/network/other
    )
});

/// Record an object store operation.
///
/// # Arguments
/// * `operation` - The operation type: "get", "put", "delete", "list"
/// * `status` - The result: "success" or "error"
/// * `duration_secs` - The operation duration in seconds
pub fn record_object_store_operation(operation: &str, status: &str, duration_secs: f64) {
    OBJECT_STORE_DURATION
        .with_label_values(&[operation, status])
        .observe(duration_secs);
}

/// Record bytes transferred to/from object store.
///
/// # Arguments
/// * `direction` - Either "read" or "write"
/// * `bytes` - Number of bytes transferred
pub fn record_object_store_bytes(direction: &str, bytes: u64) {
    OBJECT_STORE_BYTES
        .with_label_values(&[direction])
        .inc_by(bytes);
}

/// Record an object store error.
///
/// # Arguments
/// * `operation` - The operation that failed
/// * `error_type` - The error category: "timeout", "not_found", "permission", "network", "other"
pub fn record_object_store_error(operation: &str, error_type: &str) {
    OBJECT_STORE_ERRORS
        .with_label_values(&[operation, error_type])
        .inc();
}

// =============================================================================
// Object Store Health Tracking
// =============================================================================
//
// These metrics track object store connectivity to detect partial network partitions
// where the broker can reach the Raft cluster but not the object store (or vice versa).

/// Consecutive object store operation failures.
/// Used to detect partial network partitions where broker can reach Raft but not storage.
pub static OBJECT_STORE_CONSECUTIVE_FAILURES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "object_store_consecutive_failures",
        "Number of consecutive object store operation failures (reset on success)",
    )
});

/// Timestamp of last successful object store operation.
pub static OBJECT_STORE_LAST_SUCCESS_TIMESTAMP: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "object_store_last_success_timestamp_seconds",
        "Unix timestamp of last successful object store operation",
    )
});

/// Threshold for consecutive failures before considering object store unhealthy.
/// If this threshold is exceeded, the broker should consider entering a degraded state.
const OBJECT_STORE_UNHEALTHY_THRESHOLD: i64 = 10;

/// Track object store health based on consecutive failures.
///
/// Call this after every object store operation to update health state.
/// On success: reset consecutive failures and update last success timestamp.
/// On failure: increment consecutive failures.
///
/// # Arguments
/// * `success` - Whether the operation succeeded
///
/// # Returns
/// Returns `true` if the object store is considered healthy, `false` if unhealthy.
pub fn track_object_store_health(success: bool) -> bool {
    use std::time::{SystemTime, UNIX_EPOCH};

    if success {
        OBJECT_STORE_CONSECUTIVE_FAILURES.set(0);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        OBJECT_STORE_LAST_SUCCESS_TIMESTAMP.set(now);
        true
    } else {
        let failures = OBJECT_STORE_CONSECUTIVE_FAILURES.get() + 1;
        OBJECT_STORE_CONSECUTIVE_FAILURES.set(failures);
        failures < OBJECT_STORE_UNHEALTHY_THRESHOLD
    }
}

/// Check if the object store is considered healthy.
///
/// Returns `false` if too many consecutive failures have occurred,
/// indicating a potential partial network partition where the broker
/// can reach the Raft cluster but not the object store.
pub fn is_object_store_healthy() -> bool {
    OBJECT_STORE_CONSECUTIVE_FAILURES.get() < OBJECT_STORE_UNHEALTHY_THRESHOLD
}

/// Get the number of consecutive object store failures.
///
/// Useful for logging and alerting on degraded state.
pub fn object_store_consecutive_failures() -> i64 {
    OBJECT_STORE_CONSECUTIVE_FAILURES.get()
}

// =============================================================================
// Network Metrics
// =============================================================================

/// Network bytes received total.
pub static NETWORK_BYTES_RECEIVED: Lazy<IntCounter> = Lazy::new(|| {
    let counter = prometheus::IntCounter::new(
        "network_bytes_received_total",
        "Total bytes received from clients",
    )
    .expect("metric should be valid");
    match REGISTRY.register(Box::new(counter.clone())) {
        Ok(()) => counter,
        Err(e) => {
            warn!(error = %e, "Failed to register network_bytes_received_total");
            counter
        }
    }
});

/// Network bytes sent total.
pub static NETWORK_BYTES_SENT: Lazy<IntCounter> = Lazy::new(|| {
    let counter =
        prometheus::IntCounter::new("network_bytes_sent_total", "Total bytes sent to clients")
            .expect("metric should be valid");
    match REGISTRY.register(Box::new(counter.clone())) {
        Ok(()) => counter,
        Err(e) => {
            warn!(error = %e, "Failed to register network_bytes_sent_total");
            counter
        }
    }
});

/// Record bytes received from network.
pub fn record_network_received(bytes: u64) {
    NETWORK_BYTES_RECEIVED.inc_by(bytes);
}

/// Record bytes sent to network.
pub fn record_network_sent(bytes: u64) {
    NETWORK_BYTES_SENT.inc_by(bytes);
}

// =============================================================================
// Clock Skew Monitoring Metrics
// =============================================================================

/// Heartbeat round-trip time histogram.
///
/// Tracks RTT for heartbeats between brokers. This helps detect network
/// latency issues that could affect lease timing and failure detection.
pub static HEARTBEAT_RTT: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "heartbeat_rtt_seconds",
        "Heartbeat round-trip time in seconds",
        &["target_broker"],
        vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ],
    )
});

/// Estimated clock skew between brokers (in seconds).
///
/// Positive values mean the remote broker's clock is ahead.
/// Negative values mean the remote broker's clock is behind.
///
/// **ALERT**: Set up alerts when |clock_skew| > 1 second. Clock skew
/// over 5 seconds can cause lease expiration issues and false positives
/// in failure detection.
pub static CLOCK_SKEW_ESTIMATE: Lazy<prometheus::GaugeVec> = Lazy::new(|| {
    let gauge = prometheus::GaugeVec::new(
        prometheus::Opts::new(
            "clock_skew_estimate_seconds",
            "Estimated clock skew with remote brokers (positive = remote ahead). ALERT when |skew| > 1s.",
        ),
        &["remote_broker"],
    )
    .expect("metric should be valid");
    match REGISTRY.register(Box::new(gauge.clone())) {
        Ok(()) => gauge,
        Err(e) => {
            warn!(error = %e, "Failed to register clock_skew_estimate_seconds");
            gauge
        }
    }
});

/// Maximum observed clock skew across all brokers.
///
/// This is the absolute value of the largest clock skew estimate.
/// Useful for quick alerting without needing to check all broker pairs.
pub static MAX_CLOCK_SKEW: Lazy<prometheus::Gauge> = Lazy::new(|| {
    let gauge = prometheus::Gauge::new(
        "max_clock_skew_seconds",
        "Maximum absolute clock skew observed across all broker pairs. ALERT when > 1s.",
    )
    .expect("metric should be valid");
    let _ = REGISTRY.register(Box::new(gauge.clone()));
    gauge
});

/// Counter for clock skew warnings.
///
/// Incremented when clock skew exceeds warning threshold (1 second).
pub static CLOCK_SKEW_WARNINGS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "clock_skew_warnings_total",
        "Total clock skew warning events (skew > 1s)",
        &["remote_broker", "severity"], // severity = warning (1-5s) / critical (>5s)
    )
});

/// Record heartbeat RTT and estimate clock skew.
///
/// This should be called after receiving a heartbeat response to track
/// network latency and detect clock drift.
///
/// # Arguments
/// * `target_broker` - The broker ID that was sent the heartbeat
/// * `rtt_secs` - The round-trip time in seconds
/// * `remote_timestamp` - The timestamp from the remote broker's response (if available)
/// * `local_send_time` - When we sent the heartbeat (local clock)
/// * `local_receive_time` - When we received the response (local clock)
pub fn record_heartbeat_rtt_with_skew(
    target_broker: i32,
    rtt_secs: f64,
    remote_timestamp: Option<u64>,
    local_send_time: u64,
    local_receive_time: u64,
) {
    let broker_str = target_broker.to_string();

    // Record RTT
    HEARTBEAT_RTT
        .with_label_values(&[broker_str.as_str()])
        .observe(rtt_secs);

    // Estimate clock skew if we have remote timestamp
    // Using NTP-style calculation: skew = remote_time - (local_send + local_receive) / 2
    if let Some(remote_ts) = remote_timestamp {
        let local_midpoint = (local_send_time + local_receive_time) / 2;
        let skew_ms = remote_ts as i64 - local_midpoint as i64;
        let skew_secs = skew_ms as f64 / 1000.0;

        CLOCK_SKEW_ESTIMATE
            .with_label_values(&[broker_str.as_str()])
            .set(skew_secs);

        // Update max clock skew
        let abs_skew = skew_secs.abs();
        let current_max = MAX_CLOCK_SKEW.get();
        if abs_skew > current_max {
            MAX_CLOCK_SKEW.set(abs_skew);
        }

        // Log warnings for significant clock skew
        const WARNING_THRESHOLD_SECS: f64 = 1.0;
        const CRITICAL_THRESHOLD_SECS: f64 = 5.0;

        if abs_skew > CRITICAL_THRESHOLD_SECS {
            CLOCK_SKEW_WARNINGS
                .with_label_values(&[broker_str.as_str(), "critical"])
                .inc();
            tracing::error!(
                target_broker,
                skew_secs = skew_secs,
                "CRITICAL: Clock skew > 5 seconds detected! This may cause split-brain scenarios. \
                 Verify NTP synchronization on both brokers."
            );
        } else if abs_skew > WARNING_THRESHOLD_SECS {
            CLOCK_SKEW_WARNINGS
                .with_label_values(&[broker_str.as_str(), "warning"])
                .inc();
            tracing::warn!(
                target_broker,
                skew_secs = skew_secs,
                "Clock skew > 1 second detected. Check NTP configuration."
            );
        }
    }
}

/// Record heartbeat RTT without clock skew estimation.
///
/// Use this simpler version when remote timestamp is not available.
pub fn record_heartbeat_rtt(target_broker: i32, rtt_secs: f64) {
    HEARTBEAT_RTT
        .with_label_values(&[&target_broker.to_string()])
        .observe(rtt_secs);
}

/// Get current clock skew estimate for a broker.
pub fn get_clock_skew(remote_broker: i32) -> f64 {
    CLOCK_SKEW_ESTIMATE
        .with_label_values(&[&remote_broker.to_string()])
        .get()
}

/// Get maximum observed clock skew.
pub fn get_max_clock_skew() -> f64 {
    MAX_CLOCK_SKEW.get()
}

#[cfg(test)]
#[path = "metrics_tests.rs"]
mod tests;
