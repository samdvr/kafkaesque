//! Fencing circuit breaker for fail-closed unknown-error handling.
//!
//! State lives on [`crate::cluster::Metrics`] so tests can install an
//! isolated handle via [`Metrics::scope_sync`] / [`Metrics::scope_async`]
//! instead of serializing every fencing test. The Prometheus gauges below
//! remain process-global for the production scrape endpoint; isolated
//! handles register their own copies.

use once_cell::sync::Lazy;
use prometheus::{IntCounterVec, IntGauge};

use super::REGISTRY;
use super::registration::{register_int_counter_vec_safe, register_int_gauge_safe};
use crate::cluster::Metrics;

/// Circuit breaker state gauge (0 = normal, 1 = tripped).
///
/// **ALERT**: Set up alerts when this metric is 1.
pub static CIRCUIT_BREAKER_STATE: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "fencing_circuit_breaker_tripped",
        "Circuit breaker state for fail-closed fencing (0=normal, 1=tripped). ALERT when 1.",
    )
});

/// Circuit breaker trip counter.
///
/// **ALERT**: Set up alerts when this counter increases.
pub static CIRCUIT_BREAKER_TRIPS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "fencing_circuit_breaker_trips_total",
        "Total circuit breaker trip events. ALERT on any increase.",
        &["reason"],
    )
});

/// Initialize circuit breaker configuration from ClusterConfig.
///
/// Applies to the process-global [`Metrics`] handle (and therefore to
/// production). Isolated test handles configure themselves via
/// [`Metrics::configure_circuit_breaker`].
pub fn init_circuit_breaker_config(
    threshold: u64,
    base_reset_window_ms: u64,
    max_reset_window_ms: u64,
) {
    Metrics::process_global().configure_circuit_breaker(
        threshold,
        base_reset_window_ms,
        max_reset_window_ms,
    );
}

/// Check if the fail-closed circuit breaker has tripped.
pub fn fail_closed_circuit_breaker_tripped() -> bool {
    Metrics::current().circuit_breaker_tripped()
}

/// Record a fencing detection event and update circuit breaker state.
///
/// Returns whether the caller should treat the error as fencing.
pub fn record_fencing_detection_with_circuit_breaker(method: &str) -> bool {
    Metrics::current().record_fencing_with_circuit_breaker(method)
}

/// Get current circuit breaker state for monitoring.
pub fn get_circuit_breaker_state() -> (u64, bool) {
    Metrics::current().circuit_breaker_state()
}

/// Reset the circuit breaker (for testing or manual intervention).
pub fn reset_circuit_breaker() {
    Metrics::current().reset_circuit_breaker();
}
