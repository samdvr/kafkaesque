//! Safe Prometheus metric registration helpers and the custom registry.
//!
//! Registration errors are handled gracefully — if a metric fails to register,
//! a fallback unregistered metric is used instead of panicking.

use once_cell::sync::Lazy;
use prometheus::{
    HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Registry, opts,
};
use tracing::warn;

pub static REGISTRY: Lazy<Registry> = Lazy::new(|| {
    Registry::new_custom(Some("kafkaesque".to_string()), None).unwrap_or_else(|_| Registry::new())
});

// ============================================================================
// Safe metric registration helpers
// ============================================================================
//
// These functions register metrics to a custom registry and handle errors
// gracefully by returning fallback metrics instead of panicking.

/// Register an IntGauge safely, returning a fallback on error.
pub(crate) fn register_int_gauge_safe(registry: &Registry, name: &str, help: &str) -> IntGauge {
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
pub(crate) fn register_int_gauge_vec_safe(
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
pub(crate) fn register_int_counter_vec_safe(
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
pub(crate) fn register_int_counter_safe(registry: &Registry, name: &str, help: &str) -> IntCounter {
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
pub(crate) fn register_histogram_vec_safe(
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

