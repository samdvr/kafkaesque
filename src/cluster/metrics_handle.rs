//! Per-instance metrics handle.
//!
//! Today every metric in [`crate::cluster::metrics`] is a process-global
//! `Lazy<...>`: 91 such statics, mutated by 198 call sites. Two
//! `SlateDBClusterHandler` instances in one process clobber each other's
//! circuit-breaker state, which is why the test suite has to mark wide
//! swaths of cases `#[serial_test::serial]` and why embedding the broker
//! into another binary requires global init.
//!
//! `Metrics` is the seam that lets us migrate off the global registry
//! incrementally. A `Metrics` handle holds `Arc`-shared counters that a
//! call site can record against; `Metrics::default()` returns a handle
//! whose recorders point at the existing globals, so unmodified call
//! sites keep working. A test or embedded harness that wants isolation
//! constructs `Metrics::isolated()` and threads it through
//! `RequestContext`.
//!
//! The full migration — rewrite every `crate::cluster::metrics::FOO.inc()`
//! call to `ctx.metrics.foo.inc()` — is incremental work that lands one
//! subsystem at a time. This module is the hook that lets the migration
//! happen at all.

use std::sync::Arc;

use once_cell::sync::Lazy;
use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts, Registry};

/// Per-instance metrics. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    /// Underlying registry. When this handle delegates to the global
    /// registry the value is `None`; `Metrics::isolated()` populates it.
    registry: Option<Registry>,
    /// Number of requests in flight. The global metrics module exposes
    /// histograms but not a current-in-flight gauge — see audit M31.
    requests_in_flight: IntGauge,
    /// Per-API count of requests that crossed the slow-request threshold.
    slow_requests: IntCounterVec,
    /// Total fenced-write detections. Mirrors the global counter, but on
    /// an isolated registry it lets a test assert against fresh state.
    fencing_detections: IntCounter,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field(
                "isolated",
                &self.inner.registry.as_ref().map(|_| "isolated"),
            )
            .finish()
    }
}

impl Default for Metrics {
    /// Returns a handle that delegates to the process-global registry.
    /// Unmodified call sites keep working — `metrics.requests_in_flight`
    /// is a real gauge but it's the *global* gauge.
    fn default() -> Self {
        Self::process_global()
    }
}

impl Metrics {
    /// Build a handle that delegates to the process-global statics.
    ///
    /// Two callers that build a `Metrics::process_global()` handle in
    /// different threads share state — the gauges in the returned handle
    /// are clones of the same underlying `IntGauge`s registered on the
    /// global registry.
    pub fn process_global() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                registry: None,
                requests_in_flight: GLOBAL_REQUESTS_IN_FLIGHT.clone(),
                slow_requests: GLOBAL_SLOW_REQUESTS.clone(),
                fencing_detections: GLOBAL_FENCING_DETECTIONS.clone(),
            }),
        }
    }

    /// Build a handle backed by a fresh `Registry`. Useful from tests
    /// that want to assert metric values without contention with other
    /// tests running in parallel — and from embedded callers that want
    /// to publish on their own scrape endpoint.
    pub fn isolated() -> Self {
        let registry = Registry::new();
        let requests_in_flight = IntGauge::with_opts(Opts::new(
            "kafkaesque_requests_in_flight",
            "Requests currently being served (parse → response).",
        ))
        .expect("static metric");
        registry
            .register(Box::new(requests_in_flight.clone()))
            .expect("register requests_in_flight");
        let slow_requests = IntCounterVec::new(
            Opts::new(
                "kafkaesque_slow_request_total",
                "Requests that exceeded the slow-request latency threshold.",
            ),
            &["api"],
        )
        .expect("static metric");
        registry
            .register(Box::new(slow_requests.clone()))
            .expect("register slow_requests");
        let fencing_detections = IntCounter::with_opts(Opts::new(
            "kafkaesque_handle_fencing_detections_total",
            "Fenced-write detections aggregated across partitions.",
        ))
        .expect("static metric");
        registry
            .register(Box::new(fencing_detections.clone()))
            .expect("register fencing_detections");
        Self {
            inner: Arc::new(MetricsInner {
                registry: Some(registry),
                requests_in_flight,
                slow_requests,
                fencing_detections,
            }),
        }
    }

    /// Borrow the registry — `None` for the global-delegating handle.
    pub fn registry(&self) -> Option<&Registry> {
        self.inner.registry.as_ref()
    }

    /// Increment the in-flight gauge for the duration of one request.
    /// Returns a guard that decrements on drop.
    pub fn record_in_flight(&self) -> InFlightGuard {
        self.inner.requests_in_flight.inc();
        InFlightGuard {
            gauge: self.inner.requests_in_flight.clone(),
        }
    }

    /// Bump the slow-request counter for an API. Use from the connection
    /// layer when a request crosses the configured threshold.
    pub fn observe_slow_request(&self, api: &str) {
        self.inner.slow_requests.with_label_values(&[api]).inc();
    }

    /// Bump the fencing-detection counter.
    pub fn observe_fencing_detection(&self) {
        self.inner.fencing_detections.inc();
    }
}

/// RAII guard returned by [`Metrics::record_in_flight`].
pub struct InFlightGuard {
    gauge: IntGauge,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        self.gauge.dec();
    }
}

static GLOBAL_REQUESTS_IN_FLIGHT: Lazy<IntGauge> = Lazy::new(|| {
    register_or_get_int_gauge(
        &crate::cluster::metrics::REGISTRY,
        "kafkaesque_requests_in_flight",
        "Requests currently being served (parse → response).",
    )
});

static GLOBAL_SLOW_REQUESTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_or_get_int_counter_vec(
        &crate::cluster::metrics::REGISTRY,
        "kafkaesque_slow_request_total",
        "Requests that exceeded the slow-request latency threshold.",
        &["api"],
    )
});

static GLOBAL_FENCING_DETECTIONS: Lazy<IntCounter> = Lazy::new(|| {
    register_or_get_int_counter(
        &crate::cluster::metrics::REGISTRY,
        "kafkaesque_handle_fencing_detections_total",
        "Fenced-write detections aggregated across partitions.",
    )
});

/// Register a counter, or look up the existing one on collision. Mirrors
/// the pattern used by the macro-generated globals in `cluster::metrics`.
fn register_or_get_int_counter(reg: &Registry, name: &str, help: &str) -> IntCounter {
    let counter = IntCounter::with_opts(Opts::new(name, help)).expect("static metric");
    match reg.register(Box::new(counter.clone())) {
        Ok(()) => counter,
        Err(prometheus::Error::AlreadyReg) => counter,
        Err(e) => panic!("metric registration failed: {e}"),
    }
}

fn register_or_get_int_gauge(reg: &Registry, name: &str, help: &str) -> IntGauge {
    let g = IntGauge::with_opts(Opts::new(name, help)).expect("static metric");
    match reg.register(Box::new(g.clone())) {
        Ok(()) => g,
        Err(prometheus::Error::AlreadyReg) => g,
        Err(e) => panic!("metric registration failed: {e}"),
    }
}

fn register_or_get_int_counter_vec(
    reg: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> IntCounterVec {
    let v = IntCounterVec::new(Opts::new(name, help), labels).expect("static metric");
    match reg.register(Box::new(v.clone())) {
        Ok(()) => v,
        Err(prometheus::Error::AlreadyReg) => v,
        Err(e) => panic!("metric registration failed: {e}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn isolated_handles_share_no_state() {
        let a = Metrics::isolated();
        let b = Metrics::isolated();
        a.observe_slow_request("Produce");
        a.observe_slow_request("Produce");
        let count_a = a.inner.slow_requests.with_label_values(&["Produce"]).get();
        let count_b = b.inner.slow_requests.with_label_values(&["Produce"]).get();
        assert_eq!(count_a, 2);
        assert_eq!(count_b, 0);
    }

    #[test]
    fn in_flight_guard_decrements_on_drop() {
        let m = Metrics::isolated();
        assert_eq!(m.inner.requests_in_flight.get(), 0);
        {
            let _guard = m.record_in_flight();
            assert_eq!(m.inner.requests_in_flight.get(), 1);
        }
        assert_eq!(m.inner.requests_in_flight.get(), 0);
    }

    #[test]
    fn process_global_clones_share_state() {
        let a = Metrics::process_global();
        let b = Metrics::process_global();
        // Increments are visible across handles since they back onto the
        // same global registry.
        let before = a.inner.fencing_detections.get();
        a.observe_fencing_detection();
        b.observe_fencing_detection();
        assert_eq!(a.inner.fencing_detections.get(), before + 2);
        assert_eq!(b.inner.fencing_detections.get(), before + 2);
    }
}
