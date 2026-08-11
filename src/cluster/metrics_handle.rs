//! Per-instance metrics handle with process-global or isolated backing.
//!
//! # Isolation model
//!
//! Every instrumented call site eventually goes through [`Metrics::current`]:
//! - Production: defaults to the process-global handle.
//! - Tests / embedding: install an [`Metrics::isolated`] handle via
//!   [`Metrics::scope_sync`] or [`Metrics::scope_async`] so circuit-breaker
//!   state, in-flight gauges, and fencing counters do not collide across
//!   parallel tests — eliminating the need for `#[serial]` on those paths.
//!
//! [`RequestContext::metrics`] carries the same handle into handlers so
//! call sites that already have a context can use `ctx.metrics` directly.

use std::cell::RefCell;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use once_cell::sync::Lazy;
use prometheus::{IntCounter, IntCounterVec, IntGauge, Opts, Registry};
use tokio::task_local;

use crate::cluster::metrics::REGISTRY;

task_local! {
    static TASK_METRICS: Metrics;
}

thread_local! {
    static SYNC_METRICS: RefCell<Option<Metrics>> = const { RefCell::new(None) };
}

/// Shared fencing circuit-breaker atomics (one per Metrics instance).
struct CircuitBreakerState {
    consecutive_fail_closed: AtomicU64,
    last_confirmed_fencing_millis: AtomicU64,
    consecutive_trips: AtomicU64,
    threshold: AtomicU64,
    base_reset_window_ms: AtomicU64,
    max_reset_window_ms: AtomicU64,
    state_gauge: IntGauge,
    trips: IntCounterVec,
}

impl CircuitBreakerState {
    fn global() -> Arc<Self> {
        static GLOBAL: OnceLock<Arc<CircuitBreakerState>> = OnceLock::new();
        GLOBAL
            .get_or_init(|| {
                Arc::new(Self {
                    consecutive_fail_closed: AtomicU64::new(0),
                    last_confirmed_fencing_millis: AtomicU64::new(0),
                    consecutive_trips: AtomicU64::new(0),
                    threshold: AtomicU64::new(5),
                    base_reset_window_ms: AtomicU64::new(60_000),
                    max_reset_window_ms: AtomicU64::new(300_000),
                    state_gauge: crate::cluster::metrics::CIRCUIT_BREAKER_STATE.clone(),
                    trips: crate::cluster::metrics::CIRCUIT_BREAKER_TRIPS.clone(),
                })
            })
            .clone()
    }

    fn isolated(registry: &Registry) -> Arc<Self> {
        let state_gauge = IntGauge::with_opts(Opts::new(
            "fencing_circuit_breaker_tripped",
            "Circuit breaker state for fail-closed fencing (0=normal, 1=tripped).",
        ))
        .expect("static metric");
        let _ = registry.register(Box::new(state_gauge.clone()));
        let trips = IntCounterVec::new(
            Opts::new(
                "fencing_circuit_breaker_trips_total",
                "Total circuit breaker trip events.",
            ),
            &["reason"],
        )
        .expect("static metric");
        let _ = registry.register(Box::new(trips.clone()));
        Arc::new(Self {
            consecutive_fail_closed: AtomicU64::new(0),
            last_confirmed_fencing_millis: AtomicU64::new(0),
            consecutive_trips: AtomicU64::new(0),
            threshold: AtomicU64::new(5),
            base_reset_window_ms: AtomicU64::new(60_000),
            max_reset_window_ms: AtomicU64::new(300_000),
            state_gauge,
            trips,
        })
    }

    fn configure(&self, threshold: u64, base_reset_window_ms: u64, max_reset_window_ms: u64) {
        self.threshold.store(threshold, Ordering::SeqCst);
        self.base_reset_window_ms
            .store(base_reset_window_ms, Ordering::SeqCst);
        self.max_reset_window_ms
            .store(max_reset_window_ms, Ordering::SeqCst);
    }

    fn reset(&self) {
        self.consecutive_fail_closed.store(0, Ordering::SeqCst);
        self.consecutive_trips.store(0, Ordering::SeqCst);
        self.state_gauge.set(0);
    }

    fn tripped(&self) -> bool {
        self.consecutive_fail_closed.load(Ordering::SeqCst) >= self.threshold.load(Ordering::SeqCst)
    }

    fn state(&self) -> (u64, bool) {
        let count = self.consecutive_fail_closed.load(Ordering::SeqCst);
        let tripped = count >= self.threshold.load(Ordering::SeqCst);
        (count, tripped)
    }

    /// Mirror of the former process-global circuit-breaker logic.
    fn record_fencing(&self, method: &str) -> bool {
        match method {
            "typed" | "pattern" => {
                let prev = self.consecutive_fail_closed.swap(0, Ordering::SeqCst);
                if prev >= self.threshold.load(Ordering::SeqCst) {
                    self.state_gauge.set(0);
                    self.consecutive_trips.store(0, Ordering::SeqCst);
                    tracing::info!(
                        previous_fail_closed_count = prev,
                        method,
                        "Circuit breaker RESET due to confirmed fencing"
                    );
                }
                if method == "typed" {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    self.last_confirmed_fencing_millis
                        .store(now, Ordering::SeqCst);
                }
                true
            }
            "fail_closed" => {
                let count = self.consecutive_fail_closed.fetch_add(1, Ordering::SeqCst) + 1;
                let threshold = self.threshold.load(Ordering::SeqCst);
                if count >= threshold {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    let last_confirmed = self.last_confirmed_fencing_millis.load(Ordering::SeqCst);
                    let trips = self.consecutive_trips.load(Ordering::SeqCst);
                    let backoff_multiplier = 1u64 << trips.min(4);
                    let reset_window = (self.base_reset_window_ms.load(Ordering::SeqCst)
                        * backoff_multiplier)
                        .min(self.max_reset_window_ms.load(Ordering::SeqCst));

                    if last_confirmed > 0 && now - last_confirmed < reset_window {
                        true
                    } else {
                        if count == threshold {
                            self.state_gauge.set(1);
                            self.trips
                                .with_label_values(&["consecutive_unknown_errors"])
                                .inc();
                            self.consecutive_trips.fetch_add(1, Ordering::SeqCst);
                        }
                        tracing::error!(
                            consecutive_fail_closed = count,
                            threshold,
                            reset_window_ms = reset_window,
                            "CRITICAL: Fencing circuit breaker TRIPPED"
                        );
                        false
                    }
                } else {
                    true
                }
            }
            _ => false,
        }
    }
}

/// Per-instance metrics. Cheap to clone (`Arc` inside).
#[derive(Clone)]
pub struct Metrics {
    inner: Arc<MetricsInner>,
}

struct MetricsInner {
    registry: Option<Registry>,
    requests_in_flight: IntGauge,
    slow_requests: IntCounterVec,
    fencing_detections: IntCounter,
    active_connections: IntGauge,
    total_connections: IntCounterVec,
    circuit_breaker: Arc<CircuitBreakerState>,
    /// When true, this handle owns fencing-detection counter increments
    /// (isolated). When false, delegates labeled fencing counters to the
    /// process-global `FENCING_DETECTIONS` vec.
    isolated: bool,
}

impl std::fmt::Debug for Metrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Metrics")
            .field("isolated", &self.inner.isolated)
            .finish()
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self::process_global()
    }
}

impl Metrics {
    /// Process-global handle (production default).
    pub fn process_global() -> Self {
        Self {
            inner: Arc::new(MetricsInner {
                registry: None,
                requests_in_flight: GLOBAL_REQUESTS_IN_FLIGHT.clone(),
                slow_requests: GLOBAL_SLOW_REQUESTS.clone(),
                fencing_detections: GLOBAL_FENCING_DETECTIONS.clone(),
                active_connections: crate::cluster::metrics::ACTIVE_CONNECTIONS.clone(),
                total_connections: crate::cluster::metrics::TOTAL_CONNECTIONS.clone(),
                circuit_breaker: CircuitBreakerState::global(),
                isolated: false,
            }),
        }
    }

    /// Fresh registry + circuit breaker — safe for parallel tests.
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
        let active_connections = IntGauge::with_opts(Opts::new(
            "active_connections",
            "Currently open client connections.",
        ))
        .expect("static metric");
        registry
            .register(Box::new(active_connections.clone()))
            .expect("register active_connections");
        let total_connections = IntCounterVec::new(
            Opts::new("connections_total", "Total client connections accepted."),
            &["status"],
        )
        .expect("static metric");
        registry
            .register(Box::new(total_connections.clone()))
            .expect("register total_connections");
        let circuit_breaker = CircuitBreakerState::isolated(&registry);
        Self {
            inner: Arc::new(MetricsInner {
                registry: Some(registry),
                requests_in_flight,
                slow_requests,
                fencing_detections,
                active_connections,
                total_connections,
                circuit_breaker,
                isolated: true,
            }),
        }
    }

    /// Resolve the metrics handle for this task/thread, else process-global.
    pub fn current() -> Self {
        if let Ok(m) = TASK_METRICS.try_with(|m| m.clone()) {
            return m;
        }
        SYNC_METRICS
            .with(|c| c.borrow().clone())
            .unwrap_or_else(Self::process_global)
    }

    /// Install `self` as [`Metrics::current`] for the duration of `f` (sync).
    pub fn scope_sync<R>(self, f: impl FnOnce() -> R) -> R {
        SYNC_METRICS.with(|c| {
            let prev = c.replace(Some(self));
            let out = f();
            *c.borrow_mut() = prev;
            out
        })
    }

    /// Install `self` as [`Metrics::current`] for the duration of an async future.
    pub async fn scope_async<F, T>(self, fut: F) -> T
    where
        F: Future<Output = T>,
    {
        TASK_METRICS.scope(self, fut).await
    }

    pub fn registry(&self) -> Option<&Registry> {
        self.inner.registry.as_ref()
    }

    pub fn record_in_flight(&self) -> InFlightGuard {
        self.inner.requests_in_flight.inc();
        InFlightGuard {
            gauge: self.inner.requests_in_flight.clone(),
        }
    }

    pub fn observe_slow_request(&self, api: &str) {
        self.inner.slow_requests.with_label_values(&[api]).inc();
    }

    pub fn observe_fencing_detection(&self) {
        self.inner.fencing_detections.inc();
    }

    pub fn connection_opened(&self, status: &str) {
        self.inner.active_connections.inc();
        self.inner
            .total_connections
            .with_label_values(&[status])
            .inc();
    }

    pub fn connection_closed(&self) {
        self.inner.active_connections.dec();
    }

    pub fn configure_circuit_breaker(
        &self,
        threshold: u64,
        base_reset_window_ms: u64,
        max_reset_window_ms: u64,
    ) {
        self.inner
            .circuit_breaker
            .configure(threshold, base_reset_window_ms, max_reset_window_ms);
    }

    pub fn reset_circuit_breaker(&self) {
        self.inner.circuit_breaker.reset();
    }

    pub fn circuit_breaker_tripped(&self) -> bool {
        self.inner.circuit_breaker.tripped()
    }

    pub fn circuit_breaker_state(&self) -> (u64, bool) {
        self.inner.circuit_breaker.state()
    }

    /// Record a fencing detection and update the circuit breaker.
    /// Returns whether the caller should treat the error as fencing.
    pub fn record_fencing_with_circuit_breaker(&self, method: &str) -> bool {
        if self.inner.isolated {
            self.inner.fencing_detections.inc();
        } else {
            crate::cluster::metrics::FENCING_DETECTIONS
                .with_label_values(&[method])
                .inc();
            self.inner.fencing_detections.inc();
        }
        self.inner.circuit_breaker.record_fencing(method)
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
        &REGISTRY,
        "kafkaesque_requests_in_flight",
        "Requests currently being served (parse → response).",
    )
});

static GLOBAL_SLOW_REQUESTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_or_get_int_counter_vec(
        &REGISTRY,
        "kafkaesque_slow_request_total",
        "Requests that exceeded the slow-request latency threshold.",
        &["api"],
    )
});

static GLOBAL_FENCING_DETECTIONS: Lazy<IntCounter> = Lazy::new(|| {
    register_or_get_int_counter(
        &REGISTRY,
        "kafkaesque_handle_fencing_detections_total",
        "Fenced-write detections aggregated across partitions.",
    )
});

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
        let before = a.inner.fencing_detections.get();
        a.observe_fencing_detection();
        b.observe_fencing_detection();
        assert_eq!(a.inner.fencing_detections.get(), before + 2);
        assert_eq!(b.inner.fencing_detections.get(), before + 2);
    }

    #[test]
    fn isolated_circuit_breakers_do_not_collide() {
        let a = Metrics::isolated();
        let b = Metrics::isolated();
        a.configure_circuit_breaker(2, 60_000, 300_000);
        // Trip a without touching b.
        assert!(a.record_fencing_with_circuit_breaker("fail_closed"));
        assert!(!a.record_fencing_with_circuit_breaker("fail_closed"));
        assert!(a.circuit_breaker_tripped());
        assert!(!b.circuit_breaker_tripped());
    }

    #[test]
    fn scope_sync_installs_current() {
        let m = Metrics::isolated();
        m.clone().scope_sync(|| {
            Metrics::current().observe_slow_request("Fetch");
            assert_eq!(
                Metrics::current()
                    .inner
                    .slow_requests
                    .with_label_values(&["Fetch"])
                    .get(),
                1
            );
        });
    }
}
