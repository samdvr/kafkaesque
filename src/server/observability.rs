//! Observability seam between the server module and the cluster runtime.
//!
//! `src/server` is intended to be a generic Kafka-protocol library; it
//! should be useable without dragging in SlateDB, openraft, or the
//! global metrics registry. Today the connection and health modules
//! reach into `crate::cluster::metrics` and `crate::cluster::raft` for
//! counters and readiness flags. That coupling blocks splitting the
//! server out into its own crate, and embedding the server twice in
//! the same process clobbers the global metrics registry.
//!
//! This trait is the seam. The server takes an `Arc<dyn
//! ServerObservability>` at construction time and routes every
//! observability call through it. The cluster runtime supplies a
//! [`ClusterObservability`] adapter that delegates to the existing
//! `cluster::metrics` functions; embedded users supply a [`NoopObservability`]
//! and pay nothing.

use std::sync::Arc;

use super::request::ApiKey;

/// Observability hooks the server emits during request handling.
///
/// All methods are no-ops by default so a custom impl can override only
/// what it cares about. The cluster runtime overrides every method to
/// route into Prometheus; tests and embedded users keep the defaults.
pub trait ServerObservability: Send + Sync + std::fmt::Debug {
    /// Called every time a TCP connection is accepted.
    fn inc_active_connections(&self) {}

    /// Called when a TCP connection closes (clean or error).
    fn dec_active_connections(&self) {}

    /// Called once per accepted connection with a status label
    /// (`"success"`, `"tls_handshake_failed"`, etc.). Distinct from
    /// `inc_active_connections` so cumulative SLOs and live gauges can
    /// move independently.
    fn record_connection_outcome(&self, _status: &'static str) {}

    /// Called after every request with the API name, status label,
    /// wire-level error_code label, and end-to-end duration.
    fn record_request(
        &self,
        _api: &'static str,
        _status: &'static str,
        _error_code: &'static str,
        _duration_secs: f64,
    ) {
    }

    /// Called when a request body has trailing bytes after the parser
    /// finished — a strong signal the client is desynced.
    fn record_trailing_bytes(&self, _api_key: ApiKey, _len: usize) {}

    /// Health probe: is the underlying object store accepting writes?
    /// Used by the readiness endpoint. Default `true` so a no-op impl
    /// looks healthy.
    fn is_object_store_healthy(&self) -> bool {
        true
    }

    /// Health probe: has Raft elected a leader and applied the latest
    /// entries within the configured freshness window?
    fn is_raft_ready(&self) -> bool {
        true
    }
}

/// No-op observability — every method is the trait's default. Cheap
/// to construct; useful for embedded tests, the doc example, and
/// custom servers that don't want the global Prometheus registry.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopObservability;

impl ServerObservability for NoopObservability {}

/// Convenience factory: shared no-op handle.
pub fn noop_observability() -> Arc<dyn ServerObservability> {
    Arc::new(NoopObservability)
}
