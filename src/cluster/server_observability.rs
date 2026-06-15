//! Adapter that bridges the server's `ServerObservability` trait to the
//! cluster runtime's global Prometheus registry.
//!
//! The server module is generic over `dyn ServerObservability`; the
//! kafkaesque cluster runtime supplies this adapter to keep the
//! existing `cluster::metrics::*` call sites unchanged while the
//! server stops reaching directly into them.

use std::sync::Arc;

use crate::server::observability::{NoopObservability, ServerObservability};
use crate::server::request::ApiKey;

use super::metrics;

/// Routes every server-emitted observability call into the cluster's
/// global metrics registry. Construction is free; the type is
/// zero-sized.
#[derive(Debug, Default, Clone, Copy)]
pub struct ClusterObservability;

impl ClusterObservability {
    /// Build a shared handle suitable for handing to `KafkaServer::new`.
    pub fn shared() -> Arc<dyn ServerObservability> {
        Arc::new(Self)
    }
}

impl ServerObservability for ClusterObservability {
    fn inc_active_connections(&self) {
        metrics::ACTIVE_CONNECTIONS.inc();
    }

    fn dec_active_connections(&self) {
        metrics::ACTIVE_CONNECTIONS.dec();
    }

    fn record_connection_outcome(&self, status: &'static str) {
        metrics::TOTAL_CONNECTIONS
            .with_label_values(&[status])
            .inc();
    }

    fn record_request(
        &self,
        api: &'static str,
        status: &'static str,
        error_code: &'static str,
        duration_secs: f64,
    ) {
        metrics::record_request_with_code(api, status, error_code, duration_secs);
    }

    fn record_trailing_bytes(&self, api_key: ApiKey, len: usize) {
        metrics::record_request_trailing_bytes(api_key.as_str(), len);
    }

    fn is_object_store_healthy(&self) -> bool {
        metrics::is_object_store_healthy()
    }

    fn is_raft_ready(&self) -> bool {
        metrics::is_raft_ready()
    }
}

/// Re-export of the no-op impl so callers can pick "no observability"
/// without hopping through the server module.
pub type ClusterNoopObservability = NoopObservability;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::observability::ServerObservability;

    #[test]
    fn cluster_observability_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<ClusterObservability>();
        assert_send_sync::<NoopObservability>();
    }

    #[test]
    fn cluster_adapter_can_be_used_as_trait_object() {
        let _: Arc<dyn ServerObservability> = ClusterObservability::shared();
    }

    #[test]
    fn noop_returns_healthy_defaults() {
        let obs = NoopObservability;
        assert!(obs.is_object_store_healthy());
        assert!(obs.is_raft_ready());
        // No-op methods should not panic.
        obs.inc_active_connections();
        obs.dec_active_connections();
        obs.record_connection_outcome("success");
        obs.record_request("Produce", "success", "NONE", 0.001);
    }
}
