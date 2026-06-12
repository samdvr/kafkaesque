//! HTTP health and readiness endpoints for operational monitoring.
//!
//! This module provides a lightweight HTTP server that exposes health endpoints
//! for integration with load balancers, Kubernetes, and monitoring systems.
//!
//! # Endpoints
//!
//! - `GET /health` - Liveness check (always returns 200 if the server is running;
//!   the response body surfaces zombie status for visibility)
//! - `GET /ready` - Readiness check (returns 503 if in zombie mode)
//! - `GET /metrics` - Prometheus metrics in text format (optionally
//!   bearer-token-gated via [`HealthServer::with_metrics_auth`])
//!
//! # Usage
//!
//! ```rust,no_run
//! use kafkaesque::cluster::zombie_mode::ZombieModeState;
//! use kafkaesque::server::health::HealthServer;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() {
//!     let zombie_state = Arc::new(ZombieModeState::new());
//!     let server = HealthServer::new("0.0.0.0:8080", zombie_state).await.unwrap();
//!     server.run().await.unwrap();
//! }
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::{Semaphore, broadcast};
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::cluster::zombie_mode::ZombieModeState;
use crate::error::{Error, Result};

/// Read deadline for a single health-check request.
/// Health probes finish in milliseconds; anything longer is a slow-loris.
const HEALTH_READ_TIMEOUT: Duration = Duration::from_secs(2);

/// Write deadline for a single health-check response.
const HEALTH_WRITE_TIMEOUT: Duration = Duration::from_secs(2);

/// Maximum concurrent in-flight health-check connections. The server is
/// internal-facing so this only needs to absorb a probe storm; beyond this
/// we shed load (and an attacker can't pin all our memory).
const HEALTH_MAX_CONCURRENT: usize = 256;

/// Health check response status.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Service is healthy and ready.
    Healthy,
    /// Service is alive but not ready (e.g., in zombie mode).
    NotReady,
    /// Service is unhealthy.
    Unhealthy,
}

impl HealthStatus {
    /// HTTP status code for this health status.
    pub fn http_status_code(&self) -> u16 {
        match self {
            HealthStatus::Healthy => 200,
            HealthStatus::NotReady => 503,
            HealthStatus::Unhealthy => 500,
        }
    }

    /// HTTP status text for this health status.
    pub fn http_status_text(&self) -> &'static str {
        match self {
            HealthStatus::Healthy => "OK",
            HealthStatus::NotReady => "Service Unavailable",
            HealthStatus::Unhealthy => "Internal Server Error",
        }
    }
}

/// Lightweight HTTP server for health endpoints.
///
/// Runs alongside the main Kafka server to provide health/readiness checks
/// for load balancers and orchestration systems like Kubernetes.
pub struct HealthServer {
    listener: TcpListener,
    /// Shared zombie-mode state from `PartitionManager`. Probes read its
    /// `is_active()` directly; no polling mirror is involved, so a transition
    /// is visible on the next request rather than on the next tick.
    zombie_state: Arc<ZombieModeState>,
    /// Shutdown signal sender.
    shutdown_tx: broadcast::Sender<()>,
    /// Broker ID for identification in responses.
    broker_id: Option<i32>,
    /// Optional bearer token gating `/metrics`. When set, requests without a
    /// matching `Authorization: Bearer <token>` header receive 401. Liveness
    /// and readiness probes are intentionally never gated — load balancers
    /// can't be expected to carry credentials.
    metrics_auth_token: Option<Arc<str>>,
}

impl HealthServer {
    /// Create a new health server bound to the given address.
    ///
    /// # Arguments
    /// * `addr` - Address to bind to (e.g., "0.0.0.0:8080")
    /// * `zombie_state` - Shared state from `PartitionManager` indicating zombie mode
    pub async fn new(addr: &str, zombie_state: Arc<ZombieModeState>) -> Result<Self> {
        Self::with_broker_id(addr, zombie_state, None).await
    }

    /// Create a new health server with broker ID for identification.
    ///
    /// # Arguments
    /// * `addr` - Address to bind to (e.g., "0.0.0.0:8080")
    /// * `zombie_state` - Shared state from `PartitionManager` indicating zombie mode
    /// * `broker_id` - Optional broker ID for identification in responses
    pub async fn with_broker_id(
        addr: &str,
        zombie_state: Arc<ZombieModeState>,
        broker_id: Option<i32>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(Error::from)?;

        let (shutdown_tx, _) = broadcast::channel(1);

        info!(addr = %addr, "Health server listening");

        Ok(Self {
            listener,
            zombie_state,
            shutdown_tx,
            broker_id,
            metrics_auth_token: None,
        })
    }

    /// Require `Authorization: Bearer <token>` on `/metrics`. Pass `None` (or
    /// an empty string) to leave the endpoint open. Liveness and readiness
    /// remain unauthenticated regardless.
    pub fn with_metrics_auth(mut self, token: Option<String>) -> Self {
        self.metrics_auth_token = token
            .filter(|t| !t.is_empty())
            .map(|t| Arc::<str>::from(t.as_str()));
        self
    }

    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener.local_addr().map_err(Error::from)
    }

    /// Initiate graceful shutdown.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        info!("Health server shutdown signal sent");
    }

    /// Check if the server is ready to accept traffic.
    ///
    /// Returns false if any of the following gates fail — load balancers
    /// observing a `503` route around this broker until the underlying issue
    /// resolves itself. Each gate corresponds to a known operational failure
    /// mode that prior versions of `is_ready` *missed* (audit O-4):
    ///
    /// - **Zombie mode**: the broker has lost cluster coordination and writes
    ///   are rejected. Routing produces here would surface as `NotLeader`.
    /// - **Object store unreachable**: SlateDB / metadata operations are
    ///   timing out or failing in a row. Routing produces here would surface
    ///   as ack timeouts and silent durability gaps. The threshold is set on
    ///   `OBJECT_STORE_CONSECUTIVE_FAILURES` (default 10) — a single transient
    ///   500 doesn't flip readiness, but a sustained outage does.
    pub fn is_ready(&self) -> bool {
        if self.zombie_state.is_active() {
            return false;
        }
        if !crate::cluster::metrics::is_object_store_healthy() {
            return false;
        }
        true
    }

    /// Get the current health status.
    pub fn health_status(&self) -> HealthStatus {
        if !self.is_ready() {
            HealthStatus::NotReady
        } else {
            HealthStatus::Healthy
        }
    }

    /// Run the health server, handling HTTP requests.
    pub async fn run(&self) -> Result<()> {
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        // Bound the number of in-flight probes so a flood can't OOM us.
        // Probes that exceed the cap are dropped.
        let semaphore = Arc::new(Semaphore::new(HEALTH_MAX_CONCURRENT));

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    info!("Health server shutting down");
                    return Ok(());
                }
                accept_result = self.listener.accept() => {
                    match accept_result {
                        Ok((mut stream, addr)) => {
                            debug!(client_addr = %addr, "Health check connection");

                            let permit = match semaphore.clone().try_acquire_owned() {
                                Ok(p) => p,
                                Err(_) => {
                                    debug!(client_addr = %addr, "Health check rejected: too many concurrent probes");
                                    // Best-effort close; ignore errors.
                                    let _ = stream.shutdown().await;
                                    continue;
                                }
                            };

                            let zombie_state = self.zombie_state.clone();
                            let broker_id = self.broker_id;
                            let metrics_auth = self.metrics_auth_token.clone();

                            // Handle request in a separate task to avoid blocking
                            tokio::spawn(async move {
                                let _permit = permit;
                                // 1024 bytes truncated long Authorization
                                // headers (forwarded JWTs from a gateway easily
                                // exceed that), causing legitimate metrics
                                // requests to get a 401. 8 KiB is the bound
                                // most reverse proxies use for header size.
                                let mut buf = [0u8; 8192];
                                match timeout(HEALTH_READ_TIMEOUT, stream.read(&mut buf)).await {
                                    Ok(Ok(n)) if n > 0 => {
                                        let request = String::from_utf8_lossy(&buf[..n]);
                                        let response = handle_request(
                                            &request,
                                            &zombie_state,
                                            broker_id,
                                            metrics_auth.as_deref(),
                                        );
                                        if let Err(e) = timeout(
                                            HEALTH_WRITE_TIMEOUT,
                                            stream.write_all(response.as_bytes()),
                                        )
                                        .await
                                        {
                                            debug!(error = ?e, "Health response write timed out");
                                        }
                                    }
                                    Ok(Ok(_)) => {
                                        debug!("Empty request received");
                                    }
                                    Ok(Err(e)) => {
                                        debug!(error = ?e, "Failed to read health request");
                                    }
                                    Err(_) => {
                                        debug!(client_addr = %addr, "Health request read timed out");
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            warn!(error = ?e, "Failed to accept health check connection");
                        }
                    }
                }
            }
        }
    }
}

/// Handle an HTTP request and return the response.
fn handle_request(
    request: &str,
    zombie_state: &ZombieModeState,
    broker_id: Option<i32>,
    metrics_auth_token: Option<&str>,
) -> String {
    // Parse the request line to get the path
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();

    let path = if parts.len() >= 2 { parts[1] } else { "/" };

    match path {
        "/health" | "/healthz" | "/health/" => health_response(zombie_state, broker_id),
        "/ready" | "/readyz" | "/ready/" => ready_response(zombie_state, broker_id),
        "/metrics" => {
            if let Some(token) = metrics_auth_token
                && !request_has_bearer(request, token)
            {
                return unauthorized_response();
            }
            metrics_response()
        }
        "/live" | "/livez" | "/live/" => liveness_response(broker_id),
        _ => not_found_response(),
    }
}

/// Whether the request carries a matching `Authorization: Bearer <token>`
/// header. Header names are ASCII case-insensitive (RFC 7230 §3.2);
/// the token comparison is constant-time-ish in length but exact-match in
/// content — sufficient for an internal scrape token, not a user password.
fn request_has_bearer(request: &str, expected: &str) -> bool {
    for line in request.lines().skip(1) {
        if line.is_empty() {
            break;
        }
        let mut split = line.splitn(2, ':');
        let name = split.next().unwrap_or("").trim();
        let value = split.next().unwrap_or("").trim();
        if name.eq_ignore_ascii_case("authorization")
            && let Some(rest) = value
                .strip_prefix("Bearer ")
                .or_else(|| value.strip_prefix("bearer "))
        {
            // Constant-time compare: a timing-leaking byte comparison gives
            // an attacker a side channel to extract the token one byte at a
            // time over enough probes.
            use subtle::ConstantTimeEq;
            let provided = rest.trim().as_bytes();
            let expected_bytes = expected.as_bytes();
            return provided.ct_eq(expected_bytes).unwrap_u8() == 1;
        }
    }
    false
}

/// Generate liveness response (always 200 if server is running).
fn liveness_response(broker_id: Option<i32>) -> String {
    let broker_str = broker_id
        .map(|id| format!("broker_id: {}\n", id))
        .unwrap_or_default();

    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain\r\n\
         Connection: close\r\n\
         \r\n\
         status: alive\n\
         {}",
        broker_str
    )
}

/// Generate health response.
///
/// Always returns 200 — `/health` is the Kubernetes liveness signal and
/// failing it would have kubelet kill the pod even though zombie mode is
/// recoverable. We surface zombie status in the body so dashboards and
/// `curl` checks can see it without forcing a restart loop on the operator.
fn health_response(zombie_state: &ZombieModeState, broker_id: Option<i32>) -> String {
    let broker_str = broker_id
        .map(|id| format!("broker_id: {}\n", id))
        .unwrap_or_default();
    let zombie_str = if zombie_state.is_active() {
        "zombie_mode: active\n"
    } else {
        "zombie_mode: inactive\n"
    };

    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain\r\n\
         Connection: close\r\n\
         \r\n\
         status: healthy\n\
         {}{}",
        zombie_str, broker_str
    )
}

/// Generate readiness response (503 if not ready).
///
/// Returns 503 with a `reason: <gate>` body line for any failed gate so
/// operators can disambiguate "broker is in zombie mode" from "broker can't
/// reach S3" without consulting metrics.
fn ready_response(zombie_state: &ZombieModeState, broker_id: Option<i32>) -> String {
    let broker_str = broker_id
        .map(|id| format!("broker_id: {}\n", id))
        .unwrap_or_default();

    let zombie_active = zombie_state.is_active();
    let object_store_healthy = crate::cluster::metrics::is_object_store_healthy();
    let consecutive_failures = crate::cluster::metrics::object_store_consecutive_failures();

    if zombie_active {
        return format!(
            "HTTP/1.1 503 Service Unavailable\r\n\
             Content-Type: text/plain\r\n\
             Connection: close\r\n\
             \r\n\
             status: not_ready\n\
             reason: zombie_mode\n\
             {}",
            broker_str
        );
    }

    if !object_store_healthy {
        return format!(
            "HTTP/1.1 503 Service Unavailable\r\n\
             Content-Type: text/plain\r\n\
             Connection: close\r\n\
             \r\n\
             status: not_ready\n\
             reason: object_store_unhealthy\n\
             object_store_consecutive_failures: {}\n\
             {}",
            consecutive_failures, broker_str
        );
    }

    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain\r\n\
         Connection: close\r\n\
         \r\n\
         status: ready\n\
         {}",
        broker_str
    )
}

/// Generate 401 response for unauthenticated `/metrics` access when
/// `metrics_auth_token` is configured.
fn unauthorized_response() -> String {
    "HTTP/1.1 401 Unauthorized\r\n\
     Content-Type: text/plain\r\n\
     WWW-Authenticate: Bearer realm=\"metrics\"\r\n\
     Connection: close\r\n\
     \r\n\
     error: missing or invalid bearer token\n"
        .to_string()
}

/// Generate Prometheus metrics response.
fn metrics_response() -> String {
    use prometheus::{Encoder, TextEncoder};

    let encoder = TextEncoder::new();
    let metric_families = crate::cluster::metrics::gather_metrics();
    let mut buffer = Vec::new();

    match encoder.encode(&metric_families, &mut buffer) {
        Ok(_) => {
            let body = String::from_utf8_lossy(&buffer);
            format!(
                "HTTP/1.1 200 OK\r\n\
                 Content-Type: text/plain; version=0.0.4; charset=utf-8\r\n\
                 Connection: close\r\n\
                 \r\n\
                 {}",
                body
            )
        }
        Err(e) => {
            error!(error = ?e, "Failed to encode metrics");
            "HTTP/1.1 500 Internal Server Error\r\n\
             Content-Type: text/plain\r\n\
             Connection: close\r\n\
             \r\n\
             error: failed to encode metrics\n"
                .to_string()
        }
    }
}

/// Generate 404 response for unknown paths.
fn not_found_response() -> String {
    "HTTP/1.1 404 Not Found\r\n\
     Content-Type: text/plain\r\n\
     Connection: close\r\n\
     \r\n\
     Available endpoints:\n\
     - /health - Liveness check\n\
     - /ready - Readiness check (fails in zombie mode)\n\
     - /metrics - Prometheus metrics\n\
     - /live - Alias for liveness check\n"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn healthy() -> ZombieModeState {
        ZombieModeState::new()
    }

    fn zombie() -> ZombieModeState {
        let s = ZombieModeState::new();
        s.enter();
        s
    }

    #[test]
    fn test_health_status_codes() {
        assert_eq!(HealthStatus::Healthy.http_status_code(), 200);
        assert_eq!(HealthStatus::NotReady.http_status_code(), 503);
        assert_eq!(HealthStatus::Unhealthy.http_status_code(), 500);
    }

    #[test]
    fn test_handle_health_request() {
        let state = healthy();
        let response = handle_request("GET /health HTTP/1.1\r\n", &state, Some(1), None);
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: healthy"));
        assert!(response.contains("zombie_mode: inactive"));
        assert!(response.contains("broker_id: 1"));
    }

    #[test]
    fn test_handle_health_request_zombie() {
        let state = zombie();
        let response = handle_request("GET /health HTTP/1.1\r\n", &state, Some(1), None);
        // /health stays 200 even in zombie mode (Kubernetes liveness),
        // but the body surfaces the zombie state for visibility.
        assert!(response.contains("200 OK"));
        assert!(response.contains("zombie_mode: active"));
    }

    #[test]
    fn test_handle_ready_request_healthy() {
        let state = healthy();
        let response = handle_request("GET /ready HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: ready"));
    }

    #[test]
    fn test_handle_ready_request_zombie_mode() {
        let state = zombie();
        let response = handle_request("GET /ready HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("503 Service Unavailable"));
        assert!(response.contains("status: not_ready"));
        assert!(response.contains("reason: zombie_mode"));
    }

    #[test]
    fn test_handle_unknown_path() {
        let state = healthy();
        let response = handle_request("GET /unknown HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("404 Not Found"));
        assert!(response.contains("Available endpoints"));
    }

    #[test]
    fn test_handle_metrics_request() {
        let state = healthy();
        let response = handle_request("GET /metrics HTTP/1.1\r\n", &state, None, None);
        // Should return 200 OK with metrics
        assert!(response.contains("200 OK"));
        assert!(response.contains("text/plain"));
    }

    #[test]
    fn test_metrics_auth_missing_token() {
        let state = healthy();
        let response = handle_request(
            "GET /metrics HTTP/1.1\r\n\r\n",
            &state,
            None,
            Some("secret-token"),
        );
        assert!(response.contains("401 Unauthorized"));
        assert!(response.contains("WWW-Authenticate: Bearer"));
    }

    #[test]
    fn test_metrics_auth_wrong_token() {
        let state = healthy();
        let response = handle_request(
            "GET /metrics HTTP/1.1\r\nAuthorization: Bearer wrong-token\r\n\r\n",
            &state,
            None,
            Some("secret-token"),
        );
        assert!(response.contains("401 Unauthorized"));
    }

    #[test]
    fn test_metrics_auth_correct_token() {
        let state = healthy();
        let response = handle_request(
            "GET /metrics HTTP/1.1\r\nAuthorization: Bearer secret-token\r\n\r\n",
            &state,
            None,
            Some("secret-token"),
        );
        assert!(response.contains("200 OK"));
        assert!(!response.contains("401"));
    }

    #[test]
    fn test_metrics_auth_header_case_insensitive() {
        let state = healthy();
        // Header name is case-insensitive per RFC 7230.
        let response = handle_request(
            "GET /metrics HTTP/1.1\r\nauthorization: Bearer secret-token\r\n\r\n",
            &state,
            None,
            Some("secret-token"),
        );
        assert!(response.contains("200 OK"));
    }

    #[test]
    fn test_metrics_auth_does_not_gate_health_or_ready() {
        let state = healthy();
        // Even with a token configured, liveness/readiness must remain open
        // for load balancers that can't carry credentials.
        let h = handle_request("GET /health HTTP/1.1\r\n", &state, None, Some("secret"));
        assert!(h.contains("200 OK"));
        let r = handle_request("GET /ready HTTP/1.1\r\n", &state, None, Some("secret"));
        assert!(r.contains("200 OK"));
    }

    #[test]
    fn test_kubernetes_style_paths() {
        let state = healthy();

        // Test /healthz (Kubernetes style)
        let response = handle_request("GET /healthz HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("200 OK"));

        // Test /readyz (Kubernetes style)
        let response = handle_request("GET /readyz HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("200 OK"));

        // Test /livez (Kubernetes style)
        let response = handle_request("GET /livez HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("200 OK"));
    }

    #[tokio::test]
    async fn test_health_server_creation() {
        let state = Arc::new(ZombieModeState::new());
        // Use a random high port to avoid permission issues
        match HealthServer::new("127.0.0.1:0", state).await {
            Ok(server) => {
                let addr = server.local_addr().unwrap();
                assert!(addr.port() > 0);
            }
            Err(crate::error::Error::IoError(crate::error::PreservedIoError {
                kind: std::io::ErrorKind::PermissionDenied,
                ..
            })) => {
                // Skip test if we can't bind (CI environments may have restrictions)
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_is_ready() {
        let state = Arc::new(ZombieModeState::new());
        match HealthServer::new("127.0.0.1:0", state.clone()).await {
            Ok(server) => {
                assert!(server.is_ready());
                assert_eq!(server.health_status(), HealthStatus::Healthy);

                state.enter();
                assert!(!server.is_ready());
                assert_eq!(server.health_status(), HealthStatus::NotReady);
            }
            Err(crate::error::Error::IoError(crate::error::PreservedIoError {
                kind: std::io::ErrorKind::PermissionDenied,
                ..
            })) => {
                // Skip test if we can't bind (CI environments may have restrictions)
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[test]
    fn test_health_status_text() {
        assert_eq!(HealthStatus::Healthy.http_status_text(), "OK");
        assert_eq!(
            HealthStatus::NotReady.http_status_text(),
            "Service Unavailable"
        );
        assert_eq!(
            HealthStatus::Unhealthy.http_status_text(),
            "Internal Server Error"
        );
    }

    #[test]
    fn test_liveness_response() {
        let response = liveness_response(Some(42));
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: alive"));
        assert!(response.contains("broker_id: 42"));

        let response_no_broker = liveness_response(None);
        assert!(response_no_broker.contains("status: alive"));
        assert!(!response_no_broker.contains("broker_id"));
    }

    #[test]
    fn test_health_response() {
        let state = healthy();
        let response = health_response(&state, Some(1));
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: healthy"));
        assert!(response.contains("zombie_mode: inactive"));
        assert!(response.contains("broker_id: 1"));

        let response_no_broker = health_response(&state, None);
        assert!(response_no_broker.contains("status: healthy"));
        assert!(!response_no_broker.contains("broker_id"));
    }

    #[test]
    fn test_ready_response_healthy() {
        let state = healthy();
        let response = ready_response(&state, Some(5));
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: ready"));
        assert!(response.contains("broker_id: 5"));
    }

    #[test]
    fn test_ready_response_zombie() {
        let state = zombie();
        let response = ready_response(&state, Some(5));
        assert!(response.contains("503 Service Unavailable"));
        assert!(response.contains("status: not_ready"));
        assert!(response.contains("reason: zombie_mode"));
    }

    #[test]
    fn test_not_found_response() {
        let response = not_found_response();
        assert!(response.contains("404 Not Found"));
        assert!(response.contains("/health"));
        assert!(response.contains("/ready"));
        assert!(response.contains("/metrics"));
    }

    #[test]
    fn test_handle_trailing_slash_paths() {
        let state = healthy();

        // Test trailing slashes
        let response = handle_request("GET /health/ HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("200 OK"));

        let response = handle_request("GET /ready/ HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("200 OK"));

        let response = handle_request("GET /live/ HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("200 OK"));
    }

    #[test]
    fn test_handle_empty_request() {
        let state = healthy();
        let response = handle_request("", &state, None, None);
        assert!(response.contains("404 Not Found"));
    }

    #[test]
    fn test_handle_malformed_request() {
        let state = healthy();
        let response = handle_request("INVALID", &state, None, None);
        assert!(response.contains("404 Not Found"));
    }

    #[tokio::test]
    async fn test_health_server_with_broker_id() {
        let state = Arc::new(ZombieModeState::new());
        match HealthServer::with_broker_id("127.0.0.1:0", state.clone(), Some(42)).await {
            Ok(server) => {
                assert!(server.is_ready());
                let addr = server.local_addr().unwrap();
                assert!(addr.port() > 0);
            }
            Err(crate::error::Error::IoError(crate::error::PreservedIoError {
                kind: std::io::ErrorKind::PermissionDenied,
                ..
            })) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_health_server_shutdown() {
        let state = Arc::new(ZombieModeState::new());
        match HealthServer::new("127.0.0.1:0", state).await {
            Ok(server) => {
                // Shutdown should not panic
                server.shutdown();
            }
            Err(crate::error::Error::IoError(crate::error::PreservedIoError {
                kind: std::io::ErrorKind::PermissionDenied,
                ..
            })) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    // ========================================================================
    // HealthStatus Trait Tests
    // ========================================================================

    #[test]
    fn test_health_status_debug() {
        assert!(format!("{:?}", HealthStatus::Healthy).contains("Healthy"));
        assert!(format!("{:?}", HealthStatus::NotReady).contains("NotReady"));
        assert!(format!("{:?}", HealthStatus::Unhealthy).contains("Unhealthy"));
    }

    #[test]
    fn test_health_status_clone() {
        let status = HealthStatus::Healthy;
        let cloned = status;
        assert_eq!(status, cloned);
    }

    #[test]
    fn test_health_status_copy() {
        let status = HealthStatus::NotReady;
        let copied = status;
        assert_eq!(status, copied);
    }

    #[test]
    fn test_health_status_eq() {
        assert_eq!(HealthStatus::Healthy, HealthStatus::Healthy);
        assert_eq!(HealthStatus::NotReady, HealthStatus::NotReady);
        assert_eq!(HealthStatus::Unhealthy, HealthStatus::Unhealthy);

        assert_ne!(HealthStatus::Healthy, HealthStatus::NotReady);
        assert_ne!(HealthStatus::Healthy, HealthStatus::Unhealthy);
        assert_ne!(HealthStatus::NotReady, HealthStatus::Unhealthy);
    }

    // ========================================================================
    // Server Run with Shutdown Tests
    // ========================================================================

    #[tokio::test]
    async fn test_health_server_run_shutdown() {
        let state = Arc::new(ZombieModeState::new());
        match HealthServer::new("127.0.0.1:0", state).await {
            Ok(server) => {
                // Start run in background
                let server = Arc::new(server);
                let server_clone = server.clone();

                let handle = tokio::spawn(async move { server_clone.run().await });

                // Give server time to start
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;

                // Shutdown
                server.shutdown();

                // Wait for server to exit
                let result = handle.await.unwrap();
                assert!(result.is_ok());
            }
            Err(crate::error::Error::IoError(crate::error::PreservedIoError {
                kind: std::io::ErrorKind::PermissionDenied,
                ..
            })) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    // ========================================================================
    // Response Content Tests
    // ========================================================================

    #[test]
    fn test_metrics_response_format() {
        let response = metrics_response();
        // Should contain HTTP headers
        assert!(response.contains("HTTP/1.1"));
        assert!(response.contains("Content-Type: text/plain"));
        assert!(response.contains("Connection: close"));
    }

    #[test]
    fn test_response_content_type_header() {
        let state = healthy();

        let health = handle_request("GET /health HTTP/1.1\r\n", &state, None, None);
        assert!(health.contains("Content-Type: text/plain"));
        assert!(health.contains("Connection: close"));

        let ready = handle_request("GET /ready HTTP/1.1\r\n", &state, None, None);
        assert!(ready.contains("Content-Type: text/plain"));
        assert!(ready.contains("Connection: close"));
    }

    // ========================================================================
    // HTTP Method Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_head_request() {
        let state = healthy();
        // HEAD requests should work the same as GET for health endpoints
        let response = handle_request("HEAD /health HTTP/1.1\r\n", &state, None, None);
        // We parse the path from the request line, HEAD works like GET
        assert!(response.contains("200 OK"));
    }

    #[test]
    fn test_handle_post_request() {
        let state = healthy();
        // POST to health endpoints still returns health (we only look at path)
        let response = handle_request("POST /health HTTP/1.1\r\n", &state, None, None);
        assert!(response.contains("200 OK"));
    }
}
