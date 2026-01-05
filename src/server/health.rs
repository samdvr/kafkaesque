//! HTTP health and readiness endpoints for operational monitoring.
//!
//! This module provides a lightweight HTTP server that exposes health endpoints
//! for integration with load balancers, Kubernetes, and monitoring systems.
//!
//! # Endpoints
//!
//! - `GET /health` - Liveness check (always returns 200 if server is running)
//! - `GET /ready` - Readiness check (returns 503 if in zombie mode)
//! - `GET /metrics` - Prometheus metrics in text format
//!
//! # Usage
//!
//! ```rust,no_run
//! use kafkaesque::server::health::HealthServer;
//! use std::sync::Arc;
//! use std::sync::atomic::AtomicBool;
//!
//! #[tokio::main]
//! async fn main() {
//!     let zombie_mode = Arc::new(AtomicBool::new(false));
//!     let server = HealthServer::new("0.0.0.0:8080", zombie_mode).await.unwrap();
//!     server.run().await.unwrap();
//! }
//! ```

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use crate::error::{Error, Result};

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
    /// Shared flag indicating whether the broker is in zombie mode.
    /// When true, readiness checks will fail.
    zombie_mode: Arc<AtomicBool>,
    /// Shutdown signal sender.
    shutdown_tx: broadcast::Sender<()>,
    /// Broker ID for identification in responses.
    broker_id: Option<i32>,
}

impl HealthServer {
    /// Create a new health server bound to the given address.
    ///
    /// # Arguments
    /// * `addr` - Address to bind to (e.g., "0.0.0.0:8080")
    /// * `zombie_mode` - Shared flag from PartitionManager indicating zombie mode
    pub async fn new(addr: &str, zombie_mode: Arc<AtomicBool>) -> Result<Self> {
        Self::with_broker_id(addr, zombie_mode, None).await
    }

    /// Create a new health server with broker ID for identification.
    ///
    /// # Arguments
    /// * `addr` - Address to bind to (e.g., "0.0.0.0:8080")
    /// * `zombie_mode` - Shared flag from PartitionManager indicating zombie mode
    /// * `broker_id` - Optional broker ID for identification in responses
    pub async fn with_broker_id(
        addr: &str,
        zombie_mode: Arc<AtomicBool>,
        broker_id: Option<i32>,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

        let (shutdown_tx, _) = broadcast::channel(1);

        info!(addr = %addr, "Health server listening");

        Ok(Self {
            listener,
            zombie_mode,
            shutdown_tx,
            broker_id,
        })
    }

    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| Error::IoError(e.kind()))
    }

    /// Initiate graceful shutdown.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        info!("Health server shutdown signal sent");
    }

    /// Check if the server is ready to accept traffic.
    ///
    /// Returns false if:
    /// - Broker is in zombie mode (lost cluster coordination)
    pub fn is_ready(&self) -> bool {
        !self.zombie_mode.load(Ordering::SeqCst)
    }

    /// Get the current health status.
    pub fn health_status(&self) -> HealthStatus {
        if self.zombie_mode.load(Ordering::SeqCst) {
            HealthStatus::NotReady
        } else {
            HealthStatus::Healthy
        }
    }

    /// Run the health server, handling HTTP requests.
    pub async fn run(&self) -> Result<()> {
        let mut shutdown_rx = self.shutdown_tx.subscribe();

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

                            let zombie_mode = self.zombie_mode.clone();
                            let broker_id = self.broker_id;

                            // Handle request in a separate task to avoid blocking
                            tokio::spawn(async move {
                                let mut buf = [0u8; 1024];
                                match stream.read(&mut buf).await {
                                    Ok(n) if n > 0 => {
                                        let request = String::from_utf8_lossy(&buf[..n]);
                                        let response = handle_request(&request, &zombie_mode, broker_id);
                                        if let Err(e) = stream.write_all(response.as_bytes()).await {
                                            debug!(error = ?e, "Failed to write health response");
                                        }
                                    }
                                    Ok(_) => {
                                        debug!("Empty request received");
                                    }
                                    Err(e) => {
                                        debug!(error = ?e, "Failed to read health request");
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
fn handle_request(request: &str, zombie_mode: &AtomicBool, broker_id: Option<i32>) -> String {
    // Parse the request line to get the path
    let first_line = request.lines().next().unwrap_or("");
    let parts: Vec<&str> = first_line.split_whitespace().collect();

    let path = if parts.len() >= 2 { parts[1] } else { "/" };

    match path {
        "/health" | "/healthz" | "/health/" => health_response(broker_id),
        "/ready" | "/readyz" | "/ready/" => ready_response(zombie_mode, broker_id),
        "/metrics" => metrics_response(),
        "/live" | "/livez" | "/live/" => liveness_response(broker_id),
        _ => not_found_response(),
    }
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

/// Generate health response (always 200 if server is running).
fn health_response(broker_id: Option<i32>) -> String {
    let broker_str = broker_id
        .map(|id| format!("broker_id: {}\n", id))
        .unwrap_or_default();

    format!(
        "HTTP/1.1 200 OK\r\n\
         Content-Type: text/plain\r\n\
         Connection: close\r\n\
         \r\n\
         status: healthy\n\
         {}",
        broker_str
    )
}

/// Generate readiness response (503 if in zombie mode).
fn ready_response(zombie_mode: &AtomicBool, broker_id: Option<i32>) -> String {
    let is_zombie = zombie_mode.load(Ordering::SeqCst);
    let broker_str = broker_id
        .map(|id| format!("broker_id: {}\n", id))
        .unwrap_or_default();

    if is_zombie {
        format!(
            "HTTP/1.1 503 Service Unavailable\r\n\
             Content-Type: text/plain\r\n\
             Connection: close\r\n\
             \r\n\
             status: not_ready\n\
             reason: zombie_mode\n\
             {}",
            broker_str
        )
    } else {
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
    use std::sync::atomic::AtomicBool;

    #[test]
    fn test_health_status_codes() {
        assert_eq!(HealthStatus::Healthy.http_status_code(), 200);
        assert_eq!(HealthStatus::NotReady.http_status_code(), 503);
        assert_eq!(HealthStatus::Unhealthy.http_status_code(), 500);
    }

    #[test]
    fn test_handle_health_request() {
        let zombie_mode = AtomicBool::new(false);
        let response = handle_request("GET /health HTTP/1.1\r\n", &zombie_mode, Some(1));
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: healthy"));
        assert!(response.contains("broker_id: 1"));
    }

    #[test]
    fn test_handle_ready_request_healthy() {
        let zombie_mode = AtomicBool::new(false);
        let response = handle_request("GET /ready HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: ready"));
    }

    #[test]
    fn test_handle_ready_request_zombie_mode() {
        let zombie_mode = AtomicBool::new(true);
        let response = handle_request("GET /ready HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("503 Service Unavailable"));
        assert!(response.contains("status: not_ready"));
        assert!(response.contains("reason: zombie_mode"));
    }

    #[test]
    fn test_handle_unknown_path() {
        let zombie_mode = AtomicBool::new(false);
        let response = handle_request("GET /unknown HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("404 Not Found"));
        assert!(response.contains("Available endpoints"));
    }

    #[test]
    fn test_handle_metrics_request() {
        let zombie_mode = AtomicBool::new(false);
        let response = handle_request("GET /metrics HTTP/1.1\r\n", &zombie_mode, None);
        // Should return 200 OK with metrics
        assert!(response.contains("200 OK"));
        assert!(response.contains("text/plain"));
    }

    #[test]
    fn test_kubernetes_style_paths() {
        let zombie_mode = AtomicBool::new(false);

        // Test /healthz (Kubernetes style)
        let response = handle_request("GET /healthz HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("200 OK"));

        // Test /readyz (Kubernetes style)
        let response = handle_request("GET /readyz HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("200 OK"));

        // Test /livez (Kubernetes style)
        let response = handle_request("GET /livez HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("200 OK"));
    }

    #[tokio::test]
    async fn test_health_server_creation() {
        let zombie_mode = Arc::new(AtomicBool::new(false));
        // Use a random high port to avoid permission issues
        match HealthServer::new("127.0.0.1:0", zombie_mode).await {
            Ok(server) => {
                let addr = server.local_addr().unwrap();
                assert!(addr.port() > 0);
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind (CI environments may have restrictions)
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_is_ready() {
        let zombie_mode = Arc::new(AtomicBool::new(false));
        match HealthServer::new("127.0.0.1:0", zombie_mode.clone()).await {
            Ok(server) => {
                assert!(server.is_ready());
                assert_eq!(server.health_status(), HealthStatus::Healthy);

                zombie_mode.store(true, Ordering::SeqCst);
                assert!(!server.is_ready());
                assert_eq!(server.health_status(), HealthStatus::NotReady);
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
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
        let response = health_response(Some(1));
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: healthy"));
        assert!(response.contains("broker_id: 1"));

        let response_no_broker = health_response(None);
        assert!(response_no_broker.contains("status: healthy"));
        assert!(!response_no_broker.contains("broker_id"));
    }

    #[test]
    fn test_ready_response_healthy() {
        let zombie_mode = AtomicBool::new(false);
        let response = ready_response(&zombie_mode, Some(5));
        assert!(response.contains("200 OK"));
        assert!(response.contains("status: ready"));
        assert!(response.contains("broker_id: 5"));
    }

    #[test]
    fn test_ready_response_zombie() {
        let zombie_mode = AtomicBool::new(true);
        let response = ready_response(&zombie_mode, Some(5));
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
        let zombie_mode = AtomicBool::new(false);

        // Test trailing slashes
        let response = handle_request("GET /health/ HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("200 OK"));

        let response = handle_request("GET /ready/ HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("200 OK"));

        let response = handle_request("GET /live/ HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("200 OK"));
    }

    #[test]
    fn test_handle_empty_request() {
        let zombie_mode = AtomicBool::new(false);
        let response = handle_request("", &zombie_mode, None);
        assert!(response.contains("404 Not Found"));
    }

    #[test]
    fn test_handle_malformed_request() {
        let zombie_mode = AtomicBool::new(false);
        let response = handle_request("INVALID", &zombie_mode, None);
        assert!(response.contains("404 Not Found"));
    }

    #[tokio::test]
    async fn test_health_server_with_broker_id() {
        let zombie_mode = Arc::new(AtomicBool::new(false));
        match HealthServer::with_broker_id("127.0.0.1:0", zombie_mode.clone(), Some(42)).await {
            Ok(server) => {
                assert!(server.is_ready());
                let addr = server.local_addr().unwrap();
                assert!(addr.port() > 0);
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_health_server_shutdown() {
        let zombie_mode = Arc::new(AtomicBool::new(false));
        match HealthServer::new("127.0.0.1:0", zombie_mode).await {
            Ok(server) => {
                // Shutdown should not panic
                server.shutdown();
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
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
        let zombie_mode = Arc::new(AtomicBool::new(false));
        match HealthServer::new("127.0.0.1:0", zombie_mode).await {
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
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
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
        let zombie_mode = AtomicBool::new(false);

        let health = handle_request("GET /health HTTP/1.1\r\n", &zombie_mode, None);
        assert!(health.contains("Content-Type: text/plain"));
        assert!(health.contains("Connection: close"));

        let ready = handle_request("GET /ready HTTP/1.1\r\n", &zombie_mode, None);
        assert!(ready.contains("Content-Type: text/plain"));
        assert!(ready.contains("Connection: close"));
    }

    // ========================================================================
    // HTTP Method Handling Tests
    // ========================================================================

    #[test]
    fn test_handle_head_request() {
        let zombie_mode = AtomicBool::new(false);
        // HEAD requests should work the same as GET for health endpoints
        let response = handle_request("HEAD /health HTTP/1.1\r\n", &zombie_mode, None);
        // We parse the path from the request line, HEAD works like GET
        assert!(response.contains("200 OK"));
    }

    #[test]
    fn test_handle_post_request() {
        let zombie_mode = AtomicBool::new(false);
        // POST to health endpoints still returns health (we only look at path)
        let response = handle_request("POST /health HTTP/1.1\r\n", &zombie_mode, None);
        assert!(response.contains("200 OK"));
    }
}
