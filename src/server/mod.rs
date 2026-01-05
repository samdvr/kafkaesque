//! Kafka-compatible server implementation.
//!
//! This module provides a TCP server that speaks the Kafka wire protocol,
//! allowing you to build Kafka-compatible services.
//!
//! # Example
//! ```rust,no_run
//! use kafkaesque::server::{KafkaServer, Handler, RequestContext};
//! use kafkaesque::server::request::*;
//! use kafkaesque::server::response::*;
//! use kafkaesque::error::KafkaCode;
//! use async_trait::async_trait;
//!
//! struct MyHandler;
//!
//! #[async_trait]
//! impl Handler for MyHandler {
//!     async fn handle_metadata(
//!         &self,
//!         ctx: &RequestContext,
//!         request: MetadataRequestData,
//!     ) -> MetadataResponseData {
//!         // Return metadata for your topics
//!         MetadataResponseData {
//!             brokers: vec![BrokerData {
//!                 node_id: 0,
//!                 host: "localhost".to_string(),
//!                 port: 9092,
//!                 rack: None,
//!             }],
//!             controller_id: 0,
//!             topics: vec![],
//!         }
//!     }
//!     // ... implement other handlers
//! }
//!
//! #[tokio::main]
//! async fn main() {
//!     let server = KafkaServer::new("127.0.0.1:9092", MyHandler).await.unwrap();
//!     server.run().await.unwrap();
//! }
//! ```

pub mod codec;
mod connection;
mod handler;
mod handler_traits;
pub mod health;
pub mod rate_limiter;
pub mod request;
pub mod response;
pub mod versions;

#[cfg(feature = "tls")]
pub mod tls;

#[cfg(feature = "sasl")]
pub mod sasl;

pub use connection::ClientConnection;
pub use handler::{Handler, RequestContext};
pub use rate_limiter::{AuthRateLimiter, RateLimiterConfig};

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::{RwLock, broadcast};

use crate::error::{Error, Result};

use crate::constants::{DEFAULT_MAX_CONNECTIONS_PER_IP, DEFAULT_MAX_TOTAL_CONNECTIONS};

#[cfg(feature = "tls")]
use self::tls::TlsConfig;

/// A Kafka-compatible TCP server with graceful shutdown support.
pub struct KafkaServer<H: Handler> {
    listener: TcpListener,
    handler: Arc<H>,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
    /// Active connection counter
    active_connections: Arc<AtomicUsize>,
    // Per-IP connection counter to prevent DoS
    connections_per_ip: Arc<RwLock<HashMap<IpAddr, usize>>>,
    /// Maximum connections allowed per IP
    max_connections_per_ip: usize,
    /// Maximum total connections across all clients
    max_total_connections: usize,
    /// Rate limiter for authentication failures
    auth_rate_limiter: Arc<AuthRateLimiter>,
    /// Runtime handle for spawning data plane tasks.
    data_runtime: Handle,
}

impl<H: Handler + Send + Sync + 'static> KafkaServer<H> {
    /// Create a new Kafka server bound to the given address.
    ///
    /// Uses the current tokio runtime for spawning connection handlers.
    pub async fn new(addr: &str, handler: H) -> Result<Self> {
        Self::with_config(
            addr,
            handler,
            DEFAULT_MAX_CONNECTIONS_PER_IP,
            DEFAULT_MAX_TOTAL_CONNECTIONS,
            Handle::current(),
        )
        .await
    }

    /// Create a new Kafka server with custom configuration.
    ///
    /// # Arguments
    /// * `addr` - Address to bind to (e.g., "127.0.0.1:9092")
    /// * `handler` - Request handler implementation
    /// * `max_connections_per_ip` - Maximum connections allowed from a single IP
    /// * `max_total_connections` - Maximum total connections (0 = unlimited)
    /// * `data_runtime` - Runtime handle for spawning connection handler tasks
    pub async fn with_config(
        addr: &str,
        handler: H,
        max_connections_per_ip: usize,
        max_total_connections: usize,
        data_runtime: Handle,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

        let (shutdown_tx, _) = broadcast::channel(1);

        tracing::info!(
            addr = %addr,
            max_per_ip = max_connections_per_ip,
            max_total = max_total_connections,
            "Kafka server listening"
        );

        Ok(Self {
            listener,
            handler: Arc::new(handler),
            shutdown_tx,
            active_connections: Arc::new(AtomicUsize::new(0)),
            connections_per_ip: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_ip,
            max_total_connections,
            auth_rate_limiter: Arc::new(AuthRateLimiter::new()),
            data_runtime,
        })
    }

    /// Get the auth rate limiter for external access (e.g., for metrics).
    pub fn auth_rate_limiter(&self) -> Arc<AuthRateLimiter> {
        self.auth_rate_limiter.clone()
    }

    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| Error::IoError(e.kind()))
    }

    /// Get the number of active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// Initiate graceful shutdown.
    ///
    /// This signals the server to stop accepting new connections.
    /// Existing connections will be allowed to complete.
    /// Use `shutdown_and_wait` for waiting until all connections are drained.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        tracing::info!("Shutdown signal sent");
    }

    /// Initiate graceful shutdown and wait for all connections to drain.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for connections to drain
    ///
    /// Returns `true` if all connections drained within the timeout.
    pub async fn shutdown_and_wait(&self, timeout: std::time::Duration) -> bool {
        self.shutdown();

        let start = std::time::Instant::now();
        let check_interval = std::time::Duration::from_millis(100);

        while start.elapsed() < timeout {
            let active = self.active_connections.load(Ordering::SeqCst);
            if active == 0 {
                tracing::info!("All connections drained");
                return true;
            }
            tracing::debug!(
                active_connections = active,
                "Waiting for connections to drain"
            );
            tokio::time::sleep(check_interval).await;
        }

        let remaining = self.active_connections.load(Ordering::SeqCst);
        tracing::warn!(
            remaining_connections = remaining,
            "Shutdown timeout, connections still active"
        );
        false
    }

    /// Run the server, accepting connections and handling requests.
    ///
    /// This method will return when a shutdown signal is received.
    ///
    /// Implements per-IP and global connection limits, plus auth rate limiting
    /// to prevent DoS and brute-force attacks.
    pub async fn run(&self) -> Result<()> {
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                // Handle shutdown signal
                _ = shutdown_rx.recv() => {
                    tracing::info!("Server shutting down, no longer accepting connections");
                    return Ok(());
                }
                // Accept new connections
                accept_result = self.listener.accept() => {
                    let (stream, addr) = accept_result.map_err(|e| Error::IoError(e.kind()))?;
                    let ip = addr.ip();

                    // Check auth rate limit first
                    if let Some(remaining) = self.auth_rate_limiter.check_rate_limit(ip).await {
                        tracing::warn!(
                            client_ip = %ip,
                            remaining_secs = remaining.as_secs(),
                            "Rejecting connection - IP rate-limited due to auth failures"
                        );
                        // Drop the stream to close the connection
                        drop(stream);
                        continue;
                    }

                    // Check global connection limit (0 = unlimited)
                    if self.max_total_connections > 0 {
                        let current_total = self.active_connections.load(Ordering::SeqCst);
                        if current_total >= self.max_total_connections {
                            tracing::warn!(
                                client_ip = %ip,
                                current_connections = current_total,
                                max_connections = self.max_total_connections,
                                "Rejecting connection - global limit exceeded"
                            );
                            drop(stream);
                            continue;
                        }
                    }

                    // Check per-IP connection limit
                    let current_count = {
                        let counts = self.connections_per_ip.read().await;
                        *counts.get(&ip).unwrap_or(&0)
                    };

                    if current_count >= self.max_connections_per_ip {
                        tracing::warn!(
                            client_ip = %ip,
                            current_connections = current_count,
                            max_connections = self.max_connections_per_ip,
                            "Rejecting connection - per-IP limit exceeded"
                        );
                        // Drop the stream to close the connection
                        drop(stream);
                        continue;
                    }

                    // Increment per-IP count
                    {
                        let mut counts = self.connections_per_ip.write().await;
                        *counts.entry(ip).or_insert(0) += 1;
                    }

                    tracing::debug!(client_addr = %addr, "Accepted connection");

                    let handler = self.handler.clone();
                    let active_connections = self.active_connections.clone();
                    let connections_per_ip = self.connections_per_ip.clone();
                    let rate_limiter = self.auth_rate_limiter.clone();

                    // Increment active connection count
                    active_connections.fetch_add(1, Ordering::SeqCst);

                    self.data_runtime.spawn(async move {
                        let mut conn = ClientConnection::new_with_rate_limiter(stream, addr, rate_limiter);
                        if let Err(e) = conn.handle_requests(handler).await {
                            tracing::error!(client_addr = %addr, error = ?e, "Error handling connection");
                        }
                        // Decrement active connection count
                        active_connections.fetch_sub(1, Ordering::SeqCst);

                        // Decrement per-IP count
                        {
                            let mut counts = connections_per_ip.write().await;
                            if let Some(count) = counts.get_mut(&ip) {
                                *count = count.saturating_sub(1);
                                if *count == 0 {
                                    counts.remove(&ip);
                                }
                            }
                        }
                    });
                }
            }
        }
    }

    /// Run the server for a single connection (useful for testing).
    pub async fn accept_one(&self) -> Result<()> {
        let (stream, addr) = self
            .listener
            .accept()
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

        tracing::debug!(client_addr = %addr, "Accepted connection");

        let handler = self.handler.clone();
        let mut conn = ClientConnection::new(stream, addr);
        conn.handle_requests(handler).await
    }
}

/// A TLS-enabled Kafka server with graceful shutdown support.
#[cfg(feature = "tls")]
pub struct TlsKafkaServer<H: Handler> {
    listener: TcpListener,
    handler: Arc<H>,
    tls_config: TlsConfig,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
    /// Active connection counter
    active_connections: Arc<AtomicUsize>,
    /// Per-IP connection counter to prevent DoS
    connections_per_ip: Arc<RwLock<HashMap<IpAddr, usize>>>,
    /// Maximum connections allowed per IP
    max_connections_per_ip: usize,
    /// Maximum total connections across all clients
    max_total_connections: usize,
    /// Rate limiter for authentication failures
    auth_rate_limiter: Arc<AuthRateLimiter>,
    /// Runtime handle for spawning data plane tasks.
    data_runtime: Handle,
}

#[cfg(feature = "tls")]
impl<H: Handler + Send + Sync + 'static> TlsKafkaServer<H> {
    /// Create a new TLS-enabled Kafka server bound to the given address.
    ///
    /// Uses the current tokio runtime for spawning connection handlers.
    pub async fn new(addr: &str, handler: H, tls_config: TlsConfig) -> Result<Self> {
        Self::with_config(
            addr,
            handler,
            tls_config,
            DEFAULT_MAX_CONNECTIONS_PER_IP,
            DEFAULT_MAX_TOTAL_CONNECTIONS,
            Handle::current(),
        )
        .await
    }

    /// Create a new TLS-enabled Kafka server with custom configuration.
    ///
    /// # Arguments
    /// * `addr` - Address to bind to (e.g., "127.0.0.1:9093")
    /// * `handler` - Request handler implementation
    /// * `tls_config` - TLS configuration
    /// * `max_connections_per_ip` - Maximum connections allowed from a single IP
    /// * `max_total_connections` - Maximum total connections (0 = unlimited)
    /// * `data_runtime` - Runtime handle for spawning connection handler tasks
    pub async fn with_config(
        addr: &str,
        handler: H,
        tls_config: TlsConfig,
        max_connections_per_ip: usize,
        max_total_connections: usize,
        data_runtime: Handle,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr)
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

        let (shutdown_tx, _) = broadcast::channel(1);

        tracing::info!(
            addr = %addr,
            max_per_ip = max_connections_per_ip,
            max_total = max_total_connections,
            "Kafka TLS server listening"
        );

        Ok(Self {
            listener,
            handler: Arc::new(handler),
            tls_config,
            shutdown_tx,
            active_connections: Arc::new(AtomicUsize::new(0)),
            connections_per_ip: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_ip,
            max_total_connections,
            auth_rate_limiter: Arc::new(AuthRateLimiter::new()),
            data_runtime,
        })
    }

    /// Get the auth rate limiter for external access (e.g., for metrics).
    pub fn auth_rate_limiter(&self) -> Arc<AuthRateLimiter> {
        self.auth_rate_limiter.clone()
    }

    /// Get the local address the server is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr> {
        self.listener
            .local_addr()
            .map_err(|e| Error::IoError(e.kind()))
    }

    /// Get the number of active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// Initiate graceful shutdown.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        tracing::info!("TLS server shutdown signal sent");
    }

    /// Initiate graceful shutdown and wait for all connections to drain.
    pub async fn shutdown_and_wait(&self, timeout: std::time::Duration) -> bool {
        self.shutdown();

        let start = std::time::Instant::now();
        let check_interval = std::time::Duration::from_millis(100);

        while start.elapsed() < timeout {
            let active = self.active_connections.load(Ordering::SeqCst);
            if active == 0 {
                tracing::info!("All TLS connections drained");
                return true;
            }
            tokio::time::sleep(check_interval).await;
        }

        false
    }

    /// Run the server, accepting TLS connections and handling requests.
    ///
    /// Implements per-IP and global connection limits, plus auth rate limiting
    /// to prevent DoS and brute-force attacks.
    pub async fn run(&self) -> Result<()> {
        let mut shutdown_rx = self.shutdown_tx.subscribe();

        loop {
            tokio::select! {
                _ = shutdown_rx.recv() => {
                    tracing::info!("TLS server shutting down");
                    return Ok(());
                }
                accept_result = self.listener.accept() => {
                    let (stream, addr) = accept_result.map_err(|e| Error::IoError(e.kind()))?;
                    let ip = addr.ip();

                    // Check auth rate limit first
                    if let Some(remaining) = self.auth_rate_limiter.check_rate_limit(ip).await {
                        tracing::warn!(
                            client_ip = %ip,
                            remaining_secs = remaining.as_secs(),
                            "Rejecting connection - IP rate-limited due to auth failures"
                        );
                        drop(stream);
                        continue;
                    }

                    // Check global connection limit (0 = unlimited)
                    if self.max_total_connections > 0 {
                        let current_total = self.active_connections.load(Ordering::SeqCst);
                        if current_total >= self.max_total_connections {
                            tracing::warn!(
                                client_ip = %ip,
                                current_connections = current_total,
                                max_connections = self.max_total_connections,
                                "Rejecting connection - global limit exceeded"
                            );
                            drop(stream);
                            continue;
                        }
                    }

                    // Check per-IP connection limit
                    let current_count = {
                        let counts = self.connections_per_ip.read().await;
                        *counts.get(&ip).unwrap_or(&0)
                    };

                    if current_count >= self.max_connections_per_ip {
                        tracing::warn!(
                            client_ip = %ip,
                            current_connections = current_count,
                            max_connections = self.max_connections_per_ip,
                            "Rejecting connection - per-IP limit exceeded"
                        );
                        drop(stream);
                        continue;
                    }

                    // Increment per-IP count
                    {
                        let mut counts = self.connections_per_ip.write().await;
                        *counts.entry(ip).or_insert(0) += 1;
                    }

                    tracing::debug!(client_addr = %addr, "Accepted TLS connection");

                    let acceptor = self.tls_config.acceptor().clone();
                    let handler = self.handler.clone();
                    let active_connections = self.active_connections.clone();
                    let connections_per_ip = self.connections_per_ip.clone();
                    let rate_limiter = self.auth_rate_limiter.clone();

                    active_connections.fetch_add(1, Ordering::SeqCst);

                    self.data_runtime.spawn(async move {
                        match acceptor.accept(stream).await {
                            Ok(tls_stream) => {
                                let mut conn = connection::TlsClientConnection::new_with_rate_limiter(
                                    tls_stream,
                                    addr,
                                    rate_limiter,
                                );
                                if let Err(e) = conn.handle_requests(handler).await {
                                    tracing::error!(client_addr = %addr, error = ?e, "Error handling TLS connection");
                                }
                            }
                            Err(e) => {
                                tracing::error!(client_addr = %addr, error = ?e, "TLS handshake failed");
                            }
                        }
                        active_connections.fetch_sub(1, Ordering::SeqCst);

                        // Decrement per-IP connection count
                        let mut ip_counts = connections_per_ip.write().await;
                        if let Some(count) = ip_counts.get_mut(&ip) {
                            *count = count.saturating_sub(1);
                            if *count == 0 {
                                ip_counts.remove(&ip);
                            }
                        }
                    });
                }
            }
        }
    }

    /// Run the server for a single connection (useful for testing).
    pub async fn accept_one(&self) -> Result<()> {
        let (stream, addr) = self
            .listener
            .accept()
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

        tracing::debug!(client_addr = %addr, "Accepted TLS connection");

        let acceptor = self.tls_config.acceptor().clone();
        let tls_stream = acceptor
            .accept(stream)
            .await
            .map_err(|e| Error::MissingData(format!("TLS handshake failed: {}", e)))?;

        let handler = self.handler.clone();
        let rate_limiter = self.auth_rate_limiter.clone();
        let mut conn =
            connection::TlsClientConnection::new_with_rate_limiter(tls_stream, addr, rate_limiter);
        conn.handle_requests(handler).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::request::*;
    use crate::server::response::*;
    use async_trait::async_trait;

    /// Simple test handler that does nothing.
    struct TestHandler;

    #[async_trait]
    impl Handler for TestHandler {
        async fn handle_metadata(
            &self,
            _ctx: &RequestContext,
            _request: MetadataRequestData,
        ) -> MetadataResponseData {
            MetadataResponseData {
                brokers: vec![],
                controller_id: 0,
                topics: vec![],
            }
        }
    }

    // ========================================================================
    // KafkaServer Creation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_kafka_server_new() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                let addr = server.local_addr().unwrap();
                assert!(addr.port() > 0);
                server.shutdown();
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind (CI environments may have restrictions)
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_kafka_server_with_config() {
        match KafkaServer::with_config("127.0.0.1:0", TestHandler, 50, 100, Handle::current()).await
        {
            Ok(server) => {
                let addr = server.local_addr().unwrap();
                assert!(addr.port() > 0);
                assert_eq!(server.max_connections_per_ip, 50);
                assert_eq!(server.max_total_connections, 100);
                server.shutdown();
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_kafka_server_unlimited_connections() {
        // max_total_connections = 0 means unlimited
        match KafkaServer::with_config("127.0.0.1:0", TestHandler, 100, 0, Handle::current()).await
        {
            Ok(server) => {
                assert_eq!(server.max_total_connections, 0);
                server.shutdown();
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    // ========================================================================
    // KafkaServer Method Tests
    // ========================================================================

    #[tokio::test]
    async fn test_kafka_server_local_addr() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                let addr = server.local_addr().unwrap();
                assert_eq!(
                    addr.ip(),
                    std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
                );
                assert!(addr.port() > 0);
                server.shutdown();
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_kafka_server_active_connections() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                // Initially no connections
                assert_eq!(server.active_connections(), 0);
                server.shutdown();
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_kafka_server_auth_rate_limiter() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                let limiter = server.auth_rate_limiter();
                // Just verify we can get the rate limiter
                assert!(Arc::strong_count(&limiter) >= 1);
                server.shutdown();
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_kafka_server_shutdown() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                // Shutdown should not panic
                server.shutdown();
                // Can shutdown multiple times
                server.shutdown();
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    #[tokio::test]
    async fn test_kafka_server_shutdown_and_wait_no_connections() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                // With no connections, shutdown should complete immediately
                let drained = server
                    .shutdown_and_wait(std::time::Duration::from_millis(100))
                    .await;
                assert!(drained);
            }
            Err(crate::error::Error::IoError(std::io::ErrorKind::PermissionDenied)) => {
                // Skip test if we can't bind
            }
            Err(e) => panic!("Unexpected error: {:?}", e),
        }
    }

    // ========================================================================
    // Connection Counter Tests
    // ========================================================================

    #[test]
    fn test_atomic_counter_increment_decrement() {
        let counter = Arc::new(AtomicUsize::new(0));

        counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(counter.load(Ordering::SeqCst), 1);

        counter.fetch_add(1, Ordering::SeqCst);
        assert_eq!(counter.load(Ordering::SeqCst), 2);

        counter.fetch_sub(1, Ordering::SeqCst);
        assert_eq!(counter.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_per_ip_connection_map() {
        let connections_per_ip: Arc<RwLock<HashMap<IpAddr, usize>>> =
            Arc::new(RwLock::new(HashMap::new()));
        let ip = "192.168.1.1".parse::<IpAddr>().unwrap();

        // Add connection
        {
            let mut counts = connections_per_ip.write().await;
            *counts.entry(ip).or_insert(0) += 1;
        }

        // Verify count
        {
            let counts = connections_per_ip.read().await;
            assert_eq!(*counts.get(&ip).unwrap_or(&0), 1);
        }

        // Add another
        {
            let mut counts = connections_per_ip.write().await;
            *counts.entry(ip).or_insert(0) += 1;
        }

        // Verify
        {
            let counts = connections_per_ip.read().await;
            assert_eq!(*counts.get(&ip).unwrap_or(&0), 2);
        }

        // Remove one
        {
            let mut counts = connections_per_ip.write().await;
            if let Some(count) = counts.get_mut(&ip) {
                *count = count.saturating_sub(1);
                if *count == 0 {
                    counts.remove(&ip);
                }
            }
        }

        // Verify
        {
            let counts = connections_per_ip.read().await;
            assert_eq!(*counts.get(&ip).unwrap_or(&0), 1);
        }
    }

    // ========================================================================
    // Server Run Tests (with shutdown)
    // ========================================================================

    #[tokio::test]
    async fn test_kafka_server_run_shutdown() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                let server = Arc::new(server);
                let server_clone = server.clone();

                // Start server in background
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
    // Default Constants Tests
    // ========================================================================

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_default_connection_limits() {
        use crate::constants::{DEFAULT_MAX_CONNECTIONS_PER_IP, DEFAULT_MAX_TOTAL_CONNECTIONS};

        // Just verify the constants are reasonable values
        assert!(DEFAULT_MAX_CONNECTIONS_PER_IP > 0);
        assert!(DEFAULT_MAX_TOTAL_CONNECTIONS > 0);
    }

    // ========================================================================
    // Handler Trait Test
    // ========================================================================

    #[test]
    fn test_handler_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<TestHandler>();
    }
}
