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

pub use connection::{
    ClientConnection, read_kafka_frame_for_fuzz, set_global_inflight_byte_budget,
};
pub use handler::{Handler, RequestContext, SaslPostAuth};
pub use rate_limiter::{AuthRateLimiter, RateLimiterConfig};

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::runtime::Handle;
use tokio::sync::{RwLock, broadcast, watch};

use crate::error::{Error, Result};

use crate::constants::{DEFAULT_MAX_CONNECTIONS_PER_IP, DEFAULT_MAX_TOTAL_CONNECTIONS};

#[cfg(feature = "tls")]
use self::tls::TlsConfig;

/// How long a TLS client may take to complete the handshake before the
/// server gives up and releases the connection slot.
///
/// Without this bound a client that connects and never finishes (or never
/// starts) the TLS handshake holds a connection slot forever — a trivial
/// slot-exhaustion DoS. Kept local to the server module rather than
/// `constants.rs` because it only applies to the listener accept path.
pub const TLS_HANDSHAKE_TIMEOUT_SECS: u64 = 10;

/// Grace period granted to force-cancelled connection tasks during
/// `shutdown_and_wait` to unwind and release their slots.
///
/// After the caller-supplied drain timeout expires, remaining connections
/// are actively cancelled; we then wait at most this long for their
/// counters to reach zero so the drained/forced numbers reported in logs
/// are accurate. This bounds `shutdown_and_wait` to
/// `timeout + SHUTDOWN_FORCE_CANCEL_GRACE_MS`.
pub const SHUTDOWN_FORCE_CANCEL_GRACE_MS: u64 = 1_000;

/// Outcome of attempting to reserve connection-slot capacity for a newly
/// accepted connection. See [`try_reserve_connection_slots`].
enum SlotReservation {
    /// Both the global and per-IP counters were incremented; the caller
    /// owns the reservation and must release it via [`ConnectionGuard`].
    Reserved,
    /// The global connection limit was reached; nothing was incremented.
    GlobalLimitReached { current: usize },
    /// The per-IP limit was reached; the speculative global increment was
    /// rolled back, so nothing is (net) incremented.
    PerIpLimitReached { current: usize },
}

/// Atomically reserve a global + per-IP connection slot.
///
/// The previous accept path did `load` → check → `fetch_add` (and a read
/// lock check followed by a separate write lock increment for the per-IP
/// map), which is a check-then-act race: two accepts racing through the
/// window could both pass the check and exceed the configured limits.
///
/// Here the global counter uses a compare-and-swap loop (`fetch_update`)
/// that refuses to increment at capacity, and the per-IP check and
/// increment happen under a single write-lock acquisition. On a per-IP
/// rejection the global reservation taken first is rolled back, so every
/// non-`Reserved` outcome leaves both counters untouched.
async fn try_reserve_connection_slots(
    active_connections: &AtomicUsize,
    connections_per_ip: &RwLock<HashMap<IpAddr, usize>>,
    ip: IpAddr,
    max_connections_per_ip: usize,
    max_total_connections: usize,
) -> SlotReservation {
    // Global slot first: CAS-increment that rejects at capacity
    // (`max_total_connections == 0` means unlimited).
    if max_total_connections == 0 {
        active_connections.fetch_add(1, Ordering::SeqCst);
    } else if let Err(current) =
        active_connections.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |current| {
            (current < max_total_connections).then_some(current + 1)
        })
    {
        return SlotReservation::GlobalLimitReached { current };
    }

    // Per-IP slot: check + increment under the same write-lock acquisition.
    {
        let mut counts = connections_per_ip.write().await;
        let current = counts.get(&ip).copied().unwrap_or(0);
        if current >= max_connections_per_ip {
            drop(counts);
            // Roll back the global reservation taken above.
            active_connections.fetch_sub(1, Ordering::SeqCst);
            return SlotReservation::PerIpLimitReached { current };
        }
        counts.insert(ip, current + 1);
    }

    SlotReservation::Reserved
}

/// Resolves when the server force-cancels in-flight connections (drain
/// timeout during `shutdown_and_wait`).
///
/// Connection tasks `select!` on this alongside their serve future; when it
/// resolves the serve future is dropped, which closes the socket and runs
/// the [`ConnectionGuard`] cleanup. If the watch sender is dropped (the
/// server struct itself is gone) this also resolves: there is nobody left
/// to drain the connection gracefully, so cancelling is the right call.
async fn force_cancelled(mut force_rx: watch::Receiver<bool>) {
    let _ = force_rx.wait_for(|forced| *forced).await;
}

/// RAII guard for the per-connection bookkeeping counters.
///
/// Decrements `active_connections` and the per-IP entry on drop, which
/// runs even if the connection's task panics. Without this guard a panic
/// inside the spawned task would leak both counters and eventually trip
/// the connection-limit even though the slots are dead.
struct ConnectionGuard {
    active_connections: Arc<AtomicUsize>,
    connections_per_ip: Arc<RwLock<HashMap<IpAddr, usize>>>,
    ip: IpAddr,
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
        // Decrement under the async lock from a synchronous context by
        // spawning a small cleanup future on the current runtime. If the
        // runtime is gone (rare — only during process teardown) the
        // counter resets are moot.
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            let map = self.connections_per_ip.clone();
            let ip = self.ip;
            handle.spawn(async move {
                let mut counts = map.write().await;
                if let Some(count) = counts.get_mut(&ip) {
                    *count = count.saturating_sub(1);
                    if *count == 0 {
                        counts.remove(&ip);
                    }
                }
            });
        }
    }
}

/// A Kafka-compatible TCP server with graceful shutdown support.
pub struct KafkaServer<H: Handler> {
    listener: TcpListener,
    handler: Arc<H>,
    /// Shutdown signal sender
    shutdown_tx: broadcast::Sender<()>,
    /// Force-cancel signal for in-flight connections. Flipped to `true` by
    /// `shutdown_and_wait` when the graceful drain times out; every
    /// connection task selects on it and drops its socket when it fires.
    force_shutdown_tx: watch::Sender<bool>,
    /// Active connection counter
    active_connections: Arc<AtomicUsize>,
    // Per-IP connection counter to prevent DoS
    connections_per_ip: Arc<RwLock<HashMap<IpAddr, usize>>>,
    /// Maximum connections allowed per IP
    max_connections_per_ip: usize,
    /// Maximum total connections across all clients
    max_total_connections: usize,
    /// Per-connection inbound frame cap (bytes).
    max_message_size: usize,
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
        Self::with_full_config(
            addr,
            handler,
            max_connections_per_ip,
            max_total_connections,
            crate::constants::DEFAULT_MAX_MESSAGE_SIZE,
            data_runtime,
        )
        .await
    }

    /// Same as `with_config` but also sets the per-connection inbound frame cap.
    pub async fn with_full_config(
        addr: &str,
        handler: H,
        max_connections_per_ip: usize,
        max_total_connections: usize,
        max_message_size: usize,
        data_runtime: Handle,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(Error::from)?;

        let (shutdown_tx, _) = broadcast::channel(1);
        let (force_shutdown_tx, _) = watch::channel(false);

        tracing::info!(
            addr = %addr,
            max_per_ip = max_connections_per_ip,
            max_total = max_total_connections,
            max_message_size,
            "Kafka server listening"
        );

        Ok(Self {
            listener,
            handler: Arc::new(handler),
            shutdown_tx,
            force_shutdown_tx,
            active_connections: Arc::new(AtomicUsize::new(0)),
            connections_per_ip: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_ip,
            max_total_connections,
            max_message_size,
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
        self.listener.local_addr().map_err(Error::from)
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

    /// Access the wrapped handler. Used by the binary to call
    /// `handler.shutdown()` after `shutdown_and_wait()` drains connections.
    pub fn handler(&self) -> Arc<H> {
        self.handler.clone()
    }

    /// Initiate graceful shutdown and wait for all connections to drain.
    ///
    /// # Arguments
    /// * `timeout` - Maximum time to wait for connections to drain gracefully
    ///
    /// Returns `true` if all connections drained gracefully within the
    /// timeout. If they do not, the remaining in-flight connections are
    /// actively force-cancelled (their sockets are closed and their tasks
    /// unwound), we wait up to [`SHUTDOWN_FORCE_CANCEL_GRACE_MS`] for the
    /// cancellations to land, and `false` is returned. Either way this
    /// method returns in bounded time and no connection task is left
    /// running indefinitely.
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

        // Drain budget exhausted: force-cancel everything still in flight.
        // Each connection task selects on the force-shutdown watch and
        // drops its socket (releasing its slot) when it flips to true.
        let remaining = self.active_connections.load(Ordering::SeqCst);
        tracing::warn!(
            remaining_connections = remaining,
            "Shutdown drain timed out; force-cancelling remaining connections"
        );
        let _ = self.force_shutdown_tx.send(true);

        // Give cancelled tasks a short grace period to unwind so the
        // drained/forced counts reported below are accurate.
        let grace = Duration::from_millis(SHUTDOWN_FORCE_CANCEL_GRACE_MS);
        let force_start = std::time::Instant::now();
        while force_start.elapsed() < grace {
            if self.active_connections.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let still_active = self.active_connections.load(Ordering::SeqCst);
        let forced = remaining.saturating_sub(still_active);
        if still_active == 0 {
            tracing::info!(
                forced_cancelled = forced,
                "All remaining connections force-cancelled"
            );
        } else {
            tracing::warn!(
                forced_cancelled = forced,
                still_active,
                "Some connections did not exit within the force-cancel grace period"
            );
        }
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
                    let (stream, addr) = accept_result.map_err(Error::from)?;
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

                    // Atomically reserve the global + per-IP connection
                    // slots (check and increment happen together, so racing
                    // accepts can never exceed the configured limits).
                    match try_reserve_connection_slots(
                        &self.active_connections,
                        &self.connections_per_ip,
                        ip,
                        self.max_connections_per_ip,
                        self.max_total_connections,
                    )
                    .await
                    {
                        SlotReservation::Reserved => {}
                        SlotReservation::GlobalLimitReached { current } => {
                            tracing::warn!(
                                client_ip = %ip,
                                current_connections = current,
                                max_connections = self.max_total_connections,
                                "Rejecting connection - global limit exceeded"
                            );
                            drop(stream);
                            continue;
                        }
                        SlotReservation::PerIpLimitReached { current } => {
                            tracing::warn!(
                                client_ip = %ip,
                                current_connections = current,
                                max_connections = self.max_connections_per_ip,
                                "Rejecting connection - per-IP limit exceeded"
                            );
                            // Drop the stream to close the connection
                            drop(stream);
                            continue;
                        }
                    }

                    tracing::debug!(client_addr = %addr, "Accepted connection");

                    let handler = self.handler.clone();
                    let rate_limiter = self.auth_rate_limiter.clone();
                    let max_message_size = self.max_message_size;
                    // RAII guard owning the slot reservation made above.
                    // Created here (not inside the task) so the slots are
                    // released even if the runtime drops the task before
                    // ever polling it.
                    let guard = ConnectionGuard {
                        active_connections: self.active_connections.clone(),
                        connections_per_ip: self.connections_per_ip.clone(),
                        ip,
                    };
                    let force_rx = self.force_shutdown_tx.subscribe();

                    self.data_runtime.spawn(async move {
                        let _guard = guard;
                        let serve = async move {
                            let mut conn = ClientConnection::new_with_rate_limiter(stream, addr, rate_limiter);
                            conn.set_max_message_size(max_message_size);
                            // Arm the per-connection SASL gate
                            // so non-handshake API keys are refused until
                            // SaslAuthenticate succeeds. Default-off; opt in via
                            // ClusterConfig::sasl_required.
                            if handler.sasl_required() {
                                conn.require_sasl();
                            }
                            if let Err(e) = conn.handle_requests(handler).await {
                                tracing::warn!(client_addr = %addr, error = ?e, "Error handling connection");
                            }
                        };
                        tokio::select! {
                            _ = force_cancelled(force_rx) => {
                                tracing::warn!(
                                    client_addr = %addr,
                                    "Connection force-cancelled by server shutdown"
                                );
                            }
                            _ = serve => {}
                        }
                        // ConnectionGuard's Drop decrements active_connections
                        // and connections_per_ip — on normal completion,
                        // force-cancel, and panic alike.
                    });
                }
            }
        }
    }

    /// Run the server for a single connection (useful for testing).
    pub async fn accept_one(&self) -> Result<()> {
        let (stream, addr) = self.listener.accept().await.map_err(Error::from)?;

        tracing::debug!(client_addr = %addr, "Accepted connection");

        let handler = self.handler.clone();
        let mut conn = ClientConnection::new(stream, addr);
        conn.set_max_message_size(self.max_message_size);
        if handler.sasl_required() {
            conn.require_sasl();
        }
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
    /// Force-cancel signal for in-flight connections. Flipped to `true` by
    /// `shutdown_and_wait` when the graceful drain times out; every
    /// connection task selects on it and drops its socket when it fires.
    force_shutdown_tx: watch::Sender<bool>,
    /// Budget for completing the TLS handshake before the connection slot
    /// is released. Defaults to [`TLS_HANDSHAKE_TIMEOUT_SECS`].
    tls_handshake_timeout: Duration,
    /// Active connection counter
    active_connections: Arc<AtomicUsize>,
    /// Per-IP connection counter to prevent DoS
    connections_per_ip: Arc<RwLock<HashMap<IpAddr, usize>>>,
    /// Maximum connections allowed per IP
    max_connections_per_ip: usize,
    /// Maximum total connections across all clients
    max_total_connections: usize,
    /// Per-connection inbound frame cap (bytes).
    max_message_size: usize,
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
        Self::with_full_config(
            addr,
            handler,
            tls_config,
            max_connections_per_ip,
            max_total_connections,
            crate::constants::DEFAULT_MAX_MESSAGE_SIZE,
            data_runtime,
        )
        .await
    }

    /// Same as `with_config` but also sets the per-connection inbound frame cap.
    pub async fn with_full_config(
        addr: &str,
        handler: H,
        tls_config: TlsConfig,
        max_connections_per_ip: usize,
        max_total_connections: usize,
        max_message_size: usize,
        data_runtime: Handle,
    ) -> Result<Self> {
        let listener = TcpListener::bind(addr).await.map_err(Error::from)?;

        let (shutdown_tx, _) = broadcast::channel(1);
        let (force_shutdown_tx, _) = watch::channel(false);

        tracing::info!(
            addr = %addr,
            max_per_ip = max_connections_per_ip,
            max_total = max_total_connections,
            max_message_size,
            "Kafka TLS server listening"
        );

        Ok(Self {
            listener,
            handler: Arc::new(handler),
            tls_config,
            shutdown_tx,
            force_shutdown_tx,
            tls_handshake_timeout: Duration::from_secs(TLS_HANDSHAKE_TIMEOUT_SECS),
            active_connections: Arc::new(AtomicUsize::new(0)),
            connections_per_ip: Arc::new(RwLock::new(HashMap::new())),
            max_connections_per_ip,
            max_total_connections,
            max_message_size,
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
        self.listener.local_addr().map_err(Error::from)
    }

    /// Get the number of active connections.
    pub fn active_connections(&self) -> usize {
        self.active_connections.load(Ordering::SeqCst)
    }

    /// Override the TLS handshake budget. Connections that have not
    /// completed the handshake within this duration are closed and their
    /// connection slot released. Defaults to
    /// [`TLS_HANDSHAKE_TIMEOUT_SECS`]; exposed primarily for tests and
    /// operators that need a tighter bound.
    pub fn set_tls_handshake_timeout(&mut self, timeout: Duration) {
        self.tls_handshake_timeout = timeout;
    }

    /// Initiate graceful shutdown.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(());
        tracing::info!("TLS server shutdown signal sent");
    }

    /// Access the wrapped handler. Mirrors `KafkaServer::handler` so the
    /// binary can call `handler.shutdown()` after `shutdown_and_wait()`
    /// drains connections.
    pub fn handler(&self) -> Arc<H> {
        self.handler.clone()
    }

    /// Initiate graceful shutdown and wait for all connections to drain.
    ///
    /// Returns `true` if all connections drained gracefully within the
    /// timeout. If they do not, the remaining in-flight connections are
    /// actively force-cancelled, we wait up to
    /// [`SHUTDOWN_FORCE_CANCEL_GRACE_MS`] for the cancellations to land,
    /// and `false` is returned. Either way this method returns in bounded
    /// time and no connection task is left running indefinitely.
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

        // Drain budget exhausted: force-cancel everything still in flight.
        let remaining = self.active_connections.load(Ordering::SeqCst);
        tracing::warn!(
            remaining_connections = remaining,
            "TLS shutdown drain timed out; force-cancelling remaining connections"
        );
        let _ = self.force_shutdown_tx.send(true);

        let grace = Duration::from_millis(SHUTDOWN_FORCE_CANCEL_GRACE_MS);
        let force_start = std::time::Instant::now();
        while force_start.elapsed() < grace {
            if self.active_connections.load(Ordering::SeqCst) == 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let still_active = self.active_connections.load(Ordering::SeqCst);
        let forced = remaining.saturating_sub(still_active);
        if still_active == 0 {
            tracing::info!(
                forced_cancelled = forced,
                "All remaining TLS connections force-cancelled"
            );
        } else {
            tracing::warn!(
                forced_cancelled = forced,
                still_active,
                "Some TLS connections did not exit within the force-cancel grace period"
            );
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
                    let (stream, addr) = accept_result.map_err(Error::from)?;
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

                    // Atomically reserve the global + per-IP connection
                    // slots (check and increment happen together, so racing
                    // accepts can never exceed the configured limits).
                    match try_reserve_connection_slots(
                        &self.active_connections,
                        &self.connections_per_ip,
                        ip,
                        self.max_connections_per_ip,
                        self.max_total_connections,
                    )
                    .await
                    {
                        SlotReservation::Reserved => {}
                        SlotReservation::GlobalLimitReached { current } => {
                            tracing::warn!(
                                client_ip = %ip,
                                current_connections = current,
                                max_connections = self.max_total_connections,
                                "Rejecting connection - global limit exceeded"
                            );
                            drop(stream);
                            continue;
                        }
                        SlotReservation::PerIpLimitReached { current } => {
                            tracing::warn!(
                                client_ip = %ip,
                                current_connections = current,
                                max_connections = self.max_connections_per_ip,
                                "Rejecting connection - per-IP limit exceeded"
                            );
                            drop(stream);
                            continue;
                        }
                    }

                    tracing::debug!(client_addr = %addr, "Accepted TLS connection");

                    let acceptor = self.tls_config.acceptor().clone();
                    let handler = self.handler.clone();
                    let rate_limiter = self.auth_rate_limiter.clone();
                    let max_message_size = self.max_message_size;
                    let handshake_timeout = self.tls_handshake_timeout;
                    // RAII guard owning the slot reservation made above.
                    // Created here (not inside the task) so the slots are
                    // released even if the runtime drops the task before
                    // ever polling it.
                    let guard = ConnectionGuard {
                        active_connections: self.active_connections.clone(),
                        connections_per_ip: self.connections_per_ip.clone(),
                        ip,
                    };
                    let force_rx = self.force_shutdown_tx.subscribe();

                    self.data_runtime.spawn(async move {
                        let _guard = guard;
                        let serve = async move {
                            // Bound the handshake: a client that connects and
                            // never completes the TLS handshake must not hold
                            // a connection slot forever (slot-exhaustion DoS).
                            let tls_stream = match tokio::time::timeout(
                                handshake_timeout,
                                acceptor.accept(stream),
                            )
                            .await
                            {
                                Ok(Ok(tls_stream)) => tls_stream,
                                Ok(Err(e)) => {
                                    tracing::error!(client_addr = %addr, error = ?e, "TLS handshake failed");
                                    crate::cluster::metrics::TOTAL_CONNECTIONS
                                        .with_label_values(&["tls_handshake_failed"])
                                        .inc();
                                    return;
                                }
                                Err(_) => {
                                    tracing::warn!(
                                        client_addr = %addr,
                                        timeout_ms = handshake_timeout.as_millis() as u64,
                                        "TLS handshake timed out; releasing connection slot"
                                    );
                                    crate::cluster::metrics::TOTAL_CONNECTIONS
                                        .with_label_values(&["tls_handshake_timeout"])
                                        .inc();
                                    return;
                                }
                            };
                            let mut conn = connection::TlsClientConnection::new_with_rate_limiter(
                                tls_stream,
                                addr,
                                rate_limiter,
                            );
                            conn.set_max_message_size(max_message_size);
                            if handler.sasl_required() {
                                conn.require_sasl();
                            }
                            if let Err(e) = conn.handle_requests(handler).await {
                                tracing::error!(client_addr = %addr, error = ?e, "Error handling TLS connection");
                            }
                        };
                        tokio::select! {
                            _ = force_cancelled(force_rx) => {
                                tracing::warn!(
                                    client_addr = %addr,
                                    "TLS connection force-cancelled by server shutdown"
                                );
                            }
                            _ = serve => {}
                        }
                        // ConnectionGuard handles counter cleanup — on normal
                        // completion, handshake timeout, force-cancel, and
                        // panic alike.
                    });
                }
            }
        }
    }

    /// Run the server for a single connection (useful for testing).
    pub async fn accept_one(&self) -> Result<()> {
        let (stream, addr) = self.listener.accept().await.map_err(Error::from)?;

        tracing::debug!(client_addr = %addr, "Accepted TLS connection");

        let acceptor = self.tls_config.acceptor().clone();
        let tls_stream = tokio::time::timeout(self.tls_handshake_timeout, acceptor.accept(stream))
            .await
            .map_err(|_| Error::MissingData("TLS handshake timed out".to_string()))?
            .map_err(|e| Error::MissingData(format!("TLS handshake failed: {}", e)))?;

        let handler = self.handler.clone();
        let rate_limiter = self.auth_rate_limiter.clone();
        let mut conn =
            connection::TlsClientConnection::new_with_rate_limiter(tls_stream, addr, rate_limiter);
        conn.set_max_message_size(self.max_message_size);
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
    async fn test_kafka_server_unlimited_connections() {
        // max_total_connections = 0 means unlimited
        match KafkaServer::with_config("127.0.0.1:0", TestHandler, 100, 0, Handle::current()).await
        {
            Ok(server) => {
                assert_eq!(server.max_total_connections, 0);
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
    async fn test_kafka_server_active_connections() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                // Initially no connections
                assert_eq!(server.active_connections(), 0);
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

    #[tokio::test]
    async fn test_kafka_server_auth_rate_limiter() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                let limiter = server.auth_rate_limiter();
                // Just verify we can get the rate limiter
                assert!(Arc::strong_count(&limiter) >= 1);
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

    #[tokio::test]
    async fn test_kafka_server_shutdown() {
        match KafkaServer::new("127.0.0.1:0", TestHandler).await {
            Ok(server) => {
                // Shutdown should not panic
                server.shutdown();
                // Can shutdown multiple times
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
    // Slot Reservation Tests (atomic connection-limit checks)
    // ========================================================================

    #[tokio::test]
    async fn test_reserve_slots_accepts_below_limits() {
        let active = AtomicUsize::new(0);
        let per_ip = RwLock::new(HashMap::new());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        let outcome = try_reserve_connection_slots(&active, &per_ip, ip, 2, 2).await;
        assert!(matches!(outcome, SlotReservation::Reserved));
        assert_eq!(active.load(Ordering::SeqCst), 1);
        assert_eq!(*per_ip.read().await.get(&ip).unwrap(), 1);
    }

    #[tokio::test]
    async fn test_reserve_slots_rejects_at_global_limit() {
        let active = AtomicUsize::new(3);
        let per_ip = RwLock::new(HashMap::new());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        let outcome = try_reserve_connection_slots(&active, &per_ip, ip, 100, 3).await;
        assert!(matches!(
            outcome,
            SlotReservation::GlobalLimitReached { current: 3 }
        ));
        // Nothing incremented on rejection.
        assert_eq!(active.load(Ordering::SeqCst), 3);
        assert!(per_ip.read().await.is_empty());
    }

    #[tokio::test]
    async fn test_reserve_slots_rolls_back_global_on_per_ip_reject() {
        let active = AtomicUsize::new(0);
        let per_ip = RwLock::new(HashMap::new());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        per_ip.write().await.insert(ip, 5);

        let outcome = try_reserve_connection_slots(&active, &per_ip, ip, 5, 100).await;
        assert!(matches!(
            outcome,
            SlotReservation::PerIpLimitReached { current: 5 }
        ));
        // The speculative global increment must be rolled back.
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(*per_ip.read().await.get(&ip).unwrap(), 5);
    }

    #[tokio::test]
    async fn test_reserve_slots_unlimited_total() {
        let active = AtomicUsize::new(usize::MAX / 2);
        let per_ip = RwLock::new(HashMap::new());
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        // max_total_connections == 0 means unlimited.
        let outcome = try_reserve_connection_slots(&active, &per_ip, ip, 10, 0).await;
        assert!(matches!(outcome, SlotReservation::Reserved));
        assert_eq!(active.load(Ordering::SeqCst), usize::MAX / 2 + 1);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_reservations_never_exceed_global_limit() {
        const MAX_TOTAL: usize = 10;
        const ATTEMPTS: usize = 100;

        let active = Arc::new(AtomicUsize::new(0));
        let per_ip = Arc::new(RwLock::new(HashMap::new()));

        let mut tasks = Vec::with_capacity(ATTEMPTS);
        for i in 0..ATTEMPTS {
            let active = active.clone();
            let per_ip = per_ip.clone();
            // Spread across IPs so the per-IP limit never interferes.
            let ip: IpAddr = format!("10.0.0.{}", i % 250 + 1).parse().unwrap();
            tasks.push(tokio::spawn(async move {
                matches!(
                    try_reserve_connection_slots(&active, &per_ip, ip, ATTEMPTS, MAX_TOTAL).await,
                    SlotReservation::Reserved
                )
            }));
        }

        let mut accepted = 0;
        for task in tasks {
            if task.await.unwrap() {
                accepted += 1;
            }
        }

        assert_eq!(accepted, MAX_TOTAL, "exactly max_total slots reserved");
        assert_eq!(active.load(Ordering::SeqCst), MAX_TOTAL);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 8)]
    async fn test_concurrent_reservations_never_exceed_per_ip_limit() {
        const MAX_PER_IP: usize = 4;
        const ATTEMPTS: usize = 64;

        let active = Arc::new(AtomicUsize::new(0));
        let per_ip = Arc::new(RwLock::new(HashMap::new()));
        let ip: IpAddr = "10.0.0.1".parse().unwrap();

        let mut tasks = Vec::with_capacity(ATTEMPTS);
        for _ in 0..ATTEMPTS {
            let active = active.clone();
            let per_ip = per_ip.clone();
            tasks.push(tokio::spawn(async move {
                matches!(
                    try_reserve_connection_slots(&active, &per_ip, ip, MAX_PER_IP, 0).await,
                    SlotReservation::Reserved
                )
            }));
        }

        let mut accepted = 0;
        for task in tasks {
            if task.await.unwrap() {
                accepted += 1;
            }
        }

        assert_eq!(accepted, MAX_PER_IP, "exactly max_per_ip slots reserved");
        // Per-IP rejections must roll the global counter back.
        assert_eq!(active.load(Ordering::SeqCst), MAX_PER_IP);
        assert_eq!(*per_ip.read().await.get(&ip).unwrap(), MAX_PER_IP);
    }

    // ========================================================================
    // Force-cancel signal tests
    // ========================================================================

    #[tokio::test]
    async fn test_force_cancelled_resolves_on_signal() {
        let (tx, rx) = watch::channel(false);
        let waiter = tokio::spawn(force_cancelled(rx));
        tx.send(true).unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("force_cancelled must resolve once the signal fires")
            .unwrap();
    }

    #[tokio::test]
    async fn test_force_cancelled_resolves_on_sender_drop() {
        let (tx, rx) = watch::channel(false);
        let waiter = tokio::spawn(force_cancelled(rx));
        drop(tx);
        tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
            .await
            .expect("force_cancelled must resolve when the server is dropped")
            .unwrap();
    }

    #[tokio::test]
    async fn test_force_cancelled_pending_without_signal() {
        let (tx, rx) = watch::channel(false);
        let result =
            tokio::time::timeout(std::time::Duration::from_millis(50), force_cancelled(rx)).await;
        assert!(result.is_err(), "must stay pending until signalled");
        drop(tx);
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
