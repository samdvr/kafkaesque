//! Network transport for Raft communication.
//!
//! This module provides the network layer for Raft RPCs between nodes.
//! We use a simple TCP-based transport with bincode serialization. Every
//! frame is HMAC-SHA256 signed — see `super::auth` for the wire
//! format and key plumbing.
//!
//! When `RaftConfig.tls` is `Some`, the underlying TCP socket
//! is wrapped in a rustls `TlsStream` before any frame bytes flow. The
//! HMAC layer remains in place on top — TLS adds peer identity (mTLS) and
//! encryption, HMAC adds replay/integrity defense and stays cheap to verify
//! even under TLS termination at a proxy. The wrapper enum
//! [`MaybeTlsStreamServer`] / [`MaybeTlsStreamClient`] lets the same accept
//! and connect code paths feed both plain and TLS sockets to the framing
//! helpers (which are already generic over `AsyncRead`/`AsyncWrite`).

use std::collections::BTreeMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use backon::Retryable;
use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::timeout;

use super::auth::{RaftAuthKeys, read_rpc_frame, write_rpc_frame};
use super::commands::{CoordinationCommand, CoordinationResponse};
use super::tls::RaftTlsConfig;
use super::types::{RaftNodeId, TypeConfig};

/// Timeout for RPC connection establishment.
const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for RPC read/write operations.
const RPC_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum number of RPC retry attempts.
const RPC_MAX_RETRIES: u32 = 3;

/// Base delay for exponential backoff between retries.
const RPC_RETRY_BASE_DELAY: Duration = Duration::from_millis(100);

/// Maximum delay for exponential backoff.
const RPC_RETRY_MAX_DELAY: Duration = Duration::from_secs(2);

/// Number of consecutive failures before the circuit breaker opens.
const CIRCUIT_BREAKER_THRESHOLD: u32 = 5;

/// Duration to keep the circuit breaker open before allowing a probe.
const CIRCUIT_BREAKER_RESET_TIMEOUT: Duration = Duration::from_secs(30);

/// Maximum number of hops for forwarded requests to prevent loops.
const MAX_FORWARD_HOPS: u8 = 3;

/// Server-side Raft socket: either a plain TCP stream or a rustls
/// `TlsStream` produced by `TlsAcceptor::accept`. Implements `AsyncRead` /
/// `AsyncWrite` by forwarding to whichever variant is live, so the existing
/// HMAC framing helpers (`read_rpc_frame` / `write_rpc_frame`) work over
/// either transport unchanged.
pub enum MaybeTlsStreamServer {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::server::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStreamServer {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStreamServer {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Client-side counterpart to [`MaybeTlsStreamServer`]. Holds whatever
/// `TlsConnector::connect` produced when TLS is configured, or a raw
/// `TcpStream` otherwise. Used as the cached connection type on
/// [`RaftNetworkConnection`] so reused outbound sockets keep their TLS
/// session.
pub enum MaybeTlsStreamClient {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for MaybeTlsStreamClient {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for MaybeTlsStreamClient {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Self::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Establish an outbound Raft socket and apply TLS if configured. Set
/// `TCP_NODELAY` on the underlying TCP stream before the TLS handshake so
/// the small Raft frames don't sit in Nagle.
async fn connect_raft(
    addr: &str,
    tls: Option<&RaftTlsConfig>,
) -> std::io::Result<MaybeTlsStreamClient> {
    let stream = timeout(RPC_CONNECT_TIMEOUT, TcpStream::connect(addr))
        .await
        .map_err(|_| {
            std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("Connection timeout to {}", addr),
            )
        })??;
    stream.set_nodelay(true)?;

    #[cfg(feature = "tls")]
    {
        if let Some(t) = tls {
            let tls_stream = t
                .connector
                .connect(t.outbound_server_name(), stream)
                .await
                .map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("Raft TLS handshake to {} failed: {}", addr, e),
                    )
                })?;
            return Ok(MaybeTlsStreamClient::Tls(Box::new(tls_stream)));
        }
    }
    #[cfg(not(feature = "tls"))]
    let _ = tls;

    Ok(MaybeTlsStreamClient::Plain(stream))
}

/// Message types for Raft RPC.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RaftRpcMessage {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<RaftNodeId>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
    /// Client write request with term validation to prevent stale leader forwarding.
    ///
    /// This prevents the race condition where:
    /// 1. Node A thinks Node B is leader (stale metrics)
    /// 2. Node B forwards to Node C (actual leader changed)
    /// 3. Request loops or gets lost
    ClientWriteWithTerm {
        command: CoordinationCommand,
        /// Expected leader term when forwarding. The receiving node will
        /// reject the request if its current term is higher (leadership changed).
        expected_term: u64,
        /// Number of hops this request has taken. Used to prevent infinite loops.
        forward_hops: u8,
    },
    /// Request to join the cluster as a new member.
    ///
    /// Frames carrying this variant are signed with the join key (or the
    /// cluster secret as a fallback) — see `super::auth`. The server further
    /// rejects any *other* variant carried under the join purpose tag, so a
    /// holder of the join token cannot inject arbitrary `ClientWriteWithTerm`
    /// commands without also holding the cluster secret.
    JoinCluster {
        node_id: RaftNodeId,
        raft_addr: String,
    },
    /// Promote an existing learner to a voting member.
    ///
    /// Must be sent under the **cluster** HMAC purpose (not the join purpose).
    /// Splitting promotion from [`JoinCluster`] means a holder of the join
    /// token alone cannot gain voter status and take over the cluster.
    PromoteMember {
        node_id: RaftNodeId,
    },
}

/// Structured RPC error type that preserves retry semantics.
///
/// Previously all errors were converted to strings, making it impossible for
/// clients to distinguish retryable errors from permanent failures. This enum
/// allows clients to make informed decisions about retry behavior.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RpcErrorKind {
    /// Leadership changed or term is stale. Client should refresh leader info and retry.
    LeadershipChanged,
    /// This node is not the leader. Contains leader hint if known.
    NotLeader { leader_hint: Option<RaftNodeId> },
    /// Forward loop detected. Client should not retry immediately.
    ForwardLoopDetected,
    /// Request validation failed. Client should not retry with same request.
    InvalidRequest,
    /// Internal error. Client may retry with backoff.
    Internal,
    /// Timeout occurred. Client may retry with backoff.
    Timeout,
    /// Network error. Client may retry with backoff.
    Network,
}

impl RpcErrorKind {
    /// Returns true if the client should retry this error with backoff.
    pub fn is_retryable(&self) -> bool {
        match self {
            RpcErrorKind::LeadershipChanged => true, // Retry after refreshing leader
            RpcErrorKind::NotLeader { .. } => true,  // Retry after refreshing leader
            RpcErrorKind::ForwardLoopDetected => false, // Wait longer before retry
            RpcErrorKind::InvalidRequest => false,   // Don't retry
            RpcErrorKind::Internal => true,          // May be transient
            RpcErrorKind::Timeout => true,           // May be transient
            RpcErrorKind::Network => true,           // May be transient
        }
    }

    /// Returns true if the client should refresh leader information before retry.
    pub fn should_refresh_leader(&self) -> bool {
        matches!(
            self,
            RpcErrorKind::LeadershipChanged | RpcErrorKind::NotLeader { .. }
        )
    }
}

/// Structured RPC error with kind and message.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RpcErrorInfo {
    /// Error kind for programmatic handling.
    pub kind: RpcErrorKind,
    /// Human-readable error message.
    pub message: String,
}

impl RpcErrorInfo {
    /// Create a new RPC error.
    pub fn new(kind: RpcErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Create an internal error from any error type.
    pub fn internal(e: impl std::fmt::Display) -> Self {
        Self::new(RpcErrorKind::Internal, e.to_string())
    }
}

/// Response types for Raft RPC.
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum RaftRpcResponse {
    AppendEntries(AppendEntriesResponse<RaftNodeId>),
    Vote(VoteResponse<RaftNodeId>),
    InstallSnapshot(InstallSnapshotResponse<RaftNodeId>),
    /// Client write response (from leader back to forwarding node)
    ClientWriteOk(CoordinationResponse),
    /// Legacy error type (string only) - kept for backwards compatibility
    /// New code should use ErrorV2
    Error(String),
    /// Structured error with retry semantics
    ErrorV2(RpcErrorInfo),
    /// Response to join cluster request
    JoinClusterOk,
    /// Response to promote-member request
    PromoteMemberOk,
}

/// Factory for creating Raft network connections.
pub struct RaftNetworkFactoryImpl {
    /// Known node addresses.
    nodes: Arc<RwLock<BTreeMap<RaftNodeId, String>>>,
    /// HMAC keys used to sign all outbound traffic.
    auth_keys: Arc<RaftAuthKeys>,
    /// Optional mTLS config used to wrap every outbound socket.
    /// Cloned into each `RaftNetworkConnection` at construction so
    /// per-connection TLS sessions can be cached alongside the TCP socket.
    tls: Option<RaftTlsConfig>,
}

impl RaftNetworkFactoryImpl {
    /// Create a new network factory with no auth keys (legacy / dev).
    pub fn new() -> Self {
        Self::with_keys(Arc::new(RaftAuthKeys::dev_unauthenticated()))
    }

    /// Create a new network factory using the given HMAC keys.
    pub fn with_keys(auth_keys: Arc<RaftAuthKeys>) -> Self {
        Self::with_keys_and_tls(auth_keys, None)
    }

    /// Create a new network factory with HMAC keys and optional TLS.
    /// When `tls` is `Some`, every outbound Raft connection will perform
    /// a TLS handshake against the peer's listener.
    pub fn with_keys_and_tls(auth_keys: Arc<RaftAuthKeys>, tls: Option<RaftTlsConfig>) -> Self {
        Self {
            nodes: Arc::new(RwLock::new(BTreeMap::new())),
            auth_keys,
            tls,
        }
    }

    /// Add or update a node's address.
    pub async fn add_node(&self, node_id: RaftNodeId, addr: String) {
        self.nodes.write().await.insert(node_id, addr);
    }

    /// Get a node's address.
    pub async fn get_node_addr(&self, node_id: RaftNodeId) -> Option<String> {
        self.nodes.read().await.get(&node_id).cloned()
    }

    /// Borrow the auth keys this factory hands to outbound connections.
    pub fn auth_keys(&self) -> Arc<RaftAuthKeys> {
        self.auth_keys.clone()
    }
}

/// Forward a client write request with term validation and hop tracking.
///
/// This version includes explicit term validation and hop counting to prevent
/// the race condition where requests get forwarded to stale leaders.
pub async fn forward_client_write_with_term(
    addr: &str,
    command: CoordinationCommand,
    expected_term: u64,
    forward_hops: u8,
    auth_keys: &RaftAuthKeys,
    tls: Option<&RaftTlsConfig>,
) -> Result<CoordinationResponse, std::io::Error> {
    if forward_hops >= MAX_FORWARD_HOPS {
        return Err(std::io::Error::other(format!(
            "Forward loop detected: request exceeded {} hops, likely leadership instability",
            MAX_FORWARD_HOPS
        )));
    }

    let mut stream = connect_raft(addr, tls).await?;

    // Wrap the entire operation in a timeout
    let result = timeout(RPC_OPERATION_TIMEOUT, async {
        // Serialize the message with term validation
        let message = RaftRpcMessage::ClientWriteWithTerm {
            command,
            expected_term,
            forward_hops: forward_hops + 1, // Increment hop count
        };
        let data = postcard::to_stdvec(&message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Send length-prefixed, HMAC-signed message.
        write_rpc_frame(&mut stream, auth_keys, false, &data).await?;

        // Read response (length-prefixed and HMAC-verified, with size cap to
        // bound memory and close the unauthenticated-allocation hole).
        let (_purpose, response_buf) = read_rpc_frame(&mut stream, auth_keys).await?;

        // Deserialize response
        let response: RaftRpcResponse = postcard::from_bytes(&response_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok::<_, std::io::Error>(response)
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Forward to leader operation timeout",
        )
    })??;

    match result {
        RaftRpcResponse::ClientWriteOk(resp) => Ok(resp),
        RaftRpcResponse::Error(e) => Err(std::io::Error::other(e)),
        // Handle structured error and convert to appropriate io::Error kind
        RaftRpcResponse::ErrorV2(info) => {
            let kind = match info.kind {
                RpcErrorKind::LeadershipChanged => std::io::ErrorKind::NotConnected,
                RpcErrorKind::NotLeader { .. } => std::io::ErrorKind::NotConnected,
                RpcErrorKind::ForwardLoopDetected => std::io::ErrorKind::ConnectionRefused,
                RpcErrorKind::InvalidRequest => std::io::ErrorKind::InvalidInput,
                RpcErrorKind::Internal => std::io::ErrorKind::Other,
                RpcErrorKind::Timeout => std::io::ErrorKind::TimedOut,
                RpcErrorKind::Network => std::io::ErrorKind::ConnectionAborted,
            };
            Err(std::io::Error::new(kind, info.message))
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unexpected response type",
        )),
    }
}

/// Request to join a Raft cluster via a peer node.
///
/// Two-phase join:
/// 1. [`JoinCluster`] under the **join** HMAC purpose adds this node as a
///    learner only.
/// 2. [`PromoteMember`] under the **cluster** HMAC purpose promotes it to a
///    voter. A holder of the join token alone cannot complete phase 2.
pub async fn request_cluster_join(
    peer_addr: &str,
    node_id: RaftNodeId,
    raft_addr: &str,
    auth_keys: &RaftAuthKeys,
    tls: Option<&RaftTlsConfig>,
) -> Result<(), std::io::Error> {
    let mut stream = connect_raft(peer_addr, tls).await?;

    // Phase 1: learner membership (join purpose tag).
    let join_result = timeout(RPC_OPERATION_TIMEOUT, async {
        let message = RaftRpcMessage::JoinCluster {
            node_id,
            raft_addr: raft_addr.to_string(),
        };
        let data = postcard::to_stdvec(&message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        write_rpc_frame(&mut stream, auth_keys, true, &data).await?;
        let (_purpose, response_buf) = read_rpc_frame(&mut stream, auth_keys).await?;
        postcard::from_bytes(&response_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Join cluster operation timeout",
        )
    })??;

    match join_result {
        RaftRpcResponse::JoinClusterOk => {}
        RaftRpcResponse::Error(e) => return Err(std::io::Error::other(e)),
        RaftRpcResponse::ErrorV2(info) => return Err(rpc_error_to_io(info)),
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unexpected response type to JoinCluster",
            ));
        }
    }

    // Phase 2: voter promotion (cluster purpose tag). Reuse the same TCP
    // connection when the server supports a read loop; open a fresh one
    // otherwise so cached-client quirks don't block promotion.
    request_cluster_promote(peer_addr, node_id, auth_keys, tls).await
}

/// Promote a learner to a voting member. Requires the cluster HMAC purpose.
pub async fn request_cluster_promote(
    peer_addr: &str,
    node_id: RaftNodeId,
    auth_keys: &RaftAuthKeys,
    tls: Option<&RaftTlsConfig>,
) -> Result<(), std::io::Error> {
    let mut stream = connect_raft(peer_addr, tls).await?;

    let response: RaftRpcResponse = timeout(RPC_OPERATION_TIMEOUT, async {
        let message = RaftRpcMessage::PromoteMember { node_id };
        let data = postcard::to_stdvec(&message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        // Cluster purpose (join = false).
        write_rpc_frame(&mut stream, auth_keys, false, &data).await?;
        let (_purpose, response_buf) = read_rpc_frame(&mut stream, auth_keys).await?;
        postcard::from_bytes(&response_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    })
    .await
    .map_err(|_| {
        std::io::Error::new(
            std::io::ErrorKind::TimedOut,
            "Promote member operation timeout",
        )
    })??;

    match response {
        RaftRpcResponse::PromoteMemberOk => Ok(()),
        RaftRpcResponse::Error(e) => Err(std::io::Error::other(e)),
        RaftRpcResponse::ErrorV2(info) => Err(rpc_error_to_io(info)),
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unexpected response type to PromoteMember",
        )),
    }
}

fn rpc_error_to_io(info: RpcErrorInfo) -> std::io::Error {
    let kind = match info.kind {
        RpcErrorKind::LeadershipChanged => std::io::ErrorKind::NotConnected,
        RpcErrorKind::NotLeader { .. } => std::io::ErrorKind::NotConnected,
        RpcErrorKind::ForwardLoopDetected => std::io::ErrorKind::ConnectionRefused,
        RpcErrorKind::InvalidRequest => std::io::ErrorKind::InvalidInput,
        RpcErrorKind::Internal => std::io::ErrorKind::Other,
        RpcErrorKind::Timeout => std::io::ErrorKind::TimedOut,
        RpcErrorKind::Network => std::io::ErrorKind::ConnectionAborted,
    };
    std::io::Error::new(kind, info.message)
}

impl Default for RaftNetworkFactoryImpl {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for RaftNetworkFactoryImpl {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            auth_keys: self.auth_keys.clone(),
            tls: self.tls.clone(),
        }
    }
}

impl RaftNetworkFactory<TypeConfig> for RaftNetworkFactoryImpl {
    type Network = RaftNetworkConnection;

    async fn new_client(&mut self, target: RaftNodeId, node: &BasicNode) -> Self::Network {
        // Store the node address
        self.nodes.write().await.insert(target, node.addr.clone());

        RaftNetworkConnection {
            target_addr: node.addr.clone(),
            cached_conn: tokio::sync::Mutex::new(None),
            circuit_breaker: tokio::sync::Mutex::new(CircuitBreakerState::new()),
            auth_keys: self.auth_keys.clone(),
            tls: self.tls.clone(),
        }
    }
}

/// Circuit breaker state for a connection.
struct CircuitBreakerState {
    /// Number of consecutive failures.
    consecutive_failures: u32,
    /// When the last failure occurred.
    last_failure_time: Option<std::time::Instant>,
}

impl CircuitBreakerState {
    fn new() -> Self {
        Self {
            consecutive_failures: 0,
            last_failure_time: None,
        }
    }

    /// Check if the circuit is open (should not attempt connection).
    fn is_open(&self) -> bool {
        if self.consecutive_failures < CIRCUIT_BREAKER_THRESHOLD {
            return false;
        }
        // Check if enough time has passed for a probe attempt
        if let Some(last_failure) = self.last_failure_time {
            last_failure.elapsed() < CIRCUIT_BREAKER_RESET_TIMEOUT
        } else {
            false
        }
    }

    /// Record a successful operation (reset the circuit breaker).
    fn record_success(&mut self) {
        self.consecutive_failures = 0;
        self.last_failure_time = None;
    }

    /// Record a failure.
    fn record_failure(&mut self) {
        self.consecutive_failures += 1;
        self.last_failure_time = Some(std::time::Instant::now());
    }
}

/// A connection to a remote Raft node.
pub struct RaftNetworkConnection {
    target_addr: String,
    /// Cached socket for reuse. May be plain TCP or wrapped in TLS
    /// depending on `tls`; reusing it preserves the TLS session so we
    /// don't pay a handshake per RPC.
    cached_conn: tokio::sync::Mutex<Option<MaybeTlsStreamClient>>,
    /// Circuit breaker state for this connection.
    circuit_breaker: tokio::sync::Mutex<CircuitBreakerState>,
    /// HMAC keys used to sign every outbound frame and verify responses.
    auth_keys: Arc<RaftAuthKeys>,
    /// Optional mTLS configuration. When `Some`, every
    /// reconnect performs a TLS handshake before the first frame flows.
    tls: Option<RaftTlsConfig>,
}

impl RaftNetworkConnection {
    /// Send an RPC message and receive a response with timeout, retry logic, and circuit breaker.
    async fn send_rpc(&self, message: RaftRpcMessage) -> Result<RaftRpcResponse, std::io::Error> {
        // Check circuit breaker first
        {
            let cb = self.circuit_breaker.lock().await;
            if cb.is_open() {
                tracing::debug!(
                    target = %self.target_addr,
                    consecutive_failures = cb.consecutive_failures,
                    "Circuit breaker is open, rejecting RPC"
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    format!(
                        "Circuit breaker open for {} ({} consecutive failures)",
                        self.target_addr, cb.consecutive_failures
                    ),
                ));
            }
        }

        // Serialize the message once
        let data = postcard::to_stdvec(&message)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        // Drive retries through the unified retry framework so RPC backoff
        // matches the rest of the codebase's jittered exponential schedule.
        // Constants below preserve the prior behavior:
        // 3 retries, 100ms→2s, jitter — the framework's `Retryable` adds
        // jitter automatically. The circuit breaker still wraps the loop:
        // a failed final attempt records a failure on the per-target
        // breaker, which short-circuits subsequent calls until reset.
        let policy = backon::ExponentialBuilder::default()
            .with_min_delay(RPC_RETRY_BASE_DELAY)
            .with_max_delay(RPC_RETRY_MAX_DELAY)
            .with_max_times(RPC_MAX_RETRIES as usize)
            .with_jitter();

        let target = self.target_addr.clone();
        let result = (|| async { self.try_send_rpc(&data).await })
            .retry(policy)
            .notify(|err: &std::io::Error, dur| {
                tracing::debug!(
                    target = %target,
                    delay_ms = dur.as_millis(),
                    error = %err,
                    "Retrying RPC after backoff"
                );
            })
            .await;

        match result {
            Ok(response) => {
                // Reset circuit breaker on success
                self.circuit_breaker.lock().await.record_success();
                Ok(response)
            }
            Err(e) => {
                // All retries exhausted, record failure for circuit breaker
                let mut cb = self.circuit_breaker.lock().await;
                cb.record_failure();
                if cb.consecutive_failures >= CIRCUIT_BREAKER_THRESHOLD {
                    tracing::warn!(
                        target = %self.target_addr,
                        consecutive_failures = cb.consecutive_failures,
                        "Circuit breaker opened due to consecutive failures"
                    );
                }
                Err(e)
            }
        }
    }

    /// Attempt to send an RPC (single attempt, no retry).
    async fn try_send_rpc(&self, data: &[u8]) -> Result<RaftRpcResponse, std::io::Error> {
        // Try to reuse cached connection
        let mut guard = self.cached_conn.lock().await;
        if let Some(ref mut stream) = *guard {
            match Self::do_rpc_with_timeout(stream, data, &self.auth_keys).await {
                Ok(response) => return Ok(response),
                Err(_) => {
                    // Connection is broken, will reconnect below
                    *guard = None;
                }
            }
        }

        // Create new connection (with TLS handshake if configured).
        let mut stream = connect_raft(&self.target_addr, self.tls.as_ref()).await?;

        let response = Self::do_rpc_with_timeout(&mut stream, data, &self.auth_keys).await?;
        *guard = Some(stream);
        Ok(response)
    }

    /// Perform the RPC on an existing stream with timeout.
    async fn do_rpc_with_timeout<S: AsyncRead + AsyncWrite + Unpin>(
        stream: &mut S,
        data: &[u8],
        auth_keys: &RaftAuthKeys,
    ) -> Result<RaftRpcResponse, std::io::Error> {
        timeout(RPC_OPERATION_TIMEOUT, Self::do_rpc(stream, data, auth_keys))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "RPC operation timeout")
            })?
    }

    /// Perform the RPC on an existing stream.
    async fn do_rpc<S: AsyncRead + AsyncWrite + Unpin>(
        stream: &mut S,
        data: &[u8],
        auth_keys: &RaftAuthKeys,
    ) -> Result<RaftRpcResponse, std::io::Error> {
        // Send signed, length-prefixed message.
        write_rpc_frame(stream, auth_keys, false, data).await?;

        // Read signed response (length-prefixed, HMAC-verified, size-capped).
        let (_purpose, response_buf) = read_rpc_frame(stream, auth_keys).await?;

        // Deserialize response
        let response: RaftRpcResponse = postcard::from_bytes(&response_buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        Ok(response)
    }
}

impl RaftNetwork<TypeConfig> for RaftNetworkConnection {
    async fn append_entries(
        &mut self,
        req: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        AppendEntriesResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>,
    > {
        let response = self
            .send_rpc(RaftRpcMessage::AppendEntries(req))
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        match response {
            RaftRpcResponse::AppendEntries(resp) => Ok(resp),
            RaftRpcResponse::Error(e) => Err(RPCError::Network(
                openraft::error::NetworkError::new(&std::io::Error::other(e)),
            )),
            // Handle structured error in append_entries
            RaftRpcResponse::ErrorV2(info) => Err(RPCError::Network(
                openraft::error::NetworkError::new(&std::io::Error::other(info.message)),
            )),
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected response type"),
            ))),
        }
    }

    async fn vote(
        &mut self,
        req: VoteRequest<RaftNodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<RaftNodeId>, RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId>>>
    {
        let response = self
            .send_rpc(RaftRpcMessage::Vote(req))
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        match response {
            RaftRpcResponse::Vote(resp) => Ok(resp),
            RaftRpcResponse::Error(e) => Err(RPCError::Network(
                openraft::error::NetworkError::new(&std::io::Error::other(e)),
            )),
            // Handle structured error in vote
            RaftRpcResponse::ErrorV2(info) => Err(RPCError::Network(
                openraft::error::NetworkError::new(&std::io::Error::other(info.message)),
            )),
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected response type"),
            ))),
        }
    }

    async fn install_snapshot(
        &mut self,
        req: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<RaftNodeId>,
        RPCError<RaftNodeId, BasicNode, RaftError<RaftNodeId, InstallSnapshotError>>,
    > {
        let response = self
            .send_rpc(RaftRpcMessage::InstallSnapshot(req))
            .await
            .map_err(|e| RPCError::Network(openraft::error::NetworkError::new(&e)))?;

        match response {
            RaftRpcResponse::InstallSnapshot(resp) => Ok(resp),
            RaftRpcResponse::Error(e) => Err(RPCError::Network(
                openraft::error::NetworkError::new(&std::io::Error::other(e)),
            )),
            // Handle structured error in install_snapshot
            RaftRpcResponse::ErrorV2(info) => Err(RPCError::Network(
                openraft::error::NetworkError::new(&std::io::Error::other(info.message)),
            )),
            _ => Err(RPCError::Network(openraft::error::NetworkError::new(
                &std::io::Error::new(std::io::ErrorKind::InvalidData, "Unexpected response type"),
            ))),
        }
    }
}

/// Server for handling incoming Raft RPC requests.
pub struct RaftRpcServer {
    /// The Raft node to forward requests to.
    raft: Arc<openraft::Raft<TypeConfig>>,
    /// Address to listen on.
    listen_addr: String,
    /// Runtime handle for spawning connection handler tasks.
    runtime: tokio::runtime::Handle,
    /// HMAC keys used to verify every inbound frame and sign every response.
    auth_keys: Arc<RaftAuthKeys>,
    /// Optional mTLS configuration. When `Some`, every
    /// accepted socket performs a TLS handshake (with required client
    /// cert) before any frame bytes are read. HMAC framing still applies
    /// on top of the TLS session.
    tls: Option<RaftTlsConfig>,
}

impl RaftRpcServer {
    /// Create a new RPC server with optional mTLS. Pass
    /// `tls = None` for the legacy plain-TCP behavior.
    pub fn with_tls(
        raft: Arc<openraft::Raft<TypeConfig>>,
        listen_addr: String,
        runtime: tokio::runtime::Handle,
        auth_keys: Arc<RaftAuthKeys>,
        tls: Option<RaftTlsConfig>,
    ) -> Self {
        Self {
            raft,
            listen_addr,
            runtime,
            auth_keys,
            tls,
        }
    }

    /// Start the RPC server.
    pub async fn run(self) -> Result<(), std::io::Error> {
        use std::collections::HashMap;
        use std::net::IpAddr;
        use std::sync::Mutex as StdMutex;
        use tokio::sync::Semaphore;

        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        tracing::info!(
            addr = %self.listen_addr,
            cluster_secret = self.auth_keys.cluster_secret_configured(),
            join_token = self.auth_keys.join_token_configured(),
            tls = self.tls.is_some(),
            "Raft RPC server listening"
        );

        if !self.auth_keys.cluster_secret_configured() {
            tracing::warn!(
                addr = %self.listen_addr,
                "Raft RPC server running without RAFT_CLUSTER_SECRET — control plane is \
                 unauthenticated. Set RAFT_CLUSTER_SECRET on every node before exposing the \
                 Raft port to an untrusted network."
            );
        }

        // Bound concurrent inbound Raft RPC handshakes. Without an accept
        // semaphore an attacker that can route to the Raft port — even if
        // the HMAC gate rejects every frame — can exhaust file descriptors
        // and CPU on the TLS handshake. Per-IP cap further limits the
        // damage from a single hostile peer.
        const MAX_CONCURRENT_HANDSHAKES: usize = 128;
        const MAX_PER_IP: usize = 16;
        let accept_semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_HANDSHAKES));
        let per_ip_counts: Arc<StdMutex<HashMap<IpAddr, usize>>> =
            Arc::new(StdMutex::new(HashMap::new()));

        loop {
            let (stream, peer_addr) = listener.accept().await?;
            let raft = self.raft.clone();
            let auth_keys = self.auth_keys.clone();
            let tls = self.tls.clone();
            let accept_semaphore = accept_semaphore.clone();
            let per_ip_counts = per_ip_counts.clone();
            let peer_ip = peer_addr.ip();

            // Per-IP gate: drop the connection synchronously on overflow so
            // we never spawn a task for a known-flooding peer.
            let per_ip_admitted = {
                let mut counts = per_ip_counts.lock().unwrap_or_else(|e| e.into_inner());
                let n = counts.entry(peer_ip).or_insert(0);
                if *n >= MAX_PER_IP {
                    tracing::warn!(peer = %peer_addr, "Rejecting Raft RPC: per-IP connection cap reached");
                    false
                } else {
                    *n += 1;
                    true
                }
            };
            if !per_ip_admitted {
                continue;
            }

            self.runtime.spawn(async move {
                // Limit concurrent in-flight TLS handshakes globally.
                let _accept_permit = match accept_semaphore.acquire_owned().await {
                    Ok(p) => p,
                    Err(_) => return, // semaphore closed; runtime tearing down
                };
                // Decrement the per-IP counter when this task ends, no matter
                // how it ends (normal return, panic, drop on shutdown).
                struct PerIpGuard {
                    counts: Arc<StdMutex<HashMap<IpAddr, usize>>>,
                    ip: IpAddr,
                }
                impl Drop for PerIpGuard {
                    fn drop(&mut self) {
                        let mut c = self.counts.lock().unwrap_or_else(|e| e.into_inner());
                        if let Some(n) = c.get_mut(&self.ip) {
                            *n = n.saturating_sub(1);
                            if *n == 0 {
                                c.remove(&self.ip);
                            }
                        }
                    }
                }
                let _ip_guard = PerIpGuard {
                    counts: per_ip_counts.clone(),
                    ip: peer_ip,
                };
                let wrapped = match Self::accept_stream(stream, tls.as_ref()).await {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::warn!(peer = %peer_addr, error = %e, "Raft TLS accept failed");
                        return;
                    }
                };
                if let Err(e) = Self::handle_connection(raft, wrapped, auth_keys).await {
                    tracing::warn!(peer = %peer_addr, error = %e, "Error handling Raft RPC");
                }
            });
        }
    }

    /// Wrap an accepted TCP stream in TLS when configured. Kept as a free
    /// helper so `run` can early-return on handshake failure without
    /// dropping into the per-message handler with a dead socket.
    async fn accept_stream(
        stream: TcpStream,
        tls: Option<&RaftTlsConfig>,
    ) -> std::io::Result<MaybeTlsStreamServer> {
        #[cfg(feature = "tls")]
        {
            if let Some(t) = tls {
                let tls_stream = t.acceptor.accept(stream).await.map_err(|e| {
                    std::io::Error::new(
                        std::io::ErrorKind::ConnectionRefused,
                        format!("Raft TLS handshake failed: {}", e),
                    )
                })?;
                return Ok(MaybeTlsStreamServer::Tls(Box::new(tls_stream)));
            }
        }
        #[cfg(not(feature = "tls"))]
        let _ = tls;
        Ok(MaybeTlsStreamServer::Plain(stream))
    }

    /// Handle a connection, processing one or more RPC frames until EOF.
    ///
    /// The outbound client caches TCP connections; a one-shot read loop here
    /// caused every second RPC on a cached socket to time out. Keep reading
    /// until the peer closes the connection.
    async fn handle_connection(
        raft: Arc<openraft::Raft<TypeConfig>>,
        mut stream: MaybeTlsStreamServer,
        auth_keys: Arc<RaftAuthKeys>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            let (frame_purpose, msg_buf) = match read_rpc_frame(&mut stream, &auth_keys).await {
                Ok(v) => v,
                Err(e)
                    if e.kind() == std::io::ErrorKind::UnexpectedEof
                        || e.kind() == std::io::ErrorKind::ConnectionReset
                        || e.kind() == std::io::ErrorKind::BrokenPipe =>
                {
                    break;
                }
                Err(e) => return Err(e.into()),
            };

            let response =
                Self::dispatch_rpc_message(&raft, &auth_keys, frame_purpose, &msg_buf).await?;

            let response_data = postcard::to_stdvec(&response)?;
            write_rpc_frame(&mut stream, &auth_keys, false, &response_data).await?;
        }

        Ok(())
    }

    async fn dispatch_rpc_message(
        raft: &Arc<openraft::Raft<TypeConfig>>,
        auth_keys: &RaftAuthKeys,
        frame_purpose: super::auth::FramePurpose,
        msg_buf: &[u8],
    ) -> Result<RaftRpcResponse, Box<dyn std::error::Error + Send + Sync>> {
        // Convert deserialize failures into a typed RPC response instead of
        // bubbling them up as a connection-fatal error. The previous path
        // closed the socket without responding; clients then retried 3× with
        // backoff against a closing socket and saw only timeouts.
        let message: RaftRpcMessage = match postcard::from_bytes(msg_buf) {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!(error = %e, "Failed to deserialize Raft RPC frame");
                return Ok(RaftRpcResponse::ErrorV2(RpcErrorInfo::new(
                    RpcErrorKind::InvalidRequest,
                    format!("Malformed Raft RPC frame: {}", e),
                )));
            }
        };

        // Cross-check the wire purpose tag against the deserialized variant.
        // Join purpose → JoinCluster only. PromoteMember and ClientWrite
        // require the cluster purpose (or unauthenticated in dev).
        let is_join_message = matches!(message, RaftRpcMessage::JoinCluster { .. });
        let is_promote_message = matches!(message, RaftRpcMessage::PromoteMember { .. });
        let join_frame = matches!(frame_purpose, super::auth::FramePurpose::Join);
        if is_join_message != join_frame {
            tracing::warn!(
                purpose = ?frame_purpose,
                "Rejecting Raft RPC: wire purpose tag does not match message variant"
            );
            return Ok(RaftRpcResponse::ErrorV2(RpcErrorInfo::new(
                RpcErrorKind::InvalidRequest,
                "Wire purpose tag does not match message variant",
            )));
        }
        if is_promote_message && join_frame {
            tracing::warn!(
                node_id = ?match message {
                    RaftRpcMessage::PromoteMember { node_id } => Some(node_id),
                    _ => None,
                },
                "Rejecting PromoteMember sent under join purpose tag"
            );
            return Ok(RaftRpcResponse::ErrorV2(RpcErrorInfo::new(
                RpcErrorKind::InvalidRequest,
                "PromoteMember requires cluster HMAC purpose",
            )));
        }
        let _ = auth_keys; // used by read_rpc_frame on the caller side

        Ok(match message {
            RaftRpcMessage::AppendEntries(req) => match raft.append_entries(req).await {
                Ok(resp) => RaftRpcResponse::AppendEntries(resp),
                Err(e) => RaftRpcResponse::Error(e.to_string()),
            },
            RaftRpcMessage::Vote(req) => match raft.vote(req).await {
                Ok(resp) => RaftRpcResponse::Vote(resp),
                Err(e) => RaftRpcResponse::Error(e.to_string()),
            },
            RaftRpcMessage::InstallSnapshot(req) => match raft.install_snapshot(req).await {
                Ok(resp) => RaftRpcResponse::InstallSnapshot(resp),
                Err(e) => RaftRpcResponse::Error(e.to_string()),
            },
            RaftRpcMessage::ClientWriteWithTerm {
                command,
                expected_term,
                forward_hops,
            } => {
                // Validate term and hop count before processing
                //
                // This prevents:
                // 1. Processing requests forwarded to a stale leader
                // 2. Infinite forwarding loops during leadership instability

                // Check hop limit. Both this check and the symmetric pre-send
                // check on the client side use `>= MAX_FORWARD_HOPS` so the
                // boundary is identical: a request that has already taken
                // MAX_FORWARD_HOPS hops is rejected, never one hop earlier or
                // later. Mismatched operators previously let the server accept
                // a packet the client would refuse to send (or vice versa)
                // during a transient forwarding storm.
                if forward_hops >= MAX_FORWARD_HOPS {
                    // Use structured error type
                    RaftRpcResponse::ErrorV2(RpcErrorInfo::new(
                        RpcErrorKind::ForwardLoopDetected,
                        format!(
                            "Forward loop detected: {} hops exceeds limit of {}",
                            forward_hops, MAX_FORWARD_HOPS
                        ),
                    ))
                } else {
                    // Get current Raft state
                    let metrics = raft.metrics().borrow().clone();
                    let current_term = metrics.current_term;
                    let is_leader = metrics.current_leader == Some(metrics.id);

                    // Term validation: If expected_term > 0 and our term is higher,
                    // the request was forwarded to a stale leader view
                    if expected_term > 0 && current_term > expected_term {
                        tracing::warn!(
                            expected_term,
                            current_term,
                            forward_hops,
                            "Rejecting forwarded request: term is stale (leadership changed)"
                        );
                        // Use structured error type
                        RaftRpcResponse::ErrorV2(RpcErrorInfo::new(
                            RpcErrorKind::LeadershipChanged,
                            format!(
                                "Stale leader: expected term {} but current term is {} (leadership changed)",
                                expected_term, current_term
                            ),
                        ))
                    } else if !is_leader {
                        // If we're not the leader, reject rather than forward to prevent loops
                        // The original sender should retry with fresh leader info
                        let leader_hint = metrics.current_leader;
                        let leader_info = leader_hint
                            .map(|id| format!("leader is node {}", id))
                            .unwrap_or_else(|| "no leader elected".to_string());
                        tracing::debug!(
                            forward_hops,
                            is_leader,
                            "Rejecting forwarded request: this node is not the leader ({})",
                            leader_info
                        );
                        // Use structured error type with leader hint
                        RaftRpcResponse::ErrorV2(RpcErrorInfo::new(
                            RpcErrorKind::NotLeader { leader_hint },
                            format!(
                                "ForwardToLeader: this node is not the leader, {}",
                                leader_info
                            ),
                        ))
                    } else {
                        // We're the leader with valid term - process the write
                        match raft.client_write(command).await {
                            Ok(resp) => RaftRpcResponse::ClientWriteOk(resp.data),
                            // Use structured error type for internal errors
                            Err(e) => RaftRpcResponse::ErrorV2(RpcErrorInfo::internal(e)),
                        }
                    }
                }
            }
            RaftRpcMessage::JoinCluster { node_id, raft_addr } => {
                tracing::info!(
                    node_id = node_id,
                    raft_addr = %raft_addr,
                    "Received join cluster request (learner only)"
                );

                match Self::handle_join_cluster(raft, node_id, raft_addr).await {
                    Ok(()) => RaftRpcResponse::JoinClusterOk,
                    Err(e) => RaftRpcResponse::Error(e),
                }
            }
            RaftRpcMessage::PromoteMember { node_id } => {
                tracing::info!(node_id = node_id, "Received promote member request");

                match Self::handle_promote_member(raft, node_id).await {
                    Ok(()) => RaftRpcResponse::PromoteMemberOk,
                    Err(e) => RaftRpcResponse::Error(e),
                }
            }
        })
    }

    /// Add a node as a learner. Voter promotion is a separate
    /// [`Self::handle_promote_member`] step gated on the cluster secret.
    async fn handle_join_cluster(
        raft: &Arc<openraft::Raft<TypeConfig>>,
        node_id: RaftNodeId,
        raft_addr: String,
    ) -> Result<(), String> {
        // Idempotency is checked structurally against the current membership
        // before issuing the change. Inspecting the openraft error string
        // would couple us to upstream wording and silently break on a minor
        // crate bump.
        {
            let metrics = raft.metrics().borrow().clone();
            let membership = metrics.membership_config.membership();
            if membership.get_node(&node_id).is_some() {
                tracing::info!(node_id = node_id, "Node already in cluster");
                return Ok(());
            }
        }
        let node = BasicNode { addr: raft_addr };

        match raft.add_learner(node_id, node, true).await {
            Ok(_) => {
                tracing::info!(node_id = node_id, "Added node as learner");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(node_id = node_id, error = %e, "Failed to add learner");
                Err(format!("Failed to add learner: {}", e))
            }
        }
    }

    /// Promote a learner to a voting member. Leader-only.
    async fn handle_promote_member(
        raft: &Arc<openraft::Raft<TypeConfig>>,
        node_id: RaftNodeId,
    ) -> Result<(), String> {
        let metrics = raft.metrics().borrow().clone();
        if metrics.current_leader != Some(metrics.id) {
            return Err("PromoteMember rejected: this node is not the Raft leader".to_string());
        }

        let mut voters: std::collections::BTreeSet<RaftNodeId> =
            metrics.membership_config.membership().voter_ids().collect();
        if voters.contains(&node_id) {
            tracing::info!(node_id = node_id, "Node already a voter");
            return Ok(());
        }
        voters.insert(node_id);

        match raft.change_membership(voters, false).await {
            Ok(_) => {
                tracing::info!(node_id = node_id, "Promoted node to voter");
                Ok(())
            }
            Err(e) => {
                tracing::warn!(node_id = node_id, error = %e, "Failed to promote to voter");
                Err(format!("Failed to promote to voter: {}", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_raft_network_factory_new() {
        let factory = RaftNetworkFactoryImpl::new();
        // Factory should be created with empty nodes
        let nodes = factory.nodes.try_read().unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_raft_network_factory_default() {
        let factory = RaftNetworkFactoryImpl::default();
        // Default should create empty factory
        let nodes = factory.nodes.try_read().unwrap();
        assert!(nodes.is_empty());
    }

    #[test]
    fn test_raft_network_factory_clone() {
        let factory = RaftNetworkFactoryImpl::new();
        let cloned = factory.clone();
        // Clone should share the same Arc
        assert!(Arc::ptr_eq(&factory.nodes, &cloned.nodes));
    }

    #[tokio::test]
    async fn test_raft_network_factory_add_node() {
        let factory = RaftNetworkFactoryImpl::new();

        factory.add_node(1, "127.0.0.1:9093".to_string()).await;
        factory.add_node(2, "127.0.0.1:9094".to_string()).await;

        let nodes = factory.nodes.read().await;
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes.get(&1), Some(&"127.0.0.1:9093".to_string()));
        assert_eq!(nodes.get(&2), Some(&"127.0.0.1:9094".to_string()));
    }

    #[tokio::test]
    async fn test_raft_network_factory_update_node() {
        let factory = RaftNetworkFactoryImpl::new();

        // Add a node
        factory.add_node(1, "127.0.0.1:9093".to_string()).await;

        // Update the same node
        factory.add_node(1, "192.168.1.1:9093".to_string()).await;

        let nodes = factory.nodes.read().await;
        assert_eq!(nodes.len(), 1);
        assert_eq!(nodes.get(&1), Some(&"192.168.1.1:9093".to_string()));
    }

    #[test]
    fn test_raft_rpc_message_serialization() {
        // Test that messages can be serialized and deserialized
        let vote_msg = RaftRpcMessage::Vote(openraft::raft::VoteRequest {
            vote: openraft::Vote::new(1, 42),
            last_log_id: None,
        });

        let serialized = postcard::to_stdvec(&vote_msg).unwrap();
        let deserialized: RaftRpcMessage = postcard::from_bytes(&serialized).unwrap();

        match deserialized {
            RaftRpcMessage::Vote(req) => {
                assert_eq!(req.vote.leader_id().voted_for(), Some(42));
            }
            _ => panic!("Expected Vote message"),
        }
    }

    #[test]
    fn test_raft_rpc_response_serialization() {
        // Test Error response serialization
        let error_resp = RaftRpcResponse::Error("test error".to_string());
        let serialized = postcard::to_stdvec(&error_resp).unwrap();
        let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

        match deserialized {
            RaftRpcResponse::Error(msg) => {
                assert_eq!(msg, "test error");
            }
            _ => panic!("Expected Error response"),
        }
    }

    #[test]
    fn test_raft_rpc_message_debug() {
        let vote_msg = RaftRpcMessage::Vote(openraft::raft::VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        });

        let debug_str = format!("{:?}", vote_msg);
        assert!(debug_str.contains("Vote"));
    }

    #[test]
    fn test_raft_rpc_response_debug() {
        let error_resp = RaftRpcResponse::Error("debug test".to_string());
        let debug_str = format!("{:?}", error_resp);
        assert!(debug_str.contains("Error"));
        assert!(debug_str.contains("debug test"));
    }

    #[tokio::test]
    async fn test_new_client_stores_node() {
        use openraft::BasicNode;
        use openraft::network::RaftNetworkFactory;

        let mut factory = RaftNetworkFactoryImpl::new();
        let node = BasicNode::new("127.0.0.1:9999");

        let _connection = factory.new_client(42, &node).await;

        // Verify node was stored
        let nodes = factory.nodes.read().await;
        assert_eq!(nodes.get(&42), Some(&"127.0.0.1:9999".to_string()));
    }

    #[test]
    fn test_raft_rpc_message_clone() {
        let msg = RaftRpcMessage::Vote(openraft::raft::VoteRequest {
            vote: openraft::Vote::new(1, 1),
            last_log_id: None,
        });

        let cloned = msg.clone();
        match (msg, cloned) {
            (RaftRpcMessage::Vote(orig), RaftRpcMessage::Vote(clone)) => {
                assert_eq!(
                    orig.vote.leader_id().voted_for(),
                    clone.vote.leader_id().voted_for()
                );
            }
            _ => panic!("Clone should match"),
        }
    }

    #[test]
    fn test_vote_response_serialization() {
        use openraft::Vote;
        use openraft::raft::VoteResponse;

        let vote_resp = RaftRpcResponse::Vote(VoteResponse {
            vote: Vote::new(1, 1),
            vote_granted: true,
            last_log_id: None,
        });

        let serialized = postcard::to_stdvec(&vote_resp).unwrap();
        let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

        match deserialized {
            RaftRpcResponse::Vote(resp) => {
                assert!(resp.vote_granted);
            }
            _ => panic!("Expected Vote response"),
        }
    }

    #[test]
    fn test_append_entries_response_serialization() {
        use openraft::raft::AppendEntriesResponse;

        // AppendEntriesResponse is an enum in openraft 0.9
        let append_resp = RaftRpcResponse::AppendEntries(AppendEntriesResponse::Success);

        let serialized = postcard::to_stdvec(&append_resp).unwrap();
        let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

        match deserialized {
            RaftRpcResponse::AppendEntries(AppendEntriesResponse::Success) => {
                // Success - AppendEntries response serialized and deserialized correctly
            }
            _ => panic!("Expected AppendEntries Success response"),
        }
    }

    #[test]
    fn test_install_snapshot_response_serialization() {
        use openraft::raft::InstallSnapshotResponse;

        let snapshot_resp = RaftRpcResponse::InstallSnapshot(InstallSnapshotResponse {
            vote: openraft::Vote::new(1, 1),
        });

        let serialized = postcard::to_stdvec(&snapshot_resp).unwrap();
        let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

        match deserialized {
            RaftRpcResponse::InstallSnapshot(resp) => {
                assert_eq!(resp.vote.leader_id().voted_for(), Some(1));
            }
            _ => panic!("Expected InstallSnapshot response"),
        }
    }

    #[tokio::test]
    async fn test_multiple_nodes_ordering() {
        let factory = RaftNetworkFactoryImpl::new();

        // Add nodes in various order
        factory.add_node(5, "host5:9093".to_string()).await;
        factory.add_node(1, "host1:9093".to_string()).await;
        factory.add_node(3, "host3:9093".to_string()).await;

        let nodes = factory.nodes.read().await;

        // BTreeMap maintains sorted order
        let keys: Vec<_> = nodes.keys().collect();
        assert_eq!(keys, vec![&1, &3, &5]);
    }

    // ========================================================================
    // RpcErrorKind Tests
    // ========================================================================

    #[test]
    fn test_rpc_error_kind_is_retryable() {
        // Retryable errors
        assert!(RpcErrorKind::LeadershipChanged.is_retryable());
        assert!(RpcErrorKind::NotLeader { leader_hint: None }.is_retryable());
        assert!(
            RpcErrorKind::NotLeader {
                leader_hint: Some(42)
            }
            .is_retryable()
        );
        assert!(RpcErrorKind::Internal.is_retryable());
        assert!(RpcErrorKind::Timeout.is_retryable());
        assert!(RpcErrorKind::Network.is_retryable());

        // Non-retryable errors
        assert!(!RpcErrorKind::ForwardLoopDetected.is_retryable());
        assert!(!RpcErrorKind::InvalidRequest.is_retryable());
    }

    #[test]
    fn test_rpc_error_kind_should_refresh_leader() {
        // Should refresh leader
        assert!(RpcErrorKind::LeadershipChanged.should_refresh_leader());
        assert!(RpcErrorKind::NotLeader { leader_hint: None }.should_refresh_leader());
        assert!(
            RpcErrorKind::NotLeader {
                leader_hint: Some(1)
            }
            .should_refresh_leader()
        );

        // Should not refresh leader
        assert!(!RpcErrorKind::ForwardLoopDetected.should_refresh_leader());
        assert!(!RpcErrorKind::InvalidRequest.should_refresh_leader());
        assert!(!RpcErrorKind::Internal.should_refresh_leader());
        assert!(!RpcErrorKind::Timeout.should_refresh_leader());
        assert!(!RpcErrorKind::Network.should_refresh_leader());
    }

    #[test]
    fn test_rpc_error_kind_serialization() {
        let kinds = [
            RpcErrorKind::LeadershipChanged,
            RpcErrorKind::NotLeader { leader_hint: None },
            RpcErrorKind::NotLeader {
                leader_hint: Some(42),
            },
            RpcErrorKind::ForwardLoopDetected,
            RpcErrorKind::InvalidRequest,
            RpcErrorKind::Internal,
            RpcErrorKind::Timeout,
            RpcErrorKind::Network,
        ];

        for kind in kinds {
            let serialized = postcard::to_stdvec(&kind).unwrap();
            let deserialized: RpcErrorKind = postcard::from_bytes(&serialized).unwrap();

            // Verify round-trip preserves retryable semantics
            assert_eq!(
                kind.is_retryable(),
                deserialized.is_retryable(),
                "Retryable semantics should be preserved for {:?}",
                kind
            );
            assert_eq!(
                kind.should_refresh_leader(),
                deserialized.should_refresh_leader(),
                "Refresh leader semantics should be preserved for {:?}",
                kind
            );
        }
    }

    #[test]
    fn test_rpc_error_kind_debug() {
        assert!(format!("{:?}", RpcErrorKind::LeadershipChanged).contains("LeadershipChanged"));
        assert!(
            format!(
                "{:?}",
                RpcErrorKind::NotLeader {
                    leader_hint: Some(5)
                }
            )
            .contains("5")
        );
        assert!(format!("{:?}", RpcErrorKind::ForwardLoopDetected).contains("ForwardLoopDetected"));
    }

    // ========================================================================
    // RpcErrorInfo Tests
    // ========================================================================

    #[test]
    fn test_rpc_error_info_new() {
        let error = RpcErrorInfo::new(RpcErrorKind::Timeout, "Connection timed out");
        assert!(matches!(error.kind, RpcErrorKind::Timeout));
        assert_eq!(error.message, "Connection timed out");
    }

    #[test]
    fn test_rpc_error_info_internal() {
        let error = RpcErrorInfo::internal("Something went wrong");
        assert!(matches!(error.kind, RpcErrorKind::Internal));
        assert_eq!(error.message, "Something went wrong");
    }

    #[test]
    fn test_rpc_error_info_internal_from_error() {
        let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
        let error = RpcErrorInfo::internal(io_error);
        assert!(matches!(error.kind, RpcErrorKind::Internal));
        assert!(error.message.contains("File not found"));
    }

    #[test]
    fn test_rpc_error_info_serialization() {
        let error = RpcErrorInfo::new(
            RpcErrorKind::NotLeader {
                leader_hint: Some(3),
            },
            "Not the leader",
        );

        let serialized = postcard::to_stdvec(&error).unwrap();
        let deserialized: RpcErrorInfo = postcard::from_bytes(&serialized).unwrap();

        assert!(matches!(
            deserialized.kind,
            RpcErrorKind::NotLeader {
                leader_hint: Some(3)
            }
        ));
        assert_eq!(deserialized.message, "Not the leader");
    }

    #[test]
    fn test_rpc_error_info_debug() {
        let error = RpcErrorInfo::new(RpcErrorKind::Network, "Connection refused");
        let debug_str = format!("{:?}", error);
        assert!(debug_str.contains("Network"));
        assert!(debug_str.contains("Connection refused"));
    }

    // ========================================================================
    // CircuitBreakerState Tests
    // ========================================================================

    #[test]
    fn test_circuit_breaker_new_is_closed() {
        let cb = CircuitBreakerState::new();
        assert!(!cb.is_open(), "New circuit breaker should be closed");
        assert_eq!(cb.consecutive_failures, 0);
        assert!(cb.last_failure_time.is_none());
    }

    #[test]
    fn test_circuit_breaker_stays_closed_below_threshold() {
        let mut cb = CircuitBreakerState::new();

        // Record failures below threshold
        for i in 0..(CIRCUIT_BREAKER_THRESHOLD - 1) {
            cb.record_failure();
            assert!(
                !cb.is_open(),
                "Circuit should stay closed at {} failures",
                i + 1
            );
        }
    }

    #[test]
    fn test_circuit_breaker_opens_at_threshold() {
        let mut cb = CircuitBreakerState::new();

        // Record failures until threshold
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            cb.record_failure();
        }

        assert!(
            cb.is_open(),
            "Circuit should open at {} failures",
            CIRCUIT_BREAKER_THRESHOLD
        );
    }

    #[test]
    fn test_circuit_breaker_success_resets() {
        let mut cb = CircuitBreakerState::new();

        // Accumulate some failures
        for _ in 0..3 {
            cb.record_failure();
        }
        assert_eq!(cb.consecutive_failures, 3);

        // Success resets the counter
        cb.record_success();
        assert_eq!(cb.consecutive_failures, 0);
        assert!(cb.last_failure_time.is_none());
        assert!(!cb.is_open());
    }

    #[test]
    fn test_circuit_breaker_resets_after_timeout() {
        let mut cb = CircuitBreakerState::new();

        // Open the circuit breaker
        for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
            cb.record_failure();
        }
        assert!(cb.is_open());

        // Simulate time passing (beyond reset timeout)
        // We can't easily simulate time in tests, but we can test the logic
        // by setting last_failure_time to a past instant
        cb.last_failure_time = Some(
            std::time::Instant::now() - CIRCUIT_BREAKER_RESET_TIMEOUT - Duration::from_secs(1),
        );

        // Now the circuit should allow a probe (is_open returns false)
        assert!(
            !cb.is_open(),
            "Circuit should allow probe after reset timeout"
        );
    }

    #[test]
    fn test_circuit_breaker_failure_count_accumulates() {
        let mut cb = CircuitBreakerState::new();

        cb.record_failure();
        assert_eq!(cb.consecutive_failures, 1);

        cb.record_failure();
        assert_eq!(cb.consecutive_failures, 2);

        cb.record_failure();
        assert_eq!(cb.consecutive_failures, 3);
    }

    #[test]
    fn test_circuit_breaker_no_last_failure_time_is_closed() {
        let mut cb = CircuitBreakerState::new();
        cb.consecutive_failures = CIRCUIT_BREAKER_THRESHOLD + 10;
        cb.last_failure_time = None;

        // Even with high failure count, no last_failure_time means closed
        assert!(!cb.is_open());
    }

    // ========================================================================
    // ClientWriteWithTerm Message Tests
    // ========================================================================

    #[test]
    fn test_client_write_with_term_serialization() {
        use super::super::{CoordinationCommand, domains};

        let msg = RaftRpcMessage::ClientWriteWithTerm {
            command: CoordinationCommand::BrokerDomain(domains::BrokerCommand::Heartbeat {
                broker_id: 1,
                timestamp_ms: 123456,
                reported_local_timestamp_ms: 123456,
            }),
            expected_term: 42,
            forward_hops: 2,
        };

        let serialized = postcard::to_stdvec(&msg).unwrap();
        let deserialized: RaftRpcMessage = postcard::from_bytes(&serialized).unwrap();

        match deserialized {
            RaftRpcMessage::ClientWriteWithTerm {
                expected_term,
                forward_hops,
                ..
            } => {
                assert_eq!(expected_term, 42);
                assert_eq!(forward_hops, 2);
            }
            _ => panic!("Expected ClientWriteWithTerm message"),
        }
    }

    #[test]
    fn test_join_cluster_message_serialization() {
        let msg = RaftRpcMessage::JoinCluster {
            node_id: 99,
            raft_addr: "192.168.1.100:9093".to_string(),
        };

        let serialized = postcard::to_stdvec(&msg).unwrap();
        let deserialized: RaftRpcMessage = postcard::from_bytes(&serialized).unwrap();

        match deserialized {
            RaftRpcMessage::JoinCluster { node_id, raft_addr } => {
                assert_eq!(node_id, 99);
                assert_eq!(raft_addr, "192.168.1.100:9093");
            }
            _ => panic!("Expected JoinCluster message"),
        }
    }

    // ========================================================================
    // ErrorV2 Response Tests
    // ========================================================================

    #[test]
    fn test_error_v2_response_serialization() {
        let error_info = RpcErrorInfo::new(RpcErrorKind::LeadershipChanged, "Term changed");
        let resp = RaftRpcResponse::ErrorV2(error_info);

        let serialized = postcard::to_stdvec(&resp).unwrap();
        let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

        match deserialized {
            RaftRpcResponse::ErrorV2(info) => {
                assert!(matches!(info.kind, RpcErrorKind::LeadershipChanged));
                assert_eq!(info.message, "Term changed");
            }
            _ => panic!("Expected ErrorV2 response"),
        }
    }

    #[test]
    fn test_join_cluster_ok_response_serialization() {
        let resp = RaftRpcResponse::JoinClusterOk;

        let serialized = postcard::to_stdvec(&resp).unwrap();
        let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

        assert!(matches!(deserialized, RaftRpcResponse::JoinClusterOk));
    }

    // ========================================================================
    // Constants Tests
    // ========================================================================

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_max_forward_hops_is_reasonable() {
        // MAX_FORWARD_HOPS should be small to prevent loops but allow some forwarding
        assert!(MAX_FORWARD_HOPS >= 1, "Should allow at least 1 hop");
        assert!(MAX_FORWARD_HOPS <= 10, "Should prevent runaway forwarding");
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_circuit_breaker_threshold_is_reasonable() {
        // Threshold should be high enough to tolerate transient failures
        assert!(
            CIRCUIT_BREAKER_THRESHOLD >= 3,
            "Should tolerate some failures"
        );
        assert!(
            CIRCUIT_BREAKER_THRESHOLD <= 20,
            "Should not require too many failures"
        );
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_timeouts_are_reasonable() {
        assert!(
            RPC_CONNECT_TIMEOUT >= Duration::from_secs(1),
            "Connect timeout should be at least 1s"
        );
        assert!(
            RPC_OPERATION_TIMEOUT >= Duration::from_secs(5),
            "Operation timeout should be at least 5s"
        );
        assert!(
            RPC_OPERATION_TIMEOUT >= RPC_CONNECT_TIMEOUT,
            "Operation timeout should be >= connect timeout"
        );
    }

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_retry_delays_are_reasonable() {
        assert!(
            RPC_RETRY_BASE_DELAY >= Duration::from_millis(10),
            "Base delay should be at least 10ms"
        );
        assert!(
            RPC_RETRY_MAX_DELAY >= RPC_RETRY_BASE_DELAY,
            "Max delay should be >= base delay"
        );
        assert!(RPC_MAX_RETRIES >= 1, "Should allow at least 1 retry");
    }

    // ========================================================================
    // Get Node Address Tests
    // ========================================================================

    #[tokio::test]
    async fn test_get_node_addr_existing() {
        let factory = RaftNetworkFactoryImpl::new();
        factory.add_node(1, "127.0.0.1:9093".to_string()).await;

        let addr = factory.get_node_addr(1).await;
        assert_eq!(addr, Some("127.0.0.1:9093".to_string()));
    }

    #[tokio::test]
    async fn test_get_node_addr_nonexistent() {
        let factory = RaftNetworkFactoryImpl::new();

        let addr = factory.get_node_addr(999).await;
        assert_eq!(addr, None);
    }

    // ========================================================================
    // RpcErrorKind Clone Tests
    // ========================================================================

    #[test]
    fn test_rpc_error_kind_clone() {
        let original = RpcErrorKind::NotLeader {
            leader_hint: Some(42),
        };
        let cloned = original.clone();

        match (original, cloned) {
            (
                RpcErrorKind::NotLeader {
                    leader_hint: Some(a),
                },
                RpcErrorKind::NotLeader {
                    leader_hint: Some(b),
                },
            ) => {
                assert_eq!(a, b);
            }
            _ => panic!("Clone should preserve variant"),
        }
    }

    #[test]
    fn test_rpc_error_info_clone() {
        let original = RpcErrorInfo::new(RpcErrorKind::Timeout, "Test message");
        let cloned = original.clone();

        assert!(matches!(cloned.kind, RpcErrorKind::Timeout));
        assert_eq!(cloned.message, "Test message");
    }
}
