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

use super::auth::{
    MAX_RPC_MESSAGE_BYTES, PeerScope, RaftAuthKeys, read_rpc_frame, write_rpc_frame,
};
use super::commands::{CoordinationCommand, CoordinationResponse};
use super::tls::RaftTlsConfig;
use super::types::{RaftNodeId, TypeConfig};

/// Timeout for RPC connection establishment.
const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout for RPC read/write operations.
const RPC_OPERATION_TIMEOUT: Duration = Duration::from_secs(10);

/// Idle timeout on a server-side per-connection read loop. Without this an
/// authenticated peer (or anyone, in dev mode) could hold a connection slot
/// indefinitely between frames, slowloris-style. Long enough to absorb normal
/// inter-frame gaps but short enough to free slots after real disconnects.
const RPC_FRAME_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

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
        let (_purpose, response_buf) = read_rpc_frame(
            &mut stream,
            auth_keys,
            &PeerScope::from_addr_str(addr),
            MAX_RPC_MESSAGE_BYTES,
        )
        .await?;

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
        let (_purpose, response_buf) = read_rpc_frame(
            &mut stream,
            auth_keys,
            &PeerScope::from_addr_str(peer_addr),
            MAX_RPC_MESSAGE_BYTES,
        )
        .await?;
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
        let (_purpose, response_buf) = read_rpc_frame(
            &mut stream,
            auth_keys,
            &PeerScope::from_addr_str(peer_addr),
            MAX_RPC_MESSAGE_BYTES,
        )
        .await?;
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
            control_conn: tokio::sync::Mutex::new(None),
            bulk_conn: tokio::sync::Mutex::new(None),
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
///
/// Maintains two independent connection lanes to the same peer so that a
/// large `InstallSnapshot` upload (multi-second TCP write) does not
/// serialize behind the same socket as low-latency `AppendEntries` /
/// `Vote` traffic. Without separation, a snapshot install can starve
/// heartbeats long enough to trigger an election (the slow snapshot held
/// the only `cached_conn` mutex while heartbeats blocked behind it).
pub struct RaftNetworkConnection {
    target_addr: String,
    /// Control-plane lane: AppendEntries (heartbeat + log replication) and
    /// Vote. These RPCs are small and latency-sensitive — the leader's
    /// liveness signal flows here, so it must never block on bulk
    /// transfers.
    control_conn: tokio::sync::Mutex<Option<MaybeTlsStreamClient>>,
    /// Bulk lane: InstallSnapshot. Snapshots are large and slow; isolating
    /// them keeps their TCP write window from monopolizing a single
    /// cached socket. A separate connection also lets the kernel TCP
    /// stack maintain independent congestion windows for each class.
    bulk_conn: tokio::sync::Mutex<Option<MaybeTlsStreamClient>>,
    /// Circuit breaker state for this connection.
    circuit_breaker: tokio::sync::Mutex<CircuitBreakerState>,
    /// HMAC keys used to sign every outbound frame and verify responses.
    auth_keys: Arc<RaftAuthKeys>,
    /// Optional mTLS configuration. When `Some`, every
    /// reconnect performs a TLS handshake before the first frame flows.
    tls: Option<RaftTlsConfig>,
}

/// Priority class for an outbound Raft RPC. Picks which cached connection
/// `try_send_rpc` reuses, so bulk traffic doesn't serialize behind
/// control-plane traffic on the same socket.
#[derive(Debug, Clone, Copy)]
enum RpcLane {
    Control,
    Bulk,
}

impl RaftNetworkConnection {
    /// Send an RPC message and receive a response with timeout, retry logic, and circuit breaker.
    async fn send_rpc(&self, message: RaftRpcMessage) -> Result<RaftRpcResponse, std::io::Error> {
        let lane = match &message {
            RaftRpcMessage::InstallSnapshot(_) => RpcLane::Bulk,
            _ => RpcLane::Control,
        };
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
        let result = (|| async { self.try_send_rpc(&data, lane).await })
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
    ///
    /// Cancellation safety: the previous version held the lock
    /// across the RPC and left a half-written stream in the cell if the outer
    /// future was cancelled mid-call. The next caller would then read another
    /// caller's response bytes — desyncing the postcard framing and silently
    /// deserializing, e.g., a Vote reply as an AppendEntries reply.
    ///
    /// The fix is a take-then-commit pattern: we *take* the cached stream out
    /// of the cell before calling `do_rpc`. If the call succeeds we *commit*
    /// (put it back). If the call fails or the future is cancelled at any
    /// await point, the local `stream` is dropped along with the future, the
    /// TCP connection closes, and the cell stays empty — the next caller
    /// always reconnects from scratch rather than picking up a half-state
    /// stream.
    ///
    /// We still hold `cached_conn.lock()` across the RPC, so concurrent
    /// callers serialize on the same target — `do_rpc` would otherwise
    /// interleave bytes on a single TCP socket.
    async fn try_send_rpc(
        &self,
        data: &[u8],
        lane: RpcLane,
    ) -> Result<RaftRpcResponse, std::io::Error> {
        // Pick the lane-appropriate cached connection. Heartbeat / log
        // replication and snapshot install lock independent mutexes, so a
        // multi-second snapshot upload cannot stall a heartbeat.
        let cell = match lane {
            RpcLane::Control => &self.control_conn,
            RpcLane::Bulk => &self.bulk_conn,
        };
        let mut guard = cell.lock().await;

        // Acquire ownership of the stream (cached or freshly connected). The
        // cell goes to None for the duration; cancellation between any await
        // below leaves it None, which is exactly what we want.
        let mut stream = match guard.take() {
            Some(s) => s,
            None => connect_raft(&self.target_addr, self.tls.as_ref()).await?,
        };

        // Cancellation point: the cell is None, the stream is owned locally.
        // If we're cancelled here the stream is dropped on unwind.
        match Self::do_rpc_with_timeout(&mut stream, data, &self.auth_keys).await {
            Ok(response) => {
                // Commit — return the stream to the cell so the next caller
                // can reuse it. Reaching this point means the response was
                // fully read; the stream is in a clean state.
                *guard = Some(stream);
                Ok(response)
            }
            Err(e) => {
                // RPC failed; the stream may have a partial frame or pending
                // bytes. Drop it (do not commit) so the next caller starts
                // from a fresh TCP connection.
                Err(e)
            }
        }
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
        let (_purpose, response_buf) = read_rpc_frame(
            stream,
            auth_keys,
            &PeerScope::unknown(),
            MAX_RPC_MESSAGE_BYTES,
        )
        .await?;

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
    ///
    /// Stops accepting new connections when `shutdown` fires. In-flight
    /// connection tasks continue until they finish naturally.
    pub async fn run(
        self,
        mut shutdown: tokio::sync::broadcast::Receiver<()>,
    ) -> Result<(), std::io::Error> {
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
            tokio::select! {
                biased;
                _ = shutdown.recv() => {
                    tracing::info!(addr = %self.listen_addr, "Raft RPC server shutting down");
                    return Ok(());
                }
                accept_result = listener.accept() => {
                    let (stream, peer_addr) = match accept_result {
                        Ok(v) => v,
                        Err(e) => return Err(e),
                    };
                    let raft = self.raft.clone();
                    let auth_keys = self.auth_keys.clone();
                    let tls = self.tls.clone();
                    let accept_semaphore = accept_semaphore.clone();
                    let per_ip_counts = per_ip_counts.clone();
                    let peer_ip = peer_addr.ip();

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
                        let _accept_permit = match accept_semaphore.acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => return,
                        };
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
                        if let Err(e) = Self::handle_connection(raft, wrapped, auth_keys, PeerScope::from_ip(peer_ip)).await {
                            tracing::warn!(peer = %peer_addr, error = %e, "Error handling Raft RPC");
                        }
                    });
                }
            }
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
        peer: PeerScope,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        loop {
            // Wrap the per-frame read in an idle timeout. A peer that goes
            // silent (slowloris, half-open TCP, missed FIN) is dropped
            // instead of pinning a connection slot forever.
            let read_result = match timeout(
                RPC_FRAME_IDLE_TIMEOUT,
                read_rpc_frame(&mut stream, &auth_keys, &peer, MAX_RPC_MESSAGE_BYTES),
            )
            .await
            {
                Ok(r) => r,
                Err(_) => {
                    tracing::debug!("Closing idle Raft RPC connection");
                    break;
                }
            };

            let (frame_purpose, msg_buf) = match read_result {
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
#[path = "network_tests.rs"]
mod tests;
