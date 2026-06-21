//! Network primitives shared by the multiplexed Raft RPC layer.
//!
//! This module is the home of the TCP / TLS scaffolding the
//! `mux_client` and `mux_server` siblings build on top of: the
//! `MaybeTlsStream*` socket wrappers, the connect / accept helpers, the
//! HMAC + frame timing constants, and the structured RPC error type that
//! travels back to the caller.
//!
//! When `RaftConfig.tls` is `Some`, the underlying TCP socket is wrapped
//! in a rustls `TlsStream` before any frame bytes flow. The HMAC layer
//! remains in place on top — TLS adds peer identity (mTLS) and
//! encryption, HMAC adds replay/integrity defense.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio::time::timeout;

use super::tls::RaftTlsConfig;
use super::types::RaftNodeId;

/// Timeout for RPC connection establishment.
const RPC_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// Idle timeout on a server-side per-connection read loop. Without this an
/// authenticated peer (or anyone, in dev mode) could hold a connection slot
/// indefinitely between frames, slowloris-style. Long enough to absorb normal
/// inter-frame gaps but short enough to free slots after real disconnects.
pub(super) const RPC_FRAME_IDLE_TIMEOUT: Duration = Duration::from_secs(60);

/// Maximum number of hops for forwarded requests to prevent loops.
///
/// Visible to sibling modules (`mux_server`, `mux_client`) so the dispatcher
/// applies the identical hop bound on both ends. A drift between the
/// two cap values would let a request one side refuses sail through the
/// other.
pub(super) const MAX_FORWARD_HOPS: u8 = 3;

// ============================================================================
// Server-side TLS-or-plain socket wrapper.
// ============================================================================

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

// ============================================================================
// Client-side TLS-or-plain socket wrapper.
// ============================================================================

/// Client-side counterpart to [`MaybeTlsStreamServer`]. Holds whatever
/// `TlsConnector::connect` produced when TLS is configured, or a raw
/// `TcpStream` otherwise. Used as the cached connection type on the mux
/// per-peer client so reused outbound sockets keep their TLS session.
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
///
/// Visible to sibling modules (`mux_client`) so the multiplexed client
/// reuses the exact same connect path.
pub(super) async fn connect_raft(
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

/// Wrap an inbound TCP stream in TLS when configured.
///
/// `pub(super)` so the multiplexed accept loop in `mux_server` shares the
/// same handshake error mapping.
pub(super) async fn accept_raft_stream(
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

// ============================================================================
// Structured RPC error — surfaces retry semantics from the wire.
// ============================================================================

/// RPC error categories with retry semantics. Carried inside
/// `MuxRaftRpcResponse::Error` so the client can decide whether to retry
/// against the same peer, refresh leader info, or fail fast.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum RpcErrorKind {
    /// Leader changed during request processing (term moved on); retry.
    LeadershipChanged,
    /// This node is not the leader; carries an optional hint to refresh.
    NotLeader { leader_hint: Option<RaftNodeId> },
    /// Forward-loop guard tripped; bail to break the loop.
    ForwardLoopDetected,
    /// Malformed request — don't retry.
    InvalidRequest,
    /// Internal error on the receiver — may be transient, can retry.
    Internal,
    /// Operation timed out — may be transient.
    Timeout,
    /// Network error — may be transient.
    Network,
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
