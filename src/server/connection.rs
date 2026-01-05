//! Client connection handling for Kafka server.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes};
use tokio::net::TcpStream;
use tokio::time::timeout;

#[cfg(feature = "tls")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[cfg(feature = "tls")]
use tokio_rustls::server::TlsStream;

use crate::constants::{DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS, DEFAULT_REQUEST_READ_TIMEOUT_SECS};
use crate::error::{Error, KafkaCode, Result};

use super::handler::{Handler, RequestContext};
use super::rate_limiter::AuthRateLimiter;
use super::request::Request;
use super::response::Response;

use crate::encode::ToByte;

use std::time::Instant;

/// Default maximum allowed message size (100 MB).
/// This prevents memory exhaustion from malicious or malformed messages.
/// Can be overridden via KafkaServer configuration.
const DEFAULT_MAX_MESSAGE_SIZE: usize = 100 * 1024 * 1024;

/// Encoding style for Kafka responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResponseStyle {
    /// Standard response format (pre-flexible versions).
    Standard,
    /// Flexible response format (includes empty tagged fields in header).
    Flexible,
}

/// Helper to encode a response with size prefix.
///
/// This consolidated function handles both standard and flexible responses.
/// The `style` parameter controls whether tagged fields are included in the header.
///
/// Note: The `encode_response` and `encode_response_flexible` wrappers
/// below are intentionally kept for ergonomics - they simplify the 20+ call sites
/// in `dispatch_request_common` by avoiding repetitive `ResponseStyle::Standard` params.
#[inline]
fn encode_response_with_style<R: ToByte>(
    correlation_id: i32,
    resp: &R,
    style: ResponseStyle,
) -> Result<Vec<u8>> {
    match style {
        ResponseStyle::Standard => Response::new(correlation_id, resp)?.encode_with_size(),
        ResponseStyle::Flexible => Response::new_flexible(correlation_id, resp)?.encode_with_size(),
    }
}

/// Helper to encode a standard response with size prefix.
#[inline]
fn encode_response<R: ToByte>(correlation_id: i32, resp: &R) -> Result<Vec<u8>> {
    encode_response_with_style(correlation_id, resp, ResponseStyle::Standard)
}

/// Helper to encode a flexible response with size prefix.
#[inline]
fn encode_response_flexible<R: ToByte>(correlation_id: i32, resp: &R) -> Result<Vec<u8>> {
    encode_response_with_style(correlation_id, resp, ResponseStyle::Flexible)
}

/// Encode an ApiVersions response for standard versions (v0-v2).
/// This uses version-aware encoding (v0 has no throttle_time_ms, v1+ has it).
fn encode_api_versions_standard(
    correlation_id: i32,
    api_version: i16,
    resp: &super::response::ApiVersionsResponseData,
) -> Result<Vec<u8>> {
    use crate::encode::ToByte;

    // Encode the body using version-aware format
    let mut body = Vec::new();
    resp.encode_versioned(&mut body, api_version)?;

    // Build response with standard header (no tagged fields)
    let mut header = Vec::new();
    correlation_id.encode(&mut header)?;

    let total_size = (header.len() + body.len()) as i32;
    let mut result = Vec::with_capacity(4 + total_size as usize);
    total_size.encode(&mut result)?;
    result.extend_from_slice(&header);
    result.extend_from_slice(&body);
    Ok(result)
}

/// Encode an ApiVersions response for flexible version (v3+).
/// This uses the special flexible encoding for compact arrays and tagged fields.
///
/// IMPORTANT: Per KIP-511, ApiVersions is special - even v3 responses use the OLD
/// header format (just correlation_id, NO tagged fields) so clients can parse it
/// before protocol negotiation. Only the BODY uses flexible encoding.
fn encode_api_versions_flexible(
    correlation_id: i32,
    resp: &super::response::ApiVersionsResponseData,
) -> Result<Vec<u8>> {
    use crate::encode::ToByte;

    // Encode the body using flexible format
    let mut body = Vec::new();
    resp.encode_flexible(&mut body)?;

    // Build response with OLD header format (NO tagged fields per KIP-511)
    // ApiVersions is special: even v3 uses old header format for compatibility
    let mut header = Vec::new();
    correlation_id.encode(&mut header)?;
    // NOTE: No tagged fields byte here! ApiVersions v3 response uses old header format.

    let total_size = (header.len() + body.len()) as i32;
    let mut result = Vec::with_capacity(4 + total_size as usize);
    total_size.encode(&mut result)?;
    result.extend_from_slice(&header);
    result.extend_from_slice(&body);

    tracing::debug!(
        correlation_id,
        total_size,
        header_bytes = ?header,
        body_len = body.len(),
        first_body_bytes = ?&body[..body.len().min(32)],
        "Encoded ApiVersions v3 flexible response (old header format per KIP-511)"
    );

    Ok(result)
}

/// Trait for connections that support auth rate limiting.
///
/// This provides a unified interface for recording auth results,
/// reducing duplication between plain and TLS connection handlers.
pub trait AuthRateLimited {
    /// Get the rate limiter, if configured.
    fn rate_limiter(&self) -> Option<&Arc<AuthRateLimiter>>;

    /// Get the client IP address for rate limiting.
    fn client_ip(&self) -> std::net::IpAddr;

    /// Record auth result for rate limiting.
    ///
    /// This is called after processing any SASL authentication request.
    async fn record_auth_result(&self, result: AuthResult) {
        if let Some(limiter) = self.rate_limiter() {
            match result {
                AuthResult::Failure => {
                    limiter.record_failure(self.client_ip()).await;
                }
                AuthResult::Success => {
                    limiter.record_success(self.client_ip()).await;
                }
                AuthResult::NotAuth => {}
            }
        }
    }
}

/// Track connection metrics (open/close).
/// Unified metrics handling.
fn track_connection_open() {
    crate::cluster::metrics::ACTIVE_CONNECTIONS.inc();
    crate::cluster::metrics::TOTAL_CONNECTIONS
        .with_label_values(&["success"])
        .inc();
}

fn track_connection_close() {
    crate::cluster::metrics::ACTIVE_CONNECTIONS.dec();
}

/// Result of auth-related request processing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthResult {
    /// Request was not auth-related.
    NotAuth,
    /// Auth succeeded.
    Success,
    /// Auth failed.
    Failure,
}

/// Common request dispatch logic shared between plain and TLS connections.
///
/// This function extracts the duplicated dispatch logic into a single place,
/// reducing maintenance burden and potential for divergence.
///
/// Returns auth result for rate limiting purposes.
async fn dispatch_request_common<H: Handler>(
    handler: &H,
    data: Bytes,
    client_addr: SocketAddr,
    _connection_type: &str, // Used for logging to distinguish plain vs TLS
) -> (Result<Vec<u8>>, AuthResult) {
    let start = Instant::now();

    tracing::debug!(
        client = %client_addr,
        data_len = data.len(),
        first_bytes = ?&data[..data.len().min(32)],
        "Received request data"
    );

    let request = match Request::parse(data.clone()) {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(
                client = %client_addr,
                error = ?e,
                data_len = data.len(),
                first_bytes = ?&data[..data.len().min(32)],
                "Failed to parse request"
            );
            return (Err(e), AuthResult::NotAuth);
        }
    };
    let header = request.header();
    let correlation_id = header.correlation_id;

    let ctx = RequestContext {
        client_addr,
        api_version: header.api_version,
        client_id: header.client_id.clone(),
        request_id: uuid::Uuid::new_v4(),
    };

    tracing::debug!(
        request_id = %ctx.request_id,
        api_key = ?header.api_key,
        client = %client_addr,
        correlation_id,
        "Handling request"
    );

    // Use static string to avoid allocation on every request
    let api_name = header.api_key.as_str();

    // Track auth result for rate limiting
    let mut auth_result = AuthResult::NotAuth;

    let result = match request {
        Request::ApiVersions(header, req) => {
            let response = handler.handle_api_versions(&ctx, req).await;
            // ApiVersions v3+ uses flexible encoding with compact arrays
            // v0-v2 uses standard encoding (v0 has no throttle_time_ms, v1+ has it)
            if header.api_version >= 3 {
                encode_api_versions_flexible(correlation_id, &response)
            } else {
                encode_api_versions_standard(correlation_id, header.api_version, &response)
            }
        }
        Request::Metadata(_, req) => {
            encode_response(correlation_id, &handler.handle_metadata(&ctx, req).await)
        }
        Request::Produce(_, req) => {
            encode_response(correlation_id, &handler.handle_produce(&ctx, req).await)
        }
        Request::Fetch(header, req) => {
            // Fetch v0: responses only, v1+: throttle_time_ms + responses
            let resp = handler.handle_fetch(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            match Response::new_raw(correlation_id, body) {
                Ok(r) => r.encode_with_size(),
                Err(e) => return (Err(e), auth_result),
            }
        }
        Request::ListOffsets(header, req) => {
            // ListOffsets v0: topics only, v1+: throttle_time_ms + topics
            let resp = handler.handle_list_offsets(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            match Response::new_raw(correlation_id, body) {
                Ok(r) => r.encode_with_size(),
                Err(e) => return (Err(e), auth_result),
            }
        }
        Request::OffsetCommit(_, req) => encode_response(
            correlation_id,
            &handler.handle_offset_commit(&ctx, req).await,
        ),
        Request::OffsetFetch(header, req) => {
            // OffsetFetch v0-v1: topics only, v2: +error_code, v3+: +throttle_time_ms
            let resp = handler.handle_offset_fetch(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            match Response::new_raw(correlation_id, body) {
                Ok(r) => r.encode_with_size(),
                Err(e) => return (Err(e), auth_result),
            }
        }
        Request::FindCoordinator(_, req) => encode_response(
            correlation_id,
            &handler.handle_find_coordinator(&ctx, req).await,
        ),
        Request::JoinGroup(header, req) => {
            // JoinGroup v0-v1 has no throttle_time_ms, v2+ has it
            let resp = handler.handle_join_group(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            match Response::new_raw(correlation_id, body) {
                Ok(r) => r.encode_with_size(),
                Err(e) => return (Err(e), auth_result),
            }
        }
        Request::Heartbeat(header, req) => {
            // Heartbeat v0 has no throttle_time_ms, v1+ has it
            let resp = handler.handle_heartbeat(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            match Response::new_raw(correlation_id, body) {
                Ok(r) => r.encode_with_size(),
                Err(e) => return (Err(e), auth_result),
            }
        }
        Request::LeaveGroup(header, req) => {
            // LeaveGroup v0 has no throttle_time_ms, v1+ has it
            let resp = handler.handle_leave_group(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            match Response::new_raw(correlation_id, body) {
                Ok(r) => r.encode_with_size(),
                Err(e) => return (Err(e), auth_result),
            }
        }
        Request::SyncGroup(header, req) => {
            // SyncGroup v0 has no throttle_time_ms, v1+ has it
            let resp = handler.handle_sync_group(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            match Response::new_raw(correlation_id, body) {
                Ok(r) => r.encode_with_size(),
                Err(e) => return (Err(e), auth_result),
            }
        }
        Request::DescribeGroups(_, req) => encode_response(
            correlation_id,
            &handler.handle_describe_groups(&ctx, req).await,
        ),
        Request::ListGroups(_, req) => {
            encode_response(correlation_id, &handler.handle_list_groups(&ctx, req).await)
        }
        Request::DeleteGroups(_, req) => encode_response(
            correlation_id,
            &handler.handle_delete_groups(&ctx, req).await,
        ),
        Request::SaslHandshake(_, req) => encode_response(
            correlation_id,
            &handler.handle_sasl_handshake(&ctx, req).await,
        ),
        Request::SaslAuthenticate(_, req) => {
            // Track SASL auth result
            let response = handler.handle_sasl_authenticate(&ctx, req).await;
            auth_result = if response.error_code == KafkaCode::None {
                AuthResult::Success
            } else {
                AuthResult::Failure
            };
            encode_response(correlation_id, &response)
        }
        Request::CreateTopics(_, req) => encode_response(
            correlation_id,
            &handler.handle_create_topics(&ctx, req).await,
        ),
        Request::DeleteTopics(_, req) => encode_response(
            correlation_id,
            &handler.handle_delete_topics(&ctx, req).await,
        ),
        Request::InitProducerId(_, req) => {
            // InitProducerId uses flexible encoding
            encode_response_flexible(
                correlation_id,
                &handler.handle_init_producer_id(&ctx, req).await,
            )
        }
        Request::Unknown(h, body) => encode_response(
            correlation_id,
            &handler.handle_unknown(&ctx, h.api_key, body).await,
        ),
    };

    {
        let duration = start.elapsed().as_secs_f64();
        let status = if result.is_ok() { "success" } else { "error" };
        crate::cluster::metrics::record_request(api_name, status, duration);
    }

    (result, auth_result)
}

/// A client connection to the Kafka server.
pub struct ClientConnection {
    stream: TcpStream,
    addr: SocketAddr,
    /// Rate limiter for auth failures (optional for backwards compat)
    rate_limiter: Option<Arc<AuthRateLimiter>>,
}

impl ClientConnection {
    /// Create a new client connection (without rate limiter).
    pub fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr,
            rate_limiter: None,
        }
    }

    /// Create a new client connection with auth rate limiter.
    ///
    /// This constructor enables auth failure rate limiting
    /// to protect against brute-force attacks.
    pub fn new_with_rate_limiter(
        stream: TcpStream,
        addr: SocketAddr,
        rate_limiter: Arc<AuthRateLimiter>,
    ) -> Self {
        Self {
            stream,
            addr,
            rate_limiter: Some(rate_limiter),
        }
    }

    /// Record an auth failure for rate limiting.
    ///
    /// This should be called when SASL authentication fails.
    /// Deprecated: Use the AuthRateLimited trait instead.
    pub async fn record_auth_failure(&self) {
        self.record_auth_result(AuthResult::Failure).await;
    }

    /// Record an auth success (clears rate limit state).
    ///
    /// This should be called when SASL authentication succeeds.
    /// Deprecated: Use the AuthRateLimited trait instead.
    pub async fn record_auth_success(&self) {
        self.record_auth_result(AuthResult::Success).await;
    }

    /// Handle requests from this connection until closed.
    ///
    /// Uses unified metric tracking functions.
    pub async fn handle_requests<H: Handler>(&mut self, handler: Arc<H>) -> Result<()> {
        track_connection_open();

        let result = self.handle_requests_inner(handler).await;

        track_connection_close();

        result
    }

    async fn handle_requests_inner<H: Handler>(&mut self, handler: Arc<H>) -> Result<()> {
        let read_timeout = Duration::from_secs(DEFAULT_REQUEST_READ_TIMEOUT_SECS);
        let handler_timeout = Duration::from_secs(DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS);

        loop {
            // Apply timeout to request reading to prevent slowloris attacks
            let read_result = match timeout(read_timeout, self.read_request()).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(
                        client = %self.addr,
                        timeout_secs = DEFAULT_REQUEST_READ_TIMEOUT_SECS,
                        "Request read timeout - closing connection"
                    );
                    return Err(Error::MissingData("Request read timeout".to_owned()));
                }
            };

            match read_result {
                Ok(data) => {
                    // Apply timeout to request processing to prevent runaway handlers
                    let response =
                        match timeout(handler_timeout, self.dispatch_request(&handler, data)).await
                        {
                            Ok(result) => result?,
                            Err(_) => {
                                tracing::error!(
                                    client = %self.addr,
                                    timeout_secs = DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS,
                                    "Request handler timeout - closing connection"
                                );
                                return Err(Error::MissingData(
                                    "Request handler timeout".to_owned(),
                                ));
                            }
                        };
                    self.write_response(&response).await?;
                }
                Err(Error::MissingData(_)) => {
                    tracing::debug!("Client {} disconnected", self.addr);
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!("Error reading request from {}: {:?}", self.addr, e);
                    return Err(e);
                }
            }
        }
    }

    /// Read a single request from the connection.
    async fn read_request(&mut self) -> Result<Bytes> {
        // Read 4-byte size prefix
        let mut size_buf = [0u8; 4];
        let mut bytes_read = 0;

        loop {
            self.stream
                .readable()
                .await
                .map_err(|e| Error::IoError(e.kind()))?;

            match self.stream.try_read(&mut size_buf[bytes_read..]) {
                Ok(0) => {
                    return Err(Error::MissingData("Connection closed".to_owned()));
                }
                Ok(n) => {
                    bytes_read += n;
                    if bytes_read == 4 {
                        break;
                    }
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(Error::IoError(e.kind()));
                }
            }
        }

        let size = (&size_buf[..]).get_i32();

        // Validate message size bounds
        if size < 0 {
            return Err(Error::MissingData(format!(
                "Invalid negative message size: {}",
                size
            )));
        }
        let size = size as usize;
        if size > DEFAULT_MAX_MESSAGE_SIZE {
            return Err(Error::MissingData(format!(
                "Message size {} exceeds maximum allowed size {}",
                size, DEFAULT_MAX_MESSAGE_SIZE
            )));
        }

        tracing::trace!("Reading {} bytes from {}", size, self.addr);

        // Read the actual message
        let mut data = vec![0u8; size];
        let mut bytes_read = 0;

        while bytes_read < size {
            self.stream
                .readable()
                .await
                .map_err(|e| Error::IoError(e.kind()))?;

            match self.stream.try_read(&mut data[bytes_read..]) {
                Ok(0) => {
                    return Err(Error::MissingData(
                        "Connection closed mid-message".to_owned(),
                    ));
                }
                Ok(n) => {
                    bytes_read += n;
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(Error::IoError(e.kind()));
                }
            }
        }

        Ok(Bytes::from(data))
    }

    /// Dispatch a request to the appropriate handler.
    ///
    /// Tracks SASL authentication failures for rate limiting via AuthRateLimited trait.
    async fn dispatch_request<H: Handler>(&self, handler: &Arc<H>, data: Bytes) -> Result<Vec<u8>> {
        let (result, auth_result) =
            dispatch_request_common(handler.as_ref(), data, self.addr, "plain").await;

        // Track auth result for rate limiting (unified via trait)
        self.record_auth_result(auth_result).await;

        result
    }

    /// Write a response to the connection.
    async fn write_response(&mut self, response: &[u8]) -> Result<()> {
        tracing::debug!(
            client = %self.addr,
            response_len = response.len(),
            first_bytes = ?&response[..response.len().min(32)],
            "Writing response"
        );

        let mut bytes_written = 0;

        while bytes_written < response.len() {
            self.stream
                .writable()
                .await
                .map_err(|e| Error::IoError(e.kind()))?;

            match self.stream.try_write(&response[bytes_written..]) {
                Ok(n) => {
                    bytes_written += n;
                    tracing::trace!("Wrote {} bytes to {}", n, self.addr);
                }
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => {
                    continue;
                }
                Err(e) => {
                    return Err(Error::IoError(e.kind()));
                }
            }
        }

        tracing::debug!(
            client = %self.addr,
            bytes_written,
            "Response write complete"
        );

        Ok(())
    }
}

impl AuthRateLimited for ClientConnection {
    fn rate_limiter(&self) -> Option<&Arc<AuthRateLimiter>> {
        self.rate_limiter.as_ref()
    }

    fn client_ip(&self) -> std::net::IpAddr {
        self.addr.ip()
    }
}

/// A TLS client connection to the Kafka server.
#[cfg(feature = "tls")]
pub struct TlsClientConnection {
    stream: TlsStream<TcpStream>,
    addr: SocketAddr,
    /// Rate limiter for auth failures (optional for backwards compat)
    rate_limiter: Option<Arc<AuthRateLimiter>>,
}

#[cfg(feature = "tls")]
impl TlsClientConnection {
    /// Create a new TLS client connection (without rate limiter).
    #[allow(dead_code)]
    pub fn new(stream: TlsStream<TcpStream>, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr,
            rate_limiter: None,
        }
    }

    /// Create a new TLS client connection with auth rate limiter.
    ///
    /// This constructor enables auth failure rate limiting
    /// to protect against brute-force attacks.
    pub fn new_with_rate_limiter(
        stream: TlsStream<TcpStream>,
        addr: SocketAddr,
        rate_limiter: Arc<AuthRateLimiter>,
    ) -> Self {
        Self {
            stream,
            addr,
            rate_limiter: Some(rate_limiter),
        }
    }

    /// Handle requests from this connection until closed.
    ///
    ///Uses unified metric tracking functions.
    pub async fn handle_requests<H: Handler>(&mut self, handler: Arc<H>) -> Result<()> {
        track_connection_open();

        let result = self.handle_requests_inner(handler).await;

        track_connection_close();

        result
    }

    async fn handle_requests_inner<H: Handler>(&mut self, handler: Arc<H>) -> Result<()> {
        let read_timeout = Duration::from_secs(DEFAULT_REQUEST_READ_TIMEOUT_SECS);
        let handler_timeout = Duration::from_secs(DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS);

        loop {
            // Apply timeout to request reading to prevent slowloris attacks
            let read_result = match timeout(read_timeout, self.read_request()).await {
                Ok(result) => result,
                Err(_) => {
                    tracing::warn!(
                        client = %self.addr,
                        timeout_secs = DEFAULT_REQUEST_READ_TIMEOUT_SECS,
                        "TLS request read timeout - closing connection"
                    );
                    return Err(Error::MissingData("Request read timeout".to_owned()));
                }
            };

            match read_result {
                Ok(data) => {
                    // Apply timeout to request processing to prevent runaway handlers
                    let response =
                        match timeout(handler_timeout, self.dispatch_request(&handler, data)).await
                        {
                            Ok(result) => result?,
                            Err(_) => {
                                tracing::error!(
                                    client = %self.addr,
                                    timeout_secs = DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS,
                                    "TLS request handler timeout - closing connection"
                                );
                                return Err(Error::MissingData(
                                    "Request handler timeout".to_owned(),
                                ));
                            }
                        };
                    self.write_response(&response).await?;
                }
                Err(Error::MissingData(_)) => {
                    tracing::debug!("TLS client {} disconnected", self.addr);
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        "Error reading request from TLS client {}: {:?}",
                        self.addr,
                        e
                    );
                    return Err(e);
                }
            }
        }
    }

    /// Read a single request from the TLS connection.
    async fn read_request(&mut self) -> Result<Bytes> {
        // Read 4-byte size prefix
        let mut size_buf = [0u8; 4];
        if let Err(e) = self.stream.read_exact(&mut size_buf).await {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Err(Error::MissingData("Connection closed".to_owned()));
            }
            return Err(Error::IoError(e.kind()));
        }

        let size = (&size_buf[..]).get_i32();

        // Validate message size bounds
        if size < 0 {
            return Err(Error::MissingData(format!(
                "Invalid negative message size: {}",
                size
            )));
        }
        let size = size as usize;
        if size > DEFAULT_MAX_MESSAGE_SIZE {
            return Err(Error::MissingData(format!(
                "Message size {} exceeds maximum allowed size {}",
                size, DEFAULT_MAX_MESSAGE_SIZE
            )));
        }

        tracing::trace!("Reading {} bytes from TLS client {}", size, self.addr);

        // Read the actual message
        let mut data = vec![0u8; size];
        if let Err(e) = self.stream.read_exact(&mut data).await {
            if e.kind() == io::ErrorKind::UnexpectedEof {
                return Err(Error::MissingData(
                    "Connection closed mid-message".to_owned(),
                ));
            }
            return Err(Error::IoError(e.kind()));
        }

        Ok(Bytes::from(data))
    }

    /// Dispatch a request to the appropriate handler.
    ///
    /// Tracks SASL authentication failures for rate limiting via AuthRateLimited trait.
    async fn dispatch_request<H: Handler>(&self, handler: &Arc<H>, data: Bytes) -> Result<Vec<u8>> {
        let (result, auth_result) =
            dispatch_request_common(handler.as_ref(), data, self.addr, "tls").await;

        // Track auth result for rate limiting (unified via trait)
        self.record_auth_result(auth_result).await;

        result
    }

    /// Write a response to the TLS connection.
    async fn write_response(&mut self, response: &[u8]) -> Result<()> {
        self.stream
            .write_all(response)
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

        self.stream
            .flush()
            .await
            .map_err(|e| Error::IoError(e.kind()))?;

        tracing::trace!("Wrote {} bytes to TLS client {}", response.len(), self.addr);
        Ok(())
    }
}

#[cfg(feature = "tls")]
impl AuthRateLimited for TlsClientConnection {
    fn rate_limiter(&self) -> Option<&Arc<AuthRateLimiter>> {
        self.rate_limiter.as_ref()
    }

    fn client_ip(&self) -> std::net::IpAddr {
        self.addr.ip()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KafkaCode;
    use crate::server::request::ApiKey;
    use crate::server::response::{ApiVersionData, ApiVersionsResponseData};

    #[test]
    fn test_encode_api_versions_standard_v0() {
        // Test v0 encoding: no throttle_time_ms, standard header (no tagged fields)
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 9,
            }],
            throttle_time_ms: 100, // Should be ignored in v0
        };

        let result = encode_api_versions_standard(42, 0, &response).unwrap();

        // Wire format:
        // size (4) + correlation_id (4) + error_code (2) + array_len (4) + 1 item (6)
        // = 4 + 4 + 2 + 4 + 6 = 20 bytes total
        // Message size = 16 (excludes size prefix)
        assert_eq!(result.len(), 20);

        // Size prefix = 16 (0x00000010)
        assert_eq!(&result[0..4], &[0x00, 0x00, 0x00, 0x10]);

        // correlation_id = 42 (0x0000002A)
        assert_eq!(&result[4..8], &[0x00, 0x00, 0x00, 0x2A]);

        // error_code = 0
        assert_eq!(&result[8..10], &[0x00, 0x00]);

        // array_length = 1
        assert_eq!(&result[10..14], &[0x00, 0x00, 0x00, 0x01]);

        // Verify NO throttle_time_ms at end (message ends at byte 20)
    }

    #[test]
    fn test_encode_api_versions_standard_v1() {
        // Test v1 encoding: includes throttle_time_ms
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 11,
            }],
            throttle_time_ms: 500,
        };

        let result = encode_api_versions_standard(100, 1, &response).unwrap();

        // Wire format:
        // size (4) + correlation_id (4) + error_code (2) + array_len (4) + 1 item (6) + throttle_time_ms (4)
        // = 4 + 4 + 2 + 4 + 6 + 4 = 24 bytes total
        // Message size = 20 (excludes size prefix)
        assert_eq!(result.len(), 24);

        // Size prefix = 20 (0x00000014)
        assert_eq!(&result[0..4], &[0x00, 0x00, 0x00, 0x14]);

        // correlation_id = 100 (0x00000064)
        assert_eq!(&result[4..8], &[0x00, 0x00, 0x00, 0x64]);

        // throttle_time_ms = 500 (0x000001F4) at end
        assert_eq!(&result[20..24], &[0x00, 0x00, 0x01, 0xF4]);
    }

    #[test]
    fn test_encode_api_versions_standard_v2() {
        // Test v2 encoding: same as v1
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let result_v1 = encode_api_versions_standard(1, 1, &response).unwrap();
        let result_v2 = encode_api_versions_standard(1, 2, &response).unwrap();

        // v1 and v2 should be identical
        assert_eq!(result_v1, result_v2);
    }

    #[test]
    fn test_encode_api_versions_flexible_v3() {
        // Test v3 flexible encoding with OLD header format per KIP-511
        // ApiVersions is special - even v3 uses old header format for compatibility
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 9,
            }],
            throttle_time_ms: 0,
        };

        let result = encode_api_versions_flexible(123, &response).unwrap();

        // Wire format (ApiVersions v3 per KIP-511):
        // Header (OLD format, NO tagged fields):
        //   size (4) + correlation_id (4) = 8 bytes
        // Body (flexible format):
        //   error_code (2) + compact_array_len (1) + 1 item (7) + throttle_time_ms (4) + tagged_fields (1) = 15 bytes
        // Total: 23 bytes

        // Size prefix (first 4 bytes)
        let size = i32::from_be_bytes([result[0], result[1], result[2], result[3]]);
        assert_eq!(size as usize, result.len() - 4);

        // correlation_id = 123 (0x0000007B)
        assert_eq!(&result[4..8], &[0x00, 0x00, 0x00, 0x7B]);

        // NO header_tagged_fields byte per KIP-511!
        // error_code = 0 starts immediately at byte 8
        assert_eq!(&result[8..10], &[0x00, 0x00]);

        // compact_array_len = 2 (1 item + 1) at byte 10
        assert_eq!(result[10], 0x02);
    }

    #[test]
    fn test_encode_api_versions_flexible_uses_old_header_format() {
        // Per KIP-511: ApiVersions is special - even v3 uses OLD response header format
        // (just correlation_id, NO tagged fields) so clients can parse it before negotiation.
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let result = encode_api_versions_flexible(1, &response).unwrap();

        // Header should be OLD format: size (4) + correlation_id (4) = 8 bytes
        // Body starts at byte 8 with error_code
        // Byte 8 should be error_code high byte (0x00), NOT a tagged fields byte
        assert_eq!(result[8], 0x00, "Byte 8 should be error_code high byte");
        // Byte 9 should be error_code low byte (0x00)
        assert_eq!(result[9], 0x00, "Byte 9 should be error_code low byte");
        // Byte 10 should be compact array length (0x01 for empty array)
        assert_eq!(
            result[10], 0x01,
            "Byte 10 should be compact array length (1 = 0 items)"
        );
    }

    #[test]
    fn test_encode_api_versions_flexible_correct_format() {
        // Verify v3 response has correct format per KIP-511:
        // Header (OLD format): size + correlation_id (NO tagged fields!)
        // Body: error_code, api_keys, throttle_time_ms, tagged_fields
        // Note: SupportedFeatures, FinalizedFeaturesEpoch, FinalizedFeatures are OPTIONAL
        // tagged fields (tag 0, 1, 2) that we omit by using empty tagged fields.
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let result = encode_api_versions_flexible(1, &response).unwrap();

        // Calculate positions (KIP-511: OLD header format):
        // size (4) + correlation_id (4) = 8 bytes header (NO tagged fields in header!)
        // error_code (2) + compact_array_len (1) + throttle_time_ms (4) + tagged_fields (1) = 8 bytes body
        // Total: 16 bytes

        assert_eq!(
            result.len(),
            16,
            "Expected 16 bytes for minimal v3 response (old header format per KIP-511)"
        );

        // After throttle_time_ms (at byte 15), we should have empty tagged_fields in body
        assert_eq!(result[15], 0x00, "Body should have empty tagged fields");
    }

    #[test]
    fn test_encode_api_versions_v0_vs_v1_size_difference() {
        // v0 should be exactly 4 bytes smaller than v1 (no throttle_time_ms)
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Metadata,
                min_version: 0,
                max_version: 12,
            }],
            throttle_time_ms: 1000,
        };

        let v0_result = encode_api_versions_standard(1, 0, &response).unwrap();
        let v1_result = encode_api_versions_standard(1, 1, &response).unwrap();

        assert_eq!(
            v1_result.len() - v0_result.len(),
            4,
            "v1 should be 4 bytes larger than v0"
        );
    }

    #[test]
    fn test_encode_response_style_standard() {
        // Test the encode_response_with_style helper for standard
        use crate::server::response::ApiVersionsResponseData;

        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let result = encode_response_with_style(1, &response, ResponseStyle::Standard).unwrap();

        // Standard response shouldn't have tagged fields byte in header
        // Size prefix (4) + correlation_id (4) = 8, then body starts
        // Body: error_code (2) + array_len (4) + throttle_time_ms (4) = 10
        assert_eq!(result.len(), 18);
    }

    #[test]
    fn test_encode_response_style_flexible() {
        // Test the encode_response_with_style helper for flexible
        use crate::server::response::ApiVersionsResponseData;

        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let result = encode_response_with_style(1, &response, ResponseStyle::Flexible).unwrap();

        // Flexible response has tagged fields byte in header
        // Header includes 1 extra byte for tagged fields
        assert!(
            result.len() > 18,
            "Flexible encoding should be larger due to tagged fields"
        );

        // Byte 8 should be empty tagged fields (0x00)
        assert_eq!(result[8], 0x00);
    }

    #[test]
    fn test_encode_response_helper_functions() {
        // Test the convenience helper functions
        use crate::server::response::ApiVersionsResponseData;

        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let standard = encode_response(1, &response).unwrap();
        let flexible = encode_response_flexible(1, &response).unwrap();

        // Flexible should be larger due to header tagged fields
        assert!(flexible.len() > standard.len());
    }

    #[test]
    fn test_response_style_enum() {
        // Test ResponseStyle enum equality
        assert_eq!(ResponseStyle::Standard, ResponseStyle::Standard);
        assert_eq!(ResponseStyle::Flexible, ResponseStyle::Flexible);
        assert_ne!(ResponseStyle::Standard, ResponseStyle::Flexible);

        // Test Copy trait
        let style = ResponseStyle::Flexible;
        let copied = style;
        assert_eq!(style, copied);
    }

    // ========================================================================
    // AuthResult Tests
    // ========================================================================

    #[test]
    fn test_auth_result_enum() {
        // Test AuthResult variants
        let not_auth = AuthResult::NotAuth;
        let success = AuthResult::Success;
        let failure = AuthResult::Failure;

        assert_eq!(not_auth, AuthResult::NotAuth);
        assert_eq!(success, AuthResult::Success);
        assert_eq!(failure, AuthResult::Failure);

        assert_ne!(not_auth, success);
        assert_ne!(success, failure);
        assert_ne!(not_auth, failure);
    }

    #[test]
    fn test_auth_result_debug() {
        assert!(format!("{:?}", AuthResult::NotAuth).contains("NotAuth"));
        assert!(format!("{:?}", AuthResult::Success).contains("Success"));
        assert!(format!("{:?}", AuthResult::Failure).contains("Failure"));
    }

    #[test]
    fn test_auth_result_copy() {
        let result = AuthResult::Success;
        let copied = result;
        assert_eq!(result, copied);
    }

    #[test]
    fn test_auth_result_clone() {
        let result = AuthResult::Failure;
        let cloned = result;
        assert_eq!(result, cloned);
    }

    // ========================================================================
    // ResponseStyle Tests
    // ========================================================================

    #[test]
    fn test_response_style_debug() {
        assert!(format!("{:?}", ResponseStyle::Standard).contains("Standard"));
        assert!(format!("{:?}", ResponseStyle::Flexible).contains("Flexible"));
    }

    #[test]
    fn test_response_style_clone() {
        let style = ResponseStyle::Standard;
        let cloned = style;
        assert_eq!(style, cloned);
    }

    // ========================================================================
    // Metric Tracking Tests
    // ========================================================================

    #[test]
    fn test_track_connection_metrics() {
        // Just verify the functions don't panic
        // (actual metric values are tested elsewhere)
        track_connection_open();
        track_connection_close();
    }

    // ========================================================================
    // Encoding with Error Tests
    // ========================================================================

    #[test]
    fn test_encode_api_versions_with_error_code() {
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::UnsupportedVersion,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let result = encode_api_versions_standard(1, 1, &response).unwrap();

        // Verify error code is encoded
        // After size prefix (4) and correlation_id (4), error_code is at byte 8-9
        // UnsupportedVersion = 35 (0x0023)
        assert_eq!(&result[8..10], &[0x00, 0x23]);
    }

    #[test]
    fn test_encode_api_versions_flexible_with_error() {
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::UnsupportedVersion,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let result = encode_api_versions_flexible(1, &response).unwrap();

        // After size (4) and correlation_id (4), error_code is at byte 8-9
        // (no tagged fields in header per KIP-511)
        assert_eq!(&result[8..10], &[0x00, 0x23]);
    }

    // ========================================================================
    // Constants Tests
    // ========================================================================

    #[test]
    fn test_default_max_message_size() {
        // 100 MB limit
        assert_eq!(DEFAULT_MAX_MESSAGE_SIZE, 100 * 1024 * 1024);
    }

    // ========================================================================
    // Multiple API Keys Tests
    // ========================================================================

    #[test]
    fn test_encode_api_versions_multiple_keys() {
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![
                ApiVersionData {
                    api_key: ApiKey::Produce,
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionData {
                    api_key: ApiKey::Fetch,
                    min_version: 0,
                    max_version: 13,
                },
                ApiVersionData {
                    api_key: ApiKey::Metadata,
                    min_version: 0,
                    max_version: 12,
                },
            ],
            throttle_time_ms: 0,
        };

        let result = encode_api_versions_standard(1, 1, &response).unwrap();

        // Wire format:
        // size (4) + correlation_id (4) + error_code (2) + array_len (4) + 3 items (18) + throttle (4)
        // = 4 + 4 + 2 + 4 + 18 + 4 = 36 bytes
        assert_eq!(result.len(), 36);

        // Array length = 3
        assert_eq!(&result[10..14], &[0x00, 0x00, 0x00, 0x03]);
    }

    #[test]
    fn test_encode_api_versions_flexible_multiple_keys() {
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![
                ApiVersionData {
                    api_key: ApiKey::Produce,
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionData {
                    api_key: ApiKey::Fetch,
                    min_version: 0,
                    max_version: 13,
                },
            ],
            throttle_time_ms: 100,
        };

        let result = encode_api_versions_flexible(42, &response).unwrap();

        // Verify correlation_id
        assert_eq!(&result[4..8], &[0x00, 0x00, 0x00, 0x2A]);

        // Verify compact array length = 3 (2 items + 1)
        assert_eq!(result[10], 0x03);
    }

    // ========================================================================
    // Duration/Timeout Constants Tests
    // ========================================================================

    #[test]
    fn test_timeout_duration_constants() {
        use crate::constants::{
            DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS, DEFAULT_REQUEST_READ_TIMEOUT_SECS,
        };

        let read_timeout = Duration::from_secs(DEFAULT_REQUEST_READ_TIMEOUT_SECS);
        let handler_timeout = Duration::from_secs(DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS);

        // Both should be reasonable durations (at least 1 second)
        assert!(read_timeout.as_secs() >= 1);
        assert!(handler_timeout.as_secs() >= 1);
    }
}
