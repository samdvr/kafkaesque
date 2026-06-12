//! Client connection handling for Kafka server.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Semaphore;
use tokio::time::timeout;

#[cfg(feature = "tls")]
use tokio_rustls::server::TlsStream;

use crate::constants::{
    DEFAULT_MAX_MESSAGE_SIZE, DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS,
    DEFAULT_REQUEST_READ_TIMEOUT_SECS, DEFAULT_REQUEST_WRITE_TIMEOUT_SECS,
};
use crate::error::{Error, KafkaCode, Result};

use super::handler::{Handler, RequestContext};
use super::rate_limiter::AuthRateLimiter;
use super::request::Request;
use super::response::Response;

use crate::encode::ToByte;

use std::time::Instant;

/// Default global inflight-bytes budget (1 GiB). Bounds the total memory
/// the broker is willing to allocate for not-yet-parsed inbound frames so a
/// burst of large produces (or a coordinated slow-frame attack) can't OOM
/// the process. Override via [`set_global_inflight_byte_budget`] before the
/// first connection is served.
const DEFAULT_GLOBAL_INFLIGHT_BUDGET: usize = 1024 * 1024 * 1024;

/// The budget configured via [`set_global_inflight_byte_budget`], if any.
/// Once `GLOBAL_INFLIGHT` is initialized this cell is filled with the
/// effective value, so later configuration attempts fail loudly instead of
/// appearing to succeed while changing nothing.
static CONFIGURED_INFLIGHT_BUDGET: once_cell::sync::OnceCell<usize> =
    once_cell::sync::OnceCell::new();

/// Global semaphore measured in bytes. Each inbound frame acquires N permits
/// (one per byte) before the buffer is allocated and releases them on drop.
/// Initialized lazily on the first frame from [`CONFIGURED_INFLIGHT_BUDGET`]
/// (or the 1 GiB default); immutable afterwards.
static GLOBAL_INFLIGHT: once_cell::sync::Lazy<Arc<Semaphore>> = once_cell::sync::Lazy::new(|| {
    let budget = *CONFIGURED_INFLIGHT_BUDGET.get_or_init(|| DEFAULT_GLOBAL_INFLIGHT_BUDGET);
    Arc::new(Semaphore::new(budget))
});

/// Configure the process-global inflight-bytes budget.
///
/// The budget bounds the total bytes of inbound Kafka frames that may be
/// buffered (allocated but not yet fully dispatched) across ALL connections
/// at once; `read_request` reserves against it before allocating each frame
/// and rejects the frame when the budget is exhausted.
///
/// Values above tokio's `Semaphore::MAX_PERMITS` are clamped.
///
/// Call once from server startup, BEFORE accepting traffic. Returns `true`
/// if the value was applied; `false` if a budget was already fixed —
/// either by an earlier call or because a frame has already been processed
/// (which freezes the effective budget at its current value).
///
/// NOTE: server startup does not yet thread a config value through to this
/// setter (that wiring lives outside this module); until it does, every
/// process runs with the 1 GiB default and this function exists for that
/// startup code (and tests) to call.
#[allow(dead_code)] // wired from server startup once a config value exists
pub fn set_global_inflight_byte_budget(bytes: usize) -> bool {
    CONFIGURED_INFLIGHT_BUDGET
        .set(bytes.min(Semaphore::MAX_PERMITS))
        .is_ok()
}

/// The effective global inflight-bytes budget for this process.
///
/// Forces initialization: after calling this, the budget can no longer be
/// changed via [`set_global_inflight_byte_budget`].
#[allow(dead_code)] // companion introspection for the setter; used in tests
pub fn global_inflight_byte_budget() -> usize {
    // Force the semaphore so the reported value is the one actually used.
    once_cell::sync::Lazy::force(&GLOBAL_INFLIGHT);
    *CONFIGURED_INFLIGHT_BUDGET
        .get()
        .expect("filled by GLOBAL_INFLIGHT initialization")
}

/// Permit guard returned by the inflight-bytes acquire helper. Forgotten on
/// purpose: see `with_inflight_bytes`.
struct InflightPermit {
    bytes: usize,
}

impl Drop for InflightPermit {
    fn drop(&mut self) {
        GLOBAL_INFLIGHT.add_permits(self.bytes);
    }
}

/// Try to reserve `size` bytes against the global inflight budget. Returns
/// `Some` if the reservation succeeded, `None` if the budget is exhausted —
/// the caller must reject the frame in that case rather than queue.
///
/// `Semaphore::try_acquire_many` is `u32`; for frames > 4 GiB we already
/// reject above via `max_message_size`, but cap defensively to be safe.
fn try_reserve_inflight_bytes(size: usize) -> Option<InflightPermit> {
    let n = u32::try_from(size).ok()?;
    let permit = GLOBAL_INFLIGHT.try_acquire_many(n).ok()?;
    permit.forget();
    Some(InflightPermit { bytes: size })
}

/// Encoding style for Kafka responses.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ResponseStyle {
    /// Standard response format (pre-flexible versions).
    Standard,
    /// Flexible response format (includes empty tagged fields in header).
    Flexible,
}

/// Response header/body style for a specific `(api_key, version)` pair.
#[inline]
pub(crate) fn response_style_for(
    api_key: crate::server::request::ApiKey,
    api_version: i16,
) -> ResponseStyle {
    if crate::server::versions::uses_flexible_encoding(api_key, api_version) {
        ResponseStyle::Flexible
    } else {
        ResponseStyle::Standard
    }
}

/// Best-effort correlation id from a request frame body (after the 4-byte
/// size prefix has been stripped). Needs at least 8 bytes: api_key,
/// api_version, correlation_id.
fn peek_correlation_id(data: &Bytes) -> Option<i32> {
    if data.len() < 8 {
        return None;
    }
    Some(i32::from_be_bytes([data[4], data[5], data[6], data[7]]))
}

/// Minimal Kafka error response carrying `correlation_id` so clients see a
/// typed error instead of a connection reset.
fn encode_wire_error_response(correlation_id: i32, error_code: KafkaCode) -> Result<Vec<u8>> {
    use crate::server::response::ErrorResponseData;
    encode_response(correlation_id, &ErrorResponseData { error_code })
}

/// Encode a version-aware response body with the correct header style.
fn encode_versioned_raw_response(
    correlation_id: i32,
    api_key: crate::server::request::ApiKey,
    api_version: i16,
    body: Vec<u8>,
) -> Result<Vec<u8>> {
    let response = match api_key.response_style(api_version) {
        ResponseStyle::Standard => Response::new_raw(correlation_id, body)?,
        ResponseStyle::Flexible => Response::new_raw_flexible(correlation_id, body)?,
    };
    response.encode_with_size()
}

/// Read one length-prefixed Kafka request frame from any async stream.
async fn read_kafka_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_message_size: usize,
) -> Result<Bytes> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Error::MissingData("Connection closed".to_owned())
        } else {
            Error::from(e)
        }
    })?;

    let size = (&size_buf[..]).get_i32();
    if size < 0 {
        return Err(Error::MissingData(format!(
            "Invalid negative message size: {size}"
        )));
    }
    let size = size as usize;
    if size > max_message_size {
        return Err(Error::MissingData(format!(
            "Message size {size} exceeds maximum allowed size {max_message_size}"
        )));
    }

    let _budget = match try_reserve_inflight_bytes(size) {
        Some(p) => p,
        None => {
            return Err(Error::MissingData(format!(
                "Server inflight memory budget exhausted (frame size {size})"
            )));
        }
    };

    let mut data = vec![0u8; size];
    stream.read_exact(&mut data).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Error::MissingData("Connection closed mid-message".to_owned())
        } else {
            Error::from(e)
        }
    })?;

    Ok(Bytes::from(data))
}

/// Public re-export of [`read_kafka_frame`] for fuzz harnesses.
///
/// The production callers live inside this module; the frame reader itself
/// is the broker's first contact with attacker bytes, so a fuzz target
/// against it is part of the security boundary. This thin wrapper lets the
/// `kafkaesque-fuzz` crate drive the reader without re-implementing it.
pub async fn read_kafka_frame_for_fuzz<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_message_size: usize,
) -> Result<Bytes> {
    read_kafka_frame(stream, max_message_size).await
}

/// Write a length-prefixed Kafka response frame.
async fn write_kafka_frame<S: AsyncWrite + Unpin>(
    stream: &mut S,
    response: &[u8],
    client: SocketAddr,
) -> Result<()> {
    let write_deadline = Duration::from_secs(DEFAULT_REQUEST_WRITE_TIMEOUT_SECS);
    match timeout(write_deadline, async {
        stream.write_all(response).await.map_err(Error::from)?;
        stream.flush().await.map_err(Error::from)
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            tracing::warn!(
                client = %client,
                response_len = response.len(),
                timeout_secs = DEFAULT_REQUEST_WRITE_TIMEOUT_SECS,
                "Response write timed out (slow / non-reading client)"
            );
            Err(Error::MissingData("Response write timeout".to_owned()))
        }
    }
}

/// Shared request loop for plain and TLS connections.
#[allow(clippy::too_many_arguments)]
async fn serve_connection_requests<H, S>(
    stream: &mut S,
    client: SocketAddr,
    max_message_size: usize,
    handler: Arc<H>,
    auth_gate: &mut AuthGate,
    rate_limiter: Option<&Arc<AuthRateLimiter>>,
    connection_label: &'static str,
    transport_tls: bool,
) -> Result<()>
where
    H: Handler,
    S: AsyncRead + AsyncWrite + Unpin,
{
    let read_timeout = Duration::from_secs(DEFAULT_REQUEST_READ_TIMEOUT_SECS);
    let handler_timeout = Duration::from_secs(DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS);

    let result = crate::cluster::buffer_pool::run_scoped(async {
        loop {
            let read_result =
                match timeout(read_timeout, read_kafka_frame(stream, max_message_size)).await {
                    Ok(result) => result,
                    Err(_) => {
                        tracing::warn!(
                            client = %client,
                            connection = connection_label,
                            timeout_secs = DEFAULT_REQUEST_READ_TIMEOUT_SECS,
                            "Request read timeout - closing connection"
                        );
                        return Err(Error::MissingData("Request read timeout".to_owned()));
                    }
                };

            match read_result {
                Ok(data) => {
                    let correlation_id = peek_correlation_id(&data);
                    let dispatch_result = timeout(handler_timeout, async {
                        let (result, auth_result) = dispatch_request_common(
                            handler.as_ref(),
                            data,
                            client,
                            connection_label,
                            auth_gate,
                            transport_tls,
                        )
                        .await;
                        if let Some(limiter) = rate_limiter {
                            match auth_result {
                                AuthResult::Failure => {
                                    limiter.record_failure(client.ip()).await;
                                }
                                AuthResult::Success => {
                                    limiter.record_success(client.ip()).await;
                                }
                                AuthResult::NotAuth => {}
                            }
                        }
                        result
                    })
                    .await;

                    match dispatch_result {
                        Ok(Ok(response)) => write_kafka_frame(stream, &response, client).await?,
                        Ok(Err(e)) => {
                            tracing::error!(
                                client = %client,
                                connection = connection_label,
                                error = ?e,
                                "Failed to dispatch request"
                            );
                            if let Some(cid) = correlation_id
                                && let Ok(err_resp) =
                                    encode_wire_error_response(cid, KafkaCode::InvalidRequest)
                            {
                                let _ = write_kafka_frame(stream, &err_resp, client).await;
                            }
                            return Err(e);
                        }
                        Err(_) => {
                            tracing::error!(
                                client = %client,
                                connection = connection_label,
                                timeout_secs = DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS,
                                "Request handler timeout - closing connection"
                            );
                            if let Some(cid) = correlation_id
                                && let Ok(err_resp) =
                                    encode_wire_error_response(cid, KafkaCode::InvalidRequest)
                            {
                                let _ = write_kafka_frame(stream, &err_resp, client).await;
                            }
                            return Err(Error::MissingData("Request handler timeout".to_owned()));
                        }
                    }
                }
                Err(Error::MissingData(_)) => {
                    tracing::debug!(client = %client, "Client disconnected");
                    return Ok(());
                }
                Err(e) => {
                    tracing::error!(
                        client = %client,
                        connection = connection_label,
                        error = ?e,
                        "Error reading request"
                    );
                    return Err(e);
                }
            }
        }
    })
    .await;

    handler.on_connection_closed(client).await;
    result
}

/// Helper to encode a response with size prefix.
///
/// This consolidated function handles both standard and flexible responses.
/// The `style` parameter controls whether tagged fields are included in the header.
///
/// Note: The `encode_response` wrapper below is intentionally kept for
/// ergonomics - it simplifies the 20+ call sites in `dispatch_request_common`
/// by avoiding repetitive `ResponseStyle::Standard` params. (A flexible
/// counterpart used to exist for InitProducerId; that API is now encoded
/// version-aware via `encode_versioned` + `Response::new_raw_flexible`.)
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

/// Encode the error response for a request whose api_version is outside the
/// advertised range (`Request::UnsupportedVersion`).
///
/// - ApiVersions: per KIP-511 / real-broker behavior, an unsupported
///   ApiVersions version is answered with a **v0-encoded** ApiVersions
///   response carrying `UnsupportedVersion` (35) AND the full supported
///   version list, so the client can pick a version we do speak and retry.
/// - Every other API: a minimal standard-header response whose body is the
///   INT16 error code 35 (same shape `handle_unknown` uses). The connection
///   stays open.
fn encode_unsupported_version_response(
    api_key: crate::server::request::ApiKey,
    correlation_id: i32,
) -> Result<Vec<u8>> {
    use crate::server::request::ApiKey;
    use crate::server::response::{ApiVersionsResponseData, ErrorResponseData};
    use crate::server::versions::default_api_versions;

    if api_key == ApiKey::ApiVersions {
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::UnsupportedVersion,
            api_keys: default_api_versions(),
            throttle_time_ms: 0,
        };
        // Always v0 framing: the client demonstrably speaks a version we
        // don't, so the lowest common denominator is the only safe choice.
        encode_api_versions_standard(correlation_id, 0, &response)
    } else {
        encode_response(
            correlation_id,
            &ErrorResponseData {
                error_code: KafkaCode::UnsupportedVersion,
            },
        )
    }
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
    auth_gate: &mut AuthGate,
    transport_tls: bool,
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
            if let Some(correlation_id) = peek_correlation_id(&data) {
                return (
                    encode_wire_error_response(correlation_id, KafkaCode::InvalidRequest),
                    AuthResult::NotAuth,
                );
            }
            return (Err(e), AuthResult::NotAuth);
        }
    };
    let header = request.header();
    let correlation_id = header.correlation_id;

    // When SASL is required and the connection has not
    // completed authentication, refuse anything except the connection-setup
    // API keys (ApiVersions, SaslHandshake, SaslAuthenticate). Without this
    // check the dispatch table happily answers Produce/Fetch/Admin even
    // before any handshake has happened.
    if auth_gate.required && !auth_gate.authenticated && !AuthGate::allows_pre_auth(header.api_key)
    {
        tracing::warn!(
            client = %client_addr,
            api_key = ?header.api_key,
            "Rejecting unauthenticated request: SASL handshake required first"
        );
        return (
            encode_wire_error_response(correlation_id, KafkaCode::SaslAuthenticationFailed),
            AuthResult::NotAuth,
        );
    }

    // Resolve the effective principal for this request. We use
    // the gate's stored principal when present, else `User:ANONYMOUS` —
    // matching real Kafka's identity for a non-SASL connection. ACL
    // bindings against `User:ANONYMOUS` let operators allow specific things
    // through without enabling SASL.
    let principal = auth_gate
        .principal
        .clone()
        .unwrap_or_else(|| "User:ANONYMOUS".to_string());
    let client_host = client_addr.ip().to_string();

    let ctx = RequestContext {
        client_addr,
        api_version: header.api_version,
        client_id: header.client_id.clone(),
        request_id: uuid::Uuid::new_v4(),
        principal,
        client_host,
        transport_tls,
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
            let resp = handler.handle_fetch(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::ListOffsets(header, req) => {
            let resp = handler.handle_list_offsets(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::OffsetCommit(_, req) => encode_response(
            correlation_id,
            &handler.handle_offset_commit(&ctx, req).await,
        ),
        Request::OffsetFetch(header, req) => {
            let resp = handler.handle_offset_fetch(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::FindCoordinator(_, req) => encode_response(
            correlation_id,
            &handler.handle_find_coordinator(&ctx, req).await,
        ),
        Request::JoinGroup(header, req) => {
            let resp = handler.handle_join_group(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::Heartbeat(header, req) => {
            let resp = handler.handle_heartbeat(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::LeaveGroup(header, req) => {
            let resp = handler.handle_leave_group(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::SyncGroup(header, req) => {
            let resp = handler.handle_sync_group(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
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
            // Track SASL auth result. We must extract the principal from the
            // wire bytes BEFORE moving `req` into the handler so the gate
            // (and downstream authorizer) can attribute future requests to
            // a real identity. For PLAIN that's `\0user\0pass`;
            // for unsupported mechanisms we fall back to None and the
            // dispatcher treats the connection as anonymous.
            //
            // SCRAM-SHA-256 is multi-step: the first
            // round-trip returns `error_code = None` but the handshake
            // isn't done yet. The cluster handler signals this via
            // `take_sasl_post_auth`, which we consult before flipping the
            // gate.
            let plain_principal = principal_from_sasl_plain(&req.auth_bytes);
            let response = handler.handle_sasl_authenticate(&ctx, req).await;
            let post = handler.take_sasl_post_auth(client_addr).await;
            auth_result = if response.error_code == KafkaCode::None {
                match post {
                    Some(p) if p.complete => {
                        auth_gate.authenticated = true;
                        if let Some(principal) = p.principal {
                            auth_gate.principal = Some(principal);
                        } else if let Some(p) = plain_principal {
                            auth_gate.principal = Some(p);
                        }
                        AuthResult::Success
                    }
                    Some(_) => {
                        // Intermediate SCRAM step — keep the gate closed,
                        // the client will send another SaslAuthenticate
                        // and we'll re-evaluate then.
                        AuthResult::NotAuth
                    }
                    None => {
                        // Single-step PLAIN (no override): treat success
                        // the same as the original behavior.
                        auth_gate.authenticated = true;
                        if let Some(p) = plain_principal {
                            auth_gate.principal = Some(p);
                        }
                        AuthResult::Success
                    }
                }
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
        Request::InitProducerId(header, req) => {
            let resp = handler.handle_init_producer_id(&ctx, req).await;
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::UnsupportedVersion(h) => {
            // The client sent a known API key with a version outside the
            // advertised range. Answer with UnsupportedVersion (35) under
            // the request's correlation id instead of closing the
            // connection, so well-behaved clients can downgrade.
            tracing::warn!(
                client = %client_addr,
                api_key = ?h.api_key,
                api_version = h.api_version,
                correlation_id,
                "Rejecting request with unadvertised API version"
            );
            encode_unsupported_version_response(h.api_key, correlation_id)
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

/// Connection-level SASL gate state.
///
/// Tracks per-connection authentication so `dispatch_request_common` can
/// reject every non-handshake API key when `sasl_required` is true and the
/// client has not yet completed a SaslAuthenticate exchange. Without this
/// gate the server still answers Produce/Fetch/Metadata/Admin requests on
/// the same socket regardless of any subsequent SASL handshake.
///
/// Always-on (not feature-gated): when the `sasl` feature is off the
/// `required` flag stays false and this is a free no-op.
///
/// Also carries the authenticated principal. This is what the
/// authorizer keys against — so flipping `authenticated` without writing
/// the principal would let through every API on the connection. ACL
/// enforcement consults `principal` after the gate, defaulting to
/// `User:ANONYMOUS` when SASL is not required.
#[derive(Debug, Clone, Default)]
pub struct AuthGate {
    /// True if a SaslAuthenticate succeeded on this connection.
    pub authenticated: bool,
    /// True if the cluster config requires SASL on the wire. Set from
    /// `ClusterConfig::sasl_required` at connection-accept time.
    pub required: bool,
    /// The authenticated principal in `User:<name>` form, if any. `None`
    /// before the first successful SaslAuthenticate; the dispatcher promotes
    /// it to `User:ANONYMOUS` for downstream authz when the gate is off.
    pub principal: Option<String>,
}

impl AuthGate {
    /// Should we allow this API key without authentication?
    /// ApiVersions, SaslHandshake, and SaslAuthenticate are part of the
    /// connection-setup protocol clients run before they have credentials,
    /// so they bypass the gate. Everything else requires auth when the gate
    /// is enabled.
    fn allows_pre_auth(api_key: crate::server::request::ApiKey) -> bool {
        use crate::server::request::ApiKey;
        matches!(
            api_key,
            ApiKey::ApiVersions | ApiKey::SaslHandshake | ApiKey::SaslAuthenticate
        )
    }
}

/// Best-effort principal extraction from a SASL PLAIN auth payload.
///
/// PLAIN's wire format is `[authzid] \0 authcid \0 password`. We treat
/// `authcid` (the actual login name) as the principal. SCRAM and other
/// mechanisms aren't implemented yet — for those this returns `None` and
/// the dispatcher leaves the principal slot empty, which causes authz to
/// fall through to `User:ANONYMOUS`.
fn principal_from_sasl_plain(auth_bytes: &[u8]) -> Option<String> {
    let mut parts = auth_bytes.splitn(3, |b| *b == 0);
    let _authzid = parts.next()?;
    let authcid = parts.next()?;
    if authcid.is_empty() {
        return None;
    }
    let username = std::str::from_utf8(authcid).ok()?;
    Some(format!("User:{}", username))
}

/// A client connection to the Kafka server.
pub struct ClientConnection {
    stream: TcpStream,
    addr: SocketAddr,
    /// Rate limiter for auth failures (optional for backwards compat)
    rate_limiter: Option<Arc<AuthRateLimiter>>,
    /// Connection-level SASL gate. Defaults to "not required" so existing
    /// deployments continue to work; set `required=true` from config when
    /// SASL is required cluster-wide.
    auth_gate: AuthGate,
    /// Maximum allowed inbound frame size for this connection.
    max_message_size: usize,
}

impl ClientConnection {
    /// Create a new client connection (without rate limiter).
    pub fn new(stream: TcpStream, addr: SocketAddr) -> Self {
        Self {
            stream,
            addr,
            rate_limiter: None,
            auth_gate: AuthGate::default(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
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
            auth_gate: AuthGate::default(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    /// Override the per-connection inbound frame cap. Used by the server
    /// to propagate `ClusterConfig::max_message_size` down to the wire.
    pub fn set_max_message_size(&mut self, max: usize) {
        self.max_message_size = max;
    }

    /// Mark this connection as requiring SASL before any non-handshake API
    /// is served. The server's accept loop calls this when
    /// `ClusterConfig::sasl_required` is true.
    pub fn require_sasl(&mut self) {
        self.auth_gate.required = true;
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
        serve_connection_requests(
            &mut self.stream,
            self.addr,
            self.max_message_size,
            handler,
            &mut self.auth_gate,
            self.rate_limiter.as_ref(),
            "plain",
            false,
        )
        .await
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
    /// Connection-level SASL gate (mirrors `ClientConnection`). The TLS
    /// listener uses the same authz/authn machinery.
    auth_gate: AuthGate,
    /// Maximum allowed inbound frame size for this connection.
    max_message_size: usize,
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
            auth_gate: AuthGate::default(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
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
            auth_gate: AuthGate::default(),
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
        }
    }

    /// Mark this connection as requiring SASL before any non-handshake API
    /// is served. Mirrors `ClientConnection::require_sasl`.
    pub fn require_sasl(&mut self) {
        self.auth_gate.required = true;
    }

    /// Override the per-connection inbound frame cap.
    pub fn set_max_message_size(&mut self, max: usize) {
        self.max_message_size = max;
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
        serve_connection_requests(
            &mut self.stream,
            self.addr,
            self.max_message_size,
            handler,
            &mut self.auth_gate,
            self.rate_limiter.as_ref(),
            "tls",
            true,
        )
        .await
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
        // Test the convenience helper against the style-parameterized form
        use crate::server::response::ApiVersionsResponseData;

        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![],
            throttle_time_ms: 0,
        };

        let standard = encode_response(1, &response).unwrap();
        let flexible = encode_response_with_style(1, &response, ResponseStyle::Flexible).unwrap();

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
    // InitProducerId versioned response framing Tests
    // ========================================================================

    #[test]
    fn test_init_producer_id_response_v1_classic_framing() {
        // v0-v1: response header v0 (no tagged fields) + classic body
        use crate::server::response::InitProducerIdResponseData;
        let resp = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 9000,
            producer_epoch: 1,
        };

        let mut body = Vec::new();
        resp.encode_versioned(&mut body, 1).unwrap();
        // throttle (4) + error (2) + producer_id (8) + epoch (2) = 16, NO tag byte
        assert_eq!(body.len(), 16);

        let framed = Response::new_raw(7, body)
            .unwrap()
            .encode_with_size()
            .unwrap();
        // size (4) + correlation (4) + body (16) = 24
        assert_eq!(framed.len(), 24);
        assert_eq!(&framed[4..8], &7i32.to_be_bytes());
        // Body starts immediately at byte 8 (no header tagged fields):
        assert_eq!(&framed[8..12], &0i32.to_be_bytes()); // throttle_time_ms
        assert_eq!(&framed[12..14], &[0x00, 0x00]); // error_code
        assert_eq!(&framed[14..22], &9000i64.to_be_bytes()); // producer_id
        assert_eq!(&framed[22..24], &1i16.to_be_bytes()); // producer_epoch
    }

    #[test]
    fn test_init_producer_id_response_v2_flexible_framing() {
        // v2+: response header v1 (tagged fields) + flexible body
        use crate::server::response::InitProducerIdResponseData;
        let resp = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 9000,
            producer_epoch: 1,
        };

        let mut body = Vec::new();
        resp.encode_versioned(&mut body, 2).unwrap();
        // 16 fixed bytes + trailing tagged-fields byte
        assert_eq!(body.len(), 17);
        assert_eq!(*body.last().unwrap(), 0x00);

        let framed = Response::new_raw_flexible(7, body)
            .unwrap()
            .encode_with_size()
            .unwrap();
        // size (4) + correlation (4) + header tag (1) + body (17) = 26
        assert_eq!(framed.len(), 26);
        assert_eq!(&framed[4..8], &7i32.to_be_bytes());
        assert_eq!(framed[8], 0x00, "header v1 must carry empty tagged fields");
        assert_eq!(&framed[9..13], &0i32.to_be_bytes()); // throttle_time_ms
        assert_eq!(&framed[15..23], &9000i64.to_be_bytes()); // producer_id
    }

    // ========================================================================
    // UnsupportedVersion response Tests
    // ========================================================================

    #[test]
    fn test_encode_unsupported_version_response_generic_api() {
        // Produce v0 (unadvertised) must produce: size prefix, the request's
        // correlation id, and the INT16 error code 35 — nothing else, and
        // definitely not a closed connection.
        let result = encode_unsupported_version_response(ApiKey::Produce, 7777).unwrap();

        // size (4) + correlation_id (4) + error_code (2) = 10 bytes
        assert_eq!(result.len(), 10);
        // Size prefix = 6
        assert_eq!(&result[0..4], &[0x00, 0x00, 0x00, 0x06]);
        // correlation_id = 7777 (0x00001E61)
        assert_eq!(&result[4..8], &7777i32.to_be_bytes());
        // error_code = 35 (UnsupportedVersion)
        assert_eq!(&result[8..10], &[0x00, 0x23]);
    }

    #[test]
    fn test_encode_unsupported_version_response_correlation_id_passthrough() {
        for correlation_id in [0i32, -1, i32::MAX, i32::MIN, 123456] {
            let result =
                encode_unsupported_version_response(ApiKey::Fetch, correlation_id).unwrap();
            assert_eq!(&result[4..8], &correlation_id.to_be_bytes());
            assert_eq!(&result[8..10], &[0x00, 0x23]);
        }
    }

    #[test]
    fn test_encode_unsupported_version_response_api_versions() {
        // ApiVersions gets the richer treatment: a v0-encoded ApiVersions
        // response with error 35 AND the advertised ranges, so the client
        // can downgrade and retry.
        let result = encode_unsupported_version_response(ApiKey::ApiVersions, 555).unwrap();

        // correlation_id passthrough
        assert_eq!(&result[4..8], &555i32.to_be_bytes());
        // error_code = 35 right after the header (v0 body layout)
        assert_eq!(&result[8..10], &[0x00, 0x23]);
        // api_keys array length must match the advertised table
        let count = i32::from_be_bytes(result[10..14].try_into().unwrap());
        assert_eq!(
            count as usize,
            crate::server::versions::SUPPORTED_VERSIONS.len()
        );
        // v0 layout: no throttle_time_ms after the array.
        // size (4) + correlation (4) + error (2) + len (4) + count * 6
        assert_eq!(result.len(), 14 + count as usize * 6);
    }

    #[test]
    fn test_peek_correlation_id_from_request_body() {
        let mut body = Vec::new();
        // api_key Produce = 0, api_version 3, correlation_id 4242
        body.extend_from_slice(&0i16.to_be_bytes());
        body.extend_from_slice(&3i16.to_be_bytes());
        body.extend_from_slice(&4242i32.to_be_bytes());
        let data = Bytes::from(body);
        assert_eq!(peek_correlation_id(&data), Some(4242));
    }

    #[test]
    fn test_peek_correlation_id_too_short() {
        assert_eq!(
            peek_correlation_id(&Bytes::from_static(&[0, 1, 2, 3])),
            None
        );
    }

    #[test]
    fn test_encode_wire_error_response_carries_correlation_id() {
        let result = encode_wire_error_response(9001, KafkaCode::InvalidRequest).unwrap();
        assert_eq!(result.len(), 10);
        assert_eq!(&result[4..8], &9001i32.to_be_bytes());
        assert_eq!(&result[8..10], &42i16.to_be_bytes()); // InvalidRequest
    }

    #[test]
    fn test_response_style_for_init_producer_id_versions() {
        use crate::server::request::ApiKey;
        assert_eq!(
            response_style_for(ApiKey::InitProducerId, 1),
            ResponseStyle::Standard
        );
        assert_eq!(
            response_style_for(ApiKey::InitProducerId, 2),
            ResponseStyle::Flexible
        );
    }

    // ========================================================================
    // Global inflight budget Tests
    // ========================================================================

    #[test]
    fn test_inflight_budget_reserve_and_release() {
        // Reserving must succeed for a small frame and permits must come
        // back when the guard drops.
        let before = GLOBAL_INFLIGHT.available_permits();
        {
            let permit = try_reserve_inflight_bytes(1024).expect("reservation should succeed");
            assert_eq!(permit.bytes, 1024);
            assert_eq!(GLOBAL_INFLIGHT.available_permits(), before - 1024);
        }
        assert_eq!(GLOBAL_INFLIGHT.available_permits(), before);
    }

    #[test]
    fn test_set_global_inflight_byte_budget_is_one_shot() {
        // Force initialization (any earlier test or frame may already have);
        // from then on the budget is frozen and the setter must say so.
        let effective = global_inflight_byte_budget();
        assert!(effective > 0);
        assert!(
            !set_global_inflight_byte_budget(123),
            "setter must refuse once the budget is frozen"
        );
        // The effective budget is unchanged by the refused call.
        assert_eq!(global_inflight_byte_budget(), effective);
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
