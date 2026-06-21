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
    DEFAULT_IDLE_CONNECTION_TIMEOUT_SECS, DEFAULT_MAX_MESSAGE_SIZE,
    DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS, DEFAULT_REQUEST_READ_TIMEOUT_SECS,
    DEFAULT_REQUEST_WRITE_TIMEOUT_SECS,
};
use crate::error::{Error, FrameRejectReason, KafkaCode, Result};

use super::handler::{Handler, RequestContext};
use super::rate_limiter::AuthRateLimiter;
use super::request::Request;
use super::response::Response;

use crate::encode::ToByte;

/// Per-connection read buffer size for the size-prefix `read_exact`.
///
/// Body reads bypass the BufReader: `read_kafka_frame` calls `read_buf`
/// directly into a fresh `BytesMut` once the size is known. Only the 4-byte
/// size prefix benefits from buffering, so a small capacity is enough — at
/// 10k connections, 64 KiB per reader pinned ~640 MiB of idle baseline.
const CONNECTION_READ_BUF_CAPACITY: usize = 4 * 1024;

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
/// Wired from the broker binary (`kafkaesque-bin`'s `main`) from
/// `ClusterConfig::global_inflight_byte_budget`; the embedding-server and test
/// paths that don't call it run with the 1 GiB default.
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
pub(crate) struct InflightPermit {
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

/// Best-effort api key from a request frame body. Lets the dispatcher
/// short-circuit unauthenticated requests against keys that aren't part
/// of the connection-setup handshake — without paying for a full body
/// parse on hostile traffic.
fn peek_api_key_raw(data: &Bytes) -> Option<i16> {
    if data.len() < 2 {
        return None;
    }
    Some(i16::from_be_bytes([data[0], data[1]]))
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

/// Encode a versioned response body directly into a single framed buffer,
/// without an intermediate body `Vec` that would later be copied into the
/// framed output. The body is written behind a pre-zeroed header, then the
/// header is patched in place.
///
/// The previous shape — encode body into a `Vec<u8>`, then wrap it in
/// `Response::new_raw{,_flexible}` which `extend_from_slice`'d the body
/// into a fresh framed `Vec` — paid one full memcpy per response on the
/// fetch hot path. For a 10 MB fetch that's 10 MB of avoided memory
/// bandwidth and a halving of resident memory at peak.
#[allow(dead_code)]
fn encode_versioned_response_into_frame<F>(
    correlation_id: i32,
    api_key: crate::server::request::ApiKey,
    api_version: i16,
    encode_body: F,
) -> Result<Vec<u8>>
where
    F: FnOnce(&mut Vec<u8>) -> Result<()>,
{
    let flexible = matches!(api_key.response_style(api_version), ResponseStyle::Flexible);
    let header_len: usize = if flexible { 9 } else { 8 };

    let mut framed = Vec::with_capacity(header_len + 1024);
    framed.resize(header_len, 0);
    encode_body(&mut framed)?;

    let body_len = framed.len() - 4;
    let size: i32 = body_len as i32;
    framed[0..4].copy_from_slice(&size.to_be_bytes());
    framed[4..8].copy_from_slice(&correlation_id.to_be_bytes());
    if flexible {
        framed[8] = 0;
    }

    Ok(framed)
}

/// What the dispatch path produces. Most arms produce a contiguous
/// `Vec<u8>` framed buffer; the fetch arm produces a `BytesChain` so the
/// records `Bytes` from SlateDB can ride to the socket via vectored I/O
/// without being memcpy'd into a single buffer.
pub(crate) enum DispatchedResponse {
    Buffer(Vec<u8>),
    Chain(kafkaesque_protocol::bytes_chain::BytesChain),
}

impl From<Vec<u8>> for DispatchedResponse {
    fn from(v: Vec<u8>) -> Self {
        Self::Buffer(v)
    }
}

/// Encode a versioned fetch response into a `BytesChain` of zero-copy
/// chunks. The records `Bytes` from SlateDB ride through unchanged; only
/// the framing header, per-partition metadata, and length prefixes are
/// memcpy'd. For a 10 MiB multi-partition fetch this saves 10 MiB of
/// memory bandwidth and halves peak RSS — see `bytes_chain.rs` for the
/// design rationale.
fn encode_fetch_response_into_chain(
    correlation_id: i32,
    api_key: crate::server::request::ApiKey,
    api_version: i16,
    response: &crate::server::response::FetchResponseData,
) -> Result<kafkaesque_protocol::bytes_chain::BytesChain> {
    use bytes::{BufMut as _, BytesMut};
    use kafkaesque_protocol::bytes_chain::BytesChain;

    let flexible = matches!(api_key.response_style(api_version), ResponseStyle::Flexible);
    let header_len: usize = if flexible { 9 } else { 8 };

    // Build the body into a chain. Header bytes accumulate in `body_buf`
    // and get pushed onto the chain whenever a partition with records
    // forces a split (so we can wedge the records `Bytes` in zero-copy).
    let mut body_chain = BytesChain::with_capacity(
        2 + response
            .responses
            .iter()
            .map(|t| t.partitions.len())
            .sum::<usize>()
            * 2,
    );
    let mut body_buf = BytesMut::with_capacity(1024);
    response.encode_versioned_into_chain(&mut body_chain, &mut body_buf, api_version)?;
    if !body_buf.is_empty() {
        body_chain.push(body_buf.freeze());
    }

    // Patch the framing header. `body_len` is whatever's in body_chain;
    // total wire frame is 4-byte size + correlation_id + (optional
    // flexible byte) + body. The size field counts everything after
    // itself (correlation_id + flexible byte + body).
    let body_len = body_chain.len();
    let size: i32 = (body_len + (header_len - 4)) as i32;
    let mut header = BytesMut::with_capacity(header_len);
    header.extend_from_slice(&size.to_be_bytes());
    header.extend_from_slice(&correlation_id.to_be_bytes());
    if flexible {
        header.put_u8(0);
    }

    let mut out = BytesChain::with_capacity(1 + body_chain.chunks().len());
    out.push(header.freeze());
    for chunk in body_chain.into_chunks() {
        out.push(chunk);
    }
    Ok(out)
}

/// Read one length-prefixed Kafka request frame from any async stream.
///
/// Returns the decoded frame plus the inflight-byte permit that reserved
/// `frame_size` bytes against the global budget. The caller MUST keep the
/// permit alive until dispatch (parse + handler + response encode) completes
/// — dropping it the moment the frame is read defeats the bound it advertises.
async fn read_kafka_frame<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_message_size: usize,
) -> Result<(Bytes, InflightPermit)> {
    read_kafka_frame_with_deadline(stream, max_message_size, None).await
}

/// Per-read deadline window for the body slurp loop. A client that ships
/// the size prefix and then dribbles body bytes one byte at a time would
/// otherwise occupy a connection slot for the full per-frame read timeout
/// (30 s by default) — this enforces a tight inter-read deadline so a
/// trickle stalls and times out instead of pinning a slot.
const BODY_INTER_READ_TIMEOUT: Duration = Duration::from_secs(5);

async fn read_kafka_frame_with_deadline<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_message_size: usize,
    body_inter_read_timeout: Option<Duration>,
) -> Result<(Bytes, InflightPermit)> {
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.map_err(|e| {
        if e.kind() == io::ErrorKind::UnexpectedEof {
            Error::PeerClosed("Connection closed".to_owned())
        } else {
            Error::from(e)
        }
    })?;

    let size = (&size_buf[..]).get_i32();
    if size < 0 {
        return Err(Error::FrameRejected {
            reason: FrameRejectReason::MalformedSize,
            detail: format!("Invalid negative message size: {size}"),
        });
    }
    let size = size as usize;
    const MIN_REQUEST_BODY_SIZE: usize = 8; // api_key + api_version + correlation_id
    if size < MIN_REQUEST_BODY_SIZE {
        return Err(Error::FrameRejected {
            reason: FrameRejectReason::MalformedSize,
            detail: format!(
                "Message size {size} is below minimum frame body size {MIN_REQUEST_BODY_SIZE}"
            ),
        });
    }
    if size > max_message_size {
        return Err(Error::FrameRejected {
            reason: FrameRejectReason::FrameTooLarge,
            detail: format!("Message size {size} exceeds maximum allowed size {max_message_size}"),
        });
    }

    let permit = match try_reserve_inflight_bytes(size) {
        Some(p) => p,
        None => {
            return Err(Error::FrameRejected {
                reason: FrameRejectReason::InflightBudgetExhausted,
                detail: format!("Server inflight memory budget exhausted (frame size {size})"),
            });
        }
    };

    // Avoid zero-initializing the buffer before `read_exact` overwrites it.
    // For a 100 MB produce frame the previous `vec![0u8; size]` zero-fills
    // 100 MB on every read just to immediately overwrite — measurable on
    // the produce hot path. `BytesMut::with_capacity` allocates uninitialized
    // capacity and `read_buf` writes directly into it.
    //
    // Cap the initial allocation to a moderate window. A single connection
    // declaring a 100 MB size would otherwise hit the allocator with a 100 MB
    // request before any body byte is read; `read_buf` grows the buffer on
    // demand, so a 64 KiB starting window is enough to absorb most frames in
    // one allocation while bounding the up-front damage from oversized or
    // adversarial size prefixes that pass the message-size cap.
    const INITIAL_FRAME_BUF_CAPACITY: usize = 64 * 1024;
    let mut buf = bytes::BytesMut::with_capacity(size.min(INITIAL_FRAME_BUF_CAPACITY));
    {
        use tokio::io::AsyncReadExt;
        // `read_buf` advances the buffer's length as it reads. Loop until we
        // have all `size` bytes; treat 0-length reads as a closed connection
        // mid-message (matches the previous read_exact semantics).
        while buf.len() < size {
            let read_fut = stream.read_buf(&mut buf);
            let outcome = match body_inter_read_timeout {
                Some(per_read) => match timeout(per_read, read_fut).await {
                    Ok(r) => r,
                    Err(_) => {
                        return Err(Error::FrameRejected {
                            reason: FrameRejectReason::BodyReadTimeout,
                            detail: format!(
                                "Frame body inter-read timeout after {:?} ({} of {} bytes)",
                                per_read,
                                buf.len(),
                                size
                            ),
                        });
                    }
                },
                None => read_fut.await,
            };
            match outcome {
                Ok(0) => {
                    return Err(Error::PeerClosed(
                        "Connection closed mid-message".to_owned(),
                    ));
                }
                Ok(_) => {}
                Err(e) => return Err(Error::from(e)),
            }
        }
    }

    Ok((buf.freeze(), permit))
}

/// Public re-export of [`read_kafka_frame`] for fuzz harnesses.
///
/// The production callers live inside this module; the frame reader itself
/// is the broker's first contact with attacker bytes, so a fuzz target
/// against it is part of the security boundary. This thin wrapper lets the
/// `kafkaesque-fuzz` crate drive the reader without re-implementing it. The
/// permit is dropped here because fuzz drivers don't run dispatch.
pub async fn read_kafka_frame_for_fuzz<S: AsyncRead + Unpin>(
    stream: &mut S,
    max_message_size: usize,
) -> Result<Bytes> {
    read_kafka_frame(stream, max_message_size)
        .await
        .map(|(bytes, _permit)| bytes)
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

/// Write a length-prefixed Kafka response built as a chain of `Bytes`
/// chunks. Each chunk is written in order with a single `write_all` per
/// chunk; on a tokio TcpStream this lets the kernel coalesce contiguous
/// chunks into one TCP segment without us first memcpy'ing them into a
/// single buffer. The fetch path can pour records straight from SlateDB
/// into the chain (refcount-bump, no copy), and the bytes ride through
/// to TCP unchanged.
#[allow(dead_code)]
async fn write_kafka_frame_chain<S: AsyncWrite + Unpin>(
    stream: &mut S,
    chain: &kafkaesque_protocol::bytes_chain::BytesChain,
    client: SocketAddr,
) -> Result<()> {
    let write_deadline = Duration::from_secs(DEFAULT_REQUEST_WRITE_TIMEOUT_SECS);
    let total_len = chain.len();
    match timeout(write_deadline, async {
        for chunk in chain.chunks() {
            stream.write_all(chunk).await.map_err(Error::from)?;
        }
        stream.flush().await.map_err(Error::from)
    })
    .await
    {
        Ok(Ok(())) => Ok(()),
        Ok(Err(e)) => Err(e),
        Err(_) => {
            tracing::warn!(
                client = %client,
                response_len = total_len,
                timeout_secs = DEFAULT_REQUEST_WRITE_TIMEOUT_SECS,
                "Chunked response write timed out (slow / non-reading client)"
            );
            Err(Error::MissingData("Response write timeout".to_owned()))
        }
    }
}

/// Shared request loop for plain and TLS connections.
#[allow(clippy::too_many_arguments)]
async fn serve_connection_requests<H, R, W>(
    reader: &mut R,
    writer: &mut W,
    client: SocketAddr,
    max_message_size: usize,
    handler: Arc<H>,
    auth_gate: &mut AuthGate,
    rate_limiter: Option<&Arc<AuthRateLimiter>>,
    connection_label: &'static str,
    transport_tls: bool,
) -> Result<()>
where
    H: Handler + 'static,
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    // Per-connection ID. Process-unique even across TCP socket-address
    // reuse so server-side per-connection state (SASL post-auth, SCRAM
    // session, mechanism commitment) cannot leak across connections that
    // briefly share a `client_addr`. A monotonic atomic counter is
    // sufficient; we don't expose it externally so wrap-around concerns
    // are theoretical (`u64`).
    static CONN_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let conn_id = CONN_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Run on_connection_closed even on cancellation paths (forced shutdown,
    // panic, deadline aborts). Without this guard, cancellation drops the
    // outer future before the explicit call below ever runs and per-
    // connection auth/SASL state owned by the handler accumulates dead
    // entries that future connections may inherit.
    struct CloseGuard<G: Handler + 'static> {
        handler: Option<Arc<G>>,
        conn_id: u64,
    }
    impl<G: Handler + 'static> Drop for CloseGuard<G> {
        fn drop(&mut self) {
            if let Some(h) = self.handler.take() {
                let conn_id = self.conn_id;
                tokio::spawn(async move {
                    h.on_connection_closed(conn_id).await;
                });
            }
        }
    }
    let _close_guard = CloseGuard {
        handler: Some(handler.clone()),
        conn_id,
    };

    // Compute the client-host string once per connection. Every request
    // previously paid a `client_addr.ip().to_string()` + `Arc::from` per
    // dispatch; the value is invariant for the connection's lifetime.
    let client_host: Arc<str> = Arc::from(client.ip().to_string());

    let read_timeout = Duration::from_secs(DEFAULT_REQUEST_READ_TIMEOUT_SECS);
    let idle_timeout = Duration::from_secs(DEFAULT_IDLE_CONNECTION_TIMEOUT_SECS);
    let handler_timeout = Duration::from_secs(DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS);
    // Bound the FIRST frame's read with a tighter timeout. A client that
    // connects but never sends a byte ties up a connection slot for the full
    // per-frame `read_timeout` (30s by default); at the default 1024 max
    // total connections a steady ~34 conn/s pins the broker. The first-byte
    // window is generous enough for slow startup paths but short enough that
    // an idle attacker rotates through slots fast.
    let first_byte_timeout = Duration::from_secs(5).min(read_timeout);
    // Cap unauthenticated inbound frames to a tiny budget. Pre-auth API keys
    // (ApiVersions / SaslHandshake / SaslAuthenticate) all fit comfortably
    // under 16 KiB; refusing larger frames before any parsing collapses the
    // attack surface for a flood of attacker-shaped Produce/Fetch bodies on
    // a SASL-required server.
    const PRE_AUTH_MAX_FRAME_SIZE: usize = 16 * 1024;
    let mut is_first_frame = true;

    let result: Result<()> = async {
        loop {
            // Pre-auth: shrink both the maximum accepted frame size AND the
            // body inter-read window. After SASL completes the connection
            // gets the full configured budget. Keeping the deadline on after
            // auth is fine — production clients ship body bytes contiguously,
            // so a 5-second per-read window is generous for any non-malicious
            // sender.
            let pre_auth = auth_gate.required && !auth_gate.authenticated;
            // Outer timeout gates how long we wait for the next frame's size
            // prefix. The body slurp inside `read_kafka_frame_with_deadline`
            // is independently bounded by `BODY_INTER_READ_TIMEOUT`, so
            // widening this window does not weaken the slowloris guard for
            // in-progress reads.
            //
            // - First frame: tight slowloris bound on a freshly-accepted
            //   socket that hasn't sent anything yet.
            // - Pre-auth: keep the short `read_timeout` so an unauthenticated
            //   client cannot hold a slot idle between handshake frames.
            // - Post-auth (or no-auth): a producer batching messages on a
            //   slow runtime may legitimately go quiet between frames for
            //   tens of seconds. Real Kafka brokers default
            //   `connections.max.idle.ms` to 10 minutes; align with the
            //   existing `idle_timeout` (5 min) so e2e/CI runs under load
            //   don't drop kept-alive client connections mid-test.
            let effective_read_timeout = if is_first_frame {
                first_byte_timeout
            } else if pre_auth {
                read_timeout
            } else {
                idle_timeout
            };
            let effective_max_size = if pre_auth {
                max_message_size.min(PRE_AUTH_MAX_FRAME_SIZE)
            } else {
                max_message_size
            };
            let read_result = match timeout(
                effective_read_timeout,
                read_kafka_frame_with_deadline(
                    reader,
                    effective_max_size,
                    Some(BODY_INTER_READ_TIMEOUT),
                ),
            )
            .await
            {
                Ok(result) => {
                    is_first_frame = false;
                    result
                }
                Err(_) => {
                    tracing::warn!(
                        client = %client,
                        connection = connection_label,
                        timeout_secs = effective_read_timeout.as_secs(),
                        first_frame = is_first_frame,
                        "Request read timeout - closing connection"
                    );
                    return Err(Error::MissingData("Request read timeout".to_owned()));
                }
            };

            match read_result {
                Ok((data, inflight_permit)) => {
                    let correlation_id = peek_correlation_id(&data);
                    let dispatch_result = timeout(handler_timeout, async {
                        let (result, auth_result) = dispatch_request_common(
                            handler.as_ref(),
                            data,
                            client,
                            conn_id,
                            &client_host,
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
                        Ok(Ok(DispatchedResponse::Buffer(response))) => {
                            write_kafka_frame(writer, &response, client).await?
                        }
                        Ok(Ok(DispatchedResponse::Chain(chain))) => {
                            write_kafka_frame_chain(writer, &chain, client).await?
                        }
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
                                let _ = write_kafka_frame(writer, &err_resp, client).await;
                                use tokio::io::AsyncWriteExt;
                                let _ = writer.flush().await;
                                let _ = writer.shutdown().await;
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
                                    encode_wire_error_response(cid, KafkaCode::RequestTimedOut)
                            {
                                let _ = write_kafka_frame(writer, &err_resp, client).await;
                                use tokio::io::AsyncWriteExt;
                                let _ = writer.flush().await;
                                let _ = writer.shutdown().await;
                            }
                            return Err(Error::MissingData("Request handler timeout".to_owned()));
                        }
                    }
                    // Drop the permit only AFTER dispatch + write are done so
                    // the budget actually bounds bytes-allocated-but-not-yet
                    // -dispatched, not just bytes-currently-being-read.
                    drop(inflight_permit);
                }
                Err(Error::PeerClosed(_)) => {
                    tracing::debug!(client = %client, "Client disconnected");
                    return Ok(());
                }
                Err(Error::FrameRejected { reason, detail }) => {
                    // Capacity / DoS signal — meter it and log at warn so an
                    // operator can alert on oversized-frame, slow-client, or
                    // inflight-budget-exhaustion pressure. Previously these
                    // were `MissingData` and indistinguishable from a benign
                    // client disconnect (debug-level, uncounted). Close the
                    // connection cleanly: framing can no longer be trusted, but
                    // this is a deliberate shed (the metric + warn here carry
                    // the signal), not a connection error to re-log upstream.
                    crate::cluster::metrics::record_frame_rejected(reason.as_str());
                    tracing::warn!(
                        client = %client,
                        connection = connection_label,
                        reason = %reason,
                        detail = %detail,
                        "Rejected inbound frame"
                    );
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
    }
    .await;

    // _close_guard's Drop schedules handler.on_connection_closed on every
    // exit path (Ok, Err, panic, cancel).
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
    conn_id: u64,
    client_host: &Arc<str>,
    _connection_type: &str, // Used for logging to distinguish plain vs TLS
    auth_gate: &mut AuthGate,
    transport_tls: bool,
) -> (Result<DispatchedResponse>, AuthResult) {
    let start = Instant::now();

    tracing::debug!(
        client = %client_addr,
        data_len = data.len(),
        first_bytes = ?&data[..data.len().min(32)],
        "Received request data"
    );

    // Pre-parse auth gate. Rejecting before Request::parse keeps a flood of
    // attacker-shaped Produce/Fetch frames from forcing the broker through
    // body decoding, allocator pressure, and per-handler dispatch. The
    // post-parse gate below stays as a defense in depth (catches frames
    // whose api_key bytes don't decode cleanly into the ApiKey enum).
    if auth_gate.required
        && !auth_gate.authenticated
        && let Some(raw_key) = peek_api_key_raw(&data)
    {
        use crate::server::request::ApiKey;
        let key = ApiKey::from(raw_key);
        if !AuthGate::allows_pre_auth(key) {
            tracing::warn!(
                client = %client_addr,
                api_key_raw = raw_key,
                "Pre-parse rejection of unauthenticated request"
            );
            let cid = peek_correlation_id(&data).unwrap_or(0);
            return (
                encode_wire_error_response(cid, KafkaCode::SaslAuthenticationFailed)
                    .map(DispatchedResponse::from),
                AuthResult::Failure,
            );
        }
    }

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
                    encode_wire_error_response(correlation_id, KafkaCode::InvalidRequest)
                        .map(DispatchedResponse::from),
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
        // Treat the rejection as an auth Failure so the per-IP rate limiter
        // counts repeated probing. Returning NotAuth here let an attacker
        // hammer Produce/Fetch/Metadata against a SASL-required server
        // forever without ever tripping the per-IP failure threshold.
        return (
            encode_wire_error_response(correlation_id, KafkaCode::SaslAuthenticationFailed)
                .map(DispatchedResponse::from),
            AuthResult::Failure,
        );
    }

    // Resolve the effective principal for this request. We use
    // the gate's stored principal when present, else `User:ANONYMOUS` —
    // matching real Kafka's identity for a non-SASL connection. ACL
    // bindings against `User:ANONYMOUS` let operators allow specific things
    // through without enabling SASL.
    let principal: Arc<str> = auth_gate
        .principal
        .as_deref()
        .map(Arc::from)
        .unwrap_or_else(|| Arc::from("User:ANONYMOUS"));
    let client_host: Arc<str> = Arc::clone(client_host);

    let request_id = if tracing::enabled!(tracing::Level::DEBUG) {
        uuid::Uuid::new_v4()
    } else {
        uuid::Uuid::nil()
    };

    let ctx = RequestContext {
        client_addr,
        conn_id,
        api_version: header.api_version,
        client_id: header.client_id.clone(),
        request_id,
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

    // Track the wire-level error_code for the `record_request_with_code`
    // Prometheus label. Default `"NONE"` for happy paths and per-partition /
    // per-topic responses (those have no single top-level error). For
    // responses that DO carry a top-level error_code we promote it so SREs
    // can graph per-error-code rates during incidents
    let mut error_code_label: &'static str = "NONE";

    // The fetch arm produces a `BytesChain` (zero-copy records), every
    // other arm produces a contiguous `Vec<u8>`. Handle fetch first so we
    // can return early with a chain; the rest fold into a single
    // `Result<Vec<u8>>` and get wrapped at the end.
    if let Request::Fetch(header, req) = request {
        let resp = handler.handle_fetch(&ctx, req).await;
        let chain = encode_fetch_response_into_chain(
            correlation_id,
            header.api_key,
            header.api_version,
            &resp,
        );
        let duration = start.elapsed().as_secs_f64();
        let status = if chain.is_ok() { "success" } else { "error" };
        crate::cluster::metrics::record_request_with_code(
            api_name,
            status,
            error_code_label,
            duration,
        );
        return (chain.map(DispatchedResponse::Chain), auth_result);
    }

    let result = match request {
        Request::ApiVersions(header, req) => {
            let response = handler.handle_api_versions(&ctx, req).await;
            error_code_label = response.error_code.metric_label();
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
        Request::Fetch(_, _) => {
            // Handled by the early-exit zero-copy chain path above; the
            // dispatch flow returns before reaching this match.
            unreachable!("Fetch is dispatched on the chain path");
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
            error_code_label = resp.error_code.metric_label();
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::FindCoordinator(_, req) => {
            let resp = handler.handle_find_coordinator(&ctx, req).await;
            error_code_label = resp.error_code.metric_label();
            encode_response(correlation_id, &resp)
        }
        Request::JoinGroup(header, req) => {
            let resp = handler.handle_join_group(&ctx, req).await;
            error_code_label = resp.error_code.metric_label();
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::Heartbeat(header, req) => {
            let resp = handler.handle_heartbeat(&ctx, req).await;
            error_code_label = resp.error_code.metric_label();
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::LeaveGroup(header, req) => {
            let resp = handler.handle_leave_group(&ctx, req).await;
            error_code_label = resp.error_code.metric_label();
            let mut body = Vec::new();
            if let Err(e) = resp.encode_versioned(&mut body, header.api_version) {
                return (Err(e), auth_result);
            }
            encode_versioned_raw_response(correlation_id, header.api_key, header.api_version, body)
        }
        Request::SyncGroup(header, req) => {
            let resp = handler.handle_sync_group(&ctx, req).await;
            error_code_label = resp.error_code.metric_label();
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
            let resp = handler.handle_list_groups(&ctx, req).await;
            error_code_label = resp.error_code.metric_label();
            encode_response(correlation_id, &resp)
        }
        Request::DeleteGroups(_, req) => encode_response(
            correlation_id,
            &handler.handle_delete_groups(&ctx, req).await,
        ),
        Request::SaslHandshake(_, req) => {
            let resp = handler.handle_sasl_handshake(&ctx, req).await;
            error_code_label = resp.error_code.metric_label();
            encode_response(correlation_id, &resp)
        }
        Request::SaslAuthenticate(_, req) => {
            // Track SASL auth result. The handler is the sole authority on
            // *what* identity a successful SASL exchange authenticated —
            // never derive a principal from the inbound bytes here, even
            // for PLAIN. A handler that returns success without populating
            // `SaslPostAuth.principal` is buggy and we treat the
            // connection as anonymous; we DO NOT fall back to the
            // attacker-controlled wire bytes (that fallback would let any
            // client self-assign `User:admin` simply by sending
            // `\0admin\0anything`).
            //
            // SCRAM-SHA-256 is multi-step: the first round-trip returns
            // `error_code = None` but the handshake isn't done yet. The
            // cluster handler signals this via `take_sasl_post_auth`,
            // which we consult before flipping the gate.
            let response = handler.handle_sasl_authenticate(&ctx, req).await;
            let post = handler.take_sasl_post_auth(ctx.conn_id).await;
            auth_result = if response.error_code == KafkaCode::None {
                match post {
                    Some(p) if p.complete => {
                        if let Some(principal) = p.principal {
                            auth_gate.authenticated = true;
                            auth_gate.principal = Some(principal);
                            AuthResult::Success
                        } else {
                            // Handler reported success but did not return a
                            // principal — refuse to flip the gate. Anything
                            // else would let the client self-assign identity.
                            tracing::warn!(
                                client = %client_addr,
                                conn_id,
                                "Handler returned SASL success without a principal; treating as failure"
                            );
                            AuthResult::Failure
                        }
                    }
                    Some(_) => {
                        // Intermediate SCRAM step — keep the gate closed,
                        // the client will send another SaslAuthenticate
                        // and we'll re-evaluate then.
                        AuthResult::NotAuth
                    }
                    None => {
                        // No post-auth record at all means the handler did
                        // not run the SASL pipeline (e.g. SASL not
                        // configured but `error_code == None` somehow). Be
                        // conservative and refuse to authenticate.
                        AuthResult::Failure
                    }
                }
            } else {
                AuthResult::Failure
            };
            error_code_label = response.error_code.metric_label();
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
        Request::DescribeConfigs(_, req) => encode_response(
            correlation_id,
            &handler.handle_describe_configs(&ctx, req).await,
        ),
        Request::AlterConfigs(_, req) => encode_response(
            correlation_id,
            &handler.handle_alter_configs(&ctx, req).await,
        ),
        Request::InitProducerId(header, req) => {
            let resp = handler.handle_init_producer_id(&ctx, req).await;
            error_code_label = resp.error_code.metric_label();
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
            error_code_label = KafkaCode::UnsupportedVersion.metric_label();
            encode_unsupported_version_response(h.api_key, correlation_id)
        }
        Request::Unknown(h, body) => encode_response(
            correlation_id,
            &handler.handle_unknown(&ctx, h.api_key, body).await,
        ),
    };
    let result = result.map(DispatchedResponse::from);

    {
        let duration = start.elapsed().as_secs_f64();
        let status = if result.is_ok() { "success" } else { "error" };
        crate::cluster::metrics::record_request_with_code(
            api_name,
            status,
            error_code_label,
            duration,
        );
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

/// A client connection to the Kafka server.
pub struct ClientConnection {
    reader: tokio::io::BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: tokio::net::tcp::OwnedWriteHalf,
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
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: tokio::io::BufReader::with_capacity(CONNECTION_READ_BUF_CAPACITY, read_half),
            writer: write_half,
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
        let (read_half, write_half) = stream.into_split();
        Self {
            reader: tokio::io::BufReader::with_capacity(CONNECTION_READ_BUF_CAPACITY, read_half),
            writer: write_half,
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
    pub async fn handle_requests<H: Handler + 'static>(&mut self, handler: Arc<H>) -> Result<()> {
        track_connection_open();

        let result = self.handle_requests_inner(handler).await;

        track_connection_close();

        result
    }

    async fn handle_requests_inner<H: Handler + 'static>(&mut self, handler: Arc<H>) -> Result<()> {
        serve_connection_requests(
            &mut self.reader,
            &mut self.writer,
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
    reader: tokio::io::BufReader<tokio::io::ReadHalf<tokio_rustls::server::TlsStream<TcpStream>>>,
    writer: tokio::io::WriteHalf<tokio_rustls::server::TlsStream<TcpStream>>,
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
        let (read_half, write_half) = tokio::io::split(stream);
        Self {
            reader: tokio::io::BufReader::with_capacity(CONNECTION_READ_BUF_CAPACITY, read_half),
            writer: write_half,
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
        let (read_half, write_half) = tokio::io::split(stream);
        Self {
            reader: tokio::io::BufReader::with_capacity(CONNECTION_READ_BUF_CAPACITY, read_half),
            writer: write_half,
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
    pub async fn handle_requests<H: Handler + 'static>(&mut self, handler: Arc<H>) -> Result<()> {
        track_connection_open();

        let result = self.handle_requests_inner(handler).await;

        track_connection_close();

        result
    }

    async fn handle_requests_inner<H: Handler + 'static>(&mut self, handler: Arc<H>) -> Result<()> {
        serve_connection_requests(
            &mut self.reader,
            &mut self.writer,
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
#[path = "connection_tests.rs"]
mod tests;
