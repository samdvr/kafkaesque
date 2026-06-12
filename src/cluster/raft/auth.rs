//! Authentication for the Raft RPC port.
//!
//! Without this, the Raft port is plaintext bincode that any host with network
//! reachability can speak — `JoinCluster` adds the caller as a learner and
//! promotes them to voter, and the legacy `ClientWrite` accepts arbitrary
//! coordination commands. Even with the size cap, the protocol
//! itself would trust every byte.
//!
//! This module wraps every Raft RPC frame with an HMAC-SHA256 over the
//! payload. Two keys are supported:
//!
//! - `cluster_secret` (`RAFT_CLUSTER_SECRET`): used for the steady-state Raft
//!   protocol — `AppendEntries`, `Vote`, `InstallSnapshot`, and the forwarded
//!   client writes (`ClientWriteWithTerm`). All cluster members must hold the
//!   same value.
//! - `join_token` (`RAFT_JOIN_TOKEN`): used **only** for `JoinCluster`. A new
//!   broker that does not yet have the cluster secret can still authenticate
//!   the join request with the join token; the operator rotates the token
//!   after onboarding finishes.
//!
//! ## Wire format
//!
//! **Unauthenticated** (`purpose=0`, dev/legacy only):
//!
//! ```text
//! [u32 total_len]
//! [u8 purpose=0]
//! [32 bytes hmac=0]
//! [bincode payload]
//! ```
//!
//! **Authenticated** (`purpose=1|2`, cluster/join keys configured):
//!
//! ```text
//! [u32 total_len]
//! [u8 purpose]              // 1=cluster, 2=join
//! [u64 timestamp_ms BE]   // sender wall clock; replay window enforced
//! [16 bytes nonce]        // unique per frame; tracked in replay cache
//! [32 bytes hmac]         // HMAC-SHA256(key, purpose || ts || nonce || payload)
//! [bincode payload]
//! ```
//!
//! The HMAC is computed over `purpose || timestamp || nonce || payload` (or
//! `purpose || payload` for unauthenticated frames) so an attacker cannot
//! relabel a cluster-key frame as a join-key frame and have it accepted by a
//! receiver that knows both keys.
//!
//! When neither key is configured cluster-wide (legacy / dev), `purpose=0` is
//! sent with a zero-byte HMAC. A receiver that *has* a key configured will
//! reject `purpose=0`; a receiver without a key configured will accept it.
//! Operators upgrading a live cluster therefore set the secret on every node
//! before turning on the gate (default is to be permissive: accept anything,
//! including `purpose=1` if the message is well-formed and the secret matches
//! when set).

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hmac::{Hmac, Mac};
use moka::sync::Cache;
use sha2::Sha256;
use subtle::ConstantTimeEq;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

type HmacSha256 = Hmac<Sha256>;

/// Length of the HMAC-SHA256 tag in bytes.
pub(crate) const HMAC_LEN: usize = 32;

/// Length of the per-frame purpose tag.
pub(crate) const PURPOSE_LEN: usize = 1;

/// Replay-defense fields on authenticated frames.
pub(crate) const TIMESTAMP_LEN: usize = 8;
pub(crate) const NONCE_LEN: usize = 16;

/// Framing overhead for unauthenticated frames.
pub(crate) const FRAME_HEADER_LEN: usize = PURPOSE_LEN + HMAC_LEN;

/// Framing overhead for authenticated (cluster/join) frames.
pub(crate) const AUTHENTICATED_FRAME_HEADER_LEN: usize =
    PURPOSE_LEN + TIMESTAMP_LEN + NONCE_LEN + HMAC_LEN;

/// How long signed frames stay valid and how long nonces are remembered.
const REPLAY_WINDOW: Duration = Duration::from_secs(300);
/// Maximum clock skew tolerated on incoming frame timestamps.
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(60);

/// Maximum size in bytes of any single Raft RPC message (request or response)
/// we will allocate for. The wire format is `[u32 length][purpose][hmac][bincode]`,
/// and the length field is attacker-controlled on the Raft listener — without
/// this cap a single byte sequence on the Raft listener could request up to
/// 4 GiB of allocation per connection.
///
/// 64 MiB comfortably covers an `InstallSnapshot` chunk (the largest legitimate
/// message) plus serialization overhead while bounding peak memory under
/// abuse. If a real message ever needs more than this we should chunk it
/// rather than uncap the read.
pub(crate) const MAX_RPC_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Purpose tag identifying which key signed a frame, and what kind of message
/// it carries. Encoded as one byte on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub(crate) enum FramePurpose {
    /// No HMAC — accepted only when the receiver has no `cluster_secret`
    /// configured. Used for backwards-compatible deployments and tests.
    Unauthenticated = 0,
    /// Standard Raft / forwarded-write traffic, signed with `cluster_secret`.
    Cluster = 1,
    /// `JoinCluster` request, signed with `join_token`.
    Join = 2,
}

impl FramePurpose {
    fn from_u8(b: u8) -> Option<Self> {
        match b {
            0 => Some(Self::Unauthenticated),
            1 => Some(Self::Cluster),
            2 => Some(Self::Join),
            _ => None,
        }
    }
}

/// Tracks recently-seen authenticated frame nonces to reject replays.
#[derive(Clone)]
pub struct ReplayCache {
    seen: Cache<[u8; NONCE_LEN], ()>,
}

impl std::fmt::Debug for ReplayCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplayCache")
            .field("seen_entries", &self.seen.entry_count())
            .finish()
    }
}

impl Default for ReplayCache {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplayCache {
    pub fn new() -> Self {
        Self {
            seen: Cache::builder()
                .time_to_live(REPLAY_WINDOW)
                .max_capacity(200_000)
                .build(),
        }
    }

    pub(crate) fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64
    }

    pub(crate) fn fresh_nonce() -> [u8; NONCE_LEN] {
        let id = uuid::Uuid::new_v4();
        let mut nonce = [0u8; NONCE_LEN];
        nonce.copy_from_slice(&id.as_bytes()[..NONCE_LEN]);
        nonce
    }

    /// Reject frames outside the replay window or with a reused nonce.
    pub fn check_and_record(
        &self,
        timestamp_ms: u64,
        nonce: &[u8; NONCE_LEN],
    ) -> std::io::Result<()> {
        let now = Self::now_ms();
        let max_skew_ms = MAX_CLOCK_SKEW.as_millis() as u64;
        let window_ms = REPLAY_WINDOW.as_millis() as u64;
        if timestamp_ms > now.saturating_add(max_skew_ms) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Raft RPC frame timestamp too far in the future",
            ));
        }
        if now.saturating_sub(timestamp_ms) > window_ms {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Raft RPC frame timestamp outside replay window",
            ));
        }
        if self.seen.contains_key(nonce) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Rejected Raft frame: nonce replay detected",
            ));
        }
        self.seen.insert(*nonce, ());
        Ok(())
    }
}

/// Shared secrets that gate Raft RPC traffic.
///
/// Wrapped in an `Arc` and cloned cheaply across connections.
#[derive(Clone)]
pub struct RaftAuthKeys {
    /// Cluster-wide HMAC key. When `Some`, every steady-state RPC must be
    /// signed with this key; when `None`, unsigned frames are accepted only
    /// if `unauthenticated_ok` is also `true`.
    cluster_secret: Option<Arc<[u8]>>,
    /// Join-only HMAC key. When `Some`, `JoinCluster` requests must be signed
    /// with this key; when `None`, the join path falls back to
    /// `cluster_secret` (so an operator can use the same key for both, or
    /// rotate them independently).
    join_token: Option<Arc<[u8]>>,
    /// Replay cache shared across all connections on this broker.
    replay_cache: Arc<ReplayCache>,
    /// Explicit opt-in for the no-secret path. `false` everywhere except
    /// `dev_unauthenticated()` and tests. Required because programmatic
    /// constructions (`RaftAuthKeys::default()`, embedded use, tests that
    /// forget `from_env`) used to silently expose the Raft control plane
    /// to any host that could route a TCP packet.
    unauthenticated_ok: bool,
}

impl std::fmt::Debug for RaftAuthKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftAuthKeys")
            .field(
                "cluster_secret_configured",
                &self.cluster_secret_configured(),
            )
            .field("join_token_configured", &self.join_token_configured())
            .field("replay_cache", &self.replay_cache)
            .finish()
    }
}

impl Default for RaftAuthKeys {
    fn default() -> Self {
        // Deny-by-default: programmatic construction (tests, embedded use,
        // `default()` callers that never go through `from_env`) must NOT
        // silently accept unauthenticated Raft frames. Use
        // `dev_unauthenticated()` to opt into the legacy permissive variant.
        Self {
            cluster_secret: None,
            join_token: None,
            replay_cache: Arc::new(ReplayCache::new()),
            unauthenticated_ok: false,
        }
    }
}

impl RaftAuthKeys {
    /// Build keys from raw bytes. Empty/whitespace-only strings count as
    /// "not set" so that a `RAFT_CLUSTER_SECRET=""` env var doesn't enable a
    /// trivially-guessable empty secret.
    pub fn from_strings(cluster_secret: Option<String>, join_token: Option<String>) -> Self {
        let normalize = |s: Option<String>| -> Option<Arc<[u8]>> {
            s.and_then(|v| {
                let trimmed = v.trim();
                if trimmed.is_empty() {
                    None
                } else {
                    Some(Arc::<[u8]>::from(trimmed.as_bytes()))
                }
            })
        };
        let cluster_secret = normalize(cluster_secret);
        let join_token = normalize(join_token);
        // Without a configured secret we deny by default: see the
        // `unauthenticated_ok` field doc. Operators that genuinely want an
        // open Raft port (single-node dev, sandbox tests) must set
        // `RAFT_ALLOW_UNAUTHENTICATED=1` or build via `dev_unauthenticated()`.
        let unauthenticated_ok = cluster_secret.is_none()
            && std::env::var("RAFT_ALLOW_UNAUTHENTICATED")
                .ok()
                .as_deref()
                .map(|v| matches!(v, "1" | "true" | "TRUE" | "yes"))
                .unwrap_or(false);
        Self {
            cluster_secret,
            join_token,
            replay_cache: Arc::new(ReplayCache::new()),
            unauthenticated_ok,
        }
    }

    /// Load keys from the standard env vars. Returns the deny-by-default
    /// value if neither secret is set and `RAFT_ALLOW_UNAUTHENTICATED` is
    /// not opted in.
    pub fn from_env() -> Self {
        Self::from_strings(
            std::env::var("RAFT_CLUSTER_SECRET").ok(),
            std::env::var("RAFT_JOIN_TOKEN").ok(),
        )
    }

    /// Build a permissive (no-secret, no-auth) variant for development and
    /// tests. Production paths must never call this.
    pub fn dev_unauthenticated() -> Self {
        Self {
            cluster_secret: None,
            join_token: None,
            replay_cache: Arc::new(ReplayCache::new()),
            unauthenticated_ok: true,
        }
    }

    /// True when the cluster secret has been configured. Server-side this
    /// flips the gate from "permissive" to "strict": unsigned and
    /// wrong-purpose frames are rejected.
    pub fn cluster_secret_configured(&self) -> bool {
        self.cluster_secret.is_some()
    }

    /// True when the join token is configured. When false, `JoinCluster`
    /// requests fall back to validating against `cluster_secret`.
    pub fn join_token_configured(&self) -> bool {
        self.join_token.is_some()
    }

    /// Borrow the cluster secret bytes, or `None` if unset.
    fn cluster_secret(&self) -> Option<&[u8]> {
        self.cluster_secret.as_deref()
    }

    /// Borrow the effective join key. If `join_token` is set we use it;
    /// otherwise we fall back to `cluster_secret`. This lets operators
    /// optionally split the two keys.
    fn join_key(&self) -> Option<&[u8]> {
        self.join_token
            .as_deref()
            .or(self.cluster_secret.as_deref())
    }

    /// Pick the purpose to use when sending a frame, given whether this
    /// caller is forming a join request.
    pub(crate) fn outbound_purpose(&self, is_join: bool) -> FramePurpose {
        if is_join {
            if self.join_key().is_some() {
                FramePurpose::Join
            } else {
                FramePurpose::Unauthenticated
            }
        } else if self.cluster_secret().is_some() {
            FramePurpose::Cluster
        } else {
            FramePurpose::Unauthenticated
        }
    }

    /// Compute the HMAC tag for an unauthenticated (legacy) frame.
    #[cfg(test)]
    pub(crate) fn sign(&self, purpose: FramePurpose, payload: &[u8]) -> [u8; HMAC_LEN] {
        self.sign_authenticated(purpose, None, None, payload)
    }

    fn sign_authenticated(
        &self,
        purpose: FramePurpose,
        timestamp_ms: Option<u64>,
        nonce: Option<&[u8; NONCE_LEN]>,
        payload: &[u8],
    ) -> [u8; HMAC_LEN] {
        let key = match purpose {
            FramePurpose::Unauthenticated => return [0u8; HMAC_LEN],
            FramePurpose::Cluster => self.cluster_secret(),
            FramePurpose::Join => self.join_key(),
        };
        let Some(key) = key else {
            return [0u8; HMAC_LEN];
        };
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(&[purpose as u8]);
        if let (Some(ts), Some(nonce)) = (timestamp_ms, nonce) {
            mac.update(&ts.to_be_bytes());
            mac.update(nonce);
        }
        mac.update(payload);
        let result = mac.finalize().into_bytes();
        let mut out = [0u8; HMAC_LEN];
        out.copy_from_slice(&result);
        out
    }

    /// Verify a received frame (legacy unauthenticated layout).
    #[cfg(test)]
    pub(crate) fn verify(
        &self,
        purpose: FramePurpose,
        tag: &[u8; HMAC_LEN],
        payload: &[u8],
    ) -> std::io::Result<()> {
        self.verify_authenticated(purpose, None, None, tag, payload)
    }

    /// Verify a received authenticated frame, including replay defense.
    pub(crate) fn verify_authenticated(
        &self,
        purpose: FramePurpose,
        timestamp_ms: Option<u64>,
        nonce: Option<&[u8; NONCE_LEN]>,
        tag: &[u8; HMAC_LEN],
        payload: &[u8],
    ) -> std::io::Result<()> {
        let key = match purpose {
            FramePurpose::Unauthenticated => {
                if self.cluster_secret_configured() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "Rejected unauthenticated Raft frame: cluster requires HMAC signing \
                         (set RAFT_CLUSTER_SECRET on every node)",
                    ));
                }
                if !self.unauthenticated_ok {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "Rejected unauthenticated Raft frame: no cluster secret configured \
                         and the unauthenticated path is not enabled. Set RAFT_CLUSTER_SECRET, \
                         or for development build via RaftAuthKeys::dev_unauthenticated() / \
                         set RAFT_ALLOW_UNAUTHENTICATED=1.",
                    ));
                }
                return Ok(());
            }
            FramePurpose::Cluster => self.cluster_secret(),
            FramePurpose::Join => self.join_key(),
        };
        let Some(key) = key else {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                format!(
                    "Rejected signed Raft frame: no key configured for purpose {:?}",
                    purpose
                ),
            ));
        };

        let (ts, nonce) = if purpose != FramePurpose::Unauthenticated {
            let (Some(ts), Some(nonce)) = (timestamp_ms, nonce) else {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Authenticated Raft frame missing timestamp/nonce replay fields",
                ));
            };
            (Some(ts), Some(nonce))
        } else {
            (None, None)
        };

        // Verify HMAC BEFORE touching the replay cache. Inserting an
        // attacker-chosen nonce into the cache before authenticating the
        // frame lets an unauthenticated peer (a) flood the cache with
        // garbage and (b) probe whether a target nonce is already present
        // — i.e. a replay-cache DoS plus a timing oracle on live nonces.
        let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
        mac.update(&[purpose as u8]);
        if let (Some(ts), Some(nonce)) = (ts, nonce) {
            mac.update(&ts.to_be_bytes());
            mac.update(nonce);
        }
        mac.update(payload);
        let expected = mac.finalize().into_bytes();
        if !bool::from(expected.ct_eq(tag.as_ref())) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Rejected Raft frame: HMAC mismatch",
            ));
        }

        if let (Some(ts), Some(nonce)) = (ts, nonce) {
            self.replay_cache.check_and_record(ts, nonce)?;
        }

        Ok(())
    }
}

/// Read a length-prefixed Raft RPC payload from `stream`, rejecting any
/// length above `MAX_RPC_MESSAGE_BYTES` *before* allocating. Unwraps the
/// purpose byte and HMAC and verifies them against `keys`.
///
/// Returns `(purpose, payload)`. Callers that route on message type (e.g.
/// "JoinCluster must arrive under the Join purpose") use the returned
/// `purpose` to enforce that policy.
pub(crate) async fn read_rpc_frame<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    keys: &RaftAuthKeys,
) -> std::io::Result<(FramePurpose, Vec<u8>)> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await?;
    let total_len = u32::from_be_bytes(len_buf) as usize;
    if total_len > MAX_RPC_MESSAGE_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Raft RPC frame length {} exceeds cap {} bytes",
                total_len, MAX_RPC_MESSAGE_BYTES
            ),
        ));
    }
    if total_len < FRAME_HEADER_LEN {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Raft RPC frame length {} smaller than minimum header {}",
                total_len, FRAME_HEADER_LEN
            ),
        ));
    }

    let mut purpose_byte = [0u8; PURPOSE_LEN];
    stream.read_exact(&mut purpose_byte).await?;
    let purpose = FramePurpose::from_u8(purpose_byte[0]).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Unknown Raft RPC frame purpose tag {}", purpose_byte[0]),
        )
    })?;

    let (timestamp_ms, nonce, header_len) = if purpose == FramePurpose::Unauthenticated {
        if total_len < FRAME_HEADER_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Unauthenticated Raft frame too short",
            ));
        }
        (None, None, FRAME_HEADER_LEN)
    } else {
        if total_len < AUTHENTICATED_FRAME_HEADER_LEN {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Authenticated Raft frame missing replay header fields",
            ));
        }
        let mut ts_buf = [0u8; TIMESTAMP_LEN];
        stream.read_exact(&mut ts_buf).await?;
        let mut nonce = [0u8; NONCE_LEN];
        stream.read_exact(&mut nonce).await?;
        (
            Some(u64::from_be_bytes(ts_buf)),
            Some(nonce),
            AUTHENTICATED_FRAME_HEADER_LEN,
        )
    };

    let mut tag = [0u8; HMAC_LEN];
    stream.read_exact(&mut tag).await?;

    let payload_len = total_len - header_len;
    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).await?;

    keys.verify_authenticated(purpose, timestamp_ms, nonce.as_ref(), &tag, &payload)?;
    Ok((purpose, payload))
}

/// Sign and send a length-prefixed Raft RPC payload over `stream`. The chosen
/// purpose is `Cluster` for ordinary traffic and `Join` for `JoinCluster`
/// requests; falls back to `Unauthenticated` when no matching key is set.
pub(crate) async fn write_rpc_frame<S: AsyncWriteExt + Unpin>(
    stream: &mut S,
    keys: &RaftAuthKeys,
    is_join: bool,
    payload: &[u8],
) -> std::io::Result<()> {
    let purpose = keys.outbound_purpose(is_join);
    let (timestamp_ms, nonce, header_len) = if purpose == FramePurpose::Unauthenticated {
        (None, None, FRAME_HEADER_LEN)
    } else {
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        (Some(ts), Some(nonce), AUTHENTICATED_FRAME_HEADER_LEN)
    };
    let tag = keys.sign_authenticated(purpose, timestamp_ms, nonce.as_ref(), payload);
    let total_len = payload
        .len()
        .checked_add(header_len)
        .ok_or_else(|| std::io::Error::other("Raft RPC payload too large to frame"))?;
    if total_len > MAX_RPC_MESSAGE_BYTES {
        return Err(std::io::Error::other(format!(
            "Raft RPC frame length {} exceeds cap {} bytes",
            total_len, MAX_RPC_MESSAGE_BYTES
        )));
    }
    let total_len_u32: u32 = total_len.try_into().map_err(|_| {
        std::io::Error::other(format!(
            "Raft RPC frame length {} exceeds u32 range",
            total_len
        ))
    })?;
    stream.write_all(&total_len_u32.to_be_bytes()).await?;
    stream.write_all(&[purpose as u8]).await?;
    if let (Some(ts), Some(nonce)) = (timestamp_ms, nonce) {
        stream.write_all(&ts.to_be_bytes()).await?;
        stream.write_all(&nonce).await?;
    }
    stream.write_all(&tag).await?;
    stream.write_all(payload).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unauthenticated_keys_accept_unsigned() {
        let keys = RaftAuthKeys::dev_unauthenticated();
        let payload = b"hello";
        let tag = keys.sign(FramePurpose::Unauthenticated, payload);
        assert_eq!(tag, [0u8; HMAC_LEN]);
        keys.verify(FramePurpose::Unauthenticated, &tag, payload)
            .expect("unauthenticated traffic accepted with no key set");
    }

    #[test]
    fn default_keys_reject_unsigned() {
        let keys = RaftAuthKeys::default();
        let payload = b"hello";
        let tag = [0u8; HMAC_LEN];
        let err = keys
            .verify(FramePurpose::Unauthenticated, &tag, payload)
            .expect_err("default RaftAuthKeys must deny unauthenticated frames");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn configured_keys_reject_unsigned() {
        let keys = RaftAuthKeys::from_strings(Some("hunter2".into()), None);
        let payload = b"hello";
        let tag = [0u8; HMAC_LEN];
        let err = keys
            .verify(FramePurpose::Unauthenticated, &tag, payload)
            .expect_err("server with key set must reject unsigned frame");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn cluster_signed_roundtrip() {
        let keys = RaftAuthKeys::from_strings(Some("hunter2".into()), None);
        let payload = b"vote-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let tag = keys.sign_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), payload);
        keys.verify_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), &tag, payload)
            .unwrap();
    }

    #[test]
    fn cluster_signed_rejects_tamper() {
        let keys = RaftAuthKeys::from_strings(Some("hunter2".into()), None);
        let payload = b"vote-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let mut tag =
            keys.sign_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), payload);
        tag[0] ^= 0xff;
        let err = keys
            .verify_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), &tag, payload)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn cluster_signed_rejects_replay() {
        let keys = RaftAuthKeys::from_strings(Some("hunter2".into()), None);
        let payload = b"vote-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let tag = keys.sign_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), payload);
        keys.verify_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), &tag, payload)
            .unwrap();
        let err = keys
            .verify_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), &tag, payload)
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(err.to_string().contains("replay"));
    }

    #[test]
    fn cross_purpose_signature_rejected() {
        // Signing with Cluster but presenting as Join must not validate, even
        // when both keys exist (this is why we mix the purpose byte into the
        // HMAC input).
        let keys = RaftAuthKeys::from_strings(Some("clusterkey".into()), Some("joinkey".into()));
        let payload = b"join-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let cluster_tag =
            keys.sign_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), payload);
        let err = keys
            .verify_authenticated(
                FramePurpose::Join,
                Some(ts),
                Some(&nonce),
                &cluster_tag,
                payload,
            )
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn join_falls_back_to_cluster_secret_when_token_missing() {
        let keys = RaftAuthKeys::from_strings(Some("clusterkey".into()), None);
        let payload = b"join-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let tag = keys.sign_authenticated(FramePurpose::Join, Some(ts), Some(&nonce), payload);
        keys.verify_authenticated(FramePurpose::Join, Some(ts), Some(&nonce), &tag, payload)
            .unwrap();
    }

    #[test]
    fn empty_secret_treated_as_unset() {
        let keys = RaftAuthKeys::from_strings(Some("   ".into()), Some(String::new()));
        assert!(!keys.cluster_secret_configured());
        assert!(!keys.join_token_configured());
    }

    #[tokio::test]
    async fn frame_roundtrip_over_pipe() {
        use tokio::io::duplex;
        let keys = RaftAuthKeys::from_strings(Some("hunter2".into()), None);
        let (mut a, mut b) = duplex(1024);
        let payload = b"hello-raft".to_vec();

        let send_keys = keys.clone();
        let send_payload = payload.clone();
        let writer = tokio::spawn(async move {
            write_rpc_frame(&mut a, &send_keys, false, &send_payload)
                .await
                .unwrap();
        });

        let recv = read_rpc_frame(&mut b, &keys).await.unwrap();
        writer.await.unwrap();
        assert_eq!(recv.0, FramePurpose::Cluster);
        assert_eq!(recv.1, payload);
    }

    #[tokio::test]
    async fn frame_rejects_oversized_length() {
        // Hand-build a frame with a length far above the cap; reader should
        // refuse before allocating.
        use tokio::io::duplex;
        let keys = RaftAuthKeys::default();
        let (mut a, mut b) = duplex(64);
        let bogus_len = (MAX_RPC_MESSAGE_BYTES as u64 + 1) as u32;
        tokio::spawn(async move {
            let _ = a.write_all(&bogus_len.to_be_bytes()).await;
        });
        let err = read_rpc_frame(&mut b, &keys).await.unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
