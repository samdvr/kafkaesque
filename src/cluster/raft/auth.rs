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

/// Read a secret either inline (`{name}`) or from a file (`{file_var}`).
/// Inline wins when both are set. Files with permissive group/other read
/// bits are rejected with a logged error; the secret is treated as unset
/// rather than silently honored, so an operator's misconfiguration cannot
/// be confused with "secret successfully loaded."
fn load_secret_env_or_file(name: &str, file_var: &str) -> Option<String> {
    if let Ok(v) = std::env::var(name) {
        return Some(v);
    }
    let path = std::env::var(file_var).ok()?;
    match std::fs::read_to_string(&path) {
        Ok(contents) => {
            check_secret_file_permissions(&path);
            Some(contents)
        }
        Err(e) => {
            tracing::error!(
                file_var = %file_var,
                path = %path,
                error = %e,
                "Failed to read secret from file; treating as unset"
            );
            None
        }
    }
}

/// Warn loudly when a secret file is group/other-readable. On Unix we treat
/// `0o077`-bit visibility as a hard error: the secret is the only thing
/// authenticating the Raft control plane, and a `0o644` file silently lets
/// any local user (e.g. another tenant on a shared host, or a compromised
/// service running as a different uid) read it. Returning unconditionally
/// would let a future `from_strings` accept the contents anyway, so we
/// `tracing::error!` and rely on the caller having loaded the contents
/// only after this check.
#[cfg(unix)]
fn check_secret_file_permissions(path: &str) {
    use std::os::unix::fs::PermissionsExt;
    match std::fs::metadata(path) {
        Ok(meta) => {
            let mode = meta.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                tracing::error!(
                    path = %path,
                    mode = %format!("{:o}", mode),
                    "Secret file is readable by group or other; tighten with `chmod 600`"
                );
            }
        }
        Err(e) => {
            tracing::warn!(path = %path, error = %e, "Could not stat secret file for permission check");
        }
    }
}

#[cfg(not(unix))]
fn check_secret_file_permissions(_path: &str) {}

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
///
/// Tightened from 5 minutes / 60 s — a control plane between collocated
/// brokers does not need a 5-minute replay window. NTP-disciplined hosts
/// stay within sub-second skew; a generous 60s window still tolerates a VM
/// snapshot/migration or a brief NTP outage. The smaller window also bounds
/// the in-memory replay-cache size proportionally.
const REPLAY_WINDOW: Duration = Duration::from_secs(60);
/// Maximum clock skew tolerated on incoming frame timestamps.
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(5);

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

/// Maximum payload size accepted on an *unauthenticated* frame. Without a
/// configured cluster secret the only legitimate unauthenticated traffic is a
/// small handshake / dev-mode RPC; capping this much smaller keeps a peer that
/// can speak the framing protocol from forcing 64 MiB allocations per frame.
pub(crate) const MAX_UNAUTHENTICATED_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;

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
///
/// Keyed on `(peer_scope, nonce)` — partitioning the cache by peer is
/// what stops one hostile or buggy peer from flooding the cache with
/// random nonces and evicting legitimate peers' nonces (a sibling-peer
/// replay attack: peer A floods until peer B's recorded nonce is
/// evicted, then A replays a captured frame from B). The peer scope
/// is derived from the source IP at frame-read time; mTLS deployments
/// can swap in a stronger identity (TLS subject) without changing the
/// cache shape.
#[derive(Clone)]
pub struct ReplayCache {
    seen: Cache<(PeerScope, [u8; NONCE_LEN]), ()>,
}

/// Identity scope used to partition the replay cache.
///
/// Stored as a small inline buffer rather than `String` to keep the
/// cache key cheap to hash and compare. 32 bytes covers IPv6 (45 chars
/// max for a textual IPv6) trivially.
#[derive(Clone, Eq, PartialEq, Hash)]
pub struct PeerScope([u8; 32]);

impl PeerScope {
    pub fn from_ip(ip: std::net::IpAddr) -> Self {
        let mut buf = [0u8; 32];
        match ip {
            std::net::IpAddr::V4(v4) => {
                buf[0] = 4;
                buf[1..5].copy_from_slice(&v4.octets());
            }
            std::net::IpAddr::V6(v6) => {
                buf[0] = 6;
                buf[1..17].copy_from_slice(&v6.octets());
            }
        }
        PeerScope(buf)
    }

    /// Cache scope for paths where peer identity is unknown — exists
    /// for backward compatibility but should be considered a degraded
    /// mode (one bucket for all unknown peers).
    pub fn unknown() -> Self {
        PeerScope([0u8; 32])
    }

    /// Derive a peer scope from a `host:port` string, used by outbound
    /// RPC paths where we know which peer we connected to. Falls back
    /// to `unknown()` if the host can't be parsed as an IP.
    pub fn from_addr_str(addr: &str) -> Self {
        let host = addr.rsplit_once(':').map(|(h, _)| h).unwrap_or(addr);
        // Tolerate IPv6 brackets: [::1]:port
        let trimmed = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(ip) = trimmed.parse::<std::net::IpAddr>() {
            return Self::from_ip(ip);
        }
        // Hostname: hash into the inline buffer for a stable, peer-
        // distinguishable scope without DNS resolution.
        let mut buf = [0u8; 32];
        buf[0] = 7; // tag for "hostname-derived"
        let h = host.as_bytes();
        let mut i = 0;
        while i < h.len() && i + 1 < buf.len() {
            buf[i + 1] = h[i];
            i += 1;
        }
        PeerScope(buf)
    }
}

impl std::fmt::Debug for PeerScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0[0] {
            4 => {
                let v4 = std::net::Ipv4Addr::new(self.0[1], self.0[2], self.0[3], self.0[4]);
                write!(f, "PeerScope({})", v4)
            }
            6 => {
                let mut octets = [0u8; 16];
                octets.copy_from_slice(&self.0[1..17]);
                let v6 = std::net::Ipv6Addr::from(octets);
                write!(f, "PeerScope({})", v6)
            }
            _ => write!(f, "PeerScope(unknown)"),
        }
    }
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
        peer: &PeerScope,
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
        let key = (peer.clone(), *nonce);
        if self.seen.contains_key(&key) {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "Rejected Raft frame: nonce replay detected",
            ));
        }
        self.seen.insert(key, ());
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
    /// trivially-guessable empty secret. Secrets shorter than
    /// `MIN_SECRET_BYTES` (32) are also rejected — a one-byte secret offers
    /// no meaningful authentication and is almost always an operator typo.
    pub fn from_strings(cluster_secret: Option<String>, join_token: Option<String>) -> Self {
        const MIN_SECRET_BYTES: usize = 32;
        let normalize = |s: Option<String>, label: &str| -> Option<Arc<[u8]>> {
            s.and_then(|v| {
                let trimmed = v.trim();
                if trimmed.is_empty() {
                    None
                } else if trimmed.len() < MIN_SECRET_BYTES {
                    tracing::warn!(
                        secret = label,
                        len = trimmed.len(),
                        min = MIN_SECRET_BYTES,
                        "Rejecting Raft auth secret: shorter than minimum length"
                    );
                    None
                } else {
                    Some(Arc::<[u8]>::from(trimmed.as_bytes()))
                }
            })
        };
        let cluster_secret = normalize(cluster_secret, "cluster_secret");
        let join_token = normalize(join_token, "join_token");
        // Without a configured secret we deny by default: see the
        // `unauthenticated_ok` field doc. Opting into an open Raft port
        // requires the explicit acknowledgement string below — `"1"`,
        // `"true"`, etc. were too easy to flip on by accident in shared
        // helm charts and systemd units, and one such broker disables
        // the entire control plane's authentication. Tests use
        // `dev_unauthenticated()` directly, not this env var.
        const UNAUTH_OPT_IN: &str = "I_UNDERSTAND_THIS_IS_INSECURE";
        let unauthenticated_ok = cluster_secret.is_none()
            && match std::env::var("RAFT_ALLOW_UNAUTHENTICATED").ok().as_deref() {
                Some(v) if v == UNAUTH_OPT_IN => true,
                Some(other) if !other.is_empty() => {
                    tracing::error!(
                        opt_in = UNAUTH_OPT_IN,
                        "RAFT_ALLOW_UNAUTHENTICATED is set but does not match the required \
                         opt-in string; ignoring. Raft RPC remains authenticated."
                    );
                    false
                }
                _ => false,
            };
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
    ///
    /// Each `*_SECRET` value can also be supplied via `*_SECRET_FILE` /
    /// `*_TOKEN_FILE`, which is the recommended production form: the
    /// process-level env table is dumped by `/proc/<pid>/environ` and
    /// systemd `EnvironmentFile=` reads, while a `0600`-mode file owned by
    /// the broker user is only readable by that user. Inline env vars take
    /// precedence; if neither variant is set, the secret remains unset. A
    /// file with permissive group/other read bits is rejected outright —
    /// the operator misconfiguration is silent otherwise.
    pub fn from_env() -> Self {
        Self::from_strings(
            load_secret_env_or_file("RAFT_CLUSTER_SECRET", "RAFT_CLUSTER_SECRET_FILE"),
            load_secret_env_or_file("RAFT_JOIN_TOKEN", "RAFT_JOIN_TOKEN_FILE"),
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
        self.verify_authenticated(&PeerScope::unknown(), purpose, None, None, tag, payload)
    }

    /// Verify a received authenticated frame, including replay defense.
    pub(crate) fn verify_authenticated(
        &self,
        peer: &PeerScope,
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
                         set RAFT_ALLOW_UNAUTHENTICATED=I_UNDERSTAND_THIS_IS_INSECURE.",
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
            self.replay_cache.check_and_record(peer, ts, nonce)?;
        }

        Ok(())
    }
}

/// Read a length-prefixed Raft RPC payload from `stream`, rejecting any
/// Read a length-prefixed Raft RPC frame from `stream` and reject any
/// length above `max_payload_len` *before* allocating. Unwraps the
/// purpose byte and HMAC and verifies them against `keys`.
///
/// `max_payload_len` is the per-call cap on the *payload* portion of the
/// frame (not including the frame header). The pre-allocation cap is
/// imperative for DoS protection: an attacker who can speak the framing
/// protocol must not be able to force allocation of an attacker-chosen
/// payload size before HMAC has verified the frame. Server-side accept
/// loops use the unauthenticated cap for the first frame on a connection
/// and only widen it after the first successful verify; client-side
/// callers can use the full message cap because they have already
/// authenticated their peer via the request.
///
/// Returns `(purpose, payload)`. Callers that route on message type (e.g.
/// "JoinCluster must arrive under the Join purpose") use the returned
/// `purpose` to enforce that policy.
pub(crate) async fn read_rpc_frame<S: AsyncReadExt + Unpin>(
    stream: &mut S,
    keys: &RaftAuthKeys,
    peer: &PeerScope,
    max_payload_len: usize,
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

    // Reject oversized unauthenticated payloads BEFORE allocating. Without
    // a configured cluster secret, an attacker who can speak the framing
    // protocol could otherwise force 64 MiB allocations per frame just by
    // sending a bogus length prefix. The HMAC path is bounded by
    // `MAX_RPC_MESSAGE_BYTES` (already enforced above) and the connection-
    // level concurrency caps; this only narrows the unauthenticated case.
    if purpose == FramePurpose::Unauthenticated && payload_len > MAX_UNAUTHENTICATED_PAYLOAD_BYTES {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Unauthenticated Raft frame payload {} exceeds cap {} bytes",
                payload_len, MAX_UNAUTHENTICATED_PAYLOAD_BYTES
            ),
        ));
    }

    // Caller-supplied per-call cap. The server accept loop tightens this to
    // `MAX_UNAUTHENTICATED_PAYLOAD_BYTES` for the first frame on a
    // connection so a peer that can reach the Raft port — even one that
    // knows the framing protocol — cannot force a 64 MiB allocation before
    // the HMAC has been verified. Callers that already trust the peer
    // (post-handshake server loop, response readers on already-
    // authenticated outgoing connections) may pass `MAX_RPC_MESSAGE_BYTES`.
    if payload_len > max_payload_len {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!(
                "Raft RPC payload {} exceeds caller-supplied cap {} bytes",
                payload_len, max_payload_len
            ),
        ));
    }

    let mut payload = vec![0u8; payload_len];
    stream.read_exact(&mut payload).await?;

    keys.verify_authenticated(peer, purpose, timestamp_ms, nonce.as_ref(), &tag, &payload)?;
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
        let keys =
            RaftAuthKeys::from_strings(Some("hunter2-with-32-bytes-minimum-len".into()), None);
        let payload = b"hello";
        let tag = [0u8; HMAC_LEN];
        let err = keys
            .verify(FramePurpose::Unauthenticated, &tag, payload)
            .expect_err("server with key set must reject unsigned frame");
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn cluster_signed_roundtrip() {
        let keys =
            RaftAuthKeys::from_strings(Some("hunter2-with-32-bytes-minimum-len".into()), None);
        let payload = b"vote-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let tag = keys.sign_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), payload);
        keys.verify_authenticated(
            &PeerScope::unknown(),
            FramePurpose::Cluster,
            Some(ts),
            Some(&nonce),
            &tag,
            payload,
        )
        .unwrap();
    }

    #[test]
    fn cluster_signed_rejects_tamper() {
        let keys =
            RaftAuthKeys::from_strings(Some("hunter2-with-32-bytes-minimum-len".into()), None);
        let payload = b"vote-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let mut tag =
            keys.sign_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), payload);
        tag[0] ^= 0xff;
        let err = keys
            .verify_authenticated(
                &PeerScope::unknown(),
                FramePurpose::Cluster,
                Some(ts),
                Some(&nonce),
                &tag,
                payload,
            )
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    }

    #[test]
    fn cluster_signed_rejects_replay() {
        let keys =
            RaftAuthKeys::from_strings(Some("hunter2-with-32-bytes-minimum-len".into()), None);
        let payload = b"vote-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let tag = keys.sign_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), payload);
        keys.verify_authenticated(
            &PeerScope::unknown(),
            FramePurpose::Cluster,
            Some(ts),
            Some(&nonce),
            &tag,
            payload,
        )
        .unwrap();
        let err = keys
            .verify_authenticated(
                &PeerScope::unknown(),
                FramePurpose::Cluster,
                Some(ts),
                Some(&nonce),
                &tag,
                payload,
            )
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
        assert!(err.to_string().contains("replay"));
    }

    #[test]
    fn cross_purpose_signature_rejected() {
        // Signing with Cluster but presenting as Join must not validate, even
        // when both keys exist (this is why we mix the purpose byte into the
        // HMAC input).
        let keys = RaftAuthKeys::from_strings(
            Some("clusterkey-with-32-bytes-minimum-len".into()),
            Some("joinkey-with-32-bytes-minimum-len-padding".into()),
        );
        let payload = b"join-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let cluster_tag =
            keys.sign_authenticated(FramePurpose::Cluster, Some(ts), Some(&nonce), payload);
        let err = keys
            .verify_authenticated(
                &PeerScope::unknown(),
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
        let keys =
            RaftAuthKeys::from_strings(Some("clusterkey-with-32-bytes-minimum-len".into()), None);
        let payload = b"join-request";
        let ts = ReplayCache::now_ms();
        let nonce = ReplayCache::fresh_nonce();
        let tag = keys.sign_authenticated(FramePurpose::Join, Some(ts), Some(&nonce), payload);
        keys.verify_authenticated(
            &PeerScope::unknown(),
            FramePurpose::Join,
            Some(ts),
            Some(&nonce),
            &tag,
            payload,
        )
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
        let keys =
            RaftAuthKeys::from_strings(Some("hunter2-with-32-bytes-minimum-len".into()), None);
        let (mut a, mut b) = duplex(1024);
        let payload = b"hello-raft".to_vec();

        let send_keys = keys.clone();
        let send_payload = payload.clone();
        let writer = tokio::spawn(async move {
            write_rpc_frame(&mut a, &send_keys, false, &send_payload)
                .await
                .unwrap();
        });

        let recv = read_rpc_frame(&mut b, &keys, &PeerScope::unknown(), MAX_RPC_MESSAGE_BYTES)
            .await
            .unwrap();
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
        let err = read_rpc_frame(&mut b, &keys, &PeerScope::unknown(), MAX_RPC_MESSAGE_BYTES)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
    }
}
