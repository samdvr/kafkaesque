//! SASL authentication provider for the Kafka protocol.
//!
//! This module provides SASL authentication with support for:
//!
//! # Supported Mechanisms
//!
//! - **PLAIN**: Simple username/password authentication (should only be used over TLS)
//! - **SCRAM-SHA-256**: Salted Challenge-Response Authentication Mechanism per RFC 5802.
//!   Never sends the password (or anything from which it can be recovered)
//!   on the wire — safe to use over plaintext transports for credential
//!   secrecy, though TLS is still required for confidentiality of the
//!   payload data. SCRAM is the production default; PLAIN is left for
//!   backwards-compatibility with simple operator tooling.
//!
//! # Configuration
//!
//! Users can be configured via:
//! - `SASL_USERS` environment variable: `user1:pass1,user2:pass2`
//! - `SASL_USERS_FILE`: File with one `user:password` per line
//!
//! # Example
//!
//! ```rust,ignore
//! use kafkaesque::cluster::sasl_provider::SaslProvider;
//!
//! let provider = SaslProvider::new(false);
//! provider.add_user("alice", "secret123").await;
//!
//! // Handle SASL handshake
//! let mechanisms = provider.supported_mechanisms();
//!
//! // Handle SASL authenticate (PLAIN — single round-trip)
//! let result = provider.authenticate("PLAIN", auth_bytes, "session-key").await;
//! ```

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::config::ClusterConfig;
use super::scram::{ScramCredentials, ScramServerState, handle_client_final, handle_client_first};

/// Per-user record. Stores both the cleartext password (for PLAIN) and
/// SCRAM-derived credentials (for SCRAM-SHA-256). PLAIN is kept for the
/// timing-safety story; SCRAM never recovers the cleartext.
struct UserRecord {
    password: String,
    scram_creds: ScramCredentials,
}

/// SASL authentication provider for the Kafka protocol.
#[allow(dead_code)]
pub struct SaslProvider {
    /// User credentials (username -> record).
    users: Arc<RwLock<HashMap<String, UserRecord>>>,
    /// In-flight SCRAM sessions keyed by the client's `SocketAddr`. Each
    /// SCRAM exchange is two round-trips, so we hold per-connection state
    /// between them. Cleared on success/failure to bound memory; abandoned
    /// connections fall off when the connection drops (the Kafka handler
    /// has no hook here, but the map is bounded by max client connections
    /// elsewhere).
    scram_sessions: Arc<RwLock<HashMap<SocketAddr, ScramServerState>>>,
    /// Supported SASL mechanisms (advertised in SaslHandshake).
    mechanisms: Vec<String>,
    /// Whether authentication is required.
    required: bool,
    /// Reject PLAIN on non-TLS transports (credentials are cleartext on the wire).
    plain_require_tls: bool,
}

#[allow(dead_code)]
impl SaslProvider {
    /// Create a new SASL provider with no users.
    pub fn new(required: bool) -> Self {
        Self::with_options(required, true)
    }

    /// Create a SASL provider with explicit TLS and requirement settings.
    pub fn with_options(required: bool, plain_require_tls: bool) -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            scram_sessions: Arc::new(RwLock::new(HashMap::new())),
            mechanisms: vec!["SCRAM-SHA-256".to_string(), "PLAIN".to_string()],
            required,
            plain_require_tls,
        }
    }

    /// Create a SASL provider from ClusterConfig.
    ///
    /// Loads users from:
    /// 1. `SASL_USERS` environment variable (format: "user1:pass1,user2:pass2")
    /// 2. `SASL_USERS_FILE` (format: one "user:pass" per line, # for comments)
    pub async fn from_config(config: &ClusterConfig) -> Option<Self> {
        if !config.sasl_enabled {
            return None;
        }

        let provider = Self::with_options(config.sasl_required, config.sasl_plain_require_tls);
        let mut user_count = 0;

        // Load from environment variable first
        if let Ok(users_env) = std::env::var("SASL_USERS") {
            for pair in users_env.split(',') {
                let parts: Vec<&str> = pair.splitn(2, ':').collect();
                if parts.len() == 2 {
                    let username = parts[0].trim();
                    let password = parts[1].trim();
                    if !username.is_empty() && !password.is_empty() {
                        provider.add_user(username, password).await;
                        user_count += 1;
                    }
                }
            }
        }

        // Load from file (overrides env vars for same users)
        if let Some(ref file_path) = config.sasl_users_file {
            match tokio::fs::read_to_string(file_path).await {
                Ok(contents) => {
                    for line in contents.lines() {
                        let line = line.trim();
                        // Skip empty lines and comments
                        if line.is_empty() || line.starts_with('#') {
                            continue;
                        }
                        let parts: Vec<&str> = line.splitn(2, ':').collect();
                        if parts.len() == 2 {
                            let username = parts[0].trim();
                            let password = parts[1].trim();
                            if !username.is_empty() && !password.is_empty() {
                                provider.add_user(username, password).await;
                                user_count += 1;
                            }
                        }
                    }
                }
                Err(e) => {
                    warn!(
                        file = %file_path,
                        error = %e,
                        "Failed to read SASL users file"
                    );
                }
            }
        }

        if user_count == 0 {
            warn!(
                "SASL enabled but no users configured. Set SASL_USERS env var or SASL_USERS_FILE."
            );
        } else {
            info!(
                user_count,
                required = config.sasl_required,
                mechanisms = ?provider.mechanisms,
                "SASL authentication configured"
            );
        }

        Some(provider)
    }

    /// Add a user with the given password.
    ///
    /// Derives SCRAM-SHA-256 credentials at insert time and stores them
    /// alongside the cleartext password (the latter is needed for PLAIN).
    /// PBKDF2 is intentionally slow (~ms per call at 4096 iterations) — we
    /// only pay the cost once per `add_user`, not per authenticate.
    pub async fn add_user(&self, username: &str, password: &str) {
        let scram_creds = ScramCredentials::generate(password.as_bytes());
        let mut users = self.users.write().await;
        users.insert(
            username.to_string(),
            UserRecord {
                password: password.to_string(),
                scram_creds,
            },
        );
    }

    /// Remove a user.
    pub async fn remove_user(&self, username: &str) -> bool {
        let mut users = self.users.write().await;
        users.remove(username).is_some()
    }

    /// Check if a user exists.
    pub async fn has_user(&self, username: &str) -> bool {
        let users = self.users.read().await;
        users.contains_key(username)
    }

    /// Get supported SASL mechanisms.
    pub fn supported_mechanisms(&self) -> &[String] {
        &self.mechanisms
    }

    /// Check if authentication is required.
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// Check if a mechanism is supported.
    pub fn is_mechanism_supported(&self, mechanism: &str) -> bool {
        let mechanism_upper = mechanism.to_uppercase();
        self.mechanisms
            .iter()
            .any(|m| m.to_uppercase() == mechanism_upper)
    }

    /// Returns true when a SCRAM session for `client_addr` is awaiting its
    /// `client-final-message`. Used by the cluster handler's mechanism-
    /// sniffing logic so SCRAM `client-final` (which doesn't start with
    /// the GS2 header) is correctly routed to the SCRAM path even though
    /// the bytes themselves don't say "I am SCRAM".
    pub async fn has_scram_session(&self, client_addr: SocketAddr) -> bool {
        self.scram_sessions.read().await.contains_key(&client_addr)
    }

    /// Drop per-connection SCRAM state when a client disconnects.
    pub async fn clear_session(&self, client_addr: SocketAddr) {
        if self
            .scram_sessions
            .write()
            .await
            .remove(&client_addr)
            .is_some()
        {
            debug!(client = %client_addr, "Cleared abandoned SCRAM session");
        }
    }

    /// Authenticate using PLAIN mechanism.
    ///
    /// PLAIN format: [authzid] NUL authcid NUL passwd
    /// Returns (success, principal) tuple.
    pub async fn authenticate_plain(&self, auth_data: &[u8]) -> (bool, Option<String>) {
        let parts: Vec<&[u8]> = auth_data.split(|&b| b == 0).collect();

        if parts.len() < 2 {
            debug!("PLAIN auth failed: invalid format (not enough parts)");
            return (false, None);
        }

        // Get username and password from PLAIN format
        let (username, password) = if parts.len() >= 3 {
            // authzid, authcid, passwd format
            match (std::str::from_utf8(parts[1]), std::str::from_utf8(parts[2])) {
                (Ok(u), Ok(p)) => (u, p),
                _ => {
                    debug!("PLAIN auth failed: invalid UTF-8");
                    return (false, None);
                }
            }
        } else {
            // authcid, passwd format (no authzid)
            match (std::str::from_utf8(parts[0]), std::str::from_utf8(parts[1])) {
                (Ok(u), Ok(p)) => (u, p),
                _ => {
                    debug!("PLAIN auth failed: invalid UTF-8");
                    return (false, None);
                }
            }
        };

        let users = self.users.read().await;
        // Look up the user but always run the constant-time comparison —
        // including against a dummy password when the user is absent — so
        // unknown-user vs wrong-password timings can't be distinguished.
        // Don't leak user enumeration via timing or via distinct
        // log lines either.
        let stored = users.get(username);
        let candidate = stored.map(|r| r.password.as_str()).unwrap_or("");
        use subtle::ConstantTimeEq;
        let eq: bool = candidate.as_bytes().ct_eq(password.as_bytes()).into();
        if stored.is_some() && eq {
            info!(username = %username, "SASL PLAIN authentication successful");
            (true, Some(username.to_string()))
        } else {
            // Single, uniform log line for *any* authentication failure so
            // operators can't grep `wrong password` to enumerate accounts.
            warn!(username = %username, "SASL PLAIN authentication failed");
            (false, None)
        }
    }

    /// SCRAM-SHA-256 step. Each Kafka SaslAuthenticate request carries one
    /// SCRAM message; the response carries the next. Per-connection state
    /// is keyed by the client's `SocketAddr` and held in
    /// `self.scram_sessions`.
    ///
    /// Returns `(success, principal, server_response_bytes)`. Success is
    /// `true` only on the final round-trip; the first call returns
    /// `success=false, principal=None` because the protocol isn't done
    /// yet — but the response carries the server-first-message and the
    /// dispatcher writes it back to the client. The handler treats the
    /// intermediate non-error response as success only for connection-gate
    /// purposes once it sees `success=true` here.
    pub async fn authenticate_scram_sha256(
        &self,
        client_addr: SocketAddr,
        auth_data: &[u8],
    ) -> (bool, Option<String>, Vec<u8>) {
        // Look up existing session; if none, this is the client-first step.
        let existing = {
            let mut sessions = self.scram_sessions.write().await;
            sessions.remove(&client_addr)
        };
        match existing {
            None => {
                // Client-first: parse, look up creds, generate server-first.
                let users = self.users.read().await;
                let creds_lookup = |u: &str| users.get(u).map(|r| r.scram_creds.clone());
                let (state, response) = handle_client_first(auth_data, creds_lookup);
                drop(users);
                // Stash the new state if we're still in-progress; otherwise
                // just return the failure response.
                match &state {
                    ScramServerState::AwaitingClientFinal { .. } => {
                        self.scram_sessions.write().await.insert(client_addr, state);
                        debug!(client = %client_addr, "SCRAM client-first accepted");
                        // Not authenticated yet — handler still needs to
                        // send the response. We mark failure here so the
                        // gate doesn't flip; the dispatcher does flip it
                        // only on the *final* successful step.
                        (false, None, response)
                    }
                    _ => {
                        warn!(
                            client = %client_addr,
                            "SCRAM client-first rejected"
                        );
                        (false, None, response)
                    }
                }
            }
            Some(state) => {
                // Client-final: validate proof, return server-final or
                // error. State is consumed regardless of outcome.
                let (state, response) = handle_client_final(auth_data, state);
                match state {
                    ScramServerState::Authenticated(principal) => {
                        info!(
                            client = %client_addr,
                            principal = %principal,
                            "SCRAM-SHA-256 authentication successful"
                        );
                        (true, Some(principal), response)
                    }
                    ScramServerState::Failed(reason) => {
                        warn!(
                            client = %client_addr,
                            reason = %reason,
                            "SCRAM-SHA-256 authentication failed"
                        );
                        (false, None, response)
                    }
                    _ => {
                        // Should not happen — handle_client_final only
                        // returns Authenticated or Failed.
                        (false, None, response)
                    }
                }
            }
        }
    }

    /// Authenticate a SASL request.
    ///
    /// `session_id` identifies the client connection for multi-step
    /// mechanisms (SCRAM). For PLAIN it's ignored.
    ///
    /// Returns `(success, principal, response_bytes)`. The third element
    /// is the SASL payload to ship back to the client (intermediate
    /// challenges for SCRAM, empty for PLAIN).
    pub async fn authenticate_with_session(
        &self,
        client_addr: SocketAddr,
        mechanism: &str,
        auth_data: &[u8],
        transport_tls: bool,
    ) -> (bool, Option<String>, Vec<u8>) {
        let mechanism_upper = mechanism.to_uppercase();
        match mechanism_upper.as_str() {
            "PLAIN" => {
                if self.plain_require_tls && !transport_tls {
                    warn!(
                        client = %client_addr,
                        "Rejecting PLAIN SASL on non-TLS connection"
                    );
                    return (false, None, Vec::new());
                }
                let (success, principal) = self.authenticate_plain(auth_data).await;
                (success, principal, Vec::new())
            }
            "SCRAM-SHA-256" => self.authenticate_scram_sha256(client_addr, auth_data).await,
            _ => {
                warn!(mechanism = %mechanism, "Unsupported SASL mechanism");
                (false, None, Vec::new())
            }
        }
    }

    /// Authenticate a SASL request (single-step legacy API).
    ///
    /// Kept for backwards-compatibility with PLAIN-only callers and for
    /// existing tests. New callers should use [`Self::authenticate_with_session`].
    ///
    /// Returns (success, principal) tuple.
    pub async fn authenticate(&self, mechanism: &str, auth_data: &[u8]) -> (bool, Option<String>) {
        let mechanism_upper = mechanism.to_uppercase();

        match mechanism_upper.as_str() {
            "PLAIN" => self.authenticate_plain(auth_data).await,
            "SCRAM-SHA-256" => {
                warn!("SCRAM requires multi-step authentication; use authenticate_with_session");
                (false, None)
            }
            _ => {
                warn!(mechanism = %mechanism, "Unsupported SASL mechanism");
                (false, None)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sasl_provider_add_user() {
        let provider = SaslProvider::new(false);

        assert!(!provider.has_user("alice").await);

        provider.add_user("alice", "secret").await;
        assert!(provider.has_user("alice").await);

        assert!(provider.remove_user("alice").await);
        assert!(!provider.has_user("alice").await);
    }

    #[tokio::test]
    async fn test_plain_rejected_without_tls() {
        let provider = SaslProvider::new(false);
        provider.add_user("alice", "secret").await;
        let addr: SocketAddr = "127.0.0.1:49999".parse().unwrap();

        let (ok, principal, _) = provider
            .authenticate_with_session(addr, "PLAIN", b"\0alice\0secret", false)
            .await;
        assert!(!ok);
        assert!(principal.is_none());

        let (ok, principal, _) = provider
            .authenticate_with_session(addr, "PLAIN", b"\0alice\0secret", true)
            .await;
        assert!(ok);
        assert_eq!(principal.as_deref(), Some("alice"));
    }

    #[tokio::test]
    async fn test_plain_allowed_without_tls_when_configured() {
        let provider = SaslProvider::with_options(false, false);
        provider.add_user("alice", "secret").await;
        let addr: SocketAddr = "127.0.0.1:49997".parse().unwrap();

        let (ok, principal, _) = provider
            .authenticate_with_session(addr, "PLAIN", b"\0alice\0secret", false)
            .await;
        assert!(ok);
        assert_eq!(principal.as_deref(), Some("alice"));
    }

    #[tokio::test]
    async fn test_clear_session_drops_scram_state() {
        let provider = SaslProvider::new(false);
        provider.add_user("alice", "hunter2").await;
        let addr: SocketAddr = "127.0.0.1:49998".parse().unwrap();
        let client_first = b"n,,n=alice,r=AAAAAAAAAAAAAAAAAAAA";
        let _ = provider
            .authenticate_with_session(addr, "SCRAM-SHA-256", client_first, false)
            .await;
        assert!(provider.has_scram_session(addr).await);
        provider.clear_session(addr).await;
        assert!(!provider.has_scram_session(addr).await);
    }

    #[tokio::test]
    async fn test_sasl_plain_auth() {
        let provider = SaslProvider::new(false);
        provider.add_user("alice", "secret").await;

        // Valid credentials (authzid, authcid, passwd format)
        let auth_data = b"\0alice\0secret";
        let (success, principal) = provider.authenticate_plain(auth_data).await;
        assert!(success);
        assert_eq!(principal, Some("alice".to_string()));

        // Invalid password
        let auth_data = b"\0alice\0wrong";
        let (success, _) = provider.authenticate_plain(auth_data).await;
        assert!(!success);

        // Unknown user
        let auth_data = b"\0bob\0secret";
        let (success, _) = provider.authenticate_plain(auth_data).await;
        assert!(!success);
    }

    #[tokio::test]
    async fn test_sasl_plain_auth_with_authzid() {
        let provider = SaslProvider::new(false);
        provider.add_user("alice", "secret").await;

        // With authorization ID (3 parts)
        let auth_data = b"admin\0alice\0secret";
        let (success, principal) = provider.authenticate_plain(auth_data).await;
        assert!(success);
        assert_eq!(principal, Some("alice".to_string()));
    }

    #[test]
    fn test_mechanism_support() {
        let provider = SaslProvider::new(false);

        assert!(provider.is_mechanism_supported("PLAIN"));
        assert!(provider.is_mechanism_supported("plain"));
        assert!(provider.is_mechanism_supported("SCRAM-SHA-256"));
        assert!(provider.is_mechanism_supported("scram-sha-256"));
        assert!(!provider.is_mechanism_supported("UNKNOWN"));
    }

    #[tokio::test]
    async fn test_authenticate_dispatcher() {
        let provider = SaslProvider::new(false);
        provider.add_user("alice", "secret").await;

        // PLAIN should work
        let (success, _) = provider.authenticate("PLAIN", b"\0alice\0secret").await;
        assert!(success);

        // Single-step authenticate API rejects SCRAM (multi-step) — operators
        // must use authenticate_with_session.
        let (success, _) = provider.authenticate("SCRAM-SHA-256", b"").await;
        assert!(!success);

        // Unknown mechanism
        let (success, _) = provider.authenticate("UNKNOWN", b"").await;
        assert!(!success);
    }

    /// End-to-end SCRAM-SHA-256 handshake against a SaslProvider — proves
    /// the multi-step session map plumbing in `authenticate_with_session`
    /// works for the happy path.
    #[tokio::test]
    async fn test_authenticate_scram_sha256_roundtrip() {
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD as B64;

        let provider = SaslProvider::new(false);
        provider.add_user("alice", "hunter2").await;

        let addr: SocketAddr = "127.0.0.1:50000".parse().unwrap();
        let user = "alice";
        let client_nonce = "fyko+d2lbbFgONRv9qkxdawL";
        let client_first_bare = format!("n={},r={}", user, client_nonce);
        let client_first = format!("n,,{}", client_first_bare);

        // Step 1: client-first → server-first.
        let (ok, principal, server_first_bytes) = provider
            .authenticate_with_session(addr, "SCRAM-SHA-256", client_first.as_bytes(), false)
            .await;
        assert!(!ok, "intermediate step should not be 'ok' yet");
        assert!(principal.is_none());
        let server_first = std::str::from_utf8(&server_first_bytes)
            .unwrap()
            .to_string();
        assert!(
            server_first.starts_with("r="),
            "server-first must carry combined nonce"
        );
        assert!(provider.has_scram_session(addr).await);

        // Reconstruct what the client would compute.
        // Pull salt/iterations out of server-first.
        let mut combined_nonce = String::new();
        let mut salt_b64 = String::new();
        let mut iterations: u32 = 0;
        for attr in server_first.split(',') {
            if let Some(v) = attr.strip_prefix("r=") {
                combined_nonce = v.to_string();
            } else if let Some(v) = attr.strip_prefix("s=") {
                salt_b64 = v.to_string();
            } else if let Some(v) = attr.strip_prefix("i=") {
                iterations = v.parse().unwrap();
            }
        }
        let salt = B64.decode(&salt_b64).unwrap();

        // Compute proof using the same primitives as the server.
        use crate::cluster::scram::ScramCredentials;
        let creds = ScramCredentials::derive(b"hunter2", &salt, iterations);
        let cbind_b64 = B64.encode(b"n,,");
        let client_final_without_proof = format!("c={},r={}", cbind_b64, combined_nonce);
        let auth_message = format!(
            "{},{},{}",
            client_first_bare, server_first, client_final_without_proof
        );
        // client_signature = HMAC(stored_key, auth_message)
        // client_proof = client_key XOR client_signature
        // We need client_key = HMAC(salted_password, "Client Key").
        // Reuse the test helpers indirectly by re-deriving:
        use hmac::{Hmac, Mac};
        use sha2::{Digest, Sha256};
        type H = Hmac<Sha256>;
        let salted = {
            // Replicate PBKDF2 from scram.rs minimally.
            let mut salt_with_idx = Vec::with_capacity(salt.len() + 4);
            salt_with_idx.extend_from_slice(&salt);
            salt_with_idx.extend_from_slice(&1u32.to_be_bytes());
            let mut mac = H::new_from_slice(b"hunter2").unwrap();
            mac.update(&salt_with_idx);
            let mut u: [u8; 32] = mac.finalize().into_bytes().into();
            let mut out = u;
            for _ in 1..iterations {
                let mut mac = H::new_from_slice(b"hunter2").unwrap();
                mac.update(&u);
                u = mac.finalize().into_bytes().into();
                for i in 0..32 {
                    out[i] ^= u[i];
                }
            }
            out
        };
        let mut mac = H::new_from_slice(&salted).unwrap();
        mac.update(b"Client Key");
        let client_key: [u8; 32] = mac.finalize().into_bytes().into();
        let _ = creds; // ensure same iter count was used; not directly compared here
        let mut mac = H::new_from_slice(&Sha256::digest(client_key)[..]).unwrap();
        mac.update(auth_message.as_bytes());
        let client_signature: [u8; 32] = mac.finalize().into_bytes().into();
        let mut client_proof = [0u8; 32];
        for i in 0..32 {
            client_proof[i] = client_key[i] ^ client_signature[i];
        }
        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            B64.encode(client_proof)
        );

        // Step 2: client-final → server-final.
        let (ok, principal, server_final_bytes) = provider
            .authenticate_with_session(addr, "SCRAM-SHA-256", client_final.as_bytes(), false)
            .await;
        assert!(ok, "client-final with valid proof should succeed");
        assert_eq!(principal.as_deref(), Some("User:alice"));
        let server_final = std::str::from_utf8(&server_final_bytes).unwrap();
        assert!(
            server_final.starts_with("v="),
            "server-final must carry verifier"
        );

        // Session was consumed.
        assert!(!provider.has_scram_session(addr).await);
    }

    #[tokio::test]
    async fn test_authenticate_scram_sha256_wrong_password() {
        let provider = SaslProvider::new(false);
        provider.add_user("alice", "rightpw").await;

        let addr: SocketAddr = "127.0.0.1:50001".parse().unwrap();
        let client_first = b"n,,n=alice,r=AAAAAAAAAAAAAAAAAAAA";
        let (ok, _, _) = provider
            .authenticate_with_session(addr, "SCRAM-SHA-256", client_first, false)
            .await;
        assert!(!ok);

        // Wrong proof — must fail without distinguishing from unknown user.
        let bogus_final =
            b"c=biws,r=AAAAAAAAAAAAAAAAAAAAfake-server-nonce,p=AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
        let (ok, principal, _) = provider
            .authenticate_with_session(addr, "SCRAM-SHA-256", bogus_final, false)
            .await;
        assert!(!ok);
        assert!(principal.is_none());
    }
}
