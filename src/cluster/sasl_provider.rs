//! SASL authentication provider for the Kafka protocol.
//!
//! This module provides SASL authentication with support for:
//!
//! # Supported Mechanisms
//!
//! - **PLAIN**: Simple username/password authentication (should only be used over TLS)
//!
//! SCRAM-SHA-256/512 support is advertised but not yet implemented (requires
//! per-connection session state for the challenge-response protocol).
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
//! // Handle SASL authenticate
//! let result = provider.authenticate("PLAIN", auth_bytes).await;
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

use super::config::ClusterConfig;

/// SASL authentication provider for the Kafka protocol.
#[allow(dead_code)]
pub struct SaslProvider {
    /// User credentials (username -> password).
    users: Arc<RwLock<HashMap<String, String>>>,
    /// Supported SASL mechanisms.
    mechanisms: Vec<String>,
    /// Whether authentication is required.
    required: bool,
}

#[allow(dead_code)]
impl SaslProvider {
    /// Create a new SASL provider with no users.
    pub fn new(required: bool) -> Self {
        Self {
            users: Arc::new(RwLock::new(HashMap::new())),
            mechanisms: vec![
                "PLAIN".to_string(),
                "SCRAM-SHA-256".to_string(),
                "SCRAM-SHA-512".to_string(),
            ],
            required,
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

        let provider = Self::new(config.sasl_required);
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
    pub async fn add_user(&self, username: &str, password: &str) {
        let mut users = self.users.write().await;
        users.insert(username.to_string(), password.to_string());
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
        match users.get(username) {
            Some(stored_password) if stored_password == password => {
                info!(username = %username, "SASL PLAIN authentication successful");
                (true, Some(username.to_string()))
            }
            Some(_) => {
                warn!(username = %username, "SASL PLAIN authentication failed: wrong password");
                (false, None)
            }
            None => {
                warn!(username = %username, "SASL PLAIN authentication failed: unknown user");
                (false, None)
            }
        }
    }

    /// Authenticate a SASL request.
    ///
    /// Currently supports PLAIN mechanism. SCRAM mechanisms require session state
    /// and will be implemented when per-connection state tracking is added.
    ///
    /// Returns (success, principal) tuple.
    pub async fn authenticate(&self, mechanism: &str, auth_data: &[u8]) -> (bool, Option<String>) {
        let mechanism_upper = mechanism.to_uppercase();

        match mechanism_upper.as_str() {
            "PLAIN" => self.authenticate_plain(auth_data).await,
            "SCRAM-SHA-256" | "SCRAM-SHA-512" => {
                // SCRAM requires multi-step authentication with session state.
                // For now, we only support PLAIN. SCRAM will be added when
                // per-connection auth state tracking is implemented.
                warn!(
                    mechanism = %mechanism,
                    "SCRAM authentication not yet implemented - use PLAIN over TLS"
                );
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

        // SCRAM not implemented yet
        let (success, _) = provider.authenticate("SCRAM-SHA-256", b"").await;
        assert!(!success);

        // Unknown mechanism
        let (success, _) = provider.authenticate("UNKNOWN", b"").await;
        assert!(!success);
    }
}
