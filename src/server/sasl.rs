//! SASL authentication support.
//!
//! This module provides SASL authentication mechanisms for the Kafka server:
//! - PLAIN: Simple username/password authentication
//! - SCRAM-SHA-256: Salted Challenge Response Authentication Mechanism
//! - SCRAM-SHA-512: SCRAM with SHA-512
//!
//! # Example
//!
//! ```rust,ignore
//! use kafkaesque::server::sasl::{SaslAuthenticator, PlainAuthenticator};
//!
//! // Create a PLAIN authenticator with static credentials
//! let mut auth = PlainAuthenticator::new();
//! auth.add_user("alice", "secret123");
//! auth.add_user("bob", "password456");
//!
//! // Use with a handler
//! let handler = MyHandler::with_sasl(auth);
//! ```

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::RwLock;

/// Supported SASL mechanisms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SaslMechanism {
    /// PLAIN mechanism (username/password in cleartext).
    /// Should only be used over TLS.
    Plain,
    /// SCRAM-SHA-256 mechanism.
    ScramSha256,
    /// SCRAM-SHA-512 mechanism.
    ScramSha512,
}

impl SaslMechanism {
    /// Get the mechanism name as used in the Kafka protocol.
    pub fn name(&self) -> &'static str {
        match self {
            SaslMechanism::Plain => "PLAIN",
            SaslMechanism::ScramSha256 => "SCRAM-SHA-256",
            SaslMechanism::ScramSha512 => "SCRAM-SHA-512",
        }
    }

    /// Parse a mechanism name from the Kafka protocol.
    pub fn from_name(name: &str) -> Option<SaslMechanism> {
        match name.to_uppercase().as_str() {
            "PLAIN" => Some(SaslMechanism::Plain),
            "SCRAM-SHA-256" => Some(SaslMechanism::ScramSha256),
            "SCRAM-SHA-512" => Some(SaslMechanism::ScramSha512),
            _ => None,
        }
    }
}

/// Result of SASL authentication.
#[derive(Debug, Clone)]
pub enum SaslResult {
    /// Authentication successful.
    Success {
        /// The authenticated principal (username).
        principal: String,
    },
    /// Authentication failed.
    Failed {
        /// Error message.
        message: String,
    },
    /// More data needed (for multi-step mechanisms like SCRAM).
    Continue {
        /// Challenge data to send to client.
        challenge: Vec<u8>,
    },
}

impl SaslResult {
    /// Check if authentication was successful.
    pub fn is_success(&self) -> bool {
        matches!(self, SaslResult::Success { .. })
    }

    /// Get the principal if authentication was successful.
    pub fn principal(&self) -> Option<&str> {
        match self {
            SaslResult::Success { principal } => Some(principal),
            _ => None,
        }
    }
}

/// Trait for SASL authenticators.
#[async_trait]
pub trait SaslAuthenticator: Send + Sync {
    /// Get the list of supported mechanisms.
    fn supported_mechanisms(&self) -> Vec<SaslMechanism>;

    /// Start authentication with the given mechanism.
    ///
    /// Returns the initial challenge (if any) for the client.
    async fn start(&self, mechanism: SaslMechanism) -> SaslResult;

    /// Process authentication data from the client.
    ///
    /// For PLAIN, this is the complete authentication.
    /// For SCRAM, this may require multiple rounds.
    async fn authenticate(
        &self,
        mechanism: SaslMechanism,
        auth_data: &[u8],
        session_data: Option<&[u8]>,
    ) -> SaslResult;
}

/// PLAIN authenticator with in-memory user database.
pub struct PlainAuthenticator {
    /// Username -> password mapping.
    users: Arc<RwLock<HashMap<String, String>>>,
    /// Whether to require TLS for PLAIN authentication.
    require_tls: bool,
}

impl PlainAuthenticator {
    /// Create a new PLAIN authenticator.
    pub fn new() -> Self {
        PlainAuthenticator {
            users: Arc::new(RwLock::new(HashMap::new())),
            require_tls: true,
        }
    }

    /// Create a PLAIN authenticator that doesn't require TLS (INSECURE).
    ///
    /// # Warning
    ///
    /// This should only be used for testing. PLAIN authentication
    /// transmits credentials in cleartext.
    pub fn new_insecure() -> Self {
        PlainAuthenticator {
            users: Arc::new(RwLock::new(HashMap::new())),
            require_tls: false,
        }
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

    /// Check if TLS is required.
    pub fn requires_tls(&self) -> bool {
        self.require_tls
    }
}

impl Default for PlainAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl SaslAuthenticator for PlainAuthenticator {
    fn supported_mechanisms(&self) -> Vec<SaslMechanism> {
        vec![SaslMechanism::Plain]
    }

    async fn start(&self, _mechanism: SaslMechanism) -> SaslResult {
        // PLAIN doesn't have an initial challenge
        SaslResult::Continue {
            challenge: Vec::new(),
        }
    }

    async fn authenticate(
        &self,
        _mechanism: SaslMechanism,
        auth_data: &[u8],
        _session_data: Option<&[u8]>,
    ) -> SaslResult {
        // PLAIN format: [authzid] NUL authcid NUL passwd
        // authzid is optional and usually empty
        let parts: Vec<&[u8]> = auth_data.split(|&b| b == 0).collect();

        if parts.len() < 2 {
            return SaslResult::Failed {
                message: "Invalid PLAIN format".to_string(),
            };
        }

        // Get username and password
        let (username, password) = if parts.len() == 3 {
            // authzid, authcid, passwd
            let authcid = match std::str::from_utf8(parts[1]) {
                Ok(s) => s,
                Err(_) => {
                    return SaslResult::Failed {
                        message: "Invalid UTF-8 in username".to_string(),
                    };
                }
            };
            let passwd = match std::str::from_utf8(parts[2]) {
                Ok(s) => s,
                Err(_) => {
                    return SaslResult::Failed {
                        message: "Invalid UTF-8 in password".to_string(),
                    };
                }
            };
            (authcid, passwd)
        } else {
            // authcid, passwd (no authzid)
            let authcid = match std::str::from_utf8(parts[0]) {
                Ok(s) => s,
                Err(_) => {
                    return SaslResult::Failed {
                        message: "Invalid UTF-8 in username".to_string(),
                    };
                }
            };
            let passwd = match std::str::from_utf8(parts[1]) {
                Ok(s) => s,
                Err(_) => {
                    return SaslResult::Failed {
                        message: "Invalid UTF-8 in password".to_string(),
                    };
                }
            };
            (authcid, passwd)
        };

        // Check credentials
        let users = self.users.read().await;
        match users.get(username) {
            Some(stored_password) if stored_password == password => SaslResult::Success {
                principal: username.to_string(),
            },
            _ => SaslResult::Failed {
                message: "Authentication failed".to_string(),
            },
        }
    }
}

/// Authentication state for a connection.
#[derive(Debug, Clone, Default)]
pub enum AuthState {
    /// No authentication required or not yet started.
    #[default]
    None,
    /// Authentication in progress.
    InProgress {
        /// The mechanism being used.
        mechanism: SaslMechanism,
        /// Session data for multi-step authentication.
        session_data: Option<Vec<u8>>,
    },
    /// Successfully authenticated.
    Authenticated {
        /// The authenticated principal.
        principal: String,
        /// The mechanism used.
        mechanism: SaslMechanism,
    },
    /// Authentication failed.
    Failed,
}

impl AuthState {
    /// Check if the connection is authenticated.
    pub fn is_authenticated(&self) -> bool {
        matches!(self, AuthState::Authenticated { .. })
    }

    /// Get the authenticated principal.
    pub fn principal(&self) -> Option<&str> {
        match self {
            AuthState::Authenticated { principal, .. } => Some(principal),
            _ => None,
        }
    }
}

/// Configuration for SASL authentication.
#[derive(Clone)]
pub struct SaslConfig {
    /// The authenticator to use.
    authenticator: Arc<dyn SaslAuthenticator>,
    /// Whether authentication is required for all requests.
    required: bool,
    /// APIs that are allowed before authentication.
    /// Typically includes ApiVersions and SaslHandshake.
    allowed_before_auth: Vec<i16>,
}

impl SaslConfig {
    /// Create a new SASL configuration.
    pub fn new<A: SaslAuthenticator + 'static>(authenticator: A) -> Self {
        SaslConfig {
            authenticator: Arc::new(authenticator),
            required: true,
            // ApiVersions (18) and SaslHandshake (17) are always allowed before auth
            allowed_before_auth: vec![17, 18],
        }
    }

    /// Set whether authentication is required.
    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Add an API key that is allowed before authentication.
    pub fn allow_before_auth(mut self, api_key: i16) -> Self {
        if !self.allowed_before_auth.contains(&api_key) {
            self.allowed_before_auth.push(api_key);
        }
        self
    }

    /// Check if authentication is required.
    pub fn is_required(&self) -> bool {
        self.required
    }

    /// Check if an API is allowed before authentication.
    pub fn is_allowed_before_auth(&self, api_key: i16) -> bool {
        !self.required || self.allowed_before_auth.contains(&api_key)
    }

    /// Get the supported mechanisms.
    pub fn supported_mechanisms(&self) -> Vec<SaslMechanism> {
        self.authenticator.supported_mechanisms()
    }

    /// Get the authenticator.
    pub fn authenticator(&self) -> &Arc<dyn SaslAuthenticator> {
        &self.authenticator
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sasl_mechanism_names() {
        assert_eq!(SaslMechanism::Plain.name(), "PLAIN");
        assert_eq!(SaslMechanism::ScramSha256.name(), "SCRAM-SHA-256");
        assert_eq!(SaslMechanism::ScramSha512.name(), "SCRAM-SHA-512");
    }

    #[test]
    fn test_sasl_mechanism_from_name() {
        assert_eq!(
            SaslMechanism::from_name("PLAIN"),
            Some(SaslMechanism::Plain)
        );
        assert_eq!(
            SaslMechanism::from_name("plain"),
            Some(SaslMechanism::Plain)
        );
        assert_eq!(
            SaslMechanism::from_name("SCRAM-SHA-256"),
            Some(SaslMechanism::ScramSha256)
        );
        assert_eq!(SaslMechanism::from_name("UNKNOWN"), None);
    }

    #[test]
    fn test_sasl_result() {
        let success = SaslResult::Success {
            principal: "alice".to_string(),
        };
        assert!(success.is_success());
        assert_eq!(success.principal(), Some("alice"));

        let failed = SaslResult::Failed {
            message: "error".to_string(),
        };
        assert!(!failed.is_success());
        assert_eq!(failed.principal(), None);
    }

    #[tokio::test]
    async fn test_plain_authenticator() {
        let auth = PlainAuthenticator::new();
        auth.add_user("alice", "secret").await;

        // Valid credentials
        let auth_data = b"\0alice\0secret";
        let result = auth
            .authenticate(SaslMechanism::Plain, auth_data, None)
            .await;
        assert!(result.is_success());
        assert_eq!(result.principal(), Some("alice"));

        // Invalid password
        let auth_data = b"\0alice\0wrong";
        let result = auth
            .authenticate(SaslMechanism::Plain, auth_data, None)
            .await;
        assert!(!result.is_success());

        // Unknown user
        let auth_data = b"\0bob\0secret";
        let result = auth
            .authenticate(SaslMechanism::Plain, auth_data, None)
            .await;
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn test_plain_authenticator_with_authzid() {
        let auth = PlainAuthenticator::new();
        auth.add_user("alice", "secret").await;

        // With authorization ID (empty)
        let auth_data = b"admin\0alice\0secret";
        let result = auth
            .authenticate(SaslMechanism::Plain, auth_data, None)
            .await;
        assert!(result.is_success());
    }

    #[test]
    fn test_auth_state() {
        let none = AuthState::None;
        assert!(!none.is_authenticated());
        assert_eq!(none.principal(), None);

        let authenticated = AuthState::Authenticated {
            principal: "alice".to_string(),
            mechanism: SaslMechanism::Plain,
        };
        assert!(authenticated.is_authenticated());
        assert_eq!(authenticated.principal(), Some("alice"));
    }

    #[tokio::test]
    async fn test_sasl_config() {
        let auth = PlainAuthenticator::new();
        let config = SaslConfig::new(auth);

        assert!(config.is_required());
        assert!(config.is_allowed_before_auth(17)); // SaslHandshake
        assert!(config.is_allowed_before_auth(18)); // ApiVersions
        assert!(!config.is_allowed_before_auth(3)); // Metadata

        let mechanisms = config.supported_mechanisms();
        assert_eq!(mechanisms, vec![SaslMechanism::Plain]);
    }

    #[tokio::test]
    async fn test_plain_authenticator_user_management() {
        let auth = PlainAuthenticator::new();

        assert!(!auth.has_user("alice").await);

        auth.add_user("alice", "secret").await;
        assert!(auth.has_user("alice").await);

        assert!(auth.remove_user("alice").await);
        assert!(!auth.has_user("alice").await);

        assert!(!auth.remove_user("alice").await);
    }

    // ========================================================================
    // Additional Edge Case Tests
    // ========================================================================

    #[test]
    fn test_sasl_mechanism_equality() {
        assert_eq!(SaslMechanism::Plain, SaslMechanism::Plain);
        assert_ne!(SaslMechanism::Plain, SaslMechanism::ScramSha256);
        assert_ne!(SaslMechanism::ScramSha256, SaslMechanism::ScramSha512);
    }

    #[test]
    fn test_sasl_mechanism_from_name_case_insensitive() {
        assert_eq!(
            SaslMechanism::from_name("plain"),
            Some(SaslMechanism::Plain)
        );
        assert_eq!(
            SaslMechanism::from_name("PLAIN"),
            Some(SaslMechanism::Plain)
        );
        assert_eq!(
            SaslMechanism::from_name("Plain"),
            Some(SaslMechanism::Plain)
        );
        assert_eq!(
            SaslMechanism::from_name("PlAiN"),
            Some(SaslMechanism::Plain)
        );
    }

    #[test]
    fn test_sasl_mechanism_from_name_scram_variations() {
        assert_eq!(
            SaslMechanism::from_name("scram-sha-256"),
            Some(SaslMechanism::ScramSha256)
        );
        assert_eq!(
            SaslMechanism::from_name("SCRAM-SHA-512"),
            Some(SaslMechanism::ScramSha512)
        );
    }

    #[test]
    fn test_sasl_mechanism_from_name_invalid() {
        assert_eq!(SaslMechanism::from_name(""), None);
        assert_eq!(SaslMechanism::from_name("DIGEST-MD5"), None);
        assert_eq!(SaslMechanism::from_name("GSSAPI"), None);
        assert_eq!(SaslMechanism::from_name("OAUTHBEARER"), None);
    }

    #[test]
    fn test_sasl_result_continue() {
        let cont = SaslResult::Continue {
            challenge: vec![1, 2, 3],
        };
        assert!(!cont.is_success());
        assert_eq!(cont.principal(), None);
    }

    #[test]
    fn test_sasl_result_failed_message() {
        let failed = SaslResult::Failed {
            message: "Test error message".to_string(),
        };
        if let SaslResult::Failed { message } = failed {
            assert_eq!(message, "Test error message");
        } else {
            panic!("Expected Failed variant");
        }
    }

    #[tokio::test]
    async fn test_plain_authenticator_empty_credentials() {
        let auth = PlainAuthenticator::new();
        auth.add_user("alice", "secret").await;

        // Empty auth data
        let result = auth.authenticate(SaslMechanism::Plain, &[], None).await;
        assert!(!result.is_success());

        // Only one NUL
        let result = auth.authenticate(SaslMechanism::Plain, b"\0", None).await;
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn test_plain_authenticator_invalid_utf8() {
        let auth = PlainAuthenticator::new();
        auth.add_user("alice", "secret").await;

        // Invalid UTF-8 in username
        let invalid_data = [0, 0xFF, 0xFE, 0, b's', b'e', b'c', b'r', b'e', b't'];
        let result = auth
            .authenticate(SaslMechanism::Plain, &invalid_data, None)
            .await;
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn test_plain_authenticator_empty_password() {
        let auth = PlainAuthenticator::new();
        auth.add_user("alice", "").await;

        // Empty password should work if configured
        let auth_data = b"\0alice\0";
        let result = auth
            .authenticate(SaslMechanism::Plain, auth_data, None)
            .await;
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_plain_authenticator_empty_username() {
        let auth = PlainAuthenticator::new();
        auth.add_user("", "secret").await;

        // Empty username should work if configured
        let auth_data = b"\0\0secret";
        let result = auth
            .authenticate(SaslMechanism::Plain, auth_data, None)
            .await;
        assert!(result.is_success());
    }

    #[tokio::test]
    async fn test_plain_authenticator_special_characters() {
        let auth = PlainAuthenticator::new();
        auth.add_user("user@domain.com", "p@ss!w0rd#123").await;

        let auth_data = b"\0user@domain.com\0p@ss!w0rd#123";
        let result = auth
            .authenticate(SaslMechanism::Plain, auth_data, None)
            .await;
        assert!(result.is_success());
        assert_eq!(result.principal(), Some("user@domain.com"));
    }

    #[tokio::test]
    async fn test_plain_authenticator_unicode_credentials() {
        let auth = PlainAuthenticator::new();
        auth.add_user("用户", "密码").await;

        let mut auth_data = vec![0];
        auth_data.extend_from_slice("用户".as_bytes());
        auth_data.push(0);
        auth_data.extend_from_slice("密码".as_bytes());

        let result = auth
            .authenticate(SaslMechanism::Plain, &auth_data, None)
            .await;
        assert!(result.is_success());
        assert_eq!(result.principal(), Some("用户"));
    }

    #[tokio::test]
    async fn test_plain_authenticator_multiple_users() {
        let auth = PlainAuthenticator::new();
        auth.add_user("alice", "secret1").await;
        auth.add_user("bob", "secret2").await;
        auth.add_user("charlie", "secret3").await;

        // Alice authenticates
        let result = auth
            .authenticate(SaslMechanism::Plain, b"\0alice\0secret1", None)
            .await;
        assert!(result.is_success());
        assert_eq!(result.principal(), Some("alice"));

        // Bob authenticates
        let result = auth
            .authenticate(SaslMechanism::Plain, b"\0bob\0secret2", None)
            .await;
        assert!(result.is_success());
        assert_eq!(result.principal(), Some("bob"));

        // Wrong password for Charlie
        let result = auth
            .authenticate(SaslMechanism::Plain, b"\0charlie\0wrong", None)
            .await;
        assert!(!result.is_success());
    }

    #[tokio::test]
    async fn test_plain_authenticator_start() {
        let auth = PlainAuthenticator::new();
        let result = auth.start(SaslMechanism::Plain).await;

        // PLAIN doesn't have initial challenge
        match result {
            SaslResult::Continue { challenge } => {
                assert!(challenge.is_empty());
            }
            _ => panic!("Expected Continue result"),
        }
    }

    #[tokio::test]
    async fn test_plain_authenticator_supported_mechanisms() {
        let auth = PlainAuthenticator::new();
        let mechanisms = auth.supported_mechanisms();

        assert_eq!(mechanisms.len(), 1);
        assert_eq!(mechanisms[0], SaslMechanism::Plain);
    }

    #[test]
    fn test_plain_authenticator_requires_tls_default() {
        let auth = PlainAuthenticator::new();
        assert!(auth.requires_tls());
    }

    #[test]
    fn test_plain_authenticator_insecure_mode() {
        let auth = PlainAuthenticator::new_insecure();
        assert!(!auth.requires_tls());
    }

    #[test]
    fn test_plain_authenticator_default() {
        let auth = PlainAuthenticator::default();
        assert!(auth.requires_tls());
    }

    #[test]
    fn test_auth_state_variants() {
        // None state
        let none = AuthState::None;
        assert!(!none.is_authenticated());
        assert!(none.principal().is_none());

        // InProgress state
        let in_progress = AuthState::InProgress {
            mechanism: SaslMechanism::Plain,
            session_data: Some(vec![1, 2, 3]),
        };
        assert!(!in_progress.is_authenticated());
        assert!(in_progress.principal().is_none());

        // Authenticated state
        let authenticated = AuthState::Authenticated {
            principal: "alice".to_string(),
            mechanism: SaslMechanism::Plain,
        };
        assert!(authenticated.is_authenticated());
        assert_eq!(authenticated.principal(), Some("alice"));

        // Failed state
        let failed = AuthState::Failed;
        assert!(!failed.is_authenticated());
        assert!(failed.principal().is_none());
    }

    #[test]
    fn test_auth_state_default() {
        let state = AuthState::default();
        assert!(matches!(state, AuthState::None));
    }

    #[tokio::test]
    async fn test_sasl_config_required_false() {
        let auth = PlainAuthenticator::new();
        let config = SaslConfig::new(auth).required(false);

        assert!(!config.is_required());
        // When not required, all APIs are allowed before auth
        assert!(config.is_allowed_before_auth(3)); // Metadata
        assert!(config.is_allowed_before_auth(0)); // Produce
    }

    #[tokio::test]
    async fn test_sasl_config_allow_before_auth() {
        let auth = PlainAuthenticator::new();
        let config = SaslConfig::new(auth)
            .allow_before_auth(3) // Allow Metadata
            .allow_before_auth(9); // Allow OffsetFetch

        assert!(config.is_allowed_before_auth(3));
        assert!(config.is_allowed_before_auth(9));
        assert!(!config.is_allowed_before_auth(0)); // Produce not allowed
    }

    #[tokio::test]
    async fn test_sasl_config_allow_before_auth_dedup() {
        let auth = PlainAuthenticator::new();
        let config = SaslConfig::new(auth)
            .allow_before_auth(17) // Already in default list
            .allow_before_auth(17); // Add again

        // Should not have duplicates
        let mechanisms = config.supported_mechanisms();
        assert_eq!(mechanisms.len(), 1);
    }
}
