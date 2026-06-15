//! Optional TLS / mTLS for the Raft RPC port.
//!
//! HMAC framing (`super::auth`) handles peer authentication today, but mTLS
//! adds per-broker identities and lets operators rotate without restarting
//! the cluster. The two layers are complementary — every Raft frame stays
//! HMAC-signed even when wrapped in TLS — so a leaked shared secret no
//! longer grants the entire control plane.
//!
//! ## Wire layering
//!
//! ```text
//!   ┌────────── TCP ──────────┐
//!   │  TLS (rustls, mTLS)     │   ← peer identity / encryption
//!   │  ┌─ HMAC frame ─┐       │   ← message integrity / replay defense
//!   │  │  bincode payload  │  │
//!   └──┴────────────────┴─────┘
//! ```
//!
//! ## Configuration
//!
//! Set on every broker:
//!
//! - `RAFT_TLS_CERT`: path to PEM-encoded certificate chain for this broker
//! - `RAFT_TLS_KEY`: path to PEM-encoded private key
//! - `RAFT_TLS_CA`: path to PEM-encoded CA certificate (used to verify peers)
//!
//! When all three are set the Raft listener requires mTLS and outbound
//! Raft connections present this broker's cert. When unset, the listener
//! falls back to plain TCP + HMAC framing (the post-S3 default).

#[cfg(feature = "tls")]
use std::path::Path;
#[cfg(feature = "tls")]
use std::sync::Arc;

#[cfg(feature = "tls")]
use rustls::ClientConfig;
#[cfg(feature = "tls")]
use rustls::RootCertStore;
#[cfg(feature = "tls")]
use rustls::ServerConfig;
#[cfg(feature = "tls")]
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
#[cfg(feature = "tls")]
use rustls::server::WebPkiClientVerifier;
#[cfg(feature = "tls")]
use tokio_rustls::{TlsAcceptor, TlsConnector};

use crate::cluster::error::{SlateDBError, SlateDBResult};

/// TLS configuration for the Raft RPC port.
///
/// Holds both the server-side acceptor (used by `RaftRpcServer::run`) and
/// the client-side connector (used by `RaftNetworkConnection`). Wrapped in
/// an `Arc` and cloned cheaply across connections.
#[cfg(feature = "tls")]
#[derive(Clone)]
pub struct RaftTlsConfig {
    pub acceptor: TlsAcceptor,
    pub connector: TlsConnector,
    /// SNI name presented when connecting outbound. Operators set this so
    /// every broker's cert can carry a matching SAN; defaults to the
    /// literal `kafkaesque-raft` if unset, which works when certs are
    /// issued with that as a SAN.
    pub server_name: String,
}

#[cfg(feature = "tls")]
impl std::fmt::Debug for RaftTlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RaftTlsConfig")
            .field("server_name", &self.server_name)
            .field("acceptor", &"<rustls TlsAcceptor>")
            .field("connector", &"<rustls TlsConnector>")
            .finish()
    }
}

#[cfg(feature = "tls")]
impl RaftTlsConfig {
    /// Build a config from PEM files. The same cert+key are used for both
    /// the server side (accepting peers) and the client side (presenting
    /// our identity to other servers). The CA is the trust root for both
    /// directions: outgoing connections require the peer's cert to chain
    /// to it, and incoming connections require client certs that do too.
    pub fn from_pem_files(
        cert_path: &Path,
        key_path: &Path,
        ca_path: &Path,
        server_name: String,
    ) -> SlateDBResult<Self> {
        // Mirror the data-plane TLS config: install a default crypto
        // provider before building any rustls config. With multiple
        // providers in the dependency graph (aws-lc-rs and ring),
        // `ServerConfig::builder()` panics at runtime if neither has been
        // registered. Idempotent — first writer wins.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let certs = load_certs(cert_path)?;
        let key = load_private_key(key_path)?;
        let ca_certs = load_certs(ca_path)?;

        let mut roots = RootCertStore::empty();
        for ca in &ca_certs {
            roots
                .add(ca.clone())
                .map_err(|e| SlateDBError::Config(format!("Invalid Raft CA cert: {}", e)))?;
        }
        let roots = Arc::new(roots);

        // Server side: require client certs. Without this, any peer who
        // can speak TLS would be accepted; the whole point of mTLS is the
        // server validates the *client* identity too.
        let client_verifier = WebPkiClientVerifier::builder(roots.clone())
            .build()
            .map_err(|e| SlateDBError::Config(format!("Invalid Raft client verifier: {}", e)))?;
        let server_config =
            ServerConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_client_cert_verifier(client_verifier)
                .with_single_cert(certs.clone(), key.clone_key())
                .map_err(|e| SlateDBError::Config(format!("Invalid Raft server cert: {}", e)))?;

        // Client side: same identity, same trust roots.
        let client_config =
            ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
                .with_root_certificates(Arc::try_unwrap(roots).unwrap_or_else(|a| (*a).clone()))
                .with_client_auth_cert(certs, key)
                .map_err(|e| SlateDBError::Config(format!("Invalid Raft client cert: {}", e)))?;

        Ok(Self {
            acceptor: TlsAcceptor::from(Arc::new(server_config)),
            connector: TlsConnector::from(Arc::new(client_config)),
            server_name,
        })
    }

    /// Load from `RAFT_TLS_CERT` / `RAFT_TLS_KEY` / `RAFT_TLS_CA` env vars.
    /// Returns `Ok(None)` when none are set (TLS disabled), `Err` if any
    /// one is set but the bundle is invalid or incomplete.
    pub fn from_env() -> SlateDBResult<Option<Self>> {
        let cert = std::env::var("RAFT_TLS_CERT").ok();
        let key = std::env::var("RAFT_TLS_KEY").ok();
        let ca = std::env::var("RAFT_TLS_CA").ok();
        let server_name =
            std::env::var("RAFT_TLS_SERVER_NAME").unwrap_or_else(|_| "kafkaesque-raft".to_string());

        match (cert, key, ca) {
            (None, None, None) => Ok(None),
            (Some(cert), Some(key), Some(ca)) => Ok(Some(Self::from_pem_files(
                Path::new(&cert),
                Path::new(&key),
                Path::new(&ca),
                server_name,
            )?)),
            _ => Err(SlateDBError::Config(
                "Partial Raft TLS configuration: set all three of \
                 RAFT_TLS_CERT, RAFT_TLS_KEY, RAFT_TLS_CA or none"
                    .to_string(),
            )),
        }
    }

    /// Resolve the server name into a `ServerName` for outbound TLS
    /// connections. Falls back to a fixed DNS-style placeholder when the
    /// configured value isn't a valid hostname.
    pub fn outbound_server_name(&self) -> ServerName<'static> {
        ServerName::try_from(self.server_name.clone())
            .unwrap_or_else(|_| ServerName::try_from("kafkaesque-raft").unwrap())
    }
}

/// Stub when the `tls` feature is off — keeps call sites uniform.
#[cfg(not(feature = "tls"))]
#[derive(Clone, Debug)]
pub struct RaftTlsConfig;

#[cfg(not(feature = "tls"))]
impl RaftTlsConfig {
    /// Always returns `Ok(None)` — TLS support is compiled out.
    pub fn from_env() -> SlateDBResult<Option<Self>> {
        if std::env::var_os("RAFT_TLS_CERT").is_some()
            || std::env::var_os("RAFT_TLS_KEY").is_some()
            || std::env::var_os("RAFT_TLS_CA").is_some()
        {
            return Err(SlateDBError::Config(
                "RAFT_TLS_* env vars are set but kafkaesque was built without --features tls"
                    .to_string(),
            ));
        }
        Ok(None)
    }
}

#[cfg(feature = "tls")]
fn load_certs(path: &Path) -> SlateDBResult<Vec<CertificateDer<'static>>> {
    use std::fs::File;
    use std::io::BufReader;
    let f = File::open(path)
        .map_err(|e| SlateDBError::Config(format!("Open {}: {}", path.display(), e)))?;
    let mut reader = BufReader::new(f);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| SlateDBError::Config(format!("Parse certs {}: {}", path.display(), e)))
}

#[cfg(feature = "tls")]
fn load_private_key(path: &Path) -> SlateDBResult<PrivateKeyDer<'static>> {
    use std::fs::File;
    use std::io::BufReader;
    warn_if_world_readable(path);
    let f = File::open(path)
        .map_err(|e| SlateDBError::Config(format!("Open {}: {}", path.display(), e)))?;
    let mut reader = BufReader::new(f);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| SlateDBError::Config(format!("Parse key {}: {}", path.display(), e)))?
        .ok_or_else(|| SlateDBError::Config(format!("No private key found in {}", path.display())))
}

#[cfg(all(feature = "tls", unix))]
fn warn_if_world_readable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    if let Ok(meta) = std::fs::metadata(path)
        && meta.permissions().mode() & 0o077 != 0
    {
        tracing::warn!(
            path = %path.display(),
            mode = %format!("{:o}", meta.permissions().mode() & 0o777),
            "Raft TLS private key is readable by group or other; tighten with `chmod 600`"
        );
    }
}

#[cfg(all(feature = "tls", not(unix)))]
fn warn_if_world_readable(_path: &Path) {}

#[cfg(test)]
mod tests {
    use super::*;

    /// `from_env` returns `Err` when only some of the required vars are set.
    /// We can't safely toggle env vars under `forbid(unsafe_code)` so this
    /// just verifies the type signature compiles; full env coverage lives
    /// in higher-level integration tests.
    #[test]
    fn from_env_signature_compiles() {
        let _: SlateDBResult<Option<RaftTlsConfig>> = RaftTlsConfig::from_env();
    }
}
