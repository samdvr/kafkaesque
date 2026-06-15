//! TLS misconfiguration coverage.
//!
//! The audit flagged that TLS code was exercised only on the happy path.
//! This file mints adversarial certificates with `rcgen` at test time —
//! no embedded base64 blobs that go stale — and drives both the
//! configuration boundary (PEM load + cert-key pairing) and a live TLS
//! handshake to verify that:
//!
//! - **Expired certificate** is rejected by a TLS *client* (rustls
//!   server-side does not validate its own cert expiry; the failure is
//!   visible only when a peer attempts the handshake — which is exactly
//!   how it would manifest in production).
//! - **Hostname mismatch** is rejected when the SNI / expected server name
//!   doesn't appear as a SAN.
//! - **Missing client cert under mTLS** causes the server to reject the
//!   connection during the handshake.
//! - **Cert-key mismatch** is caught at config time, not at handshake time.

#![cfg(feature = "tls")]

use std::io::Write;
use std::sync::Arc;
use std::time::Duration;

use kafkaesque::server::tls::TlsConfig;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, KeyUsagePurpose};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tempfile::NamedTempFile;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

/// A self-signed leaf, plus a CA that issued it. Useful for mTLS tests.
struct TestPki {
    leaf_cert_pem: String,
    leaf_key_pem: String,
    ca_cert_pem: String,
    leaf_key_der: PrivateKeyDer<'static>,
    ca_cert_der: CertificateDer<'static>,
}

/// Build a CA + leaf cert. The leaf carries `leaf_san` as its SubjectAltName
/// and is valid between `not_before` and `not_after`.
fn issue_cert(
    leaf_san: &str,
    not_before: time::OffsetDateTime,
    not_after: time::OffsetDateTime,
) -> TestPki {
    // CA.
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "kafkaesque-test-ca");
        dn
    };
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    // Leaf.
    let mut leaf_params = CertificateParams::new(vec![leaf_san.to_string()]).unwrap();
    leaf_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, leaf_san);
        dn
    };
    leaf_params.not_before = not_before;
    leaf_params.not_after = not_after;
    let leaf_key = KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    let leaf_cert_pem = leaf_cert.pem();
    let leaf_key_pem = leaf_key.serialize_pem();
    let ca_cert_pem = ca_cert.pem();
    let leaf_key_der =
        PrivateKeyDer::try_from(leaf_key.serialize_der()).expect("rcgen produces valid PKCS#8");
    let ca_cert_der = CertificateDer::from(ca_cert.der().to_vec());

    TestPki {
        leaf_cert_pem,
        leaf_key_pem,
        ca_cert_pem,
        leaf_key_der,
        ca_cert_der,
    }
}

fn now() -> time::OffsetDateTime {
    time::OffsetDateTime::now_utc()
}

fn write_pem(pem: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().expect("tempfile");
    f.write_all(pem.as_bytes()).expect("write pem");
    f.flush().unwrap();
    f
}

/// Drive a TLS handshake from a client against a server bound on `addr`,
/// expecting the verifier built from `ca_certs` and the server name `sni`.
/// Returns the client-side handshake error, if any.
async fn client_handshake_error(
    addr: std::net::SocketAddr,
    ca_certs: &[CertificateDer<'static>],
    sni: &str,
) -> Option<String> {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let mut roots = rustls::RootCertStore::empty();
    for ca in ca_certs {
        roots.add(ca.clone()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
        .with_root_certificates(roots)
        .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));

    let stream = TcpStream::connect(addr).await.expect("tcp connect");
    let server_name = ServerName::try_from(sni.to_string()).expect("valid sni");

    match timeout(
        Duration::from_secs(5),
        connector.connect(server_name, stream),
    )
    .await
    {
        Ok(Ok(_tls)) => None,
        Ok(Err(e)) => Some(e.to_string()),
        Err(_) => Some("client handshake timed out".to_string()),
    }
}

/// Spawn a TLS-enabled echo server using `cfg`. Accepts a single
/// connection, performs the handshake, then exits. Returns the bound
/// address. Any handshake error from the server side is surfaced through
/// the returned receiver.
async fn spawn_one_shot_tls_server(
    cfg: TlsConfig,
) -> (
    std::net::SocketAddr,
    tokio::sync::oneshot::Receiver<Option<String>>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = tokio::sync::oneshot::channel();

    tokio::spawn(async move {
        let acceptor = cfg.acceptor().clone();
        match listener.accept().await {
            Ok((stream, _)) => {
                let result = timeout(Duration::from_secs(5), acceptor.accept(stream)).await;
                let err = match result {
                    Ok(Ok(mut tls)) => {
                        // Brief read then drop; we just want to confirm the
                        // handshake completed before tearing down.
                        let _ = tls.write_all(b"ok").await;
                        let _ = tls.shutdown().await;
                        None
                    }
                    Ok(Err(e)) => Some(e.to_string()),
                    Err(_) => Some("server handshake timed out".to_string()),
                };
                let _ = tx.send(err);
            }
            Err(e) => {
                let _ = tx.send(Some(format!("accept failed: {e}")));
            }
        }
    });
    (addr, rx)
}

// ============================================================================
// 1. Expired cert
// ============================================================================

#[tokio::test]
async fn expired_cert_is_rejected_by_client() {
    // Cert expired one day ago. The server-side `TlsConfig::from_pem_files`
    // accepts it without complaint — rustls server-side does not validate
    // its own NotAfter. The failure surfaces at client handshake time,
    // which is exactly how a rotated-but-not-deployed cert breaks prod.
    let pki = issue_cert(
        "localhost",
        now() - Duration::from_secs(60 * 60 * 24 * 30),
        now() - Duration::from_secs(60 * 60 * 24),
    );
    let cert_f = write_pem(&pki.leaf_cert_pem);
    let key_f = write_pem(&pki.leaf_key_pem);

    let cfg = TlsConfig::from_pem_files(cert_f.path(), key_f.path())
        .expect("server accepts an expired cert at config time — failure is at handshake");
    let (addr, _server_err) = spawn_one_shot_tls_server(cfg).await;

    let err = client_handshake_error(addr, &[pki.ca_cert_der.clone()], "localhost")
        .await
        .expect("expired cert must fail the handshake");
    assert!(
        err.to_lowercase().contains("expired") || err.to_lowercase().contains("not valid"),
        "expected expiry error, got: {err}"
    );
}

// ============================================================================
// 2. Hostname mismatch
// ============================================================================

#[tokio::test]
async fn hostname_mismatch_is_rejected_by_client() {
    // Cert is valid only for `other.example`; client demands SNI =
    // `localhost`. WebPKI must refuse rather than treat the wildcard /
    // SAN list as elastic.
    let pki = issue_cert(
        "other.example",
        now() - Duration::from_secs(3600),
        now() + Duration::from_secs(3600 * 24 * 365),
    );
    let cert_f = write_pem(&pki.leaf_cert_pem);
    let key_f = write_pem(&pki.leaf_key_pem);

    let cfg = TlsConfig::from_pem_files(cert_f.path(), key_f.path()).unwrap();
    let (addr, _server_err) = spawn_one_shot_tls_server(cfg).await;

    let err = client_handshake_error(addr, &[pki.ca_cert_der.clone()], "localhost")
        .await
        .expect("hostname mismatch must fail the handshake");
    let lower = err.to_lowercase();
    assert!(
        lower.contains("name") || lower.contains("dns") || lower.contains("subject"),
        "expected hostname/DNS error, got: {err}"
    );
}

// ============================================================================
// 3. Missing client cert when server requires mTLS
// ============================================================================

#[tokio::test]
async fn mtls_without_client_cert_is_rejected() {
    // Server is configured for mTLS via `from_pem_files_with_client_auth`.
    // A client that presents no cert (`with_no_client_auth`) must be
    // refused during the handshake, not silently authenticated.
    let pki = issue_cert(
        "localhost",
        now() - Duration::from_secs(3600),
        now() + Duration::from_secs(3600 * 24 * 365),
    );
    let cert_f = write_pem(&pki.leaf_cert_pem);
    let key_f = write_pem(&pki.leaf_key_pem);
    let ca_f = write_pem(&pki.ca_cert_pem);

    let cfg = TlsConfig::from_pem_files_with_client_auth(cert_f.path(), key_f.path(), ca_f.path())
        .expect("mTLS server config builds with valid PKI");
    let (addr, server_err_rx) = spawn_one_shot_tls_server(cfg).await;

    // Client configured without a client cert.
    let err = client_handshake_error(addr, &[pki.ca_cert_der.clone()], "localhost").await;
    let server_err = timeout(Duration::from_secs(5), server_err_rx)
        .await
        .ok()
        .and_then(|r| r.ok())
        .flatten();

    // Either side may surface the error first depending on TLS 1.3's
    // post-handshake auth timing — assert at least one side rejected.
    assert!(
        err.is_some() || server_err.is_some(),
        "mTLS handshake must fail when client presents no certificate; client_err={:?}, server_err={:?}",
        err,
        server_err
    );
}

// ============================================================================
// 4. Cert-key mismatch (config-layer failure)
// ============================================================================

#[test]
fn cert_key_mismatch_is_rejected_at_config_time() {
    // Generate two independent leaf certs, then pair leaf-A's cert with
    // leaf-B's key. rustls's `with_single_cert` validates that the public
    // key in the cert matches the private key — a mismatch is caught here,
    // before any connection is accepted.
    let pki_a = issue_cert(
        "localhost",
        now() - Duration::from_secs(3600),
        now() + Duration::from_secs(3600 * 24),
    );
    let pki_b = issue_cert(
        "other.example",
        now() - Duration::from_secs(3600),
        now() + Duration::from_secs(3600 * 24),
    );

    let cert_f = write_pem(&pki_a.leaf_cert_pem);
    let key_f = write_pem(&pki_b.leaf_key_pem);

    let result = TlsConfig::from_pem_files(cert_f.path(), key_f.path());
    let err = result.err().expect("mismatched cert+key must not build");
    let msg = err.to_string();
    assert!(
        msg.contains("TLS configuration error") || msg.to_lowercase().contains("key"),
        "expected cert/key mismatch error, got: {msg}"
    );
}

#[test]
fn from_certs_and_key_with_empty_chain_is_rejected() {
    // An explicit zero-length chain is nonsensical; the constructor must
    // not silently accept it as a valid identity.
    let pki = issue_cert(
        "localhost",
        now() - Duration::from_secs(60),
        now() + Duration::from_secs(3600),
    );
    let result = TlsConfig::from_certs_and_key(Vec::new(), pki.leaf_key_der);
    assert!(result.is_err(), "empty cert chain must be rejected");
}

#[test]
fn from_pem_files_with_client_auth_rejects_invalid_ca() {
    // A CA file that's syntactically valid PEM but contains no certs (or a
    // cert that's not valid as a trust root) must be refused so the
    // operator sees the misconfiguration at startup rather than as
    // handshake failures later.
    let pki = issue_cert(
        "localhost",
        now() - Duration::from_secs(60),
        now() + Duration::from_secs(3600),
    );
    let cert_f = write_pem(&pki.leaf_cert_pem);
    let key_f = write_pem(&pki.leaf_key_pem);
    let bogus_ca_f = write_pem("not a valid pem");

    let result =
        TlsConfig::from_pem_files_with_client_auth(cert_f.path(), key_f.path(), bogus_ca_f.path());
    assert!(result.is_err(), "invalid CA must not build mTLS config");
}
