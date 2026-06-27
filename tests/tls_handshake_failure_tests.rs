//! Additional TLS handshake failure modes that complement
//! `tls_misconfig_tests.rs`.
//!
//! The misconfig file already covers expired cert, hostname mismatch,
//! mTLS-without-client-cert, and cert-key mismatch. This file pins the
//! remaining runtime handshake failures that are externally observable:
//!
//! 1. **Untrusted CA** — a leaf certificate signed by a CA the client does
//!    not trust must be refused at verification time. This is the most
//!    common production misconfiguration: rotating a server cert without
//!    distributing the new chain.
//! 2. **TLS version downgrade** — the broker is hardcoded to TLS 1.3 only.
//!    A TLS 1.2 client must fail the handshake rather than be served over
//!    a weaker protocol.
//! 3. **Garbage on the wire** — non-TLS bytes shipped to the TLS port must
//!    cause a clean handshake failure, not hang.

#![cfg(feature = "tls")]

use std::sync::Arc;
use std::time::Duration;

use kafkaesque::server::tls::TlsConfig;
use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, KeyUsagePurpose};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::time::timeout;

struct TestPki {
    leaf_cert_pem: String,
    leaf_key_pem: String,
    ca_cert_der: CertificateDer<'static>,
}

fn now() -> time::OffsetDateTime {
    time::OffsetDateTime::now_utc()
}

fn issue_cert(leaf_san: &str) -> TestPki {
    let mut ca_params = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, "kafkaesque-handshake-test-ca");
        dn
    };
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    let ca_key = KeyPair::generate().unwrap();
    let ca_cert = ca_params.self_signed(&ca_key).unwrap();

    let mut leaf_params = CertificateParams::new(vec![leaf_san.to_string()]).unwrap();
    leaf_params.distinguished_name = {
        let mut dn = DistinguishedName::new();
        dn.push(DnType::CommonName, leaf_san);
        dn
    };
    leaf_params.not_before = now() - Duration::from_secs(3600);
    leaf_params.not_after = now() + Duration::from_secs(3600 * 24 * 365);
    let leaf_key = KeyPair::generate().unwrap();
    let leaf_cert = leaf_params.signed_by(&leaf_key, &ca_cert, &ca_key).unwrap();

    TestPki {
        leaf_cert_pem: leaf_cert.pem(),
        leaf_key_pem: leaf_key.serialize_pem(),
        ca_cert_der: CertificateDer::from(ca_cert.der().to_vec()),
    }
}

fn build_tls_config(pki: &TestPki) -> TlsConfig {
    use rustls_pemfile;
    use std::io::BufReader;

    let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(pki.leaf_cert_pem.as_bytes()))
        .collect::<Result<_, _>>()
        .expect("parse leaf cert");
    let key = rustls_pemfile::private_key(&mut BufReader::new(pki.leaf_key_pem.as_bytes()))
        .expect("parse leaf key")
        .expect("leaf key present");
    let key_der = match key {
        PrivateKeyDer::Pkcs8(k) => PrivateKeyDer::Pkcs8(k),
        PrivateKeyDer::Pkcs1(k) => PrivateKeyDer::Pkcs1(k),
        PrivateKeyDer::Sec1(k) => PrivateKeyDer::Sec1(k),
        _ => panic!("unsupported key type for test"),
    };
    TlsConfig::from_certs_and_key(certs, key_der).expect("build TLS config")
}

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

// ===========================================================================
// 1. Untrusted CA
// ===========================================================================

#[tokio::test]
async fn untrusted_ca_is_rejected_by_client() {
    // Server presents a cert chain rooted in CA-A. Client trusts only
    // CA-B. WebPKI must refuse the chain and the handshake fails before
    // any application bytes flow.
    let server_pki = issue_cert("localhost");
    let untrusted_pki = issue_cert("localhost"); // independent CA

    let cfg = build_tls_config(&server_pki);
    let (addr, _server_err) = spawn_one_shot_tls_server(cfg).await;

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(untrusted_pki.ca_cert_der.clone()).unwrap();
    let client_cfg =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS13])
            .with_root_certificates(roots)
            .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));

    let stream = TcpStream::connect(addr).await.expect("tcp connect");
    let server_name = ServerName::try_from("localhost").expect("sni");
    let result = timeout(
        Duration::from_secs(5),
        connector.connect(server_name, stream),
    )
    .await;

    let err = match result {
        Ok(Ok(_)) => panic!("untrusted CA must NOT yield a successful handshake"),
        Ok(Err(e)) => e.to_string(),
        Err(_) => panic!("client handshake hung past timeout"),
    };
    let lower = err.to_lowercase();
    assert!(
        lower.contains("unknown")
            || lower.contains("trust")
            || lower.contains("issuer")
            || lower.contains("certificate"),
        "expected trust-anchor error, got: {err}",
    );
}

// ===========================================================================
// 2. TLS version downgrade
// ===========================================================================

#[tokio::test]
async fn tls12_only_client_is_rejected_by_tls13_only_server() {
    // The broker is configured for TLS 1.3 only. A client that
    // advertises only TLS 1.2 must be rejected — the handshake fails
    // during version negotiation rather than silently downgrading.
    let pki = issue_cert("localhost");
    let cfg = build_tls_config(&pki);
    let (addr, _server_err) = spawn_one_shot_tls_server(cfg).await;

    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(pki.ca_cert_der.clone()).unwrap();
    let client_cfg =
        rustls::ClientConfig::builder_with_protocol_versions(&[&rustls::version::TLS12])
            .with_root_certificates(roots)
            .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(client_cfg));

    let stream = TcpStream::connect(addr).await.expect("tcp connect");
    let server_name = ServerName::try_from("localhost").expect("sni");
    let result = timeout(
        Duration::from_secs(5),
        connector.connect(server_name, stream),
    )
    .await;

    match result {
        Ok(Ok(_)) => panic!("TLS 1.2 client must NOT successfully connect to TLS 1.3-only server"),
        Ok(Err(_)) => { /* expected */ }
        Err(_) => panic!("client handshake hung past timeout"),
    }
}

// ===========================================================================
// 3. Non-TLS garbage at the listener
// ===========================================================================

#[tokio::test]
async fn non_tls_bytes_cause_clean_handshake_failure() {
    // A misdirected plain-Kafka client (or a port scanner) must not
    // hang the TLS acceptor. The server-side handshake error must
    // surface within the timeout window and the slot be released.
    let pki = issue_cert("localhost");
    let cfg = build_tls_config(&pki);
    let (addr, server_err_rx) = spawn_one_shot_tls_server(cfg).await;

    // Ship some plain bytes that look like a Kafka size-prefixed frame.
    let mut s = TcpStream::connect(addr).await.expect("connect");
    s.write_all(&100i32.to_be_bytes())
        .await
        .expect("size header");
    s.write_all(b"definitely not a TLS ClientHello")
        .await
        .expect("payload");
    s.flush().await.expect("flush");

    // The server must surface a handshake failure.
    let server_err = timeout(Duration::from_secs(5), server_err_rx)
        .await
        .expect("server task must report within timeout")
        .expect("oneshot recv");
    assert!(
        server_err.is_some(),
        "server must reject non-TLS bytes with a handshake error",
    );
}
