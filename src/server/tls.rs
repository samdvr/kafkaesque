//! TLS configuration and utilities.
//!
//! This module provides TLS support for the Kafka server using rustls.
//!
//! # Example
//!
//! ```rust,ignore
//! use kafkaesque::server::tls::TlsConfig;
//!
//! let tls_config = TlsConfig::from_pem_files(
//!     "server.crt",
//!     "server.key",
//! )?;
//!
//! let server = KafkaServer::new_with_tls("0.0.0.0:9093", handler, tls_config).await?;
//! ```

use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use std::sync::Arc;

use rustls::ServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::TlsAcceptor;

use crate::error::{Error, Result};

/// TLS configuration for the Kafka server.
#[derive(Clone)]
pub struct TlsConfig {
    /// The TLS acceptor for incoming connections.
    acceptor: TlsAcceptor,
}

impl TlsConfig {
    /// Create a TLS configuration from PEM-encoded certificate and key files.
    ///
    /// # Arguments
    ///
    /// * `cert_path` - Path to the PEM-encoded certificate file (can be a chain)
    /// * `key_path` - Path to the PEM-encoded private key file
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let tls_config = TlsConfig::from_pem_files("server.crt", "server.key")?;
    /// ```
    pub fn from_pem_files<P: AsRef<Path>>(cert_path: P, key_path: P) -> Result<Self> {
        let certs = load_certs(cert_path.as_ref())?;
        let key = load_private_key(key_path.as_ref())?;

        Self::from_certs_and_key(certs, key)
    }

    /// Create a TLS configuration from certificate chain and private key.
    pub fn from_certs_and_key(
        certs: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> Result<Self> {
        let config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(certs, key)
            .map_err(|e| Error::MissingData(format!("TLS configuration error: {}", e)))?;

        Ok(TlsConfig {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    /// Create a TLS configuration with mutual TLS (client certificate verification).
    ///
    /// # Arguments
    ///
    /// * `cert_path` - Path to the server certificate
    /// * `key_path` - Path to the server private key
    /// * `ca_cert_path` - Path to the CA certificate for verifying client certificates
    pub fn from_pem_files_with_client_auth<P: AsRef<Path>>(
        cert_path: P,
        key_path: P,
        ca_cert_path: P,
    ) -> Result<Self> {
        let certs = load_certs(cert_path.as_ref())?;
        let key = load_private_key(key_path.as_ref())?;
        let ca_certs = load_certs(ca_cert_path.as_ref())?;

        let mut root_store = rustls::RootCertStore::empty();
        for cert in ca_certs {
            root_store
                .add(cert)
                .map_err(|e| Error::MissingData(format!("Invalid CA certificate: {}", e)))?;
        }

        let client_verifier = rustls::server::WebPkiClientVerifier::builder(Arc::new(root_store))
            .build()
            .map_err(|e| Error::MissingData(format!("Failed to build client verifier: {}", e)))?;

        let config = ServerConfig::builder()
            .with_client_cert_verifier(client_verifier)
            .with_single_cert(certs, key)
            .map_err(|e| Error::MissingData(format!("TLS configuration error: {}", e)))?;

        Ok(TlsConfig {
            acceptor: TlsAcceptor::from(Arc::new(config)),
        })
    }

    /// Get the TLS acceptor for accepting connections.
    pub fn acceptor(&self) -> &TlsAcceptor {
        &self.acceptor
    }
}

/// Load certificates from a PEM file.
fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>> {
    let file = File::open(path).map_err(|e| {
        Error::MissingData(format!("Failed to open certificate file {:?}: {}", path, e))
    })?;
    let mut reader = BufReader::new(file);

    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| Error::MissingData(format!("Failed to parse certificates: {}", e)))?;

    if certs.is_empty() {
        return Err(Error::MissingData(format!(
            "No certificates found in {:?}",
            path
        )));
    }

    Ok(certs)
}

/// Load a private key from a PEM file.
fn load_private_key(path: &Path) -> Result<PrivateKeyDer<'static>> {
    let file = File::open(path)
        .map_err(|e| Error::MissingData(format!("Failed to open key file {:?}: {}", path, e)))?;
    let mut reader = BufReader::new(file);

    loop {
        match rustls_pemfile::read_one(&mut reader)
            .map_err(|e| Error::MissingData(format!("Failed to parse key file: {}", e)))?
        {
            Some(rustls_pemfile::Item::Pkcs1Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs1(key));
            }
            Some(rustls_pemfile::Item::Pkcs8Key(key)) => {
                return Ok(PrivateKeyDer::Pkcs8(key));
            }
            Some(rustls_pemfile::Item::Sec1Key(key)) => {
                return Ok(PrivateKeyDer::Sec1(key));
            }
            None => break,
            _ => continue,
        }
    }

    Err(Error::MissingData(format!(
        "No private key found in {:?}",
        path
    )))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn test_load_certs_file_not_found() {
        let result = load_certs(Path::new("/nonexistent/cert.pem"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Failed to open certificate file"));
    }

    #[test]
    fn test_load_private_key_file_not_found() {
        let result = load_private_key(Path::new("/nonexistent/key.pem"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Failed to open key file"));
    }

    #[test]
    fn test_load_certs_empty_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"").unwrap();

        let result = load_certs(file.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No certificates found"));
    }

    #[test]
    fn test_load_certs_invalid_pem() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"This is not a valid PEM file").unwrap();

        let result = load_certs(file.path());
        assert!(result.is_err());
        // Should fail because no certificates were found
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No certificates found"));
    }

    #[test]
    fn test_load_private_key_empty_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"").unwrap();

        let result = load_private_key(file.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No private key found"));
    }

    #[test]
    fn test_load_private_key_invalid_pem() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"This is not a valid PEM file").unwrap();

        let result = load_private_key(file.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No private key found"));
    }

    #[test]
    fn test_load_private_key_cert_instead_of_key() {
        // Write a certificate PEM (not a key)
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            b"-----BEGIN CERTIFICATE-----\n\
            MIIBkTCB+wIJAKHBfpegPjMCMA0GCSqGSIb3DQEBCwUAMBExDzANBgNVBAMMBnRl\n\
            c3RlcjAeFw0yMzAxMDEwMDAwMDBaFw0yNDAxMDEwMDAwMDBaMBExDzANBgNVBAMM\n\
            BnRlc3RlcjBcMA0GCSqGSIb3DQEBAQUAA0sAMEgCQQC7o96HtiXYWnEhkPZPcFui\n\
            fake_cert_data_for_testing_purposes_only_not_real==\n\
            -----END CERTIFICATE-----\n",
        )
        .unwrap();

        let result = load_private_key(file.path());
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No private key found"));
    }

    #[test]
    fn test_tls_config_from_pem_files_nonexistent() {
        let result = TlsConfig::from_pem_files("/nonexistent/cert.pem", "/nonexistent/key.pem");
        assert!(result.is_err());
    }

    #[test]
    fn test_tls_config_from_pem_files_with_client_auth_nonexistent() {
        let result = TlsConfig::from_pem_files_with_client_auth(
            "/nonexistent/cert.pem",
            "/nonexistent/key.pem",
            "/nonexistent/ca.pem",
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_error_message_includes_path() {
        let path = Path::new("/some/specific/path/cert.pem");
        let result = load_certs(path);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("/some/specific/path/cert.pem"));
    }

    #[test]
    fn test_load_certs_with_non_cert_pem_items() {
        // Create a file with non-certificate PEM items
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(
            b"-----BEGIN PUBLIC KEY-----\n\
            MFwwDQYJKoZIhvcNAQEBBQADSwAwSAJBAKj34GkxFhD90vcNLYLInFEX6Ppy1tPf\n\
            9Cnzj4p4WGeKLs1Pt8QuKUpRKfFLfRYC9AIKjbJTWit+CqvjWYzvQwECAwEAAQ==\n\
            -----END PUBLIC KEY-----\n",
        )
        .unwrap();

        let result = load_certs(file.path());
        assert!(result.is_err());
        // No certificates found, only public key
        let err = result.unwrap_err();
        assert!(err.to_string().contains("No certificates found"));
    }

    // Test that TlsConfig is Clone
    #[test]
    fn test_tls_config_is_clone() {
        fn assert_clone<T: Clone>() {}
        assert_clone::<TlsConfig>();
    }

    // Test path handling with various path types
    #[test]
    fn test_from_pem_files_accepts_various_path_types() {
        // Test with &str
        let result1 = TlsConfig::from_pem_files("/path/cert.pem", "/path/key.pem");
        assert!(result1.is_err());

        // Test with String
        let result2 = TlsConfig::from_pem_files(
            String::from("/path/cert.pem"),
            String::from("/path/key.pem"),
        );
        assert!(result2.is_err());

        // Test with PathBuf
        let result3 = TlsConfig::from_pem_files(
            std::path::PathBuf::from("/path/cert.pem"),
            std::path::PathBuf::from("/path/key.pem"),
        );
        assert!(result3.is_err());
    }

    #[test]
    fn test_load_private_key_handles_different_key_types() {
        // Test that we handle different key format headers
        // RSA PRIVATE KEY (PKCS#1)
        let mut file1 = NamedTempFile::new().unwrap();
        file1
            .write_all(
                b"-----BEGIN RSA PRIVATE KEY-----\n\
            invalid_key_data\n\
            -----END RSA PRIVATE KEY-----\n",
            )
            .unwrap();

        // Should fail parsing but recognize the format
        let _result1 = load_private_key(file1.path());
        // This may succeed or fail depending on the key content parsing

        // PRIVATE KEY (PKCS#8)
        let mut file2 = NamedTempFile::new().unwrap();
        file2
            .write_all(
                b"-----BEGIN PRIVATE KEY-----\n\
            invalid_key_data\n\
            -----END PRIVATE KEY-----\n",
            )
            .unwrap();

        // EC PRIVATE KEY (SEC1)
        let mut file3 = NamedTempFile::new().unwrap();
        file3
            .write_all(
                b"-----BEGIN EC PRIVATE KEY-----\n\
            invalid_key_data\n\
            -----END EC PRIVATE KEY-----\n",
            )
            .unwrap();

        // All should attempt to parse (may fail due to invalid content)
        let _ = load_private_key(file1.path());
        let _ = load_private_key(file2.path());
        let _ = load_private_key(file3.path());
    }
}
