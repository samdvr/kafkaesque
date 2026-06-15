//! Server lifecycle integration tests.
//!
//!
//!
//! 1. Graceful shutdown force-cancels in-flight connections after the drain
//!    timeout (`shutdown_and_wait` returns in bounded time and no connection
//!    task is left running).
//! 2. The TLS handshake is bounded by a timeout so a silent client cannot
//!    hold a connection slot forever (run with `--features tls`).
//! 3. Connection limits (global and per-IP) hold under concurrent connect
//!    storms, and slots are released and reusable on every exit path.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kafkaesque::server::{Handler, KafkaServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Handle;

/// Minimal handler: every method falls back to the trait defaults.
struct NoopHandler;

#[async_trait::async_trait]
impl Handler for NoopHandler {}

/// Poll `cond` every 10ms until it holds or `timeout` elapses.
/// Returns the final evaluation of `cond`.
async fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    cond()
}

/// Returns true if the socket has been closed by the peer (EOF / reset),
/// false if it is still open (the read just times out with no data).
async fn socket_closed_by_peer(stream: &mut TcpStream, probe: Duration) -> bool {
    let mut buf = [0u8; 16];
    match tokio::time::timeout(probe, stream.read(&mut buf)).await {
        // No data within the probe window: the server is still holding the
        // connection open, waiting for a complete request.
        Err(_) => false,
        // Clean EOF or a reset: the server dropped the connection.
        Ok(Ok(0)) | Ok(Err(_)) => true,
        // Unexpected payload, but the socket is definitely open.
        Ok(Ok(_)) => false,
    }
}

/// A client that connects and sends a deliberately incomplete frame: a
/// 4-byte size prefix announcing `claimed_len` bytes, followed by silence.
/// The server-side connection task blocks in `read_request` (its read
/// timeout is 30s) — far longer than any drain budget used in these tests.
async fn connect_stuck_client(addr: std::net::SocketAddr) -> TcpStream {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(&64i32.to_be_bytes())
        .await
        .expect("write partial frame");
    stream
}

// ============================================================================
// Finding 1: shutdown force-cancels stuck connections in bounded time
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_force_cancels_stuck_connection_in_bounded_time() {
    let server = Arc::new(
        KafkaServer::with_config("127.0.0.1:0", NoopHandler, 16, 16, Handle::current())
            .await
            .expect("bind server"),
    );
    let addr = server.local_addr().expect("local addr");

    let run_task = {
        let server = server.clone();
        tokio::spawn(async move { server.run().await })
    };

    // A connection that will never finish its request on its own.
    let mut stuck = connect_stuck_client(addr).await;
    assert!(
        wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
        "server should register the stuck connection"
    );

    // Drain budget of 300ms cannot possibly drain a connection whose read
    // timeout is 30s, so shutdown_and_wait must force-cancel it.
    let started = Instant::now();
    let drained = tokio::time::timeout(
        Duration::from_secs(5),
        server.shutdown_and_wait(Duration::from_millis(300)),
    )
    .await
    .expect("shutdown_and_wait must return in bounded time");
    let elapsed = started.elapsed();

    assert!(!drained, "a stuck connection cannot drain gracefully");
    // Bounded: 300ms drain budget + force-cancel grace (1s) + slack.
    assert!(
        elapsed < Duration::from_secs(3),
        "shutdown_and_wait took {elapsed:?}, expected < 3s"
    );

    // The force-cancel released the connection slot...
    assert_eq!(
        server.active_connections(),
        0,
        "force-cancelled connection must release its slot"
    );

    // ...and actually closed the client-facing socket.
    assert!(
        socket_closed_by_peer(&mut stuck, Duration::from_secs(2)).await,
        "server should have closed the stuck connection"
    );

    // The accept loop itself exited cleanly on the shutdown signal.
    run_task
        .await
        .expect("run task join")
        .expect("run returns Ok on shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_drain_returns_true_when_connections_close_in_time() {
    let server = Arc::new(
        KafkaServer::with_config("127.0.0.1:0", NoopHandler, 16, 16, Handle::current())
            .await
            .expect("bind server"),
    );
    let addr = server.local_addr().expect("local addr");

    let run_task = {
        let server = server.clone();
        tokio::spawn(async move { server.run().await })
    };

    let stuck = connect_stuck_client(addr).await;
    assert!(
        wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
        "server should register the connection"
    );

    // Client disconnects on its own while the drain window is still open.
    let closer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(200)).await;
        drop(stuck);
    });

    let drained = server.shutdown_and_wait(Duration::from_secs(5)).await;
    assert!(
        drained,
        "connections that close inside the drain window must count as graceful"
    );
    assert_eq!(server.active_connections(), 0);

    closer.await.expect("closer join");
    run_task
        .await
        .expect("run task join")
        .expect("run returns Ok on shutdown");
}

// ============================================================================
// Finding 3: connection limits hold under concurrent connect storms
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn global_connection_limit_holds_under_connect_storm() {
    const MAX_TOTAL: usize = 5;
    const STORM: usize = 40;

    let server = Arc::new(
        KafkaServer::with_config(
            "127.0.0.1:0",
            NoopHandler,
            1_000, // per-IP limit out of the way
            MAX_TOTAL,
            Handle::current(),
        )
        .await
        .expect("bind server"),
    );
    let addr = server.local_addr().expect("local addr");

    let run_task = {
        let server = server.clone();
        tokio::spawn(async move { server.run().await })
    };

    // Launch the storm: every client connects concurrently and holds its
    // socket open (accepted ones park in the server's read loop).
    let mut connects = Vec::with_capacity(STORM);
    for _ in 0..STORM {
        connects.push(tokio::spawn(
            async move { TcpStream::connect(addr).await.ok() },
        ));
    }
    let mut sockets = Vec::with_capacity(STORM);
    for task in connects {
        if let Some(stream) = task.await.expect("connect task join") {
            sockets.push(stream);
        }
    }
    assert!(
        !sockets.is_empty(),
        "at least some connects must succeed at the TCP level"
    );

    // The core invariant: while the server chews through the storm, the
    // active-connection counter must never exceed the configured limit.
    for _ in 0..60 {
        let active = server.active_connections();
        assert!(
            active <= MAX_TOTAL,
            "active connections ({active}) exceeded max_total ({MAX_TOTAL})"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // After the storm settles, exactly MAX_TOTAL clients still hold an open
    // socket; everyone else was rejected (their socket was closed).
    let mut open = 0usize;
    for stream in sockets.iter_mut() {
        if !socket_closed_by_peer(stream, Duration::from_millis(200)).await {
            open += 1;
        }
    }
    assert_eq!(
        open, MAX_TOTAL,
        "exactly max_total sockets should remain open after the storm"
    );
    assert_eq!(server.active_connections(), MAX_TOTAL);

    // Decrement path: closing the clients releases every slot.
    drop(sockets);
    assert!(
        wait_for(|| server.active_connections() == 0, Duration::from_secs(5)).await,
        "all slots must be released when clients disconnect"
    );

    // Released slots are reusable: new connections are accepted again.
    let replacement = connect_stuck_client(addr).await;
    assert!(
        wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
        "a released slot must be reusable"
    );
    drop(replacement);
    assert!(wait_for(|| server.active_connections() == 0, Duration::from_secs(5)).await);

    let drained = server.shutdown_and_wait(Duration::from_secs(2)).await;
    assert!(drained, "no connections left, drain must be graceful");
    run_task
        .await
        .expect("run task join")
        .expect("run returns Ok on shutdown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn per_ip_connection_limit_holds_under_connect_storm() {
    const MAX_PER_IP: usize = 3;
    const STORM: usize = 24;

    let server = Arc::new(
        KafkaServer::with_config(
            "127.0.0.1:0",
            NoopHandler,
            MAX_PER_IP,
            1_000, // global limit out of the way
            Handle::current(),
        )
        .await
        .expect("bind server"),
    );
    let addr = server.local_addr().expect("local addr");

    let run_task = {
        let server = server.clone();
        tokio::spawn(async move { server.run().await })
    };

    let mut connects = Vec::with_capacity(STORM);
    for _ in 0..STORM {
        connects.push(tokio::spawn(
            async move { TcpStream::connect(addr).await.ok() },
        ));
    }
    let mut sockets = Vec::with_capacity(STORM);
    for task in connects {
        if let Some(stream) = task.await.expect("connect task join") {
            sockets.push(stream);
        }
    }

    // All storm clients share 127.0.0.1, so the per-IP cap bounds the
    // active count exactly like the global cap would.
    for _ in 0..60 {
        let active = server.active_connections();
        assert!(
            active <= MAX_PER_IP,
            "active connections ({active}) exceeded max_per_ip ({MAX_PER_IP})"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut open = 0usize;
    for stream in sockets.iter_mut() {
        if !socket_closed_by_peer(stream, Duration::from_millis(200)).await {
            open += 1;
        }
    }
    assert_eq!(
        open, MAX_PER_IP,
        "exactly max_per_ip sockets should remain open after the storm"
    );

    drop(sockets);
    assert!(
        wait_for(|| server.active_connections() == 0, Duration::from_secs(5)).await,
        "per-IP slots must be released when clients disconnect"
    );

    let drained = server.shutdown_and_wait(Duration::from_secs(2)).await;
    assert!(drained);
    run_task
        .await
        .expect("run task join")
        .expect("run returns Ok on shutdown");
}

// ============================================================================
// Finding 2: TLS handshake timeout (requires --features tls)
// ============================================================================

#[cfg(feature = "tls")]
mod tls_lifecycle {
    use super::*;

    use std::io::BufReader;

    use kafkaesque::server::TlsKafkaServer;
    use kafkaesque::server::tls::TlsConfig;

    /// EC P-256 server (leaf) certificate for `localhost` / `127.0.0.1`,
    /// signed by `TEST_CA_PEM` and valid for 100 years.
    /// Test fixture only — not a real secret.
    const TEST_CERT_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBxTCCAWugAwIBAgIUBukK/AF6vNyY5HU83BEhGSCTCwIwCgYIKoZIzj0EAwIw\n\
HTEbMBkGA1UEAwwSa2Fma2Flc3F1ZS10ZXN0LWNhMCAXDTI2MDYxMTE3MjAxM1oY\n\
DzIxMjYwNTE4MTcyMDEzWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwWTATBgcqhkjO\n\
PQIBBggqhkjOPQMBBwNCAASx98CeslyK38qZLtZJfWovDGGnyS95K0K59e0iuyg9\n\
UEDGKJhuUGlpv6fbuM0PH6XEdwEZax1qTEUx56M6jZjKo4GPMIGMMBoGA1UdEQQT\n\
MBGCCWxvY2FsaG9zdIcEfwAAATAMBgNVHRMBAf8EAjAAMAsGA1UdDwQEAwIHgDAT\n\
BgNVHSUEDDAKBggrBgEFBQcDATAdBgNVHQ4EFgQU91vZgDeBSnxmO7nmUwpioeYb\n\
urkwHwYDVR0jBBgwFoAUZztXBpQiRtP7jpdd6uFgYoQpCuswCgYIKoZIzj0EAwID\n\
SAAwRQIhANFRqN0EePvXO1blZ5fSjvh440Fm3C5/nyeTmTYJtXduAiBOrFBvEHHI\n\
NsjJ6Y8I/Bz1rXzFupSgTE8pjTW+6RwDog==\n\
-----END CERTIFICATE-----\n";

    const TEST_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----\n\
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQgLt6eDuV7JEswnCjI\n\
A5bOjIbXNlOKr8HTrYrSQZ0jR2+hRANCAASx98CeslyK38qZLtZJfWovDGGnyS95\n\
K0K59e0iuyg9UEDGKJhuUGlpv6fbuM0PH6XEdwEZax1qTEUx56M6jZjK\n\
-----END PRIVATE KEY-----\n";

    /// Self-signed test CA that issued `TEST_CERT_PEM`; the client trusts
    /// this as its root.
    const TEST_CA_PEM: &str = "-----BEGIN CERTIFICATE-----\n\
MIIBkTCCATegAwIBAgIUB9VxtlE0QSL6lZEHO1QLMOD8lbwwCgYIKoZIzj0EAwIw\n\
HTEbMBkGA1UEAwwSa2Fma2Flc3F1ZS10ZXN0LWNhMCAXDTI2MDYxMTE3MjAxM1oY\n\
DzIxMjYwNTE4MTcyMDEzWjAdMRswGQYDVQQDDBJrYWZrYWVzcXVlLXRlc3QtY2Ew\n\
WTATBgcqhkjOPQIBBggqhkjOPQMBBwNCAAQ234KyzFqJ11UjX9Ce8VjkIfroAIG9\n\
Ivs/WKXy85itnDoX0PxyeKa7RohCNXgOJibgAUVVZmcXjiIBej8RKXKLo1MwUTAd\n\
BgNVHQ4EFgQUZztXBpQiRtP7jpdd6uFgYoQpCuswHwYDVR0jBBgwFoAUZztXBpQi\n\
RtP7jpdd6uFgYoQpCuswDwYDVR0TAQH/BAUwAwEB/zAKBggqhkjOPQQDAgNIADBF\n\
AiEA+qtNuJBVjt5PBOB/qH/G+ZxKM9V+StZNsQ8+m5ZBLIgCID2sX+Ra2MU83eHe\n\
lRJkoEHPGnuKBEFCqoRALWETKW3k\n\
-----END CERTIFICATE-----\n";

    fn test_tls_config() -> TlsConfig {
        let certs: Vec<_> = rustls_pemfile::certs(&mut BufReader::new(TEST_CERT_PEM.as_bytes()))
            .collect::<Result<_, _>>()
            .expect("parse test cert");
        let key = rustls_pemfile::private_key(&mut BufReader::new(TEST_KEY_PEM.as_bytes()))
            .expect("parse test key")
            .expect("test key present");
        TlsConfig::from_certs_and_key(certs, key).expect("build TLS config")
    }

    /// TLS client connector that trusts the test CA.
    fn test_tls_connector() -> tokio_rustls::TlsConnector {
        let mut roots = rustls::RootCertStore::empty();
        for cert in rustls_pemfile::certs(&mut BufReader::new(TEST_CA_PEM.as_bytes())) {
            roots.add(cert.expect("parse root cert")).expect("add root");
        }
        let config = rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth();
        tokio_rustls::TlsConnector::from(Arc::new(config))
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tls_handshake_timeout_releases_connection_slot() {
        let mut server = TlsKafkaServer::with_config(
            "127.0.0.1:0",
            NoopHandler,
            test_tls_config(),
            16,
            16,
            Handle::current(),
        )
        .await
        .expect("bind TLS server");
        // Tight handshake budget so the test runs fast.
        server.set_tls_handshake_timeout(Duration::from_millis(250));
        let server = Arc::new(server);
        let addr = server.local_addr().expect("local addr");

        let run_task = {
            let server = server.clone();
            tokio::spawn(async move { server.run().await })
        };

        // Connect at the TCP level and never send a ClientHello: this is
        // the slot-exhaustion DoS shape
        let mut silent = TcpStream::connect(addr).await.expect("connect");
        assert!(
            wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
            "silent client should hold a slot while the handshake is pending"
        );

        // The handshake timeout must release the slot without any client
        // cooperation.
        assert!(
            wait_for(|| server.active_connections() == 0, Duration::from_secs(3)).await,
            "handshake timeout must release the connection slot"
        );

        // And the server closed the socket.
        assert!(
            socket_closed_by_peer(&mut silent, Duration::from_secs(2)).await,
            "server should close a connection whose handshake timed out"
        );

        // The freed slot is immediately reusable by a well-behaved client.
        let connector = test_tls_connector();
        let stream = TcpStream::connect(addr).await.expect("reconnect");
        let domain = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
        let tls_stream = connector
            .connect(domain, stream)
            .await
            .expect("TLS handshake should succeed for a real client");
        assert!(
            wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
            "released slot must be reusable"
        );
        drop(tls_stream);
        assert!(wait_for(|| server.active_connections() == 0, Duration::from_secs(5)).await);

        let drained = server.shutdown_and_wait(Duration::from_secs(2)).await;
        assert!(drained);
        run_task
            .await
            .expect("run task join")
            .expect("run returns Ok on shutdown");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn tls_shutdown_force_cancels_stuck_connection() {
        let server = Arc::new(
            TlsKafkaServer::with_config(
                "127.0.0.1:0",
                NoopHandler,
                test_tls_config(),
                16,
                16,
                Handle::current(),
            )
            .await
            .expect("bind TLS server"),
        );
        let addr = server.local_addr().expect("local addr");

        let run_task = {
            let server = server.clone();
            tokio::spawn(async move { server.run().await })
        };

        // Complete the handshake, then send a partial frame and go silent —
        // the server-side task parks in its read loop.
        let connector = test_tls_connector();
        let stream = TcpStream::connect(addr).await.expect("connect");
        let domain = rustls::pki_types::ServerName::try_from("localhost").expect("server name");
        let mut tls_stream = connector.connect(domain, stream).await.expect("handshake");
        tls_stream
            .write_all(&64i32.to_be_bytes())
            .await
            .expect("write partial frame");
        tls_stream.flush().await.expect("flush");

        assert!(
            wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
            "server should register the stuck TLS connection"
        );

        let started = Instant::now();
        let drained = tokio::time::timeout(
            Duration::from_secs(5),
            server.shutdown_and_wait(Duration::from_millis(300)),
        )
        .await
        .expect("shutdown_and_wait must return in bounded time");
        let elapsed = started.elapsed();

        assert!(!drained, "a stuck TLS connection cannot drain gracefully");
        assert!(
            elapsed < Duration::from_secs(3),
            "shutdown_and_wait took {elapsed:?}, expected < 3s"
        );
        assert_eq!(
            server.active_connections(),
            0,
            "force-cancelled TLS connection must release its slot"
        );

        run_task
            .await
            .expect("run task join")
            .expect("run returns Ok on shutdown");
    }
}
