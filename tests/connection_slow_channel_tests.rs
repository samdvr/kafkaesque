//! Slow-channel tests: pin the per-connection timeouts that bound a slow or
//! adversarial client's ability to hold a slot indefinitely.
//!
//! The broker enforces three independent windows:
//!
//! - First-byte timeout (~5 s): a freshly accepted socket that never sends a
//!   size prefix is dropped before the read timeout expires.
//! - Body inter-read timeout (5 s): once a size prefix lands, body bytes
//!   that don't arrive within this window cause the frame to be rejected
//!   and the connection closed.
//! - Idle-after-auth timeout (5 min): a quiet but legitimate client between
//!   frames is NOT cut off — production producers batch and may go silent
//!   for tens of seconds at a time.
//!
//! Together these are the slow-loris guards. Without them, a single
//! attacker could pin every connection slot by connecting and dribbling
//! a byte every 30 s.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kafkaesque::server::{Handler, KafkaServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Handle;

struct NoopHandler;

#[async_trait::async_trait]
impl Handler for NoopHandler {}

async fn wait_for(mut cond: impl FnMut() -> bool, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if cond() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    cond()
}

async fn socket_closed_by_peer(stream: &mut TcpStream, probe: Duration) -> bool {
    let mut buf = [0u8; 16];
    match tokio::time::timeout(probe, stream.read(&mut buf)).await {
        Err(_) => false,
        Ok(Ok(0)) | Ok(Err(_)) => true,
        Ok(Ok(_)) => false,
    }
}

// ============================================================================
// 1. Silent first-frame client is dropped within ~5 s
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn silent_client_dropped_within_first_byte_timeout() {
    // The accept loop bounds the FIRST frame read by 5 s. A client that
    // connects and sends nothing must lose its slot before the per-frame
    // read timeout (30 s) — otherwise a tiny number of attacker connections
    // saturates the pool.
    let server = Arc::new(
        KafkaServer::with_config("127.0.0.1:0", NoopHandler, 16, 16, Handle::current())
            .await
            .expect("bind"),
    );
    let addr = server.local_addr().expect("local addr");

    let run_task = {
        let server = server.clone();
        tokio::spawn(async move { server.run().await })
    };

    let mut s = TcpStream::connect(addr).await.expect("connect");
    assert!(
        wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
        "server should register the connection",
    );

    // The first-byte timeout is 5 s. Allow generous margin for scheduling.
    let started = Instant::now();
    assert!(
        wait_for(|| server.active_connections() == 0, Duration::from_secs(10)).await,
        "silent client must be dropped before per-frame read timeout (30 s)",
    );
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_secs(10),
        "silent-client drop took {elapsed:?} — must be bounded by first-byte timeout",
    );

    assert!(
        socket_closed_by_peer(&mut s, Duration::from_secs(2)).await,
        "server must close the silent connection",
    );

    server.shutdown_and_wait(Duration::from_secs(2)).await;
    run_task.await.expect("join").expect("clean shutdown");
}

// ============================================================================
// 2. Dribbled body bytes trip the inter-read timeout
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dribble_body_bytes_trip_inter_read_timeout() {
    // Once the size prefix lands, the body is bounded by a 5 s
    // inter-read window. A client that ships the size prefix then goes
    // silent must be dropped within that window — otherwise a slow-frame
    // attack pins a slot for the full handler timeout (60 s).
    let server = Arc::new(
        KafkaServer::with_config("127.0.0.1:0", NoopHandler, 16, 16, Handle::current())
            .await
            .expect("bind"),
    );
    let addr = server.local_addr().expect("local addr");

    let run_task = {
        let server = server.clone();
        tokio::spawn(async move { server.run().await })
    };

    let mut s = TcpStream::connect(addr).await.expect("connect");
    // Announce a 1 KiB frame, then send only 4 of those 1024 bytes.
    s.write_all(&1024i32.to_be_bytes())
        .await
        .expect("size prefix");
    s.write_all(&[0u8; 4]).await.expect("partial body");
    s.flush().await.expect("flush");

    assert!(
        wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
        "server should register the connection",
    );

    // The body inter-read timeout is 5 s. After that window with no new
    // bytes the connection must be torn down.
    assert!(
        wait_for(|| server.active_connections() == 0, Duration::from_secs(10)).await,
        "dribble client must be dropped within inter-read timeout",
    );

    assert!(
        socket_closed_by_peer(&mut s, Duration::from_secs(2)).await,
        "server must close the dribble connection",
    );

    server.shutdown_and_wait(Duration::from_secs(2)).await;
    run_task.await.expect("join").expect("clean shutdown");
}

// ============================================================================
// 3. Slow client churn must not exhaust slots
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn slow_client_churn_does_not_exhaust_pool() {
    // A burst of N silent connections at once must NOT saturate the
    // server permanently: as the first-byte timeout fires on each, the
    // slot must be released and reusable. Pin: after ~10 s every slot is
    // free again even though we filled the pool to saturation.
    const MAX_TOTAL: usize = 4;
    let server = Arc::new(
        KafkaServer::with_config(
            "127.0.0.1:0",
            NoopHandler,
            1_000,
            MAX_TOTAL,
            Handle::current(),
        )
        .await
        .expect("bind"),
    );
    let addr = server.local_addr().expect("local addr");

    let run_task = {
        let server = server.clone();
        tokio::spawn(async move { server.run().await })
    };

    // Saturate: open exactly MAX_TOTAL silent connections.
    let mut sockets = Vec::new();
    for _ in 0..MAX_TOTAL {
        sockets.push(TcpStream::connect(addr).await.expect("connect"));
    }
    assert!(
        wait_for(
            || server.active_connections() == MAX_TOTAL,
            Duration::from_secs(2)
        )
        .await,
        "pool must saturate"
    );

    // Wait for the first-byte timeout to fire on all silent sockets.
    assert!(
        wait_for(|| server.active_connections() == 0, Duration::from_secs(15)).await,
        "every silent socket must be reaped",
    );

    // Confirm reusability — a fresh connect succeeds.
    let revived = TcpStream::connect(addr).await.expect("reconnect");
    assert!(
        wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
        "released slot must be reusable",
    );
    drop(revived);
    drop(sockets);

    server.shutdown_and_wait(Duration::from_secs(3)).await;
    run_task.await.expect("join").expect("clean shutdown");
}
