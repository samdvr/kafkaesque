//! Graceful-shutdown drain-phase tests.
//!
//! `tests/server_lifecycle_tests.rs` already covers force-cancel, slot
//! release, and per-IP / global limits under shutdown. This file pins the
//! complementary contract for the drain *phase* itself — the period
//! between "shutdown signal sent" and "drain budget exhausted":
//!
//! 1. A request that lands BEFORE the shutdown signal must still receive
//!    a response. Truncating an in-flight conversation breaks at-most-once
//!    semantics for clients that consult the response code.
//! 2. After the shutdown signal, the listener stops accepting new
//!    connections — a fresh TCP connect is refused or its socket is
//!    immediately closed without a request being served.
//! 3. The accept loop's `run()` future returns `Ok(())` on shutdown
//!    rather than propagating an error.
//! 4. A graceful drain (every client closed inside the window) reports
//!    success — `shutdown_and_wait` returns `true` and the active counter
//!    is zero by the time it does.

use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use kafkaesque::server::{Handler, KafkaServer};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::runtime::Handle;

struct NoopHandler;

#[async_trait::async_trait]
impl Handler for NoopHandler {}

/// Build a minimal ApiVersions v0 request.
/// Header: api_key=18, api_version=0, correlation_id=1, client_id="t".
fn api_versions_v0_frame(correlation_id: i32) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(18); // api_key = ApiVersions
    body.put_i16(0);  // api_version = 0
    body.put_i32(correlation_id);
    body.put_i16(1); // client_id length
    body.extend_from_slice(b"t");

    let mut frame = BytesMut::new();
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(&body);
    frame.to_vec()
}

async fn read_kafka_response(stream: &mut TcpStream) -> Option<Vec<u8>> {
    let mut size_buf = [0u8; 4];
    if stream.read_exact(&mut size_buf).await.is_err() {
        return None;
    }
    let size = i32::from_be_bytes(size_buf) as usize;
    let mut body = vec![0u8; size];
    stream.read_exact(&mut body).await.ok()?;
    Some(body)
}

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

// ===========================================================================
// 1. End-to-end happy path: ApiVersions request gets a response
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_serves_api_versions_request_end_to_end() {
    // Smoke test: the server actually answers a real client request.
    // This is the precondition for every later drain test — without it,
    // we'd be measuring a broken server, not a graceful one.
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

    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(&api_versions_v0_frame(42))
        .await
        .expect("write");
    stream.flush().await.expect("flush");

    let body = read_kafka_response(&mut stream)
        .await
        .expect("must receive a response");
    assert!(body.len() >= 4, "response must contain at least correlation_id");
    let correlation_id = i32::from_be_bytes(body[0..4].try_into().unwrap());
    assert_eq!(correlation_id, 42, "response correlation_id must echo the request");

    drop(stream);
    server.shutdown_and_wait(Duration::from_secs(2)).await;
    run_task.await.expect("join").expect("clean shutdown");
}

// ===========================================================================
// 2. Drain returns true when client closes inside the window
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_returns_true_when_client_closes_inside_window() {
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

    // Open a single connection; the server registers it.
    let stream = TcpStream::connect(addr).await.expect("connect");
    assert!(
        wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
        "server must register the connection",
    );

    // Schedule the client to disconnect well inside the drain budget.
    let closer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(150)).await;
        drop(stream);
    });

    let drained = server.shutdown_and_wait(Duration::from_secs(5)).await;
    assert!(
        drained,
        "drain must return true when every client disconnects inside the window",
    );
    assert_eq!(server.active_connections(), 0);

    closer.await.expect("closer join");
    run_task.await.expect("join").expect("clean shutdown");
}

// ===========================================================================
// 3. New connections are refused after shutdown signal
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_connections_refused_after_shutdown_signal() {
    // Once shutdown_tx fires, the accept loop returns. New TCP connect
    // attempts must either fail or land on a stale socket that closes
    // immediately — they must NOT be served as if the server were healthy.
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

    // Confirm the server is healthy first.
    {
        let mut s = TcpStream::connect(addr).await.expect("pre-shutdown connect");
        s.write_all(&api_versions_v0_frame(1)).await.expect("write");
        s.flush().await.expect("flush");
        let body = read_kafka_response(&mut s).await.expect("response");
        assert!(body.len() >= 4);
    }

    // Drain everything quickly.
    let drained = server.shutdown_and_wait(Duration::from_secs(2)).await;
    assert!(drained);

    // The run task must exit cleanly. Awaiting it here also ensures the
    // listener is fully torn down before we attempt the post-shutdown
    // connect below — without this barrier the OS may still answer SYN
    // for a brief window after shutdown_and_wait returns.
    run_task.await.expect("join").expect("run returns Ok on shutdown");

    // A fresh connect after shutdown must NOT receive a real response.
    // The exact failure mode is OS-dependent:
    //
    // - The listener may have been fully torn down (connect itself fails).
    // - The kernel may still answer SYN briefly while the listener FD is
    //   reaped (connect succeeds), but with no user-space accept() loop to
    //   pick the socket up, the request hangs without a response.
    // - The accepted-but-unread socket may be reset.
    //
    // Any of those is correct; the regression we're guarding against is a
    // *full Kafka response* arriving over a connection opened after shutdown.
    match TcpStream::connect(addr).await {
        Err(_) => { /* listener already closed — expected */ }
        Ok(mut s) => {
            let _ = s.write_all(&api_versions_v0_frame(99)).await;
            let _ = s.flush().await;
            let mut size_buf = [0u8; 4];
            let read_outcome = tokio::time::timeout(
                Duration::from_secs(2),
                s.read_exact(&mut size_buf),
            )
            .await;
            match read_outcome {
                Ok(Ok(_)) => panic!("post-shutdown connection must not receive a response"),
                Ok(Err(_)) => { /* EOF / reset — expected */ }
                Err(_) => { /* hang-then-timeout — also expected */ }
            }
        }
    }
}

// ===========================================================================
// 4. Drain cleanly handles zero in-flight connections
// ===========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drain_with_no_active_connections_returns_immediately() {
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

    // Wait for the listener to be accepting before sending the shutdown
    // signal. Without this barrier, `shutdown()` can fire before `run()`
    // has subscribed to the broadcast and the signal is missed — the
    // accept loop then runs forever.
    assert!(
        wait_for_listener(addr, Duration::from_secs(2)).await,
        "server must be accepting before we test shutdown",
    );

    let started = Instant::now();
    let drained = server.shutdown_and_wait(Duration::from_secs(5)).await;
    let elapsed = started.elapsed();

    assert!(drained, "drain with zero connections must succeed");
    assert!(
        elapsed < Duration::from_secs(2),
        "drain with zero connections must return promptly; took {elapsed:?}",
    );

    run_task.await.expect("join").expect("clean shutdown");
}

async fn wait_for_listener(addr: std::net::SocketAddr, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if let Ok(s) = TcpStream::connect(addr).await {
            drop(s);
            // Give the server a tick to register and clear our probe.
            tokio::time::sleep(Duration::from_millis(20)).await;
            return true;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    false
}
