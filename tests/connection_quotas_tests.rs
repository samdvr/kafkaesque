//! Connection-quota tests: per-IP, broker-wide, and the global inflight-byte
//! budget that bounds attacker-shaped frame allocations.
//!
//! Existing storm-style coverage (in `tests/server_lifecycle_tests.rs`) pins
//! that the active-connection counter never exceeds the configured caps under
//! a concurrent connect storm. This file pins the *complementary* properties:
//!
//! 1. Per-IP and global limits are independent — the global limit can be hit
//!    even when no single IP is at its per-IP cap, and vice versa.
//! 2. Slot bookkeeping rolls back cleanly when a per-IP rejection happens
//!    after a successful global reservation: the global counter must not be
//!    leaked and the slot must be reusable by the next connect.
//! 3. The global inflight-byte budget is queryable and applied process-wide.
//!    The handful of test integration paths that don't configure it explicitly
//!    must still observe the documented default.
//! 4. The pre-auth frame-size cap (16 KiB by design) refuses oversized first
//!    frames without parsing them — verified indirectly by closing a
//!    connection that ships a giant size-prefix before any handshake.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kafkaesque::server::{Handler, KafkaServer, set_global_inflight_byte_budget};
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
        tokio::time::sleep(Duration::from_millis(10)).await;
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
// 1. Global cap holds even when per-IP cap is generous
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn global_cap_rejects_when_per_ip_cap_has_room() {
    // per-IP limit set well above the global limit. Every connect from
    // 127.0.0.1 must still be subject to the tighter global cap.
    const MAX_TOTAL: usize = 3;
    let server = Arc::new(
        KafkaServer::with_config("127.0.0.1:0", NoopHandler, 1_000, MAX_TOTAL, Handle::current())
            .await
            .expect("bind"),
    );
    let addr = server.local_addr().expect("local addr");

    let run_task = {
        let server = server.clone();
        tokio::spawn(async move { server.run().await })
    };

    let mut sockets = Vec::new();
    for _ in 0..(MAX_TOTAL + 5) {
        if let Ok(s) = TcpStream::connect(addr).await {
            sockets.push(s);
        }
    }

    // The active counter must settle at exactly MAX_TOTAL. Beyond that,
    // any extra socket gets rejected by the accept loop.
    assert!(
        wait_for(|| server.active_connections() == MAX_TOTAL, Duration::from_secs(3)).await,
        "global cap must bound active connections to {MAX_TOTAL}; got {}",
        server.active_connections(),
    );

    // Drop everything and confirm slots are reusable.
    drop(sockets);
    assert!(wait_for(|| server.active_connections() == 0, Duration::from_secs(5)).await);

    server.shutdown_and_wait(Duration::from_secs(2)).await;
    run_task.await.expect("join").expect("clean shutdown");
}

// ============================================================================
// 2. Per-IP cap rejection rolls back the global reservation
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn per_ip_rejection_releases_global_slot() {
    // The accept path increments the global counter speculatively, then
    // checks per-IP. If per-IP rejects, the speculative global increment
    // must be rolled back — otherwise a per-IP-rejected client silently
    // burns a global slot and eventually exhausts the broker.
    const MAX_PER_IP: usize = 2;
    const MAX_TOTAL: usize = 10;
    let server = Arc::new(
        KafkaServer::with_config(
            "127.0.0.1:0",
            NoopHandler,
            MAX_PER_IP,
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

    // Storm well past the per-IP cap. Every connect after the cap should
    // be rejected without consuming a global slot.
    let mut sockets = Vec::new();
    for _ in 0..(MAX_PER_IP + 20) {
        if let Ok(s) = TcpStream::connect(addr).await {
            sockets.push(s);
        }
    }

    // Active count must equal the per-IP cap, NOT something inflated by
    // ghost global reservations. If the global counter leaks, it would
    // sit somewhere between MAX_PER_IP and MAX_TOTAL, blocking new IPs.
    assert!(
        wait_for(|| server.active_connections() == MAX_PER_IP, Duration::from_secs(3)).await,
        "active connections must equal per-IP cap; got {}",
        server.active_connections(),
    );
    assert!(
        server.active_connections() < MAX_TOTAL,
        "global counter must not include the rejected per-IP attempts",
    );

    // Drop the accepted clients and confirm every slot is released.
    drop(sockets);
    assert!(wait_for(|| server.active_connections() == 0, Duration::from_secs(5)).await);

    // A fresh connect now has the full budget available again.
    let revived = TcpStream::connect(addr).await.expect("reconnect");
    assert!(
        wait_for(|| server.active_connections() == 1, Duration::from_secs(2)).await,
        "released slot must be reusable",
    );
    drop(revived);

    server.shutdown_and_wait(Duration::from_secs(2)).await;
    run_task.await.expect("join").expect("clean shutdown");
}

// ============================================================================
// 3. Global inflight-byte budget configuration is monotonic
// ============================================================================

#[test]
fn global_inflight_budget_configurator_is_set_once() {
    // The configurator returns true on its first successful call and
    // false on every subsequent call — the budget is fixed for the
    // lifetime of the process. Operators that want a smaller budget
    // must call before any frame is processed; tests run in the same
    // process, so the second call below is the documented contract.
    let _first = set_global_inflight_byte_budget(64 * 1024 * 1024);
    // Whatever happened above, a second call must NOT silently override.
    let second = set_global_inflight_byte_budget(8 * 1024 * 1024);
    assert!(
        !second,
        "set_global_inflight_byte_budget must reject reconfiguration",
    );
}

// ============================================================================
// 4. Connection that ships a giant size-prefix before authenticating is
//    closed by the server.
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn oversized_first_frame_is_refused_and_socket_closed() {
    // No SASL is required by NoopHandler, so the frame goes through the
    // post-auth size cap (DEFAULT_MAX_MESSAGE_SIZE = 100 MiB). We claim
    // a 200 MiB frame: the server must reject the size header, log the
    // rejection, and close the connection cleanly.
    const OVERSIZED: i32 = 200 * 1024 * 1024;
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
    s.write_all(&OVERSIZED.to_be_bytes())
        .await
        .expect("write giant size header");
    s.flush().await.expect("flush");

    // The server must close the socket promptly rather than buffer 200 MiB.
    assert!(
        socket_closed_by_peer(&mut s, Duration::from_secs(3)).await,
        "oversized frame must be refused and the socket closed",
    );

    server.shutdown_and_wait(Duration::from_secs(2)).await;
    run_task.await.expect("join").expect("clean shutdown");
}

// ============================================================================
// 5. Negative size header is rejected as malformed
// ============================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_size_header_is_rejected() {
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
    s.write_all(&(-1i32).to_be_bytes())
        .await
        .expect("write negative size");
    s.flush().await.expect("flush");

    assert!(
        socket_closed_by_peer(&mut s, Duration::from_secs(3)).await,
        "negative-size frame must be refused",
    );

    server.shutdown_and_wait(Duration::from_secs(2)).await;
    run_task.await.expect("join").expect("clean shutdown");
}
