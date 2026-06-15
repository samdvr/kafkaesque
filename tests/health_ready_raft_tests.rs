//! Integration tests that drive the real `HealthServer` over TCP and assert
//! `/ready` reports `503 raft_not_in_quorum` when the Raft consensus gauge
//! says this broker is not part of a quorum.
//!
//! The handler-level case is covered in `src/server/health.rs` unit tests;
//! this file covers the end-to-end path through the actual HTTP server so
//! a wiring regression (a broker that lost its Raft state but still reports
//! 200 to Kubernetes) can't slip through.
//!
//! `RAFT_STATE` is a process-global Prometheus gauge, so these tests are
//! marked `#[serial]` to avoid racing other tests that read or set it.

use std::sync::Arc;
use std::time::Duration;

use kafkaesque::cluster::metrics::{RAFT_STATE_UNINITIALIZED, is_raft_ready, set_raft_state};
use kafkaesque::cluster::zombie_mode::ZombieModeState;
use kafkaesque::server::health::HealthServer;
use serial_test::serial;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

const RAFT_FOLLOWER: i64 = 0;
const RAFT_CANDIDATE: i64 = 1;
const RAFT_LEADER: i64 = 2;
const RAFT_SHUTDOWN: i64 = -1;

/// Reset the Raft state gauge to its pre-init sentinel on drop, so a panic
/// mid-test can't poison later tests that read the same global.
struct RaftStateGuard;

impl RaftStateGuard {
    fn set(state: i64) -> Self {
        set_raft_state(state);
        Self
    }
}

impl Drop for RaftStateGuard {
    fn drop(&mut self) {
        set_raft_state(RAFT_STATE_UNINITIALIZED);
    }
}

/// Spin up a `HealthServer` on an ephemeral port and return the bound addr
/// plus a shutdown handle. The server runs until the returned guard is
/// dropped.
async fn spawn_health_server() -> (std::net::SocketAddr, Arc<HealthServer>) {
    let zombie = Arc::new(ZombieModeState::new());
    let server = HealthServer::new("127.0.0.1:0", zombie)
        .await
        .expect("bind health server");
    let addr = server.local_addr().expect("local_addr");
    let server = Arc::new(server);
    let server_clone = server.clone();
    tokio::spawn(async move {
        let _ = server_clone.run().await;
    });
    // Yield once so the accept loop is parked on the listener before we
    // dial. Without this the first connect occasionally races the server
    // and gets ECONNRESET on slow CI runners.
    tokio::time::sleep(Duration::from_millis(20)).await;
    (addr, server)
}

/// Send `GET /ready HTTP/1.1` over TCP and return the full response.
async fn http_get_ready(addr: std::net::SocketAddr) -> String {
    let mut stream = TcpStream::connect(addr).await.expect("connect");
    stream
        .write_all(b"GET /ready HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");
    let mut buf = Vec::new();
    timeout(Duration::from_secs(5), stream.read_to_end(&mut buf))
        .await
        .expect("read timeout")
        .expect("read response");
    String::from_utf8_lossy(&buf).into_owned()
}

#[tokio::test]
#[serial]
async fn ready_returns_503_when_raft_is_candidate() {
    let _guard = RaftStateGuard::set(RAFT_CANDIDATE);
    assert!(!is_raft_ready(), "candidate must not be ready");

    let (addr, server) = spawn_health_server().await;
    let response = http_get_ready(addr).await;
    server.shutdown();

    assert!(
        response.starts_with("HTTP/1.1 503"),
        "expected 503, got: {response}"
    );
    assert!(
        response.contains("reason: raft_not_in_quorum"),
        "expected raft_not_in_quorum reason, got: {response}"
    );
    assert!(response.contains("status: not_ready"));
}

#[tokio::test]
#[serial]
async fn ready_returns_503_when_raft_state_uninitialized() {
    // A broker that's started but hasn't yet completed its first Raft tick
    // must not draw traffic — the gauge default sentinel covers exactly this
    // window between process startup and openraft's first metrics report.
    let _guard = RaftStateGuard::set(RAFT_STATE_UNINITIALIZED);

    let (addr, server) = spawn_health_server().await;
    let response = http_get_ready(addr).await;
    server.shutdown();

    assert!(response.starts_with("HTTP/1.1 503"), "got: {response}");
    assert!(response.contains("reason: raft_not_in_quorum"));
}

#[tokio::test]
#[serial]
async fn ready_returns_503_when_raft_shutdown() {
    let _guard = RaftStateGuard::set(RAFT_SHUTDOWN);

    let (addr, server) = spawn_health_server().await;
    let response = http_get_ready(addr).await;
    server.shutdown();

    assert!(response.starts_with("HTTP/1.1 503"), "got: {response}");
    assert!(response.contains("reason: raft_not_in_quorum"));
}

#[tokio::test]
#[serial]
async fn ready_returns_200_when_raft_follower_or_leader() {
    let (addr, server) = spawn_health_server().await;

    // Follower: in quorum, healthy.
    {
        let _guard = RaftStateGuard::set(RAFT_FOLLOWER);
        let response = http_get_ready(addr).await;
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "follower expected 200, got: {response}"
        );
        assert!(response.contains("status: ready"));
    }

    // Leader: in quorum, healthy.
    {
        let _guard = RaftStateGuard::set(RAFT_LEADER);
        let response = http_get_ready(addr).await;
        assert!(
            response.starts_with("HTTP/1.1 200"),
            "leader expected 200, got: {response}"
        );
        assert!(response.contains("status: ready"));
    }

    server.shutdown();
}
