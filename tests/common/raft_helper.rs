//! Test helper: single-node `RaftCoordinator` over an in-memory object
//! store, with leader-election polling already done.
//!
//! Use this from integration tests instead of constructing a fresh
//! `RaftCoordinator` directly, and instead of
//! [`kafkaesque::cluster::MockCoordinator`] when the test exercises a
//! semantic the mock used to skip (lease epoch bumps, fence-on-release,
//! linearizable owner reads). The audit's LR1 calls out the mock-real
//! drift — building tests on this helper from the start avoids the
//! drift entirely.

use std::sync::Arc;
use std::time::Duration;

use kafkaesque::cluster::{PartitionCoordinator, RaftConfig, RaftCoordinator};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::runtime::Handle;
use tokio::time::sleep;

use super::next_port;

/// Build a single-node `RaftConfig` parented in a fresh tempdir. Each
/// test gets its own log + snapshot directory, isolated from peers in
/// the same `cargo test` run.
pub fn raft_test_config(node_id: u64, port: u16) -> RaftConfig {
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.keep();
    RaftConfig {
        node_id,
        broker_id: node_id as i32,
        host: "127.0.0.1".to_string(),
        port: 9092 + node_id as i32,
        raft_addr: format!("127.0.0.1:{}", port),
        raft_log_dir: root.join("log").to_string_lossy().into_owned(),
        snapshot_dir: root.join("snapshots").to_string_lossy().into_owned(),
        ..RaftConfig::default()
    }
}

/// Wait up to 5s for the Raft node to elect itself leader. Panics if
/// it doesn't — single-node always elects unless the bootstrap is
/// misconfigured, so a hang here is a real bug.
pub async fn wait_for_raft_leader(coordinator: &RaftCoordinator) {
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        coordinator.is_leader().await,
        "expected single-node Raft to elect itself leader"
    );
}

/// Spin up a single-node `RaftCoordinator` over an in-memory object
/// store, wait for leader election, register the broker, and return a
/// shared handle. Call this in tests that previously used
/// `MockCoordinator::new(1, ...)`.
///
/// The bootstrap env var is set as a side effect; tests that need a
/// multi-node cluster should not use this helper.
pub async fn build_single_node_raft() -> Arc<RaftCoordinator> {
    build_single_node_raft_with_id(1).await
}

/// Variant that lets the caller pick the node id, for tests that spin
/// up multiple coordinators (each with a unique log/snapshot dir and
/// raft port).
pub async fn build_single_node_raft_with_id(node_id: u64) -> Arc<RaftCoordinator> {
    // SAFETY: process-global env var; every helper-built coordinator
    // wants the single-node bootstrap path. Setting it twice is a no-op.
    unsafe { std::env::set_var("RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE", "true") };

    let port = next_port();
    let config = raft_test_config(node_id, port);
    let store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let coordinator = RaftCoordinator::new(config, store, Handle::current())
        .await
        .expect("create coordinator");
    coordinator
        .initialize_cluster()
        .await
        .expect("init cluster");
    wait_for_raft_leader(&coordinator).await;
    coordinator
        .register_broker()
        .await
        .expect("register broker");
    Arc::new(coordinator)
}
