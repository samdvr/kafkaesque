//! Behavioral contract tests shared by MockCoordinator and RaftCoordinator.
//!
//! These guard against drift between the in-memory mock and the replicated
//! Raft implementation — the distributed/linearizability suites exercise the
//! mock, so any semantic mismatch would otherwise go unnoticed.

use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::Duration;

use kafkaesque::cluster::{
    ConsumerGroupCoordinator, MockCoordinator, PartitionCoordinator, RaftConfig, RaftCoordinator,
};
use object_store::memory::InMemory;
use tokio::runtime::Handle;
use tokio::time::sleep;

static PORT_COUNTER: AtomicU16 = AtomicU16::new(20000);

fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

fn raft_test_config(node_id: u64, port: u16) -> RaftConfig {
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

async fn wait_for_raft_leader(coordinator: &RaftCoordinator) {
    let start = std::time::Instant::now();
    while !coordinator.is_leader().await && start.elapsed() < Duration::from_secs(5) {
        sleep(Duration::from_millis(50)).await;
    }
    assert!(
        coordinator.is_leader().await,
        "expected single-node Raft to elect itself leader"
    );
}

async fn assert_partition_ownership_contract<C: PartitionCoordinator>(coord: &C, topic: &str) {
    coord
        .register_topic(topic, 1)
        .await
        .expect("register topic");

    let owns_before = coord
        .owns_partition_for_read(topic, 0)
        .await
        .expect("owns before acquire");
    assert!(!owns_before, "partition should be unowned before acquire");

    let acquired = coord
        .acquire_partition(topic, 0, 60)
        .await
        .expect("acquire");
    assert!(acquired, "first acquire should succeed");

    let owns_after = coord
        .owns_partition_for_read(topic, 0)
        .await
        .expect("owns after acquire");
    assert!(owns_after, "partition should be owned after acquire");

    let owner = coord
        .get_partition_owner(topic, 0)
        .await
        .expect("get owner")
        .expect("owner present");
    assert_eq!(owner, coord.broker_id(), "owner should be this broker");

    coord.release_partition(topic, 0).await.expect("release");

    let owns_released = coord
        .owns_partition_for_read(topic, 0)
        .await
        .expect("owns after release");
    assert!(!owns_released, "partition should be unowned after release");
}

async fn assert_group_join_generation_contract<C: ConsumerGroupCoordinator>(coord: &C) {
    let (gen1, _, _, _, _) = coord
        .join_group("contract-group", "member-a", b"meta-a", 10_000)
        .await
        .expect("first join");
    assert_eq!(gen1, 1, "first join should start at generation 1");

    let (gen2, _, _, _, _) = coord
        .join_group("contract-group", "member-b", b"meta-b", 10_000)
        .await
        .expect("second join");
    assert!(
        gen2 > gen1,
        "second join should bump generation (got {} after {})",
        gen2,
        gen1
    );

    let members = coord
        .get_group_members("contract-group")
        .await
        .expect("list members");
    assert!(
        members.len() >= 2,
        "group should contain both members, got {:?}",
        members
    );
}

#[tokio::test]
async fn mock_coordinator_satisfies_partition_contract() {
    let coord = MockCoordinator::new(1, "127.0.0.1", 9092);
    assert_partition_ownership_contract(&coord, "mock-contract-topic").await;
}

#[tokio::test]
async fn mock_coordinator_satisfies_group_generation_contract() {
    let coord = MockCoordinator::new(1, "127.0.0.1", 9092);
    assert_group_join_generation_contract(&coord).await;
}

#[tokio::test]
async fn raft_coordinator_satisfies_partition_contract() {
    let port = next_port();
    let config = raft_test_config(1, port);
    let store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;

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

    assert_partition_ownership_contract(&coordinator, "raft-contract-topic").await;

    coordinator.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn raft_coordinator_satisfies_group_generation_contract() {
    let port = next_port();
    let config = raft_test_config(2, port);
    let store = Arc::new(InMemory::new()) as Arc<dyn object_store::ObjectStore>;

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

    assert_group_join_generation_contract(&coordinator).await;

    coordinator.shutdown().await.expect("shutdown");
}
