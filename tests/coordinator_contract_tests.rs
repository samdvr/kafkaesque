//! Behavioral contract tests shared by MockCoordinator and RaftCoordinator.
//!
//! These guard against drift between the in-memory mock and the replicated
//! Raft implementation — the distributed/linearizability suites exercise the
//! mock, so any semantic mismatch would otherwise go unnoticed.
//!
//! # Trajectory diffs
//!
//! `record_release_acquire_trajectory` is the harness for "diff the same
//! scenario through both implementations": it runs a fixed sequence of
//! coordinator calls and returns a deterministic event vector. Adding a
//! new contract becomes "extend the scenario, run it through both, assert
//! the trajectories match (modulo documented epoch-tracking divergence)."
//! The "release didn't bump epoch" bug that landed in the wild is exactly
//! the kind of drift this catches.

use kafkaesque::cluster::{
    ConsumerGroupCoordinator, MockCoordinator, PartitionCoordinator, SlateDBResult,
};

mod common;
use common::build_single_node_raft;

/// Single observable step in a coordinator trajectory.
#[derive(Debug, Clone, PartialEq, Eq)]
enum CoordinatorEvent {
    OwnsBefore(bool),
    AcquireOk(bool),
    OwnsAfter(bool),
    Owner(Option<i32>),
    EpochAfterAcquire(Option<i32>),
    Released,
    OwnsAfterRelease(bool),
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

/// Run the canonical "register → acquire → release → re-acquire" scenario
/// against any `PartitionCoordinator` and return the trajectory of events.
/// The trajectory is the diff substrate: two coordinators that produce
/// the same trajectory are observationally equivalent for this scenario.
async fn record_release_acquire_trajectory<C: PartitionCoordinator>(
    coord: &C,
    topic: &str,
) -> SlateDBResult<Vec<CoordinatorEvent>> {
    let mut events = Vec::new();
    coord.register_topic(topic, 1).await?;

    events.push(CoordinatorEvent::OwnsBefore(
        coord.owns_partition_for_read(topic, 0).await?,
    ));
    let first_epoch = coord.acquire_partition_with_epoch(topic, 0, 60).await?;
    events.push(CoordinatorEvent::AcquireOk(first_epoch.is_some()));
    events.push(CoordinatorEvent::EpochAfterAcquire(first_epoch));
    events.push(CoordinatorEvent::OwnsAfter(
        coord.owns_partition_for_read(topic, 0).await?,
    ));
    events.push(CoordinatorEvent::Owner(
        coord.get_partition_owner(topic, 0).await?,
    ));

    coord.release_partition(topic, 0).await?;
    events.push(CoordinatorEvent::Released);
    events.push(CoordinatorEvent::OwnsAfterRelease(
        coord.owns_partition_for_read(topic, 0).await?,
    ));

    let second_epoch = coord.acquire_partition_with_epoch(topic, 0, 60).await?;
    events.push(CoordinatorEvent::AcquireOk(second_epoch.is_some()));
    events.push(CoordinatorEvent::EpochAfterAcquire(second_epoch));

    Ok(events)
}

/// Compare two trajectories. Epoch values may differ between
/// implementations — the mock's default impl returns 0 for every acquire,
/// while the real coordinator's epoch advances. Equality is therefore
/// modulo `EpochAfterAcquire`'s inner value, but the *shape* of every
/// event must match. Any non-epoch divergence is a bug.
fn assert_trajectories_agree(mock: &[CoordinatorEvent], raft: &[CoordinatorEvent]) {
    assert_eq!(
        mock.len(),
        raft.len(),
        "trajectory lengths must match: mock={mock:?} raft={raft:?}"
    );
    for (i, (m, r)) in mock.iter().zip(raft.iter()).enumerate() {
        match (m, r) {
            (CoordinatorEvent::EpochAfterAcquire(_), CoordinatorEvent::EpochAfterAcquire(_)) => {
                // Epoch numeric value may legitimately differ. The contract
                // we assert separately is that within the *real* coordinator
                // the epoch monotonically advances across release+re-acquire.
            }
            (a, b) => assert_eq!(
                a, b,
                "trajectories diverge at step {i}: mock={a:?} raft={b:?}"
            ),
        }
    }
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
    let coordinator = build_single_node_raft().await;
    assert_partition_ownership_contract(coordinator.as_ref(), "raft-contract-topic").await;
    coordinator.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn raft_coordinator_satisfies_group_generation_contract() {
    let coordinator = build_single_node_raft().await;
    assert_group_join_generation_contract(coordinator.as_ref()).await;
    coordinator.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn mock_and_raft_release_acquire_trajectories_agree() {
    let mock = MockCoordinator::new(1, "127.0.0.1", 9092);
    let mock_trace = record_release_acquire_trajectory(&mock, "trajectory-topic")
        .await
        .expect("mock trajectory");

    let raft = build_single_node_raft().await;
    let raft_trace = record_release_acquire_trajectory(raft.as_ref(), "trajectory-topic")
        .await
        .expect("raft trajectory");
    raft.shutdown().await.expect("shutdown");

    assert_trajectories_agree(&mock_trace, &raft_trace);
}

#[tokio::test]
async fn raft_release_acquire_advances_leader_epoch() {
    // The "release didn't bump epoch" bug that the audit flagged was caught
    // only after it shipped; this contract pins the invariant. The mock's
    // default impl returns 0 for every acquire, so it's exempt — but any
    // future mock that wants to claim parity must satisfy this assertion.
    let raft = build_single_node_raft().await;
    raft.register_topic("epoch-bump", 1)
        .await
        .expect("register");

    let mut epochs = Vec::new();
    for _ in 0..3 {
        let e = raft
            .acquire_partition_with_epoch("epoch-bump", 0, 60)
            .await
            .expect("acquire")
            .expect("acquired epoch");
        epochs.push(e);
        raft.release_partition("epoch-bump", 0)
            .await
            .expect("release");
    }
    raft.shutdown().await.expect("shutdown");

    let mut max_seen = i32::MIN;
    for e in &epochs {
        assert!(
            *e >= max_seen,
            "epoch regressed across release/re-acquire: epochs={epochs:?}",
        );
        max_seen = (*e).max(max_seen);
    }
    assert!(
        epochs.windows(2).any(|w| w[1] > w[0]),
        "release+re-acquire must bump epoch at least once: {epochs:?}"
    );
}
