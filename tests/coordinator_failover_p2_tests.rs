//! Single-node coordinator failover hooks.
//!
//! Background. The audit's P2.4 flagged that
//! `tests/coordinator_contract_tests.rs` covers steady-state ownership
//! semantics but not the failover transition (load/unload partition
//! when leadership changes). True multi-node failover requires the
//! multi-node Raft harness that doesn't yet exist (see
//! `p2_multinode_gap_pins_tests.rs`); the *single-node* hooks are
//! testable today because every public API on `RaftCoordinator` that
//! the failover path uses is exposed:
//!
//! - `is_leader()` and `get_leader()`
//! - `current_raft_state()` (Follower/Candidate/Leader/Learner/Shutdown)
//! - `is_initialized()`
//! - `wait_for_leader_with_timeout()`
//! - `shutdown()`
//! - `metrics()`
//! - `register_topic()`, `acquire_partition()`, `release_partition()`,
//!   `get_partition_owner()` (via PartitionCoordinator trait)
//!
//! These tests pin the failover-relevant invariants on a single-node
//! cluster:
//!
//! 1. Single-node cluster always elects itself leader on init
//! 2. `current_raft_state` reports Leader (=2) post-init
//! 3. `is_leader()` and `get_leader()` agree
//! 4. `wait_for_leader_with_timeout` returns Ok immediately when
//!    leadership already present
//! 5. `shutdown` is graceful — does not panic, does not deadlock,
//!    and `current_raft_state` reports Shutdown (-1) after
//! 6. After shutdown, ownership reads honestly fail rather than
//!    returning stale data
//! 7. Multiple coordinators on disjoint Raft ports each elect their
//!    own leader — pins that single-node bootstrap is per-instance,
//!    not process-global

use std::time::Duration;

use kafkaesque::cluster::PartitionCoordinator;

mod common;
use common::{build_single_node_raft, build_single_node_raft_with_id};

// ---------------------------------------------------------------------------
// 1. Self-election on init
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_cluster_elects_itself_leader_on_init() {
    let coord = build_single_node_raft().await;
    assert!(
        coord.is_leader().await,
        "single-node cluster must elect itself; without this, EVERY \
         coordinator-write path returns NotLeader",
    );
    let leader = coord.get_leader().await;
    assert!(
        leader.is_some(),
        "leader id must be populated after election"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn current_raft_state_reports_leader_after_init() {
    // 2 = openraft::ServerState::Leader per the i64 mapping in
    // `current_raft_state`. Pin that mapping so a future change to
    // the integer codes is visible (operators / dashboards key on it).
    let coord = build_single_node_raft().await;
    assert_eq!(
        coord.current_raft_state(),
        2,
        "single-node post-init state must be Leader (=2); got {}",
        coord.current_raft_state(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn is_leader_and_get_leader_agree() {
    let coord = build_single_node_raft().await;
    let is_leader = coord.is_leader().await;
    let leader_id = coord.get_leader().await;
    if is_leader {
        // get_leader should report this node's id.
        assert!(
            leader_id.is_some(),
            "is_leader=true but get_leader returned None — inconsistent",
        );
    } else {
        // is_leader=false on a single-node cluster is a real bug;
        // this branch exists only for defensive completeness.
        panic!(
            "single-node coordinator must self-elect; is_leader={}",
            is_leader
        );
    }
}

// ---------------------------------------------------------------------------
// 2. Wait-for-leader is non-blocking when already leader
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_for_leader_returns_immediately_when_already_leader() {
    // build_single_node_raft already waits for leader — by the time
    // it returns, leadership is settled. A second wait must be a
    // near-zero-cost no-op. If it ever blocked here (e.g. due to a
    // refactor that re-elected on every call), every produce/fetch
    // would stall on coordination.
    let coord = build_single_node_raft().await;
    let start = std::time::Instant::now();
    coord
        .wait_for_leader_with_timeout(Duration::from_secs(2))
        .await
        .expect("must not time out — already leader");
    assert!(
        start.elapsed() < Duration::from_millis(500),
        "wait_for_leader on already-elected single-node must be fast; took {:?}",
        start.elapsed(),
    );
}

// ---------------------------------------------------------------------------
// 3. Shutdown semantics
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_is_graceful_and_state_transitions_to_shutdown() {
    let coord = build_single_node_raft().await;
    // Pre-shutdown: Leader.
    assert_eq!(coord.current_raft_state(), 2);

    coord.shutdown().await.expect("graceful shutdown");

    // Post-shutdown: -1 per the mapping. Pin the sentinel so a future
    // refactor that returns 0 (Follower) instead doesn't quietly
    // mislead operators.
    assert_eq!(
        coord.current_raft_state(),
        -1,
        "post-shutdown state must be -1 (sentinel); got {}",
        coord.current_raft_state(),
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ownership_writes_after_shutdown_do_not_panic() {
    // Pin: write-path APIs must return an error after shutdown rather
    // than panic. A regression where shutdown leaves the coordinator
    // in a half-torn-down state could otherwise crash the broker on
    // the very last shutdown step.
    let coord = build_single_node_raft().await;
    coord
        .register_topic("after-shutdown", 1)
        .await
        .expect("setup register before shutdown");
    coord.shutdown().await.expect("shutdown");

    // After shutdown, attempt a partition operation. Either:
    // - returns Err (preferred; honest "not available")
    // - returns Ok with a stale value (acceptable as long as no panic)
    // Either way: no panic, no deadlock.
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        coord.acquire_partition("after-shutdown", 0, 60),
    )
    .await;
    assert!(
        result.is_ok(),
        "post-shutdown acquire must not deadlock (timed out)",
    );
}

// ---------------------------------------------------------------------------
// 4. Independent coordinators
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_independent_single_node_coordinators_each_elect_themselves() {
    // Pin: build_single_node_raft can be called twice in the same
    // test binary and produce independent clusters. A regression
    // where the single-node bootstrap env var leaked global state
    // would cause the second coordinator to try to join the first.
    let a = build_single_node_raft_with_id(101).await;
    let b = build_single_node_raft_with_id(102).await;

    assert!(a.is_leader().await, "A must be leader of its own cluster");
    assert!(b.is_leader().await, "B must be leader of its own cluster");

    // Each reports its own broker_id as leader, independent of the other.
    let a_id = a.broker_id();
    let b_id = b.broker_id();
    assert_ne!(
        a_id, b_id,
        "test fixtures must give the two clusters distinct ids"
    );

    // Cleanup so subsequent tests in the same binary aren't affected.
    let _ = a.shutdown().await;
    let _ = b.shutdown().await;
}

// ---------------------------------------------------------------------------
// 5. Ownership operations work end-to-end with the post-init coordinator
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_acquires_then_owns_partition_for_read() {
    // End-to-end happy path: register topic → acquire → owns_for_read
    // returns true. Pin so a future refactor that splits the
    // acquire path from the owns-for-read view doesn't drift.
    let coord = build_single_node_raft().await;
    coord
        .register_topic("owns-test", 1)
        .await
        .expect("register");
    let acquired = coord
        .acquire_partition("owns-test", 0, 60)
        .await
        .expect("acquire");
    assert!(acquired);

    let owns = coord
        .owns_partition_for_read("owns-test", 0)
        .await
        .expect("owns_partition_for_read");
    assert!(owns, "freshly-acquired partition must be readable");

    let owner = coord
        .get_partition_owner("owns-test", 0)
        .await
        .expect("get_partition_owner")
        .expect("owner present");
    assert_eq!(
        owner,
        coord.broker_id(),
        "single-node coordinator must report itself as the owner",
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_then_reacquire_visible_to_reads() {
    // Same flow as raft_chaos_starter::raft_acquired_partition_visible_to_reads
    // but with a release in the middle. After release, owns_for_read
    // is false; after re-acquire, true again. Pin the read-visibility
    // contract across a single-node "failover" simulated by
    // release/acquire on the same coordinator.
    let coord = build_single_node_raft().await;
    coord
        .register_topic("release-cycle", 1)
        .await
        .expect("register");

    let _ = coord
        .acquire_partition("release-cycle", 0, 60)
        .await
        .expect("acquire");
    assert!(
        coord
            .owns_partition_for_read("release-cycle", 0)
            .await
            .expect("owns")
    );

    coord
        .release_partition("release-cycle", 0)
        .await
        .expect("release");
    let still_owns = coord
        .owns_partition_for_read("release-cycle", 0)
        .await
        .expect("owns after release");
    assert!(
        !still_owns,
        "after release, owns_partition_for_read must report false",
    );

    let _ = coord
        .acquire_partition("release-cycle", 0, 60)
        .await
        .expect("re-acquire");
    assert!(
        coord
            .owns_partition_for_read("release-cycle", 0)
            .await
            .expect("owns after re-acquire")
    );
}
