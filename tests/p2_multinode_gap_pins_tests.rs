//! Multi-node Raft contract tests.
//!
//! Background. The audit's P2 items split into two halves:
//!
//! - **Single-node testable**: P2.4 (coordinator failover hooks) and
//!   P2.6 (periodic-task lifecycle) — dedicated test files.
//! - **Requires multi-node infrastructure**: P2.1 (split-vote / election
//!   uniqueness), P2.2 (snapshot install during replication), P2.3
//!   (voter-set changes), P2.5 (network-partition chaos).
//!
//! `tests/common/raft_multinode.rs` now provides that infrastructure.
//! The tests below exercise the properties that are reachable today
//! against a real N-node `RaftCoordinator` cluster. Remaining depth
//! (pre-vote disruption prevention, lagging-follower snapshot install
//! under concurrent leadership change, asymmetric partitions) still
//! needs tighter fault-injection hooks and is called out inline where
//! relevant — but the "no harness exists" gap is closed.

mod common;
use std::time::Duration;

use common::{MultiNodeRaft, build_single_node_raft};
use kafkaesque::cluster::raft::ControlCommand;
use kafkaesque::server::request::ApiKey;
use tokio::time::sleep;

// ---------------------------------------------------------------------------
// P2.1 — Election uniqueness
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_election_is_unconditional_no_split_possible() {
    // Sanity baseline: with one voter, election always succeeds with
    // that voter as leader. A regression where openraft's leader
    // election started requiring N-of-N votes would break single-node
    // bootstrap silently.
    let coord = build_single_node_raft().await;
    assert!(coord.is_leader().await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_node_cluster_elects_exactly_one_agreed_leader() {
    // P2.1 — election uniqueness. A 3-node cluster must converge on a
    // single control-group leader visible to every node. Split-brain
    // (two nodes claiming leadership, or divergent leader views that
    // never heal) fails this test.
    let cluster = MultiNodeRaft::spawn(3).await;
    let leader = cluster
        .agreed_leader()
        .await
        .expect("spawn() already waited for agreement");

    let mut leaders_claiming = 0usize;
    for node in &cluster.nodes {
        if node.is_leader().await {
            leaders_claiming += 1;
            assert_eq!(node.cluster().node_id(), leader);
        }
    }
    assert_eq!(
        leaders_claiming, 1,
        "exactly one node must claim control leadership; leader_id={leader}"
    );

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Join path (harness + mux JoinCluster / PromoteMember fan-out)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn second_node_joins_via_join_cluster_and_reaches_voter() {
    // Pins the joiner-driven bootstrap: `RaftCoordinator::join_cluster`
    // fans JoinCluster + PromoteMember across control and every shard,
    // and the resulting 2-node cluster agrees on a leader and accepts
    // writes from either side.
    let cluster = MultiNodeRaft::spawn_via_join(2).await;

    let leader = cluster.agreed_leader().await.expect("agreed leader");
    assert!(leader == 1 || leader == 2);

    // Write through the follower — must forward to the leader.
    let follower = cluster.follower_index().await;
    cluster
        .write_noop(follower)
        .await
        .expect("noop via follower must succeed through forward path");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// P2.3 — Voter-set / membership changes
// ---------------------------------------------------------------------------

#[test]
fn change_membership_is_internal_only_no_kafka_rpc_today() {
    // `RaftCluster::change_membership_all_groups` exists but is NOT
    // exposed via any Kafka RPC. Operators can't add/remove voters from
    // outside the broker process. This is intentional today (the cluster
    // is statically configured via `RAFT_PEERS`).
    let from_45 = ApiKey::try_from(45i16).expect("forward-compat: unknown maps to Unknown(_)");
    let dbg = format!("{:?}", from_45);
    assert!(
        dbg.contains("Unknown"),
        "AlterPartitionReassignments (key 45) must remain unknown until \
         multi-node membership changes are exposed; got {:?}",
        from_45,
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn change_membership_promotes_third_voter_and_preserves_quorum_writes() {
    // P2.3 — on a live 3-node cluster, shrink the voter set to {1,2},
    // confirm the reduced quorum still elects and commits, then grow
    // back to {1,2,3}.
    let cluster = MultiNodeRaft::spawn(3).await;
    let leader_idx = cluster.leader_index().await.expect("leader");

    cluster.nodes[leader_idx]
        .cluster()
        .change_membership_all_groups([1u64, 2u64])
        .await
        .expect("shrink voter set to {1,2}");

    // Re-find a leader among the remaining voters.
    let start = std::time::Instant::now();
    let leader_after_shrink = loop {
        if let Some(id) = cluster.agreed_leader().await
            && (id == 1 || id == 2)
        {
            break id;
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!("no leader among {{1,2}} after shrink");
        }
        sleep(Duration::from_millis(50)).await;
    };
    let leader_idx = cluster
        .nodes
        .iter()
        .position(|n| n.cluster().node_id() == leader_after_shrink)
        .expect("leader node present");

    cluster
        .write_noop(leader_idx)
        .await
        .expect("2-voter quorum must still commit");

    cluster.nodes[leader_idx]
        .cluster()
        .add_learner_all_groups(3, cluster.addrs[2].clone())
        .await
        .expect("re-add node 3 as learner");
    // Ensure node 3 can dial the current voters (heal any stale book).
    cluster.heal().await;
    cluster.nodes[leader_idx]
        .cluster()
        .change_membership_all_groups([1u64, 2u64, 3u64])
        .await
        .expect("grow voter set back to {1,2,3}");

    common::wait_for_agreed_leader(&cluster.nodes, Duration::from_secs(15)).await;
    cluster
        .write_noop(0)
        .await
        .expect("noop after membership round-trip");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// P2.5 — Network partition (address-book isolation)
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn minority_partition_cannot_commit_majority_continues() {
    // P2.5 — isolate one node of three. The majority partition must keep
    // committing; the isolated minority must not. Heal and confirm the
    // cluster reconverges.
    let cluster = MultiNodeRaft::spawn(3).await;
    let leader_before = cluster.agreed_leader().await.expect("leader");

    // Isolate a follower when possible so we don't force an election
    // before asserting majority progress; fall back to isolating anyone.
    let isolate_idx = match cluster.leader_index().await {
        Some(li) => (0..3).find(|&i| i != li).unwrap_or(0),
        None => 0,
    };
    let isolated_id = cluster.nodes[isolate_idx].cluster().node_id();
    cluster.isolate(isolate_idx).await;

    // Majority (the other two) must still accept a write. Prefer writing
    // through a non-isolated node.
    let majority_idx = (0..3).find(|&i| i != isolate_idx).expect("majority node");
    let mut majority_ok = false;
    let start = std::time::Instant::now();
    while start.elapsed() < Duration::from_secs(10) {
        if cluster.write_noop(majority_idx).await.is_ok() {
            majority_ok = true;
            break;
        }
        sleep(Duration::from_millis(100)).await;
    }
    assert!(
        majority_ok,
        "majority partition must commit after isolating node {isolated_id} \
         (leader_before={leader_before})"
    );

    // Minority alone must not be able to commit. Use a short timeout by
    // racing the write against a 2s sleep — a hung propose is treated as
    // "did not commit", which is the safety property we want.
    let minority = cluster.nodes[isolate_idx].clone();
    let minority_commit = tokio::time::timeout(Duration::from_secs(2), async move {
        minority.cluster().write_control(ControlCommand::Noop).await
    })
    .await;
    assert!(
        !matches!(minority_commit, Ok(Ok(_))),
        "isolated minority must not commit a control write; got {minority_commit:?}"
    );

    // Heal and reconverge.
    cluster.heal().await;
    common::wait_for_agreed_leader(&cluster.nodes, Duration::from_secs(15)).await;
    cluster
        .write_noop(isolate_idx)
        .await
        .expect("healed node must accept writes again");

    cluster.shutdown().await;
}

// ---------------------------------------------------------------------------
// Multi-node harness existence
// ---------------------------------------------------------------------------

#[test]
fn multi_node_raft_helpers_are_exported() {
    // Compile-time pin: the harness the P2 docs asked for is reachable
    // from `tests/common`. Renaming these without updating the P2 suite
    // will fail to compile here.
    let _: fn(usize) -> _ = |n| async move { MultiNodeRaft::spawn(n).await };
    let _: fn(usize) -> _ = |n| async move { MultiNodeRaft::spawn_via_join(n).await };
}
