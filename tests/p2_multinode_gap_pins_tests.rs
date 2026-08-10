//! Multi-node Raft contract pins
//!
//! Background. The audit's P2 items split into two halves:
//!
//! - **Single-node testable today**: P2.4 (coordinator failover hooks)
//!   and P2.6 (periodic-task lifecycle). Those have dedicated test
//!   files.
//! - **Requires multi-node infrastructure that doesn't yet exist**:
//!   P2.1 (split-vote / pre-vote), P2.2 (snapshot install during
//!   replication — storage-layer mechanics ARE covered in
//!   `src/cluster/raft/storage_tests.rs`, but the multi-node delivery
//!   path is not), P2.3 (voter-set changes), and P2.5 (network-partition
//!   chaos against a real Raft cluster).
//!
//! # What IS proven about multi-node today, and by what
//!
//! `scripts/run-cluster-e2e.sh` brings up three real broker processes and
//! passes: cluster formation (all three join one Raft cluster), cross-broker
//! produce/consume, multi-partition topics, consumer groups, 1000-message
//! throughput, **broker failover** (kill a broker, produce, consume from the
//! survivors, restart it, consume from the restarted node), and 500 KB
//! messages. So multi-node bring-up and failover are not unproven — they are
//! proven by a shell script that is not part of `cargo test`, and its
//! `[Multiple Topics]` step is currently failing.
//!
//! Nothing in `cargo test` brings up more than one node. That is the actual
//! gap: no in-process harness, so none of the *adversarial* multi-node
//! properties below (split vote, partition, snapshot delivery, membership
//! change) are exercised anywhere, in CI or out.
//!
//! `tests/common/raft_helper.rs::build_single_node_raft` is the only
//! Raft-cluster builder. There is no multi-node `ClusterHandle`, no
//! `tests/common/raft_multinode.rs`, and `RaftCoordinator::join_cluster` has
//! no test that brings up two coordinators and joins one to the other.
//!
//! # The implementation contract for a multi-node harness
//!
//! This list used to live in the bodies of tests that asserted nothing (or
//! asserted a tautology — one "pre-vote pin" checked that
//! `env!("CARGO_PKG_NAME") == "kafkaesque"`). Those passed unconditionally
//! and reported as coverage, which is worse than an empty file: the count
//! went up and the risk didn't go down. The TODO list is documentation, so it
//! lives here in the docs; the tests below are only the ones that assert
//! something real.
//!
//! When `build_multi_node_raft_with_n(n: usize)` (or similar) lands in
//! `tests/common/raft_helper.rs`, these become real tests:
//!
//! **P2.1 — pre-vote / split vote.** openraft 0.9.2 supports pre-vote via
//! explicit config; kafkaesque uses the default `Vote`-only path, which is a
//! no-op on a single node. Needed: a 3-node cluster where two simultaneous
//! candidates yield exactly one leader; a follower with no leader contact
//! entering PreVote rather than Candidate before bumping term; a PreVote
//! denial at a higher term not bumping the responder's term (the
//! disruption-prevention property).
//!
//! **P2.2 — snapshot install during replication.** The storage layer covers
//! `test_install_snapshot`,
//! `test_install_snapshot_rejects_corrupt_bytes_without_mutating`,
//! `test_snapshot_falls_back_to_previous_generation`,
//! `test_snapshot_persistence`, `test_snapshot_roundtrip` and
//! `test_legacy_snapshot_layout_still_loads` (all in
//! `src/cluster/raft/storage_tests.rs`). Not covered: the delivery path — a
//! leader building a snapshot, shipping it to a follower whose log is too far
//! behind, the follower applying it atomically (no half-applied window), and
//! a later fetch returning correct data; in-flight append-entries correctly
//! truncated/reordered across the install; and a concurrent snapshot install
//! plus leader change resolving to fully-applied or fully-rejected.
//!
//! **P2.3 — voter-set / membership changes.** Needed: adding a 4th node as
//! learner then promoting it, with quorum growing 2→3 atomically; removing
//! the leader, which must step down before its own removal commits;
//! concurrent removals serializing without any committed change dropping
//! below safety quorum. See also the pinned assertion below.
//!
//! **P2.5 — chaos under network partition.** The 35+ scenarios in
//! `tests/distributed_systems_tests.rs` (network partitions, clock skew,
//! crash recovery) all run against `MockCoordinator`, not real Raft;
//! `tests/raft_chaos_starter.rs` runs against the real coordinator but
//! single-node only. Needed: a 3-node partition where the minority cannot
//! make progress and catches up on heal; an asymmetric partition (A→B works,
//! B→A does not) where the half-reachable leader steps down; 30s clock skew
//! on one node not fooling lease expiry; and continuous load plus repeated
//! leader churn preserving the linearizability property that
//! `tests/linearizability_real_tests.rs` checks on one node.

mod common;
use common::build_single_node_raft;

// ---------------------------------------------------------------------------
// P2.1 — Single-node election baseline
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_node_election_is_unconditional_no_split_possible() {
    // Sanity baseline: with one voter, election always succeeds with
    // that voter as leader. A regression where openraft's leader
    // election started requiring N-of-N votes (a "no split" guarantee
    // taken too far) would break single-node bootstrap silently.
    let coord = build_single_node_raft().await;
    assert!(coord.is_leader().await);
}

// ---------------------------------------------------------------------------
// P2.3 — Voter-set / membership changes
// ---------------------------------------------------------------------------

#[test]
fn change_membership_is_internal_only_no_kafka_rpc_today() {
    // `RaftCluster::change_membership_all_groups` exists at
    // `src/cluster/raft/cluster.rs` but is NOT exposed via any Kafka
    // RPC. Operators can't add/remove voters from outside the broker
    // process. This is intentional today (the cluster is statically
    // configured via `RAFT_PEERS`), but it means a multi-node
    // membership-change test needs to invoke the internal API directly.
    //
    // Pin: change_membership is not in any handler dispatch path.
    use kafkaesque::server::request::ApiKey;
    // No DescribeQuorum / AlterPartitionReassignments / equivalent.
    // The closest standard Kafka RPC (`AlterPartitionReassignments`,
    // key 45) is not in our ApiKey enum at all (per P1.17 contract pin).
    let from_45 = ApiKey::try_from(45i16).expect("forward-compat: unknown maps to Unknown(_)");
    let dbg = format!("{:?}", from_45);
    assert!(
        dbg.contains("Unknown"),
        "AlterPartitionReassignments (key 45) must remain unknown until \
         multi-node membership changes are exposed; got {:?}",
        from_45,
    );
}

// ---------------------------------------------------------------------------
// Multi-node test harness existence
// ---------------------------------------------------------------------------

#[test]
fn only_single_node_raft_helpers_are_exported() {
    // Compile-time pin: `tests/common/` exports single-node builders only.
    // This stops compiling the day someone renames or replaces these with a
    // multi-node builder — exactly the right moment to turn the contract in
    // this file's module docs into real tests.
    let _: fn() -> _ = || async { common::build_single_node_raft().await };
    let _: fn(u64) -> _ = |id| async move { common::build_single_node_raft_with_id(id).await };
}
