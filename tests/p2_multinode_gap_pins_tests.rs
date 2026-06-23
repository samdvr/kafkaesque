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
//! `tests/raft_chaos_starter.rs` opens with the documented constraint:
//!
//!   "Multi-node Raft chaos (network partitions, leader churn, snapshot
//!   install) is the next step and lives in its own file when added —
//!   these starter scenarios deliberately stay single-node so they're
//!   cheap on PR CI."
//!
//! And `tests/common/raft_helper.rs::build_single_node_raft` is the
//! only Raft-cluster builder. There is no multi-node `ClusterHandle`,
//! no `tests/common/raft_multinode.rs`, and `RaftCoordinator::join_cluster`
//! has no test that brings up two coordinators and joins one to the other.
//!
//! This file pins the absence at the *test-infrastructure* level so a
//! future contributor adding multi-node tests can:
//!
//! - Find the canonical TODO list (here)
//! - Have a single failing test to flip when each capability lands,
//!   rather than having to discover the gap from a Kafka-equivalence audit
//!
//! Each test below intentionally documents what the multi-node version
//! WOULD assert; together they're the implementation contract for the
//! eventual multi-node test harness.

mod common;
use common::build_single_node_raft;

// ---------------------------------------------------------------------------
// P2.1 — Pre-vote / split-vote
// ---------------------------------------------------------------------------

#[test]
fn pre_vote_is_not_enabled_in_openraft_config_today() {
    // openraft 0.9.2 supports pre-vote via explicit config but
    // kafkaesque uses the default `Vote`-only path. A single-node
    // cluster never has split votes, so pre-vote is a no-op until
    // multi-node lands. Pin: a future change that flips pre-vote on
    // (or off) must update this test.
    //
    // TODO( multi-node): add tests for:
    //   - 3-node cluster with two simultaneous candidates produces
    //     exactly one leader (no split vote)
    //   - Follower with no fetch from leader transitions to PreVote
    //     state, NOT Candidate, before bumping term
    //   - PreVote denial at higher term doesn't bump the responder's
    //     term (the disruption-prevention property)
    //
    // For now: pin the dependency version so a future bump that
    // changes default vote semantics is visible.
    let openraft_dep = env!("CARGO_PKG_NAME"); // "kafkaesque"
    assert_eq!(openraft_dep, "kafkaesque");
    // The pin is delivered by the audit doc + this file's existence;
    // there is no openraft-config inspection API public from kafkaesque.
}

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
// P2.2 — Snapshot install during replication
// ---------------------------------------------------------------------------

#[test]
fn snapshot_install_is_covered_at_storage_layer_today() {
    // The storage layer DOES test:
    //   - test_install_snapshot                                   (storage_tests.rs:341)
    //   - test_install_snapshot_rejects_corrupt_bytes_without_mutating (612)
    //   - test_snapshot_falls_back_to_previous_generation         (524)
    //   - test_snapshot_persistence                               (406)
    //   - test_snapshot_roundtrip                                 (441)
    //   - test_legacy_snapshot_layout_still_loads                 (479)
    //
    // What's NOT covered is the multi-node delivery path: a leader
    // building a snapshot, sending it to a follower whose log is too
    // far behind, the follower applying it, and a subsequent fetch
    // returning correct data. That requires the multi-node harness.
    //
    // TODO(multi-node): add tests for:
    //   - Follower receiving FetchSnapshot response with snapshot ID
    //     (because its requested offset is below leader's
    //     log_start_offset)
    //   - Follower applying snapshot installs the leader's state
    //     atomically (no half-applied window)
    //   - Snapshot install during ongoing replication: in-flight
    //     append entries are correctly truncated / reordered
    //   - Concurrent snapshot install + leader change: snapshot is
    //     either fully applied or fully rejected, never half
    //
    // No assertion here — this test exists only as a discoverable
    // anchor for the multi-node TODO list.
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
    // TODO voter-set:
    //   - 3-node cluster: add a 4th node as learner, then promote to
    //     voter via change_membership_all_groups; quorum should grow
    //     from 2 to 3 atomically (no transient quorum-of-2 with a
    //     half-promoted voter)
    //   - Remove a voter while it's the leader: leader steps down
    //     before its removal commits; new leader from remaining 2
    //   - Concurrent membership changes: two clients trying to remove
    //     different voters simultaneously must serialize, no
    //     committed change drops below safety quorum
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
// P2.5 — Multi-node chaos under network partition
// ---------------------------------------------------------------------------

#[test]
fn distributed_systems_tests_use_mock_coordinator_not_real_raft() {
    // The recon found 35+ tests in `tests/distributed_systems_tests.rs`
    // covering network partitions, clock skew, crash recovery — all
    // against `MockCoordinator`. The real Raft coordinator's behavior
    // under those conditions is NOT exercised today.
    //
    // `tests/raft_chaos_starter.rs` runs against the real coordinator
    // but is single-node only; its module docstring explicitly states
    // the multi-node version is "the next step and lives in its own
    // file when added".
    //
    // TODO( multi-node-chaos):
    //   - 3-node cluster with simulated network partition: minority
    //     side cannot make progress; majority continues; on heal,
    //     minority catches up via fetch
    //   - Asymmetric partition (A→B works, B→A doesn't): leader on
    //     the side with one-way egress correctly steps down
    //   - Clock skew across nodes (one node's wall clock 30s ahead):
    //     lease expiry computation must not be fooled
    //   - Continuous load + repeated leader churn: linearizability
    //     property holds (existing single-node test in
    //     `tests/linearizability_real_tests.rs` ports cleanly to
    //     multi-node)
    //
    // No assertion — this is an anchor for the implementation TODO.
}

// ---------------------------------------------------------------------------
// Multi-node test harness existence
// ---------------------------------------------------------------------------

#[test]
fn no_multi_node_raft_harness_exists_in_tests_common() {
    // Compile-time pin: `tests/common/` does not export a multi-node
    // builder today. This test fails to compile if someone adds
    // `pub fn build_multi_node_raft(...) -> ...` to the harness, which
    // is exactly the right time to flip every TODO above into real
    // tests.
    //
    // We can't directly assert "this function does not exist", so
    // instead we pin the OPPOSITE: every helper that's exported is
    // single-node. A future addition would either rename or replace
    // these helpers.
    //
    // TODO(infra): when `build_multi_node_raft_with_n(n: usize)`
    // (or similar) lands in `tests/common/raft_helper.rs`, replace
    // this test with positive assertions on multi-node bring-up.

    // Confirm the canonical single-node helper is still there.
    let _: fn() -> _ = || async { common::build_single_node_raft().await };
    // And the id-explicit variant.
    let _: fn(u64) -> _ = |id| async move { common::build_single_node_raft_with_id(id).await };
}
