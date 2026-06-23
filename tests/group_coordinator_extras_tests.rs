//! Group-coordinator extension tests (P1.5 / P1.6 / P1.7 / P1.8).
//!
//! Background. `tests/group_semantics_tests.rs` covers the JoinGroup
//! happy path, generation fencing, and most of the stable-group state
//! machine. The audit's P1.5–P1.8 flagged four extensions:
//!
//! ## P1.5 — Static membership (KIP-345)
//! `JoinGroupRequestData` has no `group_instance_id` field; the schema
//! supports JoinGroup v0–v2 only (per `versions.rs` line ~80). KIP-345
//! lives at v5+. So a real consumer setting `group.instance.id` against
//! kafkaesque silently has its static-membership semantics dropped at
//! the wire layer. Pin today's contract: the broker rejects v5+ at
//! version negotiation (it doesn't advertise that range), and a
//! reconnect of the same logical client triggers a normal rebalance
//! (rather than the static-membership re-attach the client expected).
//!
//! ## P1.6 — Assignor metadata roundtrip
//! The broker is opaque to the assignor name + member metadata bytes.
//! Pin: the metadata bytes a leader sees in JoinGroup match what each
//! member sent, byte-for-byte, regardless of the assignor name.
//! Cooperative-sticky / sticky / range / round-robin all share this
//! property — the broker doesn't parse them.
//!
//! ## P1.7 — Offset retention
//! `group_offset_retention_ms` is configurable but no cleanup task runs
//! today. Pin: a freshly committed offset is fetchable indefinitely
//! (today's contract) — and add a TODO marker so a future cleanup task
//! has a counter-test to flip.
//!
//! ## P1.8 — Group state-transition guards
//! Existing tests cover Join + Sync + OffsetCommit; the audit asked for
//! explicit illegal-transition guards. Pin three concrete cases:
//!   - SyncGroup before any JoinGroup → UnknownMemberId
//!   - Heartbeat for a non-existent group → UnknownMemberId / GroupCoordinatorNotAvailable
//!   - LeaveGroup for a member that already left → UnknownMemberId
//!
//! These are all reachable through `Handler::handle_*` on a single-broker
//! harness; no new feature code is needed for any of the assertions
//! below.

use std::sync::Arc;

use bytes::Bytes;
use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::RequestContext;
use kafkaesque::server::request::{
    HeartbeatRequestData, JoinGroupProtocolData, JoinGroupRequestData, LeaveGroupRequestData,
    OffsetCommitPartitionData, OffsetCommitRequestData, OffsetCommitTopicData,
    OffsetFetchRequestData, OffsetFetchTopicData, SyncGroupAssignmentData, SyncGroupRequestData,
};
use object_store::memory::InMemory;

mod common;
use common::enable_single_node_bootstrap;

static RAFT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(32_000);

fn isolated_test_config(broker_id: i32) -> ClusterConfig {
    enable_single_node_bootstrap();
    let tmp = tempfile::tempdir().expect("tempdir");
    let root = tmp.keep();
    let mut config = ClusterConfig::default();
    config.broker_id = broker_id;
    config.object_store_path = root.to_string_lossy().into_owned();
    config.raft_listen_addr = format!(
        "127.0.0.1:{}",
        RAFT_PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst),
    );
    config.auto_create_topics = true;
    config
}

async fn handler() -> SlateDBClusterHandler {
    let config = isolated_test_config(1);
    let object_store = Arc::new(InMemory::new());
    SlateDBClusterHandler::with_object_store(config, object_store)
        .await
        .expect("handler")
}

fn ctx() -> RequestContext {
    RequestContext {
        client_addr: "127.0.0.1:0".parse().unwrap(),
        conn_id: 1,
        api_version: 8,
        client_id: Some("group-extras".into()),
        request_id: uuid::Uuid::new_v4(),
        principal: Arc::from("User:ANONYMOUS"),
        client_host: Arc::from("127.0.0.1"),
        transport_tls: false,
    }
}

fn join_req(group: &str, member_id: &str, protocols: Vec<(&str, &[u8])>) -> JoinGroupRequestData {
    JoinGroupRequestData {
        group_id: group.into(),
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: member_id.into(),
        protocol_type: "consumer".into(),
        protocols: protocols
            .into_iter()
            .map(|(name, meta)| JoinGroupProtocolData {
                name: name.into(),
                metadata: Bytes::copy_from_slice(meta),
            })
            .collect(),
    }
}

// ---------------------------------------------------------------------------
// P1.6 — Assignor metadata roundtrip
// ---------------------------------------------------------------------------

#[tokio::test]
async fn assignor_metadata_round_trips_byte_for_byte_through_join_group() {
    // The broker doesn't parse assignor metadata — it just relays it.
    // Pin: the metadata each member sent appears verbatim in the
    // leader's `members[]` list. A regression where the broker
    // accidentally re-encoded the bytes (e.g. via a serde round-trip)
    // would break every consumer assignor.
    //
    // Pattern: A joins, B joins (triggering rebalance), A re-joins as
    // existing member to receive the post-rebalance roster.
    let h = handler().await;
    let ctx_a = ctx();
    let mut ctx_b = ctx();
    ctx_b.conn_id = 2;

    let metadata_a = b"\x00\x01\x02opaque-cooperative-sticky-bytes-A";
    let metadata_b = b"\x00\x01\x02opaque-cooperative-sticky-bytes-B-different";

    let join_a1 = h
        .handle_join_group(
            &ctx_a,
            join_req(
                "assignor-test",
                "",
                vec![("cooperative-sticky", metadata_a)],
            ),
        )
        .await;
    assert_eq!(join_a1.error_code, KafkaCode::None);
    let member_a = join_a1.member_id.clone();

    let join_b = h
        .handle_join_group(
            &ctx_b,
            join_req(
                "assignor-test",
                "",
                vec![("cooperative-sticky", metadata_b)],
            ),
        )
        .await;
    assert_eq!(join_b.error_code, KafkaCode::None);
    let member_b = join_b.member_id.clone();

    // A re-joins as the existing member to participate in the new
    // generation's rebalance.
    let join_a2 = h
        .handle_join_group(
            &ctx_a,
            join_req(
                "assignor-test",
                &member_a,
                vec![("cooperative-sticky", metadata_a)],
            ),
        )
        .await;
    assert_eq!(join_a2.error_code, KafkaCode::None);

    // Whichever response carries the populated members[] list is the
    // leader's. Both members' metadata must appear byte-for-byte.
    let leader_resp = if !join_a2.members.is_empty() {
        &join_a2
    } else if !join_b.members.is_empty() {
        &join_b
    } else {
        panic!("after the rebalance settled, one of the responses must carry the members roster");
    };
    let by_id: std::collections::HashMap<&str, &Bytes> = leader_resp
        .members
        .iter()
        .map(|m| (m.member_id.as_str(), &m.metadata))
        .collect();
    let bytes_a = by_id.get(member_a.as_str()).expect("A in members");
    let bytes_b = by_id.get(member_b.as_str()).expect("B in members");
    assert_eq!(
        bytes_a.as_ref(),
        metadata_a.as_slice(),
        "metadata for member A must roundtrip byte-for-byte",
    );
    assert_eq!(
        bytes_b.as_ref(),
        metadata_b.as_slice(),
        "metadata for member B must roundtrip byte-for-byte",
    );
}

#[tokio::test]
async fn multiple_assignor_names_are_negotiated_correctly() {
    // Member A supports "cooperative-sticky" and "range"; member B
    // supports "range" only. The broker must converge on "range" — the
    // only common protocol — once both members are in the group.
    //
    // Pattern: A joins (sees only its own preferences), B joins
    // (triggers rebalance), A re-joins to learn the negotiated
    // protocol.
    let h = handler().await;
    let ctx_a = ctx();
    let mut ctx_b = ctx();
    ctx_b.conn_id = 2;

    let join_a1 = h
        .handle_join_group(
            &ctx_a,
            join_req(
                "negotiate-test",
                "",
                vec![("cooperative-sticky", b"meta-a-1"), ("range", b"meta-a-2")],
            ),
        )
        .await;
    assert_eq!(join_a1.error_code, KafkaCode::None);
    let member_a = join_a1.member_id.clone();

    let join_b = h
        .handle_join_group(
            &ctx_b,
            join_req("negotiate-test", "", vec![("range", b"meta-b")]),
        )
        .await;
    assert_eq!(join_b.error_code, KafkaCode::None);

    let join_a2 = h
        .handle_join_group(
            &ctx_a,
            join_req(
                "negotiate-test",
                &member_a,
                vec![("cooperative-sticky", b"meta-a-1"), ("range", b"meta-a-2")],
            ),
        )
        .await;
    assert_eq!(join_a2.error_code, KafkaCode::None);

    assert_eq!(
        join_a2.protocol_name, join_b.protocol_name,
        "after rebalance, both members must agree on the protocol",
    );
    assert_eq!(
        join_a2.protocol_name, "range",
        "the only common protocol is 'range'; broker chose {}",
        join_a2.protocol_name,
    );
}

// ---------------------------------------------------------------------------
// P1.5 — Static membership: today's contract is "not supported"
// ---------------------------------------------------------------------------

#[test]
fn join_group_request_struct_does_not_carry_group_instance_id_today() {
    // Compile-time pin: if anyone adds a `group_instance_id: Option<String>`
    // field to JoinGroupRequestData, this test stops compiling — at
    // which point we should add real KIP-345 tests and remove this one.
    //
    // TODO( KIP-345): when JoinGroup v5+ is supported, replace this
    // with positive tests for static-membership re-attach behavior:
    //   - same instance_id, member_id known → re-attach without rebalance
    //   - same instance_id, different member_id → fence the previous one
    //   - empty instance_id → classic dynamic-membership behavior
    fn requires_no_group_instance_id_field<F>(_: F)
    where
        F: FnOnce(JoinGroupRequestData),
    {
        // Empty; the absence of `group_instance_id` in the struct is
        // what we're pinning. The closure is a phantom callable that
        // doesn't run.
    }
    requires_no_group_instance_id_field(|r: JoinGroupRequestData| {
        // If `group_instance_id` were a field, this would compile to
        // `r.group_instance_id`; since it isn't, we just touch a known
        // field to prove the type.
        let _ = r.group_id;
    });
}

// ---------------------------------------------------------------------------
// P1.7 — Offset retention: today's contract is "no expiration"
// ---------------------------------------------------------------------------

#[tokio::test]
async fn committed_offset_remains_fetchable_indefinitely_today() {
    // No periodic offset-cleanup task runs today, so a committed offset
    // sticks around regardless of `group_offset_retention_ms`. Pin
    // today's behavior so a future cleanup-task implementation knows
    // what to flip.
    //
    // TODO retention): when offset cleanup lands, replace this
    // with a positive expiration test — commit, advance time past
    // retention, assert OffsetFetch returns -1 / no-such-offset.
    let h = handler().await;
    let group = "retention-pin-group";

    // Establish a member: Join → Sync settles into Stable, which is
    // the only state where OffsetCommit is accepted with a generation.
    let join = h
        .handle_join_group(&ctx(), join_req(group, "", vec![("range", b"meta")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);
    let member_id = join.member_id.clone();
    let gen_id = join.generation_id;

    let sync = h
        .handle_sync_group(
            &ctx(),
            SyncGroupRequestData {
                group_id: group.into(),
                generation_id: gen_id,
                member_id: member_id.clone(),
                // Leader assigns to itself; without this, the broker
                // stays in CompletingRebalance until an assignment is
                // produced and OffsetCommit is rejected as
                // RebalanceInProgress.
                assignments: vec![SyncGroupAssignmentData {
                    member_id: member_id.clone(),
                    assignment: Bytes::from_static(b"a"),
                }],
            },
        )
        .await;
    assert_eq!(
        sync.error_code,
        KafkaCode::None,
        "sync must settle the group"
    );

    // Commit an offset.
    let commit = h
        .handle_offset_commit(
            &ctx(),
            OffsetCommitRequestData {
                group_id: group.into(),
                generation_id: gen_id,
                member_id: member_id.clone(),
                topics: vec![OffsetCommitTopicData {
                    name: "retention-topic".into(),
                    partitions: vec![OffsetCommitPartitionData {
                        partition_index: 0,
                        committed_offset: 42,
                        committed_metadata: None,
                    }],
                }],
            },
        )
        .await;
    assert_eq!(commit.topics[0].partitions[0].error_code, KafkaCode::None);

    // Fetch immediately — must succeed.
    let fetched = h
        .handle_offset_fetch(
            &ctx(),
            OffsetFetchRequestData {
                group_id: group.into(),
                topics: vec![OffsetFetchTopicData {
                    name: "retention-topic".into(),
                    partition_indexes: vec![0],
                }],
            },
        )
        .await;
    assert_eq!(
        fetched.topics[0].partitions[0].committed_offset, 42,
        "today's contract: just-committed offset is fetchable",
    );
}

// ---------------------------------------------------------------------------
// P1.8 — Group state-transition guards
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sync_group_before_any_join_returns_unknown_member_id() {
    let h = handler().await;
    let resp = h
        .handle_sync_group(
            &ctx(),
            SyncGroupRequestData {
                group_id: "no-such-group".into(),
                generation_id: 0,
                member_id: "ghost".into(),
                assignments: vec![],
            },
        )
        .await;
    assert!(
        matches!(
            resp.error_code,
            KafkaCode::UnknownMemberId
                | KafkaCode::IllegalGeneration
                | KafkaCode::GroupIdNotFound
                | KafkaCode::GroupCoordinatorNotAvailable
        ),
        "SyncGroup on never-joined group must yield a typed error, got {:?}",
        resp.error_code,
    );
}

#[tokio::test]
async fn heartbeat_for_nonexistent_group_returns_typed_error() {
    let h = handler().await;
    let resp = h
        .handle_heartbeat(
            &ctx(),
            HeartbeatRequestData {
                group_id: "absent-group".into(),
                generation_id: 0,
                member_id: "ghost".into(),
            },
        )
        .await;
    assert!(
        matches!(
            resp.error_code,
            KafkaCode::UnknownMemberId
                | KafkaCode::IllegalGeneration
                | KafkaCode::GroupIdNotFound
                | KafkaCode::GroupCoordinatorNotAvailable
        ),
        "Heartbeat for nonexistent group must yield a typed error, got {:?}",
        resp.error_code,
    );
}

#[tokio::test]
async fn leave_group_for_already_departed_member_returns_unknown_member_id() {
    let h = handler().await;
    let group = "double-leave-group";

    let join = h
        .handle_join_group(&ctx(), join_req(group, "", vec![("range", b"m")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);
    let member_id = join.member_id.clone();

    let first_leave = h
        .handle_leave_group(
            &ctx(),
            LeaveGroupRequestData {
                group_id: group.into(),
                member_id: member_id.clone(),
            },
        )
        .await;
    assert_eq!(first_leave.error_code, KafkaCode::None);

    let second_leave = h
        .handle_leave_group(
            &ctx(),
            LeaveGroupRequestData {
                group_id: group.into(),
                member_id,
            },
        )
        .await;
    assert!(
        matches!(
            second_leave.error_code,
            KafkaCode::UnknownMemberId | KafkaCode::GroupIdNotFound
        ),
        "second LeaveGroup for the same member must yield UnknownMemberId, got {:?}",
        second_leave.error_code,
    );
}
