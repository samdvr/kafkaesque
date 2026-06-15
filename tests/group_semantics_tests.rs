//! Behavior tests for consumer-group and admin-API semantics.
//!
//! Covers  JoinGroup member metadata, SyncGroup
//! error codes, OffsetCommit generation fencing, LeaveGroup unknown-member
//! handling, protocol negotiation, CreateTopics already-exists detection,
//! and the Metadata controller_id.

#![allow(clippy::field_reassign_with_default)]

use bytes::Bytes;
use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::*;
use kafkaesque::server::{Handler, RequestContext};
use object_store::memory::InMemory;
use std::net::SocketAddr;
use std::sync::Arc;

/// Atomic port counter so each test's Raft RPC listener binds a unique port.
/// Base differs from tests/cluster_handler_tests.rs (21000) since both test
/// binaries can run concurrently in separate processes on one machine.
static RAFT_PORT_COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(23000);

/// Build an isolated `ClusterConfig` for an in-process test handler.
///
/// Mirrors `isolated_test_config` in tests/cluster_handler_tests.rs:
/// - unique local temp dir for `object_store_path` (the Raft WAL/snapshot
///   directories derive from it on the local filesystem),
/// - unique `raft_listen_addr` port (tests run concurrently in one process),
/// - `RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE` so a fresh peerless broker is
///   allowed to bootstrap.
fn isolated_test_config(broker_id: i32) -> ClusterConfig {
    // SAFETY: setting a process-global env var; all tests in this binary
    // want the same value, so concurrent writers are not a hazard.
    unsafe { std::env::set_var("RAFT_BOOTSTRAP_EXPECT_SINGLE_NODE", "true") };

    let tmp = tempfile::tempdir().expect("create test temp dir");
    let root = tmp.keep();
    let raft_port = RAFT_PORT_COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

    let mut config = ClusterConfig::default();
    config.broker_id = broker_id;
    config.object_store_path = root.to_string_lossy().into_owned();
    config.raft_listen_addr = format!("127.0.0.1:{}", raft_port);
    config
}

async fn create_test_handler() -> SlateDBClusterHandler {
    let mut config = isolated_test_config(1);
    config.auto_create_topics = true;

    let object_store = Arc::new(InMemory::new());
    SlateDBClusterHandler::with_object_store(config, object_store)
        .await
        .expect("Failed to create handler")
}

fn create_test_context() -> RequestContext {
    RequestContext {
        client_addr: "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        conn_id: 1,
        api_version: 8,
        client_id: Some("test-client".to_string()),
        request_id: uuid::Uuid::new_v4(),
        principal: Arc::from("User:ANONYMOUS"),
        client_host: Arc::from("127.0.0.1"),
        transport_tls: false,
    }
}

fn join_request(
    group_id: &str,
    member_id: &str,
    protocols: Vec<(&str, &[u8])>,
) -> JoinGroupRequestData {
    JoinGroupRequestData {
        group_id: group_id.to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        member_id: member_id.to_string(),
        protocol_type: "consumer".to_string(),
        protocols: protocols
            .into_iter()
            .map(|(name, metadata)| JoinGroupProtocolData {
                name: name.to_string(),
                metadata: Bytes::copy_from_slice(metadata),
            })
            .collect(),
    }
}

fn commit_request(
    group_id: &str,
    generation_id: i32,
    member_id: &str,
    topic: &str,
    offset: i64,
) -> OffsetCommitRequestData {
    OffsetCommitRequestData {
        group_id: group_id.to_string(),
        generation_id,
        member_id: member_id.to_string(),
        topics: vec![OffsetCommitTopicData {
            name: topic.to_string(),
            partitions: vec![OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: offset,
                committed_metadata: None,
            }],
        }],
    }
}

// ============================================================================
// Finding 1 + 5: JoinGroup per-member metadata and protocol negotiation
// ============================================================================

#[tokio::test]
async fn test_join_group_leader_receives_each_members_own_metadata() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "meta-roundtrip-group";

    // Member A joins first and becomes leader.
    let resp_a = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"meta-A")]))
        .await;
    assert_eq!(resp_a.error_code, KafkaCode::None);
    assert_eq!(resp_a.leader, resp_a.member_id);
    let member_a = resp_a.member_id.clone();

    // Leader's first response: roster contains only A with A's metadata.
    assert_eq!(resp_a.members.len(), 1);
    assert_eq!(resp_a.members[0].member_id, member_a);
    assert_eq!(resp_a.members[0].metadata.as_ref(), b"meta-A");

    // Member B joins with DIFFERENT metadata.
    let resp_b = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"meta-B")]))
        .await;
    assert_eq!(resp_b.error_code, KafkaCode::None);
    let member_b = resp_b.member_id.clone();
    assert_ne!(member_a, member_b);
    // B is a follower: no member roster (Kafka semantics).
    assert_eq!(resp_b.leader, member_a);
    assert!(resp_b.members.is_empty());

    // Leader A rejoins (rebalance after B's join). Its response must carry
    // each member's OWN subscription metadata, not the requester's blob.
    let resp_a2 = handler
        .handle_join_group(
            &ctx,
            join_request(group, &member_a, vec![("range", b"meta-A")]),
        )
        .await;
    assert_eq!(resp_a2.error_code, KafkaCode::None);
    assert_eq!(resp_a2.leader, member_a);
    assert_eq!(resp_a2.members.len(), 2);

    let meta_of = |id: &str| {
        resp_a2
            .members
            .iter()
            .find(|m| m.member_id == id)
            .unwrap_or_else(|| panic!("member {} missing from leader response", id))
            .metadata
            .clone()
    };
    assert_eq!(meta_of(&member_a).as_ref(), b"meta-A");
    assert_eq!(meta_of(&member_b).as_ref(), b"meta-B");
}

#[tokio::test]
async fn test_join_group_negotiates_common_protocol() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "protocol-negotiation-group";

    // Leader supports both protocols, preferring roundrobin.
    let resp_a = handler
        .handle_join_group(
            &ctx,
            join_request(group, "", vec![("roundrobin", b"rr-A"), ("range", b"rg-A")]),
        )
        .await;
    assert_eq!(resp_a.error_code, KafkaCode::None);
    // Single member: the leader's first preference wins.
    assert_eq!(resp_a.protocol_name, "roundrobin");
    let member_a = resp_a.member_id.clone();

    // Second member only supports range: the negotiated protocol must
    // change to the only common protocol.
    let resp_b = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"rg-B")]))
        .await;
    assert_eq!(resp_b.error_code, KafkaCode::None);
    assert_eq!(resp_b.protocol_name, "range");

    // Leader rejoins; its roster must carry each member's metadata FOR THE
    // NEGOTIATED PROTOCOL (range), not for its own first preference.
    let resp_a2 = handler
        .handle_join_group(
            &ctx,
            join_request(
                group,
                &member_a,
                vec![("roundrobin", b"rr-A"), ("range", b"rg-A")],
            ),
        )
        .await;
    assert_eq!(resp_a2.error_code, KafkaCode::None);
    assert_eq!(resp_a2.protocol_name, "range");
    let leader_entry = resp_a2
        .members
        .iter()
        .find(|m| m.member_id == member_a)
        .expect("leader missing from roster");
    assert_eq!(leader_entry.metadata.as_ref(), b"rg-A");
}

#[tokio::test]
async fn test_join_group_inconsistent_protocol_rejected() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "inconsistent-protocol-group";

    let resp_a = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"a")]))
        .await;
    assert_eq!(resp_a.error_code, KafkaCode::None);
    let generation_after_a = resp_a.generation_id;

    // A joiner with no protocol in common must be rejected...
    let resp_b = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("sticky", b"b")]))
        .await;
    assert_eq!(resp_b.error_code, KafkaCode::InconsistentGroupProtocol);

    // ...without disturbing the group: the leader can rejoin and the group
    // still has exactly one member.
    let resp_a2 = handler
        .handle_join_group(
            &ctx,
            join_request(group, &resp_a.member_id, vec![("range", b"a")]),
        )
        .await;
    assert_eq!(resp_a2.error_code, KafkaCode::None);
    assert_eq!(resp_a2.members.len(), 1);
    // The rejected join must not have bumped the generation (the rejoin
    // bumps it exactly once).
    assert_eq!(resp_a2.generation_id, generation_after_a + 1);
}

// ============================================================================
// Finding 2: SyncGroup error codes
// ============================================================================

#[tokio::test]
async fn test_sync_group_wrong_generation_returns_illegal_generation() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "sync-wrong-gen-group";

    let join = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"m")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);

    let sync = handler
        .handle_sync_group(
            &ctx,
            SyncGroupRequestData {
                group_id: group.to_string(),
                generation_id: join.generation_id + 5,
                member_id: join.member_id.clone(),
                assignments: vec![],
            },
        )
        .await;
    assert_eq!(sync.error_code, KafkaCode::IllegalGeneration);
    assert!(sync.assignment.is_empty());
}

#[tokio::test]
async fn test_sync_group_unknown_member_returns_unknown_member_id() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "sync-unknown-member-group";

    let join = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"m")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);

    let sync = handler
        .handle_sync_group(
            &ctx,
            SyncGroupRequestData {
                group_id: group.to_string(),
                generation_id: join.generation_id,
                member_id: "ghost-member".to_string(),
                assignments: vec![],
            },
        )
        .await;
    assert_eq!(sync.error_code, KafkaCode::UnknownMemberId);
    assert!(sync.assignment.is_empty());
}

#[tokio::test]
async fn test_sync_group_success_returns_assignment() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "sync-success-group";

    let join = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"m")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);
    assert_eq!(join.leader, join.member_id);

    let sync = handler
        .handle_sync_group(
            &ctx,
            SyncGroupRequestData {
                group_id: group.to_string(),
                generation_id: join.generation_id,
                member_id: join.member_id.clone(),
                assignments: vec![SyncGroupAssignmentData {
                    member_id: join.member_id.clone(),
                    assignment: Bytes::from_static(b"assignment-bytes"),
                }],
            },
        )
        .await;
    assert_eq!(sync.error_code, KafkaCode::None);
    assert_eq!(sync.assignment.as_ref(), b"assignment-bytes");
}

// ============================================================================
// Finding 3: OffsetCommit generation fencing
// ============================================================================

#[tokio::test]
async fn test_offset_commit_simple_consumer_allowed() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // generation -1 + empty member = simple consumer: allowed, no fencing,
    // even though the group has never been joined.
    let resp = handler
        .handle_offset_commit(
            &ctx,
            commit_request("simple-consumer-group", -1, "", "simple-topic", 42),
        )
        .await;
    assert_eq!(resp.topics[0].partitions[0].error_code, KafkaCode::None);

    // The commit is actually durable.
    let fetch = handler
        .handle_offset_fetch(
            &ctx,
            OffsetFetchRequestData {
                group_id: "simple-consumer-group".to_string(),
                topics: vec![OffsetFetchTopicData {
                    name: "simple-topic".to_string(),
                    partition_indexes: vec![0],
                }],
            },
        )
        .await;
    assert_eq!(fetch.topics[0].partitions[0].committed_offset, 42);
}

#[tokio::test]
async fn test_offset_commit_empty_member_with_generation_is_fenced() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "fence-empty-member-group";

    // Establish a real group with generation >= 1.
    let join = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"m")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);
    assert!(join.generation_id >= 1);

    // An empty member_id claiming a non-negative generation must NOT bypass
    // fencing ( stale consumers could clobber
    // offsets). "" is not a member of the group -> UnknownMemberId.
    let resp = handler
        .handle_offset_commit(
            &ctx,
            commit_request(group, join.generation_id, "", "fenced-topic", 7),
        )
        .await;
    assert_eq!(
        resp.topics[0].partitions[0].error_code,
        KafkaCode::UnknownMemberId
    );

    // Nothing was committed.
    let fetch = handler
        .handle_offset_fetch(
            &ctx,
            OffsetFetchRequestData {
                group_id: group.to_string(),
                topics: vec![OffsetFetchTopicData {
                    name: "fenced-topic".to_string(),
                    partition_indexes: vec![0],
                }],
            },
        )
        .await;
    assert_eq!(fetch.topics[0].partitions[0].committed_offset, -1);
}

#[tokio::test]
async fn test_offset_commit_stale_generation_rejected() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "fence-stale-gen-group";

    // Member A joins at generation g1.
    let join_a = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"a")]))
        .await;
    assert_eq!(join_a.error_code, KafkaCode::None);
    let stale_generation = join_a.generation_id;

    // Member B joins, bumping the generation past g1.
    let join_b = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"b")]))
        .await;
    assert_eq!(join_b.error_code, KafkaCode::None);
    assert!(join_b.generation_id > stale_generation);

    // A commit from A claiming the stale generation must be rejected.
    let resp = handler
        .handle_offset_commit(
            &ctx,
            commit_request(group, stale_generation, &join_a.member_id, "stale-topic", 9),
        )
        .await;
    assert_eq!(
        resp.topics[0].partitions[0].error_code,
        KafkaCode::IllegalGeneration
    );
}

#[tokio::test]
async fn test_offset_commit_unknown_member_rejected() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "fence-unknown-member-group";

    let join = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"m")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);

    let resp = handler
        .handle_offset_commit(
            &ctx,
            commit_request(group, join.generation_id, "ghost-member", "ghost-topic", 3),
        )
        .await;
    assert_eq!(
        resp.topics[0].partitions[0].error_code,
        KafkaCode::UnknownMemberId
    );
}

#[tokio::test]
async fn test_offset_commit_valid_member_succeeds() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "fence-valid-member-group";

    // Join and complete the rebalance so the group is Stable.
    let join = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"m")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);

    let sync = handler
        .handle_sync_group(
            &ctx,
            SyncGroupRequestData {
                group_id: group.to_string(),
                generation_id: join.generation_id,
                member_id: join.member_id.clone(),
                assignments: vec![SyncGroupAssignmentData {
                    member_id: join.member_id.clone(),
                    assignment: Bytes::from_static(b"a"),
                }],
            },
        )
        .await;
    assert_eq!(sync.error_code, KafkaCode::None);

    // A commit from the live member at the current generation succeeds.
    let resp = handler
        .handle_offset_commit(
            &ctx,
            commit_request(
                group,
                join.generation_id,
                &join.member_id,
                "valid-topic",
                11,
            ),
        )
        .await;
    assert_eq!(resp.topics[0].partitions[0].error_code, KafkaCode::None);
}

// ============================================================================
// Finding 4: LeaveGroup unknown member
// ============================================================================

#[tokio::test]
async fn test_leave_group_unknown_member_returns_unknown_member_id() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Group that never existed.
    let resp = handler
        .handle_leave_group(
            &ctx,
            LeaveGroupRequestData {
                group_id: "never-existed-group".to_string(),
                member_id: "ghost".to_string(),
            },
        )
        .await;
    assert_eq!(resp.error_code, KafkaCode::UnknownMemberId);

    // Group exists, member does not.
    let join = handler
        .handle_join_group(
            &ctx,
            join_request("leave-unknown-group", "", vec![("range", b"m")]),
        )
        .await;
    assert_eq!(join.error_code, KafkaCode::None);

    let resp = handler
        .handle_leave_group(
            &ctx,
            LeaveGroupRequestData {
                group_id: "leave-unknown-group".to_string(),
                member_id: "ghost".to_string(),
            },
        )
        .await;
    assert_eq!(resp.error_code, KafkaCode::UnknownMemberId);
}

#[tokio::test]
async fn test_leave_group_known_member_succeeds() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "leave-known-group";

    let join = handler
        .handle_join_group(&ctx, join_request(group, "", vec![("range", b"m")]))
        .await;
    assert_eq!(join.error_code, KafkaCode::None);

    let resp = handler
        .handle_leave_group(
            &ctx,
            LeaveGroupRequestData {
                group_id: group.to_string(),
                member_id: join.member_id.clone(),
            },
        )
        .await;
    assert_eq!(resp.error_code, KafkaCode::None);

    // Leaving a second time: the member is gone now.
    let resp = handler
        .handle_leave_group(
            &ctx,
            LeaveGroupRequestData {
                group_id: group.to_string(),
                member_id: join.member_id,
            },
        )
        .await;
    assert_eq!(resp.error_code, KafkaCode::UnknownMemberId);
}

// ============================================================================
// Finding 5: DescribeGroups reports the negotiated protocol
// ============================================================================

#[tokio::test]
async fn test_describe_groups_reports_negotiated_protocol() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();
    let group = "describe-protocol-group";

    let join = handler
        .handle_join_group(
            &ctx,
            join_request(group, "", vec![("sticky", b"s"), ("range", b"r")]),
        )
        .await;
    assert_eq!(join.error_code, KafkaCode::None);
    assert_eq!(join.protocol_name, "sticky");

    let describe = handler
        .handle_describe_groups(
            &ctx,
            DescribeGroupsRequestData {
                group_ids: vec![group.to_string()],
            },
        )
        .await;
    assert_eq!(describe.groups.len(), 1);
    assert_eq!(describe.groups[0].error_code, KafkaCode::None);
    // Negotiated protocol, not a hardcoded "range".
    assert_eq!(describe.groups[0].protocol_data, "sticky");
}

// ============================================================================
// Finding 6a/6b: CreateTopics already-exists
// ============================================================================

#[tokio::test]
async fn test_create_topics_duplicate_returns_topic_already_exists() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let req = || CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "dup-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
        validate_only: false,
    };

    let first = handler.handle_create_topics(&ctx, req()).await;
    assert_eq!(first.topics[0].error_code, KafkaCode::None);

    let second = handler.handle_create_topics(&ctx, req()).await;
    assert_eq!(second.topics[0].error_code, KafkaCode::TopicAlreadyExists);
    assert!(second.topics[0].error_message.is_some());
}

// ============================================================================
// Finding 6c: Metadata controller_id is the Raft leader
// ============================================================================

#[tokio::test]
async fn test_metadata_reports_raft_leader_as_controller() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Force a Raft write so we know a leader has been elected.
    let create = handler
        .handle_create_topics(
            &ctx,
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: "controller-topic".to_string(),
                    num_partitions: 1,
                    replication_factor: 1,
                }],
                timeout_ms: 5000,
                validate_only: false,
            },
        )
        .await;
    assert_eq!(create.topics[0].error_code, KafkaCode::None);

    let metadata = handler
        .handle_metadata(&ctx, MetadataRequestData { topics: None })
        .await;

    // Single-node cluster: this broker IS the Raft leader, so the
    // controller must be broker 1 (not -1, and derived from Raft state
    // rather than blindly echoing self).
    assert_eq!(metadata.controller_id, 1);
}
