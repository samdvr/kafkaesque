//! ACL pattern-matching enumeration tests + per-API authz matrix (P1.13).
//!
//! Background. `tests/acl_evaluator_props.rs` covers monotonicity
//! properties of `AclDomainState::is_authorized`; specific edge cases
//! (literal-vs-prefix precedence, deny-overrides, wildcard-anonymous
//! carve-out, operation implication) are not enumerated.
//!
//! `tests/acl_enforcement_tests.rs` covers a few per-handler authz
//! checks (Produce, Metadata, ListOffsets); the audit's P1.13 flagged
//! that the *full* per-API matrix isn't pinned — adding a new request
//! handler without an authorize() call would silently expose that path
//! to the public network.
//!
//! Two contracts pinned:
//!
//! ## P1.12 — Pattern-matching specifics (deterministic enumeration)
//! 1. **Literal exact match** authorizes the named resource only;
//!    a different resource of the same type is denied.
//! 2. **Prefixed match** matches any resource starting with the prefix.
//! 3. **Deny on prefix overrides Allow on literal** (deny wins).
//! 4. **`User:*` does not authorize `User:ANONYMOUS`**: the wildcard
//!    binding must NOT extend to unauthenticated requests, per the
//!    audit's documented carve-out in `acl.rs::matches_principal`.
//! 5. **`AclOperation::All` implies every other operation**.
//! 6. **`Read`/`Write`/`Delete`/`Alter` imply `Describe`** but not each
//!    other.
//! 7. **Empty ACL state → NotFound** (not Allowed, not Denied).
//!
//! ## P1.13 — Per-API authz matrix
//! 8. Every public dispatch handler that accepts a topic name returns
//!    `TopicAuthorizationFailed` (or analogue) when the principal has
//!    no matching ACL — covering Produce, Fetch, ListOffsets, Metadata,
//!    OffsetCommit, OffsetForLeaderEpoch, IncrementalAlterConfigs.
//!    Catches a regression where a new handler ships without an
//!    `authorize()` call.

use std::sync::Arc;

use bytes::Bytes;
use kafkaesque::cluster::raft::{
    AclBinding, AclCommand, AclDecision, AclDomainState, AclOperation, AclPatternType,
    AclPermissionType, AclResourceType,
};
use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::RequestContext;
use kafkaesque::server::request::{
    CreateTopicData, CreateTopicsRequestData, FetchPartitionData, FetchRequestData, FetchTopicData,
    IncrementalAlterConfigsEntry, IncrementalAlterConfigsRequestData,
    IncrementalAlterConfigsResource, IncrementalAlterOp, ListOffsetsPartitionData,
    ListOffsetsRequestData, ListOffsetsTopicData, MetadataRequestData, OffsetCommitPartitionData,
    OffsetCommitRequestData, OffsetCommitTopicData, OffsetForLeaderEpochPartitionData,
    OffsetForLeaderEpochRequestData, OffsetForLeaderEpochTopicData, ProducePartitionData,
    ProduceRequestData, ProduceTopicData,
};
use object_store::memory::InMemory;

mod common;
use common::enable_single_node_bootstrap;

// ---------------------------------------------------------------------------
// Pattern-matching helpers
// ---------------------------------------------------------------------------

fn lit(
    rt: AclResourceType,
    name: &str,
    principal: &str,
    op: AclOperation,
    perm: AclPermissionType,
) -> AclBinding {
    AclBinding {
        resource_type: rt,
        resource_name: name.into(),
        pattern_type: AclPatternType::Literal,
        principal: principal.into(),
        host: "*".into(),
        operation: op,
        permission: perm,
    }
}

fn pre(
    rt: AclResourceType,
    prefix: &str,
    principal: &str,
    op: AclOperation,
    perm: AclPermissionType,
) -> AclBinding {
    AclBinding {
        resource_type: rt,
        resource_name: prefix.into(),
        pattern_type: AclPatternType::Prefixed,
        principal: principal.into(),
        host: "*".into(),
        operation: op,
        permission: perm,
    }
}

fn build_state(bindings: Vec<AclBinding>) -> AclDomainState {
    let mut s = AclDomainState::new();
    s.apply(AclCommand::CreateAcls { bindings });
    s
}

// ---------------------------------------------------------------------------
// 1. Empty state → NotFound
// ---------------------------------------------------------------------------

#[test]
fn empty_acl_state_returns_not_found_decision() {
    let s = AclDomainState::new();
    let d = s.is_authorized(
        "User:alice",
        "1.2.3.4",
        AclOperation::Read,
        AclResourceType::Topic,
        "orders",
    );
    assert!(
        matches!(d, AclDecision::NotFound),
        "empty ACL state must return NotFound (caller decides default), got {:?}",
        d,
    );
}

// ---------------------------------------------------------------------------
// 2. Literal exact match
// ---------------------------------------------------------------------------

#[test]
fn literal_match_authorizes_only_exact_resource_name() {
    let s = build_state(vec![lit(
        AclResourceType::Topic,
        "orders",
        "User:alice",
        AclOperation::Read,
        AclPermissionType::Allow,
    )]);
    assert_eq!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Read,
            AclResourceType::Topic,
            "orders"
        ),
        AclDecision::Allowed,
    );
    // Not the same name → NotFound (not Allowed via prefix).
    assert!(matches!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Read,
            AclResourceType::Topic,
            "orders-2026"
        ),
        AclDecision::NotFound,
    ));
    assert!(matches!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Read,
            AclResourceType::Topic,
            "order"
        ),
        AclDecision::NotFound,
    ));
}

// ---------------------------------------------------------------------------
// 3. Prefix match
// ---------------------------------------------------------------------------

#[test]
fn prefixed_match_authorizes_any_starts_with_prefix() {
    let s = build_state(vec![pre(
        AclResourceType::Topic,
        "ord",
        "User:alice",
        AclOperation::Read,
        AclPermissionType::Allow,
    )]);
    for name in ["ord", "orders", "ord-2026", "ordo-frater"] {
        assert_eq!(
            s.is_authorized(
                "User:alice",
                "1.2.3.4",
                AclOperation::Read,
                AclResourceType::Topic,
                name
            ),
            AclDecision::Allowed,
            "prefix 'ord' must match name {:?}",
            name,
        );
    }
    assert!(matches!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Read,
            AclResourceType::Topic,
            "billing"
        ),
        AclDecision::NotFound,
    ));
}

// ---------------------------------------------------------------------------
// 4. Deny precedence
// ---------------------------------------------------------------------------

#[test]
fn deny_on_prefix_overrides_allow_on_literal() {
    let s = build_state(vec![
        lit(
            AclResourceType::Topic,
            "orders-secret",
            "User:alice",
            AclOperation::Read,
            AclPermissionType::Allow,
        ),
        pre(
            AclResourceType::Topic,
            "orders-",
            "User:alice",
            AclOperation::Read,
            AclPermissionType::Deny,
        ),
    ]);
    let d = s.is_authorized(
        "User:alice",
        "1.2.3.4",
        AclOperation::Read,
        AclResourceType::Topic,
        "orders-secret",
    );
    assert!(
        matches!(d, AclDecision::Denied),
        "Deny-prefix must beat Allow-literal even on the same resource, got {:?}",
        d,
    );
}

#[test]
fn deny_on_literal_overrides_allow_on_prefix() {
    let s = build_state(vec![
        pre(
            AclResourceType::Topic,
            "orders",
            "User:alice",
            AclOperation::Read,
            AclPermissionType::Allow,
        ),
        lit(
            AclResourceType::Topic,
            "orders-billing",
            "User:alice",
            AclOperation::Read,
            AclPermissionType::Deny,
        ),
    ]);
    assert!(matches!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Read,
            AclResourceType::Topic,
            "orders-billing"
        ),
        AclDecision::Denied,
    ));
    // Sibling under the prefix is still allowed.
    assert_eq!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Read,
            AclResourceType::Topic,
            "orders-payments"
        ),
        AclDecision::Allowed,
    );
}

// ---------------------------------------------------------------------------
// 5. Wildcard principal carve-out
// ---------------------------------------------------------------------------

#[test]
fn wildcard_principal_does_not_authorize_anonymous() {
    // The audit explicitly documents this carve-out: `User:*` must NOT
    // authorize `User:ANONYMOUS`, otherwise a "grant User:* describe"
    // intent silently exposes the broker to TCP clients that never
    // completed SASL.
    let s = build_state(vec![lit(
        AclResourceType::Topic,
        "public",
        "User:*",
        AclOperation::Describe,
        AclPermissionType::Allow,
    )]);
    assert!(matches!(
        s.is_authorized(
            "User:ANONYMOUS",
            "1.2.3.4",
            AclOperation::Describe,
            AclResourceType::Topic,
            "public"
        ),
        AclDecision::NotFound,
    ));
    // Real authenticated principal IS authorized.
    assert_eq!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Describe,
            AclResourceType::Topic,
            "public"
        ),
        AclDecision::Allowed,
    );
}

// ---------------------------------------------------------------------------
// 6. Operation implication
// ---------------------------------------------------------------------------

#[test]
fn all_operation_implies_every_other_operation() {
    let s = build_state(vec![lit(
        AclResourceType::Topic,
        "orders",
        "User:alice",
        AclOperation::All,
        AclPermissionType::Allow,
    )]);
    for op in [
        AclOperation::Read,
        AclOperation::Write,
        AclOperation::Create,
        AclOperation::Delete,
        AclOperation::Alter,
        AclOperation::Describe,
        AclOperation::ClusterAction,
        AclOperation::IdempotentWrite,
    ] {
        assert_eq!(
            s.is_authorized(
                "User:alice",
                "1.2.3.4",
                op,
                AclResourceType::Topic,
                "orders"
            ),
            AclDecision::Allowed,
            "All must imply {:?}",
            op,
        );
    }
}

#[test]
fn read_implies_describe_but_not_write() {
    let s = build_state(vec![lit(
        AclResourceType::Topic,
        "orders",
        "User:alice",
        AclOperation::Read,
        AclPermissionType::Allow,
    )]);
    assert_eq!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Describe,
            AclResourceType::Topic,
            "orders"
        ),
        AclDecision::Allowed,
        "Read must imply Describe (Kafka contract)",
    );
    assert!(matches!(
        s.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Write,
            AclResourceType::Topic,
            "orders"
        ),
        AclDecision::NotFound,
    ));
}

// ---------------------------------------------------------------------------
// 7. Per-API authorization matrix
// ---------------------------------------------------------------------------

static RAFT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(31_000);

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
    config
}

async fn locked_handler() -> SlateDBClusterHandler {
    // ACL on, deny-by-default, no super_users for the test principal.
    // `User:admin` IS a super-user so we can set up topics; tests then
    // act as `User:alice` who has no bindings — every authorize() call
    // must fail.
    let mut config = isolated_test_config(1);
    config.auto_create_topics = false;
    config.acl_enabled = true;
    config.acl_deny_by_default = true;
    config.super_users = vec!["User:admin".to_string()];
    let object_store = Arc::new(InMemory::new());
    SlateDBClusterHandler::with_object_store(config, object_store)
        .await
        .expect("handler")
}

fn ctx_as(principal: &str) -> RequestContext {
    RequestContext {
        client_addr: "127.0.0.1:0".parse().unwrap(),
        conn_id: 1,
        api_version: 8,
        client_id: Some("authz-matrix".into()),
        request_id: uuid::Uuid::new_v4(),
        principal: Arc::from(principal),
        client_host: Arc::from("127.0.0.1"),
        transport_tls: false,
    }
}

async fn admin_setup_topic(handler: &SlateDBClusterHandler, name: &str) {
    let admin = ctx_as("User:admin");
    let _ = handler
        .handle_create_topics(
            &admin,
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: name.into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: vec![],
                }],
                timeout_ms: 5_000,
                validate_only: false,
            },
        )
        .await;
}

#[tokio::test]
async fn produce_denied_for_unauthorized_principal() {
    let handler = locked_handler().await;
    admin_setup_topic(&handler, "secret").await;

    let payload = Bytes::from_static(b"placeholder-not-a-valid-batch");
    let resp = handler
        .handle_produce(
            &ctx_as("User:alice"),
            ProduceRequestData {
                transactional_id: None,
                acks: 1,
                timeout_ms: 5_000,
                topics: vec![ProduceTopicData {
                    name: "secret".into(),
                    partitions: vec![ProducePartitionData {
                        partition_index: 0,
                        records: payload,
                    }],
                }],
            },
        )
        .await;
    assert_eq!(
        resp.responses[0].partitions[0].error_code,
        KafkaCode::TopicAuthorizationFailed,
        "Produce without write ACL must surface TopicAuthorizationFailed",
    );
}

#[tokio::test]
async fn fetch_denied_for_unauthorized_principal() {
    let handler = locked_handler().await;
    admin_setup_topic(&handler, "secret").await;

    let resp = handler
        .handle_fetch(
            &ctx_as("User:alice"),
            FetchRequestData {
                replica_id: -1,
                max_wait_ms: 50,
                min_bytes: 0,
                max_bytes: 1024 * 1024,
                isolation_level: 0,
                session_id: 0,
                session_epoch: 0,
                topics: vec![FetchTopicData {
                    name: "secret".into(),
                    partitions: vec![FetchPartitionData {
                        partition_index: 0,
                        fetch_offset: 0,
                        log_start_offset: -1,
                        partition_max_bytes: 1024 * 1024,
                        current_leader_epoch: -1,
                    }],
                }],
                forgotten_topics: vec![],
                rack_id: String::new(),
            },
        )
        .await;
    assert_eq!(
        resp.responses[0].partitions[0].error_code,
        KafkaCode::TopicAuthorizationFailed,
    );
}

#[tokio::test]
async fn list_offsets_denied_for_unauthorized_principal() {
    let handler = locked_handler().await;
    admin_setup_topic(&handler, "secret").await;

    let resp = handler
        .handle_list_offsets(
            &ctx_as("User:alice"),
            ListOffsetsRequestData {
                replica_id: -1,
                isolation_level: 0,
                topics: vec![ListOffsetsTopicData {
                    name: "secret".into(),
                    partitions: vec![ListOffsetsPartitionData {
                        partition_index: 0,
                        timestamp: -1,
                    }],
                }],
            },
        )
        .await;
    assert_eq!(
        resp.topics[0].partitions[0].error_code,
        KafkaCode::TopicAuthorizationFailed,
    );
}

#[tokio::test]
async fn metadata_filters_unauthorized_topics_or_returns_authz_error() {
    let handler = locked_handler().await;
    admin_setup_topic(&handler, "secret").await;

    let resp = handler
        .handle_metadata(
            &ctx_as("User:alice"),
            MetadataRequestData {
                topics: Some(vec!["secret".to_string()]),
                allow_auto_topic_creation: false,
                include_cluster_authorized_operations: false,
                include_topic_authorized_operations: false,
            },
        )
        .await;
    // Either: the topic is filtered out, OR it appears with
    // TopicAuthorizationFailed. Both satisfy the contract — pin that
    // we don't surface it cleanly.
    let leaked = resp
        .topics
        .iter()
        .find(|t| t.name == "secret" && t.error_code == KafkaCode::None);
    assert!(
        leaked.is_none(),
        "denied topic must NOT appear with error_code=None; got {:?}",
        resp.topics,
    );
}

#[tokio::test]
async fn offset_commit_denied_for_unauthorized_principal() {
    let handler = locked_handler().await;
    admin_setup_topic(&handler, "secret").await;

    let resp = handler
        .handle_offset_commit(
            &ctx_as("User:alice"),
            OffsetCommitRequestData {
                group_id: "test-group".into(),
                generation_id: -1,
                member_id: String::new(),
                topics: vec![OffsetCommitTopicData {
                    name: "secret".into(),
                    partitions: vec![OffsetCommitPartitionData {
                        partition_index: 0,
                        committed_offset: 0,
                        committed_metadata: None,
                    }],
                }],
            },
        )
        .await;
    let codes: Vec<_> = resp.topics[0]
        .partitions
        .iter()
        .map(|p| p.error_code)
        .collect();
    assert!(
        codes.iter().any(|c| matches!(
            c,
            KafkaCode::TopicAuthorizationFailed | KafkaCode::GroupAuthorizationFailed
        )),
        "OffsetCommit without ACL must return Topic- or Group-AuthorizationFailed; got {:?}",
        codes,
    );
}

#[tokio::test]
async fn offset_for_leader_epoch_denied_for_unauthorized_principal() {
    let handler = locked_handler().await;
    admin_setup_topic(&handler, "secret").await;

    let resp = handler
        .handle_offset_for_leader_epoch(
            &ctx_as("User:alice"),
            OffsetForLeaderEpochRequestData {
                replica_id: -2,
                topics: vec![OffsetForLeaderEpochTopicData {
                    name: "secret".into(),
                    partitions: vec![OffsetForLeaderEpochPartitionData {
                        partition_index: 0,
                        current_leader_epoch: -1,
                        leader_epoch: 0,
                    }],
                }],
            },
        )
        .await;
    assert_eq!(
        resp.topics[0].partitions[0].error_code,
        KafkaCode::TopicAuthorizationFailed,
    );
}

#[tokio::test]
async fn incremental_alter_configs_denied_for_unauthorized_principal() {
    let handler = locked_handler().await;
    admin_setup_topic(&handler, "secret").await;

    let resp = handler
        .handle_incremental_alter_configs(
            &ctx_as("User:alice"),
            IncrementalAlterConfigsRequestData {
                resources: vec![IncrementalAlterConfigsResource {
                    resource_type: 2, // 2 = TOPIC
                    resource_name: "secret".into(),
                    configs: vec![IncrementalAlterConfigsEntry {
                        name: "retention.ms".into(),
                        op: IncrementalAlterOp::Set,
                        value: Some("60000".into()),
                    }],
                }],
                validate_only: false,
            },
        )
        .await;
    let code = resp.responses[0].error_code;
    assert!(
        matches!(
            code,
            KafkaCode::TopicAuthorizationFailed | KafkaCode::ClusterAuthorizationFailed
        ),
        "IncrementalAlterConfigs without Alter ACL must surface AuthorizationFailed; got {:?}",
        code,
    );
}
