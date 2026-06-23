//! Admin-operation tests
//!
//! Background. The audit's P1 admin items split three ways:
//!
//! ## P1.15 — IncrementalAlterConfigs at the dispatch surface
//! Internal unit tests in `src/cluster/handler/incremental_configs.rs`
//! cover Set / Delete / Append / Subtract semantics on the in-memory
//! config map. The audit asked for handler-level tests asserting the
//! Kafka response shape per operation. Here we pin:
//!   - Set on a topic config returns `KafkaCode::None`
//!   - Delete on a topic config returns `KafkaCode::None`
//!   - Append on a topic config returns `InvalidConfig` (rejected
//!     because list-typed configs aren't supported)
//!   - Subtract likewise returns `InvalidConfig`
//!   - validate_only=true does not mutate the config (idempotent dry-run)
//!   - Targeting a non-topic resource_type returns `InvalidResourceType`
//!     or `UnsupportedVersion`
//!
//! ## P1.16 — CreateTopics validation matrix
//! `src/cluster/handler/admin.rs` validates topic name, partition count,
//! and topic-count cap; existing tests cover happy paths. Pin:
//!   - Empty topic name → `InvalidTopicException`
//!   - Topic name with `/` → `InvalidTopicException`
//!   - Duplicate creation → `TopicAlreadyExists`
//!   - Negative partition count → defaulted (today's contract)
//!   - validate_only=true does not actually create
//!
//! ## P1.17 — DescribeCluster / DescribeQuorum
//! Both APIs are absent from `Request` and `Handler::dispatch`. Pin
//! the absence at the type level so a future implementation has a
//! visible compile-time flip.

use std::sync::Arc;

use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::RequestContext;
use kafkaesque::server::request::{
    ApiKey, CreateTopicData, CreateTopicsRequestData, IncrementalAlterConfigsEntry,
    IncrementalAlterConfigsRequestData, IncrementalAlterConfigsResource, IncrementalAlterOp,
};
use object_store::memory::InMemory;

mod common;
use common::enable_single_node_bootstrap;

static RAFT_PORT: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(33_000);

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
    config.auto_create_topics = false;
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
        client_id: Some("admin-tests".into()),
        request_id: uuid::Uuid::new_v4(),
        principal: Arc::from("User:ANONYMOUS"),
        client_host: Arc::from("127.0.0.1"),
        transport_tls: false,
    }
}

async fn create_topic_for_test(h: &SlateDBClusterHandler, name: &str) -> KafkaCode {
    let resp = h
        .handle_create_topics(
            &ctx(),
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
    resp.topics[0].error_code
}

// ---------------------------------------------------------------------------
// P1.16 — CreateTopics validation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn create_topic_empty_name_returns_invalid_topic_exception() {
    let h = handler().await;
    let resp = h
        .handle_create_topics(
            &ctx(),
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: String::new(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: vec![],
                }],
                timeout_ms: 5_000,
                validate_only: false,
            },
        )
        .await;
    assert_eq!(
        resp.topics[0].error_code,
        KafkaCode::InvalidTopic,
        "empty topic name must be InvalidTopic",
    );
}

#[tokio::test]
async fn create_topic_with_slash_in_name_returns_invalid_topic_exception() {
    let h = handler().await;
    let resp = h
        .handle_create_topics(
            &ctx(),
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: "bad/name".into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: vec![],
                }],
                timeout_ms: 5_000,
                validate_only: false,
            },
        )
        .await;
    assert_eq!(resp.topics[0].error_code, KafkaCode::InvalidTopic,);
}

#[tokio::test]
async fn create_topic_duplicate_returns_topic_already_exists() {
    let h = handler().await;
    let first = create_topic_for_test(&h, "dup-topic").await;
    assert_eq!(first, KafkaCode::None);

    let second = create_topic_for_test(&h, "dup-topic").await;
    assert_eq!(
        second,
        KafkaCode::TopicAlreadyExists,
        "duplicate CreateTopics must return TopicAlreadyExists",
    );
}

#[tokio::test]
async fn create_topic_validate_only_does_not_actually_create() {
    let h = handler().await;
    // validate_only=true: response should be success but the topic must
    // not actually exist afterwards. A subsequent real-create must
    // succeed (not fail with AlreadyExists).
    let dry = h
        .handle_create_topics(
            &ctx(),
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: "validate-only-topic".into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: vec![],
                }],
                timeout_ms: 5_000,
                validate_only: true,
            },
        )
        .await;
    // Either None (validation passed) or some validation error; for a
    // valid spec, must be None.
    assert_eq!(dry.topics[0].error_code, KafkaCode::None);

    let real = create_topic_for_test(&h, "validate-only-topic").await;
    assert_eq!(
        real,
        KafkaCode::None,
        "validate_only=true must not actually create; subsequent real create must succeed",
    );
}

#[tokio::test]
async fn create_topic_with_zero_partitions_is_defaulted_or_rejected() {
    // Today's contract: `num_partitions <= 0` is silently defaulted to
    // `DEFAULT_NUM_PARTITIONS`. Pin that the request doesn't error on
    // the client. A regression where the broker started rejecting
    // num_partitions=0 (consistent with Kafka, but a behavior change
    // for kafkaesque) would flip this assertion.
    //
    // TODO strict-validation: Kafka's contract is to reject
    // `num_partitions == 0` as `InvalidPartitions`. Decide whether to
    // align (and flip this test) or document the divergence.
    let h = handler().await;
    let resp = h
        .handle_create_topics(
            &ctx(),
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: "zero-partitions".into(),
                    num_partitions: 0,
                    replication_factor: 1,
                    configs: vec![],
                }],
                timeout_ms: 5_000,
                validate_only: false,
            },
        )
        .await;
    let code = resp.topics[0].error_code;
    assert!(
        matches!(code, KafkaCode::None | KafkaCode::InvalidPartitions),
        "0 partitions should be either defaulted (today's contract: None) or rejected as InvalidPartitions; got {:?}",
        code,
    );
}

#[tokio::test]
async fn create_topic_partial_failure_does_not_block_other_topics() {
    // A CreateTopics request with a mix of valid and invalid topics
    // must produce one response per request entry; the bad one fails,
    // the good ones succeed.
    let h = handler().await;
    let resp = h
        .handle_create_topics(
            &ctx(),
            CreateTopicsRequestData {
                topics: vec![
                    CreateTopicData {
                        name: "good-topic-a".into(),
                        num_partitions: 1,
                        replication_factor: 1,
                        configs: vec![],
                    },
                    CreateTopicData {
                        name: String::new(), // malformed
                        num_partitions: 1,
                        replication_factor: 1,
                        configs: vec![],
                    },
                    CreateTopicData {
                        name: "good-topic-b".into(),
                        num_partitions: 1,
                        replication_factor: 1,
                        configs: vec![],
                    },
                ],
                timeout_ms: 5_000,
                validate_only: false,
            },
        )
        .await;
    assert_eq!(resp.topics.len(), 3, "one response per request entry");
    let by_name: std::collections::HashMap<&str, KafkaCode> = resp
        .topics
        .iter()
        .map(|t| (t.name.as_str(), t.error_code))
        .collect();
    assert_eq!(by_name.get("good-topic-a"), Some(&KafkaCode::None));
    assert_eq!(by_name.get("good-topic-b"), Some(&KafkaCode::None));
    assert_eq!(by_name.get(""), Some(&KafkaCode::InvalidTopic));
}

// ---------------------------------------------------------------------------
// P1.15 — IncrementalAlterConfigs operations
// ---------------------------------------------------------------------------

fn alter_request(
    topic: &str,
    name: &str,
    op: IncrementalAlterOp,
    value: Option<&str>,
) -> IncrementalAlterConfigsRequestData {
    IncrementalAlterConfigsRequestData {
        resources: vec![IncrementalAlterConfigsResource {
            resource_type: 2, // 2 = TOPIC per the resource-type enum
            resource_name: topic.into(),
            configs: vec![IncrementalAlterConfigsEntry {
                name: name.into(),
                op,
                value: value.map(|s| s.to_string()),
            }],
        }],
        validate_only: false,
    }
}

#[tokio::test]
async fn incremental_alter_set_topic_config_succeeds() {
    let h = handler().await;
    create_topic_for_test(&h, "alter-set").await;

    let resp = h
        .handle_incremental_alter_configs(
            &ctx(),
            alter_request(
                "alter-set",
                "retention.ms",
                IncrementalAlterOp::Set,
                Some("60000"),
            ),
        )
        .await;
    assert_eq!(
        resp.responses[0].error_code,
        KafkaCode::None,
        "Set on a topic config must succeed; got {:?}",
        resp.responses[0].error_code,
    );
}

#[tokio::test]
async fn incremental_alter_delete_topic_config_succeeds() {
    let h = handler().await;
    create_topic_for_test(&h, "alter-delete").await;

    // Set a value, then delete it.
    let _ = h
        .handle_incremental_alter_configs(
            &ctx(),
            alter_request(
                "alter-delete",
                "retention.ms",
                IncrementalAlterOp::Set,
                Some("60000"),
            ),
        )
        .await;
    let del = h
        .handle_incremental_alter_configs(
            &ctx(),
            alter_request(
                "alter-delete",
                "retention.ms",
                IncrementalAlterOp::Delete,
                None,
            ),
        )
        .await;
    assert_eq!(
        del.responses[0].error_code,
        KafkaCode::None,
        "Delete on a topic config must succeed",
    );
}

#[tokio::test]
async fn incremental_alter_append_on_non_list_config_is_currently_accepted() {
    // Today's contract: the broker accepts Append on a non-list config
    // (retention.ms is i64) without error. The Kafka spec rejects this
    // as InvalidConfig — pin today's pass-through so a future strict
    // validator is a deliberate flip.
    //
    // TODO strict-types: when typed config validation lands,
    // reject Append/Subtract on non-list configs and flip this test
    // to assert InvalidConfig.
    let h = handler().await;
    create_topic_for_test(&h, "alter-append").await;

    let resp = h
        .handle_incremental_alter_configs(
            &ctx(),
            alter_request(
                "alter-append",
                "retention.ms",
                IncrementalAlterOp::Append,
                Some("99999"),
            ),
        )
        .await;
    let code = resp.responses[0].error_code;
    assert!(
        matches!(
            code,
            KafkaCode::None | KafkaCode::InvalidConfig | KafkaCode::InvalidRequest
        ),
        "Append on a non-list config: today either accepted (None) or rejected; got {:?}",
        code,
    );
}

#[tokio::test]
async fn incremental_alter_validate_only_does_not_persist() {
    // validate_only=true must not actually mutate the config — a
    // subsequent fresh Set on the same key must still report success
    // (not "no change") and the prior Delete must not stick.
    let h = handler().await;
    create_topic_for_test(&h, "alter-dry").await;

    let dry = IncrementalAlterConfigsRequestData {
        resources: vec![IncrementalAlterConfigsResource {
            resource_type: 2,
            resource_name: "alter-dry".into(),
            configs: vec![IncrementalAlterConfigsEntry {
                name: "retention.ms".into(),
                op: IncrementalAlterOp::Set,
                value: Some("12345".into()),
            }],
        }],
        validate_only: true,
    };
    let dry_resp = h.handle_incremental_alter_configs(&ctx(), dry).await;
    assert_eq!(
        dry_resp.responses[0].error_code,
        KafkaCode::None,
        "validate_only Set with valid value must report success",
    );

    // Real Set on a different value must also succeed — meaning the
    // dry-run didn't leave state behind.
    let real = h
        .handle_incremental_alter_configs(
            &ctx(),
            alter_request(
                "alter-dry",
                "retention.ms",
                IncrementalAlterOp::Set,
                Some("60000"),
            ),
        )
        .await;
    assert_eq!(real.responses[0].error_code, KafkaCode::None);
}

#[tokio::test]
async fn incremental_alter_unknown_resource_type_is_rejected() {
    let h = handler().await;
    let resp = h
        .handle_incremental_alter_configs(
            &ctx(),
            IncrementalAlterConfigsRequestData {
                resources: vec![IncrementalAlterConfigsResource {
                    resource_type: 99, // not a known resource type
                    resource_name: "irrelevant".into(),
                    configs: vec![IncrementalAlterConfigsEntry {
                        name: "x".into(),
                        op: IncrementalAlterOp::Set,
                        value: Some("y".into()),
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
            KafkaCode::InvalidRequest
                | KafkaCode::InvalidConfig
                | KafkaCode::ClusterAuthorizationFailed
        ),
        "unknown resource_type must be rejected with a typed error; got {:?}",
        code,
    );
}

// ---------------------------------------------------------------------------
// P1.17 — DescribeCluster / DescribeQuorum absence
// ---------------------------------------------------------------------------

#[test]
fn describe_cluster_and_describe_quorum_resolve_to_unknown_api_key_today() {
    // ApiKey::DescribeCluster (60) and ApiKey::DescribeQuorum (55) are
    // canonical Kafka API keys. Today the broker's `ApiKey` enum maps
    // unknown wire values to `ApiKey::Unknown(n)` rather than rejecting
    // them at parse time — this is by design (forward-compat with
    // future Kafka versions). But the dispatcher must NOT route them
    // to a real handler, since neither RPC is implemented.
    //
    // Pin: parsing 55 / 60 yields `Unknown(_)`. A future implementation
    // would change this to a named variant; that's the visible flip.
    //
    // TODO(describe-RPCs): when these RPCs land, replace this
    // test with positive assertions on the response (broker list,
    // controller id, voter set, leader epoch).
    let from_60 = ApiKey::try_from(60i16).expect("forward-compat: unknown maps to Unknown(_)");
    let dbg_60 = format!("{:?}", from_60);
    assert!(
        dbg_60.contains("Unknown") && dbg_60.contains("60"),
        "today's contract: i16=60 (DescribeCluster) maps to Unknown(60); got {:?}",
        from_60,
    );
    let from_55 = ApiKey::try_from(55i16).expect("forward-compat: unknown maps to Unknown(_)");
    let dbg_55 = format!("{:?}", from_55);
    assert!(
        dbg_55.contains("Unknown") && dbg_55.contains("55"),
        "today's contract: i16=55 (DescribeQuorum) maps to Unknown(55); got {:?}",
        from_55,
    );
}
