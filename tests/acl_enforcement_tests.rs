//! Behavioral tests for ACL enforcement on handler hot paths.
//!
//! These verify that deny-by-default ACL posture blocks produce, metadata,
//! and list-offsets for principals without bindings — the gaps called out
//! in the security audit.

#![allow(clippy::field_reassign_with_default)]

use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::*;
use kafkaesque::server::{Handler, RequestContext};
use object_store::memory::InMemory;
use std::net::SocketAddr;
use std::sync::Arc;

static RAFT_PORT_COUNTER: std::sync::atomic::AtomicU16 = std::sync::atomic::AtomicU16::new(22000);

fn isolated_test_config(broker_id: i32) -> ClusterConfig {
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

fn test_context(principal: &str) -> RequestContext {
    RequestContext {
        client_addr: "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        api_version: 8,
        client_id: Some("acl-test-client".to_string()),
        request_id: uuid::Uuid::new_v4(),
        principal: Arc::from(principal),
        client_host: Arc::from("127.0.0.1"),
        transport_tls: false,
    }
}

async fn handler_with_acl_super_user() -> SlateDBClusterHandler {
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

#[tokio::test]
async fn test_produce_denied_without_write_acl() {
    let handler = handler_with_acl_super_user().await;
    let admin_ctx = test_context("User:admin");
    let _ = handler
        .handle_create_topics(
            &admin_ctx,
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: "secret-topic".to_string(),
                    num_partitions: 1,
                    replication_factor: 1,
                }],
                timeout_ms: 5000,
                validate_only: false,
            },
        )
        .await;

    let ctx = test_context("User:alice");
    let produce_req = ProduceRequestData {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5000,
        topics: vec![ProduceTopicData {
            name: "secret-topic".to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records: bytes::Bytes::from_static(b"not-a-valid-batch"),
            }],
        }],
    };

    let response = handler.handle_produce(&ctx, produce_req).await;
    assert_eq!(response.responses.len(), 1);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::TopicAuthorizationFailed
    );
}

#[tokio::test]
async fn test_metadata_filters_unauthorized_topics() {
    let handler = handler_with_acl_super_user().await;
    let admin_ctx = test_context("User:admin");
    let _ = handler
        .handle_create_topics(
            &admin_ctx,
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: "restricted".to_string(),
                    num_partitions: 1,
                    replication_factor: 1,
                }],
                timeout_ms: 5000,
                validate_only: false,
            },
        )
        .await;

    let ctx = test_context("User:bob");
    let response = handler
        .handle_metadata(
            &ctx,
            MetadataRequestData {
                topics: Some(vec!["restricted".to_string()]),
            },
        )
        .await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].error_code,
        KafkaCode::TopicAuthorizationFailed
    );
}

#[tokio::test]
async fn test_list_offsets_denied_without_describe_acl() {
    let handler = handler_with_acl_super_user().await;
    let admin_ctx = test_context("User:admin");
    let _ = handler
        .handle_create_topics(
            &admin_ctx,
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: "offsets-topic".to_string(),
                    num_partitions: 1,
                    replication_factor: 1,
                }],
                timeout_ms: 5000,
                validate_only: false,
            },
        )
        .await;

    let ctx = test_context("User:carol");
    let response = handler
        .handle_list_offsets(
            &ctx,
            ListOffsetsRequestData {
                replica_id: -1,
                isolation_level: 0,
                topics: vec![ListOffsetsTopicData {
                    name: "offsets-topic".to_string(),
                    partitions: vec![ListOffsetsPartitionData {
                        partition_index: 0,
                        timestamp: -1,
                    }],
                }],
            },
        )
        .await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        KafkaCode::TopicAuthorizationFailed
    );
}
