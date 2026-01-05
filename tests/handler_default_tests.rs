//! Tests for Handler trait default implementations.
//!
//! These tests cover the default implementations of the Handler trait
//! to improve test coverage for server/handler.rs.

use bytes::Bytes;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::*;
use kafkaesque::server::{Handler, RequestContext};
use std::net::SocketAddr;

/// A minimal handler implementation that uses all default methods.
struct DefaultHandler;

#[async_trait::async_trait]
impl Handler for DefaultHandler {}

/// Create a test request context.
fn create_context() -> RequestContext {
    RequestContext {
        client_addr: "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        api_version: 8,
        client_id: Some("test-client".to_string()),
        request_id: uuid::Uuid::new_v4(),
    }
}

#[tokio::test]
async fn test_default_api_versions() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = ApiVersionsRequestData {
        client_software_name: None,
        client_software_version: None,
    };

    let response = handler.handle_api_versions(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    assert!(!response.api_keys.is_empty());
}

#[tokio::test]
async fn test_default_metadata() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = MetadataRequestData {
        topics: Some(vec!["test-topic".to_string()]),
    };

    let response = handler.handle_metadata(&ctx, request).await;

    assert!(response.brokers.is_empty());
    assert_eq!(response.controller_id, -1);
    assert!(response.topics.is_empty());
}

#[tokio::test]
async fn test_default_produce() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = ProduceRequestData {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5000,
        topics: vec![ProduceTopicData {
            name: "test-topic".to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records: Bytes::from(vec![0, 1, 2, 3]),
            }],
        }],
    };

    let response = handler.handle_produce(&ctx, request).await;

    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].name, "test-topic");
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::UnknownTopicOrPartition
    );
}

#[tokio::test]
async fn test_default_fetch() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = FetchRequestData {
        replica_id: -1,
        max_wait_ms: 100,
        min_bytes: 1,
        max_bytes: 1024,
        isolation_level: 0,
        topics: vec![FetchTopicData {
            name: "test-topic".to_string(),
            partitions: vec![FetchPartitionData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024,
            }],
        }],
    };

    let response = handler.handle_fetch(&ctx, request).await;

    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].name, "test-topic");
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::UnknownTopicOrPartition
    );
}

#[tokio::test]
async fn test_default_list_offsets() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopicData {
            name: "test-topic".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                timestamp: -1,
            }],
        }],
    };

    let response = handler.handle_list_offsets(&ctx, request).await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name, "test-topic");
    assert_eq!(
        response.topics[0].partitions[0].error_code,
        KafkaCode::UnknownTopicOrPartition
    );
}

#[tokio::test]
async fn test_default_offset_commit() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = OffsetCommitRequestData {
        group_id: "test-group".to_string(),
        generation_id: 1,
        member_id: "member-1".to_string(),
        topics: vec![OffsetCommitTopicData {
            name: "test-topic".to_string(),
            partitions: vec![OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 100,
                committed_metadata: Some("metadata".to_string()),
            }],
        }],
    };

    let response = handler.handle_offset_commit(&ctx, request).await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions[0].error_code, KafkaCode::None);
}

#[tokio::test]
async fn test_default_offset_fetch() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = OffsetFetchRequestData {
        group_id: "test-group".to_string(),
        topics: vec![OffsetFetchTopicData {
            name: "test-topic".to_string(),
            partition_indexes: vec![0, 1],
        }],
    };

    let response = handler.handle_offset_fetch(&ctx, request).await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].partitions.len(), 2);
    assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
    assert_eq!(response.error_code, KafkaCode::None);
}

#[tokio::test]
async fn test_default_find_coordinator() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = FindCoordinatorRequestData {
        key: "test-group".to_string(),
        key_type: 0,
    };

    let response = handler.handle_find_coordinator(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
    assert_eq!(response.node_id, -1);
}

#[tokio::test]
async fn test_default_join_group() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = JoinGroupRequestData {
        group_id: "test-group".to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 30000,
        member_id: String::new(),
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupProtocolData {
            name: "range".to_string(),
            metadata: Bytes::from(vec![1, 2, 3]),
        }],
    };

    let response = handler.handle_join_group(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
    assert_eq!(response.generation_id, -1);
}

#[tokio::test]
async fn test_default_heartbeat() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = HeartbeatRequestData {
        group_id: "test-group".to_string(),
        generation_id: 1,
        member_id: "member-1".to_string(),
    };

    let response = handler.handle_heartbeat(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
}

#[tokio::test]
async fn test_default_leave_group() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = LeaveGroupRequestData {
        group_id: "test-group".to_string(),
        member_id: "member-1".to_string(),
    };

    let response = handler.handle_leave_group(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
}

#[tokio::test]
async fn test_default_sync_group() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = SyncGroupRequestData {
        group_id: "test-group".to_string(),
        generation_id: 1,
        member_id: "member-1".to_string(),
        assignments: vec![],
    };

    let response = handler.handle_sync_group(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
}

#[tokio::test]
async fn test_default_describe_groups() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = DescribeGroupsRequestData {
        group_ids: vec!["group1".to_string(), "group2".to_string()],
    };

    let response = handler.handle_describe_groups(&ctx, request).await;

    assert_eq!(response.groups.len(), 2);
    for group in &response.groups {
        assert_eq!(group.error_code, KafkaCode::GroupCoordinatorNotAvailable);
    }
}

#[tokio::test]
async fn test_default_list_groups() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = ListGroupsRequestData {
        states_filter: vec![],
    };

    let response = handler.handle_list_groups(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.groups.is_empty());
}

#[tokio::test]
async fn test_default_delete_groups() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = DeleteGroupsRequestData {
        group_ids: vec!["group1".to_string(), "group2".to_string()],
    };

    let response = handler.handle_delete_groups(&ctx, request).await;

    assert_eq!(response.results.len(), 2);
    for result in &response.results {
        assert_eq!(result.error_code, KafkaCode::GroupCoordinatorNotAvailable);
    }
}

#[tokio::test]
async fn test_default_sasl_handshake() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = SaslHandshakeRequestData {
        mechanism: "PLAIN".to_string(),
    };

    let response = handler.handle_sasl_handshake(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::UnsupportedSaslMechanism);
    assert!(response.mechanisms.is_empty());
}

#[tokio::test]
async fn test_default_sasl_authenticate() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = SaslAuthenticateRequestData {
        auth_bytes: Bytes::from(vec![0, 1, 2]),
    };

    let response = handler.handle_sasl_authenticate(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::SaslAuthenticationFailed);
    assert!(response.error_message.is_some());
}

#[tokio::test]
async fn test_default_create_topics() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = CreateTopicsRequestData {
        topics: vec![
            CreateTopicData {
                name: "topic1".to_string(),
                num_partitions: 3,
                replication_factor: 1,
            },
            CreateTopicData {
                name: "topic2".to_string(),
                num_partitions: 5,
                replication_factor: 1,
            },
        ],
        timeout_ms: 5000,
    };

    let response = handler.handle_create_topics(&ctx, request).await;

    assert_eq!(response.topics.len(), 2);
    for topic in &response.topics {
        assert_eq!(topic.error_code, KafkaCode::None);
    }
}

#[tokio::test]
async fn test_default_delete_topics() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = DeleteTopicsRequestData {
        topic_names: vec!["topic1".to_string(), "topic2".to_string()],
        timeout_ms: 5000,
    };

    let response = handler.handle_delete_topics(&ctx, request).await;

    assert_eq!(response.responses.len(), 2);
    for topic in &response.responses {
        assert_eq!(topic.error_code, KafkaCode::None);
    }
}

#[tokio::test]
async fn test_default_init_producer_id() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let request = InitProducerIdRequestData {
        transactional_id: None,
        transaction_timeout_ms: 60000,
        producer_id: -1,
        producer_epoch: -1,
    };

    let response = handler.handle_init_producer_id(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.producer_id > 0);
    assert_eq!(response.producer_epoch, 0);
}

#[tokio::test]
async fn test_default_unknown() {
    let handler = DefaultHandler;
    let ctx = create_context();

    let response = handler
        .handle_unknown(&ctx, ApiKey::Produce, Bytes::from(vec![0, 1, 2]))
        .await;

    assert_eq!(response.error_code, KafkaCode::UnsupportedVersion);
}

#[tokio::test]
async fn test_request_context_accessors() {
    let ctx = create_context();

    // Test request_id accessor
    let request_id = ctx.request_id();
    assert_eq!(*request_id, ctx.request_id);

    // Test other fields
    assert_eq!(ctx.api_version, 8);
    assert_eq!(ctx.client_id, Some("test-client".to_string()));
}

#[tokio::test]
async fn test_default_implementations_with_multiple_items() {
    let handler = DefaultHandler;
    let ctx = create_context();

    // Test produce with multiple topics and partitions
    let produce_req = ProduceRequestData {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5000,
        topics: vec![
            ProduceTopicData {
                name: "topic1".to_string(),
                partitions: vec![
                    ProducePartitionData {
                        partition_index: 0,
                        records: Bytes::from(vec![1, 2, 3]),
                    },
                    ProducePartitionData {
                        partition_index: 1,
                        records: Bytes::from(vec![4, 5, 6]),
                    },
                ],
            },
            ProduceTopicData {
                name: "topic2".to_string(),
                partitions: vec![ProducePartitionData {
                    partition_index: 0,
                    records: Bytes::from(vec![7, 8, 9]),
                }],
            },
        ],
    };

    let produce_resp = handler.handle_produce(&ctx, produce_req).await;

    assert_eq!(produce_resp.responses.len(), 2);
    assert_eq!(produce_resp.responses[0].partitions.len(), 2);
    assert_eq!(produce_resp.responses[1].partitions.len(), 1);

    // Test fetch with multiple topics and partitions
    let fetch_req = FetchRequestData {
        replica_id: -1,
        max_wait_ms: 100,
        min_bytes: 1,
        max_bytes: 10240,
        isolation_level: 0,
        topics: vec![
            FetchTopicData {
                name: "topic1".to_string(),
                partitions: vec![
                    FetchPartitionData {
                        partition_index: 0,
                        fetch_offset: 0,
                        partition_max_bytes: 1024,
                    },
                    FetchPartitionData {
                        partition_index: 1,
                        fetch_offset: 10,
                        partition_max_bytes: 1024,
                    },
                ],
            },
            FetchTopicData {
                name: "topic2".to_string(),
                partitions: vec![FetchPartitionData {
                    partition_index: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1024,
                }],
            },
        ],
    };

    let fetch_resp = handler.handle_fetch(&ctx, fetch_req).await;

    assert_eq!(fetch_resp.responses.len(), 2);
    assert_eq!(fetch_resp.responses[0].partitions.len(), 2);
    assert_eq!(fetch_resp.responses[1].partitions.len(), 1);
}

// Note: Handler sub-trait tests are not included here because the handler_traits
// module is private. Those traits are tested through the public Handler trait
// default implementations above.
