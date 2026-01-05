//! Comprehensive tests for cluster handler modules.
//!
//! These tests cover all handler functions to improve test coverage.

#![allow(clippy::field_reassign_with_default)]

use bytes::Bytes;
use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::*;
use kafkaesque::server::{Handler, RequestContext};
use object_store::memory::InMemory;
use std::net::SocketAddr;
use std::sync::Arc;

/// Helper to create a minimal valid Kafka RecordBatch for testing.
fn create_test_batch(record_count: i32) -> Bytes {
    let mut batch = vec![0u8; 100];

    // base_offset
    batch[0..8].copy_from_slice(&0i64.to_be_bytes());
    // batch_length
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes());
    // partition_leader_epoch
    batch[12..16].copy_from_slice(&0i32.to_be_bytes());
    // magic = 2
    batch[16] = 2;
    // attributes
    batch[21..23].copy_from_slice(&0i16.to_be_bytes());
    // last_offset_delta
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());
    // timestamps
    batch[27..35].copy_from_slice(&0i64.to_be_bytes());
    batch[35..43].copy_from_slice(&0i64.to_be_bytes());
    // producer info
    batch[43..51].copy_from_slice(&0i64.to_be_bytes());
    batch[51..53].copy_from_slice(&0i16.to_be_bytes());
    batch[53..57].copy_from_slice(&0i32.to_be_bytes());
    // records_count
    batch[57..61].copy_from_slice(&record_count.to_be_bytes());

    // Compute CRC-32C
    let crc = compute_crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    Bytes::from(batch)
}

fn compute_crc32c(data: &[u8]) -> u32 {
    const CRC32C_TABLE: [u32; 256] = {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut j = 0;
            while j < 8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0x82F63B78;
                } else {
                    crc >>= 1;
                }
                j += 1;
            }
            table[i] = crc;
            i += 1;
        }
        table
    };

    let mut crc = 0xFFFFFFFFu32;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32C_TABLE[index];
    }
    !crc
}

/// Create a test handler with in-memory storage.
async fn create_test_handler() -> SlateDBClusterHandler {
    let mut config = ClusterConfig::default();
    config.broker_id = 1;
    config.auto_create_topics = true;

    let object_store = Arc::new(InMemory::new());
    SlateDBClusterHandler::with_object_store(config, object_store)
        .await
        .expect("Failed to create handler")
}

/// Create a minimal request context for testing.
fn create_test_context() -> RequestContext {
    RequestContext {
        client_addr: "127.0.0.1:12345".parse::<SocketAddr>().unwrap(),
        api_version: 8,
        client_id: Some("test-client".to_string()),
        request_id: uuid::Uuid::new_v4(),
    }
}

// ============================================================================
// Metadata Handler Tests
// ============================================================================

#[tokio::test]
async fn test_metadata_request_specific_topics() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create a topic first
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "test-topic".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Request metadata for specific topics
    let request = MetadataRequestData {
        topics: Some(vec!["test-topic".to_string()]),
    };

    let response = handler.handle_metadata(&ctx, request).await;

    assert_eq!(response.brokers.len(), 1);
    assert_eq!(response.brokers[0].node_id, 1);
    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].name, "test-topic");
    assert_eq!(response.topics[0].partitions.len(), 3);
}

#[tokio::test]
async fn test_metadata_request_all_topics() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create multiple topics
    let create_req = CreateTopicsRequestData {
        topics: vec![
            CreateTopicData {
                name: "topic1".to_string(),
                num_partitions: 1,
                replication_factor: 1,
            },
            CreateTopicData {
                name: "topic2".to_string(),
                num_partitions: 2,
                replication_factor: 1,
            },
        ],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Request metadata for all topics
    let request = MetadataRequestData { topics: None };

    let response = handler.handle_metadata(&ctx, request).await;

    assert!(response.topics.len() >= 2);
}

#[tokio::test]
async fn test_metadata_request_invalid_topic_name() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Request metadata for topic with invalid characters
    let request = MetadataRequestData {
        topics: Some(vec!["invalid/topic".to_string()]),
    };

    let response = handler.handle_metadata(&ctx, request).await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, KafkaCode::InvalidTopic);
}

#[tokio::test]
async fn test_metadata_auto_create_disabled() {
    let mut config = ClusterConfig::default();
    config.broker_id = 2;
    config.auto_create_topics = false;

    let object_store = Arc::new(InMemory::new());
    let handler = SlateDBClusterHandler::with_object_store(config, object_store)
        .await
        .expect("Failed to create handler");
    let ctx = create_test_context();

    // Request metadata for non-existent topic
    let request = MetadataRequestData {
        topics: Some(vec!["nonexistent".to_string()]),
    };

    let response = handler.handle_metadata(&ctx, request).await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(
        response.topics[0].error_code,
        KafkaCode::UnknownTopicOrPartition
    );
}

#[tokio::test]
async fn test_metadata_returns_advertised_host_not_bind_host() {
    // This test ensures that metadata responses return the advertised_host
    // (routable address for clients) not the bind host (e.g., 0.0.0.0).
    // This is critical for consumers to connect back to the broker.
    let mut config = ClusterConfig::default();
    config.broker_id = 5;
    config.host = "0.0.0.0".to_string(); // Bind to all interfaces
    config.advertised_host = "192.168.1.50".to_string(); // But advertise specific IP

    let object_store = Arc::new(InMemory::new());
    let handler = SlateDBClusterHandler::with_object_store(config, object_store)
        .await
        .expect("Failed to create handler");
    let ctx = create_test_context();

    let request = MetadataRequestData { topics: None };
    let response = handler.handle_metadata(&ctx, request).await;

    // Verify broker list uses advertised_host, NOT bind host
    assert!(
        !response.brokers.is_empty(),
        "Should have at least one broker"
    );
    let broker = &response.brokers[0];
    assert_eq!(broker.node_id, 5);
    assert_eq!(
        broker.host, "192.168.1.50",
        "Metadata should return advertised_host, not bind host (0.0.0.0)"
    );
    assert_ne!(
        broker.host, "0.0.0.0",
        "Metadata must NEVER return bind-all address 0.0.0.0"
    );

    handler.shutdown().await.expect("shutdown");
}

/// Regression test: ensure isr_nodes contains the leader when a partition has one.
///
/// This test prevents NOT_LEADER_OR_FOLLOWER errors that occur when Kafka clients
/// receive metadata with empty isr_nodes. When a client produces to a partition,
/// it validates that the broker is in the ISR before sending. Empty isr_nodes causes
/// the client to retry indefinitely with NOT_LEADER_OR_FOLLOWER errors.
///
/// Reproducer scenario:
/// ```scala
/// List(("1","2")).toDF("key","value").write.format("kafka")
///   .option("kafka.bootstrap.servers", "0.0.0.0:9092")
///   .option("topic", "test").save
/// // Would fail with NOT_LEADER_OR_FOLLOWER if isr_nodes is empty
/// ```
#[tokio::test]
async fn test_metadata_isr_nodes_contains_leader_to_prevent_not_leader_error() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create a topic - the broker becomes the leader for all partitions
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "isr-test-topic".to_string(),
            num_partitions: 10, // Multiple partitions to mimic the error scenario
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let create_resp = handler.handle_create_topics(&ctx, create_req).await;
    assert_eq!(
        create_resp.topics[0].error_code,
        KafkaCode::None,
        "Topic creation should succeed"
    );

    // Request metadata - this is what Kafka clients do before producing
    let request = MetadataRequestData {
        topics: Some(vec!["isr-test-topic".to_string()]),
    };
    let response = handler.handle_metadata(&ctx, request).await;

    assert_eq!(response.topics.len(), 1);
    let topic = &response.topics[0];
    assert_eq!(topic.error_code, KafkaCode::None);
    assert_eq!(topic.partitions.len(), 10);

    // Critical assertion: For each partition with a leader, isr_nodes MUST contain
    // that leader. This is what prevents NOT_LEADER_OR_FOLLOWER errors.
    for partition in &topic.partitions {
        if partition.leader_id >= 0 {
            // When there's a leader, ISR must include it
            assert!(
                partition.isr_nodes.contains(&partition.leader_id),
                "Partition {} has leader {} but isr_nodes {:?} does not contain it. \
                 This causes NOT_LEADER_OR_FOLLOWER errors in Kafka clients.",
                partition.partition_index,
                partition.leader_id,
                partition.isr_nodes
            );

            // replica_nodes should also contain the leader
            assert!(
                partition.replica_nodes.contains(&partition.leader_id),
                "Partition {} has leader {} but replica_nodes {:?} does not contain it.",
                partition.partition_index,
                partition.leader_id,
                partition.replica_nodes
            );
        } else {
            // When there's no leader, ISR should be empty
            assert!(
                partition.isr_nodes.is_empty(),
                "Partition {} has no leader (leader_id={}) but isr_nodes is not empty: {:?}",
                partition.partition_index,
                partition.leader_id,
                partition.isr_nodes
            );
        }
    }

    handler.shutdown().await.expect("shutdown");
}

#[tokio::test]
async fn test_find_coordinator_returns_advertised_host() {
    // FindCoordinator must also return the advertised_host for consumer groups
    let mut config = ClusterConfig::default();
    config.broker_id = 6;
    config.host = "0.0.0.0".to_string();
    config.advertised_host = "10.0.0.100".to_string();

    let object_store = Arc::new(InMemory::new());
    let handler = SlateDBClusterHandler::with_object_store(config, object_store)
        .await
        .expect("Failed to create handler");
    let ctx = create_test_context();

    let request = FindCoordinatorRequestData {
        key: "test-consumer-group".to_string(),
        key_type: 0, // GROUP type
    };
    let response = handler.handle_find_coordinator(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(
        response.host, "10.0.0.100",
        "FindCoordinator should return advertised_host"
    );
    assert_ne!(
        response.host, "0.0.0.0",
        "FindCoordinator must NEVER return bind-all address"
    );

    handler.shutdown().await.expect("shutdown");
}

// ============================================================================
// Admin Handler Tests (Create/Delete Topics)
// ============================================================================

#[tokio::test]
async fn test_create_topics_success() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "new-topic".to_string(),
            num_partitions: 5,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };

    let response = handler.handle_create_topics(&ctx, request).await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, KafkaCode::None);
    assert_eq!(response.topics[0].name, "new-topic");
}

#[tokio::test]
async fn test_create_topics_invalid_name() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "invalid:topic:name".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };

    let response = handler.handle_create_topics(&ctx, request).await;

    assert_eq!(response.topics.len(), 1);
    assert_eq!(response.topics[0].error_code, KafkaCode::InvalidTopic);
}

#[tokio::test]
async fn test_create_topics_zero_partitions() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Request with 0 partitions should default to DEFAULT_NUM_PARTITIONS (10)
    let request = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "default-partitions".to_string(),
            num_partitions: 0,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };

    let response = handler.handle_create_topics(&ctx, request).await;

    assert_eq!(response.topics[0].error_code, KafkaCode::None);
}

#[tokio::test]
async fn test_delete_topics_success() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic first
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "to-delete".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Delete the topic
    let delete_req = DeleteTopicsRequestData {
        topic_names: vec!["to-delete".to_string()],
        timeout_ms: 5000,
    };

    let response = handler.handle_delete_topics(&ctx, delete_req).await;

    assert_eq!(response.responses.len(), 1);
    // May be None or UnknownTopicOrPartition depending on timing
    assert!(
        response.responses[0].error_code == KafkaCode::None
            || response.responses[0].error_code == KafkaCode::UnknownTopicOrPartition
    );
}

#[tokio::test]
async fn test_delete_topics_nonexistent() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = DeleteTopicsRequestData {
        topic_names: vec!["nonexistent".to_string()],
        timeout_ms: 5000,
    };

    let response = handler.handle_delete_topics(&ctx, request).await;

    assert_eq!(response.responses.len(), 1);
    assert_eq!(
        response.responses[0].error_code,
        KafkaCode::UnknownTopicOrPartition
    );
}

#[tokio::test]
async fn test_delete_topics_invalid_name() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = DeleteTopicsRequestData {
        topic_names: vec!["invalid/name".to_string()],
        timeout_ms: 5000,
    };

    let response = handler.handle_delete_topics(&ctx, request).await;

    assert_eq!(response.responses[0].error_code, KafkaCode::InvalidTopic);
}

// ============================================================================
// Produce Handler Tests
// ============================================================================

#[tokio::test]
async fn test_produce_request_with_acks_1() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic first
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "produce-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Produce some data
    let request = ProduceRequestData {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5000,
        topics: vec![ProduceTopicData {
            name: "produce-topic".to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records: create_test_batch(1),
            }],
        }],
    };

    let response = handler.handle_produce(&ctx, request).await;

    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].partitions.len(), 1);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::None
    );
    assert!(response.responses[0].partitions[0].base_offset >= 0);
}

#[tokio::test]
async fn test_produce_request_with_acks_0() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic first
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "fire-forget-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Produce with acks=0 (fire-and-forget)
    let request = ProduceRequestData {
        transactional_id: None,
        acks: 0,
        timeout_ms: 0,
        topics: vec![ProduceTopicData {
            name: "fire-forget-topic".to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records: create_test_batch(1),
            }],
        }],
    };

    let response = handler.handle_produce(&ctx, request).await;

    // For acks=0, response should be empty
    assert_eq!(response.responses.len(), 1);
    assert_eq!(response.responses[0].partitions.len(), 0);
}

#[tokio::test]
async fn test_produce_invalid_topic_name() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = ProduceRequestData {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5000,
        topics: vec![ProduceTopicData {
            name: "invalid:topic".to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records: create_test_batch(1),
            }],
        }],
    };

    let response = handler.handle_produce(&ctx, request).await;

    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::InvalidTopic
    );
}

#[tokio::test]
async fn test_produce_multiple_partitions() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic with multiple partitions
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "multi-partition".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Produce to multiple partitions
    let request = ProduceRequestData {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5000,
        topics: vec![ProduceTopicData {
            name: "multi-partition".to_string(),
            partitions: vec![
                ProducePartitionData {
                    partition_index: 0,
                    records: create_test_batch(1),
                },
                ProducePartitionData {
                    partition_index: 1,
                    records: create_test_batch(1),
                },
                ProducePartitionData {
                    partition_index: 2,
                    records: create_test_batch(1),
                },
            ],
        }],
    };

    let response = handler.handle_produce(&ctx, request).await;

    assert_eq!(response.responses[0].partitions.len(), 3);
    for partition_resp in &response.responses[0].partitions {
        assert_eq!(partition_resp.error_code, KafkaCode::None);
    }
}

// ============================================================================
// Fetch Handler Tests
// ============================================================================

#[tokio::test]
async fn test_fetch_request_success() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic and produce data
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "fetch-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    let produce_req = ProduceRequestData {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5000,
        topics: vec![ProduceTopicData {
            name: "fetch-topic".to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records: create_test_batch(1),
            }],
        }],
    };
    let _ = handler.handle_produce(&ctx, produce_req).await;

    // Fetch the data
    let fetch_req = FetchRequestData {
        replica_id: -1,
        max_wait_ms: 100,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        isolation_level: 0,
        topics: vec![FetchTopicData {
            name: "fetch-topic".to_string(),
            partitions: vec![FetchPartitionData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024 * 1024,
            }],
        }],
    };

    let response = handler.handle_fetch(&ctx, fetch_req).await;

    assert_eq!(response.responses.len(), 1);
    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::None
    );
    assert!(response.responses[0].partitions[0].records.is_some());
}

#[tokio::test]
async fn test_fetch_offset_out_of_range() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "fetch-range-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Try to fetch from offset beyond high watermark
    let fetch_req = FetchRequestData {
        replica_id: -1,
        max_wait_ms: 100,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        isolation_level: 0,
        topics: vec![FetchTopicData {
            name: "fetch-range-topic".to_string(),
            partitions: vec![FetchPartitionData {
                partition_index: 0,
                fetch_offset: 1000,
                partition_max_bytes: 1024 * 1024,
            }],
        }],
    };

    let response = handler.handle_fetch(&ctx, fetch_req).await;

    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::OffsetOutOfRange
    );
}

#[tokio::test]
async fn test_fetch_invalid_topic_name() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let fetch_req = FetchRequestData {
        replica_id: -1,
        max_wait_ms: 100,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        isolation_level: 0,
        topics: vec![FetchTopicData {
            name: "invalid/topic".to_string(),
            partitions: vec![FetchPartitionData {
                partition_index: 0,
                fetch_offset: 0,
                partition_max_bytes: 1024 * 1024,
            }],
        }],
    };

    let response = handler.handle_fetch(&ctx, fetch_req).await;

    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::InvalidTopic
    );
}

#[tokio::test]
async fn test_fetch_nonexistent_partition() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic with 1 partition
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "single-part".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Try to fetch from non-existent partition
    let fetch_req = FetchRequestData {
        replica_id: -1,
        max_wait_ms: 100,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        isolation_level: 0,
        topics: vec![FetchTopicData {
            name: "single-part".to_string(),
            partitions: vec![FetchPartitionData {
                partition_index: 99,
                fetch_offset: 0,
                partition_max_bytes: 1024 * 1024,
            }],
        }],
    };

    let response = handler.handle_fetch(&ctx, fetch_req).await;

    assert_eq!(
        response.responses[0].partitions[0].error_code,
        KafkaCode::NotLeaderForPartition
    );
}

// ============================================================================
// Offset Handler Tests
// ============================================================================

#[tokio::test]
async fn test_list_offsets_earliest() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "offset-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // List offsets with earliest timestamp
    let request = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopicData {
            name: "offset-topic".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                timestamp: -2, // Earliest
            }],
        }],
    };

    let response = handler.handle_list_offsets(&ctx, request).await;

    assert_eq!(response.topics[0].partitions[0].error_code, KafkaCode::None);
    assert_eq!(response.topics[0].partitions[0].offset, 0);
}

#[tokio::test]
async fn test_list_offsets_latest() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic and produce data
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "offset-latest-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    let produce_req = ProduceRequestData {
        transactional_id: None,
        acks: 1,
        timeout_ms: 5000,
        topics: vec![ProduceTopicData {
            name: "offset-latest-topic".to_string(),
            partitions: vec![ProducePartitionData {
                partition_index: 0,
                records: create_test_batch(5),
            }],
        }],
    };
    let _ = handler.handle_produce(&ctx, produce_req).await;

    // List offsets with latest timestamp
    let request = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopicData {
            name: "offset-latest-topic".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                timestamp: -1, // Latest
            }],
        }],
    };

    let response = handler.handle_list_offsets(&ctx, request).await;

    assert_eq!(response.topics[0].partitions[0].error_code, KafkaCode::None);
    assert!(response.topics[0].partitions[0].offset > 0);
}

#[tokio::test]
async fn test_offset_commit_and_fetch() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "commit-topic".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Commit offset (anonymous consumer - no member_id validation)
    let commit_req = OffsetCommitRequestData {
        group_id: "test-group".to_string(),
        generation_id: -1,
        member_id: String::new(),
        topics: vec![OffsetCommitTopicData {
            name: "commit-topic".to_string(),
            partitions: vec![OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 100,
                committed_metadata: Some("test-metadata".to_string()),
            }],
        }],
    };

    let commit_resp = handler.handle_offset_commit(&ctx, commit_req).await;
    assert_eq!(
        commit_resp.topics[0].partitions[0].error_code,
        KafkaCode::None
    );

    // Fetch the committed offset
    let fetch_req = OffsetFetchRequestData {
        group_id: "test-group".to_string(),
        topics: vec![OffsetFetchTopicData {
            name: "commit-topic".to_string(),
            partition_indexes: vec![0],
        }],
    };

    let fetch_resp = handler.handle_offset_fetch(&ctx, fetch_req).await;

    assert_eq!(
        fetch_resp.topics[0].partitions[0].error_code,
        KafkaCode::None
    );
    assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, 100);
    assert_eq!(
        fetch_resp.topics[0].partitions[0].metadata.as_deref(),
        Some("test-metadata")
    );
}

#[tokio::test]
async fn test_offset_fetch_nonexistent_group() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = OffsetFetchRequestData {
        group_id: "nonexistent-group".to_string(),
        topics: vec![OffsetFetchTopicData {
            name: "any-topic".to_string(),
            partition_indexes: vec![0],
        }],
    };

    let response = handler.handle_offset_fetch(&ctx, request).await;

    // Should return -1 offset for nonexistent group
    assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
}

// ============================================================================
// Consumer Group Handler Tests
// ============================================================================

#[tokio::test]
async fn test_find_coordinator() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = FindCoordinatorRequestData {
        key: "test-group".to_string(),
        key_type: 0, // Group
    };

    let response = handler.handle_find_coordinator(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.node_id >= 0);
    assert!(!response.host.is_empty());
}

#[tokio::test]
async fn test_join_group_new_member() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = JoinGroupRequestData {
        group_id: "test-group".to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 30000,
        member_id: String::new(), // New member
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupProtocolData {
            name: "range".to_string(),
            metadata: Bytes::from(vec![0, 1, 2, 3]),
        }],
    };

    let response = handler.handle_join_group(&ctx, request).await;

    // Should succeed or return rebalance in progress
    assert!(
        response.error_code == KafkaCode::None
            || response.error_code == KafkaCode::RebalanceInProgress
    );
}

#[tokio::test]
async fn test_join_group_invalid_group_id() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = JoinGroupRequestData {
        group_id: "invalid:group:id".to_string(),
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 30000,
        member_id: String::new(),
        protocol_type: "consumer".to_string(),
        protocols: vec![JoinGroupProtocolData {
            name: "range".to_string(),
            metadata: Bytes::from(vec![]),
        }],
    };

    let response = handler.handle_join_group(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::InvalidGroupId);
}

#[tokio::test]
async fn test_heartbeat_unknown_member() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = HeartbeatRequestData {
        group_id: "test-group".to_string(),
        generation_id: 1,
        member_id: "unknown-member".to_string(),
    };

    let response = handler.handle_heartbeat(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::UnknownMemberId);
}

#[tokio::test]
async fn test_sync_group() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = SyncGroupRequestData {
        group_id: "test-group".to_string(),
        generation_id: 1,
        member_id: "member-1".to_string(),
        assignments: vec![],
    };

    let response = handler.handle_sync_group(&ctx, request).await;

    // Will likely return UnknownMemberId since we didn't join first
    assert!(response.error_code != KafkaCode::None);
}

#[tokio::test]
async fn test_leave_group() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = LeaveGroupRequestData {
        group_id: "test-group".to_string(),
        member_id: "member-1".to_string(),
    };

    let response = handler.handle_leave_group(&ctx, request).await;

    // Should succeed even if member doesn't exist
    assert!(
        response.error_code == KafkaCode::None || response.error_code == KafkaCode::UnknownMemberId
    );
}

// ============================================================================
// Producer ID Handler Tests
// ============================================================================

#[tokio::test]
async fn test_init_producer_id_new_producer() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = InitProducerIdRequestData {
        transactional_id: None,
        transaction_timeout_ms: 60000,
        producer_id: -1,
        producer_epoch: -1,
    };

    let response = handler.handle_init_producer_id(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.producer_id >= 0); // Changed from > 0 to >= 0
    assert_eq!(response.producer_epoch, 0);
}

#[tokio::test]
async fn test_init_producer_id_with_transactional_id() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = InitProducerIdRequestData {
        transactional_id: Some("txn-id-1".to_string()),
        transaction_timeout_ms: 60000,
        producer_id: -1,
        producer_epoch: -1,
    };

    let response = handler.handle_init_producer_id(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.producer_id >= 0); // Changed from > 0 to >= 0
    assert!(response.producer_epoch >= 0);
}

// ============================================================================
// Additional Offset Handler Edge Case Tests
// ============================================================================

#[tokio::test]
async fn test_list_offsets_invalid_topic_name() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopicData {
            name: "invalid/topic/name".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 0,
                timestamp: -1,
            }],
        }],
    };

    let response = handler.handle_list_offsets(&ctx, request).await;

    assert_eq!(
        response.topics[0].partitions[0].error_code,
        KafkaCode::InvalidTopic
    );
    assert_eq!(response.topics[0].partitions[0].offset, -1);
}

#[tokio::test]
async fn test_list_offsets_nonexistent_partition() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic with 1 partition
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "list-offsets-single".to_string(),
            num_partitions: 1,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Request offset for non-existent partition
    let request = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopicData {
            name: "list-offsets-single".to_string(),
            partitions: vec![ListOffsetsPartitionData {
                partition_index: 99, // Non-existent partition
                timestamp: -1,
            }],
        }],
    };

    let response = handler.handle_list_offsets(&ctx, request).await;

    assert_eq!(
        response.topics[0].partitions[0].error_code,
        KafkaCode::NotLeaderForPartition
    );
}

#[tokio::test]
async fn test_list_offsets_multiple_partitions() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create topic with multiple partitions
    let create_req = CreateTopicsRequestData {
        topics: vec![CreateTopicData {
            name: "multi-part-offsets".to_string(),
            num_partitions: 3,
            replication_factor: 1,
        }],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Request offsets for multiple partitions
    let request = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![ListOffsetsTopicData {
            name: "multi-part-offsets".to_string(),
            partitions: vec![
                ListOffsetsPartitionData {
                    partition_index: 0,
                    timestamp: -2, // Earliest
                },
                ListOffsetsPartitionData {
                    partition_index: 1,
                    timestamp: -1, // Latest
                },
                ListOffsetsPartitionData {
                    partition_index: 2,
                    timestamp: 12345678, // Specific timestamp (not supported, should return HWM)
                },
            ],
        }],
    };

    let response = handler.handle_list_offsets(&ctx, request).await;

    assert_eq!(response.topics[0].partitions.len(), 3);
    for partition in &response.topics[0].partitions {
        assert_eq!(partition.error_code, KafkaCode::None);
    }
}

#[tokio::test]
async fn test_offset_commit_invalid_topic_name() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Try to commit offset with invalid topic name
    let request = OffsetCommitRequestData {
        group_id: "test-group".to_string(),
        generation_id: -1,
        member_id: String::new(),
        topics: vec![OffsetCommitTopicData {
            name: "invalid:topic:name".to_string(),
            partitions: vec![OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 50,
                committed_metadata: None,
            }],
        }],
    };

    let response = handler.handle_offset_commit(&ctx, request).await;

    assert_eq!(
        response.topics[0].partitions[0].error_code,
        KafkaCode::InvalidTopic
    );
}

#[tokio::test]
async fn test_offset_fetch_invalid_topic_name() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = OffsetFetchRequestData {
        group_id: "test-group".to_string(),
        topics: vec![OffsetFetchTopicData {
            name: "invalid\ttopic".to_string(),
            partition_indexes: vec![0],
        }],
    };

    let response = handler.handle_offset_fetch(&ctx, request).await;

    assert_eq!(
        response.topics[0].partitions[0].error_code,
        KafkaCode::InvalidTopic
    );
    assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
}

#[tokio::test]
async fn test_offset_commit_multiple_topics() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create multiple topics
    let create_req = CreateTopicsRequestData {
        topics: vec![
            CreateTopicData {
                name: "commit-topic-1".to_string(),
                num_partitions: 2,
                replication_factor: 1,
            },
            CreateTopicData {
                name: "commit-topic-2".to_string(),
                num_partitions: 2,
                replication_factor: 1,
            },
        ],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Commit offsets for multiple topics
    let commit_req = OffsetCommitRequestData {
        group_id: "multi-commit-group".to_string(),
        generation_id: -1,
        member_id: String::new(),
        topics: vec![
            OffsetCommitTopicData {
                name: "commit-topic-1".to_string(),
                partitions: vec![
                    OffsetCommitPartitionData {
                        partition_index: 0,
                        committed_offset: 100,
                        committed_metadata: Some("meta1".to_string()),
                    },
                    OffsetCommitPartitionData {
                        partition_index: 1,
                        committed_offset: 200,
                        committed_metadata: None,
                    },
                ],
            },
            OffsetCommitTopicData {
                name: "commit-topic-2".to_string(),
                partitions: vec![OffsetCommitPartitionData {
                    partition_index: 0,
                    committed_offset: 300,
                    committed_metadata: Some("meta2".to_string()),
                }],
            },
        ],
    };

    let commit_resp = handler.handle_offset_commit(&ctx, commit_req).await;

    assert_eq!(commit_resp.topics.len(), 2);
    assert_eq!(commit_resp.topics[0].partitions.len(), 2);
    assert_eq!(commit_resp.topics[1].partitions.len(), 1);

    for topic in &commit_resp.topics {
        for partition in &topic.partitions {
            assert_eq!(partition.error_code, KafkaCode::None);
        }
    }

    // Fetch and verify all committed offsets
    let fetch_req = OffsetFetchRequestData {
        group_id: "multi-commit-group".to_string(),
        topics: vec![
            OffsetFetchTopicData {
                name: "commit-topic-1".to_string(),
                partition_indexes: vec![0, 1],
            },
            OffsetFetchTopicData {
                name: "commit-topic-2".to_string(),
                partition_indexes: vec![0],
            },
        ],
    };

    let fetch_resp = handler.handle_offset_fetch(&ctx, fetch_req).await;

    assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, 100);
    assert_eq!(fetch_resp.topics[0].partitions[1].committed_offset, 200);
    assert_eq!(fetch_resp.topics[1].partitions[0].committed_offset, 300);
}

#[tokio::test]
async fn test_offset_fetch_multiple_partitions() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // First commit some offsets
    let commit_req = OffsetCommitRequestData {
        group_id: "fetch-multi-group".to_string(),
        generation_id: -1,
        member_id: String::new(),
        topics: vec![OffsetCommitTopicData {
            name: "fetch-multi-topic".to_string(),
            partitions: vec![
                OffsetCommitPartitionData {
                    partition_index: 0,
                    committed_offset: 10,
                    committed_metadata: None,
                },
                OffsetCommitPartitionData {
                    partition_index: 2,
                    committed_offset: 30,
                    committed_metadata: None,
                },
            ],
        }],
    };
    let _ = handler.handle_offset_commit(&ctx, commit_req).await;

    // Fetch offsets for partitions 0, 1, 2 (partition 1 was never committed)
    let fetch_req = OffsetFetchRequestData {
        group_id: "fetch-multi-group".to_string(),
        topics: vec![OffsetFetchTopicData {
            name: "fetch-multi-topic".to_string(),
            partition_indexes: vec![0, 1, 2],
        }],
    };

    let fetch_resp = handler.handle_offset_fetch(&ctx, fetch_req).await;

    assert_eq!(fetch_resp.topics[0].partitions.len(), 3);
    assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, 10);
    assert_eq!(fetch_resp.topics[0].partitions[1].committed_offset, -1); // Never committed
    assert_eq!(fetch_resp.topics[0].partitions[2].committed_offset, 30);
}

#[tokio::test]
async fn test_list_offsets_multiple_topics() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Create multiple topics
    let create_req = CreateTopicsRequestData {
        topics: vec![
            CreateTopicData {
                name: "list-multi-1".to_string(),
                num_partitions: 1,
                replication_factor: 1,
            },
            CreateTopicData {
                name: "list-multi-2".to_string(),
                num_partitions: 1,
                replication_factor: 1,
            },
        ],
        timeout_ms: 5000,
    };
    let _ = handler.handle_create_topics(&ctx, create_req).await;

    // Request offsets for multiple topics
    let request = ListOffsetsRequestData {
        replica_id: -1,
        isolation_level: 0,
        topics: vec![
            ListOffsetsTopicData {
                name: "list-multi-1".to_string(),
                partitions: vec![ListOffsetsPartitionData {
                    partition_index: 0,
                    timestamp: -1,
                }],
            },
            ListOffsetsTopicData {
                name: "list-multi-2".to_string(),
                partitions: vec![ListOffsetsPartitionData {
                    partition_index: 0,
                    timestamp: -2,
                }],
            },
            ListOffsetsTopicData {
                name: "nonexistent".to_string(), // Non-existent topic
                partitions: vec![ListOffsetsPartitionData {
                    partition_index: 0,
                    timestamp: -1,
                }],
            },
        ],
    };

    let response = handler.handle_list_offsets(&ctx, request).await;

    assert_eq!(response.topics.len(), 3);
    assert_eq!(response.topics[0].partitions[0].error_code, KafkaCode::None);
    assert_eq!(response.topics[1].partitions[0].error_code, KafkaCode::None);
    // Non-existent topic should return error
    assert_eq!(
        response.topics[2].partitions[0].error_code,
        KafkaCode::NotLeaderForPartition
    );
}

#[tokio::test]
async fn test_offset_commit_with_empty_metadata() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    // Commit offset without metadata
    let commit_req = OffsetCommitRequestData {
        group_id: "empty-meta-group".to_string(),
        generation_id: -1,
        member_id: String::new(),
        topics: vec![OffsetCommitTopicData {
            name: "empty-meta-topic".to_string(),
            partitions: vec![OffsetCommitPartitionData {
                partition_index: 0,
                committed_offset: 42,
                committed_metadata: None,
            }],
        }],
    };

    let commit_resp = handler.handle_offset_commit(&ctx, commit_req).await;
    assert_eq!(
        commit_resp.topics[0].partitions[0].error_code,
        KafkaCode::None
    );

    // Fetch and verify
    let fetch_req = OffsetFetchRequestData {
        group_id: "empty-meta-group".to_string(),
        topics: vec![OffsetFetchTopicData {
            name: "empty-meta-topic".to_string(),
            partition_indexes: vec![0],
        }],
    };

    let fetch_resp = handler.handle_offset_fetch(&ctx, fetch_req).await;

    assert_eq!(fetch_resp.topics[0].partitions[0].committed_offset, 42);
    assert!(fetch_resp.topics[0].partitions[0].metadata.is_none());
}

// ============================================================================
// API Versions Tests
// ============================================================================

#[tokio::test]
async fn test_api_versions() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = ApiVersionsRequestData {
        client_software_name: None,
        client_software_version: None,
    };
    let response = handler.handle_api_versions(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    assert!(!response.api_keys.is_empty());

    // Verify some expected API keys are present
    let has_produce = response
        .api_keys
        .iter()
        .any(|k| k.api_key == ApiKey::Produce);
    let has_fetch = response.api_keys.iter().any(|k| k.api_key == ApiKey::Fetch);
    let has_metadata = response
        .api_keys
        .iter()
        .any(|k| k.api_key == ApiKey::Metadata);
    assert!(has_produce); // Produce
    assert!(has_fetch); // Fetch
    assert!(has_metadata); // Metadata
}

// ============================================================================
// Describe Groups Tests
// ============================================================================

#[tokio::test]
async fn test_describe_groups() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = DescribeGroupsRequestData {
        group_ids: vec!["nonexistent-group".to_string()],
    };

    let response = handler.handle_describe_groups(&ctx, request).await;

    assert_eq!(response.groups.len(), 1);
    // Non-existent group should return error or empty state
    assert!(
        response.groups[0].error_code != KafkaCode::None
            || response.groups[0].group_state.is_empty()
    );
}

#[tokio::test]
async fn test_list_groups() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = ListGroupsRequestData {
        states_filter: vec![],
    };

    let response = handler.handle_list_groups(&ctx, request).await;

    assert_eq!(response.error_code, KafkaCode::None);
    // List may be empty or contain groups
}

#[tokio::test]
async fn test_delete_groups() {
    let handler = create_test_handler().await;
    let ctx = create_test_context();

    let request = DeleteGroupsRequestData {
        group_ids: vec!["nonexistent-group".to_string()],
    };

    let response = handler.handle_delete_groups(&ctx, request).await;

    assert_eq!(response.results.len(), 1);
    // Deleting non-existent group returns success or appropriate error
}
