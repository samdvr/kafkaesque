//! Inline tests previously embedded in `mod.rs`. Moved to a sibling
//! file so the implementation isn't gated on scrolling past the test
//! block. `super::*` re-exports private helpers the tests rely on.

#![allow(clippy::module_inception)]
#![allow(unused_imports)]

use super::*;

use super::*;
use crate::protocol::CrcValidationResult;

// ========================================================================
// HealthStatus Tests
// ========================================================================

#[test]
fn test_health_status_debug() {
    assert!(format!("{:?}", HealthStatus::Healthy).contains("Healthy"));
    assert!(format!("{:?}", HealthStatus::Zombie).contains("Zombie"));
    assert!(format!("{:?}", HealthStatus::Degraded).contains("Degraded"));
    assert!(format!("{:?}", HealthStatus::ShuttingDown).contains("ShuttingDown"));
}

#[test]
fn test_health_status_clone() {
    let healthy = HealthStatus::Healthy;
    let cloned = healthy;
    assert_eq!(healthy, cloned);
}

#[test]
fn test_health_status_copy() {
    let healthy = HealthStatus::Healthy;
    let copied: HealthStatus = healthy; // Copy, not move
    assert_eq!(healthy, copied);
}

#[test]
fn test_health_status_equality() {
    assert_eq!(HealthStatus::Healthy, HealthStatus::Healthy);
    assert_eq!(HealthStatus::Zombie, HealthStatus::Zombie);
    assert_eq!(HealthStatus::Degraded, HealthStatus::Degraded);
    assert_eq!(HealthStatus::ShuttingDown, HealthStatus::ShuttingDown);

    assert_ne!(HealthStatus::Healthy, HealthStatus::Zombie);
    assert_ne!(HealthStatus::Healthy, HealthStatus::Degraded);
    assert_ne!(HealthStatus::Healthy, HealthStatus::ShuttingDown);
    assert_ne!(HealthStatus::Zombie, HealthStatus::Degraded);
}

#[test]
fn test_health_status_all_variants() {
    // Ensure we can create all variants
    let statuses = [
        HealthStatus::Healthy,
        HealthStatus::Zombie,
        HealthStatus::Degraded,
        HealthStatus::ShuttingDown,
    ];

    assert_eq!(statuses.len(), 4);

    // All should be distinct
    for (i, s1) in statuses.iter().enumerate() {
        for (j, s2) in statuses.iter().enumerate() {
            if i == j {
                assert_eq!(s1, s2);
            } else {
                assert_ne!(s1, s2);
            }
        }
    }
}

// ========================================================================
// Topic Name Cache Tests
// ========================================================================

#[test]
fn test_topic_name_cache_creation() {
    // Test that moka Cache can be created with expected capacity
    let cache: Cache<String, Arc<str>> = Cache::new(10_000);
    assert_eq!(cache.entry_count(), 0);
}

#[test]
fn test_arc_str_caching() {
    // Test the pattern used in cached_topic_name
    let cache: Cache<String, Arc<str>> = Cache::new(100);

    // First access creates the Arc
    let topic1 = cache.get_with("test-topic".to_string(), || Arc::from("test-topic"));
    assert_eq!(topic1.as_ref(), "test-topic");

    // Second access returns cached Arc (same pointer)
    let topic2 = cache.get_with("test-topic".to_string(), || Arc::from("test-topic"));
    assert!(Arc::ptr_eq(&topic1, &topic2));

    // Different topic creates different Arc
    let topic3 = cache.get_with("other-topic".to_string(), || Arc::from("other-topic"));
    assert_eq!(topic3.as_ref(), "other-topic");
    assert!(!Arc::ptr_eq(&topic1, &topic3));
}

#[test]
fn test_arc_str_cache_many_topics() {
    let cache: Cache<String, Arc<str>> = Cache::new(10);

    // Add more topics than cache capacity
    for i in 0..20 {
        let name = format!("topic-{}", i);
        let arc = cache.get_with(name.clone(), || Arc::from(name.as_str()));
        assert_eq!(arc.as_ref(), name.as_str());
    }

    // moka cache may defer eviction, so entry count could be 0 or more
    // Just verify we can call entry_count without error
    let _count = cache.entry_count();
}

// ========================================================================
// CRC Validation Response Tests
// ========================================================================

#[test]
fn test_crc_validation_result_variants() {
    // Test that CrcValidationResult can be used in pattern matching
    let valid = CrcValidationResult::Valid;
    let invalid = CrcValidationResult::Invalid {
        expected: 0x12345678,
        actual: 0x87654321,
    };
    let too_small = CrcValidationResult::TooSmall;

    match valid {
        CrcValidationResult::Valid => {}
        _ => panic!("Expected Valid"),
    }

    match invalid {
        CrcValidationResult::Invalid { expected, actual } => {
            assert_eq!(expected, 0x12345678);
            assert_eq!(actual, 0x87654321);
        }
        _ => panic!("Expected Invalid"),
    }

    match too_small {
        CrcValidationResult::TooSmall => {}
        _ => panic!("Expected TooSmall"),
    }
}

// ========================================================================
// Produce Partition Response Tests
// ========================================================================

#[test]
fn test_produce_partition_response_zombie_mode() {
    // Verify the structure of a zombie mode response
    let response = ProducePartitionResponse {
        partition_index: 5,
        error_code: KafkaCode::NotLeaderForPartition,
        base_offset: -1,
        log_append_time: -1,
        log_start_offset: -1,
        record_errors: vec![],
        error_message: None,
    };

    assert_eq!(response.partition_index, 5);
    assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
    assert_eq!(response.base_offset, -1);
    assert_eq!(response.log_append_time, -1);
}

#[test]
fn test_produce_partition_response_success() {
    let response = ProducePartitionResponse {
        partition_index: 0,
        error_code: KafkaCode::None,
        base_offset: 42,
        log_append_time: -1,
        log_start_offset: -1,
        record_errors: vec![],
        error_message: None,
    };

    assert_eq!(response.partition_index, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.base_offset, 42);
}

#[test]
fn test_produce_partition_response_corrupt_message() {
    let response = ProducePartitionResponse {
        partition_index: 3,
        error_code: KafkaCode::CorruptMessage,
        base_offset: -1,
        log_append_time: -1,
        log_start_offset: -1,
        record_errors: vec![],
        error_message: None,
    };

    assert_eq!(response.error_code, KafkaCode::CorruptMessage);
}

// ========================================================================
// Partition Metadata Tests
// ========================================================================

#[test]
fn test_partition_metadata_structure() {
    let meta = PartitionMetadata {
        error_code: KafkaCode::None,
        partition_index: 0,
        leader_id: 1,
        replica_nodes: vec![1],
        isr_nodes: vec![1],
        leader_epoch: -1,
        offline_replicas: vec![],
    };

    assert_eq!(meta.error_code, KafkaCode::None);
    assert_eq!(meta.partition_index, 0);
    assert_eq!(meta.leader_id, 1);
    assert_eq!(meta.replica_nodes, vec![1]);
    assert_eq!(meta.isr_nodes, vec![1]);
}

#[test]
fn test_topic_metadata_structure() {
    let partitions = vec![
        PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 0,
            leader_id: 1,
            replica_nodes: vec![1],
            isr_nodes: vec![1],
            leader_epoch: -1,
            offline_replicas: vec![],
        },
        PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 1,
            leader_id: 2,
            replica_nodes: vec![2],
            isr_nodes: vec![2],
            leader_epoch: -1,
            offline_replicas: vec![],
        },
    ];

    let topic = TopicMetadata {
        error_code: KafkaCode::None,
        name: "my-topic".to_string(),
        is_internal: false,
        partitions,
        topic_authorized_operations: 0,
    };

    assert_eq!(topic.name, "my-topic");
    assert!(!topic.is_internal);
    assert_eq!(topic.partitions.len(), 2);
    assert_eq!(topic.partitions[0].partition_index, 0);
    assert_eq!(topic.partitions[1].partition_index, 1);
}

// ========================================================================
// BrokerId Tests
// ========================================================================

#[test]
fn test_broker_id_new() {
    let id = BrokerId::new(42);
    assert_eq!(id.value(), 42);
}

#[test]
fn test_broker_id_zero() {
    let id = BrokerId::new(0);
    assert_eq!(id.value(), 0);
}

#[test]
fn test_broker_id_negative() {
    let id = BrokerId::new(-1);
    assert_eq!(id.value(), -1);
}

#[test]
fn test_broker_id_max() {
    let id = BrokerId::new(i32::MAX);
    assert_eq!(id.value(), i32::MAX);
}

// ========================================================================
// Topic Metadata Response Tests
// ========================================================================

#[test]
fn test_topic_metadata_internal_topic() {
    let topic = TopicMetadata {
        error_code: KafkaCode::None,
        name: "__consumer_offsets".to_string(),
        is_internal: true,
        partitions: vec![],
        topic_authorized_operations: 0,
    };

    assert!(topic.is_internal);
    assert_eq!(topic.name, "__consumer_offsets");
}

#[test]
fn test_topic_metadata_error() {
    let topic = TopicMetadata {
        error_code: KafkaCode::UnknownTopicOrPartition,
        name: "nonexistent".to_string(),
        is_internal: false,
        partitions: vec![],
        topic_authorized_operations: 0,
    };

    assert_eq!(topic.error_code, KafkaCode::UnknownTopicOrPartition);
    assert!(topic.partitions.is_empty());
}

#[test]
fn test_partition_metadata_with_replicas() {
    let meta = PartitionMetadata {
        error_code: KafkaCode::None,
        partition_index: 5,
        leader_id: 1,
        replica_nodes: vec![1, 2, 3],
        isr_nodes: vec![1, 2],
        leader_epoch: -1,
        offline_replicas: vec![],
    };

    assert_eq!(meta.partition_index, 5);
    assert_eq!(meta.replica_nodes.len(), 3);
    assert_eq!(meta.isr_nodes.len(), 2);
}

#[test]
fn test_partition_metadata_leader_election() {
    // Simulate a partition with no leader elected yet
    let meta = PartitionMetadata {
        error_code: KafkaCode::LeaderNotAvailable,
        partition_index: 0,
        leader_id: -1,
        replica_nodes: vec![1, 2, 3],
        isr_nodes: vec![],
        leader_epoch: -1,
        offline_replicas: vec![],
    };

    assert_eq!(meta.error_code, KafkaCode::LeaderNotAvailable);
    assert_eq!(meta.leader_id, -1);
    assert!(meta.isr_nodes.is_empty());
}

// ========================================================================
// KafkaCode Response Tests
// ========================================================================

#[test]
fn test_produce_error_codes() {
    // All error codes that can be returned from produce_to_partition
    let error_codes = [
        KafkaCode::None,
        KafkaCode::NotLeaderForPartition,
        KafkaCode::CorruptMessage,
        KafkaCode::Unknown,
    ];

    for code in error_codes {
        let response = ProducePartitionResponse {
            partition_index: 0,
            error_code: code,
            base_offset: if code == KafkaCode::None { 0 } else { -1 },
            log_append_time: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
        };
        assert_eq!(response.error_code, code);
    }
}

// ========================================================================
// Multiple Partition Tests
// ========================================================================

#[test]
fn test_topic_metadata_many_partitions() {
    let partitions: Vec<PartitionMetadata> = (0..100)
        .map(|i| PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: i,
            leader_id: (i % 3) + 1, // Distribute across brokers 1, 2, 3
            replica_nodes: vec![(i % 3) + 1],
            isr_nodes: vec![(i % 3) + 1],
            leader_epoch: -1,
            offline_replicas: vec![],
        })
        .collect();

    let topic = TopicMetadata {
        error_code: KafkaCode::None,
        name: "high-partition-topic".to_string(),
        is_internal: false,
        partitions,
        topic_authorized_operations: 0,
    };

    assert_eq!(topic.partitions.len(), 100);
    assert_eq!(topic.partitions[0].leader_id, 1);
    assert_eq!(topic.partitions[1].leader_id, 2);
    assert_eq!(topic.partitions[2].leader_id, 3);
    assert_eq!(topic.partitions[99].partition_index, 99);
}

// ========================================================================
// Cache Entry Tests
// ========================================================================

#[test]
fn test_arc_str_cache_unicode_topics() {
    let cache: Cache<String, Arc<str>> = Cache::new(100);

    // Test with unicode topic names
    let topics = [
        "普通话-topic",
        "日本語-トピック",
        "한국어-주제",
        "emoji-🎉-topic",
    ];

    for topic in topics {
        let arc = cache.get_with(topic.to_string(), || Arc::from(topic));
        assert_eq!(arc.as_ref(), topic);

        // Verify it's cached
        let arc2 = cache.get_with(topic.to_string(), || Arc::from(topic));
        assert!(Arc::ptr_eq(&arc, &arc2));
    }
}

#[test]
fn test_arc_str_cache_empty_string() {
    let cache: Cache<String, Arc<str>> = Cache::new(100);

    let empty = cache.get_with(String::new(), || Arc::from(""));
    assert_eq!(empty.as_ref(), "");
}

#[test]
fn test_arc_str_cache_long_topic_name() {
    let cache: Cache<String, Arc<str>> = Cache::new(100);

    // Kafka allows topic names up to 249 characters
    let long_name = "a".repeat(249);
    let arc = cache.get_with(long_name.clone(), || Arc::from(long_name.as_str()));
    assert_eq!(arc.len(), 249);
}

// ========================================================================
// Response Structure Tests
// ========================================================================

#[test]
fn test_produce_topic_response_structure() {
    let partitions = vec![
        ProducePartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            base_offset: 100,
            log_append_time: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
        },
        ProducePartitionResponse {
            partition_index: 1,
            error_code: KafkaCode::NotLeaderForPartition,
            base_offset: -1,
            log_append_time: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
        },
    ];

    let topic_response = ProduceTopicResponse {
        name: "test".to_string(),
        partitions,
    };

    assert_eq!(topic_response.name, "test");
    assert_eq!(topic_response.partitions.len(), 2);
    assert_eq!(topic_response.partitions[0].base_offset, 100);
    assert_eq!(
        topic_response.partitions[1].error_code,
        KafkaCode::NotLeaderForPartition
    );
}

#[test]
fn test_produce_response_data_structure() {
    let response = ProduceResponseData {
        responses: vec![
            ProduceTopicResponse {
                name: "topic-a".to_string(),
                partitions: vec![],
            },
            ProduceTopicResponse {
                name: "topic-b".to_string(),
                partitions: vec![],
            },
        ],
        throttle_time_ms: 100,
    };

    assert_eq!(response.responses.len(), 2);
    assert_eq!(response.throttle_time_ms, 100);
}

// ========================================================================
// CRC Validation Edge Cases
// ========================================================================

#[test]
fn test_validate_batch_crc_empty() {
    use crate::protocol::validate_batch_crc;
    use bytes::Bytes;
    let empty = Bytes::new();
    let result = validate_batch_crc(&empty);
    assert!(matches!(result, CrcValidationResult::TooSmall));
}

#[test]
fn test_validate_batch_crc_garbage() {
    use crate::protocol::validate_batch_crc;
    use bytes::Bytes;
    let garbage = Bytes::from_static(b"not a valid kafka batch");
    let result = validate_batch_crc(&garbage);
    // Should not be Valid (either Invalid or TooSmall depending on implementation)
    assert!(!matches!(result, CrcValidationResult::Valid));
}

#[test]
fn test_validate_batch_crc_minimum_size() {
    use crate::protocol::validate_batch_crc;
    use bytes::Bytes;
    // 16 bytes is less than the minimum for a valid batch header
    let too_small = Bytes::from(vec![0u8; 16]);
    let result = validate_batch_crc(&too_small);
    assert!(matches!(result, CrcValidationResult::TooSmall));
}
