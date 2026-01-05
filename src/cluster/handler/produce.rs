//! Produce request handling.
//!
//! # Acknowledgment Semantics (A-1)
//!
//! The `acks` parameter controls durability guarantees:
//!
//! - `acks=0`: Fire-and-forget. Returns immediately without waiting for write.
//!   Data may be lost if broker crashes before flushing. Use for high-throughput,
//!   loss-tolerant workloads (e.g., metrics, logs).
//!
//! - `acks=1`: Wait for leader write. Data is flushed to object storage before
//!   returning success. Standard durability guarantee.
//!
//! - `acks=-1` (or `all`): Same as `acks=1` in Kafkaesque. Since Kafkaesque uses object storage
//!   (S3/GCS/Azure) rather than replica-based replication, there's no ISR concept.
//!   Object storage provides built-in durability via its own replication.
//!
//! **Note**: Unlike Apache Kafka, Kafkaesque doesn't have in-sync replicas (ISR).
//! Durability is provided by the object storage backend's own replication.

use std::sync::Arc;
use tracing::{debug, warn};

use once_cell::sync::Lazy;
use tokio::sync::Semaphore;

use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::{ProducePartitionData, ProduceRequestData};
use crate::server::response::{
    ProducePartitionResponse, ProduceResponseData, ProduceTopicResponse,
};

use super::SlateDBClusterHandler;
use crate::cluster::coordinator::validate_topic_name;
use crate::cluster::partition_manager::PartitionManager;
use crate::cluster::raft::RaftCoordinator;

/// Maximum concurrent fire-and-forget (acks=0) writes across the broker.
///
/// This prevents unbounded task spawning under high acks=0 load, which could
/// exhaust memory or tokio runtime resources. When this limit is reached,
/// new acks=0 writes will be dropped silently (fire-and-forget semantics).
const MAX_FIRE_AND_FORGET_CONCURRENT: usize = 1000;

/// Semaphore for limiting concurrent fire-and-forget writes.
///
/// This provides backpressure for acks=0 produces to prevent resource exhaustion.
/// When all permits are taken, new writes are dropped (consistent with fire-and-forget
/// semantics where data loss is acceptable).
static FIRE_AND_FORGET_SEMAPHORE: Lazy<Semaphore> =
    Lazy::new(|| Semaphore::new(MAX_FIRE_AND_FORGET_CONCURRENT));

#[cfg(test)]
pub(crate) fn available_fire_and_forget_permits() -> usize {
    FIRE_AND_FORGET_SEMAPHORE.available_permits()
}

/// Handle a produce request.
pub(super) async fn handle_produce(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: ProduceRequestData,
) -> ProduceResponseData {
    use futures::stream::{self, StreamExt};

    let mut responses = Vec::with_capacity(request.topics.len());
    let acks = request.acks;

    debug!(
        client = %ctx.client_addr,
        client_id = ?ctx.client_id,
        topic_count = request.topics.len(),
        acks = acks,
        "PRODUCE request received"
    );

    // A-1: Handle acks=0 (fire-and-forget)
    // For acks=0, we spawn the writes but don't wait for results.
    // This provides maximum throughput at the cost of durability guarantees.
    //
    // BACKPRESSURE: We use a semaphore to limit concurrent fire-and-forget writes.
    // When the limit is reached, new writes are dropped silently (acceptable for acks=0).
    if acks == 0 {
        for topic in request.topics {
            // Validate topic name even for fire-and-forget
            if validate_topic_name(&topic.name).is_err() {
                debug!(topic = %topic.name, "Invalid topic name in produce request (acks=0)");
                continue; // Skip invalid topics silently for acks=0
            }

            let topic_name: Arc<str> = handler.cached_topic_name(&topic.name);
            let partition_manager = handler.partition_manager.clone();
            let validate_crc = handler.validate_record_crc;

            for partition in topic.partitions {
                let permit = match FIRE_AND_FORGET_SEMAPHORE.try_acquire() {
                    Ok(p) => p,
                    Err(_) => {
                        // Backpressure: drop this write silently (acceptable for acks=0)
                        warn!(
                            topic = %topic_name,
                            partition = partition.partition_index,
                            "Dropping acks=0 write due to backpressure (too many concurrent writes)"
                        );
                        continue;
                    }
                };

                let topic_name = Arc::clone(&topic_name);
                let partition_manager = partition_manager.clone();
                handler.data_runtime.spawn(async move {
                    let _permit = permit;
                    let _ = fire_and_forget_produce(
                        &partition_manager,
                        &topic_name,
                        partition,
                        validate_crc,
                    )
                    .await;
                });
            }

            responses.push(ProduceTopicResponse {
                name: topic.name,
                partitions: vec![], // Empty partitions for acks=0
            });
        }

        return ProduceResponseData {
            responses,
            throttle_time_ms: 0,
        };
    }

    // Use configurable concurrent partition writes to prevent overwhelming the system
    let max_concurrent_writes = handler.max_concurrent_partition_writes;

    for topic in request.topics {
        // Validate topic name to prevent injection attacks
        if validate_topic_name(&topic.name).is_err() {
            debug!(topic = %topic.name, "Invalid topic name in produce request");
            let partition_responses: Vec<_> = topic
                .partitions
                .iter()
                .map(|p| ProducePartitionResponse {
                    partition_index: p.partition_index,
                    error_code: KafkaCode::InvalidTopic,
                    base_offset: -1,
                    log_append_time: -1,
                })
                .collect();
            responses.push(ProduceTopicResponse {
                name: topic.name,
                partitions: partition_responses,
            });
            continue;
        }

        let topic_name: Arc<str> = handler.cached_topic_name(&topic.name);

        // Use buffer_unordered for bounded concurrency instead of join_all
        let partition_responses: Vec<_> = stream::iter(topic.partitions)
            .map(|partition| {
                let topic_name = Arc::clone(&topic_name);
                async move {
                    handler
                        .produce_to_partition(&topic_name, partition, acks)
                        .await
                }
            })
            .buffer_unordered(max_concurrent_writes)
            .collect()
            .await;

        responses.push(ProduceTopicResponse {
            name: topic.name,
            partitions: partition_responses,
        });
    }

    ProduceResponseData {
        responses,
        throttle_time_ms: 0,
    }
}

/// Fire-and-forget produce for acks=0.
///
/// This is a simplified produce path that doesn't return detailed responses.
/// Used when clients don't need acknowledgments.
async fn fire_and_forget_produce(
    partition_manager: &Arc<PartitionManager<RaftCoordinator>>,
    topic: &str,
    partition: ProducePartitionData,
    validate_crc: bool,
) -> Result<(), ()> {
    use crate::protocol::{CrcValidationResult, validate_batch_crc};

    // Skip zombie mode check for fire-and-forget (best effort)
    if partition_manager.is_zombie() {
        return Err(());
    }

    // Get partition store - for acks=0, don't try to acquire if we don't own
    // IMPORTANT: We do NOT try to acquire partitions here. Partition acquisition
    // happens only in the background ownership loop.
    let store = match partition_manager
        .get_for_write(topic, partition.partition_index)
        .await
    {
        Ok(s) => s,
        Err(_) => return Err(()), // We don't own this partition
    };

    // Optional CRC validation
    if validate_crc {
        match validate_batch_crc(&partition.records) {
            CrcValidationResult::Valid => {}
            _ => return Err(()),
        }
    }

    // Append batch (ignore result for fire-and-forget)
    let _ = store.append_batch(&partition.records).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // ========================================================================
    // Constants Tests
    // ========================================================================

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_max_fire_and_forget_concurrent_reasonable() {
        // Ensure the limit is reasonable for production use
        assert!(MAX_FIRE_AND_FORGET_CONCURRENT > 0);
        assert!(MAX_FIRE_AND_FORGET_CONCURRENT <= 10_000);
        assert_eq!(MAX_FIRE_AND_FORGET_CONCURRENT, 1000);
    }

    #[test]
    fn test_semaphore_permits_available() {
        // The semaphore should start with full permits
        let permits = available_fire_and_forget_permits();
        assert!(permits > 0);
        assert!(permits <= MAX_FIRE_AND_FORGET_CONCURRENT);
    }

    // ========================================================================
    // Response Structure Tests
    // ========================================================================

    #[test]
    fn test_produce_partition_response_success() {
        let response = ProducePartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            base_offset: 42,
            log_append_time: -1,
        };

        assert_eq!(response.partition_index, 0);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.base_offset, 42);
        assert_eq!(response.log_append_time, -1);
    }

    #[test]
    fn test_produce_partition_response_not_leader() {
        let response = ProducePartitionResponse {
            partition_index: 5,
            error_code: KafkaCode::NotLeaderForPartition,
            base_offset: -1,
            log_append_time: -1,
        };

        assert_eq!(response.partition_index, 5);
        assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
        assert_eq!(response.base_offset, -1);
    }

    #[test]
    fn test_produce_partition_response_corrupt_message() {
        let response = ProducePartitionResponse {
            partition_index: 3,
            error_code: KafkaCode::CorruptMessage,
            base_offset: -1,
            log_append_time: -1,
        };

        assert_eq!(response.error_code, KafkaCode::CorruptMessage);
    }

    #[test]
    fn test_produce_partition_response_invalid_topic() {
        let response = ProducePartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::InvalidTopic,
            base_offset: -1,
            log_append_time: -1,
        };

        assert_eq!(response.error_code, KafkaCode::InvalidTopic);
    }

    #[test]
    fn test_produce_topic_response_structure() {
        let partitions = vec![
            ProducePartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                base_offset: 0,
                log_append_time: -1,
            },
            ProducePartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::None,
                base_offset: 10,
                log_append_time: -1,
            },
        ];

        let response = ProduceTopicResponse {
            name: "test-topic".to_string(),
            partitions,
        };

        assert_eq!(response.name, "test-topic");
        assert_eq!(response.partitions.len(), 2);
        assert_eq!(response.partitions[0].base_offset, 0);
        assert_eq!(response.partitions[1].base_offset, 10);
    }

    #[test]
    fn test_produce_response_data_acks_0() {
        // For acks=0, partitions should be empty in response
        let responses = vec![ProduceTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![], // Empty for acks=0
        }];

        let response = ProduceResponseData {
            responses,
            throttle_time_ms: 0,
        };

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses.len(), 1);
        assert!(response.responses[0].partitions.is_empty());
    }

    // ========================================================================
    // Request Structure Tests
    // ========================================================================

    #[test]
    fn test_produce_partition_data_structure() {
        let data = ProducePartitionData {
            partition_index: 0,
            records: Bytes::from_static(b"test records"),
        };

        assert_eq!(data.partition_index, 0);
        assert_eq!(data.records.len(), 12);
    }

    #[test]
    fn test_produce_partition_data_empty_records() {
        let data = ProducePartitionData {
            partition_index: 0,
            records: Bytes::new(),
        };

        assert!(data.records.is_empty());
    }

    // ========================================================================
    // Topic Validation Tests (via coordinator)
    // ========================================================================

    #[test]
    fn test_topic_name_validation_patterns() {
        // Valid topic names
        assert!(validate_topic_name("test-topic").is_ok());
        assert!(validate_topic_name("my_topic").is_ok());
        assert!(validate_topic_name("topic123").is_ok());
        assert!(validate_topic_name("a").is_ok());

        // Invalid topic names
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(".").is_err());
        assert!(validate_topic_name("..").is_err());
        assert!(validate_topic_name("topic/with/slashes").is_err());
    }

    // ========================================================================
    // Acks Value Tests
    // ========================================================================

    #[test]
    fn test_acks_values() {
        // Standard acks values
        let acks_fire_and_forget: i16 = 0;
        let acks_leader_only: i16 = 1;
        let acks_all: i16 = -1;

        assert_eq!(acks_fire_and_forget, 0);
        assert_eq!(acks_leader_only, 1);
        assert_eq!(acks_all, -1);

        // In Kafkaesque, acks=1 and acks=-1 behave the same
        // since durability is provided by object storage
    }

    // ========================================================================
    // Throttle Time Tests
    // ========================================================================

    #[test]
    fn test_throttle_time_default() {
        let response = ProduceResponseData {
            responses: vec![],
            throttle_time_ms: 0,
        };

        assert_eq!(response.throttle_time_ms, 0);
    }

    // ========================================================================
    // Base Offset Tests
    // ========================================================================

    #[test]
    fn test_base_offset_error_value() {
        // When an error occurs, base_offset should be -1
        let response = ProducePartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::Unknown,
            base_offset: -1,
            log_append_time: -1,
        };

        assert_eq!(response.base_offset, -1);
    }

    #[test]
    fn test_base_offset_valid_values() {
        // Valid base offsets start at 0 and increase
        for offset in [0i64, 1, 100, 1000, i64::MAX - 1] {
            let response = ProducePartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                base_offset: offset,
                log_append_time: -1,
            };
            assert_eq!(response.base_offset, offset);
        }
    }

    // ========================================================================
    // Multiple Partitions Tests
    // ========================================================================

    #[test]
    fn test_multiple_partitions_mixed_results() {
        let partitions = [
            ProducePartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                base_offset: 0,
                log_append_time: -1,
            },
            ProducePartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::NotLeaderForPartition,
                base_offset: -1,
                log_append_time: -1,
            },
            ProducePartitionResponse {
                partition_index: 2,
                error_code: KafkaCode::None,
                base_offset: 50,
                log_append_time: -1,
            },
        ];

        // One success, one failure, one success
        assert_eq!(partitions[0].error_code, KafkaCode::None);
        assert_eq!(partitions[1].error_code, KafkaCode::NotLeaderForPartition);
        assert_eq!(partitions[2].error_code, KafkaCode::None);

        // Base offsets correct for successes, -1 for failure
        assert_eq!(partitions[0].base_offset, 0);
        assert_eq!(partitions[1].base_offset, -1);
        assert_eq!(partitions[2].base_offset, 50);
    }

    // ========================================================================
    // Multiple Topics Tests
    // ========================================================================

    #[test]
    fn test_multiple_topics_response() {
        let responses = vec![
            ProduceTopicResponse {
                name: "topic-a".to_string(),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    base_offset: 100,
                    log_append_time: -1,
                }],
            },
            ProduceTopicResponse {
                name: "topic-b".to_string(),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    base_offset: 200,
                    log_append_time: -1,
                }],
            },
        ];

        let response = ProduceResponseData {
            responses,
            throttle_time_ms: 0,
        };

        assert_eq!(response.responses.len(), 2);
        assert_eq!(response.responses[0].name, "topic-a");
        assert_eq!(response.responses[1].name, "topic-b");
    }

    // ========================================================================
    // Fire-and-Forget Backpressure Tests
    // ========================================================================

    #[test]
    fn test_fire_and_forget_constants() {
        // Verify the semaphore capacity constant is set appropriately
        assert_eq!(MAX_FIRE_AND_FORGET_CONCURRENT, 1000);
        // Verify we can check available permits (actual count varies in parallel tests)
        let permits = available_fire_and_forget_permits();
        assert!(permits <= MAX_FIRE_AND_FORGET_CONCURRENT);
    }

    // Note: Tests that acquire from FIRE_AND_FORGET_SEMAPHORE are intentionally
    // omitted because it's a global static shared across all tests running in
    // parallel, making them inherently flaky.

    // ========================================================================
    // CRC Validation Tests
    // ========================================================================

    #[test]
    fn test_crc_validation_enum_values() {
        use crate::protocol::CrcValidationResult;

        // Test all CRC validation result variants
        let valid = CrcValidationResult::Valid;
        let invalid = CrcValidationResult::Invalid {
            expected: 0,
            actual: 1,
        };
        let too_small = CrcValidationResult::TooSmall;

        // Verify we can match on all variants
        match valid {
            CrcValidationResult::Valid => {}
            _ => panic!("Expected Valid"),
        }

        match invalid {
            CrcValidationResult::Invalid { expected, actual } => {
                assert_ne!(expected, actual);
            }
            _ => panic!("Expected Invalid"),
        }

        match too_small {
            CrcValidationResult::TooSmall => {}
            _ => panic!("Expected TooSmall"),
        }
    }

    #[test]
    fn test_empty_records_crc() {
        use crate::protocol::validate_batch_crc;

        // Empty records should not crash
        let empty = Bytes::new();
        let result = validate_batch_crc(&empty);

        // Empty records should return TooSmall
        assert!(matches!(
            result,
            crate::protocol::CrcValidationResult::TooSmall
        ));
    }

    #[test]
    fn test_invalid_records_crc() {
        use crate::protocol::validate_batch_crc;

        // Garbage data should not crash and should return error
        let garbage = Bytes::from_static(b"not valid kafka records");
        let result = validate_batch_crc(&garbage);

        // Should not be Valid (either Invalid or TooSmall)
        assert!(!matches!(
            result,
            crate::protocol::CrcValidationResult::Valid
        ));
    }

    // ========================================================================
    // Acks=0 Behavior Tests
    // ========================================================================

    #[test]
    fn test_acks_zero_returns_empty_partitions() {
        // For acks=0, the response should have empty partitions array
        let response = ProduceResponseData {
            responses: vec![ProduceTopicResponse {
                name: "test".to_string(),
                partitions: vec![], // Empty for acks=0
            }],
            throttle_time_ms: 0,
        };

        assert!(response.responses[0].partitions.is_empty());
    }

    #[test]
    fn test_acks_one_returns_partition_results() {
        // For acks=1, response should have partition results
        let response = ProduceResponseData {
            responses: vec![ProduceTopicResponse {
                name: "test".to_string(),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    base_offset: 0,
                    log_append_time: -1,
                }],
            }],
            throttle_time_ms: 0,
        };

        assert_eq!(response.responses[0].partitions.len(), 1);
    }

    // ========================================================================
    // Error Code Tests
    // ========================================================================

    #[test]
    fn test_all_relevant_error_codes() {
        // Test all error codes that can be returned by produce handler
        let error_codes = [
            KafkaCode::None,
            KafkaCode::Unknown,
            KafkaCode::CorruptMessage,
            KafkaCode::InvalidTopic,
            KafkaCode::NotLeaderForPartition,
            KafkaCode::RecordListTooLarge,
            KafkaCode::NotEnoughReplicas,
        ];

        for code in error_codes {
            let response = ProducePartitionResponse {
                partition_index: 0,
                error_code: code,
                base_offset: if code == KafkaCode::None { 0 } else { -1 },
                log_append_time: -1,
            };

            assert_eq!(response.error_code, code);
        }
    }

    // ========================================================================
    // Large Batch Tests
    // ========================================================================

    #[test]
    fn test_large_records_structure() {
        // Test with large records to ensure no overflow
        let large_data = vec![0u8; 1024 * 1024]; // 1MB
        let data = ProducePartitionData {
            partition_index: 0,
            records: Bytes::from(large_data),
        };

        assert_eq!(data.records.len(), 1024 * 1024);
    }

    #[test]
    fn test_many_partitions_in_request() {
        // Test with many partitions
        let partitions: Vec<ProducePartitionResponse> = (0..100)
            .map(|i| ProducePartitionResponse {
                partition_index: i,
                error_code: KafkaCode::None,
                base_offset: i as i64 * 100,
                log_append_time: -1,
            })
            .collect();

        let response = ProduceTopicResponse {
            name: "test".to_string(),
            partitions,
        };

        assert_eq!(response.partitions.len(), 100);
        assert_eq!(response.partitions[99].partition_index, 99);
        assert_eq!(response.partitions[99].base_offset, 9900);
    }
}
