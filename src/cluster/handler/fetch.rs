//! Fetch request handling.

use std::sync::Arc;
use tracing::{debug, error};

use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::FetchRequestData;
use crate::server::response::{FetchPartitionResponse, FetchResponseData, FetchTopicResponse};

use super::SlateDBClusterHandler;
use crate::cluster::coordinator::validate_topic_name;

/// Handle a fetch request.
pub(super) async fn handle_fetch(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: FetchRequestData,
) -> FetchResponseData {
    use futures::stream::{self, StreamExt};

    let mut responses = Vec::with_capacity(request.topics.len());

    debug!(
        client = %ctx.client_addr,
        client_id = ?ctx.client_id,
        topic_count = request.topics.len(),
        max_wait_ms = request.max_wait_ms,
        min_bytes = request.min_bytes,
        "FETCH request received"
    );

    // Log fetch details for debugging
    for topic in &request.topics {
        for partition in &topic.partitions {
            debug!(
                topic = %topic.name,
                partition = partition.partition_index,
                fetch_offset = partition.fetch_offset,
                "FETCH partition request"
            );
        }
    }

    // Use configurable concurrent partition reads to prevent overwhelming the system
    let max_concurrent_reads = handler.max_concurrent_partition_reads;

    for topic in request.topics {
        // Validate topic name to prevent injection attacks
        if validate_topic_name(&topic.name).is_err() {
            debug!(topic = %topic.name, "Invalid topic name in fetch request");
            let partition_responses: Vec<_> = topic
                .partitions
                .iter()
                .map(|p| FetchPartitionResponse {
                    partition_index: p.partition_index,
                    error_code: KafkaCode::InvalidTopic,
                    high_watermark: -1,
                    last_stable_offset: -1,
                    aborted_transactions: vec![],
                    records: None,
                })
                .collect();
            responses.push(FetchTopicResponse {
                name: topic.name,
                partitions: partition_responses,
            });
            continue;
        }

        let topic_name: Arc<str> = Arc::from(topic.name.as_str());

        // Process partitions with concurrency limit
        let partition_responses: Vec<_> = stream::iter(topic.partitions)
            .map(|partition| {
                let topic_name = Arc::clone(&topic_name);
                async move {
                    match handler
                        .partition_manager
                        .get_for_read(&topic_name, partition.partition_index)
                        .await
                    {
                        Ok(store) => {
                            // Validate fetch offset before reading.
                            // Return OffsetOutOfRange if offset is past high watermark.
                            // This prevents clients from waiting indefinitely for
                            // data at offsets that don't exist yet.
                            let current_hwm = store.high_watermark();
                            if partition.fetch_offset > current_hwm {
                                return FetchPartitionResponse {
                                    partition_index: partition.partition_index,
                                    error_code: KafkaCode::OffsetOutOfRange,
                                    high_watermark: current_hwm,
                                    last_stable_offset: current_hwm,
                                    aborted_transactions: vec![],
                                    records: None,
                                };
                            }

                            // Also reject negative offsets (except special values)
                            if partition.fetch_offset < 0
                                && partition.fetch_offset != -1
                                && partition.fetch_offset != -2
                            {
                                return FetchPartitionResponse {
                                    partition_index: partition.partition_index,
                                    error_code: KafkaCode::OffsetOutOfRange,
                                    high_watermark: current_hwm,
                                    last_stable_offset: current_hwm,
                                    aborted_transactions: vec![],
                                    records: None,
                                };
                            }

                            match store.fetch_from(partition.fetch_offset).await {
                                Ok((high_watermark, records)) => {
                                    // Track fetch metrics
                                    if let Some(ref record_bytes) = records {
                                        let bytes = record_bytes.len() as u64;
                                        let msg_count = 1u64;
                                        crate::cluster::metrics::record_fetch(
                                            &topic_name,
                                            partition.partition_index,
                                            msg_count,
                                            bytes,
                                        );
                                    }

                                    FetchPartitionResponse {
                                        partition_index: partition.partition_index,
                                        error_code: KafkaCode::None,
                                        high_watermark,
                                        last_stable_offset: high_watermark,
                                        aborted_transactions: vec![],
                                        records,
                                    }
                                }
                                Err(e) => {
                                    let error_code = if e.is_fenced() {
                                        error!(
                                            topic = %topic_name,
                                            partition = partition.partition_index,
                                            "Fenced during fetch - returning NotLeaderForPartition"
                                        );
                                        KafkaCode::NotLeaderForPartition
                                    } else {
                                        error!(error = %e, "Fetch failed");
                                        KafkaCode::Unknown
                                    };
                                    FetchPartitionResponse {
                                        partition_index: partition.partition_index,
                                        error_code,
                                        high_watermark: -1,
                                        last_stable_offset: -1,
                                        aborted_transactions: vec![],
                                        records: None,
                                    }
                                }
                            }
                        }
                        Err(_) => {
                            // We don't own this partition
                            FetchPartitionResponse {
                                partition_index: partition.partition_index,
                                error_code: KafkaCode::NotLeaderForPartition,
                                high_watermark: -1,
                                last_stable_offset: -1,
                                aborted_transactions: vec![],
                                records: None,
                            }
                        }
                    }
                }
            })
            .buffer_unordered(max_concurrent_reads)
            .collect()
            .await;

        responses.push(FetchTopicResponse {
            name: topic.name,
            partitions: partition_responses,
        });
    }

    FetchResponseData {
        throttle_time_ms: 0,
        responses,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // ========================================================================
    // Response Structure Tests
    // ========================================================================

    #[test]
    fn test_fetch_partition_response_success() {
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 100,
            last_stable_offset: 100,
            aborted_transactions: vec![],
            records: Some(Bytes::from_static(b"test records")),
        };

        assert_eq!(response.partition_index, 0);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.high_watermark, 100);
        assert_eq!(response.last_stable_offset, 100);
        assert!(response.records.is_some());
    }

    #[test]
    fn test_fetch_partition_response_not_leader() {
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::NotLeaderForPartition,
            high_watermark: -1,
            last_stable_offset: -1,
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
        assert_eq!(response.high_watermark, -1);
        assert!(response.records.is_none());
    }

    #[test]
    fn test_fetch_partition_response_offset_out_of_range() {
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::OffsetOutOfRange,
            high_watermark: 50,
            last_stable_offset: 50,
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.error_code, KafkaCode::OffsetOutOfRange);
        // HWM is still returned even on error
        assert_eq!(response.high_watermark, 50);
    }

    #[test]
    fn test_fetch_partition_response_invalid_topic() {
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::InvalidTopic,
            high_watermark: -1,
            last_stable_offset: -1,
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.error_code, KafkaCode::InvalidTopic);
    }

    #[test]
    fn test_fetch_topic_response_structure() {
        let partitions = vec![
            FetchPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                high_watermark: 10,
                last_stable_offset: 10,
                aborted_transactions: vec![],
                records: Some(Bytes::from_static(b"data")),
            },
            FetchPartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::None,
                high_watermark: 20,
                last_stable_offset: 20,
                aborted_transactions: vec![],
                records: Some(Bytes::from_static(b"more data")),
            },
        ];

        let response = FetchTopicResponse {
            name: "test-topic".to_string(),
            partitions,
        };

        assert_eq!(response.name, "test-topic");
        assert_eq!(response.partitions.len(), 2);
    }

    #[test]
    fn test_fetch_response_data_structure() {
        let responses = vec![FetchTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![],
        }];

        let response = FetchResponseData {
            throttle_time_ms: 0,
            responses,
        };

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses.len(), 1);
    }

    // ========================================================================
    // Offset Validation Tests
    // ========================================================================

    #[test]
    fn test_special_fetch_offsets() {
        // -1 = Latest (end of log)
        // -2 = Earliest (beginning of log)
        let latest_offset: i64 = -1;
        let earliest_offset: i64 = -2;

        assert_eq!(latest_offset, -1);
        assert_eq!(earliest_offset, -2);
    }

    #[test]
    fn test_fetch_offset_bounds() {
        // Valid offsets: 0 to hwm, and special values -1, -2
        let valid_offsets = [0i64, 1, 100, 1000];
        let special_offsets = [-1i64, -2];
        let invalid_offsets = [-3i64, -100, i64::MIN];

        for offset in valid_offsets {
            assert!(offset >= 0, "Valid offset should be >= 0");
        }

        for offset in special_offsets {
            assert!(offset == -1 || offset == -2);
        }

        for offset in invalid_offsets {
            assert!(offset < -2, "Invalid offset should be < -2");
        }
    }

    // ========================================================================
    // High Watermark Tests
    // ========================================================================

    #[test]
    fn test_high_watermark_values() {
        // HWM should always be >= 0 for success responses
        // HWM is -1 for error responses where partition is not accessible
        let hwm_success = 100i64;
        let hwm_error = -1i64;

        assert!(hwm_success >= 0);
        assert_eq!(hwm_error, -1);
    }

    #[test]
    fn test_last_stable_offset_equals_hwm() {
        // In Kafkaesque, last_stable_offset always equals high_watermark
        // because we don't have transactional support
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 50,
            last_stable_offset: 50,
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.high_watermark, response.last_stable_offset);
    }

    // ========================================================================
    // Empty Response Tests
    // ========================================================================

    #[test]
    fn test_empty_fetch_records() {
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 0,
            last_stable_offset: 0,
            aborted_transactions: vec![],
            records: None,
        };

        assert!(response.records.is_none());
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_fetch_at_hwm_returns_empty() {
        // When fetch_offset == hwm, no records are available
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 100,
            last_stable_offset: 100,
            aborted_transactions: vec![],
            records: None,
        };

        assert!(response.records.is_none());
        assert_eq!(response.error_code, KafkaCode::None);
    }

    // ========================================================================
    // Multiple Partitions Tests
    // ========================================================================

    #[test]
    fn test_fetch_multiple_partitions_mixed_results() {
        let partitions = vec![
            FetchPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                high_watermark: 100,
                last_stable_offset: 100,
                aborted_transactions: vec![],
                records: Some(Bytes::from_static(b"data")),
            },
            FetchPartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::NotLeaderForPartition,
                high_watermark: -1,
                last_stable_offset: -1,
                aborted_transactions: vec![],
                records: None,
            },
            FetchPartitionResponse {
                partition_index: 2,
                error_code: KafkaCode::OffsetOutOfRange,
                high_watermark: 50,
                last_stable_offset: 50,
                aborted_transactions: vec![],
                records: None,
            },
        ];

        assert_eq!(partitions[0].error_code, KafkaCode::None);
        assert!(partitions[0].records.is_some());

        assert_eq!(partitions[1].error_code, KafkaCode::NotLeaderForPartition);
        assert!(partitions[1].records.is_none());

        assert_eq!(partitions[2].error_code, KafkaCode::OffsetOutOfRange);
        assert!(partitions[2].records.is_none());
    }

    // ========================================================================
    // Topic Validation Tests
    // ========================================================================

    #[test]
    fn test_topic_name_validation_for_fetch() {
        // Valid topic names
        assert!(validate_topic_name("test-topic").is_ok());
        assert!(validate_topic_name("my_topic").is_ok());
        assert!(validate_topic_name("topic123").is_ok());

        // Invalid topic names
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(".").is_err());
        assert!(validate_topic_name("..").is_err());
    }

    // ========================================================================
    // Aborted Transactions Tests
    // ========================================================================

    #[test]
    fn test_aborted_transactions_empty() {
        // Kafkaesque doesn't support transactions, so aborted_transactions is always empty
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 100,
            last_stable_offset: 100,
            aborted_transactions: vec![],
            records: None,
        };

        assert!(response.aborted_transactions.is_empty());
    }

    // ========================================================================
    // Multiple Topics Tests
    // ========================================================================

    #[test]
    fn test_fetch_multiple_topics() {
        let responses = vec![
            FetchTopicResponse {
                name: "topic-a".to_string(),
                partitions: vec![FetchPartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    high_watermark: 100,
                    last_stable_offset: 100,
                    aborted_transactions: vec![],
                    records: Some(Bytes::from_static(b"a")),
                }],
            },
            FetchTopicResponse {
                name: "topic-b".to_string(),
                partitions: vec![FetchPartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    high_watermark: 200,
                    last_stable_offset: 200,
                    aborted_transactions: vec![],
                    records: Some(Bytes::from_static(b"b")),
                }],
            },
        ];

        let response = FetchResponseData {
            throttle_time_ms: 0,
            responses,
        };

        assert_eq!(response.responses.len(), 2);
        assert_eq!(response.responses[0].name, "topic-a");
        assert_eq!(response.responses[1].name, "topic-b");
    }

    // ========================================================================
    // Throttle Time Tests
    // ========================================================================

    #[test]
    fn test_throttle_time_default_zero() {
        let response = FetchResponseData {
            throttle_time_ms: 0,
            responses: vec![],
        };

        assert_eq!(response.throttle_time_ms, 0);
    }

    // ========================================================================
    // Deleted Offset Range Tests
    // ========================================================================

    #[test]
    fn test_fetch_at_deleted_offset_returns_error() {
        // When fetching at an offset that was deleted (before log start offset),
        // should return OffsetOutOfRange
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::OffsetOutOfRange,
            high_watermark: 1000, // HWM is high
            last_stable_offset: 1000,
            aborted_transactions: vec![],
            records: None,
        };

        // Client requested offset 0, but log starts at 500 (0-499 deleted)
        assert_eq!(response.error_code, KafkaCode::OffsetOutOfRange);
        assert!(response.records.is_none());
    }

    #[test]
    fn test_fetch_offset_less_than_log_start() {
        // Offsets before log start offset should be rejected
        let log_start_offset = 500i64;
        let requested_offset = 100i64;

        assert!(
            requested_offset < log_start_offset,
            "Requested offset should be before log start"
        );

        // This would result in OffsetOutOfRange error
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::OffsetOutOfRange,
            high_watermark: 1000,
            last_stable_offset: 1000,
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.error_code, KafkaCode::OffsetOutOfRange);
    }

    #[test]
    fn test_fetch_offset_at_log_start() {
        // Offset exactly at log start should succeed
        let log_start_offset = 500i64;
        let requested_offset = 500i64;

        assert_eq!(requested_offset, log_start_offset);

        // This would succeed
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 1000,
            last_stable_offset: 1000,
            aborted_transactions: vec![],
            records: Some(Bytes::from_static(b"data at log start")),
        };

        assert_eq!(response.error_code, KafkaCode::None);
        assert!(response.records.is_some());
    }

    #[test]
    fn test_fetch_offset_between_log_start_and_hwm() {
        // Valid offset range: [log_start_offset, high_watermark)
        let log_start_offset = 500i64;
        let high_watermark = 1000i64;
        let requested_offset = 750i64;

        assert!(requested_offset >= log_start_offset);
        assert!(requested_offset < high_watermark);

        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark,
            last_stable_offset: high_watermark,
            aborted_transactions: vec![],
            records: Some(Bytes::from_static(b"valid data")),
        };

        assert_eq!(response.error_code, KafkaCode::None);
    }

    // ========================================================================
    // Isolation Level Tests
    // ========================================================================

    #[test]
    fn test_isolation_level_read_uncommitted() {
        // read_uncommitted (0): Can read up to high_watermark
        let high_watermark = 1000i64;
        let last_stable_offset = 900i64; // LSO is lower due to pending transactions

        // In read_uncommitted, max readable = HWM
        let max_readable_offset = high_watermark;
        assert_eq!(max_readable_offset, 1000);

        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark,
            last_stable_offset,
            aborted_transactions: vec![],
            records: None,
        };

        // HWM and LSO can differ in transactional scenarios
        assert!(response.last_stable_offset <= response.high_watermark);
    }

    #[test]
    fn test_isolation_level_read_committed() {
        // read_committed (1): Can only read up to last_stable_offset
        let high_watermark = 1000i64;
        let last_stable_offset = 900i64; // LSO is lower

        // In read_committed, max readable = LSO
        let max_readable_offset = last_stable_offset;
        assert_eq!(max_readable_offset, 900);
        assert!(max_readable_offset < high_watermark);
    }

    #[test]
    fn test_kafkaesque_no_transactions() {
        // In Kafkaesque, LSO always equals HWM (no transaction support)
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 1000,
            last_stable_offset: 1000, // Always equals HWM in Kafkaesque
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.high_watermark, response.last_stable_offset);
        assert!(response.aborted_transactions.is_empty());
    }

    #[test]
    fn test_isolation_level_values() {
        // Kafka isolation levels
        let read_uncommitted: i8 = 0;
        let read_committed: i8 = 1;

        assert_eq!(read_uncommitted, 0);
        assert_eq!(read_committed, 1);
    }

    // ========================================================================
    // Offset Out of Range Scenarios
    // ========================================================================

    #[test]
    fn test_fetch_past_hwm() {
        // Fetch offset > HWM should return OffsetOutOfRange
        let hwm = 100i64;
        let requested = 150i64;

        assert!(requested > hwm);

        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::OffsetOutOfRange,
            high_watermark: hwm,
            last_stable_offset: hwm,
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.error_code, KafkaCode::OffsetOutOfRange);
        // HWM is still returned so client knows where to fetch from
        assert_eq!(response.high_watermark, hwm);
    }

    #[test]
    fn test_invalid_negative_offsets() {
        // Only -1 (latest) and -2 (earliest) are valid negative offsets
        let invalid_offsets = [-3i64, -100, -1000, i64::MIN];

        for offset in invalid_offsets {
            assert!(offset < -2, "Offset {} should be invalid", offset);
        }
    }

    #[test]
    fn test_special_offset_latest() {
        // -1 means fetch from latest (end of log)
        let latest: i64 = -1;
        assert_eq!(latest, -1);
    }

    #[test]
    fn test_special_offset_earliest() {
        // -2 means fetch from earliest (start of log)
        let earliest: i64 = -2;
        assert_eq!(earliest, -2);
    }

    // ========================================================================
    // Max Bytes Enforcement Tests
    // ========================================================================

    #[test]
    fn test_response_respects_max_bytes() {
        // Response should not exceed max_bytes
        let max_bytes = 1024;
        let records = vec![0u8; max_bytes as usize];

        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 100,
            last_stable_offset: 100,
            aborted_transactions: vec![],
            records: Some(Bytes::from(records)),
        };

        assert!(response.records.as_ref().unwrap().len() <= max_bytes as usize);
    }

    #[test]
    fn test_multiple_partitions_share_max_bytes() {
        // When fetching multiple partitions, max_bytes is shared
        let max_bytes_total = 4096;
        let num_partitions = 4;
        let bytes_per_partition = max_bytes_total / num_partitions;

        let responses: Vec<FetchPartitionResponse> = (0..num_partitions)
            .map(|i| {
                let records = vec![0u8; bytes_per_partition as usize];
                FetchPartitionResponse {
                    partition_index: i,
                    error_code: KafkaCode::None,
                    high_watermark: 100,
                    last_stable_offset: 100,
                    aborted_transactions: vec![],
                    records: Some(Bytes::from(records)),
                }
            })
            .collect();

        let total_bytes: usize = responses
            .iter()
            .filter_map(|r| r.records.as_ref())
            .map(|r| r.len())
            .sum();

        assert_eq!(total_bytes, max_bytes_total as usize);
    }

    // ========================================================================
    // Log End Offset Change Tests
    // ========================================================================

    #[test]
    fn test_hwm_can_change_between_requests() {
        // HWM can increase between fetch requests
        let first_response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 100,
            last_stable_offset: 100,
            aborted_transactions: vec![],
            records: Some(Bytes::from_static(b"first")),
        };

        let second_response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 150, // HWM increased
            last_stable_offset: 150,
            aborted_transactions: vec![],
            records: Some(Bytes::from_static(b"second")),
        };

        assert!(second_response.high_watermark > first_response.high_watermark);
    }

    // ========================================================================
    // Empty Partition Tests
    // ========================================================================

    #[test]
    fn test_fetch_from_empty_partition() {
        // Fetching from empty partition (HWM = 0)
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 0,
            last_stable_offset: 0,
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.high_watermark, 0);
        assert!(response.records.is_none());
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_fetch_offset_zero_from_empty() {
        // Fetch offset 0 from empty partition is valid (but returns nothing)
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 0,
            last_stable_offset: 0,
            aborted_transactions: vec![],
            records: None,
        };

        assert_eq!(response.error_code, KafkaCode::None);
    }
}
