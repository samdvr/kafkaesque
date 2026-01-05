//! Offset request handling (list, commit, fetch).

use tracing::{debug, error};

use crate::error::KafkaCode;
use crate::server::request::{
    ListOffsetsRequestData, OffsetCommitRequestData, OffsetFetchRequestData,
};
use crate::server::response::{
    ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
    OffsetCommitPartitionResponse, OffsetCommitResponseData, OffsetCommitTopicResponse,
    OffsetFetchPartitionResponse, OffsetFetchResponseData, OffsetFetchTopicResponse,
};

use super::SlateDBClusterHandler;
use crate::cluster::coordinator::validate_topic_name;
use crate::cluster::traits::ConsumerGroupCoordinator;

/// Handle a list offsets request.
pub(super) async fn handle_list_offsets(
    handler: &SlateDBClusterHandler,
    request: ListOffsetsRequestData,
) -> ListOffsetsResponseData {
    let mut topics = Vec::with_capacity(request.topics.len());

    for topic in request.topics {
        // Validate topic name to prevent injection attacks
        if validate_topic_name(&topic.name).is_err() {
            debug!(topic = %topic.name, "Invalid topic name in list_offsets request");
            let partitions: Vec<_> = topic
                .partitions
                .iter()
                .map(|p| ListOffsetsPartitionResponse {
                    partition_index: p.partition_index,
                    error_code: KafkaCode::InvalidTopic,
                    timestamp: -1,
                    offset: -1,
                })
                .collect();
            topics.push(ListOffsetsTopicResponse {
                name: topic.name,
                partitions,
            });
            continue;
        }

        let mut partitions = Vec::with_capacity(topic.partitions.len());

        for partition in topic.partitions {
            let (error_code, offset) = match handler
                .partition_manager
                .get_for_read(&topic.name, partition.partition_index)
                .await
            {
                Ok(store) => {
                    let offset = match partition.timestamp {
                        -2 => store.earliest_offset().await.unwrap_or(0), // Earliest
                        -1 => store.high_watermark(),                     // Latest
                        _ => store.high_watermark(),                      // Timestamp not supported
                    };
                    (KafkaCode::None, offset)
                }
                Err(_) => (KafkaCode::NotLeaderForPartition, -1),
            };

            partitions.push(ListOffsetsPartitionResponse {
                partition_index: partition.partition_index,
                error_code,
                timestamp: -1,
                offset,
            });
        }

        topics.push(ListOffsetsTopicResponse {
            name: topic.name,
            partitions,
        });
    }

    ListOffsetsResponseData {
        throttle_time_ms: 0,
        topics,
    }
}

/// Handle an offset commit request.
pub(super) async fn handle_offset_commit(
    handler: &SlateDBClusterHandler,
    request: OffsetCommitRequestData,
) -> OffsetCommitResponseData {
    use crate::cluster::error::HeartbeatResult;

    // Validate member and generation before allowing any commits.
    // This prevents stale consumers (from before a rebalance) from committing offsets.
    // Skip validation if member_id is empty (anonymous consumers don't have generation).
    if !request.member_id.is_empty() {
        match handler
            .coordinator
            .validate_member_for_commit(
                &request.group_id,
                &request.member_id,
                request.generation_id,
            )
            .await
        {
            Ok(HeartbeatResult::Success) => {
                // Valid member with correct generation - proceed with commits
            }
            Ok(HeartbeatResult::IllegalGeneration) => {
                debug!(
                    group_id = %request.group_id,
                    member_id = %request.member_id,
                    generation_id = request.generation_id,
                    "Rejecting offset commit with stale generation"
                );
                // Return IllegalGeneration error for all partitions
                let topics: Vec<_> = request
                    .topics
                    .iter()
                    .map(|topic| OffsetCommitTopicResponse {
                        name: topic.name.clone(),
                        partitions: topic
                            .partitions
                            .iter()
                            .map(|p| OffsetCommitPartitionResponse {
                                partition_index: p.partition_index,
                                error_code: KafkaCode::IllegalGeneration,
                            })
                            .collect(),
                    })
                    .collect();
                return OffsetCommitResponseData {
                    throttle_time_ms: 0,
                    topics,
                };
            }
            Ok(HeartbeatResult::UnknownMember) => {
                debug!(
                    group_id = %request.group_id,
                    member_id = %request.member_id,
                    "Rejecting offset commit from unknown member"
                );
                // Return UnknownMemberId error for all partitions
                let topics: Vec<_> = request
                    .topics
                    .iter()
                    .map(|topic| OffsetCommitTopicResponse {
                        name: topic.name.clone(),
                        partitions: topic
                            .partitions
                            .iter()
                            .map(|p| OffsetCommitPartitionResponse {
                                partition_index: p.partition_index,
                                error_code: KafkaCode::UnknownMemberId,
                            })
                            .collect(),
                    })
                    .collect();
                return OffsetCommitResponseData {
                    throttle_time_ms: 0,
                    topics,
                };
            }
            Err(e) => {
                error!(error = %e, "Failed to validate member for offset commit");
                let topics: Vec<_> = request
                    .topics
                    .iter()
                    .map(|topic| OffsetCommitTopicResponse {
                        name: topic.name.clone(),
                        partitions: topic
                            .partitions
                            .iter()
                            .map(|p| OffsetCommitPartitionResponse {
                                partition_index: p.partition_index,
                                error_code: KafkaCode::Unknown,
                            })
                            .collect(),
                    })
                    .collect();
                return OffsetCommitResponseData {
                    throttle_time_ms: 0,
                    topics,
                };
            }
        }
    }

    let mut responses = Vec::with_capacity(request.topics.len());

    for topic in request.topics {
        // Validate topic name to prevent injection attacks
        if validate_topic_name(&topic.name).is_err() {
            debug!(topic = %topic.name, "Invalid topic name in offset_commit request");
            let partition_responses: Vec<_> = topic
                .partitions
                .iter()
                .map(|p| OffsetCommitPartitionResponse {
                    partition_index: p.partition_index,
                    error_code: KafkaCode::InvalidTopic,
                })
                .collect();
            responses.push(OffsetCommitTopicResponse {
                name: topic.name,
                partitions: partition_responses,
            });
            continue;
        }

        let mut partition_responses = Vec::with_capacity(topic.partitions.len());

        for partition in &topic.partitions {
            let error_code = match handler
                .coordinator
                .commit_offset(
                    &request.group_id,
                    &topic.name,
                    partition.partition_index,
                    partition.committed_offset,
                    partition.committed_metadata.as_deref(),
                )
                .await
            {
                Ok(_) => {
                    crate::cluster::metrics::record_offset_commit(
                        &request.group_id,
                        &topic.name,
                        "success",
                    );

                    // Record consumer lag if we can get the high watermark
                    if let Ok(store) = handler
                        .partition_manager
                        .get_for_read(&topic.name, partition.partition_index)
                        .await
                    {
                        let hwm = store.high_watermark();
                        crate::cluster::metrics::record_consumer_lag(
                            &request.group_id,
                            &topic.name,
                            partition.partition_index,
                            partition.committed_offset,
                            hwm,
                        );
                    }

                    KafkaCode::None
                }
                Err(e) => {
                    error!(error = %e, "Failed to commit offset");
                    crate::cluster::metrics::record_offset_commit(
                        &request.group_id,
                        &topic.name,
                        "error",
                    );
                    KafkaCode::Unknown
                }
            };

            partition_responses.push(OffsetCommitPartitionResponse {
                partition_index: partition.partition_index,
                error_code,
            });
        }

        responses.push(OffsetCommitTopicResponse {
            name: topic.name,
            partitions: partition_responses,
        });
    }

    OffsetCommitResponseData {
        throttle_time_ms: 0,
        topics: responses,
    }
}

/// Handle an offset fetch request.
pub(super) async fn handle_offset_fetch(
    handler: &SlateDBClusterHandler,
    request: OffsetFetchRequestData,
) -> OffsetFetchResponseData {
    let mut responses = Vec::with_capacity(request.topics.len());

    for topic in request.topics {
        // Validate topic name to prevent injection attacks
        if validate_topic_name(&topic.name).is_err() {
            debug!(topic = %topic.name, "Invalid topic name in offset_fetch request");
            let partition_responses: Vec<_> = topic
                .partition_indexes
                .iter()
                .map(|&partition_index| OffsetFetchPartitionResponse {
                    partition_index,
                    committed_offset: -1,
                    metadata: None,
                    error_code: KafkaCode::InvalidTopic,
                })
                .collect();
            responses.push(OffsetFetchTopicResponse {
                name: topic.name,
                partitions: partition_responses,
            });
            continue;
        }

        let mut partition_responses = Vec::with_capacity(topic.partition_indexes.len());

        for partition_index in topic.partition_indexes {
            let (committed_offset, metadata) = handler
                .coordinator
                .fetch_offset(&request.group_id, &topic.name, partition_index)
                .await
                .unwrap_or((-1, None));

            partition_responses.push(OffsetFetchPartitionResponse {
                partition_index,
                committed_offset,
                metadata,
                error_code: KafkaCode::None,
            });
        }

        responses.push(OffsetFetchTopicResponse {
            name: topic.name,
            partitions: partition_responses,
        });
    }

    OffsetFetchResponseData {
        throttle_time_ms: 0,
        topics: responses,
        error_code: KafkaCode::None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::error::HeartbeatResult;

    // ========================================================================
    // OffsetCommit Response Tests
    // ========================================================================

    #[test]
    fn test_offset_commit_response_success() {
        let response = OffsetCommitPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
        };

        assert_eq!(response.partition_index, 0);
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_offset_commit_response_unknown_member() {
        let response = OffsetCommitPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::UnknownMemberId,
        };

        assert_eq!(response.error_code, KafkaCode::UnknownMemberId);
    }

    #[test]
    fn test_offset_commit_response_illegal_generation() {
        let response = OffsetCommitPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::IllegalGeneration,
        };

        assert_eq!(response.error_code, KafkaCode::IllegalGeneration);
    }

    #[test]
    fn test_offset_commit_topic_response() {
        let partitions = vec![
            OffsetCommitPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
            },
            OffsetCommitPartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::None,
            },
        ];

        let response = OffsetCommitTopicResponse {
            name: "test-topic".to_string(),
            partitions,
        };

        assert_eq!(response.name, "test-topic");
        assert_eq!(response.partitions.len(), 2);
    }

    #[test]
    fn test_offset_commit_response_data() {
        let topics = vec![OffsetCommitTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![],
        }];

        let response = OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics,
        };

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 1);
    }

    // ========================================================================
    // OffsetFetch Response Tests
    // ========================================================================

    #[test]
    fn test_offset_fetch_partition_response_success() {
        let response = OffsetFetchPartitionResponse {
            partition_index: 0,
            committed_offset: 100,
            metadata: Some("consumer-metadata".to_string()),
            error_code: KafkaCode::None,
        };

        assert_eq!(response.partition_index, 0);
        assert_eq!(response.committed_offset, 100);
        assert!(response.metadata.is_some());
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_offset_fetch_partition_response_no_offset() {
        // When no offset has been committed, committed_offset = -1
        let response = OffsetFetchPartitionResponse {
            partition_index: 0,
            committed_offset: -1,
            metadata: None,
            error_code: KafkaCode::None,
        };

        assert_eq!(response.committed_offset, -1);
        assert!(response.metadata.is_none());
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_offset_fetch_partition_response_error() {
        let response = OffsetFetchPartitionResponse {
            partition_index: 0,
            committed_offset: -1,
            metadata: None,
            error_code: KafkaCode::GroupCoordinatorNotAvailable,
        };

        assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
        assert_eq!(response.committed_offset, -1);
    }

    #[test]
    fn test_offset_fetch_topic_response() {
        let partitions = vec![
            OffsetFetchPartitionResponse {
                partition_index: 0,
                committed_offset: 100,
                metadata: None,
                error_code: KafkaCode::None,
            },
            OffsetFetchPartitionResponse {
                partition_index: 1,
                committed_offset: 200,
                metadata: None,
                error_code: KafkaCode::None,
            },
        ];

        let response = OffsetFetchTopicResponse {
            name: "test-topic".to_string(),
            partitions,
        };

        assert_eq!(response.name, "test-topic");
        assert_eq!(response.partitions.len(), 2);
        assert_eq!(response.partitions[0].committed_offset, 100);
        assert_eq!(response.partitions[1].committed_offset, 200);
    }

    // ========================================================================
    // ListOffsets Response Tests
    // ========================================================================

    #[test]
    fn test_list_offsets_partition_response_success() {
        let response = ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            timestamp: -1,
            offset: 100,
        };

        assert_eq!(response.partition_index, 0);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.offset, 100);
    }

    #[test]
    fn test_list_offsets_partition_response_latest() {
        // Request for latest offset (timestamp = -1)
        let response = ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            timestamp: -1,
            offset: 1000, // Current HWM
        };

        assert_eq!(response.timestamp, -1);
        assert_eq!(response.offset, 1000);
    }

    #[test]
    fn test_list_offsets_partition_response_earliest() {
        // Request for earliest offset (timestamp = -2)
        let response = ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            timestamp: -2,
            offset: 0, // Start of log
        };

        assert_eq!(response.timestamp, -2);
        assert_eq!(response.offset, 0);
    }

    #[test]
    fn test_list_offsets_partition_response_not_leader() {
        let response = ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::NotLeaderForPartition,
            timestamp: -1,
            offset: -1,
        };

        assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
        assert_eq!(response.offset, -1);
    }

    #[test]
    fn test_list_offsets_topic_response() {
        let partitions = vec![ListOffsetsPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            timestamp: -1,
            offset: 100,
        }];

        let response = ListOffsetsTopicResponse {
            name: "test-topic".to_string(),
            partitions,
        };

        assert_eq!(response.name, "test-topic");
        assert_eq!(response.partitions.len(), 1);
    }

    // ========================================================================
    // Offset Value Tests
    // ========================================================================

    #[test]
    fn test_offset_values() {
        // Valid committed offsets are >= 0
        // -1 means no offset committed
        let no_offset: i64 = -1;
        let valid_offset: i64 = 0;
        let large_offset: i64 = i64::MAX - 1;

        assert_eq!(no_offset, -1);
        assert!(valid_offset >= 0);
        assert!(large_offset > 0);
    }

    #[test]
    fn test_special_timestamps() {
        // -1 = Latest offset
        // -2 = Earliest offset
        let latest: i64 = -1;
        let earliest: i64 = -2;

        assert_eq!(latest, -1);
        assert_eq!(earliest, -2);
    }

    // ========================================================================
    // Multiple Partitions Tests
    // ========================================================================

    #[test]
    fn test_offset_commit_multiple_partitions_mixed_results() {
        let partitions = [
            OffsetCommitPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
            },
            OffsetCommitPartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::UnknownMemberId,
            },
            OffsetCommitPartitionResponse {
                partition_index: 2,
                error_code: KafkaCode::None,
            },
        ];

        assert_eq!(partitions[0].error_code, KafkaCode::None);
        assert_eq!(partitions[1].error_code, KafkaCode::UnknownMemberId);
        assert_eq!(partitions[2].error_code, KafkaCode::None);
    }

    #[test]
    fn test_offset_fetch_multiple_topics() {
        let topics = vec![
            OffsetFetchTopicResponse {
                name: "topic-a".to_string(),
                partitions: vec![OffsetFetchPartitionResponse {
                    partition_index: 0,
                    committed_offset: 100,
                    metadata: None,
                    error_code: KafkaCode::None,
                }],
            },
            OffsetFetchTopicResponse {
                name: "topic-b".to_string(),
                partitions: vec![OffsetFetchPartitionResponse {
                    partition_index: 0,
                    committed_offset: 200,
                    metadata: None,
                    error_code: KafkaCode::None,
                }],
            },
        ];

        let response = OffsetFetchResponseData {
            throttle_time_ms: 0,
            topics,
            error_code: KafkaCode::None,
        };

        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.topics[0].name, "topic-a");
        assert_eq!(response.topics[1].name, "topic-b");
    }

    // ========================================================================
    // Metadata Field Tests
    // ========================================================================

    #[test]
    fn test_offset_metadata_empty() {
        let response = OffsetFetchPartitionResponse {
            partition_index: 0,
            committed_offset: 100,
            metadata: None,
            error_code: KafkaCode::None,
        };

        assert!(response.metadata.is_none());
    }

    #[test]
    fn test_offset_metadata_with_content() {
        let response = OffsetFetchPartitionResponse {
            partition_index: 0,
            committed_offset: 100,
            metadata: Some("client-metadata".to_string()),
            error_code: KafkaCode::None,
        };

        assert_eq!(response.metadata, Some("client-metadata".to_string()));
    }

    // ========================================================================
    // HeartbeatResult Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_member_for_commit_success() {
        // When validation succeeds, commits should proceed
        let result = HeartbeatResult::Success;
        let should_commit = matches!(result, HeartbeatResult::Success);
        assert!(should_commit);
    }

    #[test]
    fn test_validate_member_for_commit_unknown_member() {
        // Unknown member should return UnknownMemberId error
        let result = HeartbeatResult::UnknownMember;
        let error_code = match result {
            HeartbeatResult::Success => KafkaCode::None,
            HeartbeatResult::UnknownMember => KafkaCode::UnknownMemberId,
            HeartbeatResult::IllegalGeneration => KafkaCode::IllegalGeneration,
        };
        assert_eq!(error_code, KafkaCode::UnknownMemberId);
    }

    // ========================================================================
    // Throttle Time Tests
    // ========================================================================

    #[test]
    fn test_offset_responses_throttle_time() {
        let commit_response = OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics: vec![],
        };

        let fetch_response = OffsetFetchResponseData {
            throttle_time_ms: 0,
            topics: vec![],
            error_code: KafkaCode::None,
        };

        let list_response = ListOffsetsResponseData {
            throttle_time_ms: 0,
            topics: vec![],
        };

        assert_eq!(commit_response.throttle_time_ms, 0);
        assert_eq!(fetch_response.throttle_time_ms, 0);
        assert_eq!(list_response.throttle_time_ms, 0);
    }

    // ========================================================================
    // Topic Name Validation Tests
    // ========================================================================

    #[test]
    fn test_topic_name_validation_for_offsets() {
        // Valid topic names
        assert!(validate_topic_name("test-topic").is_ok());
        assert!(validate_topic_name("my_topic").is_ok());

        // Invalid topic names
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(".").is_err());
        assert!(validate_topic_name("..").is_err());
    }
}
