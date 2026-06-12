//! Offset request handling (list, commit, fetch).

use tracing::{debug, error, info};

use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::{
    ListOffsetsRequestData, OffsetCommitRequestData, OffsetFetchRequestData,
};
use crate::server::response::{
    ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
    OffsetCommitPartitionResponse, OffsetCommitResponseData, OffsetCommitTopicResponse,
    OffsetFetchPartitionResponse, OffsetFetchResponseData, OffsetFetchTopicResponse,
};

use super::SlateDBClusterHandler;
use crate::cluster::authorizer::{AuthorizeRequest, AuthorizeResult};
use crate::cluster::coordinator::{validate_group_id, validate_topic_name};
use crate::cluster::raft::{AclOperation, AclResourceType};
use crate::cluster::traits::ConsumerGroupCoordinator;

/// Handle a list offsets request.
///
/// ListOffsets requires `Describe` on each topic (per Kafka):
/// without this gate any client can probe high watermarks — i.e. message
/// volume and activity — for every topic in the cluster.
pub(super) async fn handle_list_offsets(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: ListOffsetsRequestData,
) -> ListOffsetsResponseData {
    // Refuse on a fenced/zombie broker. Without this gate the broker keeps
    // serving offset metadata after losing leadership; clients use those
    // values to drive consumption from the (real) new owner and can race
    // ahead of the new owner's recovered HWM.
    if handler.partition_manager.is_zombie() {
        let topics: Vec<_> = request
            .topics
            .into_iter()
            .map(|t| {
                let partitions = t
                    .partitions
                    .into_iter()
                    .map(|p| ListOffsetsPartitionResponse {
                        partition_index: p.partition_index,
                        error_code: KafkaCode::NotLeaderForPartition,
                        timestamp: -1,
                        offset: -1,
                    })
                    .collect();
                ListOffsetsTopicResponse {
                    name: t.name,
                    partitions,
                }
            })
            .collect();
        return ListOffsetsResponseData {
            throttle_time_ms: 0,
            topics,
        };
    }

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

        if handler
            .authorizer
            .authorize(AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation: AclOperation::Describe,
                resource_type: AclResourceType::Topic,
                resource_name: &topic.name,
            })
            .await
            == AuthorizeResult::Denied
        {
            info!(
                target: "audit",
                topic = %topic.name,
                principal = %ctx.principal,
                api = "ListOffsets",
                operation = "Describe",
                "ACL denied: ListOffsets"
            );
            let partitions: Vec<_> = topic
                .partitions
                .iter()
                .map(|p| ListOffsetsPartitionResponse {
                    partition_index: p.partition_index,
                    error_code: KafkaCode::TopicAuthorizationFailed,
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
            let (error_code, offset, response_timestamp) = match handler
                .partition_manager
                .get_for_read(&topic.name, partition.partition_index)
                .await
            {
                Ok(store) => match partition.timestamp {
                    // Earliest (-2): log start offset.
                    -2 => (
                        KafkaCode::None,
                        store.earliest_offset().await.unwrap_or(0),
                        -1,
                    ),
                    // Latest (-1): high watermark.
                    -1 => (KafkaCode::None, store.high_watermark(), -1),
                    // Actual timestamp: first offset whose batch timestamp
                    // is >= the target. Previously this silently returned
                    // the HWM, which made timestamp-based seeks
                    // (`offsetsForTimes`) skip ALL existing data.
                    ts => match store.offset_for_timestamp(ts).await {
                        // Hit: return both the matching offset AND the
                        // batch's max_timestamp. Kafka v1+ ListOffsets
                        // requires the response timestamp so the consumer
                        // can build `OffsetAndTimestamp` entries; returning
                        // -1 here would make clients filter out the result.
                        Ok(Some((offset, batch_ts))) => (KafkaCode::None, offset, batch_ts),
                        // No batch at/after the timestamp: Kafka returns -1
                        // ("no offset") rather than an error.
                        Ok(None) => (KafkaCode::None, -1, -1),
                        Err(e) => {
                            error!(
                                error = %e,
                                topic = %topic.name,
                                partition = partition.partition_index,
                                "Timestamp offset lookup failed"
                            );
                            (e.to_kafka_code(), -1, -1)
                        }
                    },
                },
                Err(_) => (KafkaCode::NotLeaderForPartition, -1, -1),
            };

            partitions.push(ListOffsetsPartitionResponse {
                partition_index: partition.partition_index,
                error_code,
                timestamp: response_timestamp,
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
    ctx: &RequestContext,
    request: OffsetCommitRequestData,
) -> OffsetCommitResponseData {
    use crate::cluster::error::HeartbeatResult;

    // Reject malformed group IDs at the door so an authenticated client
    // cannot grow the replicated state machine by spamming arbitrary or
    // control-character-laden `group_id` strings.
    if validate_group_id(&request.group_id).is_err() {
        debug!(group_id = %request.group_id, "Invalid group ID in offset_commit request");
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
                        error_code: KafkaCode::InvalidGroupId,
                    })
                    .collect(),
            })
            .collect();
        return OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics,
        };
    }

    // OffsetCommit requires Read on the Group resource (per
    // Kafka). Topic-level Read is also required and is enforced inline
    // per topic below.
    if handler
        .authorizer
        .authorize(AuthorizeRequest {
            principal: &ctx.principal,
            host: &ctx.client_host,
            operation: AclOperation::Read,
            resource_type: AclResourceType::Group,
            resource_name: &request.group_id,
        })
        .await
        == AuthorizeResult::Denied
    {
        info!(
            target: "audit",
            group_id = %request.group_id,
            principal = %ctx.principal,
            api = "OffsetCommit",
            operation = "Read",
            resource = "Group",
            "ACL denied: OffsetCommit (group)"
        );
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
                        error_code: KafkaCode::GroupAuthorizationFailed,
                    })
                    .collect(),
            })
            .collect();
        return OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics,
        };
    }

    // Validate member and generation before allowing any commits.
    // This prevents stale consumers (from before a rebalance) from clobbering
    // offsets.
    //
    // Kafka fencing rules:
    // - generation_id == -1 with an empty member_id is a "simple consumer"
    //   commit (manual offset management outside group membership) and is
    //   allowed WITHOUT fencing.
    // - Any commit with a non-negative generation_id OR a non-empty
    //   member_id claims group membership and MUST be validated against the
    //   group's current generation/membership (IllegalGeneration /
    //   UnknownMemberId on mismatch).
    // Only the canonical "no membership claimed" sentinel — `generation_id == -1`
    // AND empty `member_id` — counts as a simple-consumer commit. A buggy or
    // malicious client setting `generation_id = -2` with an empty member id
    // must NOT bypass fencing.
    let is_simple_consumer_commit = request.generation_id == -1 && request.member_id.is_empty();
    if is_simple_consumer_commit {
        // Refuse simple commits against a group that already has active
        // members: the caller would otherwise clobber the rebalance-managed
        // offsets with no fencing.
        match handler
            .coordinator
            .get_group_members(&request.group_id)
            .await
        {
            Ok(members) if !members.is_empty() => {
                debug!(
                    group_id = %request.group_id,
                    member_count = members.len(),
                    "Rejecting simple-consumer offset commit on a group with active members"
                );
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
            _ => {}
        }
    } else {
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
            Ok(HeartbeatResult::RebalanceInProgress) => {
                debug!(
                    group_id = %request.group_id,
                    member_id = %request.member_id,
                    "Rejecting offset commit while group is rebalancing"
                );
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
                                error_code: KafkaCode::RebalanceInProgress,
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
                let kafka_code = e.to_kafka_code();
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
                                error_code: kafka_code,
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

        // OffsetCommit also requires Read on each topic.
        if handler
            .authorizer
            .authorize(AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation: AclOperation::Read,
                resource_type: AclResourceType::Topic,
                resource_name: &topic.name,
            })
            .await
            == AuthorizeResult::Denied
        {
            info!(
                target: "audit",
                topic = %topic.name,
                principal = %ctx.principal,
                api = "OffsetCommit",
                operation = "Read",
                resource = "Topic",
                "ACL denied: OffsetCommit (topic)"
            );
            let partition_responses: Vec<_> = topic
                .partitions
                .iter()
                .map(|p| OffsetCommitPartitionResponse {
                    partition_index: p.partition_index,
                    error_code: KafkaCode::TopicAuthorizationFailed,
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
            // Cap offset-commit metadata size. The bytes go straight into the
            // replicated state machine on every commit; an unbounded blob
            // bloats Raft snapshots and stretches every subsequent
            // append-entries call. 4 KiB matches Kafka's
            // `offset.metadata.max.bytes` default.
            const OFFSET_METADATA_MAX_BYTES: usize = 4096;
            if let Some(ref m) = partition.committed_metadata
                && m.len() > OFFSET_METADATA_MAX_BYTES
            {
                debug!(
                    group_id = %request.group_id,
                    topic = %topic.name,
                    partition = partition.partition_index,
                    metadata_bytes = m.len(),
                    "Rejecting OffsetCommit: metadata exceeds {} bytes",
                    OFFSET_METADATA_MAX_BYTES
                );
                partition_responses.push(OffsetCommitPartitionResponse {
                    partition_index: partition.partition_index,
                    error_code: KafkaCode::OffsetMetadataTooLarge,
                });
                continue;
            }

            // Bound the committed offset against the partition's live
            // [log_start_offset, high_watermark] window. Without this,
            // committing `i64::MAX` makes a consumer silently skip every
            // future record; committing below the LSO commits an offset
            // whose data has already been retention-purged, surfacing as a
            // delayed `OFFSET_OUT_OF_RANGE` on the next fetch.
            let bounds_error = match handler
                .partition_manager
                .get_for_read(&topic.name, partition.partition_index)
                .await
            {
                Ok(store) => {
                    let hwm = store.high_watermark();
                    let log_start = store.log_start_offset();
                    if partition.committed_offset < log_start
                        || partition.committed_offset > hwm
                    {
                        debug!(
                            group_id = %request.group_id,
                            topic = %topic.name,
                            partition = partition.partition_index,
                            committed_offset = partition.committed_offset,
                            log_start,
                            hwm,
                            "Rejecting OffsetCommit: offset outside [log_start, hwm]"
                        );
                        Some(KafkaCode::OffsetOutOfRange)
                    } else {
                        None
                    }
                }
                // Partition not locally available — defer to the existing
                // commit path; if the topic itself is unknown the commit
                // call will surface the right error.
                Err(_) => None,
            };

            let error_code = if let Some(code) = bounds_error {
                code
            } else {
                match handler
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
                        e.to_kafka_code()
                    }
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
    ctx: &RequestContext,
    request: OffsetFetchRequestData,
) -> OffsetFetchResponseData {
    if validate_group_id(&request.group_id).is_err() {
        debug!(group_id = %request.group_id, "Invalid group ID in offset_fetch");
        return OffsetFetchResponseData {
            throttle_time_ms: 0,
            topics: vec![],
            error_code: KafkaCode::InvalidGroupId,
        };
    }
    // OffsetFetch requires Describe on the Group resource.
    if handler
        .authorizer
        .authorize(AuthorizeRequest {
            principal: &ctx.principal,
            host: &ctx.client_host,
            operation: AclOperation::Describe,
            resource_type: AclResourceType::Group,
            resource_name: &request.group_id,
        })
        .await
        == AuthorizeResult::Denied
    {
        info!(
            target: "audit",
            group_id = %request.group_id,
            principal = %ctx.principal,
            api = "OffsetFetch",
            operation = "Describe",
            resource = "Group",
            "ACL denied: OffsetFetch"
        );
        let topics: Vec<_> = request
            .topics
            .iter()
            .map(|topic| OffsetFetchTopicResponse {
                name: topic.name.clone(),
                partitions: topic
                    .partition_indexes
                    .iter()
                    .map(|&partition_index| OffsetFetchPartitionResponse {
                        partition_index,
                        committed_offset: -1,
                        metadata: None,
                        error_code: KafkaCode::GroupAuthorizationFailed,
                    })
                    .collect(),
            })
            .collect();
        return OffsetFetchResponseData {
            throttle_time_ms: 0,
            topics,
            error_code: KafkaCode::GroupAuthorizationFailed,
        };
    }

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

        // Also require Describe on each topic.
        if handler
            .authorizer
            .authorize(AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation: AclOperation::Describe,
                resource_type: AclResourceType::Topic,
                resource_name: &topic.name,
            })
            .await
            == AuthorizeResult::Denied
        {
            info!(
                target: "audit",
                topic = %topic.name,
                principal = %ctx.principal,
                api = "OffsetFetch",
                operation = "Describe",
                resource = "Topic",
                "ACL denied: OffsetFetch (topic)"
            );
            let partition_responses: Vec<_> = topic
                .partition_indexes
                .iter()
                .map(|&partition_index| OffsetFetchPartitionResponse {
                    partition_index,
                    committed_offset: -1,
                    metadata: None,
                    error_code: KafkaCode::TopicAuthorizationFailed,
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
            // Distinguish "no offset committed" (Ok(-1)) from a coordinator
            // failure (Err). Collapsing both to committed_offset=-1 with no
            // error code makes the consumer interpret a transient Raft hiccup
            // as "consume from the configured reset policy" — which on
            // `auto.offset.reset=earliest` re-reads the entire log, and on
            // `latest` silently skips records produced before the next poll.
            let (committed_offset, metadata, error_code) = match handler
                .coordinator
                .fetch_offset(&request.group_id, &topic.name, partition_index)
                .await
            {
                Ok((offset, metadata)) => (offset, metadata, KafkaCode::None),
                Err(e) => {
                    debug!(
                        topic = %topic.name,
                        partition = partition_index,
                        group = %request.group_id,
                        error = %e,
                        "Coordinator failure during fetch_offset - returning typed error"
                    );
                    (-1, None, e.to_kafka_code())
                }
            };

            partition_responses.push(OffsetFetchPartitionResponse {
                partition_index,
                committed_offset,
                metadata,
                error_code,
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
            HeartbeatResult::RebalanceInProgress => KafkaCode::RebalanceInProgress,
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
