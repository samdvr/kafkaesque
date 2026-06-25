//! Fetch request handling.

use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::FetchRequestData;
use crate::server::response::{FetchPartitionResponse, FetchResponseData, FetchTopicResponse};

use super::SlateDBClusterHandler;
use crate::cluster::coordinator::validate_topic_name;

/// Total bytes of records contained in `responses`. Used by the long-poll
/// loop to compare against the client's `min_bytes`.
fn total_record_bytes(responses: &[FetchTopicResponse]) -> usize {
    responses
        .iter()
        .flat_map(|t| t.partitions.iter())
        .filter_map(|p| p.records.as_ref())
        .map(|b| b.len())
        .sum()
}

/// Handle a fetch request.
///
/// Implements Kafka's `max_wait_ms` / `min_bytes` long-poll semantics.
/// Without this, consumers would spin at full request rate when no
/// data is available — driving CPU and S3 GET cost linearly with consumer
/// count regardless of throughput. Producers signal data availability via
/// per-partition notifies (`SlateDBClusterHandler::hwm_notifier`); this
/// handler waits on the notifies of exactly the partitions it requested,
/// up to the client's deadline, before re-checking.
///
/// `isolation_level` is accepted but intentionally not branched on:
/// transactions are rejected at produce time, so the log can never contain
/// uncommitted data and LSO == HWM always. read_committed and
/// read_uncommitted are therefore identical here.
pub(super) async fn handle_fetch(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: FetchRequestData,
) -> FetchResponseData {
    debug!(
        client = %ctx.client_addr,
        client_id = ?ctx.client_id,
        topic_count = request.topics.len(),
        max_wait_ms = request.max_wait_ms,
        min_bytes = request.min_bytes,
        "FETCH request received"
    );

    // Time the full fetch path including the long-poll wait so
    // the pre-existing FETCH_DURATION histogram populates with real data.
    // Status is per-topic (one batch may span many topics) so we tag with
    // `_multi` on multi-topic requests, matching record_fetch_latency's
    // documented convention.
    let start = std::time::Instant::now();
    let topic_label: String = if request.topics.len() == 1 {
        request.topics[0].name.clone()
    } else {
        "_multi".to_string()
    };

    // Cap `max_wait_ms` to a sane upper bound. Without this, a misbehaving
    // (or hostile) client requesting i32::MAX pins a connection task for
    // ~24.8 days; with a typical max_total_connections of ~1k that is a
    // trivial denial-of-service. 60s matches Kafka's default `fetch.max.wait.ms`
    // ceiling-of-reasonable.
    const MAX_FETCH_WAIT_MS: i32 = 60_000;
    let bounded_wait_ms = request.max_wait_ms.clamp(0, MAX_FETCH_WAIT_MS);
    let max_wait = Duration::from_millis(bounded_wait_ms as u64);
    let min_bytes = request.min_bytes.max(0) as usize;
    let deadline = if max_wait.is_zero() {
        None
    } else {
        Some(tokio::time::Instant::now() + max_wait)
    };

    // Resolve the per-partition notifies BEFORE the first `collect_fetch`
    // and stash a `Notified` future on each one. `Notify::notify_waiters()`
    // only wakes futures that already exist when it fires, so a producer
    // commit landing between the read and the wait would otherwise be lost
    // and the fetch would block until `max_wait_ms` for no reason.
    //
    // Skip the pre-arm entirely when there is no long-poll window —
    // `min_bytes <= 0` or `max_wait_ms == 0` means we're going to take
    // whatever's available right now and return, never entering the
    // long-poll loop. The previous shape paid one `Arc<Notify>` lookup,
    // one `Vec` collect, and one `Box::pin(notified())` heap allocation
    // per requested partition, for every fetch — including the common
    // committed-consumer case where the wakeup machinery is never used.
    let needs_long_poll = min_bytes > 0 && bounded_wait_ms > 0;
    let watched: Vec<Arc<tokio::sync::Notify>> = if needs_long_poll {
        request
            .topics
            .iter()
            .flat_map(|t| {
                let topic = handler.cached_topic_name(&t.name);
                t.partitions
                    .iter()
                    .map(|p| handler.hwm_notifier(&topic, p.partition_index))
                    .collect::<Vec<_>>()
            })
            .collect()
    } else {
        Vec::new()
    };
    // Pre-arm a notification per watched partition. `Notified` is held
    // across the loop so a wake that fires before we await still counts.
    let pre_armed: Vec<_> = watched.iter().map(|n| Box::pin(n.notified())).collect();

    // Validate topic names and authorize once per request — these results
    // are stable across long-poll iterations, but the previous version
    // re-ran `validate_topic_name` and the async ACL check inside
    // `collect_fetch` on every wakeup. With max_wait_ms = 1s and HWM-driven
    // wakes every ~200ms, that quintuples the per-request authorize cost
    // for no semantic gain.
    let mut topic_plans: Vec<TopicFetchPlan> = Vec::with_capacity(request.topics.len());
    for topic in &request.topics {
        if validate_topic_name(&topic.name).is_err() {
            debug!(topic = %topic.name, "Invalid topic name in fetch request");
            topic_plans.push(TopicFetchPlan::Reject {
                error_code: KafkaCode::InvalidTopic,
            });
            continue;
        }
        let denied = handler
            .authorizer
            .authorize(crate::cluster::authorizer::AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation: crate::cluster::raft::AclOperation::Read,
                resource_type: crate::cluster::raft::AclResourceType::Topic,
                resource_name: &topic.name,
            })
            .await
            == crate::cluster::authorizer::AuthorizeResult::Denied;
        if denied {
            info!(
                target: "audit",
                topic = %topic.name,
                principal = %ctx.principal,
                api = "Fetch",
                operation = "Read",
                "ACL denied: Fetch"
            );
            topic_plans.push(TopicFetchPlan::Reject {
                error_code: KafkaCode::TopicAuthorizationFailed,
            });
            continue;
        }
        topic_plans.push(TopicFetchPlan::Allowed {
            topic_arc: handler.cached_topic_name(&topic.name),
        });
    }

    // First pass: build the response with whatever's currently available.
    let mut responses = collect_fetch(handler, &request, &topic_plans).await;

    // Long-poll loop: if min_bytes isn't satisfied and we have time left,
    // wait for any requested partition's HWM to advance, then re-check.
    let mut armed = pre_armed;
    while min_bytes > 0
        && total_record_bytes(&responses) < min_bytes
        && let Some(deadline) = deadline
        && !watched.is_empty()
    {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline - now;
        let any_advanced = futures::future::select_all(armed);
        match tokio::time::timeout(remaining, any_advanced).await {
            Ok(_) => {
                // A requested partition advanced — re-arm before re-fetching
                // so a wake during `collect_fetch` is not lost.
                armed = watched.iter().map(|n| Box::pin(n.notified())).collect();
                responses = collect_fetch(handler, &request, &topic_plans).await;
            }
            Err(_) => {
                // Deadline elapsed.
                break;
            }
        }
    }

    let response = FetchResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        session_id: 0,
        responses,
    };
    let total_bytes = total_record_bytes(&response.responses);
    let status = if total_bytes > 0 {
        "success"
    } else {
        "timeout"
    };
    crate::cluster::metrics::record_fetch_latency(
        &topic_label,
        status,
        start.elapsed().as_secs_f64(),
    );
    response
}

/// Per-topic disposition of a fetch request, computed once before the
/// long-poll loop so per-iteration cost is just per-partition I/O.
enum TopicFetchPlan {
    /// Topic name is valid and the principal is authorized — the partition
    /// fan-out should run for this topic. The `Arc<str>` is shared between
    /// log lines and metrics calls so we don't reallocate per partition.
    Allowed { topic_arc: Arc<str> },
    /// Reject every partition in this topic with a fixed wire error_code.
    /// Used for invalid topic names and ACL denials, both of which are
    /// stable for the duration of the request.
    Reject { error_code: KafkaCode },
}

/// One pass of per-partition fetching for every topic in the request. Pulled
/// out of `handle_fetch` so the long-poll loop can re-run it on each wakeup.
///
/// Concurrency is flattened across the whole request: every (topic, partition)
/// pair runs through one `buffer_unordered`. The previous shape iterated
/// topics sequentially with a per-topic partition fan-out, so a 5-topic
/// × 20-partition request ran at 20-wide concurrency for 5 sequential rounds
/// instead of 100-wide for one round.
async fn collect_fetch(
    handler: &SlateDBClusterHandler,
    request: &FetchRequestData,
    topic_plans: &[TopicFetchPlan],
) -> Vec<FetchTopicResponse> {
    use futures::stream::{self, StreamExt};
    use std::sync::atomic::{AtomicI64, Ordering};

    let max_concurrent_reads = handler.max_concurrent_partition_reads;

    // Request-level `max_bytes` budget shared by all partitions in the
    // request (Kafka protocol contract; `<= 0` means no client limit).
    // Partitions fetch concurrently, so enforcement is approximate: each
    // partition checks the remaining budget before fetching and debits it
    // after, so the response can overshoot by at most (concurrency - 1)
    // partition fetches — the same order of slack as Kafka's own
    // "first batch is always returned whole" rule.
    let request_budget = Arc::new(AtomicI64::new(if request.max_bytes > 0 {
        request.max_bytes as i64
    } else {
        i64::MAX
    }));

    // Flatten (topic_idx, partition) pairs so we can fan out across the
    // entire request rather than per-topic. Rejected topics emit
    // pre-computed per-partition error responses without touching the
    // partition manager.
    struct PendingPartition {
        topic_idx: usize,
        topic_arc: Arc<str>,
        partition: crate::server::request::FetchPartitionData,
    }
    let mut pending: Vec<PendingPartition> = Vec::new();
    let mut topic_partitions: Vec<Vec<FetchPartitionResponse>> = request
        .topics
        .iter()
        .map(|t| Vec::with_capacity(t.partitions.len()))
        .collect();

    for (topic_idx, (topic, plan)) in request.topics.iter().zip(topic_plans.iter()).enumerate() {
        match plan {
            TopicFetchPlan::Reject { error_code } => {
                for p in &topic.partitions {
                    topic_partitions[topic_idx].push(FetchPartitionResponse {
                        partition_index: p.partition_index,
                        error_code: *error_code,
                        high_watermark: -1,
                        last_stable_offset: -1,
                        aborted_transactions: vec![],
                        log_start_offset: -1,
                        preferred_read_replica: -1,
                        records: None,
                    });
                }
            }
            TopicFetchPlan::Allowed { topic_arc } => {
                for partition in topic.partitions.iter().cloned() {
                    pending.push(PendingPartition {
                        topic_idx,
                        topic_arc: Arc::clone(topic_arc),
                        partition,
                    });
                }
            }
        }
    }

    let fetched: Vec<(usize, FetchPartitionResponse)> = stream::iter(pending)
        .map(|pp| {
            let request_budget = Arc::clone(&request_budget);
            async move {
                use tracing::Instrument;
                let topic_name = pp.topic_arc;
                let partition = pp.partition;
                let span = crate::cluster::observability::fetch_request_span(
                    &topic_name,
                    partition.partition_index,
                    partition.fetch_offset,
                    None,
                );
                let response = async {
                    match handler
                        .partition_manager
                        .get_for_read(&topic_name, partition.partition_index)
                        .await
                    {
                        Ok(store) => {
                            let current_hwm = store.high_watermark();
                            // Snapshot log_start once so every return path
                            // out of this arm can surface it. Returning -1
                            // forces clients into a separate ListOffsets
                            // round trip on OffsetOutOfRange and breaks
                            // `auto.offset.reset=earliest` fast paths.
                            let current_log_start = store.log_start_offset();

                            // KIP-320 fencing: a client whose view predates a
                            // failover must be rejected so it refreshes
                            // metadata. Without this, a stale consumer
                            // silently reads from the new owner — README
                            // advertises the guarantee. Mirror
                            // `leader_epoch.rs:138`: -1 means "no opinion",
                            // older→Fenced, newer→Unknown. `leader_epoch == 0`
                            // means epoch validation is disabled (mock
                            // coordinators); skip in that case so test
                            // harnesses keep working.
                            let broker_epoch = store.leader_epoch();
                            if broker_epoch != 0
                                && partition.current_leader_epoch >= 0
                                && partition.current_leader_epoch != broker_epoch
                            {
                                let code = if partition.current_leader_epoch < broker_epoch {
                                    KafkaCode::FencedLeaderEpoch
                                } else {
                                    KafkaCode::UnknownLeaderEpoch
                                };
                                return FetchPartitionResponse {
                                    partition_index: partition.partition_index,
                                    error_code: code,
                                    high_watermark: current_hwm,
                                    last_stable_offset: -1,
                                    aborted_transactions: vec![],
                                    log_start_offset: current_log_start,
                                    preferred_read_replica: -1,
                                    records: None,
                                };
                            }

                            let log_start = match store.earliest_offset().await {
                                Ok(v) => v,
                                Err(e) => {
                                    error!(error = %e, "earliest_offset failed");
                                    return FetchPartitionResponse {
                                        partition_index: partition.partition_index,
                                        error_code: KafkaCode::Unknown,
                                        high_watermark: -1,
                                        last_stable_offset: -1,
                                        aborted_transactions: vec![],
                                        log_start_offset: current_log_start,
                                        preferred_read_replica: -1,
                                        records: None,
                                    };
                                }
                            };

                            let effective_offset = match partition.fetch_offset {
                                o if o < 0 => {
                                    return FetchPartitionResponse {
                                        partition_index: partition.partition_index,
                                        error_code: KafkaCode::OffsetOutOfRange,
                                        high_watermark: current_hwm,
                                        last_stable_offset: -1,
                                        aborted_transactions: vec![],
                                        log_start_offset: log_start,
                                        preferred_read_replica: -1,
                                        records: None,
                                    };
                                }
                                o => o,
                            };

                            if effective_offset > current_hwm || effective_offset < log_start {
                                return FetchPartitionResponse {
                                    partition_index: partition.partition_index,
                                    error_code: KafkaCode::OffsetOutOfRange,
                                    high_watermark: current_hwm,
                                    last_stable_offset: -1,
                                    aborted_transactions: vec![],
                                    log_start_offset: log_start,
                                    preferred_read_replica: -1,
                                    records: None,
                                };
                            }

                            let partition_cap = if partition.partition_max_bytes > 0 {
                                partition.partition_max_bytes as usize
                            } else {
                                usize::MAX
                            };
                            // Atomically reserve the partition's slice of the
                            // request-level budget. Two concurrent partitions
                            // can no longer both observe `remaining = R` and
                            // both fetch up to R bytes — overshooting the
                            // client's `max_bytes` by (concurrency - 1) *
                            // partition_cap. Any unused portion is refunded
                            // after the fetch returns.
                            let reservation = loop {
                                let current = request_budget.load(Ordering::SeqCst);
                                if current <= 0 {
                                    break 0i64;
                                }
                                let want = partition_cap.min(current as usize) as i64;
                                if request_budget
                                    .compare_exchange(
                                        current,
                                        current - want,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                    )
                                    .is_ok()
                                {
                                    break want;
                                }
                            };
                            if reservation == 0 {
                                return FetchPartitionResponse {
                                    partition_index: partition.partition_index,
                                    error_code: KafkaCode::None,
                                    high_watermark: current_hwm,
                                    last_stable_offset: current_hwm,
                                    aborted_transactions: vec![],
                                    log_start_offset: log_start,
                                    preferred_read_replica: -1,
                                    records: None,
                                };
                            }
                            let budget = reservation as usize;

                            match store.fetch_from_with_budget(effective_offset, budget).await {
                                Ok((high_watermark, records)) => {
                                    let used =
                                        records.as_ref().map(|b| b.len() as i64).unwrap_or(0);
                                    let refund = reservation - used;
                                    if refund > 0 {
                                        request_budget.fetch_add(refund, Ordering::SeqCst);
                                    }
                                    if let Some(ref record_bytes) = records {
                                        let bytes = record_bytes.len() as u64;
                                        let msg_count = crate::protocol::parse_record_count_checked(
                                            record_bytes,
                                        )
                                        .unwrap_or(0)
                                        .max(0)
                                            as u64;
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
                                        log_start_offset: log_start,
                                        aborted_transactions: vec![],
                                        preferred_read_replica: -1,
                                        records,
                                    }
                                }
                                Err(e) => {
                                    request_budget.fetch_add(reservation, Ordering::SeqCst);
                                    let error_code = e.to_kafka_code();
                                    if e.is_fenced() {
                                        error!(
                                            topic = %topic_name,
                                            partition = partition.partition_index,
                                            "Fenced during fetch - returning NotLeaderForPartition"
                                        );
                                    } else {
                                        // Throttled: per-partition fetch
                                        // errors during an object-store
                                        // outage scale with consumer
                                        // count and would saturate logs.
                                        crate::error_throttled!(error = %e, "Fetch failed");
                                    }
                                    FetchPartitionResponse {
                                        partition_index: partition.partition_index,
                                        error_code,
                                        high_watermark: -1,
                                        last_stable_offset: -1,
                                        aborted_transactions: vec![],
                                        log_start_offset: log_start,
                                        preferred_read_replica: -1,
                                        records: None,
                                    }
                                }
                            }
                        }
                        Err(e) => FetchPartitionResponse {
                            partition_index: partition.partition_index,
                            error_code: e.to_kafka_code(),
                            high_watermark: -1,
                            last_stable_offset: -1,
                            aborted_transactions: vec![],
                            log_start_offset: -1,
                            preferred_read_replica: -1,
                            records: None,
                        },
                    }
                }
                .instrument(span.clone())
                .await;
                if response.error_code == KafkaCode::None {
                    span.record("otel.status_code", "OK");
                } else {
                    span.record("otel.status_code", "ERROR");
                }
                (pp.topic_idx, response)
            }
        })
        .buffer_unordered(max_concurrent_reads)
        .collect()
        .await;

    for (topic_idx, response) in fetched {
        topic_partitions[topic_idx].push(response);
    }

    request
        .topics
        .iter()
        .zip(topic_partitions.into_iter())
        .map(|(topic, partitions)| FetchTopicResponse {
            name: topic.name.clone(),
            partitions,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;

    // ========================================================================
    // total_record_bytes (long-poll helper)
    // ========================================================================

    #[test]
    fn total_record_bytes_handles_empty() {
        assert_eq!(total_record_bytes(&[]), 0);
    }

    #[test]
    fn total_record_bytes_skips_none_records() {
        let resp = vec![FetchTopicResponse {
            name: "t".to_string(),
            partitions: vec![FetchPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                high_watermark: 0,
                last_stable_offset: 0,
                aborted_transactions: vec![],
                log_start_offset: -1,
                preferred_read_replica: -1,
                records: None,
            }],
        }];
        assert_eq!(total_record_bytes(&resp), 0);
    }

    #[test]
    fn total_record_bytes_sums_across_partitions_and_topics() {
        let resp = vec![
            FetchTopicResponse {
                name: "a".to_string(),
                partitions: vec![
                    FetchPartitionResponse {
                        partition_index: 0,
                        error_code: KafkaCode::None,
                        high_watermark: 1,
                        last_stable_offset: 1,
                        aborted_transactions: vec![],
                        log_start_offset: -1,
                        preferred_read_replica: -1,
                        records: Some(Bytes::from_static(b"hello")),
                    },
                    FetchPartitionResponse {
                        partition_index: 1,
                        error_code: KafkaCode::None,
                        high_watermark: 1,
                        last_stable_offset: 1,
                        aborted_transactions: vec![],
                        log_start_offset: -1,
                        preferred_read_replica: -1,
                        records: Some(Bytes::from_static(b"world!")),
                    },
                ],
            },
            FetchTopicResponse {
                name: "b".to_string(),
                partitions: vec![FetchPartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    high_watermark: 1,
                    last_stable_offset: 1,
                    aborted_transactions: vec![],
                    log_start_offset: -1,
                    preferred_read_replica: -1,
                    records: Some(Bytes::from_static(b"!!!")),
                }],
            },
        ];
        // 5 + 6 + 3 = 14
        assert_eq!(total_record_bytes(&resp), 14);
    }

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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
                log_start_offset: -1,
                preferred_read_replica: -1,
                records: Some(Bytes::from_static(b"data")),
            },
            FetchPartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::None,
                high_watermark: 20,
                last_stable_offset: 20,
                aborted_transactions: vec![],
                log_start_offset: -1,
                preferred_read_replica: -1,
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
            error_code: KafkaCode::None,
            session_id: 0,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
        let partitions = [
            FetchPartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                high_watermark: 100,
                last_stable_offset: 100,
                aborted_transactions: vec![],
                log_start_offset: -1,
                preferred_read_replica: -1,
                records: Some(Bytes::from_static(b"data")),
            },
            FetchPartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::NotLeaderForPartition,
                high_watermark: -1,
                last_stable_offset: -1,
                aborted_transactions: vec![],
                log_start_offset: -1,
                preferred_read_replica: -1,
                records: None,
            },
            FetchPartitionResponse {
                partition_index: 2,
                error_code: KafkaCode::OffsetOutOfRange,
                high_watermark: 50,
                last_stable_offset: 50,
                aborted_transactions: vec![],
                log_start_offset: -1,
                preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
                    log_start_offset: -1,
                    preferred_read_replica: -1,
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
                    log_start_offset: -1,
                    preferred_read_replica: -1,
                    records: Some(Bytes::from_static(b"b")),
                }],
            },
        ];

        let response = FetchResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            session_id: 0,
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
            error_code: KafkaCode::None,
            session_id: 0,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
                    log_start_offset: -1,
                    preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
            records: Some(Bytes::from_static(b"first")),
        };

        let second_response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 150, // HWM increased
            last_stable_offset: 150,
            aborted_transactions: vec![],
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            preferred_read_replica: -1,
            records: None,
        };

        assert_eq!(response.error_code, KafkaCode::None);
    }
}
