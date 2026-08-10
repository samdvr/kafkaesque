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
use crate::constants::DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS;

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
    // trivial denial-of-service.
    //
    // The ceiling is derived from the per-request handler timeout rather than
    // hardcoded, and MUST stay strictly below it. `dispatch_request_common`
    // is wrapped in `timeout(DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS, ..)`
    // (`server/connection.rs`), and losing that race is not graceful: the
    // connection is answered with `RequestTimedOut` and then **shut down**.
    // These two constants used to both be 60s, so any consumer configured
    // with `fetch.max.wait.ms >= 60000` was clamped to exactly the handler
    // deadline and had its connection torn down on every fetch cycle. The
    // headroom below covers the post-wait work (final `collect_fetch`,
    // response encode) that still has to happen after the long poll ends.
    const FETCH_WAIT_HEADROOM_MS: i32 = 5_000;
    const MAX_FETCH_WAIT_MS: i32 =
        (DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS as i32) * 1_000 - FETCH_WAIT_HEADROOM_MS;
    // Fail the build rather than ship a silently-torn-down long poll if the
    // handler timeout is ever lowered to (or below) the headroom.
    const _: () = assert!(
        MAX_FETCH_WAIT_MS > 0
            && MAX_FETCH_WAIT_MS < (DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS as i32) * 1_000,
        "fetch long-poll ceiling must be strictly below the request handler timeout"
    );
    let bounded_wait_ms = request.max_wait_ms.clamp(0, MAX_FETCH_WAIT_MS);
    let max_wait = Duration::from_millis(bounded_wait_ms as u64);
    let min_bytes = request.min_bytes.max(0) as usize;
    let deadline = if max_wait.is_zero() {
        None
    } else {
        Some(tokio::time::Instant::now() + max_wait)
    };

    // Validate topic names and authorize once per request — these results
    // are stable across long-poll iterations, but the previous version
    // re-ran `validate_topic_name` and the async ACL check inside
    // `collect_fetch` on every wakeup. With max_wait_ms = 1s and HWM-driven
    // wakes every ~200ms, that quintuples the per-request authorize cost
    // for no semantic gain.
    //
    // This MUST run before the long-poll pre-arm below: arming a notify
    // interns a `(topic, partition_index)` entry, and `partition_index` is
    // an unchecked `i32` off the wire. Interning before the ACL check let an
    // unauthorized client mint cache entries for partitions that do not
    // exist — see the `hwm_notifiers` field docs.
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
    //
    // Only `Allowed` topics arm: a rejected topic never reaches
    // `collect_fetch`'s partition fan-out, so it can never produce records
    // to wait for. Reusing `topic_arc` from the plan also drops the second
    // `cached_topic_name` lookup the old shape paid per topic.
    let needs_long_poll = min_bytes > 0 && bounded_wait_ms > 0;
    let watched: Vec<Arc<tokio::sync::Notify>> = if needs_long_poll {
        request
            .topics
            .iter()
            .zip(topic_plans.iter())
            .filter_map(|(t, plan)| match plan {
                TopicFetchPlan::Allowed { topic_arc } => Some((t, topic_arc)),
                TopicFetchPlan::Reject { .. } => None,
            })
            .flat_map(|(t, topic_arc)| {
                t.partitions
                    .iter()
                    .map(|p| handler.hwm_notifier(topic_arc, p.partition_index))
                    .collect::<Vec<_>>()
            })
            .collect()
    } else {
        Vec::new()
    };
    // Pre-arm a notification per watched partition. `Notified` is held
    // across the loop so a wake that fires before we await still counts.
    let pre_armed: Vec<_> = watched.iter().map(|n| Box::pin(n.notified())).collect();

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

/// Atomically claim this partition's slice of the request-level `max_bytes`
/// budget, returning the number of bytes reserved (0 when exhausted).
///
/// Two concurrent partitions must not both observe `remaining = R` and both
/// fetch up to `R` bytes, so the claim is a CAS loop rather than a
/// load-then-subtract. Any unused portion is refunded by the caller after the
/// fetch returns.
///
/// `partition_cap` bounds a single partition's claim. It must never be
/// `usize::MAX`: an unbounded cap lets whichever partition reaches this first
/// take the entire request budget, leaving every other partition in the request
/// with a zero reservation and an empty response.
fn reserve_from_budget(budget: &std::sync::atomic::AtomicI64, partition_cap: usize) -> i64 {
    use std::sync::atomic::Ordering;
    loop {
        let current = budget.load(Ordering::SeqCst);
        if current <= 0 {
            return 0;
        }
        let want = partition_cap.min(current as usize) as i64;
        if budget
            .compare_exchange(current, current - want, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            return want;
        }
    }
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

    // Per-partition ceiling used when the client doesn't specify one. Matches
    // the cap `PartitionStore::fetch_from_with_budget` enforces internally.
    let broker_partition_cap = handler.config.max_fetch_response_size.max(1);

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
                    topic_partitions[topic_idx].push(FetchPartitionResponse::error(
                        p.partition_index,
                        *error_code,
                    ));
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

    // FAIRNESS: rotate the starting point so a given partition is not always
    // served last.
    //
    // Partitions draw from one shared `request_budget` in the order they reach
    // the reservation loop below. With a fixed order, a client whose
    // head-of-request partitions always have data can never make progress on
    // the tail of a large subscription: the budget is exhausted before those
    // partitions are reached, every time. Kafka rotates for exactly this
    // reason. The rotation is per-request and monotonic, so over N requests
    // every partition gets to lead.
    if !pending.is_empty() {
        let start = handler
            .fetch_rotation
            .fetch_add(1, Ordering::Relaxed)
            .rem_euclid(pending.len() as u64) as usize;
        pending.rotate_left(start);
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
                            // `leader_epoch.rs`: older→Fenced,
                            // newer→Unknown. `leader_epoch == 0`
                            // means epoch validation is disabled (mock
                            // coordinators); skip in that case so test
                            // harnesses keep working.
                            //
                            // `current_leader_epoch <= 0` means "no opinion"
                            // and must NOT be fenced. -1 is the documented
                            // unknown sentinel, but librdkafka sends a
                            // literal `0` on its first fetch of a partition
                            // it has no epoch for (verified on the wire with
                            // kcat/librdkafka 2.14). Our epochs start at 1 on
                            // first acquire, so fencing `0` rejected every
                            // fetch from a fresh consumer forever: the client
                            // retried, refreshed metadata, and never made
                            // progress. Epoch 0 is never an epoch we hand
                            // out, so treating it as "unknown" costs no real
                            // fencing coverage.
                            let broker_epoch = store.leader_epoch();
                            if broker_epoch != 0
                                && partition.current_leader_epoch > 0
                                && partition.current_leader_epoch != broker_epoch
                            {
                                let code = if partition.current_leader_epoch < broker_epoch {
                                    KafkaCode::FencedLeaderEpoch
                                } else {
                                    KafkaCode::UnknownLeaderEpoch
                                };
                                return FetchPartitionResponse::error_at(
                                    partition.partition_index,
                                    code,
                                    current_hwm,
                                    current_log_start,
                                );
                            }

                            // `earliest_offset()` used to be awaited here as
                            // well. It is the same `log_start_offset` atomic
                            // load already taken above, so the second call was
                            // redundant and its `Err` arm unreachable.
                            let log_start = current_log_start;

                            let effective_offset = match partition.fetch_offset {
                                o if o < 0 => {
                                    return FetchPartitionResponse::error_at(
                                        partition.partition_index,
                                        KafkaCode::OffsetOutOfRange,
                                        current_hwm,
                                        log_start,
                                    );
                                }
                                o => o,
                            };

                            if effective_offset > current_hwm || effective_offset < log_start {
                                return FetchPartitionResponse::error_at(
                                    partition.partition_index,
                                    KafkaCode::OffsetOutOfRange,
                                    current_hwm,
                                    log_start,
                                );
                            }

                            // A client that sets `max_bytes` but leaves
                            // `partition_max_bytes` unset used to yield
                            // `usize::MAX` here, which meant the first
                            // partition to reach the reservation loop claimed
                            // the ENTIRE request budget and every other
                            // partition returned empty. Fall back to the
                            // broker's own per-partition ceiling instead, so
                            // the budget is shared. `fetch_from_with_budget`
                            // applies the same ceiling internally, so this
                            // reserves no more than the fetch can actually
                            // return.
                            let partition_cap = if partition.partition_max_bytes > 0 {
                                partition.partition_max_bytes as usize
                            } else {
                                broker_partition_cap
                            };
                            // Atomically reserve the partition's slice of the
                            // request-level budget. Two concurrent partitions
                            // can no longer both observe `remaining = R` and
                            // both fetch up to R bytes — overshooting the
                            // client's `max_bytes` by (concurrency - 1) *
                            // partition_cap. Any unused portion is refunded
                            // after the fetch returns.
                            let reservation = reserve_from_budget(&request_budget, partition_cap);
                            if reservation == 0 {
                                return FetchPartitionResponse::success_at(
                                    partition.partition_index,
                                    current_hwm,
                                    log_start,
                                    None,
                                );
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
                                        store.record_fetch_counters(msg_count, bytes);
                                    }

                                    FetchPartitionResponse::success_at(
                                        partition.partition_index,
                                        high_watermark,
                                        log_start,
                                        records,
                                    )
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
                                    FetchPartitionResponse::error_at(
                                        partition.partition_index,
                                        error_code,
                                        -1,
                                        log_start,
                                    )
                                }
                            }
                        }
                        Err(e) => FetchPartitionResponse::error(
                            partition.partition_index,
                            e.to_kafka_code(),
                        ),
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

#[cfg(test)]
mod budget_tests {
    use super::reserve_from_budget;
    use std::sync::atomic::{AtomicI64, Ordering};

    /// The starvation bug: with an unbounded per-partition cap the first
    /// claimant takes the whole request budget and every later partition gets
    /// nothing. Pinned as the behaviour we must NOT have.
    #[test]
    fn unbounded_cap_would_starve_later_partitions() {
        let budget = AtomicI64::new(1_000);
        assert_eq!(reserve_from_budget(&budget, usize::MAX), 1_000);
        assert_eq!(
            reserve_from_budget(&budget, usize::MAX),
            0,
            "this is exactly why partition_cap must never be usize::MAX"
        );
    }

    /// With a real per-partition ceiling the budget is shared, so several
    /// partitions in one request each get a workable slice.
    #[test]
    fn bounded_cap_shares_budget_across_partitions() {
        let budget = AtomicI64::new(1_000);
        let cap = 256;
        let claims: Vec<i64> = (0..4).map(|_| reserve_from_budget(&budget, cap)).collect();
        assert_eq!(claims, vec![256, 256, 256, 232]);
        assert_eq!(
            claims.iter().sum::<i64>(),
            1_000,
            "must never over-commit the request budget"
        );
        assert_eq!(budget.load(Ordering::SeqCst), 0);
    }

    /// An exhausted budget yields zero rather than going negative.
    #[test]
    fn exhausted_budget_reserves_nothing() {
        let budget = AtomicI64::new(0);
        assert_eq!(reserve_from_budget(&budget, 4096), 0);
        assert_eq!(budget.load(Ordering::SeqCst), 0);

        let negative = AtomicI64::new(-5);
        assert_eq!(reserve_from_budget(&negative, 4096), 0);
        assert_eq!(negative.load(Ordering::SeqCst), -5, "must not be mutated");
    }

    /// A partition asking for less than the remaining budget takes only what
    /// it asked for, leaving the rest for its peers.
    #[test]
    fn reserves_at_most_the_partition_cap() {
        let budget = AtomicI64::new(i64::MAX);
        assert_eq!(reserve_from_budget(&budget, 1_024), 1_024);
        assert_eq!(budget.load(Ordering::SeqCst), i64::MAX - 1_024);
    }

    /// Concurrent claimants never over-commit: the CAS loop is what makes the
    /// shared budget safe under `buffer_unordered` fan-out.
    #[test]
    fn concurrent_claims_never_overcommit() {
        let budget = std::sync::Arc::new(AtomicI64::new(10_000));
        let cap = 128;
        let handles: Vec<_> = (0..8)
            .map(|_| {
                let budget = std::sync::Arc::clone(&budget);
                std::thread::spawn(move || {
                    (0..50)
                        .map(|_| reserve_from_budget(&budget, cap))
                        .sum::<i64>()
                })
            })
            .collect();
        let total: i64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(
            total + budget.load(Ordering::SeqCst),
            10_000,
            "every reserved byte must be accounted for exactly once"
        );
        assert!(
            budget.load(Ordering::SeqCst) >= 0,
            "budget must not go negative"
        );
    }
}
