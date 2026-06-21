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
//! - `acks=1`: Wait for the leader's write to be **durable** in object storage
//!   (SlateDB `await_durable=true`) before returning success. A crash after the
//!   ack will not lose the record.
//!
//! - `acks=-1` (or `all`): **Identical to `acks=1` in Kafkaesque.** Since
//!   Kafkaesque uses object storage (S3/GCS/Azure) rather than replica-based
//!   replication, there is no ISR (in-sync replica) concept and no second copy
//!   under broker control. `acks=all` does **not** provide any stronger
//!   guarantee than `acks=1`; both wait for exactly one durable SlateDB write.
//!
//! # Durability contract (operator-facing — read before relying on `acks=all`)
//!
//! Unlike Apache Kafka, Kafkaesque has no in-sync replicas. The *entire*
//! durability guarantee for an acknowledged write is whatever the configured
//! object store provides for the completed PUT. A client that selects
//! `acks=all` expecting multi-replica durability gets only the object store's
//! own durability SLA. **If that SLA is weaker than the operator assumes,
//! acknowledged data can still be lost in ways the `acks=all` contract implies
//! it cannot.** To make `acks=all` meaningful, the backing bucket MUST be
//! configured for the durability the workload requires, for example:
//!
//! - **Multi-AZ / multi-region durability**, not a single-AZ bucket. A
//!   single-AZ outage can otherwise lose acknowledged data.
//! - **Object versioning** enabled, so a write is not silently clobbered.
//! - Understand the **replication mode**: cross-region replication that is
//!   *asynchronous* means a region loss can drop recently acknowledged writes
//!   even though the producer received an `acks=all` success.
//!
//! There is no quorum and no pipelining: the epoch fence must precede every
//! append atomically, so per-partition durable throughput is bounded by
//! object-store PUT latency (~`1 / PUT_latency` durable appends/sec/partition).
//! Scale throughput by adding partitions/brokers. Amortizing PUT latency via
//! group-commit across `write_lock` holders is the intended future lever and
//! is tracked separately (audit P1-1); it does not weaken fencing because the
//! epoch check still gates the batch. True quorum durability would be a
//! separate architectural project, not a configuration change.

use std::sync::Arc;
use tracing::{debug, error, info, warn};

use crate::error::KafkaCode;
use crate::protocol::{CrcValidationResult, parse_producer_info, validate_batch_crc_async};
use crate::server::RequestContext;
use crate::server::request::{ApiKey, ProducePartitionData, ProduceRequestData};
use crate::server::response::{
    ProducePartitionResponse, ProduceResponseData, ProduceTopicResponse,
};

use super::SlateDBClusterHandler;
use crate::cluster::coordinator::validate_topic_name;
use crate::cluster::partition_manager::PartitionManager;
use crate::cluster::raft::RaftCoordinator;

/// True iff the batch payload carries a valid (non-default) producer-id /
/// epoch combination, i.e. the producer is using the idempotent path.
/// Kafka requires `IdempotentWrite` on the cluster resource for these
/// batches even when the principal already has `Write` on the topic; without
/// the check, a denied principal could still send batches under a stolen
/// producer-id and fence the legitimate producer.
fn batch_is_idempotent(records: &bytes::Bytes) -> bool {
    parse_producer_info(records).is_some_and(|info| info.is_idempotent())
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

    // Transactions are not implemented: there is no transaction coordinator,
    // no commit/abort markers, and fetch has no aborted-transaction
    // filtering. Accepting a transactional produce would silently downgrade
    // an EOS producer's guarantees, so reject it explicitly instead.
    if let Some(ref txn_id) = request.transactional_id {
        warn!(
            client = %ctx.client_addr,
            transactional_id = %txn_id,
            "Rejecting transactional produce: transactions are not supported"
        );
        let responses = request
            .topics
            .into_iter()
            .map(|topic| ProduceTopicResponse {
                partitions: topic
                    .partitions
                    .iter()
                    .map(|p| ProducePartitionResponse {
                        partition_index: p.partition_index,
                        error_code: KafkaCode::InvalidRequest,
                        base_offset: -1,
                        log_append_time: -1,
                        log_start_offset: -1,
                        record_errors: vec![],
                        error_message: None,
                    })
                    .collect(),
                name: topic.name,
            })
            .collect();
        return ProduceResponseData {
            responses,
            throttle_time_ms: 0,
        };
    }

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
                crate::cluster::metrics::record_produce_dropped(
                    "invalid_topic",
                    topic.partitions.len() as u64,
                );
                continue; // Skip invalid topics silently for acks=0
            }

            // Produce requires Write on the topic. acks=0 returns
            // no response, so a Denied check just drops silently — same as
            // an invalid topic on this path.
            if handler
                .authorizer
                .authorize(crate::cluster::authorizer::AuthorizeRequest {
                    principal: &ctx.principal,
                    host: &ctx.client_host,
                    operation: crate::cluster::raft::AclOperation::Write,
                    resource_type: crate::cluster::raft::AclResourceType::Topic,
                    resource_name: &topic.name,
                })
                .await
                == crate::cluster::authorizer::AuthorizeResult::Denied
            {
                // Emit a per-principal audit at warn level — operators rely on
                // this to detect mis-configured producers and credential
                // theft. The drop counter is bumped twice with different
                // resolutions: the existing `produce_dropped_total{reason}`
                // for top-line dashboards, and a per-principal counter for
                // attribution.
                tracing::warn!(
                    target: "audit",
                    topic = %topic.name,
                    principal = %ctx.principal,
                    client = %ctx.client_addr,
                    api = "Produce",
                    acks = 0,
                    "ACL denied: dropped acks=0 produce"
                );
                crate::cluster::metrics::record_acl_denial(&ctx.principal, "Produce", "Topic");
                crate::cluster::metrics::record_produce_dropped(
                    "acl_denied",
                    topic.partitions.len() as u64,
                );
                continue;
            }

            let topic_name: Arc<str> = handler.cached_topic_name(&topic.name);
            let partition_manager = handler.partition_manager.clone();
            let validate_crc = handler.validate_record_crc;

            // Resolve the cluster IdempotentWrite ACL once per topic and
            // reuse the verdict — calling `authorize_cluster_api` per
            // partition is N sequential authorizer lookups for what is a
            // single cluster-resource decision. Skip the resolve entirely
            // if no partition in this topic carries an idempotent batch.
            let has_idempotent_batch = topic
                .partitions
                .iter()
                .any(|p| batch_is_idempotent(&p.records));
            let idempotent_allowed = if has_idempotent_batch {
                handler
                    .authorize_cluster_api(ctx, ApiKey::InitProducerId)
                    .await
            } else {
                true
            };

            for partition in topic.partitions {
                if !idempotent_allowed && batch_is_idempotent(&partition.records) {
                    info!(
                        target: "audit",
                        topic = %topic_name,
                        partition = partition.partition_index,
                        principal = %ctx.principal,
                        api = "Produce",
                        operation = "IdempotentWrite",
                        acks = 0,
                        "ACL denied: acks=0 idempotent produce (cluster IdempotentWrite)"
                    );
                    crate::cluster::metrics::record_produce_dropped("idempotent_acl_denied", 1);
                    continue;
                }

                let topic_name_for_task = Arc::clone(&topic_name);
                let partition_manager_for_task = partition_manager.clone();
                let hwm_notify = handler.hwm_notifier(&topic_name, partition.partition_index);
                let shard_key = partition.partition_index;
                let admitted = handler
                    .fire_and_forget_pool
                    .try_dispatch(shard_key, async move {
                        if fire_and_forget_produce(
                            &partition_manager_for_task,
                            &topic_name_for_task,
                            partition,
                            validate_crc,
                        )
                        .await
                        .is_ok()
                        {
                            hwm_notify.notify_waiters();
                        }
                    });
                if !admitted {
                    // The shard's bounded queue is full — apply backpressure
                    // by dropping this acks=0 write (semantically OK; the
                    // contract is fire-and-forget).
                    warn!(
                        topic = %topic_name,
                        partition = shard_key,
                        "Dropping acks=0 write due to backpressure (shard queue full)"
                    );
                    crate::cluster::metrics::record_produce_dropped("backpressure", 1);
                }
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
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: None,
                })
                .collect();
            responses.push(ProduceTopicResponse {
                name: topic.name,
                partitions: partition_responses,
            });
            continue;
        }

        // Produce requires Write on the topic resource. Denied
        // requests get TopicAuthorizationFailed for every partition so the
        // client sees a clean per-partition response (matches Kafka).
        if handler
            .authorizer
            .authorize(crate::cluster::authorizer::AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation: crate::cluster::raft::AclOperation::Write,
                resource_type: crate::cluster::raft::AclResourceType::Topic,
                resource_name: &topic.name,
            })
            .await
            == crate::cluster::authorizer::AuthorizeResult::Denied
        {
            info!(
                target: "audit",
                topic = %topic.name,
                principal = %ctx.principal,
                api = "Produce",
                operation = "Write",
                "ACL denied: Produce"
            );
            let partition_responses: Vec<_> = topic
                .partitions
                .iter()
                .map(|p| ProducePartitionResponse {
                    partition_index: p.partition_index,
                    error_code: KafkaCode::TopicAuthorizationFailed,
                    base_offset: -1,
                    log_append_time: -1,
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: None,
                })
                .collect();
            responses.push(ProduceTopicResponse {
                name: topic.name,
                partitions: partition_responses,
            });
            continue;
        }

        let topic_name: Arc<str> = handler.cached_topic_name(&topic.name);

        // Per-partition idempotent gate: only batches that carry a valid
        // producer-id/epoch require IdempotentWrite on the cluster resource.
        // Resolve the cluster ACL once and reuse the verdict; skip the
        // resolve entirely if no partition in this topic is idempotent.
        let has_idempotent_batch = topic
            .partitions
            .iter()
            .any(|p| batch_is_idempotent(&p.records));
        let idempotent_allowed = if has_idempotent_batch {
            handler
                .authorize_cluster_api(ctx, ApiKey::InitProducerId)
                .await
        } else {
            true
        };

        // Use buffer_unordered for bounded concurrency instead of join_all
        let partition_responses: Vec<_> = stream::iter(topic.partitions)
            .map(|partition| {
                let topic_name = Arc::clone(&topic_name);
                let principal = ctx.principal.clone();
                async move {
                    if !idempotent_allowed && batch_is_idempotent(&partition.records) {
                        info!(
                            target: "audit",
                            topic = %topic_name,
                            partition = partition.partition_index,
                            principal = %principal,
                            api = "Produce",
                            operation = "IdempotentWrite",
                            "ACL denied: idempotent produce (cluster IdempotentWrite)"
                        );
                        return ProducePartitionResponse {
                            partition_index: partition.partition_index,
                            error_code: KafkaCode::ClusterAuthorizationFailed,
                            base_offset: -1,
                            log_append_time: -1,
                            log_start_offset: -1,
                            record_errors: vec![],
                            error_message: None,
                        };
                    }
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
    use crate::protocol::{CrcValidationResult, validate_batch_crc_async};

    // Skip zombie mode check for fire-and-forget (best effort)
    if partition_manager.is_zombie() {
        crate::cluster::metrics::record_produce_dropped("zombie", 1);
        return Err(());
    }

    // CRC-validate at ingress, before any partition lookup or lease check.
    // A flood of corrupt batches would otherwise force a per-partition
    // lookup, lease verification, and ownership check on every record before
    // rejection — multiplying the attacker's CPU draw on the broker by an
    // order of magnitude.
    if validate_crc {
        match validate_batch_crc_async(&partition.records).await {
            CrcValidationResult::Valid => {}
            _ => {
                crate::cluster::metrics::record_produce_dropped("crc_invalid", 1);
                return Err(());
            }
        }
    }

    // Get partition store - for acks=0, don't try to acquire if we don't own
    // IMPORTANT: We do NOT try to acquire partitions here. Partition acquisition
    // happens only in the background ownership loop.
    let store = match partition_manager
        .get_for_write(topic, partition.partition_index)
        .await
    {
        Ok(s) => s,
        Err(_) => {
            crate::cluster::metrics::record_produce_dropped("not_leader", 1);
            return Err(());
        } // We don't own this partition
    };

    // Append batch (ignore result for fire-and-forget)
    let _ = store.append_batch(&partition.records).await;
    Ok(())
}

impl SlateDBClusterHandler {
    /// Produce records to a single partition.
    pub(crate) async fn produce_to_partition(
        &self,
        topic: &str,
        partition: ProducePartitionData,
        acks: i16,
    ) -> ProducePartitionResponse {
        use tracing::Instrument;
        let span = crate::cluster::observability::produce_request_span(
            topic,
            partition.partition_index,
            crate::protocol::parse_record_count_checked(&partition.records)
                .unwrap_or(0)
                .max(0) as usize,
            None,
        );
        let start = std::time::Instant::now();
        let response = self
            .produce_to_partition_inner(topic, partition, acks)
            .instrument(span.clone())
            .await;
        let status = if response.error_code == KafkaCode::None {
            span.record("otel.status_code", "OK");
            "success"
        } else {
            span.record("otel.status_code", "ERROR");
            "error"
        };
        crate::cluster::metrics::record_produce_latency(
            topic,
            status,
            start.elapsed().as_secs_f64(),
        );
        response
    }

    async fn produce_to_partition_inner(
        &self,
        topic: &str,
        partition: ProducePartitionData,
        acks: i16,
    ) -> ProducePartitionResponse {
        if self.partition_manager.is_zombie() {
            return ProducePartitionResponse {
                partition_index: partition.partition_index,
                error_code: KafkaCode::NotLeaderForPartition,
                base_offset: -1,
                log_append_time: -1,
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
            };
        }

        // A NULLABLE_BYTES record_set of -1 (or an empty payload) is a legal
        // per-partition slot that carries no records. We treat it as a
        // structurally-empty produce: success with `base_offset = -1`, never
        // the partition's HWM. Returning HWM here used to leak the next
        // assignable offset, which then collided with whatever real append
        // landed next (two clients sending `produce(empty)` and `produce(rec)`
        // both saw the same `base_offset`, so a downstream consumer of those
        // ack-strings could not distinguish them). `-1` is the established
        // sentinel for "no record was written" on the produce response.
        if partition.records.is_empty() {
            return ProducePartitionResponse {
                partition_index: partition.partition_index,
                error_code: KafkaCode::None,
                base_offset: -1,
                log_append_time: -1,
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
            };
        }

        if self.validate_record_crc {
            match validate_batch_crc_async(&partition.records).await {
                CrcValidationResult::Valid => {}
                CrcValidationResult::Invalid { expected, actual } => {
                    warn!(
                        topic = %topic,
                        partition = partition.partition_index,
                        expected_crc = %format_args!("{:#x}", expected),
                        actual_crc = %format_args!("{:#x}", actual),
                        "Rejecting batch with invalid CRC"
                    );
                    crate::cluster::metrics::record_coordinator_failure("crc_validation_failed");
                    return ProducePartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: KafkaCode::CorruptMessage,
                        base_offset: -1,
                        log_append_time: -1,
                        log_start_offset: -1,
                        record_errors: vec![],
                        error_message: None,
                    };
                }
                CrcValidationResult::TooSmall => {
                    warn!(
                        topic = %topic,
                        partition = partition.partition_index,
                        batch_size = partition.records.len(),
                        "Rejecting batch too small to contain valid CRC"
                    );
                    crate::cluster::metrics::record_coordinator_failure("crc_validation_failed");
                    return ProducePartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: KafkaCode::CorruptMessage,
                        base_offset: -1,
                        log_append_time: -1,
                        log_start_offset: -1,
                        record_errors: vec![],
                        error_message: None,
                    };
                }
                CrcValidationResult::FrameMismatch {
                    claimed_size,
                    actual_size,
                } => {
                    warn!(
                        topic = %topic,
                        partition = partition.partition_index,
                        claimed_size,
                        actual_size,
                        "Rejecting batch: header batch_length disagrees with payload size"
                    );
                    crate::cluster::metrics::record_coordinator_failure("crc_validation_failed");
                    return ProducePartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: KafkaCode::CorruptMessage,
                        base_offset: -1,
                        log_append_time: -1,
                        log_start_offset: -1,
                        record_errors: vec![],
                        error_message: None,
                    };
                }
                CrcValidationResult::OffloadFailed => {
                    warn!(
                        topic = %topic,
                        partition = partition.partition_index,
                        "Rejecting batch: CRC offload failed (blocking-pool saturated or panicked)"
                    );
                    crate::cluster::metrics::record_coordinator_failure("crc_validation_failed");
                    return ProducePartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: KafkaCode::CorruptMessage,
                        base_offset: -1,
                        log_append_time: -1,
                        log_start_offset: -1,
                        record_errors: vec![],
                        error_message: None,
                    };
                }
                // The CRC enum is non_exhaustive across crate boundaries.
                // Reject conservatively if a future variant reaches us.
                _ => {
                    crate::cluster::metrics::record_coordinator_failure("crc_validation_failed");
                    return ProducePartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: KafkaCode::CorruptMessage,
                        base_offset: -1,
                        log_append_time: -1,
                        log_start_offset: -1,
                        record_errors: vec![],
                        error_message: None,
                    };
                }
            }
        }

        let store = match self
            .partition_manager
            .get_for_write(topic, partition.partition_index)
            .await
        {
            Ok(s) => s,
            Err(e) => {
                return ProducePartitionResponse {
                    partition_index: partition.partition_index,
                    error_code: e.to_kafka_code(),
                    base_offset: -1,
                    log_append_time: -1,
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: None,
                };
            }
        };

        let append_result = if acks == 0 {
            store.append_batch(&partition.records).await
        } else {
            store.append_batch_durable(&partition.records).await
        };

        match append_result {
            Ok(base_offset) => {
                let bytes = partition.records.len() as u64;
                let record_count = crate::protocol::parse_record_count_checked(&partition.records)
                    .unwrap_or(0)
                    .max(0) as u64;
                crate::cluster::metrics::record_produce(
                    topic,
                    partition.partition_index,
                    record_count.max(1),
                    bytes,
                );

                let topic_arc = self.cached_topic_name(topic);
                self.hwm_notifier(&topic_arc, partition.partition_index)
                    .notify_waiters();

                ProducePartitionResponse {
                    partition_index: partition.partition_index,
                    error_code: KafkaCode::None,
                    base_offset,
                    log_append_time: -1,
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: None,
                }
            }
            Err(e) => {
                let error_code = e.to_kafka_code();
                if e.is_not_leader() {
                    error!(
                        topic,
                        partition = partition.partition_index,
                        error = %e,
                        "Fenced during produce - returning NotLeaderForPartition"
                    );
                } else {
                    error!(error = %e, "Failed to append batch");
                }
                ProducePartitionResponse {
                    partition_index: partition.partition_index,
                    error_code,
                    base_offset: -1,
                    log_append_time: -1,
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: None,
                }
            }
        }
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
        assert_eq!(response.log_append_time, -1);
    }

    #[test]
    fn test_produce_partition_response_not_leader() {
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

    #[test]
    fn test_produce_partition_response_invalid_topic() {
        let response = ProducePartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::InvalidTopic,
            base_offset: -1,
            log_append_time: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
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
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
            },
            ProducePartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::None,
                base_offset: 10,
                log_append_time: -1,
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
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
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
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
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
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
            ProducePartitionResponse {
                partition_index: 2,
                error_code: KafkaCode::None,
                base_offset: 50,
                log_append_time: -1,
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
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
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: None,
                }],
            },
            ProduceTopicResponse {
                name: "topic-b".to_string(),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    base_offset: 200,
                    log_append_time: -1,
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: None,
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
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: None,
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
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
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
                log_start_offset: -1,
                record_errors: vec![],
                error_message: None,
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
