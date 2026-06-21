//! OffsetForLeaderEpoch (KIP-320) handler.
//!
//! Modern consumers issue this on every rebalance to validate that they
//! haven't been truncated past their fetch offset. Our object-store data
//! plane never truncates the log past `high_watermark`, so the
//! conservative correct answer for a partition we own is
//! `(current_leader_epoch, high_watermark)` — the consumer's "is my
//! offset still valid?" check passes whenever we still own the partition.
//!
//! Per-partition error mapping:
//! - Topic / partition unknown        → `UnknownTopicOrPartition`
//! - Client's `current_leader_epoch` is older than ours → `FencedLeaderEpoch`
//! - Client's `current_leader_epoch` is newer than ours → `UnknownLeaderEpoch`
//! - Partition exists but isn't owned by us              → `NotLeaderForPartition`
//! - Authz denied                                         → `TopicAuthorizationFailed`

use tracing::{info, warn};

use crate::cluster::authorizer::{AuthorizeRequest, AuthorizeResult};
use crate::cluster::coordinator::validate_topic_name;
use crate::cluster::raft::{AclOperation, AclResourceType};
use crate::cluster::traits::PartitionCoordinator;
use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::OffsetForLeaderEpochRequestData;
use crate::server::response::{
    OffsetForLeaderEpochPartitionResponse, OffsetForLeaderEpochResponseData,
    OffsetForLeaderEpochTopicResponse,
};

use super::SlateDBClusterHandler;

pub(super) async fn handle_offset_for_leader_epoch(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: OffsetForLeaderEpochRequestData,
) -> OffsetForLeaderEpochResponseData {
    let mut topics = Vec::with_capacity(request.topics.len());

    for topic in request.topics {
        // Validate topic name once per topic — every partition under it
        // shares the same fate.
        let name_ok = validate_topic_name(&topic.name).is_ok();

        // Topic-level ACL: Describe is the operation OffsetForLeaderEpoch
        // requires per Kafka. Cached "decision" so we don't re-authorize
        // for every partition under the same topic.
        let authorized = name_ok
            && handler
                .authorizer
                .authorize(AuthorizeRequest {
                    principal: &ctx.principal,
                    host: &ctx.client_host,
                    operation: AclOperation::Describe,
                    resource_type: AclResourceType::Topic,
                    resource_name: &topic.name,
                })
                .await
                == AuthorizeResult::Allowed;

        if !authorized && name_ok {
            info!(
                target: "audit",
                topic = %topic.name,
                principal = %ctx.principal,
                api = "OffsetForLeaderEpoch",
                operation = "Describe",
                "ACL denied: OffsetForLeaderEpoch"
            );
        }

        let mut partitions = Vec::with_capacity(topic.partitions.len());
        for p in topic.partitions {
            let resp = if !name_ok {
                OffsetForLeaderEpochPartitionResponse::error(
                    p.partition_index,
                    KafkaCode::InvalidTopic,
                )
            } else if !authorized {
                OffsetForLeaderEpochPartitionResponse::error(
                    p.partition_index,
                    KafkaCode::TopicAuthorizationFailed,
                )
            } else {
                resolve_partition(handler, &topic.name, &p).await
            };
            partitions.push(resp);
        }

        topics.push(OffsetForLeaderEpochTopicResponse {
            name: topic.name,
            partitions,
        });
    }

    OffsetForLeaderEpochResponseData {
        throttle_time_ms: 0,
        topics,
    }
}

async fn resolve_partition(
    handler: &SlateDBClusterHandler,
    topic: &str,
    p: &crate::server::request::OffsetForLeaderEpochPartitionData,
) -> OffsetForLeaderEpochPartitionResponse {
    let state = match handler
        .coordinator
        .get_partition_leader_epoch(topic, p.partition_index)
        .await
    {
        Ok(Some(state)) => state,
        Ok(None) => {
            return OffsetForLeaderEpochPartitionResponse::error(
                p.partition_index,
                KafkaCode::UnknownTopicOrPartition,
            );
        }
        Err(e) => {
            warn!(
                topic = %topic,
                partition = p.partition_index,
                error = %e,
                "OffsetForLeaderEpoch: coordinator read failed"
            );
            return OffsetForLeaderEpochPartitionResponse::error(
                p.partition_index,
                KafkaCode::Unknown,
            );
        }
    };
    let (owner, broker_epoch) = state;

    // KIP-320 fencing: the client tells us the epoch it last saw for
    // this partition. -1 means "no opinion, don't fence". A mismatch
    // splits two ways: client older → FencedLeaderEpoch, client newer
    // → UnknownLeaderEpoch (broker likely lagging post-restart).
    if p.current_leader_epoch >= 0 && p.current_leader_epoch != broker_epoch {
        let code = if p.current_leader_epoch < broker_epoch {
            KafkaCode::FencedLeaderEpoch
        } else {
            KafkaCode::UnknownLeaderEpoch
        };
        return OffsetForLeaderEpochPartitionResponse::error(p.partition_index, code);
    }

    // Only the current owner can speak to log_end_offset. If we don't
    // own it, return NotLeaderForPartition so the consumer refreshes
    // metadata and retries against the right broker. We do NOT lie by
    // returning a synthesized end_offset for a partition we can't read.
    if owner != Some(handler.broker_id.value()) {
        return OffsetForLeaderEpochPartitionResponse::error(
            p.partition_index,
            KafkaCode::NotLeaderForPartition,
        );
    }

    // We own it. Read high_watermark from the local store. If the store
    // isn't open yet (first acquire racing this request), fall back to
    // 0 — the consumer's check is "is my offset <= what you have?",
    // and 0 is the honest answer when we have nothing.
    let end_offset = match handler
        .partition_manager
        .get_for_read(topic, p.partition_index)
        .await
    {
        Ok(store) => store.high_watermark(),
        Err(_) => 0,
    };

    OffsetForLeaderEpochPartitionResponse::success(p.partition_index, broker_epoch, end_offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fencing_thresholds() {
        // Sanity: FencedLeaderEpoch and UnknownLeaderEpoch are distinct
        // typed codes — the handler relies on them being different so
        // clients route to the right recovery path.
        assert_ne!(
            KafkaCode::FencedLeaderEpoch as i16,
            KafkaCode::UnknownLeaderEpoch as i16
        );
    }
}
