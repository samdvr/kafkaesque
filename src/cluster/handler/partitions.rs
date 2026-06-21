//! CreatePartitions (key 37) handler.
//!
//! Grows an existing topic's partition count via the topic-registry
//! state machine. New partitions are owned by the broker the
//! consistent-hash ring designates; the ownership loop will pick them
//! up on its next pass. We also try to acquire-locally any new
//! partitions that hash to *this* broker so the topic is usable
//! without waiting a full ownership-tick after the call returns.

use futures::stream::{self, StreamExt};
use tracing::{debug, error, info, warn};

use crate::cluster::authorizer::{AuthorizeRequest, AuthorizeResult};
use crate::cluster::coordinator::validate_topic_name;
use crate::cluster::raft::{AclOperation, AclResourceType};
use crate::cluster::traits::PartitionCoordinator;
use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::CreatePartitionsRequestData;
use crate::server::response::{CreatePartitionsResponseData, CreatePartitionsTopicResult};

use super::SlateDBClusterHandler;

pub(super) async fn handle_create_partitions(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: CreatePartitionsRequestData,
) -> CreatePartitionsResponseData {
    let validate_only = request.validate_only;
    let mut results = Vec::with_capacity(request.topics.len());

    for topic in request.topics {
        let result = process_one(handler, ctx, &topic, validate_only).await;
        results.push(CreatePartitionsTopicResult {
            name: topic.name,
            error_code: result.0,
            error_message: result.1,
        });
    }

    CreatePartitionsResponseData {
        throttle_time_ms: 0,
        results,
    }
}

/// Returns `(error_code, optional_error_message)` for one topic.
async fn process_one(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    topic: &crate::server::request::CreatePartitionsTopicData,
    validate_only: bool,
) -> (KafkaCode, Option<String>) {
    if let Err(e) = validate_topic_name(&topic.name) {
        debug!(topic = %topic.name, error = %e, "CreatePartitions: invalid topic name");
        return (KafkaCode::InvalidTopic, Some(e.to_string()));
    }

    // CreatePartitions in Kafka requires `Alter` on the topic — same
    // operation as AlterConfigs. Without this gate any authenticated
    // client could grow any topic.
    if handler
        .authorizer
        .authorize(AuthorizeRequest {
            principal: &ctx.principal,
            host: &ctx.client_host,
            operation: AclOperation::Alter,
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
            api = "CreatePartitions",
            operation = "Alter",
            "ACL denied: CreatePartitions"
        );
        return (
            KafkaCode::TopicAuthorizationFailed,
            Some("Not authorized to alter this topic".to_string()),
        );
    }

    let existing = match handler.coordinator.get_partition_count(&topic.name).await {
        Ok(Some(n)) => n,
        Ok(None) => {
            return (
                KafkaCode::UnknownTopicOrPartition,
                Some("Topic does not exist".to_string()),
            );
        }
        Err(e) => {
            error!(topic = %topic.name, error = %e, "CreatePartitions: get_partition_count failed");
            return (KafkaCode::Unknown, Some(e.to_string()));
        }
    };

    if topic.count <= existing {
        // Kafka semantics: count is the new TOTAL, not a delta — and it
        // must be strictly greater. Identical-count is also rejected so
        // an idempotent re-send doesn't silently succeed.
        return (
            KafkaCode::InvalidPartitions,
            Some(format!(
                "topic '{}' already has {} partitions; requested {} (must be > existing)",
                topic.name, existing, topic.count
            )),
        );
    }

    // Cap against the per-topic ceiling — same cap CreateTopics enforces
    // so an authenticated client can't bypass DoS protection by growing
    // a small topic to millions of partitions.
    let max_partitions = handler.max_partitions_per_topic;
    if max_partitions > 0 && topic.count > max_partitions {
        warn!(
            topic = %topic.name,
            requested = topic.count,
            max_partitions,
            "CreatePartitions rejected: count exceeds limit"
        );
        return (
            KafkaCode::InvalidPartitions,
            Some(format!(
                "count {} exceeds max_partitions_per_topic {}",
                topic.count, max_partitions
            )),
        );
    }

    if validate_only {
        // Dry-run: short-circuit before issuing the Raft write.
        return (KafkaCode::None, None);
    }

    match handler
        .coordinator
        .grow_topic_partitions(&topic.name, topic.count)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            // Topic was deleted between get_partition_count and the
            // Raft write — race-y but rare and self-healing.
            return (
                KafkaCode::UnknownTopicOrPartition,
                Some("Topic disappeared during grow".to_string()),
            );
        }
        Err(e) => {
            error!(topic = %topic.name, error = %e, "CreatePartitions: grow failed");
            return (KafkaCode::Unknown, Some(e.to_string()));
        }
    }

    // Best-effort acquire of the newly added partitions. Any of them
    // hashing to a different broker will be picked up by that broker's
    // ownership loop. Acquisition failures here do NOT fail the
    // request: the registry is already updated and the loop retries.
    let topic_name = topic.name.clone();
    let max_concurrent = handler.max_concurrent_partition_writes.max(1);
    let new_range = existing..topic.count;
    let failed: Vec<(i32, String)> = stream::iter(new_range)
        .map(|p| {
            let topic_name = topic_name.clone();
            async move {
                match handler
                    .partition_manager
                    .acquire_partition(&topic_name, p)
                    .await
                {
                    Ok(_) => None,
                    Err(e) => Some((p, e.to_string())),
                }
            }
        })
        .buffer_unordered(max_concurrent)
        .filter_map(|res| async move { res })
        .collect()
        .await;

    if !failed.is_empty() {
        warn!(
            topic = %topic.name,
            failed_count = failed.len(),
            failures = ?failed,
            "Some new partitions could not be acquired locally; ownership \
             loop will retry"
        );
    }

    (KafkaCode::None, None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn invalid_partition_count_message_carries_existing_and_requested() {
        // Pin the user-visible error string: shrink/equal requests need
        // an actionable message that says what was tried vs. what
        // exists, so an operator can triage from the response without
        // re-reading the topic config.
        let result = (
            KafkaCode::InvalidPartitions,
            Some(format!(
                "topic '{}' already has {} partitions; requested {} (must be > existing)",
                "events", 12, 8
            )),
        );
        let msg = result.1.unwrap();
        assert!(msg.contains("'events'"));
        assert!(msg.contains("12"));
        assert!(msg.contains("8"));
        assert!(msg.contains("must be > existing"));
    }
}
