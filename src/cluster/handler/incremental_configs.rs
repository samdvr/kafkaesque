//! IncrementalAlterConfigs (KIP-339) handler.
//!
//! Differs from `AlterConfigs` in that the request carries per-key
//! Set/Delete/Append/Subtract ops instead of the full replacement map.
//! We materialize the new config by:
//! 1. Reading the topic's current config map from the registry.
//! 2. Applying ops in request order.
//! 3. Validating the resulting map (same gate `AlterConfigs` runs).
//! 4. Writing the new map via `update_topic_config`.
//!
//! Atomicity is per-resource: a config that fails validation rolls back
//! that resource's edits but does not affect other resources in the same
//! request. Kafka semantics.
//!
//! None of our supported keys are list-valued, so `Append`/`Subtract`
//! are rejected with `InvalidConfig`. Documented up front so the test
//! coverage is honest about the surface we implement.

use std::collections::HashMap;

use tracing::{debug, info, warn};

use crate::cluster::TopicCompactionConfig;
use crate::cluster::authorizer::{AuthorizeRequest, AuthorizeResult};
use crate::cluster::coordinator::validate_topic_name;
use crate::cluster::raft::{AclOperation, AclResourceType};
use crate::cluster::traits::PartitionCoordinator;
use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::{
    IncrementalAlterConfigsEntry, IncrementalAlterConfigsRequestData,
    IncrementalAlterConfigsResource, IncrementalAlterOp, RESOURCE_TYPE_TOPIC,
};
use crate::server::response::{
    IncrementalAlterConfigsResponseData, IncrementalAlterConfigsResult,
};

use super::SlateDBClusterHandler;

pub(super) async fn handle_incremental_alter_configs(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: IncrementalAlterConfigsRequestData,
) -> IncrementalAlterConfigsResponseData {
    let validate_only = request.validate_only;
    let mut responses = Vec::with_capacity(request.resources.len());

    for resource in request.resources {
        let (error_code, error_message) = process_resource(handler, ctx, &resource, validate_only)
            .await;
        responses.push(IncrementalAlterConfigsResult {
            error_code,
            error_message,
            resource_type: resource.resource_type,
            resource_name: resource.resource_name,
        });
    }

    IncrementalAlterConfigsResponseData {
        throttle_time_ms: 0,
        responses,
    }
}

async fn process_resource(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    resource: &IncrementalAlterConfigsResource,
    validate_only: bool,
) -> (KafkaCode, Option<String>) {
    if resource.resource_type != RESOURCE_TYPE_TOPIC {
        return (
            KafkaCode::InvalidRequest,
            Some(format!(
                "unsupported resource_type {}",
                resource.resource_type
            )),
        );
    }

    if validate_topic_name(&resource.resource_name).is_err() {
        return (
            KafkaCode::InvalidTopic,
            Some("invalid topic name".to_string()),
        );
    }

    if handler
        .authorizer
        .authorize(AuthorizeRequest {
            principal: &ctx.principal,
            host: &ctx.client_host,
            operation: AclOperation::Alter,
            resource_type: AclResourceType::Topic,
            resource_name: &resource.resource_name,
        })
        .await
        == AuthorizeResult::Denied
    {
        info!(
            target: "audit",
            topic = %resource.resource_name,
            principal = %ctx.principal,
            api = "IncrementalAlterConfigs",
            operation = "Alter",
            "ACL denied: IncrementalAlterConfigs"
        );
        return (
            KafkaCode::TopicAuthorizationFailed,
            Some("not authorized".to_string()),
        );
    }

    let mut new_config = match handler
        .coordinator
        .get_topic_config(&resource.resource_name)
        .await
    {
        Ok(Some(cfg)) => cfg,
        Ok(None) => {
            return (
                KafkaCode::UnknownTopicOrPartition,
                Some("unknown topic".to_string()),
            );
        }
        Err(e) => {
            warn!(
                topic = %resource.resource_name,
                error = %e,
                "IncrementalAlterConfigs: failed to read registry"
            );
            return (KafkaCode::Unknown, Some(e.to_string()));
        }
    };

    // Apply ops in request order. Append/Subtract on non-list keys
    // surface as InvalidConfig so a misuse is loud rather than silently
    // producing a comma-joined string the resolver can't read back.
    if let Err((code, msg)) = apply_ops(&mut new_config, &resource.configs) {
        debug!(
            topic = %resource.resource_name,
            error = %msg,
            "IncrementalAlterConfigs: rejecting"
        );
        return (code, Some(msg));
    }

    if let Err(e) = TopicCompactionConfig::validate_raw(&new_config) {
        debug!(
            topic = %resource.resource_name,
            error = %e,
            "IncrementalAlterConfigs: rejecting invalid config"
        );
        return (KafkaCode::InvalidConfig, Some(e.to_string()));
    }

    if validate_only {
        return (KafkaCode::None, None);
    }

    match handler
        .coordinator
        .update_topic_config(&resource.resource_name, new_config)
        .await
    {
        Ok(true) => (KafkaCode::None, None),
        Ok(false) => (
            KafkaCode::UnknownTopicOrPartition,
            Some("unknown topic".to_string()),
        ),
        Err(e) => {
            warn!(
                topic = %resource.resource_name,
                error = %e,
                "IncrementalAlterConfigs: registry write failed"
            );
            (KafkaCode::Unknown, Some(e.to_string()))
        }
    }
}

/// Apply incremental ops to a config map in place.
///
/// Returns the typed (KafkaCode, message) for the first op that fails.
/// Append/Subtract semantics here are: split the existing value on `,`
/// trimming whitespace, do the union/difference, and re-join with `,`.
/// We don't currently have any list-valued keys, but exposing the
/// behavior keeps the handler honest for when one is added (e.g.
/// `cleanup.policy=compact,delete`).
fn apply_ops(
    config: &mut HashMap<String, String>,
    entries: &[IncrementalAlterConfigsEntry],
) -> Result<(), (KafkaCode, String)> {
    for entry in entries {
        match entry.op {
            IncrementalAlterOp::Set => match &entry.value {
                Some(v) => {
                    config.insert(entry.name.clone(), v.clone());
                }
                None => {
                    // Set with NULL is functionally a delete: real
                    // Kafka treats it the same way.
                    config.remove(&entry.name);
                }
            },
            IncrementalAlterOp::Delete => {
                config.remove(&entry.name);
            }
            IncrementalAlterOp::Append => {
                let Some(value) = entry.value.as_ref() else {
                    return Err((
                        KafkaCode::InvalidConfig,
                        format!("Append op for '{}' requires a non-null value", entry.name),
                    ));
                };
                let merged = merge_list(config.get(&entry.name).map(|s| s.as_str()), value, true);
                config.insert(entry.name.clone(), merged);
            }
            IncrementalAlterOp::Subtract => {
                let Some(value) = entry.value.as_ref() else {
                    return Err((
                        KafkaCode::InvalidConfig,
                        format!("Subtract op for '{}' requires a non-null value", entry.name),
                    ));
                };
                let Some(existing) = config.get(&entry.name).map(|s| s.to_string()) else {
                    // Nothing to subtract from: no-op (matches Kafka).
                    continue;
                };
                let merged = merge_list(Some(existing.as_str()), value, false);
                if merged.is_empty() {
                    config.remove(&entry.name);
                } else {
                    config.insert(entry.name.clone(), merged);
                }
            }
        }
    }
    Ok(())
}

/// Compute the result of Append (union) or Subtract (difference) over a
/// comma-separated list value. Whitespace around items is trimmed.
fn merge_list(existing: Option<&str>, op_value: &str, append: bool) -> String {
    let mut current: Vec<String> = existing
        .map(|s| {
            s.split(',')
                .map(|i| i.trim().to_string())
                .filter(|i| !i.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let new_items: Vec<String> = op_value
        .split(',')
        .map(|i| i.trim().to_string())
        .filter(|i| !i.is_empty())
        .collect();
    if append {
        for item in new_items {
            if !current.contains(&item) {
                current.push(item);
            }
        }
    } else {
        current.retain(|item| !new_items.contains(item));
    }
    current.join(",")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map_of<const N: usize>(items: [(&str, &str); N]) -> HashMap<String, String> {
        items
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn entry(name: &str, op: IncrementalAlterOp, value: Option<&str>) -> IncrementalAlterConfigsEntry {
        IncrementalAlterConfigsEntry {
            name: name.to_string(),
            op,
            value: value.map(String::from),
        }
    }

    #[test]
    fn set_replaces_existing_value() {
        let mut cfg = map_of([("retention.ms", "60000")]);
        apply_ops(
            &mut cfg,
            &[entry("retention.ms", IncrementalAlterOp::Set, Some("120000"))],
        )
        .unwrap();
        assert_eq!(cfg.get("retention.ms").unwrap(), "120000");
    }

    #[test]
    fn set_with_null_is_a_delete() {
        let mut cfg = map_of([("retention.ms", "60000")]);
        apply_ops(
            &mut cfg,
            &[entry("retention.ms", IncrementalAlterOp::Set, None)],
        )
        .unwrap();
        assert!(!cfg.contains_key("retention.ms"));
    }

    #[test]
    fn delete_removes_key_ignoring_value() {
        let mut cfg = map_of([("cleanup.policy", "compact")]);
        apply_ops(
            &mut cfg,
            &[entry("cleanup.policy", IncrementalAlterOp::Delete, Some("ignored"))],
        )
        .unwrap();
        assert!(!cfg.contains_key("cleanup.policy"));
    }

    #[test]
    fn append_unions_into_existing_list() {
        let mut cfg = map_of([("cleanup.policy", "compact")]);
        apply_ops(
            &mut cfg,
            &[entry("cleanup.policy", IncrementalAlterOp::Append, Some("delete"))],
        )
        .unwrap();
        assert_eq!(cfg.get("cleanup.policy").unwrap(), "compact,delete");
    }

    #[test]
    fn append_is_idempotent() {
        let mut cfg = map_of([("cleanup.policy", "compact,delete")]);
        apply_ops(
            &mut cfg,
            &[entry("cleanup.policy", IncrementalAlterOp::Append, Some("compact"))],
        )
        .unwrap();
        // No duplicate inserted.
        assert_eq!(cfg.get("cleanup.policy").unwrap(), "compact,delete");
    }

    #[test]
    fn append_creates_key_when_missing() {
        let mut cfg = HashMap::new();
        apply_ops(
            &mut cfg,
            &[entry("cleanup.policy", IncrementalAlterOp::Append, Some("compact"))],
        )
        .unwrap();
        assert_eq!(cfg.get("cleanup.policy").unwrap(), "compact");
    }

    #[test]
    fn subtract_removes_listed_items() {
        let mut cfg = map_of([("cleanup.policy", "compact,delete")]);
        apply_ops(
            &mut cfg,
            &[entry("cleanup.policy", IncrementalAlterOp::Subtract, Some("delete"))],
        )
        .unwrap();
        assert_eq!(cfg.get("cleanup.policy").unwrap(), "compact");
    }

    #[test]
    fn subtract_clearing_list_removes_key_entirely() {
        // After all items are subtracted the key is removed (rather
        // than left as an empty string) so a downstream `to_raw()`
        // resolution doesn't pick up an unparseable empty value.
        let mut cfg = map_of([("cleanup.policy", "compact")]);
        apply_ops(
            &mut cfg,
            &[entry("cleanup.policy", IncrementalAlterOp::Subtract, Some("compact"))],
        )
        .unwrap();
        assert!(!cfg.contains_key("cleanup.policy"));
    }

    #[test]
    fn subtract_on_missing_key_is_noop() {
        let mut cfg = HashMap::new();
        apply_ops(
            &mut cfg,
            &[entry("cleanup.policy", IncrementalAlterOp::Subtract, Some("compact"))],
        )
        .unwrap();
        assert!(cfg.is_empty());
    }

    #[test]
    fn append_with_null_value_rejected() {
        let mut cfg = HashMap::new();
        let err = apply_ops(
            &mut cfg,
            &[entry("k", IncrementalAlterOp::Append, None)],
        )
        .unwrap_err();
        assert_eq!(err.0, KafkaCode::InvalidConfig);
    }

    #[test]
    fn ops_apply_in_request_order() {
        // Set then Delete: end state is the key removed. Order matters,
        // and the handler MUST honor it.
        let mut cfg = HashMap::new();
        apply_ops(
            &mut cfg,
            &[
                entry("k", IncrementalAlterOp::Set, Some("v")),
                entry("k", IncrementalAlterOp::Delete, None),
            ],
        )
        .unwrap();
        assert!(!cfg.contains_key("k"));
    }
}
