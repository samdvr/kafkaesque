//! DescribeConfigs (key 32) and AlterConfigs (key 33) handlers.
//!
//! These plumb the wire-level config map through to the Raft-backed topic
//! registry. Kafka admin tools (`kafka-configs.sh`, `KafkaAdminClient`,
//! `kcat -X describe-config`) reach this code path when an operator
//! flips, e.g., `cleanup.policy=compact` on an existing topic.
//!
//! Resource scope: only `Topic` (resource_type = 2) is supported. Broker
//! configs (resource_type = 4) are answered with `INVALID_REQUEST` so an
//! admin tool's error is informative rather than a silent no-op.

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
    AlterConfigsRequestData, DescribeConfigsRequestData, RESOURCE_TYPE_BROKER, RESOURCE_TYPE_TOPIC,
};
use crate::server::response::{
    AlterConfigsResponseData, AlterConfigsResult, CONFIG_SOURCE_DEFAULT_CONFIG,
    CONFIG_SOURCE_TOPIC_CONFIG, DescribeConfigsEntry, DescribeConfigsResponseData,
    DescribeConfigsResult,
};

use super::SlateDBClusterHandler;

// ============================================================================
// DescribeConfigs
// ============================================================================

pub(super) async fn handle_describe_configs(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: DescribeConfigsRequestData,
) -> DescribeConfigsResponseData {
    let mut results = Vec::with_capacity(request.resources.len());

    for resource in request.resources {
        if resource.resource_type == RESOURCE_TYPE_BROKER {
            // Broker configs aren't readable yet — we have a flat
            // ClusterConfig but no per-broker registry. Returning
            // InvalidRequest matches what real Kafka does for an
            // unimplemented resource type and lets admin tools surface
            // the gap clearly instead of silently returning [].
            results.push(DescribeConfigsResult {
                error_code: KafkaCode::InvalidRequest,
                error_message: Some("broker configs are not described".to_string()),
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
                configs: vec![],
            });
            continue;
        }

        if resource.resource_type != RESOURCE_TYPE_TOPIC {
            results.push(DescribeConfigsResult {
                error_code: KafkaCode::InvalidRequest,
                error_message: Some(format!(
                    "unsupported resource_type {}",
                    resource.resource_type
                )),
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
                configs: vec![],
            });
            continue;
        }

        if validate_topic_name(&resource.resource_name).is_err() {
            results.push(DescribeConfigsResult {
                error_code: KafkaCode::InvalidTopic,
                error_message: Some("invalid topic name".to_string()),
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
                configs: vec![],
            });
            continue;
        }

        // Real Kafka requires `DescribeConfigs` on the topic; our ACL model
        // uses `Describe` (mutate ops imply Describe via `implies()`).
        let decision = handler
            .authorizer
            .authorize(AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation: AclOperation::Describe,
                resource_type: AclResourceType::Topic,
                resource_name: &resource.resource_name,
            })
            .await;
        if decision == AuthorizeResult::Denied {
            info!(
                target: "audit",
                topic = %resource.resource_name,
                principal = %ctx.principal,
                api = "DescribeConfigs",
                operation = "Describe",
                "ACL denied: DescribeConfigs"
            );
            results.push(DescribeConfigsResult {
                error_code: KafkaCode::TopicAuthorizationFailed,
                error_message: Some("not authorized".to_string()),
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
                configs: vec![],
            });
            continue;
        }

        let raw_config = match handler
            .coordinator
            .get_topic_config(&resource.resource_name)
            .await
        {
            Ok(Some(cfg)) => cfg,
            Ok(None) => {
                results.push(DescribeConfigsResult {
                    error_code: KafkaCode::UnknownTopicOrPartition,
                    error_message: Some("unknown topic".to_string()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name,
                    configs: vec![],
                });
                continue;
            }
            Err(e) => {
                warn!(
                    topic = %resource.resource_name,
                    error = %e,
                    "DescribeConfigs: failed to read registry"
                );
                results.push(DescribeConfigsResult {
                    error_code: KafkaCode::Unknown,
                    error_message: Some(e.to_string()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name,
                    configs: vec![],
                });
                continue;
            }
        };

        let resolved = TopicCompactionConfig::resolve(&raw_config, &handler.config);
        let configs = build_describe_entries(&resolved, &raw_config, resource.configuration_keys);

        results.push(DescribeConfigsResult {
            error_code: KafkaCode::None,
            error_message: None,
            resource_type: resource.resource_type,
            resource_name: resource.resource_name,
            configs,
        });
    }

    DescribeConfigsResponseData {
        throttle_time_ms: 0,
        results,
    }
}

/// Project the resolved typed view onto the wire-level entry list, with the
/// per-key source (TOPIC vs DEFAULT) faithfully reported.
///
/// `key_filter`:
/// - `None` → return every recognized key (Kafka semantics for null filter).
/// - `Some(keys)` → return only those keys, in the order requested. Unknown
///   keys are returned with a null value and source DEFAULT_CONFIG so admin
///   tools see "key absent / using default" rather than a silent omission.
fn build_describe_entries(
    resolved: &TopicCompactionConfig,
    raw: &HashMap<String, String>,
    key_filter: Option<Vec<String>>,
) -> Vec<DescribeConfigsEntry> {
    let resolved_pairs = resolved.to_raw();
    let lookup: HashMap<&str, &str> = resolved_pairs
        .iter()
        .map(|(k, v)| (*k, v.as_str()))
        .collect();

    match key_filter {
        None => resolved_pairs
            .iter()
            .map(|(k, v)| make_entry(k, v, raw))
            .collect(),
        Some(keys) => keys
            .into_iter()
            .map(|k| match lookup.get(k.as_str()) {
                Some(v) => make_entry(&k, v, raw),
                None => DescribeConfigsEntry {
                    name: k,
                    value: None,
                    read_only: false,
                    config_source: CONFIG_SOURCE_DEFAULT_CONFIG,
                    is_sensitive: false,
                },
            })
            .collect(),
    }
}

fn make_entry(name: &str, value: &str, raw: &HashMap<String, String>) -> DescribeConfigsEntry {
    let source = if raw.contains_key(name) {
        CONFIG_SOURCE_TOPIC_CONFIG
    } else {
        CONFIG_SOURCE_DEFAULT_CONFIG
    };
    DescribeConfigsEntry {
        name: name.to_string(),
        value: Some(value.to_string()),
        read_only: false,
        config_source: source,
        is_sensitive: false,
    }
}

// ============================================================================
// AlterConfigs
// ============================================================================

pub(super) async fn handle_alter_configs(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: AlterConfigsRequestData,
) -> AlterConfigsResponseData {
    let validate_only = request.validate_only;
    let mut responses = Vec::with_capacity(request.resources.len());

    for resource in request.resources {
        if resource.resource_type != RESOURCE_TYPE_TOPIC {
            // Brokers and other resources aren't writable. Returning
            // InvalidRequest is loud — better than silently dropping the
            // alter and pretending we changed something.
            responses.push(AlterConfigsResult {
                error_code: KafkaCode::InvalidRequest,
                error_message: Some(format!(
                    "unsupported resource_type {}",
                    resource.resource_type
                )),
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
            });
            continue;
        }

        if validate_topic_name(&resource.resource_name).is_err() {
            responses.push(AlterConfigsResult {
                error_code: KafkaCode::InvalidTopic,
                error_message: Some("invalid topic name".to_string()),
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
            });
            continue;
        }

        let decision = handler
            .authorizer
            .authorize(AuthorizeRequest {
                principal: &ctx.principal,
                host: &ctx.client_host,
                operation: AclOperation::Alter,
                resource_type: AclResourceType::Topic,
                resource_name: &resource.resource_name,
            })
            .await;
        if decision == AuthorizeResult::Denied {
            info!(
                target: "audit",
                topic = %resource.resource_name,
                principal = %ctx.principal,
                api = "AlterConfigs",
                operation = "Alter",
                "ACL denied: AlterConfigs"
            );
            responses.push(AlterConfigsResult {
                error_code: KafkaCode::TopicAuthorizationFailed,
                error_message: Some("not authorized".to_string()),
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
            });
            continue;
        }

        // AlterConfigs (the v0–v1 non-incremental form) replaces the entire
        // config map. We honor that semantics: collect the entries with
        // non-null values into the new map, drop entries with null values
        // (Kafka's "delete this key" signal — though for the full-replace
        // semantics this just means "don't include it"). There is no merge
        // with the prior map; that's what IncrementalAlterConfigs (v3+) is
        // for, which we don't yet implement.
        let new_config: HashMap<String, String> = resource
            .configs
            .iter()
            .filter_map(|e| e.value.as_ref().map(|v| (e.name.clone(), v.clone())))
            .collect();

        if let Err(e) = TopicCompactionConfig::validate_raw(&new_config) {
            debug!(
                topic = %resource.resource_name,
                error = %e,
                "AlterConfigs: rejecting invalid config"
            );
            responses.push(AlterConfigsResult {
                error_code: KafkaCode::InvalidConfig,
                error_message: Some(e.to_string()),
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
            });
            continue;
        }

        if validate_only {
            // Dry-run: short-circuit before issuing the Raft write.
            responses.push(AlterConfigsResult {
                error_code: KafkaCode::None,
                error_message: None,
                resource_type: resource.resource_type,
                resource_name: resource.resource_name,
            });
            continue;
        }

        match handler
            .coordinator
            .update_topic_config(&resource.resource_name, new_config)
            .await
        {
            Ok(true) => {
                responses.push(AlterConfigsResult {
                    error_code: KafkaCode::None,
                    error_message: None,
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name,
                });
            }
            Ok(false) => {
                responses.push(AlterConfigsResult {
                    error_code: KafkaCode::UnknownTopicOrPartition,
                    error_message: Some("unknown topic".to_string()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name,
                });
            }
            Err(e) => {
                warn!(
                    topic = %resource.resource_name,
                    error = %e,
                    "AlterConfigs: registry write failed"
                );
                responses.push(AlterConfigsResult {
                    error_code: KafkaCode::Unknown,
                    error_message: Some(e.to_string()),
                    resource_type: resource.resource_type,
                    resource_name: resource.resource_name,
                });
            }
        }
    }

    AlterConfigsResponseData {
        throttle_time_ms: 0,
        responses,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::ClusterConfig;
    use crate::cluster::topic_config_view::{
        KEY_CLEANUP_POLICY, KEY_DELETE_RETENTION_MS, KEY_MAX_COMPACTION_LAG_MS,
        KEY_MIN_CLEANABLE_DIRTY_BYTES, KEY_MIN_COMPACTION_LAG_MS, KEY_RETENTION_MS, KEY_SEGMENT_MS,
    };

    fn defaults() -> ClusterConfig {
        ClusterConfig::default()
    }

    fn map_of<const N: usize>(items: [(&str, &str); N]) -> HashMap<String, String> {
        items
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn build_entries_no_filter_returns_every_recognized_key() {
        let raw = HashMap::new();
        let resolved = TopicCompactionConfig::resolve(&raw, &defaults());
        let entries = build_describe_entries(&resolved, &raw, None);
        let names: Vec<&str> = entries.iter().map(|e| e.name.as_str()).collect();
        for k in [
            KEY_CLEANUP_POLICY,
            KEY_RETENTION_MS,
            KEY_MIN_CLEANABLE_DIRTY_BYTES,
            KEY_MIN_COMPACTION_LAG_MS,
            KEY_MAX_COMPACTION_LAG_MS,
            KEY_DELETE_RETENTION_MS,
            KEY_SEGMENT_MS,
        ] {
            assert!(names.contains(&k), "missing key {}", k);
        }
        // None of these came from the topic — every entry must report
        // DEFAULT_CONFIG as the source.
        for e in &entries {
            assert_eq!(e.config_source, CONFIG_SOURCE_DEFAULT_CONFIG, "{:?}", e);
        }
    }

    #[test]
    fn build_entries_filter_preserves_order() {
        let raw = HashMap::new();
        let resolved = TopicCompactionConfig::resolve(&raw, &defaults());
        let filter = vec![KEY_RETENTION_MS.to_string(), KEY_CLEANUP_POLICY.to_string()];
        let entries = build_describe_entries(&resolved, &raw, Some(filter));
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].name, KEY_RETENTION_MS);
        assert_eq!(entries[1].name, KEY_CLEANUP_POLICY);
    }

    #[test]
    fn build_entries_reports_topic_source_for_overridden_keys() {
        let raw = map_of([("cleanup.policy", "compact")]);
        let resolved = TopicCompactionConfig::resolve(&raw, &defaults());
        let entries = build_describe_entries(&resolved, &raw, None);
        for e in &entries {
            if e.name == "cleanup.policy" {
                assert_eq!(e.config_source, CONFIG_SOURCE_TOPIC_CONFIG);
                assert_eq!(e.value.as_deref(), Some("compact"));
            } else {
                assert_eq!(e.config_source, CONFIG_SOURCE_DEFAULT_CONFIG);
            }
        }
    }

    #[test]
    fn build_entries_unknown_filter_key_returns_null_default() {
        // Asking for "compression.type" (unrecognized) returns an entry
        // with value=None so admin tools see "key requested but not
        // recognized" instead of a silent omission.
        let raw = HashMap::new();
        let resolved = TopicCompactionConfig::resolve(&raw, &defaults());
        let filter = vec!["compression.type".to_string()];
        let entries = build_describe_entries(&resolved, &raw, Some(filter));
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "compression.type");
        assert!(entries[0].value.is_none());
        assert_eq!(entries[0].config_source, CONFIG_SOURCE_DEFAULT_CONFIG);
    }
}
