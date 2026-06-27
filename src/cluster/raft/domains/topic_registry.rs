//! Topic registry domain — owns topic existence, partition count, configs.
//!
//! This is the control-group half of the old `partition` domain. It records
//! the canonical topic existence record (name, partition count, replication,
//! configs) and nothing else. The per-partition lease/ownership state lives in
//! the shard domain (`partition_state`).
//!
//! # CreateTopic does not seed partitions
//!
//! Unlike the legacy single-group `PartitionDomainState::apply`, `CreateTopic`
//! here does **not** insert empty `PartitionInfo` entries. Per-partition state
//! lives in the shard groups, and the reconciler propagates `InitPartition`
//! commands into the appropriate shard once the topic exists in control.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::partition::{TopicInfo, advance_lease_clock};
use super::serde_helpers::serialize_sorted_map;

/// Commands for the topic registry domain. Lives in the control group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TopicRegistryCommand {
    /// Create a new topic.
    CreateTopic {
        name: String,
        partitions: i32,
        #[serde(serialize_with = "serialize_sorted_map")]
        config: HashMap<String, String>,
        timestamp_ms: u64,
    },

    /// Delete a topic. The shard reconciler will propagate `PurgePartition`
    /// to the partition domain in the relevant shard group.
    DeleteTopic { name: String },

    /// Update topic configuration.
    UpdateTopicConfig {
        name: String,
        #[serde(serialize_with = "serialize_sorted_map")]
        config: HashMap<String, String>,
    },

    /// Grow a topic's partition count to `new_count`.
    ///
    /// Only ever adds — `new_count` must be strictly greater than the
    /// current count. Shrinking would forfeit committed data on the
    /// retired partitions, so the state machine refuses it. The
    /// per-partition lease/state for the new partitions is created
    /// lazily by the ownership loop on the topic's shard.
    GrowPartitions { name: String, new_count: i32 },
}

/// Responses from topic registry operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TopicRegistryResponse {
    /// Topic was created.
    TopicCreated { name: String, partitions: i32 },
    /// Topic already exists.
    TopicAlreadyExists { topic: String },
    /// Topic was deleted.
    TopicDeleted { name: String },
    /// Topic was not found.
    TopicNotFound { topic: String },
    /// Topic config was updated.
    TopicConfigUpdated { name: String },
    /// Partition count was grown to the new value.
    TopicGrown {
        name: String,
        old_count: i32,
        new_count: i32,
    },
    /// `GrowPartitions` was called with `new_count <= existing`. Carries
    /// both numbers so the handler can phrase a useful error to the
    /// client without re-reading the registry.
    InvalidPartitionCount {
        topic: String,
        existing: i32,
        requested: i32,
    },
}

/// State for the topic registry domain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TopicRegistryState {
    /// Topic metadata, keyed by topic name.
    #[serde(serialize_with = "serialize_sorted_map")]
    pub topics: HashMap<Arc<str>, TopicInfo>,
}

impl TopicRegistryState {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a topic registry command and return the response.
    ///
    /// `lease_clock_ms` is the control group's replicated monotonic clock —
    /// independent of the per-shard clocks in the partition domain.
    pub fn apply(
        &mut self,
        cmd: TopicRegistryCommand,
        lease_clock_ms: &mut u64,
    ) -> TopicRegistryResponse {
        match cmd {
            TopicRegistryCommand::CreateTopic {
                name,
                partitions,
                config,
                timestamp_ms,
            } => {
                advance_lease_clock(lease_clock_ms, timestamp_ms);
                let name: Arc<str> = Arc::from(name);

                if self.topics.contains_key(&name) {
                    return TopicRegistryResponse::TopicAlreadyExists {
                        topic: name.to_string(),
                    };
                }

                self.topics.insert(
                    Arc::clone(&name),
                    TopicInfo {
                        name: Arc::clone(&name),
                        partition_count: partitions,
                        created_at_ms: timestamp_ms,
                        config,
                    },
                );

                TopicRegistryResponse::TopicCreated {
                    name: name.to_string(),
                    partitions,
                }
            }

            TopicRegistryCommand::DeleteTopic { name } => {
                let name: Arc<str> = Arc::from(name);
                if self.topics.remove(&name).is_none() {
                    return TopicRegistryResponse::TopicNotFound {
                        topic: name.to_string(),
                    };
                }
                TopicRegistryResponse::TopicDeleted {
                    name: name.to_string(),
                }
            }

            TopicRegistryCommand::UpdateTopicConfig { name, config } => {
                let name: Arc<str> = Arc::from(name);
                if let Some(topic) = self.topics.get_mut(&name) {
                    topic.config = config;
                    TopicRegistryResponse::TopicConfigUpdated {
                        name: name.to_string(),
                    }
                } else {
                    TopicRegistryResponse::TopicNotFound {
                        topic: name.to_string(),
                    }
                }
            }

            TopicRegistryCommand::GrowPartitions { name, new_count } => {
                let key: Arc<str> = Arc::from(name);
                let Some(topic) = self.topics.get_mut(&key) else {
                    return TopicRegistryResponse::TopicNotFound {
                        topic: key.to_string(),
                    };
                };
                let existing = topic.partition_count;
                if new_count <= existing {
                    // Strict inequality: same-count is also rejected so a
                    // re-applied entry from a snapshot doesn't silently
                    // succeed and confuse the client about whether the op
                    // actually grew anything.
                    return TopicRegistryResponse::InvalidPartitionCount {
                        topic: key.to_string(),
                        existing,
                        requested: new_count,
                    };
                }
                topic.partition_count = new_count;
                TopicRegistryResponse::TopicGrown {
                    name: key.to_string(),
                    old_count: existing,
                    new_count,
                }
            }
        }
    }

    pub fn get_topic(&self, name: &str) -> Option<&TopicInfo> {
        self.topics.get(&Arc::<str>::from(name))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn apply(state: &mut TopicRegistryState, cmd: TopicRegistryCommand) -> TopicRegistryResponse {
        let mut clock = 0u64;
        state.apply(cmd, &mut clock)
    }

    #[test]
    fn create_topic_records_existence_without_seeding_partitions() {
        let mut state = TopicRegistryState::new();
        let resp = apply(
            &mut state,
            TopicRegistryCommand::CreateTopic {
                name: "t".into(),
                partitions: 4,
                config: HashMap::new(),
                timestamp_ms: 1000,
            },
        );
        assert!(matches!(
            resp,
            TopicRegistryResponse::TopicCreated { ref name, partitions: 4 } if name == "t"
        ));
        let topic = state.get_topic("t").unwrap();
        assert_eq!(topic.partition_count, 4);
        assert_eq!(topic.created_at_ms, 1000);
    }

    #[test]
    fn duplicate_create_returns_already_exists() {
        let mut state = TopicRegistryState::new();
        apply(
            &mut state,
            TopicRegistryCommand::CreateTopic {
                name: "t".into(),
                partitions: 1,
                config: HashMap::new(),
                timestamp_ms: 1,
            },
        );
        let resp = apply(
            &mut state,
            TopicRegistryCommand::CreateTopic {
                name: "t".into(),
                partitions: 5,
                config: HashMap::new(),
                timestamp_ms: 2,
            },
        );
        assert!(matches!(
            resp,
            TopicRegistryResponse::TopicAlreadyExists { .. }
        ));
    }

    #[test]
    fn delete_topic_removes_entry() {
        let mut state = TopicRegistryState::new();
        apply(
            &mut state,
            TopicRegistryCommand::CreateTopic {
                name: "t".into(),
                partitions: 1,
                config: HashMap::new(),
                timestamp_ms: 1,
            },
        );
        let resp = apply(
            &mut state,
            TopicRegistryCommand::DeleteTopic { name: "t".into() },
        );
        assert!(matches!(resp, TopicRegistryResponse::TopicDeleted { .. }));
        assert!(state.get_topic("t").is_none());
    }

    #[test]
    fn delete_unknown_returns_not_found() {
        let mut state = TopicRegistryState::new();
        let resp = apply(
            &mut state,
            TopicRegistryCommand::DeleteTopic {
                name: "ghost".into(),
            },
        );
        assert!(matches!(resp, TopicRegistryResponse::TopicNotFound { .. }));
    }

    #[test]
    fn update_config_replaces_config_map() {
        let mut state = TopicRegistryState::new();
        apply(
            &mut state,
            TopicRegistryCommand::CreateTopic {
                name: "t".into(),
                partitions: 1,
                config: HashMap::new(),
                timestamp_ms: 1,
            },
        );
        let mut new_cfg = HashMap::new();
        new_cfg.insert("retention.ms".to_string(), "60000".to_string());
        let resp = apply(
            &mut state,
            TopicRegistryCommand::UpdateTopicConfig {
                name: "t".into(),
                config: new_cfg.clone(),
            },
        );
        assert!(matches!(
            resp,
            TopicRegistryResponse::TopicConfigUpdated { .. }
        ));
        assert_eq!(state.get_topic("t").unwrap().config, new_cfg);
    }

    #[test]
    fn create_topic_advances_lease_clock() {
        let mut state = TopicRegistryState::new();
        let mut clock = 100u64;
        state.apply(
            TopicRegistryCommand::CreateTopic {
                name: "t".into(),
                partitions: 1,
                config: HashMap::new(),
                timestamp_ms: 500,
            },
            &mut clock,
        );
        assert_eq!(clock, 500);
    }
}
