//! Partition domain for the Raft state machine.
//!
//! Handles topic management and partition ownership.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// State of a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: Arc<str>,
    pub partition_count: i32,
    pub created_at_ms: u64,
    pub config: HashMap<String, String>,
}

/// State of a partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub topic: Arc<str>,
    pub partition: i32,
    pub owner_broker_id: Option<i32>,
    pub leader_epoch: i32,
    pub lease_expires_at_ms: u64,
}

/// Commands for the partition domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionCommand {
    /// Create a new topic.
    CreateTopic {
        name: String,
        partitions: i32,
        config: HashMap<String, String>,
        timestamp_ms: u64,
    },

    /// Delete a topic.
    DeleteTopic { name: String },

    /// Update topic configuration.
    UpdateTopicConfig {
        name: String,
        config: HashMap<String, String>,
    },

    /// Acquire ownership of a partition.
    AcquirePartition {
        topic: String,
        partition: i32,
        broker_id: i32,
        lease_duration_ms: u64,
        timestamp_ms: u64,
    },

    /// Renew partition lease.
    RenewLease {
        topic: String,
        partition: i32,
        broker_id: i32,
        lease_duration_ms: u64,
        timestamp_ms: u64,
    },

    /// Release ownership of a partition.
    ReleasePartition {
        topic: String,
        partition: i32,
        broker_id: i32,
    },

    /// Expire leases that have passed their deadline.
    ExpireLeases { current_time_ms: u64 },
}

/// Responses from partition domain operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionResponse {
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

    /// Partition was acquired.
    PartitionAcquired {
        topic: String,
        partition: i32,
        leader_epoch: i32,
        lease_expires_at_ms: u64,
    },

    /// Partition is owned by another broker.
    PartitionOwnedByOther {
        topic: String,
        partition: i32,
        owner: i32,
    },

    /// Partition lease was renewed.
    LeaseRenewed {
        topic: String,
        partition: i32,
        lease_expires_at_ms: u64,
    },

    /// Partition is not owned by the requesting broker.
    PartitionNotOwned { topic: String, partition: i32 },

    /// Partition was released.
    PartitionReleased { topic: String, partition: i32 },

    /// Leases were expired.
    LeasesExpired { count: usize },
}

/// State for the partition domain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionDomainState {
    /// Topic metadata.
    pub topics: HashMap<Arc<str>, TopicInfo>,

    /// Partition ownership: (topic, partition) -> PartitionInfo.
    pub partitions: HashMap<(Arc<str>, i32), PartitionInfo>,
}

impl PartitionDomainState {
    /// Create a new empty partition state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a partition command and return the response.
    pub fn apply(&mut self, cmd: PartitionCommand) -> PartitionResponse {
        match cmd {
            PartitionCommand::CreateTopic {
                name,
                partitions,
                config,
                timestamp_ms,
            } => {
                let name: Arc<str> = Arc::from(name);

                if self.topics.contains_key(&name) {
                    return PartitionResponse::TopicAlreadyExists {
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

                // Initialize partition states
                for partition in 0..partitions {
                    self.partitions.insert(
                        (Arc::clone(&name), partition),
                        PartitionInfo {
                            topic: Arc::clone(&name),
                            partition,
                            owner_broker_id: None,
                            leader_epoch: 0,
                            lease_expires_at_ms: 0,
                        },
                    );
                }

                PartitionResponse::TopicCreated {
                    name: name.to_string(),
                    partitions,
                }
            }

            PartitionCommand::DeleteTopic { name } => {
                let name: Arc<str> = Arc::from(name);

                if self.topics.remove(&name).is_none() {
                    return PartitionResponse::TopicNotFound {
                        topic: name.to_string(),
                    };
                }

                // Remove all partitions for this topic
                self.partitions.retain(|(topic, _), _| topic != &name);

                PartitionResponse::TopicDeleted {
                    name: name.to_string(),
                }
            }

            PartitionCommand::UpdateTopicConfig { name, config } => {
                let name: Arc<str> = Arc::from(name);

                if let Some(topic) = self.topics.get_mut(&name) {
                    topic.config = config;
                    PartitionResponse::TopicConfigUpdated {
                        name: name.to_string(),
                    }
                } else {
                    PartitionResponse::TopicNotFound {
                        topic: name.to_string(),
                    }
                }
            }

            PartitionCommand::AcquirePartition {
                topic,
                partition,
                broker_id,
                lease_duration_ms,
                timestamp_ms,
            } => {
                let topic: Arc<str> = Arc::from(topic);
                let key = (Arc::clone(&topic), partition);

                if let Some(partition_state) = self.partitions.get_mut(&key) {
                    // Check if currently owned by another broker with active lease
                    if let Some(owner) = partition_state.owner_broker_id
                        && owner != broker_id
                        && partition_state.lease_expires_at_ms > timestamp_ms
                    {
                        return PartitionResponse::PartitionOwnedByOther {
                            topic: topic.to_string(),
                            partition,
                            owner,
                        };
                    }

                    // Acquire the partition
                    partition_state.owner_broker_id = Some(broker_id);
                    partition_state.leader_epoch += 1;
                    partition_state.lease_expires_at_ms = timestamp_ms + lease_duration_ms;

                    PartitionResponse::PartitionAcquired {
                        topic: topic.to_string(),
                        partition,
                        leader_epoch: partition_state.leader_epoch,
                        lease_expires_at_ms: partition_state.lease_expires_at_ms,
                    }
                } else {
                    // Partition doesn't exist - create it (auto-create)
                    let partition_state = PartitionInfo {
                        topic: Arc::clone(&topic),
                        partition,
                        owner_broker_id: Some(broker_id),
                        leader_epoch: 1,
                        lease_expires_at_ms: timestamp_ms + lease_duration_ms,
                    };
                    let leader_epoch = partition_state.leader_epoch;
                    let lease_expires_at_ms = partition_state.lease_expires_at_ms;
                    self.partitions.insert(key, partition_state);

                    PartitionResponse::PartitionAcquired {
                        topic: topic.to_string(),
                        partition,
                        leader_epoch,
                        lease_expires_at_ms,
                    }
                }
            }

            PartitionCommand::RenewLease {
                topic,
                partition,
                broker_id,
                lease_duration_ms,
                timestamp_ms,
            } => {
                let topic: Arc<str> = Arc::from(topic);
                let key = (Arc::clone(&topic), partition);

                if let Some(partition_state) = self.partitions.get_mut(&key) {
                    if partition_state.owner_broker_id != Some(broker_id) {
                        return PartitionResponse::PartitionNotOwned {
                            topic: topic.to_string(),
                            partition,
                        };
                    }

                    partition_state.lease_expires_at_ms = timestamp_ms + lease_duration_ms;

                    PartitionResponse::LeaseRenewed {
                        topic: topic.to_string(),
                        partition,
                        lease_expires_at_ms: partition_state.lease_expires_at_ms,
                    }
                } else {
                    PartitionResponse::PartitionNotOwned {
                        topic: topic.to_string(),
                        partition,
                    }
                }
            }

            PartitionCommand::ReleasePartition {
                topic,
                partition,
                broker_id,
            } => {
                let topic: Arc<str> = Arc::from(topic);
                let key = (Arc::clone(&topic), partition);

                if let Some(partition_state) = self.partitions.get_mut(&key)
                    && partition_state.owner_broker_id == Some(broker_id)
                {
                    partition_state.owner_broker_id = None;
                    partition_state.lease_expires_at_ms = 0;
                }

                PartitionResponse::PartitionReleased {
                    topic: topic.to_string(),
                    partition,
                }
            }

            PartitionCommand::ExpireLeases { current_time_ms } => {
                let mut count = 0;
                for partition_state in self.partitions.values_mut() {
                    if partition_state.owner_broker_id.is_some()
                        && partition_state.lease_expires_at_ms <= current_time_ms
                    {
                        partition_state.owner_broker_id = None;
                        partition_state.lease_expires_at_ms = 0;
                        count += 1;
                    }
                }
                PartitionResponse::LeasesExpired { count }
            }
        }
    }

    /// Get partition info.
    pub fn get_partition(&self, topic: &str, partition: i32) -> Option<&PartitionInfo> {
        let key = (Arc::from(topic), partition);
        self.partitions.get(&key)
    }

    /// Get partition owner.
    pub fn get_owner(&self, topic: &str, partition: i32) -> Option<i32> {
        self.get_partition(topic, partition)
            .and_then(|p| p.owner_broker_id)
    }

    /// Get all partitions owned by a broker.
    pub fn partitions_owned_by(&self, broker_id: i32) -> Vec<(Arc<str>, i32)> {
        self.partitions
            .iter()
            .filter(|(_, p)| p.owner_broker_id == Some(broker_id))
            .map(|((topic, partition), _)| (Arc::clone(topic), *partition))
            .collect()
    }

    /// Release all partitions owned by a broker.
    pub fn release_broker_partitions(&mut self, broker_id: i32) {
        for partition in self.partitions.values_mut() {
            if partition.owner_broker_id == Some(broker_id) {
                partition.owner_broker_id = None;
                partition.lease_expires_at_ms = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_topic() {
        let mut state = PartitionDomainState::new();

        let response = state.apply(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        match response {
            PartitionResponse::TopicCreated { name, partitions } => {
                assert_eq!(name, "test-topic");
                assert_eq!(partitions, 3);
            }
            _ => panic!("Expected TopicCreated"),
        }

        // Verify partitions were created
        for p in 0..3 {
            assert!(state.get_partition("test-topic", p).is_some());
        }
    }

    #[test]
    fn test_create_duplicate_topic() {
        let mut state = PartitionDomainState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 5,
            config: HashMap::new(),
            timestamp_ms: 2000,
        });

        assert!(matches!(
            response,
            PartitionResponse::TopicAlreadyExists { .. }
        ));
    }

    #[test]
    fn test_delete_topic() {
        let mut state = PartitionDomainState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::DeleteTopic {
            name: "test-topic".to_string(),
        });

        assert!(matches!(response, PartitionResponse::TopicDeleted { .. }));
        assert!(state.get_partition("test-topic", 0).is_none());
    }

    #[test]
    fn test_acquire_partition() {
        let mut state = PartitionDomainState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        match response {
            PartitionResponse::PartitionAcquired {
                leader_epoch,
                lease_expires_at_ms,
                ..
            } => {
                assert!(leader_epoch > 0);
                assert_eq!(lease_expires_at_ms, 61000);
            }
            _ => panic!("Expected PartitionAcquired"),
        }

        assert_eq!(state.get_owner("test", 0), Some(1));
    }

    #[test]
    fn test_acquire_partition_owned_by_other() {
        let mut state = PartitionDomainState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        // Try to acquire with different broker while lease is active
        let response = state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 2,
            lease_duration_ms: 60000,
            timestamp_ms: 2000, // Before lease expires
        });

        assert!(matches!(
            response,
            PartitionResponse::PartitionOwnedByOther { owner: 1, .. }
        ));
    }

    #[test]
    fn test_renew_lease() {
        let mut state = PartitionDomainState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::RenewLease {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 120000,
            timestamp_ms: 50000,
        });

        match response {
            PartitionResponse::LeaseRenewed {
                lease_expires_at_ms,
                ..
            } => {
                assert_eq!(lease_expires_at_ms, 170000);
            }
            _ => panic!("Expected LeaseRenewed"),
        }
    }

    #[test]
    fn test_expire_leases() {
        let mut state = PartitionDomainState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 2,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1000,
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 1,
            broker_id: 2,
            lease_duration_ms: 1000,
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::ExpireLeases {
            current_time_ms: 10000,
        });

        match response {
            PartitionResponse::LeasesExpired { count } => {
                assert_eq!(count, 2);
            }
            _ => panic!("Expected LeasesExpired"),
        }

        assert!(state.get_owner("test", 0).is_none());
        assert!(state.get_owner("test", 1).is_none());
    }

    #[test]
    fn test_partitions_owned_by() {
        let mut state = PartitionDomainState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 2,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        let owned = state.partitions_owned_by(1);
        assert_eq!(owned.len(), 2);
    }
}
