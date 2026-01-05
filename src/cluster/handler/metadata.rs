//! Metadata request handling.
//!
//! # Topic Auto-Creation (A-2)
//!
//! When `auto_create_topics` is enabled (default: true), requesting metadata for
//! a non-existent topic will automatically create it. This matches Kafka's default
//! behavior but can mask typos.
//!
//! **Warning**: A warning is logged when topics are auto-created to help detect
//! accidental topic creation from typos.
//!
//! To disable auto-creation, set `AUTO_CREATE_TOPICS=false` environment variable
//! or configure `auto_create_topics: false` in `ClusterConfig`.

use tracing::{debug, error, warn};

use crate::error::KafkaCode;
use crate::server::request::MetadataRequestData;
use crate::server::response::{BrokerData, MetadataResponseData, TopicMetadata};

use super::SlateDBClusterHandler;
use crate::cluster::coordinator::validate_topic_name;
use crate::cluster::traits::PartitionCoordinator;

/// Handle a metadata request.
pub(super) async fn handle_metadata(
    handler: &SlateDBClusterHandler,
    request: MetadataRequestData,
) -> MetadataResponseData {
    let topics: Vec<TopicMetadata> = match request.topics {
        Some(topic_names) => {
            let mut result = Vec::new();

            for name in topic_names {
                // Validate topic name to prevent injection attacks
                if validate_topic_name(&name).is_err() {
                    debug!(topic = %name, "Invalid topic name in metadata request");
                    result.push(TopicMetadata {
                        error_code: KafkaCode::InvalidTopic,
                        name,
                        is_internal: false,
                        partitions: vec![],
                    });
                    continue;
                }

                // Check if topic exists first
                let topic_exists = handler
                    .coordinator
                    .get_partition_count(&name)
                    .await
                    .ok()
                    .flatten()
                    .is_some();

                if topic_exists {
                    // Topic exists, return its metadata
                    result.push(handler.build_topic_metadata(&name).await);
                } else if handler.auto_create_topics {
                    // A-2: Topic doesn't exist but auto-create is enabled
                    // Log a warning to help detect typos (e.g., "orders-events" vs "order-events")
                    warn!(
                        topic = %name,
                        "Auto-creating topic on first access. \
                         If this was unintentional (typo?), set AUTO_CREATE_TOPICS=false"
                    );
                    match handler.partition_manager.ensure_partition(&name, 0).await {
                        Ok(_) => {
                            result.push(handler.build_topic_metadata(&name).await);
                        }
                        Err(e) => {
                            debug!(topic = %name, error = %e, "Could not ensure partition for metadata");
                            result.push(TopicMetadata {
                                error_code: KafkaCode::LeaderNotAvailable,
                                name,
                                is_internal: false,
                                partitions: vec![],
                            });
                        }
                    }
                } else {
                    // Topic doesn't exist and auto-create is disabled
                    debug!(topic = %name, "Topic not found and auto_create_topics=false");
                    result.push(TopicMetadata {
                        error_code: KafkaCode::UnknownTopicOrPartition,
                        name,
                        is_internal: false,
                        partitions: vec![],
                    });
                }
            }

            result
        }
        None => {
            // Return all known topics
            let topic_names = match handler.coordinator.get_topics().await {
                Ok(names) => names,
                Err(e) => {
                    error!(error = %e, "Failed to get topic list from coordinator");
                    Vec::new()
                }
            };
            let mut result = Vec::with_capacity(topic_names.len());

            for name in topic_names {
                result.push(handler.build_topic_metadata(&name).await);
            }

            result
        }
    };

    // Get all live brokers
    let brokers = match handler.coordinator.get_live_brokers().await {
        Ok(broker_list) => {
            let brokers: Vec<BrokerData> = broker_list
                .into_iter()
                .map(|b| BrokerData {
                    node_id: b.broker_id,
                    host: b.host,
                    port: b.port,
                    rack: None,
                })
                .collect();
            // Log what we're returning to help debug connection issues
            for broker in &brokers {
                debug!(
                    broker_id = broker.node_id,
                    host = %broker.host,
                    port = broker.port,
                    "Metadata response includes broker"
                );
            }
            brokers
        }
        Err(e) => {
            error!(error = %e, "Failed to get live brokers from coordinator");
            vec![BrokerData {
                node_id: handler.broker_id.value(),
                host: handler.host.clone(),
                port: handler.port,
                rack: None,
            }]
        }
    };

    MetadataResponseData {
        brokers,
        controller_id: handler.broker_id.value(),
        topics,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::server::response::PartitionMetadata;

    // ========================================================================
    // TopicMetadata Response Tests
    // ========================================================================

    #[test]
    fn test_topic_metadata_success() {
        let metadata = TopicMetadata {
            error_code: KafkaCode::None,
            name: "test-topic".to_string(),
            is_internal: false,
            partitions: vec![],
        };

        assert_eq!(metadata.error_code, KafkaCode::None);
        assert_eq!(metadata.name, "test-topic");
        assert!(!metadata.is_internal);
    }

    #[test]
    fn test_topic_metadata_invalid_topic() {
        let metadata = TopicMetadata {
            error_code: KafkaCode::InvalidTopic,
            name: "".to_string(),
            is_internal: false,
            partitions: vec![],
        };

        assert_eq!(metadata.error_code, KafkaCode::InvalidTopic);
        assert!(metadata.partitions.is_empty());
    }

    #[test]
    fn test_topic_metadata_unknown_topic() {
        let metadata = TopicMetadata {
            error_code: KafkaCode::UnknownTopicOrPartition,
            name: "non-existent".to_string(),
            is_internal: false,
            partitions: vec![],
        };

        assert_eq!(metadata.error_code, KafkaCode::UnknownTopicOrPartition);
    }

    #[test]
    fn test_topic_metadata_leader_not_available() {
        let metadata = TopicMetadata {
            error_code: KafkaCode::LeaderNotAvailable,
            name: "new-topic".to_string(),
            is_internal: false,
            partitions: vec![],
        };

        assert_eq!(metadata.error_code, KafkaCode::LeaderNotAvailable);
    }

    // ========================================================================
    // BrokerData Tests
    // ========================================================================

    #[test]
    fn test_broker_data_structure() {
        let broker = BrokerData {
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            rack: None,
        };

        assert_eq!(broker.node_id, 1);
        assert_eq!(broker.host, "localhost");
        assert_eq!(broker.port, 9092);
        assert!(broker.rack.is_none());
    }

    #[test]
    fn test_broker_data_with_rack() {
        let broker = BrokerData {
            node_id: 1,
            host: "broker1.example.com".to_string(),
            port: 9092,
            rack: Some("us-east-1a".to_string()),
        };

        assert_eq!(broker.rack, Some("us-east-1a".to_string()));
    }

    #[test]
    fn test_multiple_brokers() {
        let brokers = [
            BrokerData {
                node_id: 1,
                host: "broker1".to_string(),
                port: 9092,
                rack: None,
            },
            BrokerData {
                node_id: 2,
                host: "broker2".to_string(),
                port: 9093,
                rack: None,
            },
            BrokerData {
                node_id: 3,
                host: "broker3".to_string(),
                port: 9094,
                rack: None,
            },
        ];

        assert_eq!(brokers.len(), 3);
        assert_eq!(brokers[0].node_id, 1);
        assert_eq!(brokers[1].node_id, 2);
        assert_eq!(brokers[2].node_id, 3);
    }

    // ========================================================================
    // PartitionMetadata Tests
    // ========================================================================

    #[test]
    fn test_partition_metadata_success() {
        let partition = PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 0,
            leader_id: 1,
            replica_nodes: vec![1],
            isr_nodes: vec![1],
        };

        assert_eq!(partition.error_code, KafkaCode::None);
        assert_eq!(partition.partition_index, 0);
        assert_eq!(partition.leader_id, 1);
    }

    #[test]
    fn test_partition_metadata_no_leader() {
        let partition = PartitionMetadata {
            error_code: KafkaCode::LeaderNotAvailable,
            partition_index: 0,
            leader_id: -1,
            replica_nodes: vec![],
            isr_nodes: vec![],
        };

        assert_eq!(partition.error_code, KafkaCode::LeaderNotAvailable);
        assert_eq!(partition.leader_id, -1);
    }

    #[test]
    fn test_partition_metadata_with_replicas() {
        let partition = PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 0,
            leader_id: 1,
            replica_nodes: vec![1, 2, 3],
            isr_nodes: vec![1, 2, 3],
        };

        assert_eq!(partition.replica_nodes.len(), 3);
        assert_eq!(partition.isr_nodes.len(), 3);
    }

    // ========================================================================
    // MetadataResponseData Tests
    // ========================================================================

    #[test]
    fn test_metadata_response_structure() {
        let response = MetadataResponseData {
            brokers: vec![BrokerData {
                node_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                rack: None,
            }],
            controller_id: 1,
            topics: vec![],
        };

        assert_eq!(response.controller_id, 1);
        assert_eq!(response.brokers.len(), 1);
        assert!(response.topics.is_empty());
    }

    #[test]
    fn test_metadata_response_multiple_topics() {
        let response = MetadataResponseData {
            brokers: vec![],
            controller_id: 1,
            topics: vec![
                TopicMetadata {
                    error_code: KafkaCode::None,
                    name: "topic-a".to_string(),
                    is_internal: false,
                    partitions: vec![],
                },
                TopicMetadata {
                    error_code: KafkaCode::None,
                    name: "topic-b".to_string(),
                    is_internal: false,
                    partitions: vec![],
                },
            ],
        };

        assert_eq!(response.topics.len(), 2);
        assert_eq!(response.topics[0].name, "topic-a");
        assert_eq!(response.topics[1].name, "topic-b");
    }

    // ========================================================================
    // Topic Name Validation Tests
    // ========================================================================

    #[test]
    fn test_topic_name_validation_for_metadata() {
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
    // Internal Topic Tests
    // ========================================================================

    #[test]
    fn test_internal_topic_metadata() {
        let metadata = TopicMetadata {
            error_code: KafkaCode::None,
            name: "__consumer_offsets".to_string(),
            is_internal: true,
            partitions: vec![],
        };

        assert!(metadata.is_internal);
    }

    // ========================================================================
    // Controller ID Tests
    // ========================================================================

    #[test]
    fn test_controller_id_values() {
        // Controller ID should be a valid broker ID or -1 if none
        let valid_controller = 1i32;
        let no_controller = -1i32;

        assert!(valid_controller > 0);
        assert_eq!(no_controller, -1);
    }
}
