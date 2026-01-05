//! Admin request handling (create/delete topics).

use tracing::{debug, error};

use crate::constants::DEFAULT_NUM_PARTITIONS;
use crate::error::KafkaCode;
use crate::server::request::{CreateTopicsRequestData, DeleteTopicsRequestData};
use crate::server::response::{
    CreateTopicResponseData, CreateTopicsResponseData, DeleteTopicResponseData,
    DeleteTopicsResponseData,
};

use super::SlateDBClusterHandler;
use crate::cluster::coordinator::validate_topic_name;
use crate::cluster::traits::PartitionCoordinator;

/// Handle a create topics request.
pub(super) async fn handle_create_topics(
    handler: &SlateDBClusterHandler,
    request: CreateTopicsRequestData,
) -> CreateTopicsResponseData {
    let mut topics = Vec::new();

    for topic in request.topics {
        // Validate topic name to prevent injection attacks
        if let Err(e) = validate_topic_name(&topic.name) {
            debug!(topic = %topic.name, "Invalid topic name in create_topics request");
            topics.push(CreateTopicResponseData {
                name: topic.name,
                error_code: KafkaCode::InvalidTopic,
                error_message: Some(e.to_string()),
            });
            continue;
        }

        let num_partitions = if topic.num_partitions <= 0 {
            DEFAULT_NUM_PARTITIONS
        } else {
            topic.num_partitions
        };

        // Register topic in coordinator
        if let Err(e) = handler
            .coordinator
            .register_topic(&topic.name, num_partitions)
            .await
        {
            error!(error = %e, "Failed to register topic");
            topics.push(CreateTopicResponseData {
                name: topic.name,
                error_code: KafkaCode::Unknown,
                error_message: Some(e.to_string()),
            });
            continue;
        }

        // Try to acquire partitions
        for p in 0..num_partitions {
            let _ = handler
                .partition_manager
                .acquire_partition(&topic.name, p)
                .await;
        }

        topics.push(CreateTopicResponseData {
            name: topic.name,
            error_code: KafkaCode::None,
            error_message: None,
        });
    }

    CreateTopicsResponseData {
        throttle_time_ms: 0,
        topics,
    }
}

/// Handle a delete topics request.
pub(super) async fn handle_delete_topics(
    handler: &SlateDBClusterHandler,
    request: DeleteTopicsRequestData,
) -> DeleteTopicsResponseData {
    let mut responses = Vec::new();

    for name in request.topic_names {
        // Validate topic name to prevent injection attacks
        if validate_topic_name(&name).is_err() {
            debug!(topic = %name, "Invalid topic name in delete_topics request");
            responses.push(DeleteTopicResponseData {
                name,
                error_code: KafkaCode::InvalidTopic,
            });
            continue;
        }

        // Get partition count to delete data
        let result = match handler.coordinator.get_partition_count(&name).await {
            Ok(Some(partitions)) => {
                let mut has_error = false;

                for p in 0..partitions {
                    if let Err(e) = handler
                        .partition_manager
                        .delete_partition_data(&name, p)
                        .await
                    {
                        error!(
                            topic = %name,
                            partition = p,
                            error = %e,
                            "Failed to delete partition data"
                        );
                        has_error = true;
                    }
                }

                // Delete topic metadata from coordinator
                match handler.coordinator.delete_topic(&name).await {
                    Ok(true) => {
                        if has_error {
                            Err("Partial deletion - some partition data may remain")
                        } else {
                            Ok(())
                        }
                    }
                    Ok(false) => Err("Topic not found in coordinator"),
                    Err(_) => Err("Failed to delete topic metadata"),
                }
            }
            Ok(None) => Err("Topic not found"),
            Err(e) => {
                error!(topic = %name, error = %e, "Failed to get partition count");
                Err("Failed to get topic metadata")
            }
        };

        responses.push(DeleteTopicResponseData {
            name,
            error_code: match result {
                Ok(()) => KafkaCode::None,
                Err(_) => KafkaCode::UnknownTopicOrPartition,
            },
        });
    }

    DeleteTopicsResponseData {
        throttle_time_ms: 0,
        responses,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // CreateTopics Response Tests
    // ========================================================================

    #[test]
    fn test_create_topic_response_success() {
        let response = CreateTopicResponseData {
            name: "test-topic".to_string(),
            error_code: KafkaCode::None,
            error_message: None,
        };

        assert_eq!(response.name, "test-topic");
        assert_eq!(response.error_code, KafkaCode::None);
        assert!(response.error_message.is_none());
    }

    #[test]
    fn test_create_topic_response_invalid_topic() {
        let response = CreateTopicResponseData {
            name: "".to_string(),
            error_code: KafkaCode::InvalidTopic,
            error_message: Some("Topic name cannot be empty".to_string()),
        };

        assert_eq!(response.error_code, KafkaCode::InvalidTopic);
        assert!(response.error_message.is_some());
    }

    #[test]
    fn test_create_topic_response_topic_exists() {
        let response = CreateTopicResponseData {
            name: "existing-topic".to_string(),
            error_code: KafkaCode::TopicAlreadyExists,
            error_message: Some("Topic already exists".to_string()),
        };

        assert_eq!(response.error_code, KafkaCode::TopicAlreadyExists);
    }

    #[test]
    fn test_create_topics_response_data() {
        let topics = vec![
            CreateTopicResponseData {
                name: "topic-a".to_string(),
                error_code: KafkaCode::None,
                error_message: None,
            },
            CreateTopicResponseData {
                name: "topic-b".to_string(),
                error_code: KafkaCode::None,
                error_message: None,
            },
        ];

        let response = CreateTopicsResponseData {
            throttle_time_ms: 0,
            topics,
        };

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 2);
    }

    // ========================================================================
    // DeleteTopics Response Tests
    // ========================================================================

    #[test]
    fn test_delete_topic_response_success() {
        let response = DeleteTopicResponseData {
            name: "test-topic".to_string(),
            error_code: KafkaCode::None,
        };

        assert_eq!(response.name, "test-topic");
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_delete_topic_response_not_found() {
        let response = DeleteTopicResponseData {
            name: "non-existent".to_string(),
            error_code: KafkaCode::UnknownTopicOrPartition,
        };

        assert_eq!(response.error_code, KafkaCode::UnknownTopicOrPartition);
    }

    #[test]
    fn test_delete_topic_response_invalid_topic() {
        let response = DeleteTopicResponseData {
            name: "..".to_string(),
            error_code: KafkaCode::InvalidTopic,
        };

        assert_eq!(response.error_code, KafkaCode::InvalidTopic);
    }

    #[test]
    fn test_delete_topics_response_data() {
        let responses = vec![
            DeleteTopicResponseData {
                name: "topic-a".to_string(),
                error_code: KafkaCode::None,
            },
            DeleteTopicResponseData {
                name: "topic-b".to_string(),
                error_code: KafkaCode::UnknownTopicOrPartition,
            },
        ];

        let response = DeleteTopicsResponseData {
            throttle_time_ms: 0,
            responses,
        };

        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses.len(), 2);
        assert_eq!(response.responses[0].error_code, KafkaCode::None);
        assert_eq!(
            response.responses[1].error_code,
            KafkaCode::UnknownTopicOrPartition
        );
    }

    // ========================================================================
    // Topic Name Validation Tests
    // ========================================================================

    #[test]
    fn test_topic_name_validation() {
        // Valid topic names
        assert!(validate_topic_name("test-topic").is_ok());
        assert!(validate_topic_name("my_topic").is_ok());
        assert!(validate_topic_name("topic123").is_ok());
        assert!(validate_topic_name("Topic.Name").is_ok());
        assert!(validate_topic_name("a").is_ok());

        // Invalid topic names
        assert!(validate_topic_name("").is_err());
        assert!(validate_topic_name(".").is_err());
        assert!(validate_topic_name("..").is_err());
    }

    // ========================================================================
    // Partition Count Tests
    // ========================================================================

    #[test]
    #[allow(clippy::assertions_on_constants)]
    fn test_default_partition_count() {
        // When num_partitions <= 0, DEFAULT_NUM_PARTITIONS is used
        assert!(DEFAULT_NUM_PARTITIONS > 0);
    }

    #[test]
    fn test_partition_count_values() {
        let invalid_values = [0, -1, -10];
        let valid_values = [1, 3, 10, 100];

        for val in invalid_values {
            let num_partitions = if val <= 0 {
                DEFAULT_NUM_PARTITIONS
            } else {
                val
            };
            assert_eq!(num_partitions, DEFAULT_NUM_PARTITIONS);
        }

        for val in valid_values {
            let num_partitions = if val <= 0 {
                DEFAULT_NUM_PARTITIONS
            } else {
                val
            };
            assert_eq!(num_partitions, val);
        }
    }

    // ========================================================================
    // Mixed Results Tests
    // ========================================================================

    #[test]
    fn test_create_topics_mixed_results() {
        let topics = vec![
            CreateTopicResponseData {
                name: "success-topic".to_string(),
                error_code: KafkaCode::None,
                error_message: None,
            },
            CreateTopicResponseData {
                name: "".to_string(),
                error_code: KafkaCode::InvalidTopic,
                error_message: Some("Invalid topic name".to_string()),
            },
            CreateTopicResponseData {
                name: "another-success".to_string(),
                error_code: KafkaCode::None,
                error_message: None,
            },
        ];

        let response = CreateTopicsResponseData {
            throttle_time_ms: 0,
            topics,
        };

        assert_eq!(response.topics[0].error_code, KafkaCode::None);
        assert_eq!(response.topics[1].error_code, KafkaCode::InvalidTopic);
        assert_eq!(response.topics[2].error_code, KafkaCode::None);
    }

    // ========================================================================
    // Throttle Time Tests
    // ========================================================================

    #[test]
    fn test_admin_responses_throttle_time() {
        let create_response = CreateTopicsResponseData {
            throttle_time_ms: 0,
            topics: vec![],
        };

        let delete_response = DeleteTopicsResponseData {
            throttle_time_ms: 0,
            responses: vec![],
        };

        assert_eq!(create_response.throttle_time_ms, 0);
        assert_eq!(delete_response.throttle_time_ms, 0);
    }
}
