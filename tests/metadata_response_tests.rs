//! Integration tests for metadata response types.

use kafkaesque::error::KafkaCode;
use kafkaesque::server::response::{
    BrokerData, MetadataResponseData, PartitionMetadata, TopicMetadata,
};

// ============================================================================
// MetadataResponseData Tests
// ============================================================================

#[test]
fn test_metadata_response_with_data() {
    let response = MetadataResponseData {
        brokers: vec![
            BrokerData {
                node_id: 0,
                host: "broker1.example.com".to_string(),
                port: 9092,
                rack: Some("rack-1".to_string()),
            },
            BrokerData {
                node_id: 1,
                host: "broker2.example.com".to_string(),
                port: 9092,
                rack: None,
            },
        ],
        controller_id: 0,
        topics: vec![TopicMetadata {
            error_code: KafkaCode::None,
            name: "test-topic".to_string(),
            is_internal: false,
            partitions: vec![PartitionMetadata {
                error_code: KafkaCode::None,
                partition_index: 0,
                leader_id: 0,
                replica_nodes: vec![0, 1],
                isr_nodes: vec![0, 1],
            }],
        }],
    };

    assert_eq!(response.brokers.len(), 2);
    assert_eq!(response.controller_id, 0);
    assert_eq!(response.topics.len(), 1);
}

#[test]
fn test_metadata_response_debug() {
    let response = MetadataResponseData {
        brokers: vec![],
        controller_id: -1,
        topics: vec![],
    };
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("MetadataResponseData"));
}

#[test]
fn test_metadata_response_clone() {
    let response = MetadataResponseData {
        brokers: vec![],
        controller_id: 0,
        topics: vec![],
    };
    let cloned = response.clone();
    assert_eq!(response.controller_id, cloned.controller_id);
}

// ============================================================================
// BrokerData Tests
// ============================================================================

#[test]
fn test_broker_data_with_rack() {
    let broker = BrokerData {
        node_id: 42,
        host: "kafka.local".to_string(),
        port: 9092,
        rack: Some("us-east-1a".to_string()),
    };

    assert_eq!(broker.node_id, 42);
    assert_eq!(broker.host, "kafka.local");
    assert_eq!(broker.port, 9092);
    assert_eq!(broker.rack, Some("us-east-1a".to_string()));
}

#[test]
fn test_broker_data_without_rack() {
    let broker = BrokerData {
        node_id: 1,
        host: "localhost".to_string(),
        port: 9092,
        rack: None,
    };

    assert_eq!(broker.node_id, 1);
    assert!(broker.rack.is_none());
}

#[test]
fn test_broker_data_debug() {
    let broker = BrokerData {
        node_id: 0,
        host: "".to_string(),
        port: 0,
        rack: None,
    };
    let debug_str = format!("{:?}", broker);
    assert!(debug_str.contains("BrokerData"));
}

#[test]
fn test_broker_data_clone() {
    let broker = BrokerData {
        node_id: 1,
        host: "host".to_string(),
        port: 9092,
        rack: Some("rack".to_string()),
    };
    let cloned = broker.clone();
    assert_eq!(broker.node_id, cloned.node_id);
    assert_eq!(broker.rack, cloned.rack);
}

// ============================================================================
// TopicMetadata Tests
// ============================================================================

#[test]
fn test_topic_metadata_with_error() {
    let topic = TopicMetadata {
        error_code: KafkaCode::UnknownTopicOrPartition,
        name: "missing-topic".to_string(),
        is_internal: false,
        partitions: vec![],
    };

    assert_eq!(topic.error_code, KafkaCode::UnknownTopicOrPartition);
    assert_eq!(topic.name, "missing-topic");
}

#[test]
fn test_topic_metadata_internal() {
    let topic = TopicMetadata {
        error_code: KafkaCode::None,
        name: "__consumer_offsets".to_string(),
        is_internal: true,
        partitions: vec![],
    };

    assert!(topic.is_internal);
    assert_eq!(topic.name, "__consumer_offsets");
}

#[test]
fn test_topic_metadata_with_partitions() {
    let topic = TopicMetadata {
        error_code: KafkaCode::None,
        name: "multi-partition".to_string(),
        is_internal: false,
        partitions: vec![
            PartitionMetadata {
                error_code: KafkaCode::None,
                partition_index: 0,
                leader_id: 0,
                replica_nodes: vec![0, 1, 2],
                isr_nodes: vec![0, 1, 2],
            },
            PartitionMetadata {
                error_code: KafkaCode::None,
                partition_index: 1,
                leader_id: 1,
                replica_nodes: vec![0, 1, 2],
                isr_nodes: vec![0, 1],
            },
            PartitionMetadata {
                error_code: KafkaCode::None,
                partition_index: 2,
                leader_id: 2,
                replica_nodes: vec![0, 1, 2],
                isr_nodes: vec![2],
            },
        ],
    };

    assert_eq!(topic.partitions.len(), 3);
    assert_eq!(topic.partitions[0].leader_id, 0);
    assert_eq!(topic.partitions[1].leader_id, 1);
    assert_eq!(topic.partitions[2].isr_nodes, vec![2]);
}

#[test]
fn test_topic_metadata_debug() {
    let topic = TopicMetadata {
        error_code: KafkaCode::None,
        name: "".to_string(),
        is_internal: false,
        partitions: vec![],
    };
    let debug_str = format!("{:?}", topic);
    assert!(debug_str.contains("TopicMetadata"));
}

#[test]
fn test_topic_metadata_clone() {
    let topic = TopicMetadata {
        error_code: KafkaCode::None,
        name: "t".to_string(),
        is_internal: false,
        partitions: vec![],
    };
    let cloned = topic.clone();
    assert_eq!(topic.name, cloned.name);
}

// ============================================================================
// PartitionMetadata Tests
// ============================================================================

#[test]
fn test_partition_metadata_with_values() {
    let partition = PartitionMetadata {
        error_code: KafkaCode::None,
        partition_index: 5,
        leader_id: 2,
        replica_nodes: vec![0, 1, 2],
        isr_nodes: vec![0, 2],
    };

    assert_eq!(partition.partition_index, 5);
    assert_eq!(partition.leader_id, 2);
    assert_eq!(partition.replica_nodes.len(), 3);
    assert_eq!(partition.isr_nodes.len(), 2);
}

#[test]
fn test_partition_metadata_with_error() {
    let partition = PartitionMetadata {
        error_code: KafkaCode::LeaderNotAvailable,
        partition_index: 0,
        leader_id: -1,
        replica_nodes: vec![0, 1, 2],
        isr_nodes: vec![],
    };

    assert_eq!(partition.error_code, KafkaCode::LeaderNotAvailable);
    assert_eq!(partition.leader_id, -1);
    assert!(partition.isr_nodes.is_empty());
}

#[test]
fn test_partition_metadata_debug() {
    let partition = PartitionMetadata {
        error_code: KafkaCode::None,
        partition_index: 0,
        leader_id: 0,
        replica_nodes: vec![],
        isr_nodes: vec![],
    };
    let debug_str = format!("{:?}", partition);
    assert!(debug_str.contains("PartitionMetadata"));
}

#[test]
fn test_partition_metadata_clone() {
    let partition = PartitionMetadata {
        error_code: KafkaCode::None,
        partition_index: 1,
        leader_id: 0,
        replica_nodes: vec![0, 1],
        isr_nodes: vec![0, 1],
    };
    let cloned = partition.clone();
    assert_eq!(partition.partition_index, cloned.partition_index);
    assert_eq!(partition.replica_nodes, cloned.replica_nodes);
}
