//! Metadata response encoding.

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

/// Metadata response data.
#[derive(Debug, Clone)]
pub struct MetadataResponseData {
    pub brokers: Vec<BrokerData>,
    pub controller_id: i32,
    pub topics: Vec<TopicMetadata>,
}

#[derive(Debug, Clone)]
pub struct BrokerData {
    pub node_id: i32,
    pub host: String,
    pub port: i32,
    pub rack: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub error_code: KafkaCode,
    pub name: String,
    pub is_internal: bool,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub error_code: KafkaCode,
    pub partition_index: i32,
    pub leader_id: i32,
    pub replica_nodes: Vec<i32>,
    pub isr_nodes: Vec<i32>,
}

impl ToByte for MetadataResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        encode_array(buffer, &self.brokers)?;
        self.controller_id.encode(buffer)?;
        encode_array(buffer, &self.topics)?;
        Ok(())
    }
}

impl ToByte for BrokerData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.node_id.encode(buffer)?;
        self.host.encode(buffer)?;
        self.port.encode(buffer)?;
        encode_nullable_string(self.rack.as_deref(), buffer)?;
        Ok(())
    }
}

impl ToByte for TopicMetadata {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        self.name.encode(buffer)?;
        (self.is_internal as i8).encode(buffer)?;
        encode_array(buffer, &self.partitions)?;
        Ok(())
    }
}

impl ToByte for PartitionMetadata {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        self.partition_index.encode(buffer)?;
        self.leader_id.encode(buffer)?;
        encode_array(buffer, &self.replica_nodes)?;
        encode_array(buffer, &self.isr_nodes)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metadata_response_encode() {
        let response = MetadataResponseData {
            brokers: vec![],
            controller_id: 0,
            topics: vec![],
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_broker_data_encode() {
        let broker = BrokerData {
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            rack: Some("rack1".to_string()),
        };

        let mut buffer = Vec::new();
        broker.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_broker_data_encode_no_rack() {
        let broker = BrokerData {
            node_id: 0,
            host: "kafka.local".to_string(),
            port: 9093,
            rack: None,
        };

        let mut buffer = Vec::new();
        broker.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_topic_metadata_encode() {
        let topic = TopicMetadata {
            error_code: KafkaCode::None,
            name: "test-topic".to_string(),
            is_internal: false,
            partitions: vec![],
        };

        let mut buffer = Vec::new();
        topic.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_topic_metadata_encode_internal() {
        let topic = TopicMetadata {
            error_code: KafkaCode::None,
            name: "__consumer_offsets".to_string(),
            is_internal: true,
            partitions: vec![],
        };

        let mut buffer = Vec::new();
        topic.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_partition_metadata_encode() {
        let partition = PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 0,
            leader_id: 1,
            replica_nodes: vec![1, 2, 3],
            isr_nodes: vec![1, 2],
        };

        let mut buffer = Vec::new();
        partition.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_full_metadata_response_encode() {
        let response = MetadataResponseData {
            brokers: vec![
                BrokerData {
                    node_id: 0,
                    host: "broker1.local".to_string(),
                    port: 9092,
                    rack: Some("us-east-1a".to_string()),
                },
                BrokerData {
                    node_id: 1,
                    host: "broker2.local".to_string(),
                    port: 9092,
                    rack: None,
                },
            ],
            controller_id: 0,
            topics: vec![TopicMetadata {
                error_code: KafkaCode::None,
                name: "topic1".to_string(),
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

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }
}
