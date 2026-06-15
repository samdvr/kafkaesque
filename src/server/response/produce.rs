//! Produce response encoding.

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

/// Produce response data.
#[derive(Debug, Clone)]
pub struct ProduceResponseData {
    pub responses: Vec<ProduceTopicResponse>,
    pub throttle_time_ms: i32,
}

#[derive(Debug, Clone)]
pub struct ProduceTopicResponse {
    pub name: String,
    pub partitions: Vec<ProducePartitionResponse>,
}

#[derive(Debug, Clone, Default)]
pub struct ProducePartitionResponse {
    pub partition_index: i32,
    pub error_code: KafkaCode,
    pub base_offset: i64,
    pub log_append_time: i64,
}

impl ProducePartitionResponse {
    /// Create an error response for a partition.
    ///
    /// Sets base_offset and log_append_time to -1 (invalid).
    pub fn error(partition_index: i32, error_code: KafkaCode) -> Self {
        Self {
            partition_index,
            error_code,
            base_offset: -1,
            log_append_time: -1,
        }
    }

    /// Create a success response for a partition.
    pub fn success(partition_index: i32, base_offset: i64) -> Self {
        Self {
            partition_index,
            error_code: KafkaCode::None,
            base_offset,
            log_append_time: -1,
        }
    }
}

impl ToByte for ProduceResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        encode_array(buffer, &self.responses)?;
        self.throttle_time_ms.encode(buffer)?;
        Ok(())
    }
}

impl ToByte for ProduceTopicResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        encode_array(buffer, &self.partitions)?;
        Ok(())
    }
}

impl ToByte for ProducePartitionResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.partition_index.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        self.base_offset.encode(buffer)?;
        self.log_append_time.encode(buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_produce_response_encode() {
        let response = ProduceResponseData {
            responses: vec![],
            throttle_time_ms: 100,
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        // array length (4 bytes) + throttle_time_ms (4 bytes)
        assert_eq!(buffer.len(), 8);
    }

    #[test]
    fn test_produce_topic_response_encode() {
        let topic_response = ProduceTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![],
        };

        let mut buffer = Vec::new();
        topic_response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_produce_partition_response_encode() {
        let partition_response = ProducePartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            base_offset: 12345,
            log_append_time: -1,
        };

        let mut buffer = Vec::new();
        partition_response.encode(&mut buffer).unwrap();

        // partition_index (4) + error_code (2) + base_offset (8) + log_append_time (8)
        assert_eq!(buffer.len(), 22);
    }

    #[test]
    fn test_full_produce_response_encode() {
        let response = ProduceResponseData {
            responses: vec![ProduceTopicResponse {
                name: "topic1".to_string(),
                partitions: vec![
                    ProducePartitionResponse::success(0, 100),
                    ProducePartitionResponse::error(1, KafkaCode::NotLeaderForPartition),
                ],
            }],
            throttle_time_ms: 0,
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }
}
