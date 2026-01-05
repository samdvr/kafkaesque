//! Fetch response encoding.

use bytes::{BufMut, Bytes};

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

/// Fetch response data.
#[derive(Debug, Clone)]
pub struct FetchResponseData {
    pub throttle_time_ms: i32,
    pub responses: Vec<FetchTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct FetchTopicResponse {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug, Clone, Default)]
pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: KafkaCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    pub aborted_transactions: Vec<AbortedTransaction>,
    pub records: Option<Bytes>,
}

impl FetchPartitionResponse {
    /// Create an error response for a partition.
    ///
    /// Sets watermarks to -1 and records to None.
    pub fn error(partition_index: i32, error_code: KafkaCode) -> Self {
        Self {
            partition_index,
            error_code,
            high_watermark: -1,
            last_stable_offset: -1,
            aborted_transactions: vec![],
            records: None,
        }
    }

    /// Create a success response for a partition.
    pub fn success(partition_index: i32, high_watermark: i64, records: Option<Bytes>) -> Self {
        Self {
            partition_index,
            error_code: KafkaCode::None,
            high_watermark,
            last_stable_offset: high_watermark,
            aborted_transactions: vec![],
            records,
        }
    }
}

#[derive(Debug, Clone)]
pub struct AbortedTransaction {
    pub producer_id: i64,
    pub first_offset: i64,
}

impl FetchResponseData {
    /// Encode for a specific API version.
    /// - v0: responses array only
    /// - v1+: throttle_time_ms + responses array
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 1 {
            self.throttle_time_ms.encode(buffer)?;
        }
        encode_array(buffer, &self.responses)?;
        Ok(())
    }
}

impl ToByte for FetchResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v1 format
        self.encode_versioned(buffer, 1)
    }
}

impl ToByte for FetchTopicResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        encode_array(buffer, &self.partitions)?;
        Ok(())
    }
}

impl ToByte for FetchPartitionResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.partition_index.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        self.high_watermark.encode(buffer)?;
        self.last_stable_offset.encode(buffer)?;
        encode_array(buffer, &self.aborted_transactions)?;

        match self.records.as_deref() {
            Some(bytes) => bytes.encode(buffer)?,
            None => {
                // Encode empty byte array (length 0) instead of null (-1)
                (0i32).encode(buffer)?;
            }
        }
        Ok(())
    }
}

impl ToByte for AbortedTransaction {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.producer_id.encode(buffer)?;
        self.first_offset.encode(buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fetch_response_encode() {
        let response = FetchResponseData {
            throttle_time_ms: 100,
            responses: vec![],
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        // throttle_time_ms (4 bytes) + array length (4 bytes)
        assert!(buffer.len() >= 8);
    }

    #[test]
    fn test_fetch_topic_response_encode() {
        let topic_response = FetchTopicResponse {
            name: "test-topic".to_string(),
            partitions: vec![],
        };

        let mut buffer = Vec::new();
        topic_response.encode(&mut buffer).unwrap();

        // Should have encoded topic name + array
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_fetch_partition_response_encode() {
        let partition_response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 1000,
            last_stable_offset: 1000,
            aborted_transactions: vec![],
            records: Some(Bytes::from(vec![1, 2, 3])),
        };

        let mut buffer = Vec::new();
        partition_response.encode(&mut buffer).unwrap();

        // Should encode all fields
        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_fetch_partition_response_encode_with_aborted() {
        let partition_response = FetchPartitionResponse {
            partition_index: 5,
            error_code: KafkaCode::None,
            high_watermark: 500,
            last_stable_offset: 500,
            aborted_transactions: vec![AbortedTransaction {
                producer_id: 123,
                first_offset: 100,
            }],
            records: None,
        };

        let mut buffer = Vec::new();
        partition_response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    #[test]
    fn test_aborted_transaction_encode() {
        let aborted = AbortedTransaction {
            producer_id: 12345,
            first_offset: 67890,
        };

        let mut buffer = Vec::new();
        aborted.encode(&mut buffer).unwrap();

        // producer_id (8 bytes) + first_offset (8 bytes)
        assert_eq!(buffer.len(), 16);
    }

    #[test]
    fn test_full_fetch_response_encode() {
        let response = FetchResponseData {
            throttle_time_ms: 50,
            responses: vec![FetchTopicResponse {
                name: "topic1".to_string(),
                partitions: vec![
                    FetchPartitionResponse::success(0, 100, Some(Bytes::from(vec![1, 2, 3]))),
                    FetchPartitionResponse::error(1, KafkaCode::NotLeaderForPartition),
                ],
            }],
        };

        let mut buffer = Vec::new();
        response.encode(&mut buffer).unwrap();

        assert!(!buffer.is_empty());
    }

    // ========================================================================
    // Version-specific encoding tests
    // ========================================================================

    #[test]
    fn test_fetch_response_v0_no_throttle_time() {
        let response = FetchResponseData {
            throttle_time_ms: 100, // Should be ignored in v0
            responses: vec![FetchTopicResponse {
                name: "test".to_string(),
                partitions: vec![FetchPartitionResponse::success(0, 1000, None)],
            }],
        };

        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        // v0 should be 4 bytes smaller (no throttle_time_ms)
        assert_eq!(buf_v0.len() + 4, buf_v1.len());

        // v1 should start with throttle_time_ms (100 = 0x00000064)
        assert_eq!(&buf_v1[0..4], &[0, 0, 0, 100]);
    }
}
