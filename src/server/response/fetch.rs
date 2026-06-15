//! Fetch response encoding.

use bytes::{BufMut, Bytes, BytesMut};

use crate::bytes_chain::BytesChain;
use crate::encode::{ToByte, encode_array, encode_records, encode_records_into_chain};
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

    /// Zero-copy encode into a `BytesChain`.
    ///
    /// Header fields and metadata accumulate into `header_buf`; whenever a
    /// partition carries records, the accumulated header bytes are flushed
    /// onto the chain followed by the records `Bytes` itself — a refcount
    /// bump, never a memcpy. Pour SlateDB's `Bytes` straight through to TCP
    /// via `write_kafka_frame_chain` and the records ride to the socket
    /// without a single copy on our side. The previous `encode_versioned`
    /// path `put_slice`'d every record byte into a contiguous `Vec<u8>`,
    /// adding 10 MiB of memory bandwidth + 2× peak RSS per 10 MiB fetch.
    pub fn encode_versioned_into_chain(
        &self,
        chain: &mut BytesChain,
        header_buf: &mut BytesMut,
        version: i16,
    ) -> Result<()> {
        if version >= 1 {
            self.throttle_time_ms.encode(header_buf)?;
        }
        // Top-level array length (non-flexible — Fetch v0..=v11 use plain
        // arrays; v12+ flexible arrays would call a different helper).
        crate::encode::encode_array_len_pub(header_buf, self.responses.len())?;
        for topic_resp in &self.responses {
            topic_resp.encode_into_chain(chain, header_buf)?;
        }
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

impl FetchTopicResponse {
    fn encode_into_chain(&self, chain: &mut BytesChain, header_buf: &mut BytesMut) -> Result<()> {
        self.name.encode(header_buf)?;
        crate::encode::encode_array_len_pub(header_buf, self.partitions.len())?;
        for part in &self.partitions {
            part.encode_into_chain(chain, header_buf)?;
        }
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
            Some(bytes) => encode_records(buffer, bytes)?,
            None => {
                // Encode empty byte array (length 0) instead of null (-1)
                (0i32).encode(buffer)?;
            }
        }
        Ok(())
    }
}

impl FetchPartitionResponse {
    /// Encode this partition into a chain. Header fields go into
    /// `header_buf`; on `Some(records)` we flush header_buf onto the chain
    /// and push the records `Bytes` itself (refcount bump). Subsequent
    /// fields then start a fresh accumulation in `header_buf`.
    fn encode_into_chain(&self, chain: &mut BytesChain, header_buf: &mut BytesMut) -> Result<()> {
        self.partition_index.encode(header_buf)?;
        (self.error_code as i16).encode(header_buf)?;
        self.high_watermark.encode(header_buf)?;
        self.last_stable_offset.encode(header_buf)?;
        encode_array(header_buf, &self.aborted_transactions)?;

        match self.records.clone() {
            Some(payload) => {
                // Flush accumulated header bytes, then append the records
                // as their own zero-copy chunk via `encode_records_into_chain`
                // (which pushes a tiny length-prefix BytesMut + the payload
                // Bytes itself).
                chain.push(header_buf.split().freeze());
                encode_records_into_chain(chain, payload)?;
            }
            None => {
                (0i32).encode(header_buf)?;
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
