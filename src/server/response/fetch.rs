//! Fetch response encoding.
//!
//! Encodes Fetch v0..=v11 (classic, non-flexible). Per-version deltas:
//!
//! - v1+ adds `throttle_time_ms` at the head of the response
//! - v4+ adds `last_stable_offset` and the `aborted_transactions` array
//!   per partition (target range starts at v4 — those fields are
//!   unconditional here)
//! - v5+ adds `log_start_offset` per partition
//! - v7+ adds top-level `error_code` + `session_id` (KIP-227)
//! - v11+ adds per-partition `preferred_read_replica` (KIP-392)

use bytes::{BufMut, Bytes, BytesMut};

use crate::bytes_chain::BytesChain;
use crate::encode::{ToByte, encode_array, encode_records, encode_records_into_chain};
use crate::error::{KafkaCode, Result};

/// Fetch response data.
#[derive(Debug, Clone, Default)]
pub struct FetchResponseData {
    pub throttle_time_ms: i32,
    /// v7+ session-level error code. Defaults to `None` for older
    /// versions where the field doesn't exist on the wire.
    pub error_code: KafkaCode,
    /// v7+ incremental fetch session id echoed back to (or assigned for)
    /// the client. `0` means "no session" / full fetch.
    pub session_id: i32,
    pub responses: Vec<FetchTopicResponse>,
}

#[derive(Debug, Clone)]
pub struct FetchTopicResponse {
    pub name: String,
    pub partitions: Vec<FetchPartitionResponse>,
}

#[derive(Debug, Clone)]
pub struct FetchPartitionResponse {
    pub partition_index: i32,
    pub error_code: KafkaCode,
    pub high_watermark: i64,
    pub last_stable_offset: i64,
    /// v5+ log start offset; `-1` when unknown.
    pub log_start_offset: i64,
    pub aborted_transactions: Vec<AbortedTransaction>,
    /// v11+ preferred read replica id; `-1` means "no preference".
    pub preferred_read_replica: i32,
    pub records: Option<Bytes>,
}

impl Default for FetchPartitionResponse {
    fn default() -> Self {
        Self {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 0,
            last_stable_offset: 0,
            log_start_offset: -1,
            aborted_transactions: vec![],
            preferred_read_replica: -1,
            records: None,
        }
    }
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
            log_start_offset: -1,
            aborted_transactions: vec![],
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            aborted_transactions: vec![],
            preferred_read_replica: -1,
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
    /// - v7+: throttle_time_ms + error_code + session_id + responses array
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 1 {
            self.throttle_time_ms.encode(buffer)?;
        }
        if version >= 7 {
            (self.error_code as i16).encode(buffer)?;
            self.session_id.encode(buffer)?;
        }
        crate::encode::encode_array_len_pub(buffer, self.responses.len())?;
        for topic in &self.responses {
            topic.encode_versioned(buffer, version)?;
        }
        Ok(())
    }

    /// Zero-copy encode into a `BytesChain`.
    ///
    /// Header fields and metadata accumulate into `header_buf`; whenever a
    /// partition carries records, the accumulated header bytes are flushed
    /// onto the chain followed by the records `Bytes` itself — a refcount
    /// bump, never a memcpy. Pour SlateDB's `Bytes` straight through to TCP
    /// via `write_kafka_frame_chain` and the records ride to the socket
    /// without a single copy on our side.
    pub fn encode_versioned_into_chain(
        &self,
        chain: &mut BytesChain,
        header_buf: &mut BytesMut,
        version: i16,
    ) -> Result<()> {
        if version >= 1 {
            self.throttle_time_ms.encode(header_buf)?;
        }
        if version >= 7 {
            (self.error_code as i16).encode(header_buf)?;
            self.session_id.encode(header_buf)?;
        }
        // Top-level array length (non-flexible — Fetch v0..=v11 use plain
        // arrays; v12+ flexible arrays would call a different helper).
        crate::encode::encode_array_len_pub(header_buf, self.responses.len())?;
        for topic_resp in &self.responses {
            topic_resp.encode_into_chain(chain, header_buf, version)?;
        }
        Ok(())
    }
}

impl ToByte for FetchResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v4 framing (matches the historical `encode_versioned(_, 1)`
        // behavior for callers that never set a version explicitly, while
        // keeping aborted_transactions/last_stable_offset on the wire).
        self.encode_versioned(buffer, 4)
    }
}

impl FetchTopicResponse {
    fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        self.name.encode(buffer)?;
        crate::encode::encode_array_len_pub(buffer, self.partitions.len())?;
        for part in &self.partitions {
            part.encode_versioned(buffer, version)?;
        }
        Ok(())
    }

    fn encode_into_chain(
        &self,
        chain: &mut BytesChain,
        header_buf: &mut BytesMut,
        version: i16,
    ) -> Result<()> {
        self.name.encode(header_buf)?;
        crate::encode::encode_array_len_pub(header_buf, self.partitions.len())?;
        for part in &self.partitions {
            part.encode_into_chain(chain, header_buf, version)?;
        }
        Ok(())
    }
}

impl ToByte for FetchTopicResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.encode_versioned(buffer, 4)
    }
}

impl FetchPartitionResponse {
    fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        self.partition_index.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        self.high_watermark.encode(buffer)?;
        // Target range starts at v4, so last_stable_offset and
        // aborted_transactions are unconditional. Guard them anyway so
        // this encoder remains correct if ever called at v0..=v3.
        if version >= 4 {
            self.last_stable_offset.encode(buffer)?;
        }
        if version >= 5 {
            self.log_start_offset.encode(buffer)?;
        }
        if version >= 4 {
            encode_array(buffer, &self.aborted_transactions)?;
        }
        if version >= 11 {
            self.preferred_read_replica.encode(buffer)?;
        }

        match self.records.as_deref() {
            Some(bytes) => encode_records(buffer, bytes)?,
            None => {
                // Encode empty byte array (length 0) instead of null (-1)
                (0i32).encode(buffer)?;
            }
        }
        Ok(())
    }

    /// Encode this partition into a chain. Header fields go into
    /// `header_buf`; on `Some(records)` we flush header_buf onto the chain
    /// and push the records `Bytes` itself (refcount bump). Subsequent
    /// fields then start a fresh accumulation in `header_buf`.
    fn encode_into_chain(
        &self,
        chain: &mut BytesChain,
        header_buf: &mut BytesMut,
        version: i16,
    ) -> Result<()> {
        self.partition_index.encode(header_buf)?;
        (self.error_code as i16).encode(header_buf)?;
        self.high_watermark.encode(header_buf)?;
        if version >= 4 {
            self.last_stable_offset.encode(header_buf)?;
        }
        if version >= 5 {
            self.log_start_offset.encode(header_buf)?;
        }
        if version >= 4 {
            encode_array(header_buf, &self.aborted_transactions)?;
        }
        if version >= 11 {
            self.preferred_read_replica.encode(header_buf)?;
        }

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

impl ToByte for FetchPartitionResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.encode_versioned(buffer, 4)
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
            error_code: KafkaCode::None,
            session_id: 0,
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
            log_start_offset: -1,
            aborted_transactions: vec![],
            preferred_read_replica: -1,
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
            log_start_offset: -1,
            aborted_transactions: vec![AbortedTransaction {
                producer_id: 123,
                first_offset: 100,
            }],
            preferred_read_replica: -1,
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
            error_code: KafkaCode::None,
            session_id: 0,
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
            error_code: KafkaCode::None,
            session_id: 0,
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

    /// v5 inserts a per-partition `log_start_offset` (i64) between
    /// `aborted_transactions` and... wait — actually between
    /// `last_stable_offset` and `aborted_transactions`. Verify the
    /// per-partition payload grows by exactly 8 bytes between v4 and v5.
    #[test]
    fn v5_adds_eight_bytes_per_partition_for_log_start_offset() {
        let response = FetchResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            session_id: 0,
            responses: vec![FetchTopicResponse {
                name: "t".to_string(),
                partitions: vec![FetchPartitionResponse::success(0, 100, None)],
            }],
        };
        let mut buf_v4 = Vec::new();
        response.encode_versioned(&mut buf_v4, 4).unwrap();
        let mut buf_v5 = Vec::new();
        response.encode_versioned(&mut buf_v5, 5).unwrap();
        assert_eq!(buf_v4.len() + 8, buf_v5.len());
    }

    /// v7 prepends a top-level `error_code` (i16) + `session_id` (i32)
    /// between `throttle_time_ms` and the `responses` array — 6 bytes
    /// of new header.
    #[test]
    fn v7_adds_session_header_six_bytes() {
        let response = FetchResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            session_id: 0,
            responses: vec![],
        };
        let mut buf_v6 = Vec::new();
        response.encode_versioned(&mut buf_v6, 6).unwrap();
        let mut buf_v7 = Vec::new();
        response.encode_versioned(&mut buf_v7, 7).unwrap();
        assert_eq!(buf_v6.len() + 6, buf_v7.len());
    }

    /// v7 round-trips `error_code` + `session_id` at fixed offsets.
    #[test]
    fn v7_round_trips_session_id_and_error_code() {
        let response = FetchResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::FetchSessionIdNotFound,
            session_id: 0xDEADBEEFu32 as i32,
            responses: vec![],
        };
        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 7).unwrap();
        // throttle (4) + error_code (2) + session_id (4) + array_len (4)
        assert_eq!(buf.len(), 14);
        assert_eq!(
            i16::from_be_bytes([buf[4], buf[5]]),
            KafkaCode::FetchSessionIdNotFound as i16
        );
        assert_eq!(
            i32::from_be_bytes([buf[6], buf[7], buf[8], buf[9]]),
            0xDEADBEEFu32 as i32
        );
    }

    /// v11 inserts a per-partition `preferred_read_replica` (i32, 4
    /// bytes) before the records field.
    #[test]
    fn v11_adds_preferred_read_replica_four_bytes_per_partition() {
        let response = FetchResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            session_id: 0,
            responses: vec![FetchTopicResponse {
                name: "t".to_string(),
                partitions: vec![FetchPartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    high_watermark: 0,
                    last_stable_offset: 0,
                    log_start_offset: -1,
                    aborted_transactions: vec![],
                    preferred_read_replica: 7,
                    records: None,
                }],
            }],
        };
        let mut buf_v10 = Vec::new();
        response.encode_versioned(&mut buf_v10, 10).unwrap();
        let mut buf_v11 = Vec::new();
        response.encode_versioned(&mut buf_v11, 11).unwrap();
        assert_eq!(buf_v10.len() + 4, buf_v11.len());
    }

    /// v11 chain encoder must agree with the contiguous encoder byte for
    /// byte. The chain path is the production fetch hot path.
    #[test]
    fn v11_chain_encoder_matches_buffer_encoder() {
        let response = FetchResponseData {
            throttle_time_ms: 5,
            error_code: KafkaCode::None,
            session_id: 99,
            responses: vec![FetchTopicResponse {
                name: "topic".to_string(),
                partitions: vec![
                    FetchPartitionResponse {
                        partition_index: 0,
                        error_code: KafkaCode::None,
                        high_watermark: 1000,
                        last_stable_offset: 990,
                        log_start_offset: 0,
                        aborted_transactions: vec![AbortedTransaction {
                            producer_id: 11,
                            first_offset: 1,
                        }],
                        preferred_read_replica: 2,
                        records: Some(Bytes::from(vec![1, 2, 3, 4, 5, 6, 7, 8])),
                    },
                    FetchPartitionResponse::error(1, KafkaCode::NotLeaderForPartition),
                ],
            }],
        };

        let mut contiguous = Vec::new();
        response.encode_versioned(&mut contiguous, 11).unwrap();

        let mut chain = BytesChain::new();
        let mut header_buf = BytesMut::new();
        response
            .encode_versioned_into_chain(&mut chain, &mut header_buf, 11)
            .unwrap();
        if !header_buf.is_empty() {
            chain.push(header_buf.freeze());
        }
        let mut chained = Vec::new();
        for chunk in chain.chunks() {
            chained.extend_from_slice(chunk);
        }

        assert_eq!(contiguous, chained, "chain must match buffer byte-for-byte");
    }
}
