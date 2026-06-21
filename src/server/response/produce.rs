//! Produce response encoding.
//!
//! Supports v0..=v9. Per-version deltas:
//!
//! - v1+ adds top-level `throttle_time_ms`
//! - v2+ adds per-partition `log_append_time_ms`
//! - v5+ adds per-partition `log_start_offset`
//! - v8+ adds per-partition `record_errors` array and `error_message`
//! - v9+ uses flexible encoding (response header v1: trailing tagged
//!   fields after the correlation id; compact arrays / strings / nullable
//!   strings on the body)

use bytes::BufMut;

use crate::encode::{
    ToByte, encode_array, encode_compact_array, encode_empty_tagged_fields, encode_unsigned_varint,
};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

/// Produce response data.
#[derive(Debug, Clone, Default)]
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
    /// v5+ only. `-1` when unknown.
    pub log_start_offset: i64,
    /// v8+ only. Per-record error annotations.
    pub record_errors: Vec<BatchIndexAndErrorMessage>,
    /// v8+ only. Top-level error message for this partition.
    pub error_message: Option<String>,
}

#[derive(Debug, Clone)]
pub struct BatchIndexAndErrorMessage {
    pub batch_index: i32,
    pub batch_index_error_message: Option<String>,
}

impl ProducePartitionResponse {
    /// Create an error response for a partition.
    pub fn error(partition_index: i32, error_code: KafkaCode) -> Self {
        Self {
            partition_index,
            error_code,
            base_offset: -1,
            log_append_time: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
        }
    }

    /// Create a success response for a partition.
    pub fn success(partition_index: i32, base_offset: i64) -> Self {
        Self {
            partition_index,
            error_code: KafkaCode::None,
            base_offset,
            log_append_time: -1,
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
        }
    }
}

impl ProduceResponseData {
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        let flexible = version >= 9;

        if flexible {
            encode_compact_array(buffer, &self.responses, |buf, t| {
                encode_topic_versioned(buf, t, version, true)
            })?;
        } else {
            crate::encode::encode_array_len_pub(buffer, self.responses.len())?;
            for t in &self.responses {
                encode_topic_versioned(buffer, t, version, false)?;
            }
        }
        if version >= 1 {
            self.throttle_time_ms.encode(buffer)?;
        }
        if flexible {
            encode_empty_tagged_fields(buffer);
        }
        Ok(())
    }
}

fn encode_topic_versioned<W: BufMut>(
    buffer: &mut W,
    t: &ProduceTopicResponse,
    version: i16,
    flexible: bool,
) -> Result<()> {
    if flexible {
        encode_compact_string(buffer, &t.name);
    } else {
        t.name.encode(buffer)?;
    }
    if flexible {
        encode_compact_array(buffer, &t.partitions, |buf, p| {
            encode_partition_versioned(buf, p, version, true)
        })?;
    } else {
        crate::encode::encode_array_len_pub(buffer, t.partitions.len())?;
        for p in &t.partitions {
            encode_partition_versioned(buffer, p, version, false)?;
        }
    }
    if flexible {
        encode_empty_tagged_fields(buffer);
    }
    Ok(())
}

fn encode_partition_versioned<W: BufMut>(
    buffer: &mut W,
    p: &ProducePartitionResponse,
    version: i16,
    flexible: bool,
) -> Result<()> {
    p.partition_index.encode(buffer)?;
    (p.error_code as i16).encode(buffer)?;
    p.base_offset.encode(buffer)?;
    if version >= 2 {
        p.log_append_time.encode(buffer)?;
    }
    if version >= 5 {
        p.log_start_offset.encode(buffer)?;
    }
    if version >= 8 {
        if flexible {
            encode_compact_array(buffer, &p.record_errors, |buf, e| {
                encode_batch_index_versioned(buf, e, true)
            })?;
            encode_compact_nullable_string(buffer, p.error_message.as_deref());
        } else {
            encode_array(buffer, &p.record_errors)?;
            encode_nullable_string(p.error_message.as_deref(), buffer)?;
        }
    }
    if flexible {
        encode_empty_tagged_fields(buffer);
    }
    Ok(())
}

fn encode_batch_index_versioned<W: BufMut>(
    buffer: &mut W,
    e: &BatchIndexAndErrorMessage,
    flexible: bool,
) -> Result<()> {
    e.batch_index.encode(buffer)?;
    if flexible {
        encode_compact_nullable_string(buffer, e.batch_index_error_message.as_deref());
        encode_empty_tagged_fields(buffer);
    } else {
        encode_nullable_string(e.batch_index_error_message.as_deref(), buffer)?;
    }
    Ok(())
}

fn encode_compact_string<W: BufMut>(buffer: &mut W, s: &str) {
    encode_unsigned_varint(buffer, (s.len() + 1) as u32);
    buffer.put_slice(s.as_bytes());
}

fn encode_compact_nullable_string<W: BufMut>(buffer: &mut W, s: Option<&str>) {
    match s {
        None => encode_unsigned_varint(buffer, 0),
        Some(value) => {
            encode_unsigned_varint(buffer, (value.len() + 1) as u32);
            buffer.put_slice(value.as_bytes());
        }
    }
}

impl ToByte for ProduceResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v3 framing (the first version this implementation
        // ever advertised). Maintains pre-versioned-encode call-site
        // behavior for handlers that haven't been updated.
        self.encode_versioned(buffer, 3)
    }
}

impl ToByte for ProduceTopicResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        encode_topic_versioned(buffer, self, 3, false)
    }
}

impl ToByte for ProducePartitionResponse {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        encode_partition_versioned(buffer, self, 3, false)
    }
}

impl ToByte for BatchIndexAndErrorMessage {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        encode_batch_index_versioned(buffer, self, false)
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
            log_start_offset: -1,
            record_errors: vec![],
            error_message: None,
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

    /// v5 inserts a per-partition `log_start_offset` (i64, 8 bytes)
    /// after `log_append_time`.
    #[test]
    fn v5_adds_log_start_offset_eight_bytes_per_partition() {
        let response = ProduceResponseData {
            responses: vec![ProduceTopicResponse {
                name: "t".to_string(),
                partitions: vec![ProducePartitionResponse::success(0, 100)],
            }],
            throttle_time_ms: 0,
        };
        let mut v3 = Vec::new();
        response.encode_versioned(&mut v3, 3).unwrap();
        let mut v5 = Vec::new();
        response.encode_versioned(&mut v5, 5).unwrap();
        assert_eq!(v3.len() + 8, v5.len());
    }

    /// v8 inserts per-partition `record_errors` (4 bytes for empty
    /// array length) plus `error_message` (2 bytes for null nullable
    /// string) — 6 bytes total when both are empty/null.
    #[test]
    fn v8_adds_record_errors_and_error_message() {
        let response = ProduceResponseData {
            responses: vec![ProduceTopicResponse {
                name: "t".to_string(),
                partitions: vec![ProducePartitionResponse::success(0, 100)],
            }],
            throttle_time_ms: 0,
        };
        let mut v7 = Vec::new();
        response.encode_versioned(&mut v7, 7).unwrap();
        let mut v8 = Vec::new();
        response.encode_versioned(&mut v8, 8).unwrap();
        // v8 adds: array_len (4) + nullable_string_null (2) = 6
        assert_eq!(v7.len() + 6, v8.len());
    }

    /// v9 uses flexible encoding. For an empty response the encoded
    /// size should be smaller than v8 (compact varints replace 4-byte
    /// length prefixes) but include a top-level tagged-fields trailer.
    #[test]
    fn v9_uses_flexible_compact_encoding() {
        let response = ProduceResponseData {
            responses: vec![],
            throttle_time_ms: 0,
        };
        let mut v8 = Vec::new();
        response.encode_versioned(&mut v8, 8).unwrap();
        let mut v9 = Vec::new();
        response.encode_versioned(&mut v9, 9).unwrap();
        // v8: array_len (4) + throttle_time_ms (4) = 8
        // v9: compact_array_len (1) + throttle_time_ms (4) + tagged (1) = 6
        assert_eq!(v8.len(), 8);
        assert_eq!(v9.len(), 6);
    }

    /// v9 with one partition holding a populated `error_message`. Verify
    /// the compact-nullable-string length-byte and the trailing tagged
    /// fields all line up.
    #[test]
    fn v9_round_trips_partition_error_message() {
        let response = ProduceResponseData {
            responses: vec![ProduceTopicResponse {
                name: "t".to_string(),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::CorruptMessage,
                    base_offset: -1,
                    log_append_time: -1,
                    log_start_offset: -1,
                    record_errors: vec![],
                    error_message: Some("bad".to_string()),
                }],
            }],
            throttle_time_ms: 0,
        };
        let mut buf = Vec::new();
        response.encode_versioned(&mut buf, 9).unwrap();
        // Sanity: encoded buffer must contain "bad" verbatim.
        assert!(
            buf.windows(3).any(|w| w == b"bad"),
            "compact_nullable_string payload missing"
        );
        // Final byte must be the body's tagged-fields zero.
        assert_eq!(*buf.last().unwrap(), 0);
    }
}
