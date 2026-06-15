//! Admin response encoding (CreateTopics, DeleteTopics, InitProducerId).

use bytes::BufMut;

use crate::encode::{ToByte, encode_array};
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

// ============================================================================
// CreateTopics
// ============================================================================

/// CreateTopics response data.
#[derive(Debug, Clone, Default)]
pub struct CreateTopicsResponseData {
    pub throttle_time_ms: i32,
    pub topics: Vec<CreateTopicResponseData>,
}

#[derive(Debug, Clone)]
pub struct CreateTopicResponseData {
    pub name: String,
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
}

impl ToByte for CreateTopicResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        Ok(())
    }
}

impl ToByte for CreateTopicsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_array(buffer, &self.topics)?;
        Ok(())
    }
}

// ============================================================================
// DeleteTopics
// ============================================================================

/// DeleteTopics response data.
#[derive(Debug, Clone, Default)]
pub struct DeleteTopicsResponseData {
    pub throttle_time_ms: i32,
    pub responses: Vec<DeleteTopicResponseData>,
}

#[derive(Debug, Clone)]
pub struct DeleteTopicResponseData {
    pub name: String,
    pub error_code: KafkaCode,
}

impl ToByte for DeleteTopicResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.name.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

impl ToByte for DeleteTopicsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        encode_array(buffer, &self.responses)?;
        Ok(())
    }
}

// ============================================================================
// ErrorResponse
// ============================================================================

/// Generic error response for unsupported API versions.
#[derive(Debug, Clone)]
pub struct ErrorResponseData {
    pub error_code: KafkaCode,
}

impl ToByte for ErrorResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

// ============================================================================
// InitProducerId
// ============================================================================

/// InitProducerId response data.
///
/// Per the Kafka spec InitProducerId became flexible at v2
/// (`"flexibleVersions": "2+"`). The body fields are identical across
/// v0–v4 (`throttle_time_ms`, `error_code`, `producer_id`,
/// `producer_epoch`); only the framing differs:
/// - v0–v1: classic body (no tagged fields), response header v0
/// - v2+: flexible body (trailing tagged fields), response header v1
///   (correlation id + tagged fields)
#[derive(Debug, Clone, Default)]
pub struct InitProducerIdResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
    pub producer_id: i64,
    pub producer_epoch: i16,
}

impl InitProducerIdResponseData {
    /// Encode the fixed fields shared by every version.
    fn encode_fields<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        self.producer_id.encode(buffer)?;
        self.producer_epoch.encode(buffer)?;
        Ok(())
    }

    /// Encode for classic versions (v0–v1): no tagged fields.
    pub fn encode_classic<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.encode_fields(buffer)
    }

    /// Encode for flexible versions (v2+): trailing tagged fields.
    pub fn encode_flexible<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.encode_fields(buffer)?;
        // Empty tagged fields (flexible version marker)
        buffer.put_u8(0);
        Ok(())
    }

    /// Encode the body for a specific request version (flexible from v2).
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 2 {
            self.encode_flexible(buffer)
        } else {
            self.encode_classic(buffer)
        }
    }
}

impl ToByte for InitProducerIdResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to flexible encoding; version-aware call sites should use
        // `encode_versioned` (the connection dispatch does).
        self.encode_flexible(buffer)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::{Buf, BytesMut};

    #[test]
    fn create_topics_response_encodes_throttle_array_topic_name_error_message() {
        let resp = CreateTopicsResponseData {
            throttle_time_ms: 100,
            topics: vec![CreateTopicResponseData {
                name: "events".into(),
                error_code: KafkaCode::None,
                error_message: None,
            }],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i32(), 100, "throttle_time_ms first");
        assert_eq!(bytes.get_i32(), 1, "topics array length");
        assert_eq!(bytes.get_i16(), 6, "topic name length");
        let mut name = vec![0u8; 6];
        bytes.copy_to_slice(&mut name);
        assert_eq!(name, b"events");
        assert_eq!(bytes.get_i16(), 0, "error_code");
        assert_eq!(bytes.get_i16(), -1, "null error_message");
    }

    #[test]
    fn create_topics_error_message_string_round_trips() {
        let resp = CreateTopicResponseData {
            name: "t".into(),
            error_code: KafkaCode::TopicAlreadyExists,
            error_message: Some("already exists".into()),
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let mut bytes = buf.freeze();
        let _name_len = bytes.get_i16();
        let _ = bytes.split_to(1); // skip "t"
        assert_eq!(bytes.get_i16(), KafkaCode::TopicAlreadyExists as i16);
        assert_eq!(bytes.get_i16(), "already exists".len() as i16);
    }

    #[test]
    fn delete_topics_response_encodes_array_then_topic_then_code() {
        let resp = DeleteTopicsResponseData {
            throttle_time_ms: 0,
            responses: vec![
                DeleteTopicResponseData {
                    name: "a".into(),
                    error_code: KafkaCode::None,
                },
                DeleteTopicResponseData {
                    name: "b".into(),
                    error_code: KafkaCode::UnknownTopicOrPartition,
                },
            ],
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let mut bytes = buf.freeze();
        assert_eq!(bytes.get_i32(), 0, "throttle");
        assert_eq!(bytes.get_i32(), 2, "responses array length");
    }

    #[test]
    fn init_producer_id_classic_omits_tagged_fields() {
        let resp = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 42,
            producer_epoch: 7,
        };
        let mut classic = BytesMut::new();
        resp.encode_classic(&mut classic).expect("encode classic");
        let mut flex = BytesMut::new();
        resp.encode_flexible(&mut flex).expect("encode flex");
        assert_eq!(
            flex.len(),
            classic.len() + 1,
            "flexible adds exactly one trailing tagged-fields byte"
        );
        assert_eq!(flex[flex.len() - 1], 0, "tagged-fields byte must be 0");
    }

    #[test]
    fn init_producer_id_versioned_chooses_classic_below_v2_flex_above() {
        let resp = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 1,
            producer_epoch: 0,
        };
        for v in 0..=1i16 {
            let mut buf = BytesMut::new();
            resp.encode_versioned(&mut buf, v).expect("classic");
            // 4 (throttle) + 2 (error) + 8 (id) + 2 (epoch) = 16, no tagged
            assert_eq!(buf.len(), 16, "v{v} classic must be 16 bytes");
        }
        for v in 2..=4i16 {
            let mut buf = BytesMut::new();
            resp.encode_versioned(&mut buf, v).expect("flex");
            assert_eq!(
                buf.len(),
                17,
                "v{v} flexible must be 17 bytes (extra tagged byte)"
            );
        }
    }

    #[test]
    fn error_response_encodes_only_a_kafka_code() {
        let resp = ErrorResponseData {
            error_code: KafkaCode::UnsupportedVersion,
        };
        let mut buf = BytesMut::new();
        resp.encode(&mut buf).expect("encode");
        let bytes = buf.freeze();
        assert_eq!(bytes.len(), 2, "ErrorResponse is exactly i16");
        let code = i16::from_be_bytes([bytes[0], bytes[1]]);
        assert_eq!(code, KafkaCode::UnsupportedVersion as i16);
    }
}
