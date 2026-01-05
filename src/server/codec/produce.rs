//! Produce API codec.
//!
//! Handles encoding/decoding of Kafka Produce requests and responses.

use bytes::{Bytes, BytesMut};
use nombytes::NomBytes;

use super::KafkaCodec;
use crate::encode::ToByte;
use crate::error::Result;

// Re-export request/response types from public interfaces
pub use crate::server::request::ProduceRequestData;
pub use crate::server::response::ProduceResponseData;

#[cfg(test)]
pub use crate::server::response::{ProducePartitionResponse, ProduceTopicResponse};

/// Codec for Kafka Produce API.
///
/// The Produce API allows clients to publish records to topics.
///
/// # Supported Versions
///
/// - Versions 0-9 are supported
/// - Version 3+ supports transactional IDs
/// - Version 5+ supports record headers
pub struct ProduceCodec;

impl KafkaCodec for ProduceCodec {
    type Request = ProduceRequestData;
    type Response = ProduceResponseData;

    /// Kafka API key for Produce is 0.
    fn api_key() -> i16 {
        0
    }

    /// Minimum supported version.
    fn min_version() -> i16 {
        0
    }

    /// Maximum supported version.
    fn max_version() -> i16 {
        9
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_produce_request;
        let (_, request) = parse_produce_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse produce request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, _version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(256);
        response.encode(&mut buffer)?;
        Ok(buffer.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn test_produce_codec_api_key() {
        assert_eq!(ProduceCodec::api_key(), 0);
    }

    #[test]
    fn test_produce_codec_version_range() {
        assert_eq!(ProduceCodec::min_version(), 0);
        assert_eq!(ProduceCodec::max_version(), 9);
    }

    #[test]
    fn test_produce_codec_version_supported() {
        // Valid versions
        for v in 0..=9 {
            assert!(
                ProduceCodec::is_version_supported(v),
                "Version {} should be supported",
                v
            );
        }

        // Invalid versions
        assert!(!ProduceCodec::is_version_supported(-1));
        assert!(!ProduceCodec::is_version_supported(10));
    }

    #[test]
    fn test_encode_response() {
        use crate::error::KafkaCode;

        let response = ProduceResponseData {
            responses: vec![ProduceTopicResponse {
                name: "test-topic".to_string(),
                partitions: vec![ProducePartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                    base_offset: 42,
                    log_append_time: -1,
                }],
            }],
            throttle_time_ms: 0,
        };

        let bytes = ProduceCodec::encode_response(&response, 3).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_decode_request() {
        // Build a minimal produce request
        // Format: transactional_id (nullable string) + acks (i16) + timeout (i32) + topics array
        let mut request_bytes = BytesMut::new();

        // Transactional ID: null (length = -1)
        request_bytes.put_i16(-1);

        // Acks: -1 (all replicas)
        request_bytes.put_i16(-1);

        // Timeout: 30000ms
        request_bytes.put_i32(30000);

        // Topics array: 1 topic
        request_bytes.put_i32(1);

        // Topic name: "test"
        request_bytes.put_i16(4);
        request_bytes.put_slice(b"test");

        // Partitions array: 1 partition
        request_bytes.put_i32(1);

        // Partition index: 0
        request_bytes.put_i32(0);

        // Record set size: 0 (empty)
        request_bytes.put_i32(0);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = ProduceCodec::decode_request(bytes, 3).unwrap();

        assert_eq!(request.acks, -1);
        assert_eq!(request.timeout_ms, 30000);
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name, "test");
        assert_eq!(request.topics[0].partitions.len(), 1);
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    }
}
