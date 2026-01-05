//! Fetch API codec.
//!
//! Handles encoding/decoding of Kafka Fetch requests and responses.

use bytes::{Bytes, BytesMut};
use nombytes::NomBytes;

use super::KafkaCodec;
use crate::error::Result;

// Re-export request/response types from public interfaces
pub use crate::server::request::FetchRequestData;
pub use crate::server::response::FetchResponseData;

/// Codec for Kafka Fetch API.
///
/// The Fetch API allows consumers to read records from topic partitions.
///
/// # Supported Versions
///
/// - Versions 0-11 are supported
/// - Version 1+ includes throttle_time_ms in response
/// - Version 4+ supports isolation level
pub struct FetchCodec;

impl KafkaCodec for FetchCodec {
    type Request = FetchRequestData;
    type Response = FetchResponseData;

    /// Kafka API key for Fetch is 1.
    fn api_key() -> i16 {
        1
    }

    /// Minimum supported version.
    fn min_version() -> i16 {
        0
    }

    /// Maximum supported version.
    fn max_version() -> i16 {
        11
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_fetch_request;
        let (_, request) = parse_fetch_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse fetch request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(256);
        response.encode_versioned(&mut buffer, version)?;
        Ok(buffer.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    #[test]
    fn test_fetch_codec_api_key() {
        assert_eq!(FetchCodec::api_key(), 1);
    }

    #[test]
    fn test_fetch_codec_version_range() {
        assert_eq!(FetchCodec::min_version(), 0);
        assert_eq!(FetchCodec::max_version(), 11);
    }

    #[test]
    fn test_fetch_codec_version_supported() {
        for v in 0..=11 {
            assert!(
                FetchCodec::is_version_supported(v),
                "Version {} should be supported",
                v
            );
        }
        assert!(!FetchCodec::is_version_supported(-1));
        assert!(!FetchCodec::is_version_supported(12));
    }

    #[test]
    fn test_encode_response() {
        use crate::server::response::{FetchPartitionResponse, FetchTopicResponse};

        let response = FetchResponseData {
            throttle_time_ms: 0,
            responses: vec![FetchTopicResponse {
                name: "test-topic".to_string(),
                partitions: vec![FetchPartitionResponse::success(0, 100, None)],
            }],
        };

        let bytes = FetchCodec::encode_response(&response, 3).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_decode_request() {
        let mut request_bytes = BytesMut::new();

        // replica_id: -1 (consumer)
        request_bytes.put_i32(-1);
        // max_wait_ms: 500
        request_bytes.put_i32(500);
        // min_bytes: 1
        request_bytes.put_i32(1);
        // max_bytes: 1048576
        request_bytes.put_i32(1048576);
        // isolation_level: 0 (READ_UNCOMMITTED)
        request_bytes.put_i8(0);
        // topics array: 1 topic
        request_bytes.put_i32(1);
        // topic name: "test"
        request_bytes.put_i16(4);
        request_bytes.put_slice(b"test");
        // partitions array: 1 partition
        request_bytes.put_i32(1);
        // partition_index: 0
        request_bytes.put_i32(0);
        // fetch_offset: 0
        request_bytes.put_i64(0);
        // partition_max_bytes: 1048576
        request_bytes.put_i32(1048576);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = FetchCodec::decode_request(bytes, 4).unwrap();

        assert_eq!(request.replica_id, -1);
        assert_eq!(request.max_wait_ms, 500);
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].name, "test");
        assert_eq!(request.topics[0].partitions.len(), 1);
        assert_eq!(request.topics[0].partitions[0].partition_index, 0);
    }
}
