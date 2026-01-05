//! Metadata API codec.
//!
//! Handles encoding/decoding of Kafka Metadata requests and responses.

use bytes::{Bytes, BytesMut};
use nombytes::NomBytes;

use super::KafkaCodec;
use crate::encode::ToByte;
use crate::error::Result;

// Re-export request/response types from public interfaces
pub use crate::server::request::MetadataRequestData;
pub use crate::server::response::MetadataResponseData;

/// Codec for Kafka Metadata API.
///
/// The Metadata API allows clients to discover cluster topology
/// and topic/partition information.
///
/// # Supported Versions
///
/// - Versions 0-9 are supported
/// - Version 1+ includes controller_id
/// - Version 5+ includes rack info
pub struct MetadataCodec;

impl KafkaCodec for MetadataCodec {
    type Request = MetadataRequestData;
    type Response = MetadataResponseData;

    /// Kafka API key for Metadata is 3.
    fn api_key() -> i16 {
        3
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
        use crate::server::request::parse_metadata_request;
        let (_, request) = parse_metadata_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse metadata request".to_string())
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
    fn test_metadata_codec_api_key() {
        assert_eq!(MetadataCodec::api_key(), 3);
    }

    #[test]
    fn test_metadata_codec_version_range() {
        assert_eq!(MetadataCodec::min_version(), 0);
        assert_eq!(MetadataCodec::max_version(), 9);
    }

    #[test]
    fn test_metadata_codec_version_supported() {
        for v in 0..=9 {
            assert!(
                MetadataCodec::is_version_supported(v),
                "Version {} should be supported",
                v
            );
        }
        assert!(!MetadataCodec::is_version_supported(-1));
        assert!(!MetadataCodec::is_version_supported(10));
    }

    #[test]
    fn test_encode_response() {
        use crate::error::KafkaCode;
        use crate::server::response::{BrokerData, PartitionMetadata, TopicMetadata};

        let response = MetadataResponseData {
            brokers: vec![BrokerData {
                node_id: 0,
                host: "localhost".to_string(),
                port: 9092,
                rack: None,
            }],
            controller_id: 0,
            topics: vec![TopicMetadata {
                error_code: KafkaCode::None,
                name: "test".to_string(),
                is_internal: false,
                partitions: vec![PartitionMetadata {
                    error_code: KafkaCode::None,
                    partition_index: 0,
                    leader_id: 0,
                    replica_nodes: vec![0],
                    isr_nodes: vec![0],
                }],
            }],
        };

        let bytes = MetadataCodec::encode_response(&response, 3).unwrap();
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_decode_request_all_topics() {
        let mut request_bytes = BytesMut::new();
        // topics: -1 (all topics)
        request_bytes.put_i32(-1);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = MetadataCodec::decode_request(bytes, 1).unwrap();

        assert!(request.topics.is_none());
    }

    #[test]
    fn test_decode_request_specific_topics() {
        let mut request_bytes = BytesMut::new();
        // topics: 2 topics
        request_bytes.put_i32(2);
        // topic 1: "foo"
        request_bytes.put_i16(3);
        request_bytes.put_slice(b"foo");
        // topic 2: "bar"
        request_bytes.put_i16(3);
        request_bytes.put_slice(b"bar");

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = MetadataCodec::decode_request(bytes, 1).unwrap();

        let topics = request.topics.unwrap();
        assert_eq!(topics.len(), 2);
        assert_eq!(topics[0], "foo");
        assert_eq!(topics[1], "bar");
    }
}
