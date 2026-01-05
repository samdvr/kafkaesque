//! Offset APIs codec.
//!
//! Handles encoding/decoding of Kafka offset-related requests and responses:
//! - ListOffsets (API key 2)
//! - OffsetCommit (API key 8)
//! - OffsetFetch (API key 9)

use bytes::{Bytes, BytesMut};
use nombytes::NomBytes;

use super::KafkaCodec;
use crate::encode::ToByte;
use crate::error::Result;

// Re-export request/response types from public interfaces
pub use crate::server::request::{
    ListOffsetsRequestData, OffsetCommitRequestData, OffsetFetchRequestData,
};
pub use crate::server::response::{
    ListOffsetsResponseData, OffsetCommitResponseData, OffsetFetchResponseData,
};

// ============================================================================
// ListOffsets Codec
// ============================================================================

/// Codec for Kafka ListOffsets API.
///
/// Allows clients to query the earliest/latest offsets for partitions.
///
/// # Supported Versions
///
/// - Versions 0-5 are supported
/// - Version 1+ includes timestamp in request
/// - Version 2+ includes isolation level
pub struct ListOffsetsCodec;

impl KafkaCodec for ListOffsetsCodec {
    type Request = ListOffsetsRequestData;
    type Response = ListOffsetsResponseData;

    /// Kafka API key for ListOffsets is 2.
    fn api_key() -> i16 {
        2
    }

    /// Minimum supported version.
    fn min_version() -> i16 {
        0
    }

    /// Maximum supported version.
    fn max_version() -> i16 {
        5
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_list_offsets_request;
        let (_, request) = parse_list_offsets_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse list offsets request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(128);
        response.encode_versioned(&mut buffer, version)?;
        Ok(buffer.freeze())
    }
}

// ============================================================================
// OffsetCommit Codec
// ============================================================================

/// Codec for Kafka OffsetCommit API.
///
/// Allows consumers to commit their current offsets to the coordinator.
///
/// # Supported Versions
///
/// - Versions 0-8 are supported
/// - Version 1+ includes generation ID and member ID
/// - Version 2+ includes retention time
pub struct OffsetCommitCodec;

impl KafkaCodec for OffsetCommitCodec {
    type Request = OffsetCommitRequestData;
    type Response = OffsetCommitResponseData;

    /// Kafka API key for OffsetCommit is 8.
    fn api_key() -> i16 {
        8
    }

    /// Minimum supported version.
    fn min_version() -> i16 {
        0
    }

    /// Maximum supported version.
    fn max_version() -> i16 {
        8
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_offset_commit_request;
        let (_, request) = parse_offset_commit_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse offset commit request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, _version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(64);
        response.encode(&mut buffer)?;
        Ok(buffer.freeze())
    }
}

// ============================================================================
// OffsetFetch Codec
// ============================================================================

/// Codec for Kafka OffsetFetch API.
///
/// Allows consumers to fetch their committed offsets from the coordinator.
///
/// # Supported Versions
///
/// - Versions 0-8 are supported
/// - Version 2+ includes group-level error code
/// - Version 3+ includes throttle time
pub struct OffsetFetchCodec;

impl KafkaCodec for OffsetFetchCodec {
    type Request = OffsetFetchRequestData;
    type Response = OffsetFetchResponseData;

    /// Kafka API key for OffsetFetch is 9.
    fn api_key() -> i16 {
        9
    }

    /// Minimum supported version.
    fn min_version() -> i16 {
        0
    }

    /// Maximum supported version.
    fn max_version() -> i16 {
        8
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_offset_fetch_request;
        let (_, request) = parse_offset_fetch_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse offset fetch request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(128);
        response.encode_versioned(&mut buffer, version)?;
        Ok(buffer.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    // ========================================================================
    // ListOffsets Tests
    // ========================================================================

    #[test]
    fn test_list_offsets_codec_api_key() {
        assert_eq!(ListOffsetsCodec::api_key(), 2);
    }

    #[test]
    fn test_list_offsets_codec_version_range() {
        assert_eq!(ListOffsetsCodec::min_version(), 0);
        assert_eq!(ListOffsetsCodec::max_version(), 5);
    }

    #[test]
    fn test_list_offsets_decode_request() {
        use crate::server::response::ListOffsetsTopicResponse;

        let mut request_bytes = BytesMut::new();
        // replica_id: -1
        request_bytes.put_i32(-1);
        // isolation_level: 0 (for version 2+)
        request_bytes.put_i8(0);
        // topics: 1
        request_bytes.put_i32(1);
        // topic name: "test"
        request_bytes.put_i16(4);
        request_bytes.put_slice(b"test");
        // partitions: 1
        request_bytes.put_i32(1);
        // partition_index: 0
        request_bytes.put_i32(0);
        // timestamp: -1 (latest)
        request_bytes.put_i64(-1);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = ListOffsetsCodec::decode_request(bytes, 2).unwrap();

        assert_eq!(request.replica_id, -1);
        assert_eq!(request.isolation_level, 0);
        assert_eq!(request.topics.len(), 1);

        // Test encode response
        let response = ListOffsetsResponseData {
            throttle_time_ms: 0,
            topics: vec![ListOffsetsTopicResponse {
                name: "test".to_string(),
                partitions: vec![],
            }],
        };

        let encoded = ListOffsetsCodec::encode_response(&response, 2).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========================================================================
    // OffsetCommit Tests
    // ========================================================================

    #[test]
    fn test_offset_commit_codec_api_key() {
        assert_eq!(OffsetCommitCodec::api_key(), 8);
    }

    #[test]
    fn test_offset_commit_codec_version_range() {
        assert_eq!(OffsetCommitCodec::min_version(), 0);
        assert_eq!(OffsetCommitCodec::max_version(), 8);
    }

    #[test]
    fn test_offset_commit_decode_request() {
        use crate::server::response::OffsetCommitTopicResponse;

        let mut request_bytes = BytesMut::new();
        // group_id: "test-group"
        request_bytes.put_i16(10);
        request_bytes.put_slice(b"test-group");
        // generation_id: 1
        request_bytes.put_i32(1);
        // member_id: "member-1"
        request_bytes.put_i16(8);
        request_bytes.put_slice(b"member-1");
        // topics: 1
        request_bytes.put_i32(1);
        // topic name: "test"
        request_bytes.put_i16(4);
        request_bytes.put_slice(b"test");
        // partitions: 1
        request_bytes.put_i32(1);
        // partition_index: 0
        request_bytes.put_i32(0);
        // committed_offset: 100
        request_bytes.put_i64(100);
        // committed_metadata: null
        request_bytes.put_i16(-1);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = OffsetCommitCodec::decode_request(bytes, 2).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.generation_id, 1);
        assert_eq!(request.member_id, "member-1");

        // Test encode response
        let response = OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitTopicResponse {
                name: "test".to_string(),
                partitions: vec![],
            }],
        };

        let encoded = OffsetCommitCodec::encode_response(&response, 2).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========================================================================
    // OffsetFetch Tests
    // ========================================================================

    #[test]
    fn test_offset_fetch_codec_api_key() {
        assert_eq!(OffsetFetchCodec::api_key(), 9);
    }

    #[test]
    fn test_offset_fetch_codec_version_range() {
        assert_eq!(OffsetFetchCodec::min_version(), 0);
        assert_eq!(OffsetFetchCodec::max_version(), 8);
    }

    #[test]
    fn test_offset_fetch_decode_request() {
        use crate::error::KafkaCode;
        use crate::server::response::OffsetFetchTopicResponse;

        let mut request_bytes = BytesMut::new();
        // group_id: "test-group"
        request_bytes.put_i16(10);
        request_bytes.put_slice(b"test-group");
        // topics: 1
        request_bytes.put_i32(1);
        // topic name: "test"
        request_bytes.put_i16(4);
        request_bytes.put_slice(b"test");
        // partition_indexes: 2
        request_bytes.put_i32(2);
        // partition 0
        request_bytes.put_i32(0);
        // partition 1
        request_bytes.put_i32(1);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = OffsetFetchCodec::decode_request(bytes, 1).unwrap();

        assert_eq!(request.group_id, "test-group");
        assert_eq!(request.topics.len(), 1);
        assert_eq!(request.topics[0].partition_indexes, vec![0, 1]);

        // Test encode response
        let response = OffsetFetchResponseData {
            throttle_time_ms: 0,
            topics: vec![OffsetFetchTopicResponse {
                name: "test".to_string(),
                partitions: vec![],
            }],
            error_code: KafkaCode::None,
        };

        let encoded = OffsetFetchCodec::encode_response(&response, 3).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_offset_fetch_version_encoding_differences() {
        use crate::error::KafkaCode;

        let response = OffsetFetchResponseData {
            throttle_time_ms: 100,
            topics: vec![],
            error_code: KafkaCode::None,
        };

        // v1: just topics array (4 bytes for empty array)
        let v1_bytes = OffsetFetchCodec::encode_response(&response, 1).unwrap();

        // v2: topics array + error_code (4 + 2 = 6 bytes)
        let v2_bytes = OffsetFetchCodec::encode_response(&response, 2).unwrap();

        // v3: throttle_time + topics array + error_code (4 + 4 + 2 = 10 bytes)
        let v3_bytes = OffsetFetchCodec::encode_response(&response, 3).unwrap();

        assert_eq!(v1_bytes.len(), 4); // Just array length
        assert_eq!(v2_bytes.len(), 6); // Array + error code
        assert_eq!(v3_bytes.len(), 10); // Throttle + array + error code
    }
}
