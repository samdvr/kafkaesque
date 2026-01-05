//! Kafka protocol codec layer.
//!
//! This module provides a unified trait for encoding/decoding Kafka wire protocol
//! messages. Each API (Produce, Fetch, etc.) implements `KafkaCodec` to handle
//! both request parsing and response encoding.
//!
//! # Design Goals
//!
//! 1. **Reduce duplication**: Consolidate request/ and response/ modules
//! 2. **Type safety**: Associated types ensure request/response pairing
//! 3. **Version handling**: Centralized version negotiation
//!
//! # Supported APIs
//!
//! | API | Key | Codec |
//! |-----|-----|-------|
//! | Produce | 0 | `ProduceCodec` |
//! | Fetch | 1 | `FetchCodec` |
//! | ListOffsets | 2 | `ListOffsetsCodec` |
//! | Metadata | 3 | `MetadataCodec` |
//! | OffsetCommit | 8 | `OffsetCommitCodec` |
//! | OffsetFetch | 9 | `OffsetFetchCodec` |
//! | FindCoordinator | 10 | `FindCoordinatorCodec` |
//! | JoinGroup | 11 | `JoinGroupCodec` |
//! | Heartbeat | 12 | `HeartbeatCodec` |
//! | LeaveGroup | 13 | `LeaveGroupCodec` |
//! | SyncGroup | 14 | `SyncGroupCodec` |
//! | DescribeGroups | 15 | `DescribeGroupsCodec` |
//! | ListGroups | 16 | `ListGroupsCodec` |
//! | DeleteGroups | 42 | `DeleteGroupsCodec` |
//!
//! # Example
//!
//! ```rust,ignore
//! use kafkaesque::server::codec::{KafkaCodec, ProduceCodec};
//!
//! // Decode request
//! let request = ProduceCodec::decode_request(&bytes, version)?;
//!
//! // Process request...
//!
//! // Encode response
//! let response_bytes = ProduceCodec::encode_response(&response, version)?;
//! ```

mod fetch;
mod groups;
mod metadata;
mod offsets;
mod produce;

pub use fetch::FetchCodec;
pub use groups::{
    DeleteGroupsCodec, DescribeGroupsCodec, FindCoordinatorCodec, HeartbeatCodec, JoinGroupCodec,
    LeaveGroupCodec, ListGroupsCodec, SyncGroupCodec,
};
pub use metadata::MetadataCodec;
pub use offsets::{ListOffsetsCodec, OffsetCommitCodec, OffsetFetchCodec};
pub use produce::ProduceCodec;

use bytes::Bytes;
use nombytes::NomBytes;

use crate::error::Result;

/// Trait for Kafka protocol codecs.
///
/// Each Kafka API (Produce, Fetch, etc.) implements this trait to handle
/// both request parsing and response encoding in a unified way.
pub trait KafkaCodec {
    /// The request type for this API.
    type Request;

    /// The response type for this API.
    type Response;

    /// The Kafka API key for this operation.
    fn api_key() -> i16;

    /// The minimum supported version.
    fn min_version() -> i16;

    /// The maximum supported version.
    fn max_version() -> i16;

    /// Check if a version is supported.
    fn is_version_supported(version: i16) -> bool {
        version >= Self::min_version() && version <= Self::max_version()
    }

    /// Decode a request from bytes.
    ///
    /// # Arguments
    /// * `bytes` - Raw request bytes (after header is stripped)
    /// * `version` - API version for version-specific parsing
    ///
    /// # Returns
    /// * `Ok(request)` - Parsed request
    /// * `Err(error)` - Parse error
    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request>;

    /// Encode a response to bytes.
    ///
    /// # Arguments
    /// * `response` - Response to encode
    /// * `version` - API version for version-specific encoding
    ///
    /// # Returns
    /// * `Ok(bytes)` - Encoded response bytes
    /// * `Err(error)` - Encoding error
    fn encode_response(response: &Self::Response, version: i16) -> Result<Bytes>;
}
