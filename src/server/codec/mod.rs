//! Kafka protocol codec layer (test / round-trip helpers).
//!
//! Production dispatch uses [`crate::server::request::Request::parse`] and the
//! version-aware encoders in `connection.rs`, with version bounds from
//! [`crate::server::versions::SUPPORTED_VERSIONS`]. The codecs here are thin
//! wrappers around the same parsers/encoders so tests can exercise wire
//! round-trips without duplicating version tables.
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
use crate::server::request::ApiKey;
use crate::server::versions::find_version;

/// Advertised `(min, max)` for an API key from [`crate::server::versions::SUPPORTED_VERSIONS`].
pub fn advertised_version_range(api_key: i16) -> (i16, i16) {
    find_version(ApiKey::from(api_key))
        .map(|v| (v.min_version, v.max_version))
        .unwrap_or((0, 0))
}

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

    /// Minimum advertised version (from [`crate::server::versions::SUPPORTED_VERSIONS`]).
    fn min_version() -> i16 {
        advertised_version_range(Self::api_key()).0
    }

    /// Maximum advertised version (from [`crate::server::versions::SUPPORTED_VERSIONS`]).
    fn max_version() -> i16 {
        advertised_version_range(Self::api_key()).1
    }

    /// Check if a version is within the advertised range.
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

#[cfg(test)]
mod sync_tests {
    use super::*;
    use crate::server::versions::SUPPORTED_VERSIONS;

    use super::fetch::FetchCodec;
    use super::groups::{
        DeleteGroupsCodec, DescribeGroupsCodec, FindCoordinatorCodec, HeartbeatCodec,
        JoinGroupCodec, LeaveGroupCodec, ListGroupsCodec, SyncGroupCodec,
    };
    use super::metadata::MetadataCodec;
    use super::offsets::{ListOffsetsCodec, OffsetCommitCodec, OffsetFetchCodec};
    use super::produce::ProduceCodec;

    fn assert_codec_matches_advertised<C: KafkaCodec>(api_key: ApiKey) {
        let advertised = find_version(api_key).expect("codec API must be advertised");
        assert_eq!(C::api_key(), i16::from(api_key));
        assert_eq!(C::min_version(), advertised.min_version);
        assert_eq!(C::max_version(), advertised.max_version);
    }

    #[test]
    fn codec_version_ranges_match_supported_versions() {
        assert_codec_matches_advertised::<ProduceCodec>(ApiKey::Produce);
        assert_codec_matches_advertised::<FetchCodec>(ApiKey::Fetch);
        assert_codec_matches_advertised::<ListOffsetsCodec>(ApiKey::ListOffsets);
        assert_codec_matches_advertised::<MetadataCodec>(ApiKey::Metadata);
        assert_codec_matches_advertised::<OffsetCommitCodec>(ApiKey::OffsetCommit);
        assert_codec_matches_advertised::<OffsetFetchCodec>(ApiKey::OffsetFetch);
        assert_codec_matches_advertised::<FindCoordinatorCodec>(ApiKey::FindCoordinator);
        assert_codec_matches_advertised::<JoinGroupCodec>(ApiKey::JoinGroup);
        assert_codec_matches_advertised::<HeartbeatCodec>(ApiKey::Heartbeat);
        assert_codec_matches_advertised::<LeaveGroupCodec>(ApiKey::LeaveGroup);
        assert_codec_matches_advertised::<SyncGroupCodec>(ApiKey::SyncGroup);
        assert_codec_matches_advertised::<DescribeGroupsCodec>(ApiKey::DescribeGroups);
        assert_codec_matches_advertised::<ListGroupsCodec>(ApiKey::ListGroups);
        assert_codec_matches_advertised::<DeleteGroupsCodec>(ApiKey::DeleteGroups);

        // Every advertised API that has a codec must agree on bounds.
        assert_eq!(
            SUPPORTED_VERSIONS
                .iter()
                .filter(|sv| {
                    matches!(
                        sv.api_key,
                        ApiKey::Produce
                            | ApiKey::Fetch
                            | ApiKey::ListOffsets
                            | ApiKey::Metadata
                            | ApiKey::OffsetCommit
                            | ApiKey::OffsetFetch
                            | ApiKey::FindCoordinator
                            | ApiKey::JoinGroup
                            | ApiKey::Heartbeat
                            | ApiKey::LeaveGroup
                            | ApiKey::SyncGroup
                            | ApiKey::DescribeGroups
                            | ApiKey::ListGroups
                            | ApiKey::DeleteGroups
                    )
                })
                .count(),
            14
        );
    }
}
