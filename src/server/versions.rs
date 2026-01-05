//! API version information for the Kafka protocol.
//!
//! This module centralizes all API version information, making it easier
//! to maintain and extend protocol version support.
//!
//! # Maintenance Notes
//!
//! The version numbers in `SUPPORTED_VERSIONS` are hardcoded based on the
//! Apache Kafka protocol specification. When updating to support newer
//! Kafka client versions:
//!
//! 1. Check the Kafka protocol documentation:
//!    <https://kafka.apache.org/protocol.html>
//!
//! 2. Compare with the official Kafka API versions:
//!    <https://github.com/apache/kafka/blob/trunk/clients/src/main/resources/common/message/>
//!
//! 3. Update `max_version` for APIs where we implement the new version's features
//!
//! 4. Update corresponding request/response parsing in `request/` and `response/`
//!
//! # Current Support Matrix
//!
//! | API | Min | Max | Notes |
//! |-----|-----|-----|-------|
//! | Produce | 0 | 3 | Basic produce, no transactions |
//! | Fetch | 0 | 4 | Basic fetch, no follower fetch |
//! | Metadata | 0 | 1 | Broker and topic metadata |
//! | ApiVersions | 0 | 2 | Protocol negotiation |
//!
//! See `SUPPORTED_VERSIONS` constant for full list.

use super::request::ApiKey;
use super::response::ApiVersionData;

/// Supported API version range for a specific API.
#[derive(Debug, Clone, Copy)]
pub struct SupportedVersion {
    /// The API key.
    pub api_key: ApiKey,
    /// Minimum supported version.
    pub min_version: i16,
    /// Maximum supported version.
    pub max_version: i16,
}

impl SupportedVersion {
    /// Create a new supported version entry.
    pub const fn new(api_key: ApiKey, min_version: i16, max_version: i16) -> Self {
        Self {
            api_key,
            min_version,
            max_version,
        }
    }

    /// Check if a specific version is supported.
    pub const fn supports(&self, version: i16) -> bool {
        version >= self.min_version && version <= self.max_version
    }

    /// Convert to ApiVersionData for protocol response.
    pub const fn to_api_version_data(&self) -> ApiVersionData {
        ApiVersionData {
            api_key: self.api_key,
            min_version: self.min_version,
            max_version: self.max_version,
        }
    }
}

/// Default supported API versions.
///
/// This is the standard set of API versions that Kafkaesque supports.
/// Handlers can override this in their ApiVersions response if needed.
pub const SUPPORTED_VERSIONS: &[SupportedVersion] = &[
    SupportedVersion::new(ApiKey::Produce, 0, 3),
    SupportedVersion::new(ApiKey::Fetch, 0, 4),
    SupportedVersion::new(ApiKey::ListOffsets, 0, 2),
    SupportedVersion::new(ApiKey::Metadata, 0, 1),
    SupportedVersion::new(ApiKey::OffsetCommit, 0, 2),
    SupportedVersion::new(ApiKey::OffsetFetch, 0, 1),
    SupportedVersion::new(ApiKey::FindCoordinator, 0, 1),
    SupportedVersion::new(ApiKey::JoinGroup, 0, 2),
    SupportedVersion::new(ApiKey::Heartbeat, 0, 1),
    SupportedVersion::new(ApiKey::LeaveGroup, 0, 1),
    SupportedVersion::new(ApiKey::SyncGroup, 0, 1),
    SupportedVersion::new(ApiKey::DescribeGroups, 0, 1),
    SupportedVersion::new(ApiKey::ListGroups, 0, 2),
    SupportedVersion::new(ApiKey::SaslHandshake, 0, 1),
    SupportedVersion::new(ApiKey::ApiVersions, 0, 3), // v3 uses flexible encoding
    SupportedVersion::new(ApiKey::CreateTopics, 0, 1),
    SupportedVersion::new(ApiKey::DeleteTopics, 0, 1),
    SupportedVersion::new(ApiKey::SaslAuthenticate, 0, 1),
    SupportedVersion::new(ApiKey::InitProducerId, 0, 4),
    SupportedVersion::new(ApiKey::DeleteGroups, 0, 1),
];

/// Get the default set of API version data for responses.
pub fn default_api_versions() -> Vec<ApiVersionData> {
    SUPPORTED_VERSIONS
        .iter()
        .map(|v| v.to_api_version_data())
        .collect()
}

/// Find the supported version info for a specific API key.
pub fn find_version(api_key: ApiKey) -> Option<&'static SupportedVersion> {
    SUPPORTED_VERSIONS.iter().find(|v| v.api_key == api_key)
}

/// Check if a specific API version is supported.
pub fn is_version_supported(api_key: ApiKey, version: i16) -> bool {
    find_version(api_key)
        .map(|v| v.supports(version))
        .unwrap_or(false)
}

/// Check if an API uses flexible encoding at the given version.
///
/// Flexible encoding was introduced in KIP-482 and affects wire format.
pub fn uses_flexible_encoding(api_key: ApiKey, version: i16) -> bool {
    match api_key {
        ApiKey::ApiVersions => version >= 3,
        ApiKey::InitProducerId => version >= 3,
        // Add other APIs as needed when flexible versions are implemented
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_supported_version_new() {
        let sv = SupportedVersion::new(ApiKey::Produce, 0, 5);
        assert_eq!(sv.api_key, ApiKey::Produce);
        assert_eq!(sv.min_version, 0);
        assert_eq!(sv.max_version, 5);
    }

    #[test]
    fn test_supported_version_supports() {
        let sv = SupportedVersion::new(ApiKey::Fetch, 1, 4);
        assert!(!sv.supports(0));
        assert!(sv.supports(1));
        assert!(sv.supports(2));
        assert!(sv.supports(3));
        assert!(sv.supports(4));
        assert!(!sv.supports(5));
    }

    #[test]
    fn test_supported_version_to_api_version_data() {
        let sv = SupportedVersion::new(ApiKey::Metadata, 0, 9);
        let data = sv.to_api_version_data();
        assert_eq!(data.api_key, ApiKey::Metadata);
        assert_eq!(data.min_version, 0);
        assert_eq!(data.max_version, 9);
    }

    #[test]
    fn test_default_api_versions() {
        let versions = default_api_versions();
        assert!(!versions.is_empty());
        assert_eq!(versions.len(), SUPPORTED_VERSIONS.len());

        // Verify produce is included
        let produce = versions.iter().find(|v| v.api_key == ApiKey::Produce);
        assert!(produce.is_some());
    }

    #[test]
    fn test_find_version() {
        let produce = find_version(ApiKey::Produce);
        assert!(produce.is_some());
        let produce = produce.unwrap();
        assert_eq!(produce.api_key, ApiKey::Produce);
        assert_eq!(produce.min_version, 0);
        assert_eq!(produce.max_version, 3);
    }

    #[test]
    fn test_find_version_not_found() {
        // Unknown API key should return None
        // Note: All ApiKey variants should be in SUPPORTED_VERSIONS for normal operation
        // This test verifies the logic works correctly
        let result = find_version(ApiKey::Unknown(-1));
        assert!(result.is_none());
    }

    #[test]
    fn test_is_version_supported() {
        assert!(is_version_supported(ApiKey::Produce, 0));
        assert!(is_version_supported(ApiKey::Produce, 3));
        assert!(!is_version_supported(ApiKey::Produce, 4));
        assert!(!is_version_supported(ApiKey::Produce, -1));
    }

    #[test]
    fn test_is_version_supported_unknown_api() {
        assert!(!is_version_supported(ApiKey::Unknown(-1), 0));
    }

    #[test]
    fn test_uses_flexible_encoding() {
        // ApiVersions uses flexible encoding from v3
        assert!(!uses_flexible_encoding(ApiKey::ApiVersions, 0));
        assert!(!uses_flexible_encoding(ApiKey::ApiVersions, 2));
        assert!(uses_flexible_encoding(ApiKey::ApiVersions, 3));

        // InitProducerId uses flexible encoding from v3
        assert!(!uses_flexible_encoding(ApiKey::InitProducerId, 0));
        assert!(!uses_flexible_encoding(ApiKey::InitProducerId, 2));
        assert!(uses_flexible_encoding(ApiKey::InitProducerId, 3));
        assert!(uses_flexible_encoding(ApiKey::InitProducerId, 4));
    }

    #[test]
    fn test_uses_flexible_encoding_other_apis() {
        // Most APIs don't use flexible encoding yet
        assert!(!uses_flexible_encoding(ApiKey::Produce, 0));
        assert!(!uses_flexible_encoding(ApiKey::Fetch, 4));
        assert!(!uses_flexible_encoding(ApiKey::Metadata, 1));
    }

    #[test]
    fn test_all_expected_apis_present() {
        // Verify all key APIs are present in SUPPORTED_VERSIONS
        let expected = [
            ApiKey::Produce,
            ApiKey::Fetch,
            ApiKey::ListOffsets,
            ApiKey::Metadata,
            ApiKey::OffsetCommit,
            ApiKey::OffsetFetch,
            ApiKey::FindCoordinator,
            ApiKey::JoinGroup,
            ApiKey::Heartbeat,
            ApiKey::LeaveGroup,
            ApiKey::SyncGroup,
            ApiKey::DescribeGroups,
            ApiKey::ListGroups,
            ApiKey::SaslHandshake,
            ApiKey::ApiVersions,
            ApiKey::CreateTopics,
            ApiKey::DeleteTopics,
            ApiKey::SaslAuthenticate,
            ApiKey::InitProducerId,
            ApiKey::DeleteGroups,
        ];

        for api in expected {
            assert!(find_version(api).is_some(), "Missing API: {:?}", api);
        }
    }

    #[test]
    fn test_supported_versions_is_sorted_conceptually() {
        // Verify version ranges are sensible (min <= max)
        for sv in SUPPORTED_VERSIONS {
            assert!(
                sv.min_version <= sv.max_version,
                "Invalid version range for {:?}: {} > {}",
                sv.api_key,
                sv.min_version,
                sv.max_version
            );
        }
    }
}
