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
//! Each row reflects what the parsers and encoders actually understand on the
//! wire. The `min_version` is set so that older clients get a clean
//! `UnsupportedVersion` from `ApiVersions` rather than a silent
//! mis-parse. Without this, the produce/fetch parsers (which read v3+/v4+
//! fields unconditionally) would trip a `ParsingError` on v0–v2 clients.
//!
//! | API              | Min | Max | Why min isn't 0                                |
//! |------------------|-----|-----|------------------------------------------------|
//! | Produce          | 3   | 3   | parser reads `transactional_id` (v3+)          |
//! | Fetch            | 4   | 4   | parser reads `max_bytes` (v3+) and `isolation` (v4+) |
//! | ListOffsets      | 0   | 2   | parser is version-agnostic                     |
//! | Metadata         | 0   | 1   | parser is version-agnostic                     |
//! | OffsetCommit     | 0   | 2   |                                                |
//! | OffsetFetch      | 0   | 1   |                                                |
//! | FindCoordinator  | 0   | 1   |                                                |
//! | JoinGroup        | 0   | 2   |                                                |
//! | Heartbeat        | 0   | 1   |                                                |
//! | LeaveGroup       | 0   | 1   |                                                |
//! | SyncGroup        | 0   | 1   |                                                |
//! | DescribeGroups   | 0   | 1   |                                                |
//! | ListGroups       | 0   | 2   |                                                |
//! | SaslHandshake    | 0   | 1   |                                                |
//! | ApiVersions      | 0   | 3   | v3 uses flexible encoding                      |
//! | CreateTopics     | 0   | 1   |                                                |
//! | DeleteTopics     | 0   | 1   |                                                |
//! | SaslAuthenticate | 0   | 1   |                                                |
//! | InitProducerId   | 0   | 4   | v2+ uses flexible encoding (per spec)          |
//! | DeleteGroups     | 0   | 1   |                                                |

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
/// The `min_version` for Produce and Fetch is clamped to match what the
/// parsers actually decode. Handlers may override this in their
/// ApiVersions response if needed.
pub const SUPPORTED_VERSIONS: &[SupportedVersion] = &[
    // Produce v3 added the leading nullable `transactional_id` field that the
    // parser reads unconditionally; refusing v0–v2 prevents silent misreads.
    SupportedVersion::new(ApiKey::Produce, 3, 3),
    // Fetch v3 added `max_bytes`, v4 added `isolation_level` — both are
    // unconditionally parsed today.
    SupportedVersion::new(ApiKey::Fetch, 4, 4),
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
    // DescribeConfigs / AlterConfigs: v0–v2. KafkaAdminClient defaults to
    // v2; v3+ adds incremental ops (Append/Subtract) we do not yet
    // implement. v1 added `include_synonyms`, v2 added `include_documentation`
    // — both are tolerated and ignored.
    SupportedVersion::new(ApiKey::DescribeConfigs, 0, 2),
    SupportedVersion::new(ApiKey::AlterConfigs, 0, 1),
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
        // ApiVersions: flexible from v3 (KIP-511 hybrid header).
        ApiKey::ApiVersions => version >= 3,
        // InitProducerId: flexible from v2 per the Kafka spec
        // (InitProducerIdRequest.json: "flexibleVersions": "2+").
        // This was previously (incorrectly) gated at v3, which mis-framed
        // every v2 request and response.
        ApiKey::InitProducerId => version >= 2,
        // Add other APIs as needed when flexible versions are implemented
        _ => false,
    }
}

/// What the parser/encoder layer actually decodes and emits for each API.
///
/// This is the WIRE-FORMAT coverage. `SUPPORTED_VERSIONS` is what the broker
/// ADVERTISES via `ApiVersions`. The two must agree — drift produces silent
/// mis-parses (advertise more than parser handles) or invisible API support
/// (parser handles more than advertised). The unit test
/// `parser_coverage_matches_advertised` asserts equality at every position.
///
/// To add or expand version support:
/// 1. Implement the new wire format in `request/{api}.rs` and `response/{api}.rs`.
/// 2. Bump the entry in `PARSER_ENCODER_COVERAGE` to match.
/// 3. Bump the matching entry in `SUPPORTED_VERSIONS`.
/// 4. The unit test ensures all three stay in lockstep.
pub const PARSER_ENCODER_COVERAGE: &[(ApiKey, i16, i16)] = &[
    (ApiKey::Produce, 3, 3),
    (ApiKey::Fetch, 4, 4),
    (ApiKey::ListOffsets, 0, 2),
    (ApiKey::Metadata, 0, 1),
    (ApiKey::OffsetCommit, 0, 2),
    (ApiKey::OffsetFetch, 0, 1),
    (ApiKey::FindCoordinator, 0, 1),
    (ApiKey::JoinGroup, 0, 2),
    (ApiKey::Heartbeat, 0, 1),
    (ApiKey::LeaveGroup, 0, 1),
    (ApiKey::SyncGroup, 0, 1),
    (ApiKey::DescribeGroups, 0, 1),
    (ApiKey::ListGroups, 0, 2),
    (ApiKey::SaslHandshake, 0, 1),
    (ApiKey::ApiVersions, 0, 3),
    (ApiKey::CreateTopics, 0, 1),
    (ApiKey::DeleteTopics, 0, 1),
    (ApiKey::SaslAuthenticate, 0, 1),
    (ApiKey::InitProducerId, 0, 4),
    (ApiKey::DeleteGroups, 0, 1),
    (ApiKey::DescribeConfigs, 0, 2),
    (ApiKey::AlterConfigs, 0, 1),
];

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
        // Produce min is clamped to what the parser actually supports.
        assert_eq!(produce.min_version, 3);
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
        assert!(is_version_supported(ApiKey::Produce, 3));
        assert!(!is_version_supported(ApiKey::Produce, 0));
        assert!(!is_version_supported(ApiKey::Produce, 2));
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

        // InitProducerId uses flexible encoding from v2 (per spec)
        assert!(!uses_flexible_encoding(ApiKey::InitProducerId, 0));
        assert!(!uses_flexible_encoding(ApiKey::InitProducerId, 1));
        assert!(uses_flexible_encoding(ApiKey::InitProducerId, 2));
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

    /// Single-source-of-truth check: the advertised version range must match
    /// what the parser/encoder layer actually handles. Drift is silent
    /// otherwise — advertise too much and clients trip ParsingError; too
    /// little and the support hides.
    #[test]
    fn parser_coverage_matches_advertised() {
        assert_eq!(
            SUPPORTED_VERSIONS.len(),
            PARSER_ENCODER_COVERAGE.len(),
            "SUPPORTED_VERSIONS and PARSER_ENCODER_COVERAGE must list the same APIs"
        );
        for sv in SUPPORTED_VERSIONS {
            let coverage = PARSER_ENCODER_COVERAGE
                .iter()
                .find(|(k, _, _)| *k == sv.api_key)
                .unwrap_or_else(|| {
                    panic!("PARSER_ENCODER_COVERAGE missing entry for {:?}", sv.api_key)
                });
            assert_eq!(
                (sv.min_version, sv.max_version),
                (coverage.1, coverage.2),
                "{:?}: advertised {:?} != parser/encoder coverage {:?}",
                sv.api_key,
                (sv.min_version, sv.max_version),
                (coverage.1, coverage.2),
            );
        }
        for (api_key, _, _) in PARSER_ENCODER_COVERAGE {
            assert!(
                SUPPORTED_VERSIONS.iter().any(|sv| sv.api_key == *api_key),
                "PARSER_ENCODER_COVERAGE has {:?} but SUPPORTED_VERSIONS does not",
                api_key
            );
        }
    }
}
