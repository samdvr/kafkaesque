//! Client compatibility matrix — advertised group API ceilings.
//!
//! The README "Client compatibility" section and this file must agree
//! with `SUPPORTED_VERSIONS`. Modern clients that honor ApiVersions
//! negotiate down to these ceilings; forcing a higher version must
//! yield `UnsupportedVersion` without closing the connection.
//!
//! This is the executable form of the promise that we either raise a
//! group API for real or explicitly refuse it — never silently accept
//! a half-parsed higher version.

use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::ApiKey;
use kafkaesque::server::versions::{SUPPORTED_VERSIONS, find_version, is_version_supported};

/// Documented group-API ceilings from README / CHANGELOG. If you bump
/// `SUPPORTED_VERSIONS`, update this table and the README in the same PR.
const GROUP_API_CEILINGS: &[(ApiKey, i16, i16)] = &[
    (ApiKey::OffsetCommit, 0, 6),
    (ApiKey::OffsetFetch, 0, 5),
    (ApiKey::FindCoordinator, 0, 2),
    (ApiKey::JoinGroup, 0, 4),
    (ApiKey::Heartbeat, 0, 3),
    (ApiKey::LeaveGroup, 0, 3),
    (ApiKey::SyncGroup, 0, 3),
    (ApiKey::DescribeGroups, 0, 1),
    (ApiKey::ListGroups, 0, 2),
    (ApiKey::DeleteGroups, 0, 1),
];

/// Versions modern clients commonly want that we deliberately do **not**
/// advertise. Each must remain unsupported so ApiVersions negotiation
/// (or UnsupportedVersion) is the refusal path.
const EXPLICITLY_REFUSED_MODERN_GROUP_VERSIONS: &[(ApiKey, i16)] = &[
    // KIP-345 static membership / flexible group APIs
    (ApiKey::JoinGroup, 5),
    (ApiKey::JoinGroup, 6),
    (ApiKey::Heartbeat, 4),
    (ApiKey::SyncGroup, 4),
    (ApiKey::LeaveGroup, 4),
    // OffsetCommit / OffsetFetch past classic non-flexible comfort zone
    (ApiKey::OffsetCommit, 7),
    (ApiKey::OffsetCommit, 8),
    (ApiKey::OffsetFetch, 6),
    (ApiKey::OffsetFetch, 8),
    (ApiKey::FindCoordinator, 3),
];

#[test]
fn documented_group_ceilings_match_supported_versions() {
    for (api, min, max) in GROUP_API_CEILINGS {
        let sv = find_version(*api).unwrap_or_else(|| panic!("missing {api:?}"));
        assert_eq!(
            (sv.min_version, sv.max_version),
            (*min, *max),
            "{api:?}: client-compat matrix {min}..={max} != advertised {}..={}",
            sv.min_version,
            sv.max_version
        );
    }
}

#[test]
fn modern_group_versions_are_explicitly_unsupported() {
    for (api, version) in EXPLICITLY_REFUSED_MODERN_GROUP_VERSIONS {
        assert!(
            !is_version_supported(*api, *version),
            "{api:?} v{version} must stay unsupported until implemented for real \
             (raise the ceiling in versions.rs + CHANGELOG + README together)"
        );
    }
}

#[test]
fn raised_classic_group_versions_are_supported() {
    assert!(is_version_supported(ApiKey::OffsetCommit, 5));
    assert!(is_version_supported(ApiKey::OffsetCommit, 6));
    assert!(is_version_supported(ApiKey::OffsetFetch, 5));
    assert!(is_version_supported(ApiKey::JoinGroup, 3));
    assert!(is_version_supported(ApiKey::JoinGroup, 4));
    assert!(is_version_supported(ApiKey::Heartbeat, 3));
    assert!(is_version_supported(ApiKey::SyncGroup, 3));
    assert!(is_version_supported(ApiKey::LeaveGroup, 3));
    assert!(is_version_supported(ApiKey::FindCoordinator, 2));
}

#[test]
fn join_group_v5_and_above_remain_refused() {
    for v in 5..=9 {
        assert!(
            !is_version_supported(ApiKey::JoinGroup, v),
            "JoinGroup v{v} must not be advertised (static membership)"
        );
    }
}

#[test]
fn unsupported_version_error_code_is_stable() {
    assert_eq!(KafkaCode::UnsupportedVersion as i16, 35);
}

#[test]
fn member_id_required_error_code_is_stable() {
    assert_eq!(KafkaCode::MemberIdRequired as i16, 79);
}

#[test]
fn matrix_apis_are_present_in_supported_versions() {
    for (api, _, _) in GROUP_API_CEILINGS {
        assert!(
            SUPPORTED_VERSIONS.iter().any(|sv| sv.api_key == *api),
            "{api:?} missing from SUPPORTED_VERSIONS"
        );
    }
}
