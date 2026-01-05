//! Request parsing for incoming Kafka protocol messages.
//!
//! This module provides parsers for Kafka request types, which is the reverse
//! of what the client-side protocol module does (encoding requests).

mod admin;
mod auth;
mod fetch;
mod groups;
mod metadata;
mod offsets;
mod produce;
mod versions;

use bytes::Bytes;
use nom::{
    IResult,
    number::complete::{be_i16, be_i32},
};
use nombytes::NomBytes;

use crate::error::{Error, Result};
use crate::parser::{bytes_to_string_opt, parse_nullable_string, skip_tagged_fields};

// Re-export all request data types
pub use admin::*;
pub use auth::*;
pub use fetch::*;
pub use groups::*;
pub use metadata::*;
pub use offsets::*;
pub use produce::*;
pub use versions::*;

/// API keys for Kafka protocol
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(i16)]
pub enum ApiKey {
    Produce = 0,
    Fetch = 1,
    ListOffsets = 2,
    Metadata = 3,
    LeaderAndIsr = 4,
    StopReplica = 5,
    UpdateMetadata = 6,
    ControlledShutdown = 7,
    OffsetCommit = 8,
    OffsetFetch = 9,
    FindCoordinator = 10,
    JoinGroup = 11,
    Heartbeat = 12,
    LeaveGroup = 13,
    SyncGroup = 14,
    DescribeGroups = 15,
    ListGroups = 16,
    SaslHandshake = 17,
    ApiVersions = 18,
    CreateTopics = 19,
    DeleteTopics = 20,
    InitProducerId = 22,
    SaslAuthenticate = 36,
    DeleteGroups = 42,
    Unknown(i16),
}

impl From<i16> for ApiKey {
    fn from(value: i16) -> Self {
        match value {
            0 => ApiKey::Produce,
            1 => ApiKey::Fetch,
            2 => ApiKey::ListOffsets,
            3 => ApiKey::Metadata,
            4 => ApiKey::LeaderAndIsr,
            5 => ApiKey::StopReplica,
            6 => ApiKey::UpdateMetadata,
            7 => ApiKey::ControlledShutdown,
            8 => ApiKey::OffsetCommit,
            9 => ApiKey::OffsetFetch,
            10 => ApiKey::FindCoordinator,
            11 => ApiKey::JoinGroup,
            12 => ApiKey::Heartbeat,
            13 => ApiKey::LeaveGroup,
            14 => ApiKey::SyncGroup,
            15 => ApiKey::DescribeGroups,
            16 => ApiKey::ListGroups,
            17 => ApiKey::SaslHandshake,
            18 => ApiKey::ApiVersions,
            19 => ApiKey::CreateTopics,
            20 => ApiKey::DeleteTopics,
            22 => ApiKey::InitProducerId,
            36 => ApiKey::SaslAuthenticate,
            42 => ApiKey::DeleteGroups,
            n => ApiKey::Unknown(n),
        }
    }
}

impl From<ApiKey> for i16 {
    fn from(key: ApiKey) -> Self {
        match key {
            ApiKey::Produce => 0,
            ApiKey::Fetch => 1,
            ApiKey::ListOffsets => 2,
            ApiKey::Metadata => 3,
            ApiKey::LeaderAndIsr => 4,
            ApiKey::StopReplica => 5,
            ApiKey::UpdateMetadata => 6,
            ApiKey::ControlledShutdown => 7,
            ApiKey::OffsetCommit => 8,
            ApiKey::OffsetFetch => 9,
            ApiKey::FindCoordinator => 10,
            ApiKey::JoinGroup => 11,
            ApiKey::Heartbeat => 12,
            ApiKey::LeaveGroup => 13,
            ApiKey::SyncGroup => 14,
            ApiKey::DescribeGroups => 15,
            ApiKey::ListGroups => 16,
            ApiKey::SaslHandshake => 17,
            ApiKey::ApiVersions => 18,
            ApiKey::CreateTopics => 19,
            ApiKey::DeleteTopics => 20,
            ApiKey::InitProducerId => 22,
            ApiKey::SaslAuthenticate => 36,
            ApiKey::DeleteGroups => 42,
            ApiKey::Unknown(n) => n,
        }
    }
}

impl ApiKey {
    /// Returns a static string name for this API key.
    ///
    /// This avoids allocating a new String on every request for metrics.
    /// For Unknown variants, returns "unknown" (not the numeric value).
    #[inline]
    pub fn as_str(&self) -> &'static str {
        match self {
            ApiKey::Produce => "Produce",
            ApiKey::Fetch => "Fetch",
            ApiKey::ListOffsets => "ListOffsets",
            ApiKey::Metadata => "Metadata",
            ApiKey::LeaderAndIsr => "LeaderAndIsr",
            ApiKey::StopReplica => "StopReplica",
            ApiKey::UpdateMetadata => "UpdateMetadata",
            ApiKey::ControlledShutdown => "ControlledShutdown",
            ApiKey::OffsetCommit => "OffsetCommit",
            ApiKey::OffsetFetch => "OffsetFetch",
            ApiKey::FindCoordinator => "FindCoordinator",
            ApiKey::JoinGroup => "JoinGroup",
            ApiKey::Heartbeat => "Heartbeat",
            ApiKey::LeaveGroup => "LeaveGroup",
            ApiKey::SyncGroup => "SyncGroup",
            ApiKey::DescribeGroups => "DescribeGroups",
            ApiKey::ListGroups => "ListGroups",
            ApiKey::SaslHandshake => "SaslHandshake",
            ApiKey::ApiVersions => "ApiVersions",
            ApiKey::CreateTopics => "CreateTopics",
            ApiKey::DeleteTopics => "DeleteTopics",
            ApiKey::InitProducerId => "InitProducerId",
            ApiKey::SaslAuthenticate => "SaslAuthenticate",
            ApiKey::DeleteGroups => "DeleteGroups",
            ApiKey::Unknown(_) => "Unknown",
        }
    }

    /// Returns the response encoding style for this API.
    ///
    /// Most APIs use Standard encoding, but some newer APIs (like InitProducerId)
    /// use Flexible encoding which includes tagged fields in the header.
    ///
    /// This method centralizes the style mapping to avoid manual specification
    /// in request dispatch code, reducing the risk of errors.
    ///
    /// Note: Currently the dispatch code in connection.rs uses hardcoded style
    /// selection. This method is preserved for future refactoring to use a
    /// unified dispatch pattern.
    #[inline]
    #[allow(dead_code)]
    pub(crate) fn response_style(&self) -> super::connection::ResponseStyle {
        use super::connection::ResponseStyle;
        match self {
            // InitProducerId uses flexible encoding (v3+)
            ApiKey::InitProducerId => ResponseStyle::Flexible,
            // All other APIs use standard encoding
            _ => ResponseStyle::Standard,
        }
    }
}

/// Parsed request header from incoming Kafka messages.
#[derive(Debug, Clone)]
pub struct RequestHeader {
    pub api_key: ApiKey,
    pub api_version: i16,
    pub correlation_id: i32,
    pub client_id: Option<String>,
}

/// Parse a request header from bytes.
/// Note: ApiVersions v3+ uses a hybrid format per KIP-511:
/// - client_id uses standard encoding (i16 length, not varint)
/// - but an empty tagged_fields section follows client_id
pub fn parse_request_header(s: NomBytes) -> IResult<NomBytes, RequestHeader> {
    let (s, api_key) = be_i16(s)?;
    let (s, api_version) = be_i16(s)?;
    let (s, correlation_id) = be_i32(s)?;

    // Always parse client_id as standard nullable string (i16 length)
    // This is correct even for ApiVersions v3+ per KIP-511
    let (s, client_id) = parse_nullable_string(s)?;

    // ApiVersions v3+ adds empty tagged_fields after client_id
    // (hybrid format: standard client_id encoding + tagged_fields)
    let s = if api_key == 18 && api_version >= 3 {
        let (s, _) = skip_tagged_fields(s)?;
        s
    } else {
        s
    };

    let client_id = bytes_to_string_opt(client_id)?;

    Ok((
        s,
        RequestHeader {
            api_key: ApiKey::from(api_key),
            api_version,
            correlation_id,
            client_id,
        },
    ))
}

/// Parsed Kafka request with header and body.
#[derive(Debug)]
pub enum Request {
    Produce(RequestHeader, ProduceRequestData),
    Fetch(RequestHeader, FetchRequestData),
    ListOffsets(RequestHeader, ListOffsetsRequestData),
    Metadata(RequestHeader, MetadataRequestData),
    OffsetCommit(RequestHeader, OffsetCommitRequestData),
    OffsetFetch(RequestHeader, OffsetFetchRequestData),
    FindCoordinator(RequestHeader, FindCoordinatorRequestData),
    JoinGroup(RequestHeader, JoinGroupRequestData),
    Heartbeat(RequestHeader, HeartbeatRequestData),
    LeaveGroup(RequestHeader, LeaveGroupRequestData),
    SyncGroup(RequestHeader, SyncGroupRequestData),
    DescribeGroups(RequestHeader, DescribeGroupsRequestData),
    ListGroups(RequestHeader, ListGroupsRequestData),
    SaslHandshake(RequestHeader, SaslHandshakeRequestData),
    SaslAuthenticate(RequestHeader, SaslAuthenticateRequestData),
    ApiVersions(RequestHeader, ApiVersionsRequestData),
    CreateTopics(RequestHeader, CreateTopicsRequestData),
    DeleteTopics(RequestHeader, DeleteTopicsRequestData),
    InitProducerId(RequestHeader, InitProducerIdRequestData),
    DeleteGroups(RequestHeader, DeleteGroupsRequestData),
    Unknown(RequestHeader, Bytes),
}

impl Request {
    /// Get the request header.
    pub fn header(&self) -> &RequestHeader {
        match self {
            Request::Produce(h, _) => h,
            Request::Fetch(h, _) => h,
            Request::ListOffsets(h, _) => h,
            Request::Metadata(h, _) => h,
            Request::OffsetCommit(h, _) => h,
            Request::OffsetFetch(h, _) => h,
            Request::FindCoordinator(h, _) => h,
            Request::JoinGroup(h, _) => h,
            Request::Heartbeat(h, _) => h,
            Request::LeaveGroup(h, _) => h,
            Request::SyncGroup(h, _) => h,
            Request::DescribeGroups(h, _) => h,
            Request::ListGroups(h, _) => h,
            Request::SaslHandshake(h, _) => h,
            Request::SaslAuthenticate(h, _) => h,
            Request::ApiVersions(h, _) => h,
            Request::CreateTopics(h, _) => h,
            Request::DeleteTopics(h, _) => h,
            Request::InitProducerId(h, _) => h,
            Request::DeleteGroups(h, _) => h,
            Request::Unknown(h, _) => h,
        }
    }

    /// Parse a request from raw bytes.
    pub fn parse(data: Bytes) -> Result<Self> {
        let input = NomBytes::new(data.clone());
        let (remaining, header) =
            parse_request_header(input).map_err(|_| Error::ParsingError(data.clone()))?;

        match header.api_key {
            ApiKey::Produce => {
                let (_, body) = produce::parse_produce_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::Produce(header, body))
            }
            ApiKey::Fetch => {
                let (_, body) = fetch::parse_fetch_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::Fetch(header, body))
            }
            ApiKey::ListOffsets => {
                let (_, body) = offsets::parse_list_offsets_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::ListOffsets(header, body))
            }
            ApiKey::Metadata => {
                let (_, body) = metadata::parse_metadata_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::Metadata(header, body))
            }
            ApiKey::OffsetCommit => {
                let (_, body) = offsets::parse_offset_commit_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::OffsetCommit(header, body))
            }
            ApiKey::OffsetFetch => {
                let (_, body) = offsets::parse_offset_fetch_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::OffsetFetch(header, body))
            }
            ApiKey::FindCoordinator => {
                let (_, body) =
                    groups::parse_find_coordinator_request(remaining, header.api_version)
                        .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::FindCoordinator(header, body))
            }
            ApiKey::JoinGroup => {
                let (_, body) = groups::parse_join_group_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::JoinGroup(header, body))
            }
            ApiKey::Heartbeat => {
                let (_, body) = groups::parse_heartbeat_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::Heartbeat(header, body))
            }
            ApiKey::LeaveGroup => {
                let (_, body) = groups::parse_leave_group_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::LeaveGroup(header, body))
            }
            ApiKey::SyncGroup => {
                let (_, body) = groups::parse_sync_group_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::SyncGroup(header, body))
            }
            ApiKey::SaslHandshake => {
                let (_, body) = auth::parse_sasl_handshake_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::SaslHandshake(header, body))
            }
            ApiKey::SaslAuthenticate => {
                let (_, body) =
                    auth::parse_sasl_authenticate_request(remaining, header.api_version)
                        .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::SaslAuthenticate(header, body))
            }
            ApiKey::ApiVersions => {
                let (_, body) = versions::parse_api_versions_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::ApiVersions(header, body))
            }
            ApiKey::CreateTopics => {
                let (_, body) = admin::parse_create_topics_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::CreateTopics(header, body))
            }
            ApiKey::DeleteTopics => {
                let (_, body) = admin::parse_delete_topics_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::DeleteTopics(header, body))
            }
            ApiKey::InitProducerId => {
                let (_, body) =
                    admin::parse_init_producer_id_request(remaining, header.api_version)
                        .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::InitProducerId(header, body))
            }
            ApiKey::DescribeGroups => {
                let (_, body) =
                    groups::parse_describe_groups_request(remaining, header.api_version)
                        .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::DescribeGroups(header, body))
            }
            ApiKey::ListGroups => {
                let (_, body) = groups::parse_list_groups_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::ListGroups(header, body))
            }
            ApiKey::DeleteGroups => {
                let (_, body) = groups::parse_delete_groups_request(remaining, header.api_version)
                    .map_err(|_| Error::ParsingError(data))?;
                Ok(Request::DeleteGroups(header, body))
            }
            _ => Ok(Request::Unknown(header, remaining.into_bytes())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to build a request header in wire format
    fn build_header(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&api_key.to_be_bytes());
        data.extend_from_slice(&api_version.to_be_bytes());
        data.extend_from_slice(&correlation_id.to_be_bytes());
        match client_id {
            Some(s) => {
                data.extend_from_slice(&(s.len() as i16).to_be_bytes());
                data.extend_from_slice(s.as_bytes());
            }
            None => {
                data.extend_from_slice(&(-1i16).to_be_bytes());
            }
        }
        data
    }

    #[test]
    fn test_api_key_from_i16() {
        assert_eq!(ApiKey::from(0), ApiKey::Produce);
        assert_eq!(ApiKey::from(1), ApiKey::Fetch);
        assert_eq!(ApiKey::from(2), ApiKey::ListOffsets);
        assert_eq!(ApiKey::from(3), ApiKey::Metadata);
        assert_eq!(ApiKey::from(10), ApiKey::FindCoordinator);
        assert_eq!(ApiKey::from(18), ApiKey::ApiVersions);
        assert_eq!(ApiKey::from(36), ApiKey::SaslAuthenticate);
        assert_eq!(ApiKey::from(999), ApiKey::Unknown(999));
    }

    #[test]
    fn test_api_key_to_i16() {
        assert_eq!(i16::from(ApiKey::Produce), 0);
        assert_eq!(i16::from(ApiKey::Fetch), 1);
        assert_eq!(i16::from(ApiKey::Metadata), 3);
        assert_eq!(i16::from(ApiKey::ApiVersions), 18);
        assert_eq!(i16::from(ApiKey::Unknown(42)), 42);
    }

    #[test]
    fn test_api_key_roundtrip() {
        for i in 0..=40 {
            let key = ApiKey::from(i);
            let back = i16::from(key);
            assert_eq!(back, i);
        }
    }

    #[test]
    fn test_parse_request_header() {
        let data = build_header(18, 2, 12345, Some("test-client"));
        let input = NomBytes::new(Bytes::from(data));
        let (_, header) = parse_request_header(input).unwrap();

        assert_eq!(header.api_key, ApiKey::ApiVersions);
        assert_eq!(header.api_version, 2);
        assert_eq!(header.correlation_id, 12345);
        assert_eq!(header.client_id, Some("test-client".to_string()));
    }

    #[test]
    fn test_parse_request_header_null_client_id() {
        let data = build_header(3, 5, 999, None);
        let input = NomBytes::new(Bytes::from(data));
        let (_, header) = parse_request_header(input).unwrap();

        assert_eq!(header.api_key, ApiKey::Metadata);
        assert_eq!(header.api_version, 5);
        assert_eq!(header.correlation_id, 999);
        assert_eq!(header.client_id, None);
    }

    #[test]
    fn test_parse_api_versions_request() {
        // ApiVersions has no body
        let data = build_header(18, 0, 1, Some("client"));
        let request = Request::parse(Bytes::from(data.clone())).unwrap();

        match request {
            Request::ApiVersions(header, _body) => {
                assert_eq!(header.api_key, ApiKey::ApiVersions);
                assert_eq!(header.correlation_id, 1);
            }
            _ => panic!("Expected ApiVersions request"),
        }
    }

    /// Helper to build a flexible request header (ApiVersions v3+) in wire format.
    /// Per KIP-511, ApiVersions v3 uses a HYBRID format:
    /// - client_id uses STANDARD encoding (i16 length, NOT varint)
    /// - empty tagged_fields follow client_id
    fn build_flexible_header(
        api_key: i16,
        api_version: i16,
        correlation_id: i32,
        client_id: Option<&str>,
    ) -> Vec<u8> {
        let mut data = Vec::new();
        data.extend_from_slice(&api_key.to_be_bytes());
        data.extend_from_slice(&api_version.to_be_bytes());
        data.extend_from_slice(&correlation_id.to_be_bytes());
        // Standard nullable string for client_id (i16 length, per KIP-511)
        match client_id {
            Some(s) => {
                data.extend_from_slice(&(s.len() as i16).to_be_bytes());
                data.extend_from_slice(s.as_bytes());
            }
            None => {
                data.extend_from_slice(&(-1i16).to_be_bytes());
            }
        }
        // Empty tagged fields
        data.push(0);
        data
    }

    #[test]
    fn test_parse_api_versions_request_v3_flexible() {
        // ApiVersions v3 uses flexible encoding
        // Header: api_key=18, api_version=3, correlation_id=42, client_id="rdkafka", tagged_fields=[]
        // Body: client_software_name="", client_software_version="", tagged_fields=[]
        let mut data = build_flexible_header(18, 3, 42, Some("rdkafka"));
        // Body: empty client_software_name (compact string = 1 means empty)
        data.push(0x01); // empty string
        // Body: empty client_software_version
        data.push(0x01); // empty string
        // Body: empty tagged fields
        data.push(0x00);

        let request = Request::parse(Bytes::from(data)).unwrap();

        match request {
            Request::ApiVersions(header, body) => {
                assert_eq!(header.api_key, ApiKey::ApiVersions);
                assert_eq!(header.api_version, 3);
                assert_eq!(header.correlation_id, 42);
                assert_eq!(header.client_id, Some("rdkafka".to_string()));
                // Body fields should be empty strings
                assert_eq!(body.client_software_name, Some("".to_string()));
                assert_eq!(body.client_software_version, Some("".to_string()));
            }
            _ => panic!("Expected ApiVersions request"),
        }
    }

    #[test]
    fn test_parse_api_versions_request_v3_with_software_info() {
        // ApiVersions v3 with client software info
        let mut data = build_flexible_header(18, 3, 100, Some("test-client"));
        // Body: client_software_name = "librdkafka" (length+1 = 11)
        data.push(0x0b); // 11 = length 10 + 1
        data.extend_from_slice(b"librdkafka");
        // Body: client_software_version = "2.3.0" (length+1 = 6)
        data.push(0x06); // 6 = length 5 + 1
        data.extend_from_slice(b"2.3.0");
        // Body: empty tagged fields
        data.push(0x00);

        let request = Request::parse(Bytes::from(data)).unwrap();

        match request {
            Request::ApiVersions(header, body) => {
                assert_eq!(header.api_key, ApiKey::ApiVersions);
                assert_eq!(header.api_version, 3);
                assert_eq!(header.correlation_id, 100);
                assert_eq!(header.client_id, Some("test-client".to_string()));
                assert_eq!(body.client_software_name, Some("librdkafka".to_string()));
                assert_eq!(body.client_software_version, Some("2.3.0".to_string()));
            }
            _ => panic!("Expected ApiVersions request"),
        }
    }

    #[test]
    fn test_parse_api_versions_request_v3_null_client_id() {
        // ApiVersions v3 with null client_id
        let mut data = build_flexible_header(18, 3, 200, None);
        // Body: null client_software_name
        data.push(0x00);
        // Body: null client_software_version
        data.push(0x00);
        // Body: empty tagged fields
        data.push(0x00);

        let request = Request::parse(Bytes::from(data)).unwrap();

        match request {
            Request::ApiVersions(header, body) => {
                assert_eq!(header.api_key, ApiKey::ApiVersions);
                assert_eq!(header.api_version, 3);
                assert_eq!(header.correlation_id, 200);
                assert_eq!(header.client_id, None);
                assert_eq!(body.client_software_name, None);
                assert_eq!(body.client_software_version, None);
            }
            _ => panic!("Expected ApiVersions request"),
        }
    }

    #[test]
    fn test_parse_metadata_request_all_topics() {
        // Metadata request with topic_count = -1 (all topics)
        let mut data = build_header(3, 1, 100, Some("meta-client"));
        data.extend_from_slice(&(-1i32).to_be_bytes()); // All topics

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::Metadata(header, body) => {
                assert_eq!(header.correlation_id, 100);
                assert!(body.topics.is_none());
            }
            _ => panic!("Expected Metadata request"),
        }
    }

    #[test]
    fn test_parse_metadata_request_specific_topics() {
        // Metadata request for specific topics
        let mut data = build_header(3, 1, 200, Some("client"));
        data.extend_from_slice(&2i32.to_be_bytes()); // 2 topics
        // Topic 1: "foo"
        data.extend_from_slice(&3u16.to_be_bytes());
        data.extend_from_slice(b"foo");
        // Topic 2: "bar"
        data.extend_from_slice(&3u16.to_be_bytes());
        data.extend_from_slice(b"bar");

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::Metadata(_, body) => {
                let topics = body.topics.unwrap();
                assert_eq!(topics.len(), 2);
                assert_eq!(topics[0], "foo");
                assert_eq!(topics[1], "bar");
            }
            _ => panic!("Expected Metadata request"),
        }
    }

    #[test]
    fn test_parse_find_coordinator_request() {
        let mut data = build_header(10, 0, 300, None);
        // Key: "my-group"
        data.extend_from_slice(&8u16.to_be_bytes());
        data.extend_from_slice(b"my-group");

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::FindCoordinator(header, body) => {
                assert_eq!(header.correlation_id, 300);
                assert_eq!(body.key, "my-group");
                assert_eq!(body.key_type, 0); // default for v0
            }
            _ => panic!("Expected FindCoordinator request"),
        }
    }

    #[test]
    fn test_parse_heartbeat_request() {
        let mut data = build_header(12, 0, 400, Some("hb-client"));
        // group_id: "grp"
        data.extend_from_slice(&3u16.to_be_bytes());
        data.extend_from_slice(b"grp");
        // generation_id: 5
        data.extend_from_slice(&5i32.to_be_bytes());
        // member_id: "mem-1"
        data.extend_from_slice(&5u16.to_be_bytes());
        data.extend_from_slice(b"mem-1");

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::Heartbeat(_, body) => {
                assert_eq!(body.group_id, "grp");
                assert_eq!(body.generation_id, 5);
                assert_eq!(body.member_id, "mem-1");
            }
            _ => panic!("Expected Heartbeat request"),
        }
    }

    #[test]
    fn test_parse_leave_group_request() {
        let mut data = build_header(13, 0, 500, None);
        // group_id: "lg"
        data.extend_from_slice(&2u16.to_be_bytes());
        data.extend_from_slice(b"lg");
        // member_id: "m"
        data.extend_from_slice(&1u16.to_be_bytes());
        data.extend_from_slice(b"m");

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::LeaveGroup(_, body) => {
                assert_eq!(body.group_id, "lg");
                assert_eq!(body.member_id, "m");
            }
            _ => panic!("Expected LeaveGroup request"),
        }
    }

    #[test]
    fn test_parse_sasl_handshake_request() {
        let mut data = build_header(17, 0, 600, None);
        // mechanism: "PLAIN"
        data.extend_from_slice(&5u16.to_be_bytes());
        data.extend_from_slice(b"PLAIN");

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::SaslHandshake(_, body) => {
                assert_eq!(body.mechanism, "PLAIN");
            }
            _ => panic!("Expected SaslHandshake request"),
        }
    }

    #[test]
    fn test_parse_sasl_authenticate_request() {
        let mut data = build_header(36, 0, 700, None);
        // auth_bytes: [1, 2, 3]
        data.extend_from_slice(&3i32.to_be_bytes());
        data.extend_from_slice(&[1, 2, 3]);

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::SaslAuthenticate(_, body) => {
                assert_eq!(body.auth_bytes.as_ref(), &[1, 2, 3]);
            }
            _ => panic!("Expected SaslAuthenticate request"),
        }
    }

    #[test]
    fn test_parse_unknown_api() {
        // Use an unknown API key
        let data = build_header(99, 0, 800, None);
        let request = Request::parse(Bytes::from(data)).unwrap();

        match request {
            Request::Unknown(header, _) => {
                assert_eq!(header.api_key, ApiKey::Unknown(99));
                assert_eq!(header.correlation_id, 800);
            }
            _ => panic!("Expected Unknown request"),
        }
    }

    #[test]
    fn test_request_header_accessor() {
        let data = build_header(18, 1, 42, Some("x"));
        let request = Request::parse(Bytes::from(data)).unwrap();
        let header = request.header();
        assert_eq!(header.correlation_id, 42);
    }

    #[test]
    fn test_request_header_clone() {
        let header = RequestHeader {
            api_key: ApiKey::Fetch,
            api_version: 3,
            correlation_id: 999,
            client_id: Some("test".to_string()),
        };
        let cloned = header.clone();
        assert_eq!(cloned.api_key, header.api_key);
        assert_eq!(cloned.correlation_id, header.correlation_id);
    }

    #[test]
    fn test_parse_delete_topics_request() {
        let mut data = build_header(20, 0, 900, None);
        // 2 topic names
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(&4u16.to_be_bytes());
        data.extend_from_slice(b"top1");
        data.extend_from_slice(&4u16.to_be_bytes());
        data.extend_from_slice(b"top2");
        // timeout_ms
        data.extend_from_slice(&5000i32.to_be_bytes());

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::DeleteTopics(_, body) => {
                assert_eq!(body.topic_names.len(), 2);
                assert_eq!(body.topic_names[0], "top1");
                assert_eq!(body.topic_names[1], "top2");
                assert_eq!(body.timeout_ms, 5000);
            }
            _ => panic!("Expected DeleteTopics request"),
        }
    }

    #[test]
    fn test_parse_describe_groups_request() {
        // DescribeGroups (API key 15) with 2 group IDs
        let mut data = build_header(15, 0, 1000, Some("admin-client"));
        // 2 group IDs
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(&8u16.to_be_bytes());
        data.extend_from_slice(b"my-group");
        data.extend_from_slice(&11u16.to_be_bytes());
        data.extend_from_slice(b"other-group");

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::DescribeGroups(header, body) => {
                assert_eq!(header.api_key, ApiKey::DescribeGroups);
                assert_eq!(header.correlation_id, 1000);
                assert_eq!(body.group_ids.len(), 2);
                assert_eq!(body.group_ids[0], "my-group");
                assert_eq!(body.group_ids[1], "other-group");
            }
            _ => panic!("Expected DescribeGroups request"),
        }
    }

    #[test]
    fn test_parse_describe_groups_request_empty() {
        // DescribeGroups with no group IDs
        let mut data = build_header(15, 0, 1001, None);
        data.extend_from_slice(&0i32.to_be_bytes()); // Empty array

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::DescribeGroups(_, body) => {
                assert!(body.group_ids.is_empty());
            }
            _ => panic!("Expected DescribeGroups request"),
        }
    }

    #[test]
    fn test_parse_list_groups_request_v0() {
        // ListGroups (API key 16) v0 has no body
        let data = build_header(16, 0, 1100, Some("list-client"));

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::ListGroups(header, body) => {
                assert_eq!(header.api_key, ApiKey::ListGroups);
                assert_eq!(header.correlation_id, 1100);
                assert!(body.states_filter.is_empty());
            }
            _ => panic!("Expected ListGroups request"),
        }
    }

    #[test]
    fn test_parse_list_groups_request_v2() {
        // ListGroups v2 also has no body (states_filter added in v4)
        let data = build_header(16, 2, 1101, None);

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::ListGroups(header, body) => {
                assert_eq!(header.api_version, 2);
                assert!(body.states_filter.is_empty());
            }
            _ => panic!("Expected ListGroups request"),
        }
    }

    #[test]
    fn test_parse_delete_groups_request() {
        // DeleteGroups (API key 42) with 2 group IDs
        let mut data = build_header(42, 0, 1200, Some("delete-client"));
        // 2 group IDs
        data.extend_from_slice(&2i32.to_be_bytes());
        data.extend_from_slice(&6u16.to_be_bytes());
        data.extend_from_slice(b"group1");
        data.extend_from_slice(&6u16.to_be_bytes());
        data.extend_from_slice(b"group2");

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::DeleteGroups(header, body) => {
                assert_eq!(header.api_key, ApiKey::DeleteGroups);
                assert_eq!(header.correlation_id, 1200);
                assert_eq!(body.group_ids.len(), 2);
                assert_eq!(body.group_ids[0], "group1");
                assert_eq!(body.group_ids[1], "group2");
            }
            _ => panic!("Expected DeleteGroups request"),
        }
    }

    #[test]
    fn test_parse_delete_groups_request_single() {
        // DeleteGroups with single group
        let mut data = build_header(42, 1, 1201, None);
        data.extend_from_slice(&1i32.to_be_bytes());
        data.extend_from_slice(&14u16.to_be_bytes());
        data.extend_from_slice(b"consumer-group");

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::DeleteGroups(_, body) => {
                assert_eq!(body.group_ids.len(), 1);
                assert_eq!(body.group_ids[0], "consumer-group");
            }
            _ => panic!("Expected DeleteGroups request"),
        }
    }

    #[test]
    fn test_api_key_delete_groups() {
        assert_eq!(ApiKey::from(42), ApiKey::DeleteGroups);
        assert_eq!(i16::from(ApiKey::DeleteGroups), 42);
    }

    #[test]
    fn test_api_key_describe_groups() {
        assert_eq!(ApiKey::from(15), ApiKey::DescribeGroups);
        assert_eq!(i16::from(ApiKey::DescribeGroups), 15);
    }

    #[test]
    fn test_api_key_list_groups() {
        assert_eq!(ApiKey::from(16), ApiKey::ListGroups);
        assert_eq!(i16::from(ApiKey::ListGroups), 16);
    }

    #[test]
    fn test_parse_librdkafka_api_versions_v3_request() {
        // This is an example of what librdkafka sends for ApiVersions v3
        // Per KIP-511, ApiVersions uses a HYBRID header format:
        // Request Header (hybrid):
        //   - request_api_key: INT16 = 18
        //   - request_api_version: INT16 = 3
        //   - correlation_id: INT32 = 1
        //   - client_id: NULLABLE_STRING (standard i16 length, NOT compact!) = "rdkafka"
        //   - tagged_fields: COMPACT_ARRAY = 0 (empty)
        // Request Body (flexible):
        //   - client_software_name: COMPACT_STRING = "librdkafka" (length+1 = 11)
        //   - client_software_version: COMPACT_STRING = "2.3.0" (length+1 = 6)
        //   - tagged_fields: COMPACT_ARRAY = 0 (empty)

        let mut data = Vec::new();

        // Request Header (hybrid format per KIP-511)
        data.extend_from_slice(&18i16.to_be_bytes()); // api_key = ApiVersions
        data.extend_from_slice(&3i16.to_be_bytes()); // api_version = 3
        data.extend_from_slice(&1i32.to_be_bytes()); // correlation_id = 1

        // client_id = "rdkafka" as STANDARD NULLABLE_STRING (i16 length)
        data.extend_from_slice(&7i16.to_be_bytes()); // length = 7
        data.extend_from_slice(b"rdkafka");

        // Empty tagged fields in header
        data.push(0);

        // Request Body (flexible encoding)
        // client_software_name = "librdkafka" as COMPACT_STRING
        data.push(11); // length + 1 = 11 (10 chars + 1)
        data.extend_from_slice(b"librdkafka");

        // client_software_version = "2.3.0" as COMPACT_STRING
        data.push(6); // length + 1 = 6 (5 chars + 1)
        data.extend_from_slice(b"2.3.0");

        // Empty tagged fields in body
        data.push(0);

        let request = Request::parse(Bytes::from(data)).expect("Should parse librdkafka request");

        match request {
            Request::ApiVersions(header, body) => {
                assert_eq!(header.api_key, ApiKey::ApiVersions);
                assert_eq!(header.api_version, 3);
                assert_eq!(header.correlation_id, 1);
                assert_eq!(header.client_id, Some("rdkafka".to_string()));
                assert_eq!(body.client_software_name, Some("librdkafka".to_string()));
                assert_eq!(body.client_software_version, Some("2.3.0".to_string()));
            }
            _ => panic!("Expected ApiVersions request"),
        }
    }
}
