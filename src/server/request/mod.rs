//! Request parsing for incoming Kafka protocol messages.
//!
//! This module provides parsers for Kafka request types, which is the reverse
//! of what the client-side protocol module does (encoding requests).

mod admin;
mod api_versions;
mod auth;
mod configs;
mod fetch;
mod groups;
mod incremental_configs;
mod leader_epoch;
mod metadata;
mod offsets;
mod partitions;
mod produce;

use bytes::Bytes;
use nom::{
    IResult,
    number::complete::{be_i16, be_i32},
};
use nombytes::NomBytes;

use crate::error::{Error, Result};
use crate::parser::{parse_kafka_string_opt, skip_tagged_fields};

// Re-export all request data types
pub use admin::*;
pub use api_versions::*;
pub use auth::*;
pub use configs::*;
pub use fetch::*;
pub use groups::*;
pub use incremental_configs::*;
pub use leader_epoch::*;
pub use metadata::*;
pub use offsets::*;
pub use partitions::*;
pub use produce::*;

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
    OffsetForLeaderEpoch = 23,
    DescribeConfigs = 32,
    AlterConfigs = 33,
    SaslAuthenticate = 36,
    CreatePartitions = 37,
    DeleteGroups = 42,
    IncrementalAlterConfigs = 44,
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
            23 => ApiKey::OffsetForLeaderEpoch,
            32 => ApiKey::DescribeConfigs,
            33 => ApiKey::AlterConfigs,
            36 => ApiKey::SaslAuthenticate,
            37 => ApiKey::CreatePartitions,
            42 => ApiKey::DeleteGroups,
            44 => ApiKey::IncrementalAlterConfigs,
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
            ApiKey::OffsetForLeaderEpoch => 23,
            ApiKey::DescribeConfigs => 32,
            ApiKey::AlterConfigs => 33,
            ApiKey::SaslAuthenticate => 36,
            ApiKey::CreatePartitions => 37,
            ApiKey::DeleteGroups => 42,
            ApiKey::IncrementalAlterConfigs => 44,
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
            ApiKey::OffsetForLeaderEpoch => "OffsetForLeaderEpoch",
            ApiKey::DescribeConfigs => "DescribeConfigs",
            ApiKey::AlterConfigs => "AlterConfigs",
            ApiKey::SaslAuthenticate => "SaslAuthenticate",
            ApiKey::CreatePartitions => "CreatePartitions",
            ApiKey::DeleteGroups => "DeleteGroups",
            ApiKey::IncrementalAlterConfigs => "IncrementalAlterConfigs",
            ApiKey::Unknown(_) => "Unknown",
        }
    }

    /// Response encoding style for a specific protocol version.
    #[inline]
    pub(crate) fn response_style(&self, api_version: i16) -> super::connection::ResponseStyle {
        super::connection::response_style_for(*self, api_version)
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
///
/// Flexible APIs (KIP-482) use request header v2, which keeps `client_id`
/// as a standard NULLABLE_STRING (i16 length, not varint) but appends a
/// tagged-fields section after it. ApiVersions v3+ is the KIP-511 special
/// case of the same shape. Which (api_key, version) pairs are flexible is
/// centralized in [`crate::server::versions::uses_flexible_encoding`].
pub fn parse_request_header(s: NomBytes) -> IResult<NomBytes, RequestHeader> {
    let (s, api_key) = be_i16(s)?;
    let (s, api_version) = be_i16(s)?;
    let (s, correlation_id) = be_i32(s)?;

    // Always parse client_id as standard nullable string (i16 length).
    // This is correct even for flexible header v2 (and ApiVersions v3+
    // per KIP-511): client_id never uses compact encoding in headers.
    let (s, client_id) = parse_kafka_string_opt(s)?;

    // Flexible request headers (header v2) carry tagged_fields after
    // client_id. Skipping this for flexible APIs other than ApiVersions
    // (e.g. InitProducerId v2+) previously left a stray 0x00 in front of
    // the body, mis-aligning every field after it.
    let api_key_typed = ApiKey::from(api_key);
    let s = if crate::server::versions::uses_flexible_encoding(api_key_typed, api_version) {
        let (s, _) = skip_tagged_fields(s)?;
        s
    } else {
        s
    };

    Ok((
        s,
        RequestHeader {
            api_key: api_key_typed,
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
    DescribeConfigs(RequestHeader, DescribeConfigsRequestData),
    AlterConfigs(RequestHeader, AlterConfigsRequestData),
    OffsetForLeaderEpoch(RequestHeader, OffsetForLeaderEpochRequestData),
    CreatePartitions(RequestHeader, CreatePartitionsRequestData),
    IncrementalAlterConfigs(RequestHeader, IncrementalAlterConfigsRequestData),
    /// A known API key was sent with a version outside the advertised
    /// range in [`crate::server::versions::SUPPORTED_VERSIONS`]. The body
    /// is intentionally NOT parsed (its layout is unknown to us); the
    /// connection layer answers with `KafkaCode::UnsupportedVersion`
    /// (error code 35) carrying the request's correlation id.
    UnsupportedVersion(RequestHeader),
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
            Request::DescribeConfigs(h, _) => h,
            Request::AlterConfigs(h, _) => h,
            Request::OffsetForLeaderEpoch(h, _) => h,
            Request::CreatePartitions(h, _) => h,
            Request::IncrementalAlterConfigs(h, _) => h,
            Request::UnsupportedVersion(h) => h,
            Request::Unknown(h, _) => h,
        }
    }

    /// Parse a request from raw bytes.
    ///
    /// After the header is parsed, the api_version is checked against the
    /// advertised range from [`crate::server::versions::SUPPORTED_VERSIONS`]:
    /// an unadvertised version yields [`Request::UnsupportedVersion`] instead
    /// of applying the wrong wire layout to the body bytes. The connection
    /// layer turns that into an UnsupportedVersion (35) error response —
    /// not a connection close.
    pub fn parse(data: Bytes) -> Result<Self> {
        let input = NomBytes::new(data.clone());
        let (remaining, header) = parse_request_header(input).map_err(|_| parsing_error(&data))?;

        // Enforce the advertised version range BEFORE body parsing. Each
        // per-API parser only understands the layouts for the advertised
        // versions; decoding an unadvertised version with the closest known
        // layout silently mis-frames every field. Unknown API keys are left
        // to the Unknown arm (the handler answers those itself).
        if !matches!(header.api_key, ApiKey::Unknown(_))
            && !crate::server::versions::is_version_supported(header.api_key, header.api_version)
        {
            return Ok(Request::UnsupportedVersion(header));
        }

        let version = header.api_version;
        let api_key = header.api_key;

        // Each match arm is structurally identical: invoke the per-API
        // parser, wrap the result in the matching `Request` variant.
        // Hand-written, that's 22 nearly-identical 4-line blocks (~90 lines
        // of boilerplate); keeping that shape in sync across "added a new
        // API" PRs is exactly the kind of bookkeeping that drifts. The
        // macro below collapses it to a one-liner per API.
        macro_rules! dispatch_request {
            (
                $($variant:ident => $parser:path),* $(,)?
            ) => {
                match api_key {
                    $(
                        ApiKey::$variant => {
                            let (rest, body) = $parser(remaining, version)
                                .map_err(|_| parsing_error(&data))?;
                            (rest, Request::$variant(header, body))
                        }
                    )*
                    _ => return Ok(Request::Unknown(header, remaining.into_bytes())),
                }
            };
        }

        let (rest, request) = dispatch_request!(
            Produce => produce::parse_produce_request,
            Fetch => fetch::parse_fetch_request,
            ListOffsets => offsets::parse_list_offsets_request,
            Metadata => metadata::parse_metadata_request,
            OffsetCommit => offsets::parse_offset_commit_request,
            OffsetFetch => offsets::parse_offset_fetch_request,
            FindCoordinator => groups::parse_find_coordinator_request,
            JoinGroup => groups::parse_join_group_request,
            Heartbeat => groups::parse_heartbeat_request,
            LeaveGroup => groups::parse_leave_group_request,
            SyncGroup => groups::parse_sync_group_request,
            SaslHandshake => auth::parse_sasl_handshake_request,
            SaslAuthenticate => auth::parse_sasl_authenticate_request,
            ApiVersions => api_versions::parse_api_versions_request,
            CreateTopics => admin::parse_create_topics_request,
            DeleteTopics => admin::parse_delete_topics_request,
            InitProducerId => admin::parse_init_producer_id_request,
            DescribeGroups => groups::parse_describe_groups_request,
            ListGroups => groups::parse_list_groups_request,
            DeleteGroups => groups::parse_delete_groups_request,
            DescribeConfigs => configs::parse_describe_configs_request,
            AlterConfigs => configs::parse_alter_configs_request,
            OffsetForLeaderEpoch => leader_epoch::parse_offset_for_leader_epoch_request,
            CreatePartitions => partitions::parse_create_partitions_request,
            IncrementalAlterConfigs => incremental_configs::parse_incremental_alter_configs_request,
        );

        // Trailing bytes after a successful body parse are recorded but no
        // longer block dispatch. The previous behavior — rejecting with
        // `Error::TrailingBytes` and writing a generic 2-byte error_code
        // response — broke real clients (e.g. librdkafka with
        // `broker.version.fallback=2.0.0` over-allocates request buffers and
        // ships padding bytes after a valid Produce v3 body): the response
        // was malformed for the request's API shape and the client looped
        // on `Read underflow` until the connection timed out.
        //
        // Length-prefixed framing already isolates each request, so any
        // trailing bytes within a frame cannot bleed into the next
        // request — there is no append-and-piggyback attack surface. The
        // body parser succeeded with every required field for the
        // advertised version, so the parsed `request` is complete; the
        // unconsumed tail is forward-compat / client-side padding that we
        // can safely ignore. The metric + warn keep the signal visible
        // for operators who want to alert on it.
        let trailing = rest.into_bytes();
        if !trailing.is_empty() {
            crate::cluster::metrics::record_request_trailing_bytes(
                api_key.as_str(),
                trailing.len(),
            );
            tracing::warn!(
                api_key = api_key.as_str(),
                api_version = version,
                trailing_bytes = trailing.len(),
                "Tolerating trailing bytes after request body parse"
            );
        }

        Ok(request)
    }
}

/// Maximum number of request bytes preserved in a [`Error::ParsingError`].
///
/// Errors used to clone the ENTIRE request buffer (up to the 100 MB frame
/// cap) into the error value on the hot path. A short prefix is all the
/// debugging value there is; `Bytes::slice` is a cheap refcount bump, not
/// a copy.
const PARSING_ERROR_PREFIX_LEN: usize = 256;

/// Build a `ParsingError` carrying only a bounded prefix of the request.
fn parsing_error(data: &Bytes) -> Error {
    Error::ParsingError(data.slice(..data.len().min(PARSING_ERROR_PREFIX_LEN)))
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
    fn test_api_key_response_style_matches_versions_table() {
        use crate::server::connection::ResponseStyle;
        assert_eq!(
            ApiKey::InitProducerId.response_style(1),
            ResponseStyle::Standard
        );
        assert_eq!(
            ApiKey::InitProducerId.response_style(2),
            ResponseStyle::Flexible
        );
        assert_eq!(ApiKey::Produce.response_style(3), ResponseStyle::Standard);
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

    // ========================================================================
    // Advertised-version enforcement
    // ========================================================================

    #[test]
    fn test_parse_unsupported_version_returns_typed_variant() {
        // Produce is advertised as v3..=v3; a v0 request must NOT be parsed
        // with the v3 layout. The body here is even garbage on purpose —
        // it must never be touched.
        let mut data = build_header(0, 0, 7777, Some("legacy-producer"));
        data.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF]);

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::UnsupportedVersion(header) => {
                assert_eq!(header.api_key, ApiKey::Produce);
                assert_eq!(header.api_version, 0);
                assert_eq!(header.correlation_id, 7777);
                assert_eq!(header.client_id, Some("legacy-producer".to_string()));
            }
            other => panic!("Expected UnsupportedVersion, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_unsupported_version_above_max() {
        // Fetch max is v11; v12 must be refused (v12+ is flexible and
        // intentionally out of scope).
        let data = build_header(1, 12, 31337, None);
        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::UnsupportedVersion(header) => {
                assert_eq!(header.api_key, ApiKey::Fetch);
                assert_eq!(header.api_version, 12);
                assert_eq!(header.correlation_id, 31337);
            }
            other => panic!("Expected UnsupportedVersion, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_unsupported_api_versions_version() {
        // ApiVersions itself is capped at v3; a v4 request gets the typed
        // variant so the connection layer can answer with a v0-encoded
        // ApiVersions response carrying error 35 + supported ranges.
        // (v4 still uses the flexible header, so include tagged fields.)
        let data = build_flexible_header(18, 4, 555, Some("newer-client"));
        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::UnsupportedVersion(header) => {
                assert_eq!(header.api_key, ApiKey::ApiVersions);
                assert_eq!(header.api_version, 4);
                assert_eq!(header.correlation_id, 555);
            }
            other => panic!("Expected UnsupportedVersion, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_supported_version_still_parses() {
        // A supported version must go through body parsing as before.
        let mut data = build_header(3, 1, 808, Some("ok-client"));
        data.extend_from_slice(&(-1i32).to_be_bytes()); // all topics

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::Metadata(header, body) => {
                assert_eq!(header.api_version, 1);
                assert_eq!(header.correlation_id, 808);
                assert!(body.topics.is_none());
            }
            other => panic!("Expected Metadata, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_negative_version_rejected() {
        let data = build_header(3, -1, 1, None);
        let request = Request::parse(Bytes::from(data)).unwrap();
        assert!(matches!(request, Request::UnsupportedVersion(_)));
    }

    #[test]
    fn test_unknown_api_key_bypasses_version_check() {
        // Unknown API keys keep flowing to Request::Unknown so the handler
        // can answer them; the version gate only applies to known keys.
        let data = build_header(999, 42, 2, None);
        let request = Request::parse(Bytes::from(data)).unwrap();
        assert!(matches!(request, Request::Unknown(_, _)));
    }

    #[test]
    fn test_parse_init_producer_id_v2_flexible_via_request_parse() {
        // End-to-end through Request::parse: InitProducerId v2 uses request
        // header v2 (classic client_id + tagged fields) and a flexible body.
        let mut data = Vec::new();
        data.extend_from_slice(&22i16.to_be_bytes()); // api_key = InitProducerId
        data.extend_from_slice(&2i16.to_be_bytes()); // api_version = 2 (flexible)
        data.extend_from_slice(&99i32.to_be_bytes()); // correlation_id
        data.extend_from_slice(&6i16.to_be_bytes()); // client_id (classic)
        data.extend_from_slice(b"prod-1");
        data.push(0); // header tagged fields
        data.push(0); // transactional_id = null (compact varint 0)
        data.extend_from_slice(&60000i32.to_be_bytes()); // timeout
        data.push(0); // body tagged fields

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::InitProducerId(header, body) => {
                assert_eq!(header.api_version, 2);
                assert_eq!(header.correlation_id, 99);
                assert_eq!(header.client_id, Some("prod-1".to_string()));
                assert!(body.transactional_id.is_none());
                assert_eq!(body.transaction_timeout_ms, 60000);
                assert_eq!(body.producer_id, -1);
                assert_eq!(body.producer_epoch, -1);
            }
            other => panic!("Expected InitProducerId, got {:?}", other),
        }
    }

    #[test]
    fn test_parse_init_producer_id_v1_classic_via_request_parse() {
        // v0-v1 keep the classic header (no tagged fields) and body.
        let mut data = Vec::new();
        data.extend_from_slice(&22i16.to_be_bytes()); // api_key
        data.extend_from_slice(&1i16.to_be_bytes()); // api_version = 1
        data.extend_from_slice(&44i32.to_be_bytes()); // correlation_id
        data.extend_from_slice(&(-1i16).to_be_bytes()); // null client_id
        data.extend_from_slice(&(-1i16).to_be_bytes()); // null transactional_id
        data.extend_from_slice(&30000i32.to_be_bytes()); // timeout

        let request = Request::parse(Bytes::from(data)).unwrap();
        match request {
            Request::InitProducerId(header, body) => {
                assert_eq!(header.api_version, 1);
                assert!(body.transactional_id.is_none());
                assert_eq!(body.transaction_timeout_ms, 30000);
            }
            other => panic!("Expected InitProducerId, got {:?}", other),
        }
    }

    #[test]
    fn test_parsing_error_payload_is_truncated() {
        // A large malformed request must not be cloned wholesale into the
        // error. Build a valid Metadata header followed by a body that
        // fails to parse (claims 10 topics, provides none) padded to 1 MiB.
        let mut data = build_header(3, 1, 1, None);
        data.extend_from_slice(&10i32.to_be_bytes()); // 10 topics, no data
        data.resize(1024 * 1024, 0xAA);
        // Make the first "topic" unparseable: negative string length != -1
        // is rejected, so the array parse fails regardless of the padding.
        let err = Request::parse(Bytes::from(data)).unwrap_err();
        match err {
            Error::ParsingError(prefix) => {
                assert!(
                    prefix.len() <= 256,
                    "ParsingError must carry at most a 256-byte prefix, got {}",
                    prefix.len()
                );
            }
            other => panic!("Expected ParsingError, got {:?}", other),
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
