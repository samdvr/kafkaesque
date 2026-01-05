//! Consumer Group APIs codec.
//!
//! Handles encoding/decoding of Kafka consumer group-related requests and responses:
//! - FindCoordinator (API key 10)
//! - JoinGroup (API key 11)
//! - Heartbeat (API key 12)
//! - LeaveGroup (API key 13)
//! - SyncGroup (API key 14)
//! - DescribeGroups (API key 15)
//! - ListGroups (API key 16)
//! - DeleteGroups (API key 42)

use bytes::{Bytes, BytesMut};
use nombytes::NomBytes;

use super::KafkaCodec;
use crate::encode::ToByte;
use crate::error::Result;

// Re-export request/response types from public interfaces
pub use crate::server::request::{
    DeleteGroupsRequestData, DescribeGroupsRequestData, FindCoordinatorRequestData,
    HeartbeatRequestData, JoinGroupRequestData, LeaveGroupRequestData, ListGroupsRequestData,
    SyncGroupRequestData,
};
pub use crate::server::response::{
    DeleteGroupsResponseData, DescribeGroupsResponseData, FindCoordinatorResponseData,
    HeartbeatResponseData, JoinGroupResponseData, LeaveGroupResponseData, ListGroupsResponseData,
    SyncGroupResponseData,
};

// ============================================================================
// FindCoordinator Codec
// ============================================================================

/// Codec for Kafka FindCoordinator API.
///
/// Allows clients to find the coordinator for a consumer group.
///
/// # Supported Versions
///
/// - Versions 0-4 are supported
/// - Version 1+ includes key_type field
pub struct FindCoordinatorCodec;

impl KafkaCodec for FindCoordinatorCodec {
    type Request = FindCoordinatorRequestData;
    type Response = FindCoordinatorResponseData;

    /// Kafka API key for FindCoordinator is 10.
    fn api_key() -> i16 {
        10
    }

    fn min_version() -> i16 {
        0
    }

    fn max_version() -> i16 {
        4
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_find_coordinator_request;
        let (_, request) = parse_find_coordinator_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse find coordinator request".to_string())
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
// JoinGroup Codec
// ============================================================================

/// Codec for Kafka JoinGroup API.
///
/// Allows consumers to join a consumer group.
///
/// # Supported Versions
///
/// - Versions 0-7 are supported
/// - Version 1+ includes rebalance_timeout_ms
/// - Version 2+ includes throttle_time_ms in response
pub struct JoinGroupCodec;

impl KafkaCodec for JoinGroupCodec {
    type Request = JoinGroupRequestData;
    type Response = JoinGroupResponseData;

    /// Kafka API key for JoinGroup is 11.
    fn api_key() -> i16 {
        11
    }

    fn min_version() -> i16 {
        0
    }

    fn max_version() -> i16 {
        7
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_join_group_request;
        let (_, request) = parse_join_group_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse join group request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(256);
        response.encode_versioned(&mut buffer, version)?;
        Ok(buffer.freeze())
    }
}

// ============================================================================
// Heartbeat Codec
// ============================================================================

/// Codec for Kafka Heartbeat API.
///
/// Allows consumers to send heartbeats to maintain group membership.
///
/// # Supported Versions
///
/// - Versions 0-4 are supported
/// - Version 1+ includes throttle_time_ms in response
pub struct HeartbeatCodec;

impl KafkaCodec for HeartbeatCodec {
    type Request = HeartbeatRequestData;
    type Response = HeartbeatResponseData;

    /// Kafka API key for Heartbeat is 12.
    fn api_key() -> i16 {
        12
    }

    fn min_version() -> i16 {
        0
    }

    fn max_version() -> i16 {
        4
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_heartbeat_request;
        let (_, request) = parse_heartbeat_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse heartbeat request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(16);
        response.encode_versioned(&mut buffer, version)?;
        Ok(buffer.freeze())
    }
}

// ============================================================================
// LeaveGroup Codec
// ============================================================================

/// Codec for Kafka LeaveGroup API.
///
/// Allows consumers to leave a consumer group.
///
/// # Supported Versions
///
/// - Versions 0-4 are supported
/// - Version 1+ includes throttle_time_ms in response
pub struct LeaveGroupCodec;

impl KafkaCodec for LeaveGroupCodec {
    type Request = LeaveGroupRequestData;
    type Response = LeaveGroupResponseData;

    /// Kafka API key for LeaveGroup is 13.
    fn api_key() -> i16 {
        13
    }

    fn min_version() -> i16 {
        0
    }

    fn max_version() -> i16 {
        4
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_leave_group_request;
        let (_, request) = parse_leave_group_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse leave group request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(16);
        response.encode_versioned(&mut buffer, version)?;
        Ok(buffer.freeze())
    }
}

// ============================================================================
// SyncGroup Codec
// ============================================================================

/// Codec for Kafka SyncGroup API.
///
/// Allows the group leader to distribute partition assignments.
///
/// # Supported Versions
///
/// - Versions 0-5 are supported
/// - Version 1+ includes throttle_time_ms in response
pub struct SyncGroupCodec;

impl KafkaCodec for SyncGroupCodec {
    type Request = SyncGroupRequestData;
    type Response = SyncGroupResponseData;

    /// Kafka API key for SyncGroup is 14.
    fn api_key() -> i16 {
        14
    }

    fn min_version() -> i16 {
        0
    }

    fn max_version() -> i16 {
        5
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_sync_group_request;
        let (_, request) = parse_sync_group_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse sync group request".to_string())
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
// DescribeGroups Codec
// ============================================================================

/// Codec for Kafka DescribeGroups API.
///
/// Allows clients to get detailed information about consumer groups.
///
/// # Supported Versions
///
/// - Versions 0-5 are supported
pub struct DescribeGroupsCodec;

impl KafkaCodec for DescribeGroupsCodec {
    type Request = DescribeGroupsRequestData;
    type Response = DescribeGroupsResponseData;

    /// Kafka API key for DescribeGroups is 15.
    fn api_key() -> i16 {
        15
    }

    fn min_version() -> i16 {
        0
    }

    fn max_version() -> i16 {
        5
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_describe_groups_request;
        let (_, request) = parse_describe_groups_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse describe groups request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, _version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(256);
        response.encode(&mut buffer)?;
        Ok(buffer.freeze())
    }
}

// ============================================================================
// ListGroups Codec
// ============================================================================

/// Codec for Kafka ListGroups API.
///
/// Allows clients to list all consumer groups.
///
/// # Supported Versions
///
/// - Versions 0-4 are supported
/// - Version 4+ includes states_filter in request
pub struct ListGroupsCodec;

impl KafkaCodec for ListGroupsCodec {
    type Request = ListGroupsRequestData;
    type Response = ListGroupsResponseData;

    /// Kafka API key for ListGroups is 16.
    fn api_key() -> i16 {
        16
    }

    fn min_version() -> i16 {
        0
    }

    fn max_version() -> i16 {
        4
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_list_groups_request;
        let (_, request) = parse_list_groups_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse list groups request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, _version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(128);
        response.encode(&mut buffer)?;
        Ok(buffer.freeze())
    }
}

// ============================================================================
// DeleteGroups Codec
// ============================================================================

/// Codec for Kafka DeleteGroups API.
///
/// Allows clients to delete consumer groups.
///
/// # Supported Versions
///
/// - Versions 0-2 are supported
pub struct DeleteGroupsCodec;

impl KafkaCodec for DeleteGroupsCodec {
    type Request = DeleteGroupsRequestData;
    type Response = DeleteGroupsResponseData;

    /// Kafka API key for DeleteGroups is 42.
    fn api_key() -> i16 {
        42
    }

    fn min_version() -> i16 {
        0
    }

    fn max_version() -> i16 {
        2
    }

    fn decode_request(bytes: NomBytes, version: i16) -> Result<Self::Request> {
        use crate::server::request::parse_delete_groups_request;
        let (_, request) = parse_delete_groups_request(bytes, version).map_err(|_| {
            crate::error::Error::MissingData("Failed to parse delete groups request".to_string())
        })?;
        Ok(request)
    }

    fn encode_response(response: &Self::Response, _version: i16) -> Result<Bytes> {
        let mut buffer = BytesMut::with_capacity(64);
        response.encode(&mut buffer)?;
        Ok(buffer.freeze())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BufMut;

    fn build_string(s: &str, buf: &mut BytesMut) {
        buf.put_i16(s.len() as i16);
        buf.put_slice(s.as_bytes());
    }

    // ========================================================================
    // FindCoordinator Tests
    // ========================================================================

    #[test]
    fn test_find_coordinator_codec_api_key() {
        assert_eq!(FindCoordinatorCodec::api_key(), 10);
    }

    #[test]
    fn test_find_coordinator_codec_version_range() {
        assert_eq!(FindCoordinatorCodec::min_version(), 0);
        assert_eq!(FindCoordinatorCodec::max_version(), 4);
    }

    #[test]
    fn test_find_coordinator_decode_request() {
        let mut request_bytes = BytesMut::new();
        build_string("test-group", &mut request_bytes);
        request_bytes.put_i8(0); // key_type (v1+)

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = FindCoordinatorCodec::decode_request(bytes, 1).unwrap();

        assert_eq!(request.key, "test-group");
        assert_eq!(request.key_type, 0);
    }

    #[test]
    fn test_find_coordinator_decode_request_v0() {
        let mut request_bytes = BytesMut::new();
        build_string("consumer-group", &mut request_bytes);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = FindCoordinatorCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.key, "consumer-group");
    }

    #[test]
    fn test_find_coordinator_encode_response() {
        let response = FindCoordinatorResponseData::success(0, "localhost".to_string(), 9092);
        let encoded = FindCoordinatorCodec::encode_response(&response, 1).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_find_coordinator_encode_response_error() {
        use crate::error::KafkaCode;

        let response = FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupCoordinatorNotAvailable,
            error_message: Some("No coordinator".to_string()),
            node_id: -1,
            host: String::new(),
            port: 0,
        };
        let encoded = FindCoordinatorCodec::encode_response(&response, 1).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========================================================================
    // JoinGroup Tests
    // ========================================================================

    #[test]
    fn test_join_group_codec_api_key() {
        assert_eq!(JoinGroupCodec::api_key(), 11);
    }

    #[test]
    fn test_join_group_codec_version_range() {
        assert_eq!(JoinGroupCodec::min_version(), 0);
        assert_eq!(JoinGroupCodec::max_version(), 7);
    }

    #[test]
    fn test_join_group_decode_request() {
        let mut request_bytes = BytesMut::new();
        build_string("my-group", &mut request_bytes);
        request_bytes.put_i32(30000); // session_timeout_ms
        request_bytes.put_i32(60000); // rebalance_timeout_ms (v1+)
        build_string("", &mut request_bytes); // member_id
        build_string("consumer", &mut request_bytes); // protocol_type
        request_bytes.put_i32(1); // protocols array length
        build_string("range", &mut request_bytes); // protocol name
        request_bytes.put_i32(5); // metadata length
        request_bytes.put_slice(&[0, 1, 2, 3, 4]); // metadata

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = JoinGroupCodec::decode_request(bytes, 1).unwrap();

        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.session_timeout_ms, 30000);
        assert_eq!(request.rebalance_timeout_ms, 60000);
        assert_eq!(request.protocol_type, "consumer");
        assert_eq!(request.protocols.len(), 1);
    }

    #[test]
    fn test_join_group_decode_request_v0() {
        let mut request_bytes = BytesMut::new();
        build_string("my-group", &mut request_bytes);
        request_bytes.put_i32(30000); // session_timeout_ms
        build_string("member-123", &mut request_bytes); // member_id
        build_string("consumer", &mut request_bytes); // protocol_type
        request_bytes.put_i32(0); // empty protocols array

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = JoinGroupCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.session_timeout_ms, 30000);
        assert_eq!(request.member_id, "member-123");
    }

    #[test]
    fn test_join_group_encode_response_versioned() {
        use crate::error::KafkaCode;

        let response = JoinGroupResponseData {
            throttle_time_ms: 100,
            error_code: KafkaCode::None,
            generation_id: 1,
            protocol_name: "range".to_string(),
            leader: "member-1".to_string(),
            member_id: "member-1".to_string(),
            members: vec![],
        };

        let v0_bytes = JoinGroupCodec::encode_response(&response, 0).unwrap();
        let v2_bytes = JoinGroupCodec::encode_response(&response, 2).unwrap();

        // v2 should be 4 bytes larger (throttle_time_ms)
        assert_eq!(v0_bytes.len() + 4, v2_bytes.len());
    }

    #[test]
    fn test_join_group_encode_response_with_members() {
        use crate::error::KafkaCode;
        use crate::server::response::JoinGroupMemberData;

        let response = JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id: 5,
            protocol_name: "roundrobin".to_string(),
            leader: "member-a".to_string(),
            member_id: "member-b".to_string(),
            members: vec![
                JoinGroupMemberData {
                    member_id: "member-a".to_string(),
                    metadata: Bytes::from_static(b"meta-a"),
                },
                JoinGroupMemberData {
                    member_id: "member-b".to_string(),
                    metadata: Bytes::from_static(b"meta-b"),
                },
            ],
        };

        let encoded = JoinGroupCodec::encode_response(&response, 2).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========================================================================
    // Heartbeat Tests
    // ========================================================================

    #[test]
    fn test_heartbeat_codec_api_key() {
        assert_eq!(HeartbeatCodec::api_key(), 12);
    }

    #[test]
    fn test_heartbeat_codec_version_range() {
        assert_eq!(HeartbeatCodec::min_version(), 0);
        assert_eq!(HeartbeatCodec::max_version(), 4);
    }

    #[test]
    fn test_heartbeat_decode_request() {
        let mut request_bytes = BytesMut::new();
        build_string("my-group", &mut request_bytes);
        request_bytes.put_i32(1); // generation_id
        build_string("member-1", &mut request_bytes);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = HeartbeatCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.generation_id, 1);
        assert_eq!(request.member_id, "member-1");
    }

    #[test]
    fn test_heartbeat_encode_response_versioned() {
        use crate::error::KafkaCode;

        let response = HeartbeatResponseData {
            throttle_time_ms: 100,
            error_code: KafkaCode::None,
        };

        let v0_bytes = HeartbeatCodec::encode_response(&response, 0).unwrap();
        let v1_bytes = HeartbeatCodec::encode_response(&response, 1).unwrap();

        assert_eq!(v0_bytes.len(), 2); // Just error_code
        assert_eq!(v1_bytes.len(), 6); // throttle_time + error_code
    }

    #[test]
    fn test_heartbeat_encode_response_error_codes() {
        use crate::error::KafkaCode;

        for error_code in [
            KafkaCode::None,
            KafkaCode::UnknownMemberId,
            KafkaCode::IllegalGeneration,
            KafkaCode::RebalanceInProgress,
        ] {
            let response = HeartbeatResponseData {
                throttle_time_ms: 0,
                error_code,
            };
            let encoded = HeartbeatCodec::encode_response(&response, 1).unwrap();
            assert!(!encoded.is_empty());
        }
    }

    // ========================================================================
    // LeaveGroup Tests
    // ========================================================================

    #[test]
    fn test_leave_group_codec_api_key() {
        assert_eq!(LeaveGroupCodec::api_key(), 13);
    }

    #[test]
    fn test_leave_group_codec_version_range() {
        assert_eq!(LeaveGroupCodec::min_version(), 0);
        assert_eq!(LeaveGroupCodec::max_version(), 4);
    }

    #[test]
    fn test_leave_group_decode_request() {
        let mut request_bytes = BytesMut::new();
        build_string("my-group", &mut request_bytes);
        build_string("member-1", &mut request_bytes);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = LeaveGroupCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.member_id, "member-1");
    }

    #[test]
    fn test_leave_group_encode_response_versioned() {
        use crate::error::KafkaCode;

        let response = LeaveGroupResponseData {
            throttle_time_ms: 50,
            error_code: KafkaCode::None,
        };

        let v0_bytes = LeaveGroupCodec::encode_response(&response, 0).unwrap();
        let v1_bytes = LeaveGroupCodec::encode_response(&response, 1).unwrap();

        assert_eq!(v0_bytes.len(), 2); // Just error_code
        assert_eq!(v1_bytes.len(), 6); // throttle_time + error_code
    }

    // ========================================================================
    // SyncGroup Tests
    // ========================================================================

    #[test]
    fn test_sync_group_codec_api_key() {
        assert_eq!(SyncGroupCodec::api_key(), 14);
    }

    #[test]
    fn test_sync_group_codec_version_range() {
        assert_eq!(SyncGroupCodec::min_version(), 0);
        assert_eq!(SyncGroupCodec::max_version(), 5);
    }

    #[test]
    fn test_sync_group_decode_request() {
        let mut request_bytes = BytesMut::new();
        build_string("my-group", &mut request_bytes);
        request_bytes.put_i32(1); // generation_id
        build_string("member-1", &mut request_bytes);
        request_bytes.put_i32(0); // Empty assignments

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = SyncGroupCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.generation_id, 1);
        assert!(request.assignments.is_empty());
    }

    #[test]
    fn test_sync_group_decode_request_with_assignments() {
        let mut request_bytes = BytesMut::new();
        build_string("my-group", &mut request_bytes);
        request_bytes.put_i32(5); // generation_id
        build_string("leader", &mut request_bytes);
        request_bytes.put_i32(1); // 1 assignment
        build_string("member-a", &mut request_bytes);
        request_bytes.put_i32(4); // assignment bytes length
        request_bytes.put_slice(&[1, 2, 3, 4]); // assignment data

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = SyncGroupCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.generation_id, 5);
        assert_eq!(request.assignments.len(), 1);
    }

    #[test]
    fn test_sync_group_encode_response_versioned() {
        use crate::error::KafkaCode;

        let response = SyncGroupResponseData {
            throttle_time_ms: 100,
            error_code: KafkaCode::None,
            assignment: Bytes::from(vec![1, 2, 3]),
        };

        let v0_bytes = SyncGroupCodec::encode_response(&response, 0).unwrap();
        let v1_bytes = SyncGroupCodec::encode_response(&response, 1).unwrap();

        // v0: error_code(2) + len(4) + data(3) = 9
        assert_eq!(v0_bytes.len(), 9);
        // v1: throttle(4) + error_code(2) + len(4) + data(3) = 13
        assert_eq!(v1_bytes.len(), 13);
    }

    #[test]
    fn test_sync_group_encode_response_empty_assignment() {
        use crate::error::KafkaCode;

        let response = SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            assignment: Bytes::new(),
        };

        let encoded = SyncGroupCodec::encode_response(&response, 1).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========================================================================
    // DescribeGroups Tests
    // ========================================================================

    #[test]
    fn test_describe_groups_codec_api_key() {
        assert_eq!(DescribeGroupsCodec::api_key(), 15);
    }

    #[test]
    fn test_describe_groups_codec_version_range() {
        assert_eq!(DescribeGroupsCodec::min_version(), 0);
        assert_eq!(DescribeGroupsCodec::max_version(), 5);
    }

    #[test]
    fn test_describe_groups_decode_request() {
        let mut request_bytes = BytesMut::new();
        request_bytes.put_i32(2); // 2 groups
        build_string("group-a", &mut request_bytes);
        build_string("group-b", &mut request_bytes);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = DescribeGroupsCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.group_ids.len(), 2);
        assert_eq!(request.group_ids[0], "group-a");
        assert_eq!(request.group_ids[1], "group-b");
    }

    #[test]
    fn test_describe_groups_decode_request_empty() {
        let mut request_bytes = BytesMut::new();
        request_bytes.put_i32(0); // 0 groups

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = DescribeGroupsCodec::decode_request(bytes, 0).unwrap();

        assert!(request.group_ids.is_empty());
    }

    #[test]
    fn test_describe_groups_encode_response() {
        use crate::error::KafkaCode;
        use crate::server::response::DescribedGroup;

        let response = DescribeGroupsResponseData {
            throttle_time_ms: 0,
            groups: vec![DescribedGroup {
                error_code: KafkaCode::None,
                group_id: "test-group".to_string(),
                group_state: "Stable".to_string(),
                protocol_type: "consumer".to_string(),
                protocol_data: "range".to_string(),
                members: vec![],
            }],
        };

        let encoded = DescribeGroupsCodec::encode_response(&response, 0).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========================================================================
    // ListGroups Tests
    // ========================================================================

    #[test]
    fn test_list_groups_codec_api_key() {
        assert_eq!(ListGroupsCodec::api_key(), 16);
    }

    #[test]
    fn test_list_groups_codec_version_range() {
        assert_eq!(ListGroupsCodec::min_version(), 0);
        assert_eq!(ListGroupsCodec::max_version(), 4);
    }

    #[test]
    fn test_list_groups_decode_request_v0() {
        let request_bytes = BytesMut::new();
        let bytes = NomBytes::new(request_bytes.freeze());
        let request = ListGroupsCodec::decode_request(bytes, 0).unwrap();
        assert!(request.states_filter.is_empty());
    }

    #[test]
    fn test_list_groups_decode_request_v4() {
        let mut request_bytes = BytesMut::new();
        request_bytes.put_i32(1); // 1 state filter
        build_string("Stable", &mut request_bytes);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = ListGroupsCodec::decode_request(bytes, 4).unwrap();

        assert_eq!(request.states_filter.len(), 1);
        assert_eq!(request.states_filter[0], "Stable");
    }

    #[test]
    fn test_list_groups_decode_request_v4_multiple_filters() {
        let mut request_bytes = BytesMut::new();
        request_bytes.put_i32(3); // 3 state filters
        build_string("Stable", &mut request_bytes);
        build_string("PreparingRebalance", &mut request_bytes);
        build_string("Empty", &mut request_bytes);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = ListGroupsCodec::decode_request(bytes, 4).unwrap();

        assert_eq!(request.states_filter.len(), 3);
        assert_eq!(request.states_filter[0], "Stable");
        assert_eq!(request.states_filter[1], "PreparingRebalance");
        assert_eq!(request.states_filter[2], "Empty");
    }

    #[test]
    fn test_list_groups_encode_response() {
        use crate::error::KafkaCode;
        use crate::server::response::ListedGroup;

        let response = ListGroupsResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            groups: vec![
                ListedGroup {
                    group_id: "group-1".to_string(),
                    protocol_type: "consumer".to_string(),
                    group_state: "Stable".to_string(),
                },
                ListedGroup {
                    group_id: "group-2".to_string(),
                    protocol_type: "consumer".to_string(),
                    group_state: "Empty".to_string(),
                },
            ],
        };

        let encoded = ListGroupsCodec::encode_response(&response, 0).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========================================================================
    // DeleteGroups Tests
    // ========================================================================

    #[test]
    fn test_delete_groups_codec_api_key() {
        assert_eq!(DeleteGroupsCodec::api_key(), 42);
    }

    #[test]
    fn test_delete_groups_codec_version_range() {
        assert_eq!(DeleteGroupsCodec::min_version(), 0);
        assert_eq!(DeleteGroupsCodec::max_version(), 2);
    }

    #[test]
    fn test_delete_groups_decode_request() {
        let mut request_bytes = BytesMut::new();
        request_bytes.put_i32(1); // 1 group
        build_string("old-group", &mut request_bytes);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = DeleteGroupsCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.group_ids.len(), 1);
        assert_eq!(request.group_ids[0], "old-group");
    }

    #[test]
    fn test_delete_groups_decode_request_multiple() {
        let mut request_bytes = BytesMut::new();
        request_bytes.put_i32(3); // 3 groups
        build_string("group-1", &mut request_bytes);
        build_string("group-2", &mut request_bytes);
        build_string("group-3", &mut request_bytes);

        let bytes = NomBytes::new(request_bytes.freeze());
        let request = DeleteGroupsCodec::decode_request(bytes, 0).unwrap();

        assert_eq!(request.group_ids.len(), 3);
        assert_eq!(request.group_ids[0], "group-1");
        assert_eq!(request.group_ids[1], "group-2");
        assert_eq!(request.group_ids[2], "group-3");
    }

    #[test]
    fn test_delete_groups_encode_response() {
        use crate::server::response::DeleteGroupResult;

        let response = DeleteGroupsResponseData::new(vec![DeleteGroupResult::success("group-1")]);

        let encoded = DeleteGroupsCodec::encode_response(&response, 0).unwrap();
        assert!(!encoded.is_empty());
    }

    #[test]
    fn test_delete_groups_encode_response_with_errors() {
        use crate::error::KafkaCode;
        use crate::server::response::DeleteGroupResult;

        let response = DeleteGroupsResponseData::new(vec![
            DeleteGroupResult::success("group-1"),
            DeleteGroupResult {
                group_id: "group-2".to_string(),
                error_code: KafkaCode::NotCoordinatorForGroup,
            },
        ]);

        let encoded = DeleteGroupsCodec::encode_response(&response, 0).unwrap();
        assert!(!encoded.is_empty());
    }

    // ========================================================================
    // Error Handling Tests
    // ========================================================================

    #[test]
    fn test_find_coordinator_decode_invalid_request() {
        // Invalid/truncated request
        let bytes = NomBytes::new(Bytes::from_static(&[0x00]));
        let result = FindCoordinatorCodec::decode_request(bytes, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_join_group_decode_invalid_request() {
        // Invalid/truncated request
        let bytes = NomBytes::new(Bytes::from_static(&[0x00, 0x01]));
        let result = JoinGroupCodec::decode_request(bytes, 1);
        assert!(result.is_err());
    }

    #[test]
    fn test_heartbeat_decode_invalid_request() {
        // Invalid/truncated request
        let bytes = NomBytes::new(Bytes::from_static(&[0x00]));
        let result = HeartbeatCodec::decode_request(bytes, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_leave_group_decode_invalid_request() {
        // Invalid/truncated request
        let bytes = NomBytes::new(Bytes::from_static(&[0x00]));
        let result = LeaveGroupCodec::decode_request(bytes, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_sync_group_decode_invalid_request() {
        // Invalid/truncated request
        let bytes = NomBytes::new(Bytes::from_static(&[0x00]));
        let result = SyncGroupCodec::decode_request(bytes, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_describe_groups_decode_invalid_request() {
        // Invalid/truncated request
        let bytes = NomBytes::new(Bytes::from_static(&[0x00]));
        let result = DescribeGroupsCodec::decode_request(bytes, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_list_groups_decode_invalid_request_v4() {
        // Invalid/truncated request for v4
        let bytes = NomBytes::new(Bytes::from_static(&[0x00]));
        let result = ListGroupsCodec::decode_request(bytes, 4);
        assert!(result.is_err());
    }

    #[test]
    fn test_delete_groups_decode_invalid_request() {
        // Invalid/truncated request
        let bytes = NomBytes::new(Bytes::from_static(&[0x00]));
        let result = DeleteGroupsCodec::decode_request(bytes, 0);
        assert!(result.is_err());
    }
}
