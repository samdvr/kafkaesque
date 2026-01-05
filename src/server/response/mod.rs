//! Response encoding for outgoing Kafka protocol messages.
//!
//! This module provides encoding for Kafka response types, which is the reverse
//! of what the client-side protocol module does (parsing responses).

mod admin;
mod auth;
mod fetch;
mod groups;
mod metadata;
mod offsets;
mod produce;
mod versions;

use bytes::BufMut;

use crate::encode::ToByte;
use crate::error::Result;

// Re-export all response data types
pub use admin::*;
pub use auth::*;
pub use fetch::*;
pub use groups::*;
pub use metadata::*;
pub use offsets::*;
pub use produce::*;
pub use versions::*;

/// Response header for Kafka protocol.
#[derive(Debug, Clone)]
pub struct ResponseHeader {
    pub correlation_id: i32,
}

impl ToByte for ResponseHeader {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.correlation_id.encode(buffer)
    }
}

/// Encode a nullable string in Kafka protocol format.
///
/// In the Kafka wire protocol, nullable strings are encoded as:
/// - `-1` (i16) if the string is None/null
/// - Length (i16) followed by UTF-8 bytes if the string is present
///
/// This is used for optional string fields in responses.
pub(crate) fn encode_nullable_string<W: BufMut>(s: Option<&str>, buffer: &mut W) -> Result<()> {
    match s {
        Some(val) => val.encode(buffer),
        None => (-1i16).encode(buffer),
    }
}

/// Response wrapper that includes correlation ID and response body.
pub struct Response {
    pub correlation_id: i32,
    body: Vec<u8>,
    flexible: bool,
}

impl Response {
    /// Create a new response with the given correlation ID and body.
    pub fn new<T: ToByte>(correlation_id: i32, body: &T) -> Result<Self> {
        let mut buf = Vec::new();
        body.encode(&mut buf)?;
        Ok(Self {
            correlation_id,
            body: buf,
            flexible: false,
        })
    }

    /// Create a new flexible response (for APIs using flexible versions).
    pub fn new_flexible<T: ToByte>(correlation_id: i32, body: &T) -> Result<Self> {
        let mut buf = Vec::new();
        body.encode(&mut buf)?;
        Ok(Self {
            correlation_id,
            body: buf,
            flexible: true,
        })
    }

    /// Create a new response with pre-encoded body bytes.
    /// Use this when you need version-specific encoding.
    pub fn new_raw(correlation_id: i32, body: Vec<u8>) -> Result<Self> {
        Ok(Self {
            correlation_id,
            body,
            flexible: false,
        })
    }

    /// Encode the response to a buffer with the size prefix.
    pub fn encode_with_size(&self) -> Result<Vec<u8>> {
        let mut header = Vec::new();
        self.correlation_id.encode(&mut header)?;

        // For flexible versions, add empty tagged fields to header
        if self.flexible {
            header.push(0); // Empty tagged fields in header
        }

        let total_size = (header.len() + self.body.len()) as i32;
        let mut result = Vec::with_capacity(4 + total_size as usize);
        total_size.encode(&mut result)?;
        result.extend_from_slice(&header);
        result.extend_from_slice(&self.body);
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KafkaCode;
    use crate::server::request::ApiKey;
    use bytes::Bytes;

    #[test]
    fn test_response_header_encode() {
        let header = ResponseHeader { correlation_id: 42 };
        let mut buf = Vec::new();
        header.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x2A]); // 42 as i32 big-endian
    }

    #[test]
    fn test_heartbeat_response_encode() {
        let response = HeartbeatResponseData {
            throttle_time_ms: 100,
            error_code: KafkaCode::None,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        let expected = vec![
            0x00, 0x00, 0x00, 0x64, // throttle_time_ms = 100
            0x00, 0x00, // error_code = 0
        ];
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_leave_group_response_encode() {
        let response = LeaveGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::UnknownMemberId,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        let expected = vec![
            0x00, 0x00, 0x00, 0x00, // throttle_time_ms = 0
            0x00, 0x19, // error_code = 25 (UnknownMemberId)
        ];
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_error_response_encode() {
        let response = ErrorResponseData {
            error_code: KafkaCode::UnsupportedVersion,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        let expected = vec![0x00, 0x23]; // 35 as i16
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_sasl_handshake_response_encode() {
        let response = SaslHandshakeResponseData {
            error_code: KafkaCode::None,
            mechanisms: vec!["PLAIN".to_string(), "SCRAM-SHA-256".to_string()],
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // error_code (2) + array_len (4) + "PLAIN" (2+5) + "SCRAM-SHA-256" (2+13) = 28 bytes
        assert_eq!(buf.len(), 28);
        // Check error code = 0
        assert_eq!(&buf[0..2], &[0x00, 0x00]);
        // Check array length = 2
        assert_eq!(&buf[2..6], &[0x00, 0x00, 0x00, 0x02]);
    }

    #[test]
    fn test_find_coordinator_response_encode() {
        let response = FindCoordinatorResponseData {
            throttle_time_ms: 50,
            error_code: KafkaCode::None,
            error_message: None,
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // Verify throttle_time_ms at start
        assert_eq!(&buf[0..4], &[0x00, 0x00, 0x00, 0x32]); // 50
        // Error code = 0
        assert_eq!(&buf[4..6], &[0x00, 0x00]);
        // Error message = null (-1)
        assert_eq!(&buf[6..8], &[0xFF, 0xFF]);
        // node_id = 1
        assert_eq!(&buf[8..12], &[0x00, 0x00, 0x00, 0x01]);
    }

    #[test]
    fn test_broker_data_encode() {
        let broker = BrokerData {
            node_id: 0,
            host: "127.0.0.1".to_string(),
            port: 9092,
            rack: None,
        };
        let mut buf = Vec::new();
        broker.encode(&mut buf).unwrap();

        // node_id (4) + host_len (2) + "127.0.0.1" (9) + port (4) + rack_null (2) = 21 bytes
        assert_eq!(buf.len(), 21);
        // Check node_id = 0
        assert_eq!(&buf[0..4], &[0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn test_broker_data_with_rack_encode() {
        let broker = BrokerData {
            node_id: 5,
            host: "host".to_string(),
            port: 9092,
            rack: Some("rack-1".to_string()),
        };
        let mut buf = Vec::new();
        broker.encode(&mut buf).unwrap();

        // node_id (4) + host_len (2) + "host" (4) + port (4) + rack_len (2) + "rack-1" (6) = 22 bytes
        assert_eq!(buf.len(), 22);
    }

    #[test]
    fn test_api_versions_response_encode() {
        let response = ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![ApiVersionData {
                api_key: ApiKey::Metadata,
                min_version: 0,
                max_version: 9,
            }],
            throttle_time_ms: 0,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // error_code (2) + array_len (4) + api_entry (6) + throttle_time_ms (4) = 16 bytes
        assert_eq!(buf.len(), 16);
        // Error code = 0
        assert_eq!(&buf[0..2], &[0x00, 0x00]);
        // Array length = 1
        assert_eq!(&buf[2..6], &[0x00, 0x00, 0x00, 0x01]);
        // API key = 3 (Metadata)
        assert_eq!(&buf[6..8], &[0x00, 0x03]);
    }

    #[test]
    fn test_produce_partition_response_encode() {
        let response = ProducePartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            base_offset: 100,
            log_append_time: -1,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // partition_index (4) + error_code (2) + base_offset (8) + log_append_time (8) = 22 bytes
        assert_eq!(buf.len(), 22);
        // Check partition_index = 0
        assert_eq!(&buf[0..4], &[0x00, 0x00, 0x00, 0x00]);
        // Check error_code = 0
        assert_eq!(&buf[4..6], &[0x00, 0x00]);
    }

    #[test]
    fn test_list_offsets_partition_response_encode() {
        let response = ListOffsetsPartitionResponse {
            partition_index: 1,
            error_code: KafkaCode::None,
            timestamp: 1234567890,
            offset: 500,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // partition_index (4) + error_code (2) + timestamp (8) + offset (8) = 22 bytes
        assert_eq!(buf.len(), 22);
    }

    #[test]
    fn test_offset_commit_response_encode() {
        let response = OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics: vec![OffsetCommitTopicResponse {
                name: "t1".to_string(),
                partitions: vec![OffsetCommitPartitionResponse {
                    partition_index: 0,
                    error_code: KafkaCode::None,
                }],
            }],
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + topics_len (4) + name_len (2) + "t1" (2) + parts_len (4) + part_idx (4) + err (2) = 22 bytes
        assert_eq!(buf.len(), 22);
    }

    #[test]
    fn test_offset_fetch_partition_response_encode() {
        let response = OffsetFetchPartitionResponse {
            partition_index: 0,
            committed_offset: 42,
            metadata: Some("meta".to_string()),
            error_code: KafkaCode::None,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // partition_index (4) + committed_offset (8) + metadata_len (2) + "meta" (4) + error_code (2) = 20 bytes
        assert_eq!(buf.len(), 20);
    }

    #[test]
    fn test_sync_group_response_encode() {
        let response = SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            assignment: Bytes::from(vec![1, 2, 3]),
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + error_code (2) + assignment_len (4) + bytes (3) = 13 bytes
        assert_eq!(buf.len(), 13);
    }

    #[test]
    fn test_create_topics_response_encode() {
        let response = CreateTopicsResponseData {
            throttle_time_ms: 100,
            topics: vec![CreateTopicResponseData {
                name: "new-topic".to_string(),
                error_code: KafkaCode::None,
                error_message: None,
            }],
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + topics_len (4) + name_len (2) + "new-topic" (9) + err (2) + err_msg_null (2) = 23 bytes
        assert_eq!(buf.len(), 23);
    }

    #[test]
    fn test_delete_topics_response_encode() {
        let response = DeleteTopicsResponseData {
            throttle_time_ms: 0,
            responses: vec![
                DeleteTopicResponseData {
                    name: "t1".to_string(),
                    error_code: KafkaCode::None,
                },
                DeleteTopicResponseData {
                    name: "t2".to_string(),
                    error_code: KafkaCode::UnknownTopicOrPartition,
                },
            ],
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + array_len (4) + 2 * (name_len (2) + name (2) + err (2)) = 20 bytes
        assert_eq!(buf.len(), 20);
    }

    #[test]
    fn test_init_producer_id_response_encode() {
        let response = InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 12345,
            producer_epoch: 0,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + error (2) + producer_id (8) + producer_epoch (2) + tag (1) = 17 bytes
        assert_eq!(buf.len(), 17);
    }

    #[test]
    fn test_response_new_and_encode() {
        let body = HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        };
        let response = Response::new(123, &body).unwrap();
        let encoded = response.encode_with_size().unwrap();

        // size (4) + correlation_id (4) + body (6) = 14 total, but size prefix is just for inner
        // size = correlation_id (4) + body (6) = 10
        assert_eq!(&encoded[0..4], &[0x00, 0x00, 0x00, 0x0A]); // size = 10
        assert_eq!(&encoded[4..8], &[0x00, 0x00, 0x00, 0x7B]); // correlation_id = 123
    }

    #[test]
    fn test_response_flexible_and_encode() {
        let body = HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        };
        let response = Response::new_flexible(456, &body).unwrap();
        let encoded = response.encode_with_size().unwrap();

        // size (4) + correlation_id (4) + tagged_fields (1) + body (6) = 15 total
        // size = 4 + 1 + 6 = 11
        assert_eq!(&encoded[0..4], &[0x00, 0x00, 0x00, 0x0B]); // size = 11
        assert_eq!(&encoded[4..8], &[0x00, 0x00, 0x01, 0xC8]); // correlation_id = 456
        assert_eq!(encoded[8], 0x00); // empty tagged fields
    }

    #[test]
    fn test_fetch_partition_response_with_records() {
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 100,
            last_stable_offset: 100,
            aborted_transactions: vec![],
            records: Some(Bytes::from(vec![1, 2, 3, 4])),
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // partition_index (4) + error (2) + hwm (8) + lso (8) + aborted_len (4) + records_len (4) + records (4) = 34
        assert_eq!(buf.len(), 34);
    }

    #[test]
    fn test_fetch_partition_response_null_records() {
        let response = FetchPartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            high_watermark: 0,
            last_stable_offset: 0,
            aborted_transactions: vec![],
            records: None,
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // partition_index (4) + error (2) + hwm (8) + lso (8) + aborted_len (4) + null_records (4) = 30
        assert_eq!(buf.len(), 30);
    }

    #[test]
    fn test_partition_metadata_encode() {
        let metadata = PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 0,
            leader_id: 1,
            replica_nodes: vec![1, 2, 3],
            isr_nodes: vec![1, 2],
        };
        let mut buf = Vec::new();
        metadata.encode(&mut buf).unwrap();

        // error (2) + partition_index (4) + leader_id (4) + replica_len (4) + replicas (12) + isr_len (4) + isr (8) = 38
        assert_eq!(buf.len(), 38);
    }

    #[test]
    fn test_topic_metadata_encode() {
        let topic = TopicMetadata {
            error_code: KafkaCode::None,
            name: "test".to_string(),
            is_internal: false,
            partitions: vec![],
        };
        let mut buf = Vec::new();
        topic.encode(&mut buf).unwrap();

        // error (2) + name_len (2) + "test" (4) + is_internal (1) + partitions_len (4) = 13
        assert_eq!(buf.len(), 13);
    }

    #[test]
    fn test_join_group_response_encode() {
        let response = JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id: 1,
            protocol_name: "range".to_string(),
            leader: "member-1".to_string(),
            member_id: "member-1".to_string(),
            members: vec![],
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + error (2) + generation (4) + protocol_len (2) + "range" (5) +
        // leader_len (2) + "member-1" (8) + member_id_len (2) + "member-1" (8) + members_len (4) = 41
        assert_eq!(buf.len(), 41);
    }

    #[test]
    fn test_sasl_authenticate_response_encode() {
        let response = SaslAuthenticateResponseData {
            error_code: KafkaCode::None,
            error_message: Some("ok".to_string()),
            auth_bytes: Bytes::from(vec![0xAB, 0xCD]),
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // error (2) + err_msg_len (2) + "ok" (2) + auth_len (4) + auth (2) = 12
        assert_eq!(buf.len(), 12);
    }

    // Tests for helper methods and builders

    #[test]
    fn test_produce_partition_response_error() {
        let response = ProducePartitionResponse::error(5, KafkaCode::NotLeaderForPartition);
        assert_eq!(response.partition_index, 5);
        assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
        assert_eq!(response.base_offset, -1);
        assert_eq!(response.log_append_time, -1);
    }

    #[test]
    fn test_produce_partition_response_success() {
        let response = ProducePartitionResponse::success(3, 100);
        assert_eq!(response.partition_index, 3);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.base_offset, 100);
        assert_eq!(response.log_append_time, -1);
    }

    #[test]
    fn test_fetch_partition_response_error() {
        let response = FetchPartitionResponse::error(2, KafkaCode::Unknown);
        assert_eq!(response.partition_index, 2);
        assert_eq!(response.error_code, KafkaCode::Unknown);
        assert_eq!(response.high_watermark, -1);
        assert_eq!(response.last_stable_offset, -1);
        assert!(response.aborted_transactions.is_empty());
        assert!(response.records.is_none());
    }

    #[test]
    fn test_fetch_partition_response_success() {
        let records = Bytes::from(vec![1, 2, 3]);
        let response = FetchPartitionResponse::success(1, 50, Some(records.clone()));
        assert_eq!(response.partition_index, 1);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.high_watermark, 50);
        assert_eq!(response.last_stable_offset, 50);
        assert_eq!(response.records, Some(records));
    }

    #[test]
    fn test_list_offsets_partition_response_error() {
        let response = ListOffsetsPartitionResponse::error(0, KafkaCode::NotLeaderForPartition);
        assert_eq!(response.partition_index, 0);
        assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
        assert_eq!(response.timestamp, -1);
        assert_eq!(response.offset, -1);
    }

    #[test]
    fn test_list_offsets_partition_response_success() {
        let response = ListOffsetsPartitionResponse::success(4, 999);
        assert_eq!(response.partition_index, 4);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.timestamp, -1);
        assert_eq!(response.offset, 999);
    }

    #[test]
    fn test_offset_commit_partition_response_new() {
        let response = OffsetCommitPartitionResponse::new(1, KafkaCode::Unknown);
        assert_eq!(response.partition_index, 1);
        assert_eq!(response.error_code, KafkaCode::Unknown);
    }

    #[test]
    fn test_offset_commit_partition_response_success() {
        let response = OffsetCommitPartitionResponse::success(2);
        assert_eq!(response.partition_index, 2);
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_offset_fetch_partition_response_new() {
        let response = OffsetFetchPartitionResponse::new(0, 42, Some("metadata".to_string()));
        assert_eq!(response.partition_index, 0);
        assert_eq!(response.committed_offset, 42);
        assert_eq!(response.metadata, Some("metadata".to_string()));
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_offset_fetch_partition_response_unknown() {
        let response = OffsetFetchPartitionResponse::unknown(3);
        assert_eq!(response.partition_index, 3);
        assert_eq!(response.committed_offset, -1);
        assert!(response.metadata.is_none());
    }

    #[test]
    fn test_find_coordinator_response_success() {
        let response = FindCoordinatorResponseData::success(1, "localhost".to_string(), 9092);
        assert_eq!(response.node_id, 1);
        assert_eq!(response.host, "localhost");
        assert_eq!(response.port, 9092);
        assert_eq!(response.error_code, KafkaCode::None);
        assert!(response.error_message.is_none());
    }

    #[test]
    fn test_find_coordinator_response_error() {
        let response = FindCoordinatorResponseData::error(
            KafkaCode::GroupCoordinatorNotAvailable,
            Some("test error".to_string()),
        );
        assert_eq!(response.node_id, -1);
        assert_eq!(response.host, "");
        assert_eq!(response.port, 0);
        assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
        assert_eq!(response.error_message, Some("test error".to_string()));
    }

    #[test]
    fn test_find_coordinator_response_not_available() {
        let response = FindCoordinatorResponseData::not_available("No brokers");
        assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
        assert_eq!(response.error_message, Some("No brokers".to_string()));
    }

    #[test]
    fn test_join_group_response_error() {
        let response = JoinGroupResponseData::error(KafkaCode::Unknown, "member-123".to_string());
        assert_eq!(response.error_code, KafkaCode::Unknown);
        assert_eq!(response.member_id, "member-123");
        assert_eq!(response.generation_id, -1);
        assert!(response.members.is_empty());
    }

    #[test]
    fn test_join_group_response_builder() {
        let response = JoinGroupResponseData::builder()
            .generation_id(5)
            .protocol_name("range")
            .leader("leader-1")
            .member_id("member-1")
            .add_member("member-1", Bytes::from(vec![1, 2]))
            .add_member("member-2", Bytes::from(vec![3, 4]))
            .build();

        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.generation_id, 5);
        assert_eq!(response.protocol_name, "range");
        assert_eq!(response.leader, "leader-1");
        assert_eq!(response.member_id, "member-1");
        assert_eq!(response.members.len(), 2);
        assert_eq!(response.members[0].member_id, "member-1");
        assert_eq!(response.members[1].member_id, "member-2");
    }

    #[test]
    fn test_join_group_response_builder_with_members_vec() {
        let members = vec![
            JoinGroupMemberData {
                member_id: "m1".to_string(),
                metadata: Bytes::new(),
            },
            JoinGroupMemberData {
                member_id: "m2".to_string(),
                metadata: Bytes::new(),
            },
        ];
        let response = JoinGroupResponseData::builder()
            .generation_id(1)
            .members(members)
            .build();

        assert_eq!(response.members.len(), 2);
    }

    // ========================================================================
    // DescribeGroups tests
    // ========================================================================

    #[test]
    fn test_describe_groups_response_encode() {
        let response = DescribeGroupsResponseData {
            throttle_time_ms: 0,
            groups: vec![DescribedGroup {
                error_code: KafkaCode::None,
                group_id: "grp".to_string(),
                group_state: "Stable".to_string(),
                protocol_type: "consumer".to_string(),
                protocol_data: "range".to_string(),
                members: vec![],
            }],
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + groups_len (4) + error (2) + group_id_len (2) + "grp" (3) +
        // state_len (2) + "Stable" (6) + protocol_type_len (2) + "consumer" (8) +
        // protocol_data_len (2) + "range" (5) + members_len (4) = 44 bytes
        assert_eq!(buf.len(), 44);
        // Check throttle_time_ms = 0
        assert_eq!(&buf[0..4], &[0x00, 0x00, 0x00, 0x00]);
        // Check groups array length = 1
        assert_eq!(&buf[4..8], &[0x00, 0x00, 0x00, 0x01]);
    }

    #[test]
    fn test_describe_groups_response_error() {
        let response = DescribeGroupsResponseData::error(
            "my-group".to_string(),
            KafkaCode::GroupCoordinatorNotAvailable,
        );
        assert_eq!(response.groups.len(), 1);
        assert_eq!(response.groups[0].group_id, "my-group");
        assert_eq!(
            response.groups[0].error_code,
            KafkaCode::GroupCoordinatorNotAvailable
        );
        assert!(response.groups[0].members.is_empty());
    }

    #[test]
    fn test_described_group_error() {
        let group = DescribedGroup::error("test-group".to_string(), KafkaCode::Unknown);
        assert_eq!(group.group_id, "test-group");
        assert_eq!(group.error_code, KafkaCode::Unknown);
        assert!(group.group_state.is_empty());
        assert!(group.members.is_empty());
    }

    #[test]
    fn test_described_group_builder() {
        let member = DescribedGroupMember::new(
            "member-1",
            "client-1",
            "/127.0.0.1",
            Bytes::from(vec![1, 2]),
            Bytes::from(vec![3, 4]),
        );
        let group = DescribedGroup::builder("my-group")
            .group_state("Stable")
            .protocol_type("consumer")
            .protocol_data("range")
            .add_member(member)
            .build();

        assert_eq!(group.error_code, KafkaCode::None);
        assert_eq!(group.group_id, "my-group");
        assert_eq!(group.group_state, "Stable");
        assert_eq!(group.protocol_type, "consumer");
        assert_eq!(group.protocol_data, "range");
        assert_eq!(group.members.len(), 1);
        assert_eq!(group.members[0].member_id, "member-1");
    }

    #[test]
    fn test_described_group_member_encode() {
        let member = DescribedGroupMember {
            member_id: "m1".to_string(),
            client_id: "c1".to_string(),
            client_host: "/h".to_string(),
            member_metadata: Bytes::from(vec![1, 2]),
            member_assignment: Bytes::from(vec![3]),
        };
        let mut buf = Vec::new();
        member.encode(&mut buf).unwrap();

        // member_id_len (2) + "m1" (2) + client_id_len (2) + "c1" (2) +
        // client_host_len (2) + "/h" (2) + metadata_len (4) + metadata (2) +
        // assignment_len (4) + assignment (1) = 23 bytes
        assert_eq!(buf.len(), 23);
    }

    // ========================================================================
    // ListGroups tests
    // ========================================================================

    #[test]
    fn test_list_groups_response_encode() {
        let response = ListGroupsResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            groups: vec![ListedGroup {
                group_id: "g1".to_string(),
                protocol_type: "consumer".to_string(),
                group_state: "Stable".to_string(),
            }],
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + error (2) + groups_len (4) + group_id_len (2) + "g1" (2) +
        // protocol_type_len (2) + "consumer" (8) + state_len (2) + "Stable" (6) = 32 bytes
        assert_eq!(buf.len(), 32);
        // Check error code = 0
        assert_eq!(&buf[4..6], &[0x00, 0x00]);
    }

    #[test]
    fn test_list_groups_response_error() {
        let response = ListGroupsResponseData::error(KafkaCode::GroupCoordinatorNotAvailable);
        assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
        assert!(response.groups.is_empty());
    }

    #[test]
    fn test_list_groups_response_success() {
        let groups = vec![
            ListedGroup::new("group-1", "consumer", "Stable"),
            ListedGroup::new("group-2", "consumer", "Empty"),
        ];
        let response = ListGroupsResponseData::success(groups);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.groups.len(), 2);
        assert_eq!(response.groups[0].group_id, "group-1");
        assert_eq!(response.groups[1].group_state, "Empty");
    }

    #[test]
    fn test_listed_group_new() {
        let group = ListedGroup::new("test-group", "consumer", "Stable");
        assert_eq!(group.group_id, "test-group");
        assert_eq!(group.protocol_type, "consumer");
        assert_eq!(group.group_state, "Stable");
    }

    #[test]
    fn test_listed_group_encode() {
        let group = ListedGroup {
            group_id: "g".to_string(),
            protocol_type: "c".to_string(),
            group_state: "S".to_string(),
        };
        let mut buf = Vec::new();
        group.encode(&mut buf).unwrap();

        // group_id_len (2) + "g" (1) + protocol_type_len (2) + "c" (1) +
        // state_len (2) + "S" (1) = 9 bytes
        assert_eq!(buf.len(), 9);
    }

    // ========================================================================
    // DeleteGroups tests
    // ========================================================================

    #[test]
    fn test_delete_groups_response_encode() {
        let response = DeleteGroupsResponseData {
            throttle_time_ms: 100,
            results: vec![
                DeleteGroupResult {
                    group_id: "g1".to_string(),
                    error_code: KafkaCode::None,
                },
                DeleteGroupResult {
                    group_id: "g2".to_string(),
                    error_code: KafkaCode::InvalidGroupId,
                },
            ],
        };
        let mut buf = Vec::new();
        response.encode(&mut buf).unwrap();

        // throttle (4) + results_len (4) + 2 * (group_id_len (2) + "g1"/"g2" (2) + error (2)) = 20 bytes
        assert_eq!(buf.len(), 20);
        // Check throttle_time_ms = 100
        assert_eq!(&buf[0..4], &[0x00, 0x00, 0x00, 0x64]);
    }

    #[test]
    fn test_delete_groups_response_new() {
        let results = vec![
            DeleteGroupResult::success("group-1"),
            DeleteGroupResult::error("group-2", KafkaCode::InvalidGroupId),
        ];
        let response = DeleteGroupsResponseData::new(results);
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.results.len(), 2);
    }

    #[test]
    fn test_delete_group_result_success() {
        let result = DeleteGroupResult::success("my-group");
        assert_eq!(result.group_id, "my-group");
        assert_eq!(result.error_code, KafkaCode::None);
    }

    #[test]
    fn test_delete_group_result_error() {
        let result = DeleteGroupResult::error("bad-group", KafkaCode::InvalidGroupId);
        assert_eq!(result.group_id, "bad-group");
        assert_eq!(result.error_code, KafkaCode::InvalidGroupId);
    }

    #[test]
    fn test_delete_group_result_encode() {
        let result = DeleteGroupResult {
            group_id: "grp".to_string(),
            error_code: KafkaCode::None,
        };
        let mut buf = Vec::new();
        result.encode(&mut buf).unwrap();

        // group_id_len (2) + "grp" (3) + error (2) = 7 bytes
        assert_eq!(buf.len(), 7);
    }
}
