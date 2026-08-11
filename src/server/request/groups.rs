//! Consumer group-related request parsing.

use bytes::Bytes;
use nom::{
    IResult,
    number::complete::{be_i8, be_i32},
};
use nombytes::NomBytes;

use crate::constants::MAX_MEMBER_METADATA_SIZE;
use crate::parser::{parse_array, parse_kafka_string, parse_nullable_string};

// ============================================================================
// FindCoordinator
// ============================================================================

/// FindCoordinator request data.
#[derive(Debug, Clone)]
pub struct FindCoordinatorRequestData {
    pub key: String,
    pub key_type: i8,
}

pub fn parse_find_coordinator_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, FindCoordinatorRequestData> {
    let (s, key) = parse_kafka_string(s)?;
    let (s, key_type) = if version >= 1 { be_i8(s)? } else { (s, 0i8) };

    Ok((s, FindCoordinatorRequestData { key, key_type }))
}

// ============================================================================
// JoinGroup
// ============================================================================

/// JoinGroup request data.
#[derive(Debug, Clone)]
pub struct JoinGroupRequestData {
    pub group_id: String,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub member_id: String,
    pub protocol_type: String,
    pub protocols: Vec<JoinGroupProtocolData>,
}

#[derive(Debug, Clone)]
pub struct JoinGroupProtocolData {
    pub name: String,
    pub metadata: Bytes,
}

/// Parse a JoinGroup request.
///
/// Per-version wire layout (Kafka protocol spec, JoinGroupRequest):
/// - v0: `group_id`, `session_timeout_ms`, `member_id`, `protocol_type`,
///   `[protocols]`
/// - v1: adds `rebalance_timeout_ms` (INT32) between `session_timeout_ms`
///   and `member_id` (KIP-62). For v0 clients Kafka uses the session
///   timeout as the rebalance timeout, mirrored here.
/// - v2–v4: identical request layout to v1 (v2/v3 only changed the
///   *response*; v4 is MemberIdRequired behavior in the handler).
/// - v5: adds `group_instance_id` (KIP-345) — NOT advertised; rejected
///   with UnsupportedVersion before body parsing.
pub fn parse_join_group_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, JoinGroupRequestData> {
    let (s, group_id) = parse_kafka_string(s)?;
    let (s, session_timeout_ms) = be_i32(s)?;
    // rebalance_timeout_ms: added in v1 (KIP-62); defaults to the session
    // timeout for v0, matching the Kafka broker's up-conversion.
    let (s, rebalance_timeout_ms) = if version >= 1 {
        be_i32(s)?
    } else {
        (s, session_timeout_ms)
    };
    let (s, member_id) = parse_kafka_string(s)?;
    let (s, protocol_type) = parse_kafka_string(s)?;
    let (s, protocols) = parse_array(parse_join_group_protocol)(s)?;

    Ok((
        s,
        JoinGroupRequestData {
            group_id,
            session_timeout_ms,
            rebalance_timeout_ms,
            member_id,
            protocol_type,
            protocols,
        },
    ))
}

fn parse_join_group_protocol(s: NomBytes) -> IResult<NomBytes, JoinGroupProtocolData> {
    let (s, name) = parse_kafka_string(s)?;
    let (s, metadata_len) = be_i32(s)?;
    if metadata_len < 0 || metadata_len as usize > MAX_MEMBER_METADATA_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            s,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (s, metadata) = nom::bytes::complete::take(metadata_len as usize)(s)?;

    Ok((
        s,
        JoinGroupProtocolData {
            name,
            metadata: metadata.into_bytes(),
        },
    ))
}

// ============================================================================
// Heartbeat
// ============================================================================

/// Heartbeat request data.
#[derive(Debug, Clone)]
pub struct HeartbeatRequestData {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
}

pub fn parse_heartbeat_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, HeartbeatRequestData> {
    let (s, group_id) = parse_kafka_string(s)?;
    let (s, generation_id) = be_i32(s)?;
    let (s, member_id) = parse_kafka_string(s)?;
    // v3+ group_instance_id (KIP-345): parse and ignore — static membership
    // requires JoinGroup v5 which we refuse, so an instance id here is inert.
    let (s, _group_instance_id) = if version >= 3 {
        parse_nullable_string(s)?
    } else {
        (s, None)
    };

    Ok((
        s,
        HeartbeatRequestData {
            group_id,
            generation_id,
            member_id,
        },
    ))
}

// ============================================================================
// LeaveGroup
// ============================================================================

/// One member entry in a LeaveGroup v3+ batch leave.
#[derive(Debug, Clone)]
pub struct LeaveGroupMemberData {
    pub member_id: String,
    /// Present on the wire for v3+; ignored (static membership not supported).
    pub group_instance_id: Option<String>,
}

/// LeaveGroup request data.
#[derive(Debug, Clone)]
pub struct LeaveGroupRequestData {
    pub group_id: String,
    /// Single-member id for v0–v2; empty when `members` carries the roster.
    pub member_id: String,
    /// Populated for every version: one entry for v0–v2, the batch for v3+.
    pub members: Vec<LeaveGroupMemberData>,
}

impl LeaveGroupRequestData {
    /// Classic single-member leave (v0–v2 wire shape).
    pub fn for_member(group_id: impl Into<String>, member_id: impl Into<String>) -> Self {
        let member_id = member_id.into();
        Self {
            group_id: group_id.into(),
            members: vec![LeaveGroupMemberData {
                member_id: member_id.clone(),
                group_instance_id: None,
            }],
            member_id,
        }
    }
}

pub fn parse_leave_group_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, LeaveGroupRequestData> {
    let (s, group_id) = parse_kafka_string(s)?;
    if version >= 3 {
        let (s, members) = parse_array(parse_leave_group_member)(s)?;
        let member_id = members
            .first()
            .map(|m| m.member_id.clone())
            .unwrap_or_default();
        Ok((
            s,
            LeaveGroupRequestData {
                group_id,
                member_id,
                members,
            },
        ))
    } else {
        let (s, member_id) = parse_kafka_string(s)?;
        let members = vec![LeaveGroupMemberData {
            member_id: member_id.clone(),
            group_instance_id: None,
        }];
        Ok((
            s,
            LeaveGroupRequestData {
                group_id,
                member_id,
                members,
            },
        ))
    }
}

fn parse_leave_group_member(s: NomBytes) -> IResult<NomBytes, LeaveGroupMemberData> {
    let (s, member_id) = parse_kafka_string(s)?;
    let (s, group_instance_id) = parse_nullable_string(s)?;
    Ok((
        s,
        LeaveGroupMemberData {
            member_id,
            group_instance_id: group_instance_id.map(|b| String::from_utf8_lossy(&b).into_owned()),
        },
    ))
}

// ============================================================================
// SyncGroup
// ============================================================================

/// SyncGroup request data.
#[derive(Debug, Clone)]
pub struct SyncGroupRequestData {
    pub group_id: String,
    pub generation_id: i32,
    pub member_id: String,
    pub assignments: Vec<SyncGroupAssignmentData>,
}

#[derive(Debug, Clone)]
pub struct SyncGroupAssignmentData {
    pub member_id: String,
    pub assignment: Bytes,
}

pub fn parse_sync_group_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, SyncGroupRequestData> {
    let (s, group_id) = parse_kafka_string(s)?;
    let (s, generation_id) = be_i32(s)?;
    let (s, member_id) = parse_kafka_string(s)?;
    // v3+ group_instance_id: parse and ignore (see Heartbeat).
    let (s, _group_instance_id) = if version >= 3 {
        parse_nullable_string(s)?
    } else {
        (s, None)
    };
    let (s, assignments) = parse_array(parse_sync_group_assignment)(s)?;

    Ok((
        s,
        SyncGroupRequestData {
            group_id,
            generation_id,
            member_id,
            assignments,
        },
    ))
}

fn parse_sync_group_assignment(s: NomBytes) -> IResult<NomBytes, SyncGroupAssignmentData> {
    let (s, member_id) = parse_kafka_string(s)?;
    let (s, assignment_len) = be_i32(s)?;
    if assignment_len < 0 || assignment_len as usize > MAX_MEMBER_METADATA_SIZE {
        return Err(nom::Err::Error(nom::error::Error::new(
            s,
            nom::error::ErrorKind::Verify,
        )));
    }
    let (s, assignment) = nom::bytes::complete::take(assignment_len as usize)(s)?;

    Ok((
        s,
        SyncGroupAssignmentData {
            member_id,
            assignment: assignment.into_bytes(),
        },
    ))
}

// ============================================================================
// DescribeGroups
// ============================================================================

/// DescribeGroups request data.
#[derive(Debug, Clone)]
pub struct DescribeGroupsRequestData {
    pub group_ids: Vec<String>,
}

pub fn parse_describe_groups_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, DescribeGroupsRequestData> {
    let (s, group_ids) = parse_array(parse_kafka_string)(s)?;

    Ok((s, DescribeGroupsRequestData { group_ids }))
}

// ============================================================================
// ListGroups
// ============================================================================

/// ListGroups request data.
#[derive(Debug, Clone)]
pub struct ListGroupsRequestData {
    pub states_filter: Vec<String>,
}

pub fn parse_list_groups_request(
    s: NomBytes,
    version: i16,
) -> IResult<NomBytes, ListGroupsRequestData> {
    // Version 0-3 have no request body
    // Version 4+ have states_filter
    let (s, states_filter) = if version >= 4 {
        parse_array(parse_kafka_string)(s)?
    } else {
        (s, vec![])
    };

    Ok((s, ListGroupsRequestData { states_filter }))
}

// ============================================================================
// DeleteGroups
// ============================================================================

/// DeleteGroups request data.
#[derive(Debug, Clone)]
pub struct DeleteGroupsRequestData {
    pub group_ids: Vec<String>,
}

pub fn parse_delete_groups_request(
    s: NomBytes,
    _version: i16,
) -> IResult<NomBytes, DeleteGroupsRequestData> {
    let (s, group_ids) = parse_array(parse_kafka_string)(s)?;

    Ok((s, DeleteGroupsRequestData { group_ids }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_nom_bytes(data: &[u8]) -> NomBytes {
        NomBytes::from(data)
    }

    fn build_string(s: &str, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&(s.len() as i16).to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
    }

    #[test]
    fn test_parse_find_coordinator_request_v0() {
        let mut data = Vec::new();
        build_string("test-group", &mut data);

        let input = create_nom_bytes(&data);
        let result = parse_find_coordinator_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.key, "test-group");
        assert_eq!(request.key_type, 0); // Default for v0
    }

    #[test]
    fn test_parse_find_coordinator_request_v1() {
        let mut data = Vec::new();
        build_string("test-group", &mut data);
        data.push(0); // key_type: GROUP

        let input = create_nom_bytes(&data);
        let result = parse_find_coordinator_request(input, 1);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.key, "test-group");
        assert_eq!(request.key_type, 0);
    }

    #[test]
    fn test_parse_join_group_request_v0() {
        let mut data = Vec::new();
        build_string("consumer-group", &mut data);
        data.extend_from_slice(&30000i32.to_be_bytes()); // session_timeout_ms
        // v0 doesn't have rebalance_timeout_ms, it defaults to session_timeout_ms
        build_string("", &mut data); // member_id (empty for new member)
        build_string("consumer", &mut data); // protocol_type
        // protocols array with 1 entry
        data.extend_from_slice(&1i32.to_be_bytes());
        build_string("range", &mut data); // protocol name
        let metadata = b"test-metadata";
        data.extend_from_slice(&(metadata.len() as i32).to_be_bytes());
        data.extend_from_slice(metadata);

        let input = create_nom_bytes(&data);
        let result = parse_join_group_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_id, "consumer-group");
        assert_eq!(request.session_timeout_ms, 30000);
        assert_eq!(request.rebalance_timeout_ms, 30000); // Defaults to session_timeout for v0
        assert_eq!(request.member_id, "");
        assert_eq!(request.protocol_type, "consumer");
        assert_eq!(request.protocols.len(), 1);
        assert_eq!(request.protocols[0].name, "range");
        assert_eq!(request.protocols[0].metadata.as_ref(), b"test-metadata");
    }

    #[test]
    fn test_parse_join_group_request_v1() {
        let mut data = Vec::new();
        build_string("my-consumer-group", &mut data);
        data.extend_from_slice(&45000i32.to_be_bytes()); // session_timeout_ms
        data.extend_from_slice(&60000i32.to_be_bytes()); // rebalance_timeout_ms (v1+)
        build_string("member-abc-123", &mut data); // member_id
        build_string("consumer", &mut data); // protocol_type
        // protocols array with 2 entries
        data.extend_from_slice(&2i32.to_be_bytes());
        // Protocol 1
        build_string("range", &mut data);
        let metadata1 = b"\x00\x01\x02";
        data.extend_from_slice(&(metadata1.len() as i32).to_be_bytes());
        data.extend_from_slice(metadata1);
        // Protocol 2
        build_string("roundrobin", &mut data);
        let metadata2 = b"\x03\x04\x05\x06";
        data.extend_from_slice(&(metadata2.len() as i32).to_be_bytes());
        data.extend_from_slice(metadata2);

        let input = create_nom_bytes(&data);
        let result = parse_join_group_request(input, 1);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_id, "my-consumer-group");
        assert_eq!(request.session_timeout_ms, 45000);
        assert_eq!(request.rebalance_timeout_ms, 60000);
        assert_eq!(request.member_id, "member-abc-123");
        assert_eq!(request.protocol_type, "consumer");
        assert_eq!(request.protocols.len(), 2);
        assert_eq!(request.protocols[0].name, "range");
        assert_eq!(request.protocols[0].metadata.as_ref(), b"\x00\x01\x02");
        assert_eq!(request.protocols[1].name, "roundrobin");
        assert_eq!(request.protocols[1].metadata.as_ref(), b"\x03\x04\x05\x06");
    }

    #[test]
    fn test_parse_join_group_request_v2_same_layout_as_v1() {
        // JoinGroup v2 changed only the RESPONSE (added throttle_time_ms);
        // the request layout is identical to v1 — and in particular there is
        // NO group_instance_id (that arrived in v5, which is not advertised).
        let mut data = Vec::new();
        build_string("g2", &mut data);
        data.extend_from_slice(&20000i32.to_be_bytes()); // session_timeout_ms
        data.extend_from_slice(&30000i32.to_be_bytes()); // rebalance_timeout_ms (v1+)
        build_string("member-x", &mut data); // member_id
        build_string("consumer", &mut data); // protocol_type
        data.extend_from_slice(&1i32.to_be_bytes());
        build_string("range", &mut data);
        let metadata = b"\x0A\x0B";
        data.extend_from_slice(&(metadata.len() as i32).to_be_bytes());
        data.extend_from_slice(metadata);

        let input = create_nom_bytes(&data);
        let (rest, request) = parse_join_group_request(input, 2).unwrap();
        assert!(
            rest.into_bytes().is_empty(),
            "v2 body must be fully consumed"
        );
        assert_eq!(request.group_id, "g2");
        assert_eq!(request.session_timeout_ms, 20000);
        assert_eq!(request.rebalance_timeout_ms, 30000);
        assert_eq!(request.member_id, "member-x");
        assert_eq!(request.protocols.len(), 1);
        assert_eq!(request.protocols[0].metadata.as_ref(), b"\x0A\x0B");
    }

    #[test]
    fn test_parse_join_group_request_empty_protocols() {
        let mut data = Vec::new();
        build_string("group", &mut data);
        data.extend_from_slice(&10000i32.to_be_bytes()); // session_timeout_ms
        data.extend_from_slice(&15000i32.to_be_bytes()); // rebalance_timeout_ms
        build_string("", &mut data); // member_id
        build_string("consumer", &mut data); // protocol_type
        // Empty protocols array
        data.extend_from_slice(&0i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_join_group_request(input, 1);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_id, "group");
        assert_eq!(request.session_timeout_ms, 10000);
        assert_eq!(request.rebalance_timeout_ms, 15000);
        assert!(request.protocols.is_empty());
    }

    #[test]
    fn test_parse_heartbeat_request() {
        let mut data = Vec::new();
        build_string("my-group", &mut data);
        data.extend_from_slice(&1i32.to_be_bytes()); // generation_id
        build_string("member-123", &mut data);

        let input = create_nom_bytes(&data);
        let result = parse_heartbeat_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.generation_id, 1);
        assert_eq!(request.member_id, "member-123");
    }

    #[test]
    fn test_parse_leave_group_request() {
        let mut data = Vec::new();
        build_string("group1", &mut data);
        build_string("member-1", &mut data);

        let input = create_nom_bytes(&data);
        let result = parse_leave_group_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_id, "group1");
        assert_eq!(request.member_id, "member-1");
    }

    #[test]
    fn test_parse_sync_group_request_leader() {
        let mut data = Vec::new();
        build_string("consumer-group", &mut data);
        data.extend_from_slice(&1i32.to_be_bytes()); // generation_id
        build_string("member-leader-123", &mut data); // member_id (leader sends assignments)
        // assignments array with 2 entries
        data.extend_from_slice(&2i32.to_be_bytes());
        // Assignment 1
        build_string("member-leader-123", &mut data);
        let assignment1 = b"\x00\x01topic\x00\x00\x00\x01\x00\x00\x00\x00"; // mock assignment
        data.extend_from_slice(&(assignment1.len() as i32).to_be_bytes());
        data.extend_from_slice(assignment1);
        // Assignment 2
        build_string("member-follower-456", &mut data);
        let assignment2 = b"\x00\x01topic\x00\x00\x00\x01\x00\x00\x00\x01"; // mock assignment
        data.extend_from_slice(&(assignment2.len() as i32).to_be_bytes());
        data.extend_from_slice(assignment2);

        let input = create_nom_bytes(&data);
        let result = parse_sync_group_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_id, "consumer-group");
        assert_eq!(request.generation_id, 1);
        assert_eq!(request.member_id, "member-leader-123");
        assert_eq!(request.assignments.len(), 2);
        assert_eq!(request.assignments[0].member_id, "member-leader-123");
        assert_eq!(request.assignments[0].assignment.as_ref(), assignment1);
        assert_eq!(request.assignments[1].member_id, "member-follower-456");
        assert_eq!(request.assignments[1].assignment.as_ref(), assignment2);
    }

    #[test]
    fn test_parse_sync_group_request_follower() {
        // Followers send empty assignments array
        let mut data = Vec::new();
        build_string("my-group", &mut data);
        data.extend_from_slice(&5i32.to_be_bytes()); // generation_id
        build_string("member-follower-789", &mut data);
        // Empty assignments array (followers don't send assignments)
        data.extend_from_slice(&0i32.to_be_bytes());

        let input = create_nom_bytes(&data);
        let result = parse_sync_group_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_id, "my-group");
        assert_eq!(request.generation_id, 5);
        assert_eq!(request.member_id, "member-follower-789");
        assert!(request.assignments.is_empty());
    }

    #[test]
    fn test_parse_describe_groups_request() {
        let mut data = Vec::new();
        // Array with 2 groups
        data.extend_from_slice(&2i32.to_be_bytes());
        build_string("group-a", &mut data);
        build_string("group-b", &mut data);

        let input = create_nom_bytes(&data);
        let result = parse_describe_groups_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_ids.len(), 2);
        assert_eq!(request.group_ids[0], "group-a");
        assert_eq!(request.group_ids[1], "group-b");
    }

    #[test]
    fn test_parse_list_groups_request_v0() {
        let data = Vec::new(); // v0 has no request body
        let input = create_nom_bytes(&data);
        let result = parse_list_groups_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert!(request.states_filter.is_empty());
    }

    #[test]
    fn test_parse_list_groups_request_v4() {
        let mut data = Vec::new();
        // Array with 1 state filter
        data.extend_from_slice(&1i32.to_be_bytes());
        build_string("Stable", &mut data);

        let input = create_nom_bytes(&data);
        let result = parse_list_groups_request(input, 4);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.states_filter.len(), 1);
        assert_eq!(request.states_filter[0], "Stable");
    }

    #[test]
    fn test_parse_delete_groups_request() {
        let mut data = Vec::new();
        // Array with 1 group
        data.extend_from_slice(&1i32.to_be_bytes());
        build_string("old-group", &mut data);

        let input = create_nom_bytes(&data);
        let result = parse_delete_groups_request(input, 0);

        assert!(result.is_ok());
        let (_, request) = result.unwrap();
        assert_eq!(request.group_ids.len(), 1);
        assert_eq!(request.group_ids[0], "old-group");
    }

    #[test]
    fn test_request_data_debug() {
        let find_coord = FindCoordinatorRequestData {
            key: "group".to_string(),
            key_type: 0,
        };
        assert!(format!("{:?}", find_coord).contains("FindCoordinatorRequestData"));

        let heartbeat = HeartbeatRequestData {
            group_id: "g".to_string(),
            generation_id: 1,
            member_id: "m".to_string(),
        };
        assert!(format!("{:?}", heartbeat).contains("HeartbeatRequestData"));

        let leave = LeaveGroupRequestData::for_member("g".to_string(), "m".to_string());
        assert!(format!("{:?}", leave).contains("LeaveGroupRequestData"));

        let describe = DescribeGroupsRequestData {
            group_ids: vec!["g".to_string()],
        };
        assert!(format!("{:?}", describe).contains("DescribeGroupsRequestData"));

        let list = ListGroupsRequestData {
            states_filter: vec![],
        };
        assert!(format!("{:?}", list).contains("ListGroupsRequestData"));

        let delete = DeleteGroupsRequestData { group_ids: vec![] };
        assert!(format!("{:?}", delete).contains("DeleteGroupsRequestData"));

        let join_group = JoinGroupRequestData {
            group_id: "g".to_string(),
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            member_id: "m".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![JoinGroupProtocolData {
                name: "range".to_string(),
                metadata: Bytes::from_static(b"meta"),
            }],
        };
        assert!(format!("{:?}", join_group).contains("JoinGroupRequestData"));

        let sync_group = SyncGroupRequestData {
            group_id: "g".to_string(),
            generation_id: 1,
            member_id: "m".to_string(),
            assignments: vec![],
        };
        assert!(format!("{:?}", sync_group).contains("SyncGroupRequestData"));
    }
}
