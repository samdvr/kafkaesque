//! Consumer group-related response encoding.

use bytes::{BufMut, Bytes};

use crate::encode::ToByte;
use crate::error::{KafkaCode, Result};

use super::encode_nullable_string;

// ============================================================================
// FindCoordinator
// ============================================================================

/// FindCoordinator response data.
#[derive(Debug, Clone, Default)]
pub struct FindCoordinatorResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
    pub error_message: Option<String>,
    pub node_id: i32,
    pub host: String,
    pub port: i32,
}

impl FindCoordinatorResponseData {
    /// Create a success response with coordinator info.
    pub fn success(node_id: i32, host: String, port: i32) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            error_message: None,
            node_id,
            host,
            port,
        }
    }

    /// Create an error response.
    pub fn error(error_code: KafkaCode, error_message: Option<String>) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            error_message,
            node_id: -1,
            host: String::new(),
            port: 0,
        }
    }

    /// Create a "coordinator not available" error response.
    pub fn not_available(message: &str) -> Self {
        Self::error(
            KafkaCode::GroupCoordinatorNotAvailable,
            Some(message.to_string()),
        )
    }
}

impl ToByte for FindCoordinatorResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        encode_nullable_string(self.error_message.as_deref(), buffer)?;
        self.node_id.encode(buffer)?;
        self.host.encode(buffer)?;
        self.port.encode(buffer)?;
        Ok(())
    }
}

// ============================================================================
// JoinGroup
// ============================================================================

/// JoinGroup response data.
#[derive(Debug, Clone, Default)]
pub struct JoinGroupResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
    pub generation_id: i32,
    pub protocol_name: String,
    pub leader: String,
    pub member_id: String,
    pub members: Vec<JoinGroupMemberData>,
}

impl JoinGroupResponseData {
    /// Create an error response for join group failures.
    pub fn error(error_code: KafkaCode, member_id: String) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            generation_id: -1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id,
            members: vec![],
        }
    }

    /// Create a builder for constructing a success response.
    pub fn builder() -> JoinGroupResponseBuilder {
        JoinGroupResponseBuilder::default()
    }
}

/// Builder for JoinGroupResponseData.
#[derive(Debug, Default)]
pub struct JoinGroupResponseBuilder {
    generation_id: i32,
    protocol_name: String,
    leader: String,
    member_id: String,
    members: Vec<JoinGroupMemberData>,
}

impl JoinGroupResponseBuilder {
    /// Set the generation ID.
    pub fn generation_id(mut self, id: i32) -> Self {
        self.generation_id = id;
        self
    }

    /// Set the protocol name.
    pub fn protocol_name(mut self, name: impl Into<String>) -> Self {
        self.protocol_name = name.into();
        self
    }

    /// Set the leader member ID.
    pub fn leader(mut self, leader: impl Into<String>) -> Self {
        self.leader = leader.into();
        self
    }

    /// Set this member's ID.
    pub fn member_id(mut self, id: impl Into<String>) -> Self {
        self.member_id = id.into();
        self
    }

    /// Add a member to the group.
    pub fn add_member(mut self, member_id: impl Into<String>, metadata: Bytes) -> Self {
        self.members.push(JoinGroupMemberData {
            member_id: member_id.into(),
            metadata,
        });
        self
    }

    /// Set all members at once.
    pub fn members(mut self, members: Vec<JoinGroupMemberData>) -> Self {
        self.members = members;
        self
    }

    /// Build the response.
    pub fn build(self) -> JoinGroupResponseData {
        JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id: self.generation_id,
            protocol_name: self.protocol_name,
            leader: self.leader,
            member_id: self.member_id,
            members: self.members,
        }
    }
}

#[derive(Debug, Clone)]
pub struct JoinGroupMemberData {
    pub member_id: String,
    pub metadata: Bytes,
}

impl JoinGroupResponseData {
    /// Encode for a specific API version.
    /// - v0-v1: NO throttle_time_ms
    /// - v2+: HAS throttle_time_ms
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 2 {
            self.throttle_time_ms.encode(buffer)?;
        }
        (self.error_code as i16).encode(buffer)?;
        self.generation_id.encode(buffer)?;
        self.protocol_name.encode(buffer)?;
        self.leader.encode(buffer)?;
        self.member_id.encode(buffer)?;

        (self.members.len() as i32).encode(buffer)?;
        for member in &self.members {
            member.member_id.encode(buffer)?;
            (member.metadata.len() as i32).encode(buffer)?;
            buffer.put(member.metadata.as_ref());
        }
        Ok(())
    }
}

impl ToByte for JoinGroupResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v2 format for backwards compatibility
        self.encode_versioned(buffer, 2)
    }
}

// ============================================================================
// Heartbeat
// ============================================================================

/// Heartbeat response data.
#[derive(Debug, Clone, Default)]
pub struct HeartbeatResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
}

impl HeartbeatResponseData {
    /// Encode for a specific API version.
    /// - v0: NO throttle_time_ms
    /// - v1+: HAS throttle_time_ms
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 1 {
            self.throttle_time_ms.encode(buffer)?;
        }
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

impl ToByte for HeartbeatResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v1 format
        self.encode_versioned(buffer, 1)
    }
}

// ============================================================================
// LeaveGroup
// ============================================================================

/// LeaveGroup response data.
#[derive(Debug, Clone, Default)]
pub struct LeaveGroupResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
}

impl LeaveGroupResponseData {
    /// Encode for a specific API version.
    /// - v0: NO throttle_time_ms
    /// - v1+: HAS throttle_time_ms
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 1 {
            self.throttle_time_ms.encode(buffer)?;
        }
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

impl ToByte for LeaveGroupResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v1 format
        self.encode_versioned(buffer, 1)
    }
}

// ============================================================================
// SyncGroup
// ============================================================================

/// SyncGroup response data.
#[derive(Debug, Clone, Default)]
pub struct SyncGroupResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
    pub assignment: Bytes,
}

impl SyncGroupResponseData {
    /// Encode for a specific API version.
    /// - v0: error_code, member_assignment (NO throttle_time_ms)
    /// - v1+: throttle_time_ms, error_code, member_assignment
    pub fn encode_versioned<W: BufMut>(&self, buffer: &mut W, version: i16) -> Result<()> {
        if version >= 1 {
            self.throttle_time_ms.encode(buffer)?;
        }
        (self.error_code as i16).encode(buffer)?;
        (self.assignment.len() as i32).encode(buffer)?;
        buffer.put(self.assignment.as_ref());
        Ok(())
    }
}

impl ToByte for SyncGroupResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        // Default to v1 format for backwards compatibility with existing code
        self.encode_versioned(buffer, 1)
    }
}

// ============================================================================
// DescribeGroups
// ============================================================================

/// DescribeGroups response data.
#[derive(Debug, Clone)]
pub struct DescribeGroupsResponseData {
    pub throttle_time_ms: i32,
    pub groups: Vec<DescribedGroup>,
}

impl DescribeGroupsResponseData {
    /// Create a response with a single group error.
    pub fn error(group_id: String, error_code: KafkaCode) -> Self {
        Self {
            throttle_time_ms: 0,
            groups: vec![DescribedGroup::error(group_id, error_code)],
        }
    }
}

impl ToByte for DescribeGroupsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        (self.groups.len() as i32).encode(buffer)?;
        for group in &self.groups {
            group.encode(buffer)?;
        }
        Ok(())
    }
}

/// Described group in DescribeGroups response.
#[derive(Debug, Clone)]
pub struct DescribedGroup {
    pub error_code: KafkaCode,
    pub group_id: String,
    pub group_state: String,
    pub protocol_type: String,
    pub protocol_data: String,
    pub members: Vec<DescribedGroupMember>,
}

impl DescribedGroup {
    /// Create an error response for a group.
    pub fn error(group_id: String, error_code: KafkaCode) -> Self {
        Self {
            error_code,
            group_id,
            group_state: String::new(),
            protocol_type: String::new(),
            protocol_data: String::new(),
            members: vec![],
        }
    }

    /// Create a builder for constructing a described group.
    pub fn builder(group_id: impl Into<String>) -> DescribedGroupBuilder {
        DescribedGroupBuilder::new(group_id)
    }
}

impl ToByte for DescribedGroup {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (self.error_code as i16).encode(buffer)?;
        self.group_id.encode(buffer)?;
        self.group_state.encode(buffer)?;
        self.protocol_type.encode(buffer)?;
        self.protocol_data.encode(buffer)?;
        (self.members.len() as i32).encode(buffer)?;
        for member in &self.members {
            member.encode(buffer)?;
        }
        Ok(())
    }
}

/// Builder for DescribedGroup.
#[derive(Debug, Default)]
pub struct DescribedGroupBuilder {
    group_id: String,
    group_state: String,
    protocol_type: String,
    protocol_data: String,
    members: Vec<DescribedGroupMember>,
}

impl DescribedGroupBuilder {
    fn new(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            ..Default::default()
        }
    }

    pub fn group_state(mut self, state: impl Into<String>) -> Self {
        self.group_state = state.into();
        self
    }

    pub fn protocol_type(mut self, protocol_type: impl Into<String>) -> Self {
        self.protocol_type = protocol_type.into();
        self
    }

    pub fn protocol_data(mut self, protocol_data: impl Into<String>) -> Self {
        self.protocol_data = protocol_data.into();
        self
    }

    pub fn add_member(mut self, member: DescribedGroupMember) -> Self {
        self.members.push(member);
        self
    }

    pub fn members(mut self, members: Vec<DescribedGroupMember>) -> Self {
        self.members = members;
        self
    }

    pub fn build(self) -> DescribedGroup {
        DescribedGroup {
            error_code: KafkaCode::None,
            group_id: self.group_id,
            group_state: self.group_state,
            protocol_type: self.protocol_type,
            protocol_data: self.protocol_data,
            members: self.members,
        }
    }
}

/// Member in a described group.
#[derive(Debug, Clone)]
pub struct DescribedGroupMember {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub member_metadata: Bytes,
    pub member_assignment: Bytes,
}

impl DescribedGroupMember {
    pub fn new(
        member_id: impl Into<String>,
        client_id: impl Into<String>,
        client_host: impl Into<String>,
        member_metadata: Bytes,
        member_assignment: Bytes,
    ) -> Self {
        Self {
            member_id: member_id.into(),
            client_id: client_id.into(),
            client_host: client_host.into(),
            member_metadata,
            member_assignment,
        }
    }
}

impl ToByte for DescribedGroupMember {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.member_id.encode(buffer)?;
        self.client_id.encode(buffer)?;
        self.client_host.encode(buffer)?;
        (self.member_metadata.len() as i32).encode(buffer)?;
        buffer.put(self.member_metadata.as_ref());
        (self.member_assignment.len() as i32).encode(buffer)?;
        buffer.put(self.member_assignment.as_ref());
        Ok(())
    }
}

// ============================================================================
// ListGroups
// ============================================================================

/// ListGroups response data.
#[derive(Debug, Clone)]
pub struct ListGroupsResponseData {
    pub throttle_time_ms: i32,
    pub error_code: KafkaCode,
    pub groups: Vec<ListedGroup>,
}

impl ListGroupsResponseData {
    /// Create an error response.
    pub fn error(error_code: KafkaCode) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code,
            groups: vec![],
        }
    }

    /// Create a success response with listed groups.
    pub fn success(groups: Vec<ListedGroup>) -> Self {
        Self {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            groups,
        }
    }
}

impl ToByte for ListGroupsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        (self.groups.len() as i32).encode(buffer)?;
        for group in &self.groups {
            group.encode(buffer)?;
        }
        Ok(())
    }
}

/// Listed group in ListGroups response.
#[derive(Debug, Clone)]
pub struct ListedGroup {
    pub group_id: String,
    pub protocol_type: String,
    pub group_state: String,
}

impl ListedGroup {
    pub fn new(
        group_id: impl Into<String>,
        protocol_type: impl Into<String>,
        group_state: impl Into<String>,
    ) -> Self {
        Self {
            group_id: group_id.into(),
            protocol_type: protocol_type.into(),
            group_state: group_state.into(),
        }
    }
}

impl ToByte for ListedGroup {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.group_id.encode(buffer)?;
        self.protocol_type.encode(buffer)?;
        self.group_state.encode(buffer)?;
        Ok(())
    }
}

// ============================================================================
// DeleteGroups
// ============================================================================

/// DeleteGroups response data.
#[derive(Debug, Clone)]
pub struct DeleteGroupsResponseData {
    pub throttle_time_ms: i32,
    pub results: Vec<DeleteGroupResult>,
}

impl DeleteGroupsResponseData {
    /// Create a response from results.
    pub fn new(results: Vec<DeleteGroupResult>) -> Self {
        Self {
            throttle_time_ms: 0,
            results,
        }
    }
}

impl ToByte for DeleteGroupsResponseData {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.throttle_time_ms.encode(buffer)?;
        (self.results.len() as i32).encode(buffer)?;
        for result in &self.results {
            result.encode(buffer)?;
        }
        Ok(())
    }
}

/// Result for a single group deletion.
#[derive(Debug, Clone)]
pub struct DeleteGroupResult {
    pub group_id: String,
    pub error_code: KafkaCode,
}

impl DeleteGroupResult {
    pub fn success(group_id: impl Into<String>) -> Self {
        Self {
            group_id: group_id.into(),
            error_code: KafkaCode::None,
        }
    }

    pub fn error(group_id: impl Into<String>, error_code: KafkaCode) -> Self {
        Self {
            group_id: group_id.into(),
            error_code,
        }
    }
}

impl ToByte for DeleteGroupResult {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        self.group_id.encode(buffer)?;
        (self.error_code as i16).encode(buffer)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // =========================================================================
    // JoinGroup version-specific encoding tests
    // =========================================================================

    #[test]
    fn test_join_group_response_v0_no_throttle_time() {
        let response = JoinGroupResponseData {
            throttle_time_ms: 100, // Should be ignored in v0-v1
            error_code: KafkaCode::None,
            generation_id: 1,
            protocol_name: "range".to_string(),
            leader: "leader-1".to_string(),
            member_id: "member-1".to_string(),
            members: vec![],
        };

        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();

        let mut buf_v2 = Vec::new();
        response.encode_versioned(&mut buf_v2, 2).unwrap();

        // v0 should be 4 bytes smaller (no throttle_time_ms)
        assert_eq!(buf_v0.len() + 4, buf_v2.len());

        // v0 should NOT start with throttle_time_ms (100 = 0x00000064)
        assert_ne!(&buf_v0[0..4], &[0, 0, 0, 100]);

        // v2 SHOULD start with throttle_time_ms
        assert_eq!(&buf_v2[0..4], &[0, 0, 0, 100]);
    }

    #[test]
    fn test_join_group_response_v1_no_throttle_time() {
        let response = JoinGroupResponseData {
            throttle_time_ms: 50,
            error_code: KafkaCode::None,
            generation_id: 1,
            protocol_name: "range".to_string(),
            leader: "leader".to_string(),
            member_id: "member".to_string(),
            members: vec![],
        };

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        // v1 should NOT have throttle_time_ms
        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();
        assert_eq!(buf_v0.len(), buf_v1.len());
    }

    // =========================================================================
    // SyncGroup version-specific encoding tests
    // =========================================================================

    #[test]
    fn test_sync_group_response_v0_no_throttle_time() {
        let response = SyncGroupResponseData {
            throttle_time_ms: 100,
            error_code: KafkaCode::None,
            assignment: Bytes::from(vec![1, 2, 3, 4, 5]),
        };

        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        // v0 should be 4 bytes smaller (no throttle_time_ms)
        assert_eq!(buf_v0.len() + 4, buf_v1.len());

        // v0 format: error_code (2) + assignment_len (4) + assignment (5) = 11 bytes
        assert_eq!(buf_v0.len(), 11);

        // v1 format: throttle_time_ms (4) + error_code (2) + assignment_len (4) + assignment (5) = 15 bytes
        assert_eq!(buf_v1.len(), 15);
    }

    // =========================================================================
    // Heartbeat version-specific encoding tests
    // =========================================================================

    #[test]
    fn test_heartbeat_response_v0_no_throttle_time() {
        let response = HeartbeatResponseData {
            throttle_time_ms: 100,
            error_code: KafkaCode::None,
        };

        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        // v0: just error_code (2 bytes)
        assert_eq!(buf_v0.len(), 2);

        // v1: throttle_time_ms (4) + error_code (2) = 6 bytes
        assert_eq!(buf_v1.len(), 6);
    }

    // =========================================================================
    // LeaveGroup version-specific encoding tests
    // =========================================================================

    #[test]
    fn test_leave_group_response_v0_no_throttle_time() {
        let response = LeaveGroupResponseData {
            throttle_time_ms: 100,
            error_code: KafkaCode::None,
        };

        let mut buf_v0 = Vec::new();
        response.encode_versioned(&mut buf_v0, 0).unwrap();

        let mut buf_v1 = Vec::new();
        response.encode_versioned(&mut buf_v1, 1).unwrap();

        // v0: just error_code (2 bytes)
        assert_eq!(buf_v0.len(), 2);

        // v1: throttle_time_ms (4) + error_code (2) = 6 bytes
        assert_eq!(buf_v1.len(), 6);
    }
}
