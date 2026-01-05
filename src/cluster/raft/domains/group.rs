//! Consumer group domain for the Raft state machine.
//!
//! Handles consumer group coordination, offsets, and membership.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// State of a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GroupStateEnum {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
    Dead,
}

/// State of a consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Vec<u8>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub last_heartbeat_ms: u64,
}

/// Member description for JoinGroup response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberDescription {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Vec<u8>,
}

/// Consumer group state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroupInfo {
    pub group_id: String,
    pub generation: i32,
    pub state: GroupStateEnum,
    pub leader_id: Option<String>,
    pub members: HashMap<String, MemberInfo>,
    pub assignments: HashMap<String, Vec<u8>>,
    pub protocol_type: Option<String>,
    pub protocol_name: Option<String>,
}

impl Default for ConsumerGroupInfo {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            generation: 0,
            state: GroupStateEnum::Empty,
            leader_id: None,
            members: HashMap::new(),
            assignments: HashMap::new(),
            protocol_type: None,
            protocol_name: None,
        }
    }
}

impl ConsumerGroupInfo {
    /// Safely increment generation with overflow handling.
    pub fn increment_generation(&mut self) {
        self.generation = if self.generation == i32::MAX {
            tracing::warn!(
                group_id = %self.group_id,
                "Consumer group generation ID wrapped from i32::MAX to 1"
            );
            1
        } else {
            self.generation + 1
        };
    }
}

/// Committed offset for a consumer group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedOffsetInfo {
    pub offset: i64,
    pub metadata: Option<String>,
    pub commit_timestamp_ms: u64,
}

/// Commands for the group domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GroupCommand {
    /// Join a consumer group.
    JoinGroup {
        group_id: String,
        member_id: String,
        client_id: String,
        client_host: String,
        protocol_type: String,
        protocols: Vec<(String, Vec<u8>)>,
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
        timestamp_ms: u64,
    },

    /// Sync group (leader submits assignments).
    SyncGroup {
        group_id: String,
        member_id: String,
        generation: i32,
        assignments: Vec<(String, Vec<u8>)>,
    },

    /// Member heartbeat.
    Heartbeat {
        group_id: String,
        member_id: String,
        generation: i32,
        timestamp_ms: u64,
    },

    /// Leave a consumer group.
    LeaveGroup { group_id: String, member_id: String },

    /// Expire members that haven't sent heartbeat within session timeout.
    ExpireMembers { current_time_ms: u64 },

    /// Delete a consumer group.
    DeleteGroup { group_id: String },

    /// Complete rebalance (transition group to Stable state).
    CompleteRebalance { group_id: String, generation: i32 },

    /// Trigger a rebalance for a consumer group.
    TriggerRebalance { group_id: String },

    /// Commit an offset.
    CommitOffset {
        group_id: String,
        topic: String,
        partition: i32,
        offset: i64,
        metadata: Option<String>,
        timestamp_ms: u64,
    },
}

/// Responses from group domain operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GroupResponse {
    /// Join group response.
    JoinGroupResponse {
        generation: i32,
        leader_id: String,
        member_id: String,
        members: Vec<MemberDescription>,
        protocol_name: String,
    },

    /// Sync group response (assignment for this member).
    SyncGroupResponse { assignment: Vec<u8> },

    /// Heartbeat was recorded.
    HeartbeatAck,

    /// Member left group.
    LeftGroup,

    /// Members were expired.
    MembersExpired { expired: Vec<(String, String)> },

    /// Group was deleted.
    GroupDeleted { group_id: String },

    /// Rebalance completed.
    RebalanceCompleted { group_id: String },

    /// Rebalance triggered.
    RebalanceTriggered { group_id: String },

    /// Offset was committed.
    OffsetCommitted,

    /// Group was not found.
    GroupNotFound { group_id: String },

    /// Member was not found in group.
    UnknownMember { group_id: String, member_id: String },

    /// Generation ID mismatch.
    IllegalGeneration {
        group_id: String,
        expected: i32,
        actual: i32,
    },

    /// Generic success.
    Ok,
}

/// State for the group domain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GroupDomainState {
    /// Consumer groups.
    pub groups: HashMap<String, ConsumerGroupInfo>,

    /// Committed offsets: (group_id, topic, partition) -> CommittedOffsetInfo.
    pub offsets: HashMap<(String, Arc<str>, i32), CommittedOffsetInfo>,
}

impl GroupDomainState {
    /// Create a new empty group state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a group command and return the response.
    pub fn apply(&mut self, cmd: GroupCommand) -> GroupResponse {
        match cmd {
            GroupCommand::JoinGroup {
                group_id,
                member_id,
                client_id,
                client_host,
                protocol_type,
                protocols,
                session_timeout_ms,
                rebalance_timeout_ms,
                timestamp_ms,
            } => {
                let group =
                    self.groups
                        .entry(group_id.clone())
                        .or_insert_with(|| ConsumerGroupInfo {
                            group_id: group_id.clone(),
                            generation: 0,
                            state: GroupStateEnum::Empty,
                            leader_id: None,
                            members: HashMap::new(),
                            assignments: HashMap::new(),
                            protocol_type: Some(protocol_type.clone()),
                            protocol_name: None,
                        });

                // Determine member ID (generate if empty)
                let final_member_id = if member_id.is_empty() {
                    format!("{}-{}", client_id, uuid::Uuid::new_v4())
                } else {
                    member_id
                };

                // Get metadata from first protocol
                let metadata = protocols
                    .first()
                    .map(|(_, m)| m.clone())
                    .unwrap_or_default();
                let protocol_name = protocols
                    .first()
                    .map(|(n, _)| n.clone())
                    .unwrap_or_default();

                // Add member
                group.members.insert(
                    final_member_id.clone(),
                    MemberInfo {
                        member_id: final_member_id.clone(),
                        client_id,
                        client_host,
                        metadata,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        last_heartbeat_ms: timestamp_ms,
                    },
                );

                // Set leader if this is the first member
                let is_leader = group.leader_id.is_none();
                if is_leader {
                    group.leader_id = Some(final_member_id.clone());
                }

                // Trigger rebalance
                group.increment_generation();
                group.state = GroupStateEnum::PreparingRebalance;
                group.protocol_name = Some(protocol_name.clone());
                group.assignments.clear();

                let leader_id = group.leader_id.clone().unwrap_or_default();

                // Build member descriptions (only for leader)
                let members: Vec<MemberDescription> = if is_leader {
                    group
                        .members
                        .values()
                        .map(|m| MemberDescription {
                            member_id: m.member_id.clone(),
                            client_id: m.client_id.clone(),
                            client_host: m.client_host.clone(),
                            metadata: m.metadata.clone(),
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                GroupResponse::JoinGroupResponse {
                    generation: group.generation,
                    leader_id,
                    member_id: final_member_id,
                    members,
                    protocol_name,
                }
            }

            GroupCommand::SyncGroup {
                group_id,
                member_id,
                generation,
                assignments,
            } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    if group.generation != generation {
                        return GroupResponse::IllegalGeneration {
                            group_id,
                            expected: group.generation,
                            actual: generation,
                        };
                    }

                    if !group.members.contains_key(&member_id) {
                        return GroupResponse::UnknownMember {
                            group_id,
                            member_id,
                        };
                    }

                    // If leader, store assignments
                    if group.leader_id.as_ref() == Some(&member_id) {
                        for (target_member, assignment) in assignments {
                            if group.members.contains_key(&target_member) {
                                group.assignments.insert(target_member, assignment);
                            } else {
                                tracing::warn!(
                                    group_id = %group_id,
                                    target_member = %target_member,
                                    "Skipping assignment to unknown member"
                                );
                            }
                        }
                        group.state = GroupStateEnum::CompletingRebalance;
                    }

                    // Return assignment for this member
                    let assignment = group
                        .assignments
                        .get(&member_id)
                        .cloned()
                        .unwrap_or_default();

                    GroupResponse::SyncGroupResponse { assignment }
                } else {
                    GroupResponse::GroupNotFound { group_id }
                }
            }

            GroupCommand::Heartbeat {
                group_id,
                member_id,
                generation,
                timestamp_ms,
            } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    if group.generation != generation {
                        return GroupResponse::IllegalGeneration {
                            group_id,
                            expected: group.generation,
                            actual: generation,
                        };
                    }

                    if let Some(member) = group.members.get_mut(&member_id) {
                        member.last_heartbeat_ms = timestamp_ms;
                        GroupResponse::HeartbeatAck
                    } else {
                        GroupResponse::UnknownMember {
                            group_id,
                            member_id,
                        }
                    }
                } else {
                    GroupResponse::GroupNotFound { group_id }
                }
            }

            GroupCommand::LeaveGroup {
                group_id,
                member_id,
            } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    group.members.remove(&member_id);
                    group.assignments.remove(&member_id);

                    // If leader left, elect new leader
                    if group.leader_id.as_ref() == Some(&member_id) {
                        group.leader_id = group.members.keys().next().cloned();
                    }

                    // Trigger rebalance if members remain
                    if !group.members.is_empty() {
                        group.increment_generation();
                        group.state = GroupStateEnum::PreparingRebalance;
                    } else {
                        group.state = GroupStateEnum::Empty;
                    }

                    GroupResponse::LeftGroup
                } else {
                    GroupResponse::GroupNotFound { group_id }
                }
            }

            GroupCommand::ExpireMembers { current_time_ms } => {
                const MAX_EXPIRATIONS_PER_GROUP: usize = 2;
                let mut expired = Vec::new();

                for group in self.groups.values_mut() {
                    let mut expired_members: Vec<String> = group
                        .members
                        .iter()
                        .filter(|(_, m)| {
                            let tolerance = m.session_timeout_ms as u64 / 10;
                            let effective_timeout = m.session_timeout_ms as u64 + tolerance;
                            current_time_ms.saturating_sub(m.last_heartbeat_ms) > effective_timeout
                        })
                        .map(|(id, _)| id.clone())
                        .collect();

                    expired_members.sort();
                    expired_members.truncate(MAX_EXPIRATIONS_PER_GROUP);

                    let group_had_expirations = !expired_members.is_empty();

                    for member_id in expired_members {
                        group.members.remove(&member_id);
                        group.assignments.remove(&member_id);
                        expired.push((group.group_id.clone(), member_id.clone()));

                        if group.leader_id.as_ref() == Some(&member_id) {
                            group.leader_id = group.members.keys().next().cloned();
                        }
                    }

                    if group_had_expirations && !group.members.is_empty() {
                        group.increment_generation();
                        group.state = GroupStateEnum::PreparingRebalance;
                    }
                }

                GroupResponse::MembersExpired { expired }
            }

            GroupCommand::DeleteGroup { group_id } => {
                self.groups.remove(&group_id);
                // Remove all offsets for this group
                self.offsets.retain(|(gid, _, _), _| gid != &group_id);
                GroupResponse::GroupDeleted { group_id }
            }

            GroupCommand::CompleteRebalance {
                group_id,
                generation,
            } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    if group.generation == generation {
                        group.state = GroupStateEnum::Stable;
                        GroupResponse::RebalanceCompleted { group_id }
                    } else {
                        GroupResponse::IllegalGeneration {
                            group_id,
                            expected: group.generation,
                            actual: generation,
                        }
                    }
                } else {
                    GroupResponse::GroupNotFound { group_id }
                }
            }

            GroupCommand::TriggerRebalance { group_id } => {
                if let Some(group) = self.groups.get_mut(&group_id) {
                    group.increment_generation();
                    group.state = GroupStateEnum::PreparingRebalance;
                    group.assignments.clear();
                    GroupResponse::RebalanceTriggered { group_id }
                } else {
                    GroupResponse::GroupNotFound { group_id }
                }
            }

            GroupCommand::CommitOffset {
                group_id,
                topic,
                partition,
                offset,
                metadata,
                timestamp_ms,
            } => {
                let topic: Arc<str> = Arc::from(topic);
                let key = (group_id, topic, partition);
                self.offsets.insert(
                    key,
                    CommittedOffsetInfo {
                        offset,
                        metadata,
                        commit_timestamp_ms: timestamp_ms,
                    },
                );
                GroupResponse::OffsetCommitted
            }
        }
    }

    /// Get a consumer group.
    pub fn get_group(&self, group_id: &str) -> Option<&ConsumerGroupInfo> {
        self.groups.get(group_id)
    }

    /// Get committed offset.
    pub fn get_offset(&self, group_id: &str, topic: &str, partition: i32) -> Option<i64> {
        let key = (group_id.to_string(), Arc::from(topic), partition);
        self.offsets.get(&key).map(|o| o.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_join_group() {
        let mut state = GroupDomainState::new();

        let response = state.apply(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![1, 2, 3])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: 1000,
        });

        match response {
            GroupResponse::JoinGroupResponse {
                generation,
                member_id,
                members,
                ..
            } => {
                assert_eq!(generation, 1);
                assert!(member_id.starts_with("client-1-"));
                assert_eq!(members.len(), 1); // Leader gets member list
            }
            _ => panic!("Expected JoinGroupResponse"),
        }

        let group = state.get_group("test-group").unwrap();
        assert_eq!(group.state, GroupStateEnum::PreparingRebalance);
    }

    #[test]
    fn test_sync_group() {
        let mut state = GroupDomainState::new();

        // Join first
        let join_response = state.apply(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: 1000,
        });

        let generation = match join_response {
            GroupResponse::JoinGroupResponse { generation, .. } => generation,
            _ => panic!("Expected JoinGroupResponse"),
        };

        // Sync as leader
        let response = state.apply(GroupCommand::SyncGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            generation,
            assignments: vec![("member-1".to_string(), vec![0, 1, 2])],
        });

        match response {
            GroupResponse::SyncGroupResponse { assignment } => {
                assert_eq!(assignment, vec![0, 1, 2]);
            }
            _ => panic!("Expected SyncGroupResponse"),
        }
    }

    #[test]
    fn test_heartbeat() {
        let mut state = GroupDomainState::new();

        state.apply(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: 1000,
        });

        let response = state.apply(GroupCommand::Heartbeat {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            generation: 1,
            timestamp_ms: 2000,
        });

        assert!(matches!(response, GroupResponse::HeartbeatAck));
    }

    #[test]
    fn test_commit_offset() {
        let mut state = GroupDomainState::new();

        let response = state.apply(GroupCommand::CommitOffset {
            group_id: "test-group".to_string(),
            topic: "test-topic".to_string(),
            partition: 0,
            offset: 100,
            metadata: None,
            timestamp_ms: 1000,
        });

        assert!(matches!(response, GroupResponse::OffsetCommitted));
        assert_eq!(state.get_offset("test-group", "test-topic", 0), Some(100));
    }

    #[test]
    fn test_leave_group() {
        let mut state = GroupDomainState::new();

        state.apply(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: 1000,
        });

        let response = state.apply(GroupCommand::LeaveGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
        });

        assert!(matches!(response, GroupResponse::LeftGroup));

        let group = state.get_group("test-group").unwrap();
        assert!(group.members.is_empty());
        assert_eq!(group.state, GroupStateEnum::Empty);
    }
}
