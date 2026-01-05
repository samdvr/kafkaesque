//! Separate state machine for consumer group protocol.
//!
//! This module extracts consumer group management from the monolithic `CoordinationStateMachine`,
//! providing clearer separation of concerns:
//!
//! - **ClusterStateMachine** (in state_machine.rs): Brokers, topics, partitions, ownership
//! - **GroupStateMachine** (this module): Consumer groups, offsets, rebalances
//!
//! # Design Benefits
//!
//! 1. **Isolation**: Group rebalance bugs can't corrupt cluster state
//! 2. **Testability**: Group protocol can be tested without full cluster
//! 3. **Scalability**: Future option to run as separate Raft group
//! 4. **Clarity**: Clear ownership of group-related code
//!
//! # Usage
//!
//! The `GroupStateMachine` can be used standalone for testing, or integrated
//! with `CoordinationStateMachine` for production use.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// State of a consumer group.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum GroupProtocolState {
    /// Group has no members.
    Empty,
    /// Waiting for members to join.
    PreparingRebalance,
    /// Waiting for leader to sync assignments.
    CompletingRebalance,
    /// Group is stable with active members.
    Stable,
    /// Group is being deleted or is defunct.
    Dead,
}

impl Default for GroupProtocolState {
    fn default() -> Self {
        Self::Empty
    }
}

/// State of a consumer group member.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupMember {
    /// Unique member identifier.
    pub member_id: String,
    /// Client-provided identifier.
    pub client_id: String,
    /// Client's host address.
    pub client_host: String,
    /// Subscription metadata (protocol-specific).
    pub metadata: Vec<u8>,
    /// Session timeout in milliseconds.
    pub session_timeout_ms: i32,
    /// Rebalance timeout in milliseconds.
    pub rebalance_timeout_ms: i32,
    /// Timestamp of last heartbeat.
    pub last_heartbeat_ms: u64,
}

/// Consumer group state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerGroup {
    /// Group identifier.
    pub group_id: String,
    /// Current generation ID.
    pub generation: i32,
    /// Current protocol state.
    pub state: GroupProtocolState,
    /// Leader member ID (if any).
    pub leader_id: Option<String>,
    /// Group members by member ID.
    pub members: HashMap<String, GroupMember>,
    /// Partition assignments by member ID.
    pub assignments: HashMap<String, Vec<u8>>,
    /// Protocol type (e.g., "consumer").
    pub protocol_type: Option<String>,
    /// Selected protocol name.
    pub protocol_name: Option<String>,
    /// Timestamp when rebalance started (for timeout tracking).
    #[serde(default)]
    pub rebalance_started_at_ms: Option<u64>,
}

impl Default for ConsumerGroup {
    fn default() -> Self {
        Self {
            group_id: String::new(),
            generation: 0,
            state: GroupProtocolState::Empty,
            leader_id: None,
            members: HashMap::new(),
            assignments: HashMap::new(),
            protocol_type: None,
            protocol_name: None,
            rebalance_started_at_ms: None,
        }
    }
}

impl ConsumerGroup {
    /// Create a new consumer group.
    pub fn new(group_id: String) -> Self {
        Self {
            group_id,
            ..Default::default()
        }
    }

    /// Safely increment generation with overflow handling.
    ///
    /// Wraps to 1 instead of 0 because generation 0 typically indicates
    /// "no generation" in Kafka protocol semantics.
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

    /// Check if a member is the group leader.
    pub fn is_leader(&self, member_id: &str) -> bool {
        self.leader_id.as_deref() == Some(member_id)
    }

    /// Get the number of active members.
    pub fn member_count(&self) -> usize {
        self.members.len()
    }

    /// Check if the group is empty.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Start a rebalance.
    pub fn start_rebalance(&mut self, timestamp_ms: u64) {
        self.increment_generation();
        self.state = GroupProtocolState::PreparingRebalance;
        self.assignments.clear();
        self.rebalance_started_at_ms = Some(timestamp_ms);
    }

    /// Elect a new leader from remaining members.
    pub fn elect_new_leader(&mut self) {
        self.leader_id = self.members.keys().next().cloned();
    }
}

/// Committed offset for a consumer group partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommittedOffset {
    /// The committed offset value.
    pub offset: i64,
    /// Optional commit metadata.
    pub metadata: Option<String>,
    /// When the offset was committed.
    pub commit_timestamp_ms: u64,
}

/// The complete group coordination state.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GroupCoordinationState {
    /// Consumer groups by group ID.
    pub groups: HashMap<String, ConsumerGroup>,
    /// Committed offsets: (group_id, topic, partition) -> CommittedOffset.
    pub offsets: HashMap<(String, Arc<str>, i32), CommittedOffset>,
}

impl GroupCoordinationState {
    /// Get or create a consumer group.
    pub fn get_or_create_group(&mut self, group_id: &str) -> &mut ConsumerGroup {
        self.groups
            .entry(group_id.to_string())
            .or_insert_with(|| ConsumerGroup::new(group_id.to_string()))
    }

    /// Get a consumer group if it exists.
    pub fn get_group(&self, group_id: &str) -> Option<&ConsumerGroup> {
        self.groups.get(group_id)
    }

    /// Get a mutable reference to a consumer group.
    pub fn get_group_mut(&mut self, group_id: &str) -> Option<&mut ConsumerGroup> {
        self.groups.get_mut(group_id)
    }

    /// Get committed offset for a partition.
    pub fn get_offset(
        &self,
        group_id: &str,
        topic: &Arc<str>,
        partition: i32,
    ) -> Option<&CommittedOffset> {
        let key = (group_id.to_string(), Arc::clone(topic), partition);
        self.offsets.get(&key)
    }

    /// Commit an offset.
    pub fn commit_offset(
        &mut self,
        group_id: String,
        topic: Arc<str>,
        partition: i32,
        offset: i64,
        metadata: Option<String>,
        timestamp_ms: u64,
    ) {
        let key = (group_id, topic, partition);
        self.offsets.insert(
            key,
            CommittedOffset {
                offset,
                metadata,
                commit_timestamp_ms: timestamp_ms,
            },
        );
    }

    /// Delete a group and its offsets.
    pub fn delete_group(&mut self, group_id: &str) {
        self.groups.remove(group_id);
        self.offsets.retain(|(gid, _, _), _| gid != group_id);
    }
}

/// Commands specific to consumer group management.
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

    /// Trigger a rebalance.
    TriggerRebalance { group_id: String, timestamp_ms: u64 },

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

/// Member description for join group response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct MemberInfo {
    pub member_id: String,
    pub client_id: String,
    pub client_host: String,
    pub metadata: Vec<u8>,
}

/// Responses from group commands.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum GroupResponse {
    /// Generic success.
    Ok,

    /// Join group response.
    JoinGroupSuccess {
        generation: i32,
        leader_id: String,
        member_id: String,
        members: Vec<MemberInfo>,
        protocol_name: String,
    },

    /// Sync group response.
    SyncGroupSuccess { assignment: Vec<u8> },

    /// Heartbeat acknowledged.
    HeartbeatSuccess,

    /// Offset committed.
    OffsetCommitted,

    /// Members were expired.
    MembersExpired {
        expired: Vec<(String, String)>, // (group_id, member_id)
    },

    // Errors
    /// Group not found.
    GroupNotFound { group_id: String },

    /// Unknown member.
    UnknownMember { group_id: String, member_id: String },

    /// Illegal generation.
    IllegalGeneration {
        group_id: String,
        expected: i32,
        actual: i32,
    },

    /// Rebalance in progress.
    RebalanceInProgress { group_id: String },
}

impl GroupResponse {
    /// Check if this is an error response.
    pub fn is_error(&self) -> bool {
        matches!(
            self,
            GroupResponse::GroupNotFound { .. }
                | GroupResponse::UnknownMember { .. }
                | GroupResponse::IllegalGeneration { .. }
                | GroupResponse::RebalanceInProgress { .. }
        )
    }
}

/// The group state machine for consumer group coordination.
///
/// This handles all consumer group protocol operations separately from
/// cluster infrastructure concerns.
pub struct GroupStateMachine {
    /// The current group state.
    state: Arc<RwLock<GroupCoordinationState>>,
}

impl GroupStateMachine {
    /// Create a new group state machine.
    pub fn new() -> Self {
        Self {
            state: Arc::new(RwLock::new(GroupCoordinationState::default())),
        }
    }

    /// Create from existing state.
    pub fn with_state(state: GroupCoordinationState) -> Self {
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    /// Get a read-only reference to the current state.
    pub async fn state(&self) -> tokio::sync::RwLockReadGuard<'_, GroupCoordinationState> {
        self.state.read().await
    }

    /// Get the state Arc for cloning.
    pub fn state_arc(&self) -> Arc<RwLock<GroupCoordinationState>> {
        self.state.clone()
    }

    /// Apply a command to the group state machine.
    pub async fn apply_command(&self, command: GroupCommand) -> GroupResponse {
        let mut state = self.state.write().await;

        match command {
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
                let group = state.get_or_create_group(&group_id);

                // Set protocol type if not set
                if group.protocol_type.is_none() {
                    group.protocol_type = Some(protocol_type);
                }

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
                    GroupMember {
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
                group.start_rebalance(timestamp_ms);
                group.protocol_name = Some(protocol_name.clone());

                let leader_id = group.leader_id.clone().unwrap_or_default();

                // Build member descriptions (only for leader)
                let members: Vec<MemberInfo> = if is_leader {
                    group
                        .members
                        .values()
                        .map(|m| MemberInfo {
                            member_id: m.member_id.clone(),
                            client_id: m.client_id.clone(),
                            client_host: m.client_host.clone(),
                            metadata: m.metadata.clone(),
                        })
                        .collect()
                } else {
                    Vec::new()
                };

                GroupResponse::JoinGroupSuccess {
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
                if let Some(group) = state.get_group_mut(&group_id) {
                    // Verify generation
                    if group.generation != generation {
                        return GroupResponse::IllegalGeneration {
                            group_id,
                            expected: group.generation,
                            actual: generation,
                        };
                    }

                    // Verify member is part of group
                    if !group.members.contains_key(&member_id) {
                        return GroupResponse::UnknownMember {
                            group_id,
                            member_id,
                        };
                    }

                    // If leader, store assignments
                    if group.is_leader(&member_id) {
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
                        group.state = GroupProtocolState::CompletingRebalance;
                    }

                    // Return assignment for this member
                    let assignment = group
                        .assignments
                        .get(&member_id)
                        .cloned()
                        .unwrap_or_default();

                    GroupResponse::SyncGroupSuccess { assignment }
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
                if let Some(group) = state.get_group_mut(&group_id) {
                    // Verify generation
                    if group.generation != generation {
                        return GroupResponse::IllegalGeneration {
                            group_id,
                            expected: group.generation,
                            actual: generation,
                        };
                    }

                    // Update heartbeat
                    if let Some(member) = group.members.get_mut(&member_id) {
                        member.last_heartbeat_ms = timestamp_ms;
                        GroupResponse::HeartbeatSuccess
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
                if let Some(group) = state.get_group_mut(&group_id) {
                    group.members.remove(&member_id);
                    group.assignments.remove(&member_id);

                    // If leader left, elect new leader
                    if group.leader_id.as_ref() == Some(&member_id) {
                        group.elect_new_leader();
                    }

                    // Trigger rebalance if members remain
                    if !group.is_empty() {
                        let timestamp_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .map(|d| d.as_millis() as u64)
                            .unwrap_or(0);
                        group.start_rebalance(timestamp_ms);
                    } else {
                        group.state = GroupProtocolState::Empty;
                    }

                    GroupResponse::Ok
                } else {
                    GroupResponse::GroupNotFound { group_id }
                }
            }

            GroupCommand::ExpireMembers { current_time_ms } => {
                let mut expired = Vec::new();

                // Limit expirations per group to prevent rebalance storms
                const MAX_EXPIRATIONS_PER_GROUP: usize = 2;

                for group in state.groups.values_mut() {
                    let mut expired_members: Vec<String> = group
                        .members
                        .iter()
                        .filter(|(_, m)| {
                            // Add 10% tolerance to session timeout
                            let tolerance = m.session_timeout_ms as u64 / 10;
                            let effective_timeout = m.session_timeout_ms as u64 + tolerance;
                            current_time_ms.saturating_sub(m.last_heartbeat_ms) > effective_timeout
                        })
                        .map(|(id, _)| id.clone())
                        .collect();

                    // Sort and limit for deterministic ordering across replicas
                    expired_members.sort();
                    expired_members.truncate(MAX_EXPIRATIONS_PER_GROUP);

                    let group_had_expirations = !expired_members.is_empty();

                    for member_id in expired_members {
                        group.members.remove(&member_id);
                        group.assignments.remove(&member_id);
                        expired.push((group.group_id.clone(), member_id.clone()));

                        // If leader expired, elect new leader
                        if group.leader_id.as_ref() == Some(&member_id) {
                            group.elect_new_leader();
                        }
                    }

                    // Trigger rebalance if this group had expirations and still has members
                    if group_had_expirations && !group.is_empty() {
                        group.start_rebalance(current_time_ms);
                    }
                }

                GroupResponse::MembersExpired { expired }
            }

            GroupCommand::DeleteGroup { group_id } => {
                state.delete_group(&group_id);
                GroupResponse::Ok
            }

            GroupCommand::CompleteRebalance {
                group_id,
                generation,
            } => {
                if let Some(group) = state.get_group_mut(&group_id) {
                    if group.generation == generation {
                        group.state = GroupProtocolState::Stable;
                        group.rebalance_started_at_ms = None;
                        GroupResponse::Ok
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

            GroupCommand::TriggerRebalance {
                group_id,
                timestamp_ms,
            } => {
                if let Some(group) = state.get_group_mut(&group_id) {
                    group.start_rebalance(timestamp_ms);
                    GroupResponse::Ok
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
                state.commit_offset(group_id, topic, partition, offset, metadata, timestamp_ms);
                GroupResponse::OffsetCommitted
            }
        }
    }

    /// Restore state from a snapshot.
    pub async fn restore(&self, state: GroupCoordinationState) {
        *self.state.write().await = state;
    }

    /// Take a snapshot of the current state.
    pub async fn snapshot(&self) -> GroupCoordinationState {
        self.state.read().await.clone()
    }
}

impl Default for GroupStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn current_time_ms() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }

    #[tokio::test]
    async fn test_join_group_first_member_becomes_leader() {
        let sm = GroupStateMachine::new();

        let response = sm
            .apply_command(GroupCommand::JoinGroup {
                group_id: "test-group".to_string(),
                member_id: "".to_string(), // Empty means generate
                client_id: "client-1".to_string(),
                client_host: "localhost".to_string(),
                protocol_type: "consumer".to_string(),
                protocols: vec![("range".to_string(), vec![1, 2, 3])],
                session_timeout_ms: 30000,
                rebalance_timeout_ms: 60000,
                timestamp_ms: current_time_ms(),
            })
            .await;

        match response {
            GroupResponse::JoinGroupSuccess {
                generation,
                leader_id,
                member_id,
                members,
                ..
            } => {
                assert_eq!(generation, 1);
                assert_eq!(leader_id, member_id);
                assert_eq!(members.len(), 1);
            }
            _ => panic!("Expected JoinGroupSuccess"),
        }
    }

    #[tokio::test]
    async fn test_join_group_second_member_not_leader() {
        let sm = GroupStateMachine::new();
        let ts = current_time_ms();

        // First member joins
        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts,
        })
        .await;

        // Second member joins
        let response = sm
            .apply_command(GroupCommand::JoinGroup {
                group_id: "test-group".to_string(),
                member_id: "member-2".to_string(),
                client_id: "client-2".to_string(),
                client_host: "localhost".to_string(),
                protocol_type: "consumer".to_string(),
                protocols: vec![("range".to_string(), vec![])],
                session_timeout_ms: 30000,
                rebalance_timeout_ms: 60000,
                timestamp_ms: ts,
            })
            .await;

        match response {
            GroupResponse::JoinGroupSuccess {
                leader_id,
                member_id,
                members,
                ..
            } => {
                assert_eq!(leader_id, "member-1");
                assert_eq!(member_id, "member-2");
                // Non-leaders get empty member list
                assert!(members.is_empty());
            }
            _ => panic!("Expected JoinGroupSuccess"),
        }
    }

    #[tokio::test]
    async fn test_sync_group_leader_sets_assignments() {
        let sm = GroupStateMachine::new();
        let ts = current_time_ms();

        // Join group
        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts,
        })
        .await;

        // Sync as leader
        let response = sm
            .apply_command(GroupCommand::SyncGroup {
                group_id: "test-group".to_string(),
                member_id: "member-1".to_string(),
                generation: 1,
                assignments: vec![("member-1".to_string(), vec![1, 2, 3])],
            })
            .await;

        match response {
            GroupResponse::SyncGroupSuccess { assignment } => {
                assert_eq!(assignment, vec![1, 2, 3]);
            }
            _ => panic!("Expected SyncGroupSuccess"),
        }

        // Verify state transitioned
        let state = sm.state().await;
        let group = state.get_group("test-group").unwrap();
        assert_eq!(group.state, GroupProtocolState::CompletingRebalance);
    }

    #[tokio::test]
    async fn test_heartbeat_updates_timestamp() {
        let sm = GroupStateMachine::new();
        let ts1 = current_time_ms();

        // Join group
        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts1,
        })
        .await;

        let ts2 = ts1 + 5000;
        let response = sm
            .apply_command(GroupCommand::Heartbeat {
                group_id: "test-group".to_string(),
                member_id: "member-1".to_string(),
                generation: 1,
                timestamp_ms: ts2,
            })
            .await;

        assert!(matches!(response, GroupResponse::HeartbeatSuccess));

        let state = sm.state().await;
        let group = state.get_group("test-group").unwrap();
        assert_eq!(group.members["member-1"].last_heartbeat_ms, ts2);
    }

    #[tokio::test]
    async fn test_heartbeat_wrong_generation() {
        let sm = GroupStateMachine::new();
        let ts = current_time_ms();

        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts,
        })
        .await;

        let response = sm
            .apply_command(GroupCommand::Heartbeat {
                group_id: "test-group".to_string(),
                member_id: "member-1".to_string(),
                generation: 99, // Wrong generation
                timestamp_ms: ts,
            })
            .await;

        match response {
            GroupResponse::IllegalGeneration {
                expected, actual, ..
            } => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 99);
            }
            _ => panic!("Expected IllegalGeneration"),
        }
    }

    #[tokio::test]
    async fn test_leave_group_triggers_rebalance() {
        let sm = GroupStateMachine::new();
        let ts = current_time_ms();

        // Add two members
        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts,
        })
        .await;

        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-2".to_string(),
            client_id: "client-2".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts,
        })
        .await;

        let gen_before = {
            let state = sm.state().await;
            state.get_group("test-group").unwrap().generation
        };

        // Leave group
        sm.apply_command(GroupCommand::LeaveGroup {
            group_id: "test-group".to_string(),
            member_id: "member-2".to_string(),
        })
        .await;

        let state = sm.state().await;
        let group = state.get_group("test-group").unwrap();

        assert_eq!(group.member_count(), 1);
        assert!(group.generation > gen_before);
        assert_eq!(group.state, GroupProtocolState::PreparingRebalance);
    }

    #[tokio::test]
    async fn test_expire_members() {
        let sm = GroupStateMachine::new();
        let old_ts = current_time_ms() - 60000; // 60 seconds ago

        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000, // 30 second timeout
            rebalance_timeout_ms: 60000,
            timestamp_ms: old_ts, // Stale timestamp
        })
        .await;

        let response = sm
            .apply_command(GroupCommand::ExpireMembers {
                current_time_ms: current_time_ms(),
            })
            .await;

        match response {
            GroupResponse::MembersExpired { expired } => {
                assert_eq!(expired.len(), 1);
                assert_eq!(
                    expired[0],
                    ("test-group".to_string(), "member-1".to_string())
                );
            }
            _ => panic!("Expected MembersExpired"),
        }

        let state = sm.state().await;
        let group = state.get_group("test-group").unwrap();
        assert!(group.is_empty());
    }

    #[tokio::test]
    async fn test_commit_offset() {
        let sm = GroupStateMachine::new();
        let ts = current_time_ms();

        let response = sm
            .apply_command(GroupCommand::CommitOffset {
                group_id: "test-group".to_string(),
                topic: "orders".to_string(),
                partition: 0,
                offset: 12345,
                metadata: Some("test".to_string()),
                timestamp_ms: ts,
            })
            .await;

        assert!(matches!(response, GroupResponse::OffsetCommitted));

        let state = sm.state().await;
        let offset = state
            .get_offset("test-group", &Arc::from("orders"), 0)
            .unwrap();
        assert_eq!(offset.offset, 12345);
    }

    #[tokio::test]
    async fn test_complete_rebalance() {
        let sm = GroupStateMachine::new();
        let ts = current_time_ms();

        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts,
        })
        .await;

        let response = sm
            .apply_command(GroupCommand::CompleteRebalance {
                group_id: "test-group".to_string(),
                generation: 1,
            })
            .await;

        assert!(matches!(response, GroupResponse::Ok));

        let state = sm.state().await;
        let group = state.get_group("test-group").unwrap();
        assert_eq!(group.state, GroupProtocolState::Stable);
    }

    #[tokio::test]
    async fn test_delete_group() {
        let sm = GroupStateMachine::new();
        let ts = current_time_ms();

        // Create group with offset
        sm.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts,
        })
        .await;

        sm.apply_command(GroupCommand::CommitOffset {
            group_id: "test-group".to_string(),
            topic: "orders".to_string(),
            partition: 0,
            offset: 100,
            metadata: None,
            timestamp_ms: ts,
        })
        .await;

        // Delete group
        sm.apply_command(GroupCommand::DeleteGroup {
            group_id: "test-group".to_string(),
        })
        .await;

        let state = sm.state().await;
        assert!(state.get_group("test-group").is_none());
        assert!(state.offsets.is_empty());
    }

    #[tokio::test]
    async fn test_snapshot_restore() {
        let sm1 = GroupStateMachine::new();
        let ts = current_time_ms();

        sm1.apply_command(GroupCommand::JoinGroup {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
            client_id: "client-1".to_string(),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), vec![])],
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms: ts,
        })
        .await;

        let snapshot = sm1.snapshot().await;

        // Create new state machine and restore
        let sm2 = GroupStateMachine::new();
        sm2.restore(snapshot).await;

        let state = sm2.state().await;
        assert!(state.get_group("test-group").is_some());
        assert_eq!(state.get_group("test-group").unwrap().member_count(), 1);
    }

    #[test]
    fn test_consumer_group_increment_generation() {
        let mut group = ConsumerGroup::new("test".to_string());
        assert_eq!(group.generation, 0);

        group.increment_generation();
        assert_eq!(group.generation, 1);

        group.increment_generation();
        assert_eq!(group.generation, 2);
    }

    #[test]
    fn test_consumer_group_increment_generation_overflow() {
        let mut group = ConsumerGroup::new("test".to_string());
        group.generation = i32::MAX;

        group.increment_generation();
        assert_eq!(group.generation, 1); // Wraps to 1, not 0
    }

    #[test]
    fn test_group_response_is_error() {
        assert!(!GroupResponse::Ok.is_error());
        assert!(!GroupResponse::HeartbeatSuccess.is_error());
        assert!(
            GroupResponse::GroupNotFound {
                group_id: "x".into()
            }
            .is_error()
        );
        assert!(
            GroupResponse::UnknownMember {
                group_id: "x".into(),
                member_id: "y".into()
            }
            .is_error()
        );
    }
}
