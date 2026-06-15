//! Consumer group domain for the Raft state machine.
//!
//! Handles consumer group coordination, offsets, and membership.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::serde_helpers::serialize_sorted_map;

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
    /// Full `(protocol_name, metadata)` list this member sent in its last
    /// JoinGroup, in the member's preference order. Needed so the leader's
    /// JoinGroup response can carry each member's own subscription bytes
    /// for the negotiated protocol, and so the protocol can be re-selected
    /// when membership changes.
    #[serde(default)]
    pub protocols: Vec<(String, Vec<u8>)>,
    pub session_timeout_ms: i32,
    pub rebalance_timeout_ms: i32,
    pub last_heartbeat_ms: u64,
}

impl MemberInfo {
    /// The metadata this member sent for `protocol`, falling back to the
    /// member's first-protocol metadata when the protocol is unknown
    /// (e.g. legacy members that joined without a protocol list).
    pub fn metadata_for_protocol(&self, protocol: Option<&str>) -> Vec<u8> {
        match protocol {
            Some(name) => self
                .protocols
                .iter()
                .find(|(n, _)| n == name)
                .map(|(_, m)| m.clone())
                .unwrap_or_else(|| self.metadata.clone()),
            None => self.metadata.clone(),
        }
    }
}

/// Outcome of negotiating a group protocol across members.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProtocolSelection {
    /// No member declared any protocols (legacy/simple clients).
    None,
    /// Every protocol-declaring member supports this protocol.
    Selected(String),
    /// Members declared protocols but have none in common.
    Inconsistent,
}

/// Select the group protocol the way Kafka's coordinator does.
///
/// `preference_lists` holds each member's protocol names in that member's
/// preference order. Members with empty lists (legacy clients) don't
/// constrain the choice. The candidate set is the intersection of all
/// participating members' protocols; each member then "votes" for its
/// most-preferred candidate and the candidate with the most votes wins.
///
/// The first list should be the group leader's: candidate ordering (and
/// therefore tie-breaking) follows it, which keeps the selection
/// deterministic across Raft replicas regardless of map iteration order.
pub fn select_group_protocol(preference_lists: &[Vec<String>]) -> ProtocolSelection {
    let participants: Vec<&Vec<String>> = preference_lists
        .iter()
        .filter(|prefs| !prefs.is_empty())
        .collect();
    if participants.is_empty() {
        return ProtocolSelection::None;
    }

    // Candidates: protocols every participant supports, ordered by the
    // first participant's (leader's) preference.
    let mut candidates: Vec<&String> = Vec::new();
    for name in participants[0].iter() {
        if !candidates.contains(&name) && participants.iter().all(|prefs| prefs.contains(name)) {
            candidates.push(name);
        }
    }
    if candidates.is_empty() {
        return ProtocolSelection::Inconsistent;
    }

    // Each participant votes for the first of its own protocols that made
    // the candidate set; most votes wins, ties go to the candidate the
    // leader prefers (earliest in `candidates`).
    let mut votes = vec![0usize; candidates.len()];
    for prefs in &participants {
        if let Some(idx) = prefs
            .iter()
            .find_map(|name| candidates.iter().position(|c| *c == name))
        {
            votes[idx] += 1;
        }
    }
    let mut best = 0;
    for (idx, count) in votes.iter().enumerate() {
        if *count > votes[best] {
            best = idx;
        }
    }
    ProtocolSelection::Selected(candidates[best].clone())
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
    #[serde(serialize_with = "serialize_sorted_map")]
    pub members: HashMap<String, MemberInfo>,
    #[serde(serialize_with = "serialize_sorted_map")]
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

    /// A joining member's protocol list has no protocol in common with the
    /// rest of the group (Kafka: INCONSISTENT_GROUP_PROTOCOL).
    InconsistentProtocol { group_id: String, member_id: String },

    /// Generic success.
    Ok,
}

/// State for the group domain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GroupDomainState {
    /// Consumer groups.
    #[serde(serialize_with = "serialize_sorted_map")]
    pub groups: HashMap<String, ConsumerGroupInfo>,

    /// Committed offsets: (group_id, topic, partition) -> CommittedOffsetInfo.
    #[serde(serialize_with = "serialize_sorted_map")]
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

                // Determine member ID. The proposer is responsible for assigning
                // member IDs; if the command arrives with an empty member_id we
                // derive one deterministically from the command inputs so every
                // replica produces the same value when applying this log entry.
                // Using Uuid::new_v4() here would diverge state across replicas.
                //
                // Rejoin coalescing: if an existing member already has the same
                // (client_id, client_host), reuse its id rather than minting a
                // new one. Without this a flapping client (lost session token,
                // re-connecting after a network blip) accumulates a fresh
                // member entry every join, bloating the group and the
                // generation counter.
                let final_member_id = if member_id.is_empty() {
                    if let Some(existing) = group
                        .members
                        .values()
                        .find(|m| m.client_id == client_id && m.client_host == client_host)
                    {
                        existing.member_id.clone()
                    } else {
                        format!("{}-{}-{}", client_id, timestamp_ms, group.members.len())
                    }
                } else {
                    member_id
                };

                // Negotiate the group protocol across the existing members
                // plus the joiner, BEFORE mutating any state: an incompatible
                // joiner is rejected without disturbing the group.
                //
                // The prospective leader's preferences go first (they drive
                // tie-breaking); remaining members are sorted by member id so
                // every Raft replica computes the same result.
                let prospective_leader = group
                    .leader_id
                    .clone()
                    .unwrap_or_else(|| final_member_id.clone());
                let joiner_protocol_names: Vec<String> =
                    protocols.iter().map(|(n, _)| n.clone()).collect();
                let mut member_prefs: Vec<(String, Vec<String>)> = group
                    .members
                    .iter()
                    .filter(|(id, _)| **id != final_member_id)
                    .map(|(id, m)| {
                        (
                            id.clone(),
                            m.protocols.iter().map(|(n, _)| n.clone()).collect(),
                        )
                    })
                    .collect();
                member_prefs.push((final_member_id.clone(), joiner_protocol_names));
                member_prefs.sort_by(|a, b| {
                    let a_is_leader = a.0 == prospective_leader;
                    let b_is_leader = b.0 == prospective_leader;
                    b_is_leader.cmp(&a_is_leader).then_with(|| a.0.cmp(&b.0))
                });
                let preference_lists: Vec<Vec<String>> =
                    member_prefs.into_iter().map(|(_, prefs)| prefs).collect();

                let selected_protocol = match select_group_protocol(&preference_lists) {
                    ProtocolSelection::Selected(name) => Some(name),
                    ProtocolSelection::None => None,
                    ProtocolSelection::Inconsistent => {
                        return GroupResponse::InconsistentProtocol {
                            group_id,
                            member_id: final_member_id,
                        };
                    }
                };

                // The member's `metadata` field keeps its first-protocol
                // blob for backwards compatibility; per-protocol metadata
                // lives in `protocols` and is resolved against the selected
                // protocol when building responses.
                let metadata = protocols
                    .first()
                    .map(|(_, m)| m.clone())
                    .unwrap_or_default();

                // Add (or refresh) the member
                group.members.insert(
                    final_member_id.clone(),
                    MemberInfo {
                        member_id: final_member_id.clone(),
                        client_id,
                        client_host,
                        metadata,
                        protocols,
                        session_timeout_ms,
                        rebalance_timeout_ms,
                        last_heartbeat_ms: timestamp_ms,
                    },
                );

                // Set leader if this is the first member
                if group.leader_id.is_none() {
                    group.leader_id = Some(final_member_id.clone());
                }

                // Trigger rebalance
                group.increment_generation();
                group.state = GroupStateEnum::PreparingRebalance;
                group.protocol_name = selected_protocol.clone();
                group.assignments.clear();

                let leader_id = group.leader_id.clone().unwrap_or_default();

                // Build member descriptions for the leader's response. Each
                // entry carries that member's OWN metadata for the selected
                // protocol — the leader's assignor computes assignments from
                // these subscriptions. Followers get an empty list, matching
                // Kafka. Note this keys off the joiner being the leader (not
                // "first member ever") so a rejoining leader also gets the
                // full roster.
                let members: Vec<MemberDescription> = if final_member_id == leader_id {
                    let mut described: Vec<MemberDescription> = group
                        .members
                        .values()
                        .map(|m| MemberDescription {
                            member_id: m.member_id.clone(),
                            client_id: m.client_id.clone(),
                            client_host: m.client_host.clone(),
                            metadata: m.metadata_for_protocol(selected_protocol.as_deref()),
                        })
                        .collect();
                    described.sort_by(|a, b| a.member_id.cmp(&b.member_id));
                    described
                } else {
                    Vec::new()
                };

                GroupResponse::JoinGroupResponse {
                    generation: group.generation,
                    leader_id,
                    member_id: final_member_id,
                    members,
                    protocol_name: selected_protocol.unwrap_or_default(),
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
                    // Kafka answers LeaveGroup from a non-member with
                    // UNKNOWN_MEMBER_ID; don't bump the generation or
                    // disturb group state for a member that isn't there.
                    if !group.members.contains_key(&member_id) {
                        return GroupResponse::UnknownMember {
                            group_id,
                            member_id,
                        };
                    }

                    group.members.remove(&member_id);
                    group.assignments.remove(&member_id);

                    // If leader left, elect new leader.
                    // HashMap iteration order is randomized per process, so
                    // every replica would pick a different new leader. Use the
                    // lexicographically smallest member_id for determinism.
                    if group.leader_id.as_ref() == Some(&member_id) {
                        group.leader_id = group.members.keys().min().cloned();
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
                            group.leader_id = group.members.keys().min().cloned();
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

    /// Build a JoinGroup command with the given protocols.
    fn join_cmd(
        group_id: &str,
        member_id: &str,
        protocols: Vec<(String, Vec<u8>)>,
        timestamp_ms: u64,
    ) -> GroupCommand {
        GroupCommand::JoinGroup {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            client_id: format!("client-{}", member_id),
            client_host: "localhost".to_string(),
            protocol_type: "consumer".to_string(),
            protocols,
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            timestamp_ms,
        }
    }

    #[test]
    fn test_select_group_protocol_picks_common_protocol() {
        // Leader prefers roundrobin, but only range is supported by all.
        let prefs = vec![
            vec!["roundrobin".to_string(), "range".to_string()],
            vec!["range".to_string()],
        ];
        assert_eq!(
            select_group_protocol(&prefs),
            ProtocolSelection::Selected("range".to_string())
        );
    }

    #[test]
    fn test_select_group_protocol_majority_vote_wins() {
        // Both protocols are supported by everyone; two members prefer
        // roundrobin so it wins the vote despite the leader preferring range.
        let prefs = vec![
            vec!["range".to_string(), "roundrobin".to_string()],
            vec!["roundrobin".to_string(), "range".to_string()],
            vec!["roundrobin".to_string(), "range".to_string()],
        ];
        assert_eq!(
            select_group_protocol(&prefs),
            ProtocolSelection::Selected("roundrobin".to_string())
        );
    }

    #[test]
    fn test_select_group_protocol_tie_breaks_by_leader_preference() {
        let prefs = vec![
            vec!["range".to_string(), "roundrobin".to_string()],
            vec!["roundrobin".to_string(), "range".to_string()],
        ];
        // One vote each; the leader's preference (range) wins.
        assert_eq!(
            select_group_protocol(&prefs),
            ProtocolSelection::Selected("range".to_string())
        );
    }

    #[test]
    fn test_select_group_protocol_inconsistent() {
        let prefs = vec![vec!["range".to_string()], vec!["sticky".to_string()]];
        assert_eq!(
            select_group_protocol(&prefs),
            ProtocolSelection::Inconsistent
        );
    }

    #[test]
    fn test_select_group_protocol_no_protocols() {
        let prefs: Vec<Vec<String>> = vec![vec![], vec![]];
        assert_eq!(select_group_protocol(&prefs), ProtocolSelection::None);
        assert_eq!(select_group_protocol(&[]), ProtocolSelection::None);
    }

    #[test]
    fn test_join_group_negotiates_common_protocol() {
        let mut state = GroupDomainState::new();

        state.apply(join_cmd(
            "g",
            "m1",
            vec![
                ("roundrobin".to_string(), vec![1]),
                ("range".to_string(), vec![2]),
            ],
            1000,
        ));
        let response = state.apply(join_cmd(
            "g",
            "m2",
            vec![("range".to_string(), vec![3])],
            2000,
        ));

        match response {
            GroupResponse::JoinGroupResponse { protocol_name, .. } => {
                assert_eq!(protocol_name, "range");
            }
            other => panic!("Expected JoinGroupResponse, got {:?}", other),
        }
        let group = state.get_group("g").unwrap();
        assert_eq!(group.protocol_name.as_deref(), Some("range"));
    }

    #[test]
    fn test_join_group_inconsistent_protocol_rejected_without_state_change() {
        let mut state = GroupDomainState::new();

        state.apply(join_cmd(
            "g",
            "m1",
            vec![("range".to_string(), vec![])],
            1000,
        ));
        let generation_before = state.get_group("g").unwrap().generation;

        let response = state.apply(join_cmd(
            "g",
            "m2",
            vec![("sticky".to_string(), vec![])],
            2000,
        ));

        assert!(matches!(
            response,
            GroupResponse::InconsistentProtocol { ref member_id, .. } if member_id == "m2"
        ));
        let group = state.get_group("g").unwrap();
        assert_eq!(
            group.members.len(),
            1,
            "incompatible joiner must not be added"
        );
        assert_eq!(
            group.generation, generation_before,
            "no rebalance triggered"
        );
    }

    #[test]
    fn test_join_group_leader_receives_each_members_own_metadata() {
        let mut state = GroupDomainState::new();

        // m1 (leader) joins first, then m2 with different metadata.
        state.apply(join_cmd(
            "g",
            "m1",
            vec![("range".to_string(), vec![0xAA])],
            1000,
        ));
        let follower_response = state.apply(join_cmd(
            "g",
            "m2",
            vec![("range".to_string(), vec![0xBB])],
            2000,
        ));

        // Followers don't get the member roster.
        match &follower_response {
            GroupResponse::JoinGroupResponse { members, .. } => {
                assert!(members.is_empty(), "non-leader must not receive members");
            }
            other => panic!("Expected JoinGroupResponse, got {:?}", other),
        }

        // Leader rejoins (normal rebalance flow) and must see every
        // member's OWN metadata, not its own blob duplicated.
        let leader_response = state.apply(join_cmd(
            "g",
            "m1",
            vec![("range".to_string(), vec![0xAA])],
            3000,
        ));
        match leader_response {
            GroupResponse::JoinGroupResponse {
                members, leader_id, ..
            } => {
                assert_eq!(leader_id, "m1");
                assert_eq!(members.len(), 2);
                assert_eq!(members[0].member_id, "m1");
                assert_eq!(members[0].metadata, vec![0xAA]);
                assert_eq!(members[1].member_id, "m2");
                assert_eq!(members[1].metadata, vec![0xBB]);
            }
            other => panic!("Expected JoinGroupResponse, got {:?}", other),
        }
    }

    #[test]
    fn test_leave_group_unknown_member_returns_unknown_member() {
        let mut state = GroupDomainState::new();

        state.apply(join_cmd(
            "g",
            "m1",
            vec![("range".to_string(), vec![])],
            1000,
        ));
        let generation_before = state.get_group("g").unwrap().generation;

        let response = state.apply(GroupCommand::LeaveGroup {
            group_id: "g".to_string(),
            member_id: "not-a-member".to_string(),
        });

        assert!(matches!(
            response,
            GroupResponse::UnknownMember { ref member_id, .. } if member_id == "not-a-member"
        ));
        let group = state.get_group("g").unwrap();
        assert_eq!(group.members.len(), 1);
        assert_eq!(
            group.generation, generation_before,
            "unknown-member leave must not trigger a rebalance"
        );
    }

    #[test]
    fn test_leave_group_unknown_group_returns_group_not_found() {
        let mut state = GroupDomainState::new();
        let response = state.apply(GroupCommand::LeaveGroup {
            group_id: "missing".to_string(),
            member_id: "m1".to_string(),
        });
        assert!(matches!(response, GroupResponse::GroupNotFound { .. }));
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
