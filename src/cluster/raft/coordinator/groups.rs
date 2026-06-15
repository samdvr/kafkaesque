//! ConsumerGroupCoordinator implementation for RaftCoordinator.
//!
//! This module handles:
//! - Consumer group membership (join/leave)
//! - Group rebalancing
//! - Offset commit/fetch

use std::time::Instant;

use async_trait::async_trait;

use super::super::commands::{CoordinationCommand, CoordinationResponse};
use super::super::domains::{GroupCommand, GroupResponse};
use super::{RaftCoordinator, current_time_ms};

use crate::cluster::error::{HeartbeatResult, SlateDBError, SlateDBResult};
use crate::cluster::metrics;
use crate::cluster::traits::{ConsumerGroupCoordinator, GroupJoinOutcome, GroupJoinResult};

#[async_trait]
impl ConsumerGroupCoordinator for RaftCoordinator {
    async fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        protocol_metadata: &[u8],
        session_timeout_ms: i32,
    ) -> SlateDBResult<(i32, String, bool, String, Vec<String>)> {
        // Legacy single-protocol entry point: callers that don't negotiate
        // protocols join under "range". The protocol-aware path is
        // `join_group_with_protocols`.
        let protocols = vec![("range".to_string(), protocol_metadata.to_vec())];
        match self
            .join_group_with_protocols(
                group_id,
                member_id,
                &protocols,
                session_timeout_ms,
                session_timeout_ms,
            )
            .await?
        {
            GroupJoinOutcome::Joined(result) => {
                let member_ids: Vec<String> =
                    result.members.into_iter().map(|(id, _)| id).collect();
                Ok((
                    result.generation_id,
                    result.member_id,
                    result.is_leader,
                    result.leader_id,
                    member_ids,
                ))
            }
            GroupJoinOutcome::InconsistentProtocol => Err(SlateDBError::Storage(format!(
                "Group {} rejected join: inconsistent group protocol",
                group_id
            ))),
        }
    }

    async fn join_group_with_protocols(
        &self,
        group_id: &str,
        member_id: &str,
        protocols: &[(String, Vec<u8>)],
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
    ) -> SlateDBResult<GroupJoinOutcome> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            client_id: "".to_string(),
            client_host: "".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: protocols.to_vec(),
            session_timeout_ms,
            rebalance_timeout_ms,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
                generation,
                leader_id,
                member_id,
                members,
                protocol_name,
            }) => {
                let is_leader = leader_id == member_id;
                Ok(GroupJoinOutcome::Joined(GroupJoinResult {
                    generation_id: generation,
                    member_id,
                    is_leader,
                    leader_id,
                    protocol_name,
                    members: members
                        .into_iter()
                        .map(|m| (m.member_id, m.metadata))
                        .collect(),
                }))
            }
            CoordinationResponse::GroupDomainResponse(GroupResponse::InconsistentProtocol {
                ..
            }) => Ok(GroupJoinOutcome::InconsistentProtocol),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_group_protocol(&self, group_id: &str) -> SlateDBResult<Option<String>> {
        let start = Instant::now();
        // Linearizable read: any coordinator read after a failover-induced
        // FindCoordinator hop must serve from a state that has applied
        // every entry committed before the new coordinator answered. A
        // stale local read here can return a previous generation's protocol
        // and cause SyncGroup to write assignments under the wrong key.
        self.node.ensure_linearizable().await?;
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let protocol = inner_state
            .group_domain
            .groups
            .get(group_id)
            .and_then(|g| g.protocol_name.clone());
        metrics::record_raft_query("get_group_state", start.elapsed().as_secs_f64());
        Ok(protocol)
    }

    async fn complete_rebalance(
        &self,
        group_id: &str,
        expected_generation: i32,
    ) -> SlateDBResult<bool> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::CompleteRebalance {
            group_id: group_id.to_string(),
            generation: expected_generation,
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::RebalanceCompleted {
                ..
            }) => Ok(true),
            CoordinationResponse::GroupDomainResponse(GroupResponse::IllegalGeneration {
                ..
            }) => Ok(false),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_generation(&self, group_id: &str) -> SlateDBResult<i32> {
        let start = Instant::now();
        // Linearizable: see `get_group_protocol` for the rationale. The
        // generation is what JoinGroup retries against; serving a stale
        // generation lets a member rejoin with a "valid" generation that
        // the new coordinator has already advanced past.
        self.node.ensure_linearizable().await?;
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let generation = inner_state
            .group_domain
            .groups
            .get(group_id)
            .map(|g| g.generation)
            .unwrap_or(0);
        metrics::record_raft_query("get_group_state", start.elapsed().as_secs_f64());
        Ok(generation)
    }

    async fn upsert_group_member(
        &self,
        _group_id: &str,
        _member_id: &str,
        _metadata: &[u8],
    ) -> SlateDBResult<()> {
        // This is handled by JoinGroup
        Ok(())
    }

    async fn get_group_members(&self, group_id: &str) -> SlateDBResult<Vec<String>> {
        let start = Instant::now();
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let members = inner_state
            .group_domain
            .groups
            .get(group_id)
            .map(|g| g.members.keys().cloned().collect())
            .unwrap_or_default();
        metrics::record_raft_query("get_group_state", start.elapsed().as_secs_f64());
        Ok(members)
    }

    async fn get_group_leader(&self, group_id: &str) -> SlateDBResult<Option<String>> {
        let start = Instant::now();
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let leader = inner_state
            .group_domain
            .groups
            .get(group_id)
            .and_then(|g| g.leader_id.clone());
        metrics::record_raft_query("get_group_state", start.elapsed().as_secs_f64());
        Ok(leader)
    }

    async fn remove_group_member(&self, group_id: &str, member_id: &str) -> SlateDBResult<()> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::LeaveGroup {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
        });

        self.node.write(command).await?;
        Ok(())
    }

    async fn leave_group(&self, group_id: &str, member_id: &str) -> SlateDBResult<bool> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::LeaveGroup {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::LeftGroup) => Ok(true),
            CoordinationResponse::GroupDomainResponse(
                GroupResponse::UnknownMember { .. } | GroupResponse::GroupNotFound { .. },
            ) => Ok(false),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn update_member_heartbeat_with_generation(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> SlateDBResult<HeartbeatResult> {
        // Coalesce frequent heartbeats: most heartbeats arrive far more
        // often than the FSM eviction window, so we keep a soft per-member
        // timestamp and only propose to Raft when it has aged past the
        // duty-cycle threshold (or when the FSM-read fast path tells us
        // we have to propose anyway because the in-memory map is stale).
        // The FSM `last_heartbeat_ms` then advances at most once every
        // `session_timeout_ms / HEARTBEAT_RAFT_DUTY_DIVISOR`.
        const HEARTBEAT_RAFT_DUTY_DIVISOR: u64 = 3;
        let session_timeout_ms = self.config.default_session_timeout_ms;
        let propose_interval = std::time::Duration::from_millis(
            (session_timeout_ms / HEARTBEAT_RAFT_DUTY_DIVISOR).max(1000),
        );

        let key = (group_id.to_string(), member_id.to_string());

        // Fast path: read FSM to validate (group exists, member is known,
        // generation matches, group is Stable). If any of those checks
        // fail, fall through to the full propose so the FSM has the
        // chance to react (RebalanceInProgress, IllegalGeneration, etc.).
        let fast_path = {
            use crate::cluster::raft::domains::group::GroupStateEnum;
            let state_machine = self.node.state_machine();
            let state = state_machine.read().await;
            let inner_state = state.state().await;
            let group = inner_state.group_domain.groups.get(group_id);
            match group {
                None => Some(HeartbeatResult::UnknownMember),
                Some(g) if !g.members.contains_key(member_id) => {
                    Some(HeartbeatResult::UnknownMember)
                }
                Some(g) if g.generation != generation_id => {
                    Some(HeartbeatResult::IllegalGeneration)
                }
                Some(g) if !matches!(g.state, GroupStateEnum::Stable) => {
                    Some(HeartbeatResult::RebalanceInProgress)
                }
                Some(_) => None,
            }
        };

        // If the fast path produced a definitive answer, only propose if
        // we haven't refreshed the FSM `last_heartbeat_ms` in a while; for
        // non-Stable / unknown / illegal states the response is the
        // answer regardless of whether we propose.
        if let Some(result) = fast_path {
            // Definitive non-success answers: skip the propose. The
            // member-eviction sweep is only relevant for Stable groups
            // anyway.
            return Ok(result);
        }

        // Stable + valid: only propose if our last propose for this
        // (group, member) is older than the duty-cycle interval. Updating
        // the in-memory entry on the cheap path keeps us in lockstep with
        // wall-clock without paying for Raft.
        let now = std::time::Instant::now();
        let should_propose = match self.heartbeat_propose_state.get(&key) {
            Some(entry) => now.duration_since(*entry) >= propose_interval,
            None => true,
        };
        if !should_propose {
            return Ok(HeartbeatResult::Success);
        }

        let command = CoordinationCommand::GroupDomain(GroupCommand::Heartbeat {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            generation: generation_id,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        let result = match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::HeartbeatAck) => {
                HeartbeatResult::Success
            }
            CoordinationResponse::GroupDomainResponse(GroupResponse::RebalanceInProgress {
                ..
            }) => HeartbeatResult::RebalanceInProgress,
            CoordinationResponse::GroupDomainResponse(GroupResponse::UnknownMember { .. }) => {
                HeartbeatResult::UnknownMember
            }
            CoordinationResponse::GroupDomainResponse(GroupResponse::IllegalGeneration {
                ..
            }) => HeartbeatResult::IllegalGeneration,
            other => {
                return Err(SlateDBError::Storage(format!(
                    "Unexpected response: {:?}",
                    other
                )));
            }
        };
        if matches!(result, HeartbeatResult::Success) {
            self.heartbeat_propose_state.insert(key, now);
        } else {
            // On failure (IllegalGeneration / UnknownMember / Rebalance),
            // drop the cache entry so the next heartbeat re-validates
            // through the slow path immediately.
            self.heartbeat_propose_state.remove(&key);
        }
        Ok(result)
    }

    async fn validate_member_for_commit(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> SlateDBResult<HeartbeatResult> {
        use crate::cluster::raft::domains::group::GroupStateEnum;
        // Linearizable read: this validates an OffsetCommit against the
        // current group state. A stale read after a coordinator failover
        // could return Success against a generation the new coordinator
        // has already advanced past, allowing a fenced consumer to commit
        // offsets under the old generation.
        self.node.ensure_linearizable().await?;
        // Read-only validation of member and generation for offset commit
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let group = match inner_state.group_domain.groups.get(group_id) {
            Some(g) => g,
            None => return Ok(HeartbeatResult::UnknownMember),
        };

        if !group.members.contains_key(member_id) {
            return Ok(HeartbeatResult::UnknownMember);
        }

        if group.generation != generation_id {
            return Ok(HeartbeatResult::IllegalGeneration);
        }

        if !matches!(group.state, GroupStateEnum::Stable) {
            return Ok(HeartbeatResult::RebalanceInProgress);
        }

        Ok(HeartbeatResult::Success)
    }

    async fn set_assignments_atomically(
        &self,
        group_id: &str,
        leader_id: &str,
        expected_generation: i32,
        assignments: &[(String, Vec<u8>)],
    ) -> SlateDBResult<bool> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::SyncGroup {
            group_id: group_id.to_string(),
            member_id: leader_id.to_string(),
            generation: expected_generation,
            assignments: assignments.to_vec(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::SyncGroupResponse {
                ..
            }) => Ok(true),
            CoordinationResponse::GroupDomainResponse(GroupResponse::IllegalGeneration {
                ..
            }) => Ok(false),
            CoordinationResponse::GroupDomainResponse(GroupResponse::UnknownMember { .. }) => {
                Ok(false)
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn evict_expired_members(
        &self,
        _default_session_timeout_ms: u64,
    ) -> SlateDBResult<Vec<(String, String)>> {
        // Apply skew tolerance: a leader with a fast clock would otherwise
        // mass-evict members that were still inside their session window.
        let now = current_time_ms().saturating_sub(self.config.clock_skew_tolerance_ms);
        let command = CoordinationCommand::GroupDomain(GroupCommand::ExpireMembers {
            current_time_ms: now,
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::MembersExpired {
                expired,
            }) => Ok(expired),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn expire_committed_offsets(&self, ttl_ms: u64) -> SlateDBResult<usize> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::ExpireOffsets {
            current_time_ms: current_time_ms(),
            ttl_ms,
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::OffsetsExpired {
                count,
            }) => Ok(count),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn get_all_groups(&self) -> SlateDBResult<Vec<String>> {
        let start = Instant::now();
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let groups = inner_state.group_domain.groups.keys().cloned().collect();
        metrics::record_raft_query("get_group_state", start.elapsed().as_secs_f64());
        Ok(groups)
    }

    async fn delete_consumer_group(&self, group_id: &str) -> SlateDBResult<()> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::DeleteGroup {
            group_id: group_id.to_string(),
        });
        self.node.write(command).await?;
        Ok(())
    }

    async fn set_member_assignment(
        &self,
        _group_id: &str,
        _member_id: &str,
        _assignment: &[u8],
    ) -> SlateDBResult<()> {
        // Assignments are set via SyncGroup
        Ok(())
    }

    async fn get_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> SlateDBResult<Vec<u8>> {
        let start = Instant::now();
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let assignment = inner_state
            .group_domain
            .groups
            .get(group_id)
            .and_then(|g| g.assignments.get(member_id).cloned())
            .unwrap_or_default();
        metrics::record_raft_query("get_group_state", start.elapsed().as_secs_f64());
        Ok(assignment)
    }

    async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> SlateDBResult<()> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::CommitOffset {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
            offset,
            metadata: metadata.map(|s| s.to_string()),
            timestamp_ms: current_time_ms(),
        });

        self.node.write(command).await?;
        Ok(())
    }

    async fn fetch_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<(i64, Option<String>)> {
        use std::sync::Arc;

        let start = Instant::now();
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let key = (group_id.to_string(), Arc::from(topic), partition);
        let result = inner_state
            .group_domain
            .offsets
            .get(&key)
            .map(|o| (o.offset, o.metadata.clone()))
            .unwrap_or((-1, None));
        metrics::record_raft_query("fetch_offset", start.elapsed().as_secs_f64());
        Ok(result)
    }
}
