//! ConsumerGroupCoordinator implementation for RaftCoordinator.
//!
//! This module handles:
//! - Consumer group membership (join/leave)
//! - Group rebalancing
//! - Offset commit/fetch

use async_trait::async_trait;

use super::super::commands::{CoordinationCommand, CoordinationResponse};
use super::super::domains::{GroupCommand, GroupResponse};
use super::{RaftCoordinator, current_time_ms};

use crate::cluster::error::{HeartbeatResult, SlateDBError, SlateDBResult};
use crate::cluster::traits::ConsumerGroupCoordinator;

#[async_trait]
impl ConsumerGroupCoordinator for RaftCoordinator {
    async fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        protocol_metadata: &[u8],
        session_timeout_ms: i32,
    ) -> SlateDBResult<(i32, String, bool, String, Vec<String>)> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::JoinGroup {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            client_id: "".to_string(),
            client_host: "".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![("range".to_string(), protocol_metadata.to_vec())],
            session_timeout_ms,
            rebalance_timeout_ms: session_timeout_ms,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::JoinGroupResponse {
                generation,
                leader_id,
                member_id,
                members,
                protocol_name: _,
            }) => {
                let is_leader = leader_id == member_id;
                let member_ids: Vec<String> = members.iter().map(|m| m.member_id.clone()).collect();
                Ok((generation, member_id, is_leader, leader_id, member_ids))
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
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
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        Ok(inner_state
            .group_domain
            .groups
            .get(group_id)
            .map(|g| g.generation)
            .unwrap_or(0))
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
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        Ok(inner_state
            .group_domain
            .groups
            .get(group_id)
            .map(|g| g.members.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn get_group_leader(&self, group_id: &str) -> SlateDBResult<Option<String>> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        Ok(inner_state
            .group_domain
            .groups
            .get(group_id)
            .and_then(|g| g.leader_id.clone()))
    }

    async fn remove_group_member(&self, group_id: &str, member_id: &str) -> SlateDBResult<()> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::LeaveGroup {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
        });

        self.node.write(command).await?;
        Ok(())
    }

    async fn update_member_heartbeat_with_generation(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> SlateDBResult<HeartbeatResult> {
        let command = CoordinationCommand::GroupDomain(GroupCommand::Heartbeat {
            group_id: group_id.to_string(),
            member_id: member_id.to_string(),
            generation: generation_id,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::GroupDomainResponse(GroupResponse::HeartbeatAck) => {
                Ok(HeartbeatResult::Success)
            }
            CoordinationResponse::GroupDomainResponse(GroupResponse::UnknownMember { .. }) => {
                Ok(HeartbeatResult::UnknownMember)
            }
            CoordinationResponse::GroupDomainResponse(GroupResponse::IllegalGeneration {
                ..
            }) => Ok(HeartbeatResult::IllegalGeneration),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response: {:?}",
                other
            ))),
        }
    }

    async fn validate_member_for_commit(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> SlateDBResult<HeartbeatResult> {
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
        let command = CoordinationCommand::GroupDomain(GroupCommand::ExpireMembers {
            current_time_ms: current_time_ms(),
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

    async fn get_all_groups(&self) -> SlateDBResult<Vec<String>> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        Ok(inner_state.group_domain.groups.keys().cloned().collect())
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
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        Ok(inner_state
            .group_domain
            .groups
            .get(group_id)
            .and_then(|g| g.assignments.get(member_id).cloned())
            .unwrap_or_default())
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

        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let key = (group_id.to_string(), Arc::from(topic), partition);
        Ok(inner_state
            .group_domain
            .offsets
            .get(&key)
            .map(|o| (o.offset, o.metadata.clone()))
            .unwrap_or((-1, None)))
    }
}
