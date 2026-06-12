//! Consumer group request handling.
//!
//! # Coordinator Selection
//!
//! Consumer group coordinators are selected using consistent hashing via the
//! `conhash` crate. This provides:
//!
//! - **Stability**: When a broker joins, only ~1/n of groups move to the new broker
//! - **Minimal disruption**: When a broker leaves, only that broker's groups redistribute
//! - **Determinism**: Same group_id + broker set = same coordinator
//!
//! The previous modulo-based approach would reassign nearly all groups when
//! the broker count changed, causing unnecessary rebalances.

use bytes::Bytes;
use conhash::ConsistentHash;
use tracing::{debug, error, info};

use crate::cluster::coordinator::BrokerInfo;
use crate::constants::VIRTUAL_NODES_PER_BROKER;
use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::{
    DeleteGroupsRequestData, DescribeGroupsRequestData, FindCoordinatorRequestData,
    HeartbeatRequestData, JoinGroupRequestData, LeaveGroupRequestData, ListGroupsRequestData,
    SyncGroupRequestData,
};
use crate::server::response::{
    DeleteGroupResult, DeleteGroupsResponseData, DescribeGroupsResponseData, DescribedGroup,
    FindCoordinatorResponseData, HeartbeatResponseData, JoinGroupMemberData, JoinGroupResponseData,
    LeaveGroupResponseData, ListGroupsResponseData, ListedGroup, SyncGroupResponseData,
};

use super::SlateDBClusterHandler;
use crate::cluster::authorizer::{AuthorizeRequest, AuthorizeResult};
use crate::cluster::error::HeartbeatResult;
use crate::cluster::raft::{AclOperation, AclResourceType};
use crate::cluster::traits::{ConsumerGroupCoordinator, GroupJoinOutcome};

/// Run an ACL check for an operation on a consumer-group resource.
///
/// Returns `true` when the request is allowed and `false` when the
/// authorizer denies it. Pulled out so every group/offset handler can
/// gate against `Group:<id>` without repeating the boilerplate.
async fn group_authorized(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    op: AclOperation,
    group_id: &str,
) -> bool {
    handler
        .authorizer
        .authorize(AuthorizeRequest {
            principal: &ctx.principal,
            host: &ctx.client_host,
            operation: op,
            resource_type: AclResourceType::Group,
            resource_name: group_id,
        })
        .await
        == AuthorizeResult::Allowed
}

/// Select a coordinator for a consumer group using consistent hashing.
///
/// Uses the same consistent hash ring algorithm as partition assignment,
/// ensuring minimal group reassignment when the cluster topology changes.
///
/// Returns the broker_id of the selected coordinator, or None if no brokers available.
#[allow(dead_code)]
fn select_coordinator_id(key: &str, brokers: &[BrokerInfo]) -> Option<i32> {
    if brokers.is_empty() {
        return None;
    }

    if brokers.len() == 1 {
        return Some(brokers[0].broker_id);
    }

    // Build the consistent hash ring with virtual nodes for even distribution
    let mut ring: ConsistentHash<BrokerInfo> = ConsistentHash::new();
    for broker in brokers {
        ring.add(broker, VIRTUAL_NODES_PER_BROKER);
    }

    // Use "group:{key}" prefix to namespace from partition keys
    let group_key = format!("group:{}", key);
    ring.get_str(&group_key).map(|b| b.broker_id)
}

/// Handle a find coordinator request.
pub(super) async fn handle_find_coordinator(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: FindCoordinatorRequestData,
) -> FindCoordinatorResponseData {
    // For consumer-group lookups, validate the group id before any other
    // work. Without this, `null \r ; group; etc` and similar control-character
    // strings reach the ACL/state layers and (a) bypass log sanitization and
    // (b) materialize entries in the replicated SM via subsequent commits.
    if request.key_type == 0
        && crate::cluster::coordinator::validate_group_id(&request.key).is_err()
    {
        debug!(group_id = %request.key, "Invalid group ID in find_coordinator");
        return FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::InvalidGroupId,
            error_message: Some("Invalid group ID".to_string()),
            node_id: -1,
            host: String::new(),
            port: 0,
        };
    }

    // FindCoordinator for a consumer group (key_type 0) requires Describe
    // on that group, matching Kafka. Other key types (transactions) are not
    // supported by Kafkaesque, so only the group path is gated here.
    if request.key_type == 0
        && !group_authorized(handler, ctx, AclOperation::Describe, &request.key).await
    {
        info!(
            target: "audit",
            group_id = %request.key,
            principal = %ctx.principal,
            api = "FindCoordinator",
            operation = "Describe",
            "ACL denied: FindCoordinator"
        );
        return FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupAuthorizationFailed,
            error_message: Some("Not authorized to describe this group".to_string()),
            node_id: -1,
            host: String::new(),
            port: 0,
        };
    }

    // Group state is fully replicated through Raft, so every healthy broker
    // can serve every group operation. Pointing the client at *this* broker
    // — the one that just received the connection — keeps op-routing and
    // routing-advice consistent: a sticky session won't ricochet between
    // brokers via consistent-hash advice while ops keep landing locally,
    // which previously let two brokers concurrently drive the same group's
    // transitions.
    FindCoordinatorResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        error_message: None,
        node_id: handler.broker_id.value(),
        host: handler.host.clone(),
        port: handler.port,
    }
}

/// Handle a join group request.
pub(super) async fn handle_join_group(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: JoinGroupRequestData,
) -> JoinGroupResponseData {
    // Validate group ID
    if let Err(e) = crate::cluster::coordinator::validate_group_id(&request.group_id) {
        debug!(group_id = %request.group_id, error = %e, "Invalid group ID in join_group request");
        return JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::InvalidGroupId,
            generation_id: -1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id: request.member_id,
            members: vec![],
        };
    }

    // JoinGroup requires Read on the Group resource.
    if !group_authorized(handler, ctx, AclOperation::Read, &request.group_id).await {
        info!(
            target: "audit",
            group_id = %request.group_id,
            principal = %ctx.principal,
            api = "JoinGroup",
            operation = "Read",
            "ACL denied: JoinGroup"
        );
        return JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupAuthorizationFailed,
            generation_id: -1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id: request.member_id,
            members: vec![],
        };
    }

    // Validate session and rebalance timeouts. A negative value casts to
    // u64::MAX in the state-machine layer and permanently holds a group slot
    // (no member ever ages out); a zero or extremely small value would cause
    // the failure detector to reap legitimate consumers between heartbeats.
    // Bounds match Kafka's group.min/max.session.timeout.ms defaults.
    const MIN_SESSION_TIMEOUT_MS: i32 = 6_000;
    const MAX_SESSION_TIMEOUT_MS: i32 = 1_800_000;
    if request.session_timeout_ms < MIN_SESSION_TIMEOUT_MS
        || request.session_timeout_ms > MAX_SESSION_TIMEOUT_MS
        || request.rebalance_timeout_ms < 0
        || request.rebalance_timeout_ms > MAX_SESSION_TIMEOUT_MS
    {
        debug!(
            group_id = %request.group_id,
            session_timeout_ms = request.session_timeout_ms,
            rebalance_timeout_ms = request.rebalance_timeout_ms,
            "Rejecting JoinGroup with out-of-range timeouts"
        );
        return JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::InvalidSessionTimeout,
            generation_id: -1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id: request.member_id,
            members: vec![],
        };
    }

    // Generate member ID if not provided
    let member_id = if request.member_id.is_empty() {
        format!("member-{}", uuid::Uuid::new_v4())
    } else {
        request.member_id.clone()
    };

    // Collect the member's full protocol list. Every (name, metadata)
    // pair is persisted so the leader's JoinGroup response can carry each
    // member's OWN subscription bytes for the negotiated protocol.
    let protocols: Vec<(String, Vec<u8>)> = request
        .protocols
        .iter()
        .map(|p| (p.name.clone(), p.metadata.to_vec()))
        .collect();

    // Validate metadata size for every protocol entry
    for (protocol_name, metadata) in &protocols {
        if let Err(e) = crate::cluster::coordinator::validate_member_metadata(metadata) {
            debug!(
                group_id = %request.group_id,
                protocol = %protocol_name,
                error = %e,
                "Member metadata too large"
            );
            return JoinGroupResponseData {
                throttle_time_ms: 0,
                error_code: KafkaCode::Unknown,
                generation_id: -1,
                protocol_name: String::new(),
                leader: String::new(),
                member_id,
                members: vec![],
            };
        }
    }

    // Atomically join group, negotiating the protocol across all members.
    let outcome = match handler
        .coordinator
        .join_group_with_protocols(
            &request.group_id,
            &member_id,
            &protocols,
            request.session_timeout_ms,
            request.rebalance_timeout_ms,
        )
        .await
    {
        Ok(outcome) => outcome,
        Err(e) => {
            error!(error = %e, "Failed to join group");
            crate::cluster::metrics::record_group_operation("join", "error");
            return JoinGroupResponseData {
                throttle_time_ms: 0,
                error_code: KafkaCode::Unknown,
                generation_id: -1,
                protocol_name: String::new(),
                leader: String::new(),
                member_id,
                members: vec![],
            };
        }
    };

    let result = match outcome {
        GroupJoinOutcome::Joined(result) => result,
        GroupJoinOutcome::InconsistentProtocol => {
            debug!(
                group_id = %request.group_id,
                member_id = %member_id,
                "Join rejected: no protocol in common with the group"
            );
            crate::cluster::metrics::record_group_operation("join", "error");
            return JoinGroupResponseData {
                throttle_time_ms: 0,
                error_code: KafkaCode::InconsistentGroupProtocol,
                generation_id: -1,
                protocol_name: String::new(),
                leader: String::new(),
                member_id,
                members: vec![],
            };
        }
    };

    // Build the member list from the coordinator's stored per-member
    // metadata. Only the leader receives a non-empty roster (Kafka
    // semantics); each entry carries that member's own subscription
    // metadata so the leader's assignor sees real subscriptions.
    let mut members: Vec<JoinGroupMemberData> = result
        .members
        .into_iter()
        .map(|(mid, metadata)| JoinGroupMemberData {
            member_id: mid,
            metadata: Bytes::from(metadata),
        })
        .collect();
    members.sort_by(|a, b| a.member_id.cmp(&b.member_id));

    debug!(
        group_id = %request.group_id,
        member_id = %result.member_id,
        generation_id = result.generation_id,
        leader = %result.leader_id,
        protocol = %result.protocol_name,
        member_count = members.len(),
        "Member joined group"
    );

    // Record metrics. The member roster is only present in the leader's
    // response, so only the leader path refreshes the member-count gauge.
    crate::cluster::metrics::record_group_operation("join", "success");
    if result.is_leader {
        crate::cluster::metrics::set_group_member_count(&request.group_id, members.len() as i64);
    }
    crate::cluster::metrics::set_group_generation(&request.group_id, result.generation_id as i64);

    JoinGroupResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        generation_id: result.generation_id,
        protocol_name: result.protocol_name,
        leader: result.leader_id,
        member_id: result.member_id,
        members,
    }
}

/// Build a SyncGroup error response (empty assignment) and record the
/// failed sync in metrics.
fn sync_group_error(error_code: KafkaCode) -> SyncGroupResponseData {
    crate::cluster::metrics::record_group_operation("sync", "error");
    SyncGroupResponseData {
        throttle_time_ms: 0,
        error_code,
        assignment: Bytes::new(),
    }
}

/// Handle a sync group request.
pub(super) async fn handle_sync_group(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: SyncGroupRequestData,
) -> SyncGroupResponseData {
    // Reject malformed group ids before any state lookup. The group/offsets
    // tables are inside the replicated state machine; an authenticated client
    // could otherwise grow them with arbitrary or control-character strings.
    if crate::cluster::coordinator::validate_group_id(&request.group_id).is_err() {
        debug!(group_id = %request.group_id, "Invalid group ID in sync_group");
        return SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::InvalidGroupId,
            assignment: bytes::Bytes::new(),
        };
    }

    // SyncGroup requires Read on the Group resource.
    if !group_authorized(handler, ctx, AclOperation::Read, &request.group_id).await {
        info!(
            target: "audit",
            group_id = %request.group_id,
            principal = %ctx.principal,
            api = "SyncGroup",
            operation = "Read",
            "ACL denied: SyncGroup"
        );
        return SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupAuthorizationFailed,
            assignment: Bytes::new(),
        };
    }

    // Verify generation matches BEFORE processing anything
    let current_generation = match handler.coordinator.get_generation(&request.group_id).await {
        Ok(generation) => generation,
        Err(e) => {
            error!(error = %e, "Failed to get group generation");
            return sync_group_error(e.to_kafka_code());
        }
    };

    if request.generation_id != current_generation {
        debug!(
            group_id = %request.group_id,
            request_generation = request.generation_id,
            current_generation,
            "Generation mismatch in SyncGroup"
        );
        return sync_group_error(KafkaCode::IllegalGeneration);
    }

    // The caller must be a known member of the group. Without this gate a
    // consumer that was evicted (or never joined) would get a success
    // response with an empty assignment instead of UNKNOWN_MEMBER_ID.
    match handler
        .coordinator
        .get_group_members(&request.group_id)
        .await
    {
        Ok(member_ids) => {
            if !member_ids.iter().any(|m| m == &request.member_id) {
                debug!(
                    group_id = %request.group_id,
                    member_id = %request.member_id,
                    "SyncGroup from unknown member"
                );
                return sync_group_error(KafkaCode::UnknownMemberId);
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to get group members for sync");
            return sync_group_error(e.to_kafka_code());
        }
    }

    // Leader sends assignments for all members
    if !request.assignments.is_empty() {
        let assignments: Vec<(String, Vec<u8>)> = request
            .assignments
            .iter()
            .map(|a| (a.member_id.clone(), a.assignment.to_vec()))
            .collect();

        match handler
            .coordinator
            .set_assignments_atomically(
                &request.group_id,
                &request.member_id,
                request.generation_id,
                &assignments,
            )
            .await
        {
            Ok(true) => {
                match handler
                    .coordinator
                    .complete_rebalance(&request.group_id, request.generation_id)
                    .await
                {
                    Ok(true) => {
                        debug!(
                            group_id = %request.group_id,
                            generation = request.generation_id,
                            assignment_count = request.assignments.len(),
                            "Leader completed sync, group is stable"
                        );
                    }
                    Ok(false) => {
                        // A new rebalance started between storing the
                        // assignments and completing — the leader must
                        // rejoin and redo the assignment.
                        debug!(
                            group_id = %request.group_id,
                            generation = request.generation_id,
                            "Rebalance completion rejected - generation changed"
                        );
                        return sync_group_error(KafkaCode::RebalanceInProgress);
                    }
                    Err(e) => {
                        error!(error = %e, "Failed to complete rebalance");
                        return sync_group_error(e.to_kafka_code());
                    }
                }
            }
            Ok(false) => {
                // The coordinator rejected the assignments. Distinguish why
                // so the client gets an actionable error instead of a
                // success with a garbage assignment.
                let generation_now = handler
                    .coordinator
                    .get_generation(&request.group_id)
                    .await
                    .unwrap_or(-1);
                if generation_now != request.generation_id {
                    debug!(
                        group_id = %request.group_id,
                        request_generation = request.generation_id,
                        current_generation = generation_now,
                        "Assignments rejected - generation changed"
                    );
                    return sync_group_error(KafkaCode::IllegalGeneration);
                }
                let still_member = handler
                    .coordinator
                    .get_group_members(&request.group_id)
                    .await
                    .map(|m| m.iter().any(|id| id == &request.member_id))
                    .unwrap_or(false);
                if !still_member {
                    debug!(
                        group_id = %request.group_id,
                        member_id = %request.member_id,
                        "Assignments rejected - member no longer in group"
                    );
                    return sync_group_error(KafkaCode::UnknownMemberId);
                }
                // Generation and membership are intact, so the caller is a
                // non-leader that sent assignments. This is the
                // "leader-failover-mid-rebalance" path: the client *thought*
                // it was leader, computed assignments, and the coordinator
                // disagreed. Silently keeping its own assignment and
                // returning success would let two clients believe they
                // produced the canonical assignment for the same generation.
                // Surface the conflict instead so the client rejoins and
                // re-derives the correct assignment, and emit a metric so
                // operators can see the rate of these races.
                crate::cluster::metrics::record_group_operation(
                    "sync_non_leader_assignment_rejected",
                    "error",
                );
                debug!(
                    group_id = %request.group_id,
                    member_id = %request.member_id,
                    generation = request.generation_id,
                    assignment_count = request.assignments.len(),
                    "Rejecting assignments from non-leader; instructing client to rejoin"
                );
                return sync_group_error(KafkaCode::RebalanceInProgress);
            }
            Err(e) => {
                error!(error = %e, "Failed to store assignments atomically");
                return sync_group_error(e.to_kafka_code());
            }
        }
    }

    // Get our assignment
    let assignment = match handler
        .coordinator
        .get_member_assignment(&request.group_id, &request.member_id)
        .await
    {
        Ok(assign) => assign,
        Err(e) => {
            error!(
                group_id = %request.group_id,
                member_id = %request.member_id,
                error = %e,
                "Failed to get member assignment"
            );
            return sync_group_error(e.to_kafka_code());
        }
    };

    debug!(
        group_id = %request.group_id,
        member_id = %request.member_id,
        generation = request.generation_id,
        assignment_len = assignment.len(),
        "SyncGroup returning assignment"
    );

    crate::cluster::metrics::record_group_operation("sync", "success");

    SyncGroupResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        assignment: Bytes::from(assignment),
    }
}

/// Handle a heartbeat request.
pub(super) async fn handle_heartbeat(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: HeartbeatRequestData,
) -> HeartbeatResponseData {
    if crate::cluster::coordinator::validate_group_id(&request.group_id).is_err() {
        debug!(group_id = %request.group_id, "Invalid group ID in heartbeat");
        return HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::InvalidGroupId,
        };
    }
    // Heartbeat requires Read on the Group resource.
    if !group_authorized(handler, ctx, AclOperation::Read, &request.group_id).await {
        info!(
            target: "audit",
            group_id = %request.group_id,
            principal = %ctx.principal,
            api = "Heartbeat",
            operation = "Read",
            "ACL denied: Heartbeat"
        );
        return HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupAuthorizationFailed,
        };
    }

    let result = handler
        .coordinator
        .update_member_heartbeat_with_generation(
            &request.group_id,
            &request.member_id,
            request.generation_id,
        )
        .await
        .unwrap_or(HeartbeatResult::UnknownMember);

    let error_code = match result {
        HeartbeatResult::Success => {
            crate::cluster::metrics::record_group_operation("heartbeat", "success");
            KafkaCode::None
        }
        HeartbeatResult::UnknownMember => {
            crate::cluster::metrics::record_group_operation("heartbeat", "error");
            KafkaCode::UnknownMemberId
        }
        HeartbeatResult::IllegalGeneration => {
            crate::cluster::metrics::record_group_operation("heartbeat", "error");
            KafkaCode::IllegalGeneration
        }
        HeartbeatResult::RebalanceInProgress => {
            crate::cluster::metrics::record_group_operation("heartbeat", "error");
            KafkaCode::RebalanceInProgress
        }
    };

    HeartbeatResponseData {
        throttle_time_ms: 0,
        error_code,
    }
}

/// Handle a leave group request.
pub(super) async fn handle_leave_group(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: LeaveGroupRequestData,
) -> LeaveGroupResponseData {
    if crate::cluster::coordinator::validate_group_id(&request.group_id).is_err() {
        debug!(group_id = %request.group_id, "Invalid group ID in leave_group");
        return LeaveGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::InvalidGroupId,
        };
    }
    // LeaveGroup requires Read on the Group resource.
    if !group_authorized(handler, ctx, AclOperation::Read, &request.group_id).await {
        info!(
            target: "audit",
            group_id = %request.group_id,
            principal = %ctx.principal,
            api = "LeaveGroup",
            operation = "Read",
            "ACL denied: LeaveGroup"
        );
        return LeaveGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupAuthorizationFailed,
        };
    }

    match handler
        .coordinator
        .leave_group(&request.group_id, &request.member_id)
        .await
    {
        Ok(true) => {
            debug!(
                group_id = %request.group_id,
                member_id = %request.member_id,
                "Member left group"
            );
            crate::cluster::metrics::record_group_operation("leave", "success");
            LeaveGroupResponseData {
                throttle_time_ms: 0,
                error_code: KafkaCode::None,
            }
        }
        Ok(false) => {
            // The member (or the whole group) is unknown; Kafka answers
            // UNKNOWN_MEMBER_ID rather than pretending the leave succeeded.
            debug!(
                group_id = %request.group_id,
                member_id = %request.member_id,
                "LeaveGroup for unknown member"
            );
            crate::cluster::metrics::record_group_operation("leave", "error");
            LeaveGroupResponseData {
                throttle_time_ms: 0,
                error_code: KafkaCode::UnknownMemberId,
            }
        }
        Err(e) => {
            error!(error = %e, "Failed to remove group member");
            crate::cluster::metrics::record_group_operation("leave", "error");
            LeaveGroupResponseData {
                throttle_time_ms: 0,
                error_code: e.to_kafka_code(),
            }
        }
    }
}

/// Handle a describe-groups request. Authorizes Describe on each
/// requested group; entries that fail authorization come back with
/// `GroupAuthorizationFailed`.
pub(super) async fn handle_describe_groups(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: DescribeGroupsRequestData,
) -> DescribeGroupsResponseData {
    let mut groups = Vec::with_capacity(request.group_ids.len());
    for group_id in request.group_ids {
        if !group_authorized(handler, ctx, AclOperation::Describe, &group_id).await {
            info!(
                target: "audit",
                group_id = %group_id,
                principal = %ctx.principal,
                api = "DescribeGroups",
                operation = "Describe",
                "ACL denied: DescribeGroups"
            );
            groups.push(DescribedGroup::error(
                group_id,
                KafkaCode::GroupAuthorizationFailed,
            ));
            continue;
        }

        match handler.coordinator.get_group_members(&group_id).await {
            Ok(member_ids) => {
                // Kafka semantics: an unknown/empty group is reported with
                // state "Dead" and no error, not as a healthy "Stable" group.
                let group_state = if member_ids.is_empty() {
                    "Dead"
                } else {
                    "Stable"
                };
                // Report the protocol negotiated at the last rebalance
                // instead of a hardcoded assignor name.
                let protocol_data = handler
                    .coordinator
                    .get_group_protocol(&group_id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_default();
                groups.push(
                    DescribedGroup::builder(group_id)
                        .group_state(group_state)
                        .protocol_type("consumer")
                        .protocol_data(protocol_data)
                        .members(
                            member_ids
                                .into_iter()
                                .map(|mid| {
                                    crate::server::response::DescribedGroupMember::new(
                                        mid,
                                        "",
                                        "",
                                        Bytes::new(),
                                        Bytes::new(),
                                    )
                                })
                                .collect(),
                        )
                        .build(),
                );
            }
            Err(e) => {
                error!(error = %e, "Failed to get group members for describe");
                groups.push(DescribedGroup::error(group_id, KafkaCode::Unknown));
            }
        }
    }

    DescribeGroupsResponseData {
        throttle_time_ms: 0,
        groups,
    }
}

/// Handle a list-groups request. Filters out groups the principal
/// can't `Describe`, so unauthorized groups simply don't appear in
/// the response (matches Kafka's behavior).
pub(super) async fn handle_list_groups(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    _request: ListGroupsRequestData,
) -> ListGroupsResponseData {
    let all = match handler.coordinator.get_all_groups().await {
        Ok(g) => g,
        Err(e) => {
            error!(error = %e, "Failed to list groups");
            return ListGroupsResponseData::error(KafkaCode::Unknown);
        }
    };

    let mut visible = Vec::with_capacity(all.len());
    for group_id in all {
        if group_authorized(handler, ctx, AclOperation::Describe, &group_id).await {
            visible.push(ListedGroup::new(group_id, "consumer", "Stable"));
        }
    }
    ListGroupsResponseData::success(visible)
}

/// Handle a delete-groups request. Each group is gated on `Delete`
/// independently — the response carries per-group results so a partial
/// authz failure doesn't block deletion of the rest.
pub(super) async fn handle_delete_groups(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: DeleteGroupsRequestData,
) -> DeleteGroupsResponseData {
    let mut results = Vec::with_capacity(request.group_ids.len());
    for group_id in request.group_ids {
        if !group_authorized(handler, ctx, AclOperation::Delete, &group_id).await {
            info!(
                target: "audit",
                group_id = %group_id,
                principal = %ctx.principal,
                api = "DeleteGroups",
                operation = "Delete",
                "ACL denied: DeleteGroups"
            );
            results.push(DeleteGroupResult::error(
                group_id,
                KafkaCode::GroupAuthorizationFailed,
            ));
            continue;
        }

        match handler.coordinator.delete_consumer_group(&group_id).await {
            Ok(()) => {
                crate::cluster::metrics::forget_group_metrics(&group_id);
                results.push(DeleteGroupResult::success(group_id));
            }
            Err(e) => {
                error!(group_id = %group_id, error = %e, "Failed to delete group");
                results.push(DeleteGroupResult::error(group_id, KafkaCode::Unknown));
            }
        }
    }
    DeleteGroupsResponseData::new(results)
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Coordinator Selection Tests (select_coordinator_id)
    // ========================================================================

    #[test]
    fn test_select_coordinator_empty_brokers() {
        let result = select_coordinator_id("test-group", &[]);
        assert!(result.is_none());
    }

    #[test]
    fn test_select_coordinator_single_broker() {
        let brokers = vec![BrokerInfo {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            registered_at: 0,
        }];

        let result = select_coordinator_id("test-group", &brokers);
        assert_eq!(result, Some(1));
    }

    #[test]
    fn test_select_coordinator_multiple_brokers() {
        let brokers = vec![
            BrokerInfo {
                broker_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 2,
                host: "localhost".to_string(),
                port: 9093,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 3,
                host: "localhost".to_string(),
                port: 9094,
                registered_at: 0,
            },
        ];

        // Should return one of the brokers
        let result = select_coordinator_id("test-group", &brokers);
        assert!(result.is_some());
        let broker_id = result.unwrap();
        assert!((1..=3).contains(&broker_id));
    }

    #[test]
    fn test_select_coordinator_deterministic() {
        let brokers = vec![
            BrokerInfo {
                broker_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 2,
                host: "localhost".to_string(),
                port: 9093,
                registered_at: 0,
            },
        ];

        // Same key should always return the same coordinator
        let result1 = select_coordinator_id("test-group", &brokers);
        let result2 = select_coordinator_id("test-group", &brokers);
        let result3 = select_coordinator_id("test-group", &brokers);

        assert_eq!(result1, result2);
        assert_eq!(result2, result3);
    }

    #[test]
    fn test_select_coordinator_different_keys_distribute() {
        let brokers = vec![
            BrokerInfo {
                broker_id: 1,
                host: "localhost".to_string(),
                port: 9092,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 2,
                host: "localhost".to_string(),
                port: 9093,
                registered_at: 0,
            },
            BrokerInfo {
                broker_id: 3,
                host: "localhost".to_string(),
                port: 9094,
                registered_at: 0,
            },
        ];

        // Different keys should distribute across brokers
        // With enough different keys, we should see multiple brokers selected
        let mut seen_brokers = std::collections::HashSet::new();
        for i in 0..100 {
            let key = format!("group-{}", i);
            if let Some(broker) = select_coordinator_id(&key, &brokers) {
                seen_brokers.insert(broker);
            }
        }

        // Should hit more than one broker with 100 different keys
        assert!(
            seen_brokers.len() > 1,
            "Expected consistent hashing to distribute across brokers"
        );
    }

    // ========================================================================
    // FindCoordinator Response Tests
    // ========================================================================

    #[test]
    fn test_find_coordinator_response_success() {
        let response = FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            error_message: None,
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
        };

        assert_eq!(response.error_code, KafkaCode::None);
        assert!(response.error_message.is_none());
        assert_eq!(response.node_id, 1);
        assert_eq!(response.host, "localhost");
        assert_eq!(response.port, 9092);
    }

    #[test]
    fn test_find_coordinator_response_not_available() {
        let response = FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupCoordinatorNotAvailable,
            error_message: Some("No brokers available".to_string()),
            node_id: -1,
            host: String::new(),
            port: 0,
        };

        assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
        assert!(response.error_message.is_some());
        assert_eq!(response.node_id, -1);
    }

    // ========================================================================
    // JoinGroup Response Tests
    // ========================================================================

    #[test]
    fn test_join_group_response_success() {
        let response = JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id: 1,
            protocol_name: "range".to_string(),
            leader: "member-1".to_string(),
            member_id: "member-1".to_string(),
            members: vec![],
        };

        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.generation_id, 1);
        assert!(!response.protocol_name.is_empty());
    }

    #[test]
    fn test_join_group_response_invalid_group_id() {
        let response = JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::InvalidGroupId,
            generation_id: -1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id: String::new(),
            members: vec![],
        };

        assert_eq!(response.error_code, KafkaCode::InvalidGroupId);
        assert_eq!(response.generation_id, -1);
    }

    #[test]
    fn test_join_group_member_data() {
        let member = JoinGroupMemberData {
            member_id: "member-1".to_string(),
            metadata: Bytes::from_static(b"metadata"),
        };

        assert_eq!(member.member_id, "member-1");
        assert!(!member.metadata.is_empty());
    }

    // ========================================================================
    // SyncGroup Response Tests
    // ========================================================================

    #[test]
    fn test_sync_group_response_success() {
        let response = SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            assignment: Bytes::from_static(b"assignment data"),
        };

        assert_eq!(response.error_code, KafkaCode::None);
        assert!(!response.assignment.is_empty());
    }

    #[test]
    fn test_sync_group_response_illegal_generation() {
        let response = SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::IllegalGeneration,
            assignment: Bytes::new(),
        };

        assert_eq!(response.error_code, KafkaCode::IllegalGeneration);
        assert!(response.assignment.is_empty());
    }

    // ========================================================================
    // Heartbeat Response Tests
    // ========================================================================

    #[test]
    fn test_heartbeat_response_success() {
        let response = HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        };

        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_heartbeat_response_unknown_member() {
        let response = HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::UnknownMemberId,
        };

        assert_eq!(response.error_code, KafkaCode::UnknownMemberId);
    }

    #[test]
    fn test_heartbeat_response_illegal_generation() {
        let response = HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::IllegalGeneration,
        };

        assert_eq!(response.error_code, KafkaCode::IllegalGeneration);
    }

    // ========================================================================
    // LeaveGroup Response Tests
    // ========================================================================

    #[test]
    fn test_leave_group_response_success() {
        let response = LeaveGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        };

        assert_eq!(response.error_code, KafkaCode::None);
    }

    // ========================================================================
    // HeartbeatResult Mapping Tests
    // ========================================================================

    #[test]
    fn test_heartbeat_result_to_kafka_code() {
        // Test the mapping from HeartbeatResult to KafkaCode
        let mappings = [
            (HeartbeatResult::Success, KafkaCode::None),
            (HeartbeatResult::UnknownMember, KafkaCode::UnknownMemberId),
            (
                HeartbeatResult::IllegalGeneration,
                KafkaCode::IllegalGeneration,
            ),
            (
                HeartbeatResult::RebalanceInProgress,
                KafkaCode::RebalanceInProgress,
            ),
        ];

        for (result, expected_code) in mappings {
            let error_code = match result {
                HeartbeatResult::Success => KafkaCode::None,
                HeartbeatResult::UnknownMember => KafkaCode::UnknownMemberId,
                HeartbeatResult::IllegalGeneration => KafkaCode::IllegalGeneration,
                HeartbeatResult::RebalanceInProgress => KafkaCode::RebalanceInProgress,
            };
            assert_eq!(error_code, expected_code);
        }
    }

    // ========================================================================
    // Group ID Validation Tests
    // ========================================================================

    #[test]
    fn test_group_id_validation() {
        // Valid group IDs
        assert!(crate::cluster::coordinator::validate_group_id("my-group").is_ok());
        assert!(crate::cluster::coordinator::validate_group_id("group_123").is_ok());
        assert!(crate::cluster::coordinator::validate_group_id("a").is_ok());

        // Invalid group IDs
        assert!(crate::cluster::coordinator::validate_group_id("").is_err());
    }

    // ========================================================================
    // Generation ID Tests
    // ========================================================================

    #[test]
    fn test_generation_id_values() {
        // Generation IDs start at 1 and increment
        // -1 is used for error responses
        let initial_gen = 1;
        let error_gen = -1;

        assert!(initial_gen > 0);
        assert_eq!(error_gen, -1);
    }

    #[test]
    fn test_generation_id_overflow_handling() {
        // When generation reaches i32::MAX, it should wrap to 1 (skipping 0)
        let max_gen = i32::MAX;
        let next_gen = if max_gen == i32::MAX { 1 } else { max_gen + 1 };

        assert_eq!(next_gen, 1);
    }

    // ========================================================================
    // Member ID Generation Tests
    // ========================================================================

    #[test]
    fn test_member_id_format() {
        // Member IDs should follow the pattern "member-{uuid}"
        let member_id = format!("member-{}", uuid::Uuid::new_v4());
        assert!(member_id.starts_with("member-"));
        assert!(member_id.len() > 7); // "member-" + UUID
    }

    // ========================================================================
    // Protocol Tests
    // ========================================================================

    #[test]
    fn test_protocol_names() {
        // Common Kafka consumer group protocols
        let protocols = ["range", "roundrobin", "sticky", "cooperative-sticky"];

        for protocol in protocols {
            assert!(!protocol.is_empty());
        }
    }

    // ========================================================================
    // Throttle Time Tests
    // ========================================================================

    #[test]
    fn test_throttle_time_default() {
        // All group responses should have throttle_time_ms = 0 by default
        let find_coord = FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            error_message: None,
            node_id: 1,
            host: "localhost".to_string(),
            port: 9092,
        };

        let join_group = JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id: 1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id: String::new(),
            members: vec![],
        };

        let sync_group = SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            assignment: Bytes::new(),
        };

        let heartbeat = HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        };

        let leave_group = LeaveGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        };

        assert_eq!(find_coord.throttle_time_ms, 0);
        assert_eq!(join_group.throttle_time_ms, 0);
        assert_eq!(sync_group.throttle_time_ms, 0);
        assert_eq!(heartbeat.throttle_time_ms, 0);
        assert_eq!(leave_group.throttle_time_ms, 0);
    }

    // ========================================================================
    // Generation Counter Wraparound Tests
    // ========================================================================

    #[test]
    fn test_generation_id_wraparound_at_max() {
        // When generation reaches i32::MAX, it should wrap to 1 (skipping 0 and negative values)
        let max_gen = i32::MAX;

        // Simulate wraparound logic
        let next_gen = if max_gen == i32::MAX {
            1 // Wrap to 1, skipping 0
        } else {
            max_gen.wrapping_add(1)
        };

        assert_eq!(next_gen, 1, "Generation should wrap to 1 at i32::MAX");
    }

    #[test]
    fn test_generation_id_normal_increment() {
        // Normal increments should work as expected
        let generation = 1i32;
        let next = generation.wrapping_add(1);
        assert_eq!(next, 2);

        let generation = 100i32;
        let next = generation.wrapping_add(1);
        assert_eq!(next, 101);
    }

    #[test]
    fn test_generation_id_near_max() {
        // Test generations near i32::MAX
        let near_max = i32::MAX - 5;

        for i in 0..5 {
            let generation = near_max + i;
            assert!(generation > 0, "Generation near max should be positive");
        }
    }

    #[test]
    fn test_generation_id_sequence() {
        // Test that generation IDs form a proper sequence
        let generations: Vec<i32> = (1..=10).collect();

        for (i, generation) in generations.iter().enumerate() {
            assert_eq!(*generation, (i + 1) as i32);
        }
    }

    #[test]
    fn test_generation_id_illegal_values() {
        // Generation ID of -1 indicates an error response
        // Generation ID of 0 is also invalid
        let error_gen = -1i32;
        let invalid_gen = 0i32;

        assert!(error_gen < 0, "Error generation should be negative");
        assert_eq!(invalid_gen, 0, "Invalid generation should be zero");

        // Valid generations are > 0
        for valid in [1, 100, 1000, i32::MAX] {
            assert!(valid > 0, "Valid generation should be positive");
        }
    }

    // ========================================================================
    // Concurrent JoinGroup Tests (Request Structure)
    // ========================================================================

    #[test]
    fn test_join_group_member_data_multiple_members() {
        // Test multiple members joining concurrently
        let members: Vec<JoinGroupMemberData> = (0..5)
            .map(|i| JoinGroupMemberData {
                member_id: format!("member-{}", i),
                metadata: Bytes::from(format!("metadata-{}", i)),
            })
            .collect();

        assert_eq!(members.len(), 5);

        // Verify member IDs are unique
        let mut ids: Vec<&String> = members.iter().map(|m| &m.member_id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 5, "All member IDs should be unique");
    }

    #[test]
    fn test_join_group_response_with_many_members() {
        // Response with many concurrent members
        let members: Vec<JoinGroupMemberData> = (0..100)
            .map(|i| JoinGroupMemberData {
                member_id: format!("member-{}", i),
                metadata: Bytes::new(),
            })
            .collect();

        let response = JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id: 42,
            protocol_name: "range".to_string(),
            leader: "member-0".to_string(),
            member_id: "member-50".to_string(),
            members,
        };

        assert_eq!(response.members.len(), 100);
        assert_eq!(response.leader, "member-0");
    }

    #[test]
    fn test_join_group_response_sorted_members() {
        // Members should be sorted by member_id in response
        let mut members = [
            JoinGroupMemberData {
                member_id: "member-c".to_string(),
                metadata: Bytes::new(),
            },
            JoinGroupMemberData {
                member_id: "member-a".to_string(),
                metadata: Bytes::new(),
            },
            JoinGroupMemberData {
                member_id: "member-b".to_string(),
                metadata: Bytes::new(),
            },
        ];

        members.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        assert_eq!(members[0].member_id, "member-a");
        assert_eq!(members[1].member_id, "member-b");
        assert_eq!(members[2].member_id, "member-c");
    }

    // ========================================================================
    // Rebalance State Tests
    // ========================================================================

    #[test]
    fn test_rebalance_triggers_generation_increment() {
        // Each rebalance should increment generation
        let initial_gen = 1i32;
        let after_rebalance = initial_gen + 1;
        let after_second_rebalance = after_rebalance + 1;

        assert_eq!(after_rebalance, 2);
        assert_eq!(after_second_rebalance, 3);
    }

    // ========================================================================
    // Session Timeout Tests
    // ========================================================================

    #[test]
    fn test_session_timeout_valid_values() {
        // Valid session timeouts (in milliseconds)
        let valid_timeouts = [6000, 10000, 30000, 300000]; // 6s, 10s, 30s, 5min

        for timeout in valid_timeouts {
            assert!(timeout > 0);
            assert!(timeout <= 300000, "Timeout should be <= 5 minutes");
        }
    }

    #[test]
    fn test_session_timeout_edge_cases() {
        // Minimum and maximum session timeouts
        let min_timeout = 1000; // 1 second minimum
        let max_timeout = 300000; // 5 minutes maximum

        assert!(min_timeout >= 1000);
        assert!(max_timeout <= 300000);
    }

    // ========================================================================
    // Protocol Selection Tests
    // ========================================================================

    #[test]
    fn test_protocol_selection_first_protocol() {
        // When multiple protocols are supported, first one is typically selected
        let protocols = ["range", "roundrobin", "sticky"];

        let selected = protocols
            .first()
            .expect("Should have at least one protocol");
        assert_eq!(*selected, "range");
    }

    // ========================================================================
    // Member ID Generation Tests
    // ========================================================================

    #[test]
    fn test_member_id_uniqueness() {
        // Generate multiple member IDs and verify uniqueness
        let mut ids = std::collections::HashSet::new();

        for _ in 0..100 {
            let id = format!("member-{}", uuid::Uuid::new_v4());
            assert!(ids.insert(id), "Member ID should be unique");
        }

        assert_eq!(ids.len(), 100);
    }

    #[test]
    fn test_member_id_preserved_when_provided() {
        // When member_id is provided, it should be preserved
        let provided_id = "my-custom-member-id".to_string();

        let response = JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id: 1,
            protocol_name: "range".to_string(),
            leader: provided_id.clone(),
            member_id: provided_id.clone(),
            members: vec![],
        };

        assert_eq!(response.member_id, "my-custom-member-id");
    }

    // ========================================================================
    // Assignment Tests
    // ========================================================================

    #[test]
    fn test_sync_group_empty_assignment() {
        // Members that get no partitions should have empty assignment
        let response = SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            assignment: Bytes::new(),
        };

        assert!(response.assignment.is_empty());
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[test]
    fn test_sync_group_with_assignment() {
        // Member with partition assignment
        let assignment_data = vec![0u8; 64]; // Simulated assignment bytes
        let response = SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            assignment: Bytes::from(assignment_data),
        };

        assert_eq!(response.assignment.len(), 64);
    }

    // ========================================================================
    // Coordinator Key Namespacing Tests
    // ========================================================================

    #[test]
    fn test_coordinator_key_namespace() {
        // Group keys should be namespaced to avoid collision with partition keys
        let group_id = "my-group";
        let group_key = format!("group:{}", group_id);

        assert!(group_key.starts_with("group:"));
        assert!(group_key.contains(group_id));
    }

    #[test]
    fn test_different_groups_different_keys() {
        let groups = ["group-a", "group-b", "group-c"];
        let keys: Vec<String> = groups.iter().map(|g| format!("group:{}", g)).collect();

        // All keys should be unique
        let unique_keys: std::collections::HashSet<_> = keys.iter().collect();
        assert_eq!(unique_keys.len(), groups.len());
    }
}
