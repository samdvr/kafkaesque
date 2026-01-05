//! Integration tests for group response types.
//!
//! These tests verify consumer group response construction.

use bytes::Bytes;

use kafkaesque::error::KafkaCode;
use kafkaesque::server::response::{
    FindCoordinatorResponseData, HeartbeatResponseData, JoinGroupMemberData, JoinGroupResponseData,
    LeaveGroupResponseData, SyncGroupResponseData,
};

// ============================================================================
// FindCoordinatorResponseData Tests
// ============================================================================

#[test]
fn test_find_coordinator_success() {
    let response = FindCoordinatorResponseData::success(1, "localhost".to_string(), 9092);

    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.node_id, 1);
    assert_eq!(response.host, "localhost");
    assert_eq!(response.port, 9092);
    assert!(response.error_message.is_none());
    assert_eq!(response.throttle_time_ms, 0);
}

#[test]
fn test_find_coordinator_error() {
    let response = FindCoordinatorResponseData::error(
        KafkaCode::GroupCoordinatorNotAvailable,
        Some("Coordinator unavailable".to_string()),
    );

    assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
    assert_eq!(response.node_id, -1);
    assert!(response.host.is_empty());
    assert_eq!(response.port, 0);
    assert_eq!(
        response.error_message,
        Some("Coordinator unavailable".to_string())
    );
}

#[test]
fn test_find_coordinator_not_available() {
    let response = FindCoordinatorResponseData::not_available("No coordinator");

    assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
    assert_eq!(response.error_message, Some("No coordinator".to_string()));
}

#[test]
fn test_find_coordinator_default() {
    let response = FindCoordinatorResponseData::default();

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.error_message.is_none());
}

#[test]
fn test_find_coordinator_debug() {
    let response = FindCoordinatorResponseData::success(1, "host".to_string(), 9092);
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("FindCoordinatorResponseData"));
}

#[test]
fn test_find_coordinator_clone() {
    let response = FindCoordinatorResponseData::success(1, "host".to_string(), 9092);
    let cloned = response.clone();
    assert_eq!(response.node_id, cloned.node_id);
}

// ============================================================================
// JoinGroupResponseData Tests
// ============================================================================

#[test]
fn test_join_group_error() {
    let response =
        JoinGroupResponseData::error(KafkaCode::UnknownMemberId, "member-123".to_string());

    assert_eq!(response.error_code, KafkaCode::UnknownMemberId);
    assert_eq!(response.member_id, "member-123");
    assert_eq!(response.generation_id, -1);
    assert!(response.protocol_name.is_empty());
    assert!(response.leader.is_empty());
    assert!(response.members.is_empty());
}

#[test]
fn test_join_group_builder() {
    let response = JoinGroupResponseData::builder()
        .generation_id(5)
        .protocol_name("range")
        .leader("leader-id")
        .member_id("member-id")
        .add_member("member-1", Bytes::from(vec![1, 2, 3]))
        .add_member("member-2", Bytes::from(vec![4, 5, 6]))
        .build();

    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.generation_id, 5);
    assert_eq!(response.protocol_name, "range");
    assert_eq!(response.leader, "leader-id");
    assert_eq!(response.member_id, "member-id");
    assert_eq!(response.members.len(), 2);
}

#[test]
fn test_join_group_default() {
    let response = JoinGroupResponseData::default();

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.members.is_empty());
}

#[test]
fn test_join_group_debug() {
    let response = JoinGroupResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("JoinGroupResponseData"));
}

#[test]
fn test_join_group_clone() {
    let response = JoinGroupResponseData::builder().generation_id(1).build();
    let cloned = response.clone();
    assert_eq!(response.generation_id, cloned.generation_id);
}

// ============================================================================
// JoinGroupMemberData Tests
// ============================================================================

#[test]
fn test_join_group_member_data() {
    let member = JoinGroupMemberData {
        member_id: "member-1".to_string(),
        metadata: Bytes::from(vec![1, 2, 3, 4]),
    };

    assert_eq!(member.member_id, "member-1");
    assert_eq!(member.metadata.len(), 4);
}

#[test]
fn test_join_group_member_data_debug() {
    let member = JoinGroupMemberData {
        member_id: "m".to_string(),
        metadata: Bytes::new(),
    };
    let debug_str = format!("{:?}", member);
    assert!(debug_str.contains("JoinGroupMemberData"));
}

#[test]
fn test_join_group_member_data_clone() {
    let member = JoinGroupMemberData {
        member_id: "m".to_string(),
        metadata: Bytes::from(vec![1]),
    };
    let cloned = member.clone();
    assert_eq!(member.member_id, cloned.member_id);
}

// ============================================================================
// SyncGroupResponseData Tests
// ============================================================================

#[test]
fn test_sync_group_with_assignment() {
    let assignment = Bytes::from(vec![1, 2, 3, 4, 5]);
    let response = SyncGroupResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        assignment: assignment.clone(),
    };

    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.assignment, assignment);
    assert_eq!(response.throttle_time_ms, 0);
}

#[test]
fn test_sync_group_with_error() {
    let response = SyncGroupResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::IllegalGeneration,
        assignment: Bytes::new(),
    };

    assert_eq!(response.error_code, KafkaCode::IllegalGeneration);
    assert!(response.assignment.is_empty());
}

#[test]
fn test_sync_group_default() {
    let response = SyncGroupResponseData::default();

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, KafkaCode::None);
    assert!(response.assignment.is_empty());
}

#[test]
fn test_sync_group_debug() {
    let response = SyncGroupResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("SyncGroupResponseData"));
}

#[test]
fn test_sync_group_clone() {
    let response = SyncGroupResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        assignment: Bytes::from(vec![1]),
    };
    let cloned = response.clone();
    assert_eq!(response.assignment, cloned.assignment);
}

// ============================================================================
// HeartbeatResponseData Tests
// ============================================================================

#[test]
fn test_heartbeat_with_no_error() {
    let response = HeartbeatResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
    };

    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.throttle_time_ms, 0);
}

#[test]
fn test_heartbeat_with_error() {
    let response = HeartbeatResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::RebalanceInProgress,
    };

    assert_eq!(response.error_code, KafkaCode::RebalanceInProgress);
}

#[test]
fn test_heartbeat_default() {
    let response = HeartbeatResponseData::default();

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, KafkaCode::None);
}

#[test]
fn test_heartbeat_debug() {
    let response = HeartbeatResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("HeartbeatResponseData"));
}

#[test]
fn test_heartbeat_clone() {
    let response = HeartbeatResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
    };
    let cloned = response.clone();
    assert_eq!(response.error_code, cloned.error_code);
}

// ============================================================================
// LeaveGroupResponseData Tests
// ============================================================================

#[test]
fn test_leave_group_with_no_error() {
    let response = LeaveGroupResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
    };

    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.throttle_time_ms, 0);
}

#[test]
fn test_leave_group_with_error() {
    let response = LeaveGroupResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::UnknownMemberId,
    };

    assert_eq!(response.error_code, KafkaCode::UnknownMemberId);
}

#[test]
fn test_leave_group_default() {
    let response = LeaveGroupResponseData::default();

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, KafkaCode::None);
}

#[test]
fn test_leave_group_debug() {
    let response = LeaveGroupResponseData::default();
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("LeaveGroupResponseData"));
}

#[test]
fn test_leave_group_clone() {
    let response = LeaveGroupResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
    };
    let cloned = response.clone();
    assert_eq!(response.error_code, cloned.error_code);
}
