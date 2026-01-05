//! Integration tests for extended group response types.

use bytes::Bytes;

use kafkaesque::error::KafkaCode;
use kafkaesque::server::response::{
    DeleteGroupResult, DeleteGroupsResponseData, DescribeGroupsResponseData, DescribedGroup,
    DescribedGroupMember, ListGroupsResponseData, ListedGroup,
};

// ============================================================================
// DescribeGroupsResponseData Tests
// ============================================================================

#[test]
fn test_describe_groups_response_error() {
    let response = DescribeGroupsResponseData::error(
        "missing-group".to_string(),
        KafkaCode::GroupAuthorizationFailed,
    );
    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.groups.len(), 1);
    assert_eq!(
        response.groups[0].error_code,
        KafkaCode::GroupAuthorizationFailed
    );
}

#[test]
fn test_describe_groups_response_with_groups() {
    let response = DescribeGroupsResponseData {
        throttle_time_ms: 0,
        groups: vec![
            DescribedGroup {
                error_code: KafkaCode::None,
                group_id: "group-1".to_string(),
                group_state: "Stable".to_string(),
                protocol_type: "consumer".to_string(),
                protocol_data: "range".to_string(),
                members: vec![],
            },
            DescribedGroup {
                error_code: KafkaCode::None,
                group_id: "group-2".to_string(),
                group_state: "Empty".to_string(),
                protocol_type: "consumer".to_string(),
                protocol_data: "".to_string(),
                members: vec![],
            },
        ],
    };

    assert_eq!(response.groups.len(), 2);
    assert_eq!(response.groups[0].group_id, "group-1");
    assert_eq!(response.groups[1].group_state, "Empty");
}

#[test]
fn test_describe_groups_response_debug() {
    let response = DescribeGroupsResponseData {
        throttle_time_ms: 0,
        groups: vec![],
    };
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("DescribeGroupsResponseData"));
}

#[test]
fn test_describe_groups_response_clone() {
    let response = DescribeGroupsResponseData {
        throttle_time_ms: 100,
        groups: vec![],
    };
    let cloned = response.clone();
    assert_eq!(response.throttle_time_ms, cloned.throttle_time_ms);
}

// ============================================================================
// DescribedGroup Tests
// ============================================================================

#[test]
fn test_described_group_error() {
    let group = DescribedGroup::error(
        "unauthorized-group".to_string(),
        KafkaCode::GroupAuthorizationFailed,
    );
    assert_eq!(group.error_code, KafkaCode::GroupAuthorizationFailed);
    assert_eq!(group.group_id, "unauthorized-group");
    assert!(group.group_state.is_empty());
}

#[test]
fn test_described_group_builder() {
    let group = DescribedGroup::builder("my-group")
        .group_state("Stable")
        .protocol_type("consumer")
        .protocol_data("roundrobin")
        .add_member(DescribedGroupMember::new(
            "member-1",
            "client-1",
            "/127.0.0.1",
            Bytes::from(vec![1, 2, 3]),
            Bytes::from(vec![4, 5, 6]),
        ))
        .build();

    assert_eq!(group.error_code, KafkaCode::None);
    assert_eq!(group.group_id, "my-group");
    assert_eq!(group.group_state, "Stable");
    assert_eq!(group.protocol_type, "consumer");
    assert_eq!(group.protocol_data, "roundrobin");
    assert_eq!(group.members.len(), 1);
}

#[test]
fn test_described_group_builder_with_members() {
    let members = vec![
        DescribedGroupMember::new("m1", "c1", "h1", Bytes::new(), Bytes::new()),
        DescribedGroupMember::new("m2", "c2", "h2", Bytes::new(), Bytes::new()),
    ];

    let group = DescribedGroup::builder("test-group")
        .members(members)
        .build();

    assert_eq!(group.members.len(), 2);
}

#[test]
fn test_described_group_debug() {
    let group = DescribedGroup::error("g".to_string(), KafkaCode::None);
    let debug_str = format!("{:?}", group);
    assert!(debug_str.contains("DescribedGroup"));
}

#[test]
fn test_described_group_clone() {
    let group = DescribedGroup {
        error_code: KafkaCode::None,
        group_id: "g".to_string(),
        group_state: "Stable".to_string(),
        protocol_type: "consumer".to_string(),
        protocol_data: "range".to_string(),
        members: vec![],
    };
    let cloned = group.clone();
    assert_eq!(group.group_id, cloned.group_id);
}

// ============================================================================
// DescribedGroupMember Tests
// ============================================================================

#[test]
fn test_described_group_member_new() {
    let member = DescribedGroupMember::new(
        "consumer-1-uuid",
        "my-consumer",
        "/192.168.1.100",
        Bytes::from(vec![0, 1, 0, 0, 0, 1]),
        Bytes::from(vec![0, 1, 0, 0, 0, 1, 0, 3]),
    );

    assert_eq!(member.member_id, "consumer-1-uuid");
    assert_eq!(member.client_id, "my-consumer");
    assert_eq!(member.client_host, "/192.168.1.100");
    assert_eq!(member.member_metadata.len(), 6);
    assert_eq!(member.member_assignment.len(), 8);
}

#[test]
fn test_described_group_member_debug() {
    let member = DescribedGroupMember::new("m", "c", "h", Bytes::new(), Bytes::new());
    let debug_str = format!("{:?}", member);
    assert!(debug_str.contains("DescribedGroupMember"));
}

#[test]
fn test_described_group_member_clone() {
    let member = DescribedGroupMember {
        member_id: "m".to_string(),
        client_id: "c".to_string(),
        client_host: "h".to_string(),
        member_metadata: Bytes::from(vec![1]),
        member_assignment: Bytes::from(vec![2]),
    };
    let cloned = member.clone();
    assert_eq!(member.member_id, cloned.member_id);
    assert_eq!(member.member_metadata, cloned.member_metadata);
}

// ============================================================================
// ListGroupsResponseData Tests
// ============================================================================

#[test]
fn test_list_groups_response_error() {
    let response = ListGroupsResponseData::error(KafkaCode::GroupAuthorizationFailed);

    assert_eq!(response.throttle_time_ms, 0);
    assert_eq!(response.error_code, KafkaCode::GroupAuthorizationFailed);
    assert!(response.groups.is_empty());
}

#[test]
fn test_list_groups_response_success() {
    let groups = vec![
        ListedGroup {
            group_id: "group-1".to_string(),
            protocol_type: "consumer".to_string(),
            group_state: "Stable".to_string(),
        },
        ListedGroup {
            group_id: "group-2".to_string(),
            protocol_type: "consumer".to_string(),
            group_state: "Empty".to_string(),
        },
    ];

    let response = ListGroupsResponseData::success(groups);

    assert_eq!(response.error_code, KafkaCode::None);
    assert_eq!(response.groups.len(), 2);
    assert_eq!(response.groups[0].group_id, "group-1");
}

#[test]
fn test_list_groups_response_debug() {
    let response = ListGroupsResponseData::error(KafkaCode::None);
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("ListGroupsResponseData"));
}

#[test]
fn test_list_groups_response_clone() {
    let response = ListGroupsResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        groups: vec![],
    };
    let cloned = response.clone();
    assert_eq!(response.error_code, cloned.error_code);
}

// ============================================================================
// ListedGroup Tests
// ============================================================================

#[test]
fn test_listed_group_with_data() {
    let group = ListedGroup {
        group_id: "test-group".to_string(),
        protocol_type: "consumer".to_string(),
        group_state: "Stable".to_string(),
    };

    assert_eq!(group.group_id, "test-group");
    assert_eq!(group.protocol_type, "consumer");
    assert_eq!(group.group_state, "Stable");
}

#[test]
fn test_listed_group_debug() {
    let group = ListedGroup {
        group_id: "".to_string(),
        protocol_type: "".to_string(),
        group_state: "".to_string(),
    };
    let debug_str = format!("{:?}", group);
    assert!(debug_str.contains("ListedGroup"));
}

#[test]
fn test_listed_group_clone() {
    let group = ListedGroup {
        group_id: "g".to_string(),
        protocol_type: "consumer".to_string(),
        group_state: "Empty".to_string(),
    };
    let cloned = group.clone();
    assert_eq!(group.group_id, cloned.group_id);
}

// ============================================================================
// DeleteGroupsResponseData Tests
// ============================================================================

#[test]
fn test_delete_groups_response_empty() {
    let response = DeleteGroupsResponseData {
        throttle_time_ms: 0,
        results: vec![],
    };

    assert_eq!(response.throttle_time_ms, 0);
    assert!(response.results.is_empty());
}

#[test]
fn test_delete_groups_response_with_results() {
    let response = DeleteGroupsResponseData {
        throttle_time_ms: 0,
        results: vec![
            DeleteGroupResult {
                group_id: "deleted-group-1".to_string(),
                error_code: KafkaCode::None,
            },
            DeleteGroupResult {
                group_id: "failed-group".to_string(),
                error_code: KafkaCode::GroupLoadInProgress,
            },
        ],
    };

    assert_eq!(response.results.len(), 2);
    assert_eq!(response.results[0].group_id, "deleted-group-1");
    assert_eq!(response.results[0].error_code, KafkaCode::None);
    assert_eq!(
        response.results[1].error_code,
        KafkaCode::GroupLoadInProgress
    );
}

#[test]
fn test_delete_groups_response_debug() {
    let response = DeleteGroupsResponseData {
        throttle_time_ms: 0,
        results: vec![],
    };
    let debug_str = format!("{:?}", response);
    assert!(debug_str.contains("DeleteGroupsResponseData"));
}

#[test]
fn test_delete_groups_response_clone() {
    let response = DeleteGroupsResponseData {
        throttle_time_ms: 100,
        results: vec![],
    };
    let cloned = response.clone();
    assert_eq!(response.throttle_time_ms, cloned.throttle_time_ms);
}

// ============================================================================
// DeleteGroupResult Tests
// ============================================================================

#[test]
fn test_delete_group_result_success() {
    let result = DeleteGroupResult {
        group_id: "deleted-group".to_string(),
        error_code: KafkaCode::None,
    };

    assert_eq!(result.group_id, "deleted-group");
    assert_eq!(result.error_code, KafkaCode::None);
}

#[test]
fn test_delete_group_result_not_empty() {
    let result = DeleteGroupResult {
        group_id: "active-group".to_string(),
        error_code: KafkaCode::GroupLoadInProgress,
    };

    assert_eq!(result.error_code, KafkaCode::GroupLoadInProgress);
}

#[test]
fn test_delete_group_result_not_found() {
    let result = DeleteGroupResult {
        group_id: "missing-group".to_string(),
        error_code: KafkaCode::GroupCoordinatorNotAvailable,
    };

    assert_eq!(result.error_code, KafkaCode::GroupCoordinatorNotAvailable);
}

#[test]
fn test_delete_group_result_debug() {
    let result = DeleteGroupResult {
        group_id: "".to_string(),
        error_code: KafkaCode::None,
    };
    let debug_str = format!("{:?}", result);
    assert!(debug_str.contains("DeleteGroupResult"));
}

#[test]
fn test_delete_group_result_clone() {
    let result = DeleteGroupResult {
        group_id: "g".to_string(),
        error_code: KafkaCode::None,
    };
    let cloned = result.clone();
    assert_eq!(result.group_id, cloned.group_id);
}
