//! ACL admin RPC round-trip (DescribeAcls / CreateAcls / DeleteAcls).

use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    AclCreation, AclDeletionFilter, CreateAclsRequestData, DeleteAclsRequestData,
    DescribeAclsRequestData,
};

mod common;
use common::BrokerHandle;

#[tokio::test]
async fn create_describe_delete_acls_round_trip() {
    let broker = BrokerHandle::spawn_with(ClusterProfile::Development, |cfg| {
        cfg.acl_enabled = true;
        cfg.acl_deny_by_default = true;
        cfg.super_users = vec!["User:ANONYMOUS".into()];
    })
    .await;

    let create = broker
        .handler
        .handle_create_acls(
            &broker.ctx(),
            CreateAclsRequestData {
                creations: vec![AclCreation {
                    resource_type: 2, // Topic
                    resource_name: "orders".into(),
                    pattern_type: 3, // Literal
                    principal: "User:alice".into(),
                    host: "*".into(),
                    operation: 3,       // Read
                    permission_type: 3, // Allow
                }],
            },
        )
        .await;
    assert_eq!(create.results.len(), 1);
    assert_eq!(create.results[0].error_code, KafkaCode::None);

    let describe = broker
        .handler
        .handle_describe_acls(
            &broker.ctx(),
            DescribeAclsRequestData {
                resource_type: 2,
                resource_name: Some("orders".into()),
                pattern_type: 3,
                principal: None,
                host: None,
                operation: 1,       // Any
                permission_type: 1, // Any
            },
        )
        .await;
    assert_eq!(describe.error_code, KafkaCode::None);
    assert_eq!(describe.resources.len(), 1);
    assert_eq!(describe.resources[0].resource_name, "orders");
    assert_eq!(describe.resources[0].acls.len(), 1);
    assert_eq!(describe.resources[0].acls[0].principal, "User:alice");

    let delete = broker
        .handler
        .handle_delete_acls(
            &broker.ctx(),
            DeleteAclsRequestData {
                filters: vec![AclDeletionFilter {
                    resource_type: 2,
                    resource_name: Some("orders".into()),
                    pattern_type: 3,
                    principal: Some("User:alice".into()),
                    host: None,
                    operation: 1,
                    permission_type: 1,
                }],
            },
        )
        .await;
    assert_eq!(delete.filter_results.len(), 1);
    assert_eq!(delete.filter_results[0].error_code, KafkaCode::None);
    assert_eq!(delete.filter_results[0].matching_acls.len(), 1);

    let describe_after = broker
        .handler
        .handle_describe_acls(
            &broker.ctx(),
            DescribeAclsRequestData {
                resource_type: 2,
                resource_name: Some("orders".into()),
                pattern_type: 3,
                principal: None,
                host: None,
                operation: 1,
                permission_type: 1,
            },
        )
        .await;
    assert!(describe_after.resources.is_empty() || describe_after.resources[0].acls.is_empty());
}
