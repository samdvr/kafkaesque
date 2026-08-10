//! DescribeAcls / CreateAcls / DeleteAcls handlers.
//!
//! Thin wire adapters onto [`RaftCoordinator`](crate::cluster::RaftCoordinator)
//! ACL CRUD. Authorization is cluster-scoped (`Alter` for create/delete,
//! `Describe` for describe) via [`authorize_cluster_api`](super::SlateDBClusterHandler::authorize_cluster_api).

use std::collections::BTreeMap;

use tracing::info;

use crate::cluster::raft::domains::{
    AclBinding, AclFilter, AclOperation, AclPatternType, AclPermissionType, AclResourceType,
};
use crate::error::KafkaCode;
use crate::server::RequestContext;
use crate::server::request::{
    AclCreation, AclDeletionFilter, ApiKey, CreateAclsRequestData, DeleteAclsRequestData,
    DescribeAclsRequestData,
};
use crate::server::response::{
    AclCreationResult, CreateAclsResponseData, DeleteAclsFilterResult, DeleteAclsMatchingAcl,
    DeleteAclsResponseData, DescribeAclEntry, DescribeAclsResource, DescribeAclsResponseData,
};

use super::SlateDBClusterHandler;

pub(super) async fn handle_describe_acls(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: DescribeAclsRequestData,
) -> DescribeAclsResponseData {
    if !handler
        .authorize_cluster_api(ctx, ApiKey::DescribeAcls)
        .await
    {
        info!(
            target: "audit",
            principal = %ctx.principal,
            api = "DescribeAcls",
            "ACL denied: DescribeAcls (cluster Describe)"
        );
        return DescribeAclsResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::ClusterAuthorizationFailed,
            error_message: Some("Cluster authorization failed".into()),
            resources: vec![],
        };
    }

    let filter = match wire_filter_to_domain(
        request.resource_type,
        request.resource_name,
        request.pattern_type,
        request.principal,
        request.host,
        request.operation,
        request.permission_type,
        /*allow_any=*/ true,
    ) {
        Ok(f) => f,
        Err(msg) => {
            return DescribeAclsResponseData {
                throttle_time_ms: 0,
                error_code: KafkaCode::InvalidRequest,
                error_message: Some(msg),
                resources: vec![],
            };
        }
    };

    let bindings = handler.coordinator.describe_acls(&filter).await;
    DescribeAclsResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        error_message: None,
        resources: group_describe_resources(bindings),
    }
}

pub(super) async fn handle_create_acls(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: CreateAclsRequestData,
) -> CreateAclsResponseData {
    if !handler.authorize_cluster_api(ctx, ApiKey::CreateAcls).await {
        info!(
            target: "audit",
            principal = %ctx.principal,
            api = "CreateAcls",
            "ACL denied: CreateAcls (cluster Alter)"
        );
        return CreateAclsResponseData {
            throttle_time_ms: 0,
            results: request
                .creations
                .into_iter()
                .map(|_| AclCreationResult {
                    error_code: KafkaCode::ClusterAuthorizationFailed,
                    error_message: Some("Cluster authorization failed".into()),
                })
                .collect(),
        };
    }

    let mut results = Vec::with_capacity(request.creations.len());
    let mut bindings = Vec::new();
    let mut ok_indexes = Vec::new();

    for (i, creation) in request.creations.into_iter().enumerate() {
        match wire_creation_to_binding(creation) {
            Ok(b) => {
                bindings.push(b);
                ok_indexes.push(i);
                results.push(AclCreationResult {
                    error_code: KafkaCode::None,
                    error_message: None,
                });
            }
            Err(msg) => {
                results.push(AclCreationResult {
                    error_code: KafkaCode::InvalidRequest,
                    error_message: Some(msg),
                });
            }
        }
    }

    if bindings.is_empty() {
        return CreateAclsResponseData {
            throttle_time_ms: 0,
            results,
        };
    }

    match handler.coordinator.create_acls(bindings).await {
        Ok(_) => CreateAclsResponseData {
            throttle_time_ms: 0,
            results,
        },
        Err(e) => {
            for i in ok_indexes {
                results[i] = AclCreationResult {
                    error_code: KafkaCode::Unknown,
                    error_message: Some(e.to_string()),
                };
            }
            CreateAclsResponseData {
                throttle_time_ms: 0,
                results,
            }
        }
    }
}

pub(super) async fn handle_delete_acls(
    handler: &SlateDBClusterHandler,
    ctx: &RequestContext,
    request: DeleteAclsRequestData,
) -> DeleteAclsResponseData {
    if !handler.authorize_cluster_api(ctx, ApiKey::DeleteAcls).await {
        info!(
            target: "audit",
            principal = %ctx.principal,
            api = "DeleteAcls",
            "ACL denied: DeleteAcls (cluster Alter)"
        );
        return DeleteAclsResponseData {
            throttle_time_ms: 0,
            filter_results: request
                .filters
                .into_iter()
                .map(|_| DeleteAclsFilterResult {
                    error_code: KafkaCode::ClusterAuthorizationFailed,
                    error_message: Some("Cluster authorization failed".into()),
                    matching_acls: vec![],
                })
                .collect(),
        };
    }

    let mut domain_filters = Vec::with_capacity(request.filters.len());
    let mut filter_results = Vec::with_capacity(request.filters.len());
    let mut valid_indexes = Vec::new();

    for (i, f) in request.filters.into_iter().enumerate() {
        match wire_deletion_filter_to_domain(f) {
            Ok(df) => {
                domain_filters.push(df);
                valid_indexes.push(i);
                filter_results.push(DeleteAclsFilterResult {
                    error_code: KafkaCode::None,
                    error_message: None,
                    matching_acls: vec![],
                });
            }
            Err(msg) => {
                filter_results.push(DeleteAclsFilterResult {
                    error_code: KafkaCode::InvalidRequest,
                    error_message: Some(msg),
                    matching_acls: vec![],
                });
            }
        }
    }

    if domain_filters.is_empty() {
        return DeleteAclsResponseData {
            throttle_time_ms: 0,
            filter_results,
        };
    }

    match handler
        .coordinator
        .delete_acls(domain_filters.clone())
        .await
    {
        Ok(removed) => {
            for (result_idx, filter) in valid_indexes.into_iter().zip(domain_filters.iter()) {
                let matching: Vec<DeleteAclsMatchingAcl> = removed
                    .iter()
                    .filter(|b| filter.matches(b))
                    .map(binding_to_matching_acl)
                    .collect();
                filter_results[result_idx].matching_acls = matching;
            }
            DeleteAclsResponseData {
                throttle_time_ms: 0,
                filter_results,
            }
        }
        Err(e) => {
            for i in valid_indexes {
                filter_results[i] = DeleteAclsFilterResult {
                    error_code: KafkaCode::Unknown,
                    error_message: Some(e.to_string()),
                    matching_acls: vec![],
                };
            }
            DeleteAclsResponseData {
                throttle_time_ms: 0,
                filter_results,
            }
        }
    }
}

fn group_describe_resources(bindings: Vec<AclBinding>) -> Vec<DescribeAclsResource> {
    let mut map: BTreeMap<(i8, String, i8), Vec<DescribeAclEntry>> = BTreeMap::new();
    for b in bindings {
        let key = (
            resource_type_to_wire(b.resource_type),
            b.resource_name.clone(),
            pattern_type_to_wire(b.pattern_type),
        );
        map.entry(key).or_default().push(DescribeAclEntry {
            principal: b.principal,
            host: b.host,
            operation: operation_to_wire(b.operation),
            permission_type: permission_to_wire(b.permission),
        });
    }
    map.into_iter()
        .map(
            |((resource_type, resource_name, pattern_type), acls)| DescribeAclsResource {
                resource_type,
                resource_name,
                pattern_type,
                acls,
            },
        )
        .collect()
}

fn binding_to_matching_acl(b: &AclBinding) -> DeleteAclsMatchingAcl {
    DeleteAclsMatchingAcl {
        error_code: KafkaCode::None,
        error_message: None,
        resource_type: resource_type_to_wire(b.resource_type),
        resource_name: b.resource_name.clone(),
        pattern_type: pattern_type_to_wire(b.pattern_type),
        principal: b.principal.clone(),
        host: b.host.clone(),
        operation: operation_to_wire(b.operation),
        permission_type: permission_to_wire(b.permission),
    }
}

fn wire_creation_to_binding(c: AclCreation) -> Result<AclBinding, String> {
    Ok(AclBinding {
        resource_type: resource_type_from_wire(c.resource_type, /*allow_any=*/ false)?,
        resource_name: c.resource_name,
        pattern_type: pattern_type_from_wire(c.pattern_type, /*allow_any=*/ false)?,
        principal: c.principal,
        host: c.host,
        operation: operation_from_wire(c.operation, /*allow_any=*/ false)?,
        permission: permission_from_wire(c.permission_type, /*allow_any=*/ false)?,
    })
}

fn wire_deletion_filter_to_domain(f: AclDeletionFilter) -> Result<AclFilter, String> {
    wire_filter_to_domain(
        f.resource_type,
        f.resource_name,
        f.pattern_type,
        f.principal,
        f.host,
        f.operation,
        f.permission_type,
        /*allow_any=*/ true,
    )
}

#[allow(clippy::too_many_arguments)]
fn wire_filter_to_domain(
    resource_type: i8,
    resource_name: Option<String>,
    pattern_type: i8,
    principal: Option<String>,
    host: Option<String>,
    operation: i8,
    permission_type: i8,
    allow_any: bool,
) -> Result<AclFilter, String> {
    Ok(AclFilter {
        resource_type: optional_resource_type(resource_type, allow_any)?,
        resource_name,
        pattern_type: optional_pattern_type(pattern_type, allow_any)?,
        principal,
        host,
        operation: optional_operation(operation, allow_any)?,
        permission: optional_permission(permission_type, allow_any)?,
    })
}

fn optional_resource_type(v: i8, allow_any: bool) -> Result<Option<AclResourceType>, String> {
    if allow_any && v == 1 {
        return Ok(None); // Any
    }
    Ok(Some(resource_type_from_wire(v, allow_any)?))
}

fn optional_pattern_type(v: i8, allow_any: bool) -> Result<Option<AclPatternType>, String> {
    if allow_any && v == 1 {
        return Ok(None); // Any
    }
    Ok(Some(pattern_type_from_wire(v, allow_any)?))
}

fn optional_operation(v: i8, allow_any: bool) -> Result<Option<AclOperation>, String> {
    if allow_any && v == 1 {
        return Ok(None); // Any
    }
    Ok(Some(operation_from_wire(v, allow_any)?))
}

fn optional_permission(v: i8, allow_any: bool) -> Result<Option<AclPermissionType>, String> {
    if allow_any && v == 1 {
        return Ok(None); // Any
    }
    Ok(Some(permission_from_wire(v, allow_any)?))
}

fn resource_type_from_wire(v: i8, _allow_any: bool) -> Result<AclResourceType, String> {
    match v {
        2 => Ok(AclResourceType::Topic),
        3 => Ok(AclResourceType::Group),
        4 => Ok(AclResourceType::Cluster),
        other => Err(format!(
            "unsupported ACL resource type {other} (supported: Topic=2, Group=3, Cluster=4)"
        )),
    }
}

fn pattern_type_from_wire(v: i8, _allow_any: bool) -> Result<AclPatternType, String> {
    match v {
        3 => Ok(AclPatternType::Literal),
        4 => Ok(AclPatternType::Prefixed),
        other => Err(format!(
            "unsupported ACL pattern type {other} (supported: Literal=3, Prefixed=4)"
        )),
    }
}

fn operation_from_wire(v: i8, _allow_any: bool) -> Result<AclOperation, String> {
    match v {
        2 => Ok(AclOperation::All),
        3 => Ok(AclOperation::Read),
        4 => Ok(AclOperation::Write),
        5 => Ok(AclOperation::Create),
        6 => Ok(AclOperation::Delete),
        7 => Ok(AclOperation::Alter),
        8 => Ok(AclOperation::Describe),
        9 => Ok(AclOperation::ClusterAction),
        12 => Ok(AclOperation::IdempotentWrite),
        // DescribeConfigs / AlterConfigs map to Describe / Alter.
        10 => Ok(AclOperation::Describe),
        11 => Ok(AclOperation::Alter),
        other => Err(format!("unsupported ACL operation {other}")),
    }
}

fn permission_from_wire(v: i8, _allow_any: bool) -> Result<AclPermissionType, String> {
    match v {
        2 => Ok(AclPermissionType::Deny),
        3 => Ok(AclPermissionType::Allow),
        other => Err(format!(
            "unsupported ACL permission type {other} (supported: Deny=2, Allow=3)"
        )),
    }
}

fn resource_type_to_wire(t: AclResourceType) -> i8 {
    match t {
        AclResourceType::Topic => 2,
        AclResourceType::Group => 3,
        AclResourceType::Cluster => 4,
    }
}

fn pattern_type_to_wire(t: AclPatternType) -> i8 {
    match t {
        AclPatternType::Literal => 3,
        AclPatternType::Prefixed => 4,
    }
}

fn operation_to_wire(op: AclOperation) -> i8 {
    match op {
        AclOperation::All => 2,
        AclOperation::Read => 3,
        AclOperation::Write => 4,
        AclOperation::Create => 5,
        AclOperation::Delete => 6,
        AclOperation::Alter => 7,
        AclOperation::Describe => 8,
        AclOperation::ClusterAction => 9,
        AclOperation::IdempotentWrite => 12,
    }
}

fn permission_to_wire(p: AclPermissionType) -> i8 {
    match p {
        AclPermissionType::Deny => 2,
        AclPermissionType::Allow => 3,
    }
}
