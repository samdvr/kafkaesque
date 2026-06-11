//! Request authorization surface.
//!
//! Sits between `dispatch_request_common` and the handler implementations.
//! For every Kafka API key we map the request to one or more
//! `(operation, resource)` checks, then consult the ACL state replicated
//! through Raft. Deny-by-default: when ACLs are enabled, an unmatched
//! request is denied unless the principal is a configured super-user.
//!
//! The `Authorizer` trait keeps the handler trait independent of the
//! Raft-backed implementation — tests stub it out with `AllowAll` or
//! `DenyAll` rather than spinning up a real cluster.

use std::sync::Arc;

use async_trait::async_trait;

use crate::cluster::raft::{AclDecision, AclOperation, AclResourceType, RaftCoordinator};

/// One authorization check.
#[derive(Debug, Clone)]
pub struct AuthorizeRequest<'a> {
    pub principal: &'a str,
    pub host: &'a str,
    pub operation: AclOperation,
    pub resource_type: AclResourceType,
    pub resource_name: &'a str,
}

/// Outcome of a single check — already-applied super-user / ACL-disabled
/// short-circuits collapse to `Allowed`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorizeResult {
    Allowed,
    Denied,
}

/// Pluggable authorization layer. Implementations decide how to combine
/// super-users, ACL state, and any deny-by-default policy.
#[async_trait]
pub trait Authorizer: Send + Sync {
    async fn authorize(&self, req: AuthorizeRequest<'_>) -> AuthorizeResult;
}

/// "ACLs are off" — permits everything. Used as the default when neither
/// SASL nor ACLs are configured, so existing deployments continue to work.
pub struct AllowAllAuthorizer;

#[async_trait]
impl Authorizer for AllowAllAuthorizer {
    async fn authorize(&self, _req: AuthorizeRequest<'_>) -> AuthorizeResult {
        AuthorizeResult::Allowed
    }
}

/// Authorizer backed by ACL bindings stored in Raft.
pub struct RaftAclAuthorizer {
    coordinator: Arc<RaftCoordinator>,
    /// Principals (e.g. `User:admin`) that bypass ACL checks. Useful for
    /// bootstrap and break-glass scenarios. Anonymous (`User:ANONYMOUS`) is
    /// **never** auto-promoted; if you want it as super-user, list it.
    super_users: Vec<String>,
    /// What to do when no binding matches at all. `true` denies (production
    /// posture). `false` allows (legacy / dev posture, equivalent to "ACLs
    /// auditable but not enforcing").
    deny_by_default: bool,
}

impl RaftAclAuthorizer {
    pub fn new(
        coordinator: Arc<RaftCoordinator>,
        super_users: Vec<String>,
        deny_by_default: bool,
    ) -> Self {
        Self {
            coordinator,
            super_users,
            deny_by_default,
        }
    }
}

#[async_trait]
impl Authorizer for RaftAclAuthorizer {
    async fn authorize(&self, req: AuthorizeRequest<'_>) -> AuthorizeResult {
        if self.super_users.iter().any(|u| u == req.principal) {
            return AuthorizeResult::Allowed;
        }
        match self
            .coordinator
            .is_authorized(
                req.principal,
                req.host,
                req.operation,
                req.resource_type,
                req.resource_name,
            )
            .await
        {
            AclDecision::Allowed => AuthorizeResult::Allowed,
            AclDecision::Denied => AuthorizeResult::Denied,
            AclDecision::NotFound => {
                if self.deny_by_default {
                    AuthorizeResult::Denied
                } else {
                    AuthorizeResult::Allowed
                }
            }
        }
    }
}

/// Resource name used for cluster-level ACL bindings. Matches real Kafka,
/// where the cluster resource always has the literal name `kafka-cluster`.
pub const CLUSTER_RESOURCE_NAME: &str = "kafka-cluster";

/// Static map from Kafka API keys to the operation they require on the
/// cluster resource. Topic-level / group-level checks are handled inline by
/// each handler since they need the resource name; this table only covers
/// requests whose only authz target is the cluster itself.
///
/// This table is consulted by `SlateDBClusterHandler::authorize_cluster_api`
/// before the corresponding handler runs. Returning `None` means "no
/// cluster-level check is required" — the per-resource check still runs.
pub fn cluster_operation_for_api(api_key: crate::server::request::ApiKey) -> Option<AclOperation> {
    use crate::server::request::ApiKey;
    match api_key {
        // Producer-id allocation is a cluster action (it consumes a global
        // counter and is gated by IdempotentWrite in real Kafka).
        ApiKey::InitProducerId => Some(AclOperation::IdempotentWrite),
        // ApiVersions / SASL handshake / authenticate run before authn so
        // they bypass authz entirely.
        ApiKey::ApiVersions | ApiKey::SaslHandshake | ApiKey::SaslAuthenticate => None,
        // Per-topic / per-group APIs — authz is checked inline by the
        // handler with the topic/group name in hand. This includes:
        // - Metadata: per-topic Describe, unauthorized topics get
        //   TopicAuthorizationFailed (named) or are filtered (list-all),
        //   matching real Kafka rather than gating the whole API on a
        //   cluster-level Describe that ordinary clients never hold.
        // - FindCoordinator: Describe on the group named in the request.
        // - CreateTopics / DeleteTopics: Create / Delete on the topic.
        ApiKey::Produce
        | ApiKey::Fetch
        | ApiKey::Metadata
        | ApiKey::FindCoordinator
        | ApiKey::CreateTopics
        | ApiKey::DeleteTopics
        | ApiKey::ListOffsets
        | ApiKey::OffsetCommit
        | ApiKey::OffsetFetch
        | ApiKey::JoinGroup
        | ApiKey::Heartbeat
        | ApiKey::LeaveGroup
        | ApiKey::SyncGroup
        | ApiKey::DescribeGroups
        | ApiKey::ListGroups
        | ApiKey::DeleteGroups
        | ApiKey::Unknown(_) => None,
        // Inter-broker replication APIs (LeaderAndIsr/StopReplica/etc.) are
        // ClusterAction in real Kafka. Kafkaesque doesn't implement them, so
        // we still gate them as ClusterAction — anything that does reach
        // here from a non-cluster principal should be denied by default.
        ApiKey::LeaderAndIsr
        | ApiKey::StopReplica
        | ApiKey::UpdateMetadata
        | ApiKey::ControlledShutdown => Some(AclOperation::ClusterAction),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct AlwaysDeny;

    #[async_trait]
    impl Authorizer for AlwaysDeny {
        async fn authorize(&self, _req: AuthorizeRequest<'_>) -> AuthorizeResult {
            AuthorizeResult::Denied
        }
    }

    #[tokio::test]
    async fn allow_all_authorizer_allows() {
        let a = AllowAllAuthorizer;
        assert_eq!(
            a.authorize(AuthorizeRequest {
                principal: "User:alice",
                host: "*",
                operation: AclOperation::Read,
                resource_type: AclResourceType::Topic,
                resource_name: "anything",
            })
            .await,
            AuthorizeResult::Allowed
        );
    }

    #[tokio::test]
    async fn deny_all_authorizer_denies() {
        let a = AlwaysDeny;
        assert_eq!(
            a.authorize(AuthorizeRequest {
                principal: "User:alice",
                host: "*",
                operation: AclOperation::Read,
                resource_type: AclResourceType::Topic,
                resource_name: "anything",
            })
            .await,
            AuthorizeResult::Denied
        );
    }
}
