//! ACL coordinator surface — wraps `AclCommand` writes and `AclDomainState`
//! reads.
//!
//! This module gives the request handler a typed API: callers don't have to
//! pattern-match on `CoordinationCommand`/`CoordinationResponse`, and the
//! local read path goes through the in-memory state machine without
//! triggering a Raft round-trip.

use super::super::commands::{CoordinationCommand, CoordinationResponse};
use super::super::domains::{
    AclBinding, AclCommand, AclDecision, AclFilter, AclOperation, AclResourceType, AclResponse,
};
use super::RaftCoordinator;

use crate::cluster::error::{SlateDBError, SlateDBResult};

impl RaftCoordinator {
    /// Apply a `CreateAcls` command via Raft. Returns the count of newly
    /// inserted bindings (duplicates aren't counted).
    pub async fn create_acls(&self, bindings: Vec<AclBinding>) -> SlateDBResult<usize> {
        let resp = self
            .node()
            .write(CoordinationCommand::AclDomain(AclCommand::CreateAcls {
                bindings,
            }))
            .await?;
        match resp {
            CoordinationResponse::AclDomainResponse(AclResponse::Created { created }) => Ok(created),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response to CreateAcls: {:?}",
                other
            ))),
        }
    }

    /// Apply a `DeleteAcls` command via Raft. Returns the bindings that
    /// matched and were removed.
    pub async fn delete_acls(&self, filters: Vec<AclFilter>) -> SlateDBResult<Vec<AclBinding>> {
        let resp = self
            .node()
            .write(CoordinationCommand::AclDomain(AclCommand::DeleteAcls {
                filters,
            }))
            .await?;
        match resp {
            CoordinationResponse::AclDomainResponse(AclResponse::Deleted { removed }) => {
                Ok(removed)
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response to DeleteAcls: {:?}",
                other
            ))),
        }
    }

    /// Read-only describe against the local state machine.
    pub async fn describe_acls(&self, filter: &AclFilter) -> Vec<AclBinding> {
        let sm = self.node().state_machine();
        let sm = sm.read().await;
        sm.state().await.acl_domain.describe(filter)
    }

    /// Authorize `(principal, host)` to perform `op` on
    /// `(resource_type, name)` against the local state machine.
    pub async fn is_authorized(
        &self,
        principal: &str,
        host: &str,
        op: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
    ) -> AclDecision {
        let sm = self.node().state_machine();
        let sm = sm.read().await;
        sm.state()
            .await
            .acl_domain
            .is_authorized(principal, host, op, resource_type, resource_name)
    }
}
