//! ACL coordinator surface — wraps `AclCommand` writes and `AclDomainState`
//! reads.
//!
//! ACLs are cluster-wide state: every broker authorizes against the same
//! binding set, so reads + writes route to the **control** group regardless
//! of any per-key sharding.

use super::super::commands::{ControlCommand, ControlResponse};
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
            .cluster()
            .write_control(ControlCommand::Acl(AclCommand::CreateAcls { bindings }))
            .await?;
        match resp {
            ControlResponse::Acl(AclResponse::Created { created }) => Ok(created),
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
            .cluster()
            .write_control(ControlCommand::Acl(AclCommand::DeleteAcls { filters }))
            .await?;
        match resp {
            ControlResponse::Acl(AclResponse::Deleted { removed }) => Ok(removed),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected response to DeleteAcls: {:?}",
                other
            ))),
        }
    }

    /// Read-only describe against the local control state machine.
    pub async fn describe_acls(&self, filter: &AclFilter) -> Vec<AclBinding> {
        let sm = self.cluster().control().state_machine();
        sm.state().await.acl_domain.describe(filter)
    }

    /// Authorize `(principal, host)` to perform `op` on
    /// `(resource_type, name)` against the local control state machine.
    pub async fn is_authorized(
        &self,
        principal: &str,
        host: &str,
        op: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
    ) -> AclDecision {
        let sm = self.cluster().control().state_machine();
        sm.state()
            .await
            .acl_domain
            .is_authorized(principal, host, op, resource_type, resource_name)
    }
}
