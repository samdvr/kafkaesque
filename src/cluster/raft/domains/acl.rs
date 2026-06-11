//! ACL domain for the Raft state machine.
//!
//! Implements deny-by-default authorization for the Kafka API. ACLs are
//! replicated through Raft so every broker sees the same rule set, and the
//! request dispatcher consults the local copy on every API call.
//!
//! ## Model
//!
//! Kafka's ACL model is roughly: `(principal, host) is (allowed | denied)
//! to perform <operation> on <resource>`. We mirror that structure but only
//! the parts the Kafkaesque API actually exposes today.
//!
//! - **Principal**: an opaque string of the form `User:<name>`. The
//!   wildcard `User:*` matches every authenticated principal.
//! - **Host**: an IP-or-pattern string. The wildcard `*` matches every
//!   client. (Real Kafka allows a list; we accept one entry per binding for
//!   now — operators add multiple bindings to express OR.)
//! - **Resource**: a `(type, name, pattern_type)` triple. We support
//!   `Cluster`, `Topic`, and `Group` resources, and `Literal` plus
//!   `Prefixed` pattern types (Kafka also has `Match` — out of scope).
//! - **Operation**: the verb being performed. We map every Kafka API key to
//!   one of these via `super::super::acl_check::operation_for_api`.
//! - **Permission**: `Allow` or `Deny`. Deny wins when both apply, matching
//!   Kafka's semantics.
//!
//! ## Evaluation
//!
//! Authorization is the responsibility of the request dispatcher; this
//! module only stores the bindings and answers
//! `is_authorized(principal, host, op, resource)`. The result is one of:
//!
//! - `Allowed` — at least one Allow binding matched and no Deny binding did
//! - `Denied` — a Deny binding matched
//! - `NotFound` — no binding matched (the dispatcher applies the
//!   deny-by-default policy from this — except for super-users, which short
//!   circuit higher up).

use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// Kinds of resources ACLs can be attached to.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AclResourceType {
    /// The cluster itself (used for cluster-wide admin operations).
    Cluster,
    /// A topic resource.
    Topic,
    /// A consumer group resource.
    Group,
}

/// Pattern matching style for the resource name.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AclPatternType {
    /// Exact-match name.
    Literal,
    /// Match any resource whose name *starts with* this name.
    Prefixed,
}

/// The set of operations ACLs can grant or deny.
///
/// Distinct from Kafka's wire-protocol operation enum so we can evolve
/// independently. The mapping table from Kafka API keys lives in
/// `super::super::acl_check`.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AclOperation {
    /// Implicit super-permission: grants all other operations on this
    /// resource. Mirrors Kafka's `All`.
    All,
    /// Read records / fetch / commit-offset.
    Read,
    /// Produce records / write.
    Write,
    /// Create the resource (topic, group).
    Create,
    /// Delete the resource (topic, group).
    Delete,
    /// Modify the resource configuration.
    Alter,
    /// Read the resource configuration / metadata.
    Describe,
    /// Cluster-wide admin actions (broker mgmt, etc.).
    ClusterAction,
    /// Idempotent producer write semantics (InitProducerId, etc.).
    IdempotentWrite,
}

impl AclOperation {
    /// Returns true if this operation grants `requested` per Kafka's implicit
    /// inclusion semantics. `All` grants every other operation.
    pub fn implies(self, requested: AclOperation) -> bool {
        if self == requested {
            return true;
        }
        match self {
            AclOperation::All => true,
            // Kafka treats `Read`, `Write`, `Delete`, `Alter` as implying
            // `Describe` on the same resource — code that can mutate or
            // consume can also see metadata.
            AclOperation::Read
            | AclOperation::Write
            | AclOperation::Delete
            | AclOperation::Alter => requested == AclOperation::Describe,
            _ => false,
        }
    }
}

/// Allow vs. deny. Deny wins when both apply on the same request.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum AclPermissionType {
    Allow,
    Deny,
}

/// One ACL binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct AclBinding {
    pub resource_type: AclResourceType,
    pub resource_name: String,
    pub pattern_type: AclPatternType,
    /// Principal as `User:<name>`; `User:*` is a wildcard.
    pub principal: String,
    /// Client host pattern (`*` for any).
    pub host: String,
    pub operation: AclOperation,
    pub permission: AclPermissionType,
}

impl AclBinding {
    /// Does this binding's resource pattern match `name`?
    fn matches_resource(
        &self,
        resource_type: AclResourceType,
        name: &str,
    ) -> bool {
        if self.resource_type != resource_type {
            return false;
        }
        match self.pattern_type {
            AclPatternType::Literal => self.resource_name == name,
            AclPatternType::Prefixed => name.starts_with(&self.resource_name),
        }
    }

    fn matches_principal(&self, principal: &str) -> bool {
        self.principal == "User:*" || self.principal == principal
    }

    fn matches_host(&self, host: &str) -> bool {
        self.host == "*" || self.host == host
    }
}

/// Filter for `DeleteAcls` / `DescribeAcls`. Each field is an optional
/// constraint; `None` matches every value.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct AclFilter {
    pub resource_type: Option<AclResourceType>,
    pub resource_name: Option<String>,
    pub pattern_type: Option<AclPatternType>,
    pub principal: Option<String>,
    pub host: Option<String>,
    pub operation: Option<AclOperation>,
    pub permission: Option<AclPermissionType>,
}

impl AclFilter {
    fn matches(&self, b: &AclBinding) -> bool {
        if let Some(t) = self.resource_type
            && t != b.resource_type
        {
            return false;
        }
        if let Some(n) = &self.resource_name
            && n != &b.resource_name
        {
            return false;
        }
        if let Some(p) = self.pattern_type
            && p != b.pattern_type
        {
            return false;
        }
        if let Some(p) = &self.principal
            && p != &b.principal
        {
            return false;
        }
        if let Some(h) = &self.host
            && h != &b.host
        {
            return false;
        }
        if let Some(o) = self.operation
            && o != b.operation
        {
            return false;
        }
        if let Some(p) = self.permission
            && p != b.permission
        {
            return false;
        }
        true
    }
}

/// Commands for the ACL domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AclCommand {
    /// Add ACL bindings (idempotent — already-present bindings are kept once).
    CreateAcls { bindings: Vec<AclBinding> },
    /// Remove every binding matching any of the given filters.
    DeleteAcls { filters: Vec<AclFilter> },
}

/// Responses from ACL commands.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AclResponse {
    /// `CreateAcls` finished. `created` counts only new entries; duplicates
    /// are not counted.
    Created { created: usize },
    /// `DeleteAcls` finished. The matched bindings are returned so the
    /// caller can echo them in the wire response.
    Deleted { removed: Vec<AclBinding> },
}

/// Result of an authorization check against the binding set.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AclDecision {
    /// At least one Allow binding matched and no Deny binding did.
    Allowed,
    /// A Deny binding matched. Always wins over Allow.
    Denied,
    /// No binding matched at all. The caller decides whether to apply
    /// deny-by-default (production) or allow-by-default (legacy / dev).
    NotFound,
}

/// State for the ACL domain. Bindings live in a `HashSet` so duplicates from
/// repeated `CreateAcls` are idempotent.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct AclDomainState {
    pub bindings: HashSet<AclBinding>,
}

impl AclDomainState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, cmd: AclCommand) -> AclResponse {
        match cmd {
            AclCommand::CreateAcls { bindings } => {
                let mut created = 0;
                for b in bindings {
                    if self.bindings.insert(b) {
                        created += 1;
                    }
                }
                AclResponse::Created { created }
            }
            AclCommand::DeleteAcls { filters } => {
                let mut removed = Vec::new();
                self.bindings.retain(|b| {
                    let matched = filters.iter().any(|f| f.matches(b));
                    if matched {
                        removed.push(b.clone());
                        false
                    } else {
                        true
                    }
                });
                AclResponse::Deleted { removed }
            }
        }
    }

    /// Authorize `(principal, host)` to perform `op` on `(resource_type,
    /// name)`. Deny wins; otherwise any Allow grants; otherwise `NotFound`.
    pub fn is_authorized(
        &self,
        principal: &str,
        host: &str,
        op: AclOperation,
        resource_type: AclResourceType,
        resource_name: &str,
    ) -> AclDecision {
        let mut allow = false;
        for b in &self.bindings {
            if !b.matches_resource(resource_type, resource_name) {
                continue;
            }
            if !b.matches_principal(principal) {
                continue;
            }
            if !b.matches_host(host) {
                continue;
            }
            if !b.operation.implies(op) {
                continue;
            }
            match b.permission {
                AclPermissionType::Deny => return AclDecision::Denied,
                AclPermissionType::Allow => allow = true,
            }
        }
        if allow {
            AclDecision::Allowed
        } else {
            AclDecision::NotFound
        }
    }

    /// List bindings matching `filter`.
    pub fn describe(&self, filter: &AclFilter) -> Vec<AclBinding> {
        self.bindings
            .iter()
            .filter(|b| filter.matches(b))
            .cloned()
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn binding(
        resource_type: AclResourceType,
        name: &str,
        principal: &str,
        op: AclOperation,
        permission: AclPermissionType,
    ) -> AclBinding {
        AclBinding {
            resource_type,
            resource_name: name.to_string(),
            pattern_type: AclPatternType::Literal,
            principal: principal.to_string(),
            host: "*".to_string(),
            operation: op,
            permission,
        }
    }

    #[test]
    fn empty_state_returns_not_found() {
        let state = AclDomainState::new();
        let decision = state.is_authorized(
            "User:alice",
            "1.2.3.4",
            AclOperation::Read,
            AclResourceType::Topic,
            "orders",
        );
        assert_eq!(decision, AclDecision::NotFound);
    }

    #[test]
    fn allow_binding_grants_exact_match() {
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls {
            bindings: vec![binding(
                AclResourceType::Topic,
                "orders",
                "User:alice",
                AclOperation::Read,
                AclPermissionType::Allow,
            )],
        });
        assert_eq!(
            state.is_authorized(
                "User:alice",
                "1.2.3.4",
                AclOperation::Read,
                AclResourceType::Topic,
                "orders"
            ),
            AclDecision::Allowed
        );
    }

    #[test]
    fn deny_overrides_allow() {
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls {
            bindings: vec![
                binding(
                    AclResourceType::Topic,
                    "orders",
                    "User:alice",
                    AclOperation::Read,
                    AclPermissionType::Allow,
                ),
                binding(
                    AclResourceType::Topic,
                    "orders",
                    "User:alice",
                    AclOperation::Read,
                    AclPermissionType::Deny,
                ),
            ],
        });
        assert_eq!(
            state.is_authorized(
                "User:alice",
                "*",
                AclOperation::Read,
                AclResourceType::Topic,
                "orders"
            ),
            AclDecision::Denied
        );
    }

    #[test]
    fn all_implies_specific_operations() {
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls {
            bindings: vec![binding(
                AclResourceType::Topic,
                "orders",
                "User:alice",
                AclOperation::All,
                AclPermissionType::Allow,
            )],
        });
        for op in [
            AclOperation::Read,
            AclOperation::Write,
            AclOperation::Describe,
            AclOperation::Delete,
        ] {
            assert_eq!(
                state.is_authorized(
                    "User:alice",
                    "*",
                    op,
                    AclResourceType::Topic,
                    "orders"
                ),
                AclDecision::Allowed,
                "All should imply {:?}",
                op
            );
        }
    }

    #[test]
    fn read_implies_describe() {
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls {
            bindings: vec![binding(
                AclResourceType::Topic,
                "orders",
                "User:alice",
                AclOperation::Read,
                AclPermissionType::Allow,
            )],
        });
        assert_eq!(
            state.is_authorized(
                "User:alice",
                "*",
                AclOperation::Describe,
                AclResourceType::Topic,
                "orders"
            ),
            AclDecision::Allowed
        );
    }

    #[test]
    fn principal_wildcard_matches_everyone() {
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls {
            bindings: vec![binding(
                AclResourceType::Topic,
                "orders",
                "User:*",
                AclOperation::Read,
                AclPermissionType::Allow,
            )],
        });
        assert_eq!(
            state.is_authorized(
                "User:bob",
                "*",
                AclOperation::Read,
                AclResourceType::Topic,
                "orders"
            ),
            AclDecision::Allowed
        );
    }

    #[test]
    fn prefixed_pattern_matches_prefix() {
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls {
            bindings: vec![AclBinding {
                resource_type: AclResourceType::Topic,
                resource_name: "orders-".to_string(),
                pattern_type: AclPatternType::Prefixed,
                principal: "User:alice".to_string(),
                host: "*".to_string(),
                operation: AclOperation::Write,
                permission: AclPermissionType::Allow,
            }],
        });
        assert_eq!(
            state.is_authorized(
                "User:alice",
                "*",
                AclOperation::Write,
                AclResourceType::Topic,
                "orders-2026"
            ),
            AclDecision::Allowed
        );
        assert_eq!(
            state.is_authorized(
                "User:alice",
                "*",
                AclOperation::Write,
                AclResourceType::Topic,
                "billing-2026"
            ),
            AclDecision::NotFound
        );
    }

    #[test]
    fn create_is_idempotent() {
        let mut state = AclDomainState::new();
        let b = binding(
            AclResourceType::Topic,
            "orders",
            "User:alice",
            AclOperation::Read,
            AclPermissionType::Allow,
        );
        let first = state.apply(AclCommand::CreateAcls {
            bindings: vec![b.clone()],
        });
        assert_eq!(first, AclResponse::Created { created: 1 });
        let second = state.apply(AclCommand::CreateAcls { bindings: vec![b] });
        assert_eq!(second, AclResponse::Created { created: 0 });
    }

    #[test]
    fn delete_with_filter_returns_removed() {
        let mut state = AclDomainState::new();
        state.apply(AclCommand::CreateAcls {
            bindings: vec![
                binding(
                    AclResourceType::Topic,
                    "orders",
                    "User:alice",
                    AclOperation::Read,
                    AclPermissionType::Allow,
                ),
                binding(
                    AclResourceType::Topic,
                    "billing",
                    "User:alice",
                    AclOperation::Read,
                    AclPermissionType::Allow,
                ),
            ],
        });
        let resp = state.apply(AclCommand::DeleteAcls {
            filters: vec![AclFilter {
                resource_name: Some("orders".to_string()),
                ..Default::default()
            }],
        });
        match resp {
            AclResponse::Deleted { removed } => {
                assert_eq!(removed.len(), 1);
                assert_eq!(removed[0].resource_name, "orders");
            }
            _ => panic!("Expected Deleted"),
        }
        assert_eq!(state.bindings.len(), 1);
    }
}
