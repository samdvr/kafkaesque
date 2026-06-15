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
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

use super::serde_helpers::serialize_sorted_set;

/// Kinds of resources ACLs can be attached to.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AclResourceType {
    /// The cluster itself (used for cluster-wide admin operations).
    Cluster,
    /// A topic resource.
    Topic,
    /// A consumer group resource.
    Group,
}

/// Pattern matching style for the resource name.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum AclPermissionType {
    Allow,
    Deny,
}

/// One ACL binding.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, PartialOrd, Ord)]
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
    fn matches_principal(&self, principal: &str) -> bool {
        if self.principal == "User:*" {
            // The wildcard binding must NOT authorize unauthenticated
            // (`User:ANONYMOUS`) requests; otherwise a common "grant
            // User:* describe on cluster" rule silently exposes the broker
            // to TCP clients that never completed SASL.
            return principal != "User:ANONYMOUS";
        }
        self.principal == principal
    }

    fn matches_host(&self, host: &str) -> bool {
        // Normalize both sides before comparing strings or parsing as IPs.
        // Inbound `host` is sourced from `SocketAddr` / `peer_addr()` which
        // can carry:
        //   * IPv6 zone ids: `fe80::1%eth0`
        //   * Bracketed IPv6 + port: `[fe80::1]:9092`
        //   * Plain IPv4 + port: `1.2.3.4:9092`
        // Without normalization, a literal ACL `host: "fe80::1"` matches none
        // of these (the rule string lacks the zone/port suffix), so a
        // "deny everyone except 1.2.3.4" intent silently fails open or closed
        // depending on rule order. Strip the suffixes and re-parse.
        let rule_norm = normalize_host_for_match(&self.host);
        let host_norm = normalize_host_for_match(host);
        if self.host == "*" || rule_norm == host_norm {
            return true;
        }
        if let (Ok(rule_ip), Ok(host_ip)) = (
            rule_norm.parse::<std::net::IpAddr>(),
            host_norm.parse::<std::net::IpAddr>(),
        ) {
            let rule_canonical = match rule_ip {
                std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                    Some(v4) => std::net::IpAddr::V4(v4),
                    None => std::net::IpAddr::V6(v6),
                },
                v => v,
            };
            let host_canonical = match host_ip {
                std::net::IpAddr::V6(v6) => match v6.to_ipv4_mapped() {
                    Some(v4) => std::net::IpAddr::V4(v4),
                    None => std::net::IpAddr::V6(v6),
                },
                v => v,
            };
            return rule_canonical == host_canonical;
        }
        false
    }
}

/// Strip IPv6 zone ids and trailing port suffixes from a host literal,
/// returning a stable form suitable for ACL comparison and `IpAddr::parse`.
///
/// Handles the four shapes the dispatcher actually feeds us:
/// * `fe80::1%eth0`   -> `fe80::1`        (zone id stripped)
/// * `[fe80::1]:9092` -> `fe80::1`        (brackets and port stripped)
/// * `[fe80::1]`      -> `fe80::1`        (brackets only)
/// * `1.2.3.4:9092`   -> `1.2.3.4`        (port stripped)
/// * `1.2.3.4`        -> `1.2.3.4`        (unchanged)
/// * `host.example`   -> `host.example`   (unchanged — not an IP)
///
/// A bare IPv6 literal like `fe80::1` is left alone (the `::` triggers no
/// port-strip because there is no surrounding `[]`). Wildcard `*` and other
/// non-IP rule strings pass through untouched.
fn normalize_host_for_match(host: &str) -> &str {
    let host = host.trim();
    // Bracketed IPv6: `[addr]:port` or `[addr]` — extract addr.
    if let Some(rest) = host.strip_prefix('[')
        && let Some(end) = rest.find(']')
    {
        return &rest[..end];
    }
    // Strip IPv6 zone id (`%eth0`) BEFORE considering port, since zone ids
    // never include `:` and a bare-IPv6 has no port.
    let no_zone = match host.find('%') {
        Some(idx) => &host[..idx],
        None => host,
    };
    // Trailing-port strip applies to IPv4-with-port and bare hostnames.
    // A bare IPv6 literal contains multiple `:`s; only treat the LAST `:` as a
    // port separator when there is exactly one `:` (IPv4) — otherwise the
    // string is either a hostname (no port) or a bare IPv6 we must not
    // truncate. Bracketed forms were already handled above.
    if no_zone.matches(':').count() == 1
        && let Some((addr, _port)) = no_zone.rsplit_once(':')
    {
        return addr;
    }
    no_zone
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

/// Resource-keyed index over the binding set.
///
/// `is_authorized` is on the per-request hot path, so an O(N) scan of every
/// binding becomes the request bottleneck once an operator adds more than a
/// handful of rules. The index splits Allow and Deny so the Deny short-
/// circuit is checked first, and buckets Literal bindings by their exact
/// `(type, name)` so the inner walk is over a tiny matched slice instead of
/// the whole set. Prefixed bindings are kept per resource type because the
/// pattern only matches names starting with `resource_name`, and there are
/// typically few of them in practice.
#[derive(Debug, Default)]
struct AclIndex {
    literal_deny: HashMap<(AclResourceType, String), Vec<AclBinding>>,
    literal_allow: HashMap<(AclResourceType, String), Vec<AclBinding>>,
    prefixed_deny: HashMap<AclResourceType, Vec<AclBinding>>,
    prefixed_allow: HashMap<AclResourceType, Vec<AclBinding>>,
}

impl AclIndex {
    fn build(bindings: &HashSet<AclBinding>) -> Self {
        let mut idx = AclIndex::default();
        for b in bindings {
            idx.insert(b.clone());
        }
        idx
    }

    fn insert(&mut self, b: AclBinding) {
        match (b.pattern_type, b.permission) {
            (AclPatternType::Literal, AclPermissionType::Deny) => {
                self.literal_deny
                    .entry((b.resource_type, b.resource_name.clone()))
                    .or_default()
                    .push(b);
            }
            (AclPatternType::Literal, AclPermissionType::Allow) => {
                self.literal_allow
                    .entry((b.resource_type, b.resource_name.clone()))
                    .or_default()
                    .push(b);
            }
            (AclPatternType::Prefixed, AclPermissionType::Deny) => {
                self.prefixed_deny
                    .entry(b.resource_type)
                    .or_default()
                    .push(b);
            }
            (AclPatternType::Prefixed, AclPermissionType::Allow) => {
                self.prefixed_allow
                    .entry(b.resource_type)
                    .or_default()
                    .push(b);
            }
        }
    }
}

/// State for the ACL domain. Bindings live in a `HashSet` so duplicates from
/// repeated `CreateAcls` are idempotent.
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AclDomainState {
    #[serde(serialize_with = "serialize_sorted_set")]
    pub bindings: HashSet<AclBinding>,
    /// Lazily-built resource-keyed index. Skipped on (de)serialization —
    /// snapshots only carry the canonical `bindings` set, and the index is
    /// rebuilt on first authorization check after load.
    #[serde(skip)]
    index: OnceLock<AclIndex>,
}

// `OnceLock` does not derive `Clone`. The state machine clones whole
// snapshots, so we provide an explicit clone that preserves the bindings
// and lets the receiver rebuild its own index on demand.
impl Clone for AclDomainState {
    fn clone(&self) -> Self {
        Self {
            bindings: self.bindings.clone(),
            index: OnceLock::new(),
        }
    }
}

impl AclDomainState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn apply(&mut self, cmd: AclCommand) -> AclResponse {
        // Any structural change invalidates the cached index. The next
        // `is_authorized` call rebuilds it from the canonical binding set.
        let _ = self.index.take();
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
                removed.sort_by(|a, b| {
                    (
                        a.resource_type as u8,
                        &a.resource_name,
                        a.pattern_type as u8,
                        &a.principal,
                        &a.host,
                        a.operation as u8,
                        a.permission as u8,
                    )
                        .cmp(&(
                            b.resource_type as u8,
                            &b.resource_name,
                            b.pattern_type as u8,
                            &b.principal,
                            &b.host,
                            b.operation as u8,
                            b.permission as u8,
                        ))
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
        let index = self.index.get_or_init(|| AclIndex::build(&self.bindings));

        let matches = |b: &AclBinding| -> bool {
            // `matches_resource` would re-check resource_type/name; the
            // index has already narrowed by both, so we only need
            // principal + host + operation here. Prefixed bindings still
            // need the prefix match because the bucket only narrowed by
            // resource_type.
            if b.pattern_type == AclPatternType::Prefixed
                && !resource_name.starts_with(&b.resource_name)
            {
                return false;
            }
            b.matches_principal(principal) && b.matches_host(host) && b.operation.implies(op)
        };

        if let Some(bs) = index
            .literal_deny
            .get(&(resource_type, resource_name.to_string()))
        {
            for b in bs {
                if matches(b) {
                    return AclDecision::Denied;
                }
            }
        }
        if let Some(bs) = index.prefixed_deny.get(&resource_type) {
            for b in bs {
                if matches(b) {
                    return AclDecision::Denied;
                }
            }
        }

        if let Some(bs) = index
            .literal_allow
            .get(&(resource_type, resource_name.to_string()))
        {
            for b in bs {
                if matches(b) {
                    return AclDecision::Allowed;
                }
            }
        }
        if let Some(bs) = index.prefixed_allow.get(&resource_type) {
            for b in bs {
                if matches(b) {
                    return AclDecision::Allowed;
                }
            }
        }

        AclDecision::NotFound
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
                state.is_authorized("User:alice", "*", op, AclResourceType::Topic, "orders"),
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
